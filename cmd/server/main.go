// Package main 是 slink 服务端入口。
//
// 职责仅限"装配"：
//  1. 加载 config
//  2. 建 store / cache 客户端（启动时验证依赖可达）
//  3. 建发号器（号段双 buffer + Base62 Generator）
//  4. 注册 HTTP 路由（健康检查 + API + 跳转）
//  5. 启动 server，监听信号优雅停机
//
// v0.2 关键变化：主端口从 net/http 切到 valyala/fasthttp，
// 但 pprof 仍保留在 net/http :6060（外部 go tool pprof / curl 兼容）。
//
// 业务逻辑全部住在 internal/* 下，main 不写任何业务分支。
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // 注册 /debug/pprof/* 路由到 http.DefaultServeMux
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp"

	"github.com/zombiecd/slink/internal/api"
	"github.com/zombiecd/slink/internal/cache"
	"github.com/zombiecd/slink/internal/config"
	"github.com/zombiecd/slink/internal/event"
	"github.com/zombiecd/slink/internal/id"
	"github.com/zombiecd/slink/internal/metrics"
	"github.com/zombiecd/slink/internal/store"
)

const (
	version       = "v0.3-day10"
	shutdownGrace = 10 * time.Second

	// 主端口允许的最大请求体（POST /api/links 仅吃几 KB JSON）。
	// 远小于 fasthttp 默认 4MB，给 SSRF / DoS body 保险。
	maxRequestBodySize = 16 * 1024 // 16 KB

	// fasthttp DoS 防护三件套（v0.3 hardening）：
	//   ReadTimeout/WriteTimeout 已限单请求时长，
	//   但单 IP 仍可开任意多 keep-alive 连接慢慢吐数据，
	//   或单连接复用打到默认 Concurrency=256k。
	// 这三个上限把"单 IP 滥用"和"全局并发"都封顶。
	// 值偏保守：当短链 RPS 已破 8w，单 IP 1k/连接和 16k 全局并发都远高于业务上限。
	maxConnsPerIP      = 1000
	maxRequestsPerConn = 10000
	maxConcurrency     = 16384
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	// ── 1. 加载配置 ────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// ── 2. 初始化 logger（slog JSON / text 取决于 env）────────
	logger := newLogger(cfg)
	slog.SetDefault(logger)
	slog.Info("starting slink",
		"version", version,
		"env", cfg.Env,
		"addr", cfg.Addr,
	)

	// ── 3. 建立依赖（store / cache），启动时验证可达 ─────────
	bootCtx, bootCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer bootCancel()

	pgPool, err := store.NewPool(bootCtx, store.PoolConfig{
		DSN:      cfg.PGDSN,
		MaxConns: cfg.PGMaxConns,
		MinConns: cfg.PGMinConns,
	})
	if err != nil {
		return err
	}
	defer pgPool.Close()
	slog.Info("postgres connected", "max_conns", cfg.PGMaxConns, "min_conns", cfg.PGMinConns)

	redisCli, err := cache.NewClient(bootCtx, cache.ClientConfig{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	if err != nil {
		return err
	}
	defer redisCli.Close()
	slog.Info("redis connected", "addr", cfg.RedisAddr)

	// ── 4. 建发号器 ────────────────────────────────────────
	segRepo := store.NewSegmentRepo(pgPool)
	buf, err := id.NewDoubleBuffer(bootCtx, segRepo, cfg.IDBizTag, cfg.IDStepSize, 0.9, logger)
	if err != nil {
		return err
	}
	generator := id.NewGenerator(buf)
	slog.Info("id generator ready",
		"biz_tag", cfg.IDBizTag,
		"step_size", cfg.IDStepSize)

	// ── 5. 建 cache + 异步事件链路 ─────────────────────────
	linkRepo := store.NewLinkRepo(pgPool)
	clickRepo := store.NewClickEventRepo(pgPool)
	// L1（进程内 LRU）默认开 4096 entries / 1min TTL；SLINK_LOCAL_CACHE_SIZE<=0 时禁用。
	linkCache := cache.NewLinkCache(redisCli,
		cache.WithLocalCache(cfg.LocalCacheSize, cfg.LocalCacheTTL),
	)
	slog.Info("link cache ready",
		"l1_size", cfg.LocalCacheSize,
		"l1_ttl", cfg.LocalCacheTTL)

	// ── Day 10: Prometheus metrics 注册 ──────────────────
	// 通过闭包注入业务对象的 Stats getter，metrics 包零依赖业务包。
	metricsReg := metrics.New()
	metricsReg.BindLocalCache(
		func() float64 { return float64(linkCache.LocalStats().Hits) },
		func() float64 { return float64(linkCache.LocalStats().Misses) },
	)

	// ── v0.4 Day 16 切流后：单 Kafka backend ────────────────
	// buffer/dual 模式已删除（git tag v0.3-buffer-final 留代码考古）。
	// EventBackend 字段保留接受 "kafka" 单值，给 v0.5 加新 backend 留扩展点。
	//
	// clickRepo 这里没用上 — Day 16 切流后主表写入由 cmd/consumer 负责，server 只投 Kafka。
	_ = clickRepo
	kafkaProducer, eventer, err := buildEventBackend(cfg, metricsReg)
	if err != nil {
		return err
	}
	metricsReg.BindIDGenerator(func() float64 { return generator.Stat().CurUsage })

	// ── 6. 建 API server + 路由（健康检查 + API + 跳转）─────
	//
	// fasthttp/router：静态路由（/healthz / /readyz）优先级高于参数路由（/{code}），
	// 不会被跳转处理器误吞。
	apiSrv := api.NewServer(
		api.Config{
			BaseURL:        cfg.BaseURL,
			TrustedProxies: cfg.TrustedProxies,
		},
		generator, linkRepo, linkCache, eventer,
	)
	r := apiSrv.Routes()
	r.GET("/healthz", api.Liveness(version))
	r.GET("/readyz", api.Readiness(version, pgPool, redisCli))

	// ── 6.5 fasthttp 主 server ────────────────────────────
	//
	// 关键参数：
	//   Name              暴露给 Server 响应头，便于运维识别版本
	//   MaxRequestBodySize 16KB 远小于默认 4MB，防 DoS
	//   ReadTimeout/WriteTimeout 10s 防慢速攻击
	//   IdleTimeout 60s    keep-alive 复用上限
	//   MaxConnsPerIP / MaxRequestsPerConn / Concurrency
	//                      v0.3 hardening：封单 IP 连接数 + 单连接复用 + 全局并发
	// Day 10: 包一层 Prometheus middleware（counter + histogram）
	rootHandler := metricsReg.FastHTTPMiddleware(r.Handler)

	httpSrv := &fasthttp.Server{
		Handler:            rootHandler,
		Name:               "slink/" + version,
		MaxRequestBodySize: maxRequestBodySize,
		ReadTimeout:        10 * time.Second,
		WriteTimeout:       10 * time.Second,
		IdleTimeout:        60 * time.Second,
		MaxConnsPerIP:      maxConnsPerIP,
		MaxRequestsPerConn: maxRequestsPerConn,
		Concurrency:        maxConcurrency,
	}

	// ── 6.6 pprof + admin 单独端口（仍用 net/http，外部工具兼容）────
	//
	// 为什么不迁 fasthttp：
	//   1. net/http/pprof 是标准库，go tool pprof / curl / 浏览器都直接认它
	//   2. fasthttpadaptor.NewFastHTTPHandler(http.DefaultServeMux) 能跑，
	//      但徒增一层 net/http ↔ fasthttp 适配开销，pprof 又不在 hot path
	//   3. pprof 端口本来就是低频访问 + 仅本机绑定，性能不重要
	// 业界标准（Kubernetes / Prometheus / Etcd 都把 pprof 单独绑）。
	//
	// /debug/stats（Day 9 新增）也挂这个 admin 端口：
	//   admin 数据不应跟生产流量同端口，避免被外网拉到 → 沿用 pprof 的本地绑定模型。
	if cfg.PProfAddr != "" {
		http.HandleFunc("/debug/stats", api.Stats(version, linkCache, kafkaProducer, generator, time.Now()))
		// Day 10: Prometheus /metrics 挂同一个 admin 端口
		http.Handle("/metrics", promhttp.HandlerFor(metricsReg.Registry, promhttp.HandlerOpts{
			EnableOpenMetrics: true,
		}))

		pprofSrv := &http.Server{
			Addr:              cfg.PProfAddr,
			Handler:           http.DefaultServeMux, // pprof init() + /debug/stats + /metrics
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			slog.Info("admin listening (pprof + /debug/stats + /metrics)", "addr", cfg.PProfAddr)
			if err := pprofSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("admin server", "err", err)
			}
		}()
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = pprofSrv.Shutdown(ctx)
		}()
	}

	// ── 7. 启动 + 优雅停机 ─────────────────────────────────
	serveErr := make(chan error, 1)
	go func() {
		slog.Info("listening (fasthttp)", "addr", cfg.Addr)
		if err := httpSrv.ListenAndServe(cfg.Addr); err != nil {
			serveErr <- err
		}
		close(serveErr)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serveErr:
		return err
	case sig := <-stop:
		slog.Info("shutdown signal received", "signal", sig.String())
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()

	// 优雅停机顺序（v0.4 Day 16 切流后）：
	//   1. fasthttp.Server ShutdownWithContext：停接新连接 + 等已有请求完成
	//   2. KafkaProducer Close：Flush 在飞 record
	//   3. defer 链关闭 redisCli / pgPool
	if err := httpSrv.ShutdownWithContext(shutdownCtx); err != nil {
		slog.Error("http server shutdown", "err", err)
	}
	if kafkaProducer != nil {
		if err := kafkaProducer.Close(shutdownCtx); err != nil {
			slog.Error("kafka producer close", "err", err)
		}
		slog.Info("kafka producer stats", "stats", kafkaProducer.Stats())
	}
	slog.Info("bye")
	return nil
}

// buildEventBackend 装配事件后端 + 注册 Prometheus metrics。
//
// v0.4 Day 16 切流后只剩 kafka 单档（buffer / dual 模式已删，
// 代码考古见 git tag v0.3-buffer-final）。EventBackend 字段保留是给
// v0.5 加新 backend（如 ClickHouse direct）留 switch 扩展位。
//
// 返回：
//   - kafkaProducer：admin /debug/stats + 优雅停机 close 用
//   - eventer：装到 api.Server 的 Eventer 接口
func buildEventBackend(
	cfg *config.Config,
	metricsReg *metrics.Registry,
) (*event.KafkaProducer, event.Eventer, error) {
	switch cfg.EventBackend {
	case "kafka":
		kp, err := newKafkaAndBind(cfg, metricsReg)
		if err != nil {
			return nil, nil, err
		}
		return kp, kp, nil

	default:
		return nil, nil, fmt.Errorf("unknown EventBackend %q (only \"kafka\" supported after Day 16 cutover)", cfg.EventBackend)
	}
}

// newKafkaAndBind 构造 KafkaProducer + Prometheus 绑定（不启动 — kgo client 创建即连）。
func newKafkaAndBind(
	cfg *config.Config,
	metricsReg *metrics.Registry,
) (*event.KafkaProducer, error) {
	kp, err := event.NewKafkaProducer(event.KafkaConfig{
		Brokers:               cfg.KafkaBrokers,
		Topic:                 cfg.KafkaTopic,
		SendTimeout:           cfg.KafkaSendTimeout,
		MaxBufferedRecords:    cfg.KafkaMaxBufferedRecs,
		RecordDeliveryTimeout: cfg.KafkaDeliveryTimeout,
	})
	if err != nil {
		return nil, err
	}
	slog.Info("kafka producer ready",
		"brokers", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"send_timeout", cfg.KafkaSendTimeout,
		"max_buffered", cfg.KafkaMaxBufferedRecs,
	)

	metricsReg.BindKafkaProducer(metrics.KafkaProducerGetters{
		Sent:    func() float64 { return float64(kp.Stats().Sent) },
		Acked:   func() float64 { return float64(kp.Stats().Acked) },
		Dropped: func() float64 { return float64(kp.Stats().Dropped) },
		Errors:  func() float64 { return float64(kp.Stats().Errors) },
		Healthy: func() float64 {
			if kp.Stats().Healthy {
				return 1
			}
			return 0
		},
	})
	return kp, nil
}

func newLogger(cfg *config.Config) *slog.Logger {
	level := slog.LevelInfo
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}

	if cfg.IsDev() {
		// dev 用更易读的 text handler
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	// 生产用 JSON 便于聚合
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

