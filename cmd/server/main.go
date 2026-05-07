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
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // 注册 /debug/pprof/* 路由到 http.DefaultServeMux
	"os"
	"os/signal"
	"sync"
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

	eventBuf := event.NewBuffer(clickRepo, event.BufferConfig{
		Capacity:      cfg.EventBufferCapacity,
		BatchSize:     cfg.EventBufferBatchSize,
		FlushInterval: cfg.EventBufferFlushInterval,
	})
	eventBuf.Start()
	slog.Info("event buffer started",
		"capacity", cfg.EventBufferCapacity,
		"batch_size", cfg.EventBufferBatchSize,
		"flush_interval", cfg.EventBufferFlushInterval)

	// 把 event buffer 的 stats 接入 Prometheus
	metricsReg.BindEventBuffer(metrics.EventBufferGetters{
		Enqueued: func() float64 { return float64(eventBuf.Stats().Enqueued) },
		Dropped:  func() float64 { return float64(eventBuf.Stats().Dropped) },
		Flushed:  func() float64 { return float64(eventBuf.Stats().Flushed) },
		FlushErr: func() float64 { return float64(eventBuf.Stats().FlushErr) },
		Used:     func() float64 { return float64(eventBuf.Stats().Used) },
		Capacity: func() float64 { return float64(eventBuf.Stats().Capacity) },
	})
	metricsReg.BindIDGenerator(func() float64 { return generator.Stat().CurUsage })

	// ── 6. 建 API server + 路由（健康检查 + API + 跳转）─────
	//
	// fasthttp/router：静态路由（/healthz / /readyz）优先级高于参数路由（/{code}），
	// 不会被跳转处理器误吞。
	apiSrv := api.NewServer(
		api.Config{BaseURL: cfg.BaseURL},
		generator, linkRepo, linkCache, eventBuf,
	)
	r := apiSrv.Routes()
	r.GET("/healthz", livenessHandler)
	r.GET("/readyz", readinessHandler(pgPool, redisCli))

	// ── 6.5 fasthttp 主 server ────────────────────────────
	//
	// 关键参数：
	//   Name              暴露给 Server 响应头，便于运维识别版本
	//   MaxRequestBodySize 16KB 远小于默认 4MB，防 DoS
	//   ReadTimeout/WriteTimeout 10s 防慢速攻击
	//   IdleTimeout 60s    keep-alive 复用上限
	// Day 10: 包一层 Prometheus middleware（counter + histogram）
	rootHandler := metricsReg.FastHTTPMiddleware(r.Handler)

	httpSrv := &fasthttp.Server{
		Handler:            rootHandler,
		Name:               "slink/" + version,
		MaxRequestBodySize: maxRequestBodySize,
		ReadTimeout:        10 * time.Second,
		WriteTimeout:       10 * time.Second,
		IdleTimeout:        60 * time.Second,
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
		http.HandleFunc("/debug/stats", statsHandler(linkCache, eventBuf, generator, time.Now()))
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

	// 优雅停机顺序：
	//   1. fasthttp.Server ShutdownWithContext：停接新连接 + 等已有请求完成
	//   2. EventBuffer Stop：drain channel + 最后一次 flush
	//   3. defer 链关闭 redisCli / pgPool（在 run() 顶部 defer）
	// 反过来：先关 PG → buffer flush 时无 sink → 残余事件丢失
	if err := httpSrv.ShutdownWithContext(shutdownCtx); err != nil {
		slog.Error("http server shutdown", "err", err)
	}
	if err := eventBuf.Stop(shutdownCtx); err != nil {
		slog.Error("event buffer shutdown", "err", err)
	}
	slog.Info("event buffer stats", "stats", eventBuf.Stats())
	slog.Info("bye")
	return nil
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

// ──────────────────────────────────────────────────────────
//                       健康检查
// ──────────────────────────────────────────────────────────
//
// /healthz —— Kubernetes liveness probe
//   只返回 "进程是否还活着"。绝不依赖外部组件。
//   用途：Pod 卡死时 K8s 会重启它。
//
// /readyz  —— Kubernetes readiness probe
//   返回 "服务是否准备好接流量"，依赖检查 PG + Redis。
//   用途：依赖暂时不可达时 LB 把这个 Pod 摘掉，但 Pod 不重启。

type readyResp struct {
	Status   string            `json:"status"`
	Version  string            `json:"version"`
	Backends map[string]string `json:"backends"`
}

func livenessHandler(ctx *fasthttp.RequestCtx) {
	ctx.Response.Header.Set("Content-Type", "application/json")
	_ = json.NewEncoder(ctx).Encode(map[string]string{
		"status":  "ok",
		"version": version,
	})
}

// readinessHandler 并行 ping 所有依赖。
// 全部成功才返回 200，任何一个失败返回 503。
func readinessHandler(pg interface {
	Ping(context.Context) error
}, rd *cache.Client) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		var (
			wg  sync.WaitGroup
			mu  sync.Mutex
			ok  = true
			res = make(map[string]string, 2)
		)
		check := func(name string, fn func() error) {
			defer wg.Done()
			if err := fn(); err != nil {
				mu.Lock()
				ok = false
				res[name] = "fail: " + err.Error()
				mu.Unlock()
				return
			}
			mu.Lock()
			res[name] = "ok"
			mu.Unlock()
		}

		wg.Add(2)
		go check("postgres", func() error { return pg.Ping(probeCtx) })
		go check("redis", func() error { return rd.Ping(probeCtx) })
		wg.Wait()

		ctx.Response.Header.Set("Content-Type", "application/json")
		if !ok {
			ctx.SetStatusCode(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(ctx).Encode(readyResp{
			Status:   statusFor(ok),
			Version:  version,
			Backends: res,
		})
	}
}

func statusFor(ok bool) string {
	if ok {
		return "ok"
	}
	return "degraded"
}

// ──────────────────────────────────────────────────────────
//                       /debug/stats（Day 9）
// ──────────────────────────────────────────────────────────
//
// 一站式观测：L1 命中率 + event buffer 状态 + ID 号段进度 + uptime。
// 仅挂 admin 端口（默认 127.0.0.1:6060），不暴露给外网。
//
// 用途：
//  1. bench 后核对 L1 hit rate（不再靠"profile 间接估"）
//  2. 监控 event buffer Used/Capacity，提前预警 dropped
//  3. 简历讲故事时有现成数字（"L1 命中 99.7% / dropped 0 / uptime 2 小时"）

type localCacheStatsView struct {
	Hits    uint64  `json:"hits"`
	Misses  uint64  `json:"misses"`
	HitRate float64 `json:"hit_rate"` // 计算字段，便于一眼看出
}

type linkCacheStats struct {
	L1 localCacheStatsView `json:"l1"`
}

type statsResp struct {
	Version       string          `json:"version"`
	UptimeSeconds int64           `json:"uptime_seconds"`
	LinkCache     linkCacheStats  `json:"link_cache"`
	EventBuffer   event.Stats     `json:"event_buffer"`
	IDGenerator   id.BufferStat   `json:"id_generator"`
}

func statsHandler(
	lc *cache.LinkCache,
	eb *event.Buffer,
	gen *id.Generator,
	startTime time.Time,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l1 := lc.LocalStats()
		hitRate := 0.0
		if total := l1.Hits + l1.Misses; total > 0 {
			hitRate = float64(l1.Hits) / float64(total)
		}

		resp := statsResp{
			Version:       version,
			UptimeSeconds: int64(time.Since(startTime).Seconds()),
			LinkCache: linkCacheStats{
				L1: localCacheStatsView{
					Hits:    l1.Hits,
					Misses:  l1.Misses,
					HitRate: hitRate,
				},
			},
			EventBuffer: eb.Stats(),
			IDGenerator: gen.Stat(),
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}
