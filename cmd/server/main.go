// Package main 是 slink 服务端入口。
//
// 职责仅限"装配"：
//  1. 加载 config
//  2. 建 store / cache 客户端（启动时验证依赖可达）
//  3. 建发号器（号段双 buffer + Base62 Generator）
//  4. 注册 HTTP 路由（健康检查 + API）
//  5. 启动 server，监听信号优雅停机
//
// 业务逻辑全部住在 internal/* 下，main 不写任何业务分支。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/zombiecd/slink/internal/api"
	"github.com/zombiecd/slink/internal/cache"
	"github.com/zombiecd/slink/internal/config"
	"github.com/zombiecd/slink/internal/id"
	"github.com/zombiecd/slink/internal/store"
)

const (
	version       = "v0.1-day4"
	shutdownGrace = 10 * time.Second
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

	// ── 5. 建 API server ──────────────────────────────────
	linkRepo := store.NewLinkRepo(pgPool)
	apiSrv := api.NewServer(api.Config{BaseURL: cfg.BaseURL}, generator, linkRepo)

	// ── 6. 路由（健康检查 + API）──────────────────────────
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", livenessHandler)
	mux.HandleFunc("/readyz", readinessHandler(pgPool, redisCli))
	// 把 api 子路由挂到主 mux（Go 1.22+ ServeMux 嵌套）
	mux.Handle("/api/", apiSrv.Routes())

	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// ── 7. 启动 + 优雅停机 ─────────────────────────────────
	serveErr := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", cfg.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown", "err", err)
	}
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

func livenessHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"version": version,
	})
}

// readinessHandler 并行 ping 所有依赖。
// 全部成功才返回 200，任何一个失败返回 503。
func readinessHandler(pg interface {
	Ping(context.Context) error
}, rd *cache.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
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
		go check("postgres", func() error { return pg.Ping(ctx) })
		go check("redis", func() error { return rd.Ping(ctx) })
		wg.Wait()

		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(readyResp{
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
