// Package main 是 slink Kafka consumer 入口（v0.4 Day 15 起）。
//
// 职责：
//  1. 订阅 Kafka topic slink.click_events
//  2. 攒批（≤1000 条 / 100ms）
//  3. 写下游 PG 表（默认 click_events_shadow，Day 16 切流后改 click_events）
//  4. 暴露 /metrics + /healthz
//  5. SIGTERM 优雅停机：停 poll → 最后一批 flush → close kgo client → close pool
//
// 与 cmd/server 故障域天然分离 — consumer 挂不影响主路径跳转 RPS。
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/zombiecd/slink/internal/event"
	"github.com/zombiecd/slink/internal/metrics"
	"github.com/zombiecd/slink/internal/store"
)

const (
	version       = "v0.4-day15-consumer"
	shutdownGrace = 10 * time.Second
)

// consumerConfig 是 consumer 独立配置（不复用 config.Config，
// 避免被 RedisAddr / TrustedProxies 等主路径才需要的字段卡住）。
type consumerConfig struct {
	PGDSN          string
	KafkaBrokers   []string
	KafkaTopic     string
	GroupID        string
	Table          string // 影子表 click_events_shadow / 切流后改 click_events
	BatchSize      int
	BatchTimeout   time.Duration
	SessionTimeout time.Duration
	HTTPAddr       string // /metrics + /healthz
	LogLevel       string
	EnvName        string // dev / prod
}

func loadConfig() (*consumerConfig, error) {
	_ = godotenv.Load() // .env 不存在不报错

	cfg := &consumerConfig{
		PGDSN:          os.Getenv("SLINK_PG_DSN"),
		KafkaTopic:     getenvDefault("SLINK_KAFKA_TOPIC", "slink.click_events"),
		GroupID:        getenvDefault("SLINK_CONSUMER_GROUP", "slink.click_events.pg_writer"),
		Table:          getenvDefault("SLINK_CONSUMER_TABLE", "click_events_shadow"),
		BatchSize:      mustAtoiDefault(os.Getenv("SLINK_CONSUMER_BATCH_SIZE"), 1000),
		BatchTimeout:   mustDurationDefault(os.Getenv("SLINK_CONSUMER_BATCH_TIMEOUT"), 100*time.Millisecond),
		SessionTimeout: mustDurationDefault(os.Getenv("SLINK_CONSUMER_SESSION_TIMEOUT"), 30*time.Second),
		HTTPAddr:       getenvDefault("SLINK_CONSUMER_HTTP_ADDR", ":18081"),
		LogLevel:       getenvDefault("SLINK_LOG_LEVEL", "info"),
		EnvName:        getenvDefault("SLINK_ENV", "dev"),
	}

	rawBrokers := strings.TrimSpace(os.Getenv("SLINK_KAFKA_BROKERS"))
	if rawBrokers == "" {
		return nil, errors.New("SLINK_KAFKA_BROKERS is required (comma-separated host:port)")
	}
	for _, p := range strings.Split(rawBrokers, ",") {
		if s := strings.TrimSpace(p); s != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, s)
		}
	}

	if cfg.PGDSN == "" {
		return nil, errors.New("SLINK_PG_DSN is required")
	}
	if cfg.Table == "" {
		return nil, errors.New("SLINK_CONSUMER_TABLE must not be empty")
	}
	return cfg, nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	logger := newLogger(cfg)
	slog.SetDefault(logger)
	slog.Info("starting slink consumer",
		"version", version,
		"env", cfg.EnvName,
		"brokers", cfg.KafkaBrokers,
		"topic", cfg.KafkaTopic,
		"group", cfg.GroupID,
		"table", cfg.Table,
		"batch_size", cfg.BatchSize,
	)

	// ── PG pool（consumer 写库专用，不复用 server 的 pool）─────────
	bootCtx, bootCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer bootCancel()

	pgPool, err := store.NewPool(bootCtx, store.PoolConfig{
		DSN: cfg.PGDSN,
		// consumer 写量等于 producer 入量（最大 100k+ 入），
		// 但每批 1000 条 1 个连接就够。少给点。
		MaxConns: 8,
		MinConns: 2,
	})
	if err != nil {
		return fmt.Errorf("pg pool: %w", err)
	}
	defer pgPool.Close()
	slog.Info("postgres connected")

	// ── 影子表 repo（同一 ClickEventRepo struct，传 table name）────
	shadowRepo := store.NewClickEventRepoForTable(pgPool, cfg.Table)

	// ── Kafka consumer ─────────────────────────────────────────────
	consumer, err := event.NewClickEventConsumer(event.ConsumerConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.KafkaTopic,
		GroupID:        cfg.GroupID,
		BatchSize:      cfg.BatchSize,
		BatchTimeout:   cfg.BatchTimeout,
		SessionTimeout: cfg.SessionTimeout,
	}, shadowRepo)
	if err != nil {
		return fmt.Errorf("consumer init: %w", err)
	}

	// ── Prometheus metrics ─────────────────────────────────────────
	metricsReg := metrics.New()
	metricsReg.BindKafkaConsumer(metrics.KafkaConsumerGetters{
		Polled:         func() float64 { return float64(consumer.Stats().Polled) },
		Decoded:        func() float64 { return float64(consumer.Stats().Decoded) },
		Inserted:       func() float64 { return float64(consumer.Stats().Inserted) },
		DecodeErrors:   func() float64 { return float64(consumer.Stats().DecodeErrors) },
		InsertErrors:   func() float64 { return float64(consumer.Stats().InsertErrors) },
		UnknownVersion: func() float64 { return float64(consumer.Stats().UnknownVersion) },
		LagRecords:     func() float64 { return float64(consumer.Stats().LagRecords) },
	})

	// ── HTTP admin（/metrics + /healthz + /debug/stats JSON）─────
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metricsReg.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = fmt.Fprintf(w, "ok %s\n", version)
	})
	mux.HandleFunc("/debug/stats", func(w http.ResponseWriter, _ *http.Request) {
		s := consumer.Stats()
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w,
			`{"polled":%d,"decoded":%d,"inserted":%d,"decode_errors":%d,"insert_errors":%d,"unknown_version":%d,"lag_records":%d}`,
			s.Polled, s.Decoded, s.Inserted, s.DecodeErrors, s.InsertErrors, s.UnknownVersion, s.LagRecords,
		)
	})

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		slog.Info("admin listening (/metrics + /healthz + /debug/stats)", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("admin server", "err", err)
		}
	}()

	// ── 启动 consumer 循环 ─────────────────────────────────────────
	consumer.Start()
	slog.Info("consumer started")

	// ── 优雅停机 ───────────────────────────────────────────────────
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	sig := <-stop
	slog.Info("shutdown signal received", "signal", sig.String())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()

	// 顺序：先停 admin（不再接新 /metrics 抓取）→ 停 consumer（含最后一批 flush）→ pool defer 关
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http server shutdown", "err", err)
	}
	if err := consumer.Stop(shutdownCtx); err != nil {
		slog.Error("consumer stop", "err", err)
	}
	slog.Info("consumer stats", "stats", consumer.Stats())
	slog.Info("bye")
	return nil
}

func getenvDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustAtoiDefault(raw string, def int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func mustDurationDefault(raw string, def time.Duration) time.Duration {
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

func newLogger(cfg *consumerConfig) *slog.Logger {
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
	if cfg.EnvName == "dev" {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}
