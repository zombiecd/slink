// Package config 加载并验证 slink 服务运行时配置。
//
// 设计原则（12-factor app §3）：
//   - 配置从环境变量读取，禁止从代码里硬编码
//   - 默认值放在 struct tag，便于本地开发零配置启动
//   - 必填项用 `env:"...,required"` 显式声明
//
// 优先级：环境变量 > .env 文件 > struct tag 默认值
package config

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

// Config 是 slink 服务的全部运行时配置。
//
// 任何新增配置项的步骤：
//  1. 在这里加字段（带 env tag）
//  2. 在 .env.example 加对应行
//  3. 如有跨字段约束，加到 (c *Config) Validate()
type Config struct {
	// ── HTTP 服务 ────────────────────────────────────────
	Addr     string `env:"SLINK_ADDR"      envDefault:":18080"`
	BaseURL  string `env:"SLINK_BASE_URL"  envDefault:"http://localhost:18080"`
	LogLevel string `env:"SLINK_LOG_LEVEL" envDefault:"info"`
	Env      string `env:"SLINK_ENV"       envDefault:"dev"`

	// PProfAddr 是 net/http/pprof 的监听地址。
	// 单独端口避免污染主端口、避免外部访问 profile 数据。
	// 设为空字符串则不启 pprof。
	PProfAddr string `env:"SLINK_PPROF_ADDR" envDefault:"127.0.0.1:6060"`

	// ── PostgreSQL ──────────────────────────────────────
	PGDSN      string `env:"SLINK_PG_DSN,required"`
	PGMaxConns int32  `env:"SLINK_PG_MAX_CONNS" envDefault:"20"`
	PGMinConns int32  `env:"SLINK_PG_MIN_CONNS" envDefault:"2"`

	// ── Redis ───────────────────────────────────────────
	RedisAddr     string `env:"SLINK_REDIS_ADDR" envDefault:"localhost:16379"`
	RedisPassword string `env:"SLINK_REDIS_PASSWORD"`
	RedisDB       int    `env:"SLINK_REDIS_DB" envDefault:"0"`

	// ── 短码生成（号段模式）────────────────────────────────
	IDStepSize int64  `env:"SLINK_ID_STEP_SIZE" envDefault:"1000"`
	IDBizTag   string `env:"SLINK_ID_BIZ_TAG"   envDefault:"link"`

	// ── L1 进程内缓存（Day 8）─────────────────────────────
	// LocalCacheSize <= 0 → 禁用 L1，回到只用 Redis 的两层架构。
	// LocalCacheTTL  默认 1m，远短于 Redis TTL，缩小水平扩展时多实例不一致窗口。
	LocalCacheSize int           `env:"SLINK_LOCAL_CACHE_SIZE" envDefault:"4096"`
	LocalCacheTTL  time.Duration `env:"SLINK_LOCAL_CACHE_TTL"  envDefault:"1m"`

	// ── 异步事件 buffer（Day 9 调优）─────────────────────
	// Day 8 实测 93k RPS 把 10k/1k/1s 的默认值打爆（63% 丢）。
	// 调到 50k/2k/500ms 后 dropped 趋零，内存约 5MB（5w × 100B/event）。
	EventBufferCapacity      int           `env:"SLINK_EVENT_BUFFER_CAPACITY"       envDefault:"50000"`
	EventBufferBatchSize     int           `env:"SLINK_EVENT_BUFFER_BATCH_SIZE"     envDefault:"2000"`
	EventBufferFlushInterval time.Duration `env:"SLINK_EVENT_BUFFER_FLUSH_INTERVAL" envDefault:"500ms"`
}

// Load 读取配置，按优先级合并：环境变量 > .env > 默认值。
//
// .env 不存在时不报错——生产环境应通过环境变量注入，没有 .env 是正常的。
// 解析失败、必填项缺失、跨字段校验失败时返回 error。
func Load() (*Config, error) {
	// 静默加载 .env（不存在不报错）
	_ = godotenv.Load()

	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("parse env: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &cfg, nil
}

// Validate 跨字段约束校验。
// 单字段类型由 env tag 在 Parse 阶段完成。
// 这里做：
//   - required 字段非空（env tag required 只检查 unset，空字符串放过）
//   - 跨字段约束（min/max conns 关系等）
//   - 业务约束（rate limit > 0 等）
func (c *Config) Validate() error {
	if c.PGDSN == "" {
		return errors.New("SLINK_PG_DSN is required")
	}
	if c.RedisAddr == "" {
		return errors.New("SLINK_REDIS_ADDR is required")
	}
	if c.IDBizTag == "" {
		return errors.New("SLINK_ID_BIZ_TAG is required")
	}
	if c.PGMinConns < 0 {
		return errors.New("PG_MIN_CONNS must be >= 0")
	}
	if c.PGMaxConns <= 0 {
		return errors.New("PG_MAX_CONNS must be > 0")
	}
	if c.PGMinConns > c.PGMaxConns {
		return fmt.Errorf("PG_MIN_CONNS (%d) > PG_MAX_CONNS (%d)", c.PGMinConns, c.PGMaxConns)
	}
	if c.IDStepSize <= 0 {
		return errors.New("ID_STEP_SIZE must be > 0")
	}
	if _, err := url.Parse(c.BaseURL); err != nil {
		return fmt.Errorf("BASE_URL is not a valid URL: %w", err)
	}
	if c.EventBufferCapacity <= 0 {
		return errors.New("EVENT_BUFFER_CAPACITY must be > 0")
	}
	if c.EventBufferBatchSize <= 0 {
		return errors.New("EVENT_BUFFER_BATCH_SIZE must be > 0")
	}
	if c.EventBufferBatchSize > c.EventBufferCapacity {
		return fmt.Errorf("EVENT_BUFFER_BATCH_SIZE (%d) cannot exceed EVENT_BUFFER_CAPACITY (%d)",
			c.EventBufferBatchSize, c.EventBufferCapacity)
	}
	if c.EventBufferFlushInterval <= 0 {
		return errors.New("EVENT_BUFFER_FLUSH_INTERVAL must be > 0")
	}
	return nil
}

// IsDev 返回当前是否为本地开发环境。
// dev 环境会启用更详细日志、放宽校验等。
func (c *Config) IsDev() bool {
	return c.Env == "dev" || c.Env == "development" || c.Env == "local"
}
