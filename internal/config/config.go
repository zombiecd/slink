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

	// ── 缓存策略 ────────────────────────────────────────
	CacheTTL       time.Duration `env:"SLINK_CACHE_TTL"        envDefault:"24h"`
	CacheTTLJitter time.Duration `env:"SLINK_CACHE_TTL_JITTER" envDefault:"10m"`
	CacheNullTTL   time.Duration `env:"SLINK_CACHE_NULL_TTL"   envDefault:"1m"`

	// ── 限流 ────────────────────────────────────────────
	RateLimitPerIP float64 `env:"SLINK_RATE_LIMIT_PER_IP" envDefault:"100"`
	RateLimitBurst int     `env:"SLINK_RATE_LIMIT_BURST"  envDefault:"200"`
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
	if c.CacheTTLJitter > c.CacheTTL {
		return fmt.Errorf("CACHE_TTL_JITTER (%s) cannot exceed CACHE_TTL (%s)", c.CacheTTLJitter, c.CacheTTL)
	}
	if _, err := url.Parse(c.BaseURL); err != nil {
		return fmt.Errorf("BASE_URL is not a valid URL: %w", err)
	}
	if c.RateLimitPerIP <= 0 {
		return errors.New("RATE_LIMIT_PER_IP must be > 0")
	}
	if c.RateLimitBurst <= 0 {
		return errors.New("RATE_LIMIT_BURST must be > 0")
	}
	return nil
}

// IsDev 返回当前是否为本地开发环境。
// dev 环境会启用更详细日志、放宽校验等。
func (c *Config) IsDev() bool {
	return c.Env == "dev" || c.Env == "development" || c.Env == "local"
}
