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
	"log/slog"
	"net"
	"net/netip"
	"net/url"
	"strings"
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
	// IDSource 号段来源：
	//   pg    — 默认，PG id_segment 表（v0.3 起一直跑）
	//   redis — Redis INCRBY（v0.6 §8.1 多 Pod 部署用，50μs vs PG 1ms）
	// 多 Pod 部署必须切 redis；单 Pod 用 pg 即可（性能差异在 RPS < 100k 下不显著）。
	IDSource string `env:"SLINK_ID_SOURCE" envDefault:"pg"`

	// ── L1 进程内缓存（Day 8）─────────────────────────────
	// LocalCacheSize <= 0 → 禁用 L1，回到只用 Redis 的两层架构。
	// LocalCacheTTL  默认 1m，远短于 Redis TTL，缩小水平扩展时多实例不一致窗口。
	LocalCacheSize int           `env:"SLINK_LOCAL_CACHE_SIZE" envDefault:"4096"`
	LocalCacheTTL  time.Duration `env:"SLINK_LOCAL_CACHE_TTL"  envDefault:"1m"`

	// ── 事件后端选择（v0.4 Day 16 切流后只剩 kafka 单档）─────
	// 历史：buffer / dual 模式已删（git tag v0.3-buffer-final 留代码考古）。
	// 字段保留是给 v0.5 加新 backend（如 ClickHouse direct）留 switch 扩展位。
	EventBackend string `env:"SLINK_EVENT_BACKEND" envDefault:"kafka"`

	// ── Kafka producer（v0.4 必填）─────
	// 决策稿 §5.3 参数 lz4 / linger 5ms / max in-flight 5 / acks=Leader 已固化在
	// internal/event/kafka.go，不开放为 env 避免误调。
	KafkaBrokersRaw       string        `env:"SLINK_KAFKA_BROKERS"        envDefault:""`
	KafkaTopic            string        `env:"SLINK_KAFKA_TOPIC"          envDefault:"slink.click_events"`
	KafkaSendTimeout      time.Duration `env:"SLINK_KAFKA_SEND_TIMEOUT"   envDefault:"100ms"`
	KafkaMaxBufferedRecs  int           `env:"SLINK_KAFKA_MAX_BUFFERED"   envDefault:"100000"`
	KafkaDeliveryTimeout  time.Duration `env:"SLINK_KAFKA_DELIVERY_TIMEOUT" envDefault:"5s"`
	KafkaBrokers          []string      `env:"-"` // 由 Validate 解析

	// ── ClickHouse 分析数据源（v0.5 Day 25 新增）─────────
	// CH 挂了不影响 server 启动 + PG 主路径（v0.4 立的"主路径不为下游退步"原则）。
	// 空 CHAddr 视为禁用，/api/stats/* 返回 503。
	CHAddr     string `env:"SLINK_CH_ADDR"     envDefault:""`
	CHUser     string `env:"SLINK_CH_USER"     envDefault:"slink"`
	CHPassword string `env:"SLINK_CH_PASSWORD" envDefault:"slink"`
	CHDatabase string `env:"SLINK_CH_DATABASE" envDefault:"slink_analytics"`
	CHTable    string `env:"SLINK_CH_TABLE"    envDefault:"click_events_ch"`

	// ── 反向代理白名单（v0.3 H6 hardening）────────────────
	// 仅当 RemoteAddr 落在这里面的 CIDR 时，才信任 X-Forwarded-For / X-Real-IP。
	// 默认空 = 不信任 XFF（最安全），生产部署在 LB 后面时必须配置。
	// 格式：逗号分隔的 CIDR，如 "10.0.0.0/8,172.16.0.0/12,fd00::/8"
	TrustedProxiesRaw string         `env:"SLINK_TRUSTED_PROXIES" envDefault:""`
	TrustedProxies    []netip.Prefix `env:"-"` // 由 Validate 解析，不直接吃 env
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
	switch c.IDSource {
	case "pg", "redis":
		// ok
	default:
		return fmt.Errorf("SLINK_ID_SOURCE must be pg|redis, got %q", c.IDSource)
	}
	if _, err := url.Parse(c.BaseURL); err != nil {
		return fmt.Errorf("BASE_URL is not a valid URL: %w", err)
	}
	if err := c.validateEventBackend(); err != nil {
		return err
	}
	if err := c.validatePProfAddr(); err != nil {
		return err
	}
	prefixes, err := parseTrustedProxies(c.TrustedProxiesRaw)
	if err != nil {
		return fmt.Errorf("TRUSTED_PROXIES: %w", err)
	}
	c.TrustedProxies = prefixes
	if !c.IsDev() && len(prefixes) == 0 {
		// prod 部署在 LB 后面但没配 TrustedProxies = 所有 click_event 的 IP
		// 都会变成 LB IP（功能受损但不爆炸），值得一句 warn。
		slog.Warn("TRUSTED_PROXIES is empty in non-dev env; X-Forwarded-For/X-Real-IP will be ignored",
			"env", c.Env)
	}
	return nil
}

// parseTrustedProxies 把 "10.0.0.0/8,fd00::/8" 解析成 []netip.Prefix。
// 单个 IP 不带 /N 时按主机视为 /32 或 /128 接受。空字符串返回 nil（不信任 XFF）。
func parseTrustedProxies(raw string) ([]netip.Prefix, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]netip.Prefix, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// 尝试 CIDR
		if pfx, err := netip.ParsePrefix(p); err == nil {
			out = append(out, pfx)
			continue
		}
		// 尝试单 IP
		addr, err := netip.ParseAddr(p)
		if err != nil {
			return nil, fmt.Errorf("entry %q is neither CIDR nor IP: %w", p, err)
		}
		bits := addr.BitLen()
		out = append(out, netip.PrefixFrom(addr, bits))
	}
	return out, nil
}

// validateEventBackend 校验 EventBackend 取值并解析 Kafka brokers。
//
// v0.4 Day 16 切流后 EventBackend 只接受 "kafka" 单值。
// buffer / dual 模式代码已删（git tag v0.3-buffer-final）。
// 字段保留是给 v0.5 加新 backend（如 ClickHouse direct）留 switch 扩展位。
func (c *Config) validateEventBackend() error {
	switch c.EventBackend {
	case "kafka":
		// 解析 brokers
		brokers, err := parseKafkaBrokers(c.KafkaBrokersRaw)
		if err != nil {
			return fmt.Errorf("KAFKA_BROKERS: %w", err)
		}
		if len(brokers) == 0 {
			return fmt.Errorf("EVENT_BACKEND=%q requires SLINK_KAFKA_BROKERS (comma-separated host:port)",
				c.EventBackend)
		}
		c.KafkaBrokers = brokers
		if c.KafkaTopic == "" {
			return errors.New("KAFKA_TOPIC must not be empty")
		}
		if c.KafkaSendTimeout <= 0 {
			return errors.New("KAFKA_SEND_TIMEOUT must be > 0")
		}
		if c.KafkaMaxBufferedRecs <= 0 {
			return errors.New("KAFKA_MAX_BUFFERED must be > 0")
		}
		if c.KafkaDeliveryTimeout <= 0 {
			return errors.New("KAFKA_DELIVERY_TIMEOUT must be > 0")
		}
		return nil
	default:
		return fmt.Errorf("EVENT_BACKEND=%q invalid; only \"kafka\" supported after Day 16 cutover (buffer/dual removed, see git tag v0.3-buffer-final)",
			c.EventBackend)
	}
}

// parseKafkaBrokers 把 "host1:port,host2:port" 解析成 []string。
// 空字符串返回 nil。
func parseKafkaBrokers(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(p); err != nil {
			return nil, fmt.Errorf("entry %q is not host:port: %w", p, err)
		}
		out = append(out, p)
	}
	return out, nil
}

// validatePProfAddr 防止把 pprof / /debug/stats / /metrics 端口暴露公网。
//
// 这些端点合在一起 = 自助 DoS（/debug/pprof/profile?seconds=300 拉满 CPU）+
// 数据泄漏（heap dump 含 long_url 业务数据）。Day 11 code review 标 H2。
//
// 规则：
//   - 空字符串 → 不启 pprof，放过
//   - prod 环境 → 必须 loopback（127.0.0.1 / ::1 / localhost），否则报错
//   - 非 prod  → 非 loopback 仅 warn，留 dev 环境绑 0.0.0.0 的便利
func (c *Config) validatePProfAddr() error {
	if c.PProfAddr == "" {
		return nil
	}
	host, _, err := net.SplitHostPort(c.PProfAddr)
	if err != nil {
		return fmt.Errorf("PPROF_ADDR %q: %w", c.PProfAddr, err)
	}

	if isLoopbackHost(host) {
		return nil
	}

	// prod 严格拒绝
	if !c.IsDev() {
		return fmt.Errorf("PPROF_ADDR %q binds non-loopback host %q in env=%q; "+
			"pprof / metrics 暴露公网 = 自助 DoS + 数据泄漏。"+
			"请改用 127.0.0.1 / localhost / ::1，或留空字符串关闭。",
			c.PProfAddr, host, c.Env)
	}

	// dev / staging 警告但放过（本地 docker exec 进容器调试要绑 0.0.0.0）
	slog.Warn("PProfAddr is not loopback; this is only safe in trusted dev environments",
		"addr", c.PProfAddr, "host", host, "env", c.Env)
	return nil
}

// isLoopbackHost 判断 host 是否绑回环。
// 接受空字符串（":6060" 形式，Go 把空 host 当成绑所有接口；不安全，按非 loopback 处理）。
func isLoopbackHost(host string) bool {
	if host == "" {
		return false
	}
	// 字面字符串快速路径
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// 非 IP 也非 localhost（如自定义 hostname）：保守处理为非 loopback
		return false
	}
	return ip.IsLoopback()
}

// IsDev 返回当前是否为本地开发环境。
// dev 环境会启用更详细日志、放宽校验等。
func (c *Config) IsDev() bool {
	return c.Env == "dev" || c.Env == "development" || c.Env == "local"
}
