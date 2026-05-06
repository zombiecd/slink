// Package store 封装 PostgreSQL 数据访问。
//
// v0.1 范围：连接池 + 健康检查。
// 后续版本会在此包内加 LinkRepo / ClickEventRepo / SegmentRepo 等。
//
// 选用 pgx 原生连接池而非 database/sql 包装，理由：
//   - 性能：少一层接口适配，QueryRow / Exec 直接走 pgx 协议
//   - 类型映射：pgx 直接支持 PG 高级类型（INET / UUID / JSONB / numeric）
//   - 详见 docs/concepts/pgx-connection-pool.md
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolConfig 抽象连接池构造所需的最小配置。
// 不直接吃 *config.Config 是为了让 store 包不依赖 config 包（避免循环引用 + 易测试）。
type PoolConfig struct {
	DSN      string
	MaxConns int32
	MinConns int32

	// 可选高级参数，零值有合理默认。
	MaxConnLifetime    time.Duration // 连接最大生命周期，默认 1h
	MaxConnIdleTime    time.Duration // 空闲连接超时，默认 30min
	HealthCheckPeriod  time.Duration // 健康检查频率，默认 1min
	ConnectTimeout     time.Duration // 单次建连超时，默认 5s
}

// NewPool 用给定配置构造一个 pgxpool。
//
// 函数返回前会等首批连接（MinConns）就绪，确保启动后立刻可用——
// 否则首次请求会撞上"懒建连"延迟。
func NewPool(ctx context.Context, c PoolConfig) (*pgxpool.Pool, error) {
	if c.DSN == "" {
		return nil, fmt.Errorf("store.NewPool: DSN is empty")
	}

	pgxCfg, err := pgxpool.ParseConfig(c.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}

	pgxCfg.MaxConns = c.MaxConns
	pgxCfg.MinConns = c.MinConns
	pgxCfg.MaxConnLifetime = orDefault(c.MaxConnLifetime, time.Hour)
	pgxCfg.MaxConnIdleTime = orDefault(c.MaxConnIdleTime, 30*time.Minute)
	pgxCfg.HealthCheckPeriod = orDefault(c.HealthCheckPeriod, time.Minute)
	pgxCfg.ConnConfig.ConnectTimeout = orDefault(c.ConnectTimeout, 5*time.Second)

	pool, err := pgxpool.NewWithConfig(ctx, pgxCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	// 确认首次连接可用。失败则关池后返回错误。
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping pg: %w", err)
	}

	return pool, nil
}

// Ping 走一次完整的连接借用 + 网络往返，验证 DB 真的活着。
//
// 区别于 pgxpool 内置的 HealthCheck 后台任务（只重连不验证业务可达）。
func Ping(ctx context.Context, pool *pgxpool.Pool) error {
	return pool.Ping(ctx)
}

func orDefault[T comparable](v, d T) T {
	var zero T
	if v == zero {
		return d
	}
	return v
}
