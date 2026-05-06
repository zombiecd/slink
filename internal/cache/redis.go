// Package cache 封装 Redis 客户端，是 slink 缓存层入口。
//
// v0.1：单 Redis 实例，提供基础 Get/Set/Del + Ping。
// v0.2：上层包多级缓存（本地 LRU + 当前 Redis），但本包接口不变。
//
// 选用 redis/go-redis/v9：生态最强，原生支持 context、cluster、sentinel。
package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrCacheMiss 表示 key 不存在。
// 业务层用这个 sentinel error 区分"未命中"与"真错误"。
var ErrCacheMiss = errors.New("cache miss")

// ClientConfig 抽象 Redis 客户端构造所需的最小配置。
type ClientConfig struct {
	Addr     string
	Password string
	DB       int

	// 可选高级参数，零值取合理默认。
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
	MinIdleConns int
}

// Client 是 slink 用的 Redis 客户端门面。
//
// 不直接暴露 *redis.Client 是为了：
//   - 统一 ErrCacheMiss 语义（go-redis 用 redis.Nil 表示未命中）
//   - 后续可换实现（如 cluster client）不影响调用方
type Client struct {
	rdb *redis.Client
}

// NewClient 建一个 Redis 客户端并 Ping 验证可达。
func NewClient(ctx context.Context, c ClientConfig) (*Client, error) {
	if c.Addr == "" {
		return nil, fmt.Errorf("cache.NewClient: Addr is empty")
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         c.Addr,
		Password:     c.Password,
		DB:           c.DB,
		DialTimeout:  orDefault(c.DialTimeout, 5*time.Second),
		ReadTimeout:  orDefault(c.ReadTimeout, 3*time.Second),
		WriteTimeout: orDefault(c.WriteTimeout, 3*time.Second),
		PoolSize:     orDefaultInt(c.PoolSize, 50),
		MinIdleConns: orDefaultInt(c.MinIdleConns, 5),
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return &Client{rdb: rdb}, nil
}

// Ping 走一次 PING 命令验证 Redis 可达。
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Get 返回 key 对应的字符串值。
// key 不存在时返回 ErrCacheMiss（不是裸的 redis.Nil）。
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	v, err := c.rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrCacheMiss
	}
	if err != nil {
		return "", fmt.Errorf("redis get %q: %w", key, err)
	}
	return v, nil
}

// Set 设置 key=value，带 TTL（0 表示永不过期）。
func (c *Client) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	if err := c.rdb.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("redis set %q: %w", key, err)
	}
	return nil
}

// Del 删除一个或多个 key。
// 不存在的 key 不算错。
func (c *Client) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := c.rdb.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("redis del: %w", err)
	}
	return nil
}

// Close 关闭底层连接池。优雅停机时调用。
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Raw 暴露底层 *redis.Client，用于本包未封装的命令（HLL / ZSet 等）。
// v0.2 实现 HyperLogLog UV、ZSet TopK 时会用到。
func (c *Client) Raw() *redis.Client {
	return c.rdb
}

func orDefault[T comparable](v, d T) T {
	var zero T
	if v == zero {
		return d
	}
	return v
}

func orDefaultInt(v, d int) int {
	if v <= 0 {
		return d
	}
	return v
}
