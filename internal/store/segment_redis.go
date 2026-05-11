package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// segmentRedisKeyPrefix 是号段 key 前缀。完整 key = slink:id_seq:{bizTag}。
// 不放进 config — 改 key = 数据迁移层面的破坏，应当通过迁移脚本而非 env 操作。
const segmentRedisKeyPrefix = "slink:id_seq:"

// RedisSegmentSource 用 Redis INCRBY 实现 id.SegmentSource（v0.6 §8.1 决策）。
//
// 为什么 Redis 而不是 PG 号段表：
//   - 性能：INCRBY ~50μs vs PG ~1ms，号段耗尽时不卡 P99
//   - 依赖最少：v0.3 已经依赖 Redis
//   - 实现 LOC：~50 行（PG 80 / Snowflake 150）
//
// 一致性：依赖 AOF every-second。Redis 重启最多丢秒级号段段——
// 号段单调递增，丢一段相当于跳号（不重复，可接受）。
//
// 启动期兜底：调用方应在多 Pod 部署时调 EnsureMinimum(ctx, bizTag, pgFloor)，
// 把 Redis 当前值与 PG 旧 id_segment.max_id 取大者写回，防迁移期 ID 倒退。
type RedisSegmentSource struct {
	rdb *redis.Client
}

// NewRedisSegmentSource 用现有 go-redis client 构造。
// 不持有 client 所有权，关闭由 cache.Client.Close() 负责。
func NewRedisSegmentSource(rdb *redis.Client) *RedisSegmentSource {
	if rdb == nil {
		panic("NewRedisSegmentSource: rdb is nil")
	}
	return &RedisSegmentSource{rdb: rdb}
}

func segmentRedisKey(bizTag string) string {
	return segmentRedisKeyPrefix + bizTag
}

// Acquire 用 INCRBY 原子拿号段，返回新的 max_id（即号段右端点，闭区间）。
// 调用方按 [max_id - stepSize + 1, max_id] 自增分配。
//
// Redis INCRBY 单命令原子，3 副本并发 100% 安全。
func (r *RedisSegmentSource) Acquire(ctx context.Context, bizTag string, stepSize int64) (int64, error) {
	if bizTag == "" {
		return 0, fmt.Errorf("segment_redis.Acquire: bizTag is empty")
	}
	if stepSize <= 0 {
		return 0, fmt.Errorf("segment_redis.Acquire: stepSize must be > 0, got %d", stepSize)
	}

	newMax, err := r.rdb.IncrBy(ctx, segmentRedisKey(bizTag), stepSize).Result()
	if err != nil {
		return 0, fmt.Errorf("redis INCRBY %q: %w", bizTag, err)
	}
	return newMax, nil
}

// Peek 不修改地查询当前 max_id（运维 / 监控用，不在主路径）。
// key 不存在视为 0（还没初始化）。
func (r *RedisSegmentSource) Peek(ctx context.Context, bizTag string) (int64, error) {
	if bizTag == "" {
		return 0, fmt.Errorf("segment_redis.Peek: bizTag is empty")
	}
	v, err := r.rdb.Get(ctx, segmentRedisKey(bizTag)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("redis GET %q: %w", bizTag, err)
	}
	return v, nil
}

// EnsureMinimum 把 Redis 当前 max_id 与传入 floor 取大者写回（CAS-on-larger）。
//
// 启动期用 PG 旧 id_segment.max_id 作为 floor 调一次，防迁移期 ID 倒退。
// 多 Pod 同时启动并发安全（Lua 单脚本原子）。
//
// 返回写回后的实际值（要么 floor，要么 Redis 已有的更大值）。
func (r *RedisSegmentSource) EnsureMinimum(ctx context.Context, bizTag string, floor int64) (int64, error) {
	if bizTag == "" {
		return 0, fmt.Errorf("segment_redis.EnsureMinimum: bizTag is empty")
	}
	if floor < 0 {
		return 0, fmt.Errorf("segment_redis.EnsureMinimum: floor must be >= 0, got %d", floor)
	}

	// Lua：当前值不存在或 < floor 才 SET。多 Pod 并发安全。
	const script = `
		local cur = redis.call('GET', KEYS[1])
		if cur == false or tonumber(cur) < tonumber(ARGV[1]) then
			redis.call('SET', KEYS[1], ARGV[1])
			return tonumber(ARGV[1])
		end
		return tonumber(cur)
	`
	res, err := r.rdb.Eval(ctx, script, []string{segmentRedisKey(bizTag)}, floor).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis EnsureMinimum %q: %w", bizTag, err)
	}
	return res, nil
}
