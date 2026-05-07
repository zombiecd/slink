package cache

import (
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/zombiecd/slink/internal/model"
)

// localEntry 是 L1 缓存的值类型。
//
//	link != nil → 命中 link
//	link == nil → 命中 missing 标记（DB 已确认不存在 / 防穿透）
//
// 这跟 LinkCache 在 Redis 上的语义保持一致，
// 区别只是 L1 直接存解码后的 *model.Link 对象，省一次 json.Unmarshal。
type localEntry struct {
	link *model.Link
}

// LocalCacheStats 是 L1 的运行时观测，bench 时用来核算命中率。
// 两个计数器都用 atomic 累加；读出时是某一时刻的快照。
//
// 公开类型（与 cache.Stats / event.Stats 风格一致），
// 上层（如 /debug/stats endpoint）可直接 JSON marshal。
type LocalCacheStats struct {
	Hits   uint64 `json:"hits"`
	Misses uint64 `json:"misses"`
}

// localCache 是 LinkCache 的进程内 L1。
//
// 设计：
//   - 容量 + TTL 由 hashicorp/golang-lru/v2/expirable 一站式提供
//   - hits/misses 用 atomic 累加，无锁
//   - nil-safe：receiver 是 nil 时 Get 永远 miss、Put/Del 是 noop
//     这样 LinkCache 不用到处 if local == nil 的检查
type localCache struct {
	lru    *lru.LRU[string, localEntry]
	hits   atomic.Uint64
	misses atomic.Uint64
}

// newLocalCache 构造一个本地 LRU + TTL 缓存。
// size <= 0 视作禁用（返回 nil）。
func newLocalCache(size int, ttl time.Duration) *localCache {
	if size <= 0 {
		return nil
	}
	return &localCache{
		lru: lru.NewLRU[string, localEntry](size, nil, ttl),
	}
}

// Get 查 L1。命中（且未过期）→ (entry, true)；否则 (zero, false)。
func (c *localCache) Get(key string) (localEntry, bool) {
	if c == nil {
		return localEntry{}, false
	}
	v, ok := c.lru.Get(key)
	if ok {
		c.hits.Add(1)
		return v, true
	}
	c.misses.Add(1)
	return localEntry{}, false
}

// Put 写 L1。
func (c *localCache) Put(key string, e localEntry) {
	if c == nil {
		return
	}
	c.lru.Add(key, e)
}

// Del 主动失效一个 key。
func (c *localCache) Del(key string) {
	if c == nil {
		return
	}
	c.lru.Remove(key)
}

// Stats 取一个观测快照。
func (c *localCache) Stats() LocalCacheStats {
	if c == nil {
		return LocalCacheStats{}
	}
	return LocalCacheStats{
		Hits:   c.hits.Load(),
		Misses: c.misses.Load(),
	}
}
