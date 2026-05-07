package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/zombiecd/slink/internal/model"
)

// ErrLinkNotFound 是 LinkCache 的"已知不存在"语义。
//
// 与 ErrCacheMiss 的区分：
//   - ErrCacheMiss   → Redis 里没这个 key（首次/过期），需要回源
//   - ErrLinkNotFound → Redis 命中但内容是空值标记，**已经回源过且 DB 也不存在**
//
// loader 也应该按此约定返回 ErrLinkNotFound 表示"DB 真没有"。
var ErrLinkNotFound = errors.New("link not found")

const (
	linkKeyPrefix = "link:"

	// 空值标记：写入 Redis 表示"DB 已确认不存在"。
	// 不用空字符串是为了让"长 URL 是空"和"已知不存在"语义可分。
	missingMarker = "__nil__"

	// 默认正常值 TTL = 10 分钟。
	// 短链跳转是读多写少，10 分钟既能扛热点又能让 expires_at 变更不太久后生效。
	defaultLinkTTL = 10 * time.Minute

	// 空值标记 TTL = 30 秒。
	// 比正常值短，因为：
	//   1) 防穿透只需挡住短期重复扫描，不需要长保
	//   2) 攻击者扫描完即停，30s 后过期不影响真实用户后续创建同 code 的可能（虽然概率极小）
	defaultMissingTTL = 30 * time.Second

	// TTL 抖动比例 = ±10%，防雪崩。
	// 同时入缓存的一批 key 不会同一刻一起失效。
	jitterPct = 0.10

	// L1（进程内 LRU）默认 TTL = 1 分钟。
	// 远短于 L2（Redis）的 10 分钟，目的是缩小水平扩展时多实例间的不一致窗口。
	// 1 分钟够覆盖一次 wrk 30s + 拍测，又不会让管理动作（删/改）等太久才生效。
	defaultLocalTTL = 1 * time.Minute

	// 默认回源超时 = 5 秒。
	// singleflight 闭包不再绑 caller ctx（v0.3 修 H1）后，
	// 需要一个独立超时防止 DB 卡死时无限堆积 leader goroutine。
	// PG SELECT by PK 通常 <10ms，5s 是非常宽松的兜底。
	defaultLoaderTimeout = 5 * time.Second
)

// linkCacheValue 是 Redis 里存的紧凑视图。
//
// 只装跳转路径必需的字段——short JSON = 更小的 Redis 内存 = 更高 QPS 上限。
// model.Link 里的 ID / Creator / IdempotencyKey 跳转用不到，不进缓存。
type linkCacheValue struct {
	Code      string     `json:"c"`
	LongURL   string     `json:"u"`
	ExpiresAt *time.Time `json:"e,omitempty"`
}

func (v *linkCacheValue) toLink() *model.Link {
	return &model.Link{
		Code:      v.Code,
		LongURL:   v.LongURL,
		ExpiresAt: v.ExpiresAt,
	}
}

func valueFromLink(l *model.Link) linkCacheValue {
	return linkCacheValue{
		Code:      l.Code,
		LongURL:   l.LongURL,
		ExpiresAt: l.ExpiresAt,
	}
}

// LinkLoader 是 cache miss 时的回源函数签名。
//
// 约定：DB 中真不存在时必须返回 ErrLinkNotFound（错误链中包含即可），
// 这样 LinkCache 才能写入空值标记防穿透。
type LinkLoader func(ctx context.Context) (*model.Link, error)

// LinkCache 是短链跳转专用的两层 cache-aside 封装。
//
//	请求 → L1 (in-process LRU + TTL) → L2 (Redis) → loader (DB)
//
// 三大坑防护一站式集成：
//
//	缓存穿透：DB 不存在的 code 也写入 missingMarker（短 TTL，L1/L2 都缓存）
//	缓存击穿：用 singleflight 合并相同 code 的回源请求
//	缓存雪崩：L2 TTL ±jitterPct 随机抖动
//
// L1 设计取舍（Day 8）：
//
//	好处：mixed 场景命中率 99%+，L1 命中省一次 Redis 网络 RTT + JSON 反序列化
//	代价：水平扩展时多实例 L1 不一致；通过 L1 TTL 短于 L2（1min vs 10min）压缩窗口
//	可关闭：localSize<=0 → local==nil → 走老路径只用 L2
type LinkCache struct {
	rdb        *Client
	local      *localCache // L1，nil 表示禁用
	sf         singleflight.Group
	ttl        time.Duration
	missingTTL time.Duration

	// loaderCtx 是 singleflight 闭包内打 DB / 写缓存用的 ctx。
	// 不能复用 caller ctx —— 第一个 caller cancel 会污染所有 waiter 的结果（v0.3 H1）。
	// 默认 context.Background()；server 生命周期 ctx 可通过 WithBackgroundContext 注入。
	loaderCtx context.Context
	// loaderTimeout 是单次回源的超时上限。0 = 不加超时（不推荐）。
	loaderTimeout time.Duration
}

// LinkCacheOption 用于自定义 TTL（测试时缩短）+ L1 容量。
type LinkCacheOption func(*linkCacheConfig)

type linkCacheConfig struct {
	ttl           time.Duration
	missingTTL    time.Duration
	localSize     int
	localTTL      time.Duration
	loaderCtx     context.Context
	loaderTimeout time.Duration
}

func WithTTL(ttl time.Duration) LinkCacheOption {
	return func(c *linkCacheConfig) { c.ttl = ttl }
}

func WithMissingTTL(ttl time.Duration) LinkCacheOption {
	return func(c *linkCacheConfig) { c.missingTTL = ttl }
}

// WithLocalCache 启用进程内 L1。size<=0 等于禁用。
func WithLocalCache(size int, ttl time.Duration) LinkCacheOption {
	return func(c *linkCacheConfig) {
		c.localSize = size
		c.localTTL = ttl
	}
}

// WithBackgroundContext 注入 server 生命周期 ctx，让 singleflight leader 在
// server shutdown 时能跟随 cancel；不传则用 context.Background()。
//
// 关键不变量：这个 ctx **不能** 来自任何具体 HTTP caller，否则 H1 bug 复现。
func WithBackgroundContext(ctx context.Context) LinkCacheOption {
	return func(c *linkCacheConfig) { c.loaderCtx = ctx }
}

// WithLoaderTimeout 给单次 singleflight 回源加超时上限（默认 5s）。
// 闭包不再绑 caller ctx 后必须有这个兜底，避免 DB 卡死时 leader goroutine 无限堆积。
func WithLoaderTimeout(d time.Duration) LinkCacheOption {
	return func(c *linkCacheConfig) { c.loaderTimeout = d }
}

// NewLinkCache 构造 LinkCache。
// rdb 不能为 nil；TTL 缺省 10min / 30s；L1 默认禁用（size=0），用 WithLocalCache 显式开。
func NewLinkCache(rdb *Client, opts ...LinkCacheOption) *LinkCache {
	cfg := linkCacheConfig{
		ttl:           defaultLinkTTL,
		missingTTL:    defaultMissingTTL,
		localTTL:      defaultLocalTTL,
		loaderCtx:     context.Background(),
		loaderTimeout: defaultLoaderTimeout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.loaderCtx == nil {
		cfg.loaderCtx = context.Background()
	}
	return &LinkCache{
		rdb:           rdb,
		local:         newLocalCache(cfg.localSize, cfg.localTTL),
		ttl:           cfg.ttl,
		missingTTL:    cfg.missingTTL,
		loaderCtx:     cfg.loaderCtx,
		loaderTimeout: cfg.loaderTimeout,
	}
}

// LocalStats 返回 L1 命中统计快照（bench/observability 用）。
// L1 未启用时返回零值。
func (lc *LinkCache) LocalStats() LocalCacheStats {
	return lc.local.Stats()
}

// GetOrLoad 是跳转路径的唯一入口。
//
// 流程：
//
//	1. 查 Redis
//	   ├─ 命中 link 值      → 反序列化返回
//	   ├─ 命中空值标记      → 直接返回 ErrLinkNotFound（不打 DB）
//	   ├─ Redis 真错误      → 降级到 loader（缓存挂不能让全站挂）
//	   └─ Cache miss        → 进入 2
//	2. singleflight 合并相同 code 的回源
//	3. 调 loader 回源 DB
//	   ├─ DB 命中           → 写正值缓存（带抖动 TTL）
//	   └─ ErrLinkNotFound   → 写空值标记（短 TTL）防穿透
//
// 返回值约定：
//
//	link != nil, err == nil           → 成功
//	link == nil, err == ErrLinkNotFound → 已知不存在（缓存或 DB 都说不存在）
//	link == nil, err 其他               → 真错误（DB 抖动、context 取消等）
func (lc *LinkCache) GetOrLoad(
	ctx context.Context,
	code string,
	loader LinkLoader,
) (*model.Link, error) {
	key := linkKeyPrefix + code

	// ── 0. 查 L1（进程内 LRU） ────────────────────────────
	// L1 命中直返，省一次 Redis 网络 RTT + JSON 反序列化。
	// nil cache 时 Get 永远 miss（localCache 自带 nil-safe）。
	if e, ok := lc.local.Get(key); ok {
		if e.link == nil {
			return nil, ErrLinkNotFound
		}
		return e.link, nil
	}

	// ── 1. 查 Redis ──────────────────────────────────────
	raw, err := lc.rdb.Get(ctx, key)
	switch {
	case err == nil:
		// 命中：检查是不是空值标记
		if raw == missingMarker {
			lc.local.Put(key, localEntry{link: nil}) // 回填 L1 missing
			return nil, ErrLinkNotFound
		}
		// 反序列化
		var v linkCacheValue
		if jerr := json.Unmarshal([]byte(raw), &v); jerr != nil {
			// 缓存损坏（不该发生）：删掉、记 log，往下走 loader
			slog.Warn("link cache corrupt, will reload from db",
				"code", code, "err", jerr)
			_ = lc.rdb.Del(ctx, key)
			break
		}
		link := v.toLink()
		lc.local.Put(key, localEntry{link: link}) // 回填 L1
		return link, nil

	case errors.Is(err, ErrCacheMiss):
		// 正常 miss，往下走 loader

	default:
		// Redis 真错误（连接/超时）：缓存挂了不能拖垮跳转 → 降级直接打 DB
		// 注意此路径下不写缓存（L1 也不回填），避免雪崩时把 DB 也打挂
		slog.Warn("link cache get failed, fallback to loader",
			"code", code, "err", err)
		return loader(ctx)
	}

	// ── 2. singleflight 合并并发回源（防击穿） ────────────
	// 同一 code 同一时刻并发 N 个请求，只有 1 个真打 DB，其他等结果
	//
	// v0.3 H1 修：闭包内 **不再** 用 caller 的 ctx。
	// 理由：第一个 caller 取消 → DB 调用 / 写缓存全部 ctx.Canceled →
	// 所有 N 个 waiter 拿到 Canceled 错误 → 缓存没填上 → 下一波同 key 打爆 DB。
	// 改成"server-lifetime ctx + 独立超时"后，leader 跑到底，结果落缓存。
	v, err, _ := lc.sf.Do(key, func() (any, error) {
		bgCtx := lc.loaderCtx
		if lc.loaderTimeout > 0 {
			var cancel context.CancelFunc
			bgCtx, cancel = context.WithTimeout(bgCtx, lc.loaderTimeout)
			defer cancel()
		}

		// double-check：可能在排队期间另一个 goroutine 已经填好缓存
		if raw, gerr := lc.rdb.Get(bgCtx, key); gerr == nil {
			if raw == missingMarker {
				return (*model.Link)(nil), ErrLinkNotFound
			}
			var cv linkCacheValue
			if json.Unmarshal([]byte(raw), &cv) == nil {
				return cv.toLink(), nil
			}
		}

		link, lerr := loader(bgCtx)
		if lerr != nil {
			// DB 说不存在 → 写空值标记防穿透
			if errors.Is(lerr, ErrLinkNotFound) {
				if serr := lc.setMissing(bgCtx, key); serr != nil {
					slog.Warn("set missing marker failed", "code", code, "err", serr)
				}
				return (*model.Link)(nil), ErrLinkNotFound
			}
			// DB 真错误：不写缓存，直接抛给上层
			return (*model.Link)(nil), lerr
		}

		// DB 命中 → 写正值缓存（带抖动 TTL）
		if serr := lc.setLink(bgCtx, key, link); serr != nil {
			slog.Warn("set link cache failed", "code", code, "err", serr)
		}
		return link, nil
	})
	if err != nil {
		// loader 真错误（非 NotFound）：不回填 L1（有重试机会）
		if errors.Is(err, ErrLinkNotFound) {
			lc.local.Put(key, localEntry{link: nil}) // 回填 L1 missing
			return nil, err
		}
		return nil, err
	}
	link, ok := v.(*model.Link)
	if !ok || link == nil {
		// singleflight 结果是 (*model.Link)(nil) 表示 ErrLinkNotFound
		lc.local.Put(key, localEntry{link: nil}) // 回填 L1 missing
		return nil, ErrLinkNotFound
	}
	lc.local.Put(key, localEntry{link: link}) // 回填 L1
	return link, nil
}

// Invalidate 主动失效一个 code 的缓存（L1 + L2）。
// 创建短链冲突修复 / 删除短链时调用。
//
// 注意：这只清当前实例的 L1。多实例部署时其他实例的 L1 还会持有旧值，
// 直到自然 TTL 过期（默认 1min）。Day 8 的已知 trade-off。
func (lc *LinkCache) Invalidate(ctx context.Context, code string) error {
	key := linkKeyPrefix + code
	lc.local.Del(key)
	return lc.rdb.Del(ctx, key)
}

// Set 主动写入缓存（创建短链后预热可用）。
// 失败仅记 log——预热失败不应该让创建接口报错。
func (lc *LinkCache) Set(ctx context.Context, link *model.Link) error {
	return lc.setLink(ctx, linkKeyPrefix+link.Code, link)
}

// setLink 把 link 写入缓存，TTL 带 ±jitterPct 抖动。
func (lc *LinkCache) setLink(ctx context.Context, key string, link *model.Link) error {
	v := valueFromLink(link)
	b, err := json.Marshal(&v)
	if err != nil {
		// model.Link 全是普通字段，理论不会失败
		return fmt.Errorf("marshal link cache value: %w", err)
	}
	return lc.rdb.Set(ctx, key, string(b), jitter(lc.ttl))
}

func (lc *LinkCache) setMissing(ctx context.Context, key string) error {
	return lc.rdb.Set(ctx, key, missingMarker, jitter(lc.missingTTL))
}

// jitter 给 ttl 加 ±jitterPct 随机抖动，防雪崩。
//
// 用 math/rand/v2 而非 crypto/rand：抖动是性能优化不是安全用途，
// rand/v2 无锁、单核也快。
func jitter(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	delta := float64(ttl) * jitterPct
	// rand.Float64() ∈ [0,1)，映射到 [-delta, +delta]
	offset := (rand.Float64()*2 - 1) * delta
	return ttl + time.Duration(offset)
}
