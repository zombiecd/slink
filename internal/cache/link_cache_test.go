package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zombiecd/slink/internal/model"
)

// 所有 LinkCache 测试都依赖跑着的 Redis（docker compose up）。
// 短模式下跳过。

func newTestCache(t *testing.T) (*LinkCache, *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := NewClient(ctx, ClientConfig{Addr: addr(t)})
	if err != nil {
		t.Fatalf("NewClient: %v (Redis 起着吗？)", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// 测试用短 TTL；默认不开 L1，老测试行为完全保留
	lc := NewLinkCache(c,
		WithTTL(2*time.Second),
		WithMissingTTL(1*time.Second),
	)
	return lc, c
}

// newTestCacheWithLocal 构造带 L1 的 cache，用于 Day 8 双层语义测试。
func newTestCacheWithLocal(t *testing.T) (*LinkCache, *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := NewClient(ctx, ClientConfig{Addr: addr(t)})
	if err != nil {
		t.Fatalf("NewClient: %v (Redis 起着吗？)", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	lc := NewLinkCache(c,
		WithTTL(2*time.Second),
		WithMissingTTL(1*time.Second),
		WithLocalCache(64, time.Minute),
	)
	return lc, c
}

// 工具：生成不冲突的 code，每次测试一个独立 namespace
func uniqueCode(t *testing.T) string {
	return "T_" + t.Name() + "_" + time.Now().Format("150405.000")
}

// ─────────────────────────────────────────────────────────
// 基础：cache-aside hit / miss
// ─────────────────────────────────────────────────────────

func TestLinkCache_MissThenHit(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	lc, c := newTestCache(t)
	ctx := context.Background()
	code := uniqueCode(t)
	defer c.Del(ctx, linkKeyPrefix+code)

	want := &model.Link{Code: code, LongURL: "https://example.com/x"}

	calls := atomic.Int32{}
	loader := func(ctx context.Context) (*model.Link, error) {
		calls.Add(1)
		return want, nil
	}

	// 第一次：miss → loader 调一次
	got, err := lc.GetOrLoad(ctx, code, loader)
	if err != nil {
		t.Fatalf("GetOrLoad #1: %v", err)
	}
	if got.LongURL != want.LongURL {
		t.Errorf("LongURL #1: got %q want %q", got.LongURL, want.LongURL)
	}

	// 第二次：hit → loader 不再被调
	got, err = lc.GetOrLoad(ctx, code, loader)
	if err != nil {
		t.Fatalf("GetOrLoad #2: %v", err)
	}
	if got.LongURL != want.LongURL {
		t.Errorf("LongURL #2: got %q want %q", got.LongURL, want.LongURL)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("loader calls: got %d, want 1 (cache should hit)", n)
	}
}

// ─────────────────────────────────────────────────────────
// 穿透防护：DB 不存在 → 写空值标记 → 第二次不打 DB
// ─────────────────────────────────────────────────────────

func TestLinkCache_PenetrationProtection(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	lc, c := newTestCache(t)
	ctx := context.Background()
	code := uniqueCode(t)
	defer c.Del(ctx, linkKeyPrefix+code)

	calls := atomic.Int32{}
	loader := func(ctx context.Context) (*model.Link, error) {
		calls.Add(1)
		return nil, ErrLinkNotFound
	}

	// 第一次：miss → loader 调一次 → 写入空值标记
	if _, err := lc.GetOrLoad(ctx, code, loader); !errors.Is(err, ErrLinkNotFound) {
		t.Fatalf("GetOrLoad #1: want ErrLinkNotFound, got %v", err)
	}

	// 第二次：hit 空值标记 → loader 不被调
	if _, err := lc.GetOrLoad(ctx, code, loader); !errors.Is(err, ErrLinkNotFound) {
		t.Fatalf("GetOrLoad #2: want ErrLinkNotFound, got %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("loader calls: got %d, want 1 (penetration not protected!)", n)
	}

	// 验证 Redis 里存的是 missingMarker
	raw, err := c.Get(ctx, linkKeyPrefix+code)
	if err != nil {
		t.Fatalf("get marker raw: %v", err)
	}
	if raw != missingMarker {
		t.Errorf("marker value: got %q, want %q", raw, missingMarker)
	}
}

// ─────────────────────────────────────────────────────────
// 空值标记过期后允许重新回源
// ─────────────────────────────────────────────────────────

func TestLinkCache_MissingMarkerExpires(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	c, err := NewClient(ctx, ClientConfig{Addr: addr(t)})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// 把 missing TTL 设成 300ms，便于快速测过期
	lc := NewLinkCache(c,
		WithTTL(time.Minute),
		WithMissingTTL(300*time.Millisecond),
	)
	code := uniqueCode(t)
	defer c.Del(ctx, linkKeyPrefix+code)

	calls := atomic.Int32{}
	loader := func(ctx context.Context) (*model.Link, error) {
		calls.Add(1)
		return nil, ErrLinkNotFound
	}

	// 写入空值标记
	_, _ = lc.GetOrLoad(ctx, code, loader)

	// 等过期（300ms TTL + 10% 抖动 → 最多 330ms，留点 buffer）
	time.Sleep(500 * time.Millisecond)

	// 应该重新打 loader
	_, _ = lc.GetOrLoad(ctx, code, loader)
	if n := calls.Load(); n != 2 {
		t.Errorf("after marker expired: loader calls = %d, want 2", n)
	}
}

// ─────────────────────────────────────────────────────────
// 击穿防护：N 个 goroutine 并发 → loader 只调 1 次
// ─────────────────────────────────────────────────────────

func TestLinkCache_BreakdownProtection_Singleflight(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	lc, c := newTestCache(t)
	ctx := context.Background()
	code := uniqueCode(t)
	defer c.Del(ctx, linkKeyPrefix+code)

	want := &model.Link{Code: code, LongURL: "https://example.com/breakdown"}

	calls := atomic.Int32{}
	loader := func(ctx context.Context) (*model.Link, error) {
		calls.Add(1)
		// 模拟 DB 慢查（让所有 goroutine 都在 singleflight 里排队）
		time.Sleep(50 * time.Millisecond)
		return want, nil
	}

	const N = 100
	var wg sync.WaitGroup
	wg.Add(N)
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			got, err := lc.GetOrLoad(ctx, code, loader)
			if err != nil {
				errs[i] = err
				return
			}
			if got.LongURL != want.LongURL {
				errs[i] = errors.New("wrong long_url")
			}
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Fatalf("goroutine %d: %v", i, e)
		}
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("singleflight broke: loader called %d times, want 1", n)
	}
}

// ─────────────────────────────────────────────────────────
// 雪崩防护：jitter 落在 [ttl-10%, ttl+10%) 区间
// ─────────────────────────────────────────────────────────

func TestJitter_InRange(t *testing.T) {
	const base = 10 * time.Minute
	min := time.Duration(float64(base) * (1 - jitterPct))
	max := time.Duration(float64(base) * (1 + jitterPct))

	for i := 0; i < 1000; i++ {
		got := jitter(base)
		if got < min || got >= max {
			t.Fatalf("jitter out of range: got %v, want in [%v, %v)", got, min, max)
		}
	}
}

func TestJitter_Distribution(t *testing.T) {
	// 抽样 1000 次，要求至少有一次落在上半区、一次落在下半区。
	// （朴素分布检查，避免 jitter 退化成单点）
	const base = time.Minute
	below, above := false, false
	for i := 0; i < 1000; i++ {
		got := jitter(base)
		if got < base {
			below = true
		}
		if got > base {
			above = true
		}
		if below && above {
			return
		}
	}
	t.Errorf("jitter distribution skewed: below=%v above=%v", below, above)
}

func TestJitter_ZeroTTL(t *testing.T) {
	// TTL = 0（永不过期）应原样返回
	if got := jitter(0); got != 0 {
		t.Errorf("jitter(0) = %v, want 0", got)
	}
}

// ─────────────────────────────────────────────────────────
// Invalidate 主动失效
// ─────────────────────────────────────────────────────────

func TestLinkCache_Invalidate(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	lc, c := newTestCache(t)
	ctx := context.Background()
	code := uniqueCode(t)
	defer c.Del(ctx, linkKeyPrefix+code)

	calls := atomic.Int32{}
	loader := func(ctx context.Context) (*model.Link, error) {
		calls.Add(1)
		return &model.Link{Code: code, LongURL: "https://x.com"}, nil
	}

	// 填进缓存
	_, _ = lc.GetOrLoad(ctx, code, loader)
	// 命中（loader 不增）
	_, _ = lc.GetOrLoad(ctx, code, loader)
	if calls.Load() != 1 {
		t.Fatalf("setup: loader should be called once, got %d", calls.Load())
	}

	// 主动失效
	if err := lc.Invalidate(ctx, code); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	// 再查一次：应该重新打 loader
	_, _ = lc.GetOrLoad(ctx, code, loader)
	if calls.Load() != 2 {
		t.Errorf("after Invalidate: loader calls = %d, want 2", calls.Load())
	}
}

// ─────────────────────────────────────────────────────────
// Day 8: L1（进程内 LRU）双层语义
// ─────────────────────────────────────────────────────────

// L1 命中可以让请求完全绕过 L2（Redis）。
// 验证方式：填好两层 → 直接外部删 Redis 的 key → 再调 GetOrLoad，
// 仍能拿到正确结果且 loader 不再被调，证明 L1 在工作。
func TestLinkCache_LocalHit_BypassesRedis(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	lc, c := newTestCacheWithLocal(t)
	ctx := context.Background()
	code := uniqueCode(t)
	defer c.Del(ctx, linkKeyPrefix+code)

	want := &model.Link{Code: code, LongURL: "https://example.com/local-hit"}
	calls := atomic.Int32{}
	loader := func(ctx context.Context) (*model.Link, error) {
		calls.Add(1)
		return want, nil
	}

	// 第一次：填 L1 + L2
	if _, err := lc.GetOrLoad(ctx, code, loader); err != nil {
		t.Fatalf("first GetOrLoad: %v", err)
	}

	// 直接绕过 LinkCache 删 Redis key（模拟 L2 单独失效）
	if err := c.Del(ctx, linkKeyPrefix+code); err != nil {
		t.Fatalf("del L2: %v", err)
	}

	// 再调：L2 已没了，但 L1 还在 → 应该命中 L1，loader 不增
	got, err := lc.GetOrLoad(ctx, code, loader)
	if err != nil {
		t.Fatalf("second GetOrLoad: %v", err)
	}
	if got == nil || got.LongURL != want.LongURL {
		t.Errorf("LongURL: got %v, want %q", got, want.LongURL)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("loader calls: got %d, want 1 (L1 should hit)", n)
	}
	if s := lc.LocalStats(); s.Hits == 0 {
		t.Errorf("LocalStats.Hits = 0, want > 0")
	}
}

// L1 上的 missing 标记同样能挡住 Redis。
func TestLinkCache_LocalNegative_BypassesRedis(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	lc, c := newTestCacheWithLocal(t)
	ctx := context.Background()
	code := uniqueCode(t)
	defer c.Del(ctx, linkKeyPrefix+code)

	calls := atomic.Int32{}
	loader := func(ctx context.Context) (*model.Link, error) {
		calls.Add(1)
		return nil, ErrLinkNotFound
	}

	// 第一次：写 missing 到 L1 + L2
	if _, err := lc.GetOrLoad(ctx, code, loader); !errors.Is(err, ErrLinkNotFound) {
		t.Fatalf("first: want ErrLinkNotFound, got %v", err)
	}
	// 删 L2
	if err := c.Del(ctx, linkKeyPrefix+code); err != nil {
		t.Fatalf("del L2: %v", err)
	}
	// 再调：L1 命中 missing
	if _, err := lc.GetOrLoad(ctx, code, loader); !errors.Is(err, ErrLinkNotFound) {
		t.Fatalf("second: want ErrLinkNotFound, got %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("loader calls: got %d, want 1 (L1 missing should hit)", n)
	}
}

// Invalidate 必须同时清两层。
func TestLinkCache_Invalidate_ClearsBothLayers(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	lc, c := newTestCacheWithLocal(t)
	ctx := context.Background()
	code := uniqueCode(t)
	defer c.Del(ctx, linkKeyPrefix+code)

	calls := atomic.Int32{}
	loader := func(ctx context.Context) (*model.Link, error) {
		calls.Add(1)
		return &model.Link{Code: code, LongURL: "https://example.com/inv"}, nil
	}

	_, _ = lc.GetOrLoad(ctx, code, loader) // 填两层
	_, _ = lc.GetOrLoad(ctx, code, loader) // L1 命中
	if calls.Load() != 1 {
		t.Fatalf("setup: loader = %d, want 1", calls.Load())
	}

	if err := lc.Invalidate(ctx, code); err != nil {
		t.Fatalf("Invalidate: %v", err)
	}

	// Invalidate 后再调：必须重新打 loader（L1 + L2 都被清）
	_, _ = lc.GetOrLoad(ctx, code, loader)
	if n := calls.Load(); n != 2 {
		t.Errorf("after Invalidate: loader = %d, want 2", n)
	}
}

// L1 禁用（size=0）时，行为应和老路径完全一致。
func TestLinkCache_LocalDisabled_FallsBackToL2(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	c, err := NewClient(ctx, ClientConfig{Addr: addr(t)})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	lc := NewLinkCache(c,
		WithTTL(2*time.Second),
		WithLocalCache(0, 0), // 显式禁用
	)
	code := uniqueCode(t)
	defer c.Del(ctx, linkKeyPrefix+code)

	calls := atomic.Int32{}
	loader := func(ctx context.Context) (*model.Link, error) {
		calls.Add(1)
		return &model.Link{Code: code, LongURL: "https://example.com/disabled"}, nil
	}

	_, _ = lc.GetOrLoad(ctx, code, loader)
	_, _ = lc.GetOrLoad(ctx, code, loader) // 应命中 L2

	if n := calls.Load(); n != 1 {
		t.Errorf("loader = %d, want 1 (L2 should still cache)", n)
	}
	s := lc.LocalStats()
	if s.Hits != 0 || s.Misses != 0 {
		t.Errorf("LocalStats should be zero when L1 disabled, got %+v", s)
	}
}

// L1 命中时不应再调 Redis（行为通过 stats 间接验证）：
// 第一次填两层 → 计 L1 stats baseline → 再调 N 次 → L1 hits 应增加 N
func TestLinkCache_LocalHit_StatsAccurate(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	lc, c := newTestCacheWithLocal(t)
	ctx := context.Background()
	code := uniqueCode(t)
	defer c.Del(ctx, linkKeyPrefix+code)

	loader := func(ctx context.Context) (*model.Link, error) {
		return &model.Link{Code: code, LongURL: "https://example.com/stats"}, nil
	}

	// 第一次填缓存
	_, _ = lc.GetOrLoad(ctx, code, loader)
	base := lc.LocalStats()

	const N = 10
	for i := 0; i < N; i++ {
		_, _ = lc.GetOrLoad(ctx, code, loader)
	}

	got := lc.LocalStats()
	if delta := got.Hits - base.Hits; delta != N {
		t.Errorf("L1 hits delta: got %d, want %d", delta, N)
	}
}

// ─────────────────────────────────────────────────────────
// loader 真错误（非 NotFound）不应写空值标记
// ─────────────────────────────────────────────────────────

func TestLinkCache_LoaderError_NotCached(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	lc, c := newTestCache(t)
	ctx := context.Background()
	code := uniqueCode(t)
	defer c.Del(ctx, linkKeyPrefix+code)

	dbErr := errors.New("db connection lost")
	calls := atomic.Int32{}
	loader := func(ctx context.Context) (*model.Link, error) {
		calls.Add(1)
		return nil, dbErr
	}

	if _, err := lc.GetOrLoad(ctx, code, loader); !errors.Is(err, dbErr) {
		t.Fatalf("first call: want %v, got %v", dbErr, err)
	}

	// 关键：DB 真错误不该被缓存
	// 第二次调用应该再打 loader（让上层有重试机会）
	if _, err := lc.GetOrLoad(ctx, code, loader); !errors.Is(err, dbErr) {
		t.Fatalf("second call: want %v, got %v", dbErr, err)
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("loader calls: got %d, want 2 (db error must not be cached)", n)
	}

	// Redis 里也不该有这个 key
	if _, err := c.Get(ctx, linkKeyPrefix+code); !errors.Is(err, ErrCacheMiss) {
		t.Errorf("redis should be empty, got: %v", err)
	}
}
