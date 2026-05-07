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

	// 测试用短 TTL
	lc := NewLinkCache(c,
		WithTTL(2*time.Second),
		WithMissingTTL(1*time.Second),
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
