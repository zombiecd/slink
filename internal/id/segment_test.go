package id

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ────────────────────────────────────────────────────────────
// mockSource：纯内存号段供应，便于单测
// ────────────────────────────────────────────────────────────

type mockSource struct {
	mu        sync.Mutex
	maxID     int64
	calls     int64
	failNext  bool          // 下一次 Acquire 直接返回 error
	delay     time.Duration // 模拟 DB 延迟
	failFn    func(call int64) error
}

func (m *mockSource) Acquire(ctx context.Context, bizTag string, step int64) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	atomic.AddInt64(&m.calls, 1)

	if m.failNext {
		m.failNext = false
		return 0, errors.New("mock: forced failure")
	}
	if m.failFn != nil {
		if err := m.failFn(atomic.LoadInt64(&m.calls)); err != nil {
			return 0, err
		}
	}

	m.maxID += step
	return m.maxID, nil
}

func (m *mockSource) Calls() int64 {
	return atomic.LoadInt64(&m.calls)
}

// ────────────────────────────────────────────────────────────
// 单测
// ────────────────────────────────────────────────────────────

func TestDoubleBuffer_BasicSequence(t *testing.T) {
	src := &mockSource{}
	ctx := context.Background()

	db, err := NewDoubleBuffer(ctx, src, "test", 100, 0.9, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 头 100 个 ID 应该是 1..100
	for want := int64(1); want <= 100; want++ {
		got, err := db.NextID(ctx)
		if err != nil {
			t.Fatalf("NextID(%d): %v", want, err)
		}
		if got != want {
			t.Fatalf("NextID #%d: got %d, want %d", want, got, want)
		}
	}
	// 跨段后继续 101..200
	for want := int64(101); want <= 200; want++ {
		got, err := db.NextID(ctx)
		if err != nil {
			t.Fatalf("NextID(%d): %v", want, err)
		}
		if got != want {
			t.Fatalf("NextID #%d: got %d, want %d", want, got, want)
		}
	}
}

func TestDoubleBuffer_NoSkipOrDup(t *testing.T) {
	src := &mockSource{}
	ctx := context.Background()
	db, _ := NewDoubleBuffer(ctx, src, "test", 50, 0.9, nil)

	const total = 1000
	seen := make(map[int64]struct{}, total)
	for i := 0; i < total; i++ {
		id, err := db.NextID(ctx)
		if err != nil {
			t.Fatalf("NextID: %v", err)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate ID: %d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != total {
		t.Errorf("got %d unique IDs, want %d", len(seen), total)
	}
}

func TestDoubleBuffer_Concurrent(t *testing.T) {
	src := &mockSource{delay: 200 * time.Microsecond}
	ctx := context.Background()
	db, err := NewDoubleBuffer(ctx, src, "test", 1000, 0.9, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const (
		goroutines = 16
		perG       = 1000
		total      = goroutines * perG
	)

	var (
		mu   sync.Mutex
		seen = make(map[int64]struct{}, total)
		wg   sync.WaitGroup
	)
	wg.Add(goroutines)
	start := time.Now()
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			local := make([]int64, 0, perG)
			for i := 0; i < perG; i++ {
				id, err := db.NextID(ctx)
				if err != nil {
					t.Errorf("NextID: %v", err)
					return
				}
				local = append(local, id)
			}
			mu.Lock()
			for _, id := range local {
				if _, dup := seen[id]; dup {
					t.Errorf("duplicate id: %d", id)
				}
				seen[id] = struct{}{}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	if len(seen) != total {
		t.Errorf("got %d unique IDs, want %d", len(seen), total)
	}
	t.Logf("16k IDs over 16 goroutines in %v (%.0f IDs/s, %d source calls)",
		elapsed, float64(total)/elapsed.Seconds(), src.Calls())
}

func TestDoubleBuffer_AsyncRefillTriggered(t *testing.T) {
	src := &mockSource{}
	ctx := context.Background()
	// step=10, threshold=0.9 → 第 9 次取号触发异步预取
	db, _ := NewDoubleBuffer(ctx, src, "test", 10, 0.9, nil)

	// 启动后已用 1 次 source（cur 段）
	if got := src.Calls(); got != 1 {
		t.Fatalf("after init: source calls = %d, want 1", got)
	}

	// 取 9 次到达 threshold
	for i := 0; i < 9; i++ {
		_, _ = db.NextID(ctx)
	}

	// 等异步 goroutine 跑完
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if src.Calls() >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if src.Calls() < 2 {
		t.Errorf("async refill not triggered: source calls = %d", src.Calls())
	}

	stat := db.Stat()
	if !stat.NextReady {
		t.Errorf("next segment not ready after threshold")
	}
}

func TestDoubleBuffer_StarvationFallback(t *testing.T) {
	// 模拟"异步预取失败 → 当 cur 耗尽时 starvation 同步阻塞取段"
	src := &asyncFailSource{}
	ctx := context.Background()
	db, err := NewDoubleBuffer(ctx, src, "test", 5, 0.5, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 用完整段（5 个）+ 再取 1 个 → starvation 同步取
	for i := 0; i < 6; i++ {
		_, err := db.NextID(ctx)
		if err != nil {
			t.Fatalf("NextID #%d: %v", i, err)
		}
		// 让异步预取有机会跑完（虽然会失败）
		time.Sleep(2 * time.Millisecond)
	}
	// 至少应该调用过 source 3 次：init / async fail / sync starvation
	if got := src.calls; got < 3 {
		t.Errorf("expected source called >=3 times (init+fail+sync), got %d", got)
	}
}

type asyncFailSource struct {
	mu    sync.Mutex
	calls int
	max   int64
}

func (s *asyncFailSource) Acquire(ctx context.Context, _ string, step int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	// 第 2 次（async refill）失败
	if s.calls == 2 {
		return 0, errors.New("forced async fail")
	}
	s.max += step
	return s.max, nil
}

func TestDoubleBuffer_InitFailure(t *testing.T) {
	src := &mockSource{failNext: true}
	ctx := context.Background()
	_, err := NewDoubleBuffer(ctx, src, "test", 100, 0.9, nil)
	if err == nil {
		t.Fatal("expected init failure, got nil")
	}
}

func TestDoubleBuffer_ValidateArgs(t *testing.T) {
	ctx := context.Background()
	src := &mockSource{}

	cases := []struct {
		name     string
		bizTag   string
		stepSize int64
		threshold float64
	}{
		{"empty bizTag", "", 100, 0.9},
		{"step 0", "test", 0, 0.9},
		{"step neg", "test", -1, 0.9},
		{"thresh 0", "test", 100, 0},
		{"thresh 1", "test", 100, 1},
		{"thresh neg", "test", 100, -0.5},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewDoubleBuffer(ctx, src, c.bizTag, c.stepSize, c.threshold, nil)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

func TestGenerator_NextCode(t *testing.T) {
	src := &mockSource{}
	ctx := context.Background()
	db, _ := NewDoubleBuffer(ctx, src, "test", 100, 0.9, nil)
	g := NewGenerator(db)

	codes := make(map[string]struct{})
	for i := 0; i < 200; i++ {
		c, err := g.NextCode(ctx)
		if err != nil {
			t.Fatalf("NextCode: %v", err)
		}
		if len(c) != 6 {
			t.Errorf("code length %d, want 6: %q", len(c), c)
		}
		if _, dup := codes[c]; dup {
			t.Errorf("duplicate code: %q", c)
		}
		codes[c] = struct{}{}
	}
}

// ────────────────────────────────────────────────────────────
// 集成测试（用真 PG，需要 docker compose up + migrate）
// ────────────────────────────────────────────────────────────

// 这里我们不直接 import store 包（避免循环依赖），
// 而是定义 SegmentSource 的薄封装，单独跑。
// 真实集成验证由 cmd/server 启动时的端到端测试覆盖（Day 4）。

// ────────────────────────────────────────────────────────────
// Benchmarks
// ────────────────────────────────────────────────────────────

func BenchmarkDoubleBuffer_NextID(b *testing.B) {
	src := &mockSource{}
	ctx := context.Background()
	db, err := NewDoubleBuffer(ctx, src, "bench", 10000, 0.9, nil)
	if err != nil {
		b.Fatalf("init: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = db.NextID(ctx)
	}
}

func BenchmarkDoubleBuffer_NextID_Parallel(b *testing.B) {
	src := &mockSource{}
	ctx := context.Background()
	db, err := NewDoubleBuffer(ctx, src, "bench", 10000, 0.9, nil)
	if err != nil {
		b.Fatalf("init: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = db.NextID(ctx)
		}
	})
}

func BenchmarkGenerator_NextCode(b *testing.B) {
	src := &mockSource{}
	ctx := context.Background()
	db, _ := NewDoubleBuffer(ctx, src, "bench", 10000, 0.9, nil)
	g := NewGenerator(db)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = g.NextCode(ctx)
	}
}

// ────────────────────────────────────────────────────────────
// 调试：打印一段执行轨迹（运行 go test -v -run Demo_Trace）
// ────────────────────────────────────────────────────────────

func TestDemo_Trace(t *testing.T) {
	src := &mockSource{}
	ctx := context.Background()
	db, _ := NewDoubleBuffer(ctx, src, "demo", 5, 0.6, nil)

	for i := 1; i <= 15; i++ {
		id, _ := db.NextID(ctx)
		stat := db.Stat()
		t.Logf("#%02d id=%-3d  cur[%d-%d cursor=%d use=%.0f%%]  next_ready=%v  source_calls=%d",
			i, id,
			stat.CurLow, stat.CurHigh, stat.CurCursor, stat.CurUsage*100,
			stat.NextReady, src.Calls())
		// 给异步预取一点时间
		if stat.CurUsage > 0.5 {
			time.Sleep(2 * time.Millisecond)
		}
	}
	_ = fmt.Sprintf
}
