package event

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeSink 是 buffer 测试专用 sink，记录每次 BatchInsert 的 batch。
type fakeSink struct {
	mu       sync.Mutex
	batches  [][]ClickEvent
	failNext atomic.Bool // 设为 true 时下一次 BatchInsert 返回错误
	delay    time.Duration
}

func (f *fakeSink) BatchInsert(ctx context.Context, evts []ClickEvent) error {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.failNext.Swap(false) {
		return errors.New("simulated flush failure")
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]ClickEvent, len(evts))
	copy(cp, evts)
	f.batches = append(f.batches, cp)
	return nil
}

func (f *fakeSink) totalRows() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, b := range f.batches {
		n += len(b)
	}
	return n
}

func (f *fakeSink) batchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.batches)
}

func makeEvent(code string) ClickEvent {
	return ClickEvent{EventID: code + "-id", Code: code, TS: time.Now()}
}

// ─────────────────────────────────────────────────────────
// 触发条件 1：攒够 BatchSize 立即 flush
// ─────────────────────────────────────────────────────────

func TestBuffer_FlushOnBatchFull(t *testing.T) {
	sink := &fakeSink{}
	buf := NewBuffer(sink, BufferConfig{
		Capacity:      100,
		BatchSize:     5,
		FlushInterval: time.Hour, // 防止时间触发
	})
	buf.Start()
	defer buf.Stop(context.Background())

	for i := 0; i < 5; i++ {
		if err := buf.Enqueue(context.Background(), makeEvent("a")); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	// 等 flusher goroutine 处理完
	waitFor(t, time.Second, func() bool {
		return sink.totalRows() == 5
	})

	if got := sink.batchCount(); got != 1 {
		t.Errorf("batches: got %d, want 1", got)
	}
}

// ─────────────────────────────────────────────────────────
// 触发条件 2：定时 flush（即使没攒满）
// ─────────────────────────────────────────────────────────

func TestBuffer_FlushOnInterval(t *testing.T) {
	sink := &fakeSink{}
	buf := NewBuffer(sink, BufferConfig{
		Capacity:      100,
		BatchSize:     1000, // 永远攒不满
		FlushInterval: 100 * time.Millisecond,
	})
	buf.Start()
	defer buf.Stop(context.Background())

	for i := 0; i < 3; i++ {
		_ = buf.Enqueue(context.Background(), makeEvent("b"))
	}

	// 等到第一次 ticker
	waitFor(t, time.Second, func() bool {
		return sink.totalRows() == 3
	})
}

// ─────────────────────────────────────────────────────────
// 满则丢：dropped 指标递增 + 不阻塞调用方
// ─────────────────────────────────────────────────────────

func TestBuffer_DropWhenFull(t *testing.T) {
	sink := &fakeSink{delay: time.Hour} // sink 卡死 → channel 一定会满
	buf := NewBuffer(sink, BufferConfig{
		Capacity:      4,
		BatchSize:     1000,
		FlushInterval: time.Hour,
	})
	buf.Start()
	defer buf.Stop(context.Background())

	dropped := 0
	for i := 0; i < 20; i++ {
		err := buf.Enqueue(context.Background(), makeEvent("c"))
		if errors.Is(err, ErrBufferFull) {
			dropped++
		}
	}

	if dropped == 0 {
		t.Errorf("expected some Enqueue to be dropped, got 0")
	}
	if got := buf.Stats().Dropped; int(got) != dropped {
		t.Errorf("Stats.Dropped = %d, want %d", got, dropped)
	}
}

// ─────────────────────────────────────────────────────────
// Stop 时 drain 残余事件做最后一次 flush
// ─────────────────────────────────────────────────────────

func TestBuffer_StopFlushesRemaining(t *testing.T) {
	sink := &fakeSink{}
	buf := NewBuffer(sink, BufferConfig{
		Capacity:      100,
		BatchSize:     1000,
		FlushInterval: time.Hour, // 让定时 flush 不发生
	})
	buf.Start()

	// 入 7 条，没到 batch size，没到 ticker
	for i := 0; i < 7; i++ {
		_ = buf.Enqueue(context.Background(), makeEvent("d"))
	}

	// 立即 Stop → 应当触发最后一次 flush
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := buf.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if got := sink.totalRows(); got != 7 {
		t.Errorf("after Stop: rows = %d, want 7", got)
	}
}

// ─────────────────────────────────────────────────────────
// Stop 后 Enqueue 立即拒绝
// ─────────────────────────────────────────────────────────

func TestBuffer_EnqueueAfterStop(t *testing.T) {
	sink := &fakeSink{}
	buf := NewBuffer(sink, BufferConfig{Capacity: 4})
	buf.Start()
	_ = buf.Stop(context.Background())

	err := buf.Enqueue(context.Background(), makeEvent("e"))
	if !errors.Is(err, ErrBufferStopped) {
		t.Errorf("Enqueue after Stop: got %v, want ErrBufferStopped", err)
	}
}

// ─────────────────────────────────────────────────────────
// flush 失败 → flushErr 计数 + 后续仍能继续工作
// ─────────────────────────────────────────────────────────

func TestBuffer_FlushErrorRecorded(t *testing.T) {
	sink := &fakeSink{}
	buf := NewBuffer(sink, BufferConfig{
		Capacity:      100,
		BatchSize:     2,
		FlushInterval: time.Hour,
	})
	buf.Start()
	defer buf.Stop(context.Background())

	// 第一批：失败
	sink.failNext.Store(true)
	_ = buf.Enqueue(context.Background(), makeEvent("err1"))
	_ = buf.Enqueue(context.Background(), makeEvent("err2"))

	waitFor(t, time.Second, func() bool {
		return buf.Stats().FlushErr == 1
	})

	// 第二批：成功 → flushed 应该增加
	_ = buf.Enqueue(context.Background(), makeEvent("ok1"))
	_ = buf.Enqueue(context.Background(), makeEvent("ok2"))

	waitFor(t, time.Second, func() bool {
		return buf.Stats().Flushed == 2
	})
}

// ─────────────────────────────────────────────────────────
// 并发 Enqueue：所有事件最终落到 sink（除非满则丢，这里 capacity 够大）
// ─────────────────────────────────────────────────────────

func TestBuffer_ConcurrentEnqueue(t *testing.T) {
	sink := &fakeSink{}
	buf := NewBuffer(sink, BufferConfig{
		Capacity:      10000,
		BatchSize:     100,
		FlushInterval: 50 * time.Millisecond,
	})
	buf.Start()

	const N = 1000
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = buf.Enqueue(context.Background(), makeEvent("conc"))
		}()
	}
	wg.Wait()

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = buf.Stop(stopCtx)

	if got := sink.totalRows(); got != N {
		t.Errorf("concurrent: total rows = %d, want %d (enqueued=%d dropped=%d)",
			got, N, buf.Stats().Enqueued, buf.Stats().Dropped)
	}
}

// 工具：轮询 cond 成立或超时
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for condition", timeout)
}
