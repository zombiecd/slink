package event

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Sink 是 Buffer 的下游契约：把一批事件落到持久存储。
//
// v0.1 由 store.ClickEventRepo 实现（PG COPY FROM）。
// v0.2 可换成 Kafka producer。
type Sink interface {
	BatchInsert(ctx context.Context, events []ClickEvent) error
}

// BufferConfig 是 Buffer 的可选参数。零值取默认。
type BufferConfig struct {
	// Capacity 是 channel 容量。默认 10_000。
	// 过小：短时洪峰丢事件多；过大：宕机时丢的事件也多。
	// 经验：QPS × 平均 flush 周期 × 2。10w QPS × 1s × 2 = 200k —— 太大了
	// 实际跳转事件可有损（只用于统计），10_000 即可（约 100ms 缓冲）。
	Capacity int

	// BatchSize 是攒够 N 条立即 flush 的阈值。默认 1000。
	BatchSize int

	// FlushInterval 是即使没攒够也定期 flush 的周期。默认 1s。
	FlushInterval time.Duration

	// EnqueueTimeout 是 Enqueue 的最大阻塞时间。默认 0（非阻塞，满即丢）。
	// > 0 时给予短暂背压等机会，但会拖慢跳转主链路 — 不建议。
	EnqueueTimeout time.Duration
}

func (c *BufferConfig) withDefaults() {
	if c.Capacity <= 0 {
		c.Capacity = 10_000
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	// EnqueueTimeout 默认 0 = 非阻塞
}

// Buffer 是单实例 channel-based 异步事件缓冲器。
//
// 三个核心保证：
//
//	1. Enqueue 不阻塞跳转主链路（满则丢，记 dropped 指标）
//	2. flush 由两个条件触发：攒够 BatchSize 或 FlushInterval 到点
//	3. Stop 优雅停机：drain channel + 最后一次 flush
//
// 使用：
//
//	buf := NewBuffer(repo, BufferConfig{})
//	buf.Start(ctx)
//	defer buf.Stop(stopCtx)
//
//	buf.Enqueue(ctx, evt) // 跳转 handler 调用
type Buffer struct {
	cfg  BufferConfig
	sink Sink

	ch   chan ClickEvent
	done chan struct{}
	wg   sync.WaitGroup

	// 启停状态机（防止重复 Start / Stop）
	started atomic.Bool
	stopped atomic.Bool

	// 指标（atomic 读写）
	enqueued atomic.Int64
	dropped  atomic.Int64
	flushed  atomic.Int64
	flushErr atomic.Int64
}

// ErrBufferStopped 在 Buffer.Stop 之后调用 Enqueue 会返回。
var ErrBufferStopped = errors.New("event buffer stopped")

// ErrBufferFull 在缓冲满 + 非阻塞模式下返回。
var ErrBufferFull = errors.New("event buffer full")

// NewBuffer 构造 Buffer。sink 不能为 nil（main 装配责任）。
func NewBuffer(sink Sink, cfg BufferConfig) *Buffer {
	cfg.withDefaults()
	return &Buffer{
		cfg:  cfg,
		sink: sink,
		ch:   make(chan ClickEvent, cfg.Capacity),
		done: make(chan struct{}),
	}
}

// Start 启动后台 flusher。重复调用是 no-op。
//
// 不接受 context — Stop 通过 done channel 取消，外部 ctx 用于 flush 时控时。
func (b *Buffer) Start() {
	if !b.started.CompareAndSwap(false, true) {
		return
	}
	b.wg.Add(1)
	go b.run()
}

// Enqueue 投递一个事件。
//
// 满则丢（dropped++），返回 ErrBufferFull。
// Stop 后调用返回 ErrBufferStopped。
//
// 实现 event.Eventer 接口。
func (b *Buffer) Enqueue(ctx context.Context, evt ClickEvent) error {
	if b.stopped.Load() {
		b.dropped.Add(1)
		return ErrBufferStopped
	}

	if b.cfg.EnqueueTimeout > 0 {
		// 阻塞模式（不推荐）：给点机会让 ch 流出去
		t := time.NewTimer(b.cfg.EnqueueTimeout)
		defer t.Stop()
		select {
		case b.ch <- evt:
			b.enqueued.Add(1)
			return nil
		case <-t.C:
			b.dropped.Add(1)
			return ErrBufferFull
		case <-ctx.Done():
			b.dropped.Add(1)
			return ctx.Err()
		}
	}

	// 默认：非阻塞 select
	select {
	case b.ch <- evt:
		b.enqueued.Add(1)
		return nil
	default:
		b.dropped.Add(1)
		return ErrBufferFull
	}
}

// Stop 优雅停机：
//
//  1. 标记 stopped（后续 Enqueue 立即返回 ErrBufferStopped）
//  2. close(done) 通知 run() 退出循环
//  3. drain 残余事件做最后一次 flush
//  4. wg.Wait() 等待 run() goroutine 真正退出
//
// stopCtx 控制最后一次 flush 的最长耗时。建议 5-10s。
func (b *Buffer) Stop(stopCtx context.Context) error {
	if !b.started.Load() {
		return nil
	}
	if !b.stopped.CompareAndSwap(false, true) {
		return nil
	}
	close(b.done)
	// 等 run() 退出（它会做最后一次 flush）
	doneCh := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
		return nil
	case <-stopCtx.Done():
		return stopCtx.Err()
	}
}

// Stats 返回累计指标快照。
type Stats struct {
	Enqueued int64
	Dropped  int64
	Flushed  int64
	FlushErr int64
}

func (b *Buffer) Stats() Stats {
	return Stats{
		Enqueued: b.enqueued.Load(),
		Dropped:  b.dropped.Load(),
		Flushed:  b.flushed.Load(),
		FlushErr: b.flushErr.Load(),
	}
}

// run 是后台 flusher 循环。
func (b *Buffer) run() {
	defer b.wg.Done()

	batch := make([]ClickEvent, 0, b.cfg.BatchSize)
	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.done:
			// 收到停机信号 → drain 并 flush
			b.drain(&batch)
			b.flush(batch)
			return

		case <-ticker.C:
			// 周期 flush（即使没攒满）
			if len(batch) > 0 {
				b.flush(batch)
				batch = batch[:0]
			}

		case evt := <-b.ch:
			batch = append(batch, evt)
			if len(batch) >= b.cfg.BatchSize {
				b.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

// drain 把 channel 里残余事件全部 append 到 batch。
// 用于停机最后一次 flush 之前。
func (b *Buffer) drain(batch *[]ClickEvent) {
	for {
		select {
		case evt := <-b.ch:
			*batch = append(*batch, evt)
		default:
			return
		}
	}
}

// flush 把 batch 写到 sink。失败仅记日志 + 增加 flushErr 指标。
//
// 不重试：上层（Eventer 用户）该用 metric/log 监控丢失率，
// 由运维介入决定是否补偿。v0.2 加 dead-letter 队列。
func (b *Buffer) flush(batch []ClickEvent) {
	if len(batch) == 0 {
		return
	}
	// flush 用独立 context（不绑 b.done）：即使在停机过程中也希望
	// 给最后一批一个写出机会。Stop 的 stopCtx 控总时长。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := b.sink.BatchInsert(ctx, batch); err != nil {
		b.flushErr.Add(1)
		slog.Error("event flush failed",
			"err", err,
			"batch_size", len(batch),
		)
		return
	}
	b.flushed.Add(int64(len(batch)))
}
