package id

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ────────────────────────────────────────────────────────────
// SegmentSource：号段来源抽象（便于 mock 单测）
// ────────────────────────────────────────────────────────────

// SegmentSource 抽象号段供应。
// 真实实现是 store.SegmentRepo，单测用 mock。
type SegmentSource interface {
	// Acquire 取下一段，返回新的 max_id（即号段右端点，闭区间）。
	// 调用方按 [max_id - stepSize + 1, max_id] 自增分配。
	Acquire(ctx context.Context, bizTag string, stepSize int64) (int64, error)
}

// segmentRange 是一个内存号段。所有字段在 DoubleBuffer.mu 保护下访问。
type segmentRange struct {
	low, high int64 // [low, high] 闭区间
	cursor    int64 // 已分配到的最后一个 ID；下一个是 cursor+1。初始 = low - 1。
}

func newSegment(low, high int64) *segmentRange {
	return &segmentRange{low: low, high: high, cursor: low - 1}
}

// usage 返回当前段使用率 [0, 1]。
func (s *segmentRange) usage() float64 {
	total := s.high - s.low + 1
	used := s.cursor - s.low + 1
	if total <= 0 {
		return 1.0
	}
	if used <= 0 {
		return 0.0
	}
	return float64(used) / float64(total)
}

// exhausted 是否已分配完。
func (s *segmentRange) exhausted() bool {
	return s.cursor >= s.high
}

// take 取下一个 ID。调用方先调 exhausted() 确认。
func (s *segmentRange) take() int64 {
	s.cursor++
	return s.cursor
}

// ────────────────────────────────────────────────────────────
// DoubleBuffer：双 buffer + 异步预取
// ────────────────────────────────────────────────────────────

// DoubleBuffer 实现号段模式的双 buffer 优化：
//
//	cur 用到 threshold（默认 90%）时，启动后台 goroutine 预取下一段填 next。
//	cur 用尽时，瞬时切到 next，无需等 DB——前台请求 0 阻塞。
//
// 状态机：
//
//	[启动]            cur=nil           → 第一次 NextID 同步取段填 cur
//	[正常分配]        cur 未到阈值       → 直接 take()
//	[阈值触发]        cur 用到 threshold → 异步取段填 next（refilling=true）
//	[正常切换]        cur 耗尽，next 已就绪 → cur=next, next=nil，异步再取
//	[异常 starvation] cur 耗尽，next 未就绪 → 同步取（罕见，5-50ms 抖动）
//	[异步失败]        DB 慢/断 → log warn + refilling=false，下次到阈值再试
type DoubleBuffer struct {
	mu        sync.Mutex
	cur       *segmentRange
	next      *segmentRange
	refilling bool // 异步预取进行中标记。在 mu 保护下读写。

	bizTag    string
	stepSize  int64
	src       SegmentSource
	threshold float64

	log *slog.Logger

	// 异步预取的超时（不应阻塞太久）
	asyncTimeout time.Duration
}

// NewDoubleBuffer 构造一个双 buffer 发号器。
//
// 启动期立刻同步取一段填 cur，确保第一次 NextID 不阻塞。
// 这段取段失败 → 直接返回 error（拒绝启动一个跑不起来的服务）。
func NewDoubleBuffer(
	ctx context.Context,
	src SegmentSource,
	bizTag string,
	stepSize int64,
	threshold float64,
	logger *slog.Logger,
) (*DoubleBuffer, error) {
	if src == nil {
		return nil, errors.New("NewDoubleBuffer: src is nil")
	}
	if bizTag == "" {
		return nil, errors.New("NewDoubleBuffer: bizTag is empty")
	}
	if stepSize <= 0 {
		return nil, fmt.Errorf("NewDoubleBuffer: stepSize must be > 0, got %d", stepSize)
	}
	if threshold <= 0 || threshold >= 1 {
		return nil, fmt.Errorf("NewDoubleBuffer: threshold must be in (0, 1), got %v", threshold)
	}
	if logger == nil {
		logger = slog.Default()
	}

	db := &DoubleBuffer{
		bizTag:       bizTag,
		stepSize:     stepSize,
		src:          src,
		threshold:    threshold,
		log:          logger.With("component", "id.DoubleBuffer", "biz_tag", bizTag),
		asyncTimeout: 5 * time.Second,
	}

	// 启动期同步取段
	high, err := src.Acquire(ctx, bizTag, stepSize)
	if err != nil {
		return nil, fmt.Errorf("initial Acquire: %w", err)
	}
	db.cur = newSegment(high-stepSize+1, high)
	db.log.Info("initial segment loaded", "low", db.cur.low, "high", db.cur.high)

	return db, nil
}

// NextID 取下一个 ID。
//
// 性能：
//   - 正常路径：纯内存自增 + mutex 抢锁，~50-100 ns/op
//   - 阈值触发路径：与正常路径无差（只是异步启动一个 goroutine）
//   - starvation 路径：持锁等 DB（罕见）
func (db *DoubleBuffer) NextID(ctx context.Context) (int64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 1. cur 用尽 → 切到 next 或同步补取
	if db.cur == nil || db.cur.exhausted() {
		if db.next != nil {
			// 平滑切换
			db.cur = db.next
			db.next = nil
			// 触发新的异步预取（如果当前不在预取中）
			db.maybeAsyncRefillLocked()
		} else {
			// starvation：next 还没就绪。持锁同步取（罕见）。
			db.log.Warn("starvation: synchronous segment fetch")
			high, err := db.src.Acquire(ctx, db.bizTag, db.stepSize)
			if err != nil {
				return 0, fmt.Errorf("synchronous Acquire: %w", err)
			}
			db.cur = newSegment(high-db.stepSize+1, high)
		}
	}

	// 2. 分配
	id := db.cur.take()

	// 3. 检查阈值，触发异步预取
	if db.cur.usage() >= db.threshold {
		db.maybeAsyncRefillLocked()
	}

	return id, nil
}

// maybeAsyncRefillLocked 在 next 为空且未在预取中时启动后台预取。
// 调用方必须持有 db.mu。
func (db *DoubleBuffer) maybeAsyncRefillLocked() {
	if db.next != nil || db.refilling {
		return
	}
	db.refilling = true
	go db.asyncRefill()
}

// asyncRefill 在后台 goroutine 取下一段填 next。
// 失败时把 refilling 重置为 false 让下次到阈值时再试。
func (db *DoubleBuffer) asyncRefill() {
	ctx, cancel := context.WithTimeout(context.Background(), db.asyncTimeout)
	defer cancel()

	high, err := db.src.Acquire(ctx, db.bizTag, db.stepSize)

	db.mu.Lock()
	defer db.mu.Unlock()

	defer func() { db.refilling = false }()

	if err != nil {
		// 静默重试由"下次到阈值再触发"机制完成。这里只 log 不 panic。
		db.log.Warn("async refill failed", "err", err.Error())
		return
	}

	if db.next != nil {
		// 防御：理论上不该发生（refilling=true 阻止重复触发）。
		// 但如果发生，丢弃这段，避免 next 被覆盖。号段浪费 = stepSize。
		db.log.Warn("async refill discarded: next already filled",
			"discarded_high", high)
		return
	}

	db.next = newSegment(high-db.stepSize+1, high)
	db.log.Debug("async refill succeeded", "low", db.next.low, "high", db.next.high)
}

// Stat 返回当前 buffer 状态（监控/调试用）。
func (db *DoubleBuffer) Stat() BufferStat {
	db.mu.Lock()
	defer db.mu.Unlock()

	s := BufferStat{Refilling: db.refilling}
	if db.cur != nil {
		s.CurLow, s.CurHigh, s.CurCursor = db.cur.low, db.cur.high, db.cur.cursor
		s.CurUsage = db.cur.usage()
	}
	if db.next != nil {
		s.NextReady = true
		s.NextLow, s.NextHigh = db.next.low, db.next.high
	}
	return s
}

// BufferStat 是 DoubleBuffer 的瞬时快照。
type BufferStat struct {
	CurLow    int64   `json:"cur_low"`
	CurHigh   int64   `json:"cur_high"`
	CurCursor int64   `json:"cur_cursor"`
	CurUsage  float64 `json:"cur_usage"`
	NextReady bool    `json:"next_ready"`
	NextLow   int64   `json:"next_low"`
	NextHigh  int64   `json:"next_high"`
	Refilling bool    `json:"refilling"`
}

// ────────────────────────────────────────────────────────────
// Generator：buffer + base62 编码的上层门面
// ────────────────────────────────────────────────────────────

// Generator 把发号器与编码层组装成一个开箱即用的短码生成器。
// 这是上层（API handler）唯一应该依赖的类型。
type Generator struct {
	buf *DoubleBuffer
}

// NewGenerator 用现成的 DoubleBuffer 包装。
func NewGenerator(buf *DoubleBuffer) *Generator {
	return &Generator{buf: buf}
}

// NextID 取下一个原始整数 ID。
// 主要给监控 / 测试用，业务路径调 NextCode。
func (g *Generator) NextID(ctx context.Context) (int64, error) {
	return g.buf.NextID(ctx)
}

// NextCode 取下一个 ID 并编码为 6 位 Base62 短码。
//
// 这是 v0.1 创建短链的主入口：
//
//	code, err := generator.NextCode(ctx)
//	// code 形如 "5BxX9k"
func (g *Generator) NextCode(ctx context.Context) (string, error) {
	id, err := g.buf.NextID(ctx)
	if err != nil {
		return "", err
	}
	return EncodeID(id)
}

// Stat 透传 buffer 状态。
func (g *Generator) Stat() BufferStat {
	return g.buf.Stat()
}
