package event

import (
	"context"
	"sync/atomic"
)

// DualWriter 把 ClickEvent 同时投到两个 Eventer（v0.4 灰度迁移用）。
//
// 决策稿 §8.1 双写期：
//   - primary：v0.4 新路径 KafkaProducer（待验证）
//   - secondary：v0.3 老路径 Buffer（已验证兜底）
//
// 行为契约：
//   - 串行调用两边（primary 先，secondary 后）。Buffer/Kafka 入队都 < 1ms，
//     串行总开销可忽略；并行 goroutine 创建在 86k QPS 主路径不可接受。
//   - **互不影响**：primary 失败仍调 secondary，反之亦然。
//   - 返回值：两边都失败才返回非 nil（handler 用此信号决定是否 slog.Warn）。
//     单边失败不返回错（另一边已成功，业务上算 OK；失败侧的计数器 metrics 暴露）。
//
// 退场计划：双写期 → 影子期（Day 15）→ 切流（Day 16）后删 DualWriter，
// 改为直接绑 KafkaProducer。
type DualWriter struct {
	primary   Eventer
	secondary Eventer

	// 计数器记录"双边都失败"的次数 — primary/secondary 自己各有 stats，
	// 这里只统计跨写入的整体失败信号，方便对账面板一眼看出健康度。
	bothFailed atomic.Int64
}

// NewDualWriter 构造双写外壳。primary/secondary 都不能为 nil。
//
// 命名遵循决策稿 §8.1：primary = 新路径（被验证），secondary = 兜底路径。
// 实参顺序固定，调用方不要交换。
func NewDualWriter(primary, secondary Eventer) *DualWriter {
	if primary == nil {
		panic("event: DualWriter primary is nil")
	}
	if secondary == nil {
		panic("event: DualWriter secondary is nil")
	}
	return &DualWriter{
		primary:   primary,
		secondary: secondary,
	}
}

// Enqueue 同时投到 primary + secondary。
//
// 实现 Eventer 接口。
//
// 顺序：primary 先（Kafka — 看 client buffer 满不满），secondary 后（Buffer — 兜底必到）。
// 两次调用之间不依赖：primary panic 也不会跳过 secondary（recover 在两端各自负责）。
func (d *DualWriter) Enqueue(ctx context.Context, evt ClickEvent) error {
	primaryErr := d.primary.Enqueue(ctx, evt)
	secondaryErr := d.secondary.Enqueue(ctx, evt)

	// 至少一边成功就算 OK — handler 不打 warn
	if primaryErr == nil || secondaryErr == nil {
		return nil
	}

	// 双边都失败：极少见（KafkaProducer 100ms timeout + Buffer 满 + Stop 同时触发才会到这）
	d.bothFailed.Add(1)
	// 返回 primary 错误：信息更具体（Kafka broker / network），Buffer 满几乎只会返回 ErrBufferFull。
	return primaryErr
}

// BothFailed 返回双边同时失败的累计次数。
//
// 用途：dual 模式下监控双写一致性。健康系统该值长期为 0；
// 突然增长说明 Kafka + Buffer 都打不通（极端故障）。
func (d *DualWriter) BothFailed() int64 {
	return d.bothFailed.Load()
}
