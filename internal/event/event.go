// Package event 处理 slink 的异步事件链路。
//
// v0.4 链路（Day 16 切流后）：
//
//	api/redirect.go  ──Enqueue──▶  KafkaProducer ──▶ Kafka topic
//	                                                       │
//	                                                       ▼ poll batch + COPY FROM
//	                                                ClickEventConsumer ──▶ store.ClickEventRepo
//
// v0.3 老路径（channel buffer）已在 Day 16 删除。代码考古见 git tag v0.3-buffer-final。
package event

import (
	"context"
	"net"
	"time"
)

// ClickEvent 是一次跳转的原始事件。
//
// 字段映射 PG 表 click_events（见 migrations/0001_init.up.sql）。
// IP / Country / Region 在 v0.1 仅采集 IP，地理解析放 v0.3。
type ClickEvent struct {
	EventID   string    // UUID v4
	Code      string    // 短码
	IP        net.IP    // 客户端 IP（X-Forwarded-For 或 RemoteAddr）
	UserAgent string    // 完整 UA 串
	Referer   string    // HTTP Referer 头
	Country   string    // v0.3 填
	Region    string    // v0.3 填
	TS        time.Time // 跳转发生时间（服务端记录）
}

// Eventer 是异步事件投递的契约（producer 侧）。
//
// 接口足够小：只暴露 Enqueue。
// 这样 api 层依赖接口，实现可独立演进（v0.3 channel buffer → v0.4 KafkaProducer）。
type Eventer interface {
	// Enqueue 投递一个事件到异步队列。
	//
	// 必须满足：
	//   - 不阻塞跳转主链路（要么瞬间入队，要么超时直接丢）
	//   - 上层不关心成功/失败：返回 error 仅供日志/指标用
	//   - 实现侧应内部统计丢弃数，通过 metrics 暴露
	Enqueue(ctx context.Context, evt ClickEvent) error
}

// Sink 是异步事件下游写库的契约（consumer 侧）。
//
// v0.4 由 store.ClickEventRepo 实现（PG COPY FROM）。
// ClickEventConsumer 拿一个 Sink，poll 拿到的 batch 走 BatchInsert 写到下游表。
type Sink interface {
	BatchInsert(ctx context.Context, events []ClickEvent) error
}

// NopEventer 是空实现，本地测试 / 关闭事件采集时用。
type NopEventer struct{}

func (NopEventer) Enqueue(_ context.Context, _ ClickEvent) error { return nil }
