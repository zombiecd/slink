// Package event 处理 slink 的异步事件链路。
//
// v0.1 链路：
//
//	api/redirect.go  ──Enqueue──▶  Buffer (channel)
//	                                  │
//	                                  ▼ flush（1s 或 1000 条）
//	                            store.ClickEventRepo.BatchInsert
//
// v0.2 计划：把 channel 换成 Kafka producer，store 入库改为 consumer。
// 上层 API 看到的 Eventer 接口不变。
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

// Eventer 是异步事件投递的契约。
//
// 接口足够小：只暴露 Enqueue。
// 这样 api 层依赖接口，buffer 实现可独立演进（单实例 channel → Kafka producer）。
type Eventer interface {
	// Enqueue 投递一个事件到异步队列。
	//
	// 必须满足：
	//   - 不阻塞跳转主链路（要么瞬间入 channel，要么超时直接丢）
	//   - 上层不关心成功/失败：返回 error 仅供日志/指标用
	//   - 实现侧应内部统计丢弃数（buffer 满），通过 metrics 暴露
	Enqueue(ctx context.Context, evt ClickEvent) error
}

// NopEventer 是空实现，本地测试 / 关闭事件采集时用。
type NopEventer struct{}

func (NopEventer) Enqueue(_ context.Context, _ ClickEvent) error { return nil }
