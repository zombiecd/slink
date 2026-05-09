package event

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaProducer 把 ClickEvent 异步投递到 Kafka topic（v0.4 新路径）。
//
// 实现 Eventer 接口，行为契约（决策稿 v0.4-kafka.md §5.4）：
//
//  1. Enqueue 主路径 100ms 上限：record 在 SendTimeout 内必须入 client buffer，
//     否则 callback 拿到 ctx.DeadlineExceeded → dropped++（不影响 v0.3 跳转 P99）
//  2. broker 不可达：client buffer 慢慢满 → 后续 Enqueue 直接 ctx done → dropped++
//  3. ack 错误：callback 拿到 broker 错误 → errors++
//  4. ack 成功：callback → acked++
//
// 注意：Enqueue 不返回 broker 错误。错误状态通过 Stats() 暴露，
// handler 拿到 error 仅作 slog.Warn，不能阻塞跳转。
//
// 库选型决策见 docs/concepts/kafka-client-choice.md（kgo 1.78× sarama）。
type KafkaProducer struct {
	cli   *kgo.Client
	topic string
	cfg   KafkaConfig

	// bgCtx 是 producer 生命周期 ctx，独立于 caller ctx。
	// 这样 caller cancel 不影响已交给 client buffer 的 record 投递。
	bgCtx    context.Context
	bgCancel context.CancelFunc

	// 计数器（atomic 读写）。语义见 Stats 注释。
	sent    atomic.Int64
	acked   atomic.Int64
	dropped atomic.Int64
	errors  atomic.Int64
}

// KafkaConfig 是 KafkaProducer 的可选参数。零值取默认。
type KafkaConfig struct {
	// Brokers 是 bootstrap 地址（host:port），至少 1 个。无默认。
	Brokers []string

	// Topic 是目标 topic 名。默认 "slink.click_events"。
	Topic string

	// SendTimeout 是单次 Enqueue 入 client buffer 的最大等待时间。
	// 主路径 100ms 硬上限：buffer 满时 ctx done → record 被 drop。
	// 默认 100ms，对应决策稿 §5.4 第一道闸。
	SendTimeout time.Duration

	// MaxBufferedRecords 是 kgo client 内部 record 缓冲上限。
	// 默认 100_000：86k QPS × 1s = 86k records，预留 1× 余量。
	// 满时 Produce 会阻塞等空位（ctx 超时则 cancel）。
	MaxBufferedRecords int

	// RecordDeliveryTimeout 是 record 入 buffer 后到 ack 的端到端超时。
	// 包含重试。默认 5s。超时 callback 拿到 timeout error → errors++。
	RecordDeliveryTimeout time.Duration

	// LingerMS / Compression 起步固化为决策稿 §5.3 值（5ms / lz4），
	// 不暴露为参数避免误调。后续如需调优再开。
}

func (c *KafkaConfig) withDefaults() {
	if c.Topic == "" {
		c.Topic = "slink.click_events"
	}
	if c.SendTimeout <= 0 {
		c.SendTimeout = 100 * time.Millisecond
	}
	if c.MaxBufferedRecords <= 0 {
		c.MaxBufferedRecords = 100_000
	}
	if c.RecordDeliveryTimeout <= 0 {
		c.RecordDeliveryTimeout = 5 * time.Second
	}
}

// ErrKafkaNoBrokers 在 NewKafkaProducer 收到空 brokers 时返回。
var ErrKafkaNoBrokers = errors.New("kafka: brokers required")

// NewKafkaProducer 构造并连接 Kafka client。
//
// 参数固化（决策稿 §5.3，与 spike-kgo 同口径）：
//   - DisableIdempotentWrite + LeaderAck：单 broker 单机版本，无 ISR
//   - Lz4Compression：~100B 短链事件，lz4 CPU 开销最低
//   - Linger 5ms：平衡批量收益 vs 主路径延迟
//   - MaxProduceRequestsInflightPerBroker 5：sarama 默认值，平衡吞吐和顺序
func NewKafkaProducer(cfg KafkaConfig) (*KafkaProducer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, ErrKafkaNoBrokers
	}
	cfg.withDefaults()

	cli, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.DisableIdempotentWrite(),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.MaxProduceRequestsInflightPerBroker(5),
		kgo.MaxBufferedRecords(cfg.MaxBufferedRecords),
		kgo.RecordDeliveryTimeout(cfg.RecordDeliveryTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}

	bgCtx, bgCancel := context.WithCancel(context.Background())
	return &KafkaProducer{
		cli:      cli,
		topic:    cfg.Topic,
		cfg:      cfg,
		bgCtx:    bgCtx,
		bgCancel: bgCancel,
	}, nil
}

// Enqueue 把 ClickEvent 异步投递到 Kafka。
//
// 实现 Eventer 接口。
//
// 行为：
//   - 入 client buffer 成功：sent++，立即返回 nil（ack 异步在 callback 处理）
//   - 100ms 内未入 buffer（buffer 满 + broker 不可达）：dropped++，返回 ErrKafkaTimeout
//   - 序列化失败：errors++，返回 error（永远不应发生，ClickEvent 字段都是 string/time）
//
// caller ctx 被忽略 — 主路径 ctx 取消不应影响已入 buffer 的 record。
// 100ms 上限通过 p.bgCtx + SendTimeout 保证。
func (p *KafkaProducer) Enqueue(_ context.Context, evt ClickEvent) error {
	body, err := encodeClickEvent(evt)
	if err != nil {
		p.errors.Add(1)
		return fmt.Errorf("kafka: encode: %w", err)
	}

	rec := &kgo.Record{
		Key:   []byte(evt.Code), // 同 code 落同 partition（决策稿 §4.3）
		Value: body,
		Topic: p.topic,
	}

	// 100ms 主路径硬上限：buffer 没满立即返回，buffer 满则 ctx done → callback 拿 cancel
	sendCtx, cancel := context.WithTimeout(p.bgCtx, p.cfg.SendTimeout)

	// 闭包内 cancel：record 处理完（成功或失败）才释放 ctx。
	// 不能 defer cancel — Enqueue 立即返回，callback 在 ctx 生命周期之外。
	p.cli.Produce(sendCtx, rec, func(_ *kgo.Record, ackErr error) {
		cancel()
		p.handleAck(ackErr)
	})

	p.sent.Add(1)
	return nil
}

// handleAck 在 kgo callback 里更新 ack 状态计数器。
//
// ctx done（超时/cancel）→ dropped；broker 错误 → errors；nil → acked。
func (p *KafkaProducer) handleAck(err error) {
	if err == nil {
		p.acked.Add(1)
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		p.dropped.Add(1)
		// 不打日志：100ms timeout 是预期 drop 行为，高频日志会刷屏。
		// 通过 slink_kafka_producer_dropped_total 指标 + 告警监控即可。
		return
	}
	p.errors.Add(1)
	slog.Warn("kafka producer ack error", "err", err)
}

// Close 优雅关闭 producer：停接新 Enqueue + Flush 在飞 record。
//
// stopCtx 控 Flush 的最大耗时。建议 5-10s（同 buffer.Stop）。
//
// 调用顺序（main.go 优雅停机）：
//  1. fasthttp.Server.ShutdownWithContext（停接新请求）
//  2. KafkaProducer.Close（Flush 在飞 record，类比 Buffer.Stop）
//  3. defer 关 client / DB（main 顶部 defer）
func (p *KafkaProducer) Close(stopCtx context.Context) error {
	// 标记 bgCtx done — 后续 Enqueue 调用立即 dropped
	p.bgCancel()

	// Flush 等待 client buffer 中 record 全部 ack（或 stopCtx 超时）
	if err := p.cli.Flush(stopCtx); err != nil {
		slog.Error("kafka producer flush", "err", err)
	}

	p.cli.Close()
	return nil
}

// KafkaStats 是 KafkaProducer 累计指标的快照。
//
// 字段语义：
//   - Sent: Enqueue 成功（已交给 client buffer，未必已 ack）
//   - Acked: callback 拿到 nil err（broker 已确认）
//   - Dropped: 主路径 100ms timeout 或停机后被丢
//   - Errors: broker 错误（网络 / 元数据 / 编码失败等）
//
// 健康指标：(Sent - Acked - Dropped - Errors) ≈ in-flight，应稳定在 client buffer 容量内。
// 异常信号：Dropped / Sent 比例 > 1% 表明 broker 处理不过来或挂了。
type KafkaStats struct {
	Sent    int64 `json:"sent"`
	Acked   int64 `json:"acked"`
	Dropped int64 `json:"dropped"`
	Errors  int64 `json:"errors"`
}

// Stats 返回累计指标快照。所有字段是 atomic 单点读，跨字段非一致快照。
func (p *KafkaProducer) Stats() KafkaStats {
	return KafkaStats{
		Sent:    p.sent.Load(),
		Acked:   p.acked.Load(),
		Dropped: p.dropped.Load(),
		Errors:  p.errors.Load(),
	}
}

// clickEventWire 是 ClickEvent 在 Kafka 上的 JSON 编码格式。
//
// schema 演化原则：所有字段加 omitempty，consumer 用宽松解码。
// v0.4 起 JSON，v0.5 看性能要不要切 protobuf（决策稿 §6.2）。
type clickEventWire struct {
	EventID   string `json:"event_id"`
	Code      string `json:"code"`
	IP        string `json:"ip,omitempty"`
	UserAgent string `json:"ua,omitempty"`
	Referer   string `json:"referer,omitempty"`
	Country   string `json:"country,omitempty"`
	Region    string `json:"region,omitempty"`
	TSMillis  int64  `json:"ts_ms"` // unix milli, consumer 解 → time.UnixMilli
}

func encodeClickEvent(evt ClickEvent) ([]byte, error) {
	wire := clickEventWire{
		EventID:   evt.EventID,
		Code:      evt.Code,
		UserAgent: evt.UserAgent,
		Referer:   evt.Referer,
		Country:   evt.Country,
		Region:    evt.Region,
		TSMillis:  evt.TS.UnixMilli(),
	}
	if evt.IP != nil {
		wire.IP = evt.IP.String()
	}
	return json.Marshal(&wire)
}

// decodeClickEvent 是 consumer 侧反编码。
//
// Day 15 起被 ClickEventConsumer 使用。与 encodeClickEvent 共享 clickEventWire，
// schema 演化时只改一处。
func decodeClickEvent(body []byte) (ClickEvent, error) {
	var wire clickEventWire
	if err := json.Unmarshal(body, &wire); err != nil {
		return ClickEvent{}, fmt.Errorf("kafka: decode: %w", err)
	}
	evt := ClickEvent{
		EventID:   wire.EventID,
		Code:      wire.Code,
		UserAgent: wire.UserAgent,
		Referer:   wire.Referer,
		Country:   wire.Country,
		Region:    wire.Region,
		TS:        time.UnixMilli(wire.TSMillis),
	}
	if wire.IP != "" {
		evt.IP = net.ParseIP(wire.IP)
	}
	return evt, nil
}

// ErrKafkaTimeout 不再返回（callback 内部统计），保留为文档锚点。
//
//nolint:unused // 文档锚点
var ErrKafkaTimeout = errors.New("kafka: send timeout (record buffer full or broker unreachable)")
