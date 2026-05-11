package event

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
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
	wg       sync.WaitGroup // 等 healthcheck goroutine 在 Close 前退出

	// 计数器（atomic 读写）。语义见 Stats 注释。
	sent    atomic.Int64
	acked   atomic.Int64
	dropped atomic.Int64
	errors  atomic.Int64

	// healthy 反映最近一次 cli.Ping 结果。true=最近 ping 成功，false=失败或未 ping 过。
	// 由 healthcheck goroutine 周期写入，多读者无锁读（atomic.Bool）。
	// 主路径（Enqueue）不读 healthy — 不阻断跳转，仅作 observability 信号。
	healthy atomic.Bool
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
	p := &KafkaProducer{
		cli:      cli,
		topic:    cfg.Topic,
		cfg:      cfg,
		bgCtx:    bgCtx,
		bgCancel: bgCancel,
	}
	// 初始 healthy=true：避免启动后 1s 内 healthcheck 还没跑就被判 unhealthy。
	// 真实健康由首次 ping 在 healthCheckInterval 内修正。
	p.healthy.Store(true)
	// Day 24 加：同步 warmup（broker 连接 + metadata 预拉）。
	// 失败仅 warn 不阻塞启动 — 保留"broker 失联时 server 仍可启动 + healthcheck 后续自愈"语义。
	p.warmup()
	p.wg.Add(1)
	go p.runHealthCheck()
	return p, nil
}

// warmupTimeout 是 NewKafkaProducer 同步等首次 broker 连接 + metadata 拉取的上限。
// 3s 给慢网络余量；超时不阻塞启动（healthcheck 会持续修正）。
const warmupTimeout = 3 * time.Second

// warmup 在 NewKafkaProducer 返回前同步触发 broker tcp 连接 + metadata 拉取。
//
// Day 22 P4-v2 教训：drill v2 连跑 3 轮 wrk，R1 drop 集中（max 3.38% mean 1.13%），
// R2+R3 全 0。根因是 NewKafkaProducer 返回时 broker tcp + partition metadata 都是
// lazy 状态，wrk 启动瞬间高 QPS 打爆 SendTimeout 100ms 闸 — 第一波 record 在等
// metadata 时排队超时进 dropped。
//
// 加 warmup 后：NewKafkaProducer 返回时 broker 连接 + metadata 已就位（≤ 3s），第一
// 波 Enqueue 不再撞 cold-start 延迟。partition leader connection 仍由 kgo lazy
// 建立（首条 Produce 时），但单次建链延迟通常 < 10ms 不会打爆 100ms timeout。
//
// 行为：
//   - Ping 成功 → 静默继续（healthy 已是 true）
//   - Ping 失败 → slog.Warn + 标记 unhealthy；不返回 error 不阻塞启动
//
// 不发 dummy record 的原因：会污染目标 topic，consumer 需特殊跳过逻辑。Ping 触发
// 的 broker 连接 + metadata 已覆盖大部分 cold-start 场景。
func (p *KafkaProducer) warmup() {
	ctx, cancel := context.WithTimeout(p.bgCtx, warmupTimeout)
	defer cancel()
	if err := p.cli.Ping(ctx); err != nil {
		slog.Warn("kafka producer warmup ping failed (启动继续，healthcheck 后续自愈)", "err", err)
		p.healthy.Store(false)
	}
}

// healthCheckInterval / healthCheckTimeout 固化为决策稿 §5.3 同档：
//   - Interval 1s：D5 故障演练观察 broker disconnect 5s 才报错的根因是
//     RecordDeliveryTimeout，但即使把它缩短，主路径仍依赖"主动健康信号"才能立即翻
//     metric 翻 0；1s 是上限。
//   - Timeout 500ms：ping 是 broker-only Metadata，正常 < 50ms 返回；500ms 给慢
//     网络足够余量，但不会拖累下一次 ping。
const (
	healthCheckInterval = 1 * time.Second
	healthCheckTimeout  = 500 * time.Millisecond
)

// runHealthCheck 周期性 cli.Ping，把结果写到 p.healthy。
//
// 设计点：
//  1. ping 失败 → healthy.Store(false) + slog.Warn（broker 失联感知 ≤ 1s）
//  2. ping 成功 → healthy.Store(true)（自愈感知 ≤ 1s）
//  3. bgCtx done 立即退出（Close 触发）
//  4. 不影响主路径：Enqueue 永不读 healthy，只通过 metric/Stats 暴露
//
// D5 故障演练背景（Day 16）：Round 1 stop kafka 15s 期间 producer.errors 在 ~5s 之
// 后才以 100k 整数台阶飙升 — 这是 RecordDeliveryTimeout 5s 后 callback 才触发。
// 加 healthy 后告警可以 1s 内识别 broker disconnect，与 RecordDeliveryTimeout 解耦。
func (p *KafkaProducer) runHealthCheck() {
	defer p.wg.Done()
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.bgCtx.Done():
			return
		case <-ticker.C:
		}

		pingCtx, cancel := context.WithTimeout(p.bgCtx, healthCheckTimeout)
		err := p.cli.Ping(pingCtx)
		cancel()

		if err != nil {
			// 仅在状态翻转时打日志，避免 broker 持续失联时刷屏。
			if p.healthy.Swap(false) {
				slog.Warn("kafka producer healthcheck: broker unreachable", "err", err)
			}
			continue
		}
		if !p.healthy.Swap(true) {
			slog.Info("kafka producer healthcheck: broker recovered")
		}
	}
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

// Close 优雅关闭 producer：停接新 Enqueue + Flush 在飞 record + 等 healthcheck 退出。
//
// stopCtx 控 Flush 的最大耗时。建议 5-10s（同 buffer.Stop）。
//
// 调用顺序（main.go 优雅停机）：
//  1. fasthttp.Server.ShutdownWithContext（停接新请求）
//  2. KafkaProducer.Close（Flush 在飞 record + healthcheck 退出）
//  3. defer 关 client / DB（main 顶部 defer）
func (p *KafkaProducer) Close(stopCtx context.Context) error {
	// 标记 bgCtx done — 后续 Enqueue 立即 dropped；healthcheck goroutine 也由此退出
	p.bgCancel()

	// 等 healthcheck goroutine 退出，避免 cli.Close 后还有 Ping 调用
	p.wg.Wait()

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
//   - Healthy: 最近一次 healthcheck Ping 成功（由 runHealthCheck 周期写入，1s 节奏）
//
// 健康指标：(Sent - Acked - Dropped - Errors) ≈ in-flight，应稳定在 client buffer 容量内。
// 异常信号：
//   - Healthy=false 持续 > 1s：broker 失联，告警立即触发（不需等 RecordDeliveryTimeout）
//   - Dropped / Sent 比例 > 1%：broker 处理不过来或挂了（被动信号）
type KafkaStats struct {
	Sent    int64 `json:"sent"`
	Acked   int64 `json:"acked"`
	Dropped int64 `json:"dropped"`
	Errors  int64 `json:"errors"`
	Healthy bool  `json:"healthy"`
}

// Stats 返回累计指标快照。所有字段是 atomic 单点读，跨字段非一致快照。
func (p *KafkaProducer) Stats() KafkaStats {
	return KafkaStats{
		Sent:    p.sent.Load(),
		Acked:   p.acked.Load(),
		Dropped: p.dropped.Load(),
		Errors:  p.errors.Load(),
		Healthy: p.healthy.Load(),
	}
}

// CurrentWireVersion 是 producer 当前编码使用的 schema 版本。
//
// 演化规则（v0.4 Day 17 起立）：
//   - 加可选字段（向后兼容） → 不升 version
//   - 改字段含义 / 删字段 / 加必填字段 → 升 version；老 consumer 看到新 version 走 unknown 计数不阻断
//   - 切编码格式（JSON → proto） → 升 version + 走 magic byte 在外层区分（不只靠 wire 内字段）
//
// Day 13 spike 旧 schema 不兼容血泪教训：当时 topic 残留旧格式 record，新 consumer
// decode 失败一律计 decodeErrors，分不清是"坏消息"还是"新代码不识别旧消息"。
// 加 version 后两类问题分开：JSON 解析失败 → decodeErrors；JSON 解出但 version
// 未知 → unknownVersion，可独立告警。
const CurrentWireVersion uint8 = 1

// clickEventWire 是 ClickEvent 在 Kafka 上的 JSON 编码格式。
//
// schema 演化原则：所有字段加 omitempty，consumer 用宽松解码。
// v0.4 起 JSON，v0.5 看性能要不要切 protobuf（决策稿 §6.2）。
//
// 字段 V（schema_version）：
//   - omitempty 让 v=0 时不编入 JSON，向后兼容 Day 14-16 producer 写入的无版本消息
//     （decode 默认 0 → 视作 v0 兼容路径，字段集与 v1 相同）
//   - 当前 producer 写 V=CurrentWireVersion=1
type clickEventWire struct {
	V         uint8  `json:"v,omitempty"` // schema version，见 CurrentWireVersion
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
		V:         CurrentWireVersion,
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

// ErrUnknownWireVersion 在 decode 拿到 wire 但 V 字段超出已知版本范围时返回。
//
// 与普通 decode 失败（坏 JSON）区分：consumer 见到这个错误说明 producer 端 schema
// 比当前 consumer 新。应独立告警 + 计数（不混进 decodeErrors），不阻断 commit。
var ErrUnknownWireVersion = errors.New("kafka: unknown wire schema version")

// decodeClickEvent 是 consumer 侧反编码。
//
// Day 15 起被 ClickEventConsumer 使用。与 encodeClickEvent 共享 clickEventWire，
// schema 演化时只改一处。
//
// 返回 ErrUnknownWireVersion 表示 wire 解出但版本号未知；其他 error 是 JSON 失败。
// 调用方应区分两类错误分别计数（见 ConsumerStats.UnknownVersion）。
func decodeClickEvent(body []byte) (ClickEvent, error) {
	var wire clickEventWire
	if err := json.Unmarshal(body, &wire); err != nil {
		return ClickEvent{}, fmt.Errorf("kafka: decode: %w", err)
	}
	// V==0：Day 14-16 旧 producer 无版本号 record，按 v0 兼容路径解（字段集与 v1 同）
	// V==1：当前格式
	// V>1：未知（producer 比 consumer 新），返回 ErrUnknownWireVersion 让 caller 计数
	if wire.V > CurrentWireVersion {
		return ClickEvent{}, fmt.Errorf("%w: got v=%d, support v<=%d",
			ErrUnknownWireVersion, wire.V, CurrentWireVersion)
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
