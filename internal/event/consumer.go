package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ClickEventConsumer 把 Kafka topic 的 ClickEvent 消费出来 + 攒批 + 写下游 Sink。
//
// v0.4 Day 15 灰度期使用：
//   - 上游：Kafka topic slink.click_events（producer 由 KafkaProducer 投递）
//   - 下游：影子表 click_events_shadow（NewClickEventRepoForTable）
//   - 切流后（Day 16）下游切到主表 click_events
//
// 行为契约（决策稿 v0.4-kafka.md §6.2 / §6.3）：
//
//  1. consumer group = slink.click_events.pg_writer（命名留 v0.5 加 HLL consumer 余地）
//  2. auto-commit = false，手动 commit 在 BatchInsert 成功之后
//  3. BatchInsert 失败：不 commit offset，下一轮 poll 重读（at-least-once + DB 主键去重）
//  4. decode 失败：count error 跳过（坏消息 commit 掉，避免 poison pill 卡死）
//  5. session timeout 30s（默认）
type ClickEventConsumer struct {
	cli  *kgo.Client
	sink Sink
	cfg  ConsumerConfig

	// 状态机（防重入）
	started atomic.Bool
	stopped atomic.Bool
	done    chan struct{}
	wg      sync.WaitGroup

	// 计数器（atomic 读写）。语义见 ConsumerStats 注释。
	polled       atomic.Int64
	decoded      atomic.Int64
	inserted     atomic.Int64
	decodeErrors atomic.Int64
	insertErrors atomic.Int64
}

// ConsumerConfig 是 ClickEventConsumer 的可选参数。零值取默认。
type ConsumerConfig struct {
	// Brokers 是 bootstrap 地址（host:port），至少 1 个。无默认。
	Brokers []string

	// Topic 是源 topic 名。默认 "slink.click_events"。
	Topic string

	// GroupID 是 consumer group 名。默认 "slink.click_events.pg_writer"。
	// 命名前缀让 v0.5 加 HLL consumer 时不冲突（决策稿 §6.3）。
	GroupID string

	// BatchSize 是攒够 N 条立即 BatchInsert 的阈值。默认 1000。
	// 对齐 v0.3 buffer.BufferConfig.BatchSize，COPY FROM 1000 行典型 < 5ms。
	BatchSize int

	// BatchTimeout 是即使没攒满也定期 BatchInsert 的周期。默认 100ms。
	// 短一点：上 PG 时延 < 10ms，让影子表追赶 lag 控制在毫秒量级。
	BatchTimeout time.Duration

	// SessionTimeout 是 kgo consumer group session 超时。默认 30s。
	// 太短：临时 GC 卡顿就会触发 rebalance；太长：consumer 真挂时分区无人消费。
	SessionTimeout time.Duration
}

func (c *ConsumerConfig) withDefaults() {
	if c.Topic == "" {
		c.Topic = "slink.click_events"
	}
	if c.GroupID == "" {
		c.GroupID = "slink.click_events.pg_writer"
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 1000
	}
	if c.BatchTimeout <= 0 {
		c.BatchTimeout = 100 * time.Millisecond
	}
	if c.SessionTimeout <= 0 {
		c.SessionTimeout = 30 * time.Second
	}
}

// ErrConsumerNoBrokers 在 NewClickEventConsumer 收到空 brokers 时返回。
var ErrConsumerNoBrokers = errors.New("kafka consumer: brokers required")

// ErrConsumerNoSink 在 NewClickEventConsumer 收到 nil sink 时返回。
var ErrConsumerNoSink = errors.New("kafka consumer: sink required")

// NewClickEventConsumer 构造并连接 Kafka consumer client。
//
// 不立即开始消费 — 调 Start 启动后台 poll 循环。
//
// 参数固化（决策稿 §6.3）：
//   - DisableAutoCommit：手动 commit
//   - ConsumeTopics 单 topic
//   - SessionTimeout 30s
func NewClickEventConsumer(cfg ConsumerConfig, sink Sink) (*ClickEventConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, ErrConsumerNoBrokers
	}
	if sink == nil {
		return nil, ErrConsumerNoSink
	}
	cfg.withDefaults()

	cli, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(cfg.SessionTimeout),
		// FetchMaxWait 不应超过 BatchTimeout — 否则 batch flush 永远等不到 timeout
		kgo.FetchMaxWait(cfg.BatchTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: new client: %w", err)
	}

	return &ClickEventConsumer{
		cli:  cli,
		sink: sink,
		cfg:  cfg,
		done: make(chan struct{}),
	}, nil
}

// Start 启动后台 poll + batch + flush 循环。重复调用是 no-op。
func (c *ClickEventConsumer) Start() {
	if !c.started.CompareAndSwap(false, true) {
		return
	}
	c.wg.Add(1)
	go c.run()
}

// Stop 优雅停机：
//
//  1. 标记 stopped（done channel 通知 run 退出）
//  2. 等 run 退出（含最后一批 flush）
//  3. close kgo client
//
// stopCtx 控制 wg.Wait 最长耗时。建议 5-10s。
func (c *ClickEventConsumer) Stop(stopCtx context.Context) error {
	if !c.started.Load() {
		return nil
	}
	if !c.stopped.CompareAndSwap(false, true) {
		return nil
	}
	close(c.done)

	doneCh := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
		c.cli.Close()
		return nil
	case <-stopCtx.Done():
		// 超时：仍 close client（fail-fast），但报告 ctx error
		c.cli.Close()
		return stopCtx.Err()
	}
}

// ConsumerStats 是 ClickEventConsumer 累计指标的快照。
//
// 字段语义：
//   - Polled: PollFetches 拉到的 record 总数（含 decode 失败 + 重试）
//   - Decoded: decodeClickEvent 成功的 record 数
//   - Inserted: BatchInsert 成功累积的 row 数（= 写到 PG 的事件数）
//   - DecodeErrors: JSON 反序列化失败次数（坏消息）
//   - InsertErrors: BatchInsert 失败次数（PG 抖动 / 主键冲突批量失败）
//
// 健康指标：Polled - Decoded - DecodeErrors ≈ 0（所有 record 都被处理）
// 异常信号：DecodeErrors > 0 表明 producer 侧 schema 或编码出问题
type ConsumerStats struct {
	Polled       int64 `json:"polled"`
	Decoded      int64 `json:"decoded"`
	Inserted     int64 `json:"inserted"`
	DecodeErrors int64 `json:"decode_errors"`
	InsertErrors int64 `json:"insert_errors"`
}

// Stats 返回累计指标快照。所有字段是 atomic 单点读，跨字段非一致快照。
func (c *ClickEventConsumer) Stats() ConsumerStats {
	return ConsumerStats{
		Polled:       c.polled.Load(),
		Decoded:      c.decoded.Load(),
		Inserted:     c.inserted.Load(),
		DecodeErrors: c.decodeErrors.Load(),
		InsertErrors: c.insertErrors.Load(),
	}
}

// run 是后台 poll + batch + flush 循环。
//
// 每次 PollFetches 用独立 ctx（绑 done），让 Stop 能立即打断 poll。
// BatchInsert 用独立 5s timeout ctx（同 buffer.flush），即使 done 也给最后一批写出机会。
func (c *ClickEventConsumer) run() {
	defer c.wg.Done()

	for {
		select {
		case <-c.done:
			return
		default:
		}

		// poll ctx 绑 done — Stop 时立即取消 fetch
		pollCtx, pollCancel := context.WithCancel(context.Background())
		stopWatch := make(chan struct{})
		go func() {
			select {
			case <-c.done:
				pollCancel()
			case <-stopWatch:
			}
		}()

		fetches := c.cli.PollFetches(pollCtx)
		close(stopWatch)
		pollCancel()

		// done 触发的 ctx.Canceled 不算错
		if errs := fetches.Errors(); len(errs) > 0 && !c.stopped.Load() {
			for _, e := range errs {
				slog.Warn("kafka consumer poll error",
					"topic", e.Topic,
					"partition", e.Partition,
					"err", e.Err,
				)
			}
		}

		records, ok := c.decodeFetches(fetches)
		if !ok {
			// 空 fetch（或全 decode 失败）直接进入下一轮
			continue
		}

		// PollFetches 一次可能拿回数十万条，必须按 BatchSize 切片再 flush，
		// 否则 COPY FROM 单批超大，PG 内存压力 + 错误时损失整个 batch。
		allOK := true
		for start := 0; start < len(records); start += c.cfg.BatchSize {
			end := start + c.cfg.BatchSize
			if end > len(records) {
				end = len(records)
			}
			chunk := records[start:end]

			if err := c.flushBatch(chunk); err != nil {
				c.insertErrors.Add(1)
				slog.Error("kafka consumer batch insert failed",
					"err", err,
					"batch_size", len(chunk),
				)
				allOK = false
				break // 不继续后续 chunk，下一轮 poll 同 offset 重读全部
			}
			c.inserted.Add(int64(len(chunk)))
		}

		if !allOK {
			// 任何一个 chunk 失败：不 commit，下一轮整个 fetch 重读
			continue
		}

		// 所有 chunk 都成功才 commit offset（at-least-once 保证）
		commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.cli.CommitUncommittedOffsets(commitCtx); err != nil {
			slog.Error("kafka consumer commit failed",
				"err", err,
				"batch_size", len(records),
			)
			// commit 失败：下一轮重读，DB 主键去重保证幂等
		}
		commitCancel()
	}
}

// decodeFetches 把 Kafka fetch 结果展平 + decode 全部 record。
//
// 不在这里做 BatchSize 切片 — 一次 PollFetches 可能返回数十万条 record，
// 切片由顶层 run() 循环按 BatchSize 控制（保证单次 COPY FROM 不爆 PG）。
//
// 提取出来是为了独立单测：不需要真 Kafka，传 mock fetches 即可验证 decode/计数器。
//
// 返回 (records, ok)：ok = false 时 records 为空（poll 没拿到任何 record，
// 或全部 decode 失败 — 此时也不需要走 commit 路径，让上层 continue）。
func (c *ClickEventConsumer) decodeFetches(fetches kgo.Fetches) ([]ClickEvent, bool) {
	records := make([]ClickEvent, 0, c.cfg.BatchSize)

	fetches.EachRecord(func(rec *kgo.Record) {
		c.polled.Add(1)
		evt, err := decodeClickEvent(rec.Value)
		if err != nil {
			c.decodeErrors.Add(1)
			// 坏消息跳过 — 由后续成功 batch 的 commit 一并提交 offset
			// （走到这里说明 JSON 反序列化失败；TS 字段缺失等会被 PG 校验拦下）
			return
		}
		c.decoded.Add(1)
		records = append(records, evt)
	})

	if len(records) == 0 {
		return nil, false
	}
	return records, true
}

// flushBatch 写一批到下游 Sink。独立 5s timeout ctx，对齐 buffer.flush。
func (c *ClickEventConsumer) flushBatch(batch []ClickEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.sink.BatchInsert(ctx, batch)
}
