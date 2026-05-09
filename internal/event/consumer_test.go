package event

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeSink 是 consumer 测试专用 sink，记录每次 BatchInsert 的 batch。
//
// Day 16 切流前住在 buffer_test.go（被 buffer 单测复用）。Day 16 删 buffer.go
// 后挪到这里 — 仍然是测试-only 类型，不进生产代码。
type fakeSink struct {
	mu       sync.Mutex
	batches  [][]ClickEvent
	failNext atomic.Bool // 设为 true 时下一次 BatchInsert 返回错误
}

func (f *fakeSink) BatchInsert(ctx context.Context, evts []ClickEvent) error {
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

// makeRecord 把 ClickEvent encode 成 *kgo.Record（复用 producer 编码路径）。
func makeRecord(t *testing.T, evt ClickEvent) *kgo.Record {
	t.Helper()
	body, err := encodeClickEvent(evt)
	if err != nil {
		t.Fatalf("encodeClickEvent: %v", err)
	}
	return &kgo.Record{
		Key:   []byte(evt.Code),
		Value: body,
		Topic: "slink.click_events",
	}
}

// makeBadRecord 构造 decode 必失败的 record（不是合法 JSON）。
func makeBadRecord(payload string) *kgo.Record {
	return &kgo.Record{
		Key:   []byte("bad"),
		Value: []byte(payload),
		Topic: "slink.click_events",
	}
}

// fetchesFromRecords 把 *kgo.Record 列表包成 kgo.Fetches。
//
// 利用 kgo 公共 API：构造 FetchTopic + FetchPartition，避免依赖真 broker。
func fetchesFromRecords(recs ...*kgo.Record) kgo.Fetches {
	if len(recs) == 0 {
		return kgo.Fetches{}
	}
	return kgo.Fetches{
		{
			Topics: []kgo.FetchTopic{
				{
					Topic: recs[0].Topic,
					Partitions: []kgo.FetchPartition{
						{
							Partition: 0,
							Records:   recs,
						},
					},
				},
			},
		},
	}
}

// newTestConsumer 直接构造 ClickEventConsumer，绕过 NewClickEventConsumer
// 的 broker 连接。仅测 decodeFetches / flushBatch / Stats 这一段纯逻辑。
func newTestConsumer(sink Sink, batchSize int) *ClickEventConsumer {
	cfg := ConsumerConfig{
		Brokers:   []string{"localhost:9092"}, // 不会真连
		BatchSize: batchSize,
	}
	cfg.withDefaults()
	return &ClickEventConsumer{
		cfg:  cfg,
		sink: sink,
	}
}

func sampleEvent(code string) ClickEvent {
	return ClickEvent{
		EventID:   "11111111-1111-1111-1111-111111111111",
		Code:      code,
		IP:        net.ParseIP("10.0.0.1"),
		UserAgent: "test-ua",
		Referer:   "https://x.test",
		Country:   "CN",
		Region:    "SH",
		TS:        time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC),
	}
}

func TestConsumer_ProcessFetches_Success(t *testing.T) {
	c := newTestConsumer(&fakeSink{}, 10)

	fetches := fetchesFromRecords(
		makeRecord(t, sampleEvent("a")),
		makeRecord(t, sampleEvent("b")),
		makeRecord(t, sampleEvent("c")),
	)

	batch, ok := c.decodeFetches(fetches)
	if !ok {
		t.Fatal("expected ok=true for non-empty fetches")
	}
	if len(batch) != 3 {
		t.Errorf("batch len: got %d want 3", len(batch))
	}

	stats := c.Stats()
	if stats.Polled != 3 || stats.Decoded != 3 || stats.DecodeErrors != 0 {
		t.Errorf("stats: %+v", stats)
	}
}

func TestConsumer_ProcessFetches_DecodeErrorSkipped(t *testing.T) {
	c := newTestConsumer(&fakeSink{}, 10)

	fetches := fetchesFromRecords(
		makeRecord(t, sampleEvent("good1")),
		makeBadRecord("not json"),
		makeRecord(t, sampleEvent("good2")),
		makeBadRecord("{invalid"),
	)

	batch, ok := c.decodeFetches(fetches)
	if !ok {
		t.Fatal("expected ok=true (still has good records)")
	}
	if len(batch) != 2 {
		t.Errorf("batch len: got %d want 2 (good only)", len(batch))
	}

	stats := c.Stats()
	if stats.Polled != 4 {
		t.Errorf("Polled: got %d want 4", stats.Polled)
	}
	if stats.Decoded != 2 {
		t.Errorf("Decoded: got %d want 2", stats.Decoded)
	}
	if stats.DecodeErrors != 2 {
		t.Errorf("DecodeErrors: got %d want 2", stats.DecodeErrors)
	}
}

func TestConsumer_ProcessFetches_EmptyReturnsNotOk(t *testing.T) {
	c := newTestConsumer(&fakeSink{}, 10)

	batch, ok := c.decodeFetches(kgo.Fetches{})
	if ok {
		t.Error("expected ok=false for empty fetches")
	}
	if batch != nil {
		t.Errorf("expected nil batch, got len=%d", len(batch))
	}

	stats := c.Stats()
	if stats.Polled != 0 || stats.Decoded != 0 {
		t.Errorf("stats should be zero: %+v", stats)
	}
}

func TestConsumer_ProcessFetches_AllBadRecordsReturnsNotOk(t *testing.T) {
	c := newTestConsumer(&fakeSink{}, 10)

	fetches := fetchesFromRecords(
		makeBadRecord("garbage1"),
		makeBadRecord("garbage2"),
	)

	batch, ok := c.decodeFetches(fetches)
	if ok {
		t.Error("expected ok=false when all records fail decode")
	}
	if len(batch) != 0 {
		t.Errorf("expected empty batch, got len=%d", len(batch))
	}

	stats := c.Stats()
	if stats.DecodeErrors != 2 {
		t.Errorf("DecodeErrors: got %d want 2", stats.DecodeErrors)
	}
}

func TestConsumer_FlushBatch_Success(t *testing.T) {
	sink := &fakeSink{}
	c := newTestConsumer(sink, 10)

	batch := []ClickEvent{sampleEvent("a"), sampleEvent("b")}
	if err := c.flushBatch(batch); err != nil {
		t.Fatalf("flushBatch: %v", err)
	}

	if got := sink.totalRows(); got != 2 {
		t.Errorf("sink rows: got %d want 2", got)
	}
}

func TestConsumer_FlushBatch_PropagatesError(t *testing.T) {
	sink := &fakeSink{}
	sink.failNext.Store(true)
	c := newTestConsumer(sink, 10)

	batch := []ClickEvent{sampleEvent("a")}
	err := c.flushBatch(batch)
	if err == nil {
		t.Fatal("expected flushBatch error")
	}
	if !strings.Contains(err.Error(), "simulated flush failure") {
		t.Errorf("expected wrapped sink error, got: %v", err)
	}
}

func TestConsumerConfig_WithDefaults(t *testing.T) {
	cases := []struct {
		name string
		in   ConsumerConfig
		want ConsumerConfig
	}{
		{
			name: "all zero → defaults",
			in:   ConsumerConfig{},
			want: ConsumerConfig{
				Topic:          "slink.click_events",
				GroupID:        "slink.click_events.pg_writer",
				BatchSize:      1000,
				BatchTimeout:   100 * time.Millisecond,
				SessionTimeout: 30 * time.Second,
			},
		},
		{
			name: "explicit values preserved",
			in: ConsumerConfig{
				Topic:          "custom.topic",
				GroupID:        "custom.group",
				BatchSize:      500,
				BatchTimeout:   50 * time.Millisecond,
				SessionTimeout: 10 * time.Second,
			},
			want: ConsumerConfig{
				Topic:          "custom.topic",
				GroupID:        "custom.group",
				BatchSize:      500,
				BatchTimeout:   50 * time.Millisecond,
				SessionTimeout: 10 * time.Second,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.in
			got.withDefaults()
			if got.Topic != tc.want.Topic ||
				got.GroupID != tc.want.GroupID ||
				got.BatchSize != tc.want.BatchSize ||
				got.BatchTimeout != tc.want.BatchTimeout ||
				got.SessionTimeout != tc.want.SessionTimeout {
				t.Errorf("withDefaults: got %+v want %+v", got, tc.want)
			}
		})
	}
}

func TestNewClickEventConsumer_ValidationErrors(t *testing.T) {
	t.Run("no brokers → ErrConsumerNoBrokers", func(t *testing.T) {
		_, err := NewClickEventConsumer(ConsumerConfig{}, &fakeSink{})
		if !errors.Is(err, ErrConsumerNoBrokers) {
			t.Errorf("got err=%v want ErrConsumerNoBrokers", err)
		}
	})
	t.Run("no sink → ErrConsumerNoSink", func(t *testing.T) {
		_, err := NewClickEventConsumer(
			ConsumerConfig{Brokers: []string{"localhost:9092"}},
			nil,
		)
		if !errors.Is(err, ErrConsumerNoSink) {
			t.Errorf("got err=%v want ErrConsumerNoSink", err)
		}
	})
}

func TestConsumer_StopBeforeStartIsNoOp(t *testing.T) {
	c := newTestConsumer(&fakeSink{}, 10)
	// 没 Start 就 Stop — 应该 return nil 不 panic
	if err := c.Stop(t.Context()); err != nil {
		t.Errorf("Stop without Start: %v", err)
	}
}
