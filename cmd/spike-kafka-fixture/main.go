// spike-kafka-fixture 是 v0.5 Day 18 第三组 spike (Kafka Engine 直消) 的 fixture producer。
//
// 与 cmd/spike-clickhouse-v2 / cmd/spike-ch-go 严格同口径：
//   - 5M 上限 / 30s 时间窗 / batch=1000
//   - 同 codes 池（2000）/ 同 ips 池（500）/ 同 country 轮转
//
// 关键差异：本 spike 不直接写 ClickHouse，而是把 fixture 投到 Kafka topic
// (v0.4 同 topic slink.click_events)，由 ClickHouse 端 Kafka Engine + MaterializedView
// 自动消费写入 click_events_ch_kafka_target。throughput 测量方式：
//   - 本程序结束后立即停（producer 端 send 完毕）
//   - 在 ClickHouse 上 query SELECT count() FROM click_events_ch_kafka_target
//   - 计算 0 → 5M 的 wall-clock 时长 → 算端到端 rows/s
//
// JSON wire 与 v0.4 producer 一致（internal/event/kafka.go::clickEventWire）：
//   {"v":1,"event_id":"<uuid>","code":"...","ip":"...","user_agent":"...","referer":"...","country":"...","region":"...","ts_ms":<int64>}
//
// 不是生产代码 — Day 18 选型完成后会被删/归档（同 spike-kgo / spike-sarama / spike-* 处理）。
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	duration  = 30 * time.Second
	batchSize = 1000
	totalRows = 5_000_000
)

type wire struct {
	V         uint8  `json:"v,omitempty"`
	EventID   string `json:"event_id"`
	Code      string `json:"code"`
	IP        string `json:"ip"`
	UserAgent string `json:"user_agent"`
	Referer   string `json:"referer"`
	Country   string `json:"country"`
	Region    string `json:"region"`
	TsMs      int64  `json:"ts_ms"`
}

func main() {
	brokers := flag.String("brokers", "localhost:19092", "kafka brokers (host:port)")
	topic := flag.String("topic", "slink.click_events", "topic")
	flag.Parse()

	cli, err := kgo.NewClient(
		kgo.SeedBrokers(*brokers),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
		kgo.MaxBufferedRecords(200_000),
		kgo.ProducerLinger(5*time.Millisecond),
		kgo.RecordDeliveryTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatalf("new client: %v", err)
	}
	defer cli.Close()

	ctx := context.Background()

	if err := cli.Ping(ctx); err != nil {
		log.Fatalf("ping kafka: %v", err)
	}

	codes := genCodePool(2000)
	ips := genIPPool(500)

	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	start := time.Now()
	deadline := start.Add(duration)

	var (
		sent      int64
		acked     atomic.Int64
		errored   atomic.Int64
		batchLats []time.Duration
	)

	for sent < totalRows && time.Now().Before(deadline) {
		bs := batchSize
		if remain := int(totalRows - sent); remain < bs {
			bs = remain
		}

		nowMs := time.Now().UnixMilli()
		batchStart := time.Now()

		for i := 0; i < bs; i++ {
			idx := sent + int64(i)
			w := wire{
				V:         1,
				EventID:   uuid.New().String(),
				Code:      codes[idx%int64(len(codes))],
				IP:        ips[idx%int64(len(ips))],
				UserAgent: "Mozilla/5.0 (spike) Gecko/20100101",
				Referer:   "https://x.test/spike",
				Country:   countryRotation(idx),
				Region:    "SH",
				TsMs:      nowMs + int64(i),
			}
			data, err := json.Marshal(w)
			if err != nil {
				log.Fatalf("marshal: %v", err)
			}
			cli.Produce(ctx, &kgo.Record{Topic: *topic, Value: data}, func(_ *kgo.Record, e error) {
				if e != nil {
					errored.Add(1)
				} else {
					acked.Add(1)
				}
			})
		}
		batchLats = append(batchLats, time.Since(batchStart))
		sent += int64(bs)
	}

	// 等所有 in-flight 入 broker buffer 并 ack
	if err := cli.Flush(ctx); err != nil {
		log.Printf("flush: %v", err)
	}

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	report(sent, acked.Load(), errored.Load(), batchLats, start, memBefore, memAfter)
}

func genCodePool(n int) []string {
	pool := make([]string, n)
	for i := range pool {
		var b [4]byte
		_, _ = rand.Read(b[:])
		pool[i] = hex.EncodeToString(b[:])[:6]
	}
	return pool
}

func genIPPool(n int) []string {
	pool := make([]string, n)
	for i := range pool {
		var b [4]byte
		_, _ = rand.Read(b[:])
		pool[i] = fmt.Sprintf("%d.%d.%d.%d", b[0], b[1], b[2], b[3])
	}
	return pool
}

func countryRotation(idx int64) string {
	pool := []string{"CN", "US", "JP", "DE", "GB", "FR", "IN", "BR"}
	return pool[idx%int64(len(pool))]
}

func report(sent, acked, errored int64, lats []time.Duration, start time.Time, before, after runtime.MemStats) {
	elapsed := time.Since(start)
	rps := float64(sent) / elapsed.Seconds()
	allocDelta := after.TotalAlloc - before.TotalAlloc

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	var p50, p99 time.Duration
	if len(lats) > 0 {
		p50 = lats[len(lats)/2]
		p99 = lats[len(lats)*99/100]
	}

	fmt.Printf("=== spike-kafka-fixture (kgo producer → Kafka topic) ===\n")
	fmt.Printf("  duration         %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  sent             %d\n", sent)
	fmt.Printf("  acked            %d\n", acked)
	fmt.Printf("  errored          %d\n", errored)
	fmt.Printf("  rows/s (sent)    %.0f\n", rps)
	fmt.Printf("  batch fill p50   %s\n", p50.Round(time.Microsecond))
	fmt.Printf("  batch fill p99   %s\n", p99.Round(time.Microsecond))
	fmt.Printf("  alloc/op         %.0f B/row\n", float64(allocDelta)/float64(sent))
	fmt.Printf("  total alloc      %.1f MB\n", float64(allocDelta)/1024/1024)
	fmt.Println()
	fmt.Println("→ 下一步在 ClickHouse 上 query 端到端 throughput：")
	fmt.Println("  SELECT count() FROM slink_analytics.click_events_ch_kafka_target")
	fmt.Println("  （0 → sent 行的 wall-clock 时长 = CH Kafka Engine 端到端速度）")
}
