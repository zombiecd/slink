// spike-sarama 是 v0.4 Kafka 客户端选型 spike（IBM/sarama）。
//
// 用途：在固定参数下与 spike-kgo 对比 throughput / ack 延迟 / 内存。
// 不是生产代码 — Day 13 选型完成后会被删/归档。
//
// 测法（与 spike-kgo 严格对齐）：
//   - 单 goroutine async 喂 ~100 byte click_event JSON 30s
//   - 配置：lz4 / acks=leader / linger=5ms / max in-flight=5
//   - 收 ack 回调记录 enqueue→ack 延迟
//   - 30s 后停止生产，最多等 5s 让在飞 ack 落地
//   - 输出：RPS / latency p50 p99 / 总字节 / heap alloc 增量
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

const (
	topic    = "slink.click_events"
	duration = 30 * time.Second
	drainMax = 5 * time.Second
)

// payload 模拟 ClickEvent JSON ~100 byte，避免被 lz4 过度压缩失真。
type payload struct {
	EventID string `json:"event_id"`
	Code    string `json:"code"`
	IP      string `json:"ip"`
	UA      string `json:"ua"`
	TS      int64  `json:"ts"`
}

func main() {
	brokers := flag.String("brokers", "localhost:19092", "kafka bootstrap (host)")
	flag.Parse()

	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForLocal // = LeaderAck
	cfg.Producer.Compression = sarama.CompressionLZ4
	cfg.Producer.Flush.Frequency = 5 * time.Millisecond // ≈ linger
	cfg.Producer.Flush.MaxMessages = 0
	cfg.Net.MaxOpenRequests = 5 // max in-flight per broker
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Version = sarama.V3_6_0_0

	prod, err := sarama.NewAsyncProducer([]string{*brokers}, cfg)
	if err != nil {
		log.Fatalf("new producer: %v", err)
	}
	// Close 必须显式调（关 Successes/Errors 触发 done goroutine 退出）；
	// 不用 defer 避免与末尾的显式 Close 重复 close channel 触发 panic。

	var (
		sent      atomic.Int64
		acked     atomic.Int64
		failed    atomic.Int64
		bytesOut  atomic.Int64
		latencyMu = make(chan time.Duration, 200_000)
	)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case msg, ok := <-prod.Successes():
				if !ok {
					return
				}
				if t, _ := msg.Metadata.(time.Time); !t.IsZero() {
					select {
					case latencyMu <- time.Since(t):
					default:
					}
				}
				acked.Add(1)
			case e, ok := <-prod.Errors():
				if !ok {
					return
				}
				failed.Add(1)
				log.Printf("send err: %v", e.Err)
			}
		}
	}()

	// baseline mem
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	start := time.Now()
	deadline := start.Add(duration)

	for time.Now().Before(deadline) {
		body := payload{
			EventID: "00000000-0000-0000-0000-000000000000",
			Code:    code7(sent.Load()),
			IP:      "203.0.113.42",
			UA:      "Mozilla/5.0 (X11; Linux x86_64) Spike/1.0",
			TS:      time.Now().Unix(),
		}
		buf, _ := json.Marshal(&body)
		bytesOut.Add(int64(len(buf)))

		prod.Input() <- &sarama.ProducerMessage{
			Topic:    topic,
			Key:      sarama.StringEncoder(body.Code),
			Value:    sarama.ByteEncoder(buf),
			Metadata: time.Now(),
		}
		sent.Add(1)
	}

	stopFeed := time.Now()
	feedDur := stopFeed.Sub(start)

	// 等 in-flight ack（最多 drainMax）
	drainStart := time.Now()
	for time.Since(drainStart) < drainMax {
		if acked.Load()+failed.Load() >= sent.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// 收 latency
	close(latencyMu)
	lats := make([]time.Duration, 0, len(latencyMu))
	for d := range latencyMu {
		lats = append(lats, d)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	rps := float64(sent.Load()) / feedDur.Seconds()
	mbOut := float64(bytesOut.Load()) / 1024 / 1024
	heapDelta := int64(memAfter.HeapInuse) - int64(memBefore.HeapInuse)

	fmt.Println("=== spike-sarama ===")
	fmt.Printf("duration:    %s (feed) + %s (drain)\n", feedDur.Truncate(time.Millisecond), time.Since(drainStart).Truncate(time.Millisecond))
	fmt.Printf("sent:        %d\n", sent.Load())
	fmt.Printf("acked:       %d\n", acked.Load())
	fmt.Printf("failed:      %d\n", failed.Load())
	fmt.Printf("RPS (sent):  %.0f\n", rps)
	fmt.Printf("payload:     %.1f MB written\n", mbOut)
	if len(lats) > 0 {
		fmt.Printf("ack latency: p50=%s p90=%s p99=%s max=%s (n=%d)\n",
			lats[len(lats)*50/100],
			lats[len(lats)*90/100],
			lats[min(len(lats)*99/100, len(lats)-1)],
			lats[len(lats)-1],
			len(lats),
		)
	}
	fmt.Printf("heap delta:  %+d KB\n", heapDelta/1024)
	fmt.Printf("total alloc: %d KB / mallocs=%d\n",
		(memAfter.TotalAlloc-memBefore.TotalAlloc)/1024,
		memAfter.Mallocs-memBefore.Mallocs)

	if err := prod.Close(); err != nil {
		log.Printf("close: %v", err)
	}
	<-done
}

// code7 模拟 base62 7 位短码（key 用，影响 partition hash 分布）。
func code7(n int64) string {
	const alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var b [7]byte
	for i := range b {
		b[i] = alpha[(n+int64(i)*7)%62]
	}
	return string(b[:])
}
