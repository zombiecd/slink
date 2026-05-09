// spike-clickhouse-v2 是 v0.5 ClickHouse 客户端库选型 spike（ClickHouse/clickhouse-go v2）。
//
// 与 cmd/spike-ch-go 严格同口径：
//   - 内存固定生成 N 条 ClickEvent（同 fixture），同 batch=1000
//   - Native protocol 写入 click_events_ch_spike 表（spike 专表，不污染主路径 click_events_ch）
//   - 跑满 30s 或写完 fixture（取先到者）
//   - 输出：rows/s / 总条数 / 平均 batch 延迟 / heap alloc 增量
//
// 不是生产代码 — Day 18 选型完成后会被删/归档（同 spike-kgo / spike-sarama 处理）。
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"runtime"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

const (
	duration  = 30 * time.Second
	batchSize = 1000
	totalRows = 5_000_000 // fixture 上限，30s 内打到天花板就停
)

type fixture struct {
	eventID   uuid.UUID
	code      string
	ip        string
	userAgent string
	referer   string
	country   string
	region    string
	ts        time.Time
}

func main() {
	addr := flag.String("addr", "localhost:19000", "clickhouse Native addr (host:port)")
	user := flag.String("user", "slink", "clickhouse user")
	pass := flag.String("pass", "slink", "clickhouse password")
	db := flag.String("db", "slink_analytics", "database")
	flag.Parse()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{*addr},
		Auth: clickhouse.Auth{
			Database: *db,
			Username: *user,
			Password: *pass,
		},
		// 默认 Native；保持 spike 同口径明示
		Protocol:    clickhouse.Native,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()
	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("ping: %v", err)
	}

	// 启动前 truncate spike 专表，避免上次运行残留干扰本次数字
	if err := setupSpikeTable(ctx, conn); err != nil {
		log.Fatalf("setup spike table: %v", err)
	}

	codes := genCodePool(2000)
	ips := genIPPool(500)

	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	start := time.Now()
	deadline := start.Add(duration)
	var (
		rowsWritten int64
		batches     int64
		batchLats   []time.Duration
	)

	for rowsWritten < totalRows && time.Now().Before(deadline) {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+*db+".click_events_ch_spike")
		if err != nil {
			log.Fatalf("prepare batch: %v", err)
		}

		bs := batchSize
		if remain := int(totalRows - rowsWritten); remain < bs {
			bs = remain
		}

		now := time.Now()
		for i := 0; i < bs; i++ {
			f := nextFixture(now, rowsWritten+int64(i), codes, ips)
			if err := batch.Append(
				f.eventID,
				f.code,
				f.ip,
				f.userAgent,
				f.referer,
				f.country,
				f.region,
				f.ts,
			); err != nil {
				log.Fatalf("append row %d: %v", rowsWritten+int64(i), err)
			}
		}

		batchStart := time.Now()
		if err := batch.Send(); err != nil {
			log.Fatalf("send batch %d: %v", batches, err)
		}
		batchLats = append(batchLats, time.Since(batchStart))

		rowsWritten += int64(bs)
		batches++
	}

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	report(rowsWritten, batches, batchLats, start, memBefore, memAfter)
}

func setupSpikeTable(ctx context.Context, conn driver.Conn) error {
	stmts := []string{
		`DROP TABLE IF EXISTS slink_analytics.click_events_ch_spike`,
		`CREATE TABLE slink_analytics.click_events_ch_spike (
            event_id UUID,
            code String,
            ip String,
            user_agent String,
            referer String,
            country LowCardinality(String),
            region LowCardinality(String),
            ts DateTime64(3, 'UTC')
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(ts)
        ORDER BY (code, ts)`,
	}
	for _, s := range stmts {
		if err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("exec %q: %w", s[:40], err)
		}
	}
	return nil
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

func nextFixture(base time.Time, idx int64, codes, ips []string) fixture {
	return fixture{
		eventID:   uuid.New(),
		code:      codes[idx%int64(len(codes))],
		ip:        ips[idx%int64(len(ips))],
		userAgent: "Mozilla/5.0 (spike) Gecko/20100101",
		referer:   "https://x.test/spike",
		country:   countryRotation(idx),
		region:    "SH",
		ts:        base.Add(time.Duration(idx) * time.Microsecond),
	}
}

func countryRotation(idx int64) string {
	pool := []string{"CN", "US", "JP", "DE", "GB", "FR", "IN", "BR"}
	return pool[idx%int64(len(pool))]
}

func report(rows, batches int64, lats []time.Duration, start time.Time, before, after runtime.MemStats) {
	elapsed := time.Since(start)
	rps := float64(rows) / elapsed.Seconds()
	allocDelta := after.TotalAlloc - before.TotalAlloc

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	p50 := lats[len(lats)/2]
	p99 := lats[len(lats)*99/100]

	fmt.Printf("=== spike-clickhouse-v2 (clickhouse-go/v2 Native) ===\n")
	fmt.Printf("  duration       %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  rows           %d\n", rows)
	fmt.Printf("  batches        %d (size %d)\n", batches, batchSize)
	fmt.Printf("  rows/s         %.0f\n", rps)
	fmt.Printf("  batch p50      %s\n", p50.Round(time.Microsecond))
	fmt.Printf("  batch p99      %s\n", p99.Round(time.Microsecond))
	fmt.Printf("  alloc/op       %.0f B/row\n", float64(allocDelta)/float64(rows))
	fmt.Printf("  total alloc    %.1f MB\n", float64(allocDelta)/1024/1024)
}
