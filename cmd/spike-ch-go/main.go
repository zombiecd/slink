// spike-ch-go 是 v0.5 ClickHouse 客户端库选型 spike（ClickHouse/ch-go, low-level Native）。
//
// 与 cmd/spike-clickhouse-v2 严格同口径：
//   - 同 fixture（5M 上限 / 30s 时间窗 / batch=1000）
//   - 写 click_events_ch_spike 表（spike 专表，启动前 truncate）
//   - 输出：rows/s / 总条数 / 平均 batch 延迟 / heap alloc 增量
//
// 与 v2 的区别：ch-go 直接走二进制 Native protocol，列式编码，理论上：
//   - 无 SQL 序列化层（v2 走 PrepareBatch → SQL）
//   - 列式 buffer 一次性 marshal，CPU 缓存友好
//   - 但 API 更底层：手动管理 ColStr / ColUUID / ColDateTime64 等列容器
//
// 不是生产代码 — Day 18 选型完成后会被删/归档。
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

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"
)

const (
	duration  = 30 * time.Second
	batchSize = 1000
	totalRows = 5_000_000
)

func main() {
	addr := flag.String("addr", "localhost:19000", "clickhouse Native addr (host:port)")
	user := flag.String("user", "slink", "clickhouse user")
	pass := flag.String("pass", "slink", "clickhouse password")
	db := flag.String("db", "slink_analytics", "database")
	flag.Parse()

	ctx := context.Background()
	cli, err := ch.Dial(ctx, ch.Options{
		Address:     *addr,
		Database:    *db,
		User:        *user,
		Password:    *pass,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	if err := setupSpikeTable(ctx, cli, *db); err != nil {
		log.Fatalf("setup spike table: %v", err)
	}

	codes := genCodePool(2000)
	ips := genIPPool(500)

	var memBefore runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	deadline := time.Now().Add(duration)
	start := time.Now()

	var (
		rowsWritten int64
		batches     int64
		batchLats   []time.Duration
	)

	// 列容器复用：每批 Reset 后重新 Append，省 alloc
	colEventID := new(proto.ColUUID)
	colCode := new(proto.ColStr)
	colIP := new(proto.ColStr)
	colUA := new(proto.ColStr)
	colReferer := new(proto.ColStr)
	colCountry := proto.NewLowCardinality[string](new(proto.ColStr))
	colRegion := proto.NewLowCardinality[string](new(proto.ColStr))
	colTS := &proto.ColDateTime64{Precision: proto.PrecisionMilli, PrecisionSet: true}

	input := proto.Input{
		{Name: "event_id", Data: colEventID},
		{Name: "code", Data: colCode},
		{Name: "ip", Data: colIP},
		{Name: "user_agent", Data: colUA},
		{Name: "referer", Data: colReferer},
		{Name: "country", Data: colCountry},
		{Name: "region", Data: colRegion},
		{Name: "ts", Data: colTS},
	}

	for rowsWritten < totalRows && time.Now().Before(deadline) {
		// Reset 列容器，准备本 batch
		colEventID.Reset()
		colCode.Reset()
		colIP.Reset()
		colUA.Reset()
		colReferer.Reset()
		colCountry.Reset()
		colRegion.Reset()
		colTS.Reset()
		colTS.Precision = proto.PrecisionMilli
		colTS.PrecisionSet = true

		bs := batchSize
		if remain := int(totalRows - rowsWritten); remain < bs {
			bs = remain
		}

		now := time.Now()
		for i := 0; i < bs; i++ {
			idx := rowsWritten + int64(i)
			colEventID.Append(uuid.New())
			colCode.Append(codes[idx%int64(len(codes))])
			colIP.Append(ips[idx%int64(len(ips))])
			colUA.Append("Mozilla/5.0 (spike) Gecko/20100101")
			colReferer.Append("https://x.test/spike")
			colCountry.Append(countryRotation(idx))
			colRegion.Append("SH")
			colTS.Append(now.Add(time.Duration(idx) * time.Microsecond))
		}

		batchStart := time.Now()
		// Into 生成 "INSERT INTO <table> (cols...) VALUES"；不要带 db 前缀，
		// ch.Options.Database 已经把它当默认 db。
		err := cli.Do(ctx, ch.Query{
			Body:  input.Into("click_events_ch_spike"),
			Input: input,
		})
		if err != nil {
			log.Fatalf("insert batch %d: %v", batches, err)
		}
		batchLats = append(batchLats, time.Since(batchStart))

		rowsWritten += int64(bs)
		batches++
	}

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	report(rowsWritten, batches, batchLats, start, memBefore, memAfter)
}

func setupSpikeTable(ctx context.Context, cli *ch.Client, db string) error {
	stmts := []string{
		`DROP TABLE IF EXISTS ` + db + `.click_events_ch_spike`,
		`CREATE TABLE ` + db + `.click_events_ch_spike (
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
		if err := cli.Do(ctx, ch.Query{Body: s}); err != nil {
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

	fmt.Printf("=== spike-ch-go (ch-go Native low-level) ===\n")
	fmt.Printf("  duration       %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  rows           %d\n", rows)
	fmt.Printf("  batches        %d (size %d)\n", batches, batchSize)
	fmt.Printf("  rows/s         %.0f\n", rps)
	fmt.Printf("  batch p50      %s\n", p50.Round(time.Microsecond))
	fmt.Printf("  batch p99      %s\n", p99.Round(time.Microsecond))
	fmt.Printf("  alloc/op       %.0f B/row\n", float64(allocDelta)/float64(rows))
	fmt.Printf("  total alloc    %.1f MB\n", float64(allocDelta)/1024/1024)
}
