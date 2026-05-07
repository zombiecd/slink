package store

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/zombiecd/slink/internal/event"
)

func setupClickRepo(t *testing.T) (*ClickEventRepo, *pgxpool.Pool, func()) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test; need running docker compose")
	}
	dsn := os.Getenv("SLINK_PG_DSN")
	if dsn == "" {
		dsn = "postgres://slink:slink@localhost:15432/slink?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return NewClickEventRepo(pool), pool, func() { pool.Close() }
}

// 用一个 5 月分区的时间戳（migration 已建 click_events_2026_05）
func partitionedTS() time.Time {
	return time.Date(2026, 5, 7, 12, 0, 0, 0, time.UTC)
}

func makeUUID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// 工具：清理本测试期间插入的记录
func cleanupCode(t *testing.T, pool *pgxpool.Pool, code string) {
	t.Helper()
	_, _ = pool.Exec(context.Background(), "DELETE FROM click_events WHERE code = $1", code)
}

// ─────────────────────────────────────────────────────────
// 基础：BatchInsert 写入若干条 + 查回来核对
// ─────────────────────────────────────────────────────────

func TestClickEventRepo_BatchInsert_Basic(t *testing.T) {
	repo, pool, cleanup := setupClickRepo(t)
	defer cleanup()

	code := fmt.Sprintf("clk_%d", time.Now().UnixNano())
	defer cleanupCode(t, pool, code)

	ts := partitionedTS()
	events := []event.ClickEvent{
		{
			EventID:   makeUUID(t),
			Code:      code,
			IP:        net.ParseIP("203.0.113.10"),
			UserAgent: "TestUA/1.0",
			Referer:   "https://google.com",
			TS:        ts,
		},
		{
			EventID:   makeUUID(t),
			Code:      code,
			IP:        net.ParseIP("2001:db8::1"), // IPv6
			UserAgent: "TestUA/2.0",
			TS:        ts.Add(time.Second),
		},
		{
			// 全可选字段为空（只必填 EventID/Code/TS）
			EventID: makeUUID(t),
			Code:    code,
			TS:      ts.Add(2 * time.Second),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := repo.BatchInsert(ctx, events); err != nil {
		t.Fatalf("BatchInsert: %v", err)
	}

	// 查回来核对行数
	var count int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM click_events WHERE code = $1", code,
	).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != len(events) {
		t.Errorf("inserted count: got %d, want %d", count, len(events))
	}

	// 抽查 IPv6 字段没丢
	var ipText *string
	if err := pool.QueryRow(ctx,
		"SELECT host(ip) FROM click_events WHERE event_id = $1",
		events[1].EventID,
	).Scan(&ipText); err != nil {
		t.Fatalf("query ipv6: %v", err)
	}
	if ipText == nil || *ipText != "2001:db8::1" {
		t.Errorf("ipv6 mismatch: got %v, want 2001:db8::1", ipText)
	}
}

// ─────────────────────────────────────────────────────────
// 空批：直接返回 nil，不打 PG
// ─────────────────────────────────────────────────────────

func TestClickEventRepo_BatchInsert_Empty(t *testing.T) {
	repo, _, cleanup := setupClickRepo(t)
	defer cleanup()

	if err := repo.BatchInsert(context.Background(), nil); err != nil {
		t.Errorf("BatchInsert(nil): %v", err)
	}
	if err := repo.BatchInsert(context.Background(), []event.ClickEvent{}); err != nil {
		t.Errorf("BatchInsert([]): %v", err)
	}
}

// ─────────────────────────────────────────────────────────
// 性能 sanity：1000 条 < 200ms（开发环境 PG）
// ─────────────────────────────────────────────────────────

func TestClickEventRepo_BatchInsert_1000Rows(t *testing.T) {
	repo, pool, cleanup := setupClickRepo(t)
	defer cleanup()

	code := fmt.Sprintf("perf_%d", time.Now().UnixNano())
	defer cleanupCode(t, pool, code)

	const N = 1000
	events := make([]event.ClickEvent, N)
	ts := partitionedTS()
	for i := range events {
		events[i] = event.ClickEvent{
			EventID:   makeUUID(t),
			Code:      code,
			IP:        net.ParseIP("198.51.100.1"),
			UserAgent: "TestUA",
			TS:        ts.Add(time.Duration(i) * time.Microsecond),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	if err := repo.BatchInsert(ctx, events); err != nil {
		t.Fatalf("BatchInsert: %v", err)
	}
	elapsed := time.Since(start)

	t.Logf("1000 rows COPY FROM: %v", elapsed)
	if elapsed > 500*time.Millisecond {
		t.Errorf("1000 rows took %v (>500ms — too slow for v0.1 baseline)", elapsed)
	}
}
