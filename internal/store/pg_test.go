package store

import (
	"context"
	"os"
	"testing"
	"time"
)

// 集成测试：需要 docker compose up 起着 PG。
// 否则跳过（CI 上会用 testcontainers，v0.1 暂用本地 docker）。
func dsn(t *testing.T) string {
	t.Helper()
	d := os.Getenv("SLINK_PG_DSN")
	if d == "" {
		d = "postgres://slink:slink@localhost:15432/slink?sslmode=disable"
	}
	return d
}

func TestNewPool_EmptyDSN(t *testing.T) {
	_, err := NewPool(context.Background(), PoolConfig{})
	if err == nil {
		t.Fatal("expected error for empty DSN, got nil")
	}
}

func TestNewPool_BadDSN(t *testing.T) {
	_, err := NewPool(context.Background(), PoolConfig{
		DSN:      "not-a-valid-dsn",
		MaxConns: 5,
		MinConns: 1,
	})
	if err == nil {
		t.Fatal("expected error for malformed DSN, got nil")
	}
}

func TestNewPool_PingOK(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; need running docker compose")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := NewPool(ctx, PoolConfig{
		DSN:      dsn(t),
		MaxConns: 5,
		MinConns: 1,
	})
	if err != nil {
		t.Fatalf("NewPool: %v (PG 起着吗？docker compose ps)", err)
	}
	defer pool.Close()

	if err := Ping(ctx, pool); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	stat := pool.Stat()
	if stat.MaxConns() != 5 {
		t.Errorf("MaxConns: got %d, want 5", stat.MaxConns())
	}
}

func TestNewPool_UnreachableServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := NewPool(ctx, PoolConfig{
		DSN:            "postgres://slink:slink@127.0.0.1:1/slink?sslmode=disable",
		MaxConns:       5,
		MinConns:       1,
		ConnectTimeout: 500 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected error connecting to dead port, got nil")
	}
}
