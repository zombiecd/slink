package cache

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func addr(t *testing.T) string {
	t.Helper()
	a := os.Getenv("SLINK_REDIS_ADDR")
	if a == "" {
		a = "localhost:16379"
	}
	return a
}

func TestNewClient_EmptyAddr(t *testing.T) {
	_, err := NewClient(context.Background(), ClientConfig{})
	if err == nil {
		t.Fatal("expected error for empty Addr, got nil")
	}
}

func TestNewClient_Unreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := NewClient(ctx, ClientConfig{
		Addr:        "127.0.0.1:1",
		DialTimeout: 500 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected error connecting to dead port")
	}
}

func TestClient_GetSetDel(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; need running docker compose")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := NewClient(ctx, ClientConfig{Addr: addr(t)})
	if err != nil {
		t.Fatalf("NewClient: %v (Redis 起着吗？)", err)
	}
	defer c.Close()

	key := "slink:test:" + t.Name()
	defer c.Del(ctx, key)

	// miss
	if _, err := c.Get(ctx, key); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("Get on missing key: got %v, want ErrCacheMiss", err)
	}

	// set
	if err := c.Set(ctx, key, "hello", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// hit
	v, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after Set: %v", err)
	}
	if v != "hello" {
		t.Errorf("Get: got %q, want %q", v, "hello")
	}

	// del
	if err := c.Del(ctx, key); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if _, err := c.Get(ctx, key); !errors.Is(err, ErrCacheMiss) {
		t.Fatalf("Get after Del: got %v, want ErrCacheMiss", err)
	}
}

func TestClient_Ping(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; need running docker compose")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := NewClient(ctx, ClientConfig{Addr: addr(t)})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	if err := c.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestClient_DelEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test; need running docker compose")
	}
	ctx := context.Background()
	c, err := NewClient(ctx, ClientConfig{Addr: addr(t)})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// 空 key 列表应直接返回 nil，不打 Redis
	if err := c.Del(ctx); err != nil {
		t.Errorf("Del() with no keys: %v", err)
	}
}
