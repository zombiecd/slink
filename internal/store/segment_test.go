package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// 集成测试：需要 docker compose up + migrate 已跑过。
// migrations/0001_init.up.sql 会插入 ('link', 0, 1000) 种子。

func setupRepo(t *testing.T) (*SegmentRepo, func()) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test; need running docker compose")
	}

	dsnStr := os.Getenv("SLINK_PG_DSN")
	if dsnStr == "" {
		dsnStr = "postgres://slink:slink@localhost:15432/slink?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsnStr)
	if err != nil {
		t.Fatalf("connect: %v (PG 起着吗？)", err)
	}

	return NewSegmentRepo(pool), func() { pool.Close() }
}

func TestSegmentRepo_Acquire(t *testing.T) {
	repo, cleanup := setupRepo(t)
	defer cleanup()
	ctx := context.Background()

	first, err := repo.Acquire(ctx, "link", 100)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if first <= 0 {
		t.Errorf("first acquire: expected > 0, got %d", first)
	}

	second, err := repo.Acquire(ctx, "link", 100)
	if err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	// 第二次取段应在第一次之上 +100
	if got, want := second-first, int64(100); got != want {
		t.Errorf("delta: got %d, want %d", got, want)
	}
}

func TestSegmentRepo_AcquireNotFound(t *testing.T) {
	repo, cleanup := setupRepo(t)
	defer cleanup()
	ctx := context.Background()

	_, err := repo.Acquire(ctx, "this-biz-tag-does-not-exist-"+t.Name(), 100)
	if !errors.Is(err, ErrSegmentNotFound) {
		t.Errorf("expected ErrSegmentNotFound, got %v", err)
	}
}

func TestSegmentRepo_AcquireValidation(t *testing.T) {
	repo, cleanup := setupRepo(t)
	defer cleanup()
	ctx := context.Background()

	cases := []struct {
		name    string
		bizTag  string
		step    int64
		wantSub string
	}{
		{"empty bizTag", "", 100, "bizTag"},
		{"step zero", "link", 0, "stepSize"},
		{"step negative", "link", -1, "stepSize"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := repo.Acquire(ctx, c.bizTag, c.step)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestSegmentRepo_Peek(t *testing.T) {
	repo, cleanup := setupRepo(t)
	defer cleanup()
	ctx := context.Background()

	// Peek 应返回当前 max_id（不修改）
	v1, err := repo.Peek(ctx, "link")
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	v2, err := repo.Peek(ctx, "link")
	if err != nil {
		t.Fatalf("Peek again: %v", err)
	}
	if v1 != v2 {
		t.Errorf("Peek mutated state: %d → %d", v1, v2)
	}
}
