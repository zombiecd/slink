package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/zombiecd/slink/internal/model"
)

func setupLinkRepo(t *testing.T) (*LinkRepo, func()) {
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
		t.Fatalf("connect: %v", err)
	}
	return NewLinkRepo(pool), func() { pool.Close() }
}

// 测试隔离：每个用例用独立 ID 段，结束后清理。
func uniqueID(t *testing.T) int64 {
	t.Helper()
	// 用 time.Now().UnixNano() 后缀避免重跑冲突
	return time.Now().UnixNano()
}

func TestLinkRepo_InsertAndGetByCode(t *testing.T) {
	repo, cleanup := setupLinkRepo(t)
	defer cleanup()
	ctx := context.Background()

	id := uniqueID(t)
	link := &model.Link{
		ID:      id,
		Code:    fmtCode("c", id),
		LongURL: "https://example.com/test/" + t.Name(),
	}
	defer cleanupByID(ctx, repo, id)

	if err := repo.Insert(ctx, link); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, err := repo.GetByCode(ctx, link.Code)
	if err != nil {
		t.Fatalf("GetByCode: %v", err)
	}
	if got.LongURL != link.LongURL {
		t.Errorf("LongURL: got %q, want %q", got.LongURL, link.LongURL)
	}
	if got.ID != link.ID {
		t.Errorf("ID: got %d, want %d", got.ID, link.ID)
	}
	if got.CreatedAt.IsZero() {
		t.Errorf("CreatedAt is zero")
	}
}

func TestLinkRepo_GetByCode_NotFound(t *testing.T) {
	repo, cleanup := setupLinkRepo(t)
	defer cleanup()
	ctx := context.Background()

	_, err := repo.GetByCode(ctx, "doesnotexist-"+t.Name())
	if !errors.Is(err, ErrLinkNotFound) {
		t.Errorf("expected ErrLinkNotFound, got %v", err)
	}
}

func TestLinkRepo_IdempotencyConflict(t *testing.T) {
	repo, cleanup := setupLinkRepo(t)
	defer cleanup()
	ctx := context.Background()

	id1 := uniqueID(t)
	idemKey := "test-idem-" + t.Name()
	link1 := &model.Link{
		ID:             id1,
		Code:           fmtCode("a", id1),
		LongURL:        "https://example.com/a",
		IdempotencyKey: &idemKey,
	}
	defer cleanupByID(ctx, repo, id1)

	if err := repo.Insert(ctx, link1); err != nil {
		t.Fatalf("first Insert: %v", err)
	}

	// 第二个请求带相同 idem key 但不同 ID/code → 冲突
	id2 := uniqueID(t) + 1
	link2 := &model.Link{
		ID:             id2,
		Code:           fmtCode("b", id2),
		LongURL:        "https://example.com/b",
		IdempotencyKey: &idemKey,
	}
	defer cleanupByID(ctx, repo, id2)

	err := repo.Insert(ctx, link2)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}
}

func TestLinkRepo_GetByIdempotencyKey(t *testing.T) {
	repo, cleanup := setupLinkRepo(t)
	defer cleanup()
	ctx := context.Background()

	id := uniqueID(t)
	idemKey := "lookup-" + t.Name()
	link := &model.Link{
		ID:             id,
		Code:           fmtCode("k", id),
		LongURL:        "https://example.com/by-idem",
		IdempotencyKey: &idemKey,
	}
	defer cleanupByID(ctx, repo, id)

	if err := repo.Insert(ctx, link); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	got, err := repo.GetByIdempotencyKey(ctx, idemKey)
	if err != nil {
		t.Fatalf("GetByIdempotencyKey: %v", err)
	}
	if got.Code != link.Code {
		t.Errorf("Code: got %q, want %q", got.Code, link.Code)
	}
}

func TestLinkRepo_GetByIdempotencyKey_Empty(t *testing.T) {
	repo, cleanup := setupLinkRepo(t)
	defer cleanup()

	_, err := repo.GetByIdempotencyKey(context.Background(), "")
	if !errors.Is(err, ErrLinkNotFound) {
		t.Errorf("empty key should return ErrLinkNotFound, got %v", err)
	}
}

// ────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────

func fmtCode(prefix string, id int64) string {
	// 测试期生成不撞的"code"，长度 6 位（满足 schema 没有 length check）
	const charset = "0123456789abcdefghijklmnopqrstuvwxyz"
	s := prefix
	rem := id
	for len(s) < 6 {
		s += string(charset[rem%int64(len(charset))])
		rem /= int64(len(charset))
	}
	return s[:6]
}

func cleanupByID(ctx context.Context, repo *LinkRepo, id int64) {
	_, _ = repo.pool.Exec(ctx, "DELETE FROM links WHERE id = $1", id)
}
