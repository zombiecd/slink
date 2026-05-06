package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/zombiecd/slink/internal/model"
)

// 错误哨兵 ─────────────────────────────────────────────────
var (
	// ErrLinkNotFound 表示按 code 或 idempotency key 查不到记录。
	ErrLinkNotFound = errors.New("link not found")
	// ErrLinkCodeConflict 表示 code 字段已存在（unique 约束冲突）。
	// 理论上号段保证唯一，触发此错误说明上层逻辑有 bug 或外部 race。
	ErrLinkCodeConflict = errors.New("link code conflict")
	// ErrIdempotencyConflict 表示 idem_key 已存在。
	// 上层应捕获后调用 GetByIdempotencyKey 返回已有记录。
	ErrIdempotencyConflict = errors.New("idempotency key conflict")
)

// PG 唯一约束的名称（见 migrations/0001_init.up.sql）
const (
	uniqueCodeIndex = "links_pkey"        // ID 主键冲突（号段重复，不该发生）
	uniqueCodeName  = "links_code_key"    // code 唯一索引
	uniqueIdemName  = "links_idem_unique" // idem_key 唯一约束
)

// PG 错误码：23505 = unique_violation
const pgUniqueViolation = "23505"

// LinkRepo 是 links 表的数据访问层。
type LinkRepo struct {
	pool *pgxpool.Pool
}

func NewLinkRepo(pool *pgxpool.Pool) *LinkRepo {
	return &LinkRepo{pool: pool}
}

// Insert 写入一条短链记录。
//
// 错误映射：
//   - idem_key 冲突   → ErrIdempotencyConflict
//   - code 冲突       → ErrLinkCodeConflict
//   - 其他            → fmt.Errorf 包装原错误
func (r *LinkRepo) Insert(ctx context.Context, l *model.Link) error {
	const sql = `
		INSERT INTO links (id, code, long_url, expires_at, creator, idem_key)
		VALUES ($1, $2, $3, $4, $5, $6);`

	_, err := r.pool.Exec(ctx, sql,
		l.ID,
		l.Code,
		l.LongURL,
		l.ExpiresAt,
		nullableString(l.Creator),
		l.IdempotencyKey,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			switch pgErr.ConstraintName {
			case uniqueIdemName:
				return ErrIdempotencyConflict
			case uniqueCodeName, uniqueCodeIndex:
				return ErrLinkCodeConflict
			}
		}
		return fmt.Errorf("insert link: %w", err)
	}
	return nil
}

// GetByCode 按短码查记录。
// 不存在返回 ErrLinkNotFound。
func (r *LinkRepo) GetByCode(ctx context.Context, code string) (*model.Link, error) {
	const sql = `
		SELECT id, code, long_url, created_at, expires_at, COALESCE(creator, ''), idem_key
		FROM links
		WHERE code = $1;`

	return r.scanOne(ctx, sql, code)
}

// GetByIdempotencyKey 按 idempotency key 查记录。
// 不存在返回 ErrLinkNotFound。
func (r *LinkRepo) GetByIdempotencyKey(ctx context.Context, key string) (*model.Link, error) {
	if key == "" {
		return nil, ErrLinkNotFound
	}
	const sql = `
		SELECT id, code, long_url, created_at, expires_at, COALESCE(creator, ''), idem_key
		FROM links
		WHERE idem_key = $1;`

	return r.scanOne(ctx, sql, key)
}

func (r *LinkRepo) scanOne(ctx context.Context, sql string, args ...any) (*model.Link, error) {
	var l model.Link
	err := r.pool.QueryRow(ctx, sql, args...).Scan(
		&l.ID,
		&l.Code,
		&l.LongURL,
		&l.CreatedAt,
		&l.ExpiresAt,
		&l.Creator,
		&l.IdempotencyKey,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrLinkNotFound
		}
		return nil, fmt.Errorf("scan link: %w", err)
	}
	return &l, nil
}

// nullableString 把空字符串转成 nil 指针，让 SQL 写入 NULL 而非 ''。
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
