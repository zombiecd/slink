package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrSegmentNotFound 表示 id_segment 表里查不到对应 biz_tag 的记录。
// 上层应感知这个错误，决定要不要主动 INSERT 一条种子记录。
var ErrSegmentNotFound = errors.New("segment biz_tag not found")

// SegmentRepo 提供号段表的原子取段操作。
//
// 数据流：
//
//	UPDATE id_segment
//	SET max_id = max_id + $stepSize, updated_at = now()
//	WHERE biz_tag = $bizTag
//	RETURNING max_id;
//
// PG 把 UPDATE ... RETURNING 当作单条原子语句，行级 X 锁保证多实例并发安全。
type SegmentRepo struct {
	pool *pgxpool.Pool
}

// NewSegmentRepo 用现有连接池构造 repo。
// 不持有所有权，关闭由调用方负责（main.go 里 defer pool.Close）。
func NewSegmentRepo(pool *pgxpool.Pool) *SegmentRepo {
	return &SegmentRepo{pool: pool}
}

// Acquire 取下一个号段。
//
// 返回新的 max_id（即号段右端点，闭区间）。
// 调用方按 [max_id - stepSize + 1, max_id] 自增分配。
//
// stepSize 是本次取段的大小，可以与 DB 里 step_size 字段不同——
// 后者是该 biz_tag 的"建议大小"，实际取段大小由调用方决定。
//
// biz_tag 不存在时返回 ErrSegmentNotFound。
func (r *SegmentRepo) Acquire(ctx context.Context, bizTag string, stepSize int64) (int64, error) {
	if bizTag == "" {
		return 0, fmt.Errorf("segment.Acquire: bizTag is empty")
	}
	if stepSize <= 0 {
		return 0, fmt.Errorf("segment.Acquire: stepSize must be > 0, got %d", stepSize)
	}

	const sql = `
		UPDATE id_segment
		SET max_id     = max_id + $2,
		    updated_at = now()
		WHERE biz_tag = $1
		RETURNING max_id;`

	var newMax int64
	err := r.pool.QueryRow(ctx, sql, bizTag, stepSize).Scan(&newMax)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("%w: %s", ErrSegmentNotFound, bizTag)
		}
		return 0, fmt.Errorf("acquire segment %q: %w", bizTag, err)
	}
	return newMax, nil
}

// Peek 不修改地查询当前 max_id（运维 / 监控用途，正常取段不调）。
func (r *SegmentRepo) Peek(ctx context.Context, bizTag string) (int64, error) {
	const sql = `SELECT max_id FROM id_segment WHERE biz_tag = $1`

	var maxID int64
	err := r.pool.QueryRow(ctx, sql, bizTag).Scan(&maxID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("%w: %s", ErrSegmentNotFound, bizTag)
		}
		return 0, fmt.Errorf("peek segment %q: %w", bizTag, err)
	}
	return maxID, nil
}
