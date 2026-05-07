package store

import (
	"context"
	"fmt"
	"net/netip"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/zombiecd/slink/internal/event"
)

// ClickEventRepo 是点击事件分区表 click_events 的写入器。
//
// 仅暴露 BatchInsert：单条 INSERT 在 v0.1 高 QPS 跳转下会成为瓶颈
// （n 次 RTT、n 次 commit）。1000 条/次的 COPY FROM 比 1000 次 INSERT 快 10x+。
type ClickEventRepo struct {
	pool *pgxpool.Pool
}

func NewClickEventRepo(pool *pgxpool.Pool) *ClickEventRepo {
	return &ClickEventRepo{pool: pool}
}

// BatchInsert 用 PG COPY FROM 批量写入点击事件。
//
// 性能：COPY FROM 走二进制协议，绕过 SQL parser，比 INSERT 快得多。
// 1000 行单批在本机 PG 上典型耗时 < 5ms。
//
// 参数 ip 字段处理：
//
//	net.IP 不是 nil → pgx inet 编码
//	net.IP 是  nil → 写 NULL（PG 列允许 NULL）
//
// 错误处理：
//
//	任何一行失败整批回滚（COPY 是 transactional）。
//	上层应该把这一批重新入队列，不能丢——但要小心放大风险（DB 真挂时无限重试）。
//	v0.1 简化：失败就丢，记日志。v0.2 加重试 + dead-letter。
func (r *ClickEventRepo) BatchInsert(ctx context.Context, events []event.ClickEvent) error {
	if len(events) == 0 {
		return nil
	}

	rows := make([][]any, len(events))
	for i, e := range events {
		var ipVal any
		if len(e.IP) > 0 {
			// pgx inet codec 接受 netip.Addr 是最干净的
			if a, ok := netip.AddrFromSlice(e.IP); ok {
				ipVal = a
			}
		}
		rows[i] = []any{
			e.EventID, // UUID 字符串
			e.Code,
			ipVal,
			nullableString(e.UserAgent),
			nullableString(e.Referer),
			nullableString(e.Country),
			nullableString(e.Region),
			e.TS.UTC(),
		}
	}

	n, err := r.pool.CopyFrom(
		ctx,
		pgx.Identifier{"click_events"},
		[]string{"event_id", "code", "ip", "user_agent", "referer", "country", "region", "ts"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("copy click_events (%d rows): %w", len(events), err)
	}
	if int(n) != len(events) {
		// COPY FROM 应当全成功或全失败；这条分支理论不该走到
		return fmt.Errorf("copy click_events partial: wrote %d of %d", n, len(events))
	}
	return nil
}
