// Package store - click_events_ch.go
//
// ClickEventCHRepo 是 v0.5 分析查询的 ClickHouse 数据源。
//
// 数据流：Kafka Engine + MV 自动写入 → click_events_ch（MergeTree 主表）
// 本 repo 只读，不写。写入路径见 docs/architecture/v0.5-clickhouse.md §4 决策。
//
// 客户端选 ch-go（决策稿 §4 D2）：alloc 28 B/row 列容器复用 + Native protocol
// 无 SQL 序列化层。查询路径同样受益（列式解码）。
//
// 故障域：CH 挂了不影响 PG 路径（v0.4 立的"主路径不为下游退步"原则）。
// /api/stats/* 接口在 CH 故障期直接 503，server 主链路 + PG 落库不受影响。
package store

import (
	"context"
	"fmt"
	"time"

	ch "github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// ClickEventCHRepo 读 ClickHouse click_events_ch 主表做实时分析。
type ClickEventCHRepo struct {
	cli   *ch.Client
	table string // 主表名（默认 click_events_ch，与 v0.5 决策稿 §4 D4 一致）
}

// CHConfig 是 ClickHouse 客户端配置。
type CHConfig struct {
	Addr        string        // host:port (Native protocol，默认 9000)
	User        string        // 用户名
	Password    string        // 密码
	Database    string        // 数据库
	Table       string        // 主表名，默认 click_events_ch
	DialTimeout time.Duration // dial 上限，默认 3s
}

func (c *CHConfig) withDefaults() {
	if c.Table == "" {
		c.Table = "click_events_ch"
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 3 * time.Second
	}
}

// NewClickEventCHRepo dial ClickHouse 返回 repo。
//
// dial 失败返回 error，调用方决定是否回退（v0.5 设计：CH 挂了 server 仍可启动，
// stats endpoint 503 即可，主链路不受影响）。
func NewClickEventCHRepo(ctx context.Context, cfg CHConfig) (*ClickEventCHRepo, error) {
	cfg.withDefaults()

	dialCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	defer cancel()

	cli, err := ch.Dial(dialCtx, ch.Options{
		Address:  cfg.Addr,
		Database: cfg.Database,
		User:     cfg.User,
		Password: cfg.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("ch dial %s: %w", cfg.Addr, err)
	}

	return &ClickEventCHRepo{cli: cli, table: cfg.Table}, nil
}

// Close 关 client 释放连接。
func (r *ClickEventCHRepo) Close() error {
	return r.cli.Close()
}

// formatTime 把 time.Time 序列化为 ClickHouse DateTime64(3,'UTC') 可解析的字符串。
//
// 使用 SQL 单引号包裹格式 'YYYY-MM-DD HH:MM:SS.fff'，与 click_events_ch.ts 字段
// DateTime64(3,'UTC') 完全对应。调用方需保证 t 已转 UTC（handler 入口已统一）。
func formatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05.000")
}

// UV 返回时间窗 [from, to) 内某 code 的近似 unique IP 数（uniqHLL12）。
//
// 算法选 uniqHLL12（v0.5 决策稿 §7）：0.81% 标准误差换 ~4KB / 维度内存。
// SQL injection 防御：code 已在 handler 入口 ValidateCode 校验（base62 + 长度限制），
// from/to 是 time.Time 序列化为固定格式字符串，无注入面。
func (r *ClickEventCHRepo) UV(ctx context.Context, code string, from, to time.Time) (uint64, error) {
	var col proto.ColUInt64
	q := fmt.Sprintf(
		`SELECT uniqHLL12(ip) AS uv FROM %s WHERE code = '%s' AND ts >= '%s' AND ts < '%s'`,
		r.table, code, formatTime(from), formatTime(to),
	)
	if err := r.cli.Do(ctx, ch.Query{
		Body: q,
		Result: proto.Results{
			{Name: "uv", Data: &col},
		},
	}); err != nil {
		return 0, fmt.Errorf("ch uv: %w", err)
	}
	if col.Rows() == 0 {
		return 0, nil
	}
	return col.Row(0), nil
}

// TopKEntry 是 TopK 查询单行结果。
type TopKEntry struct {
	Code  string `json:"code"`
	Count uint64 `json:"count"`
}

// TopK 返回时间窗 [from, to) 内点击数 top n 的 code 列表，倒序。
//
// n 上限 100（防止 OOM；handler 入口已校验）。CH 端 ORDER BY count() DESC LIMIT n
// 单表扫描走 (code, ts) 主索引可加速。
func (r *ClickEventCHRepo) TopK(ctx context.Context, from, to time.Time, n int) ([]TopKEntry, error) {
	var (
		colCode  proto.ColStr
		colCount proto.ColUInt64
	)
	q := fmt.Sprintf(
		`SELECT code, count() AS c FROM %s WHERE ts >= '%s' AND ts < '%s' GROUP BY code ORDER BY c DESC LIMIT %d`,
		r.table, formatTime(from), formatTime(to), n,
	)
	if err := r.cli.Do(ctx, ch.Query{
		Body: q,
		Result: proto.Results{
			{Name: "code", Data: &colCode},
			{Name: "c", Data: &colCount},
		},
	}); err != nil {
		return nil, fmt.Errorf("ch topk: %w", err)
	}
	rows := colCode.Rows()
	out := make([]TopKEntry, rows)
	for i := 0; i < rows; i++ {
		out[i] = TopKEntry{
			Code:  colCode.Row(i),
			Count: colCount.Row(i),
		}
	}
	return out, nil
}

// TimeseriesBucket 是时序聚合单桶。
type TimeseriesBucket struct {
	Bucket time.Time `json:"bucket"` // 桶起始时间
	Count  uint64    `json:"count"`  // 桶内事件数
}

// Timeseries 返回时间窗 [from, to) 内按 bucket 大小切分的点击数序列。
//
// bucketSec 必须 ≥ 1 且 ≤ 86400（1 day），由 handler 入口校验。
// 用 ClickHouse toStartOfInterval 函数对 ts 做桶聚合。
func (r *ClickEventCHRepo) Timeseries(ctx context.Context, from, to time.Time, bucketSec int) ([]TimeseriesBucket, error) {
	var (
		colBucket proto.ColDateTime
		colCount  proto.ColUInt64
	)

	// toStartOfInterval 默认返回 DateTime（秒精度），不是 DateTime64。
	// 用 proto.ColDateTime 解码，避免列类型不匹配。
	q := fmt.Sprintf(
		`SELECT toStartOfInterval(ts, INTERVAL %d SECOND) AS bucket, count() AS c
		 FROM %s WHERE ts >= '%s' AND ts < '%s'
		 GROUP BY bucket ORDER BY bucket`,
		bucketSec, r.table, formatTime(from), formatTime(to),
	)
	if err := r.cli.Do(ctx, ch.Query{
		Body: q,
		Result: proto.Results{
			{Name: "bucket", Data: &colBucket},
			{Name: "c", Data: &colCount},
		},
	}); err != nil {
		return nil, fmt.Errorf("ch timeseries: %w", err)
	}
	rows := colBucket.Rows()
	out := make([]TimeseriesBucket, rows)
	for i := 0; i < rows; i++ {
		out[i] = TimeseriesBucket{
			Bucket: colBucket.Row(i),
			Count:  colCount.Row(i),
		}
	}
	return out, nil
}
