// Package api stats.go — v0.5 分析查询 HTTP handler。
//
// 端点：
//
//	GET /api/stats/uv?code=X&from=Y&to=Z          近似 UV (uniqHLL12)
//	GET /api/stats/topk?from=Y&to=Z&n=10          时间窗内 top N 热门 code
//	GET /api/stats/timeseries?from=Y&to=Z&bucket=60  按桶聚合点击数序列
//
// 时间参数：from/to 是 RFC3339 字符串（如 "2026-05-11T03:00:00Z"），from < to，
// 时间窗最大 31 天（防大查询）。
//
// 故障域：CH 故障时 statsRepo 返回 error，handler 503，server 主链路 + PG 不受影响
// （v0.4 立的"主路径不为下游退步"原则在 v0.5 继续）。
//
// SQL 注入防御：code 走 ValidateCode 严格校验（base62 + 长度），数值参数走 strconv。
// 时间参数序列化为 "YYYY-MM-DD HH:MM:SS.fff" 固定格式，无注入面。
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/zombiecd/slink/internal/store"
)

// 时间窗 + topK n + bucket 上限，超出即 400。
const (
	statsMaxWindowDays = 31         // from/to 跨度上限 31 天
	statsMaxTopN       = 100        // /topk n 上限 100（防 CH OOM）
	statsMaxBucketSec  = 86400      // 桶大小上限 1 天
	statsMinBucketSec  = 1          // 桶大小下限 1 秒
	statsQueryTimeout  = 3 * time.Second  // 单查询超时（CH P99 < 200ms 目标 + 余量）
)

// statsRepo 是 stats handler 依赖的最小 ClickHouse 接口（便于测试 mock）。
type statsRepo interface {
	UV(ctx context.Context, code string, from, to time.Time) (uint64, error)
	TopK(ctx context.Context, from, to time.Time, n int) ([]store.TopKEntry, error)
	Timeseries(ctx context.Context, from, to time.Time, bucketSec int) ([]store.TimeseriesBucket, error)
}

// errBadParam 是 handler 入口校验失败标记。
var errBadParam = errors.New("bad param")

// validateStatsCode 校验 stats endpoint 的 code 参数。
//
// 规则：base62（0-9A-Za-z）+ 长度 [1, 16]。
// 16 字节上限：与 slink 主路径短码生成（号段模式 base62，~10 chars）+ 富余兼容。
// 防注入：限定字符集排除 SQL 控制字符（' " ; 空格等）。
func validateStatsCode(s string) error {
	if l := len(s); l < 1 || l > 16 {
		return fmt.Errorf("code length must be in [1, 16], got %d", l)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		ok := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		if !ok {
			return fmt.Errorf("code must be base62 (0-9A-Za-z), got byte %q at pos %d", c, i)
		}
	}
	return nil
}

// parseTimeWindow 解析 from/to 参数，校验 from < to + 跨度 ≤ statsMaxWindowDays。
//
// 返回 from, to UTC time.Time。失败返回 errBadParam wrap。
func parseTimeWindow(fromStr, toStr string) (time.Time, time.Time, error) {
	from, err := time.Parse(time.RFC3339, fromStr)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: from must be RFC3339: %v", errBadParam, err)
	}
	to, err := time.Parse(time.RFC3339, toStr)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: to must be RFC3339: %v", errBadParam, err)
	}
	if !from.Before(to) {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: from must be before to", errBadParam)
	}
	if to.Sub(from) > statsMaxWindowDays*24*time.Hour {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: window must be ≤ %d days", errBadParam, statsMaxWindowDays)
	}
	return from.UTC(), to.UTC(), nil
}

// writeJSONOk 写 200 + JSON。
func writeJSONOk(ctx *fasthttp.RequestCtx, v interface{}) {
	ctx.Response.Header.Set("Content-Type", "application/json")
	_ = json.NewEncoder(ctx).Encode(v)
}

// writeStatsErr 根据错误类型写 400 / 503。
func writeStatsErr(ctx *fasthttp.RequestCtx, err error) {
	ctx.Response.Header.Set("Content-Type", "application/json")
	if errors.Is(err, errBadParam) {
		ctx.SetStatusCode(http.StatusBadRequest)
		_ = json.NewEncoder(ctx).Encode(map[string]string{"error": err.Error()})
		return
	}
	// CH 后端错误 → 503 + slog.Error 落档（debug 用）
	slog.Error("stats backend error", "err", err)
	ctx.SetStatusCode(http.StatusServiceUnavailable)
	_ = json.NewEncoder(ctx).Encode(map[string]string{"error": "analytics backend unavailable"})
}

// uvResp 是 /api/stats/uv 响应。
type uvResp struct {
	Code string `json:"code"`
	From string `json:"from"`
	To   string `json:"to"`
	UV   uint64 `json:"uv"`
}

// handleStatsUV 是 GET /api/stats/uv handler。
//
// query params:
//   - code   必填，base62 短码
//   - from   必填，RFC3339 UTC
//   - to     必填，RFC3339 UTC
func (s *Server) handleStatsUV(ctx *fasthttp.RequestCtx) {
	if s.stats == nil {
		writeStatsErr(ctx, errors.New("analytics not configured"))
		return
	}
	code := string(ctx.QueryArgs().Peek("code"))
	if err := validateStatsCode(code); err != nil {
		writeStatsErr(ctx, fmt.Errorf("%w: code: %v", errBadParam, err))
		return
	}
	from, to, err := parseTimeWindow(string(ctx.QueryArgs().Peek("from")), string(ctx.QueryArgs().Peek("to")))
	if err != nil {
		writeStatsErr(ctx, err)
		return
	}
	// 用 context.Background() 做 parent 而非 fasthttp ctx：
	// fasthttp ctx 在 unit test 直接构造场景下 Done() 通道字段是 nil 触发 panic；
	// stats 查询的取消语义由 statsQueryTimeout 控制，不需要继承 request scope cancel。
	queryCtx, cancel := context.WithTimeout(context.Background(), statsQueryTimeout)
	defer cancel()
	uv, err := s.stats.UV(queryCtx, code, from, to)
	if err != nil {
		writeStatsErr(ctx, err)
		return
	}
	writeJSONOk(ctx, uvResp{Code: code, From: from.Format(time.RFC3339), To: to.Format(time.RFC3339), UV: uv})
}

// topKResp 是 /api/stats/topk 响应。
type topKResp struct {
	From    string             `json:"from"`
	To      string             `json:"to"`
	N       int                `json:"n"`
	Entries []store.TopKEntry  `json:"entries"`
}

// handleStatsTopK 是 GET /api/stats/topk handler。
//
// query params:
//   - from   必填，RFC3339 UTC
//   - to     必填，RFC3339 UTC
//   - n      可选，默认 10，上限 statsMaxTopN
func (s *Server) handleStatsTopK(ctx *fasthttp.RequestCtx) {
	if s.stats == nil {
		writeStatsErr(ctx, errors.New("analytics not configured"))
		return
	}
	from, to, err := parseTimeWindow(string(ctx.QueryArgs().Peek("from")), string(ctx.QueryArgs().Peek("to")))
	if err != nil {
		writeStatsErr(ctx, err)
		return
	}
	n := 10
	if raw := string(ctx.QueryArgs().Peek("n")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 || v > statsMaxTopN {
			writeStatsErr(ctx, fmt.Errorf("%w: n must be in [1, %d]", errBadParam, statsMaxTopN))
			return
		}
		n = v
	}
	// 用 context.Background() 做 parent 而非 fasthttp ctx：
	// fasthttp ctx 在 unit test 直接构造场景下 Done() 通道字段是 nil 触发 panic；
	// stats 查询的取消语义由 statsQueryTimeout 控制，不需要继承 request scope cancel。
	queryCtx, cancel := context.WithTimeout(context.Background(), statsQueryTimeout)
	defer cancel()
	entries, err := s.stats.TopK(queryCtx, from, to, n)
	if err != nil {
		writeStatsErr(ctx, err)
		return
	}
	writeJSONOk(ctx, topKResp{From: from.Format(time.RFC3339), To: to.Format(time.RFC3339), N: n, Entries: entries})
}

// timeseriesResp 是 /api/stats/timeseries 响应。
type timeseriesResp struct {
	From      string                    `json:"from"`
	To        string                    `json:"to"`
	BucketSec int                       `json:"bucket_sec"`
	Buckets   []store.TimeseriesBucket  `json:"buckets"`
}

// handleStatsTimeseries 是 GET /api/stats/timeseries handler。
//
// query params:
//   - from    必填，RFC3339 UTC
//   - to      必填，RFC3339 UTC
//   - bucket  可选，默认 60（秒），范围 [1, 86400]
func (s *Server) handleStatsTimeseries(ctx *fasthttp.RequestCtx) {
	if s.stats == nil {
		writeStatsErr(ctx, errors.New("analytics not configured"))
		return
	}
	from, to, err := parseTimeWindow(string(ctx.QueryArgs().Peek("from")), string(ctx.QueryArgs().Peek("to")))
	if err != nil {
		writeStatsErr(ctx, err)
		return
	}
	bucketSec := 60
	if raw := string(ctx.QueryArgs().Peek("bucket")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < statsMinBucketSec || v > statsMaxBucketSec {
			writeStatsErr(ctx, fmt.Errorf("%w: bucket must be in [%d, %d]", errBadParam, statsMinBucketSec, statsMaxBucketSec))
			return
		}
		bucketSec = v
	}
	// 用 context.Background() 做 parent 而非 fasthttp ctx：
	// fasthttp ctx 在 unit test 直接构造场景下 Done() 通道字段是 nil 触发 panic；
	// stats 查询的取消语义由 statsQueryTimeout 控制，不需要继承 request scope cancel。
	queryCtx, cancel := context.WithTimeout(context.Background(), statsQueryTimeout)
	defer cancel()
	buckets, err := s.stats.Timeseries(queryCtx, from, to, bucketSec)
	if err != nil {
		writeStatsErr(ctx, err)
		return
	}
	writeJSONOk(ctx, timeseriesResp{From: from.Format(time.RFC3339), To: to.Format(time.RFC3339), BucketSec: bucketSec, Buckets: buckets})
}
