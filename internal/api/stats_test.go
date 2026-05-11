// stats_test.go — v0.5 Day 25 分析查询 handler 单测。
//
// 覆盖：
//   - parseTimeWindow：from < to / 跨度上限 / RFC3339 格式
//   - handler 入口校验：code / from / to / n / bucket 边界
//   - statsRepo nil 时 503（未注入）
//   - statsRepo error 时 503
//   - statsRepo 成功时 200 + JSON
//
// 不覆盖：真 CH 连接（留 integration test / benchmark 验证）。
package api

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/zombiecd/slink/internal/store"
)

// fakeStatsRepo 是 statsRepo 的内存实现，用于测试。
type fakeStatsRepo struct {
	uvFn         func(ctx context.Context, code string, from, to time.Time) (uint64, error)
	topKFn       func(ctx context.Context, from, to time.Time, n int) ([]store.TopKEntry, error)
	timeseriesFn func(ctx context.Context, from, to time.Time, bucketSec int) ([]store.TimeseriesBucket, error)
}

func (f *fakeStatsRepo) UV(ctx context.Context, code string, from, to time.Time) (uint64, error) {
	return f.uvFn(ctx, code, from, to)
}
func (f *fakeStatsRepo) TopK(ctx context.Context, from, to time.Time, n int) ([]store.TopKEntry, error) {
	return f.topKFn(ctx, from, to, n)
}
func (f *fakeStatsRepo) Timeseries(ctx context.Context, from, to time.Time, bucketSec int) ([]store.TimeseriesBucket, error) {
	return f.timeseriesFn(ctx, from, to, bucketSec)
}

func TestParseTimeWindow(t *testing.T) {
	tests := []struct {
		name    string
		from    string
		to      string
		wantErr bool
	}{
		{"ok", "2026-05-11T00:00:00Z", "2026-05-11T01:00:00Z", false},
		{"from invalid", "2026/05/11", "2026-05-11T01:00:00Z", true},
		{"to invalid", "2026-05-11T00:00:00Z", "2026/05/11", true},
		{"from == to", "2026-05-11T00:00:00Z", "2026-05-11T00:00:00Z", true},
		{"from > to", "2026-05-11T01:00:00Z", "2026-05-11T00:00:00Z", true},
		{"window > 31 days", "2026-04-01T00:00:00Z", "2026-05-15T00:00:00Z", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parseTimeWindow(tt.from, tt.to)
			if (err != nil) != tt.wantErr {
				t.Fatalf("wantErr=%v got=%v", tt.wantErr, err)
			}
			if tt.wantErr && err != nil && !errors.Is(err, errBadParam) {
				t.Fatalf("expect errBadParam wrap, got %v", err)
			}
		})
	}
}

// reqStatsUV / reqStatsTopK / reqStatsTimeseries 用 fasthttp 内置工具跑 handler。
//
// 不起 net listener，直接构造 RequestCtx 调 handler — 单测覆盖路由后逻辑即可，
// 路由表绑定由 Routes 处理（编译期保证）。
func reqStatsUV(t *testing.T, s *Server, query string) *fasthttp.RequestCtx {
	t.Helper()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/api/stats/uv?" + query)
	s.handleStatsUV(ctx)
	return ctx
}
func reqStatsTopK(t *testing.T, s *Server, query string) *fasthttp.RequestCtx {
	t.Helper()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/api/stats/topk?" + query)
	s.handleStatsTopK(ctx)
	return ctx
}
func reqStatsTimeseries(t *testing.T, s *Server, query string) *fasthttp.RequestCtx {
	t.Helper()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetRequestURI("/api/stats/timeseries?" + query)
	s.handleStatsTimeseries(ctx)
	return ctx
}

func newTestServer(repo statsRepo) *Server {
	s := &Server{cfg: Config{BaseURL: "http://x.test"}}
	s.stats = repo
	return s
}

func TestHandleStatsUV_NoRepo503(t *testing.T) {
	s := newTestServer(nil)
	ctx := reqStatsUV(t, s, "code=abc&from=2026-05-11T00:00:00Z&to=2026-05-11T01:00:00Z")
	if got, want := ctx.Response.StatusCode(), fasthttp.StatusServiceUnavailable; got != want {
		t.Fatalf("status: got %d want %d", got, want)
	}
}

func TestHandleStatsUV_BadCode(t *testing.T) {
	repo := &fakeStatsRepo{uvFn: func(_ context.Context, _ string, _, _ time.Time) (uint64, error) {
		t.Fatal("should not call repo")
		return 0, nil
	}}
	s := newTestServer(repo)
	// 含非 base62 字符
	ctx := reqStatsUV(t, s, "code=ab!c&from=2026-05-11T00:00:00Z&to=2026-05-11T01:00:00Z")
	if got, want := ctx.Response.StatusCode(), fasthttp.StatusBadRequest; got != want {
		t.Fatalf("status: got %d want %d", got, want)
	}
}

func TestHandleStatsUV_BadWindow(t *testing.T) {
	repo := &fakeStatsRepo{uvFn: func(_ context.Context, _ string, _, _ time.Time) (uint64, error) {
		t.Fatal("should not call repo")
		return 0, nil
	}}
	s := newTestServer(repo)
	ctx := reqStatsUV(t, s, "code=abc123&from=2026-05-11T01:00:00Z&to=2026-05-11T00:00:00Z")
	if got, want := ctx.Response.StatusCode(), fasthttp.StatusBadRequest; got != want {
		t.Fatalf("status: got %d want %d", got, want)
	}
}

func TestHandleStatsUV_RepoErr503(t *testing.T) {
	repo := &fakeStatsRepo{uvFn: func(_ context.Context, _ string, _, _ time.Time) (uint64, error) {
		return 0, errors.New("ch dial: connection refused")
	}}
	s := newTestServer(repo)
	ctx := reqStatsUV(t, s, "code=abc123&from=2026-05-11T00:00:00Z&to=2026-05-11T01:00:00Z")
	if got, want := ctx.Response.StatusCode(), fasthttp.StatusServiceUnavailable; got != want {
		t.Fatalf("status: got %d want %d", got, want)
	}
	if !strings.Contains(string(ctx.Response.Body()), "analytics backend unavailable") {
		t.Fatalf("body should mention analytics backend: %s", ctx.Response.Body())
	}
}

func TestHandleStatsUV_OK(t *testing.T) {
	repo := &fakeStatsRepo{uvFn: func(_ context.Context, code string, from, to time.Time) (uint64, error) {
		if code != "abc123" {
			t.Errorf("code: got %q want abc123", code)
		}
		if from.IsZero() || to.IsZero() {
			t.Errorf("from/to zero")
		}
		return 42, nil
	}}
	s := newTestServer(repo)
	ctx := reqStatsUV(t, s, "code=abc123&from=2026-05-11T00:00:00Z&to=2026-05-11T01:00:00Z")
	if got, want := ctx.Response.StatusCode(), fasthttp.StatusOK; got != want {
		t.Fatalf("status: got %d want %d", got, want)
	}
	var resp uvResp
	if err := json.Unmarshal(ctx.Response.Body(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.UV != 42 || resp.Code != "abc123" {
		t.Fatalf("resp: %+v", resp)
	}
}

func TestHandleStatsTopK_DefaultN(t *testing.T) {
	repo := &fakeStatsRepo{topKFn: func(_ context.Context, _, _ time.Time, n int) ([]store.TopKEntry, error) {
		if n != 10 {
			t.Errorf("default n: got %d want 10", n)
		}
		return []store.TopKEntry{{Code: "a", Count: 100}, {Code: "b", Count: 50}}, nil
	}}
	s := newTestServer(repo)
	ctx := reqStatsTopK(t, s, "from=2026-05-11T00:00:00Z&to=2026-05-11T01:00:00Z")
	if got, want := ctx.Response.StatusCode(), fasthttp.StatusOK; got != want {
		t.Fatalf("status: got %d want %d", got, want)
	}
	var resp topKResp
	if err := json.Unmarshal(ctx.Response.Body(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.N != 10 || len(resp.Entries) != 2 {
		t.Fatalf("resp: %+v", resp)
	}
}

func TestHandleStatsTopK_OutOfRange(t *testing.T) {
	repo := &fakeStatsRepo{topKFn: func(_ context.Context, _, _ time.Time, _ int) ([]store.TopKEntry, error) {
		t.Fatal("should not call repo")
		return nil, nil
	}}
	s := newTestServer(repo)
	ctx := reqStatsTopK(t, s, "from=2026-05-11T00:00:00Z&to=2026-05-11T01:00:00Z&n=999")
	if got, want := ctx.Response.StatusCode(), fasthttp.StatusBadRequest; got != want {
		t.Fatalf("status: got %d want %d", got, want)
	}
}

func TestHandleStatsTimeseries_DefaultBucket(t *testing.T) {
	repo := &fakeStatsRepo{timeseriesFn: func(_ context.Context, _, _ time.Time, bucketSec int) ([]store.TimeseriesBucket, error) {
		if bucketSec != 60 {
			t.Errorf("default bucket: got %d want 60", bucketSec)
		}
		return []store.TimeseriesBucket{{Bucket: time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC), Count: 100}}, nil
	}}
	s := newTestServer(repo)
	ctx := reqStatsTimeseries(t, s, "from=2026-05-11T00:00:00Z&to=2026-05-11T01:00:00Z")
	if got, want := ctx.Response.StatusCode(), fasthttp.StatusOK; got != want {
		t.Fatalf("status: got %d want %d", got, want)
	}
}

func TestHandleStatsTimeseries_BucketOutOfRange(t *testing.T) {
	repo := &fakeStatsRepo{timeseriesFn: func(_ context.Context, _, _ time.Time, _ int) ([]store.TimeseriesBucket, error) {
		t.Fatal("should not call repo")
		return nil, nil
	}}
	s := newTestServer(repo)
	// bucket=0 < 下限 1
	ctx := reqStatsTimeseries(t, s, "from=2026-05-11T00:00:00Z&to=2026-05-11T01:00:00Z&bucket=0")
	if got, want := ctx.Response.StatusCode(), fasthttp.StatusBadRequest; got != want {
		t.Fatalf("bucket=0 status: got %d want %d", got, want)
	}
	// bucket=99999 > 上限 86400
	ctx2 := reqStatsTimeseries(t, s, "from=2026-05-11T00:00:00Z&to=2026-05-11T01:00:00Z&bucket=99999")
	if got, want := ctx2.Response.StatusCode(), fasthttp.StatusBadRequest; got != want {
		t.Fatalf("bucket=99999 status: got %d want %d", got, want)
	}
}
