package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zombiecd/slink/internal/event"
	"github.com/zombiecd/slink/internal/model"
)

// 工具：发起 GET /:code 请求（不跟随重定向）
func (h *harness) get(t *testing.T, code string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/"+code, nil)
	rec := httptest.NewRecorder()
	h.srv.Routes().ServeHTTP(rec, req)
	return rec
}

// 工具：先创建一条短链，返回 code
func (h *harness) createLink(t *testing.T, longURL string) string {
	t.Helper()
	body, _ := json.Marshal(model.CreateLinkRequest{LongURL: longURL})
	req := httptest.NewRequest(http.MethodPost, "/api/links", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.srv.Routes().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create link: %d %s", rec.Code, rec.Body.String())
	}
	var resp model.CreateLinkResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	return resp.Code
}

// ─────────────────────────────────────────────────────────
// 主流程：成功跳转 + 302 + Location 头
// ─────────────────────────────────────────────────────────

func TestRedirect_Success(t *testing.T) {
	h := setupHarness(t)
	longURL := "https://example.com/redirect-success?utm=test"
	code := h.createLink(t, longURL)

	rec := h.get(t, code)

	if rec.Code != http.StatusFound {
		t.Fatalf("status: got %d, want 302", rec.Code)
	}
	if loc := rec.Header().Get("Location"); loc != longURL {
		t.Errorf("Location: got %q, want %q", loc, longURL)
	}
}

// ─────────────────────────────────────────────────────────
// cache hit/miss：第二次请求不应打 DB（验证 cache 接管了）
// ─────────────────────────────────────────────────────────

func TestRedirect_CacheHitsAfterFirstMiss(t *testing.T) {
	h := setupHarness(t)
	longURL := "https://example.com/cache-hit"
	code := h.createLink(t, longURL)

	// 第一次：cache miss → DB 回源 → 写 cache
	rec1 := h.get(t, code)
	if rec1.Code != http.StatusFound {
		t.Fatalf("first: got %d", rec1.Code)
	}

	// 直接看 Redis：现在应该有 link:{code}（不是 missingMarker）
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	raw, err := h.rdb.Get(ctx, "link:"+code)
	if err != nil {
		t.Fatalf("redis get after first hit: %v", err)
	}
	if raw == "" || raw == "__nil__" {
		t.Errorf("cache should hold real value, got %q", raw)
	}

	// 第二次：直接命中
	rec2 := h.get(t, code)
	if rec2.Code != http.StatusFound {
		t.Fatalf("second: got %d", rec2.Code)
	}
	if rec2.Header().Get("Location") != longURL {
		t.Errorf("second Location wrong")
	}
}

// ─────────────────────────────────────────────────────────
// 不存在的 code：404 + 写空值标记防穿透
// ─────────────────────────────────────────────────────────

func TestRedirect_NotFound_WritesMissingMarker(t *testing.T) {
	h := setupHarness(t)
	code := "Z9zZ9z" // 不存在

	rec := h.get(t, code)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", rec.Code)
	}

	// 验证 Redis 里写了空值标记
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	raw, err := h.rdb.Get(ctx, "link:"+code)
	if err != nil {
		t.Fatalf("redis get: %v", err)
	}
	if raw != "__nil__" {
		t.Errorf("missing marker not written: got %q", raw)
	}
	// 清理
	_ = h.rdb.Del(ctx, "link:"+code)
}

// ─────────────────────────────────────────────────────────
// 过期：返回 410 Gone（不是 404，不是 302）
// ─────────────────────────────────────────────────────────

func TestRedirect_Expired_410(t *testing.T) {
	h := setupHarness(t)
	// 直接通过 store 写一条已过期的短链（绕开 API 的 expires_at 校验未实现）
	expired := time.Now().Add(-time.Hour)
	code := h.createLink(t, "https://example.com/will-expire")

	// 把刚才创建的 link 改成已过期
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := h.pool.Exec(ctx, "UPDATE links SET expires_at = $1 WHERE code = $2", expired, code)
	if err != nil {
		t.Fatalf("update expires_at: %v", err)
	}
	// 清掉 cache（前面 createLink 没自动预热，但保险起见）
	_ = h.linkCache.Invalidate(ctx, code)

	rec := h.get(t, code)
	if rec.Code != http.StatusGone {
		t.Fatalf("status: got %d, want 410", rec.Code)
	}
}

// ─────────────────────────────────────────────────────────
// 边界：超长 code / 空 code → 404
// ─────────────────────────────────────────────────────────

func TestRedirect_TooLongCode(t *testing.T) {
	h := setupHarness(t)
	// 17 字节，超过 codeMaxLen = 16
	longCode := "abcdefghijklmnopq"
	rec := h.get(t, longCode)
	if rec.Code != http.StatusNotFound {
		t.Errorf("got %d, want 404 for long code", rec.Code)
	}
}

// ─────────────────────────────────────────────────────────
// 事件投递：跳转后 Eventer.Enqueue 必被调用一次
// ─────────────────────────────────────────────────────────

type recordingEventer struct {
	count atomic.Int32
	last  atomic.Pointer[event.ClickEvent]
	mu    sync.Mutex
}

func (r *recordingEventer) Enqueue(_ context.Context, evt event.ClickEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count.Add(1)
	cp := evt
	r.last.Store(&cp)
	return nil
}

func TestRedirect_EnqueuesClickEvent(t *testing.T) {
	h := setupHarness(t)
	rec := &recordingEventer{}
	// 替换 events，其它依赖复用
	h.srv.events = rec

	code := h.createLink(t, "https://example.com/event-test")
	resp := h.get(t, code)
	if resp.Code != http.StatusFound {
		t.Fatalf("status: %d", resp.Code)
	}

	// 异步投递理论上是同步入 channel，立即可见
	if got := rec.count.Load(); got != 1 {
		t.Errorf("Enqueue calls: got %d, want 1", got)
	}
	last := rec.last.Load()
	if last == nil {
		t.Fatal("no event recorded")
	}
	if last.Code != code {
		t.Errorf("event.Code: got %q, want %q", last.Code, code)
	}
	if last.EventID == "" {
		t.Error("event.EventID empty")
	}
	if last.TS.IsZero() {
		t.Error("event.TS zero")
	}
}

// ─────────────────────────────────────────────────────────
// 路由：/api/* 不会被 GET /:code 误吞
// ─────────────────────────────────────────────────────────

func TestRouting_APIPrefixNotShadowedByCodeRoute(t *testing.T) {
	h := setupHarness(t)
	// GET /api/links 不存在，应当返回 405 Method Not Allowed（POST 才接），
	// 而不是被 GET /{code} 当作 code="api" 处理
	req := httptest.NewRequest(http.MethodGet, "/api/links", nil)
	rec := httptest.NewRecorder()
	h.srv.Routes().ServeHTTP(rec, req)

	// Go 1.22 ServeMux 对已注册的 path 但未注册的 method 返回 405
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("/api/links GET: got %d, want 405", rec.Code)
	}
}
