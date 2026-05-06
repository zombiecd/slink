package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/zombiecd/slink/internal/id"
	"github.com/zombiecd/slink/internal/model"
	"github.com/zombiecd/slink/internal/store"
)

// 集成 e2e 测试：启 Server 跑真 SQL，需要 docker compose up + migrate。
// 单元层（参数校验、JSON 解析）不需要 PG，单独测。

// ────────────────────────────────────────────────────────────
// e2e 测试基础设施
// ────────────────────────────────────────────────────────────

type harness struct {
	pool *pgxpool.Pool
	srv  *Server
}

func setupHarness(t *testing.T) *harness {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test")
	}
	dsn := os.Getenv("SLINK_PG_DSN")
	if dsn == "" {
		dsn = "postgres://slink:slink@localhost:15432/slink?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	links := store.NewLinkRepo(pool)
	segs := store.NewSegmentRepo(pool)

	// 用真实 biz_tag = "link" 避免不同 biz_tag 取到相同 ID
	// （ID → code 是双射，相同 ID 必映射相同 code → unique 冲突）。
	// 记录 setup 时的 max_id，t.Cleanup 删除测试期间创建的范围。
	const bizTag = "link"
	var startMax int64
	if err := pool.QueryRow(ctx, "SELECT max_id FROM id_segment WHERE biz_tag = $1", bizTag).Scan(&startMax); err != nil {
		t.Fatalf("read startMax: %v", err)
	}

	buf, err := id.NewDoubleBuffer(ctx, segs, bizTag, 100, 0.9, nil)
	if err != nil {
		t.Fatalf("buffer: %v", err)
	}
	gen := id.NewGenerator(buf)

	srv := NewServer(Config{BaseURL: "http://test.local"}, gen, links)
	t.Cleanup(func() {
		bg := context.Background()
		var endMax int64
		_ = pool.QueryRow(bg, "SELECT max_id FROM id_segment WHERE biz_tag = $1", bizTag).Scan(&endMax)
		_, _ = pool.Exec(bg, "DELETE FROM links WHERE id > $1 AND id <= $2", startMax, endMax)
		pool.Close()
	})
	return &harness{pool: pool, srv: srv}
}

func (h *harness) post(t *testing.T, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/links", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.srv.Routes().ServeHTTP(rec, req)
	return rec
}

// ────────────────────────────────────────────────────────────
// 用例
// ────────────────────────────────────────────────────────────

func TestCreate_Success(t *testing.T) {
	h := setupHarness(t)
	rec := h.post(t, `{"long_url":"https://example.com/foo"}`, nil)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status: got %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	var resp model.CreateLinkResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Code) != 6 {
		t.Errorf("code length %d, want 6: %q", len(resp.Code), resp.Code)
	}
	if !strings.HasPrefix(resp.ShortURL, "http://test.local/") {
		t.Errorf("short_url prefix wrong: %q", resp.ShortURL)
	}
	if !strings.HasSuffix(resp.ShortURL, resp.Code) {
		t.Errorf("short_url should end with code: %q vs %q", resp.ShortURL, resp.Code)
	}
	if resp.LongURL != "https://example.com/foo" {
		t.Errorf("long_url: got %q", resp.LongURL)
	}
}

func TestCreate_BadJSON(t *testing.T) {
	h := setupHarness(t)
	rec := h.post(t, `not json`, nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", rec.Code)
	}
}

func TestCreate_UnknownField(t *testing.T) {
	h := setupHarness(t)
	rec := h.post(t, `{"long_url":"https://x.com","unknown":1}`, nil)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("DisallowUnknownFields should reject: got %d", rec.Code)
	}
}

func TestCreate_InvalidURL(t *testing.T) {
	h := setupHarness(t)
	cases := []string{
		`{"long_url":""}`,
		`{"long_url":"javascript:alert(1)"}`,
		`{"long_url":"http://127.0.0.1/x"}`,
		`{"long_url":"http://localhost/x"}`,
	}
	for _, body := range cases {
		t.Run(body, func(t *testing.T) {
			rec := h.post(t, body, nil)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("got %d, want 400 for %s", rec.Code, body)
			}
		})
	}
}

func TestCreate_IdempotencyHit(t *testing.T) {
	h := setupHarness(t)
	idem := "test-key-" + t.Name()
	body := `{"long_url":"https://example.com/idem"}`

	// 第一次：201 Created
	rec1 := h.post(t, body, map[string]string{"Idempotency-Key": idem})
	if rec1.Code != http.StatusCreated {
		t.Fatalf("first: got %d, want 201; body=%s", rec1.Code, rec1.Body.String())
	}
	var resp1 model.CreateLinkResponse
	_ = json.Unmarshal(rec1.Body.Bytes(), &resp1)

	// 第二次同 key：200 OK + 同 code
	rec2 := h.post(t, body, map[string]string{"Idempotency-Key": idem})
	if rec2.Code != http.StatusOK {
		t.Fatalf("second: got %d, want 200; body=%s", rec2.Code, rec2.Body.String())
	}
	var resp2 model.CreateLinkResponse
	_ = json.Unmarshal(rec2.Body.Bytes(), &resp2)

	if resp1.Code != resp2.Code {
		t.Errorf("idem replay: code differs: %q vs %q", resp1.Code, resp2.Code)
	}
}

func TestCreate_IdempotencyConcurrentRace(t *testing.T) {
	h := setupHarness(t)
	idem := "race-" + t.Name()
	body := `{"long_url":"https://example.com/race"}`

	const n = 8
	codes := make([]string, n)
	statuses := make([]int, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			rec := h.post(t, body, map[string]string{"Idempotency-Key": idem})
			statuses[i] = rec.Code
			var r model.CreateLinkResponse
			_ = json.Unmarshal(rec.Body.Bytes(), &r)
			codes[i] = r.Code
		}()
	}
	wg.Wait()

	// 所有响应必须是 201 或 200，且 code 全部相同
	first := codes[0]
	for i, c := range codes {
		if c != first {
			t.Errorf("response %d code=%q differs from first=%q (status=%d)", i, c, first, statuses[i])
		}
		if statuses[i] != http.StatusCreated && statuses[i] != http.StatusOK {
			t.Errorf("response %d unexpected status %d", i, statuses[i])
		}
	}
	t.Logf("race: %d concurrent requests, all code=%q, statuses=%v", n, first, statuses)
}

// ────────────────────────────────────────────────────────────
// 单元测试（不依赖 PG）
// ────────────────────────────────────────────────────────────

func TestErrorTypes_Exposed(t *testing.T) {
	// sanity check：Day 4 暴露的 store error 哨兵在 api 包能用
	if !errors.Is(store.ErrLinkNotFound, store.ErrLinkNotFound) {
		t.Fatal("sanity")
	}
}
