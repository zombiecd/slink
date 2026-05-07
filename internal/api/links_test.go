package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"

	"github.com/zombiecd/slink/internal/cache"
	"github.com/zombiecd/slink/internal/event"
	"github.com/zombiecd/slink/internal/id"
	"github.com/zombiecd/slink/internal/model"
	"github.com/zombiecd/slink/internal/store"
)

// 集成 e2e 测试：启 Server 跑真 SQL，需要 docker compose up + migrate。
// 单元层（参数校验、JSON 解析）不需要 PG，单独测。
//
// v0.2 起底层切到 fasthttp，测试改用 fasthttputil.InmemoryListener：
//   - 启一个 fasthttp.Server 监听内存 listener
//   - http.Client 的 Transport.DialContext 走这个 listener.Dial
//   - 标准 http.Request/Response 的 assertion 全部保留
// 好处：业务断言一行不用改，只换"运输层"。

// ────────────────────────────────────────────────────────────
// e2e 测试基础设施
// ────────────────────────────────────────────────────────────

type harness struct {
	pool      *pgxpool.Pool
	srv       *Server
	linkCache *cache.LinkCache
	rdb       *cache.Client
	client    *http.Client     // 走 in-memory fasthttp server
	addr      string           // 仅用于拼 URL，dial 实际走 listener
	fhSrv     *fasthttp.Server // 持有引用便于 Cleanup 时关闭
	listener  *fasthttputil.InmemoryListener
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
	redisAddr := os.Getenv("SLINK_REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:16379"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect pg: %v", err)
	}
	links := store.NewLinkRepo(pool)
	segs := store.NewSegmentRepo(pool)

	rdb, err := cache.NewClient(ctx, cache.ClientConfig{Addr: redisAddr})
	if err != nil {
		pool.Close()
		t.Fatalf("connect redis: %v (Redis 起着吗？)", err)
	}
	// 测试用极短 TTL（避免污染后续测试）
	linkCache := cache.NewLinkCache(rdb,
		cache.WithTTL(2*time.Second),
		cache.WithMissingTTL(500*time.Millisecond),
	)

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

	srv := NewServer(
		Config{BaseURL: "http://test.local"},
		gen, links, linkCache, event.NopEventer{},
	)

	// fasthttp server + 内存 listener
	ln := fasthttputil.NewInmemoryListener()
	fhSrv := &fasthttp.Server{
		Handler:            srv.Routes().Handler,
		MaxRequestBodySize: 16 * 1024,
	}
	go func() {
		_ = fhSrv.Serve(ln)
	}()

	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return ln.Dial()
			},
		},
		// 不跟随 302（跳转测试要看 Location header / 状态码）
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: 5 * time.Second,
	}

	t.Cleanup(func() {
		bg := context.Background()
		var endMax int64
		_ = pool.QueryRow(bg, "SELECT max_id FROM id_segment WHERE biz_tag = $1", bizTag).Scan(&endMax)
		_, _ = pool.Exec(bg, "DELETE FROM links WHERE id > $1 AND id <= $2", startMax, endMax)
		_ = fhSrv.Shutdown()
		_ = ln.Close()
		_ = rdb.Close()
		pool.Close()
	})
	return &harness{
		pool:      pool,
		srv:       srv,
		linkCache: linkCache,
		rdb:       rdb,
		client:    httpClient,
		addr:      "http://test.local",
		fhSrv:     fhSrv,
		listener:  ln,
	}
}

// httpResp 是简化的响应快照，便于 assertion（不持 net.Conn / Body 句柄）。
type httpResp struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func (r *httpResp) BodyString() string { return string(r.Body) }

func (h *harness) do(t *testing.T, req *http.Request) *httpResp {
	t.Helper()
	resp, err := h.client.Do(req)
	if err != nil {
		t.Fatalf("http do: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return &httpResp{
		StatusCode: resp.StatusCode,
		Header:     resp.Header,
		Body:       body,
	}
}

func (h *harness) post(t *testing.T, body string, headers map[string]string) *httpResp {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.addr+"/api/links", strings.NewReader(body))
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return h.do(t, req)
}

// rawPost 走原始 io.Reader（用于发非 JSON 等场景）。预留，目前 post 已够用。
func (h *harness) rawPost(t *testing.T, body io.Reader) *httpResp {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, h.addr+"/api/links", body)
	req.Header.Set("Content-Type", "application/json")
	return h.do(t, req)
}

// ────────────────────────────────────────────────────────────
// 用例
// ────────────────────────────────────────────────────────────

func TestCreate_Success(t *testing.T) {
	h := setupHarness(t)
	rec := h.post(t, `{"long_url":"https://example.com/foo"}`, nil)

	if rec.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d, want 201; body=%s", rec.StatusCode, rec.BodyString())
	}
	var resp model.CreateLinkResponse
	if err := json.Unmarshal(rec.Body, &resp); err != nil {
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
	if rec.StatusCode != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", rec.StatusCode)
	}
}

func TestCreate_UnknownField(t *testing.T) {
	h := setupHarness(t)
	rec := h.post(t, `{"long_url":"https://x.com","unknown":1}`, nil)
	if rec.StatusCode != http.StatusBadRequest {
		t.Errorf("DisallowUnknownFields should reject: got %d", rec.StatusCode)
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
			if rec.StatusCode != http.StatusBadRequest {
				t.Errorf("got %d, want 400 for %s", rec.StatusCode, body)
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
	if rec1.StatusCode != http.StatusCreated {
		t.Fatalf("first: got %d, want 201; body=%s", rec1.StatusCode, rec1.BodyString())
	}
	var resp1 model.CreateLinkResponse
	_ = json.Unmarshal(rec1.Body, &resp1)

	// 第二次同 key：200 OK + 同 code
	rec2 := h.post(t, body, map[string]string{"Idempotency-Key": idem})
	if rec2.StatusCode != http.StatusOK {
		t.Fatalf("second: got %d, want 200; body=%s", rec2.StatusCode, rec2.BodyString())
	}
	var resp2 model.CreateLinkResponse
	_ = json.Unmarshal(rec2.Body, &resp2)

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
			statuses[i] = rec.StatusCode
			var r model.CreateLinkResponse
			_ = json.Unmarshal(rec.Body, &r)
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

func TestCreate_IdemKeyTooLong(t *testing.T) {
	h := setupHarness(t)
	// 129 字节，刚刚超过 MaxIdempotencyKeyLen=128
	tooLong := strings.Repeat("a", MaxIdempotencyKeyLen+1)
	body := `{"long_url":"https://example.com/idemlong"}`

	rec := h.post(t, body, map[string]string{"Idempotency-Key": tooLong})
	if rec.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400; body=%s", rec.StatusCode, rec.BodyString())
	}
	if !strings.Contains(rec.BodyString(), ErrIdemKeyTooLong) {
		t.Errorf("error code missing: %s", rec.BodyString())
	}

	// 边界值：恰好 128 字节应该通过（不命中 400 长度分支）
	exact := strings.Repeat("b", MaxIdempotencyKeyLen)
	rec2 := h.post(t, body, map[string]string{"Idempotency-Key": exact})
	if rec2.StatusCode != http.StatusCreated && rec2.StatusCode != http.StatusOK {
		t.Errorf("exact-length key should pass length check, got status %d", rec2.StatusCode)
	}
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

// 兼容性断言：harness 没用上的 helper 也保留构造，供后续测试扩展。
var _ = bytes.NewReader
