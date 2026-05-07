package api

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/zombiecd/slink/internal/cache"
	"github.com/zombiecd/slink/internal/event"
	"github.com/zombiecd/slink/internal/model"
	"github.com/zombiecd/slink/internal/store"
)

// codeMaxLen 限制 path 里的短码长度，防止恶意超长 URL 探测。
//
// Base62(int64.MaxValue) ≈ 11 字符，留 16 给未来扩展（含 "+ ID 前缀混淆"）。
const codeMaxLen = 16

// handleRedirect 处理 GET /:code。
//
// 主路径 SLA：< 5ms（命中 Redis 时）→ 简历"扛 10w QPS"故事的源头。
//
// 流程：
//
//	1. 提取并校验 code（长度 / 字符集留给 cache 层）
//	2. cache.GetOrLoad（内含三大坑防护）
//	3. 校验过期（ExpiresAt）
//	4. http.Redirect(302) ◀── 用户立即跳走
//	5. 异步 Enqueue ClickEvent（不阻塞已发出的响应）
//
// 响应码选择：
//
//	302 Found     —— 默认（每次跳转都打到服务端，**保事件采集**）
//	410 Gone      —— 已过期
//	404 Not Found —— code 不存在
//
// 不用 301 Moved Permanently：浏览器/CDN 会缓存 301，事件采集会断。
// 详见 docs/concepts/redirect-302-vs-301.md
func (s *Server) handleRedirect(w http.ResponseWriter, r *http.Request) {
	code := strings.TrimPrefix(r.URL.Path, "/")

	// 边界：空 code 或包含路径分隔符（典型探测：/api/foo, /../etc/passwd）
	// ServeMux 实际不会把这种路由进来，但兜底防御
	if code == "" || strings.ContainsAny(code, "/?#") {
		writeError(w, http.StatusNotFound, ErrNotFound, "code is empty or invalid")
		return
	}
	if len(code) > codeMaxLen {
		writeError(w, http.StatusNotFound, ErrNotFound, "code too long")
		return
	}

	// 查缓存（cache miss 自动回源 DB）
	link, err := s.linkCache.GetOrLoad(r.Context(), code, s.dbLoader(code))
	if err != nil {
		if errors.Is(err, cache.ErrLinkNotFound) {
			writeError(w, http.StatusNotFound, ErrNotFound, "no such link")
			return
		}
		// 客户端断开（context canceled / deadline exceeded）：
		// 这不是 server 错误，是用户提前关闭连接。压测下高频出现，
		// 不该刷 ERROR 日志（百万次会淹没真错误）。响应也写不出去了。
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		// 真错误：DB 抖动 / Redis 抖动
		slog.Error("redirect lookup failed", "code", code, "err", err)
		writeError(w, http.StatusInternalServerError, ErrInternal, "lookup failed")
		return
	}

	// 过期检查（ExpiresAt 为 nil 视为永不过期）
	if link.IsExpired(time.Now()) {
		// 过期短链返回 410 Gone（不是 404）
		// 客户端能区分"从来没有"vs"曾经有过但过期了"
		writeError(w, http.StatusGone, ErrNotFound, "link expired")
		return
	}

	// 重定向 → 用户立即跳走
	http.Redirect(w, r, link.LongURL, http.StatusFound)

	// 异步事件投递（**响应已发完**，这里出错也不影响用户）
	s.enqueueClickEvent(r, code)
}

// dbLoader 返回一个 LinkLoader，把 store.ErrLinkNotFound 翻译成 cache.ErrLinkNotFound。
//
// 翻译的原因：cache 包不应该 import store。约定 loader 用 cache 包的哨兵 error。
func (s *Server) dbLoader(code string) cache.LinkLoader {
	return func(ctx context.Context) (*model.Link, error) {
		link, err := s.links.GetByCode(ctx, code)
		if err != nil {
			if errors.Is(err, store.ErrLinkNotFound) {
				return nil, cache.ErrLinkNotFound
			}
			return nil, err
		}
		return link, nil
	}
}

// enqueueClickEvent 收集一个 ClickEvent 投递到 Eventer。
//
// 在 http.Redirect 之后调用：响应已写出，这里阻塞也只阻塞当前 goroutine
// 的"清理时间"，不影响用户体验。但 Eventer 实现自己应该是非阻塞的。
//
// 不传 r.Context()：跳转响应已发完，请求 context 即将被 cancel；
// 用 background context + 短 timeout 让事件入 channel 不被打断。
func (s *Server) enqueueClickEvent(r *http.Request, code string) {
	if s.events == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	evt := event.ClickEvent{
		EventID:   newEventID(),
		Code:      code,
		IP:        clientIP(r),
		UserAgent: r.UserAgent(),
		Referer:   r.Referer(),
		TS:        time.Now().UTC(),
	}

	if err := s.events.Enqueue(ctx, evt); err != nil {
		// Enqueue 失败（buffer 满 / 关闭中）：仅记日志，不影响跳转
		slog.Warn("enqueue click event failed", "code", code, "err", err)
	}
}

// clientIP 从 X-Forwarded-For / X-Real-IP / RemoteAddr 三处提取客户端 IP。
//
// 注意：生产部署在 LB 后面时 X-Forwarded-For 才可信。
// v0.1 简化处理；v0.3 加 trusted proxy 列表。
func clientIP(r *http.Request) net.IP {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// XFF 可能是 "client, proxy1, proxy2"，取第一个
		first := strings.TrimSpace(strings.SplitN(xff, ",", 2)[0])
		if ip := net.ParseIP(first); ip != nil {
			return ip
		}
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		if ip := net.ParseIP(strings.TrimSpace(xri)); ip != nil {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return net.ParseIP(host)
}

// newEventID 生成符合 RFC 4122 v4 的 UUID 字符串。
//
// 为啥不引 google/uuid？短链项目还没到引入 uuid 库的复杂度；
// 16 字节随机 + 设置 v4/variant 标记位是 ~10 行代码。
//
// 输出形如：xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx（y ∈ {8,9,a,b}）
// PG 列 event_id UUID 直接接受这种字符串。
func newEventID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand 失败极罕见（随机源损坏），退化用 ts 兜底，
		// 但此时无法构成合法 UUID — 上层会拒绝，让事件丢弃即可
		return time.Now().UTC().Format("20060102-1504-4000-8000-000000000000")
	}
	// 设置 v4 (xxxx-xxxx-4xxx-yxxx-...) 和 variant (10xx)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC4122
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
