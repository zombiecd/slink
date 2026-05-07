package api

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
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
	//
	// 手写替代 http.Redirect(w, r, url, 302)：
	//   标准库会写一段 HTML body（"<a href=...>Found</a>"）+ 设置 Content-Type，
	//   并对 url 做 escapeHTML。302 的 body 浏览器根本不展示——纯浪费。
	//   profile 显示 http.Redirect cum alloc 占 21% 总分配。
	//   手写：仅写 Location header + 302 status code → 一个写头一个写状态。
	w.Header().Set("Location", link.LongURL)
	w.WriteHeader(http.StatusFound)

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
// 在 redirect 之后调用：响应已写出，这里阻塞也只阻塞当前 goroutine
// 的"清理时间"，不影响用户体验。但 Eventer 实现自己应该是非阻塞的。
//
// 不传 r.Context()：跳转响应已发完，请求 context 即将被 cancel。
// 也不创建 context.WithTimeout —— Buffer.Enqueue 默认是 select-default
// 非阻塞路径，根本不读 ctx。每次请求 new timer + cancel 是纯浪费。
// （profile 显示 context.WithDeadlineCause 占 5.5% 总分配）
func (s *Server) enqueueClickEvent(r *http.Request, code string) {
	if s.events == nil {
		return
	}

	evt := event.ClickEvent{
		EventID:   newEventID(),
		Code:      code,
		IP:        clientIP(r),
		UserAgent: r.UserAgent(),
		Referer:   r.Referer(),
		TS:        time.Now().UTC(),
	}

	if err := s.events.Enqueue(context.Background(), evt); err != nil {
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

// hexChars 是 hex 编码查找表（小写），用于 newEventID 的零分配格式化。
const hexChars = "0123456789abcdef"

// newEventID 生成符合 RFC 4122 v4 的 UUID 字符串。
//
// 这里不用 crypto/rand：
//   - ClickEvent 的 EventID 仅用于"事件去重 / 日志关联"，不是安全凭证
//   - profile 显示 crypto/internal/sysrand.Read 走 syscall（getentropy/urandom），
//     且 fmt.Sprintf("%08x-...", b[0:4], ...) 内部 sub-slice + format 反射是 alloc 大头
//
// 改为 math/rand/v2 ChaCha8 + 直接 byte buffer 填 hex：
//   - rand/v2 是无锁、纯用户态的 ChaCha8 PRNG，每次 Uint64 几个 ns
//   - 36 字节固定栈分配 + 一次 string 转换，省掉 fmt.Sprintf 5 个 sub-slice 反射
//
// 输出形如：xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx（y ∈ {8,9,a,b}）
// PG 列 event_id UUID 直接接受这种字符串。
func newEventID() string {
	// 用 2 个 uint64（16 字节）作为随机源，避开 crypto/rand 的 syscall
	hi := rand.Uint64()
	lo := rand.Uint64()

	// 按 RFC 4122 v4 设置标记位：
	//   字节 6 高 4 位 = 0x40（version 4）
	//   字节 8 高 2 位 = 0x80（variant 10xx）
	// hi 的字节 0..7：[0..7]，字节 6 在 hi 中是 (hi >> 8) & 0xff
	// lo 的字节 8..15：[8..15]，字节 8 在 lo 中是 (lo >> 56) & 0xff
	hi = (hi & 0xffff_ffff_ffff_0fff) | 0x0000_0000_0000_4000 // version 4
	lo = (lo & 0x3fff_ffff_ffff_ffff) | 0x8000_0000_0000_0000 // variant 10xx

	// 36 字节：32 hex + 4 dash
	var buf [36]byte
	// 写 hex 的小工具（in-place）
	writeHex := func(off int, n uint64, nibbles int) {
		for i := nibbles - 1; i >= 0; i-- {
			buf[off+i] = hexChars[n&0xf]
			n >>= 4
		}
	}
	// 8-4-4-4-12 = 32 hex + 4 dash
	writeHex(0, hi>>32, 8)        // 8 hex from hi[0..3]
	buf[8] = '-'
	writeHex(9, (hi>>16)&0xffff, 4) // 4 hex from hi[4..5]
	buf[13] = '-'
	writeHex(14, hi&0xffff, 4)    // 4 hex from hi[6..7]
	buf[18] = '-'
	writeHex(19, lo>>48, 4)       // 4 hex from lo[0..1]
	buf[23] = '-'
	writeHex(24, lo&0xffff_ffff_ffff, 12) // 12 hex from lo[2..7]

	return string(buf[:])
}
