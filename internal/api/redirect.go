package api

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/zombiecd/slink/internal/cache"
	"github.com/zombiecd/slink/internal/event"
	"github.com/zombiecd/slink/internal/model"
	slinkotel "github.com/zombiecd/slink/internal/otel"
	"github.com/zombiecd/slink/internal/store"
)

// codeMaxLen 限制 path 里的短码长度，防止恶意超长 URL 探测。
//
// Base62(int64.MaxValue) ≈ 11 字符，留 16 给未来扩展（含 "+ ID 前缀混淆"）。
const codeMaxLen = 16

// handleRedirect 处理 GET /{code}。
//
// 主路径 SLA：< 5ms（命中 Redis 时）。fasthttp + L1/L2 缓存可在单机扛 9w+ RPS。
//
// fasthttp 迁移要点（vs Day 6 net/http 版）：
//   - path param 通过 ctx.UserValue("code").(string) 读取，
//     fasthttp/router 已经把 path slice 复制成 string，可直接用
//   - http.Redirect → ctx.Response.Header.Set("Location") + ctx.SetStatusCode(302)
//   - 使用 ctx 作为 context.Context 直接传给 cache（fasthttp.RequestCtx 实现了 context.Context）
//
// 流程：
//
//	1. 提取并校验 code（长度 / 字符集留给 cache 层）
//	2. cache.GetOrLoad（内含三大坑防护）
//	3. 校验过期（ExpiresAt）
//	4. 302 + Location ◀── 用户立即跳走
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
func (s *Server) handleRedirect(ctx *fasthttp.RequestCtx) {
	code, _ := ctx.UserValue("code").(string)

	// 边界：空 code 或包含路径分隔符（典型探测：/api/foo, /../etc/passwd）
	// router 实际不会把这种路由进来，但兜底防御
	if code == "" || strings.ContainsAny(code, "/?#") {
		writeError(ctx, http.StatusNotFound, ErrNotFound, "code is empty or invalid")
		return
	}
	if len(code) > codeMaxLen {
		writeError(ctx, http.StatusNotFound, ErrNotFound, "code too long")
		return
	}

	// 查缓存（cache miss 自动回源 DB）
	link, err := s.linkCache.GetOrLoad(ctx, code, s.dbLoader(code))
	if err != nil {
		if errors.Is(err, cache.ErrLinkNotFound) {
			writeError(ctx, http.StatusNotFound, ErrNotFound, "no such link")
			return
		}
		// 客户端断开：fasthttp 默认不 cancel per-request context，
		// 但底层 Redis client 可能因自身超时返回 context.DeadlineExceeded。
		// 同 Day 6 处理：不刷 ERROR 日志。
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		// 真错误：DB 抖动 / Redis 抖动
		slog.Error("redirect lookup failed", "code", code, "err", err)
		writeError(ctx, http.StatusInternalServerError, ErrInternal, "lookup failed")
		return
	}

	// 过期检查（ExpiresAt 为 nil 视为永不过期）
	if link.IsExpired(time.Now()) {
		// 过期短链返回 410 Gone（不是 404）
		// 客户端能区分"从来没有"vs"曾经有过但过期了"
		writeError(ctx, http.StatusGone, ErrNotFound, "link expired")
		return
	}

	// 重定向 → 用户立即跳走
	//
	// 手写替代 http.Redirect（与 Day 6 同思路）：
	//   只写 Location header + 302 status code，body 留空。
	//   fasthttp 不会自带 HTML body，比 net/http 还省一笔。
	ctx.Response.Header.Set("Location", link.LongURL)
	ctx.SetStatusCode(http.StatusFound)

	// 异步事件投递（**响应已发完**，这里出错也不影响用户）
	s.enqueueClickEvent(ctx, code)
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
// fasthttp 零拷贝注意：
//   - ctx.Request.Header.UserAgent() / Referer() 返回的 []byte 仅在 handler 期间有效，
//     async 入队前必须 string() 拷贝（Go string 不可变 → 安全持有）
//   - clientIP 返回 net.IP 已是新拷贝
//   - code 由 router 解析为 string，已是拷贝
//
// 不创建 context.WithTimeout —— Buffer.Enqueue 默认 select-default 非阻塞，不读 ctx。
// 同 Day 6 优化：避免 context.WithDeadlineCause 的 timer alloc。
func (s *Server) enqueueClickEvent(ctx *fasthttp.RequestCtx, code string) {
	if s.events == nil {
		return
	}

	evt := event.ClickEvent{
		EventID:   newEventID(),
		Code:      code,
		IP:        s.clientIP(ctx),
		UserAgent: string(ctx.Request.Header.UserAgent()),
		Referer:   string(ctx.Request.Header.Referer()),
		TS:        time.Now().UTC(),
	}

	// v0.6 Phase 4.1：传 OTel context（含 server span）让 KafkaProducer 能续 trace。
	// KafkaProducer.Enqueue 设计上仍忽略 caller cancel（用 p.bgCtx 创建 sendCtx），
	// 这里的 ctx 仅用于 trace propagation 不影响 cancel 语义。
	otelCtx := slinkotel.CtxFromFasthttp(ctx)
	if err := s.events.Enqueue(otelCtx, evt); err != nil {
		// Enqueue 失败（buffer 满 / 关闭中）：仅记日志，不影响跳转
		slog.Warn("enqueue click event failed", "code", code, "err", err)
	}
}

// clientIP 从 RemoteAddr / X-Forwarded-For / X-Real-IP 提取客户端 IP。
//
// v0.3 H6 hardening：
//   - 仅当 RemoteAddr 命中 cfg.TrustedProxies 时才信任 XFF / X-Real-IP
//   - 否则一律以 RemoteAddr 为准
//   - cfg.TrustedProxies 为空（默认）= 永远不信任 XFF
//
// 这样防止任意 client 通过自定义 X-Forwarded-For header 投毒 click_events.ip 列。
//
// fasthttp Header.Peek 返回 zero-copy []byte（仅 handler 期间有效），
// net.ParseIP 会做拷贝，结果安全。
func (s *Server) clientIP(ctx *fasthttp.RequestCtx) net.IP {
	var remote net.IP
	if tcp, ok := ctx.RemoteAddr().(*net.TCPAddr); ok {
		remote = tcp.IP
	}

	// 不信任 XFF：直接用 RemoteAddr
	if !s.isTrustedProxy(remote) {
		return remote
	}

	// RemoteAddr 在白名单内（确认是 LB / 反代）→ 才相信它转发的客户端 IP
	if xff := ctx.Request.Header.Peek("X-Forwarded-For"); len(xff) > 0 {
		// XFF 可能是 "client, proxy1, proxy2"，取最左（最初客户端）
		first := strings.TrimSpace(strings.SplitN(string(xff), ",", 2)[0])
		if ip := net.ParseIP(first); ip != nil {
			return ip
		}
	}
	if xri := ctx.Request.Header.Peek("X-Real-IP"); len(xri) > 0 {
		if ip := net.ParseIP(strings.TrimSpace(string(xri))); ip != nil {
			return ip
		}
	}
	return remote
}

// isTrustedProxy 判断 ip 是否落在 cfg.TrustedProxies 任一 CIDR 内。
// 名单为空时永远返回 false（最安全的默认）。
func (s *Server) isTrustedProxy(ip net.IP) bool {
	if ip == nil || len(s.cfg.TrustedProxies) == 0 {
		return false
	}
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	addr = addr.Unmap() // ::ffff:1.2.3.4 → 1.2.3.4
	for _, p := range s.cfg.TrustedProxies {
		if p.Contains(addr) {
			return true
		}
	}
	return false
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
	hi := rand.Uint64()
	lo := rand.Uint64()

	hi = (hi & 0xffff_ffff_ffff_0fff) | 0x0000_0000_0000_4000 // version 4
	lo = (lo & 0x3fff_ffff_ffff_ffff) | 0x8000_0000_0000_0000 // variant 10xx

	var buf [36]byte
	writeHex := func(off int, n uint64, nibbles int) {
		for i := nibbles - 1; i >= 0; i-- {
			buf[off+i] = hexChars[n&0xf]
			n >>= 4
		}
	}
	writeHex(0, hi>>32, 8)
	buf[8] = '-'
	writeHex(9, (hi>>16)&0xffff, 4)
	buf[13] = '-'
	writeHex(14, hi&0xffff, 4)
	buf[18] = '-'
	writeHex(19, lo>>48, 4)
	buf[23] = '-'
	writeHex(24, lo&0xffff_ffff_ffff, 12)

	return string(buf[:])
}
