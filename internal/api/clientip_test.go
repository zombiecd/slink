package api

import (
	"net"
	"net/netip"
	"testing"

	"github.com/valyala/fasthttp"
)

// H6 回归：clientIP / isTrustedProxy 行为闭环。
//
// 核心不变量：
//   - cfg.TrustedProxies == nil → 永远忽略 XFF / X-Real-IP，使用 RemoteAddr
//   - RemoteAddr 不在白名单 → 同上
//   - RemoteAddr 在白名单 → 按 XFF（最左）→ X-Real-IP → RemoteAddr 优先级返回

// fakeRemoteAddr 让 RequestCtx.RemoteAddr 返回任意 net.IP 的 TCPAddr。
type fakeRemoteAddr struct{ ip net.IP }

func (f *fakeRemoteAddr) Network() string { return "tcp" }
func (f *fakeRemoteAddr) String() string {
	if f.ip == nil {
		return ""
	}
	return (&net.TCPAddr{IP: f.ip, Port: 0}).String()
}

// 用 SetRemoteAddr 覆盖 fasthttp 默认 RemoteAddr。
func newCtxWith(remote net.IP, headers map[string]string) *fasthttp.RequestCtx {
	ctx := &fasthttp.RequestCtx{}
	if remote != nil {
		ctx.SetRemoteAddr(&net.TCPAddr{IP: remote, Port: 12345})
	}
	for k, v := range headers {
		ctx.Request.Header.Set(k, v)
	}
	return ctx
}

func mustPrefix(t *testing.T, s string) netip.Prefix {
	t.Helper()
	p, err := netip.ParsePrefix(s)
	if err != nil {
		t.Fatalf("ParsePrefix %q: %v", s, err)
	}
	return p
}

func TestClientIP_DefaultIgnoresXFF(t *testing.T) {
	srv := &Server{cfg: Config{}} // TrustedProxies 为 nil
	remote := net.ParseIP("203.0.113.1")
	ctx := newCtxWith(remote, map[string]string{
		"X-Forwarded-For": "1.2.3.4",
		"X-Real-IP":       "5.6.7.8",
	})

	got := srv.clientIP(ctx)
	if !got.Equal(remote) {
		t.Errorf("got %v, want %v (XFF must be ignored when TrustedProxies empty)", got, remote)
	}
}

func TestClientIP_TrustedProxyHonorsXFF(t *testing.T) {
	srv := &Server{cfg: Config{
		TrustedProxies: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")},
	}}
	remote := net.ParseIP("10.1.2.3") // LB
	ctx := newCtxWith(remote, map[string]string{
		"X-Forwarded-For": "1.2.3.4, 10.1.2.3",
	})

	got := srv.clientIP(ctx)
	want := net.ParseIP("1.2.3.4")
	if !got.Equal(want) {
		t.Errorf("got %v, want %v (XFF must be honored when RemoteAddr is trusted)", got, want)
	}
}

func TestClientIP_UntrustedProxyIgnoresXFF(t *testing.T) {
	srv := &Server{cfg: Config{
		TrustedProxies: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")},
	}}
	remote := net.ParseIP("203.0.113.42") // 公网 client，不在白名单
	ctx := newCtxWith(remote, map[string]string{
		"X-Forwarded-For": "1.2.3.4", // 试图投毒
	})

	got := srv.clientIP(ctx)
	if !got.Equal(remote) {
		t.Errorf("got %v, want %v (untrusted client cannot inject XFF)", got, remote)
	}
}

func TestClientIP_TrustedProxyXRealIPFallback(t *testing.T) {
	srv := &Server{cfg: Config{
		TrustedProxies: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")},
	}}
	remote := net.ParseIP("10.1.2.3")
	ctx := newCtxWith(remote, map[string]string{
		"X-Real-IP": "5.6.7.8",
		// 没有 XFF
	})

	got := srv.clientIP(ctx)
	want := net.ParseIP("5.6.7.8")
	if !got.Equal(want) {
		t.Errorf("got %v, want %v (X-Real-IP fallback when XFF missing)", got, want)
	}
}

func TestClientIP_TrustedProxyMalformedXFFFallsBackToRemote(t *testing.T) {
	srv := &Server{cfg: Config{
		TrustedProxies: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")},
	}}
	remote := net.ParseIP("10.1.2.3")
	ctx := newCtxWith(remote, map[string]string{
		"X-Forwarded-For": "not-an-ip",
	})

	got := srv.clientIP(ctx)
	if !got.Equal(remote) {
		t.Errorf("got %v, want %v (malformed XFF should fall back to remote)", got, remote)
	}
}

func TestClientIP_IPv4MappedIPv6InTrustedRange(t *testing.T) {
	// 反代用 dual-stack listening 时，RemoteAddr 可能是 ::ffff:10.1.2.3
	srv := &Server{cfg: Config{
		TrustedProxies: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")},
	}}
	remote := net.ParseIP("::ffff:10.1.2.3")
	ctx := newCtxWith(remote, map[string]string{
		"X-Forwarded-For": "1.2.3.4",
	})

	got := srv.clientIP(ctx)
	want := net.ParseIP("1.2.3.4")
	if !got.Equal(want) {
		t.Errorf("got %v, want %v (Unmap should match v4-mapped v6 to v4 CIDR)", got, want)
	}
}

func TestIsTrustedProxy_NilIP(t *testing.T) {
	srv := &Server{cfg: Config{
		TrustedProxies: []netip.Prefix{mustPrefix(t, "10.0.0.0/8")},
	}}
	if srv.isTrustedProxy(nil) {
		t.Error("nil IP must not be trusted")
	}
}
