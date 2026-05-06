package api

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
)

// 长 URL 大小上限（字符数）。
//
// 业界惯例：
//   - 浏览器普遍支持 ~2 KB
//   - HTTP 服务器（nginx 等）默认 ~8 KB
//   - 我们取 2048 防止 DoS（malicious 提交 100 MB URL）
const MaxLongURLLength = 2048

// 允许的 URL scheme 白名单。
//
// 拒绝：
//   - file://         读本地文件，安全风险
//   - javascript:    XSS
//   - data:          可绕过过滤
//   - ftp/gopher 等  少见 + 安全风险
var allowedSchemes = map[string]struct{}{
	"http":  {},
	"https": {},
}

// ValidateLongURL 校验创建短链时的 long_url 字段。
//
// 校验规则（逐条短路）：
//  1. 非空
//  2. 长度 ≤ MaxLongURLLength
//  3. 可解析为 URL
//  4. scheme ∈ {http, https}
//  5. host 非空
//  6. host 不是私网 / loopback / 链路本地（**SSRF 防御深度**）
//
// SSRF 注意：v0.1 slink **不主动 fetch 长 URL**——这一层 SSRF 校验
// 只是防御深度。真正的 SSRF 风险在 v0.3+ 加链接预览 / 健康检查时才出现。
func ValidateLongURL(s string) error {
	if s == "" {
		return errors.New("long_url is empty")
	}
	if len(s) > MaxLongURLLength {
		return fmt.Errorf("long_url length %d exceeds limit %d", len(s), MaxLongURLLength)
	}

	u, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	scheme := strings.ToLower(u.Scheme)
	if _, ok := allowedSchemes[scheme]; !ok {
		return fmt.Errorf("scheme %q not allowed (use http or https)", u.Scheme)
	}

	if u.Host == "" {
		return errors.New("URL host is empty")
	}

	host := u.Hostname() // 不含端口
	if isUnsafeHost(host) {
		return fmt.Errorf("host %q is private/loopback/link-local, not allowed", host)
	}

	return nil
}

// isUnsafeHost 判断 host 是否落在不安全的 IP 段。
//
// 仅拒绝**字面 IP** 形式（如 "127.0.0.1"）。
// 域名（如 "localhost"）单独由域名黑名单处理——避免在创建短链时
// 做 DNS 解析（DoS 风险 + DNS rebinding 攻击需要在跳转层防御）。
func isUnsafeHost(host string) bool {
	// 字面域名黑名单
	switch strings.ToLower(host) {
	case "localhost", "localhost.localdomain":
		return true
	}

	// 字面 IP 校验
	addr, err := netip.ParseAddr(host)
	if err != nil {
		// 不是 IP（如域名），不在这里处理
		return false
	}
	return addr.IsLoopback() ||
		addr.IsPrivate() ||
		addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() ||
		addr.IsMulticast() ||
		addr.IsUnspecified() ||
		isReserved(addr)
}

// isReserved 判断 v4 / v6 的保留地址段（netip 没直接覆盖的）。
//
//   - 0.0.0.0/8   "this network"
//   - 100.64.0.0/10 carrier-grade NAT
//   - 169.254.0.0/16 link-local（已被 IsLinkLocalUnicast 覆盖）
//   - 192.0.0.0/24, 192.0.2.0/24, 198.18.0.0/15 等保留段
//   - ::/128 ::1/128 已被 IsLoopback / IsUnspecified 覆盖
func isReserved(addr netip.Addr) bool {
	v4 := addr.As4()
	if !addr.Is4() && !addr.Is4In6() {
		return false
	}
	ip := net.IPv4(v4[0], v4[1], v4[2], v4[3])
	for _, cidr := range reservedV4 {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

var reservedV4 = parseCIDRs(
	"0.0.0.0/8",          // this network
	"100.64.0.0/10",      // CGN
	"192.0.0.0/24",       // IETF protocol assignments
	"192.0.2.0/24",       // TEST-NET-1
	"198.18.0.0/15",      // benchmarking
	"198.51.100.0/24",    // TEST-NET-2
	"203.0.113.0/24",     // TEST-NET-3
	"240.0.0.0/4",        // class E
	"255.255.255.255/32", // broadcast
)

func parseCIDRs(cidrs ...string) []*net.IPNet {
	out := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		_, n, err := net.ParseCIDR(c)
		if err != nil {
			panic(fmt.Sprintf("invalid CIDR in reservedV4: %s", c))
		}
		out = append(out, n)
	}
	return out
}
