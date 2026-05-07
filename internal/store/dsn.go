package store

import (
	"net/url"
	"regexp"
	"strings"
)

// RedactDSN 返回脱敏后的 PG DSN，用于日志 / 错误消息。
//
// 支持两种格式：
//   - URL 形式：postgres://user:secret@host:5432/db?sslmode=disable
//     → postgres://user:***@host:5432/db?sslmode=disable
//   - KV 形式：host=h port=5432 user=u password=secret dbname=d
//     → host=h port=5432 user=u password=*** dbname=d
//
// 空字符串原样返回。
func RedactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	// URL 形式优先
	if u, err := url.Parse(dsn); err == nil && u.Scheme != "" && u.Host != "" {
		if u.User != nil {
			if _, hasPwd := u.User.Password(); hasPwd {
				u.User = url.UserPassword(u.User.Username(), "redacted")
			}
		}
		return u.String()
	}
	// KV 形式：替换 password=...
	return passwordKVRegex.ReplaceAllString(dsn, "password=***")
}

// passwordKVRegex 匹配 KV 形式 DSN 里的 password=xxx 段。
// password 不带空格的值终止于下一个空白；带引号则交由 pg 标准但本函数仅做粗暴替换。
var passwordKVRegex = regexp.MustCompile(`password\s*=\s*\S+`)

// redactSecrets 从 msg 中清除可能从 dsn 泄漏的秘密：
//  1. msg 中包含完整 dsn 子串 → 替换为脱敏 DSN
//  2. msg 中只包含 password 子串（pgx 报错可能引）→ 替换为 ***
//
// 用于 ParseConfig 失败时的错误消息处理（pgx 解析错误可能回灌原 DSN）。
func redactSecrets(msg, dsn string) string {
	if msg == "" || dsn == "" {
		return msg
	}
	msg = strings.ReplaceAll(msg, dsn, RedactDSN(dsn))

	// URL 形式：单独剔除 password 子串
	if u, err := url.Parse(dsn); err == nil && u.User != nil {
		if pwd, ok := u.User.Password(); ok && pwd != "" {
			msg = strings.ReplaceAll(msg, pwd, "redacted")
		}
	}
	// KV 形式：扫一遍找 password=...
	if matches := passwordKVRegex.FindAllString(dsn, -1); len(matches) > 0 {
		for _, kv := range matches {
			// kv = "password=secret"
			parts := strings.SplitN(kv, "=", 2)
			if len(parts) == 2 {
				pwd := strings.TrimSpace(parts[1])
				if pwd != "" {
					msg = strings.ReplaceAll(msg, pwd, "redacted")
				}
			}
		}
	}
	return msg
}
