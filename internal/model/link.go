// Package model 定义 slink 的核心领域类型。
//
// 这些类型不依赖 store / api / id 等任何下游包——保持纯数据 +
// 行为，便于在多层之间传递而无循环依赖。
package model

import "time"

// Link 是短链的核心领域对象。
//
// 字段映射 PG `links` 表（见 migrations/0001_init.up.sql）。
type Link struct {
	ID             int64      // 号段发号器分配的递增 ID
	Code           string     // Base62 + 位置混淆后的 6 位短码
	LongURL        string     // 完整长 URL（含 query string）
	CreatedAt      time.Time  // DB now() 默认值
	ExpiresAt      *time.Time // nil = 永不过期
	Creator        string     // 创建者标识（v0.1 暂留空）
	IdempotencyKey *string    // 幂等键（可选）；DB unique 约束
}

// IsExpired 判断短链是否已过期。
// ExpiresAt 为 nil 视为永不过期。
func (l *Link) IsExpired(now time.Time) bool {
	return l.ExpiresAt != nil && !now.Before(*l.ExpiresAt)
}

// ────────────────────────────────────────────────────────────
// HTTP DTO
// ────────────────────────────────────────────────────────────

// CreateLinkRequest 是 POST /api/links 的请求体。
type CreateLinkRequest struct {
	LongURL   string     `json:"long_url"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// CreateLinkResponse 是 POST /api/links 的响应体。
type CreateLinkResponse struct {
	Code      string     `json:"code"`
	ShortURL  string     `json:"short_url"`
	LongURL   string     `json:"long_url"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}
