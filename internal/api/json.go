// Package api 实现 slink HTTP 接口层。
//
// 拆分原则：
//   - server.go    Server 装配 + Routes
//   - json.go      writeJSON / writeError 通用工具
//   - validate.go  输入校验
//   - links.go     /api/links 处理器
//
// 不做的事：
//   - 中间件链（v0.1 不需要）
//   - OpenAPI 自动生成（v0.2+）
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// ErrorResponse 是所有 4xx/5xx 错误的统一响应体。
//
//	{ "error": "invalid_url", "message": "scheme must be http or https" }
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// 业界惯例 error code（slink 用到的）。
const (
	ErrInvalidJSON         = "invalid_json"
	ErrInvalidURL          = "invalid_url"
	ErrURLTooLong          = "url_too_long"
	ErrCodeGeneration      = "code_generation_failed"
	ErrInternal            = "internal_error"
	ErrNotFound            = "not_found"
	ErrMethodNotAllowed    = "method_not_allowed"
)

// writeJSON 用 status 写出 v 的 JSON 表示。
// 失败时 fall back 到 plain text 500，避免向客户端泄漏内部错误细节。
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// 此时 header / status 已写出，能做的只有 log
		slog.Error("encode response", "err", err)
	}
}

// writeError 写出统一格式的错误响应。
//
// status: HTTP 状态码（400 / 404 / 500 等）
// code:   机器可读错误代码（如 "invalid_url"）
// msg:    人类可读说明
func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, ErrorResponse{
		Error:   code,
		Message: msg,
	})
}
