// Package api 实现 slink HTTP 接口层。
//
// 拆分原则：
//   - server.go    Server 装配 + Routes
//   - json.go      writeJSON / writeError 通用工具
//   - validate.go  输入校验
//   - links.go     /api/links 处理器
//
// v0.2 起底层 HTTP 栈从 net/http 切到 valyala/fasthttp，
// 所有 handler 签名从 (w http.ResponseWriter, r *http.Request)
// 改成 (ctx *fasthttp.RequestCtx)。
package api

import (
	"encoding/json"
	"log/slog"

	"github.com/valyala/fasthttp"
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
	ErrInvalidJSON      = "invalid_json"
	ErrInvalidURL       = "invalid_url"
	ErrURLTooLong       = "url_too_long"
	ErrCodeGeneration   = "code_generation_failed"
	ErrInternal         = "internal_error"
	ErrNotFound         = "not_found"
	ErrMethodNotAllowed = "method_not_allowed"
)

// writeJSON 用 status 写出 v 的 JSON 表示。
//
// fasthttp.RequestCtx 同时实现 io.Writer，json.Encoder 直接写到 response body buffer。
// 编码失败时 fall back 到 plain text 500，避免向客户端泄漏内部错误细节。
func writeJSON(ctx *fasthttp.RequestCtx, status int, v any) {
	ctx.Response.Header.Set("Content-Type", "application/json; charset=utf-8")
	ctx.SetStatusCode(status)
	if err := json.NewEncoder(ctx).Encode(v); err != nil {
		// 此时 header / status 已写出，能做的只有 log
		slog.Error("encode response", "err", err)
	}
}

// writeError 写出统一格式的错误响应。
//
// status: HTTP 状态码（400 / 404 / 500 等）
// code:   机器可读错误代码（如 "invalid_url"）
// msg:    人类可读说明
func writeError(ctx *fasthttp.RequestCtx, status int, code, msg string) {
	writeJSON(ctx, status, ErrorResponse{
		Error:   code,
		Message: msg,
	})
}
