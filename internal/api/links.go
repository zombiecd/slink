package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/valyala/fasthttp"

	"github.com/zombiecd/slink/internal/id"
	"github.com/zombiecd/slink/internal/model"
	"github.com/zombiecd/slink/internal/store"
)

// MaxIdempotencyKeyLen 是接受的 Idempotency-Key 最大长度（字节）。
//
// IETF draft-ietf-httpapi-idempotency-key-header 建议 ≤ 255。
// 我们更严：128 字节足以表达 UUID/ULID 等业界常见格式，
// 防止超长 key spam 打到 DB 做无意义的 SELECT、撑大 idem_key 索引。
// DB 侧另有 CHECK (char_length(idem_key) <= 255) 兜底防御（migration 0002）。
const MaxIdempotencyKeyLen = 128

// handleCreateLink 处理 POST /api/links。
//
// fasthttp 迁移要点（vs Day 6 net/http 版）：
//   - body 用 ctx.PostBody() 拿到完整 []byte（已被 fasthttp.Server.MaxRequestBodySize 限大小）
//   - 用 bytes.NewReader 包成 io.Reader 喂 json.Decoder（保留 DisallowUnknownFields）
//   - Idempotency-Key header 用 ctx.Request.Header.Peek 零拷贝读 + string() 拷贝
//   - response 用 writeJSON / writeError（已迁 fasthttp）
//
// 流程：
//
//	┌─ 解析 JSON body
//	├─ 校验 long_url（scheme / 长度 / SSRF）
//	├─ 检查 Idempotency-Key（如有）→ 命中则返回已有记录
//	├─ generator.NextCode → 短码
//	├─ id.DecodeCode(code) → 原始 ID（DB 主键）
//	├─ links.Insert
//	│   ├─ 若 ErrIdempotencyConflict → race 取已有记录返回
//	│   └─ 否则 500
//	└─ 200/201 + JSON 响应
func (s *Server) handleCreateLink(ctx *fasthttp.RequestCtx) {
	// 1. 解析
	var req model.CreateLinkRequest
	body := ctx.PostBody() // zero-copy []byte，仅 handler 期间有效（json.Decoder 在本函数内消费完即可）
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(ctx, http.StatusBadRequest, ErrInvalidJSON, err.Error())
		return
	}

	// 2. 校验
	if err := ValidateLongURL(req.LongURL); err != nil {
		writeError(ctx, http.StatusBadRequest, ErrInvalidURL, err.Error())
		return
	}

	idemKey := strings.TrimSpace(string(ctx.Request.Header.Peek("Idempotency-Key")))

	// 长度防御：超过 MaxIdempotencyKeyLen 直接 400，不让攻击者用超长随机串
	// 撑大 DB 索引或制造无效 SELECT。
	if len(idemKey) > MaxIdempotencyKeyLen {
		writeError(ctx, http.StatusBadRequest, ErrIdemKeyTooLong, "Idempotency-Key too long")
		return
	}

	// 3. 幂等命中：返回已有记录（早退避免做无用功 + 浪费号段）
	if idemKey != "" {
		existing, err := s.links.GetByIdempotencyKey(ctx, idemKey)
		if err == nil {
			writeJSON(ctx, http.StatusOK, s.toResponse(existing))
			return
		}
		if !errors.Is(err, store.ErrLinkNotFound) {
			slog.Error("lookup by idem key", "err", err, "key", idemKey)
			writeError(ctx, http.StatusInternalServerError, ErrInternal, "lookup failed")
			return
		}
		// 未命中 → 继续创建
	}

	// 4. 生成短码
	code, err := s.generator.NextCode(ctx)
	if err != nil {
		slog.Error("generate code", "err", err)
		writeError(ctx, http.StatusInternalServerError, ErrCodeGeneration, "code generation failed")
		return
	}

	// 5. 反推原始 ID（用作 DB 主键）
	originalID, err := id.DecodeCode(code)
	if err != nil {
		// 不该发生：码是我们自己刚编码的
		slog.Error("decode just-encoded code", "err", err, "code", code)
		writeError(ctx, http.StatusInternalServerError, ErrInternal, "internal encoding error")
		return
	}

	// 6. 装配 + 写入
	link := &model.Link{
		ID:        originalID,
		Code:      code,
		LongURL:   req.LongURL,
		ExpiresAt: req.ExpiresAt,
	}
	if idemKey != "" {
		k := idemKey
		link.IdempotencyKey = &k
	}

	if err := s.links.Insert(ctx, link); err != nil {
		// idempotency race：两个请求同 key 同时 Insert，第二个撞 unique 约束。
		// 此时取已有记录返回（幂等语义保留）。
		if errors.Is(err, store.ErrIdempotencyConflict) && idemKey != "" {
			existing, lookupErr := s.links.GetByIdempotencyKey(ctx, idemKey)
			if lookupErr == nil {
				writeJSON(ctx, http.StatusOK, s.toResponse(existing))
				return
			}
			slog.Error("idem race lookup", "err", lookupErr, "key", idemKey)
		}

		// code 撞 unique：理论不该发生（号段保证唯一），但记录详细日志以便排查
		if errors.Is(err, store.ErrLinkCodeConflict) {
			slog.Error("code unique violation — possible segment bug", "code", code, "id", originalID)
		}

		writeError(ctx, http.StatusInternalServerError, ErrInternal, "create failed")
		return
	}

	// CreatedAt 由 DB 默认 now() 填，但 Insert 没 RETURNING——
	// 这里走简化路径：客户端拿到的 created_at 来自服务端记录的"近似时间"。
	// v0.2 优化：INSERT ... RETURNING created_at。
	writeJSON(ctx, http.StatusCreated, s.toResponse(link))
}

// toResponse 把 model.Link 装配成 API 响应 DTO，并补上 short_url。
func (s *Server) toResponse(l *model.Link) model.CreateLinkResponse {
	return model.CreateLinkResponse{
		Code:      l.Code,
		ShortURL:  s.cfg.BaseURL + "/" + l.Code,
		LongURL:   l.LongURL,
		CreatedAt: l.CreatedAt,
		ExpiresAt: l.ExpiresAt,
	}
}
