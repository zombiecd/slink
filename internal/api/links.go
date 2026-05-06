package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/zombiecd/slink/internal/id"
	"github.com/zombiecd/slink/internal/model"
	"github.com/zombiecd/slink/internal/store"
)

// handleCreateLink 处理 POST /api/links。
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
func (s *Server) handleCreateLink(w http.ResponseWriter, r *http.Request) {
	// 1. 解析
	var req model.CreateLinkRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, MaxLongURLLength*2)) // 防超大 body DoS
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, ErrInvalidJSON, err.Error())
		return
	}

	// 2. 校验
	if err := ValidateLongURL(req.LongURL); err != nil {
		writeError(w, http.StatusBadRequest, ErrInvalidURL, err.Error())
		return
	}

	idemKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))

	// 3. 幂等命中：返回已有记录（早退避免做无用功 + 浪费号段）
	if idemKey != "" {
		existing, err := s.links.GetByIdempotencyKey(r.Context(), idemKey)
		if err == nil {
			writeJSON(w, http.StatusOK, s.toResponse(existing))
			return
		}
		if !errors.Is(err, store.ErrLinkNotFound) {
			slog.Error("lookup by idem key", "err", err, "key", idemKey)
			writeError(w, http.StatusInternalServerError, ErrInternal, "lookup failed")
			return
		}
		// 未命中 → 继续创建
	}

	// 4. 生成短码
	code, err := s.generator.NextCode(r.Context())
	if err != nil {
		slog.Error("generate code", "err", err)
		writeError(w, http.StatusInternalServerError, ErrCodeGeneration, "code generation failed")
		return
	}

	// 5. 反推原始 ID（用作 DB 主键）
	originalID, err := id.DecodeCode(code)
	if err != nil {
		// 不该发生：码是我们自己刚编码的
		slog.Error("decode just-encoded code", "err", err, "code", code)
		writeError(w, http.StatusInternalServerError, ErrInternal, "internal encoding error")
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

	if err := s.links.Insert(r.Context(), link); err != nil {
		// idempotency race：两个请求同 key 同时 Insert，第二个撞 unique 约束。
		// 此时取已有记录返回（幂等语义保留）。
		if errors.Is(err, store.ErrIdempotencyConflict) && idemKey != "" {
			existing, lookupErr := s.links.GetByIdempotencyKey(r.Context(), idemKey)
			if lookupErr == nil {
				writeJSON(w, http.StatusOK, s.toResponse(existing))
				return
			}
			slog.Error("idem race lookup", "err", lookupErr, "key", idemKey)
		}

		// code 撞 unique：理论不该发生（号段保证唯一），但记录详细日志以便排查
		if errors.Is(err, store.ErrLinkCodeConflict) {
			slog.Error("code unique violation — possible segment bug", "code", code, "id", originalID)
		}

		writeError(w, http.StatusInternalServerError, ErrInternal, "create failed")
		return
	}

	// CreatedAt 由 DB 默认 now() 填，但 Insert 没 RETURNING——
	// 这里走简化路径：客户端拿到的 created_at 来自服务端记录的"近似时间"。
	// v0.2 优化：INSERT ... RETURNING created_at。
	writeJSON(w, http.StatusCreated, s.toResponse(link))
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
