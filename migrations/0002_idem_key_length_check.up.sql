-- ============================================================
-- v0.3 hardening: idem_key 列长度兜底
--
-- 应用层在 internal/api/links.go 已有 MaxIdempotencyKeyLen=128 的
-- 入口校验，DB 这一层加 255 上限作为防御纵深：
--   - 防止绕过 fasthttp 直接连 DB 写入超长 key
--   - 防止未来其他 client（gRPC、批量导入工具）忘记应用层校验
-- 256 是 IETF draft-ietf-httpapi-idempotency-key-header 的建议上限附近。
-- ============================================================

ALTER TABLE links
    ADD CONSTRAINT links_idem_key_length_chk
    CHECK (idem_key IS NULL OR char_length(idem_key) <= 255);
