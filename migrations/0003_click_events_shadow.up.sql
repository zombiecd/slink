-- ============================================================
-- slink v0.4 — Day 15 影子表
-- 设计文档: docs/architecture/v0.4-kafka.md §8.2
--
-- 灰度策略：consumer 写 click_events_shadow，不动 v0.3 主表 click_events。
-- 跑 7 天观察后（Day 16）再切流到主表 + 删本表。
--
-- schema 严格镜像 click_events（同主键 / 同列 / 同分区策略 / 同索引），
-- 让 ClickEventRepo.BatchInsert 可以传不同 table name 复用。
-- ============================================================

CREATE TABLE IF NOT EXISTS click_events_shadow (
    event_id    UUID NOT NULL,
    code        TEXT NOT NULL,
    ip          INET,
    user_agent  TEXT,
    referer     TEXT,
    country     TEXT,
    region      TEXT,
    ts          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_id, ts)
) PARTITION BY RANGE (ts);

-- 分区起步对齐 click_events（2026-05 / 2026-06）
CREATE TABLE IF NOT EXISTS click_events_shadow_2026_05 PARTITION OF click_events_shadow
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE IF NOT EXISTS click_events_shadow_2026_06 PARTITION OF click_events_shadow
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE INDEX IF NOT EXISTS idx_click_events_shadow_code_ts
    ON click_events_shadow (code, ts DESC);
