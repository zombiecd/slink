-- ============================================================
-- 回滚 0004 — 重建影子表（schema 严格对齐 0003）
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

CREATE TABLE IF NOT EXISTS click_events_shadow_2026_05 PARTITION OF click_events_shadow
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE IF NOT EXISTS click_events_shadow_2026_06 PARTITION OF click_events_shadow
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE INDEX IF NOT EXISTS idx_click_events_shadow_code_ts
    ON click_events_shadow (code, ts DESC);
