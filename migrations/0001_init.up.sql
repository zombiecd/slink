-- ============================================================
-- slink v0.1 schema
-- 设计文档: docs/architecture.md
-- ============================================================

-- ── 号段表：发号器持久化点 ──────────────────────────────────────
-- 每个 biz_tag 对应一个独立号段空间（短链用 'link'，预留扩展）。
-- 服务启动/号段耗尽时，UPDATE max_id += step_size 一次取一段。
CREATE TABLE IF NOT EXISTS id_segment (
    biz_tag      TEXT PRIMARY KEY,
    max_id       BIGINT NOT NULL DEFAULT 0,
    step_size    BIGINT NOT NULL DEFAULT 1000,
    description  TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO id_segment (biz_tag, max_id, step_size, description)
VALUES ('link', 0, 1000, '短链 ID 号段')
ON CONFLICT (biz_tag) DO NOTHING;

-- ── 短链主表 ────────────────────────────────────────────────
-- code 是 Base62(id)，长 URL 实际大小可能 2KB+，用 TEXT 不限。
-- expires_at 为空 = 永不过期。
CREATE TABLE IF NOT EXISTS links (
    id              BIGINT PRIMARY KEY,
    code            TEXT NOT NULL UNIQUE,
    long_url        TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ,
    creator         TEXT,
    idem_key        TEXT,
    -- 反向唯一索引（同一长 URL 在同一 idem_key 下复用，避免重复创建）
    CONSTRAINT links_idem_unique UNIQUE (idem_key)
);

CREATE INDEX IF NOT EXISTS idx_links_created_at ON links (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_links_expires_at ON links (expires_at)
    WHERE expires_at IS NOT NULL;

-- ── 点击事件表（按月 RANGE 分区） ───────────────────────────
-- 每月一张子分区，老分区可 DROP 归档。
-- v0.1 用 INSERT 落库，v0.2 切 Kafka。
CREATE TABLE IF NOT EXISTS click_events (
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

-- 当前月 + 下个月的分区（生产应有定时任务自动建）
-- 这里硬编码 2026-05 / 2026-06 作为初始；后续 Day 7 加 partman 或自定义脚本
CREATE TABLE IF NOT EXISTS click_events_2026_05 PARTITION OF click_events
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE IF NOT EXISTS click_events_2026_06 PARTITION OF click_events
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE INDEX IF NOT EXISTS idx_click_events_code_ts
    ON click_events (code, ts DESC);
