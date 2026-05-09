-- v0.5 Day 20 — Kafka Engine 主线（与 0002 spike 完全分离）
--
-- 本 migration 把 Day 18 spike 验证的 Kafka Engine + MV 模式正式上主线。
-- 设计点（v0.5-clickhouse.md §4 封板）：
--
-- 与 0002 spike 的区别（关键）：
--   0002 spike：3 张表（kafka 引擎表 + spike target 表 + MV）— 临时，独立目标表，spike 完跑 down
--   0003 main： 2 张表（kafka 引擎表 + MV → 写到 0001 主表 click_events_ch）— 正式，复用主表
--
-- 三表协作（实际是两张表 + 复用主表）：
--   1. click_events_ch_kafka_main   ENGINE = Kafka       ← 从 Kafka topic 拉 JSON
--   2. click_events_ch_main_mv      MaterializedView     ← SELECT FROM kafka 表 + 类型转换 → 主表 click_events_ch
--   3. click_events_ch (0001 已建)  ENGINE = MergeTree   ← 数据落地（v0.5 §3 模块图标的「列存分析源」）
--
-- 设置参数（v0.5 §4 封板）：
--   kafka_num_consumers = 2          → topic 4 partition / 2 consumer 各占 2
--   kafka_max_block_size = 1000      → 对齐 Go consumer BatchSize=1000
--   kafka_skip_broken_messages = 0   → 不跳坏消息（坏消息进 system.kafka_consumers）
--   input_format_skip_unknown_fields = 1 → 兼容 producer 加新字段（A3 schema_version V）
--
-- group：slink.click_events.clickhouse_writer（v0.5 §7 封板，与 PG group `pg_writer` 完全独立）
-- topic：slink.click_events（与 v0.4 producer 同 topic）
-- broker：kafka:9092（容器内 INTERNAL listener，与 v0.4 consumer 同口径）
--
-- 前置：先 apply 0001（主表 click_events_ch）。本 migration 独立于 0002 spike，可在 spike 表存在时也 apply 主线。
-- 回滚：见 0003.down.sql（顺序：MV → kafka 引擎表，主表 click_events_ch 不动）

-- ── Kafka 引擎表（仅做 stream 不持久化）──
CREATE TABLE IF NOT EXISTS slink_analytics.click_events_ch_kafka_main (
    event_id   String,                   -- Kafka 表用 String 兜底，MV 里 toUUID 转换
    code       String,
    ip         String,
    user_agent String,
    referer    String,
    country    String,                   -- Kafka 表不能用 LowCardinality（消费层），落地表才转
    region     String,
    ts_ms      Int64                     -- producer wire 形态（kafka.go clickEventWire.TsMs）
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'slink.click_events',
    kafka_group_name = 'slink.click_events.clickhouse_writer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_max_block_size = 1000,
    kafka_skip_broken_messages = 0,
    input_format_skip_unknown_fields = 1;

-- ── 物化视图（自动消费 + 类型转换 + 写主表 click_events_ch）──
-- 注意：TO 子句指向主表 click_events_ch（0001 已建），不再独立 target 表
CREATE MATERIALIZED VIEW IF NOT EXISTS slink_analytics.click_events_ch_main_mv
TO slink_analytics.click_events_ch AS
SELECT
    toUUID(event_id) AS event_id,
    code,
    ip,
    user_agent,
    referer,
    country,
    region,
    fromUnixTimestamp64Milli(ts_ms) AS ts
FROM slink_analytics.click_events_ch_kafka_main;
