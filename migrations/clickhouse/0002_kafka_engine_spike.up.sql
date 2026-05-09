-- v0.5 Day 18 — 第三组 spike: Kafka Engine 直消（CH 自带 ENGINE=Kafka + MaterializedView）
--
-- 三表协作：
--   1. click_events_ch_kafka_spike   ENGINE = Kafka       ←  从 Kafka topic 消费 JSON
--   2. click_events_ch_kafka_target  ENGINE = MergeTree   ←  实际数据落地（spike 专表，独立于主表 click_events_ch）
--   3. click_events_ch_kafka_mv      MaterializedView     ←  自动 SELECT ... FROM kafka 表 INSERT INTO target
--
-- 与 cmd/spike-clickhouse-v2 / cmd/spike-ch-go 的对比定位：
--   - 后两者：Go 应用主动 INSERT，测的是 "客户端库 → CH" 链路 throughput
--   - 本组：CH 自己 pull Kafka，测的是 "CH 端消费链路" 端到端 throughput
--
-- topic / group：与 v0.4 producer 同 topic（slink.click_events），独立 group `clickhouse_writer`
-- broker 地址：容器内走 INTERNAL listener `kafka:9092`（与 v0.4 consumer 同口径）
-- spike 完后跑 0002.down 清三表，主表 click_events_ch 不动

-- ── Kafka 引擎表（仅做 stream 不持久化）──
CREATE TABLE IF NOT EXISTS slink_analytics.click_events_ch_kafka_spike (
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
    kafka_broker_list = 'kafka:9092',                                  -- container 内 INTERNAL listener
    kafka_topic_list = 'slink.click_events',                           -- v0.4 同 topic
    kafka_group_name = 'slink.click_events.clickhouse_writer',         -- v0.5 §7 已封板的 group 命名
    kafka_format = 'JSONEachRow',                                       -- 与 v0.4 producer JSON wire 一致
    kafka_num_consumers = 2,                                            -- topic 4 partition / 2 consumer 各占 2
    kafka_max_block_size = 1000,                                        -- batch 阈值，对齐 Go consumer BatchSize=1000
    kafka_skip_broken_messages = 0,                                     -- 不跳坏消息（坏消息进 _broken 字段）
    input_format_skip_unknown_fields = 1;                               -- 兼容 producer 加新字段（如 schema_version v）

-- ── 落地表（与主表 click_events_ch 同 schema 但独立 spike 专表）──
CREATE TABLE IF NOT EXISTS slink_analytics.click_events_ch_kafka_target (
    event_id    UUID,
    code        String,
    ip          String,
    user_agent  String,
    referer     String,
    country     LowCardinality(String),
    region      LowCardinality(String),
    ts          DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (code, ts)
SETTINGS index_granularity = 8192;

-- ── 物化视图（自动消费 + 类型转换 + 落表）──
CREATE MATERIALIZED VIEW IF NOT EXISTS slink_analytics.click_events_ch_kafka_mv
TO slink_analytics.click_events_ch_kafka_target AS
SELECT
    toUUID(event_id) AS event_id,                          -- String UUID → CH UUID
    code,
    ip,
    user_agent,
    referer,
    country,
    region,
    fromUnixTimestamp64Milli(ts_ms) AS ts                  -- int64 ms → DateTime64(3)
FROM slink_analytics.click_events_ch_kafka_spike;
