-- v0.5 Day 18 — click_events_ch (ClickHouse 列存分析源)
--
-- 与 PG click_events 字段一一对齐（migrations/0001_init.up.sql:43-53），但用列存优化：
--   - event_id UUID：CH 原生类型，等价 PG UUID
--   - code String：跳转主键，主查询模式按 code 聚合 → 入 ORDER BY 第一位
--   - ip String：保存原值（CH 也有 IPv4/IPv6 原生类型，但 wire 来源是 string，转一次双向开销不值）
--   - user_agent / referer：长字符串，普通 String 列
--   - country / region：枚举类高重复度 → LowCardinality(String) 字典编码减少存储
--   - ts DateTime64(3, 'UTC')：毫秒精度对齐 wire ts_ms (kafka.go:245)
--
-- 引擎与排序键决策（v0.5-clickhouse.md §7）：
--   ENGINE = MergeTree
--     - 起步最简；at-least-once + DB 主键去重已在 v0.4 producer/PG 侧做过，CH 不再去重
--     - ReplacingMergeTree / AggregatingMergeTree 留 v0.6+ 视需要再切（数据迁移有成本）
--   PARTITION BY toYYYYMM(ts)
--     - 同 PG 分区策略，对账 SQL 跨库可比对
--     - 月分区粒度足够：单月 ~3M~30M 条，CH 单分片处理无压力
--   ORDER BY (code, ts)
--     - 同 PG 索引 (code, ts DESC)，主查询模式按 code 聚合
--     - CH 的 ORDER BY 是 sparse primary index，按 code 前缀谓词查询是 O(log)
--     - ts 排第二位让"按 code 看时序"跳过额外排序
--
-- 索引（CH 在 ORDER BY 之外的二级索引用 INDEX 子句）：
--   - country_skip_idx：按 country 做 minmax skip index，加速地理维度下钻
--   - granularity 4：默认 8192 行一个 mark，4 个 mark 一个 skip index 颗粒
--
-- 不做（v0.5 红线）：
--   - 无 SAMPLE BY：还没到需要采样的体量
--   - 无 TTL：保留 90 天的策略留 v0.6 决定（用户行为审计可能要更长）
--   - 无 projection：等查询模式稳定后再加
CREATE TABLE IF NOT EXISTS slink_analytics.click_events_ch (
    event_id    UUID,
    code        String,
    ip          String,
    user_agent  String,
    referer     String,
    country     LowCardinality(String),
    region      LowCardinality(String),
    ts          DateTime64(3, 'UTC'),

    INDEX country_skip_idx country TYPE minmax GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (code, ts)
SETTINGS index_granularity = 8192;
