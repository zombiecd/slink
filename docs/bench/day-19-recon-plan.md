# Day 19 — v0.5 fixture 端到端对账方案（设计稿）

> 2026-05-09 / 设计期产出 / 用于 Day 20-21 双 consumer 期与切流前最终对账
> 关联：v0.5-clickhouse.md §6（灰度迁移路径）/ Day 15 shadow-reconcile（v0.4 同方法论）

## 1. 目的

v0.5 在 v0.4 PG consumer 之外引入 ClickHouse Kafka Engine + MV 直消（决策稿 §4 封板）。Day 20-21 起 CH 后必须验证：

- CH `click_events_ch` 行数 ≈ PG `click_events` 行数（**漂移 < 0.1% 硬指标**）
- 双侧维度聚合一致（按 code / 按时间窗）
- 双侧没有跨路径数据丢失

本文是 Day 19 设计期产出，不实跑容器。Day 20-21 起 CH 后按本方案直接执行（脚本 `scripts/recon-fixture.sh` 配套）。

## 2. 数据流回顾

```
Producer (fasthttp + KafkaProducer)
    │
    └──► Kafka topic: slink.click_events (4 partition)
              │
              ├──► PG Consumer (group=slink.click_events.pg_writer)
              │        │
              │        └──► PG click_events (行存，审计源)
              │
              └──► CH Kafka Engine + MV (group=slink.click_events.clickhouse_writer)
                       │
                       └──► CH click_events_ch (列存，分析源)
```

**关键不变量**：两个 consumer group 独立 commit / 独立 lag / 独立故障域。同一条 Kafka record 被两侧各自消费一次，理论上 PG 落 N 行 / CH 也落 N 行。

## 3. 对账三对（顺序由严到松）

| # | 对账对象 | 通过条件 | 失败处置 |
|---|---|---|---|
| **R1** | 总行数 | `\|ch_count − pg_count\| / pg_count < 0.001` (0.1%) | 看 §7 失败排查 |
| **R2** | 按 code 分组行数 | 任一 code 漂移 < 0.5%（top 100 code 看分布）| 排查 partition 不均 / MV 投影错误 |
| **R3** | 按 5 分钟时间桶分组 | 桶级别差 < 1%（弱指标，看趋势 OK） | 排查 ts 字段精度 / 时区 |

R1 是硬指标，R2/R3 是辅助看分布。R1 不过整体不过。

## 4. 时间窗口对齐（核心难点）

### 4.1 为什么不能瞬时对账

- CH Kafka Engine 默认 `kafka_flush_interval_ms=7500`（即 7.5s 才 flush MV → target table）
- PG consumer batch=1000 / flush 100ms 触发任一
- producer 仍在写新数据，瞬时 `count` 双侧绝对漂移

### 4.2 静默窗口方案

选一段「producer 已停 / 或者已经过去足够久」的窗口对账：

```
[T_from, T_to)，T_to <= NOW() - 5min
```

5 分钟 cutoff 留给 CH MV flush + PG batch flush 完全消化。Day 16 实测 CH Kafka Engine drain 30s 内 lag 归零（见 §7），5min 是 10× 余量。

### 4.3 严格 cutoff 判定

跑对账前先校验：

```sql
-- PG 侧
SELECT max(ts) FROM click_events WHERE ts < now() - interval '5 minutes';
-- CH 侧
SELECT max(ts) FROM slink_analytics.click_events_ch WHERE ts < now() - interval 5 minute;
```

两侧 max(ts) 应都接近 `T_to`。如果 `max(ts_ch) < max(ts_pg) - 30s` → CH 仍在 lag，**等 30s 重测，不要硬跑**。

## 5. 对账查询（双侧 SQL）

字段命名两侧已对齐（migrations/0001_init.up.sql + migrations/clickhouse/0001_click_events_ch.up.sql）：`code String / ts DateTime64(3)` ↔ `code text / ts timestamptz`。

### 5.1 R1 总行数

```sql
-- PG
SELECT count(*) AS pg_count FROM click_events
 WHERE ts >= $1::timestamptz AND ts < $2::timestamptz;

-- CH
SELECT count() AS ch_count FROM slink_analytics.click_events_ch
 WHERE ts >= toDateTime64({from:String}, 3, 'UTC')
   AND ts <  toDateTime64({to:String}, 3, 'UTC');
```

### 5.2 R2 按 code 分组（top 100）

```sql
-- PG
SELECT code, count(*) AS c FROM click_events
 WHERE ts >= $1 AND ts < $2
 GROUP BY code ORDER BY c DESC LIMIT 100;

-- CH
SELECT code, count() AS c FROM slink_analytics.click_events_ch
 WHERE ts >= ... AND ts < ...
 GROUP BY code ORDER BY c DESC LIMIT 100;
```

对账方式：full outer join 双结果 on code，对每一行算 `|c_ch - c_pg| / c_pg`，超过 0.5% 列出。

### 5.3 R3 时间桶分布

```sql
-- PG
SELECT date_trunc('minute', ts) - (extract(minute FROM ts)::int % 5) * interval '1 minute' AS bucket,
       count(*) AS c
FROM click_events WHERE ts >= $1 AND ts < $2
GROUP BY bucket ORDER BY bucket;

-- CH
SELECT toStartOfFiveMinute(ts) AS bucket, count() AS c
FROM slink_analytics.click_events_ch
WHERE ts >= ... AND ts < ...
GROUP BY bucket ORDER BY bucket;
```

## 6. 漂移容忍 0.1% 来源拆解

理论上**两条 group 消费同一 topic，应当 0 漂移**。容忍 0.1% 是为吸收以下噪声：

| 来源 | 量级 | 解释 |
|---|---|---|
| CH Kafka Engine micro-batch 边界 | < 0.01% | flush 时机和 partition assign 抖动 |
| 时间戳精度差 | 0 | producer 给出的 `ts_ms` Int64 → 双侧都按 ms 存（PG timestamptz 6 位 / CH DateTime64(3)），同 ms 内一致 |
| at-least-once 重复 | 0 在静默窗口 | 双侧 commit 失败重读会产生重复，但 30s 内 5-min 静默窗口能消化；若发现 ch_count > pg_count > 0.1% 必查重复 |
| MV 处理时延 | 0 在 5min 窗口 | CH MV 默认 < 1s 触发，5min 远超 |

**如果实测漂移 > 0.1% 必查根因**，不要靠"调容忍度"掩盖。

## 7. 失败排查路径

### 7.1 R1 不过

| 现象 | 根因 | 处置 |
|---|---|---|
| `ch_count < pg_count` 漂移 > 0.1% | CH 仍 lag / MV 处理失败 | 看 `system.kafka_consumers` lag / 看 `system.errors` 表 |
| `ch_count > pg_count` 漂移 > 0.1% | CH 重复消费（重启时未提交 offset 重读） | 检查 CH 重启日志 / 或 PG consumer 漏（看 PG counter） |
| 双侧都低于预期 | producer 实际 sent 不到 | 看 producer.dropped / errors |

### 7.2 R2 不过

- 若仅个别 code 漂移大 → Kafka partition rebalance 期间 MV 处理特定 partition 卡住
- 若 head/tail code 都漂移 → schema 不一致（早期 Day 14 类似 wire 字段问题）

### 7.3 R3 不过

- 若漂移集中在某 5min 桶 → 该窗口期 CH 故障 / OOM / flush 卡顿
- 若漂移在窗口边缘 → cutoff 还不够，扩到 10min 窗口重测

## 8. CH 侧诊断查询（fail-safe）

```sql
-- CH consumer lag
SELECT topic, partition, current_offset, num_committed_messages
FROM system.kafka_consumers
WHERE database = 'slink_analytics';

-- CH MV 错误统计
SELECT name, value, last_error_time, last_error_message
FROM system.errors
WHERE name LIKE '%Kafka%' OR name LIKE '%MV%'
ORDER BY value DESC LIMIT 20;

-- CH 表自检
SELECT table, parts, rows, latest_event_time
FROM system.tables
WHERE database = 'slink_analytics' AND name = 'click_events_ch';
```

## 9. 接入 Day 20-21 流程

### 9.1 双 consumer 期（Day 20）

启 CH + apply 0001 main schema 后立即跑 producer baseline 60s wrk + 5min 静默 → 跑 recon-fixture.sh 一次。验收门槛：R1 < 0.1%。

### 9.2 持续监控（Day 20-21）

每 5 分钟自动跑一次 recon-fixture.sh（cron 或 tmux 轮询），连续 3 次过算稳定。任一次失败暂停 Day 21 进度，先排查。

### 9.3 切流前最终对账（Day 21 末 / Day 22 初）

producer 静默 / 1h 窗口，跑完整 R1+R2+R3。三者全过 + CH 查询 P99 < 200ms 才能进 Day 22 分析查询接入。

## 10. 与 Day 15 shadow-reconcile 的对照

| | Day 15（v0.4 影子期） | Day 19（v0.5 fixture 对账）|
|---|---|---|
| 对账维度 | PG main 表 vs PG shadow 表 | PG `click_events` vs CH `click_events_ch` |
| 对账主键 | path counter == 表行数 | 双侧表行数互比 |
| 工具 | wrk + SQL count | wrk + 跨库 SQL count |
| 容忍 | 0（必须严格相等）| < 0.1% 漂移（异步双 group）|
| 失败处置 | 查 path counter 错配 | 查 CH lag / MV 错误 |

**关键区别**：v0.4 是同 PG 双表（counter 锚定）/ v0.5 是跨库异步双消费（必须用静默窗口锚定）。

## 11. 后续 spec 演化

- **R0 实时对账**：Day 22-23 加 producer 计数器对账（kafka.go atomic counter sent vs CH count），更早发现漂移
- **跨数据中心扩展**：v0.6 K8s 多副本时 CH 单实例可能不够，本节按多副本场景再升一版
- **schema 演化兼容**：A3 wire schema_version 已留扩展点，CH MV `input_format_skip_unknown_fields=1` 容忍 producer 加字段（migrations/clickhouse/0002 §36）
