# Day 19 — v0.5 ClickHouse 故障演练编排（设计稿）

> 2026-05-09 / 设计期产出 / 用于 Day 20-21 双 consumer 期实跑
> 关联：v0.5-clickhouse.md §6（灰度迁移路径）/ Day 16 fault-3rounds（v0.4 同方法论）

## 1. 目的

v0.5 引入 CH 后故障域从 v0.4 的 3（Kafka / PG / Server）扩到 4（Kafka / PG / CH / Server）。本演练验证：

1. **主路径 RPS 不退步** — Producer 完全感知不到 CH 故障（架构稿 §1.2 第一红线）
2. **PG 路径 0 漏** — `pg_writer` consumer group 不被 `clickhouse_writer` group 牵连
3. **CH 故障恢复后 60s drain 自愈** — backlog 由 Kafka 兜住，CH 重启后 lag 归零

本文是 Day 19 设计期产出，配套 `scripts/failure-drill-ch.sh`。Day 20-21 起 CH 后实跑，数字落 `docs/bench/day-20-failure-drill-ch.md`。

## 2. 测试设计

### 2.1 共性框架（沿用 v0.4 D5）

每轮独立 baseline：
- TRUNCATE PG `click_events` + TRUNCATE CH `click_events_ch` + delete/recreate kafka topic（清前轮 offset）
- 起 fresh server + PG consumer + CH（apply 0001 / detach-attach 重置 kafka engine）
- wrk 60s mixed 100 codes（同 Day 16 口径）
- t=10s 注入故障
- t=25s 恢复
- t=60s wrk 结束 + 60s drain CH lag

### 2.2 三轮场景

| Round | 故障模式 | 注入命令 | 恢复命令 | 预期主路径影响 |
|---|---|---|---|---|
| **A** | CH 容器整停 | `docker stop slink-clickhouse` | `docker start slink-clickhouse` | 0 退步（producer 不依赖 CH）|
| **B** | CH 慢消费 | `docker pause slink-clickhouse` | `docker unpause slink-clickhouse` | 0 退步（Kafka 缓冲 backlog）|
| **C** | CH MV 处理失败 | `DETACH TABLE click_events_ch_mv` | `ATTACH TABLE click_events_ch_mv` | 0 退步（kafka engine 仍消费但 target 不写）|

**为什么这三轮**：
- A = 整个 CH 实例挂（最坏情况，OOM / 节点崩）
- B = CH 资源紧张但活着（更常见，比如 backup 抢 IO / 长查询拖死消费）
- C = MV 单点失败（schema 漂移 / 子查询语法改坏）— 应用层故障

跟 v0.4 D5 (kafka/pg/consumer) 三轮的对比：v0.4 已经验证 kafka/pg 故障 → 主路径不退步，本演练只测 CH 这层。

## 3. 三个验证点（硬指标）

### 3.1 主路径 RPS 不退步

baseline RPS（无故障）93k（v0.4 Day 16 切流后实测）。本演练每轮：
- ✅ RPS 退步 < 5%（Round A/B/C 全过）
- ✅ wrk timeout = 0（任何 16M reqs 不能有一个）

### 3.2 PG 路径 0 漏 + CH 路径恢复后 0 漏

drain 60s 后跑 recon-fixture.sh（见 `day-19-recon-plan.md`）：
- PG: 行数 = consumer.inserted（Day 15 同方法）
- CH: 行数 = PG 行数 ± 0.1%
- 故障期间 CH lag 涨幅 ≈ producer.sent × 故障时长（kafka_max_block_size=1000 单 partition 视角）

### 3.3 CH 自愈

恢复后采集时序，CH lag 归零时间：
- Round A（容器重启）：60s 内归零（重启 + 重连 + 追 1.5M backlog）
- Round B（pause/unpause）：30s 内归零（无重启，直接续）
- Round C（DETACH/ATTACH MV）：30s 内归零（MV 重建后 kafka engine 已有 buffered，重新流入）

## 4. 数据采集（每秒一次）

写 `/tmp/slink-day19/round-{A,B,C}.csv`，列定义：

```
ts_unix,
producer_sent, producer_acked, producer_dropped, producer_errors,
pg_inserted, pg_insert_err,
ch_kafka_consumer_lag,    -- system.kafka_consumers
ch_target_rows,           -- count() FROM click_events_ch
ch_mv_errors,             -- system.errors WHERE name LIKE '%MV%'
phase                     -- baseline / fault_injected / recovered / draining
```

### 4.1 CH 侧采集 SQL（每秒）

```sql
-- lag
SELECT sum(num_committed_messages) AS lag
FROM system.kafka_consumers
WHERE database = 'slink_analytics' AND table LIKE '%kafka%';

-- target 行数
SELECT count() FROM slink_analytics.click_events_ch;

-- MV 错误（增量）
SELECT value FROM system.errors WHERE name = 'StorageKafkaMVError';
```

### 4.2 producer / PG counter 采集

复用 v0.4 已有：`/debug/stats` (server :18080) + `/debug/stats` (consumer :18081)。

## 5. 编排时序图

```
t=0s     ┌─ wrk start (60s mixed @ 256 conn)
         │
t=10s    │  ┌─ inject fault (Round A: docker stop / B: pause / C: DETACH MV)
         │  │
t=10-25s │  │  fault window (15s)
         │  │
t=25s    │  └─ recover fault
         │
t=60s    └─ wrk end
         │
t=60-120s   drain phase（CH 追 backlog）
         │
t=120s   recon-fixture.sh 跑一次（验证 R1 R2 R3）
```

## 6. 预期数据形态（提前写预期 = 反向防止 cargo cult）

### 6.1 Round A — stop CH 容器 15s

| 时刻 | producer_sent | producer_acked | pg_inserted | ch_target_rows | ch_lag |
|---|---:|---:|---:|---:|---:|
| t=10s 停前 | 1.0M | 0.99M | 500k | 500k | ~0 |
| t=15s 故障期 | 1.5M | 1.5M | 750k | 500k 卡 | 250k ↑ |
| t=25s 恢复 | 2.5M | 2.5M | 1.25M | 500k | **1.5M ↑** |
| t=30s | 3.0M | 3.0M | 1.5M | 800k ↑ | 1.2M ↓ |
| t=60s wrk 结束 | 5.6M | 5.6M | 2.8M | 4.0M | 1.6M |
| t=120s drain 结束 | 5.6M | 5.6M | **5.6M ✅** | **5.6M ± 0.1% ✅** | **0 ✅** |

预期主路径 RPS 退步 < 2%（CH 故障对 producer 0 直接影响，仅竞争 host CPU 微小）。

### 6.2 Round B — pause CH 15s

数据形态类似 Round A，但 CH 无重启 → drain 更快（30s 内 lag 归零）。RPS 退步预期 < 1%。

### 6.3 Round C — DETACH MV 15s

最微妙：CH kafka engine 仍在消费（继续 commit offset），MV 不投影到 target。

| 时刻 | ch_target_rows | ch_lag |
|---|---:|---:|
| t=10s 停前 | 500k | ~0 |
| t=15s DETACH 后 | 500k 卡 | 0 ← kafka 消费正常 |
| t=25s ATTACH 后 | 500k → ? | 0 |

**关键问题**：DETACH 期间 kafka engine 已 commit 的 offset 对应数据，是否补回 target？

- 标准答案：**不会**。Kafka Engine + MV 是 fire-and-forget 流式，commit 后没有重放机制
- 实操：需要在 ATTACH 后手动从 PG `click_events` 把 DETACH 期间的数据 INSERT 回 CH
- **本测试预期 R1 不过**，证明这是 CH MV 的真实风险点 → Day 22-23 须加补偿任务

如果 Round C 实测 R1 不过，结论不是「演练失败」而是「发现 MV 单点风险，必加补偿」。这是设计期就要预期的。

## 7. 失败排查（同 Day 16 套路）

| 现象 | 可能根因 | 处置 |
|---|---|---|
| RPS 退步 > 5% | producer 100ms send timeout 在 CH 故障期被错误触发？（不该）| 看 producer 错误日志，确认是否真在 producer→kafka 路径 |
| timeout > 0 | handler 卡住 | 同上，重大 bug，回归 |
| PG 漏 | PG consumer 也挂了（不该独立故障）| 看 PG consumer 日志，确认 group 是否被串了 |
| CH drain 不完 | CH 重启后未正确加载 kafka engine | 看 CH 启动日志，看 system.errors |

## 8. dry-run 清单（Day 15 教训：编排脚本投正式前必须 dry-run 90s）

- [ ] wrk 命令在当前栈跑 60s 有数（不是 0 RPS）
- [ ] `docker stop slink-clickhouse && docker start slink-clickhouse` 单独成功
- [ ] `docker pause slink-clickhouse && docker unpause slink-clickhouse` 单独成功
- [ ] CH `DETACH TABLE / ATTACH TABLE` MV 语法正确
- [ ] CSV 采集脚本输出 1s 一行非 0 数据
- [ ] recon-fixture.sh 在 baseline 数据上跑通

预算 30 分钟 dry-run，比 60s 空跑省 15 分钟（Day 15 wrk --noproxy 教训）。

## 9. 与 v0.4 D5（Day 16）的对照

| | v0.4 D5（kafka/pg/consumer）| v0.5 D5（CH 三轮）|
|---|---|---|
| 故障域数 | 3 | 4 |
| 主路径退步硬指标 | < 5% | < 5% |
| 端到端漏数 | 0 | < 0.1%（CH MV 异步特性）|
| 自愈窗口 | 30s drain | 60s drain（CH 重启 + backlog 吃完更慢）|
| 关键发现 | PG/Consumer 故障 RPS 反涨（L1 + 让出 CPU）| 预期 RPS 退步 < 2%（CH 对 producer 完全解耦）|

v0.4 验证了「PG/Consumer 故障主路径不退步」，v0.5 把 CH 加进来后**不应该**回退这个性质。任何退步都说明 CH 引入了不该有的耦合（如共享 host 资源不限制 / docker 网络抖动等）。

## 10. 接入后续

### 10.1 Day 20-21 实跑

- 起 CH + apply 0001 → wrk baseline 60s（无故障）拿 baseline RPS
- 跑三轮（A B C），数字落 `docs/bench/day-20-failure-drill-ch.md`
- 跨轮跑 recon-fixture.sh 验证 R1/R2/R3
- 三个验证点全过才算 Day 20 完工

### 10.2 Day 22-23 补偿任务（视 Round C 结果）

如果 Round C 验证 MV detach 期间数据不补回 → Day 22-23 加：
- 离线补偿任务：从 PG `click_events` 按时间窗口 INSERT 到 CH（worker 或 cron）
- 或：用 `INSERT ... SELECT FROM remote('postgres', ...)` 直接跨库捞

具体方案待 Round C 实测决定。本节预留扩展点。
