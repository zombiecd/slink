# Day 20 — v0.5 起步 baseline（含异常发现）

> 2026-05-10 / mac local / wrk -t4 -c256 -d60s mixed 100 codes / SLINK_EVENT_BACKEND=kafka
> 关联：v0.5-clickhouse.md §6（灰度路径）/ Day 16 v0.4 切流 baseline 93k

## 摘要 — 期望 vs 实际（不达预期）

按 Day 19 plan 跑 v0.5 起步 baseline，期望「主路径 RPS ≈ v0.4 93k / 跨路径 0.1% 漂移内」。**实测两项均不达**：

| 维度 | v0.4 切流后 (Day 16) | v0.5 起步 (Day 20) | Δ |
|---|---:|---:|---|
| 主路径 RPS | 93,607 | **83,327** | **-11%** ⚠️ |
| Producer dropped 比 | ~10-20% (Day 16 切流) | **47%** | ⚠️⚠️ |
| 跨路径 capture (CH vs wrk) | n/a (v0.4 无 CH) | **53.4%**（13min 仍在追）| 与 Day 18 spike 161k/s 差 40× |

主路径 RPS 退步可接受（CH consumer 抢 host CPU），但 **Producer dropped 47% + CH ingest 速率 ~3.7k/s** 是两个独立的严重问题，跑后续 recon-fixture / failure-drill 已无意义（baseline 已脏）。**按 meta-cognition §6 停手报告**。

## 1. wrk 数字（主路径）

```
60s mixed 100 codes (./scripts/bench/run.sh mixed @ BENCH_DURATION=60s)

5,006,445 requests in 60.08s, 687.06MB read
Socket errors: connect 0, read 115, write 0, timeout 0
RPS: 83,327
P50=1.35ms / P90=48.48ms / P99=119.81ms / max=313.28ms
```

- ✅ 0 timeout
- ⚠️ RPS 83k vs v0.4 baseline 93k = **-11%**
- ✅ P99 119ms 在可接受范围（v0.4 同条件 89ms，提高 30ms）

## 2. Producer / Consumer / CH 路径数据

### Server `/debug/stats`（admin :6060）

```json
{
  "kafka_producer": {
    "sent": 5006609,
    "acked": 2672833,
    "dropped": 2333776,
    "errors": 0,
    "healthy": true
  },
  "link_cache": {"l1": {"hit_rate": 0.9996}}
}
```

**dropped 47%**（v0.4 Day 16 切流后约 10-20%）— 100ms send timeout 在 100k+ RPS 下大量触发。

### PG Consumer `/debug/stats`（admin :18081）

```json
{
  "polled": 2993691,
  "decoded": 2993691,
  "inserted": 2779401,
  "decode_errors": 0,
  "insert_errors": 6,
  "unknown_version": 0,
  "lag_records": 0
}
```

**polled = 2.99M ≈ broker LEO 总和**（partition LEO 总 ≈ 2992k）。consumer 已**完整消费**所有 broker 接收的 record，0 lag。

`inserted 2.78M < polled 2.99M`：差 214k 是 batch 切片 + insert_errors 残留（Day 15 已经分析过的现象）。

### CH Kafka Engine state（system.kafka_consumers）

| consumer | partition | offset | LEO | lag |
|---|---|---|---|---|
| consumer-0 (parts 0,1) | [0,1] | [595060, 654833] | [595060, 654833] | **0**（已读完）|
| consumer-1 (parts 2,3) | [2,3] | [722610, 703390] | [903120, 840678] | **317,798** 仍在追 |

CH 总 read = 1,249,893 + 1,426,000 = **2,675,893** ≈ broker LEO 2,992,691

**13 分钟后 CH 主表 click_events_ch 仍只 2.67M 行，partition 2/3 仍有 ~318k lag。**

### Day 18 spike vs Day 20 baseline ingest 速率对照

| | Day 18 spike (target=独立表) | Day 20 baseline (target=主表) |
|---|---:|---:|
| Producer 模式 | spike-kafka-fixture 一次性投 5M | wrk 60s 持续 100k RPS |
| Producer rate | 446k/s | 83k RPS（含 47% drop） |
| broker LEO 受 | 5M | 2.99M |
| CH 端到端 wall-clock drain | 31s | **>13 min 仍未完** |
| **CH 端到端 ingest rate** | **161k rows/s** | **~3.7k rows/s** |
| 比例 | 1× | **0.023× = 慢 43×** |

## 3. 根因候选（待 Day 21 验证）

按差异度从高到低排：

### 候选 A：主表 `INDEX country_skip_idx` 写入开销（最可能）

主表 0001 click_events_ch 有 `INDEX country_skip_idx country TYPE minmax GRANULARITY 4`。
spike target 表（0002）没有 INDEX。
区别：主表每个 part 多构建一份 minmax 元数据。

**验证方法**：DROP click_events_ch + 重建无 INDEX 版本，重测同样 wrk 60s，对比 ingest 速率。

### 候选 B：MergeTree metadata / parts 历史污染

主表在 Day 18 期间已经有 wrk 写入数据（虽然 Day 20 truncate 了）。MergeTree 内部 part_log 历史 `RemovePart=1630`、`MergeParts=49`，可能影响后续写入路径。

**验证方法**：DROP click_events_ch + 重建（新表元数据干净），重测对比。

### 候选 C：Producer 100ms send timeout 配置不适配 100k+ RPS

`SLINK_KAFKA_SEND_TIMEOUT=100ms` + `SLINK_KAFKA_MAX_BUFFERED=100000`。在 100k RPS 持续负载下，buffer 满 + callback 慢导致 47% drop。

**验证方法**：调 timeout 200ms / max_buffered 200k，重跑 wrk 看 drop 比例。

候选 A/B 解释 CH ingest 慢；候选 C 解释 producer drop 高。两组问题独立。

## 4. 故障演练 + recon-fixture 暂停理由

按 Day 19 plan 段 5/6 应跑 recon + failure-drill A，但：

- **R1 < 0.1% 不可能过**：CH 13min 后还差 PG 318k record，跑 recon = 必 fail
- **failure-drill A 的 baseline 已脏**：Producer drop 47% / CH ingest 异常，叠加 stop CH 故障无法分离「故障引入的退步 vs baseline 退步」
- **元认知 §6**：发现架构性问题立刻停止后续，等用户决策

## 5. Day 21 计划（按候选根因迭代）

### 优先级 P0：CH ingest 速率根因（候选 A/B）

1. 跑 spike-kafka-fixture 直接投 5M record 到现有 click_events_ch_kafka_main（主线 kafka 表 + MV → 主表）
2. 测 CH wall-clock drain rate
3. 如 161k 量级 → 主表 INDEX/metadata 不是问题（候选 A/B 排除）→ 进 P1
4. 如 ~3.7k 量级 → DROP click_events_ch 重建无 INDEX 版本对照 → 锁定 A 还是 B

### 优先级 P1：Producer drop 47% 根因（候选 C）

1. 调 send timeout 200ms 重跑 60s wrk，看 drop 比例
2. 调 max_buffered 200k 重跑，对照
3. 取较好者作为 v0.5 producer 默认配置

### 优先级 P2：达成 baseline 后再跑 recon + failure-drill A

调通 CH ingest + producer drop 后，**重新执行 Day 20 plan 的段 4-6**（baseline + recon + Round A）。

预估 Day 21 ~3-4h（P0 1-1.5h / P1 1h / P2 1-1.5h）。

## 6. 反思 — 设计期 vs 实跑期的差距

Day 19 设计稿假设 CH ingest ≈ Day 18 spike 161k/s，从未质疑过这个数字会因「主表 INDEX」或「producer 持续负载 vs 一次性灌库」改变。**spike 数字是最佳条件下的上限，不是连续负载下的稳态**。

教训：spike → baseline 之间应该有**「同条件复现 spike 数字」的对账步骤**。下次 v0.6 引入新组件时，spike 后第一件事是用 spike 同方法复跑 baseline 看是否复现。
