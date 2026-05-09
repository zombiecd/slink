# Day 21 — 主表 spike 复现（候选 A/B 排除）

> 2026-05-09 / mac local / clean env (down -v 删卷重建)
> 关联：Day 18 spike (`day-18-spike.md` §Spike #3) / Day 20 baseline (`day-20-baseline.md`)

## 摘要 — 候选 A/B 排除

按 Day 20 doc §5 P0 计划：用 Day 18 同 fixture（`cmd/spike-kafka-fixture`）投 5M 行到 Kafka topic，但**目标表换成主表 `click_events_ch`**（带 `INDEX country_skip_idx`，via 0003 Kafka Engine + MV）。

实测端到端 drain rate **155,463 rows/s**，与 Day 18 独立表 spike 161,290 在 noise 范围内（差 -3.6%）。

**结论**：

| 候选根因 | 状态 |
|---|---|
| A：主表 INDEX `country_skip_idx` 写入开销 | **排除** — INDEX 顶多 ~5% 开销，不是 Day 20 看到的 40× 拖慢 |
| B：MergeTree metadata 污染 | **排除** — 全新 volume + 全新主表跑出来仍 155k，不是 ~3.7k 量级 |
| C：wrk 持续负载特性 / producer drop / 配置问题 | **唯一可能** — 留待 Day 22 验证 |

## 同口径对照

| 项 | Day 18 spike #3 | Day 21 P0 |
|---|---|---|
| Fixture | `cmd/spike-kafka-fixture` 5M / batch 1000 | **同** |
| Producer | kgo MaxBuffered=200k / Linger=5ms / DeliveryTimeout=5s | **同** |
| Kafka | 3.9.2 KRaft 单节点 / 4 partition | **同** |
| ClickHouse | 24.10.2.80-alpine 单容器 | **同** |
| 目标表 ENGINE | MergeTree（独立 spike target，无 INDEX，无 MV）| **MergeTree（主表 click_events_ch，带 INDEX，经 MV 转换）** |
| 卷状态 | Day 18 当时 fresh | **fresh（Day 21 down -v 重建）** |
| Producer rate (sent) | 446,923 rows/s | 428,115 rows/s（-4%） |
| 端到端 wall-clock drain | 31s（5M / 31s = 161k）| **32.16s（5M / 32.16s = 155k）** |
| 端到端 ingest rate | **161,290 rows/s** | **155,463 rows/s** |
| 比例 | 1× | **0.964×（差 3.6%，noise）** |

## 实测原始数据

### Producer 侧（spike-kafka-fixture）

```
=== spike-kafka-fixture (kgo producer → Kafka topic) ===
  duration         11.679s
  sent             5000000
  acked            5000000
  errored          0
  rows/s (sent)    428115
  batch fill p50   1.806ms
  batch fill p99   7.937ms
  alloc/op         1235 B/row
  total alloc      5891.0 MB
```

acked=5M / errored=0 / dropped=0（5s deliveryTimeout + 200k buffer 在一次性灌库下零 drop）。

### CH 端 drain 时间线（poll CSV 关键节点）

| epoch_ms | count | 备注 |
|---|---:|---|
| 1778335078837 | 0 | poller 启动 |
| 1778335091662 | 0 | 最后一次 0（fixture ~ 此前 200ms 内启动）|
| **1778335092832** | **54000** | **首次非零 — CH MV 第一次 flush** |
| 1778335100365 | 1,032,000 | t+7.5s |
| 1778335109898 | 2,972,276 | t+17s |
| 1778335119248 | 4,995,914 | t+26s |
| **1778335124994** | **5,000,000** | **drain 完成** |

**drain 计算**：
- t0 = 首次非零 = 1778335092832 ms
- t1 = 首次到 5M = 1778335124994 ms
- elapsed = 32,162 ms = 32.162s
- rate = 5,000,000 / 32.162 = **155,463 rows/s**

### Kafka + CH 状态

```
SELECT count(), uniqExact(event_id) FROM click_events_ch
→ 5,000,000 / 5,000,000  （Day 18 当时是 5,001,000 / 残留 retention 重发）

CH parts (post-drain):
  202605_1_972_16        36.72 MiB   (consumer-0 partition 0/1 主块)
  202605_973_1934_16     36.36 MiB   (consumer-1 partition 2/3 主块)
  202605_1935_2235_15    11.16 MiB   (合并后小块)
  ...
  total ~94 MiB / 5M rows = 18.8 B/row 磁盘
```

CH 自动 merge 把零碎 part 合到 ~7 个，证明 background merge 正常工作（Day 20 当时没看 parts 状态，事后看可能合并节奏被 producer 流量打乱）。

## 方法论修正 — 跨日测试 down -v 必做

Day 20 异常的**第一层原因**不是数据，是**测试方法**：跨日测试没 down -v 删 volume，导致前一天 spike 残留 + Day 20 baseline 数据混在一起，CH parts 历史 `RemovePart=1630 / MergeParts=49` 已经污染 metadata。

Day 21 的纪律：
1. **跨日重测必 `docker compose down -v`** — 删所有 named volume，从 0 开始
2. **L2 操作（删 named volume）必须先列清单 + 用户审 + 再执行** — 走 operational-safety §2 流程
3. **spike vs baseline 对比必须同 fixture 同 producer 配置** — Day 20 用 v0.4 应用 producer（100ms timeout / 100k buffer）vs Day 18 用 spike fixture（5s timeout / 200k buffer），所以 dropped 47% vs 0%，本来就不是 apple-to-apple

这条 SOP 已升级到 memory `lib-selection-spike-sop.md` 第 12 节。

## 反直觉发现

### 1. INDEX `country_skip_idx` 几乎无影响

预期：minmax INDEX 每 4 granule 算一次 min/max，写入路径多一次 metadata 算法 → 5-15% 拖慢。
实际：Day 21 主表 155k vs Day 18 独立表 161k = -3.6%，几乎在 noise 范围。

启发：`TYPE minmax GRANULARITY 4` 极廉价，不是写入瓶颈候选。skipping index 应该首选 minmax/set，不是 bloom_filter（后者构建开销大几个数量级）。

### 2. Day 20 异常根因不是 CH，是 producer 路径

Day 20 看到「CH ingest 3.7k/s」就脑补成 CH 端问题（INDEX / metadata），实际 CH 端独立测一次能跑 155k。Day 20 真问题是：
- Producer drop 47% → broker LEO 增速 ~50k/s 而不是 100k/s
- 但 CH 也只消费到 ~3.7k/s — broker 50k/s × 13min = 39M，但 broker LEO 只到 2.99M，所以 broker 实际 50k/s 持续了 ~60s 然后停（wrk 只跑 60s）
- CH 在 wrk 停 13min 后还有 318k lag — 说明 CH 即使在 0 producer 流量下，消费 318k 也要 13min？不可能，CH spike 32s 能消 5M

可能的解释：Day 20 量度方式不对 — 测的是 wrk 跑完后某瞬间 CH count，并不是 drain rate 上限。Day 22 P1 必须用 monotonic poll（每秒记录 count）才能算真实 ingest rate。

### 3. 一次性 spike 11s 投 5M / wrk 60s 不到 5M

spike fixture 一秒投 ~430k 行（无应用层 overhead），wrk 60s × 100k RPS 也才到 6M target，实际 acked 只 2.67M。**wrk → 应用 → producer 链路** 才是 v0.5 真正限速点，不是 CH 写入。

启发：v0.5 后续优化方向应该是 **producer 配置**（buffer / timeout / linger）+ **应用层 hot path**（fasthttp + L1 cache 命中率），不是 CH。

## Day 22 接力（待办）

P1（producer 配置 + baseline 二跑）+ P2（recon-fixture + failure-drill A）按 Day 20 §5 计划继续，但有两条新约束：

1. **CH 端 drain rate 必须用 monotonic poller 测** — 不能再用「跑完后 t=N min 时 CH count」单点估算
2. **producer 配置应同时调两组对照**：(a) 100ms→500ms timeout (b) 100k→200k buffer，跑完看 dropped 比例 + acked 总量

预估 Day 22 ~3h（P1 1.5h / P2 1-1.5h）。
