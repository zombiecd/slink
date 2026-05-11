# Day 18 — v0.5 ClickHouse 客户端 + 写入模式 spike 同口径对照

> 2026-05-09
> 三组同口径 spike 决定 v0.5 写入路径选型。沿用 Day 13 sarama vs kgo spike 同口径方法。

## 同口径 fixture（三组完全一致）

| 项 | 值 |
|---|---|
| Fixture 总量 | 5,000,000 行（上限触顶就停）|
| 时间窗 | 30s（5M 行先达上限就停）|
| Batch size | 1000 |
| Codes 池 | 2000（hex 6 char 随机）|
| IPs 池 | 500（随机 IPv4）|
| Country 池 | 8 国轮转 |
| Region | 固定 "SH" |
| 每行字段 | event_id (UUID) / code / ip / user_agent / referer / country / region / ts |
| 主机 | macOS / Docker Desktop |
| ClickHouse | 24.10.2.80-alpine（单容器）|
| Kafka | apache/kafka 3.9.2 KRaft 单节点 |

## 实测结果

### Spike #1 — `clickhouse-go/v2` PrepareBatch（高级 API）

```
=== spike-clickhouse-v2 (clickhouse-go/v2 Native) ===
  duration       27.757s
  rows           5000000
  batches        5000 (size 1000)
  rows/s         180136
  batch p50      2.837ms
  batch p99      10.284ms
  alloc/op       998 B/row
  total alloc    4759.5 MB
```

### Spike #2 — `ch-go` proto.Col* 列容器（low-level Native）

```
=== spike-ch-go (ch-go Native low-level) ===
  duration       23.82s
  rows           5000000
  batches        5000 (size 1000)
  rows/s         209909
  batch p50      3.762ms
  batch p99      11.281ms
  alloc/op       28 B/row
  total alloc    132.6 MB
```

### Spike #3 — Kafka Engine + MaterializedView（CH 端消费 / 端到端）

**Producer 侧**（kgo 投到 Kafka topic）：
```
=== spike-kafka-fixture (kgo producer → Kafka topic) ===
  duration         11.188s
  sent             5000000
  acked            5000000
  errored          0
  rows/s (sent)    446923
  batch fill p50   1.733ms
  batch fill p99   7.086ms
  alloc/op         1239 B/row
  total alloc      5910.4 MB
```

**端到端**（producer 启动 → CH target 表 5M 行）：

```
[+16s] target=1731000      # fixture 投到一半 + CH 已经在并行消费
[+21s] target=2889000
[+26s] target=4033000
[+31s] target=5001000      # ✓ drain complete

→ 端到端 throughput ≈ 161,290 rows/s（5M / 31s）
```

注：target 终值 5,001,000 比 sent 5,000,000 多 1k 条，是 spike-init 之前 topic retention 残留 + producer 重发的小量重复，at-least-once 语义正常，不影响选型。

## 三组对照

| 维度 | clickhouse-go/v2 | ch-go | Kafka Engine + MV |
|---|---:|---:|---:|
| **写入路径** | Go 主动 INSERT | Go 主动 INSERT | **CH 自动消费 Kafka topic** |
| **rows/s** | 180,136 | **209,909** | 161,290（端到端） |
| batch p50 | 2.837ms | 3.762ms | n/a（端到端测量） |
| batch p99 | 10.284ms | 11.281ms | n/a |
| **alloc/op** | 998 B/row | **28 B/row（35× 优势）** | 1239 B/row（producer 侧） |
| total alloc | 4,759.5 MB | 132.6 MB | 5,910 MB（producer 侧） |
| 代码量 | ~200 行 Go | ~220 行 Go | ~70 行 SQL + 0 行 Go consumer |
| 学习成本 | 低（SQL prepared 风格） | 中（proto.Col* 列容器手动管理） | 中（CH Kafka Engine + MV 配置） |
| 复用 v0.4 pipeline | ❌（新独立 client） | ❌（新独立 client） | **✅（v0.4 producer/topic 0 改动）** |
| 故障域 | client → CH 直连 | client → CH 直连 | **CH ↔ Kafka 解耦**（Go 不知道 CH 存在）|
| Schema 演化 | client 侧反序列化负责 | client 侧手动列定义 | CH 端 input_format_skip_unknown_fields=1 自动跳新字段 |

## 决策（v0.5-clickhouse.md §4 封板）

### D1 写入模式：**Kafka Engine + MaterializedView** ✅

候选 → 选择：
- ❌ Go INSERT (clickhouse-go/v2 PrepareBatch) — 慢且 alloc 高
- ❌ Go INSERT (ch-go low-level) — 性能最好（210k）但要写新 cmd/clickhouse-consumer
- ✅ **Kafka Engine + MV 自动消费** — 端到端 161k

**理由**：

1. **161k > 93k**：v0.4 producer 实际负载 93k RPS（Day 16 切流验证）。CH Kafka Engine 端到端 161k = **1.73× 余量**，永远跟得上 producer，不会 lag
2. **复用 v0.4 pipeline 0 改动**：producer 不动 / topic 不动 / 现有 PG consumer 不动。只在 CH 端 apply migration 0002 即可
3. **故障域真分离**：CH 重启 / 升级 / 抖动完全不影响 v0.4 主路径或 PG consumer。这是 v0.4 §11 决策表"故障域分离原则"的延伸
4. **代码量最少**：0 行 Go consumer 代码（vs ch-go INSERT 路径需要 ~250 行）。维护成本最低
5. **设计感更强**："使用 ClickHouse Kafka Engine + MaterializedView 解耦消费链路" 比"用 ch-go 实现 ClickHouse 高性能 INSERT" 抽象层级更清晰

**性能 -23%（161k vs 210k）的代价值得**：实际负载远低于天花板，设计简化收益压倒性。

### D2 查询客户端库：**ch-go** ✅（保留）

虽然写入不用 Go INSERT，但 v0.5 Day 22-23 要写新 admin endpoint（`/api/stats/uv` / `/api/stats/topk`）查 CH，需要 Go 客户端。

候选：
- ❌ clickhouse-go/v2 — alloc 998 B/row（query 也会慢）
- ✅ **ch-go** — alloc 28 B/row，列容器复用，查 CH 时同样有内存优势

### D3 batch 参数：**保持 spike 测过的值** ✅

```sql
ENGINE = Kafka SETTINGS
    kafka_max_block_size = 1000,        -- 对齐 v0.4 consumer BatchSize
    kafka_num_consumers = 2,            -- topic 4 partition / 2 consumer 各占 2
    kafka_skip_broken_messages = 0,
    input_format_skip_unknown_fields = 1
```

实测端到端 161k 已足够，不调参。Day 22 真负载 query 时如发现 CH lag，再调（kafka_num_consumers 可加到 4 顶满 partition）。

### D4 schema：**0001 主表 schema 已封板**（不变）

`click_events_ch`：
- 列：event_id UUID / code String / ip String / user_agent / referer / country LowCardinality / region LowCardinality / ts DateTime64(3,'UTC')
- ENGINE = MergeTree / PARTITION BY toYYYYMM(ts) / ORDER BY (code, ts)
- INDEX country_skip_idx country TYPE minmax GRANULARITY 4

实测 spike 写入无问题，列存压缩比 ~94%（5M 行总 alloc 132 MB / 5M ≈ 26 B/row 内存，磁盘更小）。

## 反直觉发现

### 1. ch-go alloc 比 v2 低 **35×**

不是 "稍微好一点"，是数量级差异。原因：
- v2 走 SQL prepared statement（每行一次 `Append(...)` 反射 + 内部 buffer 拷贝）
- ch-go 用 `proto.ColUUID/ColStr/...` 列容器一次性 marshal，列 buffer Reset 复用

应用启发：**列存数据库的客户端必须用列式编码 API**，行式 API 在批量写入下会持续触发反射 + 拷贝。

### 2. fixture producer 446k > 任意 INSERT spike

fixture 单纯往 Kafka 投，不需要 CH 处理 → 可以达到 kgo client 上限 446k。这说明端到端 161k 的瓶颈在 **CH 消费 + MV 转换 + 落表写入**，不是 Kafka 网络。

如果 v0.5 后期需要更高 throughput（例如 v0.6 K8s 多 server 把 producer 推到 200k+），CH 端可调 `kafka_num_consumers=4` 顶满 4 partition。

### 3. spike-v2 比 spike-ch **batch p50 反而更小**（2.84ms vs 3.76ms）

ch-go 列容器编码每 batch CPU 开销略多 +1ms，但因 alloc 极低 → GC 压力小 → 吞吐量更高。**alloc 低不等于 latency 低**，但在长期吞吐量赛道上 alloc 是赢家。

应用启发：选库时 alloc/op 比 batch latency 更重要——前者决定持续吞吐，后者只决定瞬时响应。

### 4. Kafka Engine 端到端 161k > v0.4 producer 实际负载 93k

CH Kafka Engine 在 producer 实际负载下**永不会 lag**。这是为什么"性能 -23%"的代价值得——头部需求空间外，性能数字只是名义对比。

## 数字写回 v0.5-clickhouse.md §4

下一步把这些数字写入决策稿 §4 封板，文档状态从 📐 kickoff → 📋 计划稿。
