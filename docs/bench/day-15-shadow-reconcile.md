# Day 15 — 影子表端到端对账

> 2026-05-09 / mac local / wrk -t4 -c256 -d30s mixed 100 codes / SLINK_EVENT_BACKEND=dual

## 目的

验证 v0.4 灰度方案 §8.2 影子期：
- **Producer 路径**（KafkaProducer → topic → ClickEventConsumer → click_events_shadow）端到端不丢
- **Buffer 路径**（v0.3 老路径 → click_events 主表）保持原行为
- 两条路径相互独立、互不影响

## 环境

- mac M1 / 8 核 / docker compose（PG 16 + Kafka KRaft 3.9.2 单节点 / 4 partition）
- slink server: `bin/slink` (v0.3-day10)，dual mode
- slink consumer: `bin/slink-consumer` (v0.4-day15-consumer)，写 `click_events_shadow`
- wrk：`./scripts/bench/run.sh mixed`（同 Day 14 口径）
- 起测前两表均已 `TRUNCATE`，topic 已 `delete + recreate`（清 Day 13 spike 旧数据）

## 数字

### wrk 输出

```
2338651 requests in 30.05s, 320.94MB read
Socket errors: connect 0, read 121, write 0, timeout 0
Requests/sec:  77831.41
Latency  P50=1.42ms  P90=41.26ms  P99=113.50ms  max=305.71ms
```

> 与 Day 14 dual 数字 109k RPS / P99 67ms 偏低/偏高，归因：
> - 8 核 mac 同时跑 server + consumer + 6 个依赖容器，资源更紧
> - 抓 prometheus 5s 一次额外吃 CPU
> - 一次性测试，未做 N=3 取中位数（与 Day 14 同样为单次）
> - 这个数字不是 P4 的核心 — 核心是**对账闭环**

### Producer 计数器（server 侧）

| 计数 | 值 | 占 sent 比 |
|---|---:|---:|
| Kafka sent | 2,338,898 | 100% |
| Kafka acked | 2,161,995 | 92.4% |
| Kafka dropped | 176,903 | 7.6% |
| Kafka errors | 0 | 0% |
| Buffer enqueued | 1,494,995 | 63.9% |
| Buffer dropped | 843,903 | 36.1% |
| Buffer flushed | 1,494,995 | 63.9% |
| Buffer flushErr | 0 | 0% |

### Consumer 计数器

| 计数 | 值 |
|---|---:|
| Polled | 2,170,721 |
| Decoded | 2,170,721 |
| Inserted | 2,170,721 |
| Decode errors | 0 |
| Insert errors | 0 |

### PG 实际行数（对账核心）

```sql
SELECT 'click_events' AS t, count(*) FROM click_events
UNION ALL
SELECT 'click_events_shadow', count(*) FROM click_events_shadow;
```

| 表 | 行数 | 路径 counter | 差距 |
|---|---:|---:|---:|
| `click_events`（主表） | **1,494,995** | buffer.flushed = 1,494,995 | **0** ✅ |
| `click_events_shadow`（影子） | **2,170,721** | consumer.inserted = 2,170,721 | **0** ✅ |

## 结论

### 1. 两条路径端到端零丢失

`buffer.flushed == main 行数`，`consumer.inserted == shadow 行数`，差距均为 0。说明：
- Buffer flush → COPY FROM 主表 100% 写入
- Consumer poll → decode → COPY FROM 影子表 100% 写入

### 2. 跨路径数字不同是 v0.4 设计本意

|  | 捕获 | drop |
|---|---:|---:|
| Buffer 路径 | 64% (1,494,995 / 2,338,651) | 36% (cap 50k 装不下 78k RPS sustained) |
| Kafka 路径 | 92.8% (2,170,721 / 2,338,651) | 7.2% (100ms send timeout 边缘 drop) |

**Kafka 路径少 28.8 pp drop，这是 v0.4 路线证据**：决策稿 §1.1 预测 v0.3 在 100k 入速下 49% 稳态丢，本次 78k RPS 实测 36% drop，趋势吻合；切到纯 Kafka 后丢失率应进一步下降到 < 8%。

### 3. consumer.Polled - producer.Acked = 8,726 不一致解释

Consumer 实际写入 2,170,721 > producer Acked 2,161,995。差距 8,726 来自：
- producer 的 `Dropped` 是"100ms 内未拿到 ack"的 callback 计数，但 record 可能仍被 client buffer 继续投递并 broker 收到
- consumer 看到的是真实 broker 落盘的所有记录
- 也就是说真实成功率 ≈ 92.8%（consumer 视角）> producer "acked" 92.4%（producer 视角）

后续可以加一个 `slink_kafka_producer_late_acks_total`（callback 在 timeout 之后才回来）单独计数，但目前不必要。

## Bug 修复（P3 → P4 间发现）

### Bug B：`processFetches` 不切片

第一版 consumer 一次 PollFetches 拿回的 record 全攒一个 batch（实测见 batch_size=590k+），COPY FROM 单批爆 PG 内存。

**修法**：renamed `processFetches → decodeFetches`，run() 循环按 BatchSize=1000 切片再 flush。

### Bug A：Day 13 spike-kgo 旧数据 schema 不兼容

旧 spike-kgo 写的是 `{"ts": <unix-seconds>}`，current schema 是 `{"ts_ms": <unix-millis>}`。decodeClickEvent 设 TSMillis=0 → time.UnixMilli(0)=1970 → PG "no partition for row"。

**修法**：删 topic + 重建（这些是 Day 13 spike + Day 14 dual 测试残留，非真实数据）。

> **教训**：schema 演化要么在 producer 侧加版本字段，要么在 consumer decode 时校验 TS 是否在合理范围。v0.5 看是否值得加。

## 下一步

P5 故障演练 — `docker stop kafka` 中途看 producer dropped 飙升 + RPS 不退步 + 重连后 consumer 自动追上。
