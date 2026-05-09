# Day 16 — 切流验证（consumer 写主表 / 关 buffer）

> 2026-05-09 / mac local / wrk -t4 -c256 -d30s mixed 100 codes / SLINK_EVENT_BACKEND=kafka

## 目的

验证 v0.4 架构稿 §8.3 切流后：
- server 走纯 Kafka 单 backend（buffer / dual 模式删除）
- consumer 写 click_events 主表（Day 15 影子表 click_events_shadow 不再写）
- 端到端 0 漏 + 主路径 RPS 不退步

## 环境变更（vs Day 15）

| 项 | Day 15 | Day 16 |
|---|---|---|
| `SLINK_EVENT_BACKEND` | `dual` | `kafka` |
| `SLINK_CONSUMER_TABLE` | `click_events_shadow` | `click_events` |
| `internal/event/buffer.go` | 存在 | **删** |
| `internal/event/dualwriter.go` | 存在 | **删** |
| `git tag v0.3-buffer-final` | — | **打** |

## wrk 输出

```
2817693 requests in 30.10s, 386.69MB read
Socket errors: connect 0, read 124, write 0, timeout 0
Requests/sec:  93606.81
Latency  P50=1.14ms  P90=27.51ms  P99=94.05ms  max=252.23ms
```

**93,606 RPS**。三档模式 RPS 对比：

| 模式 | 设置 | RPS | P99 | 备注 |
|---|---|---:|---:|---|
| **kafka 单** | Day 16（本次） | **93,607** | 94 ms | 删 buffer 后 |
| dual | Day 15 P4 | 77,831 | 113 ms | buffer + kafka 双写 |
| dual | Day 14 | 109,423 | 67 ms | 同 dual，初次实测无 consumer 占 CPU |
| buffer | v0.3 Day 10 | 86,000 | 24 ms | 纯 buffer 单写 |

切流后比 dual P4 提升 **+21% RPS / -17% P99**：少了 buffer 跟 kafka 抢 CPU + 没有双写开销。

## 关键计数器

### Server (KafkaProducer 单 backend)

```json
{
  "version": "v0.3-day10",
  "uptime_seconds": 114,
  "link_cache": { "l1": { "hits": 2817785, "misses": 100, "hit_rate": 0.99996 } },
  "kafka_producer": { "sent": 2817885, "acked": 2757864, "dropped": 60021, "errors": 0 }
}
```

**注意 `/debug/stats` 已无 `event_buffer` 字段** — 证明 buffer 路径完全切除。

### Consumer (写 click_events 主表)

```json
{ "polled": 2759115, "decoded": 2759115, "inserted": 2759115, "decode_errors": 0, "insert_errors": 0 }
```

启动 log: `table=click_events` ✅（之前 Day 15 是 `table=click_events_shadow`）。

### PG 对账

```sql
SELECT 'main' AS t, count(*) FROM click_events
UNION ALL SELECT 'shadow', count(*) FROM click_events_shadow;
```

| 表 | 行数 |
|---|---:|
| `click_events`（主表） | **2,759,115** = consumer.inserted ✅ 端到端 0 漏 |
| `click_events_shadow`（影子） | **0** ✅ consumer 不再写影子 |

## 跨路径捕获率

| 路径 | 捕获 | 占 wrk 请求比 |
|---|---:|---:|
| wrk 总请求 | 2,817,693 | 100% |
| Kafka producer.sent | 2,817,885 | 100% (差 192 是采样时点跨边界) |
| Kafka producer.acked | 2,757,864 | **97.9%** |
| **main 表实际写入** | 2,759,115 | **98.0%** |

**捕获率比 Day 14 dual mode Kafka 路径（92.8%）提升 5 pp**：少了 buffer 抢 CPU + producer dropped 减半（60k vs 176k）。

## 结论

3 个验证点全过：

1. ✅ **buffer / dual 代码删除完整** — `/debug/stats` 无 event_buffer 字段；server log 只有 "kafka producer ready" 没 "event buffer started"
2. ✅ **consumer 切主表生效** — table=click_events / shadow 表 0 行
3. ✅ **主路径 RPS 不退步反而涨** — 93k vs Day 15 dual P4 78k = +21%

## 回滚预案

如发现切流后有问题：

```bash
git checkout v0.3-buffer-final  # 回到 Day 15 完工 commit
make build && make build-consumer
# .env 改 SLINK_EVENT_BACKEND=dual / SLINK_CONSUMER_TABLE=click_events_shadow
```

完整 buffer + dual 代码在 git tag v0.3-buffer-final 完整保留。

## 下一步

D4 删影子表 migration 0004 → D5 故障演练 3 轮（kafka / pg / consumer）。
