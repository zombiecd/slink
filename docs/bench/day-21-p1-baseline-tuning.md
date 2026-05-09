# Day 21 P1 — clean env baseline 二跑 + producer 调参（候选 C 确证）

> 2026-05-09 / mac local / clean env (down -v 后 up + apply migration) / 7 容器全栈
> 关联：Day 21 P0 (`day-21-spike-replay.md`) / Day 20 baseline (`day-20-baseline.md`)

## 摘要 — 候选 C 完全确证

按 Day 21 morning plan + Day 20 §5 P1 计划，**clean env 跑两次 wrk 60s mixed 4×256 baseline**，对照 Day 20 同配置数字。

| 实验 | 环境 | producer config | RPS | dropped % | 结论 |
|---|---|---|---:|---:|---|
| **R1** | clean env (down -v) | stock (100ms / 100k) | **79,314** | **50.09%** | 复现 Day 20 量级 |
| Day 20 | polluted env | stock (100ms / 100k) | 83,327 | 47% | — |
| **R2** | clean env (down -v) | tuned (500ms / 200k) | **79,925** | **10.39%** | dropped -79% |

**两项关键结论**：

1. **污染不是凶手** — R1 (clean env + stock config) 与 Day 20 (polluted + stock config) 的 dropped 比例 **50% vs 47%** 几乎一致；wrk RPS **79k vs 83k** 差 5% 在 noise 范围。Day 20 异常的真正原因不在 volume 污染。

2. **Producer 配置就是凶手** — 仅把 `SEND_TIMEOUT` 100ms→500ms + `MAX_BUFFERED` 100k→200k，dropped 从 50% 降到 10%（**-79%**），acked 从 2.38M 飙到 4.30M（**+81%**），broker LEO 总量从 ~2.7M 升到 ~4.4M。**候选 C 确证**。

## 实验设计

```
共同条件（R1/R2 各跑一次）：
  环境：    docker compose down -v + up -d 全 7 容器（fresh）
  Migrations: PG 0001-0004 + CH 0001 + CH 0003
  Topic：   slink.click_events / 4 partitions / replication 1
  压测：    wrk -t4 -c256 -d60s mixed 100 codes
  Server：  cmd/server v0.3-day10 (fasthttp :18080 / admin :6060)
  Consumer：cmd/consumer (PG group / admin :18081)
  CH Poller：1s 间隔 SELECT count() FROM click_events_ch

R1 producer config (Day 20 stock)：
  SLINK_KAFKA_SEND_TIMEOUT=100ms
  SLINK_KAFKA_MAX_BUFFERED=100000

R2 producer config (tuned)：
  SLINK_KAFKA_SEND_TIMEOUT=500ms        # 5× stock
  SLINK_KAFKA_MAX_BUFFERED=200000       # 2× stock
```

## 详细数据

### R1 — stock config（Day 20 同配置）

```
=== wrk 60s mixed ===
  4,762,980 requests in 60.05s, 653.65MB read
  Socket errors: connect 0, read 117, write 0, timeout 0
  Latency Distribution
    50%    1.44ms
    75%   12.63ms
    90%   53.48ms
    99%  130.10ms
  Requests/sec:  79,314.20
  Transfer/sec:  10.88MB

=== producer (server :6060/debug/stats) ===
  sent:    4,763,173
  acked:   2,377,276
  dropped: 2,385,897   ← 50.09%
  errors:  0
  healthy: true

=== consumer (PG :18081/debug/stats) ===
  polled:   2,716,167   ← broker LEO total
  inserted: 2,415,731
  insert_errors: 5
  lag: 0

=== CH ingest (poller CSV) ===
  first nonzero: t=0 (count=28,744)
  burst end:     t=62s (count=2,599,266)
  burst rate:    (2,570,522 / 61.8s) = ~41,580 rows/s
  tail rate:     ~1,000 rows/s（5.78s 间隔 +1000，疑 stream_flush_interval）
  最终 plateau:  ~2,620,000（broker LEO 2.72M 持平）
```

### R2 — tuned config（500ms / 200k）

```
=== server log（确认 config 生效）===
  kafka producer ready brokers=[localhost:19092] topic=slink.click_events
    send_timeout=500ms max_buffered=200000

=== wrk 60s mixed ===
  4,802,514 requests in 60.09s, 659.07MB read
  Socket errors: connect 0, read 119, write 0, timeout 0
  Latency Distribution
    50%    1.48ms
    75%   11.75ms
    90%   49.75ms
    99%  125.52ms
  Requests/sec:  79,925.93
  Transfer/sec:  10.97MB

=== producer ===
  sent:    4,802,673
  acked:   4,303,761  ← +81% vs R1
  dropped: 498,912    ← 10.39%
  errors:  0
  healthy: true

=== consumer (final) ===
  polled:   4,362,908  ← broker LEO total +60% vs R1
  inserted: 4,362,908  ← 完整消费
  insert_errors: 0
  lag: 0

=== CH ingest ===
  first nonzero: t=0 (count=16,099)
  bulk burst:    t=0..72s 至 4,166,099（+58k/s 平均）
  端尾爆发：     t=60s..68s 一段从 2.6M → 4.04M（+1.42M / 7.3s = 194k/s）
                  → 推测：wrk 停后 producer kgo flush in-flight 200k buffer，broker 一次性吸收
  最终 plateau:  ~4,180,000+（持续微涨，CH 稍落后 broker 4.36M ~180k 量级）
```

## R1 vs R2 对照表

| metric | R1 stock | R2 tuned | Δ |
|---|---:|---:|---|
| wrk RPS | 79,314 | 79,925 | +0.8%（noise） |
| wrk P99 | 130.1ms | 125.5ms | -3.5% |
| Producer sent | 4,763,173 | 4,802,673 | +0.8% |
| **Producer acked** | **2,377,276** | **4,303,761** | **+81%** ⭐ |
| **Producer dropped** | **2,385,897** | **498,912** | **-79%** ⭐ |
| **Drop %** | **50.09%** | **10.39%** | **-39.70 pp** ⭐ |
| Producer errored | 0 | 0 | 0 |
| Broker LEO total | ~2.72M | ~4.36M | +60% |
| CH burst drain rate | ~42k/s | ~58k/s | +38% |
| Consumer lag (final) | 0 | 0 | 0 |

## 反直觉发现

### 1. wrk RPS 几乎不变（79k vs 80k）

producer drop 比例从 50% 降到 10%，但 wrk RPS 几乎没动。原因：

- **wrk 看到的 redirect 响应是 fasthttp 直接返回的 302**，不等 producer ack
- producer.Send 是 fire-and-forget（非 blocking），即使 buffer 满 drop，redirect handler 已 return 302
- 所以 producer 健康度只影响**事件落地率**（Kafka/PG/CH），不影响 redirect RPS

启发：v0.3 之后的 RPS 上限被 **fasthttp + L1 cache hit_rate** 决定（实测 99.97%），producer 仅是「下游事件管道」，drop 高 = 事件丢失而不是 redirect 卡顿。**producer 健康度必须独立监控**，不能从 RPS 判断。

### 2. R2 wrk 结束后的「端尾爆发」 194k/s

R2 的 CH count 在 t=60s..68s（wrk 刚停）期间从 2.6M 飙到 4.04M = +1.42M / 7.3s = **194,000 rows/s**。这接近 Day 18 spike 的 161k 量级。

机制推测：
- wrk 60s 结束 → fasthttp handler 不再调 producer.Send
- kgo client 持有 ~200k 未 flush 记录在 buffer
- 没有新 Send 竞争 producer goroutine 的 CPU
- kgo 在 ~1s 内把 200k buffer 全 flush 到 broker
- broker 接收速率瞬时拉到 ~200k/s
- CH Kafka Engine 此时也没竞争，按 161k spike 量级消费

启发：**producer drop 的根因是 200k buffer + Send latency 互相竞争 CPU**，不是 broker bandwidth 问题。如果能改用 ProducerLinger=10ms + 更大 buffer，可能进一步降 drop。

### 3. CH burst rate (R2 58k/s) < spike rate (P0 155k/s) 差 2.7×

R2 持续负载下 CH burst rate ~58k/s，远低于 P0 一次性 spike 投递的 155k/s。差 2.7×。

机制：spike 一次性投 5M 11s 内完成 → CH 消费时 broker 单 partition 始终有 1000+ 消息可拉，CH `kafka_max_block_size=1000` 总能填满 → 高吞吐。
持续负载下 broker 接收速率 ~50-70k/s ÷ 4 partitions ≈ 12-18k/s/partition → 1000 块填满需要 ~70ms，但 CH 还要等 `stream_flush_interval_ms` 默认 7.5s（取小值）→ 实际 effective rate 受限。

启发：v0.5 后期如果 producer 调好且持续 RPS 拉到 100k+，CH 端可考虑：
- 调小 `kafka_max_block_size=500`（更快 flush）
- 调小 `stream_flush_interval_ms=2000`
- 加 `kafka_num_consumers=4`（顶满 4 partition）

## Day 22 接力计划

### P3（可选 / 必跑）— recon-fixture R1 < 0.1%

R2 配置下 PG inserted 4,362,908 = CH 4,180,000+ 仍有 ~180k lag（CH 还在追）。等几分钟 CH 追平再跑：
```bash
./scripts/recon-fixture.sh
# 期望：R1（PG count vs CH count）漂移 < 0.1%
```

### P3 续 — failure-drill A（stop CH 30s）

按 Day 19 plan 跑 Round A：tap=10s baseline → stop CH 30s → start CH → drain 30s → 验证 PG/CH 双侧漂移仍 < 0.1%。

### P4（v0.5 主线）— producer 配置最终化

R2 dropped 10.39% 仍超「≤ 5% 健康线」。可继续调：
- SEND_TIMEOUT 500ms → 1s（5.5MB 网络足够 1s 内 ack）
- MAX_BUFFERED 200k → 500k（按 max_in_flight × batch_size）
- ProducerLinger 5ms → 10-20ms（吃 batching 红利）

目标：100k RPS 持续负载下 dropped ≤ 5%。这块跟 v0.5 决策稿 §11 「producer 健康监控」打包一起做。

## 方法论纪律 — Day 21 升级版

1. **跨日重测必 down -v**（Day 21 P0 已确证 SOP §12）
2. **throughput 测速必须 monotonic poller**（不能单点估算 — Day 20 错过这步导致 "3.7k" 错读 → 真实是 ~42k 量级）
3. **异常差 1 个数量级以上 → 先怀疑测试方法/配置，不是实现细节**（Day 20 把矛头指向 INDEX/metadata 是 confirmation bias）
4. **producer 健康度独立于 RPS**（wrk RPS 不能反映事件管道健康，必须看 server :6060/debug/stats `kafka_producer.dropped`）
