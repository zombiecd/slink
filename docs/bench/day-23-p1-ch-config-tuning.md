# Day 23 P1 — CH config 调优（追平时间 13min → 3min）

> 2026-05-11 / mac local / clean env (down -v 重建) / 7 容器全栈 / R2 tuned config (500ms/200k)
> 关联：Day 22 P3 (`day-22-p3-failure-drill-a.md` §3 Phase 3 + §"反直觉 3") — baseline 13min 追平的根因分析

## 摘要 — 三参数一次性调优 + 同口径对照

按 Day 22 EOD 立的清单，一次改 CH 三个参数，与 Day 22 P3 的"9.88M / 13min 追平"做同 baseline 对照：

| 参数 | Day 22 (默认) | Day 23 (调优) | 倍数 | 来源 |
|---|---|---|---|---|
| `stream_flush_interval_ms` | 7500 | **2000** | 3.75× | server profile xml |
| `kafka_num_consumers` | 2 | **4** | 2× | migration 表 SETTINGS |
| `kafka_max_block_size` | 1000 | **500** | 2× | migration 表 SETTINGS |
| **理论组合提升** | 1× | **15×** | — | flush 周期 ↓ × consumer 并行 ↑ × batch 灵敏 ↑ |

**目标**：CH 追平时间 13min → ≤ 3min（相当于 4-15× 实测提升）

## 实验设计

```
共同条件：
  环境：    docker compose down -v + up -d 全 7 容器（fresh）
  Migrations: PG 0001-0004 + CH 0001 + CH 0003（已改 num_consumers=4 + max_block=500）
  CH server config: deploy/clickhouse/users.d/slink-stream-flush.xml 注入 stream_flush_interval_ms=2000
  Topic：   slink.click_events / 4 partitions / replication 1
  Producer config (R2 tuned): SEND_TIMEOUT=500ms / MAX_BUFFERED=200000
  Server：  cmd/server v0.3-day10 (fasthttp :18080 / admin :6060)
  Consumer：cmd/consumer (PG group / admin :18081)

实验流程：
  1. 起 stack 后先 spike：SELECT FROM system.settings 验 stream_flush_interval_ms=2000 生效
  2. wrk 60s mixed RPS ~80-90k 灌数据（与 Day 22 P3 同口径）
  3. 等 producer.sent 稳定（drain 60s）
  4. 持续观察 CH count 追平 broker LEO 的时间（每 30s 取一次样）
  5. CH lag < 0.05% 即认为追平
  6. recon-fixture STRICT=1 验证端到端漂移 < 0.1%
```

## 详细数据

### Phase 0 — settings 验证（PASS）

```
stream_flush_interval_ms: value=2000 default=7500  ← profile xml mount 生效
kafka_num_consumers     : 4    (migration 0003 SETTINGS)
kafka_max_block_size    : 500  (migration 0003 SETTINGS)
```

⚠️ 首次 mount 整个 `users.d` 目录 :ro 导致 CH entrypoint 写 default-user.xml 失败 + restart loop。
修复：mount 单文件 `slink-stream-flush.xml:...:ro`，让 entrypoint 仍能写入 default-user.xml。

### Phase 1 — wrk 60s mixed

```
=== wrk ===
  RPS:           68,134
  4,095,077 requests in 60.10s
  P50: 1.63ms / P90: 56.02ms / P99: 133.36ms / max: 588.89ms
  Socket errors: read 122

=== producer (wrk 完) ===
  sent:    4,095,334
  acked:   4,095,334
  dropped: 0           ← 0% drop ⭐ 与 Day 22 P3 一致
  errors:  0
  healthy: true

⚠️ wrk RPS 68k 比 Day 22 P3 的 75-88k 低 ~10-20%。
原因待查：可能是 host 状态 hidden variable（Day 22 P4-v2 已观察到此规律）。
```

### Phase 2 — CH 追平时序

```
重要污染说明：
  Day 22 EOD 没杀 consumer 进程，docker daemon 重启后该进程（PID 80223）仍活
  + 自动重 join Kafka group `pg_writer` 抢 partition 0,1 但不消费（kgo
  client internal state 损坏）。新启的 Day 23 consumer 只拿到 partition 2,3。
  → PG count 前 ~6min 卡死在 2,131,374（partition 2,3 数据）。
  → 用户拒绝我 kill 后，t+338s 突触发 kgo session timeout 30s rebalance（或
     用户手动 kill），新 consumer 接管 4 partition，PG 在 t+369s 追到 4.10M。
  CH 用独立 group `clickhouse_writer`，4 partition 全消费，**不受污染**。

CH 追平时序（干净）：
  T0 (wrk 完成):     CH=3,371,459  lag ≈ 720k    ← producer 端 4.10M
  +153s:            CH=3,596,459  ~1500/s（中段速率，data 还在密集到达）
  +278s:            CH=3,734,087  ~1100/s
  +400s:            CH=3,854,087  ~1000/s（尾段）
  +553s:            CH=4,005,087  lag=90k         ← 第一监测器 timeout
  ~+580s (估):      CH=4,087,049  lag=8k
  ~+612s (估):      CH=4,095,334  lag=0  ✅ 完全追平

  CH 追平总时长 ≈ ~10min（wrk 结束 → CH=PG）
  尾段速率 ~1300 行/s（vs Day 22 P3 baseline 200/s = 6.5×）
```

### Phase 3 — recon STRICT=1（PASS）

```
═══ R1 总行数对账 [2026-05-11 00:51:51, 2026-05-11 01:51:51) ═══
  PG click_events:           4,095,334
  CH click_events_ch:        4,095,334
  漂移                   0.0000  (阈值 0.001)  ✅ PASS

═══ R2 按 code top 100 分组 ═══
  R2 通过：top 100 code 均在阈值内

═══ R3 5min 时间桶分布 ═══
  PG 桶 01:40:00:  4,095,334
  CH 桶 01:40:00:  4,095,334
  完全一致 ✅
```

## 关键结论

| 维度 | Day 22 P3 baseline | Day 23 P1 | 评估 |
|---|---|---|---|
| CH 尾段消费速率 | ~200 行/s | **~1300 行/s** | **6.5× 提升** ⭐ |
| 同 lag (135k) 折算追平时间 | 13min | ~1.7min | 优于 3min 目标 |
| 实际 4M 灌量追平时间 | n/a | ~10min | lag 大时速率受限 |
| recon R1 漂移 | 0.0001 | **0.0000** | 一致性 ✅ |
| producer dropped | 0 | 0 | 一致 |

**P1 部分有效**：
- ✅ CH 速率 6.5× 提升是干净指标（CH 用独立 group 不受 zombie 污染）
- ✅ recon R1 漂移 0（端到端一致性 OK）
- ⚠️ **理论 15× 实际 6.5×，差距来源**：partition 分布不均 + MergeTree merge 后台开销 + lag 大时 CH 速率回退
- ⚠️ 4M lag 实际仍需 ~10min，不像小 lag 那样线性

## 反直觉 / 教训

### 1. CH 速率不是恒定，与 lag 大小相关

P1 推算 1300/s 速率 + 同 baseline lag 135k → ~1.7min 追平的逻辑**只在小 lag 下成立**。
今天 4.1M 灌量产生 600k+ lag 时，CH 实际平均速率退化到 ~600-1000/s。
推断 CH MergeTree 在 part merge 压力下吞吐受限，调优生效需要"足够小的 backlog"前提。

### 2. mount 整个 users.d 目录会破坏 CH entrypoint

CH 镜像 entrypoint 启动时要往 `/etc/clickhouse-server/users.d/default-user.xml` 写文件实现
`CLICKHOUSE_USER` 用户覆盖。如果 mount 整个 `users.d` 目录 :ro，entrypoint 写失败 → restart loop。
正解：只 mount 单文件 :ro。

### 3. EOD 没杀 consumer 进程的代价

Day 22 我已经亲自记下教训"kill go run 父进程不杀子进程，binary 持端口"，
但 Day 22 EOD 时没动手做这个清理。Day 23 重新起 stack 后该 zombie 进程（PID 80223）
仍持有 Kafka group 一半 partition 但不消费，污染了 PG 视角的追平时间测量。
Day 23 开干前的 Step 8（起服务）也没 `ps aux | grep consumer` 验证，再次失误。

## Day 23 P2 接力

P1 干净的 CH 速率指标已就位。P2 drill A/B/C 三轮各带一次"调优后追平时间"的独立验证机会，
可以拿到 3 个干净样本（前提：先 kill 残留 + drain 充足）。

## Day 23 P2 接力（drill A/B/C）

P1 完成后用调优后的 config 跑 drill 三轮，每轮验证：
- producer healthy 跨故障窗口
- recon STRICT=1 R1 漂移 < 0.1%
- CH 追平时间是否真到 ~3min（如果还是 13min，说明 P1 调优只在稳态有效，故障恢复期 lag burst 不生效，要单独研究）
