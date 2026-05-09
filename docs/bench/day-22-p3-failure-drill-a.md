# Day 22 P3 — failure-drill Round A（stop CH 30s 故障演练）

> 2026-05-09 night → 2026-05-10 凌晨 / mac local / clean env (down -v + 重新 up + apply migration) / 7 容器全栈 / R2 tuned config (500ms/200k)
> 关联：Day 19 plan (`day-19-failure-drill-plan.md`) / Day 22 P2 (`day-22-p2-recon.md`)

## 摘要 — Round A 实质通过 + 暴露 reset_baseline bug + CH 追平延迟

按 Day 19 plan Round A 节奏跑「stop slink-clickhouse 30s 故障演练」，同时跑 wrk 60s mixed 持续负载。

| 验证目标 | 结论 |
|---|---|
| **故障期 producer 不受影响** | ✅ sent=acked=9,882,687 / dropped=0（跨故障窗口 0 drop） |
| **故障期 PG consumer 持续落地** | ✅ PG 最终 9,882,687 = 100% 落地 |
| **CH 重启后从 last offset 继续消费** | ✅ 未出 broken parts / system.errors 干净 |
| **端到端 PG ↔ CH 漂移 < 0.1%** | ✅ R1 漂移 0.0001 < 0.001 PASS |
| **CH 追平耗时** | ⚠️ ~13 min（受默认 `stream_flush_interval_ms=7500` 限速）— 留 Day 23 调 CH config |

**意外收获**：第一次跑 `failure-drill-ch.sh A` 暴露 `reset_baseline` 函数与 Go consumer (kgo client) 不兼容 bug — 见 §4。

## 实验设计

```
共同条件：
  环境：    docker compose down -v + up -d 全 7 容器（fresh，第二次 down -v）
  Migrations: PG 0001-0004 + CH 0001 + CH 0003
  Topic：   slink.click_events / 4 partitions / replication 1
  Producer config (R2 tuned)：SEND_TIMEOUT=500ms / MAX_BUFFERED=200000
  Server：  cmd/server v0.3-day10 (fasthttp :18080 / admin :6060)
  Consumer：cmd/consumer (PG group / admin :18081)

实验流程（A 路径，绕开 failure-drill-ch.sh 的 reset_baseline bug）：
  1. wrk 60s mixed —— 建 baseline，灌 ~4.55M 条事件到 PG / CH
  2. 等 CH 部分追平 broker（~75s 后 lag ~55k）
  3. 故障窗口：背景 docker stop slink-clickhouse 30s + 前台 wrk 60s
     - t=0  wrk 启动
     - t+10s docker stop slink-clickhouse
     - t+41s docker start slink-clickhouse（实际 31s 故障，docker stop/start 自身延迟）
     - t+60s wrk 结束
  4. drain 60s 让 stack 恢复
  5. 等 CH 追平 broker LEO（~13 min 持续追）
  6. recon-fixture STRICT=1 验证 R1 < 0.1%
```

## 详细数据

### Phase 1 — Baseline wrk 60s

```
=== wrk ===
  Requests/sec: 75,791.88
  4,552,980 requests in 60.07s, 624.83MB read
  P50: 1.42ms / P99: 122.06ms
  Socket errors: read 118

=== producer (baseline 结束时) ===
  sent:    4,553,131
  acked:   4,553,131
  dropped: 0           ← 0% drop ⭐
  healthy: true

=== CH 端追平（部分） ===
  baseline wrk 结束时 CH 4,477,504 / PG 4,553,131 / lag 75,627
  CH 追速 ~200 行/s（受 stream_flush_interval_ms=7500 默认限速）
  90s 后 lag 降到 ~55k → 进入 P3 fault round
```

### Phase 2 — Fault Round（wrk 60s + CH stop 30s）

```
=== 故障窗口时序 ===
  23:44:28  wrk 60s 启动（baseline 结束 90s 后）
  23:44:38  [t+10s] docker stop slink-clickhouse
  23:45:09  [t+41s] docker start slink-clickhouse（31s 故障）
  23:45:28  wrk 60s 结束

=== wrk (fault round) ===
  Requests/sec: 88,680.05  ← 反而最高的一次 RPS（CH 故障不影响 redirect path）
  5,329,260 requests in 60.10s, 731.36MB read
  P50: 1.22ms / P99: 106.03ms
  Socket errors: read 121

=== producer (fault round 结束) ===
  sent:    9,882,687     ← 总累计（baseline 4.55M + fault 5.33M）
  acked:   9,882,687
  dropped: 0             ← 跨故障窗口 0 drop ⭐⭐
  errors:  0
  healthy: true

=== PG 端 (consumer 视角) ===
  最终 PG 表 count: 9,882,687（= producer sent，100% 落地）
  PG 在 23:45:51 时仍 8,298,247（消费滞后 wrk ~23s）
  PG 在 23:46:24 追到 9,882,687（消费完成，距 wrk 结束 56s）
  PG consumer 速率 ~70-100k/s 持续

=== CH 端 ===
  Phase 2 结束时 CH count 9,730,552（broker 9.88M lag 仍在）
  Phase 1 + Phase 2 桶分布：
    bucket 15:40:00 (UTC):  PG=CH=7,302,912 ✓ 完全追平
    bucket 15:45:00 (UTC):  PG=2,579,775 / CH=lag (后续追)
```

### Phase 3 — drain（CH 追平 broker）

```
=== CH 追平时序（整 13 min）===
  23:45:51  CH=9,730,552 / PG=9,882,687 / lag 1,432,305 (-14.5%)  ← drain 开始
  23:48:41  CH=9,762,054 / lag 120,633 (1.22%)                    ← +3 min
  23:53:47  CH=9,818,054 / lag 64,633 (0.65%)                     ← +8 min
  23:58:48  CH=9,872,054 / lag 10,633 (0.11%)                     ← +13 min
  23:59:20  CH=9,878,054 / lag 4,633 (0.047%)                     ← +13.5 min ✓ < 0.05%

=== CH 平均追平速率 ===
  ~190-200 行/s（恒定）
  原因：stream_flush_interval_ms=7500 默认 → 4 partitions × kafka_max_block_size=1000 / 7.5s ≈ 533/s 理论上限
  实测 200/s 偏低，可能 broker partition 分布不均 / CPU schedule 影响
```

### Phase 4 — recon STRICT=1

```
═══ R1 总行数对账 [2026-05-09 14:00:00, 2026-05-09 16:30:00 UTC) ═══
  PG click_events:           9,882,687
  CH click_events_ch:        9,882,054
  漂移                   0.0001  (阈值 0.001)
  R1 通过：漂移 0.0001 < 0.001 ⭐ PASS

═══ R2 按 code top 100 分组（漂移 > 0.005 列出）═══
  R2 通过：top 100 code 均在阈值内（最大 code-level drift 0.024%）

═══ R3 5min 时间桶分布 ═══
  PG 桶 15:40:00:  7,302,912    CH 桶 15:40:00:  7,302,912  ✓ 完全追平
  PG 桶 15:45:00:  2,579,775    CH 桶 15:45:00:  2,579,142  ✓ diff 633 (0.024%)

═══ 汇总 ═══
  R1 行数:   PASS
  R2 维度:   OK
  R3 时间:   OK
```

## 反直觉 / 教训

### 1. wrk RPS 在故障期间反而最高（88.7k vs baseline 75.8k）

故障期 wrk 完全不感知 CH 状态：fasthttp redirect path 只到 PG/Redis，不依赖 CH。CH 30s stop 反而**释放了 host 的部分 I/O 资源**（CH 读写关掉），让 server/consumer 跑得更顺。

启发：**任何"分析 RPS 跟故障的相关性"必须明确 RPS 是哪条链路**。事件管道的故障跟 redirect 链路 RPS 无关。这点 P2 已经讨论过（producer 健康度独立 RPS），P3 再次确认。

### 2. Producer 跨 9.88M 0 drop（不是 R2 config 稳定，是机器状态变化）

R2 config 三次跑 dropped 数据：

| 跑次 | 总 sent | dropped | 说明 |
|---|---:|---:|---|
| Day 21 R2 | 4,802,673 | 498,912 (10.39%) | 初次跑 R2 |
| Day 22 P2 | 5,113,350 | 43,979 (0.86%) | down -v 重建后 |
| **Day 22 P3** | **9,882,687** | **0 (0.00%)** | **down -v 第二次重建** |

同 R2 config / 同代码 / 同 wrk 命令 / 同 host，dropped 从 10% → 0.86% → 0%。**不是 R2 config 稳定**，而是 **host 状态 / docker daemon 状态 / kgo client warmup 等 hidden variable**。

启发：**单跑 R2 config 数据不能下"配置足够稳"结论**。P4 producer 配置最终化必须连跑 ≥3 次同 config 看 dropped 分布（已经在 P2 文档 §"P3/P4 接力计划调整"立 P4-v2 计划）。

### 3. CH 追平延迟 ~13 min（默认 config 限速）

drain 期 CH 追速恒定 ~200 行/s。lag 1.4M 行 → 7000s = 117min？实际只追了 13min 到 < 0.1%，意味着大部分 lag 在 wrk 期间已被 CH 消费（CH 在故障 30s 之外的 90s 内吸收了 ~1.3M），剩 ~135k 行需 13min。

CH config 默认值：
- `kafka_max_block_size = 1000`（一批 1000 行）
- `stream_flush_interval_ms = 7500`（每 7.5s flush 一次或满 block flush）
- `kafka_num_consumers = 1`（默认每 CH replica 一个 consumer）

理论值：4 partitions × 1000 / 7.5s ≈ 533 行/s/partition × 4 = 2.1k/s 上限
实测：200/s — 推测 partition 分布不均 + CH 单 consumer 串行处理 4 partition

**Day 23 CH config 调优清单**（推后做）：
- `kafka_max_block_size=500`（更敏锐 flush）
- `stream_flush_interval_ms=2000`（缩短 flush 周期）
- `kafka_num_consumers=4`（一个 consumer / partition）
- 期望追平时间从 13min 压到 ~3min

### 4. ⚠️ 暴露 reset_baseline bug — failure-drill-ch.sh 与 Go consumer 不兼容

**第一次尝试用 `./scripts/failure-drill-ch.sh A` 跑 P3 时**，脚本的 `reset_baseline` 函数做了 `kafka-topics.sh --delete + --create`，导致 Go consumer (kgo client) 进入永久错误状态：

| 端 | 行为 | 状态 |
|---|---|---|
| **CH Kafka Engine** | topic 重建后从 earliest 自动重订阅 | ✅ 正常消费 |
| **Go consumer (kgo)** | topic delete 把 client 打到错误状态，CREATE 后未重新 join group | ❌ polled/inserted 永久卡死，lag_records 永远增长 |

**bug 影响**：
- 第一轮 P3：PG 表 count = 0（consumer 没消费任何新 topic 数据）/ CH 表 count = 5,988,292（正常消费）/ 推 reset_baseline 触发的 stop CH 故障演练完全没法验证 PG 端

**绕开方案（A 路径，本文档实验）**：不用 `failure-drill-ch.sh A` 编排，手工 docker stop/start CH（不 delete topic）。

**根因 + 修复方案** → 见本日 commit 的 `scripts/failure-drill-ch.sh` 改动 + Day 23 cmd/consumer kgo 健壮性增强 issue。

## 与 Day 19 plan 偏离

Day 19 plan 写的是「Round A: stop CH 容器 15s（INJECT_AT=10s, RECOVER_AT=25s）」。本次实跑用 30s（INJECT=10s, RECOVER=40s），原因：30s 更接近真实运维场景（容器健康检查 grace period 一般 ≥ 30s，15s 故障可能在 healthy 探测前就恢复）。

## P4 重新定位 → P4-v2「R2 config 稳态分布验证」

按 P2 文档 §"P3/P4 接力计划调整"，P4 不再做 producer 配置进一步收紧（R2 已 0% drop），改为：

```
P4-v2 计划（推迟到 Day 23 — 今日时间盒已超）：
  连跑 3 次 wrk 60s mixed（同 R2 config / 同环境 / 中间不 down -v / 不重启 server）
  统计：dropped mean / stdev / max / 最坏 5%-tile
  通过条件：3 次 dropped 都 ≤ 5% 且 mean < 2%
```

---

**P3 收口完成（实质通过 + bug 暴露 + CH 调优清单立）。P4-v2 + reset_baseline bug 修复 + CH config 调优 → Day 23。**
