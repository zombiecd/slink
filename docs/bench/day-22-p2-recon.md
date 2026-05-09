# Day 22 P2 — recon-fixture 端到端漂移验证（PG vs CH）

> 2026-05-09 evening / mac local / clean env (down -v 后 up + apply migration) / R2 tuned config（500ms / 200k）
> 关联：Day 21 P1 (`day-21-p1-baseline-tuning.md`) / Day 19 recon plan (`day-19-recon-plan.md`)

## 摘要 — recon 全 PASS

按 Day 21 收口计划 P2，clean env 跑一次 R2 tuned config wrk 60s mixed，等 CH 追平 PG 后跑 `recon-fixture.sh`。

| 维度 | 阈值 | 实测 | 结论 |
|---|---|---|---|
| **R1 总行数** | drift < 0.1% | PG=5,092,765 / CH=5,092,765 / **drift 0.0000** | ✅ PASS |
| **R2 按 code top 100** | drift < 0.5% | 全部在阈值内 | ✅ PASS |
| **R3 5min 时间桶** | 双侧一致 | 桶 `2026-05-09 15:05:00` PG=CH=5,092,765 | ✅ PASS |

**STRICT=1 模式通过**，端到端 PG ↔ Kafka ↔ CH 链路漂移 0.0000，远低于 0.1% 健康线。

## 实验设计

```
共同条件：
  环境：    docker compose down -v + up -d 全 7 容器（fresh）
  Migrations: PG 0001-0004 + CH 0001 + CH 0003（跳 0002 spike）
  Topic：   slink.click_events / 4 partitions / replication 1
  压测：    wrk -t4 -c256 -d60s mixed 100 codes
  Server：  cmd/server v0.3-day10 (fasthttp :18080 / admin :6060)
  Consumer：cmd/consumer (PG group / admin :18081)
  Producer config (R2 tuned)：
    SLINK_KAFKA_SEND_TIMEOUT=500ms
    SLINK_KAFKA_MAX_BUFFERED=200000
  Recon 窗口：FROM=2026-05-09 14:00:00 UTC / TO=16:00:00 UTC
```

## 详细数据

### wrk 60s mixed

```
4 threads and 256 connections
  Latency Distribution
    50%    1.32ms
    75%   11.38ms
    90%   47.46ms
    99%  117.85ms
  5,113,138 requests in 60.10s, 701.70MB read
  Socket errors: connect 0, read 120, write 0, timeout 0
  Requests/sec: 85,079.49
  Transfer/sec: 11.68MB
```

### Producer (server :6060/debug/stats)

```
sent:    5,113,350
acked:   5,069,371   ← 99.14%
dropped: 43,979      ← 0.86% ⭐ 远低于 P4 目标 5%
errors:  0
healthy: true
L1 hit_rate: 99.97%
```

### Consumer (PG :18081/debug/stats)

```
polled:    5,092,765
inserted:  5,092,765   ← 100% PG 落地
decode_errors: 0
insert_errors: 0
lag_records: 0
```

### CH 端

```
追平耗时：~75s（wrk 结束后 ~1.5min CH count = PG count）
最终 count：5,092,765（与 PG / consumer.polled 完全一致）
追平节奏：约 1k/s 尾速 → 解释见反直觉 §3
```

### recon-fixture STRICT=1 输出

```
═══ R1 总行数对账 [2026-05-09 14:00:00, 2026-05-09 16:00:00) ═══
  PG click_events:           5,092,765
  CH click_events_ch:        5,092,765
  漂移                   0.0000  (阈值 0.001)
  R1 通过：漂移 0.0000 < 0.001

═══ R2 按 code top 100 分组（漂移 > 0.005 列出）═══
  R2 通过：top 100 code 均在阈值内

═══ R3 5min 时间桶分布 ═══
  PG 桶: 2026-05-09 15:05:00+00 | 5092765
  CH 桶: 2026-05-09 15:05:00    | 5092765
  R3 已输出供人工核对

═══ 汇总 ═══
  R1 行数:   PASS
  R2 维度:   OK
  R3 时间:   OK
```

## Day 21 R2 vs Day 22 P2 反差观察

**同 R2 tuned config，不同跑**：

| metric | Day 21 R2 | **Day 22 P2** | Δ |
|---|---:|---:|---|
| wrk RPS | 79,925 | **85,079** | +6.5% |
| Producer sent | 4,802,673 | 5,113,350 | +6.5% |
| Producer acked | 4,303,761 | 5,069,371 | **+17.8%** |
| **Producer dropped** | **498,912 (10.39%)** | **43,979 (0.86%)** | **-91.2%** ⭐⭐ |
| Producer errored | 0 | 0 | 0 |
| Broker LEO total | ~4,360,000 | ~5,092,765 | +16.8% |
| Consumer lag (final) | 0 | 0 | 0 |

**关键反差**：同 producer config 跑两次，dropped 从 10.39% 跌到 0.86%（-91%）。这是 P4 producer 配置最终化的核心问题 — **R2 dropped 数字到底是真稳态还是瞬态偶发**。

### 候选解释（待 P4 进一步压测排除）

1. **机器负载状态差异** — Day 21 同日上午跑过 P0 spike + P1 R1 + 多次 down -v；P2 同日晚跑前已 down 8h+，host 状态更"凉"，无 docker daemon I/O 排队 / kgo client GC 残留
2. **kgo client cold-start 开销** — Day 21 R2 是 5/9 当日第三次 docker up，go run 进程之前已 GC 多次；P2 是当日新 server 进程，kgo TCP connection 池建立的瞬时 backpressure 不同
3. **macOS 系统瞬态** — `purgeable=2847 pages` 量级波动；TCP listen backlog / kernel mbuf 状态不同
4. **wrk 自身 latency 分布微差** — Day 21 R2 P99=125.5ms / P2 P99=117.9ms 差 6%，间接反映 host CPU schedule jitter

**单跑数据不能下"R2 配置足够稳"结论**，需要 P4 阶段连跑 3-5 次 R2 配置、统计 dropped 分布（mean / stdev / max）来确认稳态范围。

## 反直觉

### 1. CH 追平耗时 75s vs broker LEO 5.09M

wrk 60s 结束 → CH count 4,989,537（占 PG 98%）→ 接下来 75s 追完剩 ~100k → 平均尾速 ~1.3k/s。这跟 R2 burst rate 58k/s 差 45×。

机制：wrk 停后 producer 不再有新 Send，broker LEO 锁定 5.09M。CH Kafka Engine 此时受两个限速器制约：
- `kafka_max_block_size=1000`（默认）→ 一批 1000 行
- `stream_flush_interval_ms=7500`（默认）→ 7.5s 才 flush 一次
- 每 7.5s 拉一次 4 partition × 1000 = 4000 行 → ~533 行/s 平均

实测 ~1.3k/s 略高于理论值，可能 `min_block_size` 满 4000 后立即 flush，不等 7.5s。

启发：**CH "追平延迟"是 Kafka Engine 默认参数效应，不是性能瓶颈**。如果业务对延迟敏感（recon 窗口想缩短），调小 `kafka_max_block_size=500` + `stream_flush_interval_ms=2000` 可把追平时间压到 ~30s。这条留作 v0.5 后期 CH 调参备忘。

### 2. wrk RPS 85k > Day 21 R2 79k 的真因

不是产品代码差异（同 v0.3-day10 binary）、不是 producer config 差异（同 R2 500ms/200k）、不是 wrk 命令差异（同 -t4 -c256 -d60s）。

最可能：**wrk client 自身 cold-start jitter**。wrk 60s 不够长（20%+ 时间在暖 TCP connection / fasthttp accept loop），wrk RPS 5-7% 的 run-to-run 波动是已知现象。

启发：**任何 baseline 数字 ±5-10% 是 wrk 60s 测量噪声范围**。Day 21 R2 vs Day 22 P2 的 RPS 差异本身不是反差，**真正的反差是 dropped 10.39% → 0.86% 这种 12× 量级差**。

### 3. R1 严格 0.0000 vs broker side 仍有 dropped 0.86%

R1 验证 PG vs CH 漂移 0.0000，但 producer 端仍 dropped 44k 行。这看起来矛盾，但解释清楚：

- Producer dropped 44k 行 = **从未到达 broker** 的 click 事件（client 侧 buffer drop）
- Broker LEO 5.09M = **真正落 broker** 的事件
- Consumer polled 5.09M = 全量消费 broker
- PG inserted = CH inserted = 5.09M = R1 漂移 0

R1 衡量的是 **broker → PG / CH 双链路一致性**，跟 producer client 侧 drop 无关。R1 0.0000 + dropped 0.86% **没有矛盾，是不同层的 metric**：
- producer dropped → 上游事件丢失率（健康监控）
- recon R1 → 双链路 fan-out 一致性（数据一致性）

启发：**recon R1 通过 ≠ "事件没丢"**。事件可能在 producer client 端就 drop 了。Grafana dashboard 必须 producer dropped 和 recon drift **双轨监控**。

## P3 / P4 接力计划调整

按 Day 21 EOD 计划 P3 + P4 还要做：

### P3 — failure-drill A（stop CH 30s）

P2 已经把 stack 跑到稳态（PG = CH = 5.09M / consumer lag = 0）。直接接 P3：

```
1. baseline tap 10s（双侧 count 不变化 → 确认 idle）
2. docker stop slink-clickhouse 30s
3. 期间继续 wrk 30s mixed → 验证 PG 端持续落地、CH 端 0 入
4. docker start slink-clickhouse → drain 30s
5. 跑 recon-fixture STRICT=1 → 验证 R1 仍 < 0.1%
```

### P4 — producer 配置最终化 → **降级为可选**

R2 config dropped 0.86% **已远超 P4 原计划目标 5%**，P4 主线动机消失。但 D21→D22 反差暴露**单跑数据不可靠**问题，P4 重新定位为：

**「R2 配置稳态分布验证」** — 同配置连跑 3 次，看 dropped 是否稳定 < 1% 而非偶发 10%。

```
P4-v2 计划：
  连跑 3 次 wrk 60s mixed（中间不 down -v / 不重启 server）
  统计：dropped mean / stdev / max
  通过条件：3 次 dropped 都 ≤ 5% 且 mean < 2%
```

---

**P2 收口完成。P3/P4 接续 — Day 22 文档继续追加。**
