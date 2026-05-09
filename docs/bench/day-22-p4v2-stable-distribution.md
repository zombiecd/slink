# Day 22 P4-v2 — R2 config 稳态 dropped 分布验证

> 2026-05-10 凌晨 / mac local / Day 22 同一 stack 复用（不 down -v / 不重启 server / 同 R2 config 500ms+200k）
> 关联：Day 22 P2 (`day-22-p2-recon.md`) §"P3/P4 接力计划调整" / Day 22 P3 (`day-22-p3-failure-drill-a.md`) §3 反差观察

## 摘要 — PASS + 暴露 cold-start 集中现象

按 P2 文档立的 P4-v2 计划「连跑 3 次 R2 config 看 dropped mean / stdev / max」，**3 轮全跑通**：

| 通过条件 | 实测 | 状态 |
|---|---:|---|
| 3 轮 dropped 都 ≤ 5% | max 3.38% | ✅ PASS |
| dropped mean < 2% | 1.13% | ✅ PASS |

**意外发现**：dropped **不是均匀分布在 3 轮**，而是 **R1 集中（3.38%）/ R2+R3 完全 0**。这指向 producer cold-start / warmup 期是 drop 主要发生点，不是稳态问题。

## 实验设计

```
共同条件：
  环境：    Day 22 P3 同 stack（已跑 9.88M 累计，server uptime ~8h+）
  Migrations: 已 apply（PG 0001-0004 / CH 0001 + 0003）
  Producer config (R2 tuned)：SEND_TIMEOUT=500ms / MAX_BUFFERED=200000
  Server：  cmd/server v0.3-day10（**不重启**）
  Consumer：cmd/consumer（**不重启**）

实验流程（3 轮）：
  t=0    Round 1: wrk 60s mixed
  t=60   drain 60s
  t=120  Round 2: wrk 60s mixed
  t=180  drain 60s
  t=240  Round 3: wrk 60s mixed
  t=300  end

每轮结束取 producer.stats 快照，计算 dropped delta = current - prev。
```

## 详细数据

### 三轮原始数据

| Round | wrk_start | RPS | P99 | sent_delta | acked_delta | dropped_delta | drop % |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 | 07:29:50 | 79,861 | 128.61ms | 4,798,511 | 4,636,111 | **162,400** | **3.3844%** |
| 2 | 07:31:52 | 90,180 | 110.74ms | 5,414,435 | 5,414,435 | **0** | **0.0000%** |
| 3 | 07:33:53 | 93,832 | 106.34ms | 5,637,732 | 5,637,732 | **0** | **0.0000%** |

### 累计 (t0 baseline → R3 end)

| metric | t0 (P3 末) | R3 end | Δ over P4-v2 |
|---|---:|---:|---:|
| sent | 9,882,687 | 25,733,365 | +15,850,678 |
| acked | 9,882,687 | 25,570,965 | +15,688,278 |
| dropped | 0 | 162,400 | +162,400 |
| **总 drop %** | — | — | **1.025%** over 15.85M |

### 统计指标

```
N = 3 rounds
sample = [3.3844, 0.0000, 0.0000]
mean   = 1.1281%
stdev  = 1.9540%
max    = 3.3844%
min    = 0.0000%
mean+1σ = 3.0821%
mean+2σ = 5.0361%

通过判定：
  max 3.38% ≤ 5%       ✅ PASS
  mean 1.13% < 2%      ✅ PASS
  超 5% 概率（正态假设）≈ 5% — 临界
```

## 反直觉 / 关键发现

### 1. ⭐ Cold-start 集中现象 — dropped 不是均匀分布

R1 dropped 162,400 / R2+R3 dropped 0。**不是稳态过程的随机噪声**。

可能机制（按合理性排序）：

**1.1 kgo client TCP 连接池冷启动**
- R1 wrk 启动瞬间 producer 第一次 batch send，kgo TCP 连接还没建到 broker 4 partition
- 前 ~3-5 秒 producer.Send 全部塞 buffer，等 connection establish
- buffer 100ms 超时（虽然配 500ms 但 kgo 内部还有其他 timeout）就 drop
- 一旦连接建好（~ 5s 后），稳态吞吐就上来，drop 归 0

**1.2 broker partition leader 状态校准**
- 4 partitions 在长时间无写入后（P3 结束到 P4-v2 R1 启动间隔 ~8h+），broker partition leader metadata 可能 stale
- R1 启动时 producer 第一次拉 metadata 有 latency
- broker 端拒绝 / 重试期间 buffer 满 drop

**1.3 Go runtime warmup**
- server 进程虽然 uptime 8h+ 但长期低负载，goroutine pool / sync.Pool / GC heap 都 idle
- 突然 wrk 100k+ RPS 冲击让 GC 触发更频繁，前 ~10s GC pause 拉高 producer.Send latency
- buffer 满 drop

### 2. 历史四次跑 R2 config dropped 数据汇总

| 跑次 | sent | dropped | drop % | server 状态 |
|---|---:|---:|---:|---|
| Day 21 R2 | 4,802,673 | 498,912 | **10.39%** | 当日第三次启动（cold） |
| Day 22 P2 | 5,113,350 | 43,979 | 0.86% | 当日第一次启动（cold） |
| Day 22 P3 baseline | 4,553,131 | 0 | 0.00% | server uptime ~3min（warmed） |
| Day 22 P3 fault round | 5,329,260 | 0 | 0.00% | server uptime ~5min（warmed）|
| **Day 22 P4-v2 R1** | **4,798,511** | **162,400** | **3.38%** | server uptime **8h+ idle**（半冷半暖） |
| Day 22 P4-v2 R2 | 5,414,435 | 0 | 0.00% | warm |
| Day 22 P4-v2 R3 | 5,637,732 | 0 | 0.00% | warm |

**模式发现**：dropped 跟 server uptime 强相关，**不跟 R2 config 相关**：

| server 状态 | dropped % |
|---|---:|
| **冷启动后第一次 wrk** | 0.86% - 10.39%（高方差） |
| **warm 状态 wrk** | 0.00%（稳定） |
| **半冷半暖（idle 8h+）** | 3.38%（中间值） |

**真正的优化方向不是 R2 config**：
- 调 SEND_TIMEOUT / MAX_BUFFERED 收益边际递减（已经 0% drop 没空间往下压）
- 真正的方向是 **producer warmup 机制**：server 启动后或长期 idle 后，做一次预热 batch（kgo client 内部预连接 + 状态校准），之后才接受真实流量

### 3. R1 RPS 偏低（79.8k vs R2/R3 90+k）也支持 cold-start 假说

| Round | RPS | Δ vs R3 | 说明 |
|---|---:|---:|---|
| 1 | 79,861 | -14.9% | wrk 前 5-10s GC pause + kgo 建连让 fasthttp 接受 latency 也偏高 |
| 2 | 90,180 | -3.9% | warm 进入稳态 |
| 3 | 93,832 | 0 | full warm |

R1 RPS 80k 跟 P3 baseline RPS 75k 接近（也是冷启）。R2+R3 RPS 90+k 接近 P2 (85k) / P3 fault round (88k) 的 warm RPS 量级。

**结论**：cold-start drop + cold-start RPS 低 都来自同一个 host/process 状态因子，不是独立现象。

## 启发（待 v0.5 决策稿吸收）

### 1. **producer 健康监控必须看 first-N-seconds 时序，不是单点 dropped %**

R1 跑完看 dropped 162k 看起来吓人，但其实可能集中在前 5-10s（占 R1 60s 的 1/12 时间），稳态后 drop 立刻归 0。Grafana dashboard 应该加：
- `kafka_producer_dropped_total` 的 **rate 1s** 指标（看 burst）
- 上线后第一次故障演练前 **跑一次 warmup wrk 30s**（让 server 暖起来）再做正式压测

### 2. **server warm-up 机制可加进 cmd/server**

启动后第一次接收真实流量前，server 内部对 kgo client 做一次：
- 拉一次 metadata（顶替 lazy fetch）
- 发一条 health-check 消息到 broker（建连接池）
- 等 ~1-2s 后才标 `healthy=true`

这样冷启动后立即接 100k+ RPS 时不会 drop 第一波。

### 3. **R2 config 已是稳态最优，P4 系列收口**

D21→D22 共 5 次 R2 config 跑，warmed 状态下 dropped 全是 0%。R2 config (500ms / 200k) 在稳态下完全足够。继续调 SEND_TIMEOUT 1s 或 MAX_BUFFERED 500k 是过度优化。

P4 系列（producer config 调优）**实质收口**。后续问题在 cold-start 路径，不在 config 大小。

## Day 23 接力调整

P4-v2 PASS 改变 Day 23 优先级：

### 高优先（v0.5 主线）

1. **CH config 调优**（13min → 3min 追平）— 不变，原 #2 提到 #1
2. **failure-drill-ch.sh 用修复后的 reset_baseline 跑 Round A/B/C** — 不变
3. **producer warmup 机制研究 + 实现**（新增）— P4-v2 暴露的 cold-start 问题，加到 v0.5 §"健康监控"

### 中优先

4. cmd/consumer kgo 健壮性（topic delete + recreate 自愈）
5. Grafana dashboard 加 `kafka_producer_dropped_total` 的 1s rate 指标
6. consumer.inserted 字段语义核查

### 低优先

7. v0.5 决策稿补完（CH 调优 + drill 三轮 + producer warmup）
8. 简历更新 — 新增"压测发现 cold-start drop 集中现象 + 提出 warmup 方案"作为 v0.5 武器

---

**P4-v2 收口 + producer cold-start drop 集中现象暴露 + Day 23 优先级调整。**
