# Day 9 — event buffer 调参 + /debug/stats endpoint

> 2026-05-07 / slink v0.3-day9
>
> 主战场：Day 8 收口的两个待办 ——
>   1. 修异步 click event buffer overflow（Day 8 实测 63% drop）
>   2. 暴露 `/debug/stats` 让 L1 命中率不再靠"profile 间接估"

## 一、目标与背景

Day 8 收口暴露两个问题：
- **Buffer overflow**：93k RPS 下 buffer 容量 10k / batch 1k / flush 1s 在 ~2 秒内打爆，server log 1,782,964 条 `event buffer full`
- **观测盲区**：L1 命中率"应该 99%+"是从 profile 间接估算（"redis 没进 top 40 → miss <1%"），没有实测数据

Day 9 两件事：
1. event.Buffer.Stats 加 Capacity/Used + config 化 buffer 参数（默认 50k/2k/500ms）
2. `/debug/stats` JSON endpoint，挂 admin 端口（pprof :6060），暴露 L1/event/id 三处运行时指标

## 二、改动概览

| 文件 | 改动 |
|---|---|
| `internal/event/buffer.go` | `Stats` 加 `Capacity`/`Used` 字段，含 JSON tag；`Stats()` 现读 `cap(b.ch)` / `len(b.ch)` |
| `internal/event/buffer_test.go` | 新增 `TestBuffer_Stats_CapacityAndUsed` |
| `internal/cache/local_cache.go` | `localStats` → `LocalCacheStats`（公开），加 JSON tag |
| `internal/cache/link_cache.go` | `LocalStats()` 签名同步换 `LocalCacheStats` |
| `internal/id/segment.go` | `BufferStat` 字段加 JSON tag（含 cur_low/cur_high/cur_cursor/cur_usage 等）|
| `internal/config/config.go` | 新增 `EventBufferCapacity/BatchSize/FlushInterval`（默认 50000/2000/500ms）+ Validate 跨字段约束 |
| `cmd/server/main.go` | wiring buffer config；新增 `statsHandler` 挂 `/debug/stats`；version → v0.3-day9 |
| `.env.example` | 增 `SLINK_EVENT_BUFFER_*` 三项 |

## 三、`/debug/stats` 输出示例

```json
{
  "version": "v0.3-day9",
  "uptime_seconds": 92,
  "link_cache": {
    "l1": {
      "hits": 3768487,
      "misses": 1195,
      "hit_rate": 0.9996829971334452
    }
  },
  "event_buffer": {
    "enqueued": 1905685,
    "dropped": 1863997,
    "flushed": 1854068,
    "flush_err": 0,
    "capacity": 50000,
    "used": 49617
  },
  "id_generator": {
    "cur_low": 24401,
    "cur_high": 25400,
    "cur_cursor": 24400,
    "cur_usage": 0,
    "next_ready": false,
    "next_low": 0,
    "next_high": 0,
    "refilling": false
  }
}
```

挂在 admin 端口（默认 `127.0.0.1:6060`），只本机绑定，不暴露给外网。

## 四、调参 bench 对比

同 mixed 场景（4t/256c/30s）：

| 指标 | Day 8 (10k/1k/1s) | Day 9 conservative (50k/2k/500ms) | Day 9 aggressive (100k/5k/200ms) |
|---|---|---|---|
| **RPS** | 93,508 | **94,354** | 77,733 ⬇ |
| P50 | 1.70ms | 1.92ms | 2.27ms |
| P99 | 32.52ms | **28.23ms** | 64.69ms ⬇ |
| **L1 hit rate**（实测）| 估算 ≥99% | **99.97%** | 99.98% |
| **dropped %** | ~63% | **49.4%** | 55.4% ⬇ |
| flush 出口速率 | 估 30k/s | **61.8k/s** | 42.6k/s ⬇ |
| flush_err | 未观测 | 0 | 0 |

### 4.1 关键发现

**激进调参（batch 5000 / flush 200ms）反而把 RPS 拉下 17.6%**。原因：

1. batch 5000 太大 → PG 单次 COPY 写入耗时显著增加（5x 数据 vs 2.5x batch 大小）
2. flusher 是单 goroutine → 写一次 5000 条期间整体阻塞 → 出口反而变慢
3. flusher 被堵 → channel 堆满更快 → fasthttp enqueue 时也变慢（select default 也要走调度）→ 主路径连带变慢
4. P99 从 28ms 涨到 64ms 是整体堵塞的直接表现

**实测教训**：buffer 调参不是越大越好。当前出口（PG batch insert）有它自己的甜蜜点（实测 batch 2000 / flush 500ms 时 61.8k/s），调过头反而劣化整体性能。

**这是 profile-first SOP 的生动反例**：靠拍脑袋调参可能反而变差，要先看 PG 自己的指标（每批插入耗时分布）才知道真正的瓶颈在哪。

### 4.2 真正的出口瓶颈

实测 PG 单 flusher 在 batch 2000 / 500ms 配置下能 flush 出 **61.8k/s**。

vs 入速：93k RPS × 1 click/req = 93k clicks/s

净缺口：93 - 61.8 = **31.2k/s 净入 buffer** → 50k buffer 在 ~1.6s 满 → 此后稳态丢 31.2k/s 的 click。

要彻底消 dropped，**buffer 调参解决不了**——必须改造出口侧：

| # | 方案 | 预期 | 难度 |
|---|---|---|---|
| 1 | 多 flusher worker 并行（4 worker × 30k/s ≈ 120k/s 出口）| 消 dropped | 中（要分片 channel 或加 worker pool）|
| 2 | 采样（按 1% 采样）| dropped 趋零 | 低，但数据失真 |
| 3 | 直接异步写 Kafka，不写 PG | 出口几乎无限 | 高（v0.4 路线图）|

是 v0.3 后续 / v0.4 的活，不在 Day 9 范围。

### 4.3 L1 hit rate 实测验证 Day 8 估算

Day 8 报告写"L1 hit rate ≥99%"，是基于"redis 没进 profile top 40 → miss <1%"的间接估算。

Day 9 实测：**99.97%**（30s 内 L1 hits 3,768,487 / total 3,769,682）。

差距很小（估算 ≥99% vs 实测 99.97%），证明 Day 8 的判断方向对，但只有实测数字才是真硬通货。

## 五、Day 8 假设的真伪

Day 8 写："要消 dropped，可以提高容量 + batch + flush 频率"。

✅ 部分对：50k/2k/500ms 比 10k/1k/1s 把 dropped 从 63% 降到 49%（-14pp）。
❌ 部分错：以为继续加大就行，实测 100k/5k/200ms 反而更糟。

为什么估错？低估了"PG 单连接 batch insert 耗时"对 flusher 的反向阻塞。
**PG 不是无限快的 sink**，单 connection batch insert 在 batch=2000 时有甜蜜点；
继续加 batch 是把"一次 flush 持续时间"拉长，实际 flusher 整体吞吐反而下降。

## 六、关键命令复现

```sh
# Conservative 配置（默认）
SLINK_PPROF_ADDR=127.0.0.1:6060 ./bin/server

# Aggressive 配置（用于对照）
SLINK_PPROF_ADDR=127.0.0.1:6060 \
  SLINK_EVENT_BUFFER_CAPACITY=100000 \
  SLINK_EVENT_BUFFER_BATCH_SIZE=5000 \
  SLINK_EVENT_BUFFER_FLUSH_INTERVAL=200ms \
  ./bin/server

# 跑 bench + 抓 stats
unset http_proxy https_proxy
CODES_FILE=/tmp/slink-codes.txt wrk -t4 -c256 -d30s -s scripts/bench/mixed.lua http://localhost:18080
curl -sS http://127.0.0.1:6060/debug/stats | python3 -m json.tool
```

## 七、Day 10 候选

按 ROI：

| # | 方向 | 预期 | 难度 |
|---|---|---|---|
| 1 | **多 flusher worker 并行**（4 × 30k/s）| 真正消 dropped | 中 |
| 2 | metrics + Prometheus（基于已有 stats endpoint，几乎"白嫖"）| 工程可见性 | 低 |
| 3 | Docker compose 工程化（多服务编排）| 可发布给 reviewer 看 | 低 |
| 4 | 限流 + 退化（突破 buffer 时返回 503/记 sample）| 不丢 click 的兜底 | 中 |

**推荐 Day 10**：#2 + #3（半天搞定可观测性 + 容器化），收口 v0.3 工程化。
#1 留给 v0.4 异步 Kafka 重构（与路线图一致：Kafka 削峰是 v0.4 主战场）。

## 八、数据归档

- bench 输出：`/tmp/slink-day9/bench-d9.txt`（conservative）+ `bench-aggressive2.txt`（aggressive）
- server log：`/tmp/slink-day9/server.log` + `server-aggressive2.log`
- 对比基准：`docs/bench/day-08-localcache.md`
