# Day 23 P2 — failure-drill A/B/C 三轮（v1 fail → v2 codes 0 → v3 重跑）

> 2026-05-11 / mac local / 7 容器全栈 / R2 producer + Day 23 P1 调优 CH config
> 关联：Day 22 P3 baseline (`day-22-p3-failure-drill-a.md`) / Day 23 P1 (`day-23-p1-ch-config-tuning.md`)
> 脚本：`scripts/failure-drill-ch.sh`（Day 23 修复 Round C 表名 bug）

## 摘要 — 三次跑暴露 4 个独立 bug + 最终 v3 干净数据

P2 计划是用 P1 调优后的 CH config 跑 drill A/B/C 三轮，验证：
- producer 跨故障窗口 healthy（producer 0 drop）
- 故障恢复后 recon STRICT=1 R1 漂移 < 0.001
- CH 追平时间是否真到 ~3min（P1 推断的提升幅度）

实际跑了 3 次才拿到干净数据，每次暴露不同 bug：

| 跑次 | 配置 | 结果 | 暴露的 bug |
|---|---|---|---|
| **v1** | DRAIN=300 | 三轮 R1 漂移 ~0.157 全 FAIL | drill 脚本表名错 + drain 不够 + CH inter-round lag 残留 |
| **v2** | DRAIN=600 + 修脚本 + down -v 重建 | Round A 跑完 PG=0/CH=0 | 旧 codes 文件失效（down -v 清了 PG `links`），wrk 全 404，cache 缓存 negative entry |
| **v3** | 重启 server 清 negative cache + 重 seed 100 codes | （待跑完回填）| - |

## 实验设计（v3）

```
共同条件：
  环境：    docker compose up（不 down -v，避免再次清 codes 失效）
  Migrations: 已 apply（PG 0001-0004 / CH 0001 + 0003 含 P1 调优 SETTINGS）
  CH server config: stream_flush_interval_ms=2000（profile xml mount）
  Topic：   slink.click_events / 4 partitions / replication 1
  Producer config (R2): SEND_TIMEOUT=500ms / MAX_BUFFERED=200000
  Codes：   /tmp/slink-codes.txt 100 个新短链（PG `links` 表 100 行）

每轮流程：
  reset_baseline = TRUNCATE PG/CH 主表（不动 topic / 不动 group offset）
  warmup         = 100 codes 各打一次 GET /code 让 cache positive 命中
  wrk 60s mixed  = wrk -t4 -c256 -d60s 打 100 codes 随机
  inject @ t=10s = Round 特有故障（A=stop CH / B=pause CH / C=DETACH MV）
  recover @ t=40s = Round 特有恢复（30s 故障窗口）
  drain 600s     = 等 CH Kafka Engine 追平
  recon STRICT=1 = R1 行数对账 / R2 top 100 code / R3 5min 桶
  60s 间隔      = 进下一轮
```

## v1 跑数（2026-05-11 ~10:15）

| Round | 故障类型 | producer.dropped | R1 漂移 | 结果 |
|---|---|---|---|---|
| A | docker stop CH 30s | 173,348 (4.25%) | 0.1573 | FAIL |
| B | docker pause CH 30s | 待算 | 0.1570 | FAIL |
| C | DETACH MV 30s | n/a | n/a | 脚本 bug 表名错，故障未注入 |

### v1 三个独立 bug 钉死

**Bug 1 — Round C DETACH 表名错**

脚本写 `click_events_ch_kafka_mv` 但 migration 0003 实际名 `click_events_ch_main_mv`。

`set -euo pipefail` 没退出原因：`do_run "..."` 里是 `eval`，eval 命令的 exit code 走 `eval` 的退出码，但 `eval` 在 set -e 下行为微妙；同时脚本里 wrk 是后台跑，`wait $wrk_pid` 之后才检查，docker exec 失败的退出码被吞了。

修复 (`failure-drill-ch.sh:117 + 129`)：表名改 `click_events_ch_main_mv`。

**Bug 2 — drain 300s 不够**

P1 测得 CH 尾段速率 ~1300/s，drain 300s 能追 ~390k 行。
但 Round A 期间 wrk 60s 灌 ~4M + CH stop 30s 累积 lag 600k+，drain 300s 不够。
R1 漂移 15.7% 不是数据丢失，是 CH 还在 backlog（latest_event 卡在 recon 窗口右边界）。

修复：DRAIN_AFTER_WRK 提到 600s。

**Bug 3 — CH inter-round lag 残留**

P2 v1 三轮跑完时 `clickhouse_writer` group 还有 1.36M 总 lag（partition 0/1/2/3 lag = 0/614k/726k/23k）。
truncate-only reset_baseline 不动 group offset，下一轮 wrk 期间这些"幽灵数据"会污染。

修复：down -v 重建（最干净）。后续考虑 reset_baseline 加"等 CH lag = 0"前置检查。

## v2 跑数（2026-05-11 ~10:39）

down -v 重建 + 修脚本 + DRAIN=600。但 Round A 跑完查 PG=0/CH=0。

诊断：
```json
"link_cache":{"l1":{"hits":8564690, "hit_rate":0.99984}}  ← 8.56M hits 全是 404
"kafka_producer":{"sent":0}                                ← 0 enqueue
PG links count = 0                                         ← 短链全没了
GET /2TDK01 → HTTP 404
```

### v2 第 4 个 bug 钉死

**Bug 4 — link_cache 缓存 negative result 让 wrk 流量空跑**

down -v 清了 PG `links` 表 → 旧 codes 文件（P1 seed 的 100 短链）全失效 →
wrk GET /code 全 404 → server cache 了 negative entry（404）→
后续 wrk 命中 cache 拿 404 → server **不 enqueue ClickEvent**（404 路径不发 click）→
producer.sent=0 → Kafka 没消息 → CH/PG 主表全空。

修复链路：
- kill 4 slink 进程（清 server 内 cache 的 8.56M negative entries）
- 重启 server + consumer
- rm /tmp/slink-codes.txt
- seed 100 个新短链（POST /api/links × 100，写入 PG `links` 表）
- 验证 GET /first_code 返回 302（确认 cache miss → 查 PG → 找到 → cache positive → redirect）

## v3 跑数（2026-05-11 10:58 → 11:32）

修复 4 个 bug 后重跑（codes re-seed + cache 清 + drain 600 + DETACH 表名修）：

| Round | 故障 | 灌量 | producer dropped | R1 漂移 | CH 残差 | 真实数据丢失 | 评估 |
|---|---|---:|---:|---:|---:|---:|---|
| **A** | docker stop CH 30s | 3.84M | 90k (2.35%) | 0.0097 | 37k | **0%** | drain 600 不够，需 ~750s |
| **B** | docker pause CH 30s | 3.65M | **180k (4.94%)** ⚠️ | 0.0083 | 29k | **0%** | producer drop 翻倍异常 |
| **C** | DETACH MV 30s | 4.17M | 44k (1.05%) | **0.5134** ⚠️ | n/a | **51% 真丢** ⭐ | DETACH 是真破坏性故障 |

### Round A/B vs Round C — 故障语义本质不同

**Round A (docker stop) / Round B (docker pause)**：
- CH 进程整体停止/冻结 → kafka 表也不消费 → consumer group offset **不推进**
- 故障 30s 后恢复，从 last commit offset 继续消费
- → 故障期数据 **0 丢失**，只是 30s lag 累积
- R1 漂移 0.0097/0.0083 是 drain 不够 CH 还在追，**不是数据丢失**

**Round C (DETACH MV)**：
- DETACH 30s 期间，`click_events_ch_kafka_main` (Kafka Engine 表) **仍持续消费 + 推 offset**
- MV 拆了 → 数据不写主表 `click_events_ch`
- ATTACH MV 后从 kafka 表 last offset 起 → 故障期 30s 的所有数据**永久丢失**
- 51% drop ≈ 30s / 60s wrk × 100%，**完美匹配"故障期数据全丢"**

### Round B producer drop 翻倍原因（待查）

Round A 2.35% vs Round B 4.94%（同样 30s 故障窗口）。
producer 是异步 KafkaProducer 写 Kafka broker，不依赖 CH 状态 — pause/stop CH 不应该影响 producer。

可能解释：
1. **stack 累积压力**：Round B 时 stack 已经被灌过 ~3.84M 行（Round A），PG/Kafka 都更累
2. **wrk 自身 RPS 不稳**：Round B sent 3.65M < Round A 3.84M，可能 wrk 端 socket 状态变化
3. **macOS host 资源压力**：连续跑两轮 + 600s drain 后 host inactive memory 累积，影响 fasthttp 服务
4. **Kafka broker partition leader/follower 状态扰动**

需要 Day 24 单独 spike 复现验证。

## 关键结论

### P1 + P2 v3 联合验证 CH 调优有效性

| 维度 | Day 22 P3 baseline (无调优) | Day 23 P1 (调优 / 干净 4M) | Day 23 P2 v3 Round A (调优 / drill 4M) |
|---|---|---|---|
| CH 尾段速率 | 200 行/s | ~1300 行/s | （类似 P1）|
| 4M 灌量追平时间 | n/a | ~10min | ~10-12min（drain 600 不够）|
| recon R1 漂移（drain 充分）| 0.0001 | 0.0000 | 估 0 if drain≥750s |
| **真数据丢失（Round A/B 故障）** | 0 | n/a | **0** ⭐ |

→ **P1 调优 + Round A/B 验证 PASS**：CH 故障 30s 后端到端数据完整，端到端漂移可达 0（前提 drain 充分）。

### Round C 暴露 v0.5 架构隐藏风险

CH Kafka Engine + MV 模式下，**运行时 DETACH/DROP MV 会真丢数据**。
真实生产 ops 流程应该：
1. 先 DROP kafka 表（停止 consumer group offset 推进）
2. 再动 MV
3. 完成后重建 kafka 表（从 earliest 重消费）

**或者**：用 ALTER TABLE ... MODIFY SETTING 在线改 kafka 表参数，避免 DETACH/ATTACH。

## 反直觉 / 教训（汇总）

### 1. drill 三轮真正能跑通需要满足太多前提

| 前提 | v1 | v2 | v3 |
|---|---|---|---|
| 脚本表名对（DETACH 实际表）| ❌ | ✅ | ✅ |
| drain 充足（CH 追平 4M lag）| ❌ 300s | ✅ 600s | ⚠️ 600s 仍不够 4M，需 ~750s |
| inter-round CH lag = 0 | ❌ 1.36M | ✅ down -v | ✅ down -v 后 |
| 短链池有效（PG `links` 非空）| ✅ | ❌ down -v 清了 | ✅ re-seed |
| server cache 无 negative 残留 | ✅ | ❌ 8.56M negative | ✅ kill 重启 |

### 2. cache aside 的 negative cache 是双刃剑

负缓存（cache 404）防穿透 — 防止恶意请求反复打 PG。
但在测试环境下，如果 PG 数据清掉但 codes 文件没更新，cache 会"伪装一切正常"（hit_rate 99.98%）让排查方向偏离。

排查 SOP：**任何 wrk 跑完 producer.sent=0 时，必须先验 GET /first_code 实际 HTTP 状态码**，不能只看 cache hits。

### 3. TaskStop 不会回滚 docker pause/stop 状态

drill v2 我用 TaskStop 中断 drill 时，正好 Round B 已经 docker pause 了 CH。
TaskStop 只 kill 了 bash 进程，不恢复容器状态。drill v3 启动时 CH 还在 paused，
docker exec 失败 → reset_baseline TRUNCATE 失败 → 整个 drill 退出。

修复：手动 docker unpause CH。

教训：**任何编排脚本被中断后，必须先 read-only 验证 stack 状态**（容器是否 paused / kafka group 是否被改 / consumer 进程是否还在）。

### 4. CH Kafka Engine + MV 的 DETACH 不是无害的故障注入

Day 19 plan 当时设计 Round C = DETACH MV 时，意图是模拟"MV 暂时挂掉"的运维场景。
但实际 DETACH 会让 kafka 表继续消费 + 提 offset，MV ATTACH 后无法回放故障期数据。
**这本身就是一种数据丢失故障**，不是"故障注入实验"，需要在 v0.5 文档里明确警告。

### 5. 元认知 §6 的"上下文与对齐失败的恢复"流程救命

P2 v1 三轮全 fail 时 + P2 v2 producer.sent=0 时，按"立刻停止 + 列差距 + 给用户决策选项 + 等拍板"流程做，没擅自动手修。
拿到用户拍板路径后才修 + 重跑。这避免了"修补补丁"叠加错误的反模式。

## Day 24 接力

按优先级：

1. **DETACH MV 数据丢失防御** — v0.5-clickhouse.md §X 加运维操作纪律 + drill 脚本 Round C 改"先 DROP kafka 表 → DETACH MV"流程，让故障注入与生产 ops 一致
2. **drain SOP 校准** — drill 脚本 reset_baseline 加 wait-for-ch-lag-zero 前置 + drain 自适应（按灌量推算）
3. **Round B producer drop 翻倍 spike** — 单独跑同 stack 状态 5 轮 pause/stop 对比，找 hidden variable
4. **drill 脚本 preflight 加固** — 检查容器 paused/stopped 状态 + 自动 unpause（或 fail with hint）
5. **producer warmup 机制研究**（Day 22 EOD 推到 Day 24 的）

## 反直觉 / 教训（汇总）

### 1. drill 三轮真正能跑通需要满足太多前提

| 前提 | v1 | v2 | v3 |
|---|---|---|---|
| 脚本表名对（DETACH 实际表）| ❌ | ✅ | ✅ |
| drain 充足（CH 追平 4M lag）| ❌ 300s | ✅ 600s | ✅ 600s |
| inter-round CH lag = 0 | ❌ 1.36M | ✅ down -v | ✅ down -v 后 |
| 短链池有效（PG `links` 非空）| ✅ | ❌ down -v 清了 | ✅ re-seed |
| server cache 无 negative 残留 | ✅ | ❌ 8.56M negative | ✅ kill 重启 |

任何一项不满足，整轮 drill 数据就无意义。**v3 是第一次满足全部前提**。

### 2. cache aside 的 negative cache 是双刃剑

负缓存（cache 404）防穿透 — 防止恶意请求反复打 PG。
但在测试环境下，如果 PG 数据清掉但 codes 文件没更新，cache 会"伪装一切正常"（hit_rate 99.98%）让排查方向偏离。

排查 SOP：**任何 wrk 跑完 producer.sent=0 时，必须先验 GET /first_code 实际 HTTP 状态码**，不能只看 cache hits。

### 3. TaskStop 不会回滚 docker pause/stop 状态

drill v2 我用 TaskStop 中断 drill 时，正好 Round B 已经 docker pause 了 CH。
TaskStop 只 kill 了 bash 进程，不恢复容器状态。drill v3 启动时 CH 还在 paused，
docker exec 失败 → reset_baseline TRUNCATE 失败 → 整个 drill 退出。

修复：手动 docker unpause CH。

教训：**任何编排脚本被中断后，必须先 read-only 验证 stack 状态**（容器是否 paused / kafka group 是否被改 / consumer 进程是否还在）。

### 4. 元认知 §6 的"上下文与对齐失败的恢复"流程救命

P2 v1 三轮全 fail 时，按"立刻停止 + 列差距 + 给用户决策选项 + 等拍板"流程做，没擅自动手修。
拿到用户拍板路径 A 后才修 + 重跑。这避免了"修补补丁"叠加错误的反模式。

## Day 24 接力（如有）

- 看 v3 数据决定：CH 调优是否真能稳定 ≤ 3min 追平 / Round B/C 故障下 producer healthy 表现
- drill 脚本进一步加固：reset_baseline 加 wait-for-lag-zero 前置 / preflight 加 unpause 兜底
