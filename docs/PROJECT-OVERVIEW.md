# slink 项目总览（v0.1 → v0.6-final）

> 一个 Go 实现的高并发短链服务，按 v0.X 节奏迭代到 K8s 部署形态。
> 本文是项目档案：现在做到哪了、关键决策、各阶段数字。
> 各阶段技术决策稿在 `architecture/v0.X-*.md`，benchmark 数字在 `bench/`，技术随笔在 `blog/`。

---

## 阶段地图

| 阶段 | 主线 | 收口 RPS / 关键指标 | 状态 |
|---|---|---:|---|
| v0.1 立项 + 单机基线 | PG + 号段双 buffer + Redis + net/http | 21k RPS | ✅ |
| v0.2 探底 + 换栈底 | pprof + fasthttp | 24k RPS | ✅ |
| v0.3 L1 cache + 工程化 | LRU L1 + Prom/Grafana + 安全 hardening | 86k RPS | ✅ |
| v0.4 异步链路 + 故障域分离 | Kafka pipeline + 双写→影子→切流 | 93k RPS / 0 漏 | ✅ |
| v0.5 OLAP 分析侧 | ClickHouse + HLL UV + DETACH MV 6.8× 消除 | endpoint P99 1.5ms | ✅ |
| v0.6 部署侧 K8s | kind + 滚动 + Ingress + NetworkPolicy + OTel | 247k reqs / 0 5xx / drift 0.0000 | ✅ |

**当前 tag**：`v0.6-final`（Phase 4 全收口）。

---

## v0.1（Day 1-5）— 立项 + 单机基线

**目标**：从 0 起一个真实可跑的短链服务。

- 项目骨架：PG + 号段双 buffer 表 / cmd+internal 布局 / docker-compose / 端口偏移避冲突
- config + 健康检查：caarlos0/env / pgxpool / liveness vs readiness 分离
- 短码生成核心：双 buffer + Base62 + 位置混淆 (id × P) mod N 双射 / mulModSafe 防 int64 溢出
- POST /api/links：Stripe 风格幂等 / DB unique 兜底 / SSRF 4 层防御
- GET /:code 跳转 + 压测：LinkCache.GetOrLoad 一站式 / 302 vs 301 / **21k RPS = 单进程 net/http 天花板**

**v0.1 数字**：21k RPS（瓶颈：syscall 69% / Go runtime 调度）

---

## v0.2（Day 6-7）— 探底 + 换栈底

**目标**：用真实 profile 数据替代猜测，决定下一个优化方向。

- pprof 探底 + 3 alloc 优化：profile-first / **syscall 69% 是天花板**（验证不是 alloc 不是 GC）
- 整层换 fasthttp：栈底替换 / 零拷贝陷阱 / 瓶颈平移到 Redis / **+13% RPS / -29% CPU per req**

**v0.2 数字**：24k RPS（瓶颈平移到 Redis 网络往返）

---

## v0.3（Day 8-11）— L1 cache + 工程化收口

**目标**：消除 Redis 网络瓶颈 + 把项目工程化到生产可用形态。

- 进程内 LRU L1 cache：两层 cache + nil-safe + TTL 分层 + negative cache / **93k RPS（+294%）/ -78.6% alloc per req**
- event buffer 调参 + /debug/stats：L1 hit 99.97% / **调参反向劣化复盘** / PG 单 flusher 是新瓶颈
- Prometheus + Grafana + 一键起栈：闭包注入 vs interface / normalizePath 控基数 / **9% middleware 开销** / 86k RPS（含监控）
- v0.3 收口三件套 + 安全 hardening：code review 6 HIGH 一次清完（DoS / SSRF / 秘密脱敏 / 缓存中毒）/ main.go -34%

**v0.3 数字**：**86k RPS / P99 24.7ms / L1 hit 99.97% / 0 安全 HIGH 留存**

---

## v0.4（Day 12-17）— Kafka 异步 + 故障域分离 ★ 最大跨越

**目标**：把"click 事件写 PG"从主路径剥离到异步链路，验证故障域分离。

- kickoff Kafka 决策稿：`architecture/v0.4-kafka.md` 13 节 / 10 决策（事后 100% 兑现）
- 客户端 spike：**kgo 788k vs sarama 444k = 1.78×**（同口径 30s pubsub bench）→ 决策稿封板
- 双写架构落地：KafkaProducer + DualWriter + feature flag SLINK_EVENT_BACKEND={buffer\|kafka\|dual} / alloc/req 1001B vs 940B 守红线 / Kafka 100% acked
- 影子期：Consumer + 影子表 + binary / 端到端 0 漏 / **Kafka 92.8% vs Buffer 64% +28.8pp** / 故障 15s 主路径 0 timeout
- 切流（删 buffer/dualwriter -1064 行）：git tag `v0.3-buffer-final` 兜底 / **93k RPS +21%** / **PG 故障期 RPS +4% / Consumer 故障期 +8% 反涨**（故障域分离硬证据）/ 3 轮 16M reqs 0 timeout
- v0.4 收口：A1 producer healthcheck / A2 真实 lag (kadm) / A3 wire schema_version

### v0.4 最终数字

| 指标 | 数字 |
|---|---:|
| 单路径 RPS | **93,607**（vs Day 5 起点 21k = **+346%**） |
| 端到端 0 漏 | 0 / 2,759,115 |
| Kafka 路径捕获率 | **97.9%** |
| Kafka 故障 15s timeout | 0 / 3.79M reqs |
| **PG 故障期 RPS** | **+4% 反涨**（L1 99.99% 兜住，故障域真分离） |
| **Consumer 故障期 RPS** | **+8% 反涨**（独立进程不抢 CPU） |
| 故障恢复 producer 重连 | 1s |
| 故障恢复 consumer 30s drain | 追写 3M+ 条 |
| alloc/req | 1001 B（< 1KB 红线） |

### v0.4 沉淀的工程习惯五件套

1. **决策稿先行**（v0.4-kafka.md 13 节 / 100% 兑现，过度准备 = 实施轻松）
2. **feature flag**（SLINK_EVENT_BACKEND 在双写 / 影子 / 切流 3 次救场）
3. **git tag 兜底**（删代码前必打 tag = 回滚锚点）
4. **同口径 spike**（kgo 1.78× sarama 一锤定音）
5. **故障演练**（独立 60s wrk + 15s 故障注入，反直觉数字 = 设计 invariant 硬证据）

---

## v0.5（Day 18-25）— ClickHouse + HLL UV + MV 优化

**目标**：解决 v0.4 留的"数据落 PG 但分析无能"问题（COUNT DISTINCT / TopK 在 PG 行存上扫月分区是分钟级）。

- ClickHouse 入栈：24.10.2-alpine / 18123 HTTP / 19000 Native / `0001_click_events_ch` MergeTree LowCardinality
- 写入模式：**Kafka Engine + MaterializedView 直消**（0 行 Go consumer / 复用 v0.4 pipeline）
- HLL UV：uniqHLL12 / 0.81% 误差换 1-2 量级内存
- 端到端对账演练：CH ≈ PG ± 0.1% / ClickHouse 故障注入 + 恢复
- /api/stats/* endpoint：UV / TopK / 时序聚合 / **endpoint P99 1.5ms（< 200ms 目标 130×）**
- **DETACH MV 优化**：写入 fanout 路径暴露，原 51% CPU → **7.5%（6.8× 消除）**
- tag `v0.5-clickhouse-final`

### v0.5 关键数字

| 指标 | 数字 |
|---|---:|
| Kafka Engine 端到端 rows/s | 161k（> v0.4 producer 实际负载 93k = 1.73× 余量） |
| HLL UV 误差 | 0.81% |
| /api/stats endpoint P99 | **1.5 ms**（目标 200ms） |
| MV CPU 消耗 | **51% → 7.5%（6.8× 消除）** |
| 端到端对账 drift | < 0.1% |

---

## v0.6 + Phase 4（Day 26-30）— K8s + 全链路收口

**目标**：把项目搬到 K8s + 通过滚动/驱逐演练验证 Pod 级故障域。

### v0.6 Phase 1-3：K8s 基础部署 + 演练

- kind 单节点集群 + 多 Pod 副本 + 灰度滚动
- 抓到 **Kafka K8s advertised listener 真隐患**：fresh K8s Pod sent=N acked=0 dropped=100%（v0.6 Phase 1 起从未工作过，演练成功但实际异步路径 0 工作 — vacuous truth 现场触发）
- 修法：docker-compose 加 EXTERNAL_K8S `:19093 advertised=host.docker.internal:19093` + Pod brokers 指向新 listener
- 3 轮演练（修后）：`kubectl delete pod` / `scale 0→3` / `cordon+drain`，总 **2,221,382 reqs / 0 timeout / 0 5xx / 3× PG=CH drift 0.0000**

### v0.6 Phase 4：OTel + Ingress AuthN + NetworkPolicy + consumer K8s + recon v2

- OTel SDK v1.36 接入 + fasthttp middleware + KafkaProducer span（W3C traceparent inject）+ collector Deployment + Jaeger backend
- Ingress AuthN：ingress-nginx + slink-business/slink-stats 双 Ingress + basic-auth Secret
- consumer 进 K8s：Dockerfile.consumer distroless 22MB + replicas=2 + NetworkPolicy（默认 deny + 白名单）
- recon v2：`AUTO_WINDOW=1 LAST_N_MIN=N` 从 PG max(ts) 反推时间窗
- **5 轮故障演练（R1-R5）**：总 **247,556 reqs / 0 5xx / drift 0.0000-0.0001 PASS**
- tag `v0.6-final`

### v0.6 关键数字

| 指标 | 数字 |
|---|---:|
| Phase 3 演练总 reqs | 2,221,382 / 0 timeout / 0 5xx |
| Phase 3 PG=CH drift | 3× **0.0000** |
| Phase 4 演练总 reqs | 247,556 / 0 5xx |
| Phase 4 drift | R1-R3 **0.0000-0.0001** |
| K8s 部署组件 | server + consumer + otel-collector + jaeger + ingress-nginx |

---

## 工程习惯沉淀（贯穿全期）

1. **决策稿先行**（v0.4 / v0.5 / v0.6 / Phase 4 各一份）
2. **feature flag + git tag**（每个删代码前必打 tag）
3. **同口径 spike**（kgo vs sarama / ch-go vs clickhouse-go / 不同 K8s ingress）
4. **故障演练 + 端到端对账**（业务面 metric 是必要不充分，必须 PG=CH drift）
5. **profile-first**（pprof 找瓶颈，不凭直觉）

---

## 项目档案资产（公开仓）

| 类型 | 数量 / 内容 |
|---|---|
| 架构决策稿 | `architecture/` 4 篇：v0.4-kafka / v0.5-clickhouse / v0.6-k8s / v0.6-phase4 |
| benchmark 报告 | `bench/` 多篇覆盖 v0.1-v0.6 各阶段数字 |
| 技术博客 | `blog/` 9 篇：Prom/Grafana 工程化 / Kafka 异步管线 / ClickHouse MV 优化 / K8s 滚动演练 / vacuous truth 反思 等 |
| ADR | `adr/` 关键架构决策 |
| K8s 部署 | `deploy/k8s/` 7 yaml（namespace / deployment / service / ingress / network-policy / consumer / otel-collector） |
| 演练脚本 | `scripts/` recon-fixture / failure-drill-ch 等 |

---

## 一句话总结

**v0.1 net/http 21k 起步 → v0.6 Phase 4 K8s 多副本 + 全链路演练 247k reqs / 0 5xx / PG=CH drift 0.0000**。30 天单线条把"短链服务"从单机 baseline 推进到带 OTel + Ingress AuthN + NetworkPolicy + 端到端对账的 K8s 部署形态。

---

**版本**：v2.0 · v0.6-final 收口同步 · 维护建议：每个 v0.X 收口时同步更新
