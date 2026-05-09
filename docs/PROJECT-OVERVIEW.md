# slink 项目总览（Day 1 → Day 18）

> 项目档案。回头查"我们做到哪了 / 接下来去哪 / 当前最终版本数字是多少"。
> 详细每天细节见 `journal/day-NN.md` + `walkthrough/day-NN.md`。
> 路线决策稿在 `architecture/v0.X-*.md`，复盘在 `retrospect/v0.X-retro.md`。

---

## 阶段地图

| 阶段 | Day | 主线 | 收口 RPS | 状态 |
|---|---|---|---:|---|
| v0.1 立项 + 单机基线 | 1-5 | PG + 号段双 buffer + Redis + net/http | 21k | ✅ |
| v0.2 探底 + 换栈底 | 6-7 | pprof + fasthttp | 24k | ✅ |
| v0.3 L1 cache + 工程化 | 8-11 | LRU L1 + Prom/Grafana + 6 HIGH hardening | 86k | ✅ |
| v0.4 异步链路 + 故障域分离 | 12-17 | Kafka pipeline + 双写→影子→切流 + hardening | **93k** | ✅ |
| **v0.5 OLAP 分析侧** | 18-24 | ClickHouse + HLL UV 实时聚合 | — | 🚧 Day 18 ✅ → Day 19+ |
| v0.6 部署侧 | 待 | K8s + server 无状态化 + OpenTelemetry | — | 📋 规划 |
| v0.7+ 候选（不入路线图） | — | Schema Registry / Multi-broker / 真 E2E OTel | — | 💭 占位 |

---

## v0.1（Day 1-5）— 立项 + 单机基线

**目标**：从 0 起一个真实可跑的短链服务。

| Day | 主题 | 关键决策 |
|---|---|---|
| 1 | 项目骨架 | PG + 号段双 buffer 表 / cmd+internal 布局 / docker-compose / 端口偏移避冲突 |
| 2 | config + 健康检查 | caarlos0/env / pgxpool / liveness vs readiness 分离 |
| 3 | 短码生成核心 | 双 buffer + Base62 + 位置混淆 (id × P) mod N 双射 / mulModSafe 防 int64 溢出 |
| 4 | POST /api/links | Stripe 风格幂等 / DB unique 兜底 / SSRF 4 层防御 / 双射 ID 冲突测试 |
| 5 | GET /:code 跳转 + 压测 | LinkCache.GetOrLoad 一站式 / 302 vs 301 / **wrk 21k RPS = 单进程 net/http 天花板** |

**v0.1 数字**：21k RPS（瓶颈：syscall 69% / Go runtime 调度）

---

## v0.2（Day 6-7）— 探底 + 换栈底

**目标**：用真实 profile 数据替代猜测，决定下一个优化方向。

| Day | 主题 | 关键收获 |
|---|---|---|
| 6 | pprof 探底 + 3 alloc 优化 | profile-first / **syscall 69% 是天花板**（验证不是 alloc 不是 GC） |
| 7 | 整层换 fasthttp | 栈底替换 / 零拷贝陷阱 / 瓶颈平移到 Redis / **+13% RPS / -29% CPU per req** |

**v0.2 数字**：24k RPS（瓶颈平移到 Redis 网络往返）

---

## v0.3（Day 8-11）— L1 cache + 工程化收口

**目标**：消除 Redis 网络瓶颈 + 把项目工程化到能上简历。

| Day | 主题 | 关键收获 |
|---|---|---|
| 8 | 进程内 LRU L1 cache | 两层 cache + nil-safe + TTL 分层 + negative cache / **93k RPS（+294%）/ -78.6% alloc per req** |
| 9 | event buffer 调参 + /debug/stats | L1 hit 99.97% / **调参反向劣化复盘**（buffer 出口慢拖累主路径）/ PG 单 flusher 是新瓶颈 |
| 10 | Prometheus + Grafana + 一键起栈 | 闭包注入 vs interface / normalizePath 控基数 / **9% middleware 开销** / 86k RPS（含监控） |
| 11 | v0.3 收口三件套 + 安全 hardening | retrospect + blog + 简历 v4 / code review 6 HIGH 一次清完（DoS / SSRF / 秘密脱敏 / 缓存中毒）/ main.go -34% |

**v0.3 数字**：**86k RPS / P99 24.7ms / L1 hit 99.97% / 0 安全 HIGH 留存** — 简历压舱石

---

## v0.4（Day 12-17）— Kafka 异步 + 故障域分离 ★ 最大跨越

**目标**：把"click 事件写 PG"从主路径剥离到异步链路，验证故障域分离。

| Day | 主题 | 关键收获 |
|---|---|---|
| 12 | kickoff Kafka 决策稿 | `architecture/v0.4-kafka.md` 13 节 / 10 决策（事后 100% 兑现） |
| 13 | 客户端 spike | **kgo 788k vs sarama 444k = 1.78×**（同口径 30s pubsub bench）→ 决策稿封板 |
| 14 | 双写架构落地 | KafkaProducer + DualWriter + feature flag SLINK_EVENT_BACKEND={buffer\|kafka\|dual} / alloc/req 1001B vs 940B 守红线 / Kafka 100% acked |
| 15 | 影子期 | Consumer + 影子表 + binary / 端到端 0 漏 / **Kafka 92.8% vs Buffer 64% +28.8pp** / 故障 15s 主路径 0 timeout |
| 16 | 切流（删 buffer/dualwriter -1064 行） | git tag `v0.3-buffer-final` 兜底 / **93k RPS +21%** / **PG 故障期 RPS +4% / Consumer 故障期 +8% 反涨**（故障域分离硬证据）/ 3 轮 16M reqs 0 timeout |
| 17 | v0.4 收口 + v0.5 kickoff | A1 producer healthcheck / A2 真实 lag (kadm) / A3 wire schema_version / 五张脸 retrospect / v0.5 决策稿 8 项封板 |

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
2. **feature flag**（SLINK_EVENT_BACKEND 在 Day 14 双写 / 15 影子 / 16 切流期 3 次救场）
3. **git tag 兜底**（删代码前必打 tag = 回滚锚点）
4. **同口径 spike**（Day 13 kgo 1.78× sarama 一锤定音）
5. **故障演练**（独立 60s wrk + 15s 故障注入，反直觉数字 = 设计 invariant 硬证据）

---

## v0.5（Day 18+，进行中）— ClickHouse + HLL UV

**目标**：解决 v0.4 留的"数据落 PG 但分析无能"问题（COUNT DISTINCT / TopK 在 PG 行存上扫月分区是分钟级）。

### Day 18 完成（2026-05-09）✅

| 项 | 状态 | 备注 |
|---|---|---|
| ClickHouse 容器入 docker-compose.yml | ✅ | 24.10.2-alpine / 18123 HTTP / 19000 Native |
| migration `0001_click_events_ch` | ✅ | MergeTree / LowCardinality / DateTime64(3) / ORDER BY (code, ts) / country_skip_idx |
| migration `0002_kafka_engine_spike` | ✅ | 三表（Kafka Engine + target + MV） |
| 三组同口径 spike 全跑通 | ✅ | spike-v2 / spike-ch / spike-kafka-fixture + Kafka Engine 端到端 |
| 决策稿 §4 五项封板（D1-D4 + HLL）| ✅ | 见 `architecture/v0.5-clickhouse.md` §4 |
| §10 状态升级 📐 → 📋 | ✅ | 计划稿就位 |
| bench 数字落档 | ✅ | `bench/day-18-spike.md` |
| **操作纪律工程化升级**（同日三连事故触发） | ✅ | `~/.claude/rules/operational-safety.md` + `/CLAUDE.md` + 本文 |

### Day 18 三组 spike 数字摘要

| 维度 | clickhouse-go/v2 | ch-go | Kafka Engine + MV |
|---|---:|---:|---:|
| rows/s | 180k | **210k** ★ | 161k 端到端 |
| alloc/op | 998 B/row | **28 B/row** ★（35×） | 1239 B/row (producer) |
| 复用 v0.4 pipeline | ❌ | ❌ | **✅** ★ |
| Go consumer 代码量 | ~250 行 | ~250 行 | **0 行** ★ |

### Day 18 关键决策（D1）

**写入模式：Kafka Engine + MaterializedView 直消**（不写 Go consumer）。

性能 -23%（161k vs ch-go 直插 210k）的代价被压倒：
- 161k > v0.4 producer 实际负载 93k = 1.73× 余量
- 复用 v0.4 pipeline 0 改动 / 0 行 Go consumer / 故障域真分离

### v0.5 已封板 8 项决策（Day 17 kickoff）

| 决策 | 选了 | 理由 |
|---|---|---|
| 主线方向 | **ClickHouse**（vs K8s / Schema Registry） | v0.4 决策稿 §3 预留点 |
| 数据流向 | **并行**（PG + CH） | PG 留作审计源 |
| Consumer 模型 | **独立 group** | v0.4 故障域分离原则 |
| group 命名 | **slink.click_events.clickhouse_writer** | v0.4 命名前缀习惯 |
| UV 算法 | **HLL（uniqHLL12）** | 0.81% 误差换 1-2 量级内存 |
| 表引擎 | **MergeTree** | 起步最简 / 去重已在 producer/PG 做 |
| 时间分区 | **toYYYYMM(ts)** | 同 PG 分区策略 |
| order by | **(code, ts)** | 同 PG 索引 |

### v0.5 灰度路径（按 v0.5-clickhouse.md §6）

- Day 18-19：spike + 决策稿封板
- Day 20-21：双 consumer 期 + 端到端对账（CH ≈ PG ± 0.1%）+ ClickHouse 故障演练
- Day 22-23：分析查询接入（/api/stats/uv / topk / 时序聚合 P99 < 200ms）
- Day 24：收口 + tag `v0.5-clickhouse-final`

### v0.5 范围红线（明确不做）

- ❌ 替换 PG 路径 / ❌ 多 ClickHouse 副本 / ❌ K8s
- ❌ OpenTelemetry / ❌ 改 producer / ❌ Schema Registry / ❌ 新业务功能

---

## v0.6（规划）— 部署侧 K8s

**为什么 v0.6 才做 K8s 不是 v0.5**（retrospect §4 / v0.5 §2.2 已论证）：
- K8s 多副本会触发 server 无状态化的连锁问题（id 号段共享 / L1 一致性 / Pod 滚动 + 优雅停机）
- 是改核心架构，不是单纯加组件
- "一次只动一个维度"原则：v0.5 只动分析侧 / v0.6 只动部署侧

预期内容：K8s 多副本 + 真 E2E OpenTelemetry trace + Pod 滚动期间 0 timeout 验证。

---

## v0.7+（候选，不入路线图）

| 候选 | 触发条件 |
|---|---|
| Proto + Schema Registry | v0.5 上 ClickHouse 后；A3 V 字段是 JSON 内 schema 演化的过渡方案 |
| Multi-broker Kafka 集群 | 单 broker v0.5 仍够用 |
| True end-to-end OpenTelemetry | 和 K8s 一起做（v0.6 同步） |

---

## 项目档案资产

| 类型 | 数量 / 规模 |
|---|---|
| walkthrough（第一人称回放） | ~9700 行（Day 1-10 + 13-17，Day 11/12 在 retrospect/architecture） |
| journal（结果总结） | 16 篇 |
| retrospect（项目复盘） | 3 个：v0.3-retro / v0.4-retro / code-review-debt |
| architecture（决策稿） | 2 个：v0.4-kafka.md 13 节 / v0.5-clickhouse.md 10 节 |
| 技术博客 | 8 篇（v0.3 收口时简历卖点）+ v0.4 Kafka pipeline 博客 5000 字（Day 16 草稿） |

---

## 项目定位

- **不追商业化**：Go 生态 LLM-first 短链需求小众，star 1-3K 是顶
- **简历压舱石**：现状能写"自建 9w QPS 短链系统 + 故障域分离演练 + 9 篇技术博客"
- **学习载体**：每个 v0.X 落地一组核心技术（v0.1 立项 / v0.2 性能 / v0.3 cache+工程化 / v0.4 异步链路 / v0.5 OLAP / v0.6 K8s）
- **节奏**：每天 ~3 小时，双 commit（feat + docs），push origin/main

---

## 一句话总结

**Day 1 net/http 21k 起步 → Day 17 v0.4 收口 93k RPS + 故障域分离硬证据 + 工程习惯五件套**。当前在 Day 18 v0.5 ClickHouse spike 中段（代码就位但实测数字未收，死机中断）。v0.5 收口后简历可加"+ 实时 OLAP 聚合 + 故障域 3 个"，v0.6 收口后能加"+ K8s 多副本 + E2E trace"。

---

**版本**：v1.0 · 2026-05-09 立 · 维护建议：每个 v0.X 收口时同步更新本文件
