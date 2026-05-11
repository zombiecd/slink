# slink v0.6 — K8s 多副本滚动 + 故障演练，以及"通过演练"的 vacuous truth

> 3 day 单日打穿 4 个 Phase（Phase 0 决策 / Phase 1 单 Pod / Phase 2 三连锁 / Phase 3 演练）。最大收获不是 2.22M reqs / 0 timeout 这串数字，而是 Day 27 演练"成功"在 Day 28 端到端对账下被发现是 vacuous truth——v0.6 K8s Pod 的 Kafka producer 从 Phase 1 起 100% silent drop 持续 ~3 day。

## 1. 起点：v0.5 留下的"server 单点"

slink v0.5 收口（Day 25）时链路是这样的：

```
       client
         │
         ▼
  fasthttp（单进程，单机）
         │
   ┌─────┼─────┐
   ▼     ▼     ▼
  PG    Redis  Kafka ──► PG Consumer + ClickHouse Engine
                          （已多消费者组，已故障域分离）
```

后端侧已经在 v0.4 切流 + v0.5 drill 验证过 3 个故障域互不影响，但 **server 本身仍是单进程单机**：

- 进程挂 = 100% 流量挂
- 滚动升级 = 一段全量漏
- 演练时只杀过后端，没杀过 server 自己——in-flight 请求 / Kafka producer buffer / L1 状态在 SIGTERM 下怎么处理，没答案

v0.6 的目标很清晰：**server 容器化 + K8s 多副本 + 三大无状态化连锁解决 + 滚动 0 漏**。

## 2. 路线选型：为什么是 K8s（不是 docker swarm / nomad / OTel 先做）

候选评估在决策稿 §2 里详细展开，结论：

- swarm 实质半弃维 / nomad 简历叙事价值远低于 K8s（招聘搜索量比 ~50:1）
- "先起多副本不做无状态化" 是错的——id 号段不共享会发**重复 ID**给客户端 → PK 冲突 / 错误短链。这是正确性 bug 不是性能问题，不能延期
- "先 OTel 再 K8s" 是错的——OTel 在 K8s 上做 sidecar 注入更顺，先 OTel 二次注入要重做

**红线**：v0.6 K8s 范围只覆盖 server。PG / Redis / Kafka / CH 全部留 host 网络（ExternalName 路由）。后端 K8s 化留 v0.7+。这条边界后面会成为 Day 28 真隐患的伏笔（埋了 Kafka K8s advertised listener 的雷）。

## 3. 三大无状态化连锁拍板（决策稿 §8）

K8s 多副本是表象，真正的难度是 server 无状态化的三连锁问题：

| 连锁 | 问题 | 拍板 | 兜底 |
|---|---|---|---|
| **§8.1 id 号段共享** | 多 Pod 进程内号段会发重复 ID | **Redis INCRBY**（50μs / 50 LOC / AOF） | spike P99 < 100μs；不达回退 PG 号段表 |
| **§8.2 L1 cache 一致性** | Pod A 写后 Pod B 读到旧值 | **接受短期 miss + L1 TTL 5s 上限** | 0 改动主路径；删除接口加入再走 pub/sub |
| **§8.3 Pod 滚动 0 漏** | SIGTERM 期间 in-flight + Kafka batch 丢 | **S1-S7 组合拳**（preStop / readiness 503 / fasthttp.Shutdown / KafkaProducer.Close / drain 25s / maxSurge=1 / 1 Pod 不并发）| spike 单 Pod kill |

每条都是分布式系统教材的核心三件套，v0.6 一次解掉。

## 4. Phase 1 单 Pod 跑通：distroless 22.3MB + ExternalName 路由 4 后端

Dockerfile multi-stage Go build + `distroless/static-debian12:nonroot` base，image 22.3MB / 无 shell / 无 package mgr / runtime safe。

K8s 部署：

- `namespace.yaml` + `deployment.yaml` (replicas=1) + `service.yaml` (NodePort)
- 4 个 ExternalName Service：`pg / redis / kafka / clickhouse` → `host.docker.internal` 让 Pod DNS 透明访问 host docker compose 栈
- kind cluster + `kind-config.yaml` extraPortMapping 让 host:18080 → NodePort 30080

第一次跑 wrk 通过 `kubectl port-forward` 只跑 22k RPS。怀疑 fasthttp / distroless image 退化。

但 docker stats 看到 **Pod CPU 仅 6%**——立刻定位是 port-forward SPDY 单通道瓶颈，不是 Pod 慢。改 NodePort + extraPortMapping → **44k RPS（1.95× 提升）**。CPU bump 2→4 核 RPS 不变 = 网络层（iptables DNAT + bridge）固有上限。

**单 Pod 物理上限 ~44k = v0.5 baseline 93k 的 47%**。这是 kind on macOS Docker Desktop dev 环境网络层固有 overhead，不是 slink 退化（Pod CPU < 6% 是硬证据）。Phase 3 真集群验才有可比性。

诚实写在 retro 里 = 不能伪装"差不多达标"。

## 5. Phase 2 三连锁全部落地（同日第 5 段）

### 5.1 §8.1 Redis INCRBY 现实校准

3 workers × 200 acquires spike：

| 指标 | 实测 | 原 §8.1 估值 |
|---|---:|---:|
| P50 | 474μs | 50μs |
| **P99** | **1295μs** | **< 100μs** |
| Max | 3451μs | — |
| 0 重复 / 0 漏 | ✅ | — |

**撞标准 13×**。第一反应不是"升级到 P99 < 2ms"包装，而是回 spec 找根因（meta-cognition §3 现场触发）：

- 原 §8.1 引用「Redis INCRBY 50μs」是裸进程内数字
- 实测 docker compose host network TCP round-trip 主导（~470μs），Redis 服务端单 INCRBY 处理仅 ~10μs（占总耗时 < 5%）
- **网络不是 Redis 本身的锅**

修订决策稿 §8.1 标准：P99 < 100μs → < 2ms。**保留 Redis INCRBY 不回退 PG**——stepSize=1000 + 双 buffer 异步预取让拿号段阻塞 P99 完全掩盖，不上主路径 P99。

多 Pod EnsureMinimum 启动期 Lua CAS-on-larger 兜底：3 Pod bootstrap 全部 pg_floor=3000 → Pod A [3001,4000] / Pod B [4001,5000] / Pod C [5001,6000] 0 重复。

### 5.2 §8.2 跨 Pod L1 一致性

200 samples Pod A 创建 → Pod B 读 spike，0 失败 P99 11ms < 30ms 标准。99.98% L1 命中保留。

### 5.3 §8.3 优雅停机时序 5.001s

distroless 镜像无 `/bin/sh`，原 yaml `preStop exec sleep 5` 不可用。

三选一里选 **C：Go SIGTERM handler 内 sleep**（vs A 换 distroless-debug +22MB 攻击面 / B Sidecar 容器开销）。

实测时序：

```
SIGTERM → readiness 503 → drain 5s → fasthttp 0 in-flight → kafka close → bye
t=0       t=0              t=5s         t=5s                  t=5s          t=5.001s
```

**5.001s 总闭环 << 30s grace period**。

反直觉收获：distroless 强制把时序进 Go 进程是**好事**——比 yaml preStop hook 可控 100×，时序可断言、可日志、可测。

### 5.4 滚动期 60s wrk + delete 1 Pod

| 指标 | 静态 | 滚动期 |
|---|---:|---:|
| RPS | 42,277 | 41,964 (-0.7%) |
| P99 | 18.16ms | 16.94ms (-7%) |
| **Timeout** | 0 | **0** ⭐ |
| 5xx | 0 | 0 |

3 副本 RPS 42k 没翻倍。诊断：后端 CPU 全闲（PG 0.02% / Redis 0.92% / Kafka 1.74% / CH 6.78%）+ kind container CPU 16% + Pod CPU < 6% = kind on macOS NodePort 网络层固有上限。

不强行调（fix 该 fix 的，不 fix 该接受的）—— 归档解读 + Phase 3 真集群验。

**Phase 2 落档"成功"**。

## 6. ★ Phase 3 开工抓到 — Kafka K8s producer 100% silent drop

Day 28 起手按 Phase 3 计划：3 轮故障演练（delete pod / scale 0/3 / cordon+drain）。

但开工第一动作不是 inject 故障，而是按 v0.5 留账的 recon-fixture（即使有时间窗 bug）做端到端对账。**Day 27 决策稿 §7 标 "recon-fixture Phase 4 顺手做"是软规则；Day 28 强制升级为演练标准步骤**。

业务闭环 smoke 跑完，查 admin stats（NodePort 16060）：

```json
{
  "kafka_producer": {
    "sent": 186368,
    "acked": 0,
    "dropped": 186368,    // ⚠️ 100% dropped！
    "healthy": true
  }
}
```

**healthy=true 但 acked=0 / dropped=100%**。

立刻 rollout restart Pod 拿 fresh stats：`sent=6 / acked=0 / dropped=6` 仍然 100% dropped。

回头看 Day 27 演练时 stats `Sent:0 Acked:0 Dropped:0 Healthy:true`——SIGTERM 前业务没真请求 click_events，全 0 看着像 healthy 实际上是 vacuous truth。

**Day 27 Phase 2 测试有盲点**：只看 RPS / timeout / 5xx，没做端到端对账。v0.6 K8s 路径 Kafka producer 从 Phase 1 起从未工作过——**~3 day silent failure**。

诊断根因：

```
# Pod 内 SLINK_KAFKA_BROKERS=kafka:19092
# K8s Service kafka 是 ExternalName → host.docker.internal:19092
# host 上 kafka 暴露 19092 / advertised=localhost:19092
```

**Kafka 协议特性**：client 第一次连 broker 走 ExternalName 路径到 host 没问题；但 Kafka 在 metadata 响应里告诉 client「broker 真实地址 = `localhost:19092`」。Pod 收到后 dial **Pod 内自己的 localhost:19092** → 永远连不通 → send_timeout 100ms 触发 → 全 dropped。

K8s ExternalName Service 只解决 DNS 路由；**Kafka 协议 metadata redirect 这层 K8s 抽象拦不住**。

## 7. 修选项 A：加独立 EXTERNAL_K8S listener

三选一：

| 选项 | 改动 | 副作用 | 决策 |
|---|---|---|---|
| **A 加 EXTERNAL_K8S listener** | docker-compose + deployment env | 0（host cmd/consumer 仍走 :19092）| **✅ 选** |
| B EXTERNAL advertised 直接改 host.docker.internal:19092 | 1 行 | host cmd/consumer 绕路（仍通但多跳）| 不选 |
| C 接受 Kafka K8s 不通仅业务面 | 0 | "0 漏"无法验证 vacuous truth | 不选 |

修法：

```yaml
# docker-compose.yml
ports:
  - "19092:19092"      # 原 EXTERNAL → host cmd/consumer
  - "19093:19093"      # 新 EXTERNAL_K8S → K8s Pod
environment:
  KAFKA_LISTENERS: "...,EXTERNAL_K8S://:19093,..."
  KAFKA_ADVERTISED_LISTENERS: "...,EXTERNAL_K8S://host.docker.internal:19093"
```

```yaml
# deploy/k8s/20-deployment.yaml
- name: SLINK_KAFKA_BROKERS
  value: "kafka:19093"            # 原 :19092
- name: SLINK_KAFKA_SEND_TIMEOUT
  value: "500ms"                   # 原 100ms（Day 20-21 教训：docker network 下 100ms timeout 太紧）
- name: SLINK_KAFKA_MAX_BUFFERED
  value: "200000"
```

修后验证：fresh Pod 20 个 GET → `sent=20 / acked=20 / dropped=0` ✅。

## 8. 修后 3 轮独立演练全过

| Round | 故障 inject | reqs | timeout | 5xx | PG ⇄ CH drift |
|---|---|---:|---:|---:|---:|
| Round 1 | `kubectl delete pod` 单杀 | 1,368,573 | **0** | **0** | **0.0000** ⭐ |
| Round 2 | `scale --replicas=0/3` 全杀 9s | 562,449 | **0** | **0** | **0.0000** ⭐ |
| Round 3 | `cordon` + `drain` 节点撤场 8s | 290,360 | **0** | **0** | **0.0000** ⭐ |
| **总计** | — | **2,221,382** | **0** | **0** | **3× 0.0000** ⭐⭐⭐ |

三个独立窗口端到端 PG ⇄ CH drift 全 0.0000。这次的"通过"才不是 vacuous truth。

但仍需要诚实标注语义：

- **wrk timeout=0 ≠ 业务可用性 100%**：connection refused 算 read err 不算 timeout。R2 全停期 read err 22k = 真停服 ~30s
- **0 漏的 vacuous 微妙**：scale=0 期间没请求 → 没事件 → 不需要兑现 → drift=0 是真实但 vacuous（成功请求的事件全到位才是真可靠）
- **kind 单节点 cordon 全停 ≠ 真生产**：真集群多节点 drain 一个节点 Pod 自动迁移，业务不停。Phase 3 留 v0.7 真集群再验

## 9. 反直觉发现 + 简历素材

### 反直觉发现

1. **"通过演练" ≠ "可靠性达标"**：RPS+timeout+5xx 是必要条件不是充分条件。**端到端对账是充分条件**。Day 27 因为缺这一步让 Kafka 100% silent drop 持续 ~3 day。
2. **K8s ExternalName 拦不住 Kafka 协议特性**：metadata redirect 让 Pod client 看到 broker advertise 的地址。生产环境若 Kafka 在 K8s 外，必须配 K8s 专用 listener。常见踩坑（不只 slink）。
3. **Redis INCRBY P99 1.3ms ≈ PG 1ms 同口径**：跨容器网络对所有 KV/RDBMS 一视同仁吃 ~500μs round-trip。Redis 仍占优在于单命令 vs PG 三段事务，但差距不是 20× 而是 ~2×。
4. **3 副本 RPS 没翻倍但优雅停机 0 timeout**：性能没赚到，可靠性赚到。Phase 2 真正解锁的是「滚动期不漏单」而不是「RPS 多副本叠加」。后者 v0.7 真集群解锁。
5. **distroless 强制 Go 内时序是好事**：被迫把 S1-S4 全做进 Go 进程比 yaml preStop hook 可控 100×，可断言、可日志、可测。

### 简历素材（v7 已 paste）

- **量化**："K8s 多副本 + 滚动期 60s wrk + 3 轮独立故障演练，**总 2.22M reqs / 0 timeout / 0 5xx / PG⇄CH drift × 3 次 0.0000**"
- **架构**："server 容器化 distroless 22.3MB + replicas=3 + ExternalName 路由后端 + S1-S7 优雅停机时序 5.001s"
- **真事件**："抓到 Kafka K8s 外部部署 advertised listener 经典踩坑——v0.6 Phase 1 起 Pod producer 100% silent drop 持续 ~3 day，Day 28 通过补端到端对账暴露 + 选项 A 修复 + 沉淀通用 wiki"
- **工程化纪律**："演练设计 client 面 metric 是必要不充分条件，端到端对账才是充分条件——v0.6 Day 27 演练'通过'是 vacuous truth 的反面教材"

### 元认知一句话

v0.5 撞 DETACH MV 真破坏教会我"演练 inject 类型决定故障性质"；v0.6 撞 silent drop 教会我"演练验证维度决定演练充分性"。两者合起来 = 演练设计必须先列"什么算成功 / 什么算失败 / 用什么 metric 判定"，再 inject。

下一站 v0.7：真集群（GKE/EKS）验 RPS 可比性 + Phase 4（OTel / NetworkPolicy / AuthN / consumer 入 K8s）收口。

---

**仓库**：https://github.com/zombiecd/slink
**配套文档**：`docs/architecture/v0.6-k8s.md`（11 节决策稿）/ `docs/retrospect/v0.6-retro.md`（7 张脸）/ `docs/bench/day-28-rolling-drill.md`（演练数据归档）
**沉淀 wiki**：可靠性演练必须端到端对账 / Kafka K8s 外部部署 advertised listener 配置
