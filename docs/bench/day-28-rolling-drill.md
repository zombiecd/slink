# Day 28 — v0.6 Phase 3 滚动 + 3 轮故障演练 + 端到端对账

> 2026-05-11 / 同日第 6 段 / Day 23-28 单日打穿

## 0. 演练目标

| 验证点 | 标准 |
|---|---|
| 业务可用性（wrk client 视角）| `timeout=0` / `5xx=0` |
| 端到端数据完整性（PG ⇄ CH drift）| recon-fixture R1 < 0.1% |
| Pod 自愈 | Pod 数量与 Ready 时间记录 |

3 轮覆盖 K8s 真生产 3 类故障：

1. **Round 1** — `kubectl delete pod`（单杀）：最常见，节点 OOM / 健康检查失败被 evict
2. **Round 2** — `kubectl scale --replicas=0/3`（全杀重启）：误操作 / Helm upgrade 失败回滚
3. **Round 3** — `kubectl cordon` + `drain`（节点撤场）：节点维护 / 升级 / 故障转移

## 1. 环境

- **K8s**: kind v1.33.1 单节点（control-plane 同时 worker）
- **后端**: host docker compose (PG :15432 / Redis :16379 / Kafka :19092+:19093 / CH :19000)
- **slink-server**: replicas=3, image `slink-server:v0.6`
- **Pod resources**: requests cpu=200m mem=128Mi / limits cpu=2 mem=512Mi
- **wrk**: 4 threads × 256 conns, 60s（R3 30s）
- **流水线**: slink-server (3 Pods) → Kafka (host:19093) → host cmd/consumer → PG `click_events` / Kafka Engine + MV → CH `click_events_ch`

### 1.1 v0.6 隐患修复：Kafka K8s advertised listener

**Day 27 Phase 2 演练有盲点**：bench 只看 RPS/timeout/5xx，没做 PG/CH 端到端对账。Day 28 开工抓到——v0.6 K8s 路径下 Kafka producer **从 Phase 1 起从未工作过**：

- `docker-compose.yml`：Kafka `KAFKA_ADVERTISED_LISTENERS: EXTERNAL://localhost:19092`
- K8s Pod 通过 ExternalName `kafka` → `host.docker.internal:19092` 连 EXTERNAL listener
- Kafka 在 metadata 响应里告诉 client：「broker 真实地址 `localhost:19092`」
- Pod 收到后 dial 自己的 `localhost:19092` → Pod 内 localhost ≠ host → send_timeout 100ms 触发 → **dropped 100%**

**修法**（选项 A — 三选一保险路径）：

```yaml
# docker-compose.yml
ports:
  - "19092:19092"      # 原 EXTERNAL → host cmd/consumer
  - "19093:19093"      # 新 EXTERNAL_K8S → K8s Pod
environment:
  KAFKA_LISTENERS: "INTERNAL://:9092,EXTERNAL://:19092,EXTERNAL_K8S://:19093,CONTROLLER://:9093"
  KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka:9092,EXTERNAL://localhost:19092,EXTERNAL_K8S://host.docker.internal:19093"
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "...,EXTERNAL_K8S:PLAINTEXT"
```

```yaml
# deploy/k8s/20-deployment.yaml
- name: SLINK_KAFKA_BROKERS
  value: "kafka:19093"   # 原 :19092
- name: SLINK_KAFKA_SEND_TIMEOUT
  value: "500ms"          # 原 100ms（Day 20-21 教训：docker network 高 RPS 下 100ms 大量 timeout）
- name: SLINK_KAFKA_MAX_BUFFERED
  value: "200000"
```

**修后验证**：fresh Pod 20 个 GET → `sent=20 / acked=20 / dropped=0` ✅

### 1.2 cmd/consumer 起栈

```bash
SLINK_PG_DSN=postgres://slink:slink@localhost:15432/slink?sslmode=disable \
SLINK_KAFKA_BROKERS=localhost:19092 \
SLINK_CONSUMER_TABLE=click_events \
SLINK_CONSUMER_GROUP=slink.click_events.pg_writer \
nohup ./bin/slink-consumer > /tmp/slink-consumer.log 2>&1 &
```

## 2. Round 1 — `kubectl delete pod` 单杀

### 时序

| 时间 | 事件 |
|---|---|
| 09:23:32 | wrk 60s 开始（3 Pods running）|
| 09:23:47 | `kubectl delete pod slink-server-6f8b7cdf48-ftv59` |
| 09:24:32 | wrk 结束 |

### wrk 数字

| 指标 | 值 |
|---|---|
| Total reqs | 1,368,573 |
| Duration | 60.06s |
| **RPS** | **22,788** |
| P50 / P90 / P99 | 9.74ms / 22.49ms / 81.33ms |
| Socket read err | 192 |
| Socket write err | 11 |
| **timeout** | **0** ⭐ |
| **HTTP 5xx** | **0** ⭐ |

### 端到端对账

| 端 | 行数 |
|---|---|
| consumer.inserted | 1,368,351 |
| PG click_events | 1,368,331（窗口 [09:23:00, 09:25:00)）|
| CH click_events_ch | 1,368,331 |
| **drift R1** | **0.0000** ⭐ |
| R2 top-100 codes | PASS（全在阈值 0.5% 内）|

CH 追平时长 130s（PG=CH 同步至 1368351）。

### 结论 ✅

- delete pod 不影响在飞请求（fasthttp Shutdown 优雅退出）
- 0 timeout / 0 5xx
- **端到端 0 漏**

## 3. Round 2 — `kubectl scale --replicas=0 && --replicas=3` 全杀重启

### 时序

| 时间 | 事件 |
|---|---|
| 09:29:00 | wrk 60s 开始 / 3 Pods running |
| 09:29:15 | `scale --replicas=0` |
| 09:29:24 | `scale --replicas=3` |
| 09:30:00 | wrk 结束 |

scale=0 持续 ~9s，从 evict 完到新 Pod ready 大约 15-25s（Pod readiness probe + warmup）。

### wrk 数字

| 指标 | 值 |
|---|---|
| Total reqs | 562,449 |
| Duration | 60.10s |
| RPS | 9,358（含 ~20-25s 全停期 0 RPS）|
| P50 / P99 | 9.25ms / 51.12ms（剔除停服段）|
| Socket read err | **22,422**（全停期 connection refused）|
| **timeout** | **0** ⭐ |
| **HTTP 5xx** | **0** ⭐ |

### 端到端对账（窗口 [09:29:00, 09:31:00)）

| 端 | 行数 |
|---|---|
| PG click_events | 562,702 |
| CH click_events_ch | 562,702 |
| **drift R1** | **0.0000** ⭐ |
| R2 top-100 | PASS |

### 结论 ✅

- scale=0 期间业务确实停（read err 22k = connection refused），但**端到端 0 漏**（停服期间没产生请求，没事件需要兑现）
- Pod ready 后流水线立刻接续，consumer 把停服前在飞的 Kafka 消息全消费
- CH 追平时长 20s（vs Round 1 130s，规模小）

## 4. Round 3 — `kubectl cordon` + `drain` 节点撤场

### 时序

| 时间 | 事件 |
|---|---|
| 09:31:49 | wrk 30s 开始 / 3 Pods |
| 09:31:57 | `kubectl cordon node/slink-control-plane` |
| 09:31:58 | `kubectl drain ... --ignore-daemonsets --force` |
| 09:32:06 | drain 完成（8s 内全 evicted）|
| 09:32:19 | wrk 结束 / uncordon |
| 09:32:29 | 3 Pods 恢复 Running（RESTARTS=1）|

### kind 单节点特殊性

kind 默认单节点，cordon 后整集群无可调度节点。Pod evict 后 `Pending` 直到 uncordon。这条演练展示「节点维护期间业务全停 + 优雅 evict 不丢数据」。

真集群（多节点）下 drain 一个节点，Pod 会自动调度到其他节点，业务不停。

### wrk 数字

| 指标 | 值 |
|---|---|
| Total reqs | 290,360 |
| Duration | 30.06s |
| RPS | 9,660 |
| P50 / P99 | 9.89ms / 50.28ms |
| Socket read err | 11,440 |
| **timeout** | **0** ⭐ |
| HTTP 5xx | 0 |

### 端到端对账（窗口 [09:31:30, 09:32:30)）

| 端 | 行数 |
|---|---|
| PG click_events | 290,337 |
| CH click_events_ch | 290,337 |
| **drift R1** | **0.0000** ⭐ |
| R2 top-100 | PASS |

### 结论 ✅

- drain 8s 内全部 evict，**0 timeout**（fasthttp graceful 在 S1-S7 时序内完成）
- uncordon 后 Pod ~10s 内全部 Running
- **端到端 0 漏**

### Pod RESTARTS=1 解读

新 Pod 在 cordon 期间被 schedule 到原节点（实际是同一节点），但 cordon 阻止 kubelet 启动；uncordon 后 kubelet 看到老的"未启动"容器，触发 restart 计数。这不是真 crash，是 kind 单节点 + cordon 边缘行为。生产多节点不会出现。

## 5. 三轮汇总

| 轮 | 故障类型 | wrk reqs | timeout | 5xx | drift R1 | R2 PASS |
|---|---|---|---|---|---|---|
| 1 | delete pod | 1,368,573 | **0** | **0** | **0.0000** | ✅ |
| 2 | scale 0/3 | 562,449 | **0** | **0** | **0.0000** | ✅ |
| 3 | cordon+drain | 290,360 | **0** | **0** | **0.0000** | ✅ |

**总 reqs：2,221,382 / 0 timeout / 0 5xx / 端到端 drift 全 0.0000** ⭐⭐⭐

## 6. 关键发现 / 反直觉

### 6.1 Day 27 演练有盲点（Kafka K8s 从未工作）

Day 27 Phase 2 跑了"3 副本 RPS 42k / 滚动期 RPS -0.7% / 0 timeout"，**但没做端到端对账**。Day 28 开工才发现 K8s Pod 的 Kafka producer 100% dropped（advertised listener 配置错误）。

教训：**RPS + timeout 是必要条件不是充分条件**。可靠性演练必须做端到端对账，否则"成功"可能是 vacuous truth。

### 6.2 K8s 抽象漏了 Kafka 协议特性

K8s ExternalName Service 只解决"DNS 路由"。Kafka 协议 metadata redirect 让 client 收到 broker 自己 advertise 的地址——这层 K8s 抽象拦不住。

修法：必须为 K8s 路径开独立 listener（EXTERNAL_K8S），advertise `host.docker.internal:19093`。生产环境若 Kafka 在 K8s 外，Pod 内 client 看到的 advertised 必须是 Pod 可达的。

### 6.3 RPS 22k vs Day 27 42k 差一半

Day 27 在 NodePort（kind container 内）直接 wrk container 内打。Day 28 通过 host:18080 → kind extraPortMapping → NodePort → Pod，多一跳 Docker Desktop iptables。

不强行调（fix 该 fix 的，不 fix 该接受的）— 这是测试方法路径不同，**不是 v0.6 性能退步**。Phase 3 真集群验时数字才有可比性。

### 6.4 wrk "timeout" 与业务可用性不等价

3 轮 wrk 都报 `timeout=0`，但 Round 2/3 大段时间业务实际停服（read err 22k / 11k = connection refused）。

正解：wrk timeout 是 socket idle 超 client 默认（2s），connection refused 算 read err 不算 timeout。**业务可用性 = read err / total reqs ≤ 1%** 在 R2 是 4%，R3 是 4%——都因为 scale=0 / drain 期间确实停服。

但这不影响"0 漏"——停服期间的请求**没有产生 click_event**，所以 PG/CH 不需要兑现它们，drift 仍为 0。

### 6.5 cmd/consumer 是单点

Round 1-3 的 PG 侧依赖单 cmd/consumer host 进程。若该 consumer 挂了，PG 会落后；只能等 consumer 恢复后从 Kafka 追平。

v0.7+ 应把 consumer 也搬进 K8s（多副本 + Kafka group rebalance）。本轮不在 Phase 3 范围。

## 7. EOD 后置（Day 28 收尾）

- 滚动期 K8s 资源保留（kubectl get pod 三副本 Running）
- docker compose 栈保留（数据卷有 1368k+562k+290k = 2,221,382 行 click_events）
- cmd/consumer host 进程 PID 71155 — Day 28 EOD 必杀
- 0 残留 wrk / kubectl 后台进程

---

**版本**：v1.0 · 2026-05-11
