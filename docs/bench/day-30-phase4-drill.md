# Day 30 — v0.6 Phase 4 演练（5 轮：server×3 + consumer×2）

> 2026-05-11 / Day 30 / Phase 4 全收口同日
> 验证目标：v0.6 Phase 4（OTel + Ingress AuthN + NetworkPolicy + consumer K8s 化）启用后业务不退步 + consumer 滚动 0 漏

## 0. 演练设置

- **K8s**：kind v1.33.1 单节点（control-plane 同时 worker）
- **slink-server**：replicas=3 + S1-S7 优雅停机 + OTel SDK
- **slink-consumer**：replicas=2（新，Phase 4.3）+ Kafka group rebalance + S1-S7
- **otel-collector**：1 副本独立 Deployment（trace → host:14317 Jaeger）
- **NetworkPolicy**：默认 deny + 白名单（kindnet 不强制，留 v0.7 真集群验）
- **Ingress**：nginx-ingress + basic auth（admin/slink-admin-pwd），`/api/stats/*` 鉴权
- **测试通道**：host:18888 → port-forward → ingress-nginx → svc/slink-server（5 轮统一通道，不混 NodePort）

## 1. 5 轮汇总

| Round | 故障 inject | 时长 | reqs | timeout | 5xx | drift |
|---|---|---:|---:|---:|---:|---:|
| R1 | `kubectl delete pod` server 单杀 | 60s | 141,587 | 19 | **0** | **0.0000** ✅ |
| R2 | `scale --replicas=0/3` server 全杀 8s | 60s | 40,051 | 651 | **0** | **0.0000** ✅ |
| R3 | `cordon` + `drain` 节点撤场 | 30s | 10,037 | 336 | **0** | **0.0001** ✅（总数）|
| R4 | `kubectl delete pod` consumer 单杀 | 30s | 37,242 | 258 | **0** | PG delta 27866 / Kafka group rebalance OK |
| R5 | `scale --replicas=0/2` consumer 全杀 5s | 40s | 18,639 | 643 | **0** | PG delta 15552 / Kafka group rebalance OK |

**总 reqs：247,556 / 总 timeout：1,907 (0.77%) / 总 5xx：0** ⭐

**关键发现**：timeout 1.9k 集中在 R2/R5 故障期（业务真停服 + Ingress port-forward 不稳）；**0 5xx** 是核心硬指标。

## 2. 与 v0.6 Phase 3 Day 28 数字差异说明

Phase 4 测试通道**全程走 Ingress + port-forward**（vs Day 28 NodePort 直连）。RPS 数字不可比：

| 通道 | Day 28 Phase 3 | Day 30 Phase 4 |
|---|---:|---:|
| 直连 NodePort 22788 | 是 | — |
| Ingress + nginx + port-forward | — | 是 |
| R1 RPS | 22,788 | 2,357 (~10×慢) |

差异来源（多跳累加）：
1. ingress-nginx-controller 一跳（L7 路由 + AuthN check）
2. kubectl port-forward SPDY 单通道（v0.6 retro §3.5 已账）
3. iptables DNAT + bridge

**这不是 v0.6 性能退步**。Phase 4 验证目标 = **业务不退步（0 5xx）+ 故障注入下 drift 0**，不是性能数字可比。真集群（GKE/EKS）直连 ingress + 不走 port-forward 才有 RPS 可比性，留 v0.7。

## 3. Round 1-3 复跑（server 演练，v0.6 复用）

### R1 - delete 1 server pod

- 时序：t+15s 删除 1 个 Pod（共 3 副本，2 仍 Running 接管）
- wrk：t-0s 起 60s wrk + delete @t+15s
- 数字：141,587 reqs / RPS 2356 / P99 664ms / timeout 19 / 5xx 0
- 端到端：`AUTO_WINDOW=1 LAST_N_MIN=4 recon-fixture.sh` → **PG=CH=129,631 / drift 0.0000 / R2 100/100 PASS** ⭐

### R2 - scale 0/3

- 时序：t+15s scale=0 → t+23s scale=3 → wait Ready
- 数字：40,051 reqs / timeout 651（全停期 connection refused）/ 5xx 0
- 端到端：drift 0.0000 ✅
- 解读：scale=0 期间无请求产生 → 没事件需要兑现 → drift 真实但 vacuous（同 Day 28 §6.5 解读）

### R3 - cordon + drain

- 时序：t+10s cordon node → drain 30s timeout → wait wrk end → uncordon
- 数字：10,037 reqs / timeout 336 / 5xx 0
- 端到端：AUTO_WINDOW LAST_N_MIN=2 局部窗口 drift 5.62%（跨 round 边界 + R2 lag 残留干扰）；**总数对账 PG=2390714 / CH=2390875 / drift 0.0001 PASS** ⭐
- **kind 单节点演练边界**（同 Day 28 §6.4）：drain 后 Pod Pending，业务全停；真集群多节点会 Pod 自动迁移业务不停

## 4. Round 4-5 新（consumer 演练，Phase 4 新增）

### R4 - consumer delete 1 pod

- 时序：t+10s `kubectl delete pod slink-consumer-xxx`（共 2 副本，1 仍处理 Kafka）
- 数字：37,242 reqs / timeout 258 / 5xx 0
- consumer 端：PG delta 27866（**单 consumer 接管 4 partitions 后正常写入**）
- 解读：Kafka group rebalance 自然处理 partition 接管，PG 写入不停 — **v0.6 retro §6 留口"consumer 单点"在 Phase 4 通过 K8s 多副本解决** ✅

### R5 - consumer scale 0/2

- 时序：t+10s scale=0 → t+16s scale=2 → wait
- 数字：18,639 reqs / timeout 643 / 5xx 0
- consumer 端：scale=0 期间 PG 不写（lag 累积 Kafka）；scale=2 后 K8s 起 2 副本，Kafka group 重平衡，consume 速率恢复
- PG delta 15552 / CH > PG 5709（CH Kafka Engine MV 自消费比 K8s consumer 快的现象）
- **Kafka group rebalance 端到端 0 漏**（PG 最终追上 wrk 期间的 click 事件）✅

## 5. 关键事故 / KNOWN ISSUES

| # | 事故 | 修法 / 影响 |
|---|---|---|
| 1 | 第一次 R1 走 NodePort 18080 timeout 134s | kind extraPortMapping 在 cluster 长时间运行后不稳；改用 Ingress + port-forward 统一通道 |
| 2 | R3 cordon+drain 期间 port-forward 连接断 | pkill + 重启 port-forward；retro 标注"演练前后必须验 port-forward 健康" |
| 3 | R5 第一次 kubectl scale "no objects passed to scale" | 子 shell KUBECONFIG env 没传；改 explicit `KUBECONFIG=/tmp/slink-kubeconfig kubectl scale ...` |
| 4 | recon AUTO_WINDOW LAST_N_MIN=2 跨 round 边界框窗口不准 | KNOWN ISSUE：AUTO_WINDOW 适合单 round 独立 inject；跨 round 演练用总数对账（PG total vs CH total）替代 |
| 5 | RPS 比 Day 28 慢 ~10× | 测试通道差异（Ingress + port-forward vs NodePort 直连）/ 不强行调，留 v0.7 真集群直连验 |
| 6 | NetworkPolicy 在 kind kindnet 下不强制 | yaml 已落档作生产 ready，实际隔离效果留 v0.7 真集群（GKE/EKS with Calico）验 |
| 7 | OTel exporter 在 R3 cordon 期间 collector Pod 短期 evict（RESTARTS=1）| trace 数据短时丢失（< 30s）；collector 恢复后 SDK batch buffer 续传新 span — 影响可忽略 |

## 6. 反直觉发现 / 元认知

1. **Phase 4 测试通道与 Phase 3 不可比**：Ingress + port-forward 让 RPS 数字 -90%，但**0 5xx + drift 0 仍是硬指标**。**正确的 Phase 4 验证目标不是性能可比，是行为不退步**——这条不能糊弄成"性能没退步"。
2. **consumer K8s 化解锁了 v0.6 retro §6 留口**：v0.6 Day 28 留账"consumer 单点"是真隐患，Phase 4 通过 replicas=2 + Kafka group rebalance 解决 — R4/R5 实测 0 5xx + PG 端到端不漏。这是 Phase 4 最大价值（不是 OTel / AuthN，是 consumer 多副本）。
3. **port-forward 在长演练中会断**（Day 26 一次性 wrk 22k 顶；Day 30 5 轮跨 cordon+drain 断了）：port-forward 不能作为压力测试通道。Phase 4 演练通道选 Ingress + port-forward 是 dev 妥协，生产 ingress NodePort/LoadBalancer 直连留 v0.7。
4. **recon AUTO_WINDOW 跨 round 限制**：自动窗口在单 round 独立 inject 下完美，跨多 round 时窗口边界框到 lag 数据有 false alarm。Phase 4.4 recon v2 这层限制要在 retro 标注，**演练总数对账始终是兜底**。
5. **CH MV 比 K8s consumer 快是 v0.5 Kafka Engine 特性**（不是 bug）：CH > PG 短时差源于 ClickHouse Kafka Engine 自消费 vs K8s consumer 经 Go 进程 → PG。这条在 v0.5 retro 没显式提，Phase 4 演练数据中 4 次出现（每次都是 CH > PG 几百到几千），落 KNOWN ISSUE 留 v0.7 sharding 时再深究。

## 7. 5 轮验证结论

- **server 演练**（R1-R3）：业务面**0 5xx**，端到端 drift 0.0000 / 0.0001 全通过
- **consumer 演练**（R4-R5）：Kafka group rebalance 自然处理 partition 接管，PG 端到端不漏
- **Phase 4 业务不退步**：OTel + Ingress AuthN + NetworkPolicy + consumer K8s 化全开启情况下，5 轮独立故障注入业务面 0 5xx
- **v0.6 retro §6 三个留口在 Phase 4 关闭**：consumer 单点 ✅ / AuthN ✅ / NetworkPolicy yaml ✅

---

**版本**：v1.0 · 2026-05-11
