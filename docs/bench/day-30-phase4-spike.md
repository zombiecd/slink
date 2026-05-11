# Day 30 — Phase 4 spike 5 项预拍板

> 2026-05-11 / Day 30 / Phase 4.1 起步
> 仿 v0.6-k8s.md §8 模板，但 Phase 4 是配置层不是性能选型，**直接拍板不真跑 spike**（节省 Day 30 单日打穿时间盒）

## 拍板表

| 项 | 候选 | 选 | 理由 | 风险 / 兜底 |
|---|---|---|---|---|
| **OTel SDK 注入位置** | code-level（应用代码内）/ auto-instrumentation（agent 注入）| **code-level** | trace 完整可控 / Go 项目 SDK 标准做法 / agent 注入对 fasthttp 兼容性差 | LOC ~150 行（fasthttp middleware + Kafka producer/consumer hook + PG/Redis/CH client wrap）|
| **OTel collector 部署形式** | sidecar（每 Pod 一个）/ DaemonSet / 独立 Deployment | **独立 Deployment** | kind 单节点 sidecar 浪费资源 / 配置统一 / collector 重启不影响业务 Pod | sidecar 模式留 v0.7 真集群多节点验 |
| **Ingress controller** | nginx-ingress / traefik | **nginx-ingress** | 生态最广 / AuthN annotation 模块完整 / kind 官方支持 manifest | 装机超 30min 立 pivot traefik |
| **AuthN 方案** | basic auth + Secret(htpasswd) / oauth2-proxy | **basic auth + Secret** | 个人项目 oauth2-proxy 过度设计 / Secret 已有 K8s native 管理 / nginx-ingress annotation 一行启用 | 生产场景留 v0.7 oauth2-proxy 升级 |
| **consumer 部署 spec** | Deployment + replicas=2 / StatefulSet | **Deployment + replicas=2** | 无状态（kafka offset 在 broker）/ 滚动语义清晰 / Kafka group rebalance 自然处理 partition | replicas=2 应对 Kafka 4 partitions 各 2 partition / 单 Pod 挂另一 Pod 接管 |

## 与 v0.6-phase4.md §7 决策一致性核对

| Phase 4 决策稿 §7 项 | spike 拍板 | 一致 |
|---|---|---|
| Phase 4 vs v0.7 起步 | Phase 4 现在做 | ✅ |
| OTel 优先级 | 必做（4.1 子阶段） | ✅ |
| AuthN 方式 | basic auth | ✅（决策稿留 oauth2-proxy 候选 spike 后定，本表确认 basic auth） |
| NetworkPolicy 启用 | 默认 deny + 白名单 | ✅ |
| consumer 副本数 | 2 | ✅ |
| consumer 部署 spec | Deployment | ✅ |
| recon-fixture 重写时机 | Phase 4.4（段 5）| ✅ |
| Phase 4 演练范围 | server + consumer 5 轮 | ✅ |

## Day 30 单日打穿后续段编排

按本拍板表，后续 6 段（段 2-7）执行路径已明确：

- 段 2：OTel code-level 接入 + 独立 Deployment collector + Jaeger UI 验证
- 段 3：nginx-ingress 装 + basic auth Secret + Ingress 资源 yaml
- 段 4：consumer Dockerfile + Deployment replicas=2 + NetworkPolicy 默认 deny + 白名单
- 段 5：recon-fixture v2 时间窗算法重写
- 段 6：5 轮演练（v0.6 三轮复跑 + Round 4 consumer 单杀 + Round 5 consumer 全杀 Kafka rebalance）
- 段 7：commit + tag v0.6-final + push origin

## 时间盒

每项决策上限：5min（决策稿已封板，spike 是确认不是探索）。本 spike 文档总耗时 < 30min。

实测耗时：~10min（决策已封板，本文是写归档）。

---

**版本**：v1.0 · 2026-05-11
