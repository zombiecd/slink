# slink

> A Go-native, single-binary, high-performance URL shortener built for the **大促 traffic spike** scenario — self-hosted, observable, and engineered to outlive your campaign.

[![Go](https://img.shields.io/badge/Go-1.24-00ADD8.svg)](https://go.dev)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-v0.1--wip-orange.svg)]()

---

## 这是什么

**slink** 是一个用 Go 写的自托管短链服务，目标是替代 bit.ly / 阿里云短链 / 腾讯云短链等收费 SaaS，在 **企业内部** 跑 **大促级流量**：

```
T-1 天 : 1000 万营销短信群发，含 https://o.cn/aB3xY9
T+0    : 短信触达，10 万 QPS 跳转峰值
T+实时 : 运营看大屏 — PV / UV / 城市分布 / TopK 短链
T+周   : 全文检索"双11 母婴落地页"过去 7 天点击
T+月   : 历史数据归档，仍可查
```

不是给个人用的玩具，不是 SaaS 平台。**给企业内部技术团队，自托管，数据握在自己手里。**

## 为什么需要

| 现成方案 | 痛点 |
|---|---|
| bit.ly | 数据出境，国内访问慢，大促被限流 |
| 阿里云/腾讯云短链 | 按量计费，大促一次烧 3000 元起 |
| 自己写 50 行 | 跑不到 5k QPS，大促直接挂 |
| 业界开源（YOURLS / Polr / Kutt） | 偏功能堆叠，不是工程化高并发方案 |

slink 的差异化定位：**Go 单二进制 / 零外部依赖（v0.1）/ 面向高并发场景 / 工程化压测 + 可观测**。

## 当前状态

🚧 **v0.1 开发中** — 不要在生产环境使用。

| 版本 | 状态 | 内容 |
|---|---|---|
| v0.1 | 🚧 In Progress | 单机 API + 跳转 + Redis 缓存 + 异步入 PG + Docker 部署 + wrk 压测 |
| v0.2 | 📋 Planned | Kafka 削峰 / 多级缓存 / 实时聚合（HLL UV、TopK）/ 防刷 |
| v0.3 | 📋 Planned | ES 历史检索 / PG 分区归档 / K8s 分布式部署 |

## 快速开始

> 需要 Go 1.24+、Docker、make。

```bash
# 1. 起依赖（PG + Redis）
make up

# 2. 跑迁移（建表）
make migrate

# 3. 拷贝环境变量
cp .env.example .env

# 4. 跑服务
make run
```

服务起在 `http://localhost:18080`。

### 创建短链

```bash
curl -X POST http://localhost:18080/api/links \
  -H 'Content-Type: application/json' \
  -d '{"long_url": "https://example.com/some/very/long/url"}'

# 返回
# {"code": "5BxX", "short_url": "http://localhost:18080/5BxX", "long_url": "..."}
```

### 跳转

```bash
curl -i http://localhost:18080/5BxX

# HTTP/1.1 302 Found
# Location: https://example.com/some/very/long/url
```

## 架构（v0.1）

```
        ┌──────────┐ POST /api/links
client ─┤          ├─────────────────► Create API ──► Segment ID ──► Base62 ──► PG (links)
        │          │
        │  HTTP    │ GET /:code
        │          ├─────────────────► Resolve  ──► Redis cache ──► PG fallback ──► 302
        └──────────┘                       │
                                           └─► Async event channel ──► batch INSERT to click_events
```

## 设计文档

- [架构与权衡](docs/architecture.md) — 为什么号段而非 Snowflake、为什么 302 而非 301
- [短码生成算法](docs/codegen.md) — 号段双 buffer + Base62 编码
- [缓存策略](docs/caching.md) — 三大坑 + 多级降级（v0.2 加深）
- [性能压测报告](docs/benchmark.md) — wrk 压测脚本与单机基线（v0.1 末发布）

## 路线图与博客

每个里程碑配套技术博客（公开发布）：

- [ ] v0.1 完成 → 「从 0 到 8w QPS:一个短链服务的诞生」
- [ ] v0.2 完成 → 「为什么我把异步写 PG 改成了 Kafka」
- [ ] v0.2 完成 → 「多级缓存命中率从 92% 到 99.5%」
- [ ] v0.3 完成 → 「百亿点击事件如何检索」

## 开发

```bash
make test       # 跑所有单元测试
make bench      # 跑 benchmark
make lint       # 静态检查
make migrate    # 跑数据库迁移
make up / down  # 启停 docker-compose
```

## License

MIT

## 致谢

灵感来自美团 Leaf（号段算法）、Twitter Snowflake（分布式 ID 思想），以及大量在生产环境被打挂的短链服务的事故复盘。
