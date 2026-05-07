# 给 fasthttp 项目接入 Prometheus + Grafana：闭包注入、Label 基数与跨平台 Docker

> 2026-05-07 / slink Day 10 / Go 1.24 / fasthttp v1.x / prometheus client_golang
>
> 这不是一篇"如何 hello world Prometheus"的入门文，而是把一个真实压到 8w+ QPS 的 Go 项目接入 Prom + Grafana 时踩过的几个非显然坑写下来：闭包注入避循环依赖、path label 基数爆炸、Grafana provisioning 的 datasource uid、host.docker.internal 的跨平台兼容。

---

## 0. 项目背景

[slink](https://github.com/zombiecd/slink) 是我个人开源的 Go 高并发短链服务，在写本文前的状态：

- fasthttp + fasthttp/router
- 多级缓存：L1 进程内 LRU（hashicorp/golang-lru）+ L2 Redis
- PostgreSQL 16 主存储 + 号段 ID 生成器 + channel 异步 click 写库
- 单机 mac 实测 94k QPS / L1 hit 99.97% / P99 28ms
- 已经有 `/debug/stats` JSON endpoint 暴露所有内部指标

简而言之，**业务和压测都跑通了**，差的就是把 stats 升级成 Prometheus 标准 + Grafana dashboard，docker compose 把全栈一键起来。

目标：

```sh
docker compose up -d   # 起 PG + Redis + Prometheus + Grafana
./bin/server           # 起 server
open http://localhost:13000/d/slink-overview  # 看 dashboard
```

听起来很简单，实际写下来踩了 4 个坑。下面按"代码 → 配置 → 验证"顺序讲。

---

## 1. metrics 包设计：为什么用闭包，不用 interface

第一直觉是写一个 `MetricsProvider` interface，让业务对象（cache / event buffer / id generator）实现它，metrics 包持有这些 provider 调用。

```go
// 第一直觉的设计
type LocalCacheStatsProvider interface {
    LocalStatsHits() int64
    LocalStatsMisses() int64
    LocalStatsHitRate() float64
}

type EventBufferStatsProvider interface {
    StatsEnqueued() int64
    StatsDropped() int64
    StatsFlushed() int64
    StatsFlushErr() int64
    StatsUsed() int64
    StatsCapacity() int64
}
// ... 等等
```

问题：

1. 每个业务对象要加 6-9 个 wrapper 方法（"业务对象不应该长成 metrics 形状"）
2. metrics 包要 `import` cache / event / id 包——反向依赖闻起来很糟，未来很容易绕回循环
3. 单元测试要 mock 这些 interface，又是一堆样板代码

改成 **`func() float64` 闭包** 之后，干净得多：

```go
// internal/metrics/metrics.go
type EventBufferGetters struct {
    Enqueued func() float64
    Dropped  func() float64
    Flushed  func() float64
    FlushErr func() float64
    Used     func() float64
    Capacity func() float64
}

func (r *Registry) BindEventBuffer(g EventBufferGetters) {
    r.registry.MustRegister(prometheus.NewCounterFunc(
        prometheus.CounterOpts{Name: "slink_event_buffer_enqueued_total"},
        g.Enqueued,
    ))
    // ...
}
```

main.go 装配点：

```go
metricsReg.BindEventBuffer(metrics.EventBufferGetters{
    Enqueued: func() float64 { return float64(eventBuf.Stats().Enqueued) },
    Dropped:  func() float64 { return float64(eventBuf.Stats().Dropped) },
    Flushed:  func() float64 { return float64(eventBuf.Stats().Flushed) },
    FlushErr: func() float64 { return float64(eventBuf.Stats().FlushErr) },
    Used:     func() float64 { return float64(eventBuf.Stats().Used) },
    Capacity: func() float64 { return float64(eventBuf.Stats().Capacity) },
})
```

收益：

| | interface 版 | 闭包版 |
|---|---|---|
| metrics 包业务依赖 | 3 包（cache/event/id）| **0 包** |
| 业务包 wrapper 方法 | 18 个 | **0 个** |
| 类型转换写在哪 | 业务包内（污染业务）| main.go 装配点（隔离）|
| 单测 mock 复杂度 | 要 mock 3 个 interface | 直接传 `func() float64 { return 42 }` |
| main.go 装配行数 | 短 | 略长（每类 6 行）|

**唯一的成本**是 main.go 装配点变长。但它本来就是装配的地方——所有"上下文转换 / 类型转换"的脏活集中到这里，反而是单一职责的体现。

> 如果你的 metrics 库没提供 CounterFunc / GaugeFunc 这种"读取时回调"的 API，这招用不了。Prometheus 的 client_golang 内置了，VictoriaMetrics/metrics 的 NewIntFunc 也类似，OpenTelemetry 的 ObservableCounter 同理。

---

## 2. 第一个坑：path label 基数爆炸

加 HTTP middleware 是接 Prometheus 的标准动作。fasthttp 里写起来这样：

```go
func (r *Registry) FastHTTPMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
    return func(ctx *fasthttp.RequestCtx) {
        start := time.Now()
        next(ctx)
        elapsed := time.Since(start).Seconds()

        path := string(ctx.Path())     // ⚠️ 看这里
        method := string(ctx.Method())
        status := strconv.Itoa(ctx.Response.StatusCode())

        r.HTTP.Requests.WithLabelValues(path, method, status).Inc()
        r.HTTP.Duration.WithLabelValues(path).Observe(elapsed)
    }
}
```

写完跑一下 wrk 再 curl /metrics：

```
slink_http_requests_total{method="GET",path="/abc123",status="302"} 1
slink_http_requests_total{method="GET",path="/abc124",status="302"} 1
slink_http_requests_total{method="GET",path="/abc125",status="302"} 1
... 1,000,000 行 ...
```

每个真实短码变成一个 label 值。slink 设计 568 亿短码空间，跑一段时间 Prometheus 内存就爆了。

**根因**：fasthttp 不像 Spring Boot 那样自带 "matched route pattern" 的暴露。`ctx.UserValue("__matched_path")` 之类的特殊 key 也没有标准。fasthttp/router v1.5.4 内部有个 trie，但它不暴露给 middleware。

**解决**：手写 path normalizer，把真实 path 收敛到几种 pattern：

```go
func normalizePath(p string) string {
    // 完全匹配的，原样返回
    switch p {
    case "/", "/healthz", "/readyz", "/api/links":
        return p
    }
    // /api/* 兜底
    if strings.HasPrefix(p, "/api/") {
        return "/api/*"
    }
    // 单段路径视为短码：/{code}
    if strings.Count(p, "/") == 1 {
        return "/:code"
    }
    return "unknown"
}
```

middleware 改成：

```go
path := normalizePath(string(ctx.Path()))
```

label 基数从 ∞ 降到 5-6。

**取舍**：每加一条新路由要手动维护 normalizePath。slink 总路由数 < 10，维护成本极低。如果你的项目有几十条路由，可以考虑：

- 让 router 注册时把 pattern 写到 `ctx.UserValue("__route_pattern")`，middleware 里读
- fork 一个支持 pattern 暴露的 router（fasthttp/router 之外的 fasthttp 兼容 router）
- 用 chi / gin 这种自带 pattern 暴露的框架

但绝对不能不处理就直接 `string(ctx.Path())`——这是会上线就出事故的级别。

---

## 3. 端到端装配：admin endpoint 同时挂 pprof / stats / metrics

slink 的设计是**业务端口和 admin 端口分离**：

| 端口 | 用途 | 暴露 |
|---|---|---|
| `:18080` | 业务流量（创建短链、跳转）| 公网 / LB 后 |
| `:6060` | admin（pprof, /debug/stats, /metrics） | 仅本机 / 内网 |

Prometheus 抓 admin 端口，业务流量不受影响。这条原则是 Day 6 接 pprof 时定下来的，到 Day 10 加 /metrics 就是顺手的事：

```go
// cmd/server/main.go
adminMux := http.NewServeMux()
adminMux.HandleFunc("/debug/pprof/", pprof.Index)
adminMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
// ... 其他 pprof ...
adminMux.HandleFunc("/debug/stats", statsHandler(...))
adminMux.Handle("/metrics", promhttp.HandlerFor(metricsReg.Registry(), promhttp.HandlerOpts{
    EnableOpenMetrics: true,
}))
```

`EnableOpenMetrics: true` 让响应同时支持 OpenMetrics（更新标准）+ Prometheus 老格式，Prom server 通过 Accept header 协商，对老 client 也兼容。

业务端口的 fasthttp server 用 middleware：

```go
rootHandler := metricsReg.FastHTTPMiddleware(router.Handler)
httpSrv := &fasthttp.Server{Handler: rootHandler, ...}
```

---

## 4. 第二个坑：docker-compose 跨平台 — host.docker.internal

slink 的部署模式：server 跑 host（不进容器）、PG/Redis/Prom/Grafana 都跑 docker-compose。这样开发时改代码 `go build && ./bin/server` 一行重启，不用每次都 rebuild 容器镜像。

Prometheus 在容器里要去抓 host 上的 server :6060——这就是 `host.docker.internal` 的用武之地。

mac / Windows 的 Docker Desktop 自带这个 DNS 名，直接用：

```yaml
# deploy/observability/prometheus.yml
scrape_configs:
  - job_name: slink
    scrape_interval: 5s
    static_configs:
      - targets: ['host.docker.internal:6060']
```

**Linux 默认没有 host.docker.internal**。Linux 上 docker-compose 用户会拿到 "Name or service not known"。

修法：在 docker-compose.yml 给容器显式加一行：

```yaml
services:
  prometheus:
    image: prom/prometheus:v2.55.1
    extra_hosts:
      - "host.docker.internal:host-gateway"
    # ...
```

`host-gateway` 是 Docker 20.10+ 的特殊别名，会被解析成"宿主机相对于容器的网关 IP"。这样 mac/Linux/Windows 三个平台用同一份 docker-compose.yml 都能跑。

如果你不想用 host.docker.internal，另外两个选择：

1. **server 也跑容器**：直接 `services.slink.image: ...`，prometheus 用 service name `slink:6060`。优点：100% 兼容；缺点：开发迭代慢
2. **network_mode: host**（仅 Linux）：让 prometheus 用 host 网络空间，直接 `localhost:6060`。优点：性能最好；缺点：mac/Windows 不支持

slink 选 host.docker.internal 是为了开发体验。生产部署会改成 server-in-container + service discovery。

---

## 5. 第三个坑：Grafana datasource uid 必须显式设

Grafana provisioning 是把 datasource 和 dashboard 用 yaml/json 文件预配置，启动自动加载，不在 UI 里手点。这是 GitOps 最佳实践。

```yaml
# deploy/observability/grafana/provisioning/datasources/prometheus.yml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

dashboard JSON 在 panel 里引用 datasource：

```json
{
  "datasource": {
    "type": "prometheus",
    "uid": "prometheus"   // ← 这里
  },
  "targets": [
    {"expr": "sum(rate(slink_http_requests_total[1m]))"}
  ]
}
```

启动 Grafana 后 dashboard 加载了，但所有 panel 显示 **"Datasource not found"**。

**根因**：dashboard JSON 引用 datasource 是用 `uid` 字段，Grafana 内部主键。但 datasource yml 里没显式声明 uid → Grafana 自动生成一个 UUID。dashboard JSON 里硬编码的 `"uid": "prometheus"` 当然找不到。

修法：datasource yml 显式加 uid：

```yaml
datasources:
  - name: Prometheus
    type: prometheus
    uid: prometheus     # ← 显式 uid
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

dashboard JSON 同步引用 `"uid": "prometheus"` —— 一致了。

这个坑的难处在于错误信息很模糊（"Datasource not found"），第一反应会去查 prometheus url 通不通、network 配置对不对，绕一大圈才发现是 uid 不一致。

---

## 6. 验证：三层数据通路一致性

工程化做完要验证全链路。slink 有三条独立的"读 metrics"路径：

1. **`/debug/stats` JSON**（手写）：直接 atomic load 计数器，序列化 JSON
2. **`/metrics` Prometheus** 格式：通过 client_golang 的 CounterFunc 读同一组 atomic 计数器
3. **Grafana dashboard**：通过 Prometheus query

如果三条路径数字不一致，说明数据通路有 bug。验证：

```sh
# 1. /debug/stats
$ curl http://127.0.0.1:6060/debug/stats | jq '.link_cache.l1'
{"hits": 3415650, "misses": 616, "hit_rate": 0.9998196861719784}

# 2. /metrics
$ curl http://127.0.0.1:6060/metrics | grep ^slink_l1
slink_l1_hits_total 3.41565e+06     # = 3,415,650 ✓
slink_l1_misses_total 616            # ✓

# 3. Prometheus query
$ curl -G "http://localhost:19090/api/v1/query" \
    --data-urlencode 'query=slink_l1_hits_total'
{"status":"success","data":{"result":[{"value":[..., "3415650"]}]}}

# 4. Grafana → Prometheus
$ curl -u admin:admin -G \
    "http://localhost:13000/api/datasources/proxy/uid/prometheus/api/v1/query" \
    --data-urlencode 'query=slink_l1_hits_total'
{"status":"success","data":{"result":[{"value":[..., "3415650"]}]}}
```

四个数字完全一致——三条数据通路都读同一个 atomic 计数器，没有漂移。

> 这种"多通路验证"看起来啰嗦，但 metrics 系统出事故时第一时间想知道的就是"是哪一段错了"。Day 1 把 /debug/stats 留着没删，Day 10 用上的就是这种"事故时降级到 raw"的备份能力。

---

## 7. 第四个坑：9% 中间件开销值不值

加完 middleware 重新跑 wrk：

```
Day 9（无 middleware）: 94,354 RPS / P99 28.23ms
Day 10（含 middleware）: 85,940 RPS / P99 24.70ms   ⬇ -9.0%
```

每次请求多了：

- 1 次 counter Inc（atomic.AddUint64，~1ns）
- 1 次 histogram Observe（找 bucket + atomic.AddUint64，~5ns）
- 标签 lookup（map 内部锁）

**9% 大头来自 prometheus client 库内部的 lock + map lookup**，不是 atomic 本身。

值不值？我的判断是值。理由：

1. **可观测性是单调累积价值**：一次 bench 的 9% 损失换永久的 metrics，从今天到项目下线都有用
2. **生产场景没人敢只跑 wrk 看数字**：必须 Prometheus 长期看，9% 是必交的"租金"
3. **简历角度**："86k QPS + 完整监控" 比 "94k QPS 但没监控" 显著更可信
4. **未来调优要看 metrics**：v0.4 加 Kafka / 多 flusher 时，要先看 metrics 才能判断改对了没

如果硬要追究，优化路径有几条：

- 用 `ConstLabels` 减少 map lookup（slink 的 path/method/status 都不能 const，但部分场景可以）
- 换更轻量的 lib：[VictoriaMetrics/metrics](https://github.com/VictoriaMetrics/metrics) 用 RWMutex + 字符串拼接 label，单次 ~50ns
- 关闭 histogram 改用 summary（quantile 在客户端算，更轻但不能聚合）

但这些都是"省 9% 的微优化"，相比"先有 metrics 比没有强"是次要矛盾。

---

## 8. 总结：4 个坑 + 5 个决策点

| # | 决策点 | 我的选择 |
|---|---|---|
| 1 | metrics 包注入业务对象的方式 | **闭包注入**（避免循环依赖 + 业务包不污染）|
| 2 | path label 基数控制 | **手写 normalizePath**（fasthttp/router 不暴露 matched route）|
| 3 | server 是否进容器 | 不进，prometheus 用 `host.docker.internal:6060` 抓 host |
| 4 | Linux 兼容 | `extra_hosts: ["host.docker.internal:host-gateway"]` |
| 5 | Grafana datasource provisioning | **显式 uid: prometheus**，dashboard JSON 引用一致 |

| # | 坑 | 现象 | 修法 |
|---|---|---|---|
| 1 | path label 基数爆炸 | 每个真实 code 一个 label 值 | normalizePath 收敛 |
| 2 | host.docker.internal 在 Linux 不存在 | prometheus 抓不到 host | extra_hosts: host-gateway |
| 3 | Grafana datasource uid 不一致 | dashboard 显示 "Datasource not found" | datasource yml 显式 uid |
| 4 | 9% middleware 开销 | RPS 94k → 86k | 接受（永久可观测性的租金）|

---

## 9. 仓库 + 完整代码

- 主仓库：[github.com/zombiecd/slink](https://github.com/zombiecd/slink)
- Day 10 commit：`bce435a feat(d10): Prometheus + Grafana + Docker compose 工程化收尾（v0.3 收口）`
- 关键文件：
  - `internal/metrics/metrics.go` —— Registry / Bind* / FastHTTPMiddleware / normalizePath
  - `cmd/server/main.go` —— 装配
  - `docker-compose.yml` —— prometheus + grafana 服务
  - `deploy/observability/prometheus.yml` —— scrape config
  - `deploy/observability/grafana/provisioning/` —— datasource + dashboard provider
  - `deploy/observability/grafana/dashboards/slink-overview.json` —— 7 panel dashboard

整个 v0.3 演进路径：

| Day | 数字 | 主要技术 |
|---|---|---|
| 5 | 21k RPS | net/http + cache-aside |
| 6 | 21k RPS（alloc -10%）| pprof 调优 |
| 7 | 24k RPS | fasthttp 整层迁移 |
| 8 | **94k RPS** | L1 LRU + TTL + singleflight |
| 9 | 94k + 99.97% L1 hit 实测 | /debug/stats observability |
| 10 | **86k + Prom/Grafana** | client_golang + docker compose |

下一阶段 v0.4 主战场：Kafka 异步 click 写库 + HLL UV / TopK 实时聚合。

---

## 附：可复制的最小起栈步骤

```sh
# 1. clone
git clone https://github.com/zombiecd/slink.git
cd slink

# 2. 起完整栈（PG + Redis + Prometheus + Grafana）
docker compose up -d

# 3. 起 server
go build -o bin/server ./cmd/server
SLINK_PPROF_ADDR=127.0.0.1:6060 ./bin/server &

# 4. 看 dashboard
open http://localhost:13000/d/slink-overview
# user/pass: admin/admin

# 5. 看 raw metrics
curl http://127.0.0.1:6060/metrics | grep ^slink_

# 6. 看 Prometheus targets
open http://localhost:19090/targets
```
