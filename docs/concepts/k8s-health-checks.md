# Kubernetes 健康检查

> 5 分钟讲透：liveness / readiness / startup 三种探针的语义差异、为什么 healthz 不该依赖外部、503 设计、slink 的实现。
> 对应文件：[`cmd/server/main.go`](../../cmd/server/main.go) 第 130-200 行

## 一、问题：服务"健康"是什么意思？

朴素答案：**"服务能正常响应请求"**。

但这个定义在分布式系统下太粗糙：

```
场景 A：服务进程跑着，但 Redis 挂了
  → 请求大概率失败
  → 应该把流量切走，但**不应该重启服务**（重启不能让 Redis 复活）

场景 B：服务进程死循环卡死
  → 一切请求都不响应
  → 必须重启服务

场景 C：服务刚启动，正在加载缓存预热（30 秒）
  → 还不能接流量，但已经活着
  → 不应该重启，也不应该派流量
```

**三个完全不同的状态**，需要三种健康检查。

## 二、Kubernetes 的三种探针

K8s 把健康检查标准化为三种 probe：

| 探针 | 失败时 K8s 做什么 | 用来判断什么 |
|---|---|---|
| **livenessProbe** | **重启 Pod** | "服务进程是否还活着" |
| **readinessProbe** | **从 Service Endpoints 摘掉**（停止发流量） | "服务是否准备好接流量" |
| **startupProbe** | 暂缓 liveness/readiness 检查 | "服务是否还在启动期" |

每个 probe 可以用 `httpGet` / `tcpSocket` / `exec` 三种方式。

slink 用 HTTP 探针：

```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 18080
  initialDelaySeconds: 10
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /readyz
    port: 18080
  initialDelaySeconds: 5
  periodSeconds: 5
  failureThreshold: 3
```

## 三、致命错误：把 readiness 的逻辑写到 liveness 里

最常见的反模式：

```go
// ❌ 错误：liveness 检查 PG 和 Redis
func livenessHandler(w http.ResponseWriter, r *http.Request) {
    if err := pg.Ping(); err != nil {
        w.WriteHeader(503)  // PG 挂 → liveness 失败 → K8s 重启 Pod
        return
    }
    if err := redis.Ping(); err != nil {
        w.WriteHeader(503)  // Redis 挂 → 同上
        return
    }
    w.WriteHeader(200)
}
```

**后果**：

1. PG / Redis 挂时 → 全部 Pod 同时 liveness 失败 → 全部重启
2. 重启不会让 PG / Redis 复活
3. 重启过程中所有 Pod 同时不可用 → **雪崩**
4. 即便依赖恢复，所有 Pod 同时启动会撞依赖（"惊群效应"）

**正确做法**：

```go
// ✅ liveness 只验证进程没死锁
func livenessHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// ✅ readiness 验证依赖
func readinessHandler(...) http.HandlerFunc {
    // ping pg + ping redis，全 ok 才 200，否则 503
}
```

PG 挂时：
- liveness 仍 200 → 不重启
- readiness 503 → 从 LB 摘掉
- PG 恢复后 → readiness 200 → 自动加回 LB
- Pod 不重启，不撞惊群

## 四、startup probe（K8s 1.16+）

解决"启动慢"服务的问题：

```
没有 startup probe：
  initialDelaySeconds: 60   ← 假设服务最慢启动 60s
  
但偶尔启动 30s 就好 → 浪费 30s 不接流量
偶尔启动 90s → 60s 后就被 liveness 判定死亡 → 重启
```

startup probe 解决：

```yaml
startupProbe:
  httpGet:
    path: /healthz
    port: 18080
  failureThreshold: 30  # 30 次 × 10s = 5 分钟启动窗口
  periodSeconds: 10

livenessProbe:
  ...
  # 在 startup 成功前，liveness 不会被检查
```

slink v0.1 启动很快（< 1s），暂不需要 startup probe。

## 五、503 vs 其他状态码

readiness 失败用什么状态码？

| 状态 | 含义 | 用途 |
|---|---|---|
| 200 | OK | 健康 |
| 503 | Service Unavailable | **依赖暂时不可用**，符合 readiness 语义 |
| 500 | Internal Server Error | 服务器 bug，readiness 不该用这个 |
| 429 | Too Many Requests | 限流，不是健康问题 |

**slink 用 503**——明确告诉负载均衡器 "我现在没准备好，不要派单"。

## 六、并行 ping vs 串行 ping

slink 的 readyz 实现：

```go
wg.Add(2)
go check("postgres", func() error { return pg.Ping(ctx) })
go check("redis", func() error { return rd.Ping(ctx) })
wg.Wait()
```

**为什么并行**：

- 串行：PG 慢 100ms + Redis 慢 100ms → 总 200ms
- 并行：max(PG, Redis) → 总 100ms

K8s probe 默认 `timeoutSeconds=1`，慢了会被判失败。**并行是必要的**。

## 七、context 超时（保护探针自身）

```go
func readinessHandler(...) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
        defer cancel()
        ...
    }
}
```

**为什么超时 2s 而不是直接用 r.Context()**：

- K8s timeoutSeconds 默认 1s，到点客户端断
- 我们设 2s 给业务侧些余量但仍硬限——避免 PG 慢查询拖垮探针
- 没有 timeout：依赖卡住的话探针请求堆积，最终 OOM

## 八、failureThreshold 和扁平期

```yaml
readinessProbe:
  failureThreshold: 3
  periodSeconds: 5
```

**含义**：连续 3 次失败（15 秒）才判定"未就绪"。**容忍偶发**抖动。

| 参数 | slink 推荐 | 理由 |
|---|---|---|
| `initialDelaySeconds` | liveness 10s，readiness 5s | 服务启动期不要立即检查 |
| `periodSeconds` | 10s（liveness）、5s（readiness） | readiness 反应快些 |
| `timeoutSeconds` | 1s | 探针不能慢 |
| `failureThreshold` | liveness 3，readiness 3 | 容忍偶发 |
| `successThreshold` | 1 | 默认 |

## 九、slink 实现的关键点

```go
// 接口定义只要 Ping(ctx) error，方便单测
func readinessHandler(pg interface {
    Ping(context.Context) error
}, rd *cache.Client) http.HandlerFunc {
```

**为什么不直接传 *pgxpool.Pool**：

- 解耦实现：readiness 只依赖 "能 Ping 的东西"
- 单测：可以传 mock

**结果格式**：

```json
{
  "status": "degraded",
  "version": "v0.1-day2",
  "backends": {
    "postgres": "ok",
    "redis": "fail: dial tcp [::1]:16379: connect: connection refused"
  }
}
```

- 字段化：监控/SRE 可以解析（不只是看状态码）
- 错误细节：dev 调试方便
- 注意：**生产环境可能需要脱敏 backends 错误**（不暴露内部 IP / 端口给外界）

## 十、踩坑清单

| 坑 | 后果 | 解法 |
|---|---|---|
| liveness 依赖外部 → 雪崩 | 全 Pod 同时重启 | liveness 只看进程 |
| 探针无超时 | 慢依赖拖死探针 | context.WithTimeout |
| failureThreshold = 1 | 网络抖动判定失败 | 至少 2-3 |
| readiness 太激进 | 频繁切流量 | 加 successThreshold = 2 |
| 探针 path 跟业务混 | DDoS / 监控拉死业务接口 | 单独 /healthz /readyz |
| 依赖暴露错误细节 | 信息泄漏 | 生产脱敏 |
| 探针 log 太多 | 日志爆炸 | 单独 access log 过滤探针 |

## 十一、5 分钟自检

合上文档：

1. liveness 和 readiness 失败时 K8s 各做什么？
2. 为什么 healthz 绝对不该依赖 PG / Redis？
3. 探针为什么必须并行 ping 多个依赖？
4. 503 状态码在 readiness 上的语义是什么？
5. slink 的 readyz 在 PG 挂时返回 200 还是 503？为什么？

## 十二、延伸阅读

- [Kubernetes Probes 官方文档](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)
- [Liveness probes are dangerous](https://srcco.de/posts/kubernetes-liveness-probes-are-dangerous.html)（应避免依赖外部）
- [Health Check Patterns — Brendan Burns](https://www.oreilly.com/library/view/designing-distributed-systems/9781491983638/ch04.html)
