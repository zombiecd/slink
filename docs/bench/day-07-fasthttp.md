# Day 7 — fasthttp 替换 net/http (v0.2)

> 2026-05-07 / slink v0.2-day7
>
> 主战场：**整层换栈底** → fasthttp + fasthttp/router 替代 net/http →
> 验证 Day 6 关于"net/http syscall 天花板"的假设是否成立。

## 一、目标与背景

Day 6 收口结论：
- mixed 场景 21,012 RPS / P99 19.72ms（4t / 256c / 30s）
- CPU profile：`syscall.syscall + runtime.kevent` = **79% CPU**
- 假设：**net/http per-conn-goroutine 模型** 是 21k RPS 天花板，
  要破必须换 fasthttp 或上多进程

Day 6 把 fasthttp 列为 v0.2 工作（"破坏标准库兼容"），不在 Day 6 预算内。
Day 7 正式开干。

## 二、改动概览

| 文件 | 改动 |
|---|---|
| `cmd/server/main.go` | 主端口从 `http.Server` 切到 `fasthttp.Server`；pprof :6060 保留 net/http |
| `internal/api/server.go` | `*http.ServeMux` → `*fasthttp/router.Router`；handler 接口 `(w, r)` → `(*fasthttp.RequestCtx)` |
| `internal/api/redirect.go` | `r.Header.Get` → `ctx.Request.Header.Peek` 等；零拷贝 byte slice 转 string 注意 async enqueue |
| `internal/api/links.go` | `r.Body` (io.Reader) → `ctx.PostBody()` ([]byte 视图) + bytes.NewReader |
| `internal/api/json.go` | writeJSON / writeError 接 fasthttp.RequestCtx |
| `internal/api/redirect_test.go` | httptest.ResponseRecorder → fasthttputil.InmemoryListener + http.Client |
| `internal/api/links_test.go` | 同上 + 新 harness 抽象 |

依赖新增：
```
github.com/valyala/fasthttp v1.71.0
github.com/fasthttp/router  v1.5.4
```

**保留 net/http**：仅 pprof :6060。原因：
1. `net/http/pprof` 是标准库，go tool pprof / curl / 浏览器都直接认它
2. `fasthttpadaptor` 能把 net/http handler 套进 fasthttp，但徒增一层适配开销
3. pprof 端口本来就低频 + 仅本机绑定，性能不重要
4. 业界标准（Kubernetes / Prometheus / Etcd 都把 pprof 单独绑）

## 三、对照 bench

同样 mixed 场景（10s 预热 + 30s 压 t=4 / c=256，100 个 code 池）：

| 指标 | Day 6 (net/http after) | Day 7 (fasthttp) | Δ |
|---|---|---|---|
| **RPS** | 21,012 | **23,722** | **+12.9%** |
| P50 | 11.89ms | 10.36ms | -12.9% |
| P75 | 13.27ms | — | — |
| P90 | 14.71ms | 13.45ms | -8.6% |
| **P99** | 19.72ms | **19.29ms** | **-2.2%** |
| Transfer | 2.36 MB/s | 3.23 MB/s | +37% |
| Socket errors | read 93 | read 119 | +28 |
| **CPU 总采样** | 65s / 30s = 217% | 52s / 30s = **173%** | -20% 总 CPU |
| **CPU 每请求** | 103µs/req | **73µs/req** | **-29%** |

注：Day 7 一共跑了两次 30s 测试，第二次（与 pprof 同步抓的）数字偏低 23.7k；
单跑（无 pprof 干扰）首次 24.7k。pprof 自身约带来 ~4% RPS 损耗。
为对照 fair 取与 pprof 同时段的 23.7k。

## 四、CPU profile 对比 — 瓶颈走向

### 4.1 syscall 占比变化

| 函数 | Day 6 (net/http) | Day 7 (fasthttp) | Δ |
|---|---|---|---|
| `syscall.syscall` flat% | **69.74%** | **55.87%** | **-13.9 pp** |
| `runtime.kevent` flat% | 9.60% | (未单列) | 收敛 |
| 总 syscall 类 | ~79% | ~56% | **-23 pp** |

**核心结论：fasthttp 真的减了 syscall**——14 个百分点的 syscall.syscall 减少，
对应到约 **16s CPU/30s 的纯节省**。这是 fasthttp 的核心价值。

### 4.2 但 RPS 只涨 +13%，CPU 节省去哪了？

CPU 总采样 **-20%**（65s → 52s），RPS 只 **+13%**。多省的 CPU 没换成更多 RPS，
而是变成"更轻的进程"。换算成每请求 CPU：

```
Day 6:  65s CPU / 630,813 reqs = 103 µs / req
Day 7:  52s CPU / 712,594 reqs =  73 µs / req
```

每请求 **少 30 µs CPU**。体现在哪？fasthttp 的几个关键优化：
- 复用 `Request` / `Response` / `Buffer` 对象池（`sync.Pool` 全栈用满）
- 单 goroutine 处理一个 conn 上的多次请求（vs net/http 每 req 一个 goroutine 切换）
- 自实现 byte-slice 操作避免 string ↔ []byte 来回 conversion

### 4.3 那为什么 RPS 没翻倍？— 瓶颈往后挪了

| 函数 | Day 6 cum% | Day 7 cum% | 解读 |
|---|---|---|---|
| `redis.cmdable.Get` | 29.09% | **35.92%** | 占比上升 — Redis 现在主导 |
| `redis.(*baseClient)._process` | (未单列) | 35.84% | 同上 |
| `bufio.(*Writer).Flush` | 32.42% | (隐入 fasthttp) | net/http 写 socket 显式开销 |
| `internal/api.handleRedirect` | 32.17% | 37.11% | handler 自身占比上升（分母变小） |

**瓶颈从"net/http 调度 + write socket"挪到了"Redis 网络 IO"。**

24k RPS = 24k 次 Redis Get/秒。我用 single-process Redis client（go-redis v9），每个 Get：
1. 走 TCP roundtrip 到 redis :6379
2. 序列化命令、读响应、反序列化
3. 如果 conn pool 有竞争还要等

go-redis 默认 pool size 跟 GOMAXPROCS 走，在我这台 8 核 Mac 上是 8。
24k RPS / 8 conn ≈ 3000 RPS/conn，单 conn 上 ~330µs 处理一次 Redis Get。
看起来还有空间，但 Redis client 的内部 lock + 协议 codec 已经吃了 36% CPU。

### 4.4 真要破 24k 怎么办（v0.3 候选）

按 ROI 排：

| # | 方向 | 预期 | 工作量 |
|---|---|---|---|
| 1 | **进程内 LRU local cache**（hot 100 个 code 内存命中）| ~3-5x（消 Redis Get）| 中（侵入 cache 层）|
| 2 | Redis pipeline 多路复用 | +20-50% | 中（异步聚合 batch）|
| 3 | Redis cluster + 一致性 hash 分片 | +N×（按节点数）| 高（部署 + 运维改动）|
| 4 | 主进程多 process / SO_REUSEPORT | ~2-3×（多核 socket fan-out）| 中（部署改）|
| 5 | gnet（user-space netpoll）| 边际 | 高 |

**最大的一刀是 #1**：mixed 场景命中率本来就 ~99%，做个 5MB 内存 LRU 几乎所有
读都不出进程。但局限性也明显：水平扩 server 时多实例缓存会不一致。

## 五、alloc 对比

| 指标 | Day 6 (net/http after) | Day 7 (fasthttp) | Δ |
|---|---|---|---|
| 总 alloc | 3,484MB | **3,132MB** | **-10.1%** |
| handleRedirect cum | 1,322MB (1.32GB) | 1,300MB | -1.7% |
| GetOrLoad cum | 663MB | 1,216MB | +83% (注 1) |
| net/http.Header.Clone | 350MB | **0** | 完全消失 |
| net/http.Redirect | 0 | 0 | 同 Day 6 已干掉 |
| context.WithDeadline | 0 | 0 | 同 Day 6 已干掉 |
| crypto/rand UUID | 0 | 0 | 同 Day 6 已干掉 |

注 1：GetOrLoad cum 上升是**因为 RPS 涨了 13%，绝对调用次数也涨了**，
GetOrLoad 内 redis.cmdable.Get + json.Unmarshal 的单次 alloc 没变。

**fasthttp 直接消掉了 net/http 内部的 Header.Clone（350MB）**——这部分不是
Day 6 优化能动到的（在 net/http 内部）。

## 六、这次实验学到什么

### 6.1 Day 6 的假设：部分被验证

Day 6 写："net/http 单进程 21k 是天花板"。

**对的部分**：syscall % 真的从 70% 降到 56%，证明 net/http 有 14 pp 的"调度 + IO 仪式"开销。
**错的部分**：以为破了 net/http 就能 +50-100% RPS。

为什么估错？因为没意识到 **22% 的总 CPU 是 Redis Get**（cum 29% in Day 6）。
那 22% 跟 HTTP 栈无关，换 fasthttp 也不会少。当 syscall 从 70% → 56% 释放出 14 pp CPU，
Redis Get 的占比就从 29% 涨到 36%——它没变多，是别的变少了。

**真实瓶颈结构**：
```
Day 6 (21k RPS):
  syscall(70%) + Redis(29%) + others(1%) = 100% CPU
  → throughput ∝ 1/(syscall + Redis + others)

Day 7 (24k RPS):  
  syscall(56%) + Redis(36%) + others(8%) = 100% CPU
  → 同样 1.7 cores，多干 13% 活
```

13% 是 syscall 节省（14pp）反映到吞吐的正比换算结果，不是 fasthttp"自带魔力"。

### 6.2 单进程瓶颈现在卡在哪

24k RPS × 1 Redis Get/req = 24k Redis ops/sec。

参考 Redis 官方 benchmark：单实例 GET 吞吐在 100k+ ops/sec（1ms latency）。
我们离 Redis 真正瓶颈还远。但 **go-redis client 在 client side 的 codec + pool 锁** 吃了 36% CPU。
要破它得换 client（如 rueidis 用 RESP3 + auto-pipelining）或自己上 pipeline。

这是 v0.3 的活，不在 Day 7。

### 6.3 fasthttp 不是免费午餐

迁移代价：
- 新依赖 2 个（fasthttp + router）+ 6 个间接依赖
- handler 接口完全变了（`(w, r)` → `(*RequestCtx)`），接口面广的项目工作量大
- 零拷贝陷阱：Header.Peek / PostBody 返回的 []byte 仅 handler 期间有效，
  异步入队/log/cache 必须 string() 拷贝（我在 enqueueClickEvent 里就踩了一脚）
- httptest.NewRecorder 不能用了，所有测试要改 InmemoryListener
- net/http/pprof 不能直接挂同端口（fasthttpadaptor 能但有开销）

收益：30% CPU 效率（每请求少 30µs）+ -10% 总 alloc + 13-17% RPS。

**写在 PR description 里就是**："换 fasthttp 是因为 Day 6 profile 发现 net/http
有 14pp 的 syscall 浮渣可以省。换完省下来 30% CPU/req，但 Redis Get 同时浮上来成为新瓶颈，
所以净 RPS 提升只有 13%。要继续往上推，下个目标是进程内 LRU local cache 消掉 Redis Get。"

## 七、Day 8 候选方向

按 Day 7 profile 重排：

| # | 方向 | 预期 | 难度 | 备注 |
|---|---|---|---|---|
| 1 | **本地 LRU cache**（hot codes 内存直读）| 50k+ RPS | 中 | 最大 ROI；但需要面对水平扩展时多实例缓存不一致 |
| 2 | 修 cache 层 ctx-cancel 误降级（Day 6 列的）| 消日志噪音 | 低 | 顺手做 |
| 3 | Redis pipeline 异步聚合 | +20-50% | 中 | 复杂度高 |
| 4 | SO_REUSEPORT 多 process | +50-100% | 中 | 部署侧改动 |
| 5 | rueidis 替换 go-redis | +20%? | 中 | 客户端 codec 优化 |

最大的一刀仍是 **#1 本地 cache**：mixed 场景命中率本来 99%+，做个 5MB 进程内 LRU
基本不用打 Redis。但和水平扩展互斥——多实例下缓存一致性会成新坑。

## 八、数据归档

- profile 文件：`/tmp/slink-day7/{cpu,heap,allocs}.pb.gz`
- bench 输出：`/tmp/slink-day7/bench-fasthttp.txt`
- server log：`/tmp/slink-day7/server.log`
- 对比基准：`docs/bench/day-06-baseline.md`（net/http after）

## 九、关键命令复现

```sh
# 1. 启 server
SLINK_PPROF_ADDR=127.0.0.1:6060 ./bin/server

# 2. 预热（10s）
unset http_proxy https_proxy
CODES_FILE=/tmp/slink-codes.txt wrk -t4 -c256 -d10s \
  -s scripts/bench/mixed.lua http://localhost:18080

# 3. bench + pprof 同步抓（30s）
curl -sS "http://127.0.0.1:6060/debug/pprof/profile?seconds=30" \
  -o /tmp/slink-day7/cpu.pb.gz &
sleep 1
CODES_FILE=/tmp/slink-codes.txt wrk -t4 -c256 -d30s \
  -s scripts/bench/mixed.lua http://localhost:18080

# 4. snapshot
curl -sS http://127.0.0.1:6060/debug/pprof/heap   -o /tmp/slink-day7/heap.pb.gz
curl -sS http://127.0.0.1:6060/debug/pprof/allocs -o /tmp/slink-day7/allocs.pb.gz

# 5. 看 top
go tool pprof -top -cum -nodecount=15 /tmp/slink-day7/cpu.pb.gz
go tool pprof -top -cum -nodecount=15 /tmp/slink-day7/allocs.pb.gz
```
