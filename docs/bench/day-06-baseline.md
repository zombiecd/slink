# Day 6 — pprof 探底 + redirect handler 微优化

> 2026-05-07 / slink v0.1
>
> 主战场：**接 pprof 找瓶颈** → 三个低成本 alloc 优化 → re-bench 对比

## 一、目标与背景

Day 5 收口数据：
- mixed 场景 **21,545 RPS / P99 6.84ms**（端口 :18080，t=4 / c=256 / 30s）
- 距 v0.1 目标 50,000 QPS 仍有 **2.4×** gap

Day 5 列了 6 项候选优化，预算明确：v0.1 不上 fasthttp / 多级缓存（破坏架构），Day 6 主攻"低难度高 ROI"那一档。

但有一条原则不能违反 —— **优化前必须 profile**。Day 5 候选清单是基于代码阅读猜的，Day 6 第一步必须用真实数据校准。

## 二、Step 1：接 pprof（业界标准做法）

### 2.1 单独端口 vs 主端口

参考 Kubernetes / Prometheus / etcd：pprof 单独跑一个 listener，理由有三：

1. **安全**：pprof 暴露 goroutine / heap profile，外部访问 = 信息泄漏。单独端口绑 `127.0.0.1` 即可只允许本机
2. **路由污染**：业务端口加 `/debug/pprof` 路由会影响 main mux 性能
3. **改动面小**：pprof 默认注册到 `http.DefaultServeMux`，独立 server 复用即可

### 2.2 改动

```go
// cmd/server/main.go
import _ "net/http/pprof" // 注册 /debug/pprof/* 到 http.DefaultServeMux

if cfg.PProfAddr != "" {
    pprofSrv := &http.Server{
        Addr:              cfg.PProfAddr,
        Handler:           http.DefaultServeMux,
        ReadHeaderTimeout: 5 * time.Second,
    }
    go func() {
        if err := pprofSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
            slog.Error("pprof server", "err", err)
        }
    }()
    defer func() {
        ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
        defer cancel()
        _ = pprofSrv.Shutdown(ctx)
    }()
}
```

新配置项：`SLINK_PPROF_ADDR=127.0.0.1:6060`（默认）。

抓 profile：
```sh
# 30s CPU
curl -o cpu.pb.gz "http://127.0.0.1:6060/debug/pprof/profile?seconds=30"
# 即时 heap / alloc snapshot
curl -o allocs.pb.gz "http://127.0.0.1:6060/debug/pprof/allocs"
curl -o heap.pb.gz "http://127.0.0.1:6060/debug/pprof/heap"
```

## 三、Step 2：跑 baseline 拿 profile

### 3.1 baseline 数据

预热 10s + 压 30s（mixed 100 个 code 池）：

| 指标 | Day 6 baseline (before) |
|---|---|
| RPS | **20,824** |
| P50 | 11.92ms |
| P75 | 13.30ms |
| P90 | 14.83ms |
| P99 | **20.93ms** |
| Transfer | 4.17 MB/s |
| Socket errors | read 122 |

注：与 Day 5 21,545 RPS / P99 6.84 略有差异。原因：当时 Day 5 跑 hot 单 code 场景拿到 6.84ms，mixed 场景实际就在 20-21k 量级。Day 6 全程用 mixed 对齐对比。

### 3.2 CPU profile 真相（Top 10）

```
flat  flat%   sum%        cum   cum%
45.35s 69.74% 69.74%     45.48s 69.94%  syscall.syscall          ◀── 大头！
 6.24s  9.60% 79.33%      6.24s  9.60%  runtime.kevent           ◀── kqueue（macOS netpoll）
 2.84s  4.37% 83.70%      2.84s  4.37%  runtime.pthread_cond_wait
 2.82s  4.34% 88.04%      2.82s  4.34%  syscall.syscall6
 1.77s  2.72% 90.76%      1.77s  2.72%  runtime.usleep
 1.73s  2.66% 93.42%      1.73s  2.66%  runtime.pthread_cond_signal

 cum 关键节点：
 21.08s (32.42%)  bufio.(*Writer).Flush         ◀── 写 socket
 18.92s (29.09%)  redis.cmdable.Get             ◀── Redis 读
 20.92s (32.17%)  internal/api.handleRedirect   ◀── 跳转 handler 自身
```

**核心结论：syscall + netpoll = 79% CPU**。

这意味着：在当前 net/http 标准库 + 单进程下，**21k RPS 就是天花板**。所有计算（Redis 查、JSON 解析、UUID 生成）加起来不到 5%，alloc / GC 加起来不到 3%。

Day 5 列的优化清单里：
| 候选 | profile 验证 | 预期收益 |
|---|---|---|
| 1. EventID 改便宜算法 | crypto/rand 占 1.13s (1.74%) | 上限 ~2% |
| 2. sync.Pool 复用 ClickEvent | 不在 hot path | <1% |
| 3. 手写 Redirect 省 body | http.Redirect cum 824MB alloc (21%) | ~3% |
| 4. GOGC/GOMAXPROCS | mallocgc 仅 0.68s (1.05%) | <1% |
| 5. fasthttp/gnet | 直接绕开 syscall 开销 | **+50-100%** |
| 6. 多级缓存 | 不在 hot path | 仅 C 场景 |

只有 5 才能动 70% 的 syscall 大头。Day 6 预算不上 5，所以 RPS 提升空间小（< 5%）。

### 3.3 alloc profile（Top 15）

```
flat  flat%   sum%        cum   cum%
349.60MB  8.94%  8.94%   349.60MB  8.94%  net/http.Header.Clone        ◀── net/http 内部
336.59MB  8.61% 17.55%   336.59MB  8.61%  net/textproto.readMIMEHeader
312.60MB  7.99% 25.54%   312.60MB  7.99%  net/textproto.MIMEHeader.Set
306.55MB  7.84% 33.38%  1102.74MB 28.20%  net/http.(*conn).readRequest
215.03MB  5.50% 44.99%   215.03MB  5.50%  net/url.parse
161.39MB  4.13% 49.11%   163.39MB  4.18%  store.ClickEventRepo.BatchInsert
122.01MB  3.12% 52.23%   216.02MB  5.52%  context.WithDeadlineCause    ◀── enqueueClickEvent 的 50ms timeout
117.52MB  3.01% 55.24%   208.02MB  5.32%  encoding/json.Unmarshal
104.50MB  2.67% 63.72%   142.50MB  3.64%  internal/api.newEventID      ◀── crypto/rand + fmt.Sprintf
 95.50MB  2.44% 66.16%   662.55MB 16.95%  cache.(*LinkCache).GetOrLoad
 94.01MB  2.40% 70.97%    94.01MB  2.40%  time.newTimer                ◀── context.WithTimeout 内部
   61MB  1.56% 79.74%   824.21MB 21.08%  net/http.Redirect            ◀── 写 HTML body 的 alloc 链
```

handleRedirect cum 1.83GB **占总 alloc 47.90%**，三个明显的"非必要" alloc：

| # | 位置 | 大小 | 原因 |
|---|---|---|---|
| A | `http.Redirect` | 824MB cum | 写 `<a href=...>Found</a>` HTML body + escapeHTML + Content-Type |
| B | `context.WithTimeout(50ms)` | 122MB + 94MB timer | enqueueClickEvent 创了个 50ms 超时，但 `Buffer.Enqueue` 默认走 select-default 非阻塞路径，**根本不读 ctx** |
| C | `newEventID` | 142MB cum | crypto/rand 走 syscall + `fmt.Sprintf("%08x-...", b[0:4], ...)` 5 个 sub-slice + 反射 |

## 四、Step 3：三个低成本优化

### 4.1 Patch A — 手写 redirect 替代 `http.Redirect`

```go
// before
http.Redirect(w, r, link.LongURL, http.StatusFound)

// after
w.Header().Set("Location", link.LongURL)
w.WriteHeader(http.StatusFound)
```

302 响应浏览器看 Location header 立即跳走，body 永远不展示。`http.Redirect` 写的 HTML 是给禁用 redirect 的爬虫看的兜底。短链场景纯浪费。

### 4.2 Patch B — 去掉无效的 `context.WithTimeout`

```go
// before
ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
defer cancel()
s.events.Enqueue(ctx, evt)

// after
s.events.Enqueue(context.Background(), evt)
```

读了 `event/buffer.go:Enqueue`：默认配置 `EnqueueTimeout=0`，走 `select { case ch <- evt: ... default: dropped }` 非阻塞分支，根本不读 ctx。

每次请求 new timer + cancel = `122MB context + 94MB timer` 总 alloc，纯浪费。

### 4.3 Patch C — newEventID 用 `math/rand/v2` + 字节填充

```go
// before
import "crypto/rand"
import "fmt"

func newEventID() string {
    var b [16]byte
    rand.Read(b[:])  // syscall
    b[6] = (b[6] & 0x0f) | 0x40
    b[8] = (b[8] & 0x3f) | 0x80
    return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
        b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])  // 5 sub-slice + reflect
}

// after
import "math/rand/v2"

const hexChars = "0123456789abcdef"

func newEventID() string {
    hi := rand.Uint64()  // ChaCha8 PRNG, 几个 ns，无 syscall
    lo := rand.Uint64()
    hi = (hi & 0xffff_ffff_ffff_0fff) | 0x0000_0000_0000_4000  // version 4
    lo = (lo & 0x3fff_ffff_ffff_ffff) | 0x8000_0000_0000_0000  // variant 10xx
    var buf [36]byte
    writeHex := func(off int, n uint64, nibbles int) {
        for i := nibbles - 1; i >= 0; i-- {
            buf[off+i] = hexChars[n&0xf]
            n >>= 4
        }
    }
    writeHex(0, hi>>32, 8); buf[8] = '-'
    writeHex(9, (hi>>16)&0xffff, 4); buf[13] = '-'
    writeHex(14, hi&0xffff, 4); buf[18] = '-'
    writeHex(19, lo>>48, 4); buf[23] = '-'
    writeHex(24, lo&0xffff_ffff_ffff, 12)
    return string(buf[:])
}
```

**安全性说明**：EventID 仅用于事件去重 / 日志关联，**不是安全凭证**。不需要 crypto/rand 的密码学随机性。math/rand/v2 的 ChaCha8 是 Go 1.22+ 默认源，碰撞概率 = 2^64 之一，对 click 事件足够。

10w 次输出全部通过 `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$` 校验。

## 五、re-bench 对比

同样 mixed 场景（10s 预热 + 30s 压 t=4/c=256）：

| 指标 | Before | After | Δ |
|---|---|---|---|
| **RPS** | 20,824 | **21,012** | **+0.9%** |
| P50 | 11.92ms | 11.89ms | -0.3% |
| P75 | 13.30ms | 13.27ms | -0.2% |
| P90 | 14.83ms | 14.71ms | -0.8% |
| **P99** | 20.93ms | **19.72ms** | **-5.8%** |
| Transfer | 4.17 MB/s | **2.36 MB/s** | -43%（body 减半） |
| 总 alloc | 3,909MB | **3,484MB** | **-10.9%** |
| handleRedirect cum alloc | 1.83GB | **1.32GB** | **-28%** |

三个 alloc 大头**全部消失**：
- `http.Redirect` 824MB → **0**
- `context.WithDeadline + timer` 216MB → **0**
- `newEventID(crypto/rand + fmt)` 142MB → **0**
- `crypto/internal/sysrand.Read` 1.13s CPU → **0**

## 六、复盘 — 这次探底学到什么

### 6.1 Profile 是真理，印象是猜测

Day 5 凭代码读出来的优化清单是**对的**（三个 alloc 大头都对），但**收益预估全错**：

| 项 | Day 5 估 | profile 验证 |
|---|---|---|
| EventID crypto/rand | 10-20% RPS | 实际 < 1%（CPU 占 1.74%） |
| sync.Pool ClickEvent | 5-10% | 不在 hot path |
| 手写 Redirect | 3-5% | 实际 alloc -28%，RPS < 1% |
| GOGC | 5% | mallocgc 占 1.05%，无收益空间 |

**真实瓶颈**：syscall 占 69%。Day 5 没意识到 net/http 的 read/write socket 才是大头，把 alloc 改动估值放大了 5-10×。

### 6.2 alloc 改动 ≠ RPS 改动

10.9% 总 alloc 减少 → 0.9% RPS 提升。原因：
- mallocgc 在 CPU 总量里只占 ~1%，alloc 减一半也只能省 0.5% CPU
- 但 P99 改善 5.8%，**GC pause 减少的尾延迟收益**（虽然平均 CPU 没省多少，但避免了几次 stop-the-world）
- Transfer 减半（少写 ~150 bytes/req）= 节省 bandwidth + 客户端解析时间

**结论**：alloc 优化对**尾延迟和带宽**有意义，对 throughput 上限影响极小。

### 6.3 21k RPS 是真天花板

CPU profile 显示 syscall + netpoll = 79%。这不是 slink 业务代码的问题，是 net/http 标准库的 per-connection-goroutine 模型本身的开销：
- 每个 connection 一个 goroutine
- Read/Write 走 syscall（在 macOS 是 read/write/kevent）
- bufio.Writer.Flush → write syscall

要破 5w：
- **路径 A（上 fasthttp / gnet）**：单 process 减少 50%+ syscall（共享 reader/writer pool）。预期 35-50k。**Day 5 预算明确不做**
- **路径 B（多 process / SO_REUSEPORT）**：8 核 × 21k ≈ 168k 理论上限。生产部署常用
- **路径 C（横向扩 LB 后挂多 server）**：标准做法，与单机性能无关

Day 6 不上述任一项 —— v0.1 的目标本来就不是在单进程做到 5w。**真实定位 + 诚实记录**比凑数字优化重要。

## 七、Day 7 候选方向

按 ROI 排（已剔除 Day 6 验证收益不大的项）：

| # | 方向 | 预期 | 难度 | 价值 |
|---|---|---|---|---|
| 1 | **修 cache 层 ctx-cancel 误降级 bug**（看 §八）| 减日志噪音、消假 DB 查 | 低 | 高 |
| 2 | 多 process（SO_REUSEPORT）|+50-200% RPS| 中（部署改 + listener 共享） | 高（最接近生产） |
| 3 | fasthttp 替换 net/http | +50-100% | 高（破坏标准库兼容） | v0.2 再说 |
| 4 | LinkCache JSON → 紧凑二进制 | -alloc 17% → 期望 -P99 | 中 | 中 |
| 5 | Redirect 路径 dbLoader 闭包消除 | 闭包 alloc | 中 | 低 |
| 6 | server WriteTimeout / IdleTimeout 调参 | -socket errors | 低 | 中 |

## 八、新发现 — cache 层 ctx-cancel 错误降级

压测期间观察到 server log：
```
WARN link cache get failed, fallback to loader code=PRP8dj err="redis get: context canceled"
```

19 条同时刻刷出。根因：
1. wrk 关连接 → r.Context() canceled
2. `lc.rdb.Get(ctx, key)` 返回 ctx.Err()，**不是 redis.Nil**
3. LinkCache 走 default 分支判定为"Redis 真错误"，**降级 fallback to loader 走 DB**
4. loader 也用同一 ctx，DB 查询同样返回 canceled

后果：
- 高并发关连接时刷 WARN 日志（污染真错误）
- 假"Redis 挂了 → DB 查询"扇出（DB 也接到 cancel，没真打到 PG，但是逻辑链已经走过）

修法（Day 7）：
```go
case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
    return nil, err  // 客户端断开，不降级、不 log
```

放在 `LinkCache.GetOrLoad` 的 switch case 里，优先级高于 default。

## 九、统计

- 改动：3 个 patch（redirect.go），删 import `crypto/rand` + `fmt` + `net/http.Redirect`
- pprof 接入：`cmd/server/main.go` + `internal/config/config.go`（新配置 `SLINK_PPROF_ADDR`）
- alloc 减少 -10.9%（426MB / 30s）
- P99 减少 -5.8%
- RPS 微升 +0.9%
- 单测：API 包全绿，新 newEventID 10w 次 UUID 格式校验全通过

收尾原则：**今天数字提升不大，但探底彻底**。结论：Day 6 用 pprof 定位到瓶颈在 syscall 而非 alloc，单进程 21k 是 net/http 标准库天花板。要破必须横向多 process —— 这是 v0.2 的活。
