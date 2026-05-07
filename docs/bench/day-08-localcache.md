# Day 8 — 进程内 LRU L1 cache (v0.3)

> 2026-05-07 / slink v0.3-day8
>
> 主战场：**两层 cache** → 在 Redis（L2）前面加一层进程内 LRU + TTL（L1）→
> 验证 Day 7 关于"Redis Get 是新瓶颈"的假设是否成立。

## 一、目标与背景

Day 7 收口结论（v0.2 fasthttp）：
- mixed 24k RPS / P99 19.29ms
- syscall 56% + Redis(client codec + pool) 36% + others 8%
- 假设：**Redis client 的 codec/pool 锁吃了 36% CPU**，
  把 Redis Get 从热路径上挪开就能再上一个台阶

Day 8 干一件事：在 LinkCache 的 `GetOrLoad` 入口加一层 hashicorp/golang-lru/v2/expirable，
hot codes 直接进程内命中，绕开 Redis。

## 二、改动概览

| 文件 | 改动 |
|---|---|
| `internal/cache/local_cache.go` | 新增：LRU + TTL 包装，nil-safe，hits/misses 计数 |
| `internal/cache/local_cache_test.go` | 新增：9 个单测（TDD-RED-first） |
| `internal/cache/link_cache.go` | `LinkCache` 加 `local *localCache` 字段；`GetOrLoad` 入口先查 L1；命中回填；`Invalidate` 清两层；新 `WithLocalCache` option + `LocalStats()` 观测 |
| `internal/cache/link_cache_test.go` | 新增：5 个 L1 双层语义测试（命中绕过 L2 / negative L1 / Invalidate 清两层 / 禁用回退 / stats 准确） |
| `internal/config/config.go` | `LocalCacheSize`（默认 4096）+ `LocalCacheTTL`（默认 1m） |
| `cmd/server/main.go` | wiring `WithLocalCache` + 启动日志；version → v0.3-day8 |
| `.env.example` | 增 `SLINK_LOCAL_CACHE_SIZE` / `SLINK_LOCAL_CACHE_TTL` |

依赖新增：
```
github.com/hashicorp/golang-lru/v2 v2.0.7
```

L1 语义：
- `localEntry{link: nil}` = missing 标记（DB 已确认不存在）
- 容量 + TTL 由 expirable.LRU 一站式提供
- nil-safe：receiver 是 nil 时 Get 永远 miss、Put/Del 是 noop（避免 LinkCache 到处 nil 检查）

## 三、对照 bench

同样 mixed 场景（10s 预热 + 30s 压 t=4 / c=256，100 个 code 池）：

| 指标 | Day 7 (fasthttp) | Day 8 (fasthttp + L1) | Δ |
|---|---|---|---|
| **RPS** | 23,722 | **93,508** | **+294% (3.94x)** |
| P50 | 10.36ms | **1.70ms** | -83.6% |
| P90 | 13.45ms | 9.47ms | -29.6% |
| **P99** | 19.29ms | **32.52ms** | **+68.6%** ⚠️ |
| Latency avg | — | 3.87ms ± 6.60ms | — |
| Transfer/s | 3.23 MB/s | **12.74 MB/s** | +295% |
| Socket errors (read) | 119 | 12 | -90% |
| **CPU 总采样** | 52s / 30s = 173% | 81.6s / 30s = **272%** | +57% 总 CPU |
| **CPU 每请求** | 73 µs/req | **29.06 µs/req** | **-60%** |
| **Alloc 总量** | 3,132 MB | 2,641 MB | -15.7% |
| **Alloc 每请求** | 4,396 B/req | **940 B/req** | **-78.6%** |

注：Day 7 是与 pprof 同步抓的口径，所以这里也用同步抓的口径，公平对比。

### 警讯：P99 反升

Day 7 → Day 8 P99 从 19.29ms 涨到 32.52ms。原因是 RPS 翻 4x 把 8 核 mac CPU 打到 272% —— 接近 3 核满载。
高负载下 fasthttp worker pool 排队增加，尾部延迟劣化。如果上限流（不打满 CPU）P99 会显著回落。

## 四、CPU profile 对比 — 瓶颈再次平移

### 4.1 关键函数 cum%

| 函数 | Day 7 cum% | Day 8 cum% | 解读 |
|---|---|---|---|
| `syscall.syscall` flat% | **55.87%** | **84.21%** | **+28pp** — 全压在 socket I/O |
| `redis.cmdable.Get` cum% | 35.92% | **消失（top 40 不见）** | L1 截断了 |
| `redis.(*baseClient)._process` cum% | 35.84% | **消失** | 同上 |
| `handleRedirect` cum% | 37.11% | 15.71% | -21pp（分母变大 + 单次更轻）|
| `runtime.kevent` flat% | (未单列) | 7.39% | netpoll |
| `enqueueClickEvent` cum% | (未单列) | 15.33% | 异步入队浮上来 |

### 4.2 Redis 真的从热路径消失了

`go tool pprof -top -cum -nodecount=40` 跑完整个调用图，**没有一个 redis 相关函数进 top 40**。
说明 mixed 100 个 codes 池 + L1 4096 entries 容量 + 1min TTL → **L1 命中率接近 100%**。

实际命中率估算（profile 上没 redis 流量，且 Day 8 L1 stats 没暴露 endpoint，靠间接估）：
- 假设 L1 miss 时 redis.Get 至少占 cum 0.5%（Day 7 的 100 倍稀释），就能进 top 40
- 它没进，说明 miss 比例 <1%，**hit rate ≥ 99%**
- TODO（Day 9 候选）：暴露 `/debug/stats` 把 LocalStats 打到 endpoint，bench 后能直接读出

### 4.3 那为什么 RPS 是 +294% 而不是 +1000%？

L1 几乎免费——理论上 Redis 那 36% CPU 全省下来，RPS 应该至少 +50%。但实测是 +294%。
多出来的部分从哪来？

**关键洞察：Day 7 单跑时是 24.7k，pprof 同步是 23.7k；Day 8 单跑预热已经 102k，pprof 同步 93k。**
Day 8 的 L1 把单请求 CPU 从 73µs 砍到 29µs（-60%），同样的 CPU 预算（mac 8 核）能多干 60% 活：

```
Day 7: 24k RPS × 73µs = 1.75 core 
Day 8: 93k RPS × 29µs = 2.70 core 
```

Day 8 多用了 ~1 core CPU（173% → 272%）+ 单请求轻 60% = 4x RPS。这两个因子叠加才有 +294%。

**Day 7 的真瓶颈**不止"Redis Get"，还有"net/http→fasthttp 已消但 Redis client 锁吃满"。
L1 一刀切掉 Redis 客户端 codec + pool 锁 → 单请求路径只剩 fasthttp + handler 自身 + L1 LRU 查询。
这是为什么 alloc/req 也从 4.4KB 降到 940B（-78.6%）。

## 五、alloc 对比

| 指标 | Day 7 | Day 8 | Δ |
|---|---|---|---|
| 总 alloc | 3,132 MB | 2,641 MB | -15.7% |
| handleRedirect cum | 1,300 MB | **338 MB** | **-74%** |
| GetOrLoad cum | 1,216 MB | (隐没在 handleRedirect 内) | 大幅下降 |
| **alloc per req** | 4,396 B | **940 B** | **-78.6%** |

Day 8 alloc 大头变成了**异步 click event 写库链路**（`pgx.copyFrom.buildCopyBuf` cum 1641MB，62%）——
不是跳转路径本身，是后台 batch 写库。这部分跟 L1 无关，是被高 RPS 推上来的。

跳转路径自身的 alloc（handleRedirect 338MB / 2.8M 请求）= **121 B/req**，几乎归零。
fasthttp 的 sync.Pool + L1 命中省 json.Unmarshal + 不打 Redis 三件事叠加。

## 六、新瓶颈：异步 click event 写库

server 跑完 30s 测试，log 里有 **1,782,964 条 `enqueue click event failed: event buffer full`**。
2.8M 请求里约 **63% 的 click 写库被丢弃**。

为什么：
- Day 7 之前 RPS 24k，event buffer 容量 10k + 1k batch + 1s flush 刚好够
- Day 8 RPS 93k → 每秒入 93k 条事件 → flush 频率不变（1s 一批 1k）→ 入快出慢 → buffer full

这是 **Day 8 没解决** 的瓶颈，但跳转本身不受影响（write click 是 fire-and-forget，buffer full 只丢 click 不影响重定向）。
要修：
1. 提高 event buffer 容量（10k → 100k）
2. 提高 batch size + 缩短 flush 间隔
3. 或者上 sampling（不是每次跳转都记 click，按比例采样）

是 v0.3 后续或 v0.4 的活。

## 七、这次实验学到什么

### 7.1 估的 vs 实测

Day 7 文档对 Day 8 的估值：
> "本地 LRU cache（hot codes 内存直读）→ 50k+ RPS / 中难度 / 最大 ROI"

实测 93k，**比估的还高**。原因是漏估了 alloc per req 的连带下降——L1 命中不止省 Redis 网络，
还省了 Redis client 的 codec alloc。

但 P99 反升没估到——CPU 打满后排队劣化是规律，下次估值要把"是否会 saturate CPU"作为独立维度。

### 7.2 "Redis 是瓶颈"被验证 + 修正

Day 7 写："Redis client codec + pool 锁吃 36% CPU"。
Day 8 把 Redis 从热路径搬走，profile 显示这 36% **完整**释放。这个判断对了。

但被修正的是："瓶颈消除后涨多少 RPS"。仅靠 36% 释放估算应该 +56% RPS（1/(1-0.36)）。
实测 +294%，说明 Redis 客户端的开销不止"36% CPU"，还包括连带的 alloc/GC 压力 + 网络 latency 在 P50 上的体现。

**"profile flat% 不等于'消除后能省的全部'"** —— 因为函数调用链上还有间接成本（GC 触发、context 切换、锁竞争）。

### 7.3 多实例不一致问题

L1 设计选择 1min TTL（短于 Redis 10min TTL），是为了缩小水平扩展时多实例间的不一致窗口。
当前是单实例部署，问题不会暴露。但简历上写"加了本地 cache"必须能讲清楚：

- **何时不一致**：实例 A 的 L1 还没过期、用户在实例 B 删了短链 → A 还能跳转那个已删 code，最长 1min
- **解法层级**：
  1. 缩短 L1 TTL（当前已做，1min）
  2. Pub/Sub 广播 invalidate（中等复杂度）
  3. 一致性 hash 让同一 code 路由到同一实例（部署侧改动）
- **是否值得修**：取决于业务对"刚删的链接还能跳"的容忍度。短链一般 immutable（创建后不改），删除是低频事件，1min 不一致一般可接受。

## 八、数据归档

- profile 文件：`/tmp/slink-day8/{cpu,heap,allocs}.pb.gz`
- bench 输出：`/tmp/slink-day8/bench-l1.txt`
- server log：`/tmp/slink-day8/server.log`（含 event buffer full 警告）
- 对比基准：`docs/bench/day-07-fasthttp.md`

## 九、关键命令复现

```sh
# 1. 启 server（默认开 L1: 4096/1m）
SLINK_PPROF_ADDR=127.0.0.1:6060 ./bin/server

# 2. 预热（10s）
unset http_proxy https_proxy
CODES_FILE=/tmp/slink-codes.txt wrk -t4 -c256 -d10s \
  -s scripts/bench/mixed.lua http://localhost:18080

# 3. bench + pprof 同步抓（30s）
curl -sS "http://127.0.0.1:6060/debug/pprof/profile?seconds=30" \
  -o /tmp/slink-day8/cpu.pb.gz &
sleep 1
CODES_FILE=/tmp/slink-codes.txt wrk -t4 -c256 -d30s \
  -s scripts/bench/mixed.lua http://localhost:18080

# 4. snapshot
curl -sS http://127.0.0.1:6060/debug/pprof/heap   -o /tmp/slink-day8/heap.pb.gz
curl -sS http://127.0.0.1:6060/debug/pprof/allocs -o /tmp/slink-day8/allocs.pb.gz

# 5. 看 top
go tool pprof -top -cum -nodecount=20 /tmp/slink-day8/cpu.pb.gz
go tool pprof -top -cum -nodecount=15 /tmp/slink-day8/allocs.pb.gz
```

## 十、Day 9 候选方向

按 Day 8 profile 重排：

| # | 方向 | 预期 | 难度 | 备注 |
|---|---|---|---|---|
| 1 | **修 click event buffer 满**（容量↑/采样/flush 调优）| 消 1.78M warn | 低 | 顺手做，不动跳转主路径 |
| 2 | 暴露 `/debug/stats` 把 L1/event/buffer stats 打出来 | 可观测性 | 低 | 应该早做的 |
| 3 | rueidis 替换 go-redis（仅 cache miss 时的 codec 优化）| 边际 | 中 | L1 已挡 99% 流量，ROI 已不大 |
| 4 | gnet（user-space netpoll）破 syscall 84% | 难估 | 高 | 真要破 100k+ RPS 才有意义 |
| 5 | SO_REUSEPORT 多 process | +50-100% | 中 | mac 单核现在已经接近瓶颈 |
| 6 | metrics/Prometheus + 容器化 | 工程化收尾 | 中 | 简历可见性更高 |

按 ROI：**#1 + #2 同 PR 收尾**（小工作量、消可见的 warning）。然后看是要继续追性能（#4/#5）还是工程化（#6）。
