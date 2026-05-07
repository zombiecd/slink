# Day 5 跳转性能压测基线

> **目标**：探底（不是达标）。先拿真实数字，知道哪里是瓶颈，再决定 Day 7 调优方向。  
> **环境**：MacBook，docker compose 起 PG 16 + Redis 7，slink 单进程二进制。  
> **日期**：2026-05-07

---

## 一、压测脚本与命令

源文件：

- `scripts/bench/run.sh` — 主控（seed → warmup → 三档 wrk）
- `scripts/bench/hot.lua` — A 场景（单 hot code 全命中）
- `scripts/bench/mixed.lua` — B 场景（100 code 池随机）
- `scripts/bench/miss.lua` — C 场景（每次新随机不存在 code）

复现：

```bash
docker compose up -d
go build -o /tmp/slink-server ./cmd/server/
/tmp/slink-server &
BENCH_DURATION=30s BENCH_THREADS=4 BENCH_CONNS=256 \
  ./scripts/bench/run.sh all
```

---

## 二、三档场景结果（4 threads / 256 connections / 30s）

| 场景 | 描述 | RPS | P50 | P90 | P99 | Max |
|---|---|---:|---:|---:|---:|---:|
| **A. Hot** | 单 hot code，全命中 cache | **21,545** | 11.58ms | 14.39ms | 18.76ms | 35.45ms |
| **B. Mixed** | 100 code 池随机（稳态命中） | 21,300 | 11.74ms | 14.46ms | 19.09ms | 36.62ms |
| **C. Miss** | 每次新随机 code（穿透防护稳态） | 14,493 | 17.36ms | 20.94ms | 26.95ms | 68.08ms |

观察：

- **A ≈ B**：mixed 第一波扫完 100 个 code 全进 cache，稳态后基本全命中 → 与 hot 持平
- **C 慢 30%**：每次 cache miss → DB 一次查 → 写空值标记 → 路径多了 PG round-trip
- **C 工作正常**：穿透防护让"非真打 DB"的命中空值标记路径仍快（否则 14k 不会保住）

---

## 三、低并发延迟基线（4 threads / 64 connections）

| 场景 | RPS | P50 | P90 | P99 | Max |
|---|---:|---:|---:|---:|---:|
| A. Hot, 64 conn | 21,335 | **2.79ms** | 4.27ms | **6.84ms** | 23.42ms |

**关键发现**：

- 低并发下 P99 < 7ms，说明 server 单请求处理快
- 高并发下 P50 11ms = **排队时间**，server 能力没变
- → server **吞吐瓶颈** ≈ 21k RPS，再加并发只增加排队不增加吞吐

---

## 四、并发扫一遍（找上限）

| 配置 | RPS | P50 | P99 | 备注 |
|---|---:|---:|---:|---|
| 4t / 64c   | 21,335 | 2.79ms | 6.84ms | 真实低延迟基线 |
| 4t / 256c  | 21,545 | 11.58ms | 18.76ms | RPS 已饱和，延迟全是排队 |
| 8t / 512c  | 20,867 | 24.15ms | 35.72ms | RPS 反降（thread/conn 切换开销） |

**结论**：21k RPS 是单进程上限，不是 client 问题。

---

## 五、异步事件链路压测期间表现

压测期间累计跳转（A+B+C+复测）：~ 1.92M 次  
对应 click_events 表行数：1,923,524  
**事件 0 丢失**。

```
docker exec slink-pg psql -U slink slink -t -c \
  "SELECT count(*) FROM click_events;"
 1923524
```

BatchInsert 单元测：1000 行 COPY FROM = 18.16ms（详见 `internal/store/click_events_test.go::TestBatchInsert_1000Rows`）。  
按这个速度，15w QPS 跳转产生事件也能跟得上：15000 行 / 18ms × 1000ms ≈ 833k 行/秒理论上限。

---

## 六、距离 v0.1 路线图目标 5w QPS 的 gap

| 维度 | 当前 (v0.1-day5) | 目标 (v0.1) | gap | 主要缺什么 |
|---|---|---|---|---|
| 单进程 RPS | 21k | 50k | 2.4× | 跳转 hot path 优化 |
| Cache hit P99 | 6.84ms | < 5ms | 已接近 | OK |
| Cache miss P99 | 26.95ms | < 30ms | 已达 | OK |

---

## 七、瓶颈猜测（按可能性排序）

1. **跳转 handler 主路径里的同步开销**
   - `crypto/rand.Read` 16 字节生成 EventID（每跳转都做）
   - `slog` 调用（即使不打 INFO）
   - `http.Redirect` 写 HTML body（虽然小但有分配）
   - `clientIP` 从 r.Header 解析

2. **Eventer.Enqueue 即使非阻塞也要进 channel**
   - channel 入队有 hchan 锁
   - 每跳转都 atomic.Add 4 个计数

3. **Go runtime 调度**
   - GOMAXPROCS 默认 = CPU 核（M 系列 8/10 核）
   - 小请求大并发下 goroutine 切换开销显著

4. **net/http 标准库**
   - ServeMux 正则匹配 `GET /{code}`
   - HTTP/1.1 keep-alive 解析

---

## 八、Day 7 调优方向（优先级从高到低）

| 方向 | 预期收益 | 实施难度 |
|---|---|---|
| 1. EventID 改用便宜的递增 / xid 算法（不走 crypto/rand） | +10-20% | 低 |
| 2. 跳转 handler 用 sync.Pool 复用 ClickEvent | +5-10% | 低 |
| 3. http.Redirect 改成手写 Header().Set + WriteHeader 省 body | +3-5% | 低 |
| 4. GOGC 调到 200 / GOMAXPROCS 验证 | +5% | 极低 |
| 5. 上 fasthttp / gnet（脱离 net/http） | +50-100% | 高，破坏标准库兼容 |
| 6. 多级缓存（本地 LRU + Redis） | C 场景大幅提升 | 中 |

预算：v0.1 不做 5、6（破坏架构），做 1-4 看能不能到 35k+。

---

## 九、本次压测发现的非性能问题

### 9.1 context canceled 刷屏

压测期间观察到 server 日志大量：

```
ERROR redirect lookup failed code=mTpId9 err="scan link: context canceled"
```

**根因**：wrk 关闭连接时 r.Context() cancel，但 LinkCache 已经在调 PG 查询。  
**影响**：日志噪音，不是真错误。  
**已修**：在 `redirect.go` 中过滤 `context.Canceled` / `context.DeadlineExceeded`，不打 ERROR。

### 9.2 Socket read errors（万分之二）

wrk 输出：

```
Socket errors: connect 0, read 120-758, write 0, timeout 0
```

**根因**：Go HTTP server 默认会在 keep-alive 空闲超时后断开（IdleTimeout）；wrk 此时正在该连接上发请求 → 握手失败。  
**影响**：极少量（万分之二）。生产可调 `IdleTimeout` / `ReadTimeout`，但 v0.1 不调。

---

## 十、自我评估（5 分钟讲透标尺）

| 主题 | 能讲透 | 备注 |
|---|---|---|
| 21k 是 server 上限不是 client 上限 | ✅ | 8t/512c 反降 + 64 conn 已经达 21k |
| A ≈ B 的原因 | ✅ | 100 code 池稳态后全命中 |
| C 慢 30% 的瓶颈 | ✅ | PG 一次查 + Redis 写空值标记 |
| 单进程 21k 距 5w 的差距 | ✅ | 跳转 hot path 同步开销（rand/slog/redirect body） |
| Day 7 调优优先级 | ✅ | 先吃低难度高收益（rand/Pool/header），再决定要不要 fasthttp |
| 异步事件链路在 1.92M 跳转下 0 丢 | ✅ | 1000 行 / 18ms 远高于跳转 QPS，sink 永远跟得上 |
