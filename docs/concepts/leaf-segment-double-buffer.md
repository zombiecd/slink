# 双 Buffer 号段：从 Leaf 算法到 Go 实现

> 5 分钟讲透：朴素号段的痛点 → 双 buffer 状态机 → 异步预取 → 失败容错。
> 对应文件：[`internal/id/segment.go`](../../internal/id/segment.go)

## 一、问题：朴素号段的"取段抖动"

[ADR-0002](../adr/0002-id-segment-not-snowflake.md) 决定了 slink 用号段模式。最朴素的号段是**单 buffer**：

```
启动     → 取 [1, 1000]，cursor = 0
NextID   → cursor = 1, 2, 3, ..., 999
NextID   → cursor = 1000，**段耗尽**
NextID   → 同步 UPDATE id_segment ... → 取 [1001, 2000] → cursor = 1001
```

**痛点**：第 1000 次的 NextID 会**阻塞 5-50ms**（取决于 DB 响应）。在大促创建场景：

- 用户 A 第 999 次创建：~50 µs（内存）
- 用户 B 第 1000 次创建：~30 ms（撞 DB） ← 抖动
- 用户 C 第 1001 次创建：~50 µs

**P99 受这个抖动拖累**。

## 二、解法：美团 Leaf 双 buffer

**核心思想**：在 cur 段用到 90% 时，**异步**预取下一段填进 next。当 cur 耗尽时，**瞬时切换**到 next，前台请求 0 阻塞。

```
[初始]
cur:  [1, 1000]      cursor=0
next: nil
        ↓ NextID×900
[到达阈值 90%]
cur:  [1, 1000]      cursor=900
next: nil      → 触发异步预取
        ↓ 后台 goroutine UPDATE → 拿到 [1001, 2000]
cur:  [1, 1000]      cursor=900..999
next: [1001, 2000]   ready
        ↓ NextID×100 用完 cur
[切换]
cur:  [1001, 2000]   cursor=1000  ← 瞬间切换
next: nil            → 立刻触发新一轮预取
```

**收益**：DB 取段始终在后台进行，前台 NextID 永远 ~100 ns。

## 三、状态机（slink 真实实现）

slink 在 mock source（无 DB 延迟）下跑出来的轨迹：

```
#01 id=1   cur[1-5 cursor=1 use=20%]   next_ready=false   src_calls=1
#02 id=2   cur[1-5 cursor=2 use=40%]   next_ready=false   src_calls=1
#03 id=3   cur[1-5 cursor=3 use=60%]   next_ready=false   src_calls=1
#04 id=4   cur[1-5 cursor=4 use=80%]   next_ready=true    src_calls=2  ← 60% 触发的预取完成
#05 id=5   cur[1-5 cursor=5 use=100%]  next_ready=true    src_calls=2  ← cur 耗尽
#06 id=6   cur[6-10 cursor=6 use=20%]  next_ready=false   src_calls=2  ← 切换到 next
#07 id=7   cur[6-10 cursor=7 use=40%]  next_ready=true    src_calls=3  ← 切换瞬间触发新预取
...
```

阈值在示例里设的 60%（slink 默认 90%），但状态机相同。

## 四、Go 实现（核心代码）

### 4.1 数据结构

```go
type DoubleBuffer struct {
    mu        sync.Mutex
    cur       *segmentRange   // 当前段
    next      *segmentRange   // 预取段（可能 nil）
    refilling bool            // 异步预取进行中？

    bizTag    string
    stepSize  int64
    src       SegmentSource
    threshold float64         // 默认 0.9
    log       *slog.Logger
}

type segmentRange struct {
    low, high int64           // [low, high] 闭区间
    cursor    int64           // 已分配到的最后一个 ID
}
```

### 4.2 NextID 主路径

```go
func (db *DoubleBuffer) NextID(ctx context.Context) (int64, error) {
    db.mu.Lock()
    defer db.mu.Unlock()

    // 1. cur 用尽 → 切到 next 或同步补取
    if db.cur == nil || db.cur.exhausted() {
        if db.next != nil {
            db.cur = db.next
            db.next = nil
            db.maybeAsyncRefillLocked()  // 立刻预取下一段
        } else {
            // starvation：异步预取没赶上 → 同步取
            high, err := db.src.Acquire(ctx, db.bizTag, db.stepSize)
            if err != nil { return 0, err }
            db.cur = newSegment(high-db.stepSize+1, high)
        }
    }

    // 2. 分配
    id := db.cur.take()

    // 3. 阈值触发预取
    if db.cur.usage() >= db.threshold {
        db.maybeAsyncRefillLocked()
    }

    return id, nil
}

func (db *DoubleBuffer) maybeAsyncRefillLocked() {
    if db.next != nil || db.refilling { return }
    db.refilling = true
    go db.asyncRefill()
}
```

### 4.3 异步预取

```go
func (db *DoubleBuffer) asyncRefill() {
    ctx, cancel := context.WithTimeout(context.Background(), db.asyncTimeout)
    defer cancel()

    high, err := db.src.Acquire(ctx, db.bizTag, db.stepSize)

    db.mu.Lock()
    defer db.mu.Unlock()
    defer func() { db.refilling = false }()  // 失败也复位，下次到阈值再试

    if err != nil {
        db.log.Warn("async refill failed", "err", err)
        return
    }
    if db.next != nil {
        // 防御：理论上不该发生
        db.log.Warn("async refill discarded: next already filled")
        return
    }
    db.next = newSegment(high-db.stepSize+1, high)
}
```

## 五、并发安全分析

### 单 mu 守护一切

slink 用一个 sync.Mutex 守护所有共享状态（cur / next / refilling）。

**为什么不用 atomic + lock-free**：

- 状态机 5 个变量耦合（cur / next / cursor / refilling），lock-free 算法极易 bug
- 持锁时间极短（内存自增 + 简单判断 ~100 ns）
- mutex 在 Go 里是 fast path（spin + futex），抢锁竞争不严重

**Benchmark 数据**：

```
BenchmarkDoubleBuffer_NextID-8          11576338    105.2 ns/op    0 B/op
BenchmarkDoubleBuffer_NextID_Parallel-8 11551674    107.9 ns/op    0 B/op
```

并发 8 核也维持 ~100ns/op——锁竞争**没成为瓶颈**。每秒 ~1000 万 ID 远超业务需求。

### 异步预取的锁顺序

```
后台 goroutine：
  1. db.src.Acquire(ctx, ...)         ← 不持锁（DB 操作不能持锁）
  2. db.mu.Lock()
  3. 写 db.next 和 db.refilling
  4. db.mu.Unlock()

前台 NextID：
  1. db.mu.Lock()
  2. ... 读写
  3. db.mu.Unlock()
```

**关键**：异步 goroutine 的 DB 调用**在锁外**。否则前台请求会被 5-50ms 的 DB 延迟阻塞 → 双 buffer 失去意义。

## 六、starvation 处理

**场景**：异步预取**比 cur 耗尽更慢**：

- DB 慢
- 网络抖动
- 服务突然爆 QPS（cur 几秒就用完）

**slink 的策略**：

```go
if db.cur == nil || db.cur.exhausted() {
    if db.next != nil {
        // 正常路径
    } else {
        // starvation 路径：持锁同步取（罕见）
        db.log.Warn("starvation: synchronous segment fetch")
        high, err := db.src.Acquire(ctx, ...)
        ...
    }
}
```

**为什么持锁同步取**：

- 解锁再获取会引入复杂的"再检查"逻辑（其他 goroutine 可能已修改 cur）
- starvation 罕见——v0.1 接受 5-50ms 抖动

**生产监控**：starvation log 应作为告警指标。频繁 starvation = step_size 设太小或 DB 太慢。

## 七、失败容错

异步预取失败不应让服务崩。slink 的设计：

```
async refill 失败：
  1. log warn（错误细节）
  2. refilling = false（允许下次到阈值再触发）
  3. 不重试（避免重试风暴打死 DB）

下一次 NextID：
  - cur 还没耗尽 → 继续分配，下次到阈值再触发预取
  - cur 耗尽 → 走 starvation 同步路径

效果：
  - DB 短暂抖动：自然恢复，最差表现为偶发 starvation
  - DB 长期挂：所有 NextID 同步阻塞 → 上层超时 → 用户看到错误
```

**Trade-off**：

- 没有重试 → 短暂故障下表现差一点
- 有重试 → DB 雪崩时被打得更惨

slink v0.1 选**无重试**，依赖 DB 的高可用而非应用层兜底。生产可加 exponential backoff（v0.2+）。

## 八、step_size 怎么调

```
step_size 太小：
  - 取段太频繁，DB 压力大
  - 重启浪费小

step_size 太大：
  - 取段稀疏，DB 压力小
  - 重启浪费大
  - 单段持有时间长（数据量异常时号段可能耗尽）
```

**经验**：

```
step_size ≈ 创建 QPS × 期望取段间隔（秒）

创建 QPS = 1k，希望每 1 秒取一次段 → step_size = 1000  ← slink 默认
创建 QPS = 10k，每 5 秒一次 → step_size = 50000
```

slink v0.1 取 1000，应对到 1w QPS 创建毫无压力。

## 九、还有什么没做（v0.5+ 范围）

| 优化 | 说明 |
|---|---|
| 重试 + backoff | DB 短暂抖动时减少 starvation |
| 多段预读 | 一次取 N 段，next 满了还可以填到 nextNext |
| 自适应 step_size | 监控 starvation 频率，自动调 step |
| 跨机房 | 每机房独立 biz_tag（如 link_dc1, link_dc2） |
| 持久化重启状态 | 启动时不浪费当前段 |

slink v0.1 故意不做——简单优先。

## 十、踩坑清单

| 坑 | 后果 | 解法 |
|---|---|---|
| 异步取段持锁 | 阻塞前台请求 | DB 操作必在锁外 |
| 异步失败不复位 refilling | 永远不再预取 → starvation | defer reset |
| 切换 cur/next 不原子 | ID 重复或丢失 | 单 mu 守护整个状态机 |
| ctx 不传 | 异步取段卡死 | asyncRefill 用 background + 自带 timeout |
| starvation 没监控 | 调优瓶颈不知道 | log warn + 指标暴露 |
| 假设 step_size = DB 字段 | 加灵活性 | 调用方决定每次取多少（slink 这么做） |

## 十一、5 分钟自检

合上文档：

1. 朴素号段的痛点是什么？体现在 P99 哪里？
2. 双 buffer 状态机的 5 个状态？
3. 为什么异步 goroutine 的 DB 调用必须在锁外？
4. starvation 何时发生？slink 怎么处理？
5. step_size = 1000 是怎么估算出来的？

## 十二、延伸阅读

- [美团 Leaf 分布式 ID 生成系统](https://tech.meituan.com/2017/04/21/mt-leaf.html)（号段 + 双 buffer 思想）
- [Leaf 源码（Java）](https://github.com/Meituan-Dianping/Leaf)
- [Twitter Snowflake](https://github.com/twitter-archive/snowflake)
- [Sony Sonyflake](https://github.com/sony/sonyflake)
- [Discord 公司用 Snowflake 的故事](https://discord.com/blog/how-discord-stores-billions-of-messages)
- ADR-0002: 选号段不选 Snowflake
