# 异步事件链路：channel buffer + batch flusher + COPY FROM

> **5 分钟标尺**：能讲清"为什么不能同步写"、channel + batch 怎么救场、停机怎么不丢数据。  
> **slink 位置**：`internal/event/buffer.go` + `internal/store/click_events.go`。

---

## 一、问题：每次跳转都要落事件，怎么不拖慢主链路

跳转是高 QPS 路径（slink v0.1 目标 5w QPS）。每跳转产生一条点击事件：

```
ClickEvent{event_id, code, ip, user_agent, ts, ...}
```

如果**同步写 PG**：
- 单条 INSERT ~5ms
- 跳转响应延迟 += 5ms（P99 直接翻倍）
- DB 单核 IOPS 上限 ~2 万写/秒，5w QPS 直接打挂 DB

**结论**：事件必须**异步**入库 + **批量**写。

---

## 二、设计目标三件套

| 目标 | 取舍 |
|---|---|
| 1. 跳转主链路不阻塞 | Enqueue 必须 ≪ 1ms，宁可丢 |
| 2. 入库吞吐 ≥ 跳转 QPS | 批量 + COPY FROM |
| 3. 停机不丢残余事件 | 优雅停机 + 最后一次 flush |

---

## 三、架构：channel + 后台 flusher

```
跳转 handler ────Enqueue (非阻塞 select)────▶ chan ClickEvent ──┐
                                                                 │
                                          ┌──────────────────────┘
                                          ▼
                                   后台 goroutine (run)
                                          │
                              ┌───────────┴────────────┐
                              ▼                        ▼
                        攒满 BatchSize=1000       FlushInterval=1s
                              │                        │
                              └──────── flush ─────────┘
                                          │
                                          ▼
                              store.ClickEventRepo
                                          │
                                          ▼
                                   PG COPY FROM
```

### 为什么用 channel

- 天然并发安全（Go runtime 保证）
- 满则丢的语义可以用 `select default` 一行实现
- 容量是上限保护（防止流量洪峰把内存撑爆）

### 为什么 BatchSize=1000 + FlushInterval=1s

- BatchSize=1000：满 1000 立即 flush。1000 条 COPY FROM 实测 18ms（见 `click_events_test.go::TestBatchInsert_1000Rows`）
- FlushInterval=1s：低 QPS 时（夜间）攒不满 1000 也每秒 flush，保证事件能近实时到 DB

经验值：BatchSize × FlushInterval 大致等于"一次 flush 周期处理的事件数 / 秒"，要让 sink 写完时间 < 周期。1000 条 / 18ms ≪ 1s → 安全。

---

## 四、Enqueue 的非阻塞 select

```go
// 简化版
select {
case b.ch <- evt:
    b.enqueued.Add(1)
    return nil
default:
    b.dropped.Add(1)
    return ErrBufferFull
}
```

**核心是 `default`**：channel 满时不阻塞，立刻走 default 分支。

如果不写 default：

```go
b.ch <- evt  // ← channel 满时这里会阻塞！跳转主链路被卡住
```

---

## 五、运行循环 run() 的核心

```go
for {
    select {
    case <-b.done:
        // 停机：drain channel 残余 + 最后一次 flush
        b.drain(&batch)
        b.flush(batch)
        return

    case <-ticker.C:
        // 周期 flush
        if len(batch) > 0 {
            b.flush(batch)
            batch = batch[:0]  // 复用 slice
        }

    case evt := <-b.ch:
        batch = append(batch, evt)
        if len(batch) >= b.cfg.BatchSize {
            b.flush(batch)
            batch = batch[:0]
        }
    }
}
```

三个 case 的优先级在 select 里是**随机**的——这刚好是我们要的：done 信号有时机收到、ticker 不会被高 QPS 饿死、事件正常处理。

`batch = batch[:0]` 是 Go 优化技巧：复用底层数组，避免每次 flush 后重新分配。

---

## 六、PG COPY FROM 比 INSERT 快多少

INSERT 1000 条：
- 1000 次 round-trip RTT（即使一个 batch 包一起发，PG 还要解析 1000 个 SQL）
- 每条 INSERT 都要走 SQL parser + planner

COPY FROM：
- 走二进制协议，绕过 parser
- 单次 round-trip，传输纯数据
- PG 内部用 fast-path 写入

slink 实测：

```
TestBatchInsert_1000Rows: 1000 rows COPY FROM: 18.16ms
```

平均 **0.018ms / 行**，比典型 INSERT 快 ~10x。

```go
// internal/store/click_events.go
n, err := r.pool.CopyFrom(
    ctx,
    pgx.Identifier{"click_events"},
    []string{"event_id", "code", "ip", "user_agent", "referer", "country", "region", "ts"},
    pgx.CopyFromRows(rows),
)
```

`pgx.CopyFromRows` 接受 `[][]any`，每个内层 slice 对应一行。pgx 内部做类型编码。

---

## 七、优雅停机：drain + 最后一次 flush

停机顺序很重要：

```go
// cmd/server/main.go
// 1. HTTP server Shutdown：停接新连接 + 等已有请求完成
httpSrv.Shutdown(shutdownCtx)
// 2. EventBuffer Stop：drain channel + 最后一次 flush
eventBuf.Stop(shutdownCtx)
// 3. defer 链关闭 redisCli / pgPool（在 run() 顶部 defer）
```

**反过来不行**：先关 PG → buffer flush 时 sink 已经死 → 残余事件全丢。

`Stop` 内部：

```go
func (b *Buffer) Stop(stopCtx context.Context) error {
    b.stopped.Store(true)  // 后续 Enqueue 立即返回 ErrBufferStopped
    close(b.done)          // 通知 run() 退出循环
    // 等 run() 真退出（它会做最后一次 flush）
    select {
    case <-doneCh:
        return nil
    case <-stopCtx.Done():
        return stopCtx.Err()  // 超时
    }
}
```

---

## 八、丢失场景与应对

| 场景 | 后果 | 应对 |
|---|---|---|
| Enqueue 时 buffer 满 | 该条事件直接丢，dropped++ | metric 告警 + 调大 Capacity |
| 停机时 stopCtx 超时 | 残余事件丢（drain 没走完） | shutdownGrace 给充足时间（10s 起） |
| flush 时 PG 抖动 | 整批丢，flushErr++ | v0.2 加 dead-letter 队列重试 |
| 进程 SIGKILL（kill -9） | 内存里没 flush 的全丢 | k8s preStop hook + livenessProbe 给 SIGTERM 时间 |

slink v0.1 接受"少量丢失" —— 点击事件用于统计而非交易，丢万分之一不影响业务。  
v0.2 切 Kafka 后 producer ack + 持久化，丢失率可降到 ~0。

---

## 九、运行时指标（atomic 统计）

`Buffer.Stats()` 返回：

```go
type Stats struct {
    Enqueued int64  // 累计入队
    Dropped  int64  // 累计丢弃（满则丢）
    Flushed  int64  // 累计成功落库
    FlushErr int64  // 累计 flush 失败次数
}
```

健康指标：

- **Dropped / Enqueued < 0.001**：buffer 容量够
- **Flushed ≈ Enqueued**：极端情况下 Flushed < Enqueued 说明有 flush 失败堆积
- **FlushErr 长期为 0**：sink 健康

main.go 在停机时打一份：

```go
slog.Info("event buffer stats", "stats", eventBuf.Stats())
```

生产应该 expose 到 Prometheus 做曲线。

---

## 十、5 分钟讲透自检

| 问题 | 能讲透 | 关键回答 |
|---|---|---|
| 为什么不能同步写 PG？ | ✅ | 5ms × 5w QPS = DB 早就挂了 + 跳转 P99 翻倍 |
| 为啥用 channel 不用 list+lock？ | ✅ | channel 是 Go 原生并发原语 + select 天然支持非阻塞/超时 |
| 满则丢怎么写？为啥不用阻塞？ | ✅ | `select { case ch<-x: case default: }` ；阻塞会拖垮跳转 |
| 触发 flush 的两个条件？ | ✅ | 攒满 BatchSize=1000 / 时间到 FlushInterval=1s |
| COPY FROM 比 INSERT 快多少？ | ✅ | ~10x（绕 SQL parser + 二进制协议 + 单次 RTT），slink 实测 1000 行 18ms |
| 停机时怎么不丢残余？ | ✅ | drain channel + 最后一次 flush，且 PG/Redis 关在 buffer.Stop 之后 |
| select 三个 case 优先级？ | ✅ | Go runtime 随机选，刚好让 done/ticker/事件 都不饿死 |
| `batch = batch[:0]` 是干嘛？ | ✅ | 复用底层数组，避免每次 flush 后重新分配 |
