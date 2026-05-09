# slink v0.4 — 用 Kafka 把"主路径"和"事件落库"彻底解耦

> 5 天 5 commit。从 v0.3 PG 单 flusher 96.5% drop，到 v0.4 切流后 0 timeout + 故障域分离 + main 表 0 漏。

## 1. 起点：v0.3 真瓶颈

slink 在 v0.3 收口时拿到 86k RPS。表面看不错，但 Day 9 调参时发现一个尴尬数字：

- L1 cache 命中 99.97%
- 跳转主路径 RPS 86k
- **click_event PG 写入 dropped 96.5%**

也就是说 100 个 click 事件到达，PG 实际只落 3-4 条。剩下 96 个被 channel buffer 满了之后 silently drop。

为什么？v0.3 的事件链路是：

```
api/redirect.go ──Enqueue──▶ event.Buffer (channel cap=50k)
                                  │
                                  ▼ 1 个 flusher goroutine
                            store.ClickEventRepo.BatchInsert (COPY FROM PG)
```

单 flusher 出速 ~62k events/s（PG COPY FROM 单批 1000 行 ~16ms × 4 个 worker 会被串行化）。当 click 入速 100k+ 时稳态 49% 丢；当业务突增到 200k 时 96.5% 丢。

加 flusher 数量呢？决策稿 §2 算过：4 个 flusher 让出速到 250k，但 PG IOPS 已是天花板，再加效益归零；而且 PG 是 slink 的强一致性主存储，被 click 写打爆主创建链路也跟着抖。

**真问题**：click 落库不应该跟主存储写在同一台 PG 上。这是事件流 vs 业务写的本质差别。

## 2. 路线选型：为什么是 Kafka

候选三条路：

| 路线 | 解决了什么 | 没解决什么 | 验收成本 |
|---|---|---|---|
| **多 flusher + PG 分库** | 写吞吐线性扩 | 主路径仍然耦合 PG | 中 |
| **ClickHouse 直写** | 分析型存储天生扛 | 主存储替换 + 全部 schema 迁移 | 高 |
| **Kafka + Consumer 写 PG** | 主路径解耦 + 削峰 + 故障域分离 | Kafka 自身运维成本 | 中 |

选 Kafka 的理由（决策稿 §2.4）：
1. 主路径只 ↑ 一个 producer 调用，失败路径就是 v0.3 同款 drop+warn
2. Consumer 跟 server 不同进程 → 故障域天然分离
3. 客户端 buffer 100ms 缓冲 + broker 削峰 + retention 7d 兜底
4. 单 broker 单机起步，团队上手成本低（vs ClickHouse 全栈替换）

红线很明确：**v0.4 不是为了引入 Kafka 而引入**，是为了解 v0.3 的具体瓶颈。

## 3. 客户端选型：sarama vs kgo

决定用 Kafka 后第二天就遇到客户端选型。Go 生态有 3 个：sarama / kgo (franz-go) / confluent-kafka-go。

跑了 1 小时同口径 spike（30s / 单 producer goroutine / 100B JSON / lz4 / acks=leader / linger=5ms）：

| | sarama 1.45.2 | kgo 1.19.5 | 差距 |
|---|---:|---:|---:|
| RPS | 443,842 | **788,183** | kgo **1.78×** |
| ack p99 | 30.4 ms | 31.1 ms | 持平（broker bound）|
| heap inuse Δ | +8.6 MB | +19.0 MB | sarama 省 10MB |
| mallocs 30s | 134 M | 263 M | kgo 2× |

p99 持平的原因：两端都被 broker 处理速度封顶（broker bound），客户端再快也压不过 broker ack 时间。

但 kgo client 上限 1.78× → 当业务 100k+ QPS 时 sarama 已撞 client 天花板，kgo 还有余量。

选 kgo + 接受 mallocs 2× 的风险。回退预案：替换 `internal/event/kafka.go` 一个文件，接口 `Eventer` 不变。

> 详细决策稿在 `docs/concepts/kafka-client-choice.md`，可以当作"库选型同口径 spike"模板抄。

## 4. 落地节奏：双写 → 影子 → 切流

按决策稿 §8 三步走，每步前置一个 git tag 兜底回滚：

### Day 14 — 双写期

```
type DualWriter struct {
    primary   Eventer  // KafkaProducer (新)
    secondary Eventer  // event.Buffer (v0.3 兜底)
}
```

`SLINK_EVENT_BACKEND=dual` 同时投两边，串行调用 < 1ms 总开销。primary 失败仍调 secondary，反之亦然。

最关键的设计点是 100ms ctx 怎么写。直觉做法：

```go
sendCtx, cancel := context.WithTimeout(p.bgCtx, 100*time.Millisecond)
defer cancel()
p.cli.Produce(sendCtx, rec, callback)
```

**这是错的**。Enqueue 立即返回 → defer cancel 立即触发 → callback 还没跑就拿到 ctx.Canceled。

正确写法：cancel 移到闭包内：

```go
sendCtx, cancel := context.WithTimeout(p.bgCtx, p.cfg.SendTimeout)
p.cli.Produce(sendCtx, rec, func(_ *kgo.Record, ackErr error) {
    cancel()  // 闭包内 cancel — record 处理完才释放 ctx
    p.handleAck(ackErr)
})
```

Day 14 实测 alloc/req 1001 B vs 940 B 基线 = +6.5% < 10% 红线。Kafka producer 6.4M sent / 100% acked / 0 dropped。

### Day 15 — 影子期

新建 `cmd/consumer/main.go` 独立 binary（与 server 不同进程，故障域分离）。consumer 写 `click_events_shadow` 影子表（不动主表）。

按 spec §6.3：
- consumer group `slink.click_events.pg_writer`
- BatchSize ≤ 1000 / BatchTimeout 100ms
- DisableAutoCommit + 手动 commit 在 BatchInsert 成功后
- session timeout 30s

第一版 consumer 出了个真 bug：`processFetches` 不切片，单 PollFetches 拿回 590k record 全攒一个 batch，COPY FROM 单批爆 PG 内存。修法：rename `processFetches → decodeFetches`（语义只是 decode），切片移到 run() 循环按 BatchSize=1000 走。

**单测覆盖不到这个 bug** — 手写 fetches 最多 4 record，BatchSize=10 永远走不到切片分支。Smoke test 是最后一道闸。

Day 15 P4 端到端对账：
- Buffer → main: 1,494,995 = buffer.flushed = 0 漏
- Kafka → consumer → shadow: 2,170,721 = consumer.inserted = 0 漏
- **Kafka 路径捕获 92.8% vs Buffer 64% = +28.8 pp**

跨路径数字不等是 v0.4 设计本意：buffer cap 50k 装不下 78k RPS sustained，drop 36%；Kafka 100k buffer 只 drop 7%。

### Day 16 — 切流

3 步：
1. `SLINK_CONSUMER_TABLE=click_events`（consumer 改写主表）
2. `SLINK_EVENT_BACKEND=kafka`（关 Buffer 路径）
3. **删 buffer.go + dualwriter.go + Sink 接口搬到 event.go**

切流前先打 git tag `v0.3-buffer-final` — 删除前的回滚锚点。生产事故可 `git checkout` 回去。

切流后 wrk 实测 **93,607 RPS** 比 dual mode 78k 提升 **+21%**。原因：少了 buffer 跟 kafka 抢 CPU + 没有双写开销。

跨路径 capture 97.9%，比 dual mode Kafka 路径 92.8% 还高 5 pp。

## 5. 故障演练 — 真正的差距在这里

Day 16 切流后跑 3 轮独立故障演练（kafka / pg / consumer 各停 15s），每轮独立 baseline + 60s wrk。

| 故障 | RPS | 退步 | timeout | 主路径 |
|---|---:|---:|---:|---|
| baseline | 93k | — | 0 | 正常 |
| **PG 停 15s** | **97k** | **+4%** | 0 | 完全不感知（L1 99.99% 兜住）|
| **Consumer 停 15s** | **101k** | **+8%** | 0 | 完全不感知（独立进程）|
| Kafka 停 15s | 63k | -32% | 0 | 100ms timeout 反复触发 |

**关键洞察**：
- PG 故障 + Consumer 故障 RPS 都涨了。原因：故障期对应组件不再抢 CPU，server 反而更快。这是**故障域分离的硬证据**
- Kafka 故障 RPS 退 32%，但仍 0 timeout — handler 没卡。生产 3-broker ISR=2 配置下单 broker 挂不会触发 worst-case
- 所有 3 轮端到端 0 漏（恢复后 producer/consumer 30s 内追上）

对比 v0.3 的故障图景：v0.3 PG 抖动 → 主路径不卡（有 cache）但 click 全部 dropped。**主路径稳定性 == v0.3，但故障表面积变小，事件可恢复**。

## 6. 一些反直觉的发现

### 6.1 producer.dropped ≠ "没送出去"

Day 15 P4 看到一个奇怪数字：consumer.inserted = 2,170,721 > producer.acked = 2,161,995。consumer 多写了 8,726 条 — 凭空冒出来？

挖出来：`dropped` 是 callback "100ms 内没回执" 的计数。但这条 record 仍在 client buffer 里，几百毫秒后真到 broker，consumer 看到的是真实落盘。

**真实成功率（92.8%）> producer 自报的 acked/sent（92.4%）**。drop 计数器的语义是"我们放弃跟踪"，不是"没送达"。

### 6.2 Kafka client buffer 故障期 retry 满 5s 才 fail

Day 15 P5 看到 producer.errors 跳整 100k 台阶（0 → 100k → 200k → 300k 卡住）。

这是 kgo `MaxBufferedRecords=100000` × `RecordDeliveryTimeout=5s` 的边界 — 每次 buffer 翻一遍 fail 一批。

不影响功能（事件已被 dropped + errors 表达），但暗示有 fail-fast 优化空间：broker disconnect 期间直接 fail 所有 buffered，不等满 5s。v0.5 看是否值得。

### 6.3 单测 + smoke 是双层防线

Day 15 那个 `processFetches` 不切片的 bug，10 个单测全过，是 P3 smoke test 才暴露的。

教训：**生产规模下才显现的 bug 必须靠 smoke test 拦**。单测保函数级正确性，端到端 smoke 是最后一道闸。CI 跑单测过了不代表 ship-ready。

## 7. v0.4 完整数字（简历素材）

| 指标 | 数字 |
|---|---|
| 切流后单路径 RPS | 93,607 (+21% vs dual) |
| 端到端零漏（main 表） | 0 / 2,759,115 |
| Kafka 路径捕获率 | 97.9% (vs buffer 64%) |
| 故障期主路径 timeout | 0 / 3,795,697 reqs（kafka 故障 15s）|
| 故障恢复 producer 重连 | 1s（kafka healthy 即恢复）|
| 故障恢复 consumer 追平 | 30s 内追写 ~3M 条 |
| PG/Consumer 故障对主路径 | 0 影响（RPS 反而 +4-8%）|
| spike 客户端 1.78× | sarama 444k / kgo 788k RPS |
| alloc/req 守红线 | 940B → 1001B = +6.5% < 10% |

## 8. 后续（v0.5 候选）

不进 v0.4 范围内的事，留 v0.5：

1. ClickHouse 加在 consumer 端做实时 UV/HLL 聚合
2. producer.errors 的整 100k 台阶 fail-fast 优化
3. consumer.lag_seconds 真实指标（kgo admin API 拿）
4. clickEventWire 加 version 字段做 schema 演化
5. K8s 多副本部署 + OpenTelemetry trace

## 9. 写在最后

v0.4 用 6 个 work day 落了"Kafka 异步事件"。每天 ~3-4h，每天双 commit + 推 origin。

最重要的不是数字，是 3 件事：
1. **决策稿先于代码**（13 节 / 10 决策 / 范围红线 / Day-by-day 计划）
2. **每步 git tag 兜底**（删 buffer 前 v0.3-buffer-final）
3. **bench 文档跟代码同步落库**（`docs/bench/day-NN-*.md`）

代码可以删可以重写，但**那段时间你为什么这样想**只能在文档里捞回来。这就是 walkthrough + journal + bench 三套文档体系存在的理由。

> 完整源码 + walkthrough + bench 数据：[zombiecd/slink](https://github.com/zombiecd/slink)
