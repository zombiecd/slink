# Kafka Go 客户端选型 — sarama vs franz-go (kgo)

> 状态：✅ 已决策（2026-05-07，Day 13 spike）· 决策：**采用 franz-go (kgo)**
> 配套架构稿：`docs/architecture/v0.4-kafka.md` §5.1
> spike 代码：`cmd/spike-sarama/main.go` + `cmd/spike-kgo/main.go`（选型完成后归档/删除）

---

## 一、TL;DR

| | sarama | **franz-go (kgo)** |
|---|---|---|
| 吞吐（RPS） | 443,842 | **788,183** |
| ack p99 | 30.4 ms | 31.1 ms |
| heap inuse Δ | +8.6 MB | +19.0 MB |
| API 风格 | struct config | **函数 option（context-first）** |
| cgo 依赖 | 无 | **无** |
| 维护方 | IBM 接管 fork | twmb 个人 + 活跃 |

**结论**：kgo **吞吐 1.78×**，p99/max 与 sarama 持平（broker bound），API 更现代。slink v0.4 producer 选 **kgo**。

---

## 二、为什么需要选

架构稿 §5.1 列了三个候选：sarama / franz-go (kgo) / confluent-kafka-go。

- **confluent-kafka-go**：性能最强但带 cgo（librdkafka），交叉编译/容器镜像变复杂。slink Dockerfile 现在是 scratch / distroless 思路，cgo 是负担。**先排除**。
- **sarama**：Go 生态最广（曾是 Shopify 维护，现在 IBM fork 接管）。API 是经典 struct config，"成熟但陈旧"。
- **franz-go (kgo)**：相对新，但 design-from-scratch（不是 wrap librdkafka），全 Go。API 是函数 option，context-first，符合现代 Go 风格。

剩 sarama vs kgo 二选一，**唯一仲裁标准是数字**。

## 三、Spike 设置

两个 spike main 严格同口径：

| 维度 | 值 |
|---|---|
| Topic | `slink.click_events`（4 partitions, RF=1） |
| Broker | apache/kafka:3.9.2 KRaft 单节点（本机 docker-compose） |
| 时长 | 30s feed + ≤5s drain |
| 并发 | 单 goroutine 喂消息（async） |
| 消息 | ~100 byte JSON click_event payload（key = base62 7 位 code） |
| acks | LeaderAck（kgo 需 `DisableIdempotentWrite()`） |
| 压缩 | lz4 |
| linger | 5 ms |
| max in-flight per broker | 5 |
| 测量 | enqueue→ack 延迟（callback / metadata 时间戳）+ runtime.MemStats |

> **关于 LeaderAck**：单 broker 没 ISR，LeaderAck 与 acks=all 等价。kgo 默认开 idempotency（要求 acks=all），spike 显式关掉以与 sarama 同口径。生产模式下 v0.4 起步沿用 LeaderAck（架构稿 §5.3）。

## 四、稳态数字（第二次跑，已剔除冷启动 GC + metadata fetch 噪声）

| 指标 | sarama 1.45.2 | kgo 1.19.5 | 倍率/差距 |
|---|---:|---:|---:|
| **RPS sent** | 443,842 | **788,183** | **kgo 1.78×** |
| sent / 30s | 13.3 M | 23.6 M | — |
| acked == sent | ✓ | ✓ | 0 fail 双方 |
| **ack p50** | 7.79 ms | **4.90 ms** | kgo -37% |
| ack p90 | 11.39 ms | 8.79 ms | kgo -23% |
| **ack p99** | 30.38 ms | 31.10 ms | **持平** |
| ack max | 35.21 ms | 35.23 ms | **持平** |
| heap inuse Δ | +8.6 MB | +19.0 MB | sarama 省 10 MB |
| total alloc 30s | 21.7 GB | 24.9 GB | kgo +15% |
| mallocs 30s | 134 M | **263 M** | kgo 2× |
| binary 大小 | 11.1 MB | 12.0 MB | kgo +8% |

> 第一次跑 sarama p99=95ms，是首次拉 metadata + cold GC 影响。二次跑稳态后 p99 与 kgo 持平。所有数字以二次跑为准。

## 五、解读

### 5.1 吞吐差距 ~1.78× 的原因

- **kgo sticky partitioner**：单 producer 喂消息时倾向粘在同一 partition 攒大 batch，broker 端处理单 batch 比处理碎 batch 高效得多。
- sarama 默认 hash partitioner 把 key=code 打散到 4 partitions，单 30s 内每个 partition 各自集 batch。
- 同样 `linger=5ms`，kgo 一个 batch 攒得更大 → broker 一次写盘的 amortized cost 更低。

**实战意义**：v0.3 入速 ~93k/s 的真实场景，单 producer 远低于这两个 spike 的极限（443k / 788k）。差距在生产端不会被感知 — broker 才是真瓶颈。但 client CPU/alloc 占用低，主进程 P99 退步风险低。

### 5.2 p99 持平 = broker bound

两个 client 的 p99 都在 ~30ms，max 都在 ~35ms — 这说明 30s 持续高压下，**瓶颈是 broker 处理单个 produce request 的耗时**，不是 client encoder/network。换句话说：选谁 client，p99 都是这个数。

> 这个观察让选型不那么"性能压倒一切"，可以更看重 API、生态、维护性。

### 5.3 mallocs 2× 是 kgo 的代价

kgo 每条消息一个 callback 闭包 + Record struct，分配频率高。但 heap inuse 19MB 仍可控，GC 没压垮。

**Day 14 接生产代码时验证**：跑 fasthttp 主进程压测，看 alloc/req 是否退步（v0.3 实测 940 B/req）。如果退步显著（> 10%），考虑 record pool。

## 六、其他维度

| 维度 | sarama | kgo | 取舍 |
|---|---|---|---|
| API 风格 | `cfg.Producer.Compression = sarama.CompressionLZ4` | `kgo.ProducerBatchCompression(kgo.Lz4Compression())` | kgo 函数 option 更现代 |
| context-first | 部分（同步/批量 API 有 ctx，async 没有） | 全 API 默认带 ctx | **kgo 胜** |
| 错误处理 | `Successes()` / `Errors()` channel | callback `func(*Record, error)` | 各有适用，无明显优劣 |
| 测试支持 | `mocks/` 子包 | `pkg/kfake` 内嵌 fake broker | kgo 略优 |
| 文档 | godoc + 示例多 | godoc + 大量 example main | 持平 |
| 维护 | IBM 接管 Shopify fork（2023+） | twmb 一个人 + 活跃（v1.21 2025） | sarama 更稳 |
| 依赖图 | 较干净 | 较干净（自带 kmsg / kgo 拆分） | 持平 |

**API 现代度**是 kgo 决定性优势。架构稿 §5 列的 KafkaProducer 接口是 context-first，kgo 直接 fit。

## 七、决策

**采用 franz-go (kgo)**。理由排序：

1. **吞吐 1.78×**（在 client 不饱和场景虽不一定用得到，但代表"少了一道天花板"）
2. **API context-first** + 函数 option，与 v0.3 现有代码风格一致（fasthttp router 也是函数 option）
3. **纯 Go 无 cgo**（与 confluent-kafka-go 区分）
4. **p99 与 sarama 持平**（broker bound），无性能劣势
5. **生态新但活跃**（twmb 维护频繁）

## 八、回退预案

如果 Day 14 全链路压测发现 kgo malloc 推高主进程 P99 / 主机 CPU > 15%，回退到 sarama（成本：替换 internal/event/kafka.go 一个文件，接口 Eventer 不变）。

回退触发条件留在 Day 14 journal 跟踪。

---

## 九、原始数据

```
=== spike-sarama (二次稳态) ===
sent:        13,315,390
acked:       13,315,390
RPS:         443,842
ack:         p50=7.79ms p90=11.39ms p99=30.38ms max=35.21ms
heap delta:  +8,816 KB
total alloc: 22,173,007 KB / mallocs=134,477,909

=== spike-kgo (二次稳态) ===
sent:        23,645,507
acked:       23,645,507
RPS:         788,183
ack:         p50=4.90ms p90=8.79ms p99=31.10ms max=35.23ms
heap delta:  +19,440 KB
total alloc: 25,502,517 KB / mallocs=263,322,520
```

环境：M-series macOS / Docker Desktop / apache/kafka:3.9.2 KRaft 单节点 / Topic RF=1 P=4 / lz4。
