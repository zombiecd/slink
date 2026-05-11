# Day 23 P3 — consumer.inserted 字段语义核查

> 2026-05-11 / 纯代码核查 / 不动 stack
> 关联：Day 22 P3 末观察的 `inserted=5M vs PG count=9.88M` 矛盾

## 摘要 — 不是字段 bug，是诊断时点错误

Day 22 EOD 立的 T6 任务"consumer.inserted 字段语义不明"。本次代码核查 + Day 22 walkthrough 时序对照后**钉死结论**：

> **inserted 字段语义无歧义** — process-lifetime atomic.Int64 累计，consumer 启动后只增不减。
> Day 22 journal §4 记的"5M vs 9.88M 矛盾"是**两个不同时点 + 不同 consumer 进程的数字**，不是同一指标的不一致。

## 代码层面

### 字段定义 (`internal/event/consumer.go:44`)

```go
inserted       atomic.Int64
```

注释 (`internal/event/consumer.go:199`):
> Inserted: BatchInsert 成功累积的 row 数（= 写到 PG 的事件数）

### 累加点 (`internal/event/consumer.go:361`)

```go
for start := 0; start < len(records); start += c.cfg.BatchSize {
    chunk := records[start:end]
    if err := c.flushBatch(chunk); err != nil {
        c.insertErrors.Add(1)
        allOK = false
        break  // 后续 chunk 不再处理
    }
    c.inserted.Add(int64(len(chunk)))  ← 每个 chunk 成功后累加
}
if !allOK {
    continue  // 不 commit offset，下一轮重读整个 fetch
}
// 所有 chunk OK 才 commit
c.cli.CommitUncommittedOffsets(commitCtx)
```

### 语义结论

| 维度 | 行为 |
|---|---|
| 累计粒度 | process-lifetime atomic（consumer 进程启动 → 退出，单调递增）|
| 累加时机 | 每个 BatchSize=1000 chunk **flush 成功**后 |
| 失败行为 | chunk 失败 break，**已成功的 chunk 已经 .Add 过**，offset 不 commit → 下轮重读 |
| 与 PG count 关系 | **inserted ≥ PG count**（重复累加可能让 inserted 偏高，PG 主键去重保证幂等）|
| 进程重启 | atomic 计数器随进程死亡丢失，新进程从 0 起 |

## Day 22 P3 矛盾的真因

### 时序还原（Day 22 实测时序）

```
T1: P2 baseline wrk 60s 跑完
    → consumer.polled = 5,092,765
    → consumer.inserted = 5,092,765
    → PG 表 count ≈ 5.09M

T2: 中段 docker desktop 自爆 + 用户重启 docker
    → server/consumer 进程仍活（host 上的 nohup go run）
    → stack 容器恢复后 consumer 自动 reconnect

T3: P3 first try 跑 failure-drill-ch.sh A
    → reset_baseline 做 kafka-topics --delete + --create
    → kgo client 永久错误状态（topic delete bug）
    → consumer.polled / inserted 卡在 5,092,765（P2 残留数字）
    → consumer.lag_records 持续增长（broker 上数据没消费）
    → PG 真实 count = 0（reset_baseline TRUNCATE 后没新 INSERT）

T4: P3 A 路径
    → kill server + consumer
    → docker compose down -v + up
    → migrations + kafka-bootstrap
    → 重启 server + consumer  ← **新 consumer 进程，inserted 从 0 开始**
    → wrk 60s baseline + 60s fault round
    → 等 13min CH 追平
    → PG 表 count = 9,882,687
```

### 矛盾来源

journal §4 写的 `inserted=5,032,705 vs PG count=9,882,687` 不是同一 consumer 进程在同一时点的指标对比：

- `inserted=5,032,705` ≈ T3 时点（bug 触发后）的卡死值，本质是 T1 P2 baseline 的累计
- `PG count=9,882,687` 是 T4 A 路径跑完后的最终值，对应**新 consumer 进程**的累计

A 路径开始时 `down -v` 删了 PG/CH 主表 + 重启了 consumer 进程 → 新 consumer 的 inserted 计数从 0 起，跑完应该 ≈ 9.88M（journal 没记新 consumer 的 inserted 值，可能根本没拿）。

如果当时拿了新 consumer 进程的 stats，inserted 应当 ≈ PG count 9.88M，无矛盾。

## 教训 — Day 22 推断错的根因

journal §4 列了 3 个候选解释：

1. ❌ "consumer 进程中途短暂崩并重启（计数器重置）" — 部分正确（A 路径确实重启过，但 journal 没意识到 5M 那个数字属于旧进程）
2. ❌ "inserted 字段语义是上次 reset 之后的累计" — 错，atomic 是 process-lifetime
3. ❌ "数据竞态：inserted 计数和 PG 实际写入有 race condition" — 错，没 race，flushBatch 成功才 .Add

**真根因**：Day 22 在归档时**把不同时点 + 不同 consumer 进程的数字当作同一时点对比**。`5M` 那个 stats 是 P3 first try bug 触发瞬间的快照（属于 T3 时点），P3 A 路径完成后没重新拿 stats（属于 T4 时点的应有数据）。

## 元认知教训

按元认知 §3「被质疑时：诚实 > 看起来专业」，当观察到反差时第一动作应该是回 spec/代码逐项核对，而不是凭印象推断 3 个候选解释让用户来判。

Day 22 journal §4 写"待 Day 23 查代码确认"是正确的暂存，但 24h 后 Day 23 P3 这次核查只用了 ~5min（grep + 读 200 行代码 + 对照 walkthrough 时序），证明这本应在 Day 22 当下就完成 — 当时手头有进程、stats 还能拿、能复现。

**SOP 落地**：任何 stats 字段下次依赖前必须先 `grep` 定义 + 看一段累加点。

## 不需要 follow-up

代码层面无 bug，文档层面这个"矛盾"是误记，**T6 收口**。

后续如果需要 PG count 对账，**首选**是直接 `SELECT count(*) FROM click_events`，不要拿 inserted 推算（重复 chunk 会让 inserted 偏高，进程重启会让 inserted 偏低，都不可靠）。

inserted 的正确用途：
- 监测 consumer 是否在持续工作（速率 = inserted 在两次采样间的 delta）
- 不能用作"PG 表里有多少行"的代理指标
