# Cache-Aside 三大坑：穿透 / 击穿 / 雪崩

> **5 分钟标尺**：能用通俗语言解释三个坑分别是什么、为什么会塌、怎么挡，不背名词。  
> **在 slink 的位置**：`internal/cache/link_cache.go` 的 `LinkCache.GetOrLoad`。

---

## 一、Cache-Aside 模式本身

最朴素的"缓存当读路径加速器"模式：

```
请求来 ─→ 查 Redis ─┬─ 命中 → 直接返回
                    └─ 不命中 → 查 DB → 把结果写回 Redis → 返回
```

3 行伪代码：

```go
v, err := redis.Get(key)
if err == redis.Nil {
    v = db.Query(...)
    redis.Set(key, v, ttl)
}
return v
```

朴素方案在三种场景下会塌——三个坑都是"恶意/极端流量打穿缓存层，把 DB 打挂"的变形。

---

## 二、坑 1：缓存穿透（Penetration）

### 现象

攻击者构造大量**根本不存在**的 key（比如随机 8 位字符串），每次请求：

```
请求 → Redis 查 → MISS → DB 查 → DB 也没有 → 不写缓存 → 返回 404
```

下次同样的 key 来，又是 MISS → DB → 没有 → 没缓存。**Redis 完全没保护到 DB**。

### 为什么 DB 会塌

DB 单查不慢，但量级一上来：
- 1 万 QPS 全打 DB
- 每条查询 ~5ms × 1 万 = DB 连接池立刻爆满
- 真实用户的"查存在 key"也连不上 DB → **整个站挂**

### 防护：空值缓存

DB 说"不存在"时，**把"不存在"这个事实也缓存起来**：

```
请求 → Redis 查 ─┬─ 命中真值 → 返回
                 ├─ 命中"不存在标记" → 直接返回 404（不打 DB）
                 └─ MISS → 查 DB
                              ├─ 真有 → 写真值 + 长 TTL
                              └─ 没有 → 写"不存在标记" + 短 TTL
```

slink 实现：

```go
// internal/cache/link_cache.go
const missingMarker = "__nil__"
const defaultMissingTTL = 30 * time.Second
```

### 关键设计点

1. **空值标记必须和正常值区分得开**——不能用空字符串（"长 URL 真的是空"和"不存在"语义会冲突）
2. **空值 TTL 要短**——30s 即可：攻击者扫一波就停，长 TTL 反而会让"后来真创建了同 code"的用户拿不到结果
3. **空值标记和正常值都要带抖动**——见下面雪崩

### 还有更狠的：布隆过滤器

如果"不存在的 key 数量级 >> 存在的"（典型：用户 ID 暴破），空值缓存会撑爆 Redis 内存。
此时上**布隆过滤器**：把所有真实 key 编进 BloomFilter，请求来先查 BF，BF 说"绝对不存在" → 直接拒绝，不走 Redis。
v0.2 计划加。

---

## 三、坑 2：缓存击穿（Breakdown）

### 现象

某个**热点 key 突然过期**（比如热搜短链的 cache 到期），同一刻 1 万个请求并发查这个 key：

```
1 万请求 → Redis 查 → 全部 MISS → 1 万次并发打 DB → DB 爆
```

### 为什么和穿透不一样

穿透是"key 本来就不存在"，击穿是"key 真实存在、只是缓存过期了瞬间"。  
击穿可能比穿透更可怕——**真实热点 key** 才会有 1 万并发，冷 key 没人查。

### 防护：singleflight（合并并发回源）

让"同一个 key 的并发回源请求只跑 1 次"：

```
goroutine 1 ─→ 查 DB ────────→ 写 cache → 返回
goroutine 2 ─→ 等 1 ──────────────────→ 共享结果 → 返回
goroutine 3 ─→ 等 1 ──────────────────→ 共享结果 → 返回
...
goroutine 10000 ─→ 等 1 ─────────────→ 共享结果 → 返回
```

### slink 实现

```go
import "golang.org/x/sync/singleflight"

type LinkCache struct {
    sf singleflight.Group
    ...
}

func (lc *LinkCache) GetOrLoad(...) (*model.Link, error) {
    // ... cache miss 后 ...
    v, err, _ := lc.sf.Do(key, func() (any, error) {
        // 同一个 key 的并发只有第一个进入这里
        return loader(ctx)
    })
    return v.(*model.Link), err
}
```

### 关键设计点

1. **key 是合并粒度**：不同 key 的请求不互相阻塞
2. **double-check**：进入 sf 函数后再查一次 cache——可能在排队期间另一个 goroutine 已经填好了
3. **回源失败也要返回错误**：所有等待者都拿到同一个错误，比"全部独立失败"信息更一致

### 锁的两种粒度

- **进程内 singleflight**：单实例 OK；多实例时每个实例还会各打 1 次 DB（10 实例 = 10 个并发，可接受）
- **分布式锁**（Redis SETNX）：跨实例只允许 1 个回源；但锁本身就是热点，简单场景不必上

slink v0.1 单进程 → singleflight 够用。

---

## 四、坑 3：缓存雪崩（Avalanche）

### 现象

大批 key **同一刻全部过期**，瞬间所有请求都 MISS，并发打到 DB：

```
00:00:00 — 1 万 key 同时过期
00:00:01 — 1 万 QPS 同时打 DB → DB 挂
```

典型触发：
- 服务启动时批量预热缓存（全部 TTL = 现在 + 10 分钟，10 分钟后一起死）
- Redis 重启后空 cache + 流量冲入
- 凌晨批处理把一批 key 批量写入

### 为什么 singleflight 救不了

singleflight 是按 key 合并，雪崩是**很多不同的 key** 同一刻 miss。

### 防护：TTL 抖动

每条记录的 TTL 在基础值上加 ±10% 的随机扰动：

```go
// internal/cache/link_cache.go
const jitterPct = 0.10

func jitter(ttl time.Duration) time.Duration {
    delta := float64(ttl) * jitterPct
    offset := (rand.Float64()*2 - 1) * delta  // [-delta, +delta]
    return ttl + time.Duration(offset)
}
```

10 分钟 TTL → 实际在 [9 分钟, 11 分钟) 随机分布。  
1 万 key 即使同一刻写入，也会在 2 分钟窗口里逐渐过期，DB 压力被摊薄到 ~83 QPS / 秒。

### 抖动比例怎么定

- 太小（1%）：摊薄不够，雪崩还在
- 太大（50%）：缓存命中率下降（更早失效）
- **经验值 10%**：摊薄到原来 1/N（N = TTL × 抖动比例 / 1秒），命中率影响微乎其微

### 雪崩还有别的防线

1. **多级缓存**：本地 LRU + Redis，本地缓存就算 Redis 全没也不全打 DB（v0.2）
2. **限流降级**：DB 前面加令牌桶，超过阈值返回降级响应
3. **熔断**：DB 错误率超阈值时直接断开，宁可全 503 不要把 DB 拖挂

---

## 五、三个坑放一起的对照表

| 坑 | 触发场景 | 关键症状 | slink 防护 | 代码位置 |
|---|---|---|---|---|
| **穿透** | 不存在的 key 被反复查 | DB 收到大量 NULL 查询 | 空值缓存 (`__nil__` + 30s TTL) | `setMissing` |
| **击穿** | 单个热点 key 同时刻过期 | DB 收到同 key 的并发查 | singleflight 合并 | `sf.Do(key, ...)` |
| **雪崩** | 大批 key 同时刻过期 | DB 收到大量不同 key 的查询 | TTL ±10% 抖动 | `jitter()` |

---

## 六、slink 的实测验证

`internal/cache/link_cache_test.go` 三个坑各一组测：

```bash
go test ./internal/cache/... -run "TestLinkCache_PenetrationProtection" -v
go test ./internal/cache/... -run "TestLinkCache_BreakdownProtection_Singleflight" -v
go test ./internal/cache/... -run "TestJitter" -v
```

击穿测试：**100 个 goroutine 并发打同一 code，loader 只被调用 1 次** —— singleflight 工作。

---

## 七、5 分钟讲透自检

| 问题 | 能讲透 | 关键回答 |
|---|---|---|
| 穿透是什么？怎么挡？ | ✅ | 不存在的 key 反复查 → 空值缓存 |
| 空值 TTL 为什么短？ | ✅ | 攻击者扫一波就停 + 让"后来真创建"快速生效 |
| 击穿和穿透有什么不一样？ | ✅ | 击穿是真 key 过期瞬间被并发；穿透是 key 根本不存在 |
| singleflight 怎么实现合并？ | ✅ | 同 key 第一个进入 sf 函数，其他等结果共享 |
| 雪崩 singleflight 救不救？ | ✅ | 救不了，雪崩是不同 key 同时 miss |
| TTL 抖动 10% 怎么来的？ | ✅ | 摊薄到 1/N 秒级（10min × 10% = 1min 窗口）+ 命中率影响小 |
| 三个坑都用最强方案不行吗？ | ✅ | 布隆过滤器内存大、分布式锁热点、抖动 50% 命中率掉——按场景选 |
