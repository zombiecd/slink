# 号段表设计

> 5 分钟讲透：为什么号段是短链 ID 生成的最佳方案、表结构每一列的目的、双 buffer 优化、与 Snowflake/UUID 的对比。
> 对应文件：[`migrations/0001_init.up.sql`](../../migrations/0001_init.up.sql) 第 9-22 行

## 一、问题：短链需要什么样的 ID？

短链 ID 的需求很特殊：

| 需求 | 来源 |
|---|---|
| **整数**（用于 Base62 编码） | 短码越短越好；字符串 ID 编完更长 |
| **可控范围** | 6 位 Base62 = 568 亿，要在这个空间内 |
| **递增（不要求严格）** | 简化运维（按 ID 排序事件） |
| **创建 QPS 1k+ 不能阻塞** | 营销活动批量创建 |
| **重启不能丢号** | 已发出的短链必须能解码 |
| **多实例可同时分配** | 横向扩展 |

逐一对比候选：

| 方案 | 整数 | 短 | 不阻塞 | 多实例 | 短链合不合适 |
|---|---|---|---|---|---|
| `SERIAL` / `nextval` 直接查 | ✅ | ✅ | ❌ 每次查 DB | ✅ | 性能差 |
| Snowflake | ✅ | ❌ 64 位 → 11 位短码 | ✅ | ✅ | **太长** |
| UUID v4 | ❌ 字符串 | ❌ | ✅ | ✅ | **完全不行** |
| Hash(URL) 截断 | ✅ | ✅ | ✅ | ✅ | 碰撞处理麻烦 |
| **号段模式（Leaf-segment）** | ✅ | ✅ | ✅ | ✅ | **完美** |

**结论**：号段是短链场景的最优解。详细决策记录见 [ADR-0002](../adr/0002-id-segment-not-snowflake.md)。

## 二、号段模式核心思想

```
朴素发号器（每次查 DB）：
  服务 A → SELECT nextval('seq')   ← 一次网络往返
  服务 A → SELECT nextval('seq')   ← 又一次
  ...
  
号段模式：
  服务 A 启动 → UPDATE id_segment SET max_id = max_id + 1000 → 拿到 [5001, 6000]
  服务 A 内存自增分配 1000 次          ← 0 次网络往返
  6000 用完 → 再取下一段 [6001, 7000]
```

**核心**：把"取一个 ID"的频次从 *每次创建短链* 降到 *每 1000 次创建短链一次*。

## 三、slink 号段表逐列拆解

```sql
CREATE TABLE id_segment (
    biz_tag      TEXT PRIMARY KEY,
    max_id       BIGINT NOT NULL DEFAULT 0,
    step_size    BIGINT NOT NULL DEFAULT 1000,
    description  TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### `biz_tag` — 业务标签（多空间隔离）

为什么有这一列：

```
slink v0.1：只有一个 biz_tag = 'link'
slink v0.5：可能扩展支持多种短链类型
  biz_tag = 'link'           -- 普通短链 (o.cn/...)
  biz_tag = 'qr'             -- 二维码短链 (q.o.cn/...)
  biz_tag = 'campaign'       -- 营销活动短链（独立空间，事后审计）
  biz_tag = 'admin'          -- 运维内部短链
```

每个 biz_tag 有独立 ID 空间，互不干扰。这是美团 Leaf 算法的关键设计——**一张表服务多个业务**。

PRIMARY KEY 在 `biz_tag` 上：取号段时按 biz_tag 行级锁，多租户互不阻塞。

### `max_id` — 当前已分配上限

服务取号段：

```sql
UPDATE id_segment 
SET max_id = max_id + step_size,
    updated_at = now()
WHERE biz_tag = 'link'
RETURNING max_id;
-- 假设之前 max_id = 5000，UPDATE 后变 6000，返回 6000
-- 服务记下：内存号段 = [5001, 6000]
```

**单条 UPDATE 自带行锁**——多个服务实例并发拿号段不会冲突。

### `step_size` — 号段大小（关键调参）

| step_size | DB 压力 | 重启浪费 | 推荐场景 |
|---|---|---|---|
| 100 | 高（每秒 10 次取段） | 极小 | 创建 QPS 极低、不能浪费号 |
| **1000** | 中（每秒 1 次取段） | 中（重启浪费 ~500） | **slink v0.1 默认** |
| 10000 | 低 | 大（重启浪费 ~5000） | 创建 QPS 高、号段空间大 |
| 100000 | 极低 | 极大 | 离线批处理 |

**怎么估算**：

> step_size ≈ 创建 QPS × 期望取段间隔（秒）

slink v0.1 假设创建 QPS ~1k，希望每秒取段不超过 1 次 → step_size = 1000。

### `description` — 业务说明

文档化用途。生产环境运维查表能立即看出 `biz_tag` 是干嘛的。

### `updated_at` — 更新时间

监控用。运维定期检查：哪个 biz_tag 长期没动？哪个突发取段过快？

## 四、并发安全分析

### 单机多 goroutine

```go
type Segment struct {
    mu      sync.Mutex
    current uint64
    max     uint64
}

func (s *Segment) NextID() uint64 {
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.current >= s.max {
        s.refill()  // 去 DB 取下一段
    }
    s.current++
    return s.current
}
```

`sync.Mutex` 保证内存 ID 自增不冲突。

**性能上限**：mutex 抢锁 ~50ns/次 → 单实例 ~2000 万 ID/s。号段模式的瓶颈不在内存自增，而在 refill 时的 DB UPDATE。

### 多机实例

实例 A 拿到 [5001, 6000]，实例 B 拿到 [6001, 7000]——**互不重叠**，因为 UPDATE 行锁串行化了取段操作。

```
实例 A: UPDATE ... → 拿 [5001, 6000]
实例 B: UPDATE ... → 拿 [6001, 7000]   ← B 的 UPDATE 等 A 提交后才执行
实例 C: UPDATE ... → 拿 [7001, 8000]
```

**关键**：UPDATE 是原子的、自带 X 锁、提交后行版本可见。

### 跨数据中心

slink v0.1 单机房。多机房场景需要每个机房一个独立 biz_tag（如 `link_dc1`, `link_dc2`），编码时加机房标识位前缀。这是 v0.5+ 范围。

## 五、双 buffer 优化（Day 3 实现，先理解原理）

### 朴素号段的痛点

```
内存号段用到 5999 → 第 6000 次请求触发 refill → 等 DB UPDATE → 阻塞 5-50ms
```

第 6000 次请求**抖动**——突然慢一下。在 P99 敏感场景这是问题。

### 双 buffer 方案（美团 Leaf 核心优化）

```
持有两个 buffer：
  current  = [5001, 6000]   ← 正在分配
  next     = [6001, 7000]   ← 已预取，待用

current 用到 90%（5900）时：
  异步 goroutine 去 DB 取 [7001, 8000] → 填 next（旧 next 升级为 current）

current 用完时：
  无缝切到 next，用户请求不感知
```

**效果**：refill 永远在后台异步完成，前台请求 0 阻塞。

### Go 实现骨架（Day 3 会写）

```go
type DoubleBuffer struct {
    mu       sync.Mutex
    current  *segment
    next     *segment
    refilling atomic.Bool   // 防止重复触发 refill
    threshold float64        // 90% 触发预取
}

func (d *DoubleBuffer) NextID() uint64 {
    d.mu.Lock()
    defer d.mu.Unlock()
    
    id, ok := d.current.next()
    if !ok {
        // 当前段用完，切到 next
        d.current = d.next
        d.next = nil
        go d.asyncRefill()
        return d.current.mustNext()
    }
    
    // 检查是否到达阈值，触发异步预取
    if d.current.usage() > d.threshold && d.next == nil {
        if d.refilling.CompareAndSwap(false, true) {
            go d.asyncRefill()
        }
    }
    return id
}
```

## 六、踩坑清单

| 坑 | 后果 | 解法 |
|---|---|---|
| step_size 设太小 | 取段频繁，DB 压力 | 按 QPS 调大 |
| step_size 设太大 | 重启浪费多 | 平衡值，slink 选 1000 |
| 没有索引/PK | UPDATE 全表锁 | PK 在 biz_tag 上 |
| refill 失败处理 | 服务挂死 | 重试 + 降级（如紧急切短期 step_size） |
| 跨多机房号段冲突 | ID 重复 | 每机房独立 biz_tag |
| 监控缺失 | 号段耗尽不知道 | 监控 max_id 增长率 + 内存 buffer 余量 |

## 七、对比 Twitter Snowflake

| 维度 | 号段（Leaf-segment） | Snowflake |
|---|---|---|
| 中心节点 | 需要（DB 一张表） | 不需要 |
| ID 长度 | 任意（取决于 step 累计） | 固定 64 位 |
| 短码长度 | 6 位起步 | 11 位起步 |
| 时钟依赖 | 无 | 强（时钟回拨会重复） |
| 部署复杂度 | 极低 | 需要分配 worker_id |
| QPS 上限 | DB UPDATE 上限 / step_size | 单机 4096/ms × 节点数 |
| 适合场景 | **短链、订单（要短）** | 用户 ID、消息 ID（不在意长度） |

短链选号段，**不是因为 Snowflake 不好，是因为短链对短码长度极度敏感**。

## 八、5 分钟自检

合上文档：

1. step_size 怎么取？取 1000 和取 10 万分别有什么后果？
2. 多个服务实例并发取号段，怎么保证不重复？
3. 为什么 PRIMARY KEY 在 `biz_tag` 上而不是 `id`？
4. 双 buffer 解决了朴素号段的什么问题？

## 九、延伸阅读

- [美团 Leaf 分布式 ID 生成系统](https://tech.meituan.com/2017/04/21/mt-leaf.html)（号段模式发源地）
- [Twitter Snowflake](https://github.com/twitter-archive/snowflake)
- [Why UUIDs are bad for performance in MySQL](https://www.percona.com/blog/2014/12/19/store-uuid-optimized-way/)（同样原理适用于 PG）
- [ADR-0002: 选号段不选 Snowflake](../adr/0002-id-segment-not-snowflake.md)
