# PostgreSQL 分区表深度

> 5 分钟讲透：为什么海量事件表必须分区、声明式 vs 继承、RANGE/LIST/HASH 三种、分区剪裁原理、归档策略。
> 对应文件：[`migrations/0001_init.up.sql`](../../migrations/0001_init.up.sql) 第 47-66 行

## 一、为什么分区

设想 click_events 表跑半年的数据：

```
单表 50 亿行 (300B/行 ~ 1.5TB)：
  - 索引膨胀：B+ 树深度从 4 层涨到 6 层，单次查询 IOPS 翻倍
  - VACUUM 一次跑 6 小时
  - 备份 pg_dump 跑不完
  - 删除老数据：DELETE 10 亿行 = WAL 爆炸 + 锁竞争 = 灾难
  - ANALYZE 慢，统计信息不准 → 查询计划差
```

**根本问题**：单表越大，所有操作都是 *O(N)* 或更糟。

分区把一张大表**物理上**拆成多张小表（按某列），PG 自动路由查询，对应用透明。带来：

```
分区后（按月，每月一张子表）：
  - 单子表只有 50 亿/6 = 8 亿行
  - 索引只在子表上，深度浅
  - 查询"最近 7 天"只扫 1-2 个分区
  - 删除老数据：DROP TABLE click_events_2025_11 → 瞬间，不写 WAL
  - 备份/VACUUM/ANALYZE 都按子表并行
```

**核心收益**：把"删除老数据"从 *O(N)* 降到 *O(1)*。

## 二、PG 分区两种形态

### 2.1 继承式分区（旧，PG 9 之前唯一选择）

```sql
-- 父表
CREATE TABLE click_events (...);

-- 子表继承
CREATE TABLE click_events_2026_05 (
    CHECK (ts >= '2026-05-01' AND ts < '2026-06-01')
) INHERITS (click_events);

-- 应用层用触发器路由
CREATE TRIGGER ... ON INSERT ... CASE WHEN ts >= '2026-05-01' ... ;
```

痛点：

- 路由触发器要自己写
- 主键、唯一约束**不能跨分区**
- ON CONFLICT 不工作
- 代码复杂、bug 多

### 2.2 声明式分区（PG 10+，业界标准）

```sql
CREATE TABLE click_events (
    event_id UUID,
    code     TEXT,
    ts       TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_id, ts)
) PARTITION BY RANGE (ts);                    -- 关键

CREATE TABLE click_events_2026_05 PARTITION OF click_events
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
```

优势：

- PG **内核级**支持，自动路由 INSERT
- PG 11+ 主键可跨分区（约束包含分区键）
- ON CONFLICT 工作
- PG 12+ 支持外键引用分区表

**slink 用声明式分区**。继承式只在维护老系统时遇到。

## 三、三种分区方法（RANGE / LIST / HASH）

```sql
PARTITION BY RANGE (col)   -- 按范围
PARTITION BY LIST  (col)   -- 按枚举值
PARTITION BY HASH  (col)   -- 按 hash 取模
```

### RANGE：按范围（最常见）

```sql
CREATE TABLE click_events_2026_05 PARTITION OF click_events
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');  -- 左闭右开
```

适合：**时间序列数据**、按 ID 范围分。

slink 选这个：按月 RANGE 分区 click_events。

### LIST：按枚举值

```sql
CREATE TABLE orders_us PARTITION OF orders FOR VALUES IN ('US', 'CA');
CREATE TABLE orders_cn PARTITION OF orders FOR VALUES IN ('CN', 'HK', 'TW');
```

适合：枚举值少且固定的列（国家、租户 ID、业务线）。

### HASH：按 hash 取模

```sql
CREATE TABLE users PARTITION OF user_main
    FOR VALUES WITH (modulus 16, remainder 0);
-- ... 0..15 共 16 个分区
```

适合：**均匀分布**、无明显时间/枚举特征的数据（user_id 分散）。

**短链用什么**：

- `links` 表：暂不分区（可控大小）；如果未来过亿，按 `id` HASH 分。
- `click_events` 表：按 `ts` RANGE 分。**绝对不能 HASH**——HASH 后无法 DROP 老数据。

## 四、分区剪裁（Partition Pruning）

**性能的关键**：PG 怎么知道一次查询只需要扫一个分区？

```sql
-- 假设有 6 个月份分区
EXPLAIN SELECT * FROM click_events 
WHERE ts >= '2026-05-15' AND ts < '2026-05-20';
```

PG 看到 WHERE 条件包含分区键 `ts`，**编译期**剪裁掉所有不相关分区，最终执行计划只扫 `click_events_2026_05`。

```
Append
  -> Seq Scan on click_events_2026_05
       Filter: ts >= '2026-05-15' AND ts < '2026-05-20'
```

**剪裁失效的常见情况**（必须避免）：

```sql
-- ❌ 函数包裹分区键
SELECT * FROM click_events WHERE date_trunc('day', ts) = '2026-05-15';
-- PG 不能确定 date_trunc(ts) 的范围 → 扫所有分区

-- ✅ 改成范围条件
SELECT * FROM click_events WHERE ts >= '2026-05-15' AND ts < '2026-05-16';
```

**slink 应用层规约**：所有事件查询必须带 `ts` 范围谓词，且不能用函数包裹。

## 五、约束与索引

### 5.1 主键

PG 11+ 要求**主键必须包含所有分区键列**：

```sql
-- ❌ 这样建会报错
PRIMARY KEY (event_id)   -- 只有 event_id，不含 ts

-- ✅ slink 这样建
PRIMARY KEY (event_id, ts)
```

含义：**event_id 在某个时间点唯一**，跨时间可能重复。短链事件可接受这个语义（事件本来就带时间）。

### 5.2 索引

声明在父表上的索引，会**自动**在所有子分区创建：

```sql
CREATE INDEX idx_click_events_code_ts 
    ON click_events (code, ts DESC);

-- 自动等价于：
-- CREATE INDEX ... ON click_events_2026_05 (code, ts DESC);
-- CREATE INDEX ... ON click_events_2026_06 (code, ts DESC);
```

**新建分区会自动继承父表索引**——不需要每次建子分区时手动加索引。

### 5.3 唯一索引

唯一索引也必须包含分区键，理由同主键。**实际短链场景中，跨分区唯一性几乎没用**——按时间分区下 event_id 自带时间属性。

## 六、自动建分区（生产必备）

```sql
-- 当前我们手动预建 2026-05 / 2026-06
-- 但 2026-07 来了之前必须建好，否则插入报错：
-- ERROR: no partition of relation "click_events" found for row
```

**生产方案**：

### 方案 1：pg_partman 扩展

业界主流。配置好"按月分区，提前建 3 个月，保留 12 个月"，定时任务自动管。

```sql
SELECT partman.create_parent(
    p_parent_table => 'public.click_events',
    p_control      => 'ts',
    p_type         => 'native',
    p_interval     => '1 month',
    p_premake      => 3
);
```

### 方案 2：自己写定时脚本

```sql
-- 每月 1 号跑：
DO $$
DECLARE
    next_month date := date_trunc('month', now() + interval '2 month')::date;
    next_after date := next_month + interval '1 month';
    pname      text := 'click_events_' || to_char(next_month, 'YYYY_MM');
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF click_events 
         FOR VALUES FROM (%L) TO (%L)',
        pname, next_month, next_after);
END $$;
```

放进 cron / Kubernetes CronJob / pg_cron 扩展。

**slink 路线**：v0.1 暂手动建。Day 7 或 v0.2 加 pg_partman。

## 七、归档与冷数据

按月分区的最大价值是**归档便宜**：

```sql
-- 90 天前的数据归档：
-- 方案 A：直接删
DROP TABLE click_events_2026_02;        -- 瞬间完成

-- 方案 B：先 detach 再操作
ALTER TABLE click_events DETACH PARTITION click_events_2026_02;
-- 这张表脱离父表但数据还在
COPY click_events_2026_02 TO '/archive/click_events_2026_02.csv';
DROP TABLE click_events_2026_02;

-- 方案 C：迁到冷盘 / 对象存储
-- 配合 pg_dump + tablespace
```

**slink 归档策略（v0.3 计划）**：

- 0-7 天：当前分区，热盘 + 完整索引
- 8-90 天：保留分区，可考虑去掉部分索引节省空间
- 90 天+：DETACH 后导出到 ES / S3，DROP 原表

## 八、踩坑清单

| 坑 | 症状 | 解法 |
|---|---|---|
| 没建对应分区 | INSERT 报 "no partition for row" | 提前建 / pg_partman / DEFAULT 分区兜底 |
| WHERE 不含分区键 | 扫所有分区 | 应用层约束查询必须带分区键 |
| 函数包裹分区键 | 剪裁失效 | 写成范围条件 |
| 主键不含分区键 | DDL 报错 | 主键加上分区键列 |
| 跨分区唯一性 | 不能保证 | 业务上接受这个限制，或用应用层 ID 生成保证 |
| 默认分区被滥用 | INSERT 不报错但数据进 default | 监控 default 分区行数 |

## 九、5 分钟自检

合上文档：

1. 为什么不能用 HASH 分区 click_events？
2. PG 怎么决定一次查询要扫哪些分区？什么情况下剪裁失效？
3. 90 天前的数据要归档，DROP PARTITION 和 DELETE 的代价差多少？
4. 分区表的主键为什么必须包含分区键？

## 十、延伸阅读

- [PostgreSQL 16 — Table Partitioning](https://www.postgresql.org/docs/16/ddl-partitioning.html)
- [pg_partman](https://github.com/pgpartman/pg_partman)
- [pg_cron](https://github.com/citusdata/pg_cron)
- [Citus Architecture: scaling Postgres horizontally](https://www.citusdata.com/blog/2017/01/27/why-postgres-partitioning-is-a-better-fit-for-postgres/)（分区 vs 分片对比）
