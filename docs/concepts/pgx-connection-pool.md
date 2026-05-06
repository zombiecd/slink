# pgx 连接池深度

> 5 分钟讲透：为什么 pgx 不要走 database/sql、连接池五个关键参数、调优方法、常见踩坑。
> 对应文件：[`internal/store/pg.go`](../../internal/store/pg.go)

## 一、为什么数据库需要连接池

每次操作都新建 TCP 连接的代价（PG 单连接建立约 5-15ms）：

- TCP 三次握手
- TLS 握手（如启用）
- PG 鉴权（密码 / SCRAM-SHA-256）
- 后端 fork 进程（PG 是 process-per-connection 模型）
- 应用层 Session 初始化

**没有连接池**：1w QPS = 每秒 1w 次建连 → 全是握手开销 + PG 进程数爆炸 → 服务不可用。

**连接池**：维护 N 个常驻连接，请求来了借出一个用完归还。建连成本摊到一开始的几次。

## 二、Go 数据库的两条路线

### A. database/sql（标准库）+ 任意驱动

```go
import "database/sql"
import _ "github.com/lib/pq"  // 或 _ "github.com/jackc/pgx/v5/stdlib"

db, _ := sql.Open("postgres", dsn)
db.QueryRow("SELECT ...").Scan(&x)
```

✅ 可换驱动（lib/pq、pgx/stdlib、其他 DB 驱动）
✅ 标准库，无外部接口契约
❌ **类型适配损失**：所有值过 `driver.Value`（`int64 / string / []byte / float64 / time.Time / bool / nil`），PG 高级类型（INET、UUID、JSONB、numeric、array）需要应用层手动 marshal/unmarshal
❌ 连接池配置粗糙（只有 MaxOpen / MaxIdle / MaxLifetime / MaxIdleTime 4 个）
❌ 性能损失约 20-30%（多一层接口反射）

### B. pgx 原生（slink 选）

```go
import "github.com/jackc/pgx/v5/pgxpool"

pool, _ := pgxpool.New(ctx, dsn)
pool.QueryRow(ctx, "SELECT ...").Scan(&x)
```

✅ **类型直通**：pgtype 包原生映射 PG 全部类型
  - `INET` → `netip.Addr`
  - `UUID` → `uuid.UUID`
  - `JSONB` → 任意 struct（json.Marshal）
  - `numeric` → `decimal.Decimal`
  - 数组、范围类型、复合类型全部原生
✅ 连接池配置丰富（10+ 参数，看 §三）
✅ 性能更好（少一层 sql.Open 包装）
✅ COPY 协议、batch 协议、监听 LISTEN/NOTIFY 原生
❌ 锁定 PG（不能换 MySQL 等）

### slink 选 pgx 原生

slink 已选 PG 作为唯一存储（[ADR-0001](../adr/0001-postgres-as-primary-store.md)），pgx 是事实标准。click_events 表用 INET 字段——这个类型 database/sql 处理麻烦，pgx 原生支持，决定性的差异。

## 三、五个关键连接池参数

```go
pgxCfg.MaxConns           = 20            // [1]
pgxCfg.MinConns           = 2             // [2]
pgxCfg.MaxConnLifetime    = 1 * time.Hour // [3]
pgxCfg.MaxConnIdleTime    = 30 * time.Minute // [4]
pgxCfg.HealthCheckPeriod  = 1 * time.Minute  // [5]
```

### [1] `MaxConns` — 池容量上限

PG 默认 `max_connections = 100`，给 OS / 运维 / 备份 / 监控留出 20，每个应用实例预算 = `(100 - 20) / 实例数`。

**slink v0.1 单实例 → MaxConns = 20** 留余量。

**陷阱**：单实例不要把 PG 全打满，否则其他工具（pg_dump / 监控）连不上。

### [2] `MinConns` — 池容量下限

启动时预先创建的连接数，永不释放（除非过 lifetime）。**目的**：第一波请求不撞冷启动。

**slink 取 2** —— 启动后 `/readyz` 立即可服务两个并发探针。

### [3] `MaxConnLifetime` — 连接最大生命周期

任何连接活过这个时间被强制销毁。

**为什么需要**：
- 长连接累积内存（PG 进程的 work_mem 等）
- 路由层（PgBouncer / HAProxy）可能 idle timeout 单方面断
- DNS 变更后需要重新解析

**典型值**：30min - 2h。slink 取 **1h**。

### [4] `MaxConnIdleTime` — 空闲连接保留时长

借出归还后，超过此时长的空闲连接销毁（直至 MinConns）。

**典型值**：5-30min。slink 取 **30min**——本地开发不在乎，生产略短防止占用 PG 进程。

### [5] `HealthCheckPeriod` — 后台健康检查周期

pgx 后台 goroutine 周期性 ping 池里的空闲连接，把死的剔除。**不是替代应用层 Ping**：HealthCheck 只保证池里的连接有效，不证明业务可达。

**典型值**：30s - 5min。slink 取 **1min**。

## 四、调优方法（不是套公式）

### 4.1 看真实指标

```sql
-- PG 端：当前连接数
SELECT state, count(*) FROM pg_stat_activity GROUP BY state;

-- pgxpool 端：池统计
stat := pool.Stat()
slog.Info("pool",
    "total", stat.TotalConns(),     -- 当前池中连接数
    "acquired", stat.AcquiredConns(),-- 借出中
    "idle", stat.IdleConns(),       -- 空闲
    "max", stat.MaxConns(),
)
```

### 4.2 调优逻辑

```
症状：QueryRow 偶发 200ms+
原因可能：
  - 池满了，请求等连接（看 acquire_duration）
  - PG 单查询慢（看 pg_stat_statements）
  - 网络抖动（看 wait_count）

解法：
  - 池满 → 加 MaxConns 或减 MaxConnLifetime
  - 查询慢 → 加索引 / 优化 SQL
  - 网络 → 不是池的问题
```

### 4.3 初始值经验法则

| 服务规模 | MaxConns | MinConns |
|---|---|---|
| 单机本地 dev | 5-10 | 1-2 |
| 单实例生产小服务 | 20-30 | 2-5 |
| 生产中等（10-50 实例）| 10-20/实例 | 2-5/实例 |
| 生产大型（百+ 实例）| 上 PgBouncer 池子化 | n/a |

**注意**：实例数 × MaxConns 不能超 PG `max_connections - 预留`。

### 4.4 PgBouncer 是另一个故事

实例数过百时单 PG 连接数会爆。PgBouncer 在中间做"连接复用"——应用与 PgBouncer 维持长连接，PgBouncer 与 PG 维持少量长连接。这是 v0.3+ 范围。

## 五、slink 的 PoolConfig 抽象

```go
// internal/store/pg.go
type PoolConfig struct {
    DSN      string
    MaxConns int32
    MinConns int32
    // 可选高级参数，零值取合理默认。
    MaxConnLifetime    time.Duration
    MaxConnIdleTime    time.Duration
    HealthCheckPeriod  time.Duration
    ConnectTimeout     time.Duration
}
```

**设计原则**：

- 不直接吃 `*config.Config`：让 store 包独立于 config（避免循环引用 + 易测试）
- 默认值在 store 包内（`orDefault`）：调用方零参数即可工作
- 关键参数（DSN / MaxConns / MinConns）显式传：避免无意义默认掩盖配置错误

## 六、Ping 的两层语义

```go
pool.Ping(ctx)   // 借一个连接，发 SELECT 1，归还。验证「业务可达」
```

vs

```go
pool 后台 HealthCheckPeriod 循环   // pgx 自检池内连接活性
```

**slink `/readyz` 用 `pool.Ping(ctx)`**：

```go
func (h *readinessHandler) check(ctx context.Context) {
    if err := h.pg.Ping(ctx); err != nil { ... }
}
```

每次 readyz 请求都 Ping 一次。代价小（毫秒级），换来真实健康判断。

## 七、踩坑清单

| 坑 | 现象 | 解法 |
|---|---|---|
| 忘了 `defer pool.Close()` | 优雅停机时活跃事务丢失 | main 里 defer |
| MaxConns 设太小 | P99 高（等连接） | 监控 acquire_duration |
| MaxConns 设太大 | PG 拒绝新连接 | 总量不超 max_connections |
| 没设 ConnectTimeout | 启动失败时挂 30s | 设 5s 内合理超时 |
| 长事务持锁 | 整池阻塞 | 监控 long-running txn |
| ctx 没传 | 不能取消查询 | 所有 Query/Exec 必传 ctx |
| Scan 类型不匹配 | 运行时 panic | 用 pgtype 或测试覆盖 |

## 八、5 分钟自检

合上文档：

1. database/sql + pgx/stdlib 和 pgxpool 原生的本质区别？
2. MaxConns 设 100 会出什么事？
3. MinConns 的真实价值是什么？
4. MaxConnLifetime 解决了什么问题？
5. pool.Ping 和后台 HealthCheck 的差别？

## 九、延伸阅读

- [pgx 官方文档](https://github.com/jackc/pgx)
- [pgx vs database/sql benchmark](https://github.com/jackc/pgx/wiki/Getting-started-with-pgx#choosing-between-the-pgx-and-databasesql-interfaces)
- [PostgreSQL connection limits — Citus blog](https://www.citusdata.com/blog/2020/10/08/most-postgres-tuning/)
- [PgBouncer in production](https://www.pgbouncer.org/usage.html)
