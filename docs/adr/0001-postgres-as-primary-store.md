# ADR-0001: 使用 PostgreSQL 作为主存储

- **Status**: Accepted
- **Date**: 2026-05-06
- **Deciders**: @zombiecd
- **Phase**: v0.1 立项

## Context（背景）

slink 需要一个主存储，承担：

1. 持久化 `links` 表（短码 ↔ 长 URL 映射）
2. 持久化号段（`id_segment` 表）
3. 持久化点击事件（`click_events`，未来海量）
4. 提供事务保证（创建短链时的幂等键约束）
5. 提供分区能力（事件表按月分区归档）
6. 提供索引能力（短码唯一索引、事件表 code+ts 复合索引）

候选：PostgreSQL 16、MySQL 8.0、CockroachDB、TiDB、SQLite。

## Options Considered（候选方案）

### 选项 A：PostgreSQL 16（最终选择）

✅ 原生声明式分区（v10+），声明清晰、自动路由、自动索引继承
✅ 部分索引（`WHERE expires_at IS NOT NULL`），slink 用得到
✅ 高级类型：`UUID`、`INET`、`JSONB`、`TIMESTAMPTZ` 原生支持
✅ CTE / LATERAL / FILTER 等强 SQL 表达力（运营查询用得上）
✅ pgx 是 Go 生态最成熟的 PG 驱动，性能优秀
✅ 社区活跃（pg_partman、pg_cron、TimescaleDB 等扩展）
✅ 团队/作者熟悉（已有 28 万字 PG 学习笔记）

❌ 写性能略低于 MySQL（但 slink 写场景是 batch insert，影响小）
❌ 内存占用高于 MySQL

### 选项 B：MySQL 8.0

✅ 写性能强、社区大
✅ 团队多数后端工程师更熟

❌ 分区支持弱：MySQL 分区是表上的属性而非独立子表，操作笨
❌ 不支持部分索引、不支持 INET 类型
❌ JSON 类型不如 JSONB 灵活
❌ Window 函数 / CTE / LATERAL 表达力不如 PG

### 选项 C：CockroachDB / TiDB（NewSQL）

✅ 水平扩展、分布式事务
✅ 兼容 PG/MySQL 协议

❌ 单 region 部署相对单机 PG 慢 2-5 倍（共识协议代价）
❌ 运维复杂度高
❌ slink v0.1-v0.3 没有水平扩展需求
❌ 杀鸡用牛刀

### 选项 D：SQLite

✅ 零运维、单文件、嵌入式
✅ 适合 demo 项目

❌ 不支持声明式分区
❌ 不支持高并发写
❌ 不能多实例共享
❌ 不符合"扛大促"目标

## Decision（最终决策）

**选 PostgreSQL 16 alpine**。

理由综合：

1. **分区支持是硬需求**：click_events 表必须按月分区归档，PG 声明式分区是最佳实现
2. **写场景经过 batch 优化后 PG 足够快**（v0.1 单机目标 1w 事件/s，PG batch insert 轻松到 10w+）
3. **数据类型适配短链场景**（INET 存 IP、UUID 存 event_id、JSONB 存扩展元数据）
4. **作者已有 PG 深度积累**（学习成本接近 0）
5. **未来横向扩展路径清晰**：PG 单机不够时上 Citus 或迁 TiDB 都不破坏 schema

## Consequences（后果）

### 正面

- 分区表能力开箱即用
- 事件表归档（DROP PARTITION）瞬间完成
- 应用层代码简洁（pgx + 标准 SQL）
- 文档/扩展生态丰富

### 负面

- 单机 PG 是横向扩展瓶颈（v0.3 解决：读写分离 / 主从 / 分片）
- PG 运维知识门槛比 MySQL 略高（VACUUM、autovacuum、bloat）
- Redis 挂掉时全部回压到 PG，必须做好限流与熔断（v0.2 解决）

### 监控点

后续运维需要关注：

- 连接池利用率（默认 max_connections=200，预留 20% 给运维）
- 表膨胀（`pg_stat_user_tables.n_dead_tup`）
- 分区数（防止分区无限增长）
- 慢查询（`log_min_duration_statement`）
- WAL 写入速率（评估归档/复制带宽）

## Related（相关）

- [PG 分区表深度](../concepts/postgres-partitioning.md)
- [号段表设计](../concepts/id-segment-schema.md)
- [docker-compose PG 配置说明](../concepts/docker-compose-engineering.md)

## Revisit（何时重审）

如下条件触发本 ADR 的重新审视：

1. v0.3+ 单实例 PG 写入 > 5w/s，需要考虑分片
2. 跨数据中心部署需求出现
3. 业务方要求 99.99% 可用性（PG 单点不够）
