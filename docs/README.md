# slink 文档

> 项目的"为什么"住在这里。代码回答 *what / how*，文档回答 ***why***。

## 给不同读者的入口

### 🧭 我想总览这个项目
1. 先看仓库根目录 [README.md](../README.md)
2. 再看 [architecture/v0.1.md](architecture/v0.1.md) — 当前版本的整体架构
3. 想读决策史就翻 [adr/](adr/)

### 🔬 我想深挖某个知识点
直接进 [concepts/](concepts/)。每篇按"5 分钟讲透"标尺写：
- [go-project-layout.md](concepts/go-project-layout.md) — Go 项目目录约定
- [docker-compose-engineering.md](concepts/docker-compose-engineering.md) — Docker Compose 工程实践
- [postgres-partitioning.md](concepts/postgres-partitioning.md) — PG 分区表
- [id-segment-schema.md](concepts/id-segment-schema.md) — 号段表设计

### 📅 我想看作者每天踩了什么坑
[journal/](journal/) 按天记录：
- [day-01.md](journal/day-01.md) — 项目骨架与基础设施

### ⚖️ 我想审计架构决策
[adr/](adr/) — 架构决策记录（Architecture Decision Records）。每个决策一份永久档案，即便后来被推翻也保留。
- [0001-postgres-as-primary-store.md](adr/0001-postgres-as-primary-store.md)
- [0002-id-segment-not-snowflake.md](adr/0002-id-segment-not-snowflake.md)

---

## 目录约定

```
docs/
├── README.md         ← 你在这
├── journal/          ← 实战日志（时间序）
├── architecture/     ← 架构总览（每个版本一篇）
├── concepts/         ← 知识点深度讲解（主题序，可被任何文档反向引用）
└── adr/              ← 架构决策记录（编号永久不变）
```

## 写作准则

每篇文档遵循三条硬规则：

1. **5 分钟讲透标尺**：合上文档后，能不看资料用 5 分钟向同事讲清楚核心，否则不算合格
2. **Why 优先于 What**：代码已经回答 *what*，文档要回答 *为什么这么选 / 不选另一种 / 边界在哪*
3. **链回代码**：所有概念必须给 slink 仓库里具体的文件路径作为锚点（如 `migrations/0001_init.up.sql:38-49`）

## ADR 编号

ADR 编号一旦分配**永不复用**。即便决策被新 ADR 推翻，旧 ADR 仍保留并标记 `Status: Superseded by ADR-NNNN`。

ADR 模板：

```markdown
# ADR-NNNN: 决策标题

- **Status**: Accepted | Deprecated | Superseded by ADR-NNNN
- **Date**: YYYY-MM-DD
- **Deciders**: @user
- **Context**: 当时面临什么决策
- **Options Considered**: 候选方案
- **Decision**: 最终选了什么
- **Consequences**: 这个决定带来的好处和代价
```
