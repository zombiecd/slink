# Go 项目布局：cmd / internal / pkg

> 5 分钟讲透：Go 项目目录约定从哪来、为什么 `internal` 是关键、slink 的取舍。

## 一、问题背景

新建一个 Go 项目，第一个问题是：**代码放哪？**

新手常见做法是 flat 布局：

```
myproject/
├── main.go
├── handler.go
├── db.go
├── cache.go
└── go.mod
```

flat 布局在 < 500 行时可行。一旦项目变大就出现：

- main 文件越长越乱
- `handler.go` 里既有 HTTP handler 又有业务逻辑
- 想拆包但不知道按什么维度拆
- 别人 import 你的项目时拿到一堆**本不该被外部用**的内部细节

Go 社区演化出一套约定来解决这些问题。

## 二、三个核心目录约定

### `cmd/` — 程序入口

> 编译产物放这里。每个子目录 = 一个可执行 binary。

```
cmd/
├── server/
│   └── main.go        ← 编出 ./bin/server
└── migrator/
    └── main.go        ← 编出 ./bin/migrator
```

为什么需要：

- 一个项目可能有多个 binary（主服务 + CLI 工具 + 迁移工具）
- main 包**只做组装**：读 config、连接依赖、启动 server。**不写业务逻辑**
- 业务逻辑全在 `internal/`

**slink 的做法**：

```go
// cmd/server/main.go 只做这些事：
// 1. 读 config（从 internal/config）
// 2. 建 DB / Redis 客户端（从 internal/store, internal/cache）
// 3. 装配 service（从 internal/api, internal/id, internal/event）
// 4. 启动 HTTP server，监听信号优雅停机
```

### `internal/` — 私有代码（**Go 编译器强制**）

> Go 工具链特殊处理的目录：放进去的包**只能被这个项目自己 import**，外部任何项目 import 都会编译失败。

```
my.com/proj/internal/foo  ← 只能被 my.com/proj/* 自己 import
my.com/other/main.go      ← import "my.com/proj/internal/foo" → 编译错误
```

这是 Go 1.4 开始官方支持的**编译期保证**，不是约定。

为什么这是项目工程化的关键：

1. **明确暴露面**：根目录的包是公开 API，`internal/` 里全是实现细节
2. **重构自由**：`internal/` 里的包可以随便改名/拆/合，不会破坏外部用户
3. **防止误用**：别人不能因为方便就 import 你的内部状态机

**slink 的 internal 拆分**（按职责）：

| 包 | 职责 | 依赖谁 |
|---|---|---|
| `internal/config` | 读环境变量、配置结构体 | 无 |
| `internal/model` | 领域类型（Link, ClickEvent） | 无 |
| `internal/store` | PG 数据访问（pgx） | model |
| `internal/cache` | Redis 客户端封装 | model |
| `internal/id` | 号段发号器 + Base62 | store（写号段） |
| `internal/event` | 异步事件 buffer | store |
| `internal/api` | HTTP handler | id, store, cache, event |

依赖方向只能**向下**：api → id/store/cache/event → store → model。**反向 import 编译器会报循环依赖**——这是 Go 强制的工程纪律。

### `pkg/` — 公开复用包（**slink 不用**）

> 给外部项目复用的代码放这里。

```
pkg/
└── retry/
    └── retry.go     ← my.com/proj/pkg/retry 可被任何项目 import
```

**社区争论**：

| 观点 | 主张 |
|---|---|
| 支持 `pkg/` | 区分公开/私有清晰；与 `cmd/internal/` 三件套对称 |
| 反对 `pkg/`（Go core team 多人持此观点） | 根目录就是公开包，`pkg/` 是冗余的一层 |

Rob Pike、Russ Cox 等 Go 设计者**反对** `pkg/`。但流行项目（Kubernetes、Prometheus）都用了。

**slink 立场**：v0.1 不用 `pkg/`。理由：

- 没有打算被外部项目 import 复用
- 一切都是 `internal/`，未来要开放某个能力时再讨论是否搬到 `pkg/` 或独立模块

## 三、还可能见到的目录

| 目录 | 用途 | slink 用吗 |
|---|---|---|
| `api/` | OpenAPI / proto 文件 | v0.2 加 OpenAPI 后用 |
| `web/` 或 `ui/` | 前端代码 | v0.3 加 Web UI 时用 |
| `scripts/` | 运维脚本 | ✅ 已建（含 wrk 压测脚本） |
| `deploy/` | K8s yaml / Helm chart / 容器配置 mount 源 | ✅ 已建（含 observability/） |
| `configs/` | 应用自身配置模板 | ❌ 不用——slink 走 12-factor `.env` |
| `docs/` | 文档（你在这） | ✅ |
| `test/` | 集成测试 / e2e | v0.2 加 |
| `examples/` | 示例代码 | 暂不需要 |
| `tools/` | 项目内部工具 | 暂不需要 |
| `vendor/` | 锁定依赖 | ❌ 用 go.sum 替代 |

## 四、流派之争

GitHub `golang-standards/project-layout` 仓库（4 万+ star）给了一套"标准"，但 Go 官方没有采纳。

**Go 官方立场**（[Russ Cox 2024 talk](https://research.swtch.com/vgo-tour)）：
> The standard project layout is **not** an official standard. Use what makes sense for your project.

**实践经验法则**：

1. 项目小（< 5k 行）→ flat 布局或最小 `cmd/internal`
2. 项目中（5k-50k 行）→ `cmd/server + internal/`
3. 项目大（50k+ 行 / 多 binary / 公开复用）→ 完整 `cmd/internal/pkg/api/scripts/...`

slink 现在 < 1k 行，但**直接用 `cmd/internal/` 中等布局**。理由：

- 现在不立规矩，后面拆很痛
- `internal/` 边界给的"私有保护"对学习项目特别有用——逼你区分"对外接口"和"实现细节"

## 五、slink 的最终目录

```
slink/
├── cmd/
│   └── server/main.go         ← 唯一 binary 入口
├── internal/
│   ├── api/                   ← HTTP handler
│   ├── cache/                 ← Redis
│   ├── config/                ← 配置
│   ├── event/                 ← 异步事件
│   ├── id/                    ← 号段 + Base62
│   ├── model/                 ← 领域类型
│   └── store/                 ← PG
├── migrations/                ← SQL 迁移
├── deploy/                    ← 部署资产
│   └── observability/         ← prom/grafana 容器 mount 源
├── scripts/                   ← 工具脚本（压测脚本等）
├── docs/                      ← 文档（你在这）
├── docker-compose.yml         ← 本地依赖
├── Makefile                   ← 工程命令
├── go.mod
└── README.md
```

## 六、5 分钟自检

合上文档，回答：

1. `cmd/` 和 `internal/` 各起什么作用？
2. 把一个包放进 `internal/` 之后，外部项目 import 它会发生什么？
3. 为什么 Go 官方不推 `golang-standards/project-layout`？
4. slink 为什么连 `pkg/` 都不要？

讲不出来 → 回去再读 §2 §3。

## 七、延伸阅读

- [Russ Cox: Go Modules and golang-standards/project-layout](https://github.com/golang-standards/project-layout/issues/117)（官方反对意见）
- [Go internal packages](https://go.dev/doc/go1.4#internalpackages)（编译器强制规则）
- [Style guideline for Go packages — Dave Cheney](https://dave.cheney.net/2016/04/24/gophers-please-tag-your-releases)
