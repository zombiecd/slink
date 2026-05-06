# ADR-0003: 配置加载选用 caarlos0/env + godotenv

- **Status**: Accepted
- **Date**: 2026-05-06
- **Deciders**: @zombiecd
- **Phase**: v0.1 Day 2

## Context（背景）

slink 服务有 ~15 个配置项（地址 / DSN / TTL / 限流阈值等），需要从环境变量读取（[12-factor §3](../concepts/12-factor-config.md)），并支持本地 .env 文件方便开发。

候选库：

1. 标准库 `os.Getenv` 自己解析
2. `kelseyhightower/envconfig`（老牌 2014）
3. `caarlos0/env/v11`（活跃 2025）
4. `spf13/viper`（多源全家桶）
5. `knadh/koanf`（viper 替代品）

## Options Considered（候选方案）

### A. 标准库 os.Getenv

```go
addr := os.Getenv("SLINK_ADDR")
if addr == "" { addr = ":18080" }

maxConns, err := strconv.ParseInt(os.Getenv("SLINK_PG_MAX_CONNS"), 10, 32)
if err != nil { return err }
```

✅ 零依赖
❌ 15 个字段每个都手写 ~5 行 → 无谓重复
❌ time.Duration / URL 解析自己写
❌ required 校验、跨字段约束散落各处
❌ 无类型安全

### B. kelseyhightower/envconfig

```go
type Config struct {
    Addr       string `envconfig:"ADDR" default:":18080"`
    MaxConns   int    `envconfig:"PG_MAX_CONNS" default:"20"`
    Timeout    time.Duration `envconfig:"TIMEOUT" default:"5s"`
}
err := envconfig.Process("SLINK", &cfg)
```

✅ 老牌（10+ 年）
✅ 默认值通过 tag 声明
✅ duration 原生支持

❌ 维护节奏慢（最近 commit 数月一次）
❌ tag 风格啰嗦（envconfig + default 分开）
❌ prefix 强制（`Process("SLINK", ...)`，灵活性弱）

### C. caarlos0/env/v11（最终选择）

```go
type Config struct {
    Addr       string        `env:"SLINK_ADDR" envDefault:":18080"`
    MaxConns   int32         `env:"SLINK_PG_MAX_CONNS" envDefault:"20"`
    Timeout    time.Duration `env:"SLINK_TIMEOUT" envDefault:"5s"`
    DBDsn      string        `env:"SLINK_PG_DSN,required"`
}
err := env.Parse(&cfg)
```

✅ 活跃维护（2025 年 v11 稳定版）
✅ `env` + `envDefault` 同 tag 内
✅ `required` 修饰符直接声明
✅ 类型支持广（duration / URL / slice / map / IP）
✅ ParseAs / ParseAsWithOptions 高级用法
✅ 零外部依赖（除 reflect）

❌ 不支持多源（YAML / 远程配置中心）—— **slink 不需要**

### D. spf13/viper

```go
viper.SetConfigName("config")
viper.AutomaticEnv()
viper.ReadInConfig()
addr := viper.GetString("addr")
```

✅ 多源（YAML / TOML / JSON / etcd / consul）
✅ 实时监听文件变化
✅ K/V 数据库支持
❌ **重**：30+ 间接依赖
❌ 类型不安全（`viper.GetXxx`）
❌ 多源优先级配置容易 bug
❌ 实时监听对 slink 没用（环境变量改了重启即可）

### E. knadh/koanf

类似 viper 但更模块化、更轻。

✅ 比 viper 轻
❌ 仍是多源框架，对单源（环境变量）杀鸡用牛刀

## Decision（最终决策）

**选 caarlos0/env/v11 + joho/godotenv**：

- caarlos0/env：解析环境变量到 struct
- godotenv：加载 .env 文件到环境变量

两者职责清晰分离。

## Consequences（后果）

### 正面

- 配置定义在一个 struct 内，**强类型 + 编译期检查**
- tag 简洁（一行声明字段 + 默认值 + required）
- 单测易写（用 `t.Setenv` 改值，无需磁盘 I/O）
- 依赖少（caarlos0/env 自身只依赖 reflect）
- 跨字段校验 / 业务校验通过显式 `Validate()` 方法处理（库不强制）

### 负面

- 锁定环境变量源——未来要支持远程配置中心需重写或上 viper
- caarlos0/env 的 `required` 只检查 unset，不检查空值
  - **缓解**：应用层 `Validate()` 做空字符串检查（[config.go:80-93](../../internal/config/config.go)）
  - 这个边界 Day 2 早晨被单测捕获，已加入双重防御

### 不会发生的反例

- ❌ 不会在生产中发现某个 config 字段拼错（编译期 struct 字段名约束）
- ❌ 不会在 server 启动后才发现必填项缺失（`env.Parse` 直接 fail）

## 影响范围

变更点：

- `internal/config/config.go` — Config struct + Load + Validate
- `internal/config/config_test.go` — 7 个测试覆盖默认值/必填/越界
- `cmd/server/main.go` — `config.Load()` 入口
- `go.mod` — 新增依赖：caarlos0/env/v11 v11.4.1, joho/godotenv v1.5.1

## Related（相关）

- [12-factor-config 概念深度](../concepts/12-factor-config.md)
- ADR-0001: 选 PG 不选 MySQL（DB 配置项的存储基础）
- ADR-0002: 号段 vs Snowflake（IDStepSize / IDBizTag 配置项的来源）

## Revisit（何时重审）

如下条件触发重新审视：

1. slink 引入 SaaS 形态需要远程配置中心
2. 多租户需要按 tenant 不同 config（这种情况要评估 koanf 或自研）
3. caarlos0/env 进入维护停滞状态
