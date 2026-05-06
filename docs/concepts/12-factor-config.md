# 12-Factor Config

> 5 分钟讲透：为什么配置必须从环境变量读、不能写在代码或配置文件里、库选型对比。
> 对应文件：[`internal/config/config.go`](../../internal/config/config.go)

## 一、12-Factor 第三条：Config

> *"Strict separation of config from code."*  
> — [12factor.net §3](https://12factor.net/config)

定义：**任何在不同部署（dev / staging / prod）之间会变的值都是 config**。

| 是 config | 不是 config |
|---|---|
| DB 连接字符串 | Spring Bean 配置 |
| Redis 地址 | 路由表 |
| API 密钥 | 业务规则常量 |
| 第三方服务 endpoint | 协议定义 |
| 限流阈值 | 数据结构 |
| feature flag 默认值 | 算法实现 |

**判断标准**：换个环境（笔记本 → CI → 生产）时这个值要不要变？要变 → config。

## 二、为什么环境变量胜过配置文件

| 方案 | 优 | 劣 |
|---|---|---|
| 配置文件（YAML/TOML/JSON）| 表达力强 | 易被 git commit 进库（**密钥泄露**）；改一行要重新部署；多环境 N 套文件 |
| 环境变量 | OS 内核级支持，跨语言；Docker / K8s 原生友好；改值不重新部署 | 表达力弱（嵌套结构难） |
| 启动参数 (`-flag`) | 简单 | 不适合大量配置；密钥进 ps 输出 |
| 远程配置中心（Apollo / Nacos）| 实时下发 | 强依赖外部服务；冷启动慢 |

**12-factor 推环境变量**：跨平台、防泄密、零依赖。

## 三、密钥泄露的真实事故

```
团队 A 把生产 DB 密码写进 config.prod.yaml，commit 进 GitHub 私有仓库
半年后某员工把项目 fork 到自己 GitHub 公开仓库做"个人作品"
攻击者扫到密码 → 拖库 → 上新闻
```

**环境变量天然不进 git**——除非你明知故犯把 export 写进 `.bashrc` 提交。这是工程纪律，不是技术能力。

## 四、slink 的实践

```bash
# .env.example 提交进 git（无密钥，是模板）
SLINK_PG_DSN=postgres://slink:slink@localhost:15432/slink?sslmode=disable

# .env 不提交（在 .gitignore 里），开发者自行 cp .env.example .env
# 生产环境：通过 K8s Secret / Docker env / CI/CD 注入
```

加载策略（[`config.Load()`](../../internal/config/config.go)）：

```
优先级：环境变量 > .env 文件 > struct tag 默认值

dev    : .env 文件 → cfg.PGDSN
prod   : K8s Secret 注入到环境变量 → cfg.PGDSN
test   : t.Setenv → cfg.PGDSN
```

## 五、库选型：caarlos0/env vs envconfig vs viper

候选三个主流 Go config 库：

### A. [kelseyhightower/envconfig](https://github.com/kelseyhightower/envconfig)（老牌）

```go
type Config struct {
    Port int `envconfig:"PORT" default:"8080"`
}
err := envconfig.Process("MYAPP", &cfg)
```

✅ 老牌（2014），稳定，无依赖
❌ 默认值用单独 tag，啰嗦
❌ 不支持 duration 直接解析（要自定义类型）
❌ 维护节奏慢（最近 commit 多年）

### B. [caarlos0/env](https://github.com/caarlos0/env)（slink 选）

```go
type Config struct {
    Port    int           `env:"PORT"     envDefault:"8080"`
    Timeout time.Duration `env:"TIMEOUT"  envDefault:"5s"`
    DBDsn   string        `env:"DB_DSN,required"`
}
err := env.Parse(&cfg)
```

✅ tag 简洁，default 跟字段在一起
✅ 原生支持 `time.Duration`、URL、slice、map
✅ `required` tag 直接声明
✅ 维护活跃，v11 是当前主线
✅ 几乎零外部依赖

❌ 不支持 YAML / TOML 等多源（不是缺点——专注做一件事）

### C. [spf13/viper](https://github.com/spf13/viper)（重）

```go
viper.SetConfigName("config")
viper.AddConfigPath(".")
viper.AutomaticEnv()
viper.ReadInConfig()
port := viper.GetInt("port")
```

✅ 支持 YAML / TOML / JSON / env / etcd / consul 多源
✅ 实时监听文件变化、热更新
❌ **重**：~30 个间接依赖
❌ 多源优先级配置复杂、容易出 bug
❌ 不强类型，全是 `viper.GetXxx`

### slink 决策

**选 caarlos0/env**。理由：

1. slink 只需要"环境变量 + .env 默认值"，viper 杀鸡用牛刀
2. `time.Duration` 原生支持是关键（缓存 TTL、超时大量用）
3. 强类型 struct 而非 `map[string]any`，编译期就能发现拼写错误
4. 依赖少 = 攻击面小 = 升级少烦

详见 [ADR-0003](../adr/0003-config-library.md)。

## 六、godotenv 的角色

`caarlos0/env` 只读环境变量本身。`.env` 文件加载靠 [`joho/godotenv`](https://github.com/joho/godotenv)：

```go
// internal/config/config.go
_ = godotenv.Load()        // 静默加载 .env 到环境变量（不存在不报错）
env.Parse(&cfg)            // 然后 caarlos0/env 读环境变量
```

**为什么静默不报错**：

- dev：有 .env 文件
- prod：没有 .env，配置全靠 K8s Secret 注入

**警告**：godotenv 不会覆盖已设置的环境变量（默认行为）。如果你 `export FOO=bar` 后再 `godotenv.Load()`，FOO 还是 bar。这是正确语义——CI/生产显式注入的应优先于 .env。

## 七、安全实践

1. **`.env` 进 .gitignore**：永远不提交
2. **`.env.example` 进 git**：只放模板和占位，无真实密钥
3. **生产用 secret 管理器**：K8s Secret / AWS Secrets Manager / HashiCorp Vault
4. **密钥泄露后立即轮换**，不要"暂时用着"
5. **审计 git 历史**：`git log -p .env` 应永远空
6. **CI 检查**：用 `gitleaks` / `truffleHog` 自动扫密钥
7. **避免 log 打印 config**：slink 启动只 log 非敏感字段（max_conns / addr 而不是 PGDSN）

## 八、slink 还做了什么

### 8.1 字段级类型转换

```go
CacheTTL time.Duration `env:"SLINK_CACHE_TTL" envDefault:"24h"`
```

env 的值是字符串 `"24h"`，库自动解析为 `time.Duration`。无效格式会在 `env.Parse` 时报错——**启动失败而不是运行时崩**。

### 8.2 跨字段约束

```go
// Validate 跨字段约束
if c.PGMinConns > c.PGMaxConns {
    return fmt.Errorf("PG_MIN_CONNS (%d) > PG_MAX_CONNS (%d)", ...)
}
if c.CacheTTLJitter > c.CacheTTL {
    return fmt.Errorf("...")
}
```

env tag 只能管单字段。跨字段约束写在 `Validate()`，启动期检查。**配置错的服务一秒都不该跑**。

### 8.3 双重防御 required

```go
// caarlos0/env 的 required 只检查 unset
PGDSN string `env:"SLINK_PG_DSN,required"`

// 应用层再查空字符串（防 PGDSN= 这种"设置为空"的情况）
if c.PGDSN == "" {
    return errors.New("SLINK_PG_DSN is required")
}
```

发现自 Day 2 早晨的 bug：`t.Setenv("X","")` 被 caarlos0/env 视为"已设置"。教训：**不要假设第三方库的边界行为，单测 + 双重防御**。

## 九、踩坑清单

| 坑 | 现象 | 解法 |
|---|---|---|
| .env 提交进 git | 密钥泄露 | .gitignore 加 .env |
| 生产没设 env | 启动报 required | CI/CD 模板补全 |
| envDefault 拼成 default | tag 不生效 | caarlos0/env 用 envDefault 不是 default |
| time.Duration 不识别 | 解析失败 | 写 "24h" 不是 "24" |
| log 打印 config | 密钥进日志 | log 选择性打印非敏感字段 |
| t.Setenv("X","") 模拟 unset | 行为不一致 | 用 os.Unsetenv 或加应用层校验 |

## 十、5 分钟自检

合上文档：

1. 配置和代码分离的核心收益是什么？为什么配置文件不行？
2. caarlos0/env 比 envconfig 好在哪？
3. godotenv.Load() 不报错的设计意图？
4. required tag + 应用层空字符串校验，为什么要双重？
5. 生产环境密钥怎么传给服务？

## 十一、延伸阅读

- [The 12-Factor App §3 - Config](https://12factor.net/config)
- [caarlos0/env README](https://github.com/caarlos0/env)
- [Go: Environment Configuration in Containers and Kubernetes](https://12factor.net/)
- [gitleaks](https://github.com/gitleaks/gitleaks) — 扫 git 历史里的泄露密钥
