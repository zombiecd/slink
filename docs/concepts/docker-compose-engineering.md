# Docker Compose 工程实践

> 5 分钟讲透：为什么本地依赖要用 docker-compose、健康检查的真实价值、Redis 内存策略、PG 启动参数取舍。
> 对应文件：[`docker-compose.yml`](../../docker-compose.yml)

## 一、为什么用 docker-compose（不直接装 brew）

| 方案 | 痛点 |
|---|---|
| `brew install postgresql redis` | 版本飘、卸载残留、和系统服务打架；同事环境不一致 |
| 各装一个虚拟机 | 重 |
| **docker-compose** | 一行 `make up` 起齐、版本写死、彻底隔离、随时 `make down` 干净 |

更重要：**docker-compose.yml 是「我的本地环境是什么」的可执行规约**。新人 clone 项目 5 分钟跑起来，不用文档。

## 二、关键字段逐一拆解

### 2.1 `image: postgres:16-alpine`

**为什么 16**：当前 PG 主版本，**逻辑分区**（声明式）从 PG 10 起，但 PG 16 的分区性能、并发优化最好（[Release Notes](https://www.postgresql.org/docs/16/release-16.html)）。生产用 16，本地也用 16，避免版本差异。

**为什么 alpine 不用 debian 基础**：

| 维度 | postgres:16 (Debian-based) | postgres:16-alpine |
|---|---|---|
| 镜像大小 | ~430MB | ~240MB |
| 启动速度 | 同 | 同 |
| 兼容性 | musl libc 偶有问题 | musl libc 偶有问题 |

本地开发选 alpine 省下载时间、磁盘。生产环境如果有特殊 glibc 依赖再换 Debian 版。

### 2.2 健康检查（最容易被忽视的关键字段）

```yaml
healthcheck:
  test: ["CMD-SHELL", "pg_isready -U slink -d slink"]
  interval: 5s
  timeout: 3s
  retries: 10
```

**没有健康检查会怎样**：

容器 `Up` 状态 ≠ 服务可用。PG 启动需 5-15 秒做初始化、加载数据。如果你的应用容器在 PG 启动那一秒就开始连接，会看到：

```
dial tcp 127.0.0.1:5432: connect: connection refused
```

或更隐蔽的：

```
FATAL: the database system is starting up
```

**有了健康检查**：

- `docker compose ps` 显示 `(healthy)` 才是真正可用
- 应用容器可以用 `depends_on: condition: service_healthy` 等待依赖就绪
- CI 脚本可以 `until docker compose ps | grep healthy; do sleep 2; done`

**参数取舍**：

| 参数 | 含义 | slink 取值 | 为什么 |
|---|---|---|---|
| `interval` | 多久检查一次 | 5s | 太频繁浪费 CPU；太少新启动等太久 |
| `timeout` | 单次检查超时 | 3s | pg_isready 应秒回，3s 给足容错 |
| `retries` | 连续失败几次判定 unhealthy | 10 | 容忍偶发抖动 |
| `start_period` | 启动期宽限（不计入 retries） | 默认 | 也可显式给 30s |

**slink 用什么测**：

- PG: `pg_isready` —— 内建工具，不假设具体表存在
- Redis: `redis-cli ping` —— 期望返回 `PONG`

### 2.3 Volume 持久化

```yaml
volumes:
  - slink-pg-data:/var/lib/postgresql/data
```

不写 volume 的后果：`docker compose down -v` 把数据全清。开发流程中：

- 改 docker-compose.yml 重启 → 数据保留 ✅
- `docker compose down -v` → 数据清空（用于重置环境）

**两种写法**：

```yaml
# 命名 volume（推荐）
volumes:
  - slink-pg-data:/var/lib/postgresql/data

# bind mount（直接挂主机目录）
volumes:
  - ./data/pg:/var/lib/postgresql/data
```

| | 命名 volume | bind mount |
|---|---|---|
| 性能（macOS）| 好 | 慢（osxfs 同步开销） |
| 可见性 | 用 `docker volume` 管理 | 主机目录直接看 |
| 跨平台 | 一致 | 路径要小心 |

slink 用命名 volume——macOS 性能影响真实存在。

### 2.4 端口映射

```yaml
ports:
  - "5432:5432"
```

端口形式 `host:container`，**冒号左边是主机端口**，可以改：

```yaml
- "15432:5432"  # 主机 15432 → 容器 5432
```

适合主机已经有 PG 占用 5432 的场景。slink v0.1 默认 5432，开发简单。

### 2.5 PG 启动参数

```yaml
command:
  - "postgres"
  - "-c"
  - "max_connections=200"
  - "-c"
  - "shared_buffers=256MB"
```

**为什么显式给参数**：

- PG 默认 `max_connections=100`：本地开发跑压测会被打满
- `shared_buffers` 默认 128MB：建议系统内存 25%，给 256MB 让本地够用

**生产**：通过 `postgresql.conf` 完整调优（这里 inline 命令只为简化本地开发）。

### 2.6 Redis 启动参数

```yaml
command:
  - "redis-server"
  - "--appendonly"
  - "yes"
  - "--maxmemory"
  - "512mb"
  - "--maxmemory-policy"
  - "allkeys-lru"
```

**`--appendonly yes`**：开启 AOF（Append-Only File）持久化。

| 持久化模式 | 数据安全 | 性能 | slink 取什么 |
|---|---|---|---|
| 不持久化 | 重启全丢 | 最快 | ❌ |
| RDB（快照） | 可能丢几分钟数据 | 快 | 不够 |
| **AOF** | 最多丢 1s 数据（默认 fsync everysec） | 略慢 | ✅ |
| AOF + RDB 混合 | 最强 | 略慢 | 生产才考虑 |

**`--maxmemory 512mb` + `--maxmemory-policy allkeys-lru`**：当 Redis 内存达到 512MB 时按 LRU 淘汰任意 key。

**为什么这个组合很重要**：

短链的访问是**幂律分布**（10% key 拿 80% 流量）。开 LRU 后：

- 内存有限 → 自动只留热点
- 冷数据被淘汰 → MISS 后从 PG 回填 → 又变热数据
- **不需要手动设 TTL**，Redis 自动管理

**几种淘汰策略对比**：

| 策略 | 行为 | 何时用 |
|---|---|---|
| `noeviction` | 内存满直接拒绝写 | 关键数据不能丢 |
| `allkeys-lru` | 任何 key，LRU 淘汰 | **slink 选这个**——纯缓存 |
| `volatile-lru` | 只淘汰带 TTL 的 key | 部分数据是缓存部分是常驻 |
| `allkeys-random` | 随机淘汰 | 极少用 |
| `volatile-ttl` | 优先淘汰快过期的 | 偶尔有用 |
| `allkeys-lfu` (Redis 4+) | 按访问频次（不是时间） | 长期热点更准 |

**slink v0.1 选 `allkeys-lru` 而非 `allkeys-lfu`** 的原因：LRU 实现简单、行为可预测；LFU 在大促场景对"突然热"反应慢（要时间累积频次）。

## 三、`docker compose` v2 vs `docker-compose` v1

```bash
docker compose up    # ← v2，Docker 内置子命令（推荐）
docker-compose up    # ← v1，独立 Python 工具（已弃用）
```

slink 文档统一用 `docker compose`（无连字符）。如果你跑的是 Mac/Linux 上的 Docker Desktop ≥ 4.x，自动是 v2。

## 四、常见踩坑清单

| 现象 | 原因 | 解法 |
|---|---|---|
| `make up` 后立刻连接失败 | 容器 `Up` 但 PG 未 ready | 加 healthcheck，等 `(healthy)` |
| 改了 docker-compose.yml 不生效 | 旧容器还在跑 | `docker compose up -d --force-recreate` |
| 数据莫名消失 | 不小心 `down -v` | 数据备份、谨慎用 `-v` |
| 端口冲突 | 主机已有服务占 5432/6379 | 改主机端口映射或停占用进程 |
| 压测时 PG 报 `too many connections` | `max_connections` 默认 100 不够 | 启动参数调到 200+ |
| Redis 莫名丢数据 | 没开 AOF / 主机重启 | `--appendonly yes` |
| 容器内时间不对 | 时区问题 | `TZ=Asia/Shanghai` 环境变量 |

## 五、5 分钟自检

合上文档：

1. 没有 healthcheck 会出现什么具体故障？
2. `allkeys-lru` 和 `volatile-lru` 的区别是什么？slink 为什么选前者？
3. 命名 volume 和 bind mount 在 macOS 上为什么有性能差异？
4. AOF 默认 fsync 频率是多少？最多丢多久数据？

讲不出来 → 回去再读对应章节。

## 六、延伸阅读

- [PostgreSQL 16 release notes](https://www.postgresql.org/docs/16/release-16.html)
- [Redis Eviction Policies](https://redis.io/docs/latest/develop/reference/eviction/)
- [Redis Persistence](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)
- [Docker compose healthcheck reference](https://docs.docker.com/reference/compose-file/services/#healthcheck)
