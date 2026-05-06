# 幂等键（Idempotency-Key）

> 5 分钟讲透：幂等性是什么、Stripe 的实践、DB unique 约束如何保证幂等、并发竞态怎么处理。
> 对应文件：[`internal/api/links.go`](../../internal/api/links.go)

## 一、幂等是什么（一句话）

> **同一个操作执行 N 次的效果与执行 1 次相同。**

形式化：

```
f(x)         = y
f(f(x))      = y
f(f(f(x)))   = y
...
```

**HTTP 方法的天然幂等性**：

| 方法 | 默认幂等？ | 原因 |
|---|---|---|
| GET / HEAD | ✅ 应该是 | 只读 |
| PUT | ✅ | "把资源设为这个状态" 重复设置无差 |
| DELETE | ✅ | 删除一个不存在的资源应返回 204/404，效果稳定 |
| **POST** | ❌ | "创建新资源" 重复就创建多个 |

短链创建是 POST → **天然非幂等**。

## 二、为什么短链需要幂等

**真实场景**：

```
用户 A 在网络抖动时点 "创建短链" 按钮
  → client 发请求 → server 收到 + 处理 + 写库
  → 但 response 被网络丢了
  → client 没收到回复 → 重试
  → server 又收到一次 → 又创建一个新短链 ❌

结果：同一个长 URL 被创建出 2 个不同短码
- 客户端不知道用哪个
- 营销活动统计被污染
- 数据库脏
```

**解法**：让 POST 也变得幂等。

## 三、Stripe 模式（业界标准）

Stripe 在 2015 年推出了 [`Idempotency-Key` HTTP header](https://stripe.com/docs/api/idempotent_requests)，业界沿用。

```http
POST /api/links HTTP/1.1
Content-Type: application/json
Idempotency-Key: 8f3e9d2a-7b4c-4e5f-9a1b-2c3d4e5f6a7b

{"long_url":"https://example.com"}
```

服务端逻辑：

```
1. 客户端生成 UUID（每次"逻辑请求"用同一个，重试时复用）
2. 服务端收到请求：
   a. 查 DB：这个 key 已经处理过吗？
      - 是 → 直接返回上次的结果（不再创建新资源）
      - 否 → 继续 3
   b. 创建资源 + 写入 DB（含 idempotency_key 字段）
   c. 返回结果
3. 网络抖动重试：
   - 同 key 重试 → 步骤 2a 命中 → 返回上次结果
```

**关键约束**：服务端必须**持久化** key → result 的映射。Stripe 默认存 24 小时。

## 四、slink 的实现

### 4.1 数据库约束

```sql
-- migrations/0001_init.up.sql
CREATE TABLE links (
    ...
    idem_key TEXT,
    CONSTRAINT links_idem_unique UNIQUE (idem_key)
);
```

**为什么用 unique 约束而不是应用层"先查再写"**：

```
应用层 check-then-insert 模式：
  client A: SELECT WHERE idem_key='X' → 不存在
  client B: SELECT WHERE idem_key='X' → 不存在  ← 并发
  client A: INSERT
  client B: INSERT  ← 居然成功了，违反幂等

DB unique 约束模式：
  client A: SELECT → 不存在
  client B: SELECT → 不存在
  client A: INSERT  ← 成功
  client B: INSERT  ← unique 冲突，拒绝
  client B: 捕获冲突 → SELECT 一次 → 返回 A 创建的结果
```

**应用层的并发控制不可信。DB 的 unique 约束才是最后一道防线。**

### 4.2 主路径

```go
// internal/api/links.go
idemKey := r.Header.Get("Idempotency-Key")

// 1. 早查：key 已处理过 → 直接返回（不浪费号段）
if idemKey != "" {
    existing, err := s.links.GetByIdempotencyKey(ctx, idemKey)
    if err == nil {
        writeJSON(w, 200, toResponse(existing))
        return
    }
    // 未命中（ErrLinkNotFound）→ 继续创建
}

// 2. 生成 + 写库
code := generator.NextCode(ctx)
err := s.links.Insert(ctx, &model.Link{..., IdempotencyKey: &idemKey})
if errors.Is(err, store.ErrIdempotencyConflict) {
    // 3. race 兜底：两个请求同 key 同时进，第二个撞 unique
    existing, _ := s.links.GetByIdempotencyKey(ctx, idemKey)
    writeJSON(w, 200, toResponse(existing))
    return
}
```

**双层防御**：

| 时机 | 防御 | 何时生效 |
|---|---|---|
| 写库前 | SELECT WHERE idem_key | 客户端串行重试（最常见） |
| 写库时 | UNIQUE 约束 + 捕获冲突 | 客户端并发重试（race） |

### 4.3 状态码语义

| 场景 | 状态码 | 理由 |
|---|---|---|
| 第一次创建成功 | **201 Created** | 标准 POST 创建成功 |
| 同 key 重放（已存在） | **200 OK** | 不是新建——告诉客户端"我用了上次的"|
| 同 key 但 long_url 改了 | 200（slink）| 也可严格返回 409 Conflict（v0.5+） |
| key 缺失 | 201 | 未启用幂等，正常创建 |

slink 选 **201 / 200** 区分，方便客户端区分"刚创建" vs "重放命中"。

## 五、关键设计抉择

### 5.1 key 是否必传

| 方案 | 优 | 劣 |
|---|---|---|
| 必传 | 强制幂等 | 客户端生成 key 是负担 |
| **可选**（slink）| 客户端按需启用 | 不传 = 不幂等（接受） |

slink v0.1 选可选。简单 demo 客户端不需要 key 也能用；生产客户端按 SDK 强制传。

### 5.2 key 应该是什么

| 候选 | 优 | 劣 |
|---|---|---|
| **UUID v4**（推荐） | 全局唯一、客户端易生成 | 36 字符稍长 |
| ULID | 时间排序 | 客户端库稍小众 |
| 时间戳 + 随机 | 易读 | 易冲突 |
| MD5(请求体) | 自动幂等 | 改 1 字节就不幂等 |

slink 不限制格式（任意字符串），但**推荐 UUID v4**。

### 5.3 key 的有效期

| 方案 | slink |
|---|---|
| 永久（DB 永远查得到）| ✅ slink v0.1 |
| 24 小时 TTL | Stripe |
| 自定义 TTL | v0.5+ |

slink 永久保存，因为 idem_key 字段在 links 表里，跟 link 同生命周期。**这意味着 key 永不可重用**——客户端必须用真随机 UUID。

### 5.4 key 与请求体不一致怎么办

```
请求 1: idem_key=K, long_url=https://A
请求 2: idem_key=K, long_url=https://B  ← 不一致
```

候选行为：

| 方案 | 行为 |
|---|---|
| **slink v0.1** | 直接返回请求 1 的结果（忽略请求 2 的 long_url） |
| Stripe 严格 | 检查请求体哈希是否一致，不一致返回 409 |

slink v0.1 简化。生产严格场景按 Stripe 模式（v0.5+ 加请求体哈希校验）。

## 六、并发竞态的真实演示

slink 测试 [`TestCreate_IdempotencyConcurrentRace`](../../internal/api/links_test.go) 跑 8 个并发请求，同一个 key：

```
8 个 goroutine 同时 POST /api/links Idempotency-Key=race-X

可能的执行交错：
  T0: G1 SELECT → 不存在
  T0: G2 SELECT → 不存在
  T1: G1 INSERT → 成功（拿到 code1）
  T1: G2 INSERT → unique 冲突
  T2: G2 catch ErrIdempotencyConflict → SELECT → 拿到 code1

期望：所有 8 个响应的 code 字段相同 = code1
```

测试验证通过——证明 DB unique 约束 + race 兜底逻辑工作。

## 七、踩坑清单

| 坑 | 后果 | 解法 |
|---|---|---|
| 只用应用层 SELECT 防重 | 并发下失效 | DB unique 约束兜底 |
| 不处理 unique violation | 返回 500 给客户端 | 捕获后 SELECT 返回已有 |
| key 太短（数字 ID） | 全局碰撞 | 用 UUID v4 |
| key 过期被复用 | 返回旧结果给新业务 | TTL 设置 + 客户端规范 |
| 跨服务用同一 key | 跨服务串数据 | 服务前缀（"orders:K", "links:K"） |
| 把 idem 表跟业务表分开 | 多余的 join | slink 把 idem_key 直接存 links 表 |
| 不区分 200/201 状态码 | 客户端不知道是新建还是重放 | 严格区分 |

## 八、5 分钟自检

合上文档：

1. POST 为什么默认非幂等？
2. 为什么应用层 SELECT-then-INSERT 不可信？
3. Idempotency-Key 失败时的 race 怎么处理？
4. 200 vs 201 在幂等场景的语义？
5. 为什么 slink 把 idem_key 存在 links 表而不是单独表？

## 九、延伸阅读

- [Stripe API Reference: Idempotent Requests](https://stripe.com/docs/api/idempotent_requests)
- [Stripe Engineering: Designing robust and predictable APIs with idempotency](https://stripe.com/blog/idempotency)
- [PostgreSQL ON CONFLICT vs UNIQUE constraint](https://www.postgresql.org/docs/current/sql-insert.html#SQL-ON-CONFLICT)
- [HTTP method idempotency — RFC 9110 §9.2.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2)
