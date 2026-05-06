# ADR-0004: 短码采用乘法可逆变换做位置混淆

- **Status**: Accepted
- **Date**: 2026-05-06
- **Deciders**: @zombiecd
- **Phase**: v0.1 Day 3

## Context（背景）

号段模式 + Base62 朴素编码会让连续 ID 产生连续短码：

```
ID 1   → "000001"
ID 2   → "000002"
...
ID 100 → "00001C"
```

**结果**：攻击者写 30 行脚本就能枚举出系统的所有短链：

```bash
for i in $(seq 0 99); do
  code=$(printf "%06d" $i)
  curl -s -o /dev/null -w "%{http_code}\n" https://o.cn/$code
done
```

商业损失（[ADR-0002](0002-id-segment-not-snowflake.md) 已识别）：

- 营销活动落地页地址泄露 → 竞争对手分析
- 数据库实际短链数量 / 创建速率被推断（商业秘密）
- 用户行为分析（"哪些短链被点过"）泄漏

我们需要一种**输出短码但不可猜测**的编码方案。

## Options Considered（候选方案）

### A. 不混淆（接受可枚举）

❌ 商业风险不可接受
❌ 上线后改算法 = 历史短码全失效

### B. 随机生成 + DB 查重

```go
for {
    code = randomBase62(6)
    if !dbExists(code) { return code }
}
```

✅ 短码完全随机，不可预测
❌ **每次创建都查 DB**——号段模式优势丢失
❌ 短码空间填满后插入失败率上升
❌ 没有 ID → code 双射，运维不能根据 ID 找短码

### C. Hash(URL) 截断

```go
hash := md5(longURL)
code := base62(hash[:5])
```

✅ 相同 URL 复用同一短码（去重）
❌ **碰撞**：不同 URL → 同 hash → 同短码。要加随机 salt → 失去去重优势
❌ 攻击者构造碰撞（生日攻击 6 位 Base62 ≈ 24 万尝试）
❌ 不能从 code 反查 ID（不是双射）

### D. Hashids / Sqids 库

```go
hashids := hashids.New(salt, 6)
code := hashids.encode(id)
```

✅ 业界库，多语言生态
✅ salt 自定义
❌ 长度不定（输入越大输出越长）
❌ 内部 char shuffle 算法不透明
❌ 第三方库版本碎片化、维护节奏不可控
❌ 引入额外依赖

### E. Skip32 / FFX 加密

```go
encrypted := skip32(id, key)
code := base62(encrypted, 6)
```

✅ 加密强度高（防"算法已知 + key 未知"破解）
❌ 引入加密库依赖（Skip32 不在 stdlib）
❌ 杀鸡用牛刀：slink 防爬虫枚举不防国家级攻击
❌ 算法相对小众，社区资料少

### F. 乘法可逆变换（最终选择）

```go
masked = (id × P) mod N
id     = (masked × P⁻¹) mod N

N = 62^6 = 56,800,235,584
P = 2,654,435,761  // 质数，与 N 互质
```

✅ 双射（每个 ID 唯一映射，可逆解码）
✅ 长度恒定 6 位
✅ 数学清晰：3 行核心逻辑（互质 / 模逆 / mulModSafe）
✅ 零依赖
✅ 性能 ~150 ns/op（足够 1w+ QPS 创建）
❌ 不防"P / N 已知 + 暴力反推"——但 slink 防的是脚本爬虫，不是逆向工程

## Decision（最终决策）

**选 F：乘法可逆变换**。

数学公式：

```
encode(id)     = (id × P) mod N
decode(masked) = (masked × P⁻¹) mod N
```

参数：

- `N = 62^6 = 56,800,235,584` —— 6 位 Base62 短码空间
- `P = 2,654,435,761` —— 质数，FNV-32 hash 用过，与 N（= 2^6 × 31^6）互质
- `P⁻¹` —— 由扩展欧几里得算法在 `init()` 阶段计算
- `mulModSafe` —— 俄罗斯农民乘法，O(log b)，避免 int64 溢出

**关键实现细节**：

- 启动期断言 `gcd(P, N) == 1` —— 守护互质不变式
- 启动期断言 `(P × P⁻¹) mod N == 1` —— 守护逆元正确性
- 不变式断言用 `mulModSafe`，**不**用裸 int64 乘法（会溢出）

## Consequences（后果）

### 正面

- 攻击者枚举 6 位空间需 568 亿次请求；配合 IP 限流根本跑不完
- 双射 + 可逆 → 运维有 ID → code 工具用于审计 / 调试
- 短码长度 6 位恒定 → 营销 / 短信 / 二维码 UI 一致
- 零依赖、纯算法 → 编码层无外部故障点

### 负面

- 短码空间最多 568 亿。slink 全生命周期累计创建 > 568 亿短链需要升 7 位（v0.5+）
- "算法已知 + 暴力反推 P" 仍可能（但需要拿到大量 (id, code) 对）—— slink 的真实威胁模型是脚本爬虫，本方案够用
- charset 顺序、N、P 一旦上线**绝对不能改**（改了历史短码全失效）

### 升级路径（v0.5+）

| 触发条件 | 升级方案 |
|---|---|
| 累计创建 > 100 亿 | 升 7 位（N = 62^7），同时换新 P，新短链长 7 位，旧短链仍 6 位 |
| 业务需要"算法对抗"（如金融） | 换 Skip32 / AES-FF1，引入加密库 |
| 跨多机房需要号段隔离 | 编码时加机房前缀位 |

升级方案在 v0.1 都不需要做，但**接口隔离要为它们留路径**——`Generator` 接口 + `EncodeID` 函数都不暴露内部 P / N，未来可换实现。

## 风险点

| 风险 | 概率 | 影响 | 缓解 |
|---|---|---|---|
| 启动期 init panic（P/N 不互质） | 极低 | 服务不能启动 | 单测 + 启动断言双保险 |
| int64 溢出（P × P⁻¹）| 中（首版踩过）| panic | mulModSafe 替代裸乘法 |
| charset 改动 | 低 | 历史短码失效 | 文档强标注 + code review |
| P 被反推 | 极低 | 防爬虫机制失效 | 监控异常枚举模式 |
| 短码碰撞 | 0 | n/a | 数学保证（双射） |

## Related（相关）

- [base62-encoding 概念深度](../concepts/base62-encoding.md)
- ADR-0001: 选 PG 不选 MySQL
- ADR-0002: 选号段不选 Snowflake
- ADR-0003: 配置库选 caarlos0/env
- [`internal/id/base62.go`](../../internal/id/base62.go) — 实现

## Revisit（何时重审）

- 累计创建短链 > 100 亿（接近 N 空间 1/5）
- 出现真实"短码被反推枚举"安全事件
- 业务需要"相同长 URL 复用短码"（重新评估 Hash + 号段混合）
- 监管要求短码不可逆（极端场景，需要换加密方案）
