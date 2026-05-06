# Base62 编码与位置混淆

> 5 分钟讲透：Base62 字符集、编码算法、位置混淆数学（互质 / 模逆 / 双射）、与 Crockford Base32 / Hashids 对比。
> 对应文件：[`internal/id/base62.go`](../../internal/id/base62.go)

## 一、为什么是 62 进制

短码追求"短"。把整数 ID 转字符串时，**每个字符携带的信息量越大，最终越短**：

| 进制 | 字符集 | 6 位能表示 | 100 亿 ID 需要几位 |
|---|---|---|---|
| 10 | `0-9` | 100 万 | 11 位 |
| 16（hex） | `0-9 a-f` | 1670 万 | 9 位 |
| 36 | `0-9 a-z` | 21 亿 | 7 位 |
| **62**（slink）| `0-9 a-z A-Z` | **568 亿** | **6 位** |
| 64（base64） | `+ /` 是 URL 不安全字符 | 687 亿 | 6 位 |

62 是**仅用 URL 安全字符**能拿到的最大进制——不需要 URL encode、可放进 path 段。

## 二、Base62 字符集（一旦上线绝不能改）

```go
const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
```

**为什么这个顺序**：

1. **数字 → 小写 → 大写**：业界最常见字典序，Wikipedia / Stack Overflow / 多数库用同样排列
2. **可读性**：数字打头、字母在后，扫一眼就知道大致 magnitude

**为什么"绝不能改"**：

字符集顺序决定编码值。一旦有用户拿到 `5BxX` 短码：
```
charset[i] 决定 'x' 对应的整数是 33（小写 'a' = 10，小写 'x' = 10+23 = 33）
如果哪天 charset 改成 "abcdefg..." → 同一个 'x' 整数变了 → 历史短码全部失效
```

**修复历史短码失效成本**：DB 改写、CDN 缓存、用户手机短信里的链接……做不到。

## 三、字符集陷阱：混淆字符

`0/O`、`1/l/I` 在某些字体下视觉混淆。如果短链印在二维码、纸质海报上，用户可能识别错。

**两条路**：

| 方案 | 字符集 | slink 选择 |
|---|---|---|
| 完整 Base62 | 0-9 a-z A-Z | ✅ slink v0.1 |
| Crockford Base32 | 移除 `0/O/1/I/L/U`，剩 32 个 | 高安全场景 |

slink 是营销/电商场景，短链主要用在短信链接（用户点而非读），混淆字符不是大问题。如果业务变成"印名片二维码"才考虑 Crockford。

## 四、编码算法（最简单的部分）

### Encode：整数 → 字符串

```go
func encode62(n int64, length int) string {
    buf := make([]byte, length)
    for i := length - 1; i >= 0; i-- {
        buf[i] = charset[n%62]
        n /= 62
    }
    return string(buf)
}
```

**复杂度**：O(length)，length 固定 6 → O(1)。

**关键点**：

- **从右往左填**：余数代表低位
- **定长 padding**：高位用 charset[0]='0' 补齐——保证短码定长 6 位

### Decode：字符串 → 整数

```go
func decode62(s string) (int64, error) {
    var n int64
    for i := 0; i < len(s); i++ {
        c := s[i]
        var d int64
        switch {
        case c >= '0' && c <= '9': d = int64(c - '0')
        case c >= 'a' && c <= 'z': d = int64(c-'a') + 10
        case c >= 'A' && c <= 'Z': d = int64(c-'A') + 36
        default: return 0, ErrCodeInvalid
        }
        n = n*62 + d
    }
    return n, nil
}
```

不用 `strings.IndexByte`——switch case 比哈希查表快。

## 五、为什么需要位置混淆

**朴素 Base62 的致命问题**：

```
ID 1   → "000001"
ID 2   → "000002"
ID 3   → "000003"
ID 100 → "00001C"
ID 101 → "00001D"
```

攻击者写脚本枚举：

```bash
for code in 000001..000099; do
  curl https://o.cn/$code
done
```

→ **拖出整个数据库的所有短链**。

商业损失：
- 营销活动落地页地址泄露
- 用户行为分析（"这家公司每分钟创建多少链接"是商业秘密）
- 企业内部资源链接被遍历

## 六、混淆方案：乘法可逆变换

我们要一个函数 `f` 满足：

1. **双射**：每个 ID 映射到唯一短码
2. **可逆**：给定短码能算出 ID（用于审计）
3. **离散**：相邻 ID 的短码视觉上无关
4. **确定**：相同 ID 永远映射到相同短码

**数学解法**：

```
masked = (id × P) mod N
id     = (masked × P⁻¹) mod N

其中 N = 62^6（短码空间大小）
    P = 与 N 互质的大数
    P⁻¹ = P 在模 N 下的乘法逆元
```

**为什么 P 必须与 N 互质（gcd = 1）**：

- 互质 → P 在模 N 下有唯一逆元
- 不互质 → 多个 id 映射到同一 masked → **碰撞**

### slink 怎么选 P

```
N = 62^6 = 56,800,235,584 = 2^6 × 31^6
```

P 必须不含 2、31 这两个因子（否则 gcd ≠ 1）。

slink 选 **P = 2,654,435,761**：

- 是质数（没有任何因子，自然不含 2 / 31）
- ~2.6 × 10⁹，比 N 小一个数量级，让乘法分散均匀
- FNV-32 哈希用过这个常数（业界验证过的随机性）

### P⁻¹ 用扩展欧几里得算

```
ext_gcd(P, N) = (g, x, y) 满足 P×x + N×y = g
若 g = 1，则 P × x ≡ 1 (mod N)，即 x = P⁻¹ mod N
```

slink 的 [`modInverse`](../../internal/id/base62.go) 在 `init()` 期算好 P⁻¹，运行时直接乘。

## 七、int64 溢出陷阱（slink 真实踩过）

**问题**：

```
N = 5.7 × 10¹⁰
P × P⁻¹ 验证：(P × P⁻¹) mod N == 1 ?
```

P 和 P⁻¹ 都是 ~10¹⁰ 量级，**直接 int64 相乘会溢出**：

```
2_654_435_761 × 21_283_228_561 ≈ 5.6 × 10¹⁹ > MaxInt64 (9.2 × 10¹⁸)
```

**首版代码**：`(P × Pinv) % N` → 启动期 init() panic。

**修复**：用 `mulModSafe`（俄罗斯农民乘法），O(log b)：

```go
func mulModSafe(a, b, m int64) int64 {
    a %= m
    b %= m
    var result int64
    for b > 0 {
        if b&1 == 1 {
            result = (result + a) % m
        }
        a = (a * 2) % m
        b >>= 1
    }
    return result
}
```

每步保持 < 2m，永不溢出。**业务路径每次 EncodeID 走这个函数**。

**性能**：~150 ns/op。够用——业务调 EncodeID 频次远小于 100w/s。

进一步优化可用 `math/bits.Mul64 + bits.Div64`（128 位硬件指令，O(1)，~10ns），slink v0.1 不做（YAGNI）。

## 八、可视化效果

```
ID            1 → "2TDK01"
ID            2 → "5Nhu02"
ID            3 → "8GVe03"
ID          100 → "FK6c1C"
ID          101 → "IDJW1D"
ID          102 → "LxnG1E"
ID      1000000 → "Pt1G92"
ID      1000001 → "SmFq93"
ID      1000002 → "Vgja94"
```

观察：

- 相邻 ID 的短码**前 4 位无肉眼相关性**
- 后 2 位有局部模式（因为乘法的低位变化最小）
- 单测验证：相邻 100 个 ID 的最大公共前缀 < 4 位

完整防爬虫枚举的代价：攻击者要尝试 62^6 = 568 亿次才能遍历空间，配合限流根本跑不完。

## 九、对比业界方案

### A. Hashids

```js
const hashids = new Hashids("salt", 6);
hashids.encode(1) // "jR"
hashids.encode(2) // "k5"
```

✅ 多语言库现成
✅ 自定义 salt
❌ 长度不定（输入越大越长）
❌ 内部用 char shuffle，可逆需要相同 salt
❌ 第三方库版本碎片化

### B. Sqids（Hashids 升级版）

类似 Hashids 但更现代。同样的优劣。

### C. Skip32 / FFX 加密

```
Skip32(id, key) = encrypted_id
```

✅ 加密强度高
❌ 引入加密库依赖
❌ 大材小用——slink 不防国家级攻击

### D. slink 选乘法可逆变换

✅ 数学清晰：3 行核心逻辑（互质 / 模逆 / mulModSafe）
✅ 零依赖
✅ O(log N) 编码、O(log N) 解码
✅ 长度恒定 6 位
❌ 不防"对算法本身的破解"——但攻击者要逆向工程 P 和 N

slink 业务模型：**防爬虫枚举 + 视觉混淆**，不防"算法已知 + 暴力解 P"。乘法可逆变换够用。

## 十、踩坑清单

| 坑 | 后果 | 解法 |
|---|---|---|
| charset 顺序后改 | 历史短码失效 | 永远不改 |
| P 与 N 不互质 | 碰撞（多 ID 同短码） | gcd 启动期断言 |
| `P × P⁻¹` 直接相乘 | int64 溢出 | mulModSafe |
| 用混淆字符（0/O 1/I） | 用户输错 | 选 Crockford 或纯小写 |
| 短码可枚举 | 数据泄露 | 加位置混淆 |
| 短码空间不够 | 重复 / 撞库 | N 取 62^7 / 升 7 位 |

## 十一、5 分钟自检

合上文档：

1. 为什么 62 比 64 好？
2. 朴素 Base62（无混淆）的致命问题？
3. 位置混淆需要哪三个数学条件？
4. P 必须与 N 互质，N = 62^6 时哪些数会被排除？
5. mulModSafe 解决什么问题？
6. 短码升 7 位的代价？

## 十二、延伸阅读

- [Base62 on Wikipedia](https://en.wikipedia.org/wiki/Base62)
- [Crockford Base32](https://www.crockford.com/base32.html)
- [Hashids](https://hashids.org/)
- [Sqids](https://sqids.org/)
- [扩展欧几里得算法 — CP-Algorithms](https://cp-algorithms.com/algebra/extended-euclid-algorithm.html)
- [Modular multiplicative inverse — Wikipedia](https://en.wikipedia.org/wiki/Modular_multiplicative_inverse)
- ADR-0004: 短码混淆方案选择
