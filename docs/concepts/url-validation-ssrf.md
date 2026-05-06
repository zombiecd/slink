# URL 校验与 SSRF 防御

> 5 分钟讲透：URL 校验范围、SSRF 攻击模型、scheme 白名单、私网 IP 拦截、DNS rebinding 边界。
> 对应文件：[`internal/api/validate.go`](../../internal/api/validate.go)

## 一、问题：为什么 URL 校验是必须的

短链系统接收用户提交的 long_url，**任何**用户输入都是不可信的。校验有两层目标：

1. **数据正确性**：URL 解析得了、能跳转
2. **安全防护**：禁止恶意 URL（钓鱼 / 跨站 / SSRF）

不校验的后果：

| 攻击类型 | 攻击者提交 | 后果 |
|---|---|---|
| **XSS via 跳转** | `javascript:alert(1)` | 跳转时注入脚本 |
| **任意文件读** | `file:///etc/passwd` | 浏览器/客户端读本地文件 |
| **数据 URL XSS** | `data:text/html,<script>...</script>` | 内联 HTML 执行 |
| **SSRF**（v0.3+ 链接预览功能后才严重） | `http://127.0.0.1:6379/...` | 通过你的服务访问内部资源 |
| **DoS** | 100MB long_url | 数据库 / 内存爆 |

## 二、slink 的校验链（5 步短路）

```go
// internal/api/validate.go
func ValidateLongURL(s string) error {
    // 1. 非空
    if s == "" { return ... }
    // 2. 长度上限
    if len(s) > MaxLongURLLength { return ... }
    // 3. 可解析
    u, err := url.Parse(s)
    if err != nil { return ... }
    // 4. scheme 白名单
    if u.Scheme not in {http, https} { return ... }
    // 5. 私网 / loopback IP 拦截
    if isUnsafeHost(u.Hostname()) { return ... }
}
```

**为什么 5 步顺序很重要**：

- 早 fail 早返回（性能）
- 失败信息精确（"empty" 比 "invalid URL" 更有用）
- 后续步骤依赖前面成功（u.Scheme 必须先 Parse）

## 三、scheme 白名单（最重要）

```go
var allowedSchemes = map[string]struct{}{
    "http":  {},
    "https": {},
}
```

**为什么是白名单不是黑名单**：

| scheme | 黑名单方案 | 白名单方案 |
|---|---|---|
| `javascript:` | 必须显式拒绝 | 不在 http/https 白名单里 → 拒绝 |
| `data:` | 必须显式拒绝 | 同上 |
| `file://` | 必须显式拒绝 | 同上 |
| `gopher://` | 必须显式拒绝 | 同上 |
| `ws://` | 忘了？没拒绝 → 漏 | 自动拒绝 |
| 未来发明的 `quux://` | 没拒绝 → 漏 | 自动拒绝 |

**安全黄金法则**：能用白名单就用白名单。

## 四、SSRF 攻击与防御

### 4.1 什么是 SSRF（Server-Side Request Forgery）

> 攻击者诱导**你的服务器**去访问**它不应该访问的资源**。

**经典案例**：

```
你的服务有"链接预览"功能：用户提交 URL → 服务 fetch → 渲染 og:title

攻击者提交：http://169.254.169.254/latest/meta-data/iam/security-credentials/
（AWS 实例元数据 endpoint）

你的服务在 AWS 上跑 → fetch 这个 URL → 拿到 IAM 凭证 → 返回给攻击者
```

历史事件：

- **Capital One 2019** 数据泄露 1 亿用户：通过 SSRF 拿到 AWS S3 凭证
- **Shopify 2018** 通过 SSRF 攻击内网 Redis

### 4.2 slink 的 SSRF 风险评估

| slink 版本 | 是否 fetch 长 URL | SSRF 风险 |
|---|---|---|
| v0.1 创建 + 跳转 | ❌ 不 fetch | 低（仅数据存储 + 302） |
| v0.3+ 链接预览 | ✅ 服务主动 fetch | **高** |
| v0.3+ 健康检查长 URL 是否 200 | ✅ | 高 |

**slink v0.1 当前 SSRF 风险其实不高**——服务只存 + 302，浏览器去 fetch（受同源策略保护）。

但我们仍加 SSRF 防御，理由：

1. **防御深度**（defense-in-depth）：未来加 fetch 功能时已有保护
2. **攻击者会尝试**：黑产扫到自托管短链服务总会试
3. **不可逆数据**：错误数据进 DB 后改不掉

### 4.3 拦截规则

```go
// internal/api/validate.go isUnsafeHost
拒绝以下：
  - "localhost" / "localhost.localdomain"
  - 字面 IP 落在：
    * loopback (127.0.0.0/8, ::1)
    * 私网 (10/8, 172.16/12, 192.168/16)
    * 链路本地 (169.254/16, fe80::/10)
    * unspecified (0.0.0.0)
    * multicast / broadcast
    * IETF reserved (TEST-NET, CGN, class E)
```

`netip.Addr.IsPrivate / IsLoopback / IsLinkLocalUnicast` 等标准库方法 + 手动 CIDR 列表覆盖剩余保留段。

### 4.4 没拦截的 case（DNS rebinding）

最阴险的 SSRF 攻击：

```
攻击者控制域名 evil.com，A 记录返回 1.2.3.4 (公网 IP)

时刻 T0：攻击者提交 http://evil.com/
        slink 校验：DNS 解析 evil.com → 1.2.3.4 → 公网 IP → 通过
        slink 存进 DB

时刻 T1：攻击者改 evil.com 的 DNS 记录 → 127.0.0.1

时刻 T2：浏览器访问 https://o.cn/abc → 302 → http://evil.com/
        浏览器再 DNS 解析 evil.com → 127.0.0.1
        → 浏览器访问 http://127.0.0.1/  ← 内网穿透
```

**slink v0.1 不解决这个**。理由：

- v0.1 不在服务侧 fetch，DNS rebinding 影响的是**用户浏览器**——这个攻击对短链系统本身无影响
- v0.3+ 加 fetch 时会引入 DNS rebinding 防护（解析 IP 后用解析结果再校验，使用单一连接）

## 五、几个细节决策

### 5.1 字面域名（"localhost"）拦不拦

slink **拦** "localhost" 字面字符串。理由：

- 用户大概率手抖输入了开发地址
- 即便 DNS 解析后是公网（极罕见），也是数据错误
- 规则简单

不在校验时**做 DNS 解析**：

- 解析有 TTL 缓存问题
- 慢（DNS 100ms+）
- DoS：攻击者提交 100 个不存在域名 → 100 次 DNS 超时
- DNS rebinding 防不住

### 5.2 IPv6 处理

`netip.ParseAddr("[::1]")` 不能解析（带方括号）—— `url.Parse` 已经把方括号去掉，所以 `u.Hostname()` 返回 "::1"，可被 `netip.ParseAddr` 解析。

**slink 测试覆盖** `http://[::1]/` → 拒绝 ✓

### 5.3 端口字段

```
http://192.168.1.1:8080/x
              ↑↑↑↑↑↑↑↑↑↑
              u.Host 含端口
              u.Hostname() 不含
```

slink 用 `u.Hostname()` 取纯 host，不要 split 自己来。

### 5.4 中文域名（IDN）

```
https://例如.中国/path
```

`url.Parse` 直接接受，`u.Host` = "例如.中国"。slink 当前**通过**这种 URL（白名单 scheme + 非私网域名）。

**潜在问题**：未来 fetch 时 IDN 转 punycode（"xn--fsq...")，可能 DNS 解析行为不同。v0.3+ fetch 实现要专门处理。

## 六、URL 长度上限怎么定

```go
const MaxLongURLLength = 2048
```

### 业界参考

| 方 | 上限 |
|---|---|
| 浏览器（Chrome） | URL 路径 ~32K，整体 ~64K |
| nginx 默认 | 8K |
| 大部分 SaaS API | 2K - 8K |

### slink 选 2048 的理由

1. **够用**：99% 真实长 URL（含 utm 参数）< 1500 字符
2. **防 DoS**：100MB long_url 提交会撑爆解析 + DB
3. **DB 友好**：PG TEXT 字段无限制，但索引和缓存都受益于短字段
4. **可调**：未来通过 config 调高（v0.5+）

校验逻辑早 fail：

```go
if len(s) > MaxLongURLLength {
    return fmt.Errorf("...")
}
```

不要先 url.Parse 再判长度——前者可能崩或慢。

## 七、踩坑清单

| 坑 | 后果 | 解法 |
|---|---|---|
| scheme 黑名单 | 漏拦新型 scheme | 用白名单 |
| 没限长度 | DoS | 早 fail |
| 接受字面 "localhost" | 内网泄露 | 字面字符串黑名单 |
| 校验时做 DNS 解析 | 慢 + 易绕（DNS rebinding） | 不解析，仅查字面 IP |
| 拦私网但漏链路本地 169.254 | 漏 AWS metadata | 用 netip.IsLinkLocalUnicast 等齐 |
| IPv6 不处理 | http://[::1]/ 通过 | url.Hostname() 配合 netip 验证 |
| 校验放业务里 | 多处校验不一致 | 统一 ValidateLongURL 函数 |
| 用 net/url Parse 后没看 scheme | "example.com" 解析后 scheme="" | 检查 scheme 非空 |

## 八、5 分钟自检

合上文档：

1. SSRF 攻击的本质是什么？最经典的目标 endpoint？
2. 白名单 vs 黑名单为什么必须用白名单？
3. 校验时为什么不该做 DNS 解析？
4. DNS rebinding 攻击怎么绕过 IP 校验？slink 怎么应对？
5. URL 长度上限定多少？为什么不能太长？

## 九、延伸阅读

- [OWASP: Server-Side Request Forgery (SSRF)](https://owasp.org/www-community/attacks/Server_Side_Request_Forgery)
- [Capital One Breach Analysis](https://krebsonsecurity.com/2019/08/what-we-can-learn-from-the-capital-one-hack/)
- [DNS Rebinding — Wikipedia](https://en.wikipedia.org/wiki/DNS_rebinding)
- [HackerOne SSRF Bug Bounty Disclosures](https://hackerone.com/reports?q=ssrf)
- [Go netip package](https://pkg.go.dev/net/netip)
- [IETF reserved IP allocations](https://www.iana.org/assignments/iana-ipv4-special-registry/iana-ipv4-special-registry.xhtml)
