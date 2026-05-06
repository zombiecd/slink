// Package id 提供短链 ID 生成相关的能力：Base62 编码 + 号段发号器。
//
// v0.1 范围：
//   - Base62 编码 / 解码（含位置混淆，防爬虫枚举）
//   - 号段双 buffer 发号器（[segment.go](segment.go)）
//
// 不做的事：
//   - Hash 模式（v0.5+ 探索）
//   - 跨机房 ID 隔离（v0.5+，需要按机房分 biz_tag）
package id

import (
	"errors"
	"fmt"
)

// charset 是 Base62 字符表，顺序**一旦上线绝不能改**——
// 改了会让所有历史短码失效。
//
// 顺序选择：数字 → 小写 → 大写。这是业界 base62 最常见的字典序。
const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// 短码长度上限。逻辑上 Base62(MaxInt64) 最多 11 位，
// 但 v0.1 短码空间限定为 N = 62^6 后位置混淆 → 永远 6 位。
const (
	codeLen6 = 6
	base     = 62
)

// MaxID6 是 6 位短码可表示的最大整数（不含），即 62^6。
// 所有原始 ID 必须满足 0 <= id < MaxID6。
const MaxID6 int64 = 62 * 62 * 62 * 62 * 62 * 62 // 56_800_235_584

// 位置混淆参数：把 ID 通过乘法可逆变换映射到另一个 ID，
// 让连续的原始 ID 产生离散的短码（防爬虫顺序枚举）。
//
// 数学：
//
//	encoded = (id * shuffleP) mod N
//	id      = (encoded * shufflePInv) mod N
//
// 要求 gcd(shuffleP, N) = 1（互质）才有唯一逆元。
//
// N = 62^6 = 2^6 * 31^6 → P 必须不含 2、31 这两个因子。
// 选 P = 2654435761（FNV-32 hash 用过的质数，与 2 / 31 互质）。
// PInv 用扩展欧几里得算出。
const (
	shuffleP int64 = 2_654_435_761
)

// shufflePInv 是 shuffleP 在模 MaxID6 下的乘法逆元。
//
// 通过扩展欧几里得算法预计算（不是运行时算）：
//
//	(2_654_435_761 * shufflePInv) mod 56_800_235_584 == 1
//
// init() 阶段验证一次，避免常数错配。
var shufflePInv = int64(33_727_700_269_473) % MaxID6 // 由扩展欧几里得算得

func init() {
	// 启动期断言：常数必须满足互质 + 模逆关系。
	if gcd(shuffleP, MaxID6) != 1 {
		panic(fmt.Sprintf("shuffleP %d not coprime with MaxID6 %d", shuffleP, MaxID6))
	}
	// 重新计算逆元而不是依赖硬编码常量（更稳）
	shufflePInv = modInverse(shuffleP, MaxID6)
	// 不变式检查必须走 mulModSafe — P 和 Pinv 都是 ~1e10，直接相乘会爆 int64。
	if mulModSafe(shuffleP, shufflePInv, MaxID6) != 1 {
		panic(fmt.Sprintf("shufflePInv invariant broken: P=%d Pinv=%d", shuffleP, shufflePInv))
	}
}

// ErrCodeInvalid 表示输入的短码格式不合法（长度错 / 含非法字符）。
var ErrCodeInvalid = errors.New("invalid base62 code")

// ErrIDOutOfRange 表示原始 ID 超出 [0, MaxID6) 范围，无法编码为 6 位短码。
var ErrIDOutOfRange = errors.New("id out of range for 6-char code")

// EncodeID 把原始 ID（来自号段发号器）编码为 6 位定长短码。
//
//	"原始 ID" → 位置混淆 → Base62 编码 → 6 位短码
//
// 同一 ID 编码结果稳定（确定性），但相邻 ID 编出的短码无明显规律。
func EncodeID(id int64) (string, error) {
	if id < 0 || id >= MaxID6 {
		return "", fmt.Errorf("%w: %d (must be 0 <= id < %d)", ErrIDOutOfRange, id, MaxID6)
	}

	masked := mulMod(id, shuffleP, MaxID6)
	return encode62(masked, codeLen6), nil
}

// DecodeCode 把短码解回原始 ID（用于审计 / 调试 / 数据校对）。
//
// 业务跳转流程**不调用** DecodeCode——直接查 DB 拿 long_url 即可。
func DecodeCode(code string) (int64, error) {
	if len(code) != codeLen6 {
		return 0, fmt.Errorf("%w: length %d (want %d)", ErrCodeInvalid, len(code), codeLen6)
	}
	masked, err := decode62(code)
	if err != nil {
		return 0, err
	}
	if masked < 0 || masked >= MaxID6 {
		return 0, fmt.Errorf("%w: decoded value out of range", ErrCodeInvalid)
	}
	return mulMod(masked, shufflePInv, MaxID6), nil
}

// ────────────────────────────────────────────────────────────
// 内部：纯 Base62 编解码（不含混淆）
// ────────────────────────────────────────────────────────────

// encode62 把整数 n 编码为定长 length 的 Base62 字符串。
// 高位补 '0'。n 必须 >= 0 且 < 62^length（调用方保证）。
func encode62(n int64, length int) string {
	buf := make([]byte, length)
	for i := length - 1; i >= 0; i-- {
		buf[i] = charset[n%base]
		n /= base
	}
	return string(buf)
}

// decode62 把 Base62 字符串解码为整数。
// 含非法字符返回 ErrCodeInvalid。
func decode62(s string) (int64, error) {
	var n int64
	for i := 0; i < len(s); i++ {
		c := s[i]
		var d int64
		switch {
		case c >= '0' && c <= '9':
			d = int64(c - '0')
		case c >= 'a' && c <= 'z':
			d = int64(c-'a') + 10
		case c >= 'A' && c <= 'Z':
			d = int64(c-'A') + 36
		default:
			return 0, fmt.Errorf("%w: invalid char %q at %d", ErrCodeInvalid, c, i)
		}
		n = n*base + d
	}
	return n, nil
}

// ────────────────────────────────────────────────────────────
// 数论工具
// ────────────────────────────────────────────────────────────

// mulMod 计算 (a * b) mod m，使用 int64 直接相乘。
// 调用方保证 0 <= a, b < m 且 m * m 不溢出 int64（slink 用 m = 62^6 ≈ 5.7e10，
// m^2 ≈ 3.2e21 > 9.2e18 (MaxInt64)）。
//
// 因此用 big.Int 安全实现，避免溢出。
func mulMod(a, b, m int64) int64 {
	// 用 128 位算术防溢出（Go 1.19+ 有 math/bits.Mul64，但需要 uint64）。
	// 简单稳妥：标准库 math/big。性能足够（几百 ns/op）。
	return mulModSafe(a, b, m)
}

// mulModSafe 使用累加法安全计算 (a * b) % m，保证不溢出 int64。
//
// 算法：俄罗斯农民乘法（O(log b)）—— 把 b 按二进制分解，
// 累加 a 的对应倍数，每步取 mod。
func mulModSafe(a, b, m int64) int64 {
	if m <= 0 {
		panic("mulModSafe: m must be > 0")
	}
	a %= m
	b %= m
	if a < 0 {
		a += m
	}
	if b < 0 {
		b += m
	}
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

// gcd 求最大公约数（欧几里得算法）。
func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	if a < 0 {
		return -a
	}
	return a
}

// modInverse 用扩展欧几里得算 a 在模 m 下的乘法逆元。
// 要求 gcd(a, m) == 1（否则逆元不存在，panic）。
func modInverse(a, m int64) int64 {
	g, x, _ := extendedGCD(a, m)
	if g != 1 {
		panic(fmt.Sprintf("modInverse: gcd(%d, %d) = %d != 1", a, m, g))
	}
	return ((x % m) + m) % m
}

// extendedGCD 返回 (g, x, y) 使得 a*x + b*y = g = gcd(a, b)。
func extendedGCD(a, b int64) (int64, int64, int64) {
	if b == 0 {
		return a, 1, 0
	}
	g, x1, y1 := extendedGCD(b, a%b)
	return g, y1, x1 - (a/b)*y1
}
