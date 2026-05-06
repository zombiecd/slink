package id

import (
	"errors"
	"strings"
	"testing"
)

func TestEncodeDecode_RoundTrip(t *testing.T) {
	// 关键样本：边界 + 随机分布
	samples := []int64{
		0,
		1,
		61,
		62,
		63,
		1000,
		999_999,
		1_234_567,
		MaxID6 - 1,
	}
	for _, id := range samples {
		t.Run("", func(t *testing.T) {
			code, err := EncodeID(id)
			if err != nil {
				t.Fatalf("EncodeID(%d): %v", id, err)
			}
			if len(code) != codeLen6 {
				t.Errorf("EncodeID(%d) = %q, len=%d, want %d", id, code, len(code), codeLen6)
			}
			got, err := DecodeCode(code)
			if err != nil {
				t.Fatalf("DecodeCode(%q): %v", code, err)
			}
			if got != id {
				t.Errorf("roundtrip: %d → %q → %d", id, code, got)
			}
		})
	}
}

func TestEncodeID_OutOfRange(t *testing.T) {
	cases := []int64{-1, MaxID6, MaxID6 + 1, MaxID6 * 2}
	for _, id := range cases {
		_, err := EncodeID(id)
		if !errors.Is(err, ErrIDOutOfRange) {
			t.Errorf("EncodeID(%d): expected ErrIDOutOfRange, got %v", id, err)
		}
	}
}

func TestDecodeCode_BadLength(t *testing.T) {
	cases := []string{"", "a", "abcde", "abcdefg", "12345678"}
	for _, c := range cases {
		_, err := DecodeCode(c)
		if !errors.Is(err, ErrCodeInvalid) {
			t.Errorf("DecodeCode(%q): expected ErrCodeInvalid, got %v", c, err)
		}
	}
}

func TestDecodeCode_BadChar(t *testing.T) {
	// '+' 不在 charset 里，'/' 也不在
	cases := []string{"abcde+", "abcd/e", "abc-de", "abc de"}
	for _, c := range cases {
		_, err := DecodeCode(c)
		if !errors.Is(err, ErrCodeInvalid) {
			t.Errorf("DecodeCode(%q): expected ErrCodeInvalid, got %v", c, err)
		}
	}
}

// 混淆有效性：连续 ID 编出的短码不应肉眼相似。
//
// 朴素 Base62（无混淆）：1, 2, 3 → "000001", "000002", "000003"
// 加混淆后：每对相邻 ID 编出的短码 Hamming 距离应较大。
func TestEncodeID_ShufflingDispersion(t *testing.T) {
	const samples = 100
	codes := make([]string, samples)
	for i := 0; i < samples; i++ {
		c, err := EncodeID(int64(i))
		if err != nil {
			t.Fatalf("EncodeID(%d): %v", i, err)
		}
		codes[i] = c
	}

	// 检查相邻 ID 编出的短码不应有公共前缀（朴素 Base62 会有 5 位前缀相同）。
	maxCommonPrefix := 0
	for i := 1; i < samples; i++ {
		cp := commonPrefix(codes[i-1], codes[i])
		if cp > maxCommonPrefix {
			maxCommonPrefix = cp
		}
	}
	if maxCommonPrefix >= 4 {
		t.Errorf("shuffling failed: max common prefix between adjacent codes = %d (>=4 means scarcely shuffled)", maxCommonPrefix)
	}

	// 简单的"全部不同"验证（碰撞率应为 0）
	seen := make(map[string]struct{}, samples)
	for _, c := range codes {
		if _, dup := seen[c]; dup {
			t.Errorf("shuffling produced duplicate: %q", c)
		}
		seen[c] = struct{}{}
	}
}

// 编码合法性：所有输出必须在 charset 里。
func TestEncodeID_ValidCharset(t *testing.T) {
	for _, id := range []int64{0, 1, 12345, MaxID6 - 1} {
		c, _ := EncodeID(id)
		for i, ch := range c {
			if !strings.ContainsRune(charset, ch) {
				t.Errorf("EncodeID(%d) char[%d]=%q not in charset", id, i, ch)
			}
		}
	}
}

// 演示：连续 ID 编出的短码看起来是离散的（视觉验证混淆有效）。
// 跑 `go test -v -run TestDemo_SequentialEncoding` 查看。
func TestDemo_SequentialEncoding(t *testing.T) {
	for _, id := range []int64{1, 2, 3, 100, 101, 102, 1_000_000, 1_000_001, 1_000_002} {
		c, _ := EncodeID(id)
		d, _ := DecodeCode(c)
		t.Logf("ID %12d → %q → decoded %d", id, c, d)
	}
}

// 决定性：相同输入恒产生相同输出。
func TestEncodeID_Deterministic(t *testing.T) {
	for _, id := range []int64{42, 100_000, 999_999} {
		var first string
		for i := 0; i < 5; i++ {
			c, _ := EncodeID(id)
			if i == 0 {
				first = c
			} else if c != first {
				t.Errorf("EncodeID(%d) not deterministic: %q vs %q", id, first, c)
			}
		}
	}
}

func TestModInverse_Invariant(t *testing.T) {
	// 用 mulModSafe 检查（直接乘 int64 会溢出）
	if got := mulModSafe(shuffleP, shufflePInv, MaxID6); got != 1 {
		t.Errorf("shuffleP * shufflePInv mod MaxID6 != 1: P=%d Pinv=%d got=%d",
			shuffleP, shufflePInv, got)
	}
}

func TestGCD(t *testing.T) {
	cases := []struct {
		a, b, want int64
	}{
		{12, 18, 6},
		{100, 75, 25},
		{17, 31, 1},
		{shuffleP, MaxID6, 1}, // 必须互质
	}
	for _, c := range cases {
		got := gcd(c.a, c.b)
		if got != c.want {
			t.Errorf("gcd(%d, %d) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

// ────────────────────────────────────────────────────────────
// Benchmarks
// ────────────────────────────────────────────────────────────

func BenchmarkEncodeID(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = EncodeID(int64(i % int(MaxID6)))
	}
}

func BenchmarkDecodeCode(b *testing.B) {
	code, _ := EncodeID(1_234_567)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeCode(code)
	}
}

func BenchmarkRoundTrip(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c, _ := EncodeID(int64(i % int(MaxID6)))
		_, _ = DecodeCode(c)
	}
}

// ────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────

func commonPrefix(a, b string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}
