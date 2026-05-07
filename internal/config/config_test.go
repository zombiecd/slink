package config

import (
	"strings"
	"testing"
	"time"
)

// 设置环境变量的小工具，自动在测试结束时还原。
func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func TestLoad_Defaults(t *testing.T) {
	// 必填项最少给一个，其他全走默认
	setEnv(t, map[string]string{
		"SLINK_PG_DSN": "postgres://test:test@localhost/test",
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"Addr", cfg.Addr, ":18080"},
		{"BaseURL", cfg.BaseURL, "http://localhost:18080"},
		{"LogLevel", cfg.LogLevel, "info"},
		{"Env", cfg.Env, "dev"},
		{"PGMaxConns", cfg.PGMaxConns, int32(20)},
		{"PGMinConns", cfg.PGMinConns, int32(2)},
		{"RedisAddr", cfg.RedisAddr, "localhost:16379"},
		{"IDStepSize", cfg.IDStepSize, int64(1000)},
		{"IDBizTag", cfg.IDBizTag, "link"},
		{"LocalCacheSize", cfg.LocalCacheSize, 4096},
		{"LocalCacheTTL", cfg.LocalCacheTTL, time.Minute},
		{"EventBufferCapacity", cfg.EventBufferCapacity, 50000},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestLoad_RequiredMissing(t *testing.T) {
	// 不设 SLINK_PG_DSN
	t.Setenv("SLINK_PG_DSN", "")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error when SLINK_PG_DSN is missing, got nil")
	}
	if !strings.Contains(err.Error(), "PG_DSN") && !strings.Contains(err.Error(), "PGDSN") {
		t.Errorf("expected error to mention missing PG_DSN, got: %v", err)
	}
}

func TestLoad_EnvOverridesDefault(t *testing.T) {
	setEnv(t, map[string]string{
		"SLINK_PG_DSN":            "postgres://x:y@h/db",
		"SLINK_ADDR":              ":9090",
		"SLINK_PG_MAX_CONNS":      "50",
		"SLINK_LOCAL_CACHE_TTL":   "5m",
		"SLINK_LOCAL_CACHE_SIZE":  "8192",
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Addr != ":9090" {
		t.Errorf("Addr override failed: got %s", cfg.Addr)
	}
	if cfg.PGMaxConns != 50 {
		t.Errorf("PGMaxConns override failed: got %d", cfg.PGMaxConns)
	}
	if cfg.LocalCacheTTL != 5*time.Minute {
		t.Errorf("LocalCacheTTL override failed: got %s", cfg.LocalCacheTTL)
	}
	if cfg.LocalCacheSize != 8192 {
		t.Errorf("LocalCacheSize override failed: got %d", cfg.LocalCacheSize)
	}
}

func TestValidate_Errors(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantSub string
	}{
		{"min > max conns", func(c *Config) { c.PGMinConns = 30 }, "PG_MIN_CONNS"},
		{"max conns 0", func(c *Config) { c.PGMaxConns = 0 }, "PG_MAX_CONNS"},
		{"step size 0", func(c *Config) { c.IDStepSize = 0 }, "ID_STEP_SIZE"},
		{"event buf capacity 0", func(c *Config) { c.EventBufferCapacity = 0 }, "EVENT_BUFFER_CAPACITY"},
		{"event batch > capacity", func(c *Config) {
			c.EventBufferBatchSize = 100000
		}, "EVENT_BUFFER_BATCH_SIZE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validBase()
			tc.mutate(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not mention %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestIsDev(t *testing.T) {
	for _, env := range []string{"dev", "development", "local"} {
		c := &Config{Env: env}
		if !c.IsDev() {
			t.Errorf("IsDev() should be true for env=%q", env)
		}
	}
	for _, env := range []string{"prod", "staging", "production"} {
		c := &Config{Env: env}
		if c.IsDev() {
			t.Errorf("IsDev() should be false for env=%q", env)
		}
	}
}

// validBase 返回一个通过校验的基础 Config，用于负向测试 mutate。
func validBase() *Config {
	return &Config{
		Addr:                     ":18080",
		BaseURL:                  "http://localhost:18080",
		LogLevel:                 "info",
		Env:                      "dev",
		PGDSN:                    "postgres://x:y@h/db",
		PGMaxConns:               20,
		PGMinConns:               2,
		RedisAddr:                "localhost:16379",
		IDStepSize:               1000,
		IDBizTag:                 "link",
		LocalCacheSize:           4096,
		LocalCacheTTL:            time.Minute,
		EventBufferCapacity:      50000,
		EventBufferBatchSize:     2000,
		EventBufferFlushInterval: 500 * time.Millisecond,
	}
}
