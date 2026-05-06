package api

import (
	"strings"
	"testing"
)

func TestValidateLongURL_OK(t *testing.T) {
	cases := []string{
		"https://example.com",
		"http://example.com/path",
		"https://example.com/path?query=1&utm=source",
		"https://www.example.com:8443/p/q",
		"https://例如.中国/path",
	}
	for _, s := range cases {
		t.Run(s, func(t *testing.T) {
			if err := ValidateLongURL(s); err != nil {
				t.Errorf("ValidateLongURL(%q): %v", s, err)
			}
		})
	}
}

func TestValidateLongURL_Empty(t *testing.T) {
	if err := ValidateLongURL(""); err == nil {
		t.Error("expected error for empty URL")
	}
}

func TestValidateLongURL_TooLong(t *testing.T) {
	long := "https://example.com/" + strings.Repeat("a", MaxLongURLLength)
	err := ValidateLongURL(long)
	if err == nil {
		t.Error("expected error for over-length URL")
	}
	if !strings.Contains(err.Error(), "exceeds limit") {
		t.Errorf("expected 'exceeds limit' in error, got: %v", err)
	}
}

func TestValidateLongURL_BadScheme(t *testing.T) {
	cases := []string{
		"file:///etc/passwd",
		"javascript:alert(1)",
		"data:text/html,<script>alert(1)</script>",
		"ftp://example.com",
		"gopher://example.com",
		"//example.com",        // 无 scheme
		"example.com",          // 无 scheme
	}
	for _, s := range cases {
		t.Run(s, func(t *testing.T) {
			if err := ValidateLongURL(s); err == nil {
				t.Errorf("expected error for %q", s)
			}
		})
	}
}

func TestValidateLongURL_SSRF(t *testing.T) {
	cases := []string{
		"http://localhost/x",
		"http://127.0.0.1/",
		"http://127.0.0.2/",     // 127.x.x.x 整段
		"http://10.0.0.1/admin",
		"http://192.168.1.1/",
		"http://172.16.0.1/",
		"http://169.254.169.254/", // AWS metadata（链路本地）
		"http://[::1]/",            // IPv6 loopback
		"http://0.0.0.0/",
		"http://255.255.255.255/",
		"http://100.64.0.1/",       // CGN
	}
	for _, s := range cases {
		t.Run(s, func(t *testing.T) {
			err := ValidateLongURL(s)
			if err == nil {
				t.Errorf("expected SSRF rejection for %q", s)
			}
		})
	}
}

func TestValidateLongURL_NoHost(t *testing.T) {
	if err := ValidateLongURL("https:///"); err == nil {
		t.Error("expected error for empty host")
	}
}
