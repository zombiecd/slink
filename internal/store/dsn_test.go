package store

import (
	"strings"
	"testing"
)

func TestRedactDSN(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
		// want substrings 必须在结果中出现（以及"绝对不能"出现的子串）
		mustContain    []string
		mustNotContain []string
	}{
		{
			name:           "empty",
			dsn:            "",
			mustContain:    []string{""},
			mustNotContain: nil,
		},
		{
			name:           "url with password",
			dsn:            "postgres://slink:supersecret@db.example.com:5432/slink?sslmode=disable",
			mustContain:    []string{"slink", "redacted", "db.example.com", "5432", "/slink"},
			mustNotContain: []string{"supersecret"},
		},
		{
			name:           "url no password",
			dsn:            "postgres://slink@db/slink",
			mustContain:    []string{"slink", "db"},
			mustNotContain: []string{"redacted"}, // 没密码就不该插 ***
		},
		{
			name:           "url empty password (special form)",
			dsn:            "postgres://user:@host/db",
			mustContain:    []string{"user", "host", "/db"},
			mustNotContain: nil,
		},
		{
			name:           "kv form",
			dsn:            "host=db port=5432 user=slink password=topsecret dbname=slink",
			mustContain:    []string{"host=db", "user=slink", "password=***"},
			mustNotContain: []string{"topsecret"},
		},
		{
			name:           "kv form with quotes",
			dsn:            "host=db user=slink password='quoted-pw' dbname=slink",
			mustContain:    []string{"password=***"},
			mustNotContain: []string{"quoted-pw"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactDSN(tc.dsn)
			for _, sub := range tc.mustContain {
				if !strings.Contains(got, sub) {
					t.Errorf("RedactDSN(%q) = %q; missing substring %q", tc.dsn, got, sub)
				}
			}
			for _, sub := range tc.mustNotContain {
				if sub != "" && strings.Contains(got, sub) {
					t.Errorf("RedactDSN(%q) = %q; LEAKED forbidden substring %q", tc.dsn, got, sub)
				}
			}
		})
	}
}

func TestRedactSecrets(t *testing.T) {
	dsn := "postgres://slink:supersecret@db/slink"

	cases := []struct {
		name           string
		msg            string
		mustNotContain []string
	}{
		{
			name:           "msg quotes full DSN",
			msg:            "parse failed: cannot parse `" + dsn + "`",
			mustNotContain: []string{"supersecret"},
		},
		{
			name:           "msg quotes only password",
			msg:            "auth failed for password 'supersecret'",
			mustNotContain: []string{"supersecret"},
		},
		{
			name:           "msg has no secret",
			msg:            "connection refused: db:5432",
			mustNotContain: []string{"supersecret"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactSecrets(tc.msg, dsn)
			for _, sub := range tc.mustNotContain {
				if strings.Contains(got, sub) {
					t.Errorf("redactSecrets leaked %q in: %s", sub, got)
				}
			}
		})
	}
}

func TestRedactSecrets_KVForm(t *testing.T) {
	dsn := "host=db user=slink password=topsecret dbname=d"
	msg := "ParseConfig failed for: " + dsn
	got := redactSecrets(msg, dsn)
	if strings.Contains(got, "topsecret") {
		t.Errorf("kv password leaked: %s", got)
	}
}
