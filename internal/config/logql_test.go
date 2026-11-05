package config

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestTailDefaults(t *testing.T) {
	c, err := load(t, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.LogQL.Tail.BufferEntries != DefaultTailBufferEntries || c.LogQL.Tail.FlushInterval != DefaultTailFlushInterval {
		t.Fatalf("tail defaults = %+v", c.LogQL.Tail)
	}
	if len(c.LogQL.Tail.IngesterURLs) != 0 {
		t.Fatalf("IngesterURLs = %q, want empty", c.LogQL.Tail.IngesterURLs)
	}
	// The default role set includes the ingester, so a same-process querier
	// gets one derived loopback URL.
	if got, want := c.TailIngesterURLs(), []string{"http://127.0.0.1:3100"}; !slices.Equal(got, want) {
		t.Fatalf("TailIngesterURLs() = %q, want %q", got, want)
	}
}

func TestTailEnvAndFlagPrecedence(t *testing.T) {
	env := map[string]string{
		"PARKANA_TAIL_INGESTER_URLS":  "http://env1:3100, https://env2:3100/path",
		"PARKANA_TAIL_BUFFER_ENTRIES": "2048",
		"PARKANA_TAIL_FLUSH_INTERVAL": "250ms",
	}
	c, err := load(t, nil, env)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := c.LogQL.Tail.IngesterURLs, []string{"http://env1:3100", "https://env2:3100/path"}; !slices.Equal(got, want) {
		t.Fatalf("IngesterURLs = %q, want %q", got, want)
	}
	if c.LogQL.Tail.BufferEntries != 2048 || c.LogQL.Tail.FlushInterval != 250*time.Millisecond {
		t.Fatalf("env tail = %+v", c.LogQL.Tail)
	}

	c, err = load(t, []string{
		"-tail.ingester-urls=http://flag:3100",
		"-tail.buffer-entries=32",
		"-tail.flush-interval=5ms",
	}, env)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := c.LogQL.Tail.IngesterURLs, []string{"http://flag:3100"}; !slices.Equal(got, want) {
		t.Fatalf("flags should beat env: IngesterURLs = %q", got)
	}
	if c.LogQL.Tail.BufferEntries != 32 || c.LogQL.Tail.FlushInterval != 5*time.Millisecond {
		t.Fatalf("flags should beat env: tail = %+v", c.LogQL.Tail)
	}
}

func TestTailValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"zero buffer", []string{"-tail.buffer-entries=0"}},
		{"negative buffer", []string{"-tail.buffer-entries=-1"}},
		{"zero flush", []string{"-tail.flush-interval=0s"}},
		{"negative flush", []string{"-tail.flush-interval=-1ms"}},
		{"unsupported scheme", []string{"-tail.ingester-urls=ftp://ingester:3100"}},
		{"missing scheme", []string{"-tail.ingester-urls=ingester:3100"}},
		{"empty host", []string{"-tail.ingester-urls=http://"}},
		{"credentials", []string{"-tail.ingester-urls=http://user:pass@ingester:3100"}},
		{"query string", []string{"-tail.ingester-urls=http://ingester:3100/?token=secret"}},
		{"empty query", []string{"-tail.ingester-urls=http://ingester:3100/?"}},
		{"fragment", []string{"-tail.ingester-urls=http://ingester:3100/#frag"}},
		{"malformed", []string{"-tail.ingester-urls=http://ingester:3100/%zz"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := load(t, tc.args, nil); err == nil {
				t.Fatalf("Load(%q) accepted", tc.args)
			}
		})
	}
}

func TestTailValidationDoesNotEchoSecrets(t *testing.T) {
	for _, args := range [][]string{
		{"-tail.ingester-urls=http://user:s3cr3t@ingester:3100"},
		{"-tail.ingester-urls=http://ingester:3100/?token=s3cr3t"},
		{"-tail.ingester-urls=http://user:s3cr3t@[::1"},
	} {
		_, err := load(t, args, nil)
		if err == nil {
			t.Fatalf("Load(%q) accepted", args)
		}
		if strings.Contains(err.Error(), "s3cr3t") {
			t.Fatalf("error leaks credentials: %v", err)
		}
	}
}

func TestTailValidationAcceptsHTTPURLs(t *testing.T) {
	c, err := load(t, []string{
		"-tail.ingester-urls=http://ingester-a:3100,https://ingester-b.example/base",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(c.LogQL.Tail.IngesterURLs); got != 2 {
		t.Fatalf("IngesterURLs = %q, want 2", c.LogQL.Tail.IngesterURLs)
	}
}

func TestTailIngesterURLDerivation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		roles []Role
		addr  string
		want  []string
	}{
		{"querier only has no same-process ingester", []Role{RoleQuerier}, ":3100", nil},
		{"compactor only has no same-process ingester", []Role{RoleCompactor}, ":3100", nil},
		{"wildcard port", []Role{RoleIngester}, ":3100", []string{"http://127.0.0.1:3100"}},
		{"wildcard IPv4", []Role{RoleIngester}, "0.0.0.0:8080", []string{"http://127.0.0.1:8080"}},
		{"wildcard IPv6", []Role{RoleIngester}, "[::]:8080", []string{"http://127.0.0.1:8080"}},
		{"explicit loopback", []Role{RoleIngester}, "127.0.0.1:3100", []string{"http://127.0.0.1:3100"}},
		{"explicit IPv6 loopback", []Role{RoleIngester}, "[::1]:3100", []string{"http://[::1]:3100"}},
		{"hostname", []Role{RoleIngester}, "localhost:3100", []string{"http://localhost:3100"}},
		{"missing port", []Role{RoleIngester}, "127.0.0.1", []string{"http://127.0.0.1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Config{Roles: tc.roles, HTTPAddr: tc.addr}
			if got := c.TailIngesterURLs(); !slices.Equal(got, tc.want) {
				t.Fatalf("TailIngesterURLs() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTailIngesterURLsConfiguredWin(t *testing.T) {
	c := Config{
		Roles:    []Role{RoleIngester},
		HTTPAddr: ":3100",
		LogQL:    LogQL{Tail: Tail{IngesterURLs: []string{"http://remote:3100"}}},
	}
	got := c.TailIngesterURLs()
	if want := []string{"http://remote:3100"}; !slices.Equal(got, want) {
		t.Fatalf("TailIngesterURLs() = %q, want %q", got, want)
	}
	got[0] = "http://mutated"
	if c.LogQL.Tail.IngesterURLs[0] != "http://remote:3100" {
		t.Fatal("TailIngesterURLs returned the caller's backing slice")
	}
}
