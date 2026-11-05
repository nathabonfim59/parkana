package config

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestQuerierDefaults(t *testing.T) {
	c, err := load(t, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	q := c.Querier
	if q.CacheDir != "" || q.CacheSize != 10*GiB || q.SidecarCacheSize != 64*MiB || q.Timeout != 2*time.Minute {
		t.Fatalf("unexpected querier defaults: %+v", q)
	}
	if got := q.CachePath(c.DataDir); got != filepath.Join("data", "cache") {
		t.Fatalf("default cache path = %q, want %q", got, filepath.Join("data", "cache"))
	}
}

func TestQuerierEnvAndFlagPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want Querier
	}{
		{
			name: "env applies when flags absent",
			env: map[string]string{
				EnvName("querier.cache-dir"):          "/env/cache",
				EnvName("querier.cache-size"):         "1GiB",
				EnvName("querier.sidecar-cache-size"): "32MiB",
				EnvName("querier.timeout"):            "30s",
			},
			want: Querier{CacheDir: "/env/cache", CacheSize: GiB, SidecarCacheSize: 32 * MiB, Timeout: 30 * time.Second},
		},
		{
			name: "flags win over env",
			args: []string{"-querier.cache-dir=/flag/cache", "-querier.cache-size=2GiB", "-querier.sidecar-cache-size=128MiB", "-querier.timeout=45s"},
			env: map[string]string{
				EnvName("querier.cache-dir"):          "/env/cache",
				EnvName("querier.cache-size"):         "1GiB",
				EnvName("querier.sidecar-cache-size"): "32MiB",
				EnvName("querier.timeout"):            "30s",
			},
			want: Querier{CacheDir: "/flag/cache", CacheSize: 2 * GiB, SidecarCacheSize: 128 * MiB, Timeout: 45 * time.Second},
		},
		{
			name: "env per flag: only unset flags take env",
			args: []string{"-querier.timeout=45s"},
			env: map[string]string{
				EnvName("querier.cache-size"):         "1GiB",
				EnvName("querier.sidecar-cache-size"): "32MiB",
				EnvName("querier.timeout"):            "30s",
			},
			want: Querier{CacheDir: "", CacheSize: GiB, SidecarCacheSize: 32 * MiB, Timeout: 45 * time.Second},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if c.Querier != tc.want {
				t.Fatalf("got %+v, want %+v", c.Querier, tc.want)
			}
		})
	}
}

func TestQuerierInvalid(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr string // substring the error must name, if set
	}{
		{"timeout zero flag", []string{"-querier.timeout=0"}, nil, ""},
		{"timeout negative flag", []string{"-querier.timeout=-5s"}, nil, ""},
		{"timeout not a duration", []string{"-querier.timeout=abc"}, nil, ""},
		{"cache size zero", []string{"-querier.cache-size=0"}, nil, ""},
		{"cache size negative", []string{"-querier.cache-size=-1GiB"}, nil, ""},
		{"cache size not a size", []string{"-querier.cache-size=huge"}, nil, ""},
		{"sidecar cache size zero", []string{"-querier.sidecar-cache-size=0"}, nil, "querier.sidecar-cache-size"},
		{"sidecar cache size negative", []string{"-querier.sidecar-cache-size=-1MiB"}, nil, "querier.sidecar-cache-size"},
		{"sidecar cache size not a size", []string{"-querier.sidecar-cache-size=huge"}, nil, "querier.sidecar-cache-size"},
		{"timeout invalid env", nil, map[string]string{EnvName("querier.timeout"): "abc"}, ""},
		{"timeout negative env", nil, map[string]string{EnvName("querier.timeout"): "-1s"}, ""},
		{"cache size invalid env", nil, map[string]string{EnvName("querier.cache-size"): "abc"}, ""},
		{"cache size negative env", nil, map[string]string{EnvName("querier.cache-size"): "-1"}, ""},
		{"sidecar cache size invalid env", nil, map[string]string{"PARKANA_QUERIER_SIDECAR_CACHE_SIZE": "abc"}, "PARKANA_QUERIER_SIDECAR_CACHE_SIZE"},
		{"sidecar cache size negative env", nil, map[string]string{EnvName("querier.sidecar-cache-size"): "-1"}, "PARKANA_QUERIER_SIDECAR_CACHE_SIZE"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(t, tc.args, tc.env)
			if err == nil {
				t.Fatal("expected error, got none")
			}
			if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not name %q", err, tc.wantErr)
			}
		})
	}
}

func TestQuerierCachePath(t *testing.T) {
	for _, tc := range []struct {
		name    string
		querier Querier
		dataDir string
		want    string
	}{
		{"empty derives from data dir", Querier{}, "/srv/data", "/srv/data/cache"},
		{"explicit cache dir wins", Querier{CacheDir: "/elsewhere"}, "/srv/data", "/elsewhere"},
		{"custom data dir", Querier{}, "data", filepath.Join("data", "cache")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.querier.CachePath(tc.dataDir); got != tc.want {
				t.Fatalf("CachePath(%q) = %q, want %q", tc.dataDir, got, tc.want)
			}
		})
	}
}
