package config

import (
	"strings"
	"testing"
	"time"
)

func TestSQLDefaults(t *testing.T) {
	c, err := load(t, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := SQL{
		MaxRows:        10000,
		DefaultLimit:   1000,
		MaxResultBytes: 8 * MiB,
		Timeout:        30 * time.Second,
		MemoryLimit:    GiB,
		Threads:        1,
		MaxConcurrent:  2,
	}
	if c.SQL != want {
		t.Fatalf("unexpected SQL defaults: %+v", c.SQL)
	}
}

func TestSQLEnvAndFlagPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want SQL
	}{
		{
			name: "env applies when flags absent",
			env: map[string]string{
				EnvName("sql.max-rows"):         "5000",
				EnvName("sql.default-limit"):    "500",
				EnvName("sql.max-result-bytes"): "4MiB",
				EnvName("sql.timeout"):          "10s",
				EnvName("sql.memory-limit"):     "512MiB",
				EnvName("sql.threads"):          "4",
				EnvName("sql.max-concurrent"):   "8",
			},
			want: SQL{MaxRows: 5000, DefaultLimit: 500, MaxResultBytes: 4 * MiB, Timeout: 10 * time.Second, MemoryLimit: 512 * MiB, Threads: 4, MaxConcurrent: 8},
		},
		{
			name: "flags win over env",
			args: []string{
				"-sql.max-rows=2000", "-sql.default-limit=200", "-sql.max-result-bytes=16MiB",
				"-sql.timeout=45s", "-sql.memory-limit=2GiB", "-sql.threads=8", "-sql.max-concurrent=4",
			},
			env: map[string]string{
				EnvName("sql.max-rows"):         "5000",
				EnvName("sql.default-limit"):    "500",
				EnvName("sql.max-result-bytes"): "4MiB",
				EnvName("sql.timeout"):          "10s",
				EnvName("sql.memory-limit"):     "512MiB",
				EnvName("sql.threads"):          "4",
				EnvName("sql.max-concurrent"):   "8",
			},
			want: SQL{MaxRows: 2000, DefaultLimit: 200, MaxResultBytes: 16 * MiB, Timeout: 45 * time.Second, MemoryLimit: 2 * GiB, Threads: 8, MaxConcurrent: 4},
		},
		{
			name: "env per flag: only unset flags take env",
			args: []string{"-sql.timeout=45s"},
			env: map[string]string{
				EnvName("sql.max-rows"):         "5000",
				EnvName("sql.max-result-bytes"): "4MiB",
				EnvName("sql.timeout"):          "10s",
				EnvName("sql.memory-limit"):     "512MiB",
			},
			want: SQL{MaxRows: 5000, DefaultLimit: 1000, MaxResultBytes: 4 * MiB, Timeout: 45 * time.Second, MemoryLimit: 512 * MiB, Threads: 1, MaxConcurrent: 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if c.SQL != tc.want {
				t.Fatalf("got %+v, want %+v", c.SQL, tc.want)
			}
		})
	}
}

func TestSQLInvalid(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr string // substring the error must name, if set
	}{
		{"max rows zero", []string{"-sql.max-rows=0"}, nil, "sql.max-rows"},
		{"max rows negative", []string{"-sql.max-rows=-1"}, nil, "sql.max-rows"},
		{"max rows not an int", []string{"-sql.max-rows=many"}, nil, ""},
		{"default limit zero", []string{"-sql.default-limit=0"}, nil, "sql.default-limit"},
		{"default limit negative", []string{"-sql.default-limit=-1"}, nil, "sql.default-limit"},
		{"default limit above max rows", []string{"-sql.max-rows=100", "-sql.default-limit=200"}, nil, "sql.default-limit"},
		{"default limit above default max rows", []string{"-sql.default-limit=20000"}, nil, "sql.default-limit"},
		{"max result bytes zero", []string{"-sql.max-result-bytes=0"}, nil, "sql.max-result-bytes"},
		{"max result bytes negative", []string{"-sql.max-result-bytes=-1MiB"}, nil, "sql.max-result-bytes"},
		{"max result bytes not a size", []string{"-sql.max-result-bytes=huge"}, nil, "sql.max-result-bytes"},
		{"timeout zero", []string{"-sql.timeout=0"}, nil, "sql.timeout"},
		{"timeout negative", []string{"-sql.timeout=-5s"}, nil, "sql.timeout"},
		{"timeout not a duration", []string{"-sql.timeout=abc"}, nil, ""},
		{"memory limit zero", []string{"-sql.memory-limit=0"}, nil, "sql.memory-limit"},
		{"memory limit negative", []string{"-sql.memory-limit=-1GiB"}, nil, "sql.memory-limit"},
		{"memory limit not a size", []string{"-sql.memory-limit=huge"}, nil, "sql.memory-limit"},
		{"threads zero", []string{"-sql.threads=0"}, nil, "sql.threads"},
		{"threads negative", []string{"-sql.threads=-2"}, nil, "sql.threads"},
		{"threads not an int", []string{"-sql.threads=many"}, nil, ""},
		{"max concurrent zero", []string{"-sql.max-concurrent=0"}, nil, "sql.max-concurrent"},
		{"max concurrent negative", []string{"-sql.max-concurrent=-1"}, nil, "sql.max-concurrent"},
		{"max rows invalid env", nil, map[string]string{EnvName("sql.max-rows"): "abc"}, "PARKANA_SQL_MAX_ROWS"},
		{"max rows zero env", nil, map[string]string{EnvName("sql.max-rows"): "0"}, "sql.max-rows"},
		{"default limit above max env", nil, map[string]string{EnvName("sql.default-limit"): "20000"}, "sql.default-limit"},
		{"max result bytes invalid env", nil, map[string]string{EnvName("sql.max-result-bytes"): "abc"}, "PARKANA_SQL_MAX_RESULT_BYTES"},
		{"timeout invalid env", nil, map[string]string{EnvName("sql.timeout"): "abc"}, "PARKANA_SQL_TIMEOUT"},
		{"timeout negative env", nil, map[string]string{EnvName("sql.timeout"): "-1s"}, "sql.timeout"},
		{"memory limit invalid env", nil, map[string]string{EnvName("sql.memory-limit"): "abc"}, "PARKANA_SQL_MEMORY_LIMIT"},
		{"memory limit negative env", nil, map[string]string{EnvName("sql.memory-limit"): "-1"}, "PARKANA_SQL_MEMORY_LIMIT"},
		{"threads invalid env", nil, map[string]string{EnvName("sql.threads"): "abc"}, "PARKANA_SQL_THREADS"},
		{"max concurrent zero env", nil, map[string]string{EnvName("sql.max-concurrent"): "0"}, "sql.max-concurrent"},
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
