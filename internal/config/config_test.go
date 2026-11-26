package config

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/catalog/catalogtest"
)

func load(t *testing.T, args []string, env map[string]string) (Config, error) {
	t.Helper()
	return Load(args, func(k string) string { return env[k] }, io.Discard)
}

func TestDefaults(t *testing.T) {
	c, err := load(t, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.Roles, AllRoles) || c.HTTPAddr != ":3100" || c.Storage.Backend != "fs" ||
		c.Ingest.MemtableMaxBytes != 256*MiB || c.Ingest.FlushInterval != 5*time.Second ||
		!slices.Equal(c.StreamLabels, []string{"service_name", "job"}) || c.LogLevel != slog.LevelInfo ||
		c.Catalog.Driver != "sqlite" || c.Catalog.DSN != filepath.Join(c.DataDir, "catalog.db") ||
		c.Storage.FSDir != filepath.Join(c.DataDir, "objects") {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestDataPathsDeriveFromDataDir(t *testing.T) {
	for _, tc := range []struct {
		name      string
		args      []string
		env       map[string]string
		wantDSN   string
		wantFSDir string
	}{
		{
			name:      "default data dir",
			wantDSN:   filepath.Join("data", "catalog.db"),
			wantFSDir: filepath.Join("data", "objects"),
		},
		{
			name:      "custom data dir",
			args:      []string{"-data.dir=/srv/parkana"},
			wantDSN:   "/srv/parkana/catalog.db",
			wantFSDir: "/srv/parkana/objects",
		},
		{
			name:      "flags override derived paths",
			args:      []string{"-data.dir=/srv/parkana", "-catalog.dsn=/flags/catalog.db", "-storage.fs.dir=/flags/objects"},
			wantDSN:   "/flags/catalog.db",
			wantFSDir: "/flags/objects",
		},
		{
			name:      "env overrides derived paths",
			args:      []string{"-data.dir=/srv/parkana"},
			env:       map[string]string{"PARKANA_CATALOG_DSN": "/env/catalog.db", "PARKANA_STORAGE_FS_DIR": "/env/objects"},
			wantDSN:   "/env/catalog.db",
			wantFSDir: "/env/objects",
		},
		{
			name:      "empty flags resolve under data dir",
			args:      []string{"-data.dir=/srv/parkana", "-catalog.dsn=", "-storage.fs.dir="},
			wantDSN:   "/srv/parkana/catalog.db",
			wantFSDir: "/srv/parkana/objects",
		},
		{
			name:      "empty env resolves under data dir",
			args:      []string{"-data.dir=/srv/parkana"},
			env:       map[string]string{"PARKANA_CATALOG_DSN": "", "PARKANA_STORAGE_FS_DIR": ""},
			wantDSN:   "/srv/parkana/catalog.db",
			wantFSDir: "/srv/parkana/objects",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if c.Catalog.DSN != tc.wantDSN {
				t.Errorf("Catalog.DSN = %q, want %q", c.Catalog.DSN, tc.wantDSN)
			}
			if c.Storage.FSDir != tc.wantFSDir {
				t.Errorf("Storage.FSDir = %q, want %q", c.Storage.FSDir, tc.wantFSDir)
			}
		})
	}
}

func TestEnvAndFlagPrecedence(t *testing.T) {
	env := map[string]string{
		"PARKANA_HTTP_ADDR":             ":9999",
		"PARKANA_ROLES":                 "querier,ingester",
		"PARKANA_INGEST_MEMTABLE_MAX":   "64MiB",
		"PARKANA_STORAGE_S3_SECRET_KEY": "s3cr3t",
		"PARKANA_INGEST_UPLOAD_MAX_AGE": "1m",
		"PARKANA_LOG_LEVEL":             "debug",
	}
	c, err := load(t, []string{"-http.addr=:1234"}, env)
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":1234" {
		t.Errorf("flag should beat env: %s", c.HTTPAddr)
	}
	if !slices.Equal(c.Roles, []Role{RoleIngester, RoleQuerier}) {
		t.Errorf("roles = %v", c.Roles)
	}
	if c.Ingest.MemtableMaxBytes != 64*MiB || c.Ingest.UploadMaxAge != time.Minute || c.Storage.S3.SecretKey != "s3cr3t" || c.LogLevel != slog.LevelDebug {
		t.Errorf("env not applied: %+v", c)
	}
}

func TestS3CreateBucketOption(t *testing.T) {
	base := []string{"-storage.backend=s3", "-storage.s3.endpoint=127.0.0.1:9000", "-storage.s3.bucket=parkana"}
	for _, tc := range []struct {
		name string
		args []string
		env  map[string]string
		want bool
	}{
		{"default off", base, nil, false},
		{"flag on", slices.Concat(base, []string{"-storage.s3.create-bucket"}), nil, true},
		{"flag explicit off", slices.Concat(base, []string{"-storage.s3.create-bucket=false"}), nil, false},
		{"env on", base, map[string]string{"PARKANA_STORAGE_S3_CREATE_BUCKET": "true"}, true},
		{"flag off beats env on", slices.Concat(base, []string{"-storage.s3.create-bucket=false"}), map[string]string{"PARKANA_STORAGE_S3_CREATE_BUCKET": "true"}, false},
		{"flag on beats env off", slices.Concat(base, []string{"-storage.s3.create-bucket"}), map[string]string{"PARKANA_STORAGE_S3_CREATE_BUCKET": "false"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, tc.env)
			if err != nil {
				t.Fatal(err)
			}
			if c.Storage.S3.CreateBucket != tc.want {
				t.Errorf("Storage.S3.CreateBucket = %t, want %t", c.Storage.S3.CreateBucket, tc.want)
			}
		})
	}
}

func TestInvalid(t *testing.T) {
	for _, args := range [][]string{
		{"-roles=router"},
		{"-roles="},
		{"-storage.backend=gcs"},
		{"-storage.backend=s3"},
		{"-storage.backend=s3", "-storage.s3.endpoint=127.0.0.1:9000", "-storage.s3.bucket=parkana", "-storage.s3.create-bucket=maybe"},
		{"-catalog.driver=pg"},
		{"-catalog.driver=postgres"},
		{"-log.format=xml"},
		{"-ingest.memtable-max=lots"},
		{"-multi-tenant"},
		{"extra"},
	} {
		if _, err := load(t, args, nil); err == nil {
			t.Errorf("Load(%q) accepted", args)
		}
	}
	if _, err := load(t, []string{"-h"}, nil); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("-h: %v", err)
	}
}

func TestByteSize(t *testing.T) {
	for in, want := range map[string]ByteSize{"512": 512, "64KiB": 64 << 10, "2GiB": 2 << 30, "100MB": 100e6, "7 MiB": 7 << 20} {
		var b ByteSize
		if err := b.Set(in); err != nil || b != want {
			t.Errorf("Set(%q) = %d, %v; want %d", in, b, err, want)
		}
	}
	for b, want := range map[ByteSize]string{256 * MiB: "256MiB", 2 * GiB: "2GiB", 1536 * KiB: "1536KiB", 100: "100"} {
		if got := b.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", int64(b), got, want)
		}
	}
}

// TestCompactorOrphanFlags covers the orphan GC knobs: the 1h grace and 0.1
// safety-stop fraction defaults, the env fallback, 0 grace turning orphan
// collection off, and the rejected values: a negative grace, a grace below
// the 1h the ingester's 30-minute publish window relies on, and a fraction
// outside (0, 1].
func TestCompactorOrphanFlags(t *testing.T) {
	for _, tc := range []struct {
		name         string
		args         []string
		env          map[string]string
		wantGrace    time.Duration
		wantFraction float64
		wantErr      string // substring; empty means Load succeeds
	}{
		{name: "defaults", wantGrace: time.Hour, wantFraction: 0.1},
		{name: "grace zero disables", args: []string{"-compactor.orphan-grace=0"}, wantGrace: 0, wantFraction: 0.1},
		{name: "grace exactly 1h", args: []string{"-compactor.orphan-grace=1h"}, wantGrace: time.Hour, wantFraction: 0.1},
		{name: "grace raised", args: []string{"-compactor.orphan-grace=6h"}, wantGrace: 6 * time.Hour, wantFraction: 0.1},
		{name: "fraction one turns the stop off", args: []string{"-compactor.orphan-max-fraction=1"}, wantGrace: time.Hour, wantFraction: 1},
		{name: "fraction small", args: []string{"-compactor.orphan-max-fraction=0.01"}, wantGrace: time.Hour, wantFraction: 0.01},
		{
			name:         "env fallback",
			env:          map[string]string{"PARKANA_COMPACTOR_ORPHAN_GRACE": "2h", "PARKANA_COMPACTOR_ORPHAN_MAX_FRACTION": "0.25"},
			wantGrace:    2 * time.Hour,
			wantFraction: 0.25,
		},
		{
			name:         "flag beats env",
			args:         []string{"-compactor.orphan-grace=3h", "-compactor.orphan-max-fraction=0.5"},
			env:          map[string]string{"PARKANA_COMPACTOR_ORPHAN_GRACE": "2h", "PARKANA_COMPACTOR_ORPHAN_MAX_FRACTION": "0.25"},
			wantGrace:    3 * time.Hour,
			wantFraction: 0.5,
		},
		{name: "negative grace", args: []string{"-compactor.orphan-grace=-1s"}, wantErr: "-compactor.orphan-grace"},
		{name: "grace below 1h", args: []string{"-compactor.orphan-grace=30m"}, wantErr: "-compactor.orphan-grace"},
		{name: "grace just below 1h", args: []string{"-compactor.orphan-grace=59m59s"}, wantErr: "-compactor.orphan-grace"},
		{name: "env grace below 1h", env: map[string]string{"PARKANA_COMPACTOR_ORPHAN_GRACE": "30m"}, wantErr: "-compactor.orphan-grace"},
		{name: "fraction zero", args: []string{"-compactor.orphan-max-fraction=0"}, wantErr: "-compactor.orphan-max-fraction"},
		{name: "fraction negative", args: []string{"-compactor.orphan-max-fraction=-0.1"}, wantErr: "-compactor.orphan-max-fraction"},
		{name: "fraction above one", args: []string{"-compactor.orphan-max-fraction=1.5"}, wantErr: "-compactor.orphan-max-fraction"},
		{name: "fraction NaN", args: []string{"-compactor.orphan-max-fraction=NaN"}, wantErr: "-compactor.orphan-max-fraction"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, tc.env)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Load(%q) err = %v, want one naming %q", tc.args, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.Compactor.OrphanGrace != tc.wantGrace {
				t.Errorf("Compactor.OrphanGrace = %s, want %s", c.Compactor.OrphanGrace, tc.wantGrace)
			}
			if c.Compactor.OrphanMaxFraction != tc.wantFraction {
				t.Errorf("Compactor.OrphanMaxFraction = %v, want %v", c.Compactor.OrphanMaxFraction, tc.wantFraction)
			}
		})
	}
}

// TestCompactorOrphanHelp ties the orphan flags' help text to the constants
// it quotes, so changing MinOrphanGrace or OrphanMinCandidates cannot leave
// the help (and the generated flag reference) stale.
func TestCompactorOrphanHelp(t *testing.T) {
	if got := shortDuration(MinOrphanGrace); got != "1h" {
		t.Fatalf("shortDuration(MinOrphanGrace) = %q, want 1h; update the docs that quote the minimum grace", got)
	}
	if OrphanMinCandidates != 10 {
		t.Fatalf("OrphanMinCandidates = %d, want 10; update the docs that quote the safety-stop floor", OrphanMinCandidates)
	}
	usage := make(map[string]string)
	for _, f := range flags() {
		usage[f.Name] = f.Usage
	}
	for name, want := range map[string]string{
		"compactor.orphan-grace":        "at least " + shortDuration(MinOrphanGrace),
		"compactor.orphan-max-fraction": fmt.Sprintf("whose %d or more candidates", OrphanMinCandidates),
	} {
		if !strings.Contains(usage[name], want) {
			t.Errorf("-%s help = %q, want it to contain %q", name, usage[name], want)
		}
	}
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{time.Hour, "1h"},
		{90 * time.Minute, "1h30m"},
		{2*time.Hour + 30*time.Second, "2h0m30s"},
		{30 * time.Minute, "30m"},
		{45 * time.Second, "45s"},
	} {
		if got := shortDuration(tc.d); got != tc.want {
			t.Errorf("shortDuration(%s) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

func TestCatalogDriver(t *testing.T) {
	const pgURL = "postgres://parkana@db:5432/parkana?sslmode=disable"
	for _, tc := range []struct {
		name       string
		args       []string
		env        map[string]string
		wantDriver string
		wantDSN    string
		wantErr    string // substring; empty means Load succeeds
	}{
		{
			name:       "sqlite default unchanged",
			args:       []string{"-data.dir=/srv/parkana"},
			wantDriver: "sqlite",
			wantDSN:    "/srv/parkana/catalog.db",
		},
		{
			name:       "memory gets no derived DSN",
			args:       []string{"-data.dir=/srv/parkana", "-catalog.driver=memory"},
			wantDriver: "memory",
		},
		{
			name:       "memory ignores a DSN",
			args:       []string{"-catalog.driver=memory", "-catalog.dsn=/whatever"},
			wantDriver: "memory",
			wantDSN:    "/whatever",
		},
		{
			name:       "postgres from flags",
			args:       []string{"-catalog.driver=postgres", "-catalog.dsn=" + pgURL},
			wantDriver: "postgres",
			wantDSN:    pgURL,
		},
		{
			name:       "postgres from env",
			env:        map[string]string{"PARKANA_CATALOG_DRIVER": "postgres", "PARKANA_CATALOG_DSN": pgURL},
			wantDriver: "postgres",
			wantDSN:    pgURL,
		},
		{
			name:       "postgres keyword DSN from env",
			env:        map[string]string{"PARKANA_CATALOG_DRIVER": "postgres", "PARKANA_CATALOG_DSN": "host=db dbname=parkana"},
			wantDriver: "postgres",
			wantDSN:    "host=db dbname=parkana",
		},
		{
			name:       "flags beat env",
			args:       []string{"-catalog.driver=postgres", "-catalog.dsn=" + pgURL},
			env:        map[string]string{"PARKANA_CATALOG_DRIVER": "sqlite", "PARKANA_CATALOG_DSN": "/env/catalog.db"},
			wantDriver: "postgres",
			wantDSN:    pgURL,
		},
		{
			name:       "env driver, flag DSN",
			args:       []string{"-catalog.dsn=" + pgURL},
			env:        map[string]string{"PARKANA_CATALOG_DRIVER": "postgres"},
			wantDriver: "postgres",
			wantDSN:    pgURL,
		},
		{
			name:    "postgres without DSN",
			args:    []string{"-data.dir=/srv/parkana", "-catalog.driver=postgres"},
			wantErr: "-catalog.dsn is required for -catalog.driver=postgres",
		},
		{
			name:    "postgres with empty DSN flag",
			args:    []string{"-catalog.driver=postgres", "-catalog.dsn="},
			wantErr: "-catalog.dsn is required",
		},
		{
			name:    "postgres with blank DSN",
			args:    []string{"-catalog.driver=postgres", "-catalog.dsn=  "},
			wantErr: "-catalog.dsn is required",
		},
		{
			name:    "postgres from env without DSN",
			env:     map[string]string{"PARKANA_CATALOG_DRIVER": "postgres", "PARKANA_CATALOG_DSN": ""},
			wantErr: "-catalog.dsn is required",
		},
		{
			name:    "unknown driver lists postgres",
			args:    []string{"-catalog.driver=mysql"},
			wantErr: "-catalog.driver must be memory, sqlite or postgres",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := load(t, tc.args, tc.env)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Load error = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.Catalog.Driver != tc.wantDriver || c.Catalog.DSN != tc.wantDSN {
				t.Errorf("Catalog = %q %q, want %q %q", c.Catalog.Driver, c.Catalog.DSN, tc.wantDriver, tc.wantDSN)
			}
		})
	}
}

// TestCatalogPasswordNeverPrinted feeds a password through every path that
// can echo a configuration value (flag parse errors, unexpected arguments,
// env errors, validation errors) and checks that neither the error nor what
// Load writes to its output contains it, and that formatting or logging a
// loaded Config does not either.
func TestCatalogPasswordNeverPrinted(t *testing.T) {
	const secret = "Sup3rS3cret"
	urlDSN := "postgres://parkana:" + secret + "@db:5432/parkana"
	for _, tc := range []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr bool
	}{
		{name: "URL DSN flag", args: []string{"-catalog.driver=postgres", "-catalog.dsn=" + urlDSN}},
		{name: "URL DSN env", env: map[string]string{"PARKANA_CATALOG_DRIVER": "postgres", "PARKANA_CATALOG_DSN": urlDSN}},
		{name: "postgresql scheme", args: []string{"-catalog.driver=postgres", "-catalog.dsn=postgresql://parkana:" + secret + "@db/parkana"}},
		{name: "password with @", args: []string{"-catalog.driver=postgres", "-catalog.dsn=postgres://parkana:p@ss" + secret + "@db/parkana"}},
		{name: "password with %40", args: []string{"-catalog.driver=postgres", "-catalog.dsn=postgres://parkana:p%40ss" + secret + "@db/parkana"}},
		{name: "env DSN with an invalid other env", env: map[string]string{"PARKANA_CATALOG_DRIVER": "postgres", "PARKANA_CATALOG_DSN": urlDSN, "PARKANA_INGEST_MEMTABLE_MAX": "lots"}, wantErr: true},
		{name: "env DSN with an invalid flag", args: []string{"-log.format=xml"}, env: map[string]string{"PARKANA_CATALOG_DRIVER": "postgres", "PARKANA_CATALOG_DSN": urlDSN}, wantErr: true},
		{name: "bad catalog.dsn flag syntax", args: []string{"--=catalog.dsn=" + urlDSN}, wantErr: true},
		{name: "empty user", args: []string{"-catalog.driver=postgres", "-catalog.dsn=postgres://:" + secret + "@db/parkana"}},
		{name: "query password", args: []string{"-catalog.driver=postgres", "-catalog.dsn=postgres://db/parkana?sslmode=disable&password=" + secret}},
		{name: "keyword DSN", args: []string{"-catalog.driver=postgres", "-catalog.dsn=host=db password=" + secret + " dbname=parkana"}},
		{name: "keyword DSN quoted", env: map[string]string{"PARKANA_CATALOG_DRIVER": "postgres", "PARKANA_CATALOG_DSN": "host=db password = 'a b\\' " + secret + "' dbname=parkana"}},
		{name: "keyword DSN upper case", args: []string{"-catalog.driver=postgres", "-catalog.dsn=host=db PASSWORD=" + secret}},
		{name: "bad flag syntax", args: []string{"---catalog.dsn=" + urlDSN}, wantErr: true},
		{name: "DSN as positional argument", args: []string{"-catalog.driver=postgres", urlDSN}, wantErr: true},
		{name: "DSN in driver flag", args: []string{"-catalog.driver=" + urlDSN}, wantErr: true},
		{name: "DSN in driver env", env: map[string]string{"PARKANA_CATALOG_DRIVER": urlDSN}, wantErr: true},
		{name: "keyword DSN in driver flag", args: []string{"-catalog.driver=host=db password=" + secret}, wantErr: true},
		{name: "DSN in bool flag", args: []string{"-multi-tenant=" + urlDSN}, wantErr: true},
		{name: "DSN in sized env", env: map[string]string{"PARKANA_INGEST_MEMTABLE_MAX": urlDSN}, wantErr: true},
		{name: "DSN in log level", args: []string{"-log.level=" + urlDSN}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			c, err := Load(tc.args, func(k string) string { return tc.env[k] }, &out)
			if tc.wantErr {
				if err == nil {
					t.Fatal("Load accepted the configuration")
				}
				if strings.Contains(err.Error(), secret) {
					t.Errorf("error shows the password: %v", err)
				}
			} else if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if strings.Contains(out.String(), secret) {
				t.Errorf("output shows the password:\n%s", out.String())
			}
			if tc.wantErr {
				return
			}

			// A loaded config holds the password (it must reach the
			// driver) but never prints it.
			if !strings.Contains(c.Catalog.DSN, secret) {
				t.Fatalf("Catalog.DSN = %q lost the password", c.Catalog.DSN)
			}
			var logs bytes.Buffer
			slog.New(slog.NewTextHandler(&logs, nil)).Info("text", "catalog", c.Catalog, "cfg", c)
			slog.New(slog.NewJSONHandler(&logs, nil)).Info("json", "catalog", c.Catalog)
			for name, s := range map[string]string{
				"%v":   fmt.Sprintf("%v", c),
				"%+v":  fmt.Sprintf("%+v", c),
				"%s":   c.Catalog.String(),
				"slog": logs.String(),
			} {
				if strings.Contains(s, secret) {
					t.Errorf("%s shows the password: %s", name, s)
				}
			}
		})
	}
	t.Run("help keeps ErrHelp", func(t *testing.T) {
		var out bytes.Buffer
		_, err := Load([]string{"-catalog.dsn=" + urlDSN, "-h"}, func(k string) string {
			return map[string]string{"PARKANA_CATALOG_DSN": urlDSN}[k]
		}, &out)
		if !errors.Is(err, flag.ErrHelp) {
			t.Errorf("-h: %v, want flag.ErrHelp", err)
		}
		if !strings.Contains(out.String(), "-catalog.dsn") || strings.Contains(out.String(), secret) {
			t.Errorf("usage missing or shows the password:\n%s", out.String())
		}
	})
	t.Run("Validate", func(t *testing.T) {
		c, err := load(t, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		c.Catalog.Driver = urlDSN
		err = c.Validate()
		if err == nil || strings.Contains(err.Error(), secret) || !strings.Contains(err.Error(), "-catalog.driver must be") {
			t.Errorf("Validate error = %v, want a redacted driver error", err)
		}
	})
}

func TestRedactSecrets(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		// Nothing to hide: unchanged.
		{"", ""},
		{"/srv/parkana/catalog.db", "/srv/parkana/catalog.db"},
		{"file:catalog.db?_pragma=busy_timeout(5000)", "file:catalog.db?_pragma=busy_timeout(5000)"},
		{"postgres://parkana@db:5432/parkana", "postgres://parkana@db:5432/parkana"},
		{"postgres://db/parkana?sslmode=disable", "postgres://db/parkana?sslmode=disable"},
		{"host=db user=parkana dbname=parkana", "host=db user=parkana dbname=parkana"},
		{"prefer the env var when it holds a password)", "prefer the env var when it holds a password)"},
		{"passwordless=yes", "passwordless=yes"},
		// URL userinfo.
		{"postgres://u:pw@db/p", "postgres://u:xxxxx@db/p"},
		{"postgresql://u:pw@db:5432/p?sslmode=require", "postgresql://u:xxxxx@db:5432/p?sslmode=require"},
		{"postgres://:pw@db/p", "postgres://:xxxxx@db/p"},
		{"postgres://u:@db/p", "postgres://u:xxxxx@db/p"},
		{"postgres://u:p@ss@db/p", "postgres://u:xxxxx@db/p"},
		{"postgres://u:p%40ss@db/p", "postgres://u:xxxxx@db/p"},
		{`config: unexpected arguments ["postgres://u:pw@db/p" "x"]`, `config: unexpected arguments ["postgres://u:xxxxx@db/p" "x"]`},
		{"a postgres://u:pw@h/p and mysql://v:pw2@h/q", "a postgres://u:xxxxx@h/p and mysql://v:xxxxx@h/q"},
		// Keyword values and query parameters.
		{"host=db password=pw dbname=p", "host=db password=xxxxx dbname=p"},
		{"host=db PassWord = pw", "host=db PassWord = xxxxx"},
		{"host=db password='a b' dbname=p", "host=db password=xxxxx dbname=p"},
		{`host=db password='a\' b' dbname=p`, "host=db password=xxxxx dbname=p"},
		{"host=db password='unterminated rest", "host=db password=xxxxx"},
		{"postgres://db/p?password=pw&sslmode=disable", "postgres://db/p?password=xxxxx"},
		{"postgres://u:pw@db/p?password=pw2", "postgres://u:xxxxx@db/p?password=xxxxx"},
	} {
		if got := redactSecrets(tc.in); got != tc.want {
			t.Errorf("redactSecrets(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestRedactSecretsSharedCases runs the catalog's DSN redaction cases
// (catalogtest.RedactionCases) through redactSecrets, alone and inside a
// flag-style error message, so the config and catalog redactors cannot drift
// apart.
func TestRedactSecretsSharedCases(t *testing.T) {
	for _, tc := range catalogtest.RedactionCases {
		t.Run(tc.Name, func(t *testing.T) {
			for _, in := range []string{
				tc.DSN,
				`invalid value "` + tc.DSN + `" for flag -catalog.dsn: bad`,
			} {
				got := redactSecrets(in)
				if secret, ok := tc.Leak(got); ok {
					t.Errorf("redactSecrets(%q) = %q, leaks %q", in, got, secret)
				}
			}
		})
	}
}

func TestRedactErrKeepsChain(t *testing.T) {
	plain := fmt.Errorf("config: %w", flag.ErrHelp)
	if got := redactErr(plain); got != plain {
		t.Errorf("redactErr changed an error without a password: %v", got)
	}
	if redactErr(nil) != nil {
		t.Error("redactErr(nil) != nil")
	}
	leaky := fmt.Errorf("open postgres://u:pw@db/p: %w", flag.ErrHelp)
	got := redactErr(leaky)
	if got.Error() != "open postgres://u:xxxxx@db/p: flag: help requested" {
		t.Errorf("redactErr = %q", got)
	}
	if errors.Is(got, flag.ErrHelp) {
		t.Error("a redacted error must not unwrap to the original, which still holds the password")
	}
}
