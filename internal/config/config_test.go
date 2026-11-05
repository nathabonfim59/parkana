package config

import (
	"errors"
	"flag"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"testing"
	"time"
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
