package app_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// loadConfig parses args with no environment fallback.
func loadConfig(t *testing.T, args ...string) config.Config {
	t.Helper()
	cfg, err := config.Load(args, func(string) string { return "" }, io.Discard)
	if err != nil {
		t.Fatalf("config.Load(%q): %v", args, err)
	}
	return cfg
}

func TestDefaultCatalogIsSQLite(t *testing.T) {
	cfg := loadConfig(t)
	if cfg.Catalog.Driver != "sqlite" {
		t.Fatalf("default catalog driver = %q, want sqlite", cfg.Catalog.Driver)
	}
}

func TestOpenCatalogSQLiteCreatesDirectoryAndPersists(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "nested", "data")
	cfg := loadConfig(t, "-data.dir="+dataDir)
	if want := filepath.Join(dataDir, "catalog.db"); cfg.Catalog.DSN != want {
		t.Fatalf("Catalog.DSN = %q, want derived %q", cfg.Catalog.DSN, want)
	}

	cat, err := app.OpenCatalog(cfg)
	if err != nil {
		t.Fatalf("OpenCatalog: %v", err)
	}
	t.Cleanup(func() { cat.Close() })
	if _, err := os.Stat(cfg.Catalog.DSN); err != nil {
		t.Fatalf("catalog file not created: %v", err)
	}

	ctx := t.Context()
	minTS := time.Date(2026, 11, 3, 12, 0, 0, 123456000, time.UTC)
	want := catalog.File{
		Tenant:      tenant.Default,
		Stream:      "checkout",
		Key:         objstore.NewDataKey(tenant.Default, "checkout", minTS),
		MinTS:       minTS,
		MaxTS:       minTS.Add(2 * time.Second),
		Rows:        3,
		Bytes:       42,
		LabelValues: map[string][]string{"service_name": {"checkout"}},
	}
	if err := cat.AddFiles(ctx, want); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	if err := cat.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cat, err = app.OpenCatalog(cfg)
	if err != nil {
		t.Fatalf("reopen OpenCatalog: %v", err)
	}
	var got []catalog.File
	for f, err := range cat.ListFiles(ctx, catalog.Filter{Tenant: tenant.Default, Stream: "checkout"}) {
		if err != nil {
			t.Fatalf("ListFiles: %v", err)
		}
		got = append(got, f)
	}
	if len(got) != 1 {
		t.Fatalf("reopened catalog has %d files, want 1", len(got))
	}
	if got[0].Key != want.Key || got[0].Rows != want.Rows || got[0].Bytes != want.Bytes {
		t.Errorf("reopened file = %+v, want key %q rows %d bytes %d", got[0], want.Key, want.Rows, want.Bytes)
	}
	if !got[0].MinTS.Equal(want.MinTS) || !got[0].MaxTS.Equal(want.MaxTS) {
		t.Errorf("reopened timestamps = [%v, %v], want [%v, %v]", got[0].MinTS, got[0].MaxTS, want.MinTS, want.MaxTS)
	}
}

func TestOpenCatalogMemory(t *testing.T) {
	cfg := loadConfig(t, "-catalog.driver=memory")
	cat, err := app.OpenCatalog(cfg)
	if err != nil {
		t.Fatalf("OpenCatalog: %v", err)
	}
	t.Cleanup(func() { cat.Close() })
	if _, ok := cat.(*catalog.Memory); !ok {
		t.Fatalf("OpenCatalog returned %T, want *catalog.Memory", cat)
	}
	minTS := time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	file := catalog.File{
		Tenant: tenant.Default,
		Stream: "checkout",
		Key:    objstore.NewDataKey(tenant.Default, "checkout", minTS),
		MinTS:  minTS,
		MaxTS:  minTS.Add(time.Minute),
	}
	if err := cat.AddFiles(t.Context(), file); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
}

func TestOpenStoreFS(t *testing.T) {
	dir := t.TempDir()
	cfg := loadConfig(t, "-storage.backend=fs", "-storage.fs.dir="+dir)
	store, err := app.OpenStore(cfg)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	ctx := t.Context()
	key := objstore.NewDataKey(tenant.Default, "checkout", time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC))
	if err := store.Put(ctx, key, strings.NewReader("rows"), 4); err != nil {
		t.Fatalf("Put: %v", err)
	}
	r, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "rows" {
		t.Errorf("Get = %q, want rows", got)
	}
}

func TestOpenStoreS3ConstructionDoesNotTouchNetwork(t *testing.T) {
	// Any request fails the test through the handler, so a regression that
	// starts contacting the endpoint is reported instead of hanging.
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		t.Errorf("S3 constructor made a request: %s %s", r.Method, r.URL)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	cfg := loadConfig(t,
		"-storage.backend=s3",
		"-storage.s3.endpoint="+strings.TrimPrefix(srv.URL, "http://"),
		"-storage.s3.bucket=parkana",
		"-storage.s3.access-key=test",
		"-storage.s3.secret-key=test",
		"-storage.s3.insecure",
	)
	store, err := app.OpenStore(cfg)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	if store == nil {
		t.Fatal("OpenStore returned a nil store")
	}
	if err := store.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if n := requests.Load(); n != 0 {
		t.Errorf("S3 constructor made %d network requests, want 0", n)
	}
}

func TestOpenStoreS3CreateBucketOptIn(t *testing.T) {
	srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(srv.Close)

	cfg := loadConfig(t,
		"-storage.backend=s3",
		"-storage.s3.endpoint="+strings.TrimPrefix(srv.URL, "http://"),
		"-storage.s3.bucket=parkana",
		"-storage.s3.access-key=test",
		"-storage.s3.secret-key=test",
		"-storage.s3.insecure",
		"-storage.s3.create-bucket",
	)
	if !cfg.Storage.S3.CreateBucket {
		t.Fatal("config did not enable S3 bucket creation")
	}
	store, err := app.OpenStore(cfg)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	// The fake bucket starts empty, so the round trip only works when
	// OpenStore created the configured bucket.
	ctx := t.Context()
	key := objstore.NewDataKey(tenant.Default, "checkout", time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC))
	if err := store.Put(ctx, key, strings.NewReader("rows"), 4); err != nil {
		t.Fatalf("Put: %v", err)
	}
	r, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "rows" {
		t.Errorf("Get = %q, want rows", got)
	}
}

func TestOpenStoreErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  config.Config
		want string
	}{
		{
			name: "unknown backend",
			cfg:  config.Config{Storage: config.Storage{Backend: "gcs"}},
			want: "unknown storage backend",
		},
		{
			name: "s3 without endpoint",
			cfg:  config.Config{Storage: config.Storage{Backend: "s3", S3: config.S3{Bucket: "parkana"}}},
			want: "endpoint is required",
		},
		{
			name: "s3 without bucket",
			cfg:  config.Config{Storage: config.Storage{Backend: "s3", S3: config.S3{Endpoint: "192.0.2.1:9000"}}},
			want: "bucket is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := app.OpenStore(tc.cfg)
			if err == nil {
				store.Close()
				t.Fatal("OpenStore accepted invalid config")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("OpenStore error = %q, want it to contain %q", err, tc.want)
			}
			if store != nil {
				t.Error("OpenStore returned a store with an error")
			}
		})
	}
}

func TestOpenCatalogErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  config.Config
		want string
	}{
		{
			name: "unknown driver",
			cfg:  config.Config{Catalog: config.Catalog{Driver: "postgres"}},
			want: "unknown catalog driver",
		},
		{
			name: "sqlite without path",
			cfg:  config.Config{Catalog: config.Catalog{Driver: "sqlite"}},
			want: "empty database path",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat, err := app.OpenCatalog(tc.cfg)
			if err == nil {
				cat.Close()
				t.Fatal("OpenCatalog accepted invalid config")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("OpenCatalog error = %q, want it to contain %q", err, tc.want)
			}
			if cat != nil {
				t.Error("OpenCatalog returned a catalog with an error")
			}
		})
	}
}
