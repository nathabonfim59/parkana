package catalog

import (
	"database/sql"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// TestSQLiteMigratesV5 seeds a database at schema version 5 (as shipped before
// pipelines) with an existing file row, then opens it through NewSQLite and
// checks migration 6 kept the files and made pipelines usable.
func TestSQLiteMigratesV5(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.db")
	db, err := sql.Open(sqliteDriver, sqliteDSN(path, "busy_timeout(10000)"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_migrations (
		"version" INTEGER PRIMARY KEY,
		"name" TEXT NOT NULL,
		"applied_at" INTEGER NOT NULL
	)`); err != nil {
		db.Close()
		t.Fatalf("create schema_migrations: %v", err)
	}
	migrations, err := loadMigrations()
	if err != nil {
		db.Close()
		t.Fatalf("loadMigrations: %v", err)
	}
	for _, m := range migrations {
		if m.version > 5 {
			continue
		}
		if _, err := db.Exec(m.stmt); err != nil {
			db.Close()
			t.Fatalf("apply %s: %v", m.name, err)
		}
		if _, err := db.Exec(`INSERT INTO schema_migrations ("version", "name", "applied_at") VALUES (?, ?, ?)`,
			m.version, m.name, time.Now().UnixMicro()); err != nil {
			db.Close()
			t.Fatalf("record %s: %v", m.name, err)
		}
	}
	start := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	if _, err := db.Exec(`INSERT INTO files (
		"tenant", "stream", "key", "min_ts", "max_ts", "rows", "bytes",
		"label_values", "labels_complete", "has_sidecar", "created_at", "deleted_at"
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		string(tenant.Default), "api", "old.parquet", start.UnixMicro(), start.Add(time.Minute).UnixMicro(),
		7, 4096, `{"level":["info"]}`, 1, 1, start.UnixMicro()); err != nil {
		db.Close()
		t.Fatalf("seed v5 file: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close v5 database: %v", err)
	}

	c, err := NewSQLite(path)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	var applied int
	if err := c.read.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM schema_migrations WHERE "version" = 6`).Scan(&applied); err != nil {
		t.Fatalf("read migration 6: %v", err)
	}
	if applied != 1 {
		t.Fatalf("migration 6 applied %d times, want 1", applied)
	}

	var keys []string
	for f, err := range c.ListFiles(t.Context(), Filter{Tenant: tenant.Default}) {
		if err != nil {
			t.Fatalf("ListFiles: %v", err)
		}
		keys = append(keys, f.Key)
	}
	if !slices.Equal(keys, []string{"old.parquet"}) {
		t.Fatalf("files after migration = %v, want [old.parquet]", keys)
	}

	token, err := c.PipelineChangeToken(t.Context(), tenant.Default)
	if err != nil || token != 0 {
		t.Fatalf("token on migrated database = %d, %v, want 0", token, err)
	}
	def := PipelineDefinition{
		Tenant: tenant.Default, Name: "p", Stage: PipelineStageIngest,
		Selector: "api", Spec: []byte(`{"steps":[]}`),
	}
	got, err := c.PutPipeline(t.Context(), def, 0)
	if err != nil {
		t.Fatalf("PutPipeline on migrated database: %v", err)
	}
	if got.Version != 1 {
		t.Fatalf("version = %d, want 1", got.Version)
	}
}

func TestPipelineCounterExhaustion(t *testing.T) {
	now := time.Now().UTC()
	base := PipelineDefinition{
		Tenant: tenant.Default, Name: "p", Stage: PipelineStageIngest,
		Selector: "api", Spec: []byte(`{}`), Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	input := PipelineDefinition{
		Tenant: tenant.Default, Name: "p", Stage: PipelineStageIngest,
		Selector: "api", Spec: []byte(`{}`),
	}

	t.Run("memory", func(t *testing.T) {
		c := NewMemory()
		reset := func(version, token int64) {
			d := base
			d.Version = version
			c.pipelines[tenant.Default] = map[string]pipelineRecord{"p": {def: d, token: token}}
		}
		record := func(t *testing.T) (int64, int64, bool) {
			t.Helper()
			rec := c.pipelines[tenant.Default]["p"]
			return rec.def.Version, rec.token, rec.def.Deleted()
		}

		reset(maxInt64, 1)
		if _, err := c.PutPipeline(t.Context(), input, maxInt64); !errors.Is(err, errVersionExhausted) {
			t.Fatalf("version overflow update = %v, want errVersionExhausted", err)
		}
		if version, token, deleted := record(t); version != maxInt64 || token != 1 || deleted {
			t.Fatalf("after version overflow: version %d token %d deleted %v", version, token, deleted)
		}

		reset(1, maxInt64)
		if _, err := c.PutPipeline(t.Context(), input, 1); !errors.Is(err, errVersionExhausted) {
			t.Fatalf("token overflow update = %v, want errVersionExhausted", err)
		}
		if version, token, deleted := record(t); version != 1 || token != maxInt64 || deleted {
			t.Fatalf("after token overflow: version %d token %d deleted %v", version, token, deleted)
		}

		reset(maxInt64, 1)
		if _, err := c.DeletePipeline(t.Context(), tenant.Default, "p", maxInt64); !errors.Is(err, errVersionExhausted) {
			t.Fatalf("version overflow delete = %v, want errVersionExhausted", err)
		}
		if version, token, deleted := record(t); version != maxInt64 || token != 1 || deleted {
			t.Fatalf("after delete version overflow: version %d token %d deleted %v", version, token, deleted)
		}

		reset(1, maxInt64)
		if _, err := c.DeletePipeline(t.Context(), tenant.Default, "p", 1); !errors.Is(err, errVersionExhausted) {
			t.Fatalf("token overflow delete = %v, want errVersionExhausted", err)
		}
		if version, token, deleted := record(t); version != 1 || token != maxInt64 || deleted {
			t.Fatalf("after delete token overflow: version %d token %d deleted %v", version, token, deleted)
		}
	})

	t.Run("sqlite", func(t *testing.T) {
		c, err := NewSQLite(filepath.Join(t.TempDir(), "catalog.db"))
		if err != nil {
			t.Fatalf("NewSQLite: %v", err)
		}
		t.Cleanup(func() { c.Close() })
		if _, err := c.PutPipeline(t.Context(), input, 0); err != nil {
			t.Fatalf("create: %v", err)
		}
		force := func(t *testing.T, version, token int64) {
			t.Helper()
			if _, err := c.write.ExecContext(t.Context(),
				`UPDATE pipelines SET "version" = ?, "change_token" = ? WHERE "tenant" = ? AND "name" = ?`,
				version, token, string(tenant.Default), "p"); err != nil {
				t.Fatalf("force counters: %v", err)
			}
		}
		record := func(t *testing.T) (int64, int64, bool) {
			t.Helper()
			var version, token int64
			var deleted sql.NullInt64
			if err := c.read.QueryRowContext(t.Context(),
				`SELECT "version", "change_token", "deleted_at" FROM pipelines WHERE "tenant" = ? AND "name" = ?`,
				string(tenant.Default), "p").Scan(&version, &token, &deleted); err != nil {
				t.Fatalf("read counters: %v", err)
			}
			return version, token, deleted.Valid
		}

		force(t, maxInt64, 1)
		if _, err := c.PutPipeline(t.Context(), input, maxInt64); !errors.Is(err, errVersionExhausted) {
			t.Fatalf("version overflow update = %v, want errVersionExhausted", err)
		}
		if version, token, deleted := record(t); version != maxInt64 || token != 1 || deleted {
			t.Fatalf("after version overflow: version %d token %d deleted %v", version, token, deleted)
		}

		force(t, 1, maxInt64)
		if _, err := c.PutPipeline(t.Context(), input, 1); !errors.Is(err, errVersionExhausted) {
			t.Fatalf("token overflow update = %v, want errVersionExhausted", err)
		}
		if version, token, deleted := record(t); version != 1 || token != maxInt64 || deleted {
			t.Fatalf("after token overflow: version %d token %d deleted %v", version, token, deleted)
		}

		force(t, maxInt64, 1)
		if _, err := c.DeletePipeline(t.Context(), tenant.Default, "p", maxInt64); !errors.Is(err, errVersionExhausted) {
			t.Fatalf("version overflow delete = %v, want errVersionExhausted", err)
		}
		if version, token, deleted := record(t); version != maxInt64 || token != 1 || deleted {
			t.Fatalf("after delete version overflow: version %d token %d deleted %v", version, token, deleted)
		}

		force(t, 1, maxInt64)
		if _, err := c.DeletePipeline(t.Context(), tenant.Default, "p", 1); !errors.Is(err, errVersionExhausted) {
			t.Fatalf("token overflow delete = %v, want errVersionExhausted", err)
		}
		if version, token, deleted := record(t); version != 1 || token != maxInt64 || deleted {
			t.Fatalf("after delete token overflow: version %d token %d deleted %v", version, token, deleted)
		}
	})
}
