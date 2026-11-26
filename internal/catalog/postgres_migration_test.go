package catalog_test

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/catalog/catalogtest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// checkLedger fails t unless the ledger has exactly one row per embedded
// migration, in order, each with the version its name starts with.
func checkLedger(t *testing.T, ledger []ledgerRow, migrations []string) {
	t.Helper()
	if len(ledger) != len(migrations) {
		t.Fatalf("ledger has %d rows %+v, want one per migration %v", len(ledger), ledger, migrations)
	}
	for i, r := range ledger {
		prefix, _, _ := strings.Cut(migrations[i], "_")
		version, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil || r.name != migrations[i] || r.version != version {
			t.Fatalf("ledger row %d = %+v, want version %s name %s", i, r, prefix, migrations[i])
		}
	}
}

// TestPostgresReopenKeepsDataAndLedger checks a second NewPostgres on a
// migrated schema keeps every file and pipeline and does not run any
// migration again: the ledger rows, applied_at included, are unchanged.
// Needs PARKANA_TEST_POSTGRES_DSN.
func TestPostgresReopenKeepsDataAndLedger(t *testing.T) {
	dsn := catalogtest.PostgresDSN(t)
	admin := pgAdmin(t, dsn)
	ctx := boundedContext(t)
	migrations := embeddedMigrations(t)

	c1, err := catalog.NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	if err := c1.AddFiles(ctx, testFile("a", "api", 0, time.Minute), testFile("b", "web", time.Minute, 2*time.Minute)); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	if err := c1.ReplaceFiles(ctx, tenant.Default, []string{"b"}, nil); err != nil {
		t.Fatalf("ReplaceFiles: %v", err)
	}
	if _, err := c1.PutPipeline(ctx, pipelineDef(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"v":1}`), 0); err != nil {
		t.Fatalf("PutPipeline: %v", err)
	}
	before := readLedger(t, admin)
	checkLedger(t, before, migrations)
	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	c2 := openPostgres(t, dsn)
	if after := readLedger(t, admin); !slices.Equal(after, before) {
		t.Fatalf("ledger after reopen = %+v, want unchanged %+v", after, before)
	}
	if got := listKeys(t, c2, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("live after reopen = %v, want [a]", got)
	}
	if got := listKeys(t, c2, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("all after reopen = %v, want [a b]", got)
	}
	if got, err := c2.GetPipeline(ctx, tenant.Default, "p"); err != nil || got.Version != 1 || string(got.Spec) != `{"v":1}` {
		t.Fatalf("pipeline after reopen = %+v, %v", got, err)
	}
	if got := pipelineToken(t, c2, tenant.Default); got != 1 {
		t.Fatalf("token after reopen = %d, want 1", got)
	}
}

// TestPostgresConcurrentMigrate starts eight NewPostgres at once on one
// fresh schema: all succeed, the ledger has exactly one row per migration,
// and every catalog works. Without the per-schema advisory lock the
// concurrent CREATE TABLEs collide. Needs PARKANA_TEST_POSTGRES_DSN.
func TestPostgresConcurrentMigrate(t *testing.T) {
	dsn := catalogtest.PostgresDSN(t)
	admin := pgAdmin(t, dsn)
	ctx := boundedContext(t)
	const openers = 8

	start := make(chan struct{})
	cats := make([]*catalog.Postgres, openers)
	errs := make([]error, openers)
	var wg sync.WaitGroup
	for i := range openers {
		wg.Go(func() {
			<-start
			cats[i], errs[i] = catalog.NewPostgres(ctx, dsn)
		})
	}
	close(start)
	wg.Wait()
	for _, c := range cats {
		if c != nil {
			t.Cleanup(func() { c.Close() })
		}
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("opener %d: %v", i, err)
		}
	}
	checkLedger(t, readLedger(t, admin), embeddedMigrations(t))
	for i, c := range cats {
		exerciseCatalog(t, ctx, c, fmt.Sprintf("o%d", i))
	}
}

// TestPostgresMissingSchema opens a DSN whose search_path names no existing
// schema: NewPostgres fails with an error that says so, and creates nothing.
// Needs PARKANA_TEST_POSTGRES_DSN.
func TestPostgresMissingSchema(t *testing.T) {
	dsn := catalogtest.PostgresDSN(t)
	suffix := make([]byte, 8)
	rand.Read(suffix)
	missing := "parkana_missing_" + hex.EncodeToString(suffix)

	ctx := boundedContext(t)
	c, err := catalog.NewPostgres(ctx, withParam(t, dsn, "search_path", missing))
	if err == nil {
		c.Close()
		t.Fatalf("NewPostgres with search_path=%s succeeded, want an error", missing)
	}
	if msg := err.Error(); !strings.HasPrefix(msg, "catalog: postgres: ") || !strings.Contains(msg, "no current schema") {
		t.Fatalf("error = %q, want a catalog: postgres: error naming the missing current schema", msg)
	}
	var exists bool
	if err := pgAdmin(t, dsn).QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, missing).Scan(&exists); err != nil {
		t.Fatalf("look up schema: %v", err)
	}
	if exists {
		t.Fatalf("NewPostgres created schema %s", missing)
	}
}

// TestPostgresReopenStatementCache covers the pgx statement cache across
// migration runs (plan addition A1): a catalog reopened on a migrated
// schema, and an instance opened before another NewPostgres migrated the same
// schema again, both serve every Catalog method afterwards, also on
// connections whose statements were prepared before that run. Needs
// PARKANA_TEST_POSTGRES_DSN.
func TestPostgresReopenStatementCache(t *testing.T) {
	dsn := catalogtest.PostgresDSN(t)
	ctx := boundedContext(t)

	first, err := catalog.NewPostgres(ctx, dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	exerciseCatalog(t, ctx, first, "first")
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// early is opened, and has statements cached on its connections, before
	// late runs the migration step on the same schema.
	early := openPostgres(t, dsn)
	exerciseCatalog(t, ctx, early, "early-before")
	late := openPostgres(t, dsn)
	exerciseCatalog(t, ctx, early, "early-after")
	exerciseCatalog(t, ctx, late, "late")

	// Each instance sees the other's writes.
	for _, c := range []*catalog.Postgres{early, late} {
		streams, err := c.Streams(ctx, tenant.Default)
		if err != nil {
			t.Fatalf("Streams: %v", err)
		}
		for _, want := range []string{"first", "early-before", "early-after", "late"} {
			if !slices.Contains(streams, want) {
				t.Fatalf("Streams = %v, missing %s", streams, want)
			}
		}
	}
	if _, err := early.GetPipeline(ctx, tenant.Default, "late-p"); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("early GetPipeline(late-p) = %v, want ErrNotFound (deleted by late)", err)
	}
}

// TestPostgresRefusesNewerSchema opens a schema whose ledger records a
// migration newer than any this binary embeds, as a newer Parkana would leave
// it: NewPostgres fails with ErrSchemaTooNew, names both versions, and the
// migration transaction rolls back, so the ledger is unchanged. A schema at
// exactly the embedded version opens. Needs PARKANA_TEST_POSTGRES_DSN.
func TestPostgresRefusesNewerSchema(t *testing.T) {
	dsn := catalogtest.PostgresDSN(t)
	admin := pgAdmin(t, dsn)
	ctx := boundedContext(t)
	migrations := embeddedMigrations(t)
	prefix, _, _ := strings.Cut(migrations[len(migrations)-1], "_")
	latest, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil {
		t.Fatalf("parse version of %s: %v", migrations[len(migrations)-1], err)
	}

	c := openPostgres(t, dsn)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Equal version: reopening a fully migrated schema works.
	openPostgres(t, dsn)

	newer := latest + 1
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO schema_migrations ("version", "name", "applied_at") VALUES ($1, $2, $3)`,
		newer, fmt.Sprintf("%04d_from_the_future.sql", newer), time.Now().UnixMicro()); err != nil {
		t.Fatalf("record future migration: %v", err)
	}
	before := readLedger(t, admin)

	refused, err := catalog.NewPostgres(ctx, dsn)
	if err == nil {
		refused.Close()
		t.Fatalf("NewPostgres at version %d succeeded, want ErrSchemaTooNew", newer)
	}
	if !errors.Is(err, catalog.ErrSchemaTooNew) {
		t.Fatalf("NewPostgres = %v, want ErrSchemaTooNew", err)
	}
	want := fmt.Sprintf("catalog: postgres: schema version %d is newer than this binary supports (%d)", newer, latest)
	if msg := err.Error(); !strings.HasPrefix(msg, want) {
		t.Fatalf("error = %q, want prefix %q", msg, want)
	}
	if after := readLedger(t, admin); !slices.Equal(after, before) {
		t.Fatalf("ledger after refused open = %+v, want unchanged %+v", after, before)
	}
}
