package catalog_test

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// This file holds helpers of the Postgres-specific tests. The tests that
// need a server get their DSN from catalogtest.PostgresDSN, which skips them
// unless PARKANA_TEST_POSTGRES_DSN is set.

// openPostgres opens a catalog on dsn and closes it when t ends.
func openPostgres(t *testing.T, dsn string) *catalog.Postgres {
	t.Helper()
	c, err := catalog.NewPostgres(t.Context(), dsn)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

// pgAdmin opens a plain database/sql pool on dsn, for tests that inspect or
// lock rows behind the catalog's back. It is closed when t ends.
func pgAdmin(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// holder is a transaction on its own connection that holds row locks (or
// uncommitted rows) the catalog under test must wait for.
type holder struct {
	tx  *sql.Tx
	pid int
}

// newHolder begins a transaction on a dedicated connection of db and
// returns it with the connection's backend pid. The transaction is rolled
// back when t ends unless the test ended it.
func newHolder(t *testing.T, ctx context.Context, db *sql.DB) *holder {
	t.Helper()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("holder connection: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	h := &holder{}
	if err := conn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&h.pid); err != nil {
		t.Fatalf("holder pid: %v", err)
	}
	if h.tx, err = conn.BeginTx(ctx, nil); err != nil {
		t.Fatalf("holder begin: %v", err)
	}
	t.Cleanup(func() { h.tx.Rollback() })
	return h
}

// exec runs stmt in the holder's transaction.
func (h *holder) exec(t *testing.T, ctx context.Context, stmt string, args ...any) {
	t.Helper()
	if _, err := h.tx.ExecContext(ctx, stmt, args...); err != nil {
		t.Fatalf("holder %s: %v", stmt, err)
	}
}

// insertUncommitted inserts a live file row for key in the holder's
// transaction, so any other insert of key waits for the holder to end.
func (h *holder) insertUncommitted(t *testing.T, ctx context.Context, key string) {
	t.Helper()
	h.exec(t, ctx, `INSERT INTO files ("tenant", "key", "stream", "min_ts", "max_ts", "rows", "bytes", "label_values", "created_at")
		VALUES ($1, $2, 'api', 0, 0, 0, 0, 'null', 0)`, string(tenant.Default), key)
}

// lockFile locks the live file row of key in the holder's transaction.
func (h *holder) lockFile(t *testing.T, ctx context.Context, key string) {
	t.Helper()
	var got string
	if err := h.tx.QueryRowContext(ctx,
		`SELECT "key" FROM files WHERE "tenant" = $1 AND "key" = $2 FOR UPDATE`,
		string(tenant.Default), key).Scan(&got); err != nil {
		t.Fatalf("holder lock %s: %v", key, err)
	}
}

// waitBlocked returns once some backend waits on a lock the holder holds:
// the catalog call the test started is now parked behind the holder. It polls
// the server (a condition, not a timed sleep) until ctx ends, and fails if
// the call reports on done before it blocked.
func (h *holder) waitBlocked(t *testing.T, ctx context.Context, db *sql.DB, done <-chan error) {
	t.Helper()
	for {
		select {
		case err := <-done:
			t.Fatalf("call returned before blocking on the holder: %v", err)
		default:
		}
		var blocked bool
		err := db.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY (pg_blocking_pids("pid")))`,
			h.pid).Scan(&blocked)
		if err != nil {
			t.Fatalf("wait until blocked by holder: %v", err)
		}
		if blocked {
			return
		}
	}
}

// ghostTimeout bounds how long a canceled call's server backend may keep
// waiting on the holder after the call returned.
const ghostTimeout = 5 * time.Second

// waitNoWaiters returns once no backend waits on a lock the holder holds. A
// canceled catalog call must not leave its server-side statement queued
// behind the holder: such an orphan keeps the locks it already took (rows
// it inserted or locked) and would apply its statement when the holder lets
// go. It fails t if a waiter remains after ghostTimeout.
func (h *holder) waitNoWaiters(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, ghostTimeout)
	defer cancel()
	for {
		var waiters string
		err := db.QueryRowContext(ctx,
			`SELECT COALESCE(string_agg("pid"::text, ','), '') FROM pg_stat_activity WHERE $1 = ANY (pg_blocking_pids("pid"))`,
			h.pid).Scan(&waiters)
		if ctx.Err() != nil {
			t.Fatalf("backends [%s] still wait on the holder %v after the call returned: the canceled statement was not canceled on the server", waiters, ghostTimeout)
		}
		if err != nil {
			t.Fatalf("wait until nothing waits on the holder: %v", err)
		}
		if waiters == "" {
			return
		}
	}
}

// ledgerRow is one row of schema_migrations.
type ledgerRow struct {
	version   int64
	name      string
	appliedAt int64
}

// readLedger returns schema_migrations ordered by version.
func readLedger(t *testing.T, db *sql.DB) []ledgerRow {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT "version", "name", "applied_at" FROM schema_migrations ORDER BY "version"`)
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	defer rows.Close()
	var out []ledgerRow
	for rows.Next() {
		var r ledgerRow
		if err := rows.Scan(&r.version, &r.name, &r.appliedAt); err != nil {
			t.Fatalf("read ledger: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	return out
}

// embeddedMigrations returns the names of the Postgres migrations in
// version order, read from the package directory the binary embeds.
func embeddedMigrations(t *testing.T) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join("postgres_migrations", "*.sql"))
	if err != nil || len(names) == 0 {
		t.Fatalf("list postgres_migrations: %v (%d files)", err, len(names))
	}
	for i, n := range names {
		names[i] = filepath.Base(n)
	}
	// Names are {version}_{description}.sql; order by numeric version.
	version := func(name string) int {
		prefix, _, _ := strings.Cut(name, "_")
		v, err := strconv.Atoi(prefix)
		if err != nil {
			t.Fatalf("migration %s: bad version prefix", name)
		}
		return v
	}
	slices.SortFunc(names, func(a, b string) int { return cmp.Compare(version(a), version(b)) })
	return names
}

// withParam returns dsn with the connection parameter key set to value
// (pgx sends parameters it does not know, such as search_path or planner
// settings, to the server as session settings).
func withParam(t *testing.T, dsn, key, value string) string {
	t.Helper()
	if !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://") {
		// A later keyword overrides an earlier one.
		return dsn + " " + key + "=" + value
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("DSN in URL form does not parse")
	}
	q := u.Query()
	q.Set(key, value)
	u.RawQuery = q.Encode()
	return u.String()
}

// startBlocked runs call in a goroutine with a context of its own, waits
// until it blocks on h, and returns the channel its error arrives on and the
// cancel function of its context.
func startBlocked(t *testing.T, ctx context.Context, db *sql.DB, h *holder, call func(context.Context) error) (<-chan error, context.CancelFunc) {
	t.Helper()
	callCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- call(callCtx) }()
	h.waitBlocked(t, ctx, db, done)
	return done, cancel
}

// sqlState returns the SQLSTATE of err, or "" if it carries no Postgres
// error.
func sqlState(err error) string {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code
	}
	return ""
}

// pgLockNotAvailable is SQLSTATE lock_not_available (NOWAIT, lock_timeout).
const pgLockNotAvailable = "55P03"

// probeInsert tries to insert a live file row for key on its own
// transaction, rolled back afterwards, waiting at most briefly for a
// conflicting uncommitted row: it returns nil when no other transaction has
// key, and a lock_not_available error while one has inserted it.
func probeInsert(t *testing.T, ctx context.Context, db *sql.DB, key string) error {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("probe begin: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = '200ms'`); err != nil {
		t.Fatalf("probe lock_timeout: %v", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO files ("tenant", "key", "stream", "min_ts", "max_ts", "rows", "bytes", "label_values", "created_at")
		VALUES ($1, $2, 'probe', 0, 0, 0, 0, 'null', 0)`, string(tenant.Default), key)
	return err
}

// probeLock tries to lock the file row of key with NOWAIT on its own
// transaction: nil when no other transaction holds it, lock_not_available
// while one does.
func probeLock(t *testing.T, ctx context.Context, db *sql.DB, key string) error {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("probe begin: %v", err)
	}
	defer tx.Rollback()
	var got string
	return tx.QueryRowContext(ctx, `SELECT "key" FROM files WHERE "tenant" = $1 AND "key" = $2 FOR UPDATE NOWAIT`,
		string(tenant.Default), key).Scan(&got)
}

// exerciseCatalog calls every Catalog method on c with keys, a stream and a
// pipeline named after tag, and checks each result, so a broken connection,
// pool or cached statement in any method fails the test. Several calls with
// distinct tags may share one catalog.
func exerciseCatalog(t *testing.T, ctx context.Context, c catalog.Catalog, tag string) {
	t.Helper()
	key := func(s string) string { return tag + "-" + s }
	f1 := testFile(key("1"), tag, 0, time.Minute)
	f1.LabelValues = map[string][]string{"app": {"x"}}
	f1.LabelsComplete = true
	f2 := testFile(key("2"), tag, time.Minute, 2*time.Minute)
	if err := c.AddFiles(ctx, f1, f2); err != nil {
		t.Fatalf("%s: AddFiles: %v", tag, err)
	}
	if err := c.ReplaceFiles(ctx, tenant.Default, []string{key("1")}, []catalog.File{testFile(key("3"), tag, 0, 2*time.Minute)}); err != nil {
		t.Fatalf("%s: ReplaceFiles: %v", tag, err)
	}
	if err := c.SetSidecar(ctx, tenant.Default, key("3")); err != nil {
		t.Fatalf("%s: SetSidecar: %v", tag, err)
	}
	var live []string
	for f, err := range c.ListFiles(ctx, catalog.Filter{Tenant: tenant.Default, Stream: tag, Match: map[string]string{"app": "x"}}) {
		if err != nil {
			t.Fatalf("%s: ListFiles: %v", tag, err)
		}
		if f.Key == key("3") && !f.HasSidecar {
			t.Fatalf("%s: %s has no sidecar flag", tag, f.Key)
		}
		live = append(live, f.Key)
	}
	if want := []string{key("3"), key("2")}; !slices.Equal(live, want) {
		t.Fatalf("%s: ListFiles = %v, want %v", tag, live, want)
	}
	streams, err := c.Streams(ctx, tenant.Default)
	if err != nil || !slices.Contains(streams, tag) {
		t.Fatalf("%s: Streams = %v, %v, want it to contain %s", tag, streams, err, tag)
	}
	purged, err := c.PurgeDeleted(ctx, tenant.Default, time.Now().Add(time.Hour))
	if err != nil || !slices.Contains(purged, key("1")) {
		t.Fatalf("%s: PurgeDeleted = %v, %v, want it to contain %s", tag, purged, err, key("1"))
	}

	token0, err := c.PipelineChangeToken(ctx, tenant.Default)
	if err != nil {
		t.Fatalf("%s: PipelineChangeToken: %v", tag, err)
	}
	name := key("p")
	def := catalog.PipelineDefinition{Tenant: tenant.Default, Name: name, Stage: catalog.PipelineStageIngest, Selector: tag, Spec: []byte(`{"v":1}`)}
	if got, err := c.PutPipeline(ctx, def, 0); err != nil || got.Version != 1 {
		t.Fatalf("%s: PutPipeline create = %+v, %v", tag, got, err)
	}
	def.Spec = []byte(`{"v":2}`)
	if got, err := c.PutPipeline(ctx, def, 1); err != nil || got.Version != 2 {
		t.Fatalf("%s: PutPipeline update = %+v, %v", tag, got, err)
	}
	if got, err := c.GetPipeline(ctx, tenant.Default, name); err != nil || got.Version != 2 || string(got.Spec) != `{"v":2}` {
		t.Fatalf("%s: GetPipeline = %+v, %v", tag, got, err)
	}
	list, err := c.ListPipelines(ctx, tenant.Default)
	if err != nil || !slices.ContainsFunc(list, func(d catalog.PipelineDefinition) bool { return d.Name == name }) {
		t.Fatalf("%s: ListPipelines = %v, %v, want it to contain %s", tag, list, err, name)
	}
	if _, err := c.DeletePipeline(ctx, tenant.Default, name, 2); err != nil {
		t.Fatalf("%s: DeletePipeline: %v", tag, err)
	}
	if _, err := c.GetPipeline(ctx, tenant.Default, name); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("%s: GetPipeline after delete = %v, want ErrNotFound", tag, err)
	}
	if token, err := c.PipelineChangeToken(ctx, tenant.Default); err != nil || token < token0+3 {
		t.Fatalf("%s: PipelineChangeToken = %d, %v, want at least %d", tag, token, err, token0+3)
	}
}
