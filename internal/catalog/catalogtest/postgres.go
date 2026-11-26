package catalogtest

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// postgresDSNEnv names the base DSN of the Postgres integration tests.
const postgresDSNEnv = "PARKANA_TEST_POSTGRES_DSN"

// PostgresDSN returns a DSN for a fresh, empty Postgres schema, for tests of
// catalog.Postgres. It skips t unless PARKANA_TEST_POSTGRES_DSN is set to a
// DSN (postgres:// URL or keyword/value form) of a database the test may
// create schemas in. The schema is named parkana_t_<random hex>, is the
// returned DSN's search_path, and is dropped (CASCADE) when t ends, so tests
// and packages running in parallel never share tables.
func PostgresDSN(t testing.TB) string {
	t.Helper()
	base := os.Getenv(postgresDSNEnv)
	if base == "" {
		t.Skip(postgresDSNEnv + " not set; skipping Postgres integration test")
	}
	suffix := make([]byte, 8)
	rand.Read(suffix) // never fails, per crypto/rand
	schema := "parkana_t_" + hex.EncodeToString(suffix)

	if err := adminExec(t.Context(), base, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("catalogtest: create schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		// t.Context is already canceled when cleanups run.
		if err := adminExec(context.Background(), base, `DROP SCHEMA `+schema+` CASCADE`); err != nil {
			t.Errorf("catalogtest: drop schema %s: %v", schema, err)
		}
	})
	dsn, err := withSearchPath(base, schema)
	if err != nil {
		t.Fatalf("catalogtest: %s: %v", postgresDSNEnv, err)
	}
	return dsn
}

// adminExec runs one statement on a single connection to dsn and closes it,
// so schema setup holds no connection while the test runs.
func adminExec(ctx context.Context, dsn, stmt string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	_, err = db.ExecContext(ctx, stmt)
	return errors.Join(err, db.Close())
}

// withSearchPath returns dsn with search_path set to schema.
func withSearchPath(dsn, schema string) (string, error) {
	if !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://") {
		// Keyword/value form: a later keyword overrides an earlier one.
		return dsn + " search_path=" + schema, nil
	}
	u, err := url.Parse(dsn)
	if err != nil {
		// Do not echo the DSN or url's error: they may hold a password.
		return "", errors.New("DSN in URL form does not parse")
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
