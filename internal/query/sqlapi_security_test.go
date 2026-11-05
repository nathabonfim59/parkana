package query

// Independent adversarial tests of the SQL API (C2b). Every case goes through
// the real executor — catalog listing, object lease, sandbox materialization,
// lockdown and validation — so a rejection is the one the endpoint surfaces.
// Each case proves its target block was reached (the classified error names
// the guilty function, relation or statement), and planted sentinels never
// appear in a result or as a file side effect. Positive controls keep the
// rejections honest: the same data and the same harmless constructs work.
//
// Potentially expensive statements run under a finite context. Explicit
// recursive CTEs must fail with errSQLRecursive (the reviewed WITH RECURSIVE
// lexical ban and AST fallback); implicit recursion (no RECURSIVE keyword) and
// recurring.* are covered here too and must fail with a concrete binder or
// policy cause.

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/server"
	"github.com/nathabonfim59/parkana/internal/tenant"
	"uuid"
)

// sqlSecurityQuery runs q over the standard test span under a finite
// five-second context and returns the raw result and error, so tests assert
// the exact failure class themselves. A regression that reaches network or
// autoload still cannot hang the suite indefinitely.
func sqlSecurityQuery(t *testing.T, e *sqlExec, q string) (sqlResult, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	return e.Query(ctx, sqlRequest{
		Tenant: tenant.Default, Query: q,
		From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
	})
}

// sqlBoundedQuery runs q through the executor with a finite context and a
// small result limit, returning how long the request took. No security case
// may rely on the process staying alive to bound a query's cost.
func sqlBoundedQuery(t *testing.T, e *sqlExec, q string, limit int, timeout time.Duration) (sqlResult, time.Duration, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	start := time.Now()
	res, err := e.Query(ctx, sqlRequest{
		Tenant: tenant.Default, Query: q, Limit: limit,
		From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
	})
	return res, time.Since(start), err
}

// sqlErrText formats err for an assertion message, including a nil error, so
// an unexpected acceptance reports the query instead of panicking.
func sqlErrText(err error) string {
	if err == nil {
		return "<nil> (query was accepted)"
	}
	return err.Error()
}

// assertStatementRejected proves q was rejected by the statement/content
// policy (not by DuckDB failing later) and that the message names why.
func assertStatementRejected(t *testing.T, e *sqlExec, q, want string) {
	t.Helper()
	res, err := sqlSecurityQuery(t, e, q)
	if !errors.Is(err, errSQLStatement) {
		t.Fatalf("query %q = %v, want errSQLStatement", q, err)
	}
	if want != "" && !strings.Contains(err.Error(), want) {
		t.Fatalf("query %q = %v, want it to name %q", q, err, want)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("query %q returned %d rows despite rejection", q, len(res.Rows))
	}
}

// assertNoRowsContain proves a successful result never carries a sentinel.
func assertNoRowsContain(t *testing.T, res sqlResult, sentinel string) {
	t.Helper()
	for _, row := range res.Rows {
		for _, cell := range row {
			if s, ok := cell.(string); ok && strings.Contains(s, sentinel) {
				t.Fatalf("result leaked sentinel %q: %q", sentinel, s)
			}
		}
	}
}

// assertResultEncodable proves a successful result can cross the frozen JSON
// response contract; an executor value the encoder refuses would become a 500
// at the endpoint instead of a controlled client error.
func assertResultEncodable(t *testing.T, res sqlResult) {
	t.Helper()
	if _, err := json.Marshal(res); err != nil {
		t.Fatalf("result is not JSON-encodable: %v (rows %#v)", err, res.Rows)
	}
}

// assertPathAbsent proves a rejected write left no file or directory behind.
func assertPathAbsent(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected write created %s (stat err = %v)", path, err)
	}
}

// oneKeepFile is the positive-control fixture every security test starts
// from: exactly one in-range row in the default tenant.
func oneKeepFile() []plantest.File {
	return []plantest.File{{
		Tenant: tenant.Default, Stream: "api",
		Rows: []plan.Row{{TS: plantest.T0, Line: "IN-RANGE-KEEP", Labels: logs.FromMap(map[string]string{"service_name": "api"})}},
	}}
}

// loadSQLHTTPFixture stores files the way the ingester does and catalogs them
// for the real endpoint, so endpoint tests read the same on-disk fixtures as
// the executor tests rather than a fake runner.
func loadSQLHTTPFixture(t *testing.T, store objstore.Store, cat catalog.Catalog, files []plantest.File) []catalog.File {
	t.Helper()
	listed := make([]catalog.File, 0, len(files))
	for _, f := range files {
		data, st, err := plantest.Encode(f)
		if err != nil {
			t.Fatalf("encode %s/%s: %v", f.Tenant, f.Stream, err)
		}
		key := objstore.DataKey(f.Tenant, f.Stream, st.MinTS, uuid.NewV7())
		if err := store.Put(t.Context(), key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		cf := catalog.File{
			Tenant: f.Tenant, Stream: f.Stream, Key: key,
			MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(len(data)),
			LabelValues: st.LabelValues, CreatedAt: time.Now().Truncate(time.Microsecond),
		}
		if err := cat.AddFiles(t.Context(), cf); err != nil {
			t.Fatalf("catalog add %s: %v", key, err)
		}
		listed = append(listed, cf)
	}
	return listed
}

// sqlHTTPFixture is the endpoint window fixture: one default-tenant row inside
// sqlValidBody's 2024-01-01..02 window, one default row on each side of it,
// and another tenant's row inside the same window.
func sqlHTTPFixture() []plantest.File {
	at := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	row := func(ts time.Time, line string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "api"})}
	}
	return []plantest.File{
		{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{
			row(at.Add(-13*time.Hour), "OUT-OF-WINDOW-SENTINEL"),
			row(at, "IN-RANGE-KEEP"),
			row(at.Add(13*time.Hour), "AFTER-WINDOW-SENTINEL"),
		}},
		{Tenant: plantest.OtherTenant, Stream: "api", Rows: []plan.Row{
			row(at, "OTHER-TENANT-SENTINEL"),
		}},
	}
}

// TestSQLSecurityHarmlessMetadata proves generic engine metadata that reads
// no file and exposes no setting stays available, and that its value carries
// no path or tenant information (settings and catalog introspection are the
// documented narrower rejection, covered above).
func TestSQLSecurityHarmlessMetadata(t *testing.T) {
	e, _ := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())
	res, err := sqlSecurityQuery(t, e, "SELECT version()")
	if err != nil {
		t.Fatalf("version() = %v, want success", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("version() returned %d rows", len(res.Rows))
	}
	version, ok := res.Rows[0][0].(string)
	if !ok || version == "" {
		t.Fatalf("version() = %#v, want a non-empty string", res.Rows[0][0])
	}
	for _, leak := range []string{"/", "\\", "default", "OtherTenant", "sentinel"} {
		if strings.Contains(version, leak) {
			t.Fatalf("version() = %q leaks %q", version, leak)
		}
	}
	assertResultEncodable(t, res)
}

// TestSQLSecurityResultShape proves the normal typed temporal results stay
// encodable and precise, and that DuckDB's non-JSON temporal encodings are a
// controlled typed rejection instead of an unencodable cell. The rejection
// cases are the regression for the M1 conversion guard: DATE/TIMESTAMP/
// TIMESTAMPTZ infinity and finite years outside JSON's [0,9999] range, at the
// top level and nested in lists and structs, must fail as errSQLRejected.
func TestSQLSecurityResultShape(t *testing.T) {
	e, _ := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())

	for _, tc := range []struct {
		name string
		q    string
	}{
		{"date_year_9999", "SELECT DATE '9999-12-31'"},
		{"date_year_1", "SELECT DATE '0001-01-01'"},
		{"timestamp_year_9999", "SELECT TIMESTAMP '9999-12-31 23:59:59.999999'"},
		{"timestamptz_year_9999", "SELECT TIMESTAMPTZ '9999-12-31 23:59:59.999999+00'"},
		{"pre_epoch_us", "SELECT TIMESTAMP '1969-12-31 23:59:59.999999'"},
		{"time_clock", "SELECT TIME '23:59:59.999999'"},
		{"timetz_clock", "SELECT TIMETZ '23:59:59.999999+00'"},
		{"interval", "SELECT INTERVAL '1 month 2 days 3 microseconds'"},
		{"list_date_year_9999", "SELECT [DATE '9999-12-31']"},
		{"struct_date_year_9999", "SELECT {'d': DATE '9999-12-31'}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := sqlSecurityQuery(t, e, tc.q)
			if err != nil {
				t.Fatalf("%s = %v, want success", tc.q, err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%s returned %d rows, want 1", tc.q, len(res.Rows))
			}
			assertResultEncodable(t, res)
		})
	}

	// NULL is a value, not an encoding failure.
	res, err := sqlSecurityQuery(t, e, "SELECT NULL")
	if err != nil {
		t.Fatalf("SELECT NULL = %v", err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) != 1 || res.Rows[0][0] != nil {
		t.Fatalf("SELECT NULL = %#v, want one nil cell", res.Rows)
	}
	assertResultEncodable(t, res)

	// Pre-epoch time keeps whole microseconds after UTC conversion.
	res, err = sqlSecurityQuery(t, e, "SELECT TIMESTAMP '1969-12-31 23:59:59.999999'")
	if err != nil {
		t.Fatalf("pre-epoch timestamp = %v", err)
	}
	ts, ok := res.Rows[0][0].(time.Time)
	if !ok || ts.Year() != 1969 || ts.Nanosecond()%1000 != 0 || ts.Nanosecond() != 999999000 {
		t.Fatalf("pre-epoch cell = %#v (%T), want 1969-12-31T23:59:59.999999Z", res.Rows[0][0], res.Rows[0][0])
	}

	for _, tc := range []struct {
		name string
		q    string
	}{
		{"date_infinity", "SELECT DATE 'infinity'"},
		{"date_neg_infinity", "SELECT DATE '-infinity'"},
		{"date_year_10000", "SELECT DATE '10000-01-01'"},
		{"date_negative_year", "SELECT DATE '-0001-01-01'"},
		{"timestamp_infinity", "SELECT TIMESTAMP 'infinity'"},
		{"timestamp_neg_infinity", "SELECT TIMESTAMP '-infinity'"},
		{"timestamptz_infinity", "SELECT TIMESTAMPTZ 'infinity'"},
		{"timestamptz_neg_infinity", "SELECT TIMESTAMPTZ '-infinity'"},
		{"list_date_infinity", "SELECT [DATE 'infinity']"},
		{"list_timestamp_neg_infinity", "SELECT [TIMESTAMP '-infinity']"},
		{"struct_date_infinity", "SELECT struct_pack(d := DATE 'infinity')"},
		{"struct_timestamp_infinity", "SELECT {'d': TIMESTAMP 'infinity'}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, elapsed, err := sqlBoundedQuery(t, e, tc.q, 5, time.Second)
			if !errors.Is(err, errSQLRejected) {
				t.Fatalf("%s = %v, want errSQLRejected (non-JSON temporal value)", tc.q, err)
			}
			if len(res.Rows) != 0 {
				t.Fatalf("%s returned %d rows despite rejection", tc.q, len(res.Rows))
			}
			if elapsed > time.Second {
				t.Fatalf("%s took %s, want under the finite 1s context", tc.q, elapsed)
			}
		})
	}
}

// TestSQLSecurityImplicitRecursionRejected covers the recursion forms that do
// not use the explicit WITH RECURSIVE pair. The pinned binder does not make
// them recursive: inside a body named logs, logs resolves to the materialized
// relation, so the recursion column fails to bind; recurring.* is a qualified
// relation; and a self-reference to any other name is a scope violation. Each
// case requires its concrete cause, never just "some error".
func TestSQLSecurityImplicitRecursionRejected(t *testing.T) {
	e, _ := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())

	// Non-recursive shadowing is a legitimate, reachable construct: the same
	// names must keep working so the rejections below are about recursion and
	// scope, not about the name.
	for _, q := range []string{
		"WITH logs AS (SELECT 1 AS x) SELECT * FROM logs",
		"WITH c AS (SELECT 1 AS x) SELECT * FROM c AS logs",
		"WITH logs AS (SELECT line FROM logs) SELECT count(*) FROM logs",
	} {
		res, err := sqlSecurityQuery(t, e, q)
		if err != nil {
			t.Fatalf("shadowing control %q = %v, want success", q, err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("shadowing control %q returned %d rows", q, len(res.Rows))
		}
		assertResultEncodable(t, res)
	}

	const binderColumn = `Binder Error: Referenced column "n" not found in FROM clause!`
	for _, q := range []string{
		"WITH logs(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM logs WHERE n < 5) SELECT n FROM logs ORDER BY n",
		"WITH logs(n) USING KEY (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM logs WHERE n < 5) SELECT n FROM logs ORDER BY n",
	} {
		res, elapsed, err := sqlBoundedQuery(t, e, q, 5, time.Second)
		if !errors.Is(err, errSQLRejected) {
			t.Fatalf("implicit recursion %q = %v, want errSQLRejected", q, err)
		}
		if !strings.Contains(err.Error(), binderColumn) {
			t.Fatalf("implicit recursion %q = %v, want the missing-column binder cause", q, err)
		}
		if len(res.Rows) != 0 || elapsed > time.Second {
			t.Fatalf("implicit recursion %q: %d rows in %s, want 0 rows promptly", q, len(res.Rows), elapsed)
		}
	}

	// Self-reference to any name other than logs fails the relation policy
	// before DuckDB: the name is not a visible CTE inside its own body.
	assertStatementRejected(t, e,
		"WITH t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5) SELECT n FROM t",
		`relation "t" is not the logs relation or a CTE visible at this scope`)

	// recurring.<name> only exists inside a recursive CTE and is a qualified
	// relation everywhere; it must never reach DuckDB.
	for _, q := range []string{
		"WITH logs(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM recurring.logs WHERE n < 5) SELECT n FROM logs",
		"WITH logs(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM logs WHERE n < 5) SELECT n FROM recurring.logs",
	} {
		assertStatementRejected(t, e, q, "qualified relation")
	}
}

// TestSQLSecurityFilesystemEscapes proves every file-reading relation and the
// replacement-scan shortcut fail at the content policy, including inside
// unused and nested CTE bodies and through lateral/scalar subqueries: the
// secret file and the object cache's own parquet path never contribute a row.
func TestSQLSecurityFilesystemEscapes(t *testing.T) {
	secretDir := t.TempDir()
	secret := filepath.Join(secretDir, "secret.txt")
	secretParquet := filepath.Join(secretDir, "secret.parquet")
	for _, p := range []string{secret, secretParquet} {
		if err := os.WriteFile(p, []byte("FILESYSTEM-SECRET-SENTINEL"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// Reachability control: the sentinel files are readable outside the
	// sandbox, so a rejection below really lacks filesystem access and is not
	// a disguised missing-file error.
	if got, err := os.ReadFile(secret); err != nil || !strings.Contains(string(got), "FILESYSTEM-SECRET-SENTINEL") {
		t.Fatalf("secret control = %q, %v", got, err)
	}
	e, env := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())
	cached := env.path(env.files[0].Key)
	if _, err := os.Stat(cached); err != nil {
		t.Fatalf("cached object control: %v", err)
	}

	for _, tc := range []struct {
		name string
		q    string
		want string
	}{
		{"read_csv_auto", "SELECT * FROM read_csv_auto('" + secret + "')", `function "read_csv_auto"`},
		{"read_parquet_secret", "SELECT * FROM read_parquet('" + secretParquet + "')", `function "read_parquet"`},
		{"read_parquet_cached", "SELECT * FROM read_parquet('" + cached + "')", `function "read_parquet"`},
		{"read_blob", "SELECT * FROM read_blob('" + secret + "')", `function "read_blob"`},
		{"read_text", "SELECT * FROM read_text('" + secret + "')", `function "read_text"`},
		{"read_json_auto", "SELECT * FROM read_json_auto('" + secret + "')", `function "read_json_auto"`},
		{"glob", "SELECT * FROM glob('" + secretDir + "/*')", `function "glob"`},
		{"parquet_scan", "SELECT * FROM parquet_scan('" + cached + "')", `function "parquet_scan"`},
		{"sniff_csv", "SELECT * FROM sniff_csv('" + secret + "')", `function "sniff_csv"`},
		{"replacement_scan_secret", "SELECT * FROM '" + secretParquet + "'", "is not the logs relation"},
		{"replacement_scan_cached", "SELECT * FROM '" + cached + "'", "is not the logs relation"},
		{"unused_cte", "WITH c AS (SELECT * FROM read_parquet('" + cached + "')) SELECT 1", `function "read_parquet"`},
		{"nested_unused_cte", "WITH c AS (WITH d AS (SELECT * FROM read_parquet('" + secret + "')) SELECT 1) SELECT 1", `function "read_parquet"`},
		{"lateral", "SELECT * FROM logs, LATERAL (SELECT * FROM read_parquet('" + cached + "')) s", `function "read_parquet"`},
		{"scalar_subquery", "SELECT (SELECT count(*) FROM read_csv_auto('" + secret + "'))", `function "read_csv_auto"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertStatementRejected(t, e, tc.q, tc.want)
		})
	}

	res := sqlQuery(t, e, "SELECT line FROM logs")
	if len(res.Rows) != 1 || res.Rows[0][0] != "IN-RANGE-KEEP" {
		t.Fatalf("positive control = %#v, want the one in-range row", res.Rows)
	}
	assertNoRowsContain(t, res, "SENTINEL")
	assertResultEncodable(t, res)
}

// TestSQLSecurityNetworkAndExtensionEscapes proves network object URLs are
// refused at the policy, ATTACH/INSTALL/LOAD never execute, and table
// functions that would need an extension fail at the disabled autoload layer
// (a different, still exact, rejection class).
func TestSQLSecurityNetworkAndExtensionEscapes(t *testing.T) {
	e, _ := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())
	for _, tc := range []struct {
		name string
		q    string
		want string
	}{
		{"read_csv_http", "SELECT * FROM read_csv_auto('https://example.invalid/x.csv')", `function "read_csv_auto"`},
		{"read_parquet_s3", "SELECT * FROM read_parquet('s3://parkana-test/x.parquet')", `function "read_parquet"`},
		{"read_json_http", "SELECT * FROM read_json_auto('http://127.0.0.1:1/x.json')", `function "read_json_auto"`},
		{"replacement_scan_url", "SELECT * FROM 'https://example.invalid/x.parquet'", "is not the logs relation"},
		{"attach_http", "ATTACH 'https://example.invalid/x.db' AS remote", `must start with SELECT or WITH, leads with "attach"`},
		{"attach_s3", "ATTACH 's3://parkana-test/x.db' AS remote", `must start with SELECT or WITH, leads with "attach"`},
		{"install", "INSTALL httpfs", `must start with SELECT or WITH, leads with "install"`},
		{"install_case_comment", "/* leading */ InStAlL httpfs", `must start with SELECT or WITH, leads with "install"`},
		{"load", "LOAD httpfs", `must start with SELECT or WITH, leads with "load"`},
		{"install_then_select", "INSTALL httpfs; SELECT 1", `must start with SELECT or WITH, leads with "install"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertStatementRejected(t, e, tc.q, tc.want)
		})
	}

	// Extension-backed table functions are not in the sandbox's catalog and
	// autoload is off; the driver error is the disabled-extension layer, not a
	// silent success and not a file read.
	for _, fn := range []string{"delta_scan", "iceberg_scan"} {
		q := "SELECT * FROM " + fn + "('/tmp/does-not-exist')"
		res, err := sqlSecurityQuery(t, e, q)
		if !errors.Is(err, errSQLRejected) {
			t.Fatalf("%s = %v, want errSQLRejected (extension autoload disabled)", fn, err)
		}
		if !strings.Contains(err.Error(), fn) {
			t.Fatalf("%s error %v does not name the function", fn, err)
		}
		if len(res.Rows) != 0 {
			t.Fatalf("%s returned rows despite rejection", fn)
		}
	}

	res := sqlQuery(t, e, "SELECT 1")
	if len(res.Rows) != 1 {
		t.Fatalf("positive control returned %d rows", len(res.Rows))
	}
	assertResultEncodable(t, res)
}

// TestSQLSecurityWriteSideEffects proves COPY/EXPORT/IMPORT/ATTACH/DETACH
// never run: each is rejected as a non-SELECT naming its lead keyword, and no
// target file or directory appears.
func TestSQLSecurityWriteSideEffects(t *testing.T) {
	e, _ := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())
	dir := t.TempDir()
	copyCSV := filepath.Join(dir, "copy.csv")
	copyParquet := filepath.Join(dir, "copy.parquet")
	exportDir := filepath.Join(dir, "export")
	attachDB := filepath.Join(dir, "attached.duckdb")
	for _, tc := range []struct {
		name string
		q    string
		want string
	}{
		{"copy_csv", "COPY (SELECT * FROM logs) TO '" + copyCSV + "'", `must start with SELECT or WITH, leads with "copy"`},
		{"copy_parquet", "COPY logs TO '" + copyParquet + "' (FORMAT parquet)", `must start with SELECT or WITH, leads with "copy"`},
		{"export", "EXPORT DATABASE '" + exportDir + "'", `must start with SELECT or WITH, leads with "export"`},
		{"import", "IMPORT DATABASE '" + dir + "'", `must start with SELECT or WITH, leads with "import"`},
		{"attach", "ATTACH '" + attachDB + "' AS planted", `must start with SELECT or WITH, leads with "attach"`},
		{"attach_memory", "ATTACH ':memory:' AS planted", `must start with SELECT or WITH, leads with "attach"`},
		{"detach", "DETACH planted", `must start with SELECT or WITH, leads with "detach"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertStatementRejected(t, e, tc.q, tc.want)
		})
	}
	for _, p := range []string{copyCSV, copyParquet, exportDir, attachDB} {
		assertPathAbsent(t, p)
	}
	res := sqlQuery(t, e, "SELECT count(*) FROM logs")
	if len(res.Rows) != 1 {
		t.Fatalf("positive control returned %d rows", len(res.Rows))
	}
}

// TestSQLSecurityDDLAndDMLSideEffects proves write statements and batches
// whose dangerous half is not first never execute: after every rejection the
// sandbox still has exactly its one logs table and zero rows, proving no
// CREATE/INSERT/UPDATE/DELETE/DROP/PREPARE/EXECUTE/CALL took effect.
func TestSQLSecurityDDLAndDMLSideEffects(t *testing.T) {
	queries := []struct {
		q    string
		want string
	}{
		{"CREATE TABLE planted(i INT)", `leads with "create"`},
		{"CREATE OR REPLACE TABLE logs AS SELECT 1 AS x", `leads with "create"`},
		{"INSERT INTO logs VALUES (now(), 'planted')", `leads with "insert"`},
		{"UPDATE logs SET line = 'planted'", `leads with "update"`},
		{"DELETE FROM logs", `leads with "delete"`},
		{"DROP TABLE logs", `leads with "drop"`},
		{"TRUNCATE logs", `leads with "truncate"`},
		{"ALTER TABLE logs ADD COLUMN planted INT", `leads with "alter"`},
		{"PREPARE p AS SELECT 1", `leads with "prepare"`},
		{"EXECUTE p", `leads with "execute"`},
		{"CALL pragma_version()", `leads with "call"`},
		{"CREATE SECRET s (TYPE S3, KEY_ID 'x', SECRET 'y')", `leads with "create"`},
		{"SELECT line FROM logs; DROP TABLE logs", "Only SELECT statements can be serialized to json!"},
		{"DROP TABLE logs; SELECT line FROM logs", `leads with "drop"`},
		{"SELECT line FROM logs; CREATE TABLE planted AS SELECT 1", "Only SELECT statements can be serialized to json!"},
		{"WITH c AS (SELECT * FROM logs) INSERT INTO logs SELECT * FROM c", "Only SELECT statements can be serialized to json!"},
		{"SELECT 1; SET memory_limit='1TB'", "Only SELECT statements can be serialized to json!"},
		{"SELECT 1; SELECT 2", "2 statements, want exactly one"},
		{"WITH c AS (SELECT 1) SELECT * FROM c; SELECT 2", "2 statements, want exactly one"},
	}
	res, err := sandboxTrusted(t, func(sb *sqlSandbox) error {
		if err := sb.materialize(t.Context(), nil, time.Unix(0, 0), time.Unix(1, 0)); err != nil {
			return err
		}
		for _, tc := range queries {
			err := sb.validate(t.Context(), tc.q)
			if !errors.Is(err, errSQLStatement) {
				return errors.New("unexpected classification for " + tc.q + ": " + sqlErrText(err))
			}
			if !strings.Contains(err.Error(), tc.want) {
				return errors.New("query " + tc.q + " error does not name " + tc.want + ": " + sqlErrText(err))
			}
		}
		return nil
	}, "SELECT count(*) = 0 FROM logs")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != true {
		t.Fatalf("logs row inventory after rejected writes = %#v, want exactly zero rows", res.Rows)
	}

	// A separate trusted pass checks the table inventory after the same
	// rejections: exactly one user table, named logs. Counting every user
	// table (not just those named logs) catches a planted extra relation.
	res, err = sandboxTrusted(t, func(sb *sqlSandbox) error {
		if err := sb.materialize(t.Context(), nil, time.Unix(0, 0), time.Unix(1, 0)); err != nil {
			return err
		}
		for _, tc := range queries {
			if err := sb.validate(t.Context(), tc.q); !errors.Is(err, errSQLStatement) {
				return errors.New("unexpected classification for " + tc.q + ": " + sqlErrText(err))
			}
		}
		return nil
	}, "SELECT count(*) = 1 AND bool_and(table_name = 'logs') FROM duckdb_tables() WHERE NOT internal")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != true {
		t.Fatalf("user table inventory after rejected DDL = %#v, want exactly one table named logs", res.Rows)
	}
}

// TestSQLSecuritySettingsIntrospection proves sensitive settings reads and
// writes are policy rejections through the executor, and that rejected
// SET/RESET/PRAGMA statements leave every sandbox setting unchanged (checked
// through the trusted internal path, since user SQL cannot read settings).
func TestSQLSecuritySettingsIntrospection(t *testing.T) {
	e, _ := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())
	for _, tc := range []struct {
		name string
		q    string
		want string
	}{
		{"current_setting", "SELECT current_setting('memory_limit')", `function "current_setting"`},
		{"current_setting_mixed_case", "SELECT CURRENT_SETTING('home_directory')", `function "current_setting"`},
		{"getenv", "SELECT getenv('HOME')", `function "getenv"`},
		{"getvariable", "SELECT getvariable('x')", `function "getvariable"`},
		{"duckdb_settings", "SELECT * FROM duckdb_settings()", `function "duckdb_settings"`},
		{"duckdb_tables", "SELECT * FROM duckdb_tables()", `function "duckdb_tables"`},
		{"duckdb_databases", "SELECT * FROM duckdb_databases()", `function "duckdb_databases"`},
		{"pragma_function", "SELECT * FROM pragma_version()", `function "pragma_version"`},
		{"set", "SET memory_limit='1TB'", `must start with SELECT or WITH, leads with "set"`},
		{"reset", "RESET memory_limit", `must start with SELECT or WITH, leads with "reset"`},
		{"pragma_statement", "PRAGMA database_list", `must start with SELECT or WITH, leads with "pragma"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertStatementRejected(t, e, tc.q, tc.want)
		})
	}

	for _, probe := range []struct {
		q    string
		want any
	}{
		{"SELECT current_setting('memory_limit') = '1.0 GiB'", true},
		{"SELECT current_setting('enable_external_access')", false},
		{"SELECT current_setting('lock_configuration')", true},
		{"SELECT current_setting('temp_directory') = ''", true},
		{"SELECT current_setting('allow_persistent_secrets')", false},
		{"SELECT current_setting('autoinstall_known_extensions')", false},
		{"SELECT current_setting('autoload_known_extensions')", false},
	} {
		res, err := sandboxTrusted(t, func(sb *sqlSandbox) error {
			for _, q := range []string{
				"SET memory_limit='1TB'",
				"RESET memory_limit",
				"PRAGMA database_list",
				"SELECT 1; SET enable_external_access=true",
			} {
				if err := sb.validate(t.Context(), q); !errors.Is(err, errSQLStatement) {
					return errors.New("unexpected classification for " + q + ": " + sqlErrText(err))
				}
			}
			return nil
		}, probe.q)
		if err != nil {
			t.Fatalf("probe %q: %v", probe.q, err)
		}
		if len(res.Rows) != 1 || res.Rows[0][0] != probe.want {
			t.Fatalf("probe %q = %#v, want %v", probe.q, res.Rows, probe.want)
		}
	}
}

// TestSQLSecuritySystemRelationsAndScope proves database/system relations are
// unreachable in every position, while CTE declarations that actually are in
// scope remain usable. Scope escapes name the offending relation; qualified
// names name the qualification.
func TestSQLSecuritySystemRelationsAndScope(t *testing.T) {
	e, _ := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())
	const (
		relMsg  = `is not the logs relation or a CTE visible at this scope`
		qualMsg = "qualified relation"
		fnMsg   = `function "duckdb_databases"`
	)
	for _, tc := range []struct {
		name string
		q    string
		want string
	}{
		{"pg_settings", "SELECT * FROM pg_settings", relMsg},
		{"sqlite_master", "SELECT * FROM sqlite_master", relMsg},
		{"information_schema", "SELECT * FROM information_schema.tables", qualMsg},
		{"pg_catalog", "SELECT * FROM pg_catalog.pg_class", qualMsg},
		{"qualified_logs", "SELECT * FROM main.logs", qualMsg},
		{"qualified_information_logs", "SELECT * FROM information_schema.logs", qualMsg},
		{"duckdb_databases", "SELECT * FROM duckdb_databases()", fnMsg},
		{"join_system", "SELECT * FROM logs, pg_settings", relMsg},
		{"subquery_system", "SELECT * FROM logs WHERE EXISTS (SELECT 1 FROM pg_settings)", relMsg},
		{"unused_cte_system", "WITH c AS (SELECT * FROM pg_settings) SELECT 1", relMsg},
		{"forward_reference", "WITH a AS (SELECT * FROM b), b AS (SELECT 1) SELECT * FROM a", `relation "b"`},
		{"inner_scope_escape", "SELECT * FROM pg_settings WHERE EXISTS (WITH pg_settings AS (SELECT 1) SELECT 1)", relMsg},
		{"sibling_escape", "SELECT * FROM logs WHERE EXISTS (WITH sqlite_master AS (SELECT 1) SELECT 1) AND EXISTS (SELECT * FROM sqlite_master)", `relation "sqlite_master"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertStatementRejected(t, e, tc.q, tc.want)
		})
	}

	// In-scope declarations and harmless shadowing keep working.
	for _, tc := range []struct {
		name string
		q    string
		rows int
	}{
		{"shadow_system_name", "WITH pg_settings AS (SELECT 1) SELECT * FROM pg_settings", 1},
		{"shadow_logs", "WITH logs AS (SELECT 1 AS x) SELECT * FROM logs", 1},
		{"alias_logs", "WITH c AS (SELECT 1) SELECT * FROM c AS logs", 1},
		{"prior_sibling", "WITH a AS (SELECT 1), b AS (SELECT * FROM a) SELECT * FROM b", 1},
		{"nested_in_scope", "WITH c AS (SELECT 1 AS x) SELECT * FROM (WITH d AS (SELECT * FROM c) SELECT * FROM d) s", 1},
		{"quoted_logs", `SELECT * FROM "LOGS"`, 1},
		{"body_scans_base", "WITH logs AS (SELECT line FROM logs) SELECT count(*) FROM logs", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := sqlSecurityQuery(t, e, tc.q)
			if err != nil {
				t.Fatalf("%s = %v, want success", tc.q, err)
			}
			if len(res.Rows) != tc.rows {
				t.Fatalf("%s returned %d rows, want %d", tc.q, len(res.Rows), tc.rows)
			}
			assertResultEncodable(t, res)
		})
	}
}

// TestSQLSecurityIndirection proves dynamic-SQL entry points are rejected in
// every position: scalar, table function, CTE body and lateral subquery.
func TestSQLSecurityIndirection(t *testing.T) {
	e, _ := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())
	for _, tc := range []struct {
		name string
		q    string
		want string
	}{
		{"query_scalar", "SELECT query('SELECT 1')", `function "query"`},
		{"query_from", "SELECT * FROM query('SELECT * FROM logs')", `function "query"`},
		{"query_table", "SELECT * FROM query_table('logs')", `function "query_table"`},
		{"json_execute", "SELECT * FROM json_execute_serialized_sql('{}')", `function "json_execute_serialized_sql"`},
		{"query_unused_cte", "WITH c AS (SELECT * FROM query('SELECT 1')) SELECT 1", `function "query"`},
		{"query_in_subquery", "SELECT * FROM logs WHERE line IN (SELECT * FROM query('SELECT line FROM logs'))", `function "query"`},
		{"query_lateral", "SELECT * FROM logs, LATERAL (SELECT * FROM query('SELECT 1')) s", `function "query"`},
		{"query_in_where", "SELECT * FROM logs WHERE line = query('SELECT 1')", `function "query"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertStatementRejected(t, e, tc.q, tc.want)
		})
	}
}

// TestSQLSecurityOtherTenantAndTimeSentinels proves the materialized relation
// is the only reachable data: another tenant's planted file, and the same
// tenant's out-of-window rows in a listed file, never appear, while the
// legitimate per-tenant and in-window request still works.
func TestSQLSecurityOtherTenantAndTimeSentinels(t *testing.T) {
	rw := func(ts time.Time, line string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "api"})}
	}
	files := []plantest.File{
		{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{
			rw(plantest.T0.Add(-time.Second), "BEFORE-WINDOW-SENTINEL"),
			rw(plantest.T0, "IN-RANGE-KEEP"),
			rw(plantest.T0.Add(time.Second), "AFTER-WINDOW-SENTINEL"),
		}},
		{Tenant: plantest.OtherTenant, Stream: "api", Rows: []plan.Row{
			rw(plantest.T0, "OTHER-TENANT-SENTINEL"),
		}},
	}
	e, env := openSQLExecEnv(t, files, testSQLOptions())
	from, to := plantest.T0.Add(-500*time.Millisecond), plantest.T0.Add(500*time.Millisecond)

	// Reachability control: the planted files exist in the cache.
	for _, f := range env.files {
		if _, err := os.Stat(env.path(f.Key)); err != nil {
			t.Fatalf("cached object control %s: %v", f.Key, err)
		}
	}

	res, err := e.Query(t.Context(), sqlRequest{Tenant: tenant.Default, Query: "SELECT line FROM logs ORDER BY _ts", From: from, To: to})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != "IN-RANGE-KEEP" {
		t.Fatalf("in-window result = %#v, want only IN-RANGE-KEEP", res.Rows)
	}
	assertNoRowsContain(t, res, "SENTINEL")
	assertResultEncodable(t, res)

	otherCached := env.path(env.files[1].Key)
	mineCached := env.path(env.files[0].Key)
	for _, tc := range []struct {
		q    string
		want string
	}{
		{"SELECT * FROM read_parquet('" + otherCached + "')", `function "read_parquet"`},
		{"SELECT * FROM read_csv_auto('" + otherCached + "')", `function "read_csv_auto"`},
		{"SELECT * FROM '" + otherCached + "'", "is not the logs relation"},
		{"SELECT * FROM read_parquet('" + mineCached + "')", `function "read_parquet"`},
		{"SELECT * FROM '" + mineCached + "'", "is not the logs relation"},
		{"SELECT * FROM read_parquet(['" + otherCached + "', '" + mineCached + "'])", `function "read_parquet"`},
		{"SELECT * FROM parquet_scan('" + mineCached + "')", `function "parquet_scan"`},
		{"SELECT * FROM glob('" + filepath.Dir(mineCached) + "/*.parquet')", `function "glob"`},
	} {
		assertStatementRejected(t, e, tc.q, tc.want)
	}

	// The other tenant's own legitimate request sees only its sentinel, never
	// the default tenant's in-window or out-of-window rows.
	other, err := e.Query(t.Context(), sqlRequest{Tenant: plantest.OtherTenant, Query: "SELECT line FROM logs ORDER BY _ts", From: from, To: to})
	if err != nil {
		t.Fatal(err)
	}
	if len(other.Rows) != 1 || other.Rows[0][0] != "OTHER-TENANT-SENTINEL" {
		t.Fatalf("other-tenant result = %#v, want only its own row", other.Rows)
	}
	assertNoRowsContain(t, other, "BEFORE-WINDOW-SENTINEL")
	assertNoRowsContain(t, other, "AFTER-WINDOW-SENTINEL")
	assertResultEncodable(t, other)
}

// TestSQLSecurityLexicalSmuggling proves comments, casing, quoted semicolons,
// E-strings, dollar quotes and CR/LF handling cannot hide a dangerous
// statement, while inert strings and quoted identifiers still work.
func TestSQLSecurityLexicalSmuggling(t *testing.T) {
	e, _ := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())
	for _, tc := range []struct {
		name string
		q    string
		want string
	}{
		{"leading_comment_case", "/* hidden */ SeLeCt * FROM pg_settings", `relation "pg_settings"`},
		{"nested_comment", "/* /* nested */ */ SELECT * FROM pg_settings", `relation "pg_settings"`},
		{"comment_semicolon_batch", "SELECT 1 /* ; */ ; SELECT * FROM pg_settings", "2 statements"},
		{"line_comment_batch", "SELECT 'x' -- ;\n; SELECT * FROM pg_settings", "2 statements"},
		{"cr_comment_relation", "SELECT 1 -- comment\rFROM pg_settings", `relation "pg_settings"`},
		{"cr_comment_batch", "SELECT 1 -- comment\r; SELECT * FROM pg_settings", "2 statements"},
		{"e_string_batch", "SELECT E'it\\'s' ; SELECT * FROM pg_settings", "2 statements"},
		{"cte_shadow_system_name", "WITH pg_settings AS (SELECT * FROM pg_settings) SELECT * FROM pg_settings", `relation "pg_settings"`},
		{"qualified_logs", "SELECT * FROM main.logs", "qualified relation"},
		{"qualified_memory_logs", "SELECT * FROM memory.logs", "qualified relation"},
		{"batch_two_selects", "SELECT 1; SELECT 2", "2 statements"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertStatementRejected(t, e, tc.q, tc.want)
		})
	}

	// Inert lexical constructs: the dangerous-looking text is data, not a
	// statement, and each query runs against only the logs relation.
	for _, tc := range []struct {
		name string
		q    string
		want string
	}{
		{"semicolon_in_string", "SELECT 'a;b'", "a;b"},
		{"dollar_quote", "SELECT $$a; SELECT * FROM pg_settings$$", "a; SELECT * FROM pg_settings"},
		{"tagged_dollar_quote", "SELECT $tag$a; SELECT * FROM pg_settings$tag$", "a; SELECT * FROM pg_settings"},
		{"unicode_dollar_quote", "SELECT $é$; SELECT * FROM pg_settings$é$", "; SELECT * FROM pg_settings"},
		{"vt_stays_in_comment", "SELECT 'comment-safe' -- comment\vFROM pg_settings", "comment-safe"},
		{"quoted_ident_upper", `SELECT * FROM "LOGS"`, ""},
		{"trailing_semicolon", "SELECT line FROM logs;", "IN-RANGE-KEEP"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := sqlSecurityQuery(t, e, tc.q)
			if err != nil {
				t.Fatalf("inert query %q = %v, want success", tc.q, err)
			}
			if tc.want != "" && (len(res.Rows) != 1 || res.Rows[0][0] != tc.want) {
				t.Fatalf("inert query %q = %#v, want %q", tc.q, res.Rows, tc.want)
			}
			assertResultEncodable(t, res)
		})
	}
}

// TestSQLSecurityExplicitRecursionRejected proves the reviewed WITH RECURSIVE
// lexical ban rejects every explicit recursive CTE with errSQLRecursive before
// DuckDB parses the text: name shadowing, USING KEY, comments/casing, nesting,
// system-relation bodies and recurring.* are all the same ban, and the query
// never executes a row. Implicit recursion (no RECURSIVE keyword) is covered
// separately by TestSQLSecurityImplicitRecursionRejected, where the binder and
// scope layers are the authority.
func TestSQLSecurityExplicitRecursionRejected(t *testing.T) {
	e, _ := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())

	for _, tc := range []struct {
		name string
		q    string
	}{
		{"named_logs", "WITH RECURSIVE logs(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM logs WHERE n < 5) SELECT n FROM logs ORDER BY n"},
		{"using_key", "WITH RECURSIVE logs(n) USING KEY (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM logs WHERE n < 5) SELECT n FROM logs ORDER BY n"},
		{"using_key_aggregate", "WITH RECURSIVE logs(n, m) USING KEY (n, max(m)) AS (SELECT 1, 1 UNION ALL SELECT n + 1, n + 1 FROM logs WHERE n < 5) SELECT n FROM logs ORDER BY n"},
		{"lowercase", "with recursive logs(n) as (select 1 union all select n + 1 from logs where n < 5) select n from logs"},
		{"comment_between", "WITH /* hidden */ RECURSIVE logs(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM logs WHERE n < 5) SELECT n FROM logs"},
		{"line_comment_between", "WITH -- hidden\nRECURSIVE logs(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM logs WHERE n < 5) SELECT n FROM logs"},
		{"nested_subquery", "SELECT * FROM (WITH RECURSIVE logs(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM logs WHERE n < 5) SELECT n FROM logs) s"},
		{"non_logs", "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t WHERE n < 5) SELECT n FROM t"},
		{"system_body", "WITH RECURSIVE logs(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM pg_settings WHERE n < 5) SELECT n FROM logs"},
		{"recurring_reference", "WITH RECURSIVE logs(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM recurring.logs WHERE n < 5) SELECT n FROM logs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, elapsed, err := sqlBoundedQuery(t, e, tc.q, 5, time.Second)
			if !errors.Is(err, errSQLRecursive) {
				t.Fatalf("%s = %v, want errSQLRecursive", tc.q, err)
			}
			if !errors.Is(err, errSQLStatement) {
				t.Fatalf("%s = %v, want the ban to keep wrapping errSQLStatement", tc.q, err)
			}
			if len(res.Rows) != 0 {
				t.Fatalf("%s returned %d rows despite the recursion ban", tc.q, len(res.Rows))
			}
			if elapsed > time.Second {
				t.Fatalf("%s took %s, want under the finite 1s context", tc.q, elapsed)
			}
		})
	}

	// The ban is lexical, not a false positive on inert text: the words in a
	// string, a quoted identifier or a comment are not the token pair.
	for _, q := range []string{
		"SELECT 'WITH RECURSIVE'",
		"SELECT $$WITH RECURSIVE$$",
		"WITH /* RECURSIVE */ c AS (SELECT 1 AS x) SELECT * FROM c",
		`WITH "RECURSIVE" AS (SELECT 1 AS x) SELECT * FROM "RECURSIVE"`,
	} {
		res, err := sqlSecurityQuery(t, e, q)
		if err != nil {
			t.Fatalf("inert recursion text %q = %v, want success", q, err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("inert recursion text %q returned %d rows", q, len(res.Rows))
		}
	}
}

// TestSQLAPISecurityRealDataAndPolicy drives the registered route through the
// real executor and on-disk fixture: tenant and time sentinels never cross the
// response, and filesystem/settings/batch/recursive/qualified SQL is a bounded
// 400 that names the public class without echoing the query.
func TestSQLAPISecurityRealDataAndPolicy(t *testing.T) {
	deps := newTestDeps(t, func(c *config.Config) { c.MultiTenant = true })
	// The shared test deps build their server before mutate runs; rebuild the
	// server and registry so the multi-tenant middleware is actually enabled.
	reg := prometheus.NewRegistry()
	deps.Metrics = reg
	deps.HTTP = server.New(server.Options{Logger: deps.Logger, Registry: reg, MultiTenant: true})
	loadSQLHTTPFixture(t, deps.Store, deps.Catalog, sqlHTTPFixture())
	newTestService(t, deps, &recordingEngine{})
	if !deps.HTTP.Registered(sqlAPIPattern) {
		t.Fatalf("route %q is not registered", sqlAPIPattern)
	}
	handler := deps.HTTP.Handler()

	rec := serveSQL(t, handler, t.Context(), sqlValidBody("SELECT line FROM logs ORDER BY _ts"))
	if rec.Code != http.StatusOK {
		t.Fatalf("default-tenant status = %d, want 200; body = %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "IN-RANGE-KEEP") {
		t.Fatalf("default-tenant response = %q, want the in-window row", rec.Body.String())
	}
	for _, leak := range []string{"OUT-OF-WINDOW-SENTINEL", "AFTER-WINDOW-SENTINEL", "OTHER-TENANT-SENTINEL"} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Fatalf("default-tenant response leaked %q: %q", leak, rec.Body.String())
		}
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("content type = %q, want application/json", got)
	}

	otherReq := httptest.NewRequest(http.MethodPost, "/api/v1/sql", strings.NewReader(sqlValidBody("SELECT line FROM logs ORDER BY _ts")))
	otherReq.Header.Set(tenant.Header, string(plantest.OtherTenant))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, otherReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("other-tenant status = %d, want 200; body = %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "OTHER-TENANT-SENTINEL") {
		t.Fatalf("other-tenant response = %q, want its own row", rec.Body.String())
	}
	for _, leak := range []string{"IN-RANGE-KEEP", "OUT-OF-WINDOW-SENTINEL", "AFTER-WINDOW-SENTINEL"} {
		if strings.Contains(rec.Body.String(), leak) {
			t.Fatalf("other-tenant response leaked %q: %q", leak, rec.Body.String())
		}
	}

	for _, tc := range []struct {
		name  string
		query string
		want  string
		leak  string
	}{
		{"filesystem", "SELECT * FROM read_csv_auto('/etc/passwd')", sqlPublicStatement, "/etc/passwd"},
		{"settings", "SELECT current_setting('memory_limit')", sqlPublicStatement, "memory_limit"},
		{"batch", "SELECT 1; SELECT 2", sqlPublicStatement, "SELECT 2"},
		{"recursive", "WITH RECURSIVE logs(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM logs WHERE n < 5) SELECT n FROM logs", sqlPublicRecursive, "WITH RECURSIVE"},
		{"qualified", "SELECT * FROM main.logs", sqlPublicStatement, "main.logs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := serveSQL(t, handler, t.Context(), sqlValidBody(tc.query))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body = %q", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.want) {
				t.Fatalf("body = %q, want public class %q", rec.Body.String(), tc.want)
			}
			if strings.Contains(rec.Body.String(), tc.leak) {
				t.Fatalf("body leaked %q: %q", tc.leak, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), `"columns"`) {
				t.Fatalf("rejection leaked partial success JSON: %q", rec.Body.String())
			}
		})
	}
}

// TestSQLAPISecurityRealCatalogFaultAndDeadline keeps genuine executor faults
// sanitized and proves a timed-out request still leaves the endpoint usable:
// a catalog row whose object is missing is a 500 with no path or key detail,
// and a real deadline 504 is followed by a successful query over the fixture.
func TestSQLAPISecurityRealCatalogFaultAndDeadline(t *testing.T) {
	t.Run("catalog object missing", func(t *testing.T) {
		deps := newTestDeps(t, nil)
		at := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		key := objstore.DataKey(tenant.Default, "api", at, uuid.NewV7())
		if err := deps.Catalog.AddFiles(t.Context(), catalog.File{
			Tenant: tenant.Default, Stream: "api", Key: key,
			MinTS: at, MaxTS: at, Rows: 1, Bytes: 1,
		}); err != nil {
			t.Fatal(err)
		}
		newTestService(t, deps, &recordingEngine{})

		rec := serveSQL(t, deps.HTTP.Handler(), t.Context(), sqlValidBody("SELECT 1"))
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("missing-object status = %d, want 500; body = %q", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), sqlInternalMessage) {
			t.Fatalf("body = %q, want the sanitized internal message", rec.Body.String())
		}
		for _, leak := range []string{key, deps.Config.DataDir, "acquire"} {
			if strings.Contains(rec.Body.String(), leak) {
				t.Fatalf("internal response leaked %q: %q", leak, rec.Body.String())
			}
		}
	})

	t.Run("deadline then data recovery", func(t *testing.T) {
		deps := newTestDeps(t, func(c *config.Config) {
			c.SQL = config.SQL{
				MaxRows:        10000,
				DefaultLimit:   1000,
				MaxResultBytes: 8 * config.MiB,
				Timeout:        100 * time.Millisecond,
				MemoryLimit:    512 * config.MiB,
				Threads:        1,
				MaxConcurrent:  2,
			}
		})
		loadSQLHTTPFixture(t, deps.Store, deps.Catalog, sqlHTTPFixture())
		newTestService(t, deps, &recordingEngine{})
		handler := deps.HTTP.Handler()

		rec := serveSQL(t, handler, t.Context(), sqlValidBody("SELECT count(*) FROM range(100000000000)"))
		if rec.Code != http.StatusGatewayTimeout {
			t.Fatalf("timed-out status = %d, want 504; body = %q", rec.Code, rec.Body.String())
		}

		rec = serveSQL(t, handler, t.Context(), sqlValidBody("SELECT line FROM logs ORDER BY _ts"))
		if rec.Code != http.StatusOK {
			t.Fatalf("post-timeout status = %d, want 200; body = %q", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "IN-RANGE-KEEP") {
			t.Fatalf("post-timeout response = %q, want the fixture row", rec.Body.String())
		}
	})
}
