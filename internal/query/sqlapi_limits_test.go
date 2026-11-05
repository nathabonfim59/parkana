package query

// Independent limit tests of the SQL API: the lexical text cap and nesting
// depth are enforced before any catalog or object resource is touched, the
// row cap truncates rather than overflows, and the memory and wall-clock
// budgets fail with their own typed errors while a later request still works.
// Every potentially expensive statement runs under a finite context deadline.

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// rowsFile builds one default-tenant file of n rows, each with a distinct
// line, all inside the standard test span.
func rowsFile(n int) []plantest.File {
	rows := make([]plan.Row, n)
	for i := range rows {
		rows[i] = plan.Row{
			TS:     plantest.T0.Add(time.Duration(i) * time.Second),
			Line:   "ROW-" + string(rune('A'+i)),
			Labels: logs.FromMap(map[string]string{"service_name": "api"}),
		}
	}
	return []plantest.File{{Tenant: tenant.Default, Stream: "api", Rows: rows}}
}

// sqlLimitQuery runs q with an explicit result limit over the standard span
// under a finite five-second context: no limit test may depend on the process
// staying alive to bound a query's cost.
func sqlLimitQuery(t *testing.T, e *sqlExec, q string, limit int) (sqlResult, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	return e.Query(ctx, sqlRequest{
		Tenant: tenant.Default, Query: q, Limit: limit,
		From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
	})
}

// TestSQLLimitsTextCap proves the 64 KiB byte cap is exact, and an over-cap
// query is rejected before the catalog is even listed.
func TestSQLLimitsTextCap(t *testing.T) {
	_, env := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())
	exact := "SELECT '" + strings.Repeat("a", sqlMaxQueryBytes-len("SELECT ''")) + "'"
	if len(exact) != sqlMaxQueryBytes {
		t.Fatalf("fixture length = %d, want %d", len(exact), sqlMaxQueryBytes)
	}

	cat := &listCountingCatalog{Catalog: env.cat}
	e, err := newSQLExec(testSQLOptions(), cat, fixtureResolver{env.dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := sqlLimitQuery(t, e, exact, 1)
	if err != nil {
		t.Fatalf("query of exactly %d bytes = %v, want success", sqlMaxQueryBytes, err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("exact-cap query returned %d rows", len(res.Rows))
	}

	// One byte over: rejected at preflight, and the counting catalog proves
	// the cap ran before any listing or lease.
	over := &listCountingCatalog{Catalog: env.cat}
	eOver, err := newSQLExec(testSQLOptions(), over, fixtureResolver{env.dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err = sqlLimitQuery(t, eOver, exact+" ", 1)
	if !errors.Is(err, errSQLText) {
		t.Fatalf("over-cap query = %v, want errSQLText", err)
	}
	if !strings.Contains(err.Error(), "65536-byte SQL text cap") {
		t.Fatalf("over-cap error %v does not name the 65536-byte cap", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("over-cap query returned %d rows", len(res.Rows))
	}
	if got := over.count(); got != 0 {
		t.Fatalf("over-cap query listed the catalog %d times, want 0", got)
	}
}

// TestSQLLimitsLexicalDepth proves the lexical depth cap rejects at 1001
// before DuckDB, while 1000 reaches DuckDB's own max_expression_depth guard;
// parentheses inside strings and comments never count.
func TestSQLLimitsLexicalDepth(t *testing.T) {
	_, env := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())

	over := &listCountingCatalog{Catalog: env.cat}
	e, err := newSQLExec(testSQLOptions(), over, fixtureResolver{env.dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	deep := "SELECT " + strings.Repeat("(", sqlMaxLexDepth+1) + "1" + strings.Repeat(")", sqlMaxLexDepth+1)
	res, err := sqlLimitQuery(t, e, deep, 1)
	if !errors.Is(err, errSQLText) {
		t.Fatalf("depth %d = %v, want errSQLText", sqlMaxLexDepth+1, err)
	}
	if !strings.Contains(err.Error(), "nesting depth 1001 exceeds the maximum of 1000") {
		t.Fatalf("depth error %v does not name the 1001/1000 boundary", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("over-depth query returned %d rows", len(res.Rows))
	}
	if got := over.count(); got != 0 {
		t.Fatalf("over-depth query listed the catalog %d times, want 0", got)
	}

	// Exactly the boundary passes preflight but DuckDB's own depth guard
	// rejects it: a second, independent layer.
	atLimit := "SELECT " + strings.Repeat("(", sqlMaxLexDepth) + "1" + strings.Repeat(")", sqlMaxLexDepth)
	if _, err := sqlLimitQuery(t, e, atLimit, 1); !errors.Is(err, errSQLStatement) {
		t.Fatalf("depth %d = %v, want DuckDB errSQLStatement", sqlMaxLexDepth, err)
	} else if !strings.Contains(err.Error(), "Max expression depth limit of 1000 exceeded") {
		t.Fatalf("depth %d error %v does not name DuckDB's limit", sqlMaxLexDepth, err)
	}

	// Parentheses in a string and in a comment are text, not nesting.
	for _, q := range []string{
		"SELECT '" + strings.Repeat("(", 5000) + "'",
		"SELECT 1 /* " + strings.Repeat("(", 5000) + " */",
	} {
		if _, err := sqlLimitQuery(t, e, q, 1); err != nil {
			t.Fatalf("inert nesting %q = %v, want success", q[:24], err)
		}
	}
}

// TestSQLLimitsRowCapAndTruncation proves the response never exceeds the row
// cap: the limit+1th row only sets truncated and is dropped, the default
// limit applies when the request omits one, and an out-of-range explicit
// limit is rejected before listing.
func TestSQLLimitsRowCapAndTruncation(t *testing.T) {
	opts := testSQLOptions()
	opts.MaxRows = 3
	opts.DefaultLimit = 2
	e, env := openSQLExecEnv(t, rowsFile(5), opts)
	for _, tc := range []struct {
		name      string
		limit     int
		wantRows  int
		truncated bool
	}{
		{"default limit", 0, 2, true},
		{"explicit below cap", 2, 2, true},
		{"explicit at cap", 3, 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := sqlLimitQuery(t, e, "SELECT line FROM logs ORDER BY _ts", tc.limit)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Rows) != tc.wantRows || res.Truncated != tc.truncated {
				t.Fatalf("limit %d: %d rows truncated=%v, want %d/%v", tc.limit, len(res.Rows), res.Truncated, tc.wantRows, tc.truncated)
			}
		})
	}

	exact, _ := openSQLExecEnv(t, rowsFile(3), opts)
	res, err := sqlLimitQuery(t, exact, "SELECT line FROM logs ORDER BY _ts", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 3 || res.Truncated {
		t.Fatalf("exact-fit limit: %d rows truncated=%v, want 3/false", len(res.Rows), res.Truncated)
	}

	for _, limit := range []int{4, -1} {
		cat := &listCountingCatalog{Catalog: env.cat}
		eOver, err := newSQLExec(opts, cat, fixtureResolver{env.dir}, nil)
		if err != nil {
			t.Fatal(err)
		}
		res, err := sqlLimitQuery(t, eOver, "SELECT line FROM logs", limit)
		if !errors.Is(err, errSQLLimits) {
			t.Fatalf("limit %d = %v, want errSQLLimits", limit, err)
		}
		if !strings.Contains(err.Error(), "above the 3-row maximum") {
			t.Fatalf("limit %d error %v does not name the row maximum", limit, err)
		}
		if len(res.Rows) != 0 {
			t.Fatalf("limit %d returned %d rows despite rejection", limit, len(res.Rows))
		}
		if got := cat.count(); got != 0 {
			t.Fatalf("limit %d listed the catalog %d times, want 0", limit, got)
		}
	}
}

// TestSQLLimitsMemory proves a query that exceeds the buffer-manager budget
// fails as the typed memory-limit error (not a generic SQL error) and that
// spill is off, then that a later ordinary request still works.
func TestSQLLimitsMemory(t *testing.T) {
	opts := testSQLOptions()
	opts.MemoryLimitBytes = 16 << 20
	e, _ := openSQLExecEnv(t, oneKeepFile(), opts)

	// The aggregate cannot be answered in the budget and there is no spill
	// directory, so DuckDB reports Out of Memory; the executor classifies it.
	res, err := sqlLimitQuery(t, e, "SELECT string_agg(repeat('x', 1000), '') FROM range(1000000)", 1)
	if !errors.Is(err, errSQLMemory) {
		t.Fatalf("memory-heavy query = %v, want errSQLMemory", err)
	}
	for _, want := range []string{"sql.memory-limit", "temporary directory"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("memory error %v does not mention %q", err, want)
		}
	}
	if len(res.Rows) != 0 {
		t.Fatalf("memory-heavy query returned %d rows", len(res.Rows))
	}

	res, err = sqlLimitQuery(t, e, "SELECT 42", 1)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("recovery query = %d rows, %v; want 1 row, nil", len(res.Rows), err)
	}
}

// TestSQLLimitsDeadlineDuringQueryAndRecovery proves the request deadline
// interrupts a query that has started executing (the classified error names
// the execute step, not startup), returns promptly, and leaves the executor
// usable for the next request.
func TestSQLLimitsDeadlineDuringQueryAndRecovery(t *testing.T) {
	e, _ := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())
	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	start := time.Now()
	res, err := e.Query(ctx, sqlRequest{
		Tenant: tenant.Default, Query: "SELECT sum(i) FROM range(2000000000) t(i)", Limit: 1,
		From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
	})
	elapsed := time.Since(start)
	cancel()

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out query = %v, want context.DeadlineExceeded", err)
	}
	if !strings.Contains(err.Error(), "execute:") {
		t.Fatalf("timed-out query = %v, want the execute-step error (proves query started)", err)
	}
	if elapsed < 150*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("timed-out query took %s, want it interrupted near the 250ms deadline", elapsed)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("timed-out query returned %d rows", len(res.Rows))
	}

	res, err = sqlLimitQuery(t, e, "SELECT 42", 1)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("recovery query = %d rows, %v; want 1 row, nil", len(res.Rows), err)
	}
}

// TestSQLLimitsPreCanceledContext proves an already-canceled request never
// walks the catalog or leases an object.
func TestSQLLimitsPreCanceledContext(t *testing.T) {
	_, env := openSQLExecEnv(t, oneKeepFile(), testSQLOptions())
	cat := &listCountingCatalog{Catalog: env.cat}
	e, err := newSQLExec(testSQLOptions(), cat, fixtureResolver{env.dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = e.Query(ctx, sqlRequest{
		Tenant: tenant.Default, Query: "SELECT 1",
		From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled request = %v, want context.Canceled", err)
	}
	if got := cat.count(); got != 0 {
		t.Fatalf("canceled request listed the catalog %d times, want 0", got)
	}
}

// TestSQLAPILimitsRealEndpoint proves the registered endpoint's body, SQL text
// and explicit-limit bounds are controlled 400s, and that the result byte cap
// rejects a modest oversized scalar before any success body while releasing
// the single admission slot for the next request. Metrics record every failure
// and return inflight to zero.
func TestSQLAPILimitsRealEndpoint(t *testing.T) {
	deps := newTestDeps(t, func(c *config.Config) {
		c.SQL = config.SQL{
			MaxRows:        100,
			DefaultLimit:   10,
			MaxResultBytes: 4096,
			Timeout:        2 * time.Second,
			MemoryLimit:    config.GiB,
			Threads:        1,
			MaxConcurrent:  1,
		}
	})
	newTestService(t, deps, &recordingEngine{})
	reg, ok := deps.Metrics.(*prometheus.Registry)
	if !ok {
		t.Fatalf("metrics = %T, want *prometheus.Registry", deps.Metrics)
	}
	handler := deps.HTTP.Handler()

	limitBody := strings.Replace(sqlValidBody("SELECT 1"), "}", `,"limit":101}`, 1)
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"body cap", sqlValidBody(strings.Repeat("a", sqlAPIMaxBodyBytes)), "request body exceeds the 1048576-byte limit"},
		{"text cap", sqlValidBody(strings.Repeat("a", sqlMaxQueryBytes+1)), "65536-byte SQL text cap"},
		{"explicit limit", limitBody, `field "limit" must be between 1 and 100`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := serveSQL(t, handler, t.Context(), tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body = %q", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.want) {
				t.Fatalf("body = %q, want %q", rec.Body.String(), tc.want)
			}
			if strings.Contains(rec.Body.String(), `"columns"`) || strings.Contains(rec.Body.String(), `"rows"`) {
				t.Fatalf("limit rejection leaked success JSON: %q", rec.Body.String())
			}
		})
	}
	if got := sqlMetricValue(t, reg, "parkana_sql_requests_total", map[string]string{"outcome": "client_error"}); got != 3 {
		t.Errorf("client_error requests = %v, want 3", got)
	}

	// A real oversized scalar: the encoded result exceeds the 4096-byte cap, so
	// the request is a 400 with no partial success body.
	rec := serveSQL(t, handler, t.Context(), sqlValidBody("SELECT repeat('x', 8192)"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("oversized scalar status = %d, want 400; body = %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), sqlPublicTooLarge) {
		t.Fatalf("body = %q, want the result-byte public class", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), `"columns"`) || strings.Contains(rec.Body.String(), `"rows"`) {
		t.Fatalf("oversized scalar leaked partial success JSON: %q", rec.Body.String())
	}
	if len(rec.Body.String()) > 1024 {
		t.Fatalf("oversized scalar error body is %d bytes, want a bounded message", len(rec.Body.String()))
	}

	// The one admission slot is free again and a later normal request succeeds.
	rec = serveSQL(t, handler, t.Context(), sqlValidBody("SELECT 1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("post-overflow status = %d, want 200; body = %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"rows":[[1]]`) {
		t.Fatalf("post-overflow response = %q, want the normal row", rec.Body.String())
	}
	if got := sqlMetricValue(t, reg, "parkana_sql_requests_total", map[string]string{"outcome": "client_error"}); got != 4 {
		t.Errorf("client_error requests = %v, want 4", got)
	}
	if got := sqlMetricValue(t, reg, "parkana_sql_requests_total", map[string]string{"outcome": "success"}); got != 1 {
		t.Errorf("success requests = %v, want 1", got)
	}
	if got := sqlMetricValue(t, reg, "parkana_sql_inflight", nil); got != 0 {
		t.Errorf("inflight = %v, want 0 after recovery", got)
	}
}
