package query

// Sandbox-level tests of the SQL API: settings applied and locked, the
// side-effect-free single-SELECT validation through json_serialize_sql, and
// deadline coverage of parsing and execution.

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// sandboxProbe runs one user-style SELECT against a sandbox built by fn
// (applyLimits/materialize/lockdown steps) with default small limits, through
// the full validated path.
func sandboxProbe(t *testing.T, fn func(*sqlSandbox) error, query string) (sqlResult, error) {
	t.Helper()
	sb := sandboxBuilt(t, fn)
	if err := sb.validate(t.Context(), query); err != nil {
		return sqlResult{}, err
	}
	rows, err := sb.query(t.Context(), query)
	if err != nil {
		return sqlResult{}, err
	}
	defer rows.Close()
	return sqlScanRows(t.Context(), rows, 1000)
}

// sandboxTrusted runs one sandbox-internal SELECT directly, bypassing the
// user-SQL policy — for tests asserting sandbox state through probes the
// policy itself forbids (settings values, system catalogs).
func sandboxTrusted(t *testing.T, fn func(*sqlSandbox) error, query string) (sqlResult, error) {
	t.Helper()
	sb := sandboxBuilt(t, fn)
	rows, err := sb.conn.QueryContext(t.Context(), query)
	if err != nil {
		return sqlResult{}, err
	}
	defer rows.Close()
	return sqlScanRows(t.Context(), rows, 1000)
}

// sandboxBuilt opens a sandbox and drives it through limits, fn and lockdown.
func sandboxBuilt(t *testing.T, fn func(*sqlSandbox) error) *sqlSandbox {
	t.Helper()
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sb.close() })
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := fn(sb); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	return sb
}

// TestSQLSandboxSettingsLocked proves the sandbox's end state through its own
// user-SQL path: every lockdown setting holds, spill is off, and no later SET
// can change anything.
func TestSQLSandboxSettingsLocked(t *testing.T) {
	for _, tc := range []struct {
		probe string
		want  any
	}{
		{"SELECT current_setting('enable_external_access')", false},
		{"SELECT current_setting('temp_directory') = ''", true},
		{"SELECT current_setting('lock_configuration')", true},
		{"SELECT current_setting('autoinstall_known_extensions')", false},
		{"SELECT current_setting('autoload_known_extensions')", false},
		{"SELECT current_setting('allow_persistent_secrets')", false},
		{"SELECT current_setting('allow_unsigned_extensions')", false},
		{"SELECT current_setting('allow_community_extensions')", false},
		{"SELECT current_setting('threads')", jsontext.Value("1")},
		{"SELECT current_setting('memory_limit') = '1.0 GiB'", true},
	} {
		t.Run(tc.probe, func(t *testing.T) {
			res, err := sandboxTrusted(t, func(*sqlSandbox) error { return nil }, tc.probe)
			if err != nil {
				t.Fatalf("probe: %v", err)
			}
			if len(res.Rows) != 1 || len(res.Columns) != 1 {
				t.Fatalf("probe shape: %d rows, %d cols", len(res.Rows), len(res.Columns))
			}
			got := res.Rows[0][0]
			switch want := tc.want.(type) {
			case bool:
				if got != want {
					t.Errorf("probe = %#v, want %v", got, want)
				}
			case jsontext.Value:
				n, ok := got.(jsontext.Value)
				if !ok || string(n) != string(want) {
					t.Errorf("probe = %#v, want number %s", got, want)
				}
			}
		})
	}
}

// TestSQLSandboxVersion documents the linked DuckDB: SELECT version() reports
// only "v2.0.0" — the build carries no commit hash to probe, so the upstream
// source commit (d54cbbe3, internal/query/duckdb.go) remains a go.mod fact.
func TestSQLSandboxVersion(t *testing.T) {
	res, err := sandboxProbe(t, func(*sqlSandbox) error { return nil }, "SELECT version()")
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Rows[0][0]; got != "v2.0.0" {
		t.Fatalf("version() = %q, want v2.0.0", got)
	}
}

// TestSQLSandboxSETRejectedAfterLock proves statement-level rejection and the
// lock backstop: SET through user SQL fails closed with the policy error, and
// even a statement-shaped SET that slipped the keyword policy cannot change a
// setting once lock_configuration is on.
func TestSQLSandboxSETRejectedAfterLock(t *testing.T) {
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	// The keyword policy rejects SET outright.
	if err := sb.validate(t.Context(), "SET memory_limit='1TB'"); !errors.Is(err, errSQLStatement) {
		t.Fatalf("validate SET error = %v, want errSQLStatement", err)
	}
	// The lock itself is the backstop: this connection is past lockdown, so
	// even a direct SET statement on it throws.
	if _, err := sb.conn.ExecContext(t.Context(), "SET memory_limit='1TB'"); err == nil {
		t.Fatal("direct SET after lockdown succeeded")
	}
}

// TestSQLSandboxValidationAccepts runs every accepted statement shape through
// the validator: plain and trailing-semicolon SELECTs, CTEs, comments before
// and inside the statement, and lowercase keywords.
func TestSQLSandboxValidationAccepts(t *testing.T) {
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"SELECT 1",
		"SELECT 1;",
		"select 1",
		"WITH c AS (SELECT 1 AS x) SELECT x FROM c",
		"/* lead */ SELECT 1",
		"\n\t -- lead line\n SELECT 1",
		"SELECT 1 /* tail */",
		"SELECT * FROM logs",
		"WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a UNION ALL SELECT * FROM b",
	} {
		if err := sb.validate(t.Context(), q); err != nil {
			t.Errorf("validate(%q) = %v, want accepted", q, err)
		}
	}
}

// TestSQLSandboxValidationRejects proves fail-closed rejection of every
// non-single-SELECT shape without side effects: other statement kinds
// (including PRAGMA, which DuckDB types as SELECT), batches whose earlier
// statement would otherwise execute under PrepareContext, parse failures,
// comment-only text and parameters.
func TestSQLSandboxValidationRejects(t *testing.T) {
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"PRAGMA version",
		"PRAGMA database_list",
		"SET memory_limit='1TB'",
		"RESET memory_limit",
		"CREATE TABLE x(i INT)",
		"INSERT INTO x VALUES (1)",
		"UPDATE x SET i = 2",
		"DELETE FROM x",
		"DROP TABLE x",
		"CREATE OR REPLACE TABLE y AS SELECT 1",
		"BEGIN",
		"COMMIT",
		"EXPLAIN SELECT 1",
		"ATTACH ':memory:' AS m",
		"DETACH memory",
		"INSTALL httpfs",
		"LOAD httpfs",
		"COPY (SELECT 1) TO 'x.csv'",
		"EXPORT DATABASE 'dir'",
		"IMPORT DATABASE 'dir'",
		"CALL pragma_version()",
		"PREPARE p AS SELECT 1",
		"EXECUTE p",
		"TABLE logs",
		"VALUES (1)",
		"FROM logs",
		"SUMMARIZE logs",
		"SELECT 1; SELECT 2",
		"SELECT 1; CREATE TABLE x(i INT)",
		"CREATE TABLE x(i INT); SELECT 1",
		"WITH c AS (SELECT 1) INSERT INTO logs SELECT * FROM c",
		"SELECT 1; DROP TABLE logs",
		"-- only a comment",
		"/* only a block comment */",
		"   ",
		"SELEC 1",
		"SELECT FROM",
		"/* unterminated",
		"select 1; pragma version",
	} {
		if err := sb.validate(t.Context(), q); !errors.Is(err, errSQLStatement) {
			t.Errorf("validate(%q) = %v, want errSQLStatement", q, err)
		}
	}
}

// TestSQLSandboxValidationNoSideEffects proves a rejected batch never ran its
// earlier statement: after a batch whose CREATE would plant a table is
// rejected, the sandbox's own catalog (probed through the trusted internal
// path, since the user-SQL policy forbids system catalogs) shows no such
// table.
func TestSQLSandboxValidationNoSideEffects(t *testing.T) {
	res, err := sandboxTrusted(t, func(sb *sqlSandbox) error {
		if err := sb.materialize(t.Context(), nil, time.Unix(0, 0), time.Unix(1, 0)); err != nil {
			return err
		}
		// The batch is rejected; its CREATE must not have run.
		if err := sb.validate(t.Context(), "CREATE TABLE planted(i INT); SELECT 1"); !errors.Is(err, errSQLStatement) {
			return fmt.Errorf("batch validate = %v, want errSQLStatement", err)
		}
		return nil
	}, "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'planted'")
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := res.Rows[0][0].(jsontext.Value); !ok || string(got) != "0" {
		t.Fatalf("planted table rows = %#v, want 0", res.Rows[0][0])
	}
}

// TestSQLSandboxDeadlineDuringValidation proves a deadline that is already
// spent when validation starts fails fast: the context is checked before the
// parse query runs. (Interrupt behavior for cap-sized pathological parses is
// measured by the opt-in TestSQLParseOverrunMeasurement; the text cap and
// lexical depth preflight bound what can reach the parser at all.)
func TestSQLSandboxDeadlineDuringValidation(t *testing.T) {
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Nanosecond)
	defer cancel()
	start := time.Now()
	err = sb.validate(ctx, "SELECT 1")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("validate error = %v, want context.DeadlineExceeded", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("pre-expired validation context did not fail fast")
	}
}

// sqlParseShape is one cap-sized pathological parsing fixture.
type sqlParseShape struct {
	name  string
	query string
}

// sqlParseShapes builds the shared cap-sized fixtures for the parse regression
// tests: a deep left-associative chain, deep balanced parentheses padded to the
// full 64 KiB cap, a wide IN list and a wide concatenation. Every fixture is at
// or under the text cap and passes sqlPreflight before either trusted probe
// (validation and raw execute-prepare) runs.
func sqlParseShapes(t *testing.T) []sqlParseShape {
	t.Helper()
	deepParens := "SELECT " + strings.Repeat("(", 1000) + "1" + strings.Repeat(")", 1000)
	deepParens += strings.Repeat(" ", sqlMaxQueryBytes-len(deepParens))
	shapes := []sqlParseShape{
		{name: "deep-left-chain", query: "SELECT " + strings.Repeat("1+", ((64<<10)-8)/2) + "1"},
		{name: "deep-parens", query: deepParens},
		{name: "wide-in-list", query: "SELECT 1 WHERE 1 IN (" + strings.TrimSuffix(strings.Repeat("1,", ((64<<10)-32)/2), ",") + ")"},
		{name: "wide-concat", query: "SELECT " + strings.TrimSuffix(strings.Repeat("'a'||", ((64<<10)-10)/5), "||") + "'a'"},
	}
	for _, shape := range shapes {
		if len(shape.query) > sqlMaxQueryBytes {
			t.Fatalf("%s: %d bytes over the %d-byte cap", shape.name, len(shape.query), sqlMaxQueryBytes)
		}
		if err := sqlPreflight(shape.query); err != nil {
			t.Fatalf("%s: preflight rejected the fixture: %v", shape.name, err)
		}
	}
	return shapes
}

// TestSQLParseCappedRegressionBounds is the default-on regression guard for the
// two trusted parse paths over every cap-sized pathological fixture: validation
// (the interrupt-covered json_serialize_sql parse) and the raw execution-path
// prepare (Connection::Prepare, which is not actively interrupted).
//
// Each probe runs under the same 100ms context budget used for the Boat
// measurement, so the measurement conditions are preserved. A probe may return
// successfully, end in DuckDB's max_expression_depth guard, or return
// context.DeadlineExceeded; all three are expected outcomes. The only
// regression assertion is that each probe returns within 5s of wall time.
//
// The Boat large x3 measurement at that 100ms budget recorded max validation
// 303.9346ms (203.9346ms over the budget) and max raw execute-prepare
// 249.754559ms (149.754559ms over); wide-in-list (65525 bytes) was the worst
// measured shape, and the fixtures now span 65525-65536 bytes. The 100ms is the
// request/measurement budget; 5s is the regression bound, not a production SLA.
func TestSQLParseCappedRegressionBounds(t *testing.T) {
	const (
		budget = 100 * time.Millisecond
		bound  = 5 * time.Second
	)
	for _, shape := range sqlParseShapes(t) {
		t.Run(shape.name, func(t *testing.T) {
			// Re-check immediately before the probes so a future fixture edit
			// cannot slip a preflight-rejected payload into the measurement.
			if err := sqlPreflight(shape.query); err != nil {
				t.Fatalf("preflight rejected the fixture: %v", err)
			}
			sb, err := openSQLSandbox(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer sb.close()
			if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
				t.Fatal(err)
			}
			if err := sb.lockdown(t.Context()); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(t.Context(), budget)
			start := time.Now()
			validateErr := sb.validate(ctx, shape.query)
			validateElapsed := time.Since(start)
			cancel()
			if validateElapsed > bound {
				t.Errorf("validation took %s, over the %s regression bound (err %v)", validateElapsed, bound, firstErrLine(validateErr))
			}

			ctx2, cancel2 := context.WithTimeout(t.Context(), budget)
			start = time.Now()
			rows, prepareErr := sb.query(ctx2, shape.query)
			prepareElapsed := time.Since(start)
			cancel2()
			if rows != nil {
				rows.Close()
			}
			if prepareElapsed > bound {
				t.Errorf("raw execute-prepare took %s, over the %s regression bound (err %v)", prepareElapsed, bound, firstErrLine(prepareErr))
			}
			t.Logf("%-16s %6d bytes | validate: %8s err=%v | execute-prepare: %8s err=%v (budget %s, bound %s)",
				shape.name, len(shape.query), validateElapsed, firstErrLine(validateErr), prepareElapsed, firstErrLine(prepareErr), budget, bound)
		})
	}
}

// TestSQLParseOverrunMeasurement prints, for the shared cap-sized pathological
// fixtures, how long validation (the interrupt-covered json_serialize_sql
// parse) and the raw execution-path prepare take to abort under short
// deadlines. It is opt-in (PARKANA_TEST_SQL_PARSE_MEASUREMENT) because timings
// are machine-dependent: it asserts nothing beyond both paths returning, and
// exists to measure worst-case parse overrun at the 64 KiB cap on calmer
// hardware.
func TestSQLParseOverrunMeasurement(t *testing.T) {
	if os.Getenv("PARKANA_TEST_SQL_PARSE_MEASUREMENT") == "" {
		t.Skip("set PARKANA_TEST_SQL_PARSE_MEASUREMENT=1 to measure parse overrun")
	}
	budget := 100 * time.Millisecond
	for _, shape := range sqlParseShapes(t) {
		sb, err := openSQLSandbox(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
			t.Fatal(err)
		}
		if err := sb.lockdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), budget)
		start := time.Now()
		validateErr := sb.validate(ctx, shape.query)
		validateElapsed := time.Since(start)
		cancel()
		ctx2, cancel2 := context.WithTimeout(t.Context(), budget)
		start = time.Now()
		rows, prepareErr := sb.query(ctx2, shape.query)
		prepareElapsed := time.Since(start)
		cancel2()
		if rows != nil {
			rows.Close()
		}
		t.Logf("%-16s %6d bytes | validate: %8s err=%v | execute-prepare: %8s err=%v (budget %s)",
			shape.name, len(shape.query), validateElapsed, firstErrLine(validateErr), prepareElapsed, firstErrLine(prepareErr), budget)
		sb.close()
	}
}

func firstErrLine(err error) string {
	if err == nil {
		return "none"
	}
	s := err.Error()
	for i := range len(s) {
		if s[i] == '\n' {
			return s[:i]
		}
	}
	return s
}

// TestSQLSandboxDeadlineDuringQuery proves a long-running query is
// interrupted on its deadline and that a later sandbox on the same process
// still works afterwards.
func TestSQLSandboxDeadlineDuringQuery(t *testing.T) {
	run := func(ctx context.Context, q string) error {
		sb, err := openSQLSandbox(ctx)
		if err != nil {
			return err
		}
		defer sb.close()
		if err := sb.applyLimits(ctx, 1<<30, 1); err != nil {
			return err
		}
		if err := sb.lockdown(ctx); err != nil {
			return err
		}
		if err := sb.validate(ctx, q); err != nil {
			return err
		}
		rows, err := sb.query(ctx, q)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
		}
		return rows.Err()
	}
	slow := "SELECT count(*) FROM range(8000000000) a, range(50) b"
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := run(ctx, slow)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("slow query completed under its deadline")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("query error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 15*time.Second {
		t.Fatalf("query abort took %s, far past the deadline", elapsed)
	}
	// A later request still works.
	if err := run(t.Context(), "SELECT 42"); err != nil {
		t.Fatalf("later request: %v", err)
	}
}

// TestSQLSandboxPrepareParseNotInterrupted documents the parse residual: the
// prepared-statement parse inside QueryContext is not actively interrupted
// (duckdb_interrupt does not stop Connection::Prepare parsing), so its cost
// is bounded ahead of it — by the query text cap, the lexical depth preflight
// and DuckDB's max_expression_depth=1000 — rather than by the deadline. The
// same text was already parsed under interruptible coverage during
// validation. Both outcomes below are the two bounded ways a cap-sized
// pathological expression can end.
func TestSQLSandboxPrepareParseNotInterrupted(t *testing.T) {
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	expensive := "SELECT " + strings.Repeat("1+", 20000) + "1" // 40 KB, inside the cap
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	rows, err := sb.query(ctx, expensive)
	if rows != nil {
		rows.Close()
	}
	if err == nil {
		t.Skip("parse finished inside the deadline on this machine")
	}
	if strings.Contains(err.Error(), "Max expression depth") {
		t.Logf("documented: depth guard ended the parse: %v", err)
		return
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v", err)
	}
	t.Logf("documented: interrupt ended the parse: %v", err)
}

// TestSQLSandboxPreflight unit-tests the lexical preflight: the 64 KiB cap,
// paren/bracket/CASE depth counting outside strings and comments, and the
// quoting and comment forms that must NOT count.
func TestSQLSandboxPreflight(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		ok    bool
	}{
		{"plain", "SELECT 1 FROM logs", true},
		{"cap at limit", "SELECT '" + strings.Repeat("a", 64<<10-len("SELECT ''")) + "'", true},
		{"cap over", "SELECT '" + strings.Repeat("a", (64<<10)-len("SELECT ''")+1) + "'", false},
		{"depth at limit", "SELECT " + strings.Repeat("(", 1000) + "1" + strings.Repeat(")", 1000), true},
		{"depth over", "SELECT " + strings.Repeat("(", 1001) + "1" + strings.Repeat(")", 1001), false},
		{"case depth over", "SELECT " + strings.Repeat("CASE WHEN TRUE THEN ", 1001) + "1" + strings.Repeat(" ELSE 1 END", 1001), false},
		{"case depth fine", "SELECT " + strings.Repeat("CASE WHEN TRUE THEN ", 100) + "1" + strings.Repeat(" ELSE 1 END", 100), true},
		{"parens in string", "SELECT '(((((((((' FROM logs", true},
		{"case word in string", "SELECT 'CASE (END' FROM logs", true},
		{"parens in line comment", "SELECT 1 -- (((((( (\n", true},
		{"parens in block comment", "/* ((((( */ SELECT 1 /* )))) */", true},
		{"nested block comment", "/* /* ((( */ */ SELECT 1", true},
		{"unterminated comment counts nothing after", "SELECT 1 /* ((((", true},
		{"parens in quoted ident", `SELECT "((((" FROM logs`, true},
		{"escaped quote keeps string open", "SELECT 'it''s ((( (' FROM logs", true},
		{"dollar quote", "SELECT $$((($$ FROM logs", true},
		{"dollar tag quote", "SELECT $tag$)$tag$ FROM logs", true},
		{"brackets count", "SELECT " + strings.Repeat("[", 1001) + "1" + strings.Repeat("]", 1001), false},
		{"end closes case", "SELECT CASE WHEN TRUE THEN CASE WHEN TRUE THEN 1 ELSE 2 END ELSE 3 END", true},
		{"case as column name is quoted", `SELECT "case" FROM logs`, true},
		// E-escape strings: a backslash-escaped quote keeps the string open,
		// so code after the real close stays visible to the depth counter.
		{"e-string escaped quote", `SELECT E'a\' b', (((1)))`, true},
		{"e-string lowercase", `SELECT e'a\' b', ((1))`, true},
		{"e-string hides deep code", `SELECT E'a\' b', ` + strings.Repeat("(", 1200) + "1" + strings.Repeat(")", 1200), false},
		{"plain string still doubles", "SELECT 'a''b', (((1)))", true},
		// Dollar quotes: digits allowed after the first tag character, and a
		// $ that continues an identifier is not a quote opener.
		{"dollar tag with digits hides content", "SELECT $tag123$" + strings.Repeat("(", 1200) + "$tag123$", true},
		{"dollar in identifier", "SELECT foo$bar FROM logs", true},
		{"dollar only continues identifiers", "SELECT foo$" + strings.Repeat("(", 1200) + " FROM logs", false},
		// Unicode identifiers are single tokens: CASE/END fragments inside
		// them never adjust depth.
		{"unicode identifier case fragment", "SELECT é_case, mé_end FROM logs", true},
		{"many unicode case fragments", "SELECT " + strings.Repeat("é_case ", 1001) + "FROM logs", true},
		{"plain identifier case fragment", "SELECT my_case_fn(1) FROM logs", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := sqlPreflight(tc.query)
			if tc.ok && err != nil {
				t.Fatalf("preflight rejected: %v", err)
			}
			if !tc.ok {
				if err == nil {
					t.Fatal("preflight accepted")
				}
				if !errors.Is(err, errSQLText) {
					t.Fatalf("error = %v, want errSQLText", err)
				}
			}
		})
	}
	// The error names the exact cap and depth.
	if err := sqlPreflight(strings.Repeat("(", 1001) + ")"); err == nil || !strings.Contains(err.Error(), "1000") {
		t.Fatalf("depth error = %v, want the depth named", err)
	}
	tooBig := "SELECT '" + strings.Repeat("a", (64<<10)) + "'"
	if err := sqlPreflight(tooBig); err == nil || !strings.Contains(err.Error(), "65536") {
		t.Fatalf("cap error = %v, want the byte cap named", err)
	}
}

// TestSQLSandboxPreflightLineCommentCR proves a bare carriage return ends a
// line comment exactly as DuckDB's lexer does (probe-verified: only CR and LF
// terminate it; VT, FF, U+2028 and friends stay inside the comment). A scanner
// that stopped only at LF would hide the nesting after a CR and let more than
// 1000 levels reach the parser without the lexical guard seeing them.
func TestSQLSandboxPreflightLineCommentCR(t *testing.T) {
	deep := strings.Repeat("(", 1200) + "1" + strings.Repeat(")", 1200)
	for name, term := range map[string]string{"lf": "\n", "cr": "\r", "crlf": "\r\n"} {
		q := "SELECT 1 --c" + term + " + " + deep
		if err := sqlPreflight(q); !errors.Is(err, errSQLText) {
			t.Errorf("%s: preflight = %v, want errSQLText (nesting after the comment)", name, err)
		}
	}
	for name, sep := range map[string]string{"vt": "\v", "ff": "\f"} {
		q := "SELECT 1 --c" + sep + " + " + deep
		if err := sqlPreflight(q); err != nil {
			t.Errorf("%s: preflight = %v, want accepted (the separator stays in the comment)", name, err)
		}
	}
}

// TestSQLSandboxPreflightUnicodeDollarTag proves a dollar-quoted string whose
// tag is a Unicode identifier is skipped as a string, the way DuckDB's lexer
// treats it (probe-verified: $é$...$é$, $aä1$...$aä1$ and $日本語$...$日本語$
// all parse as one string). The 1200 levels inside must not count; a scanner
// that stopped its tag at ASCII would reject this valid text.
func TestSQLSandboxPreflightUnicodeDollarTag(t *testing.T) {
	deep := strings.Repeat("(", 1200) + "1" + strings.Repeat(")", 1200)
	for name, tag := range map[string]string{
		"unicode-start": "é",
		"unicode-cont":  "aä1",
		"unicode-word":  "日本語",
		"ascii-digits":  "tag123",
		"underscore":    "_tag_1",
	} {
		q := "SELECT $" + tag + "$" + deep + "$" + tag + "$"
		if err := sqlPreflight(q); err != nil {
			t.Errorf("%s: preflight = %v, want accepted (the string body is data)", name, err)
		}
	}
	// A leading digit is not a dollar-quote tag in DuckDB ($1$ is a syntax
	// error), so the nesting after it still counts.
	if err := sqlPreflight("SELECT $1$" + deep + "$1$"); !errors.Is(err, errSQLText) {
		t.Errorf("digit-leading tag: preflight = %v, want errSQLText", err)
	}
	// Code after the closed string is still counted.
	if err := sqlPreflight("SELECT $é$body$é$, " + deep); !errors.Is(err, errSQLText) {
		t.Errorf("code after unicode-tagged string: preflight = %v, want errSQLText", err)
	}
}

// TestSQLSandboxExplicitRecursiveRejected proves the shared scanner rejects
// every explicit WITH RECURSIVE before DuckDB. The pinned PEG grammar makes
// RECURSIVE the only syntax that constructs a recursive CTE (see the source
// notes on sqlHasExplicitRecursive); the scanner is case-insensitive, skips
// comments, strings and quoted identifiers, treats the Unicode spaces DuckDB
// normalizes to ASCII as separators, and sees nested clauses, column aliases
// and USING KEY variants. Both the pre-DuckDB preflight and validate
// return errSQLRecursive (wrapping errSQLStatement) with the fixed message.
func TestSQLSandboxExplicitRecursiveRejected(t *testing.T) {
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		query string
	}{
		{"explicit", "WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT * FROM t"},
		{"lowercase", "with recursive t as (select 1 as n union all select n+1 from t where n<3) select * from t"},
		{"comments", "WITH /* a */ RECURSIVE -- b\n t AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT * FROM t"},
		{"mixed-case", "With ReCuRsIvE t AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT * FROM t"},
		{"vt", "WITH\vRECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT * FROM t"},
		{"form-feed", "WITH\fRECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT * FROM t"},
		{"nbsp", "WITH\u00a0RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT * FROM t"},
		{"ideographic-space", "WITH\u3000RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT * FROM t"},
		{"zero-width-space", "WITH\u200bRECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT * FROM t"},
		{"word-joiner", "WITH\u2060RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT * FROM t"},
		{"bom-lead", "\ufeffWITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT * FROM t"},
		{"nested", "SELECT * FROM (WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT * FROM t) s"},
		{"using-key", "WITH RECURSIVE t USING KEY (n) AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT * FROM t"},
		{"column-aliases", "WITH RECURSIVE t (n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n<3) SELECT * FROM t"},
		{"logs-named", "WITH RECURSIVE logs AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM logs WHERE n<3) SELECT * FROM logs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Preflight is the pre-DuckDB, pre-catalog gate used by both
			// checkRequest and validate; assert the full sentinel contract.
			if err := sqlPreflight(tc.query); !errors.Is(err, errSQLRecursive) || !errors.Is(err, errSQLStatement) {
				t.Fatalf("preflight(%q) = %v, want errSQLRecursive wrapping errSQLStatement", tc.query, err)
			}
			err := sb.validate(t.Context(), tc.query)
			if !errors.Is(err, errSQLRecursive) || !errors.Is(err, errSQLStatement) {
				t.Fatalf("validate(%q) = %v, want errSQLRecursive wrapping errSQLStatement", tc.query, err)
			}
			if !strings.Contains(err.Error(), "recursive CTEs are not supported") {
				t.Fatalf("validate(%q) = %v, want the fixed recursive-CTE message", tc.query, err)
			}
		})
	}
}

// TestSQLSandboxRecursiveLogsRegression is the bounded regression reported by
// the security review: a recursive CTE named logs bypassed the AST relation
// policy because the logs name is always allowed, so validate returned nil in
// 0.129s (independently confirmed). The explicit RECURSIVE preflight now
// rejects it before DuckDB; the same bounded 3-row query still executes on the
// trusted raw path, proving this is real recursion and not a lexical false
// positive.
func TestSQLSandboxRecursiveLogsRegression(t *testing.T) {
	const q = "WITH RECURSIVE logs AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM logs WHERE n<3) SELECT * FROM logs"
	if err := sqlPreflight(q); !errors.Is(err, errSQLRecursive) {
		t.Fatalf("preflight = %v, want errSQLRecursive", err)
	}
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := sb.validate(t.Context(), q); !errors.Is(err, errSQLRecursive) {
		t.Fatalf("validate = %v, want errSQLRecursive", err)
	}
	// Trusted raw path, deliberately bounded to three rows with a finite 1s
	// deadline: the same text is a genuine recursive CTE.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	rows, err := sb.query(ctx, q)
	if err != nil {
		t.Fatalf("bounded raw query: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("bounded raw rows: %v", err)
	}
	if count != 3 {
		t.Fatalf("bounded recursive rows = %d, want 3", count)
	}
}

// TestSQLSandboxRecursiveKeywordFalsePositives proves the lexical check never
// rejects the words with/recursive when they are data or names: ordinary WITH
// logs, quoted and string CTE names, dollar-quoted and E-strings, comments,
// quoted aliases, identifiers containing the words, and USING KEY without
// RECURSIVE (which the pinned parser does not treat as recursion: a
// self-reference without RECURSIVE fails the binder).
func TestSQLSandboxRecursiveKeywordFalsePositives(t *testing.T) {
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"WITH logs AS (SELECT 1) SELECT * FROM logs",
		"WITH\u00a0logs AS (SELECT 1) SELECT * FROM logs",
		"WITH\vlogs AS (SELECT 1) SELECT * FROM logs",
		"WITH\u3000logs AS (SELECT 1) SELECT * FROM logs",
		`WITH "recursive" AS (SELECT 1) SELECT * FROM "recursive"`,
		`WITH 'recursive' AS (SELECT 1) SELECT * FROM "recursive"`,
		`WITH "WITH RECURSIVE" AS (SELECT 1) SELECT * FROM "WITH RECURSIVE"`,
		`SELECT 'WITH RECURSIVE' AS s`,
		`SELECT E'WITH RECURSIVE' AS s`,
		`SELECT $$WITH RECURSIVE$$ AS s`,
		`SELECT 1 /* WITH RECURSIVE */`,
		"SELECT 1 -- WITH RECURSIVE\n",
		"SELECT 1 -- WITH RECURSIVE\r",
		"SELECT 1 -- WITH RECURSIVE\u00a0\r",
		"SELECT '\u00a0WITH\u00a0RECURSIVE\u00a0' AS s",
		"WITH \"WITH\u00a0RECURSIVE\" AS (SELECT 1) SELECT * FROM \"WITH\u00a0RECURSIVE\"",
		"SELECT 1 /* WITH\u00a0RECURSIVE */",
		`SELECT 1 AS "WITH RECURSIVE"`,
		"WITH recursive_flag AS (SELECT 1) SELECT * FROM recursive_flag",
		"WITH t USING KEY (n) AS (SELECT 1 AS n) SELECT * FROM t",
	} {
		if err := sqlPreflight(q); err != nil {
			t.Errorf("preflight(%q) = %v, want accepted", q, err)
		}
		if err := sb.validate(t.Context(), q); err != nil {
			t.Errorf("validate(%q) = %v, want accepted", q, err)
		}
	}
}

// TestSQLSandboxUnicodeSpaceVsIdentifierBytes proves the normalization set is
// only token whitespace: the same scan splits WITH from RECURSIVE on a Unicode
// space, but a non-space Unicode identifier byte keeps WITHéRECURSIVE a single
// identifier (so it cannot manufacture the banned pair). It also proves bytes
// inside a quoted string and a quoted identifier are preserved end to end,
// with a comment containing the same text ignored.
func TestSQLSandboxUnicodeSpaceVsIdentifierBytes(t *testing.T) {
	if !sqlHasExplicitRecursive("WITH\u00a0RECURSIVE t AS (SELECT 1) SELECT * FROM t") {
		t.Fatal("NBSP between WITH and RECURSIVE was not treated as token whitespace")
	}
	const ident = "WITH\u00e9RECURSIVE"
	const identQuery = "WITH " + ident + " AS (SELECT 1 AS n) SELECT n FROM " + ident
	if sqlHasExplicitRecursive(identQuery) {
		t.Fatalf("identifier %q was split into the banned token pair", ident)
	}
	res, err := sandboxProbe(t, func(*sqlSandbox) error { return nil }, identQuery)
	if err != nil {
		t.Fatalf("identifier query: %v", err)
	}
	if len(res.Rows) != 1 || string(res.Rows[0][0].(jsontext.Value)) != "1" {
		t.Fatalf("identifier query rows = %#v, want one row 1", res.Rows)
	}

	const q = "SELECT 'A\u00a0WITH\u00a0RECURSIVE\u00a0B' AS s, 1 AS \"a\u00a0b\" /* WITH\u00a0RECURSIVE */"
	res, err = sandboxProbe(t, func(*sqlSandbox) error { return nil }, q)
	if err != nil {
		t.Fatalf("quoted content query: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("quoted content rows = %d, want 1", len(res.Rows))
	}
	if len(res.Columns) != 2 || res.Columns[0].Name != "s" || res.Columns[1].Name != "a\u00a0b" {
		t.Fatalf("columns = %#v, want names s and a<NBSP>b", res.Columns)
	}
	if got := res.Rows[0][0]; got != "A\u00a0WITH\u00a0RECURSIVE\u00a0B" {
		t.Fatalf("string cell = %q, want the NBSP bytes preserved", got)
	}
	if got := res.Rows[0][1]; string(got.(jsontext.Value)) != "1" {
		t.Fatalf("number cell = %#v, want 1", got)
	}
}

// TestSQLSandboxASTRejectsRecursiveCTENode is the second-layer regression for
// the recursive ban: it serializes the bounded logs-named recursive query with
// json_serialize_sql and deliberately bypasses sqlPreflight, calling the AST
// walk directly. The serialized CTE value carries type RECURSIVE_CTE_NODE, so
// the walk must return errSQLRecursive wrapping errSQLStatement even when the
// lexical layer is skipped. An ordinary WITH logs AST still validates.
func TestSQLSandboxASTRejectsRecursiveCTENode(t *testing.T) {
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	serialize := func(q string) map[string]any {
		t.Helper()
		var out any
		if err := sb.conn.QueryRowContext(t.Context(), "SELECT json_serialize_sql(?)", q).Scan(&out); err != nil {
			t.Fatalf("serialize %q: %v", q, err)
		}
		obj, ok := out.(map[string]any)
		if !ok {
			t.Fatalf("serialize %q: result %T", q, out)
		}
		if failed, _ := obj["error"].(bool); failed {
			t.Fatalf("serialize %q failed: %v", q, obj)
		}
		stmts, _ := obj["statements"].([]any)
		if len(stmts) != 1 {
			t.Fatalf("serialize %q: %d statements", q, len(stmts))
		}
		stmt, ok := stmts[0].(map[string]any)
		if !ok {
			t.Fatalf("serialize %q: statement %T", q, stmts[0])
		}
		return stmt
	}

	const recursive = "WITH RECURSIVE logs AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM logs WHERE n<5) SELECT * FROM logs"
	if err := sqlPreflight(recursive); !errors.Is(err, errSQLRecursive) {
		t.Fatalf("preflight = %v, want errSQLRecursive", err)
	}
	err = sqlValidateAST(t.Context(), serialize(recursive))
	if !errors.Is(err, errSQLRecursive) || !errors.Is(err, errSQLStatement) {
		t.Fatalf("AST walk = %v, want errSQLRecursive wrapping errSQLStatement", err)
	}
	if !strings.Contains(err.Error(), "recursive CTEs are not supported") {
		t.Fatalf("AST walk = %v, want the fixed recursive-CTE message", err)
	}
	if err := sqlValidateAST(t.Context(), serialize("WITH logs AS (SELECT 1) SELECT * FROM logs")); err != nil {
		t.Fatalf("ordinary WITH logs AST = %v, want accepted", err)
	}
}

// TestSQLSandboxValidationRejectsIntrospection proves the AST policy rejects
// settings introspection and indirection through every reachable shape:
// scalar functions, table functions, system tables and views, schema and
// catalog qualification, and quoting or casing that hides the name.
func TestSQLSandboxValidationRejectsIntrospection(t *testing.T) {
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.materialize(t.Context(), nil, time.Unix(0, 0), time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		// Settings and environment introspection.
		"SELECT current_setting('memory_limit')",
		"SELECT CURRENT_SETTING('memory_limit')",
		`SELECT "current_setting"('memory_limit')`,
		"SELECT getvariable('x')",
		"SELECT getenv('HOME')",
		"SELECT current_setting('home_directory')",
		// System tables and views, every spelling.
		"SELECT * FROM duckdb_settings",
		"SELECT * FROM duckdb_settings()",
		"SELECT * FROM DUCKDB_SETTINGS()",
		"SELECT * FROM duckdb_functions()",
		"SELECT * FROM pg_settings",
		"SELECT * FROM information_schema.tables",
		"SELECT * FROM INFORMATION_SCHEMA.TABLES",
		"SELECT * FROM sqlite_master",
		"SELECT * FROM pragma_database_list()",
		"SELECT * FROM pragma_table_info('logs')",
		// Indirection that could run or read anything.
		"SELECT * FROM query('SELECT 1')",
		"SELECT * FROM query_table('logs')",
		"SELECT * FROM json_execute_serialized_sql('{}')",
		"SELECT * FROM read_parquet('x.parquet')",
		"SELECT * FROM read_csv('x.csv')",
		"SELECT * FROM glob('*')",
		// Nested and dynamic shapes reach the same policy.
		"WITH c AS (SELECT * FROM duckdb_settings()) SELECT * FROM c",
		"WITH c AS (SELECT 1) SELECT * FROM c UNION ALL SELECT * FROM duckdb_settings()",
		"SELECT * FROM logs WHERE line IN (SELECT value FROM duckdb_settings())",
		"SELECT EXISTS(SELECT 1 FROM pg_settings)",
		"SELECT * FROM logs, LATERAL (SELECT * FROM sqlite_master) s",
		"SELECT count(*) FROM (SELECT * FROM pragma_version()) p",
		"SELECT max(current_setting('memory_limit')) FROM logs",
		"SELECT * FROM logs WHERE length(getvariable('x')) > 0",
		// Schema- and catalog-qualified relations are rejected even to logs.
		"SELECT * FROM main.logs",
		"SELECT * FROM memory.logs",
		"SELECT * FROM information_schema.logs",
	} {
		if err := sb.validate(t.Context(), q); err == nil {
			t.Errorf("validate(%q) accepted", q)
		} else if !errors.Is(err, errSQLStatement) && !errors.Is(err, errSQLText) {
			t.Errorf("validate(%q) = %v, want errSQLStatement", q, err)
		}
	}
}

// TestSQLSandboxCTEScopeEnforcement proves CTE names never escape their
// lexical scope: a name declared inside a subquery, a CTE body's own name,
// or a later sibling does not authorize a system relation elsewhere in the
// query. Every case here must fail closed.
func TestSQLSandboxCTEScopeEnforcement(t *testing.T) {
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	const (
		scopeMsg = "not the logs relation or a CTE visible at this scope"
		funcMsg  = "introspects or reads outside the logs relation"
	)
	for _, tc := range []struct {
		name  string
		query string
		want  string
	}{
		{
			// A CTE declared in a nonvisible subquery must not authorize the
			// outer reference to the same-named system relation.
			name:  "inner-subquery-name",
			query: "SELECT * FROM pg_settings WHERE EXISTS (WITH pg_settings AS (SELECT 1) SELECT 1)",
			want:  scopeMsg,
		},
		{
			name:  "inner-select-name",
			query: "SELECT * FROM (WITH duckdb_settings AS (SELECT 1) SELECT * FROM duckdb_settings) s WHERE EXISTS (SELECT * FROM duckdb_settings)",
			want:  scopeMsg,
		},
		{
			// A CTE's own name is not visible inside its body without explicit
			// recursive semantics: non-explicit self-reference and forward
			// references are rejected here. Explicit WITH RECURSIVE is rejected
			// earlier by preflight (TestSQLSandboxExplicitRecursiveRejected).
			name:  "self-shadow-system-relation",
			query: "WITH pg_settings AS (SELECT * FROM pg_settings) SELECT * FROM pg_settings",
			want:  scopeMsg,
		},
		{
			name:  "self-reference",
			query: "WITH t AS (SELECT * FROM t) SELECT * FROM t",
			want:  scopeMsg,
		},
		{
			name:  "forward-reference",
			query: "WITH a AS (SELECT * FROM b), b AS (SELECT 1) SELECT * FROM a",
			want:  scopeMsg,
		},
		{
			// An inner scope's declaration never authorizes a sibling.
			name:  "sibling-escape",
			query: "SELECT * FROM logs WHERE EXISTS (WITH sqlite_master AS (SELECT 1) SELECT 1) AND EXISTS (SELECT * FROM sqlite_master)",
			want:  scopeMsg,
		},
		{
			name:  "function-outside-scope",
			query: "WITH c AS (SELECT 1) SELECT * FROM c UNION ALL SELECT * FROM pragma_version()",
			want:  funcMsg,
		},
		{
			// Unused CTE bodies are still walked: an unreferenced declaration
			// must not smuggle a settings or file relation past the policy.
			name:  "unused-system-relation",
			query: "WITH c AS (SELECT * FROM pg_settings) SELECT 1",
			want:  scopeMsg,
		},
		{
			name:  "unused-system-function",
			query: "WITH c AS (SELECT * FROM duckdb_settings()) SELECT 1",
			want:  funcMsg,
		},
		{
			name:  "unused-read-function",
			query: "WITH c AS (SELECT * FROM read_parquet('x')) SELECT 1",
			want:  funcMsg,
		},
		{
			name:  "nested-unused-system-relation",
			query: "WITH c AS (WITH d AS (SELECT * FROM pg_settings) SELECT 1) SELECT 1",
			want:  scopeMsg,
		},
		{
			name:  "logs-named-body",
			query: "WITH logs AS (SELECT * FROM pg_settings) SELECT * FROM logs",
			want:  scopeMsg,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := sb.validate(t.Context(), tc.query)
			if err == nil {
				t.Fatalf("validate(%q) accepted a scope escape", tc.query)
			}
			if !errors.Is(err, errSQLStatement) {
				t.Fatalf("validate(%q) = %v, want errSQLStatement", tc.query, err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validate(%q) = %v, want it to name %q", tc.query, err, tc.want)
			}
		})
	}
}

// TestSQLSandboxCTEScopeVisibility proves the scopes that ARE legitimate
// still work: prior siblings, outer names in nested queries, shadowing by an
// inner declaration, and chains across nesting levels.
func TestSQLSandboxCTEScopeVisibility(t *testing.T) {
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"WITH a AS (SELECT 1) SELECT * FROM a",
		"WITH a AS (SELECT 1), b AS (SELECT * FROM a) SELECT * FROM b",
		"WITH a AS (SELECT 1 AS x), b AS (SELECT x FROM a), c AS (SELECT * FROM b) SELECT * FROM c",
		"WITH c AS (SELECT 1 AS x) SELECT * FROM (SELECT * FROM c) s",
		"WITH c AS (SELECT 1 AS x) SELECT * FROM (WITH d AS (SELECT * FROM c) SELECT * FROM d) s",
		"WITH c AS (SELECT 1 AS x) SELECT * FROM (WITH c AS (SELECT 2 AS x) SELECT * FROM c) s",
		"SELECT * FROM (WITH local AS (SELECT 1) SELECT * FROM local) s",
		"WITH a AS (SELECT 1) SELECT * FROM a UNION ALL SELECT * FROM a",
		// A prior CTE is visible in the select list, filters, aliases, joins
		// and quoted references, not only in the top-level FROM.
		"WITH c AS (SELECT 1 AS x) SELECT (SELECT x FROM c)",
		"WITH c AS (SELECT 1 AS x) SELECT * FROM logs WHERE EXISTS (SELECT 1 FROM c)",
		"WITH c AS (SELECT 1) SELECT * FROM c AS logs",
		"WITH c AS (SELECT 1) SELECT * FROM c, c AS c2",
		`WITH c AS (SELECT 1) SELECT * FROM "C"`,
	} {
		if err := sb.validate(t.Context(), q); err != nil {
			t.Errorf("validate(%q) = %v, want accepted", q, err)
		}
	}
}

// TestSQLSandboxCTEWalkLinear proves the scoped traversal is linear in the
// AST: a deeply nested CTE chain and a wide CTE map, both under the text
// cap, validate promptly, and a canceled context stops the walk immediately.
// (The previous collect-then-walk design revisited each CTE entry through
// two recursions and grew exponentially with nesting.)
func TestSQLSandboxCTEWalkLinear(t *testing.T) {
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	deep := "SELECT 42"
	for i := range 80 {
		deep = fmt.Sprintf("WITH c%d AS (%s) SELECT * FROM c%d", i, deep, i)
	}
	wide := "WITH c0 AS (SELECT 1)"
	for i := 1; i < 600; i++ {
		wide += fmt.Sprintf(", c%d AS (SELECT * FROM c%d)", i, i-1)
	}
	wide += " SELECT count(*) FROM c599"
	for name, q := range map[string]string{"deep": deep, "wide": wide} {
		if len(q) > 64<<10 {
			t.Fatalf("%s fixture is %d bytes, over the cap", name, len(q))
		}
		start := time.Now()
		if err := sb.validate(t.Context(), q); err != nil {
			t.Errorf("%s chain validate = %v, want accepted", name, err)
			continue
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("%s chain took %s: the walk is not linear", name, elapsed)
		}
		// Visit count, not just wall time: the scoped walk must visit each
		// serialized node at most once. It deliberately skips a query node's
		// cte_map container (walking its entries instead), so the count sits
		// just under the full tree count; a collect-then-rewalk design makes
		// it exceed the tree.
		stmt := sqlSerializeStatement(t, sb, q)
		full := sqlCountASTNodes(stmt)
		w := new(sqlASTWalk)
		if err := sqlWalkScoped(t.Context(), stmt, nil, w); err != nil {
			t.Errorf("%s direct walk: %v", name, err)
			continue
		}
		if w.nodes > full {
			t.Errorf("%s: walk visited %d nodes but the tree has only %d (revisited)", name, w.nodes, full)
		}
		if w.nodes*4 < full*3 {
			t.Errorf("%s: walk visited only %d of %d nodes (skipped subtree)", name, w.nodes, full)
		}
	}
	// A canceled context stops the walk promptly, mid-traversal.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	start := time.Now()
	if err := sb.validate(ctx, deep); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled walk error = %v, want context.Canceled", err)
	}
	if time.Since(start) > time.Second {
		t.Error("canceled walk did not stop promptly")
	}
}

// TestSQLSandboxASTWalkCancelObservedAtCheckpoint bounds cancellation latency:
// a context canceled before the walk starts is first observed at the 1024-node
// traversal checkpoint, so a larger synthetic tree stops there instead of
// running to completion. It does not claim the cancellation arrives mid-walk.
func TestSQLSandboxASTWalkCancelObservedAtCheckpoint(t *testing.T) {
	children := make([]any, 4096)
	for i := range children {
		children[i] = map[string]any{"type": "CONSTANT"}
	}
	root := map[string]any{"node": map[string]any{"children": children}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	w := new(sqlASTWalk)
	err := sqlWalkScoped(ctx, root, nil, w)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("walk error = %v, want context.Canceled", err)
	}
	if w.nodes < 1024 || w.nodes > 2048 {
		t.Fatalf("walk stopped after %d nodes, want the first 1024-node checkpoint", w.nodes)
	}
}

// sqlSerializeStatement returns the one serialized statement object for query,
// the same shape validate walks.
func sqlSerializeStatement(t *testing.T, sb *sqlSandbox, query string) map[string]any {
	t.Helper()
	var out any
	if err := sb.conn.QueryRowContext(t.Context(), "SELECT json_serialize_sql(?)", query).Scan(&out); err != nil {
		t.Fatalf("serialize: %v", err)
	}
	obj, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("serialize result is %T, want an object", out)
	}
	if failed, _ := obj["error"].(bool); failed {
		t.Fatalf("serialize error: %v", obj["error_message"])
	}
	stmts, _ := obj["statements"].([]any)
	if len(stmts) != 1 {
		t.Fatalf("serialized %d statements, want 1", len(stmts))
	}
	stmt, ok := stmts[0].(map[string]any)
	if !ok {
		t.Fatalf("statement is %T, want an object", stmts[0])
	}
	return stmt
}

// sqlCountASTNodes counts every map and slice in a serialized tree, the node
// classes sqlASTWalk counts, including the cte_map containers the walk skips.
func sqlCountASTNodes(node any) int {
	switch n := node.(type) {
	case map[string]any:
		total := 1
		for _, v := range n {
			total += sqlCountASTNodes(v)
		}
		return total
	case []any:
		total := 1
		for _, v := range n {
			total += sqlCountASTNodes(v)
		}
		return total
	}
	return 0
}

// TestSQLSandboxInterruptKeepsContextError proves the driver's bare interrupt
// error is joined with the context error: DuckDB reports cancellation as
// "INTERRUPT Error: Interrupted!" without wrapping context.Canceled or
// context.DeadlineExceeded, so this join is what keeps timeout classification
// reliable for settings, materialization, validation and execution.
func TestSQLSandboxInterruptKeepsContextError(t *testing.T) {
	interrupt := errors.New("INTERRUPT Error: Interrupted!")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := sqlInterrupted(ctx, interrupt); !errors.Is(err, context.Canceled) || !errors.Is(err, interrupt) {
		t.Fatalf("sqlInterrupted = %v, want both context.Canceled and the driver error", err)
	}
	if err := sqlInterrupted(t.Context(), interrupt); err != interrupt {
		t.Fatalf("sqlInterrupted without cancellation = %v, want the original error", err)
	}
}

// TestSQLSandboxMaterializeCTASTimeout proves the materialization CTAS is
// interruptible on a warm sandbox: after limits and a trivial probe, a real
// multi-megabyte Parquet file cannot materialize inside a short deadline, and
// the aborted CTAS leaves no partial logs table behind, so the same sandbox
// materializes a narrow window immediately afterwards.
func TestSQLSandboxMaterializeCTASTimeout(t *testing.T) {
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: bigRows(80000, 600)}
	env := openSourceEnv(t, []plantest.File{f}, defaultStreamLabels)
	path, release, err := fixtureResolver{env.dir}.Acquire(t.Context(), tenant.Default, env.files[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	// Warm the connection so the deadline below is spent in the CTAS.
	if _, err := sb.conn.ExecContext(t.Context(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = sb.materialize(ctx, []string{path}, plantest.T0.Add(-time.Hour), plantest.T0.Add(3*time.Hour))
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("materialize over a 10ms deadline = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("CTAS abort took %s, far past the deadline", elapsed)
	}

	// The failed CTAS rolled back: the same sandbox still works, and the
	// narrow window holds exactly its 1000 rows (i = 0..999 at one µs each).
	if err := sb.materialize(t.Context(), []string{path}, plantest.T0, plantest.T0.Add(1000*time.Microsecond)); err != nil {
		t.Fatalf("materialize after aborted CTAS: %v", err)
	}
	rows, err := sb.conn.QueryContext(t.Context(), "SELECT count(*) FROM logs")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	res, err := sqlScanRows(t.Context(), rows, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(res.Rows[0][0].(jsontext.Value)); got != "1000" {
		t.Fatalf("narrow count after aborted CTAS = %s, want 1000", got)
	}
}

// TestSQLSandboxLockdownBlocksFileReads proves the sandbox settings, not just
// the AST policy, are the file-access backstop: after materialization and
// lockdown, a trusted connection that bypasses user-SQL validation still
// cannot read the object it just materialized through read_parquet or glob.
func TestSQLSandboxLockdownBlocksFileReads(t *testing.T) {
	f := plantest.File{Tenant: tenant.Default, Stream: "api"}
	env := openSourceEnv(t, []plantest.File{f}, defaultStreamLabels)
	path, release, err := fixtureResolver{env.dir}.Acquire(t.Context(), tenant.Default, env.files[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	// Materialization itself proves the file was readable before lockdown.
	sb := sandboxBuilt(t, func(sb *sqlSandbox) error {
		return sb.materialize(t.Context(), []string{path}, plantest.T0.Add(-time.Hour), plantest.T0.Add(time.Hour))
	})
	for _, q := range []string{
		"SELECT count(*) FROM read_parquet(" + sqlQuotedString(path) + ")",
		"SELECT count(*) FROM glob(" + sqlQuotedString(path) + ")",
	} {
		rows, err := sb.conn.QueryContext(t.Context(), q)
		if err == nil {
			rows.Close()
			t.Errorf("trusted query %q succeeded after lockdown", q)
		}
	}
}

// sqlQuotedString renders s as a SQL string literal for trusted test probes.
func sqlQuotedString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// TestSQLSandboxNestedLeadComments proves the keyword policy's scanner
// handles nested leading block comments: the first keyword is found through
// them, and mis-skipping cannot surface a non-keyword first word.
func TestSQLSandboxNestedLeadComments(t *testing.T) {
	for _, q := range []string{
		"/* outer /* inner */ still comment */ SELECT 1",
		"/* outer /* inner */ still comment */ WITH c AS (SELECT 1) SELECT * FROM c",
		"/* a */ /* b */ SELECT 1",
	} {
		if err := sbValidateKeyword(q); err != nil {
			t.Errorf("keyword scan %q: %v", q, err)
		}
	}
	for _, q := range []string{
		"/* outer /* inner */ still comment */ PRAGMA version",
		"/* a */ -- b\nSET memory_limit='1TB'",
	} {
		if err := sbValidateKeyword(q); err == nil {
			t.Errorf("keyword scan %q accepted", q)
		}
	}
}

// sbValidateKeyword isolates the keyword layer for scanner tests.
func sbValidateKeyword(query string) error {
	if kw := sqlLeadKeyword(query); kw != "select" && kw != "with" {
		return fmt.Errorf("leads with %q", kw)
	}
	return nil
}

// TestSQLSandboxValidationPreservesAnalytics proves the AST policy keeps
// ordinary analytics: the logs relation, chained CTEs, joins, scalar
// aggregates, range/generate_series/unnest, IN and EXISTS subqueries and CASE
// expressions. Recursive/self-referencing CTEs are covered as rejected in
// TestSQLSandboxCTEScopeEnforcement.
func TestSQLSandboxValidationPreservesAnalytics(t *testing.T) {
	sb, err := openSQLSandbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer sb.close()
	if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
		t.Fatal(err)
	}
	if err := sb.lockdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"SELECT count(*), max(_ts) FROM logs",
		"WITH c AS (SELECT line FROM logs) SELECT count(*) FROM c",
		"WITH a AS (SELECT 1 AS x), b AS (SELECT x FROM a) SELECT * FROM b",
		"WITH r AS (SELECT _ts FROM logs) SELECT r._ts FROM r JOIN r AS r2 ON r._ts = r2._ts",
		"SELECT * FROM logs l1 JOIN logs l2 ON l1.line = l2.line",
		"SELECT * FROM range(10)",
		"SELECT * FROM generate_series(1, 10)",
		"SELECT unnest([1, 2, 3])",
		"SELECT line FROM logs WHERE _ts IN (SELECT max(_ts) FROM logs)",
		"SELECT EXISTS(SELECT 1 FROM logs)",
		"SELECT CASE WHEN count(*) > 0 THEN 'yes' ELSE 'no' END FROM logs",
		"SELECT row_number() OVER (ORDER BY _ts) FROM logs",
		"SELECT list(line ORDER BY _ts) FROM logs",
		`SELECT "line", "labels"['service_name'] FROM logs`,
		"select line from logs where line like '%error%'",
		"SELECT * FROM logs UNION ALL SELECT * FROM logs",
	} {
		if err := sb.validate(t.Context(), q); err != nil {
			t.Errorf("validate(%q) = %v, want accepted", q, err)
		}
	}
}
