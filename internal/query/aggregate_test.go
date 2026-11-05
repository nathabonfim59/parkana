package query

import (
	"database/sql"
	"fmt"
	"iter"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// TestAggregateSQLParameterized checks the Aggregate compiler the way
// TestSelectSQLParameterized checks Select's: hostile matcher names and
// values, line filter values and file paths appear only as bound arguments,
// the placeholder count matches the argument count, the bucket values bind
// in text order (origin seconds, origin nanoseconds, width) after the two
// inlined saturation constants, and Selection Direction and Limit are
// ignored — no ORDER BY, no LIMIT is compiled. TestAggregateSQLExecution
// proves the same statements against real DuckDB.
func TestAggregateSQLParameterized(t *testing.T) {
	hostileName := `we"ird OR 1=1--`
	hostileValue := `x'; DROP TABLE t;--`
	sel := plan.Selection{
		Tenant: tenant.Default,
		Matchers: []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, hostileName, hostileValue),
			plan.MustMatcher(plan.MatchRegexp, "env", `pro\d`),
		},
		LineFilters: []plan.LineFilter{plan.MustLineFilter(plan.LineContains, "%'_`\\' OR '1'='1")},
		From:        plantest.T0,
		Through:     plantest.T0.Add(time.Hour),
		Direction:   plan.Backward, // Aggregate ignores it
		Limit:       42,            // Aggregate ignores it
	}
	// 1µs + 500ns: a sub-µs origin, so the remainder binds 500, not 0.
	agg := plan.BucketAgg{Width: 7 * time.Second, Origin: plantest.T0.Add(1500 * time.Nanosecond)}
	paths := []string{`/tmp/o'brien/a"b.parquet`, `/tmp/x y/ünïcode.parquet`}
	groups := []fileGroup{
		{promoted: []string{`we"ird`, "level"}, paths: paths},
		{promoted: nil, paths: paths[:1]},
	}
	q := aggregateSQL(sel, agg, groups)

	for _, leak := range []string{hostileValue, "OR 1=1", `o'brien`, "ünïcode", "%'_", `pro\d`} {
		if strings.Contains(q.sql, leak) {
			t.Fatalf("client string %q spliced into SQL: %s", leak, q.sql)
		}
	}
	if n, args := strings.Count(q.sql, "?"), len(q.args); n != args {
		t.Errorf("%d placeholders for %d arguments: %s", n, args, q.sql)
	}
	for _, want := range []string{
		" GROUP BY 1, 2",
		"count(*)",
		`CAST(sum(octet_length(encode("line"))) AS BIGINT)`,
		`CAST(epoch_us("_ts") AS HUGEINT) * 1000 - (CAST(? AS HUGEINT) * 1000000000 + ?)`,
		"CAST((d - ((d % w + w) % w)) // w AS BIGINT) AS bucket",
		"? AS w, ",
		" AS d, ",
		fmt.Sprintf("greatest(%d, least(%d, ", nsSatMin, nsSatMax),
	} {
		if !strings.Contains(q.sql, want) {
			t.Errorf("missing %q in compiled SQL: %s", want, q.sql)
		}
	}
	if !strings.HasSuffix(q.sql, ")) GROUP BY 1, 2") {
		t.Errorf("statement does not end with the positional GROUP BY: %s", q.sql)
	}
	for _, banned := range []string{" ORDER BY", " LIMIT"} {
		if strings.Contains(q.sql, banned) {
			t.Errorf("Aggregate compiled a %q clause: %s", strings.TrimSpace(banned), q.sql)
		}
	}
	// Bucket values in text order, after the inlined saturation constants:
	// origin seconds, origin nanoseconds, width ns.
	if sec, ok := q.args[0].(int64); !ok || sec != agg.Origin.Unix() {
		t.Errorf("arg 0 = %v (%T), want origin seconds %d", q.args[0], q.args[0], agg.Origin.Unix())
	}
	if nsec, ok := q.args[1].(int64); !ok || nsec != 1500 {
		t.Errorf("arg 1 = %v (%T), want origin nanoseconds 1500", q.args[1], q.args[1])
	}
	if w, ok := q.args[2].(int64); !ok || w != agg.Width.Nanoseconds() {
		t.Errorf("arg 2 = %v (%T), want width %d ns", q.args[2], q.args[2], agg.Width.Nanoseconds())
	}
	if !slices.Contains(q.args, any(paths[0])) || !slices.Contains(q.args, any(paths[1])) {
		t.Errorf("paths not bound: %v", q.args)
	}
	if !slices.Contains(q.args, any(hostileName)) || !slices.Contains(q.args, any(hostileValue)) {
		t.Errorf("hostile matcher name/value not bound: %v", q.args)
	}
	// The width binds once as the constant column w.
	if got := strings.Count(q.sql, "? AS w"); got != 1 {
		t.Errorf("width bound %d times, want the single constant column: %s", got, q.sql)
	}
	// The normalized relation is shared: per-group subqueries, UNION ALL'd,
	// with the canonical label map and the identity metadata map built in
	// every group.
	if got := strings.Count(q.sql, "UNION ALL"); got != len(groups)-1 {
		t.Errorf("%d UNION ALL separators for %d groups: %s", got, len(groups), q.sql)
	}
	if got := strings.Count(q.sql, "map_from_entries"); got != 2*len(groups) {
		t.Errorf("canonical map expressions = %d, want %d (labels plus identity metadata per group): %s", got, 2*len(groups), q.sql)
	}
	if !strings.Contains(q.sql, " WHERE TRUE ") {
		t.Errorf("predicate list not compiled by normRelation: %s", q.sql)
	}

	// A whole-µs origin binds remainder 0.
	q = aggregateSQL(plan.Selection{
		Tenant: tenant.Default,
		Matchers: []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "service_name", "api"),
		},
		From: plantest.T0, Through: plantest.T0.Add(time.Minute),
	}, plan.BucketAgg{Width: time.Minute, Origin: plantest.T0}, groups[:1])
	if nsec, ok := q.args[1].(int64); !ok || nsec != 0 {
		t.Errorf("whole-second origin nanoseconds = %v (%T), want 0", q.args[1], q.args[1])
	}
}

// TestDuckDBAggregateArithmetic pins the DuckDB behaviors the Aggregate
// statement relies on, the way TestDuckDBSQLContract pins Select's: //
// truncates toward zero (why the compiled floor subtracts the non-negative
// remainder first), % carries the dividend's sign (the adjustment is
// correct under either sign), the floored-division expression equals Go's
// floor for negative and sub-width distances, octet_length counts UTF-8
// bytes over encode's BLOB — the VARCHAR overload does not exist — where
// length counts codepoints, the clamped HUGEINT distance saturates at
// exactly time.Duration's bounds before the floored division, epoch_us
// reads the stored TIMESTAMP WITH TIME ZONE as whole microseconds,
// sum(octet_length) widens to HUGEINT (why the CAST exists) while count(*)
// stays BIGINT, positional GROUP BY groups by output column, and BIGINT
// multiplication wraps silently past the nanosecond span — the HUGEINT
// distance is what keeps saturation exact instead.
func TestDuckDBAggregateArithmetic(t *testing.T) {
	db := openTestDB(t)
	ctx := t.Context()
	queryInt64 := func(q string, args ...any) (int64, error) {
		var n int64
		err := db.QueryRowContext(ctx, q, args...).Scan(&n)
		return n, err
	}

	t.Run("truncating-division", func(t *testing.T) {
		for _, tc := range []struct {
			q    string
			args []any
			want int64
		}{
			{q: "SELECT 7 // 2", want: 3},
			{q: "SELECT -7 // 2", want: -3}, // toward zero, NOT floor
			{q: "SELECT -6 // 3", want: -2}, // exact multiples are unaffected
			{q: "SELECT ? // ?", args: []any{int64(-1), int64(3000000000)}, want: 0},
		} {
			got, err := queryInt64(tc.q, tc.args...)
			if err != nil {
				t.Fatalf("%s: %v", tc.q, err)
			}
			if got != tc.want {
				t.Errorf("%s = %d, want %d (truncation toward zero)", tc.q, got, tc.want)
			}
		}
	})

	t.Run("remainder-sign", func(t *testing.T) {
		got, err := queryInt64("SELECT -7 % 2")
		if err != nil {
			t.Fatal(err)
		}
		if got != -1 {
			t.Errorf("-7 %% 2 = %d; the floor expression must not rely on %%'s sign", got)
		}
	})

	// The compiled floor: subtract the non-negative remainder
	// ((d % w + w) % w) so the dividend becomes an exact multiple of w,
	// which truncating division then divides exactly — Go's floor, for any
	// sign of d (the reference's k in plan.BucketAgg.Start).
	t.Run("floored-division-expression", func(t *testing.T) {
		for _, tc := range []struct {
			d, w int64
			want int64
		}{
			{d: -7, w: 2, want: -4},
			{d: 7, w: 2, want: 3},
			{d: -6, w: 3, want: -2},
			{d: -1, w: 3000000000, want: -1},
			{d: -999, w: 1000, want: -1},
			{d: 1999700, w: 1000000, want: 1},
			{d: -3000000, w: 3000000, want: -1},
		} {
			q := fmt.Sprintf("SELECT (? - ((? %% %d + %d) %% %d)) // %d", tc.w, tc.w, tc.w, tc.w)
			got, err := queryInt64(q, tc.d, tc.d)
			if err != nil {
				t.Fatalf("d=%d w=%d: %v", tc.d, tc.w, err)
			}
			if got != tc.want {
				t.Errorf("floor(%d/%d) = %d, want %d", tc.d, tc.w, got, tc.want)
			}
		}
	})

	t.Run("octet-length-bytes", func(t *testing.T) {
		for _, s := range []string{"café ☕ ünïcode", "", "multi\nline", "a\x00b", "plain"} {
			got, err := queryInt64("SELECT octet_length(encode(?))", s)
			if err != nil {
				t.Fatalf("octet_length(encode(%q)): %v", s, err)
			}
			if got != int64(len(s)) {
				t.Errorf("octet_length(encode(%q)) = %d, want len %d", s, got, len(s))
			}
		}
		got, err := queryInt64("SELECT length(?)", "café ☕")
		if err != nil {
			t.Fatal(err)
		}
		if want := int64(utf8.RuneCountInString("café ☕")); got != want {
			t.Errorf("length() = %d, want %d codepoints (bytes would be %d): length() must not back the bytes bucket", got, want, len("café ☕"))
		}
	})

	// The exact distance-and-index semantics aggregateSQL compiles, with
	// hand-computed expectations mirroring Go: clamp to the Duration bounds
	// time.Sub saturates to, then floor-divide. The saturation cases sit far
	// beyond the ~292 years an int64 nanosecond count spans, and the
	// max-whole-µs-width case drives the adjustment's intermediate
	// (d - remainder) past int64 range, so a narrowing to BIGINT anywhere in
	// the expression shows up as a wrong index.
	t.Run("clamped-hugeint-distance", func(t *testing.T) {
		dist := fmt.Sprintf("greatest(%d, least(%d, CAST(? AS HUGEINT) * 1000 - (CAST(? AS HUGEINT) * 1000000000 + ?)))", nsSatMin, nsSatMax)
		q := "WITH dist AS (SELECT " + dist + " AS d, ? AS w) SELECT CAST((d - ((d % w + w) % w)) // w AS BIGINT) FROM dist"
		// maxWholeUSWidth is the widest Width BucketAgg.Validate accepts.
		const maxWholeUSWidth = 9223372036854775 * 1000
		// An origin in year 295000 with a sub-µs remainder: representable
		// exactly as seconds plus nanoseconds.
		farOrigin := time.Date(295000, 1, 1, 0, 0, 0, 1500, time.UTC)
		for _, tc := range []struct {
			name                     string
			tsUS, sec, nsec, widthNS int64
			want                     int64
		}{
			// d = 2e21 ns, clamps to nsSatMax; k = nsSatMax/1s = 9223372036
			// (remainder 854775807 >= 0 keeps the floor at the quotient).
			{name: "saturate-max", tsUS: 2000000000000000000, sec: 0, nsec: 0, widthNS: 1000000000, want: 9223372036},
			// d = -2e21 ns, clamps to nsSatMin; Go: nsSatMin/1s truncates to
			// -9223372036 with remainder -854775808 < 0, so k-- gives
			// -9223372037 — the floor.
			{name: "saturate-min", tsUS: -2000000000000000000, sec: 0, nsec: 0, widthNS: 1000000000, want: -9223372037},
			// The same saturation edges at the smallest accepted width, 1µs:
			// k = floor(±nsSat/1000) = ±9223372036854775 and -...776.
			{name: "saturate-max-one-us", tsUS: 2000000000000000000, sec: 0, nsec: 0, widthNS: 1000, want: 9223372036854775},
			{name: "saturate-min-one-us", tsUS: -2000000000000000000, sec: 0, nsec: 0, widthNS: 1000, want: -9223372036854776},
			// The widest accepted width at the negative saturation edge:
			// d - remainder = -18446744073709550000 needs 65 bits, so only
			// HUGEINT arithmetic survives; k = floor(nsSatMin/w) = -2.
			{name: "saturate-min-max-width", tsUS: -2000000000000000000, sec: 0, nsec: 0, widthNS: maxWholeUSWidth, want: -2},
			// d = 1999700 ns: 2000µs apart from an origin 499.7µs before the
			// epoch, bound as -1s + 999500300ns.
			{name: "submicro-no-clamp", tsUS: 1500, sec: -1, nsec: 999500300, widthNS: 1000000, want: 1},
			// d = -999 ns: one µs apart minus the origin's sub-µs remainder,
			// a negative sub-µs delta floors to -1.
			{name: "negative-submicro-floor", tsUS: 1000, sec: 0, nsec: 1000999, widthNS: 1000, want: -1},
			// d = -3000000 ns: an exact negative multiple of the width.
			{name: "exact-negative-multiple", tsUS: 0, sec: 0, nsec: 3000000, widthNS: 3000000, want: -1},
			// An origin in year 295000 (+1500ns) against 2026 data: the true
			// distance (~-9.3e21 ns) clamps to nsSatMin; with a 1h width
			// k = floor(-9223372036854775808/3600000000000) = -2562048.
			{name: "origin-year-295000", tsUS: 1793527200000000, sec: farOrigin.Unix(), nsec: 1500, widthNS: 3600000000000, want: -2562048},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := queryInt64(q, tc.tsUS, tc.sec, tc.nsec, tc.widthNS)
				if err != nil {
					t.Fatalf("%s: %v", tc.name, err)
				}
				if got != tc.want {
					t.Errorf("%s: k = %d, want %d", tc.name, got, tc.want)
				}
			})
		}
		var typ string
		if err := db.QueryRowContext(ctx, "SELECT typeof(greatest(?, least(?, CAST(? AS HUGEINT) * 1000 - (CAST(? AS HUGEINT) * 1000000000 + ?))))",
			any(int64(nsSatMin)), int64(nsSatMax), int64(0), int64(0), int64(0)).Scan(&typ); err != nil {
			t.Fatal(err)
		}
		if typ != "HUGEINT" {
			t.Errorf("typeof(clamped distance) = %q, want HUGEINT (a BIGINT product overflows past 292 years)", typ)
		}
		// The floored index must stay wide until its CAST too: at the
		// saturation edge the adjustment's dividend exceeds int64.
		if err := db.QueryRowContext(ctx,
			"WITH dist AS (SELECT greatest(?, least(?, CAST(? AS HUGEINT) * 1000 - (CAST(? AS HUGEINT) * 1000000000 + ?))) AS d, ? AS w) SELECT typeof((d - ((d % w + w) % w)) // w) FROM dist",
			any(int64(nsSatMin)), int64(nsSatMax), int64(-2000000000000000000), int64(0), int64(0), int64(maxWholeUSWidth)).Scan(&typ); err != nil {
			t.Fatal(err)
		}
		if typ != "HUGEINT" {
			t.Errorf("typeof(floored index) = %q, want HUGEINT (the pre-CAST floor must not narrow)", typ)
		}
	})

	t.Run("epoch-us-microseconds", func(t *testing.T) {
		f := plantest.File{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{
			{TS: plantest.T0.Add(60*time.Second + time.Millisecond), Line: "one",
				Labels: logs.FromMap(map[string]string{"service_name": "s"})},
		}}
		p := writeParquet(t, t.TempDir(), "epoch.parquet", f)
		got, err := queryInt64(`SELECT epoch_us("_ts") FROM read_parquet(?) WHERE "line" = ?`, p, "one")
		if err != nil {
			t.Fatal(err)
		}
		if want := f.Rows[0].TS.UnixMicro(); got != want {
			t.Errorf("epoch_us(_ts) = %d, want %d", got, want)
		}
	})

	t.Run("sum-and-count-types", func(t *testing.T) {
		var typ string
		if err := db.QueryRowContext(ctx, "SELECT typeof(sum(octet_length(encode('x'))))").Scan(&typ); err != nil {
			t.Fatal(err)
		}
		if typ != "HUGEINT" {
			t.Errorf("typeof(sum(octet_length(encode)))) = %q, want HUGEINT (the CAST is what keeps the scan int64)", typ)
		}
		if err := db.QueryRowContext(ctx, "SELECT typeof(count(*))").Scan(&typ); err != nil {
			t.Fatal(err)
		}
		if typ != "BIGINT" {
			t.Errorf("typeof(count(*)) = %q, want BIGINT", typ)
		}
		got, err := queryInt64("SELECT CAST(sum(octet_length(encode(?))) AS BIGINT)", "café")
		if err != nil {
			t.Fatal(err)
		}
		if got != int64(len("café")) {
			t.Errorf("cast sum = %d, want %d", got, len("café"))
		}
	})

	t.Run("positional-group-by", func(t *testing.T) {
		rows, err := db.QueryContext(ctx, `SELECT a % 2 AS p, b, count(*) FROM (VALUES (1, 'x'), (3, 'x'), (2, 'y')) t(a, b) GROUP BY 1, 2`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var got []string
		for rows.Next() {
			var p int64
			var b string
			var n int64
			if err := rows.Scan(&p, &b, &n); err != nil {
				t.Fatal(err)
			}
			got = append(got, fmt.Sprintf("%d/%s/%d", p, b, n))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		want := []string{"0/y/1", "1/x/2"}
		if !slices.Equal(got, want) {
			t.Errorf("positional GROUP BY = %v, want %v", got, want)
		}
	})

	t.Run("bigint-multiply-past-span", func(t *testing.T) {
		// Within the int64 span BIGINT multiplication is exact...
		got, err := queryInt64("SELECT (?::BIGINT) * 1000", int64(9223372036854775))
		if err != nil {
			t.Fatalf("in-range product: %v", err)
		}
		if got != int64(9223372036854775000) {
			t.Errorf("in-range product = %d, want 9223372036854775000", got)
		}
		// ...but 9223372036854776 µs * 1000 = 9223372036854776000 ns
		// exceeds int64 (max 9223372036854775807): whatever BIGINT does
		// there — wrap or error — it cannot carry the true distance, which
		// is why the compiled distance runs in HUGEINT and clamps. No
		// int64 result can be the faithful product, so record the shape;
		// only a correct-looking result would falsify the design.
		got, err = queryInt64("SELECT (?::BIGINT) * 1000", int64(9223372036854776))
		switch {
		case err != nil:
			t.Logf("BIGINT overflow errors loudly: %v", err)
		case got > 0 && got/1000 == 9223372036854776:
			t.Errorf("BIGINT carried the product correctly past the int64 ns span; revisit the HUGEINT motivation")
		default:
			t.Logf("BIGINT product wrapped silently to %d", got)
		}
	})
}

// collectBuckets drains an Aggregate sequence.
func collectBuckets(seq iter.Seq2[plan.Bucket, error]) ([]plan.Bucket, error) {
	var out []plan.Bucket
	for b, err := range seq {
		if err != nil {
			return out, err
		}
		out = append(out, b)
	}
	return out, nil
}

// aggregateFixtureFiles returns the compact Aggregate fixtures: a promoted
// file, an all-map twin of part of it, a Level file and a far-years file,
// as described on aggregateBucketKey and TestAggregateSQLExecution.
func aggregateFixtureFiles() []plantest.File {
	rw := func(ts time.Time, line string, ls map[string]string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	shared := func(level, env string) map[string]string {
		return map[string]string{"service_name": "s", "env": env, "level": level}
	}
	base := plantest.T0
	fCol := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{"level", "env"}, Rows: []plan.Row{
		rw(base, "café ☕ ünïcode", shared("info", "prod")),
		rw(base.Add(time.Second), "", shared("info", "prod")), // empty line: zero bytes
		rw(base.Add(2*time.Second), "multi\nline", shared("warn", "dev")),
		rw(base.Add(3*time.Second), "plain ascii", shared("error", "prod")),
	}}
	fMap := plantest.File{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{
		// Same label set as fCol's first two rows, stored entirely in the
		// map: one bucket with them, not two.
		rw(base, "map twin", shared("info", "prod")),
		rw(base.Add(2*time.Second), "map warn", shared("warn", "dev")),
		rw(base.Add(4*time.Second), "extra label", map[string]string{"service_name": "s", "env": "prod", "level": "info", "extra": "v"}),
	}}
	fCase := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{"Level"}, Rows: []plan.Row{
		rw(base.Add(5*time.Second), "upper high", map[string]string{"service_name": "s", "env": "prod", "Level": "High"}),
		rw(base.Add(6*time.Second), "upper low with line labels", map[string]string{"service_name": "s", "env": "prod", "Level": "Low", "LINE": "x", "Labels": "y"}),
	}}
	// fFar's rows sit 526 and 474 years from the 2026 origins: further than
	// the ~292 years an int64 nanosecond span holds, so both this Source
	// and MemSource saturate, and identical saturation arithmetic must
	// yield identical buckets. Ascending _ts, as the writer requires.
	farPast := time.Date(1500, 1, 1, 0, 0, 0, 0, time.UTC)
	farFuture := time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)
	fFar := plantest.File{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{
		rw(farPast, "far past", map[string]string{"service_name": "s", "far": "yes"}),
		rw(farFuture, "far future", map[string]string{"service_name": "s", "far": "yes"}),
	}}
	return []plantest.File{fCol, fMap, fCase, fFar}
}

// aggregateBucketKey identifies a bucket for multiset comparison. It never
// uses UnixNano or UnixMicro: those wrap outside 1678..2262 and would hide
// start bugs in the far-origin cases; seconds plus the nanosecond offset
// identify every instant a time.Time can hold.
func aggregateBucketKey(b plan.Bucket) string {
	return fmt.Sprintf("%d.%09d|%s|%d|%d", b.Start.Unix(), b.Start.Nanosecond(), b.Labels, b.Count, b.Bytes)
}

// TestAggregateSQLExecution runs compiled Aggregate statements through real
// DuckDB directly over fixture files — a focused matrix beside the wired
// path's full conformance (TestSourceConformance, TestSourceBackends) — and
// compares every bucket with the MemSource reference: same start
// (nanosecond-exact for sub-µs origins, wrapped k*w beyond the ~292-year
// saturation span included), same complete label set, same count, same byte
// total. The fixtures mix promotion layouts of one label set (a promoted
// file, an all-map file and a Level file), so equal label maps with
// different stored shapes and map orders group together while Level stays
// distinct from level; lines cover multi-byte Unicode, an empty line and an
// embedded newline; rows 474 and 526 years from the origins exercise
// time.Sub-identical saturation; bucket parameters cover origins before,
// inside and after the data, a shifted whole-µs origin, a sub-µs origin, a
// tiny width, the 1µs minimum width, the maximum whole-µs width and origins
// in the years ±295000.
func TestAggregateSQLExecution(t *testing.T) {
	files := aggregateFixtureFiles()
	base := plantest.T0
	farPast, farFuture := files[3].Rows[0].TS, files[3].Rows[1].TS

	dir := t.TempDir()
	var cfs []catalog.File
	var paths []string
	for i, f := range files {
		data, st, err := plantest.Encode(f)
		if err != nil {
			t.Fatalf("encode file %d: %v", i, err)
		}
		p := filepath.Join(dir, fmt.Sprintf("agg-%d.parquet", i))
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		cfs = append(cfs, catalog.File{
			Tenant: f.Tenant, Stream: f.Stream, Key: fmt.Sprintf("%s/logs/s/agg-%d.parquet", f.Tenant, i),
			MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(len(data)),
			LabelValues: st.LabelValues, CreatedAt: time.Now().Truncate(time.Microsecond),
		})
		paths = append(paths, p)
	}
	db := openTestDB(t)
	ref := plantest.Mem(files)
	groups := groupFiles(cfs, paths)

	eq := func(n, v string) plan.Matcher { return plan.MustMatcher(plan.MatchEqual, n, v) }
	contains := func(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineContains, v) }
	svc := []plan.Matcher{eq("service_name", "s")}
	sels := map[string]plan.Selection{
		"all":                     {Tenant: tenant.Default, Matchers: svc, From: base.Add(-time.Hour), Through: base.Add(time.Hour)},
		"level-mixed-promotion":   {Tenant: tenant.Default, Matchers: []plan.Matcher{eq("service_name", "s"), eq("level", "warn")}, From: base.Add(-time.Hour), Through: base.Add(time.Hour)},
		"level-case-distinct":     {Tenant: tenant.Default, Matchers: []plan.Matcher{eq("service_name", "s"), eq("Level", "High")}, From: base.Add(-time.Hour), Through: base.Add(time.Hour)},
		"level-empty-sees-both":   {Tenant: tenant.Default, Matchers: []plan.Matcher{eq("service_name", "s"), eq("level", "")}, From: base.Add(-time.Hour), Through: base.Add(time.Hour)},
		"unicode-contains-filter": {Tenant: tenant.Default, Matchers: svc, LineFilters: []plan.LineFilter{contains("café")}, From: base.Add(-time.Hour), Through: base.Add(time.Hour)},
		"window-trims-rows":       {Tenant: tenant.Default, Matchers: svc, From: base.Add(time.Second), Through: base.Add(3 * time.Second)},
		"regexp-alternation":      {Tenant: tenant.Default, Matchers: []plan.Matcher{plan.MustMatcher(plan.MatchRegexp, "level", "info|warn")}, From: base.Add(-time.Hour), Through: base.Add(time.Hour)},
		"far-future-window":       {Tenant: tenant.Default, Matchers: svc, From: farFuture.Add(-time.Hour), Through: farFuture.Add(time.Hour)},
		"far-past-window":         {Tenant: tenant.Default, Matchers: svc, From: farPast.Add(-time.Hour), Through: farPast.Add(time.Hour)},
	}
	aggs := map[string]plan.BucketAgg{
		"minute":                   {Width: time.Minute, Origin: base},
		"odd-width-shifted":        {Width: 7 * time.Second, Origin: base.Add(time.Microsecond)},
		"submicrosecond-origin":    {Width: 7 * time.Second, Origin: base.Add(1500 * time.Nanosecond)},
		"origin-before-data":       {Width: time.Hour, Origin: base.Add(-30 * time.Minute)},
		"origin-after-data":        {Width: 10 * time.Second, Origin: base.Add(3 * time.Hour)}, // negative buckets
		"tiny-inside-data":         {Width: 3 * time.Millisecond, Origin: base.Add(400 * time.Millisecond)},
		"one-microsecond-width":    {Width: time.Microsecond, Origin: base},
		"max-whole-micro-width":    {Width: 9223372036854775 * time.Microsecond, Origin: base},
		"origin-year-295000":       {Width: time.Hour, Origin: time.Date(295000, 1, 1, 0, 0, 0, 1500, time.UTC)},
		"origin-year-minus-295000": {Width: time.Hour, Origin: time.Date(-295000, 1, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, aname := range slices.Sorted(maps.Keys(aggs)) {
		agg := aggs[aname]
		for _, sname := range slices.Sorted(maps.Keys(sels)) {
			sel := sels[sname]
			t.Run(aname+"/"+sname, func(t *testing.T) {
				q := aggregateSQL(sel, agg, groups)
				rows, err := db.QueryContext(t.Context(), q.sql, q.args...)
				if err != nil {
					t.Fatalf("execute: %v\nSQL: %s\nargs: %v", err, q.sql, q.args)
				}
				defer rows.Close()
				var got []plan.Bucket
				for rows.Next() {
					var k, count, bytes int64
					var labelsJSON sql.NullString
					if err := rows.Scan(&k, &labelsJSON, &count, &bytes); err != nil {
						t.Fatal(err)
					}
					labels, err := decodeMap(labelsJSON)
					if err != nil {
						t.Fatalf("decode labels %q: %v", labelsJSON.String, err)
					}
					// Bucket start rebuilt exactly as plan.BucketAgg.Start
					// does: origin plus k widths, preserving a sub-µs
					// origin's nanoseconds and the reference's wrapped
					// multiplication beyond the saturation span.
					got = append(got, plan.Bucket{
						Start:  agg.Origin.Add(time.Duration(k) * agg.Width),
						Labels: logs.FromMap(labels),
						Count:  count,
						Bytes:  bytes,
					})
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				want, err := collectBuckets(ref.Aggregate(t.Context(), sel, agg))
				if err != nil {
					t.Fatalf("reference Aggregate: %v", err)
				}
				key := aggregateBucketKey
				g := make([]string, len(got))
				for i, b := range got {
					g[i] = key(b)
					if b.Start.Location() != time.UTC {
						t.Fatalf("bucket start %s is not UTC", b.Start)
					}
				}
				w := make([]string, len(want))
				for i, b := range want {
					w[i] = key(b)
				}
				slices.Sort(g)
				slices.Sort(w)
				if !slices.Equal(g, w) {
					t.Fatalf("buckets differ:\n got  %v\n want %v", g, w)
				}

				// Cross-layout grouping, made explicit once: the
				// promoted file's first two rows and the map file's twin
				// row share one bucket.
				if aname == "minute" && sname == "all" {
					sharedLabels := logs.FromMap(map[string]string{"service_name": "s", "env": "prod", "level": "info"})
					wantBytes := int64(len("café ☕ ünïcode") + len("") + len("map twin"))
					var found bool
					for _, b := range got {
						if b.Labels.String() == sharedLabels.String() && b.Start.Equal(base) {
							found = true
							if b.Count != 3 || b.Bytes != wantBytes {
								t.Errorf("cross-layout bucket = count %d bytes %d, want 3 and %d (two promoted rows + one map twin)", b.Count, b.Bytes, wantBytes)
							}
						}
					}
					if !found {
						t.Errorf("no bucket at %s with labels %s", base, sharedLabels)
					}
				}
			})
		}
	}
}

// TestAggregateFarOrigins drives the PUBLIC source.Aggregate — not the raw
// compiled statement — with the extreme origins and widths, against the
// MemSource oracle: origins in the years ±295000 (far outside int64 µs,
// well inside the seconds+nanoseconds binding), a sub-µs origin, the 1µs
// minimum width and the maximum whole-µs width. Every case must succeed
// and match, so reintroducing any origin rejection or narrowing here fails
// this test. Bucket comparison uses the wrap-free key (aggregateBucketKey):
// UnixNano would wrap at these starts and hide year-offset bugs.
func TestAggregateFarOrigins(t *testing.T) {
	env := openSourceEnv(t, aggregateFixtureFiles(), defaultStreamLabels)
	ref := plantest.Mem(aggregateFixtureFiles())
	base := plantest.T0
	sel := plan.Selection{
		Tenant: tenant.Default,
		Matchers: []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "service_name", "s"),
		},
		From: base.Add(-time.Hour), Through: base.Add(time.Hour),
	}
	for _, tc := range []struct {
		name string
		agg  plan.BucketAgg
	}{
		{name: "origin-year-295000", agg: plan.BucketAgg{Width: time.Hour, Origin: time.Date(295000, 1, 1, 0, 0, 0, 1500, time.UTC)}},
		{name: "origin-year-minus-295000", agg: plan.BucketAgg{Width: time.Hour, Origin: time.Date(-295000, 1, 1, 0, 0, 0, 0, time.UTC)}},
		{name: "submicrosecond-origin", agg: plan.BucketAgg{Width: 7 * time.Second, Origin: base.Add(1500 * time.Nanosecond)}},
		{name: "one-microsecond-width", agg: plan.BucketAgg{Width: time.Microsecond, Origin: base}},
		{name: "max-whole-micro-width", agg: plan.BucketAgg{Width: 9223372036854775 * time.Microsecond, Origin: base}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := collectBuckets(env.source.Aggregate(t.Context(), sel, tc.agg))
			if err != nil {
				t.Fatalf("source.Aggregate: %v", err)
			}
			want, err := collectBuckets(ref.Aggregate(t.Context(), sel, tc.agg))
			if err != nil {
				t.Fatalf("reference Aggregate: %v", err)
			}
			if len(got) == 0 {
				t.Fatal("no buckets; the fixture rows must be bucketed")
			}
			g := make([]string, len(got))
			for i, b := range got {
				g[i] = aggregateBucketKey(b)
				if b.Start.Location() != time.UTC {
					t.Fatalf("bucket start %s is not UTC", b.Start)
				}
			}
			w := make([]string, len(want))
			for i, b := range want {
				w[i] = aggregateBucketKey(b)
			}
			slices.Sort(g)
			slices.Sort(w)
			if !slices.Equal(g, w) {
				t.Fatalf("buckets differ:\n got  %v\n want %v", g, w)
			}
		})
	}
}

// TestAggregateSubmicroBoundary drives the PUBLIC source.Aggregate — through
// openSourceEnv and the plantest loader, not the raw compiled statement — at
// the sharpest sub-µs boundary the bucket arithmetic has: rows exactly one
// microsecond apart while the origin carries a 1500ns remainder. With
// Width = 1µs and Origin = base+1500ns, a row at base+1µs is 500ns before
// the origin (d = -500ns), so the floored index must step back to
// base+500ns, while a row at base+2µs is 500ns after it and stays in the
// origin's own bucket at base+1500ns. Each bucket holds exactly one row with
// its own byte count — "a" and "bb" — proving the row-to-bucket mapping, and
// the whole set must match MemSource bucket for bucket.
//
// The pair is a regression for two independent mistakes a wider width or a
// whole-second origin cannot distinguish: binding only Origin.Nanosecond()'s
// whole microseconds (rows would land at base+1500ns and base+2500ns while
// Go still rebuilds starts from the real origin), and replacing the floor
// with truncating division (both rows would share the origin's bucket). The
// pre-epoch subtest repeats the same points one second before the epoch, so
// the seconds/nanoseconds split and the floor stay exact across the epoch
// too. Timestamps stay stored at whole microseconds.
func TestAggregateSubmicroBoundary(t *testing.T) {
	// One second before the epoch: Unix() -1 with a positive 1500ns
	// remainder once the origin is added — the mirror of T0.
	preEpoch := time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC)
	cases := []struct {
		name string
		base time.Time
	}{
		{name: "post-epoch", base: plantest.T0},
		{name: "pre-epoch", base: preEpoch},
	}
	row := func(ts time.Time, line, boundary string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{
			"service_name": "s",
			"boundary":     boundary,
		})}
	}
	files := make([]plantest.File, 0, len(cases))
	for _, tc := range cases {
		files = append(files, plantest.File{
			Tenant: tenant.Default,
			Stream: "s",
			Rows: []plan.Row{
				row(tc.base.Add(time.Microsecond), "a", tc.name),
				row(tc.base.Add(2*time.Microsecond), "bb", tc.name),
			},
		})
	}
	env := openSourceEnv(t, files, defaultStreamLabels)
	ref := plantest.Mem(files)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			labels := logs.FromMap(map[string]string{"service_name": "s", "boundary": tc.name})
			sel := plan.Selection{
				Tenant: tenant.Default,
				Matchers: []plan.Matcher{
					plan.MustMatcher(plan.MatchEqual, "service_name", "s"),
					plan.MustMatcher(plan.MatchEqual, "boundary", tc.name),
				},
				From:    tc.base.Add(-time.Hour),
				Through: tc.base.Add(time.Hour),
			}
			agg := plan.BucketAgg{Width: time.Microsecond, Origin: tc.base.Add(1500 * time.Nanosecond)}

			got, err := collectBuckets(env.source.Aggregate(t.Context(), sel, agg))
			if err != nil {
				t.Fatalf("source.Aggregate: %v", err)
			}
			want, err := collectBuckets(ref.Aggregate(t.Context(), sel, agg))
			if err != nil {
				t.Fatalf("reference Aggregate: %v", err)
			}
			// The exact expected buckets, independent of MemSource: the
			// 500ns floor and the 1500ns origin bucket, each with the row
			// that belongs to it.
			explicit := []plan.Bucket{
				{Start: tc.base.Add(500 * time.Nanosecond), Labels: labels, Count: 1, Bytes: int64(len("a"))},
				{Start: tc.base.Add(1500 * time.Nanosecond), Labels: labels, Count: 1, Bytes: int64(len("bb"))},
			}

			keys := func(bs []plan.Bucket) []string {
				out := make([]string, len(bs))
				for i, b := range bs {
					out[i] = aggregateBucketKey(b)
					if b.Start.Location() != time.UTC {
						t.Fatalf("bucket start %s is not UTC", b.Start)
					}
				}
				slices.Sort(out)
				return out
			}
			g, w, e := keys(got), keys(want), keys(explicit)
			if !slices.Equal(g, w) {
				t.Fatalf("buckets differ from MemSource:\n got  %v\n want %v", g, w)
			}
			if !slices.Equal(g, e) {
				t.Fatalf("buckets differ from the explicit sub-µs boundary:\n got  %v\n want %v", g, e)
			}
		})
	}
}

// TestAggregateEarlyBreakReleasesLeases is Aggregate's twin of
// TestSourceRetryEarlyBreakReleasesLeases: breaking out of the PUBLIC
// Aggregate iteration after the first bucket must release every object
// lease and leave the cache with no references and nothing in flight, over
// both real backends.
func TestAggregateEarlyBreakReleasesLeases(t *testing.T) {
	for _, kind := range []backendKind{backendFS, backendS3} {
		t.Run(kind.String(), func(t *testing.T) {
			env := newBackendEnv(t, kind, retryFiles())
			agg := plan.BucketAgg{Width: time.Second, Origin: plantest.T0}
			seen := 0
			for _, err := range env.src.Aggregate(t.Context(), retrySelection(), agg) {
				if err != nil {
					t.Fatalf("Aggregate: %v", err)
				}
				seen++
				break
			}
			if seen != 1 {
				t.Fatalf("yielded %d buckets before the break, want 1", seen)
			}
			acquires, releases := env.res.counts()
			if acquires == 0 {
				t.Fatal("no object was acquired; the lease assertion is vacuous")
			}
			if acquires != releases {
				t.Errorf("acquired %d leases but released %d after the early break", acquires, releases)
			}
			if leaks := env.res.liveLeases(); leaks != 0 {
				t.Errorf("%d leases still held after the early break", leaks)
			}
			env.cache.mu.Lock()
			refs := 0
			for _, e := range env.cache.entries {
				refs += e.refs
			}
			inflight := len(env.cache.inflight)
			env.cache.mu.Unlock()
			if refs != 0 {
				t.Errorf("cache holds %d references after the early break", refs)
			}
			if inflight != 0 {
				t.Errorf("%d cache downloads still in flight after the early break", inflight)
			}
		})
	}
}
