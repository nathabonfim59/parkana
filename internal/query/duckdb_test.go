package query

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// writeParquet encodes f as a Parquet file under dir and returns its path.
func writeParquet(t *testing.T, dir, name string, f plantest.File) string {
	t.Helper()
	data, _, err := plantest.Encode(f)
	if err != nil {
		t.Fatalf("encode %s/%s: %v", f.Tenant, f.Stream, err)
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	return p
}

// openTestDB opens a DuckDB handle closed with the test.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := OpenDB(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestDuckDBReadsParkanaParquet pins what the foundation relies on: DuckDB
// reads files written by internal/storage, _ts comes back as a UTC µs
// timestamp, and the labels/meta maps and promoted columns are addressable.
func TestDuckDBReadsParkanaParquet(t *testing.T) {
	files := plantest.Dataset()
	dir := t.TempDir()
	var paths []string
	for i, f := range files[:2] { // api with level promoted, then api with level in the map
		p := writeParquet(t, dir, f.Stream+string(rune('a'+i))+".parquet", f)
		paths = append(paths, p)
	}
	db := openTestDB(t)

	var minTS time.Time
	var rows int64
	var errors int64
	q := `SELECT min(_ts), count(*),
	        count(*) FILTER (WHERE coalesce(level, labels['level']) = 'error')
	      FROM read_parquet([?, ?], union_by_name = true)`
	if err := db.QueryRowContext(t.Context(), q, paths[0], paths[1]).Scan(&minTS, &rows, &errors); err != nil {
		t.Fatal(err)
	}
	want := int64(len(files[0].Rows) + len(files[1].Rows))
	if rows != want {
		t.Errorf("rows = %d, want %d", rows, want)
	}
	if !minTS.Equal(plantest.T0) || minTS.Location().String() != "UTC" {
		t.Errorf("min(_ts) = %s, want %s UTC", minTS, plantest.T0)
	}
	if errors == 0 {
		t.Error("no level=error rows found through the promoted column or the labels map")
	}
	var us time.Time
	if err := db.QueryRowContext(t.Context(), `SELECT _ts FROM read_parquet(?) WHERE line = 'boundary plus one'`, paths[0]).Scan(&us); err != nil {
		t.Fatal(err)
	}
	if got := us.Sub(plantest.T0); got != 60*time.Second+time.Microsecond {
		t.Errorf("µs timestamp offset = %s, want 1m0.000001s", got)
	}
}

// TestDuckDBSQLContract pins the DuckDB and driver behaviors the Source's SQL
// relies on: bound parameters in read_parquet lists and LIMIT, DESCRIBE
// schema discovery over a bound union, labels map subscript binding with
// missing keys reading as NULL, promoted/map label coalescing, contains as a
// byte substring, RE2 anchored and unanchored matching, to_json of the label
// maps, exact microsecond timestamp windows and quoted identifiers for
// hostile column names. If DuckDB or the driver changes any of these, this
// test says so before the Source silently misbehaves.
func TestDuckDBSQLContract(t *testing.T) {
	files := plantest.Dataset()
	dir := t.TempDir()
	apiPromoted := writeParquet(t, dir, "api-promoted.parquet", files[0]) // level column
	apiMap := writeParquet(t, dir, "api-map.parquet", files[1])           // level in map
	bulk := writeParquet(t, dir, "bulk.parquet", files[4])                // level + pod columns
	hostileRow := func(line string) plan.Row {
		return plan.Row{TS: plantest.T0, Line: line,
			Labels: logs.FromMap(map[string]string{"service_name": "weird", `we"ird`: "x"})}
	}
	weirdCol := writeParquet(t, dir, "weird-col.parquet", plantest.File{
		Tenant: tenant.Default, Stream: "weird", Promoted: []string{`we"ird`},
		Rows: []plan.Row{hostileRow("promoted hostile")},
	})
	weirdMap := writeParquet(t, dir, "weird-map.parquet", plantest.File{
		Tenant: tenant.Default, Stream: "weird",
		Rows: []plan.Row{hostileRow("map hostile")},
	})
	db := openTestDB(t)
	ctx := t.Context()
	union := []string{apiPromoted, apiMap, bulk}

	// countRows counts fixture rows across files that pass keep.
	countRows := func(keep func(plan.Row) bool, fs ...plantest.File) int {
		n := 0
		for _, f := range fs {
			for _, r := range f.Rows {
				if keep(r) {
					n++
				}
			}
		}
		return n
	}
	label := func(r plan.Row, name string) string { v, _ := r.Labels.Get(name); return v }
	queryCount := func(q string, args ...any) int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return n
	}

	t.Run("bound-limit", func(t *testing.T) {
		var n int
		q := `SELECT count(*) FROM (SELECT line FROM read_parquet([?], union_by_name = true)
		      ORDER BY _ts DESC LIMIT ?)`
		if err := db.QueryRowContext(ctx, q, apiPromoted, 3).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Errorf("limited count = %d, want 3", n)
		}
	})

	t.Run("describe-union-schema", func(t *testing.T) {
		q := `DESCRIBE SELECT * FROM read_parquet([?, ?, ?], union_by_name = true)`
		rows, err := db.QueryContext(ctx, q, any(union[0]), union[1], union[2])
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		cols := make(map[string]string)
		for rows.Next() {
			var name, typ string
			var null, key, def, extra any // DESCRIBE carries six columns
			if err := rows.Scan(&name, &typ, &null, &key, &def, &extra); err != nil {
				t.Fatal(err)
			}
			cols[name] = typ
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		want := map[string]string{
			"_ts":    "TIMESTAMP WITH TIME ZONE",
			"line":   "VARCHAR",
			"labels": "MAP(VARCHAR, VARCHAR)",
			"meta":   "MAP(VARCHAR, VARCHAR)",
			"level":  "VARCHAR",
			"pod":    "VARCHAR",
		}
		if len(cols) != len(want) {
			t.Errorf("union columns = %v, want exactly %v", cols, want)
		}
		for name, typ := range want {
			if cols[name] != typ {
				t.Errorf("column %s type = %q, want %q", name, cols[name], typ)
			}
		}
	})

	t.Run("map-subscript-bound-param", func(t *testing.T) {
		q := `SELECT count(*) FROM read_parquet([?, ?], union_by_name = true)
		      WHERE coalesce(labels[?], '') = ?`
		got := queryCount(q, apiPromoted, apiMap, "service_name", "api")
		want := countRows(func(r plan.Row) bool { return label(r, "service_name") == "api" }, files[0], files[1])
		if got != want {
			t.Errorf("count = %d, want %d", got, want)
		}
	})

	t.Run("map-missing-key-is-null", func(t *testing.T) {
		n := queryCount(`SELECT count(*) FROM read_parquet(?) WHERE labels[?] IS NOT NULL`, apiPromoted, "no-such-label")
		if n != 0 {
			t.Errorf("%d rows have the missing key", n)
		}
		n = queryCount(`SELECT count(*) FROM read_parquet(?) WHERE coalesce(labels[?], '') <> ''`, apiPromoted, "no-such-label")
		if n != 0 {
			t.Errorf("%d rows read the missing key as non-empty", n)
		}
	})

	t.Run("promoted-map-coalesce", func(t *testing.T) {
		q := `SELECT count(*) FROM read_parquet([?, ?], union_by_name = true)
		      WHERE coalesce(coalesce("level", labels[?]), '') = ?`
		got := queryCount(q, apiPromoted, apiMap, "level", "error")
		want := countRows(func(r plan.Row) bool { return label(r, "level") == "error" }, files[0], files[1])
		if got != want {
			t.Errorf("count = %d, want %d", got, want)
		}
	})

	t.Run("contains-byte-substring", func(t *testing.T) {
		for _, sub := range []string{"%", "_", "café", `\`, "'"} {
			f := plan.MustLineFilter(plan.LineContains, sub)
			got := queryCount(`SELECT count(*) FROM read_parquet(?) WHERE contains(line, ?)`, apiPromoted, sub)
			want := countRows(func(r plan.Row) bool { return f.Matches(r.Line) }, files[0])
			if got != want {
				t.Errorf("contains(%q): count = %d, want %d", sub, got, want)
			}
		}
	})

	// Why the invalid-UTF-8 needle fallback exists: contains has no BLOB
	// overload reachable with a []byte parameter, so partial-rune bytes
	// cannot ride the fast path.
	t.Run("blob-contains-unavailable", func(t *testing.T) {
		for _, q := range []string{
			`SELECT count(*) FROM read_parquet(?) WHERE contains(line, ?)`,
			`SELECT count(*) FROM read_parquet(?) WHERE strpos(line, ?) > 0`,
			`SELECT count(*) FROM read_parquet(?) WHERE position(? IN line) > 0`,
		} {
			if _, err := db.QueryContext(ctx, q, apiPromoted, []byte("error")); err == nil {
				t.Errorf("%s: []byte parameter unexpectedly bound", q)
			}
		}
	})

	// The fallback itself: a byte-aligned hex regexp over lower(hex(line))
	// — ^(?:..)* consumes whole bytes so the needle never matches on an odd
	// nibble — equals strings.Contains on raw bytes, including the
	// odd-nibble false-positive shape hex("aé") = "61c3a9" vs needle 0x1c.
	t.Run("hex-regexp-byte-alignment", func(t *testing.T) {
		needle := "é"[:1] // 0xC3, invalid UTF-8
		pattern := "^(?:..)*" + hex.EncodeToString([]byte(needle))
		got := queryCount(`SELECT count(*) FROM read_parquet(?) WHERE regexp_matches(lower(hex(line)), ?)`, apiPromoted, pattern)
		want := countRows(func(r plan.Row) bool { return strings.Contains(r.Line, needle) }, files[0])
		if got != want {
			t.Errorf("aligned hex regexp: count = %d, want %d", got, want)
		}
		if n := queryCount(`SELECT count(*) FROM read_parquet(?) WHERE regexp_matches(lower(hex(line)), ?)`, apiPromoted, "^(?:..)*1c"); n != 0 {
			t.Errorf("odd-nibble pattern matched %d lines, want 0", n)
		}
	})

	t.Run("regexp-anchored-full-match", func(t *testing.T) {
		q := `SELECT count(*) FROM read_parquet([?, ?], union_by_name = true)
		      WHERE regexp_full_match(coalesce(coalesce("level", labels[?]), ''), ?)`
		got := queryCount(q, apiPromoted, apiMap, "level", "error")
		want := countRows(func(r plan.Row) bool { return label(r, "level") == "error" }, files[0], files[1])
		if got != want {
			t.Errorf("full match count = %d, want %d", got, want)
		}
		// Anchored: 'pro' must not match 'prod'-like values.
		got = queryCount(q, apiPromoted, apiMap, "level", "erro")
		if got != 0 {
			t.Errorf("anchored 'erro' matched %d rows, want 0", got)
		}
	})

	t.Run("regexp-unanchored-line", func(t *testing.T) {
		got := queryCount(`SELECT count(*) FROM read_parquet(?) WHERE regexp_matches(line, ?)`, apiPromoted, "^GET")
		f := plan.MustLineFilter(plan.LineRegexp, "^GET")
		want := countRows(func(r plan.Row) bool { return f.Matches(r.Line) }, files[0])
		if got != want {
			t.Errorf("line regexp count = %d, want %d", got, want)
		}
	})

	t.Run("to-json-maps", func(t *testing.T) {
		var labelsJSON, metaJSON string
		q := `SELECT CAST(to_json(labels) AS VARCHAR), CAST(to_json(meta) AS VARCHAR)
		      FROM read_parquet(?) WHERE cardinality(meta) > 0 LIMIT 1`
		if err := db.QueryRowContext(ctx, q, apiPromoted).Scan(&labelsJSON, &metaJSON); err != nil {
			t.Fatal(err)
		}
		var labels, meta map[string]string
		if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
			t.Fatalf("labels to_json %q: %v", labelsJSON, err)
		}
		if err := json.Unmarshal([]byte(metaJSON), &meta); err != nil {
			t.Fatalf("meta to_json %q: %v", metaJSON, err)
		}
		if labels["service_name"] != "api" || len(meta) == 0 {
			t.Errorf("decoded labels = %v, meta = %v", labels, meta)
		}
	})

	t.Run("timestamp-microsecond-window", func(t *testing.T) {
		b := plantest.T0.Add(60 * time.Second)
		q := `SELECT count(*) FROM read_parquet(?) WHERE _ts >= ? AND _ts < ?`
		got := queryCount(q, apiPromoted, b.UTC(), b.Add(time.Microsecond).UTC())
		want := countRows(func(r plan.Row) bool {
			return !r.TS.Before(b) && r.TS.Before(b.Add(time.Microsecond))
		}, files[0])
		if got != want {
			t.Errorf("µs window count = %d, want %d (exact-boundary rows only)", got, want)
		}
	})

	t.Run("map-column-direct-scan", func(t *testing.T) {
		// The driver may hand MAP columns back as native maps; if so the
		// Source can scan labels directly instead of round-tripping JSON.
		var labels map[string]string
		err := db.QueryRowContext(ctx, `SELECT labels FROM read_parquet(?) LIMIT 1`, apiPromoted).Scan(&labels)
		if err != nil {
			t.Logf("direct MAP scan unsupported (%v); the Source uses to_json", err)
			return
		}
		if labels["service_name"] != "api" {
			t.Errorf("direct-scanned labels = %v, want service_name=api", labels)
		}
	})

	t.Run("quoted-hostile-identifier", func(t *testing.T) {
		q := `SELECT count(*) FROM read_parquet([?, ?], union_by_name = true)
		      WHERE coalesce(coalesce("we""ird", labels[?]), '') = ?`
		got := queryCount(q, weirdCol, weirdMap, `we"ird`, "x")
		if got != 2 {
			t.Errorf("hostile label count = %d, want 2 (promoted and map rows)", got)
		}
	})
}

// TestDuckDBCaseFoldingBehavior documents the DuckDB behavior the Source's
// per-group queries are built around: identifiers are case-insensitive even
// quoted (its keywords_and_identifiers docs), so a single read_parquet over
// files promoting level and Level folds them into one column, mislabeling
// rows before SQL ever sees them. The schema contract (schema.Collide,
// case-insensitive IsReserved) keeps fold collisions out of any one file,
// and the Source never unions different promoted sets in one read_parquet.
// These probes pin both sides of that bargain; the strict Source-level
// regression is TestSelectCaseSensitiveLabels.
func TestDuckDBCaseFoldingBehavior(t *testing.T) {
	rw := func(ts time.Time, line string, ls map[string]string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	base := plantest.T0
	fileLower := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{"level"}, Rows: []plan.Row{
		rw(base, "from lower file", map[string]string{"service_name": "s", "level": "info"}),
	}}
	fileUpper := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{"Level"}, Rows: []plan.Row{
		rw(base.Add(time.Second), "from upper file", map[string]string{"service_name": "s", "Level": "SEVERE"}),
	}}

	t.Run("writer-rejects-same-file-fold", func(t *testing.T) {
		f := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{"level", "Level"}, Rows: []plan.Row{
			rw(base, "x", map[string]string{"service_name": "s", "level": "info", "Level": "SEVERE"}),
		}}
		if _, _, err := plantest.Encode(f); err == nil {
			t.Error("Encode accepted promoted labels differing only in case")
		}
	})
	t.Run("writer-rejects-case-reserved", func(t *testing.T) {
		for _, name := range []string{"LINE", "Labels", "META", "_ts"} {
			f := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{name}, Rows: []plan.Row{
				rw(base, "x", map[string]string{"service_name": "s", name: "v"}),
			}}
			if _, _, err := plantest.Encode(f); err == nil {
				t.Errorf("Encode accepted promoted %q, which folds onto a base/reserved name", name)
			}
		}
	})

	dir := t.TempDir()
	pLower := writeParquet(t, dir, "lower.parquet", fileLower)
	pUpper := writeParquet(t, dir, "upper.parquet", fileUpper)
	db := openTestDB(t)
	ctx := t.Context()

	probe := func(name string, fn func(t *testing.T)) {
		t.Run(name, func(t *testing.T) { fn(t) })
	}
	describeNames := func(paths ...string) []string {
		q := "DESCRIBE SELECT * FROM read_parquet([" + placeholders(len(paths)) + "], union_by_name = true)"
		args := make([]any, len(paths))
		for i, p := range paths {
			args[i] = p
		}
		rows, err := db.QueryContext(ctx, q, args...)
		if err != nil {
			t.Fatalf("describe: %v", err)
		}
		defer rows.Close()
		var names []string
		for rows.Next() {
			var name, typ string
			var null, key, def, extra any
			if err := rows.Scan(&name, &typ, &null, &key, &def, &extra); err != nil {
				t.Fatal(err)
			}
			names = append(names, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return names
	}

	// Why the Source never unions different promoted sets: the fold happens
	// in the reader, mislabeling values before SQL can recover them.
	probe("union-folds-cross-file", func(t *testing.T) {
		t.Logf("union describe: %v", describeNames(pLower, pUpper))
		var js string
		if err := db.QueryRowContext(ctx, `SELECT CAST(to_json(t) AS VARCHAR) FROM read_parquet([?, ?], union_by_name = true) AS t WHERE "line" = ?`,
			pLower, pUpper, "from upper file").Scan(&js); err != nil {
			t.Fatal(err)
		}
		t.Logf("upper-file row through the union: %s (Level mislabeled as level)", js)
		if !strings.Contains(js, `"level":"SEVERE"`) || strings.Contains(js, `"Level":"SEVERE"`) {
			t.Errorf("union did not fold Level into level: %s", js)
		}
	})

	// Per-group reads are safe: a single file (or files with identical
	// promoted sets) binds its own exact-case names.
	probe("per-file-no-folding", func(t *testing.T) {
		lower := describeNames(pLower)
		upper := describeNames(pUpper)
		t.Logf("describe(lower alone): %v", lower)
		t.Logf("describe(upper alone): %v", upper)
		if !slices.Contains(lower, "level") || !slices.Contains(upper, "Level") {
			t.Errorf("per-file bindings lost exact-case names: %v / %v", lower, upper)
		}
	})

	// The per-group + UNION ALL + one outer ORDER BY shape the Source uses.
	probe("union-all-outer-order", func(t *testing.T) {
		q := `SELECT * FROM (
			SELECT "_ts", "line", "level" AS c0
			FROM read_parquet(?, union_by_name = true)
			UNION ALL
			SELECT "_ts", "line", "Level" AS c0
			FROM read_parquet(?, union_by_name = true)
		) ORDER BY "_ts" ASC LIMIT ?`
		rows, err := db.QueryContext(ctx, q, pLower, pUpper, int64(10))
		if err != nil {
			t.Fatalf("union: %v", err)
		}
		defer rows.Close()
		var got []string
		for rows.Next() {
			var ts time.Time
			var line string
			var c0 sql.NullString
			if err := rows.Scan(&ts, &line, &c0); err != nil {
				t.Fatal(err)
			}
			got = append(got, fmt.Sprintf("%s/%s", line, c0.String))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		want := []string{"from lower file/info", "from upper file/SEVERE"}
		if !slices.Equal(got, want) {
			t.Errorf("union rows = %v, want %v", got, want)
		}
	})
}

// TestDuckDBMapCanonicalization probes the SQL pieces the normalized
// relation uses to build the complete, case-sensitive, canonically ordered
// label map inside each per-group subquery: map_entries field names, struct
// literals with bound keys and quoted column values, list_contains with a
// bound name list, list_sort on struct lists, map_from_entries and the exact
// to_json key order.
func TestDuckDBMapCanonicalization(t *testing.T) {
	files := plantest.Dataset()
	dir := t.TempDir()
	caseFile := writeParquet(t, dir, "case.parquet", files[5]) // promotes Level; map has level/LINE/Labels
	var wantRow *plan.Row
	for i := range files[5].Rows {
		if i%5 == 0 && i%4 < 2 && i%2 == 0 { // has LINE/Labels, level, Level=High
			r := files[5].Rows[i]
			wantRow = &r
			break
		}
	}
	if wantRow == nil {
		t.Fatal("no fixture row with all case labels")
	}
	db := openTestDB(t)
	ctx := t.Context()

	one := func(q string, args ...any) string {
		t.Helper()
		var s sql.NullString
		if err := db.QueryRowContext(ctx, q, args...).Scan(&s); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return s.String
	}

	t.Run("map-entries-fields", func(t *testing.T) {
		t.Logf("map_entries(labels) = %s", one(`SELECT CAST(map_entries("labels") AS VARCHAR) FROM read_parquet(?) LIMIT 1`, caseFile))
	})
	t.Run("bound-struct-literal", func(t *testing.T) {
		got := one(`SELECT CAST([{'key': ?, 'value': "Level"}] AS VARCHAR) FROM read_parquet(?) LIMIT 1`, "Level", caseFile)
		t.Logf("struct literal = %s", got)
	})
	t.Run("list-contains-bound-list", func(t *testing.T) {
		got := one(`SELECT CAST(list_filter(map_entries("labels"), lambda e: NOT list_contains(?, e.key)) AS VARCHAR) FROM read_parquet(?) LIMIT 1`,
			[]string{"Level"}, caseFile)
		t.Logf("filtered entries = %s", got)
	})
	t.Run("canonical-labels-json", func(t *testing.T) {
		q := `SELECT CAST(to_json(map_from_entries(list_sort(list_concat(
			list_filter(map_entries("labels"), lambda e: NOT list_contains(?, e.key)),
			list_filter([{'key': ?, 'value': "Level"}], lambda e: e.value IS NOT NULL))))) AS VARCHAR)
			FROM read_parquet(?) WHERE "line" = ?`
		got := one(q, []string{"Level"}, "Level", caseFile, wantRow.Line)
		t.Logf("canonical labels = %s", got)
		var m map[string]string
		if err := json.Unmarshal([]byte(got), &m); err != nil {
			t.Fatalf("parse %q: %v", got, err)
		}
		for k, v := range wantRow.Labels.Map() {
			if m[k] != v {
				t.Errorf("label %s = %q, want %q (got %v)", k, m[k], v, m)
			}
		}
		if len(m) != len(wantRow.Labels) {
			t.Errorf("canonical map has %d entries, want %d: %v", len(m), len(wantRow.Labels), m)
		}
		keys := slices.Sorted(maps.Keys(m))
		for i := 1; i < len(keys); i++ {
			if keys[i-1] >= keys[i] {
				t.Errorf("keys not canonically sorted: %v", keys)
			}
		}
	})
	t.Run("null-promoted-omitted", func(t *testing.T) {
		var noLevel *plan.Row
		for i := range files[5].Rows {
			if i%4 >= 2 && i%5 != 0 { // no level, no LINE/Labels
				r := files[5].Rows[i]
				noLevel = &r
				break
			}
		}
		if noLevel == nil {
			t.Fatal("no fixture row without map level")
		}
		q := `SELECT CAST(to_json(map_from_entries(list_sort(list_concat(
			list_filter(map_entries("labels"), lambda e: NOT list_contains(?, e.key)),
			list_filter([{'key': ?, 'value': "Level"}], lambda e: e.value IS NOT NULL))))) AS VARCHAR)
			FROM read_parquet(?) WHERE "line" = ?`
		got := one(q, []string{"Level"}, "Level", caseFile, noLevel.Line)
		t.Logf("row without map level: canonical = %s", got)
		if strings.Contains(got, `"level"`) {
			t.Errorf("absent level label leaked into canonical map: %s", got)
		}
	})
	t.Run("insertion-order-preserved", func(t *testing.T) {
		// map_from_entries keeps insertion order and to_json emits it, so
		// without list_sort two promotion layouts of one label set
		// serialize differently (promoted entries append after map
		// entries). The sort is what makes labels_json canonical for
		// GROUP BY; this probe proves it has something to do.
		a := one(`SELECT CAST(to_json(map_from_entries([{'key': ?, 'value': ?}, {'key': ?, 'value': ?}])) AS VARCHAR)`, "b", "2", "a", "1")
		b := one(`SELECT CAST(to_json(map_from_entries([{'key': ?, 'value': ?}, {'key': ?, 'value': ?}])) AS VARCHAR)`, "a", "1", "b", "2")
		if a == b {
			t.Fatalf("map_from_entries lost insertion order; the list_sort guard is vacuous: %s", a)
		}
		t.Logf("insertion order preserved: %s vs %s", a, b)
	})
	t.Run("empty-labels-map", func(t *testing.T) {
		f := plantest.File{Tenant: tenant.Default, Stream: "s",
			Rows: []plan.Row{{TS: plantest.T0, Line: "no labels"}}}
		p := writeParquet(t, t.TempDir(), "nolabels.parquet", f)
		// The exact no-promoted-labels expression normRelation emits.
		got := one(`SELECT CAST(to_json(coalesce(map_from_entries(list_sort(map_entries("labels"))), map([], []))) AS VARCHAR) FROM read_parquet(?)`, p)
		t.Logf("empty labels canonical = %q", got)
		if got != "{}" {
			t.Errorf("empty labels canonical = %q, want {}", got)
		}
	})
}

// TestOpenDBConnectionInitAfterCancel checks the OpenDB fix: connection
// initialization must run on a connection-lifetime context, never the
// context OpenDB was called with. Connections are opened lazily, long after
// any request or constructor context may have expired, and one expired
// context must not poison every future connection.
func TestOpenDBConnectionInitAfterCancel(t *testing.T) {
	// capturedCancel reproduces the old OpenDB: the per-connection
	// initializer executes on the canceled context that created the DB.
	// It must fail, proving the regression detects the bug it guards
	// against.
	t.Run("captured-canceled-context-fails", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		conn, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
			_, err := execer.ExecContext(ctx, "SET TimeZone='UTC'", nil)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		db := sql.OpenDB(conn)
		defer db.Close()
		db.SetMaxIdleConns(0) // every Conn opens and initializes a fresh driver connection
		if _, err := db.Conn(t.Context()); err == nil {
			t.Fatal("connection initialized through a canceled context; the guard no longer proves the bug")
		}
	})

	// The regression itself: create with a cancelable context, use a
	// connection, cancel, then open new connections and verify they
	// initialize with UTC session settings.
	t.Run("fresh-connection-after-cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		db, err := OpenDB(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		db.SetMaxIdleConns(0) // every Conn opens and initializes a fresh driver connection

		var tz string
		if err := db.QueryRowContext(t.Context(), "SELECT current_setting('TimeZone')").Scan(&tz); err != nil {
			t.Fatalf("query before cancel: %v", err)
		}
		if !strings.EqualFold(tz, "UTC") {
			t.Fatalf("TimeZone before cancel = %q, want UTC", tz)
		}

		cancel()
		for i := range 2 { // two fresh connections, each re-initialized after the cancel
			conn, err := db.Conn(t.Context())
			if err != nil {
				t.Fatalf("connection %d after creation context canceled: %v", i, err)
			}
			var mc string
			if err := conn.QueryRowContext(t.Context(), "SELECT current_setting('TimeZone'), current_setting('parquet_metadata_cache')").Scan(&tz, &mc); err != nil {
				t.Fatalf("query on connection %d: %v", i, err)
			}
			if !strings.EqualFold(tz, "UTC") {
				t.Fatalf("connection %d TimeZone = %q, want UTC", i, tz)
			}
			if mc != "true" {
				t.Fatalf("connection %d parquet_metadata_cache = %q, want true", i, mc)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
}

// TestOpenDBConnectionSettings pins the session every OpenDB connection gets:
// TimeZone=UTC, the Parquet footer cache on, and the external-file byte cache
// left at its own default (Parkana never touches it). Two connections are held
// simultaneously, and a session-local override on one must not leak to the
// other: benchmark controls toggle parquet_metadata_cache per connection, and
// a leaked override would silently poison both arms of a comparison.
func TestOpenDBConnectionSettings(t *testing.T) {
	db := openTestDB(t)
	db.SetMaxIdleConns(0) // every Conn opens and initializes a fresh driver connection
	ctx := t.Context()
	c1, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	assertSettings := func(conn *sql.Conn, wantMeta string) {
		t.Helper()
		var tz, meta, ext, localFiles string
		err := conn.QueryRowContext(ctx,
			"SELECT current_setting('TimeZone'), current_setting('parquet_metadata_cache'), current_setting('enable_external_file_cache'), current_setting('cache_local_files')").
			Scan(&tz, &meta, &ext, &localFiles)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.EqualFold(tz, "UTC") {
			t.Errorf("TimeZone = %q, want UTC", tz)
		}
		if meta != wantMeta {
			t.Errorf("parquet_metadata_cache = %q, want %q", meta, wantMeta)
		}
		if ext != "true" {
			t.Errorf("enable_external_file_cache = %q, want the untouched default true", ext)
		}
		if localFiles != "false" {
			t.Errorf("cache_local_files = %q, want the untouched default false", localFiles)
		}
	}
	assertSettings(c1, "true")
	assertSettings(c2, "true")

	if _, err := c1.ExecContext(ctx, "SET parquet_metadata_cache=false"); err != nil {
		t.Fatal(err)
	}
	assertSettings(c1, "false")
	assertSettings(c2, "true")
}

// backdateFile moves a fixture's atime and mtime safely into the past. This is
// deterministic setup only — not a requirement for local footer reuse: the
// pinned reader validates local entries by a version tag over device, inode,
// size and mtime truncated to seconds, where tag equality decides validity
// outright. The tag-less remote branch of the same validity function would
// additionally require an mtime more than ten seconds old, so fixtures model
// that shape too rather than depending on wall-clock age.
func backdateFile(t *testing.T, path string, age time.Duration) {
	t.Helper()
	past := time.Now().Add(-age)
	if err := os.Chtimes(path, past, past); err != nil {
		t.Fatalf("backdate %s: %v", path, err)
	}
}

// flipLastByte XORs the final byte of path with mask in place through the
// same inode, reading the byte first so calling it twice restores the original
// value, and restores the exact nanosecond mtimes it saw, so size, mtime
// second and inode — the whole local version tag — stay identical across the
// flip. XORing the trailing PAR1 magic makes any on-disk footer re-read fail,
// so a footer-cache-on read that still succeeds proves the footer came from
// the cache, not from disk.
func flipLastByte(t *testing.T, path string, mask byte) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close() // covers the Fatal paths below; idempotent after the explicit Close
	var b [1]byte
	if _, err := f.ReadAt(b[:], st.Size()-1); err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	b[0] ^= mask
	if _, err := f.WriteAt(b[:], st.Size()-1); err != nil {
		t.Fatalf("flip %s: %v", path, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, st.ModTime(), st.ModTime()); err != nil {
		t.Fatalf("restore mtime %s: %v", path, err)
	}
}

// TestParquetMetadataCacheLifetime proves the correctness side of
// SET parquet_metadata_cache=true against real local files written by the
// storage stack: cache hits serve correct rows and never read the on-disk
// footer, an evicted-and-redownloaded immutable path reloads rather than
// serving stale footers, a disappeared file still surfaces the exact DuckDB
// error the Source's retry classification matches, and a same-path rewrite
// with a different layout (different size, new inode — the shape every
// Parkana Put produces through temp+rename) is never served stale. See the
// OpenDB doc comment for the pinned DuckDB validity chain and the layered
// Parkana lifecycle proof: revalidation makes any temp+rename rewrite reload
// its footer, and the producer lifecycle keeps same-key re-Puts off every
// key the catalog still knows about.
func TestParquetMetadataCacheLifetime(t *testing.T) {
	// countLine returns the only line of a one-row fixture, or the query error.
	countLine := func(t *testing.T, q rowQuerier, path string) (string, error) {
		t.Helper()
		var line string
		err := q.QueryRowContext(t.Context(), "SELECT min(line) FROM read_parquet(?)", path).Scan(&line)
		return line, err
	}

	t.Run("cached-footer-serves-without-disk-read", func(t *testing.T) {
		db := openTestDB(t)
		dir := t.TempDir()
		p := writeParquet(t, dir, "one.parquet", plantest.File{
			Tenant: tenant.Default, Stream: "s",
			Rows: []plan.Row{{TS: plantest.T0, Line: "alpha", Labels: logs.FromMap(map[string]string{"service_name": "s"})}},
		})
		backdateFile(t, p, 60*time.Second)
		// Dedicated connections for both arms, each with its settings pinned
		// by footerCacheConn (footer cache on/off; byte cache enabled but
		// never serving local files). The byte cache cannot explain a
		// successful post-corruption read: with cache_local_files=false,
		// footer bytes come from disk whenever the footer cache is off.
		connOn := footerCacheConn(t, db, true)
		connOff := footerCacheConn(t, db, false)

		if line, err := countLine(t, connOn, p); err != nil || line != "alpha" {
			t.Fatalf("first read = (%q, %v), want alpha", line, err)
		}

		// One flip must change the on-disk bytes; two flips must restore
		// them exactly. This is asserted on the raw bytes, not through
		// DuckDB, so a broken flip helper cannot hide behind the cache.
		orig, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		flipLastByte(t, p, 0xFF) // XOR the trailing PAR1 magic, tag unchanged
		once, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if once[len(once)-1] != orig[len(orig)-1]^0xFF {
			t.Fatalf("one flip left byte %#x, want %#x", once[len(once)-1], orig[len(orig)-1]^0xFF)
		}
		if line, err := countLine(t, connOn, p); err != nil || line != "alpha" {
			t.Fatalf("read after footer corruption = (%q, %v), want alpha from the cached footer", line, err)
		}
		// Control: with the footer cache off the same corrupt file must fail,
		// proving the toggle works and that the success above really came
		// from the footer cache, not from the byte cache or a lucky re-read.
		if _, err := countLine(t, connOff, p); err == nil || !strings.Contains(err.Error(), "No magic bytes found") {
			t.Fatalf("cache-off read of corrupt footer = %v, want a magic-bytes error", err)
		}

		flipLastByte(t, p, 0xFF) // XOR again restores the original byte
		twice, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(twice, orig) {
			t.Fatal("two flips did not restore the original file bytes")
		}
		// Prove the restoration through the cache-off connection: it must
		// read the repaired footer from disk and succeed.
		if line, err := countLine(t, connOff, p); err != nil || line != "alpha" {
			t.Fatalf("cache-off read after restore = (%q, %v), want alpha from the repaired file", line, err)
		}
		if line, err := countLine(t, connOn, p); err != nil || line != "alpha" {
			t.Fatalf("cache-on read after restore = (%q, %v), want alpha", line, err)
		}
	})

	t.Run("redownloaded-key-reloads-not-stale", func(t *testing.T) {
		db := openTestDB(t)
		dir := t.TempDir()
		f := plantest.File{Tenant: tenant.Default, Stream: "s",
			Rows: []plan.Row{{TS: plantest.T0, Line: "immutable", Labels: logs.FromMap(map[string]string{"service_name": "s"})}}}
		data, _, err := plantest.Encode(f)
		if err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(dir, "same-key.parquet")
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		backdateFile(t, p, 60*time.Second)
		if line, err := countLine(t, db, p); err != nil || line != "immutable" {
			t.Fatalf("first read = (%q, %v), want immutable", line, err)
		}

		// Evict, then redownload the same immutable key to the same path the
		// way the querier's object cache does: temp file, rename into place,
		// fresh mtime. Correct rows alone cannot distinguish a reload from a
		// still-valid cached footer (the bytes are identical), so assert the
		// provenance that forces the reload: same size, but a different
		// mtime second — two components of the local version tag. The tag
		// therefore differs, the cached footer cannot be reused, and the
		// correct rows come from a fresh footer load. What this test does
		// NOT prove — that a same-tag rewrite with a different layout would
		// be caught — is exactly the unreachable residual documented on
		// OpenDB; the different-layout subtest is the discriminating control.
		before, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
		tmp := p + ".part"
		if err := os.WriteFile(tmp, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, p); err != nil {
			t.Fatal(err)
		}
		after, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		if after.Size() != before.Size() {
			t.Fatalf("redownloaded size = %d, want %d (same immutable bytes)", after.Size(), before.Size())
		}
		if after.ModTime().Equal(before.ModTime()) || after.ModTime().Unix() == before.ModTime().Unix() {
			t.Fatalf("redownloaded mtime %v not in a different second than %v; the version tag would not be forced to change", after.ModTime(), before.ModTime())
		}
		if line, err := countLine(t, db, p); err != nil || line != "immutable" {
			t.Fatalf("read after redownload = (%q, %v), want immutable", line, err)
		}
		if line, err := countLine(t, db, p); err != nil || line != "immutable" {
			t.Fatalf("warm read after redownload = (%q, %v), want immutable", line, err)
		}
	})

	t.Run("disappeared-file-still-surfaces", func(t *testing.T) {
		db := openTestDB(t)
		dir := t.TempDir()
		p := writeParquet(t, dir, "gone.parquet", plantest.File{
			Tenant: tenant.Default, Stream: "s",
			Rows: []plan.Row{{TS: plantest.T0, Line: "here", Labels: logs.FromMap(map[string]string{"service_name": "s"})}},
		})
		backdateFile(t, p, 60*time.Second)
		if line, err := countLine(t, db, p); err != nil || line != "here" {
			t.Fatalf("first read = (%q, %v), want here", line, err)
		}
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
		_, err := countLine(t, db, p)
		if err == nil {
			t.Fatal("read of a deleted input succeeded; the footer cache must not resurrect files")
		}
		lower := strings.ToLower(err.Error())
		if !strings.Contains(lower, "no files found that match the pattern") {
			t.Errorf("deleted-input error = %v, want the disappearance phrase the Source's retry classification matches", err)
		}
		if !strings.Contains(err.Error(), p) {
			t.Errorf("deleted-input error = %v, want it to name %s", err, p)
		}
	})

	t.Run("different-layout-rewrite-never-stale", func(t *testing.T) {
		db := openTestDB(t)
		dir := t.TempDir()
		f := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{"level"},
			Rows: []plan.Row{
				{TS: plantest.T0, Line: "r1", Labels: logs.FromMap(map[string]string{"service_name": "s", "level": "info"})},
				{TS: plantest.T0.Add(time.Second), Line: "r2", Labels: logs.FromMap(map[string]string{"service_name": "s", "level": "warn"})},
				{TS: plantest.T0.Add(2 * time.Second), Line: "r3", Labels: logs.FromMap(map[string]string{"service_name": "s", "level": "error"})},
			}}
		data, _, err := plantest.Encode(f)
		if err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(dir, "rewrite.parquet")
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		backdateFile(t, p, 60*time.Second)
		var n int
		if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM read_parquet(?)", p).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("rows before rewrite = %d, want 3", n)
		}

		// Replace the path's layout the way every Parkana Put does: different
		// bytes and size, written to a temp name and renamed over the key.
		// Size and inode change the version tag, the footer reloads and the
		// new layout is what SQL sees.
		repl := plantest.File{Tenant: tenant.Default, Stream: "s",
			Rows: []plan.Row{{TS: plantest.T0, Line: "replaced", Labels: logs.FromMap(map[string]string{"service_name": "s"})}}}
		data2, _, err := plantest.Encode(repl)
		if err != nil {
			t.Fatal(err)
		}
		if len(data2) == len(data) {
			t.Fatalf("replacement fixture is %d bytes, same as the original; the rewrite would not change the version tag", len(data2))
		}
		tmp := p + ".part"
		if err := os.WriteFile(tmp, data2, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, p); err != nil {
			t.Fatal(err)
		}
		var line string
		if err := db.QueryRowContext(t.Context(), "SELECT count(*), min(line) FROM read_parquet(?)", p).Scan(&n, &line); err != nil {
			t.Fatal(err)
		}
		if n != 1 || line != "replaced" {
			t.Fatalf("after rewrite = (%d rows, %q), want (1, replaced) from the new layout (no stale footer)", n, line)
		}
	})
}

// rowQuerier is the read surface the cache-lifetime probes run on: either the
// pooled *sql.DB or one dedicated *sql.Conn.
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}
