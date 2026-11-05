package query

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// TestSelectSQLParameterized checks that compiled SQL never splices client
// strings: hostile matcher names and values, line filter values and file
// paths appear only as bound arguments, identifiers are quoted with doubled
// quotes, the placeholder count matches the argument count, and unmatched
// labels stay map accesses instead of nonexistent column references.
// TestSelectSQLExecution proves the same statements against real DuckDB.
func TestSelectSQLParameterized(t *testing.T) {
	hostileName := `we"ird OR 1=1--`
	hostileValue := `x'; DROP TABLE t;--`
	sel := plan.Selection{
		Tenant: tenant.Default,
		Matchers: []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, hostileName, hostileValue),
			plan.MustMatcher(plan.MatchRegexp, "env", `pro\d`),
			plan.MustMatcher(plan.MatchEqual, "level", "info"),
		},
		LineFilters: []plan.LineFilter{plan.MustLineFilter(plan.LineContains, "%'_`\\' OR '1'='1")},
		From:        plantest.T0,
		Through:     plantest.T0.Add(time.Hour),
		Direction:   plan.Backward,
		Limit:       42,
	}
	paths := []string{`/tmp/o'brien/a"b.parquet`, `/tmp/x y/ünïcode.parquet`}
	groups := []fileGroup{
		{promoted: []string{`we"ird`, "level"}, paths: paths},
		{promoted: nil, paths: paths[:1]},
	}
	q := selectSQL(sel, groups)

	for _, leak := range []string{hostileValue, "OR 1=1", `o'brien`, "ünïcode", "%'_", `pro\d`} {
		if strings.Contains(q.sql, leak) {
			t.Fatalf("client string %q spliced into SQL: %s", leak, q.sql)
		}
	}
	if n, args := strings.Count(q.sql, "?"), len(q.args); n != args {
		t.Errorf("%d placeholders for %d arguments: %s", n, args, q.sql)
	}
	if !strings.Contains(q.sql, `"we""ird"`) {
		t.Errorf("hostile promoted column not safely quoted: %s", q.sql)
	}
	if !strings.Contains(q.sql, " ORDER BY ") || !strings.Contains(q.sql, " DESC") {
		t.Errorf("backward direction missing its sort: %s", q.sql)
	}
	if !strings.HasSuffix(q.sql, " LIMIT ?") {
		t.Errorf("limit not bound last: %s", q.sql)
	}
	if !slices.Contains(q.args, any(paths[0])) || !slices.Contains(q.args, any(paths[1])) {
		t.Errorf("paths not bound: %v", q.args)
	}
	if !slices.Contains(q.args, any(hostileName)) || !slices.Contains(q.args, any(hostileValue)) {
		t.Errorf("hostile matcher name/value not bound: %v", q.args)
	}
	if last := q.args[len(q.args)-1]; last != 42 {
		t.Errorf("last argument = %v, want the limit 42", last)
	}
	// Two groups with different promoted sets: one statement, per-group
	// subqueries, UNION ALL'd.
	if got := strings.Count(q.sql, "UNION ALL"); got != len(groups)-1 {
		t.Errorf("%d UNION ALL separators for %d groups: %s", got, len(groups), q.sql)
	}
	// Every group builds its canonical label map and its row-identity
	// metadata map in SQL.
	if got := strings.Count(q.sql, "map_from_entries"); got != 2*len(groups) {
		t.Errorf("canonical map expressions = %d, want %d (labels plus identity metadata per group): %s", got, 2*len(groups), q.sql)
	}
	if !strings.Contains(q.sql, "list_sort") || !strings.Contains(q.sql, "list_contains") {
		t.Errorf("canonical map pipeline missing: %s", q.sql)
	}
	// Promoted matchers read the column with a map fallback; a label no
	// group promotes stays a map access with no identifier.
	if !strings.Contains(q.sql, `coalesce(coalesce("level", "labels"[?]), '')`) {
		t.Errorf("promoted label expression shape missing: %s", q.sql)
	}
	if strings.Contains(q.sql, `"env"`) || strings.Contains(q.sql, `"foo"`) {
		t.Errorf("unpromoted matcher names became column references: %s", q.sql)
	}
	if !strings.Contains(q.sql, `coalesce("labels"[?], '')`) {
		t.Errorf("map-only label expression shape missing: %s", q.sql)
	}

	q = selectSQL(plan.Selection{
		Tenant: tenant.Default,
		Matchers: []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "foo", "x"),
		},
		From: plantest.T0, Through: plantest.T0.Add(time.Hour),
	}, groups[:1])
	if strings.Contains(q.sql, " LIMIT") {
		t.Errorf("zero limit must not add LIMIT: %s", q.sql)
	}
}

// TestResidualIdentityDirectMapSQL pins the canonical metadata identity as a
// direct transformation of the stored MAP: each group sorts that map's
// entries and serializes the rebuilt map once. It compiles through the
// existing selectSQL/aggregateSQL entrypoints (no helper added by the direct
// path), so the same test compiles against the baseline: there the raw
// meta_json was parsed back with CAST(meta_json AS JSON) and serialized a
// second time in the DISTINCT wrapper, which this regression rejects.
// IncludeMeta on and off both carry the row identity, so both are checked.
func TestResidualIdentityDirectMapSQL(t *testing.T) {
	sel := plan.Selection{
		Tenant: tenant.Default,
		Matchers: []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "service_name", "s"),
		},
		From: plantest.T0, Through: plantest.T0.Add(time.Hour),
		Direction: plan.Forward,
	}
	groups := []fileGroup{
		{promoted: []string{"level"}, paths: []string{"/tmp/a.parquet"}},
		{promoted: nil, paths: []string{"/tmp/b.parquet"}},
	}
	check := func(t *testing.T, q selectQuery) {
		t.Helper()
		// No JSON read-back of the metadata identity.
		if strings.Contains(q.sql, "AS JSON") || strings.Contains(q.sql, "meta_json AS") {
			t.Errorf("metadata identity parses JSON back into a map: %s", q.sql)
		}
		// Exactly one labels and one metadata serialization per group; the
		// wrapper adds none.
		if got, want := strings.Count(q.sql, "to_json"), 2*len(groups); got != want {
			t.Errorf("to_json count = %d, want %d (labels plus direct metadata per group): %s", got, want, q.sql)
		}
		if got := strings.Count(q.sql, `map_entries(coalesce("meta", map([], [])))`); got != len(groups) {
			t.Errorf("direct stored-MAP metadata canonicalization count = %d, want %d: %s", got, len(groups), q.sql)
		}
	}
	check(t, selectSQL(sel, groups))
	for _, includeMeta := range []bool{false, true} {
		agg := plan.BucketAgg{Width: time.Minute, Origin: plantest.T0, IncludeMeta: includeMeta}
		check(t, aggregateSQL(sel, agg, groups))
	}
}

// directMetaEntry is one stored metadata map entry; null writes a NULL map
// value, which no plan.Row can express.
type directMetaEntry struct {
	key   string
	value string
	null  bool
}

// directMetaRow is one hand-written row for the direct metadata
// canonicalization test. A nil meta slice writes a NULL meta column; an empty
// non-nil slice writes an empty map.
type directMetaRow struct {
	ts     time.Time
	line   string
	labels logs.Labels
	meta   []directMetaEntry
}

// writeDirectMetaFile writes rows as one Parquet file in Parkana's base schema
// and returns its path. The meta field is made nullable in this test-only
// schema so a NULL map column survives the writer (the ingester's schema
// leaves it non-nullable and normalizes NULL to an empty map).
func writeDirectMetaFile(t *testing.T, dir, name string, rows []directMetaRow) string {
	t.Helper()
	fields := schema.Base()
	fields[3].Nullable = true
	sc := arrow.NewSchema(fields, nil)
	rb := array.NewRecordBuilder(memory.NewGoAllocator(), sc)
	defer rb.Release()
	builders := rb.Fields()
	labels := builders[2].(*array.MapBuilder)
	meta := builders[3].(*array.MapBuilder)
	for _, r := range rows {
		builders[0].(*array.TimestampBuilder).Append(arrow.Timestamp(schema.TS(r.ts)))
		builders[1].(*array.StringBuilder).Append(r.line)

		labels.Append(true)
		lb := labels.KeyBuilder().(*array.StringBuilder)
		ib := labels.ItemBuilder().(*array.StringBuilder)
		for _, l := range r.labels {
			lb.Append(l.Name)
			ib.Append(l.Value)
		}

		if r.meta == nil {
			meta.AppendNull()
			continue
		}
		meta.Append(true)
		mkb := meta.KeyBuilder().(*array.StringBuilder)
		mib := meta.ItemBuilder().(*array.StringBuilder)
		for _, e := range r.meta {
			mkb.Append(e.key)
			if e.null {
				mib.AppendNull()
			} else {
				mib.Append(e.value)
			}
		}
	}
	rec := rb.NewRecordBatch()
	defer rec.Release()
	var buf bytes.Buffer
	w, err := storage.NewWriter(&buf, sc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestSelectDirectMapMetadataSQL executes selectSQL over real DuckDB with
// hand-written metadata maps and checks the raw canonical JSON and row
// counts. It covers physical key reordering (one row), NULL and empty maps
// (one row), a present empty value and a NULL map value (each distinct from
// the empty set, two rows), and awkward keys and values (quotes, backslashes,
// newlines, tabs, non-ASCII) stored in two orders (one row). The NULL-value
// proof stays at SQL level because decodeMap coalesces NULL to "".
func TestSelectDirectMapMetadataSQL(t *testing.T) {
	base := plantest.T0
	labels := logs.FromMap(map[string]string{"service_name": "s"})
	newRow := func(ts time.Time, line string, meta []directMetaEntry) directMetaRow {
		return directMetaRow{ts: ts, line: line, labels: labels, meta: meta}
	}
	escapedA := []directMetaEntry{
		{key: `a"b`, value: `x"y`},
		{key: `back\slash`, value: "line1\nline2"},
		{key: "tab\tkey", value: "tab\tvalue"},
		{key: "ünïcode", value: "café ☕"},
		{key: "empty", value: ""},
	}
	escapedB := slices.Clone(escapedA)
	slices.Reverse(escapedB)

	fileA := []directMetaRow{
		newRow(base, "reorder", []directMetaEntry{
			{key: "a", value: "1"}, {key: "b", value: "2"}, {key: "c", value: "3"},
		}),
		newRow(base.Add(time.Second), "null-empty", nil),
		newRow(base.Add(2*time.Second), "present-empty", []directMetaEntry{}),
		newRow(base.Add(3*time.Second), "null-value", []directMetaEntry{{key: "k", null: true}}),
		newRow(base.Add(4*time.Second), "escapes", escapedA),
	}
	fileB := []directMetaRow{
		newRow(base, "reorder", []directMetaEntry{
			{key: "c", value: "3"}, {key: "a", value: "1"}, {key: "b", value: "2"},
		}),
		newRow(base.Add(time.Second), "null-empty", []directMetaEntry{}),
		newRow(base.Add(2*time.Second), "present-empty", []directMetaEntry{{key: "trace_id", value: ""}}),
		newRow(base.Add(3*time.Second), "null-value", []directMetaEntry{{key: "k", value: ""}}),
		newRow(base.Add(4*time.Second), "escapes", escapedB),
	}

	dir := t.TempDir()
	paths := []string{
		writeDirectMetaFile(t, dir, "a.parquet", fileA),
		writeDirectMetaFile(t, dir, "b.parquet", fileB),
	}
	db := openTestDB(t)
	ctx := t.Context()
	// Prove the hand-written inputs really carry the shapes this regression
	// names: a NULL meta column (not normalized to an empty map) and a map
	// with a NULL value, and two different physical key orders for reorder.
	var nullMaps, nullValues int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FILTER (WHERE "meta" IS NULL),
		        count(*) FILTER (WHERE len(list_filter(map_values("meta"), lambda v: v IS NULL)) > 0)
		 FROM read_parquet(?)`, paths[0],
	).Scan(&nullMaps, &nullValues); err != nil {
		t.Fatalf("precheck physical metadata shape: %v", err)
	}
	if nullMaps != 1 || nullValues != 1 {
		t.Fatalf("fixture has %d NULL meta columns and %d NULL map values, want 1 and 1", nullMaps, nullValues)
	}
	rawOrder := func(p string) string {
		t.Helper()
		var s string
		if err := db.QueryRowContext(ctx,
			`SELECT CAST(to_json("meta") AS VARCHAR) FROM read_parquet(?) WHERE "line" = 'reorder'`, p,
		).Scan(&s); err != nil {
			t.Fatalf("precheck raw key order: %v", err)
		}
		return s
	}
	if rawOrder(paths[0]) == rawOrder(paths[1]) {
		t.Fatal("reorder fixture stores one physical key order; the canonicalization proof is vacuous")
	}

	q := selectSQL(plan.Selection{
		Tenant:   tenant.Default,
		Matchers: []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "service_name", "s")},
		From:     base.Add(-time.Hour), Through: base.Add(time.Hour),
		Direction: plan.Forward,
	}, []fileGroup{{paths: paths}})

	rows, err := db.QueryContext(ctx, q.sql, q.args...)
	if err != nil {
		t.Fatalf("execute: %v\nSQL: %s", err, q.sql)
	}
	defer rows.Close()
	metaByLine := make(map[string][]string)
	for rows.Next() {
		var ts time.Time
		var line string
		var labelsJSON, metaJSON sql.NullString
		if err := rows.Scan(&ts, &line, &labelsJSON, &metaJSON); err != nil {
			t.Fatal(err)
		}
		metaByLine[line] = append(metaByLine[line], metaJSON.String)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	for line, want := range map[string]int{
		"reorder":       1,
		"null-empty":    1,
		"present-empty": 2,
		"null-value":    2,
		"escapes":       1,
	} {
		if got := len(metaByLine[line]); got != want {
			t.Errorf("line %q returned %d rows, want %d: %v", line, got, want, metaByLine[line])
		}
	}
	if got := metaByLine["reorder"]; len(got) == 1 && got[0] != `{"a":"1","b":"2","c":"3"}` {
		t.Errorf("reordered metadata = %s, want the canonically key-sorted map", got[0])
	}
	if got := metaByLine["null-empty"]; len(got) == 1 && got[0] != "{}" {
		t.Errorf("NULL and empty metadata = %s, want {}", got[0])
	}
	if got := metaByLine["present-empty"]; !slices.Contains(got, "{}") || !slices.Contains(got, `{"trace_id":""}`) {
		t.Errorf("present empty value rows = %v, want the empty set and trace_id=\"\" distinct", got)
	}
	if got := metaByLine["null-value"]; !slices.Contains(got, `{"k":null}`) || !slices.Contains(got, `{"k":""}`) {
		t.Errorf("NULL map value rows = %v, want a JSON null distinct from the empty string", got)
	}
	if got := metaByLine["escapes"]; len(got) == 1 {
		decoded, err := decodeMap(sql.NullString{String: got[0], Valid: true})
		if err != nil {
			t.Fatalf("decode escaped metadata %q: %v", got[0], err)
		}
		want := map[string]string{
			`a"b`:        `x"y`,
			`back\slash`: "line1\nline2",
			"tab\tkey":   "tab\tvalue",
			"ünïcode":    "café ☕",
			"empty":      "",
		}
		if !maps.Equal(decoded, want) {
			t.Errorf("escaped metadata = %v, want %v (raw %s)", decoded, want, got[0])
		}
	}
}

// TestCeilMicro pins the microsecond ceiling for nanosecond bounds,
// including pre-epoch times where floor-plus-one must still be the exact
// ceiling.
func TestCeilMicro(t *testing.T) {
	for _, tc := range []struct {
		in   time.Time
		want int64
	}{
		{time.UnixMicro(5).UTC(), 5},
		{time.UnixMicro(5).Add(1 * time.Nanosecond), 6},
		{time.UnixMicro(5).Add(500 * time.Nanosecond), 6},
		{time.UnixMicro(5).Add(999 * time.Nanosecond), 6},
		{time.UnixMicro(5).Add(1500 * time.Nanosecond), 7},
		{time.UnixMicro(-5).UTC(), -5},
		{time.UnixMicro(-5).Add(500 * time.Nanosecond), -4},
		{time.UnixMicro(-5).Add(1500 * time.Nanosecond), -3},
		{time.UnixMicro(-5).Add(-500 * time.Nanosecond), -5},
		{plantest.T0, plantest.T0.UnixMicro()},
	} {
		if got := ceilMicro(tc.in); got != tc.want {
			t.Errorf("ceilMicro(%s) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// mapResolver resolves keys to fixed local paths, checking tenant ownership.
type mapResolver map[string]string

func (m mapResolver) Acquire(ctx context.Context, t tenant.ID, key string) (string, func(), error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if !strings.HasPrefix(key, string(t)+"/") {
		return "", nil, fmt.Errorf("map resolver: key %q is not owned by tenant %q", key, t)
	}
	p, ok := m[key]
	if !ok {
		return "", nil, fmt.Errorf("map resolver: %s: no such object", key)
	}
	return p, func() {}, nil
}

// TestSelectSQLExecution runs compiled statements through real DuckDB over
// files with hostile promoted column names, reserved-looking label names in
// the map, mixed promotion and missing labels, comparing complete rows with
// the MemSource reference: parameterization proven by execution, not string
// snapshots.
func TestSelectSQLExecution(t *testing.T) {
	rw := func(ts time.Time, line string, ls map[string]string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	base := plantest.T0
	fCol := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{`we"ird`, "level"}, Rows: []plan.Row{
		// level promoted as a column, we"ird promoted as a column.
		rw(base, `c1 100% "done"`, map[string]string{"service_name": "s", `we"ird`: "x", "level": "info"}),
		rw(base.Add(time.Second), `c2 it's \ok`, map[string]string{"service_name": "s", `we"ird`: "y", "level": "warn"}),
	}}
	fMap := plantest.File{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{
		// level only in the map, in a file with no promotion at all.
		rw(base.Add(2*time.Second), "m1 error", map[string]string{"service_name": "s", `we"ird`: "x", "level": "error"}),
		// both labels missing entirely (mixed-null empty-equality case).
		rw(base.Add(3*time.Second), "m2 plain", map[string]string{"service_name": "s"}),
		// reserved-looking names are ordinary map labels, never columns.
		rw(base.Add(4*time.Second), "m3 reserved", map[string]string{
			"service_name": "s", "line": "as a label", "labels": "as a label", "_ts": "not a timestamp"}),
	}}
	dir := t.TempDir()

	cat := catalog.NewMemory()
	resolve := mapResolver{}
	for _, f := range []plantest.File{fCol, fMap} {
		data, st, err := plantest.Encode(f)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("%s/logs/s/%s.parquet", f.Tenant, uuid.NewV7())
		p := filepath.Join(dir, fmt.Sprintf("%s.parquet", uuid.NewV7()))
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		resolve[key] = p
		if err := cat.AddFiles(t.Context(), catalog.File{
			Tenant: f.Tenant, Stream: f.Stream, Key: key,
			MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(len(data)),
			LabelValues: st.LabelValues, CreatedAt: time.Now().Truncate(time.Microsecond),
		}); err != nil {
			t.Fatal(err)
		}
	}
	src := newSource(openTestDB(t), cat, resolve, defaultStreamLabels)
	ref := plantest.Mem([]plantest.File{fCol, fMap})

	eq := func(n, v string) plan.Matcher { return plan.MustMatcher(plan.MatchEqual, n, v) }
	ne := func(n, v string) plan.Matcher { return plan.MustMatcher(plan.MatchNotEqual, n, v) }
	re := func(n, v string) plan.Matcher { return plan.MustMatcher(plan.MatchRegexp, n, v) }
	contains := func(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineContains, v) }
	lre := func(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineRegexp, v) }
	from, through := base.Add(-time.Hour), base.Add(time.Hour)

	for _, tc := range []struct {
		name     string
		matchers []plan.Matcher
		filters  []plan.LineFilter
		limit    int
	}{
		{name: "hostile-promoted-equal", matchers: []plan.Matcher{eq(`we"ird`, "x")}},
		{name: "hostile-promoted-regexp", matchers: []plan.Matcher{re(`we"ird`, "x|y")}},
		{name: "level-mixed-promotion", matchers: []plan.Matcher{eq("level", "error")}},
		{name: "empty-equality-mixed-null", matchers: []plan.Matcher{eq("service_name", "s"), eq("level", "")}},
		{name: "not-equal-missing-label", matchers: []plan.Matcher{eq("service_name", "s"), ne("level", "info")}},
		{name: "reserved-names-are-map-labels", matchers: []plan.Matcher{eq("_ts", "not a timestamp"), eq("line", "as a label"), eq("labels", "as a label")}},
		{name: "absent-label-equals-empty", matchers: []plan.Matcher{eq("foo", "")}},
		{name: "contradictory-equalities", matchers: []plan.Matcher{eq("level", "info"), eq("level", "error")}},
		{name: "line-hostile-quotes", filters: []plan.LineFilter{contains(`"done"`)}, matchers: []plan.Matcher{eq("service_name", "s")}},
		{name: "line-percent", filters: []plan.LineFilter{contains("100%")}, matchers: []plan.Matcher{eq("service_name", "s")}},
		{name: "line-backslash-regexp", filters: []plan.LineFilter{lre(`\\ok`)}, matchers: []plan.Matcher{eq("service_name", "s")}},
		{name: "limit-backward", matchers: []plan.Matcher{eq("service_name", "s")}, limit: 2},
	} {
		for _, dir := range []plan.Direction{plan.Forward, plan.Backward} {
			sel := plan.Selection{
				Tenant: tenant.Default, Matchers: tc.matchers, LineFilters: tc.filters,
				From: from, Through: through, Direction: dir, Limit: tc.limit,
			}
			t.Run(fmt.Sprintf("%s/%s", tc.name, dir), func(t *testing.T) {
				got, err := collectRows(src.Select(t.Context(), sel))
				if err != nil {
					t.Fatalf("Select: %v", err)
				}
				want, err := collectRows(ref.Select(t.Context(), sel))
				if err != nil {
					t.Fatal(err)
				}
				if tc.limit == 0 && len(got) != len(want) {
					t.Fatalf("got %d rows, want %d", len(got), len(want))
				}
				if tc.limit > 0 {
					if len(got) != tc.limit {
						t.Fatalf("got %d rows, want the limit %d", len(got), tc.limit)
					}
					for i := range got {
						if !got[i].TS.Equal(want[i].TS) {
							t.Fatalf("row %d TS = %s, want %s", i, got[i].TS, want[i].TS)
						}
					}
				}
				keys := func(rs []plan.Row) []string {
					out := make([]string, len(rs))
					for i, r := range rs {
						out[i] = rowKey(r)
					}
					return out
				}
				g, w := keys(got), keys(want)
				slices.Sort(g)
				slices.Sort(w)
				if !slices.Equal(g, w) {
					t.Fatalf("rows differ:\n got  %v\n want %v", g, w)
				}
			})
		}
	}
}

// TestSelectCaseSensitiveLabels regresses Parkana's case-sensitive label
// semantics against DuckDB's case-insensitive identifiers (even quoted, per
// its keywords_and_identifiers docs). The schema contract now rejects
// same-file case-fold collisions and promoted names folding onto base
// columns (asserted in TestDuckDBCaseFoldingBehavior), so what remains —
// and what this regression covers — is the cross-file case: one file of a
// stream promotes "level", another "Level", and map labels like "LINE" and
// "Labels" differ from base column names only in case. Every matcher and
// every reconstructed label set is compared with the MemSource reference.
func TestSelectCaseSensitiveLabels(t *testing.T) {
	rw := func(ts time.Time, line string, ls map[string]string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	base := plantest.T0
	fileLower := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{"level"}, Rows: []plan.Row{
		rw(base, "from lower file", map[string]string{"service_name": "s", "level": "info"}),
		rw(base.Add(time.Second), "lower without", map[string]string{"service_name": "s"}),
	}}
	fileUpper := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{"Level"}, Rows: []plan.Row{
		rw(base.Add(2*time.Second), "from upper file", map[string]string{"service_name": "s", "Level": "SEVERE", "LINE": "x", "Labels": "y"}),
		rw(base.Add(3*time.Second), "upper without", map[string]string{"service_name": "s"}),
	}}

	eq := func(n, v string) plan.Matcher { return plan.MustMatcher(plan.MatchEqual, n, v) }
	ne := func(n, v string) plan.Matcher { return plan.MustMatcher(plan.MatchNotEqual, n, v) }
	re := func(n, v string) plan.Matcher { return plan.MustMatcher(plan.MatchRegexp, n, v) }
	from, through := base.Add(-time.Hour), base.Add(time.Hour)

	src, ref := caseTestEnv(t, fileLower, fileUpper)
	for _, tc := range []struct {
		name     string
		matchers []plan.Matcher
	}{
		{"lower-file-only", []plan.Matcher{eq("level", "info")}},
		{"upper-file-only", []plan.Matcher{eq("Level", "SEVERE")}},
		{"lower-missing-is-upper", []plan.Matcher{eq("level", "")}},
		{"upper-missing-is-lower", []plan.Matcher{eq("Level", "")}},
		{"case-insensitive-regex-stays-exact", []plan.Matcher{re("level", "(?i)SEVERE|info")}},
		{"map-labels-case-distinct", []plan.Matcher{eq("LINE", "x"), eq("Labels", "y")}},
		{"map-label-case-variant-absent", []plan.Matcher{eq("service_name", "s"), eq("line", ""), eq("labels", "")}},
		{"not-equal-sees-only-its-case", []plan.Matcher{eq("service_name", "s"), ne("Level", "")}},
		{"all-rows", []plan.Matcher{eq("service_name", "s")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertRowsAgainstRef(t, src, ref, plan.Selection{
				Tenant: tenant.Default, Matchers: tc.matchers, From: from, Through: through, Direction: plan.Forward,
			})
		})
	}
}

// TestSelectCanonicalLabels checks the normalization contract downstream
// packages rely on: the complete label set is built as one canonical,
// case-sensitive map in SQL, so the same logical label set stored under
// different promotion layouts yields identical labels — ready for B1c's
// GROUP BY and B2c's DISTINCT over the same normalized relation. Level and
// level stay distinct labels throughout. The proof runs at both levels:
// decoded labels against MemSource, and the RAW labels_json strings the
// relation emits (what a SQL GROUP BY would key on), which must be
// byte-identical across layouts — three distinct values for six rows.
func TestSelectCanonicalLabels(t *testing.T) {
	rw := func(ts time.Time, line string, ls map[string]string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	base := plantest.T0
	rows := []plan.Row{
		rw(base, "r1", map[string]string{"service_name": "s", "env": "prod", "Level": "High", "level": "info"}),
		rw(base.Add(time.Second), "r2", map[string]string{"service_name": "s", "Level": "Low"}),
		rw(base.Add(2*time.Second), "r3", map[string]string{"service_name": "s"}),
	}
	fCol := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{"Level", "env"}, Rows: rows}
	fMap := plantest.File{Tenant: tenant.Default, Stream: "s", Rows: rows} // same rows, everything in the map

	dir := t.TempDir()
	cat := catalog.NewMemory()
	resolve := mapResolver{}
	var cfs []catalog.File
	var paths []string
	for _, f := range []plantest.File{fCol, fMap} {
		data, st, err := plantest.Encode(f)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("%s/logs/s/%s.parquet", tenant.Default, uuid.NewV7())
		p := filepath.Join(dir, fmt.Sprintf("%s.parquet", uuid.NewV7()))
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		resolve[key] = p
		cf := catalog.File{
			Tenant: f.Tenant, Stream: f.Stream, Key: key,
			MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(len(data)),
			LabelValues: st.LabelValues, CreatedAt: time.Now().Truncate(time.Microsecond),
		}
		if err := cat.AddFiles(t.Context(), cf); err != nil {
			t.Fatal(err)
		}
		cfs = append(cfs, cf)
		paths = append(paths, p)
	}
	db := openTestDB(t)
	src := newSource(db, cat, resolve, defaultStreamLabels)
	ref := plantest.Mem([]plantest.File{fCol, fMap})
	sel := plan.Selection{
		Tenant: tenant.Default,
		Matchers: []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "service_name", "s"),
		},
		From: base.Add(-time.Hour), Through: base.Add(time.Hour), Direction: plan.Forward,
	}
	// Each logical row appears twice (once per file) with identical labels;
	// the MemSource multiset comparison enforces exact label equality.
	assertRowsAgainstRef(t, src, ref, sel)

	// Raw-proof level: execute the raw normalized relation directly, before
	// the row-identity DISTINCT wrapper, and require byte-identical
	// labels_json for the copies of each logical row, and exactly len(rows)
	// distinct values overall. The duplicate copies stay visible here: the
	// physical proof is of layout canonicalization, not of dedup.
	c := compiler{}
	normRelation(&c, sel, groupFiles(cfs, paths))
	q := selectQuery{sql: c.b.String(), args: c.args}
	qr, err := db.QueryContext(t.Context(), q.sql, q.args...)
	if err != nil {
		t.Fatalf("execute normalized relation: %v", err)
	}
	defer qr.Close()
	byLine := make(map[string]string)
	distinct := make(map[string]int)
	for qr.Next() {
		var ts time.Time
		var line string
		var labelsJSON, metaJSON sql.NullString
		if err := qr.Scan(&ts, &line, &labelsJSON, &metaJSON); err != nil {
			t.Fatal(err)
		}
		if prev, ok := byLine[line]; ok && prev != labelsJSON.String {
			t.Errorf("row %q raw labels_json differs across promotion layouts:\n %s\n %s", line, prev, labelsJSON.String)
		}
		byLine[line] = labelsJSON.String
		distinct[labelsJSON.String]++
	}
	if err := qr.Err(); err != nil {
		t.Fatal(err)
	}
	if len(distinct) != len(rows) {
		t.Errorf("%d distinct labels_json values, want %d (one per logical row): %v", len(distinct), len(rows), distinct)
	}
	for js, n := range distinct {
		if n != 2 {
			t.Errorf("labels_json %s appears %d times, want 2 (both layouts)", js, n)
		}
	}
}

// TestSelectMergedMapFallback regresses compaction against query pruning:
// storage.Merge unions schemas additively, so a merged file that promotes
// level keeps map-only inputs' rows with level in the labels map beside a
// NULL column. Complete summaries (catalog.File.LabelsComplete) record the
// effective value for every row, including map fallbacks (decision 13), so
// the merged file's summary covers both the column and the map values. A
// level=warn query must still find the map-fallback rows: Select prunes with
// catalog.Filter.Matches over the equality matchers, and the complete
// summary's values include the map fallback, so pruning cannot drop them.
func TestSelectMergedMapFallback(t *testing.T) {
	rw := func(ts time.Time, line string, ls map[string]string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	base := plantest.T0
	colFile := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{"level"}, Rows: []plan.Row{
		rw(base, "column row", map[string]string{"service_name": "s", "level": "info"}),
	}}
	mapFile := plantest.File{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{
		rw(base.Add(time.Second), "map row", map[string]string{"service_name": "s", "level": "warn"}),
	}}
	var readers []*storage.Reader
	for _, f := range []plantest.File{colFile, mapFile} {
		data, _, err := plantest.Encode(f)
		if err != nil {
			t.Fatal(err)
		}
		r, err := storage.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { r.Close() })
		readers = append(readers, r)
	}
	var merged bytes.Buffer
	st, err := storage.Merge(t.Context(), &merged, readers, []string{"level"})
	if err != nil {
		t.Fatal(err)
	}
	// Complete summaries include the map-fallback value as well as the
	// column value: the merged file's summary lists both effective values.
	if got := st.LabelValues["level"]; !slices.Equal(got, []string{"info", "warn"}) {
		t.Fatalf("merged LabelValues[level] = %v, want complete values [info warn]", got)
	}
	if !st.LabelsComplete {
		t.Fatal("merged LabelsComplete = false, want true for a complete summary")
	}
	p := filepath.Join(t.TempDir(), "merged.parquet")
	if err := os.WriteFile(p, merged.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewMemory()
	key := fmt.Sprintf("%s/logs/s/%s.parquet", tenant.Default, uuid.NewV7())
	if err := cat.AddFiles(t.Context(), catalog.File{
		Tenant: tenant.Default, Stream: "s", Key: key,
		MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(merged.Len()),
		LabelValues: st.LabelValues, LabelsComplete: st.LabelsComplete,
		CreatedAt: time.Now().Truncate(time.Microsecond),
	}); err != nil {
		t.Fatal(err)
	}
	src := newSource(openTestDB(t), cat, mapResolver{key: p}, defaultStreamLabels)
	ref := plantest.Mem([]plantest.File{colFile, mapFile})
	eq := func(n, v string) plan.Matcher { return plan.MustMatcher(plan.MatchEqual, n, v) }
	for _, tc := range []struct {
		name     string
		matchers []plan.Matcher
	}{
		{"map-fallback-value", []plan.Matcher{eq("level", "warn")}},
		{"column-value", []plan.Matcher{eq("level", "info")}},
		{"all-rows", []plan.Matcher{eq("service_name", "s")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertRowsAgainstRef(t, src, ref, plan.Selection{
				Tenant: tenant.Default, Matchers: tc.matchers,
				From: base.Add(-time.Hour), Through: base.Add(time.Hour), Direction: plan.Forward,
			})
		})
	}
}

// encodeKeepInMap writes f like plantest.Encode, except every label named in
// keep stays in the labels map with a NULL promoted column: a shape only a
// hand writer produces, used to pin the promoted-value/map-value fallback.
func encodeKeepInMap(f plantest.File, keep ...string) ([]byte, storage.FileStats, error) {
	sc, err := schema.ForStream(f.Promoted)
	if err != nil {
		return nil, storage.FileStats{}, err
	}
	rb := array.NewRecordBuilder(memory.NewGoAllocator(), sc)
	defer rb.Release()
	fields := rb.Fields()
	appendMap := func(b *array.MapBuilder, ls logs.Labels) {
		b.Append(true)
		kb := b.KeyBuilder().(*array.StringBuilder)
		ib := b.ItemBuilder().(*array.StringBuilder)
		for _, l := range ls {
			kb.Append(l.Name)
			ib.Append(l.Value)
		}
	}
	for _, r := range f.Rows {
		kept, rest := r.Labels.Split(f.Promoted)
		var nulled []string
		for _, name := range f.Promoted {
			if v, ok := kept.Get(name); ok && slices.Contains(keep, name) {
				rest = logs.New(append(rest, logs.Label{Name: name, Value: v})...)
				nulled = append(nulled, name)
			}
		}
		fields[0].(*array.TimestampBuilder).Append(arrow.Timestamp(schema.TS(r.TS)))
		fields[1].(*array.StringBuilder).Append(r.Line)
		appendMap(fields[2].(*array.MapBuilder), rest)
		appendMap(fields[3].(*array.MapBuilder), r.Meta)
		for i, name := range f.Promoted {
			pb := fields[4+i].(*array.StringBuilder)
			if v, ok := kept.Get(name); ok && !slices.Contains(nulled, name) {
				pb.Append(v)
			} else {
				pb.AppendNull()
			}
		}
	}
	rec := rb.NewRecordBatch()
	defer rec.Release()
	var buf bytes.Buffer
	w, err := storage.NewWriter(&buf, sc, f.Promoted)
	if err != nil {
		return nil, storage.FileStats{}, err
	}
	if err := w.Write(rec); err != nil {
		return nil, storage.FileStats{}, err
	}
	st, err := w.Close()
	if err != nil {
		return nil, storage.FileStats{}, err
	}
	return buf.Bytes(), st, nil
}

// TestSelectNullPromotedFallsBackToMap pins the fallback inside the
// canonical label assembly: a promoted column that is NULL for a row falls
// back to the labels-map entry of the same name — the same rule the matcher
// predicates use — so the complete label set never silently loses the label.
// The fixture is a hand-written non-compliant file (the ingester's Split
// never stores a promoted label in the map), so both the matcher and the
// canonical map must agree with MemSource on it.
func TestSelectNullPromotedFallsBackToMap(t *testing.T) {
	rw := func(ts time.Time, line string, ls map[string]string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	base := plantest.T0
	rows := []plan.Row{
		rw(base, "fallback", map[string]string{"service_name": "s", "level": "warn"}),
		rw(base.Add(time.Second), "column", map[string]string{"service_name": "s", "level": "info"}),
	}
	f := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{"level"}, Rows: rows}
	data, st, err := encodeKeepInMap(f, "level")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	key := fmt.Sprintf("%s/logs/s/%s.parquet", tenant.Default, uuid.NewV7())
	p := filepath.Join(dir, fmt.Sprintf("%s.parquet", uuid.NewV7()))
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cat := catalog.NewMemory()
	// The hand-written file keeps level in the map while its column is
	// all-null; the writer's stats would report an empty value set, which
	// catalog Match pruning reads as "level never matches". For THIS
	// fixture the map carries the values, so list them for the catalog —
	// the regression targets the SQL fallback, not pruning.
	lv := st.LabelValues
	if lv == nil {
		lv = map[string][]string{}
	}
	lv["level"] = []string{"info", "warn"}
	if err := cat.AddFiles(t.Context(), catalog.File{
		Tenant: tenant.Default, Stream: "s", Key: key,
		MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(len(data)),
		LabelValues: lv, CreatedAt: time.Now().Truncate(time.Microsecond),
	}); err != nil {
		t.Fatal(err)
	}
	src := newSource(openTestDB(t), cat, mapResolver{key: p}, defaultStreamLabels)
	ref := plantest.Mem([]plantest.File{f})
	eq := func(n, v string) plan.Matcher { return plan.MustMatcher(plan.MatchEqual, n, v) }
	for _, tc := range []struct {
		name     string
		matchers []plan.Matcher
	}{
		{"all-rows-full-labels", []plan.Matcher{eq("service_name", "s")}},
		{"fallback-value-matches", []plan.Matcher{eq("level", "warn")}},
		{"column-value-matches", []plan.Matcher{eq("level", "info")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertRowsAgainstRef(t, src, ref, plan.Selection{
				Tenant: tenant.Default, Matchers: tc.matchers,
				From: base.Add(-time.Hour), Through: base.Add(time.Hour), Direction: plan.Forward,
			})
		})
	}
}

// TestSelectSQLEmptyPredicates pins the shared relation's shape for callers
// with no matchers and no line filters (the private metadata reader): the
// predicate list compiles to WHERE TRUE plus the time range, and the
// statement executes. plan.Source keeps requiring at least one matcher;
// this exercises the SQL helper directly, not Select validation.
func TestSelectSQLEmptyPredicates(t *testing.T) {
	base := plantest.T0
	f := plantest.File{Tenant: tenant.Default, Stream: "s", Promoted: []string{"level"}, Rows: []plan.Row{
		{TS: base, Line: "one", Labels: logs.FromMap(map[string]string{"service_name": "s", "level": "info"})},
		{TS: base.Add(time.Second), Line: "two", Labels: logs.FromMap(map[string]string{"service_name": "s"})},
	}}
	p := writeParquet(t, t.TempDir(), "nopred.parquet", f)
	q := selectSQL(plan.Selection{
		Tenant: tenant.Default,
		From:   base.Add(-time.Hour), Through: base.Add(time.Hour), Direction: plan.Forward,
	}, []fileGroup{{promoted: []string{"level"}, paths: []string{p}}})
	if !strings.Contains(q.sql, " WHERE TRUE ") {
		t.Errorf("empty predicate list did not compile to WHERE TRUE: %s", q.sql)
	}
	t.Logf("compiled: %s", q.sql)
	db := openTestDB(t)
	rows, err := db.QueryContext(t.Context(), q.sql, q.args...)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var ts time.Time
		var line, labelsJSON, metaJSON sql.NullString
		if err := rows.Scan(&ts, &line, &labelsJSON, &metaJSON); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line.String)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(lines, []string{"one", "two"}) {
		t.Errorf("rows = %v, want both fixture lines", lines)
	}
}

// TestSelectByteSubstringSemantics pins the frozen Source contract that |=
// and |! are byte substring tests (strings.Contains), not codepoint tests:
// needles cut out of multi-byte runes (a lone leading or continuation byte,
// invalid UTF-8) match the line containing the full rune via the hex
// fallback, an empty needle matches every line, and NUL and control bytes
// compare as ordinary bytes. The guard case is invalid needle 0xC1 against
// line "l\x13": lower(hex) is "6c13", which contains "c1" only at an odd
// offset, so the byte-aligned ^(?:..)* pattern correctly finds nothing
// while an unaligned substring would — the direct proof at the bottom shows
// the unaligned pattern matching, so dropping the alignment would fail
// these cases. Every case compares full rows against MemSource for Contains
// and NotContains.
func TestSelectByteSubstringSemantics(t *testing.T) {
	café := "café ☕" // é = 0xC3 0xA9, ☕ = 0xE2 0x98 0x95
	base := plantest.T0
	rows := []plan.Row{
		{TS: base, Line: café, Labels: logs.FromMap(map[string]string{"service_name": "s"})},
		{TS: base.Add(time.Second), Line: "a\x00b", Labels: logs.FromMap(map[string]string{"service_name": "s"})},
		{TS: base.Add(2 * time.Second), Line: "aé", Labels: logs.FromMap(map[string]string{"service_name": "s"})},
		{TS: base.Add(3 * time.Second), Line: "plain", Labels: logs.FromMap(map[string]string{"service_name": "s"})},
		{TS: base.Add(4 * time.Second), Line: "l\x13", Labels: logs.FromMap(map[string]string{"service_name": "s"})}, // hex 6c13
	}
	f := plantest.File{Tenant: tenant.Default, Stream: "s", Rows: rows}
	dir := t.TempDir()
	p := writeParquet(t, dir, "bytes.parquet", f)
	db := openTestDB(t)
	cat := catalog.NewMemory()
	resolve := mapResolver{}
	addFile := func(ff plantest.File, path string) {
		data, st, err := plantest.Encode(ff)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("%s/logs/s/%s.parquet", tenant.Default, uuid.NewV7())
		resolve[key] = path
		if err := cat.AddFiles(t.Context(), catalog.File{
			Tenant: ff.Tenant, Stream: ff.Stream, Key: key,
			MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(len(data)),
			LabelValues: st.LabelValues, CreatedAt: time.Now().Truncate(time.Microsecond),
		}); err != nil {
			t.Fatal(err)
		}
	}
	addFile(f, p)
	src := newSource(db, cat, resolve, defaultStreamLabels)
	ref := plantest.Mem([]plantest.File{f})
	svc := []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "service_name", "s")}
	contains := func(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineContains, v) }
	notContains := func(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineNotContains, v) }

	for _, tc := range []struct {
		name   string
		filter plan.LineFilter
	}{
		{"full-rune", contains("é")},
		{"leading-byte", contains("é"[:1])},       // 0xC3, invalid UTF-8
		{"continuation-byte", contains("é"[1:2])}, // 0xA9
		{"emoji-leading-byte", contains("☕"[:1])}, // 0xE2
		{"emoji-continuation-byte", contains("☕"[1:2])},
		{"hex-odd-nibble-no-false-positive", contains("\xc1")}, // "6c13" has "c1" at odd offset only
		{"control-byte-needle", contains("\x13")},              // valid UTF-8, fast path
		{"empty-needle", contains("")},
		{"nul-needle", contains("\x00")},
		{"not-full-rune", notContains("é")},
		{"not-leading-byte", notContains("é"[:1])},
		{"not-hex-odd-nibble", notContains("\xc1")},
		{"not-nul-needle", notContains("\x00")},
		{"not-empty-needle", notContains("")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertRowsAgainstRef(t, src, ref, plan.Selection{
				Tenant: tenant.Default, Matchers: svc, LineFilters: []plan.LineFilter{tc.filter},
				From: base.Add(-time.Hour), Through: base.Add(time.Hour), Direction: plan.Forward,
			})
		})
	}

	// The alignment proof: on this fixture the unaligned pattern DOES match
	// (the odd-offset "c1"), so the byte-aligned ^(?:..)* prefix is what
	// keeps the Source from returning those rows as false positives.
	count := func(pattern string) int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(t.Context(),
			`SELECT count(*) FROM read_parquet(?) WHERE regexp_matches(lower(hex("line")), ?)`, p, pattern,
		).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := count("c1"); n == 0 {
		t.Fatal("unaligned pattern found no odd-offset hex; the guard case is vacuous")
	}
	if n := count("^(?:..)*c1"); n != 0 {
		t.Errorf("aligned pattern matched %d lines, want 0", n)
	}
}

// caseTestEnv builds a Source over given files with the in-memory catalog
// and a map resolver, plus the MemSource reference. Catalog rows carry the
// writer's stats, LabelValues included: the Source reads each file's
// promoted-label names from them.
func caseTestEnv(t *testing.T, files ...plantest.File) (plan.Source, *plan.MemSource) {
	t.Helper()
	dir := t.TempDir()
	cat := catalog.NewMemory()
	resolve := mapResolver{}
	for _, f := range files {
		data, st, err := plantest.Encode(f)
		if err != nil {
			t.Fatal(err)
		}
		key := fmt.Sprintf("%s/logs/s/%s.parquet", f.Tenant, uuid.NewV7())
		p := filepath.Join(dir, fmt.Sprintf("%s.parquet", uuid.NewV7()))
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		resolve[key] = p
		if err := cat.AddFiles(t.Context(), catalog.File{
			Tenant: f.Tenant, Stream: f.Stream, Key: key,
			MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(len(data)),
			LabelValues: st.LabelValues, CreatedAt: time.Now().Truncate(time.Microsecond),
		}); err != nil {
			t.Fatal(err)
		}
	}
	return newSource(openTestDB(t), cat, resolve, defaultStreamLabels), plantest.Mem(files)
}

// assertRowsAgainstRef compares a full Select against the reference,
// requiring exact row multisets (labels included).
func assertRowsAgainstRef(t *testing.T, src plan.Source, ref *plan.MemSource, sel plan.Selection) {
	t.Helper()
	got, err := collectRows(src.Select(t.Context(), sel))
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	want, err := collectRows(ref.Select(t.Context(), sel))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	keys := func(rs []plan.Row) []string {
		out := make([]string, len(rs))
		for i, r := range rs {
			out[i] = rowKey(r)
		}
		return out
	}
	g, w := keys(got), keys(want)
	slices.Sort(g)
	slices.Sort(w)
	for i := range g {
		if g[i] != w[i] {
			t.Fatalf("row %d differs:\n got  %s\n want %s", i, g[i], w[i])
		}
	}
}
