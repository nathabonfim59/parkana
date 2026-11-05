package storage_test

import (
	"bytes"
	"maps"
	"slices"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// dedupKV is one map entry with its stored order; null marks a null item
// value, which only foreign writers produce.
type dedupKV struct {
	key  string
	val  string
	null bool
}

// dedupRow is one physical input row: ts in Unix microseconds, a nil line
// for a null line, nil labels or meta for a null map and an empty non-nil
// slice for an empty one, and the row's promoted-column values in cols
// (absent meaning null).
type dedupRow struct {
	ts     int64
	line   *string
	labels []dedupKV
	meta   []dedupKV
	cols   map[string]string
}

// dedupFile is one input file: the extra columns its schema carries (any
// subset of the merge's promoted list, plus pipeline columns outside it) and
// its rows in ascending _ts order.
type dedupFile struct {
	cols []string
	rows []dedupRow
}

// dedupCase is one matrix case: the merge's promoted list, the input files,
// the expected surviving and removed row counts, and whether the rows have a
// plan.Row form for the plan.MemSource cross-check (null map items and null
// lines do not).
type dedupCase struct {
	name     string
	promoted []string
	files    []dedupFile
	wantRows int64
	wantDups int64
	memOK    bool
}

// dedupS boxes s so dedupRow.line can hold an explicit "" next to nil.
func dedupS(s string) *string { return &s }

// dedupEntries converts m to entries sorted by key.
func dedupEntries(m map[string]string) []dedupKV {
	out := make([]dedupKV, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		out = append(out, dedupKV{key: k, val: m[k]})
	}
	return out
}

// dedupRowOf builds a row from plain values. A nil labels or meta map is a
// null map; an empty non-nil one is an empty map.
func dedupRowOf(ts int64, line string, labels, meta, cols map[string]string) dedupRow {
	r := dedupRow{ts: ts, line: &line, cols: cols}
	if labels != nil {
		r.labels = dedupEntries(labels)
	}
	if meta != nil {
		r.meta = dedupEntries(meta)
	}
	return r
}

// dedupMR is the matrix's standard row: one label, an empty metadata map.
func dedupMR(ts int64, line string) dedupRow {
	return dedupRowOf(ts, line, map[string]string{"service_name": "api"}, map[string]string{}, nil)
}

// dedupInput encodes f as one Parquet input file. Map entries keep the given
// order and null items survive, so foreign layouts are expressible; the
// plain Parquet writer is used (not storage.Writer) so nulls stay
// controllable.
func dedupInput(t *testing.T, f dedupFile) *storage.Reader {
	t.Helper()
	fields := schema.Base()
	for _, c := range f.cols {
		fields = append(fields, mergeField(c, arrow.BinaryTypes.String))
	}
	// Foreign files may carry null lines, labels and meta maps, so the base
	// fields are relaxed to nullable for the fixtures that need them.
	for i := range 3 {
		fields[i+1].Nullable = true
	}
	s := arrow.NewSchema(fields, nil)

	var data []byte
	if len(f.rows) == 0 {
		data = mergeParquet(t, s)
	} else {
		rb := array.NewRecordBuilder(memory.DefaultAllocator, s)
		defer rb.Release()
		fb := rb.Fields()
		appendMap := func(b *array.MapBuilder, entries []dedupKV) {
			if entries == nil {
				b.AppendNull()
				return
			}
			b.Append(true)
			kb := b.KeyBuilder().(*array.StringBuilder)
			ib := b.ItemBuilder().(*array.StringBuilder)
			for _, e := range entries {
				kb.Append(e.key)
				if e.null {
					ib.AppendNull()
				} else {
					ib.Append(e.val)
				}
			}
		}
		for _, r := range f.rows {
			fb[0].(*array.TimestampBuilder).Append(arrow.Timestamp(r.ts))
			if r.line == nil {
				fb[1].(*array.StringBuilder).AppendNull()
			} else {
				fb[1].(*array.StringBuilder).Append(*r.line)
			}
			appendMap(fb[2].(*array.MapBuilder), r.labels)
			appendMap(fb[3].(*array.MapBuilder), r.meta)
			for i, c := range f.cols {
				pb := fb[4+i].(*array.StringBuilder)
				if v, ok := r.cols[c]; ok {
					pb.Append(v)
				} else {
					pb.AppendNull()
				}
			}
		}
		rec := rb.NewRecordBatch()
		defer rec.Release()
		data = mergeParquet(t, s, rec)
	}
	r, err := storage.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("dedupInput NewReader: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// dedupMemSelect returns every row mem holds, in forward order.
func dedupMemSelect(t *testing.T, mem *plan.MemSource) []plan.Row {
	t.Helper()
	sel := plan.Selection{
		Tenant:    tenant.Default,
		Matchers:  []plan.Matcher{plan.MustMatcher(plan.MatchRegexp, "any", ".*")},
		From:      schema.Time(-1 << 52),
		Through:   schema.Time(1 << 52),
		Direction: plan.Forward,
	}
	var out []plan.Row
	for r, err := range mem.Select(t.Context(), sel) {
		if err != nil {
			t.Fatalf("MemSource Select: %v", err)
		}
		out = append(out, r)
	}
	return out
}

// dedupMemPlanRows converts a case's physical rows to the effective
// plan.Rows read-time dedup sees: a promoted column value wins over a
// conflicting labels-map entry, a null column falls back to the map, and a
// label absent from both is omitted (the same rule internal/query compiles,
// derived here from the case description alone, independently of the merge
// identity code under test).
func dedupMemPlanRows(t *testing.T, tc dedupCase) []plan.Row {
	t.Helper()
	var rows []plan.Row
	for _, f := range tc.files {
		for _, r := range f.rows {
			if r.line == nil {
				t.Fatalf("%s: null line has no plan.Row form", tc.name)
			}
			var pairs []logs.Label
			for _, e := range r.labels {
				if e.null {
					t.Fatalf("%s: null label item has no plan.Row form", tc.name)
				}
				if slices.Contains(tc.promoted, e.key) && slices.Contains(f.cols, e.key) {
					if _, ok := r.cols[e.key]; ok {
						continue // shadowed by the row's non-null column
					}
				}
				pairs = append(pairs, logs.Label{Name: e.key, Value: e.val})
			}
			for _, p := range tc.promoted {
				if v, ok := r.cols[p]; ok && slices.Contains(f.cols, p) {
					pairs = append(pairs, logs.Label{Name: p, Value: v})
				}
			}
			var meta []logs.Label
			for _, e := range r.meta {
				if e.null {
					t.Fatalf("%s: null meta item has no plan.Row form", tc.name)
				}
				meta = append(meta, logs.Label{Name: e.key, Value: e.val})
			}
			rows = append(rows, plan.Row{
				TS:     schema.Time(r.ts),
				Line:   *r.line,
				Labels: logs.New(pairs...),
				Meta:   logs.New(meta...),
			})
		}
	}
	return rows
}

// TestMergeDedupMatrix runs the identity matrix: every pair of rows that
// must collapse, and every pair that must survive, including zero and
// negative timestamps. Each case is merged for real and cross-checked against
// plan.MemSource loaded with the same rows as effective plan.Rows (approved
// amendment 3): whenever Merge collapses a pair, read-time dedup must
// collapse it too, so an over-eager identity fails beside an under-eager one.
func TestMergeDedupMatrix(t *testing.T) {
	for _, tc := range dedupMatrix() {
		t.Run(tc.name, func(t *testing.T) {
			var readers []*storage.Reader
			var physical int64
			for _, f := range tc.files {
				readers = append(readers, dedupInput(t, f))
				physical += int64(len(f.rows))
			}
			out := &bytes.Buffer{}
			stats, err := storage.Merge(t.Context(), out, readers, tc.promoted)
			if err != nil {
				t.Fatalf("Merge: %v", err)
			}
			if stats.Rows != tc.wantRows || stats.DeduplicatedRows != tc.wantDups {
				t.Fatalf("stats.Rows = %d, DeduplicatedRows = %d, want %d/%d",
					stats.Rows, stats.DeduplicatedRows, tc.wantRows, tc.wantDups)
			}
			if got := stats.Rows + stats.DeduplicatedRows; got != physical {
				t.Fatalf("rows %d + dups %d = %d, want the %d physical input rows",
					stats.Rows, stats.DeduplicatedRows, got, physical)
			}
			if !tc.memOK {
				return
			}
			mem := plan.NewMemSource()
			mem.Add(tenant.Default, dedupMemPlanRows(t, tc)...)
			want := dedupMemSelect(t, mem)
			if len(want) != int(tc.wantRows) {
				t.Fatalf("MemSource kept %d rows, want %d: Merge and read-time dedup disagree", len(want), tc.wantRows)
			}
			// The merged output's effective rows must be exactly the rows
			// read-time dedup keeps, in sequence, not just in count.
			got := dedupMergedPlanRows(t, out.Bytes(), tc.promoted)
			if len(got) != int(tc.wantRows) {
				t.Fatalf("merged output holds %d effective rows, want %d", len(got), tc.wantRows)
			}
			for i := range want {
				w, g := want[i], got[i]
				if !g.TS.Equal(w.TS) || g.Line != w.Line || !slices.Equal(g.Labels, w.Labels) || !slices.Equal(g.Meta, w.Meta) {
					t.Fatalf("row %d: merged {%v %q %v %v} != MemSource {%v %q %v %v}",
						i, g.TS, g.Line, g.Labels, g.Meta, w.TS, w.Line, w.Labels, w.Meta)
				}
			}
		})
	}
}

func dedupMatrix() []dedupCase {
	return []dedupCase{
		// Pairs that must collapse.
		{name: "adjacent rows in one input",
			files:    []dedupFile{{rows: []dedupRow{dedupMR(10, "x"), dedupMR(10, "x")}}},
			wantRows: 1, wantDups: 1, memOK: true},
		{name: "nonadjacent rows in one input",
			files:    []dedupFile{{rows: []dedupRow{dedupMR(10, "x"), dedupMR(10, "y"), dedupMR(10, "x")}}},
			wantRows: 2, wantDups: 1, memOK: true},
		{name: "across two inputs",
			files: []dedupFile{
				{rows: []dedupRow{dedupMR(10, "x")}},
				{rows: []dedupRow{dedupMR(10, "x")}},
			}, wantRows: 1, wantDups: 1, memOK: true},
		{name: "across three inputs with middle distinct",
			files: []dedupFile{
				{rows: []dedupRow{dedupMR(10, "x")}},
				{rows: []dedupRow{dedupMR(10, "y")}},
				{rows: []dedupRow{dedupMR(10, "x")}},
			}, wantRows: 2, wantDups: 1, memOK: true},
		{name: "reordered labels map entries",
			files: []dedupFile{
				{rows: []dedupRow{{ts: 10, line: dedupS("x"),
					labels: []dedupKV{{key: "b", val: "2"}, {key: "a", val: "1"}},
					meta:   []dedupKV{}}}},
				{rows: []dedupRow{{ts: 10, line: dedupS("x"),
					labels: []dedupKV{{key: "a", val: "1"}, {key: "b", val: "2"}},
					meta:   []dedupKV{}}}},
			}, wantRows: 1, wantDups: 1, memOK: true},
		{name: "reordered meta map entries",
			files: []dedupFile{
				{rows: []dedupRow{{ts: 10, line: dedupS("x"),
					labels: []dedupKV{{key: "sn", val: "api"}},
					meta:   []dedupKV{{key: "b", val: "2"}, {key: "a", val: "1"}}}}},
				{rows: []dedupRow{{ts: 10, line: dedupS("x"),
					labels: []dedupKV{{key: "sn", val: "api"}},
					meta:   []dedupKV{{key: "a", val: "1"}, {key: "b", val: "2"}}}}},
			}, wantRows: 1, wantDups: 1, memOK: true},
		{name: "promoted column equals map-only label",
			promoted: []string{"level"},
			files: []dedupFile{
				{cols: []string{"level"}, rows: []dedupRow{dedupRowOf(10, "x",
					map[string]string{"service_name": "api"}, map[string]string{},
					map[string]string{"level": "info"})}},
				{rows: []dedupRow{dedupRowOf(10, "x",
					map[string]string{"service_name": "api", "level": "info"}, map[string]string{}, nil)}},
			}, wantRows: 1, wantDups: 1, memOK: true},
		{name: "promoted column shadows conflicting map entries",
			promoted: []string{"level"},
			files: []dedupFile{
				{cols: []string{"level"}, rows: []dedupRow{dedupRowOf(10, "x",
					map[string]string{"service_name": "api", "level": "map-y"}, map[string]string{},
					map[string]string{"level": "col"})}},
				{cols: []string{"level"}, rows: []dedupRow{dedupRowOf(10, "x",
					map[string]string{"service_name": "api", "level": "map-z"}, map[string]string{},
					map[string]string{"level": "col"})}},
			}, wantRows: 1, wantDups: 1, memOK: true},
		{name: "null column falls back to the map",
			promoted: []string{"level"},
			files: []dedupFile{
				{cols: []string{"level"}, rows: []dedupRow{dedupRowOf(10, "x",
					map[string]string{"service_name": "api", "level": "m"}, map[string]string{}, nil)}},
				{rows: []dedupRow{dedupRowOf(10, "x",
					map[string]string{"service_name": "api", "level": "m"}, map[string]string{}, nil)}},
			}, wantRows: 1, wantDups: 1, memOK: true},
		{name: "null labels and meta maps equal empty ones",
			files: []dedupFile{
				{rows: []dedupRow{{ts: 10, line: dedupS("x"), labels: nil, meta: nil}}},
				{rows: []dedupRow{{ts: 10, line: dedupS("x"), labels: []dedupKV{}, meta: []dedupKV{}}}},
			}, wantRows: 1, wantDups: 1, memOK: true},
		{name: "pipeline columns are payload, not identity",
			files: []dedupFile{
				{cols: []string{"pipe"}, rows: []dedupRow{dedupRowOf(10, "x",
					map[string]string{"service_name": "api"}, map[string]string{},
					map[string]string{"pipe": "first"})}},
				{cols: []string{"pipe"}, rows: []dedupRow{dedupRowOf(10, "x",
					map[string]string{"service_name": "api"}, map[string]string{},
					map[string]string{"pipe": "second"})}},
			}, wantRows: 1, wantDups: 1, memOK: true},
		{name: "invalid utf8 line bytes compare raw",
			files:    []dedupFile{{rows: []dedupRow{dedupMR(10, "\xff\xfe"), dedupMR(10, "\xff\xfe")}}},
			wantRows: 1, wantDups: 1, memOK: true},
		{name: "present-empty label values collapse",
			files: []dedupFile{{rows: []dedupRow{
				dedupRowOf(10, "x", map[string]string{"k": ""}, map[string]string{}, nil),
				dedupRowOf(10, "x", map[string]string{"k": ""}, map[string]string{}, nil),
			}}}, wantRows: 1, wantDups: 1, memOK: true},
		{name: "empty line and empty maps collapse at timestamp zero",
			files: []dedupFile{{rows: []dedupRow{
				dedupRowOf(0, "", map[string]string{}, map[string]string{}, nil),
				dedupRowOf(0, "", map[string]string{}, map[string]string{}, nil),
			}}}, wantRows: 1, wantDups: 1, memOK: true},
		{name: "zero and negative timestamps collapse within their runs",
			files: []dedupFile{{rows: []dedupRow{
				dedupRowOf(-5000, "neg", map[string]string{}, map[string]string{}, nil),
				dedupRowOf(-5000, "neg", map[string]string{}, map[string]string{}, nil),
				dedupRowOf(0, "zero", map[string]string{}, map[string]string{}, nil),
				dedupRowOf(0, "zero", map[string]string{}, map[string]string{}, nil),
			}}}, wantRows: 2, wantDups: 2, memOK: true},
		// Duplicate map keys are pathological (Arrow maps should have unique
		// keys; the ingester never writes them) and have no plan.Row form:
		// logs.Labels keeps one value per name, so the MemSource
		// cross-check does not apply; the canonical sort still collapses
		// reordered copies.
		{name: "reordered duplicate map keys collapse",
			files: []dedupFile{
				{rows: []dedupRow{{ts: 10, line: dedupS("x"),
					labels: []dedupKV{{key: "k", val: "b"}, {key: "k", val: "a"}},
					meta:   []dedupKV{}}}},
				{rows: []dedupRow{{ts: 10, line: dedupS("x"),
					labels: []dedupKV{{key: "k", val: "a"}, {key: "k", val: "b"}},
					meta:   []dedupKV{}}}},
			}, wantRows: 1, wantDups: 1, memOK: false},
		{name: "reordered duplicate keys under a promoted name collapse",
			promoted: []string{"level"},
			files: []dedupFile{
				{cols: []string{"level"}, rows: []dedupRow{{ts: 10, line: dedupS("x"),
					labels: []dedupKV{{key: "level", val: "b"}, {key: "level", val: "a"}, {key: "sn", val: "api"}},
					meta:   []dedupKV{}}}},
				{rows: []dedupRow{{ts: 10, line: dedupS("x"),
					labels: []dedupKV{{key: "sn", val: "api"}, {key: "level", val: "a"}, {key: "level", val: "b"}},
					meta:   []dedupKV{}}}},
			}, wantRows: 1, wantDups: 1, memOK: false},
		{name: "null and value under one name sort canonically",
			files: []dedupFile{
				{rows: []dedupRow{{ts: 10, line: dedupS("x"),
					labels: []dedupKV{}, meta: []dedupKV{{key: "k", null: true}, {key: "k", val: "a"}}}}},
				{rows: []dedupRow{{ts: 10, line: dedupS("x"),
					labels: []dedupKV{}, meta: []dedupKV{{key: "k", val: "a"}, {key: "k", null: true}}}}},
			}, wantRows: 1, wantDups: 1, memOK: false},
		{name: "interleaved duplicate keys under two promoted names stay distinct",
			// The a entries interleave with b's, so a grouping that assumed
			// one name's entries are contiguous would drop the differing
			// second a entry and falsely collapse the rows.
			promoted: []string{"a", "b"},
			files: []dedupFile{{cols: []string{"a", "b"}, rows: []dedupRow{
				{ts: 10, line: dedupS("x"), labels: []dedupKV{
					{key: "a", val: "first"}, {key: "b", val: "B"}, {key: "a", val: "second"},
				}, meta: []dedupKV{}},
				{ts: 10, line: dedupS("x"), labels: []dedupKV{
					{key: "a", val: "first"}, {key: "b", val: "B"}, {key: "a", val: "different"},
				}, meta: []dedupKV{}},
			}}}, wantRows: 2, wantDups: 0, memOK: false},
		{name: "reordered interleaved duplicate keys collapse",
			promoted: []string{"a", "b"},
			files: []dedupFile{{cols: []string{"a", "b"}, rows: []dedupRow{
				{ts: 10, line: dedupS("x"), labels: []dedupKV{
					{key: "a", val: "first"}, {key: "b", val: "B"}, {key: "a", val: "second"},
				}, meta: []dedupKV{}},
				{ts: 10, line: dedupS("x"), labels: []dedupKV{
					{key: "b", val: "B"}, {key: "a", val: "second"}, {key: "a", val: "first"},
				}, meta: []dedupKV{}},
			}}}, wantRows: 1, wantDups: 1, memOK: false},
		{name: "shadowing column skips interleaved entries without losing the next name",
			// a's non-null column shadows all a entries; b's fallback must
			// still see its own entry after skipping a's interleaved run.
			promoted: []string{"a", "b"},
			files: []dedupFile{
				{cols: []string{"a", "b"}, rows: []dedupRow{
					{ts: 10, line: dedupS("x"), labels: []dedupKV{
						{key: "a", val: "first"}, {key: "b", val: "B"}, {key: "a", val: "second"},
					}, meta: []dedupKV{}, cols: map[string]string{"a": "col"}},
				}},
				{cols: []string{"a", "b"}, rows: []dedupRow{
					{ts: 10, line: dedupS("x"), labels: []dedupKV{{key: "b", val: "B"}},
						meta: []dedupKV{}, cols: map[string]string{"a": "col"}},
				}},
			}, wantRows: 1, wantDups: 1, memOK: false},
		{name: "promoted names absent from a following row stay distinct",
			promoted: []string{"a", "b"},
			files: []dedupFile{{cols: []string{"a", "b"}, rows: []dedupRow{
				dedupRowOf(10, "x", map[string]string{"a": "A", "b": "B"}, map[string]string{}, nil),
				dedupRowOf(10, "x", map[string]string{}, map[string]string{}, nil),
			}}}, wantRows: 2, wantDups: 0, memOK: true},
		{name: "one of two promoted names present-empty stays distinct from absent",
			promoted: []string{"a", "b"},
			files: []dedupFile{
				{cols: []string{"a", "b"}, rows: []dedupRow{dedupRowOf(10, "x",
					map[string]string{"a": "A"}, map[string]string{}, nil)}},
				{cols: []string{"a", "b"}, rows: []dedupRow{dedupRowOf(10, "x",
					map[string]string{"a": "A", "b": ""}, map[string]string{}, nil)}},
			}, wantRows: 2, wantDups: 0, memOK: true},

		// Pairs that must survive.
		{name: "different line",
			files:    []dedupFile{{rows: []dedupRow{dedupMR(10, "x"), dedupMR(10, "y")}}},
			wantRows: 2, wantDups: 0, memOK: true},
		{name: "invalid bytes are not normalized",
			files:    []dedupFile{{rows: []dedupRow{dedupMR(10, "\xff"), dedupMR(10, "\xef\xbf\xbd")}}},
			wantRows: 2, wantDups: 0, memOK: true},
		{name: "different label value",
			files: []dedupFile{{rows: []dedupRow{
				dedupRowOf(10, "x", map[string]string{"service_name": "api", "k": "a"}, map[string]string{}, nil),
				dedupRowOf(10, "x", map[string]string{"service_name": "api", "k": "b"}, map[string]string{}, nil),
			}}}, wantRows: 2, wantDups: 0, memOK: true},
		{name: "absent label versus present-empty label",
			files: []dedupFile{{rows: []dedupRow{
				dedupRowOf(10, "x", map[string]string{}, map[string]string{}, nil),
				dedupRowOf(10, "x", map[string]string{"k": ""}, map[string]string{}, nil),
			}}}, wantRows: 2, wantDups: 0, memOK: true},
		{name: "promoted column present-empty versus absent",
			promoted: []string{"k"},
			files: []dedupFile{
				{cols: []string{"k"}, rows: []dedupRow{dedupRowOf(10, "x",
					map[string]string{}, map[string]string{}, map[string]string{"k": ""})}},
				{rows: []dedupRow{dedupRowOf(10, "x", map[string]string{}, map[string]string{}, nil)}},
			}, wantRows: 2, wantDups: 0, memOK: true},
		{name: "label names differ only in case",
			files: []dedupFile{
				{rows: []dedupRow{dedupRowOf(10, "x", map[string]string{"Level": "x"}, map[string]string{}, nil)}},
				{rows: []dedupRow{dedupRowOf(10, "x", map[string]string{"level": "x"}, map[string]string{}, nil)}},
			}, wantRows: 2, wantDups: 0, memOK: true},
		{name: "different meta value",
			files: []dedupFile{{rows: []dedupRow{
				dedupRowOf(10, "x", map[string]string{"service_name": "api"}, map[string]string{"t": "a"}, nil),
				dedupRowOf(10, "x", map[string]string{"service_name": "api"}, map[string]string{"t": "b"}, nil),
			}}}, wantRows: 2, wantDups: 0, memOK: true},
		{name: "meta entry versus null meta",
			files: []dedupFile{
				{rows: []dedupRow{{ts: 10, line: dedupS("x"),
					labels: []dedupKV{{key: "sn", val: "api"}}, meta: []dedupKV{{key: "t", val: "a"}}}}},
				{rows: []dedupRow{{ts: 10, line: dedupS("x"),
					labels: []dedupKV{{key: "sn", val: "api"}}, meta: nil}}},
			}, wantRows: 2, wantDups: 0, memOK: true},
		{name: "meta present-empty versus absent entry",
			files: []dedupFile{{rows: []dedupRow{
				dedupRowOf(10, "x", map[string]string{"service_name": "api"}, map[string]string{}, nil),
				dedupRowOf(10, "x", map[string]string{"service_name": "api"}, map[string]string{"k": ""}, nil),
			}}}, wantRows: 2, wantDups: 0, memOK: true},
		{name: "different timestamp",
			files:    []dedupFile{{rows: []dedupRow{dedupMR(10, "x"), dedupMR(20, "x")}}},
			wantRows: 2, wantDups: 0, memOK: true},
		{name: "timestamp change resets state across zero",
			files: []dedupFile{{rows: []dedupRow{
				dedupMR(-5, "x"), dedupMR(-5, "y"), dedupMR(0, "x"), dedupMR(0, "y"),
			}}}, wantRows: 4, wantDups: 0, memOK: true},
		{name: "different promoted column values",
			promoted: []string{"level"},
			files: []dedupFile{
				{cols: []string{"level"}, rows: []dedupRow{dedupRowOf(10, "x",
					map[string]string{"service_name": "api"}, map[string]string{}, map[string]string{"level": "a"})}},
				{cols: []string{"level"}, rows: []dedupRow{dedupRowOf(10, "x",
					map[string]string{"service_name": "api"}, map[string]string{}, map[string]string{"level": "b"})}},
			}, wantRows: 2, wantDups: 0, memOK: true},

		// Defensive kinds with no plan.Row form: null items and null lines
		// stay distinct from empty strings (read-time dedup keeps them apart,
		// so Merge must too).
		{name: "null meta item versus empty-string item",
			files: []dedupFile{{rows: []dedupRow{
				{ts: 10, line: dedupS("x"), labels: []dedupKV{}, meta: []dedupKV{{key: "k", null: true}}},
				{ts: 10, line: dedupS("x"), labels: []dedupKV{}, meta: []dedupKV{{key: "k", val: ""}}},
			}}}, wantRows: 2, wantDups: 0, memOK: false},
		{name: "equal null meta items collapse",
			files: []dedupFile{{rows: []dedupRow{
				{ts: 10, line: dedupS("x"), labels: []dedupKV{}, meta: []dedupKV{{key: "k", null: true}}},
				{ts: 10, line: dedupS("x"), labels: []dedupKV{}, meta: []dedupKV{{key: "k", null: true}}},
			}}}, wantRows: 1, wantDups: 1, memOK: false},
		{name: "null label item versus empty-string item",
			files: []dedupFile{{rows: []dedupRow{
				{ts: 10, line: dedupS("x"), labels: []dedupKV{{key: "k", null: true}}, meta: []dedupKV{}},
				{ts: 10, line: dedupS("x"), labels: []dedupKV{{key: "k", val: ""}}, meta: []dedupKV{}},
			}}}, wantRows: 2, wantDups: 0, memOK: false},
		{name: "null line versus empty line",
			files: []dedupFile{{rows: []dedupRow{
				{ts: 10, line: nil, labels: []dedupKV{}, meta: []dedupKV{}},
				{ts: 10, line: dedupS(""), labels: []dedupKV{}, meta: []dedupKV{}},
			}}}, wantRows: 2, wantDups: 0, memOK: false},
		{name: "null lines collapse",
			files: []dedupFile{{rows: []dedupRow{
				{ts: 10, line: nil, labels: []dedupKV{}, meta: []dedupKV{}},
				{ts: 10, line: nil, labels: []dedupKV{}, meta: []dedupKV{}},
			}}}, wantRows: 1, wantDups: 1, memOK: false},

		// Empty inputs.
		{name: "empty inputs",
			files:    []dedupFile{{}, {}},
			wantRows: 0, wantDups: 0, memOK: true},
	}
}

// TestMergeDedupLongEqualRun crosses every boundary the dedup state must
// survive: one input's equal-timestamp run larger than the reader's 8192-row
// record batches and the merge's 1024-row output batches, with duplicates
// planted mid-batch, adjacent across an output batch boundary (rows
// 1023/1024), nonadjacent far apart (rows 2000/3000) and across the reader
// record boundary (rows 8191/8192 duplicating rows of the first record). The
// trailing row at a new timestamp repeats an earlier identity and must
// survive: the seen set is released when the global timestamp advances.
func TestMergeDedupLongEqualRun(t *testing.T) {
	const n = 9000
	rows := make([]dedupRow, n)
	for i := range n {
		rows[i] = dedupMR(42, "r"+strconv.Itoa(i))
	}
	rows[500] = dedupMR(42, "r499")        // adjacent, mid batch
	rows[1024] = dedupMR(42, "r1023")      // adjacent across the 1024-row output batch boundary
	rows[3000] = dedupMR(42, "r2000")      // nonadjacent, another output batch
	rows[8191] = dedupMR(42, "r0")         // last row of the first reader record
	rows[8192] = dedupMR(42, "r1")         // first row of the second reader record
	rows = append(rows, dedupMR(43, "r0")) // new timestamp: state resets, survives

	in := dedupInput(t, dedupFile{rows: rows})
	out := &bytes.Buffer{}
	stats, err := storage.Merge(t.Context(), out, []*storage.Reader{in}, nil)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if stats.Rows != n+1-5 || stats.DeduplicatedRows != 5 {
		t.Fatalf("stats.Rows = %d, DeduplicatedRows = %d, want %d/5", stats.Rows, stats.DeduplicatedRows, n+1-5)
	}

	_, got := mergeRows(t, out.Bytes())
	counts := map[string]int{}
	for _, row := range got {
		if ts := row[0].(int64); ts != 42 && ts != 43 {
			t.Fatalf("ts = %d, want 42 or 43", ts)
		}
		counts[row[1].(string)]++
	}
	if len(got) != n+1-5 {
		t.Fatalf("rows = %d, want %d", len(got), n+1-5)
	}
	// The planted copies are gone, and the identity before the timestamp
	// change survives twice: once per run.
	for line, want := range map[string]int{
		"r0": 2, "r499": 1, "r1023": 1, "r2000": 1,
		"r500": 0, "r1024": 0, "r3000": 0, "r8191": 0, "r8192": 0,
	} {
		if got := counts[line]; got != want {
			t.Fatalf("line %q appears %d times, want %d", line, got, want)
		}
	}
}

// TestMergeDedupSurvivorKeepsFirstInputValues checks the stable survivor
// order: the first row in merge order wins, so its pipeline column values are
// the ones stored.
func TestMergeDedupSurvivorKeepsFirstInputValues(t *testing.T) {
	f0 := dedupFile{cols: []string{"pipe"}, rows: []dedupRow{dedupRowOf(10, "x",
		map[string]string{"service_name": "api"}, map[string]string{}, map[string]string{"pipe": "first"})}}
	f1 := dedupFile{cols: []string{"pipe"}, rows: []dedupRow{dedupRowOf(10, "x",
		map[string]string{"service_name": "api"}, map[string]string{}, map[string]string{"pipe": "second"})}}
	var readers []*storage.Reader
	for _, f := range []dedupFile{f0, f1} {
		readers = append(readers, dedupInput(t, f))
	}
	out := &bytes.Buffer{}
	stats, err := storage.Merge(t.Context(), out, readers, nil)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if stats.Rows != 1 || stats.DeduplicatedRows != 1 {
		t.Fatalf("stats = %d/%d, want 1/1", stats.Rows, stats.DeduplicatedRows)
	}
	_, rows := mergeRows(t, out.Bytes())
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if got := rows[0][4]; got != "first" {
		t.Fatalf("survivor pipe = %v, want \"first\" from input 0", got)
	}
}

// dedupMergedPlanRows reads a merged file back as effective plan.Rows, with
// the same effective-label rule as dedupMemPlanRows, computed independently
// of the merge identity code under test.
func dedupMergedPlanRows(t *testing.T, data []byte, promoted []string) []plan.Row {
	t.Helper()
	r, err := storage.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("dedupMergedPlanRows NewReader: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	var rows []plan.Row
	for rec, err := range r.Records(t.Context()) {
		if err != nil {
			t.Fatalf("dedupMergedPlanRows Records: %v", err)
		}
		ts := rec.Column(0).(*array.Timestamp)
		line := rec.Column(1).(*array.String)
		labels := rec.Column(2).(*array.Map)
		meta := rec.Column(3).(*array.Map)
		lk := labels.Keys().(*array.String)
		li := labels.Items().(*array.String)
		mk := meta.Keys().(*array.String)
		mi := meta.Items().(*array.String)
		promotedCols := map[string]*array.String{}
		for c := 4; c < int(rec.NumCols()); c++ {
			name := rec.Schema().Field(c).Name
			if slices.Contains(promoted, name) {
				promotedCols[name] = rec.Column(c).(*array.String)
			}
		}
		for i := range int(rec.NumRows()) {
			labelsMap := map[string]string{}
			if !labels.IsNull(i) {
				beg, end := labels.ValueOffsets(i)
				for k := beg; k < end; k++ {
					labelsMap[lk.Value(int(k))] = li.Value(int(k))
				}
			}
			var pairs []logs.Label
			for k, v := range labelsMap {
				if !slices.Contains(promoted, k) {
					pairs = append(pairs, logs.Label{Name: k, Value: v})
				}
			}
			for _, p := range promoted {
				switch {
				case promotedCols[p] != nil && !promotedCols[p].IsNull(i):
					pairs = append(pairs, logs.Label{Name: p, Value: promotedCols[p].Value(i)})
				default:
					if v, ok := labelsMap[p]; ok {
						pairs = append(pairs, logs.Label{Name: p, Value: v})
					}
				}
			}
			var metaPairs []logs.Label
			if !meta.IsNull(i) {
				beg, end := meta.ValueOffsets(i)
				for k := beg; k < end; k++ {
					metaPairs = append(metaPairs, logs.Label{Name: mk.Value(int(k)), Value: mi.Value(int(k))})
				}
			}
			rows = append(rows, plan.Row{
				TS:     schema.Time(int64(ts.Value(i))),
				Line:   line.Value(i),
				Labels: logs.New(pairs...),
				Meta:   logs.New(metaPairs...),
			})
		}
	}
	return rows
}

// TestMergeDedupPlantestFixture merges the real plantest dataset's a1 file
// (202 rows, level promoted) with its dups file (32 rows: thirty re-delivered
// a1 rows, one of them twice, plus one metadata-distinct copy) the way
// compaction would: 234 physical rows collapse to 203 with 31 removed, the
// metadata-distinct copy survives, and the surviving sequence equals what
// plan.MemSource, the reference read-time dedup, returns for the same rows.
func TestMergeDedupPlantestFixture(t *testing.T) {
	var a1File, dupsFile plantest.File
	for _, f := range plantest.Dataset() {
		switch {
		case f.Tenant == plantest.OtherTenant:
		case f.NoSidecar:
			dupsFile = f
		case slices.Equal(f.Promoted, []string{"level"}) && len(f.Rows) == 202:
			a1File = f
		}
	}
	if a1File.Rows == nil || dupsFile.Rows == nil {
		t.Fatal("plantest fixture files not found")
	}

	var readers []*storage.Reader
	var physical int64
	for _, f := range []plantest.File{a1File, dupsFile} {
		data, st, err := plantest.Encode(f)
		if err != nil {
			t.Fatalf("plantest encode: %v", err)
		}
		if st.DeduplicatedRows != 0 {
			t.Fatalf("plain Writer set DeduplicatedRows = %d, want 0", st.DeduplicatedRows)
		}
		r, err := storage.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		t.Cleanup(func() { _ = r.Close() })
		readers = append(readers, r)
		physical += st.Rows
	}
	if physical != 234 {
		t.Fatalf("physical input rows = %d, want 234", physical)
	}

	var merged bytes.Buffer
	stats, err := storage.Merge(t.Context(), &merged, readers, []string{"level"})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if stats.Rows != 203 || stats.DeduplicatedRows != 31 {
		t.Fatalf("stats rows/dups = %d/%d, want 203/31", stats.Rows, stats.DeduplicatedRows)
	}
	if stats.Rows+stats.DeduplicatedRows != physical {
		t.Fatalf("rows %d + dups %d != physical %d", stats.Rows, stats.DeduplicatedRows, physical)
	}
	if !stats.LabelsComplete {
		t.Fatal("LabelsComplete = false, want true")
	}

	want := dedupMemSelect(t, plantest.Mem([]plantest.File{a1File, dupsFile}))
	got := dedupMergedPlanRows(t, merged.Bytes(), []string{"level"})
	if len(want) != 203 || len(got) != 203 {
		t.Fatalf("MemSource rows = %d, merged rows = %d, want 203/203", len(want), len(got))
	}
	for i := range want {
		w, g := want[i], got[i]
		if !g.TS.Equal(w.TS) || g.Line != w.Line || !slices.Equal(g.Labels, w.Labels) || !slices.Equal(g.Meta, w.Meta) {
			t.Fatalf("row %d: merged {%v %v %v %v} != MemSource {%v %v %v %v}",
				i, g.TS, g.Line, g.Labels, g.Meta, w.TS, w.Line, w.Labels, w.Meta)
		}
	}
	// The one metadata-distinct re-delivery survives exactly once.
	var distinct int
	for _, r := range got {
		if v, ok := r.Meta.Get("trace_id"); ok && v == "retry-differs" {
			distinct++
		}
	}
	if distinct != 1 {
		t.Fatalf("metadata-distinct copy appears %d times, want 1", distinct)
	}
}
