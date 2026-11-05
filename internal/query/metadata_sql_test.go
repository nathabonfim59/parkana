// Executor tests for the SQL metadata reader (B2c): real Parquet objects
// written the way the ingester stores them, listed by a SQLite catalog and
// read through DuckDB, checked against a plain reference over the fixture rows.
// The HTTP shapes are covered by the fake-reader tests in metadata_http_test.go
// and are deliberately not repeated here.
package query

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// openMetaEnv loads files like the ingester through the narrow recording
// catalog and returns the env plus a metadata reader over it. It is used only
// where the catalog listing filter itself is asserted; every ordinary executor
// test uses openMetaReader's real FS/cache env.
func openMetaEnv(t *testing.T, files []plantest.File) (*sourceEnv, metaReader) {
	t.Helper()
	env := openSourceEnv(t, files, defaultStreamLabels)
	return env, newMetadataReader(env.source)
}

// openMetaReader returns a metadata reader over the real backend environment
// (FS object store, real object cache as resolver, SQLite catalog, DuckDB).
func openMetaReader(t *testing.T, files []plantest.File) metaReader {
	t.Helper()
	return newMetadataReader(newBackendEnv(t, backendFS, files).src)
}

// metaRow builds one fixture row from name/value pairs.
func metaRow(ts time.Time, line string, kv ...string) plan.Row {
	m := make(map[string]string, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(m)}
}

// metaEq is an equality matcher as a test table writes it.
func metaEq(name, value string) plan.Matcher {
	return plan.MustMatcher(plan.MatchEqual, name, value)
}

// metaWindow is a query window over the whole fixture dataset, End inclusive.
func metaWindow(tid tenant.ID, groups ...[]plan.Matcher) metaQuery {
	return metaQuery{
		Tenant: tid,
		Start:  plantest.T0.Add(-time.Hour),
		End:    plantest.T0.Add(time.Hour),
		Groups: groups,
	}
}

// metaMatched returns the fixture rows the query must answer from: tenant,
// inclusive [start, end] and any of groups (AND within a group, OR across
// groups; no groups means every row). Rows carry their complete label map, so
// promotion cannot change the reference answer. Exact stored copies collapse
// under the Source contract — identity is TS, complete labels, full metadata
// and line — so stats and volume count each logical row once; rows differing in
// any identity field survive, including metadata-only differences.
func metaMatched(files []plantest.File, tid tenant.ID, start, end time.Time, groups [][]plan.Matcher) []plan.Row {
	type identity struct {
		us                 int64
		labels, meta, line string
	}
	seen := make(map[identity]struct{})
	var out []plan.Row
	for _, f := range files {
		if f.Tenant != tid {
			continue
		}
		for _, r := range f.Rows {
			if r.TS.Before(start) || r.TS.After(end) || !metaGroupMatches(r.Labels, groups) {
				continue
			}
			id := identity{r.TS.UnixMicro(), r.Labels.String(), r.Meta.String(), r.Line}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, r)
		}
	}
	return out
}

// metaGroupMatches applies Loki's selector semantics to a row's labels.
func metaGroupMatches(ls logs.Labels, groups [][]plan.Matcher) bool {
	if len(groups) == 0 {
		return true
	}
	for _, group := range groups {
		matched := true
		for _, m := range group {
			if !m.MatchesLabels(ls) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

// metaRefNames is the sorted distinct label-name set of rows.
func metaRefNames(rows []plan.Row) []string {
	var names []string
	for _, r := range rows {
		for _, l := range r.Labels {
			names = append(names, l.Name)
		}
	}
	return slices.Compact(slices.Sorted(slices.Values(names)))
}

// metaRefValues is the sorted distinct value set of name; an absent label
// contributes nothing and an empty value is a value.
func metaRefValues(rows []plan.Row, name string) []string {
	var values []string
	for _, r := range rows {
		if v, ok := r.Labels.Get(name); ok {
			values = append(values, v)
		}
	}
	return slices.Compact(slices.Sorted(slices.Values(values)))
}

// metaRefSeries is the distinct label sets of rows in canonical string order.
func metaRefSeries(rows []plan.Row) []logs.Labels {
	seen := make(map[string]logs.Labels)
	for _, r := range rows {
		seen[r.Labels.String()] = r.Labels
	}
	sets := slices.Collect(maps.Values(seen))
	slices.SortFunc(sets, func(a, b logs.Labels) int { return strings.Compare(a.String(), b.String()) })
	return sets
}

// metaRefStats is the approved Parquet meaning of stats over rows; chunks is
// asserted separately because it counts read files, not matched rows.
func metaRefStats(rows []plan.Row) metaStats {
	streams := make(map[string]struct{})
	var bytes uint64
	for _, r := range rows {
		streams[r.Labels.String()] = struct{}{}
		bytes += uint64(len(r.Line))
	}
	return metaStats{Streams: uint64(len(streams)), Bytes: bytes, Entries: uint64(len(rows))}
}

// metaChunks is the number of fixture files a listing of tid's files in the
// inclusive window that forces stream must return: [MinTS, MaxTS] overlaps
// [start, end+1ns], and the stream matches when one is forced.
func metaChunks(files []plantest.File, tid tenant.ID, start, end time.Time, stream string) uint64 {
	var n uint64
	for _, f := range files {
		if f.Tenant != tid || stream != "" && f.Stream != stream {
			continue
		}
		if len(f.Rows) == 0 {
			continue
		}
		minTS, maxTS := f.Rows[0].TS, f.Rows[0].TS
		for _, r := range f.Rows[1:] {
			if r.TS.Before(minTS) {
				minTS = r.TS
			}
			if r.TS.After(maxTS) {
				maxTS = r.TS
			}
		}
		if maxTS.Before(start) || minTS.After(end.Add(time.Nanosecond)) {
			continue
		}
		n++
	}
	return n
}

// TestMetaSQLLabelNames covers the sorted distinct names the reader returns for
// the mixed-promotion dataset, with and without selectors.
func TestMetaSQLLabelNames(t *testing.T) {
	files := plantest.Dataset()
	r := openMetaReader(t, files)
	start, end := plantest.T0.Add(-time.Hour), plantest.T0.Add(time.Hour)

	for _, tc := range []struct {
		name   string
		groups [][]plan.Matcher
	}{
		{"all", nil},
		{"api selector", [][]plan.Matcher{{metaEq("service_name", "api")}}},
		{"batch selector", [][]plan.Matcher{{metaEq("service_name", "batch")}}},
		{"union selector", [][]plan.Matcher{
			{metaEq("service_name", "api")},
			{metaEq("service_name", "web")},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := metaQuery{Tenant: tenant.Default, Start: start, End: end, Groups: tc.groups}
			got, err := r.LabelNames(t.Context(), q)
			if err != nil {
				t.Fatalf("LabelNames: %v", err)
			}
			want := metaRefNames(metaMatched(files, tenant.Default, start, end, tc.groups))
			if !slices.Equal(got, want) {
				t.Fatalf("LabelNames = %v, want %v", got, want)
			}
		})
	}

	// Case is significant: "Level" and "level" are different labels, and the
	// map-only LINE/Labels labels are names too.
	got, err := r.LabelNames(t.Context(), metaQuery{Tenant: tenant.Default, Start: start, End: end})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Level", "level", "LINE", "Labels", "Weird_Label9"} {
		if !slices.Contains(got, name) {
			t.Errorf("LabelNames %v is missing %q", got, name)
		}
	}
}

// TestMetaSQLLabelValues covers sorted distinct values, case-sensitive names,
// absent labels, empty-string values and inverted ranges.
func TestMetaSQLLabelValues(t *testing.T) {
	files := plantest.Dataset()
	r := openMetaReader(t, files)
	start, end := plantest.T0.Add(-time.Hour), plantest.T0.Add(time.Hour)

	for _, tc := range []struct {
		name   string
		label  string
		groups [][]plan.Matcher
	}{
		{"env all streams", "env", nil},
		{"level api", "level", [][]plan.Matcher{{metaEq("service_name", "api")}}},
		{"capital Level", "Level", nil},
		{"missing label", "nope", nil},
		{"map-only label", "LINE", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.LabelValues(t.Context(), metaQuery{
				Tenant: tenant.Default, Start: start, End: end, Groups: tc.groups,
			}, tc.label)
			if err != nil {
				t.Fatalf("LabelValues(%q): %v", tc.label, err)
			}
			want := metaRefValues(metaMatched(files, tenant.Default, start, end, tc.groups), tc.label)
			if !slices.Equal(got, want) {
				t.Fatalf("LabelValues(%q) = %v, want %v", tc.label, got, want)
			}
		})
	}

	// An empty value is a value; a row without the label adds nothing.
	empty := []plantest.File{{
		Tenant: tenant.Default, Stream: "s",
		Rows: []plan.Row{
			metaRow(plantest.T0, "empty", "service_name", "s", "blank", ""),
			metaRow(plantest.T0.Add(time.Second), "absent", "service_name", "s"),
		},
	}}
	er := openMetaReader(t, empty)
	got, err := er.LabelValues(t.Context(), metaWindow(tenant.Default), "blank")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{""}) {
		t.Fatalf("LabelValues(blank) = %q, want one empty value", got)
	}

	// End before Start finds nothing, like Loki's index lookups.
	inverted := metaQuery{Tenant: tenant.Default, Start: end, End: start}
	if got, err := r.LabelValues(t.Context(), inverted, "env"); err != nil || len(got) != 0 {
		t.Fatalf("inverted LabelValues = %v, %v; want empty, nil", got, err)
	}
	if got, err := r.Series(t.Context(), inverted); err != nil || len(got) != 0 {
		t.Fatalf("inverted Series = %v, %v; want empty, nil", got, err)
	}
}

// TestMetaSQLLabelValuesInvalidName pins the pinned-Loki answer for a label
// name DuckDB cannot bind as a VARCHAR: HTTP 200 with no values, not an error.
// The valid-name call is the positive control that the fixture has data.
func TestMetaSQLLabelValuesInvalidName(t *testing.T) {
	start := plantest.T0
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "api", Promoted: []string{"level"},
		Rows: []plan.Row{
			metaRow(start, "first line", "service_name", "api", "env", "dev", "level", "info"),
			metaRow(start.Add(time.Second), "second line", "service_name", "api", "env", "prod", "level", "info"),
		},
	}}
	r := openMetaReader(t, files)
	q := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(time.Hour)}

	got, err := r.LabelValues(t.Context(), q, "\xff")
	if err != nil {
		t.Fatalf("LabelValues(invalid UTF-8 name) = %v, want empty like Loki", err)
	}
	if len(got) != 0 {
		t.Fatalf("LabelValues(invalid UTF-8 name) = %q, want no values", got)
	}
	valid, err := r.LabelValues(t.Context(), q, "env")
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"dev", "prod"}; !slices.Equal(valid, want) {
		t.Fatalf("positive control LabelValues(env) = %v, want %v", valid, want)
	}
}

// TestMetaSQLSeries covers distinct canonical label sets across mixed promotion,
// including equal sets stored with different promoted columns and case-only
// label differences.
func TestMetaSQLSeries(t *testing.T) {
	files := plantest.Dataset()
	r := openMetaReader(t, files)
	start, end := plantest.T0.Add(-time.Hour), plantest.T0.Add(time.Hour)

	for _, tc := range []struct {
		name   string
		groups [][]plan.Matcher
	}{
		{"all", nil},
		{"api", [][]plan.Matcher{{metaEq("service_name", "api")}}},
		{"case labels", [][]plan.Matcher{{metaEq("service_name", "api"), metaEq("Level", "High")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.Series(t.Context(), metaQuery{
				Tenant: tenant.Default, Start: start, End: end, Groups: tc.groups,
			})
			if err != nil {
				t.Fatalf("Series: %v", err)
			}
			want := metaRefSeries(metaMatched(files, tenant.Default, start, end, tc.groups))
			if len(got) != len(want) {
				t.Fatalf("Series returned %d sets, want %d", len(got), len(want))
			}
			for i := range want {
				if got[i].String() != want[i].String() {
					t.Fatalf("Series[%d] = %s, want %s", i, got[i], want[i])
				}
			}
		})
	}

	// The same label set promoted differently in different files is one set.
	ls := map[string]string{"service_name": "same", "env": "prod", "level": "info"}
	same := []plantest.File{
		{Tenant: tenant.Default, Stream: "same", Promoted: []string{"level"}, Rows: []plan.Row{
			metaRow(plantest.T0, "a", "service_name", "same", "env", "prod", "level", "info"),
		}},
		{Tenant: tenant.Default, Stream: "same", Rows: []plan.Row{
			metaRow(plantest.T0.Add(time.Second), "b", "service_name", "same", "env", "prod", "level", "info"),
		}},
		{Tenant: tenant.Default, Stream: "same", Promoted: []string{"env", "level"}, Rows: []plan.Row{
			metaRow(plantest.T0.Add(2*time.Second), "c", "service_name", "same", "env", "prod", "level", "info"),
		}},
	}
	sr := openMetaReader(t, same)
	got, err := sr.Series(t.Context(), metaWindow(tenant.Default))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].String() != logs.FromMap(ls).String() {
		t.Fatalf("Series over mixed promotion = %v, want one %s", got, logs.FromMap(ls))
	}
}

// TestMetaSQLStats covers the approved Parquet meanings: distinct label sets,
// rows, UTF-8 line bytes and the acquired Parquet file count, including files
// read while zero rows match.
func TestMetaSQLStats(t *testing.T) {
	files := plantest.Dataset()
	r := openMetaReader(t, files)

	t.Run("whole dataset", func(t *testing.T) {
		q := metaWindow(tenant.Default)
		got, err := r.Stats(t.Context(), q)
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		want := metaRefStats(metaMatched(files, tenant.Default, q.Start, q.End, nil))
		want.Chunks = metaChunks(files, tenant.Default, q.Start, q.End, "")
		if got != want {
			t.Fatalf("Stats = %+v, want %+v", got, want)
		}
	})

	t.Run("stream pruned", func(t *testing.T) {
		q := metaWindow(tenant.Default, []plan.Matcher{metaEq("service_name", "api")})
		got, err := r.Stats(t.Context(), q)
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		want := metaRefStats(metaMatched(files, tenant.Default, q.Start, q.End, q.Groups))
		want.Chunks = metaChunks(files, tenant.Default, q.Start, q.End, "api")
		if got != want {
			t.Fatalf("Stats = %+v, want %+v", got, want)
		}
	})

	t.Run("zero matched rows still counts files", func(t *testing.T) {
		q := metaWindow(tenant.Default, []plan.Matcher{metaEq("service_name", "api"), metaEq("env", "nowhere")})
		got, err := r.Stats(t.Context(), q)
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		want := metaStats{Chunks: metaChunks(files, tenant.Default, q.Start, q.End, "api")}
		if got != want {
			t.Fatalf("Stats = %+v, want %+v (only files read)", got, want)
		}
		if want.Chunks == 0 {
			t.Fatal("fixture must read at least one file for this case")
		}
	})

	t.Run("narrow window reads one file", func(t *testing.T) {
		start, end := plantest.T0.Add(600*time.Second), plantest.T0.Add(640*time.Second)
		q := metaQuery{Tenant: tenant.Default, Start: start, End: end}
		got, err := r.Stats(t.Context(), q)
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		want := metaRefStats(metaMatched(files, tenant.Default, start, end, nil))
		want.Chunks = metaChunks(files, tenant.Default, start, end, "")
		if want.Chunks != 1 {
			t.Fatalf("fixture window must read exactly 1 file, got %d", want.Chunks)
		}
		if got != want {
			t.Fatalf("Stats = %+v, want %+v", got, want)
		}
	})

	t.Run("tenant isolation", func(t *testing.T) {
		q := metaWindow(plantest.OtherTenant)
		got, err := r.Stats(t.Context(), q)
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		want := metaRefStats(metaMatched(files, plantest.OtherTenant, q.Start, q.End, nil))
		want.Chunks = metaChunks(files, plantest.OtherTenant, q.Start, q.End, "")
		if got != want {
			t.Fatalf("Stats = %+v, want %+v", got, want)
		}
	})

	t.Run("empty store", func(t *testing.T) {
		er := openMetaReader(t, nil)
		got, err := er.Stats(t.Context(), metaWindow(tenant.Default))
		if err != nil {
			t.Fatalf("Stats: %v", err)
		}
		if got != (metaStats{}) {
			t.Fatalf("Stats over an empty store = %+v, want all zero", got)
		}
	})

	t.Run("empty and unicode lines count bytes", func(t *testing.T) {
		er := openMetaReader(t, []plantest.File{{
			Tenant: tenant.Default, Stream: "b", Promoted: []string{"level"},
			Rows: []plan.Row{
				metaRow(plantest.T0, "", "service_name", "b", "level", "info"),
				metaRow(plantest.T0.Add(time.Second), "café ☕", "service_name", "b", "level", "info"),
			},
		}})
		got, err := er.Stats(t.Context(), metaWindow(tenant.Default))
		if err != nil {
			t.Fatal(err)
		}
		if want := uint64(len("café ☕")); got.Bytes != want || got.Entries != 2 {
			t.Fatalf("Stats = %+v, want %d bytes over 2 entries", got, want)
		}
	})
}

// TestMetaSQLInclusiveEnd pins Loki's inclusive index window at every endpoint,
// including nanosecond End boundaries over whole-microsecond stored rows.
func TestMetaSQLInclusiveEnd(t *testing.T) {
	base := plantest.T0.Add(time.Minute)
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "s", Promoted: []string{"level"},
		Rows: []plan.Row{
			metaRow(base.Add(-time.Microsecond), "before", "service_name", "s", "slot", "before"),
			metaRow(base, "at end", "service_name", "s", "slot", "at"),
			metaRow(base.Add(time.Microsecond), "after", "service_name", "s", "slot", "after"),
		},
	}}
	r := openMetaReader(t, files)
	q := metaQuery{Tenant: tenant.Default, Start: base.Add(-time.Microsecond), End: base}

	names, err := r.LabelNames(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names, []string{"service_name", "slot"}) {
		t.Fatalf("LabelNames = %v", names)
	}
	values, err := r.LabelValues(t.Context(), q, "slot")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(values, []string{"at", "before"}) {
		t.Fatalf("LabelValues = %v, want the row exactly at End and the one before", values)
	}
	sets, err := r.Series(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 2 {
		t.Fatalf("Series = %v, want two sets (the row after End must be excluded)", sets)
	}
	stats, err := r.Stats(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Entries != 2 || stats.Bytes != uint64(len("before")+len("at end")) {
		t.Fatalf("Stats = %+v, want 2 entries and their bytes", stats)
	}

	// A nanosecond End over stored whole microseconds still includes the row
	// at floor(End); one nanosecond later it must not.
	nsQuery := q
	nsQuery.End = base.Add(500 * time.Nanosecond)
	if values, err := r.LabelValues(t.Context(), nsQuery, "slot"); err != nil {
		t.Fatal(err)
	} else if !slices.Equal(values, []string{"at", "before"}) {
		t.Fatalf("nanosecond End LabelValues = %v, want the whole-microsecond row at End included", values)
	}
}

// TestMetaSQLMultiGroupSelector covers OR-combined selector groups on every
// endpoint that accepts several: overlapping groups must count a row once,
// listing empty means zero chunks, and an empty group matches everything.
func TestMetaSQLMultiGroupSelector(t *testing.T) {
	files := plantest.Dataset()
	r := openMetaReader(t, files)
	q := metaWindow(tenant.Default,
		[]plan.Matcher{metaEq("service_name", "api")},
		[]plan.Matcher{metaEq("env", "prod")},
	)
	rows := metaMatched(files, tenant.Default, q.Start, q.End, q.Groups)

	sets, err := r.Series(t.Context(), q)
	if err != nil {
		t.Fatalf("multi-group Series: %v", err)
	}
	want := metaRefSeries(rows)
	if len(sets) != len(want) {
		t.Fatalf("multi-group Series returned %d sets, want %d", len(sets), len(want))
	}
	for i := range want {
		if sets[i].String() != want[i].String() {
			t.Fatalf("Series[%d] = %s, want %s", i, sets[i], want[i])
		}
	}

	stats, err := r.Stats(t.Context(), q)
	if err != nil {
		t.Fatalf("multi-group Stats: %v", err)
	}
	wantStats := metaRefStats(rows)
	// Overlapping groups must not multiply rows: entries are the distinct
	// matched rows, not the sum per group.
	if stats.Entries != wantStats.Entries {
		t.Fatalf("multi-group Stats entries = %d, want %d (rows counted once)", stats.Entries, wantStats.Entries)
	}
	if stats.Bytes != wantStats.Bytes || stats.Streams != wantStats.Streams {
		t.Fatalf("multi-group Stats = %+v, want %+v", stats, wantStats)
	}

	vol, err := r.Volume(t.Context(), metaVolumeRequest{Query: q})
	if err != nil {
		t.Fatalf("multi-group Volume: %v", err)
	}
	total := uint64(0)
	for _, e := range vol {
		for _, s := range e.Buckets {
			total += s.Volume
		}
	}
	if total != wantStats.Bytes {
		t.Fatalf("multi-group Volume total = %d, want %d", total, wantStats.Bytes)
	}

	// An empty selector group matches every row, and the compiled OR must keep
	// its parentheses balanced (the draft left the empty-group arm unclosed).
	all := metaWindow(tenant.Default, []plan.Matcher{metaEq("service_name", "batch")}, nil)
	stats, err = r.Stats(t.Context(), all)
	if err != nil {
		t.Fatalf("empty-group Stats: %v", err)
	}
	wantAll := metaRefStats(metaMatched(files, tenant.Default, all.Start, all.End, all.Groups))
	if stats.Entries != wantAll.Entries || stats.Bytes != wantAll.Bytes {
		t.Fatalf("empty-group Stats = %+v, want %+v", stats, wantAll)
	}
}

// TestMetaSQLVolumeInstant covers the step-0 whole-range bucket: every matched
// row counted exactly once, grouped by the pinned matcher-name projection, with
// Loki's ordering.
func TestMetaSQLVolumeInstant(t *testing.T) {
	files := plantest.Dataset()
	r := openMetaReader(t, files)
	q := metaWindow(tenant.Default, []plan.Matcher{metaEq("service_name", "api")})
	req := metaVolumeRequest{Query: q}
	shape := metaTestVolumeShapeOf(q, req.TargetLabels)

	got, err := r.Volume(t.Context(), req)
	if err != nil {
		t.Fatalf("Volume: %v", err)
	}
	rows := metaMatched(files, tenant.Default, q.Start, q.End, shape.groups)
	want := metavolumeWant(rows, newVolumeGrid(q.Start, q.End, 0), req, shape)
	metaAssertVolumes(t, got, want)
	metaAssertVolumeOrder(t, got)

	// The selector names only service_name, so series volume projects onto
	// that matcher name: one result, not one per stored label set.
	if len(got) != 1 || got[0].Metric.String() != `{service_name="api"}` {
		t.Fatalf("matcher-name projected volume = %v, want exactly {service_name=\"api\"}", got)
	}

	stats, err := r.Stats(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	var total uint64
	for _, e := range got {
		for _, s := range e.Buckets {
			total += s.Volume
		}
	}
	if total != stats.Bytes {
		t.Fatalf("instant volume total = %d, want stats bytes %d", total, stats.Bytes)
	}
	for _, e := range got {
		if len(e.Buckets) != 1 {
			t.Fatalf("instant volume entry %s has %d buckets, want 1", e.Metric, len(e.Buckets))
		}
		if !e.Buckets[0].End.Equal(q.End) {
			t.Fatalf("instant bucket End = %s, want request End %s", e.Buckets[0].End, q.End)
		}
	}

	t.Run("limit", func(t *testing.T) {
		got, err := r.Volume(t.Context(), metaVolumeRequest{Query: q, Limit: 2})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) > 2 {
			t.Fatalf("Volume with Limit 2 returned %d entries", len(got))
		}
	})
}

// TestMetaSQLVolumeRange covers the continuous window partition: rows in the last
// microsecond before a boundary, exactly at a boundary and exactly at End,
// with Loki's sample timestamps and sum(range) == instant bytes.
func TestMetaSQLVolumeRange(t *testing.T) {
	start := plantest.T0
	end := start.Add(3 * time.Second)
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "vol", Promoted: []string{"level"},
		Rows: []plan.Row{
			metaRow(start, "a", "service_name", "vol", "level", "info"),
			metaRow(start.Add(time.Second-time.Microsecond), "é", "service_name", "vol", "level", "info"),
			metaRow(start.Add(time.Second), "bb", "service_name", "vol", "level", "info"),
			metaRow(start.Add(2*time.Second-time.Microsecond), "語", "service_name", "vol", "level", "info"),
			metaRow(start.Add(2*time.Second), "d", "service_name", "vol", "level", "info"),
			metaRow(end, "ee", "service_name", "vol", "level", "info"),
			metaRow(end.Add(time.Microsecond), "outside", "service_name", "vol", "level", "info"),
		},
	}}
	r := openMetaReader(t, files)
	q := metaQuery{Tenant: tenant.Default, Start: start, End: end}

	got, err := r.Volume(t.Context(), metaVolumeRequest{Query: q, Step: time.Second})
	if err != nil {
		t.Fatalf("Volume: %v", err)
	}
	const key = `{level="info", service_name="vol"}`
	want := map[time.Time]map[string]uint64{
		start.Add(time.Second - time.Millisecond).UTC(): {
			key: uint64(len("a") + len("é")),
		},
		start.Add(2*time.Second - time.Millisecond).UTC(): {
			key: uint64(len("bb") + len("語")),
		},
		end.UTC(): {
			key: uint64(len("d") + len("ee")),
		},
	}
	metaAssertVolumes(t, got, want)

	instant, err := r.Volume(t.Context(), metaVolumeRequest{Query: q})
	if err != nil {
		t.Fatal(err)
	}
	var rangeTotal, instantTotal uint64
	for _, e := range got {
		for _, s := range e.Buckets {
			rangeTotal += s.Volume
		}
	}
	for _, e := range instant {
		for _, s := range e.Buckets {
			instantTotal += s.Volume
		}
	}
	if rangeTotal != instantTotal {
		t.Fatalf("range volume total = %d, instant volume total = %d over the same range", rangeTotal, instantTotal)
	}
	if wantInstant := uint64(len("a") + len("é") + len("bb") + len("語") + len("d") + len("ee")); instantTotal != wantInstant {
		t.Fatalf("instant total = %d, want %d (the row after End is excluded)", instantTotal, wantInstant)
	}
}

// TestMetaSQLVolumePartialFirstWindow covers a Start that is not on the step grid:
// the first window begins at Start, later windows at whole boundaries.
func TestMetaSQLVolumePartialFirstWindow(t *testing.T) {
	start := plantest.T0.Add(250 * time.Millisecond)
	end := plantest.T0.Add(3 * time.Second)
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "vol", Promoted: []string{"level"},
		Rows: []plan.Row{
			metaRow(start, "a", "service_name", "vol", "level", "info"),
			metaRow(plantest.T0.Add(999*time.Millisecond), "bb", "service_name", "vol", "level", "info"),
			metaRow(plantest.T0.Add(time.Second), "ccc", "service_name", "vol", "level", "info"),
			metaRow(plantest.T0.Add(2*time.Second), "dddd", "service_name", "vol", "level", "info"),
			metaRow(end, "eeeee", "service_name", "vol", "level", "info"),
		},
	}}
	r := openMetaReader(t, files)
	q := metaQuery{Tenant: tenant.Default, Start: start, End: end}

	got, err := r.Volume(t.Context(), metaVolumeRequest{Query: q, Step: time.Second})
	if err != nil {
		t.Fatalf("Volume: %v", err)
	}
	const key = `{level="info", service_name="vol"}`
	want := map[time.Time]map[string]uint64{
		plantest.T0.Add(time.Second - time.Millisecond).UTC(): {
			key: uint64(len("a") + len("bb")),
		},
		plantest.T0.Add(2*time.Second - time.Millisecond).UTC(): {
			key: uint64(len("ccc")),
		},
		end.UTC(): {
			key: uint64(len("dddd") + len("eeeee")),
		},
	}
	metaAssertVolumes(t, got, want)
}

// TestMetaSQLVolumeStartEqualsEnd pins the approved one-point-bucket exception:
// Loki's ForInterval emits no bucket at all, Parkana keeps exactly one final
// bucket at End.
func TestMetaSQLVolumeStartEqualsEnd(t *testing.T) {
	for _, tc := range []struct {
		name  string
		point time.Time
		step  time.Duration
	}{
		{"aligned", plantest.T0, time.Second},
		{"unaligned", plantest.T0.Add(500 * time.Millisecond), time.Second},
		{"instant step", plantest.T0.Add(500 * time.Millisecond), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := []plantest.File{{
				Tenant: tenant.Default, Stream: "p", Promoted: []string{"level"},
				Rows: []plan.Row{
					metaRow(tc.point.Add(-time.Microsecond), "before", "service_name", "p", "level", "info"),
					metaRow(tc.point, "at", "service_name", "p", "level", "info"),
					metaRow(tc.point.Add(time.Microsecond), "after", "service_name", "p", "level", "info"),
				},
			}}
			r := openMetaReader(t, files)
			q := metaQuery{Tenant: tenant.Default, Start: tc.point, End: tc.point}
			got, err := r.Volume(t.Context(), metaVolumeRequest{Query: q, Step: tc.step})
			if err != nil {
				t.Fatalf("Volume: %v", err)
			}
			if len(got) != 1 || len(got[0].Buckets) != 1 {
				t.Fatalf("point volume = %v, want one entry with one bucket", got)
			}
			if want := uint64(len("at")); got[0].Buckets[0].Volume != want {
				t.Fatalf("point bucket volume = %d, want %d", got[0].Buckets[0].Volume, want)
			}
			if !got[0].Buckets[0].End.Equal(tc.point) {
				t.Fatalf("point bucket End = %s, want %s", got[0].Buckets[0].End, tc.point)
			}
		})
	}
}

// TestMetaSQLVolumePreEpoch covers the first-partial-window guard on pre-epoch
// timestamps: no bucket index goes negative, and every row is counted once.
func TestMetaSQLVolumePreEpoch(t *testing.T) {
	start := time.UnixMilli(-1500).UTC()
	end := time.UnixMilli(2500).UTC()
	at := func(ms int64, line string) plan.Row {
		return metaRow(time.UnixMilli(ms).UTC(), line, "service_name", "pre", "level", "info")
	}
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "pre", Promoted: []string{"level"},
		Rows: []plan.Row{
			at(-1499, "a"), at(-1000, "bb"), at(-1, "ccc"),
			at(0, "dddd"), at(999, "eeeee"),
			at(1000, "ffffff"), at(2000, "ggggggg"),
			at(2500, "hhhhhhhh"),
		},
	}}
	r := openMetaReader(t, files)
	q := metaQuery{Tenant: tenant.Default, Start: start, End: end}
	got, err := r.Volume(t.Context(), metaVolumeRequest{Query: q, Step: time.Second})
	if err != nil {
		t.Fatalf("Volume: %v", err)
	}
	const key = `{level="info", service_name="pre"}`
	want := map[time.Time]map[string]uint64{
		time.UnixMilli(-1).UTC(): {
			key: uint64(len("a") + len("bb") + len("ccc")),
		},
		time.UnixMilli(999).UTC(): {
			key: uint64(len("dddd") + len("eeeee")),
		},
		time.UnixMilli(1999).UTC(): {
			key: uint64(len("ffffff")),
		},
		end: {
			key: uint64(len("ggggggg") + len("hhhhhhhh")),
		},
	}
	metaAssertVolumes(t, got, want)
}

// TestMetaSQLVolumeAggregateByLabels covers aggregateBy=labels: grouping by every
// label name, raw-name tie ordering (not Metric.String(), which orders
// {"a"} after {"a0"}), per-bucket Limit trimming and target projection.
func TestMetaSQLVolumeAggregateByLabels(t *testing.T) {
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "l", Promoted: []string{"a"},
		Rows: []plan.Row{
			metaRow(plantest.T0, "xx", "a", "1"),
			metaRow(plantest.T0, "yy", "a0", "1"),
		},
	}}
	r := openMetaReader(t, files)
	q := metaWindow(tenant.Default)

	// A tie between label names "a" and "a0" must keep "a" at Limit 1: raw
	// name order, where Metric.String() would compare {a=""} after {a0=""}.
	got, err := r.Volume(t.Context(), metaVolumeRequest{Query: q, AggregateByLabels: true, Limit: 1})
	if err != nil {
		t.Fatalf("Volume aggregateBy=labels: %v", err)
	}
	if len(got) != 1 || got[0].Metric.String() != `{a=""}` {
		t.Fatalf("labels Limit 1 tie kept %v, want the raw-name first label {a=\"\"}", got)
	}

	// Without a limit both names appear, ordered by raw name on the tie.
	got, err = r.Volume(t.Context(), metaVolumeRequest{Query: q, AggregateByLabels: true})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range got {
		names = append(names, e.Metric.String())
	}
	if !slices.Equal(names, []string{`{a=""}`, `{a0=""}`}) {
		t.Fatalf("labels volume order = %v, want raw-name order", names)
	}

	// Target projection keeps only the requested names.
	projFiles := []plantest.File{{
		Tenant: tenant.Default, Stream: "p", Promoted: []string{"a"},
		Rows: []plan.Row{
			metaRow(plantest.T0, "xx", "service_name", "p", "a", "1"),
			metaRow(plantest.T0, "yy", "service_name", "p", "a0", "1"),
		},
	}}
	pr := openMetaReader(t, projFiles)
	got, err = pr.Volume(t.Context(), metaVolumeRequest{
		Query: metaWindow(tenant.Default), AggregateByLabels: true, TargetLabels: []string{"service_name"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Metric.String() != `{service_name=""}` || got[0].Buckets[0].Volume != 4 {
		t.Fatalf("labels with TargetLabels = %v, want only the service_name name with both rows' bytes", got)
	}

	// A stepped range unnesting label names into buckets: each name reports
	// only the buckets where it has volume.
	rangeFiles := []plantest.File{{
		Tenant: tenant.Default, Stream: "r", Promoted: []string{"a"},
		Rows: []plan.Row{
			metaRow(plantest.T0, "xx", "a", "1"),
			metaRow(plantest.T0.Add(time.Second), "yy", "a0", "1"),
		},
	}}
	rr := openMetaReader(t, rangeFiles)
	start := plantest.T0
	rg := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(2 * time.Second)}
	got, err = rr.Volume(t.Context(), metaVolumeRequest{Query: rg, Step: time.Second, AggregateByLabels: true})
	if err != nil {
		t.Fatal(err)
	}
	want := map[time.Time]map[string]uint64{
		start.Add(time.Second - time.Millisecond).UTC(): {`{a=""}`: 2},
		start.Add(2 * time.Second).UTC():                {`{a0=""}`: 2},
	}
	metaAssertVolumes(t, got, want)
}

// TestMetaSQLVolumeSeriesTargetLabels covers aggregateBy=series target-label
// projection and Loki's target presence matchers: a target no matcher names
// gets target=~".+" so streams missing it are excluded, while a target a
// matcher already names gets no presence filter and a missing value simply
// drops out of the projection; streams differing only outside the projection
// share a result.
func TestMetaSQLVolumeSeriesTargetLabels(t *testing.T) {
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "s", Promoted: []string{"pod"},
		Rows: []plan.Row{
			metaRow(plantest.T0, "12", "service_name", "s", "env", "prod", "pod", "p1"),
			metaRow(plantest.T0, "3", "service_name", "s", "env", "prod", "pod", "p2"),
			metaRow(plantest.T0, "45", "service_name", "s", "env", "dev", "pod", "p3"),
			metaRow(plantest.T0, "6", "job", "batch"),
		},
	}}
	r := openMetaReader(t, files)
	q := metaWindow(tenant.Default)

	got, err := r.Volume(t.Context(), metaVolumeRequest{
		Query: q, TargetLabels: []string{"service_name", "env"},
	})
	if err != nil {
		t.Fatalf("Volume projected: %v", err)
	}
	want := map[string]uint64{
		`{env="prod", service_name="s"}`: 3,
		`{env="dev", service_name="s"}`:  2,
	}
	if len(got) != len(want) {
		t.Fatalf("projected volume returned %v, want %d entries (the stream without a target must be excluded by presence)", got, len(want))
	}
	for _, e := range got {
		if len(e.Buckets) != 1 || e.Buckets[0].Volume != want[e.Metric.String()] {
			t.Fatalf("projected entry %s = %v, want volume %d", e.Metric, e.Buckets, want[e.Metric.String()])
		}
	}

	// A target already named by a matcher gets no presence filter: the matcher
	// level!="hidden" names level, so the row without level is still matched
	// and its projection simply omits the label.
	named := []plantest.File{{
		Tenant: tenant.Default, Stream: "n",
		Rows: []plan.Row{
			metaRow(plantest.T0, "a", "service_name", "n", "level", "info"),
			metaRow(plantest.T0, "bb", "service_name", "n"),
		},
	}}
	nr := openMetaReader(t, named)
	nq := metaWindow(tenant.Default, []plan.Matcher{plan.MustMatcher(plan.MatchNotEqual, "level", "hidden")})
	got, err = nr.Volume(t.Context(), metaVolumeRequest{Query: nq, TargetLabels: []string{"level"}})
	if err != nil {
		t.Fatal(err)
	}
	wantNamed := map[string]uint64{
		`{level="info"}`: 1,
		`{}`:             2,
	}
	if len(got) != len(wantNamed) {
		t.Fatalf("named-target volume = %v, want %d entries", got, len(wantNamed))
	}
	for _, e := range got {
		if len(e.Buckets) != 1 || e.Buckets[0].Volume != wantNamed[e.Metric.String()] {
			t.Fatalf("named-target entry %s = %v, want volume %d", e.Metric, e.Buckets, wantNamed[e.Metric.String()])
		}
	}

	// Per-bucket Limit: two names compete in both buckets with the winner
	// switching, so a name survives only the buckets where it stayed in the
	// top N.
	start := plantest.T0
	limited := []plantest.File{{
		Tenant: tenant.Default, Stream: "lim", Promoted: []string{"who"},
		Rows: []plan.Row{
			metaRow(start, "aaa", "who", "A"),
			metaRow(start, "b", "who", "B"),
			metaRow(start.Add(time.Second), "c", "who", "A"),
			metaRow(start.Add(time.Second), "bbb", "who", "B"),
		},
	}}
	lr := openMetaReader(t, limited)
	lq := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(2 * time.Second)}
	got, err = lr.Volume(t.Context(), metaVolumeRequest{Query: lq, Step: time.Second, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	wantLimit := map[string]time.Time{
		`{who="A"}`: start.Add(time.Second - time.Millisecond),
		`{who="B"}`: start.Add(2 * time.Second),
	}
	if len(got) != len(wantLimit) {
		t.Fatalf("limited volume = %v, want A in the first bucket and B in the second", got)
	}
	for _, e := range got {
		end, ok := wantLimit[e.Metric.String()]
		if !ok || len(e.Buckets) != 1 || !e.Buckets[0].End.Equal(end) {
			t.Fatalf("limited entry %s = %v, want exactly its winning bucket ending %s", e.Metric, e.Buckets, end)
		}
	}
}

// TestMetaSQLVolumeMatcherProjection pins the pinned-Loki default: with no
// targetLabels, series volume groups by the selector's matcher names, not by
// the full stored label set, and a matcher on an absent label still projects
// that (absent) name.
func TestMetaSQLVolumeMatcherProjection(t *testing.T) {
	start := plantest.T0
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "s", Promoted: []string{"pod"},
		Rows: []plan.Row{
			metaRow(start, "aaa", "service_name", "s", "env", "prod", "pod", "p1"),
			metaRow(start, "bb", "service_name", "s", "env", "dev", "pod", "p2"),
			metaRow(start, "c", "service_name", "s"),
		},
	}}
	r := openMetaReader(t, files)
	window := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(time.Hour)}

	// One matcher name: every stream of "s" collapses into one series, even
	// though env and pod differ.
	onlyS := window
	onlyS.Groups = [][]plan.Matcher{{metaEq("service_name", "s")}}
	got, err := r.Volume(t.Context(), metaVolumeRequest{Query: onlyS})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Metric.String() != `{service_name="s"}` || got[0].Buckets[0].Volume != 6 {
		t.Fatalf("selector-projected volume = %v, want one {service_name=\"s\"} of 6 bytes", got)
	}

	// Two matcher names: the projection keeps exactly those, in canonical
	// label order, regardless of the other stored labels.
	byEnv := window
	byEnv.Groups = [][]plan.Matcher{{metaEq("env", "prod"), metaEq("service_name", "s")}}
	got, err = r.Volume(t.Context(), metaVolumeRequest{Query: byEnv})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Metric.String() != `{env="prod", service_name="s"}` || got[0].Buckets[0].Volume != 3 {
		t.Fatalf("two-name projected volume = %v, want one {env=\"prod\", service_name=\"s\"} of 3 bytes", got)
	}

	// A matcher on an absent label projects an empty set for rows without it;
	// a present empty value is still a label entry.
	absent := []plantest.File{{
		Tenant: tenant.Default, Stream: "a",
		Rows: []plan.Row{
			metaRow(start, "x", "service_name", "a"),
			metaRow(start, "yy", "service_name", "a", "foo", ""),
			metaRow(start, "zzz", "service_name", "a", "foo", "v"),
		},
	}}
	ar := openMetaReader(t, absent)
	aq := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(time.Hour), Groups: [][]plan.Matcher{{metaEq("foo", "")}}}
	got, err = ar.Volume(t.Context(), metaVolumeRequest{Query: aq})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]uint64{`{}`: 1, `{foo=""}`: 2}
	if len(got) != len(want) {
		t.Fatalf("absent-label projected volume = %v, want %v", got, want)
	}
	for _, e := range got {
		if e.Buckets[0].Volume != want[e.Metric.String()] {
			t.Fatalf("absent-label entry %s volume = %d, want %d", e.Metric, e.Buckets[0].Volume, want[e.Metric.String()])
		}
	}
}

// TestMetaSQLVolumeLabelsSelectorKeepsAllNames pins that aggregateBy=labels
// without targetLabels groups by every label name even when a selector is
// present: the selector narrows rows, never the label names collected.
func TestMetaSQLVolumeLabelsSelectorKeepsAllNames(t *testing.T) {
	start := plantest.T0
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "api", Promoted: []string{"level"},
		Rows: []plan.Row{
			metaRow(start, "aaa", "service_name", "api", "env", "prod", "level", "info"),
			metaRow(start, "bb", "service_name", "api", "env", "dev"),
			metaRow(start, "c", "service_name", "web", "env", "prod", "level", "warn"),
		},
	}}
	r := openMetaReader(t, files)
	q := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(time.Hour)}
	q.Groups = [][]plan.Matcher{{metaEq("service_name", "api")}}

	got, err := r.Volume(t.Context(), metaVolumeRequest{Query: q, AggregateByLabels: true})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]uint64{`{service_name=""}`: 5, `{env=""}`: 5, `{level=""}`: 3}
	if len(got) != len(want) {
		t.Fatalf("labels volume with selector = %v, want every label name %v", got, want)
	}
	for _, e := range got {
		if e.Buckets[0].Volume != want[e.Metric.String()] {
			t.Fatalf("labels entry %s volume = %d, want %d", e.Metric, e.Buckets[0].Volume, want[e.Metric.String()])
		}
	}
}

// TestMetaSQLVolumeEmptyTargetElements pins the live-Loki answer for CSV
// elements that are empty: the elements are kept (not dropped), the presence
// filter for an empty name matches nothing, and the request succeeds with an
// empty result on both volume routes and both aggregate modes.
func TestMetaSQLVolumeEmptyTargetElements(t *testing.T) {
	start := plantest.T0
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "api", Promoted: []string{"level"},
		Rows: []plan.Row{
			metaRow(start, "first line", "service_name", "api", "env", "dev", "level", "info"),
			metaRow(start.Add(time.Second), "second line", "service_name", "api", "env", "prod", "level", "info"),
		},
	}}
	r := openMetaReader(t, files)
	q := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(time.Hour), Groups: [][]plan.Matcher{{metaEq("service_name", "api")}}}

	for _, tc := range []struct {
		name     string
		targets  []string
		step     time.Duration
		byLabels bool
	}{
		{"trailing empty", []string{"env", ""}, 0, false},
		{"sole empty", []string{""}, 0, false},
		{"consecutive empty", []string{"", ""}, 0, false},
		{"leading space", []string{" env"}, 0, false},
		{"trailing empty range", []string{"env", ""}, time.Second, false},
		{"trailing empty labels", []string{"env", ""}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.Volume(t.Context(), metaVolumeRequest{
				Query: q, TargetLabels: tc.targets, Step: tc.step, AggregateByLabels: tc.byLabels,
			})
			if err != nil {
				t.Fatalf("Volume(targets %q) = %v, want a successful empty result", tc.targets, err)
			}
			if len(got) != 0 {
				t.Fatalf("Volume(targets %q) = %v, want empty", tc.targets, got)
			}
		})
	}

	// Positive control: the same fixture with a valid target aggregates.
	got, err := r.Volume(t.Context(), metaVolumeRequest{Query: q, TargetLabels: []string{"env"}})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]uint64{`{env="dev"}`: uint64(len("first line")), `{env="prod"}`: uint64(len("second line"))}
	if len(got) != len(want) {
		t.Fatalf("positive control volume = %v, want %v", got, want)
	}
	for _, e := range got {
		if e.Buckets[0].Volume != want[e.Metric.String()] {
			t.Fatalf("positive control entry %s volume = %d, want %d", e.Metric, e.Buckets[0].Volume, want[e.Metric.String()])
		}
	}
}

// TestMetaSQLVolumeInvalidTargetName pins the live-Loki answer for an invalid
// UTF-8 target name: HTTP 200 with an empty result, because the presence
// matcher and projection compare label bytes that no stored series has.
func TestMetaSQLVolumeInvalidTargetName(t *testing.T) {
	start := plantest.T0
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "api", Promoted: []string{"level"},
		Rows: []plan.Row{
			metaRow(start, "first line", "service_name", "api", "env", "dev", "level", "info"),
			metaRow(start.Add(time.Second), "second line", "service_name", "api", "env", "prod", "level", "info"),
		},
	}}
	r := openMetaReader(t, files)
	q := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(time.Hour), Groups: [][]plan.Matcher{{metaEq("service_name", "api")}}}

	got, err := r.Volume(t.Context(), metaVolumeRequest{Query: q, TargetLabels: []string{"\xff"}})
	if err != nil {
		t.Fatalf("Volume(invalid target) = %v, want empty like Loki", err)
	}
	if len(got) != 0 {
		t.Fatalf("Volume(invalid target) = %v, want empty", got)
	}
	valid, err := r.Volume(t.Context(), metaVolumeRequest{Query: q, TargetLabels: []string{"env"}})
	if err != nil || len(valid) != 2 {
		t.Fatalf("positive control volume = %v, %v; want two env entries", valid, err)
	}
}

// TestMetaSQLVolumeInvalidMatcherNameProjection pins that a selector matcher
// whose name is invalid UTF-8 still yields a volume grouping: the projection
// compares bytes and no row has that label, so every row projects to the empty
// label set instead of reaching DuckDB as an un-bindable VARCHAR.
func TestMetaSQLVolumeInvalidMatcherNameProjection(t *testing.T) {
	start := plantest.T0
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "api", Promoted: []string{"level"},
		Rows: []plan.Row{
			metaRow(start, "first line", "service_name", "api", "env", "dev", "level", "info"),
			metaRow(start.Add(time.Second), "second line", "service_name", "api", "env", "prod", "level", "info"),
		},
	}}
	r := openMetaReader(t, files)
	q := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(time.Hour)}
	q.Groups = [][]plan.Matcher{{plan.MustMatcher(plan.MatchRegexp, "bad\xff", ".*")}}

	got, err := r.Volume(t.Context(), metaVolumeRequest{Query: q})
	if err != nil {
		t.Fatalf("Volume(invalid matcher name) = %v, want an empty projected map", err)
	}
	wantBytes := uint64(len("first line") + len("second line"))
	if len(got) != 1 || got[0].Metric.String() != `{}` || got[0].Buckets[0].Volume != wantBytes {
		t.Fatalf("invalid matcher-name volume = %v, want one {} entry of %d bytes", got, wantBytes)
	}

	// Positive control: a valid matcher name projects that label.
	control := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(time.Hour)}
	control.Groups = [][]plan.Matcher{{metaEq("env", "dev")}}
	got, err = r.Volume(t.Context(), metaVolumeRequest{Query: control})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Metric.String() != `{env="dev"}` {
		t.Fatalf("positive control volume = %v, want one {env=\"dev\"}", got)
	}
}

// TestMetaSQLVolumeTargetPresence covers Loki's per-arm target presence
// filters: an explicit target no matcher names requires a non-empty value,
// while a target a matcher names gets no presence filter. Loki parses its
// presence regexp with syntax.DotNL, so a newline alone is a present value.
func TestMetaSQLVolumeTargetPresence(t *testing.T) {
	start := plantest.T0
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "s",
		Rows: []plan.Row{
			metaRow(start, "aaa", "service_name", "s", "env", "prod"),
			metaRow(start, "bb", "service_name", "s"),
			metaRow(start, "c", "service_name", "s", "env", ""),
			metaRow(start, "dd", "service_name", "s", "env", "\n"),
		},
	}}
	r := openMetaReader(t, files)
	window := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(time.Hour)}

	got, err := r.Volume(t.Context(), metaVolumeRequest{Query: window, TargetLabels: []string{"env"}})
	if err != nil {
		t.Fatal(err)
	}
	// The missing and empty env rows are excluded by env=~".+", but a newline
	// value is present: the pinned dot-all semantics accept it.
	wantSeries := map[string]uint64{`{env="prod"}`: 3, `{env="\n"}`: 2}
	if len(got) != len(wantSeries) {
		t.Fatalf("presence-filtered volume = %v, want %v", got, wantSeries)
	}
	for _, e := range got {
		if e.Buckets[0].Volume != wantSeries[e.Metric.String()] {
			t.Fatalf("presence entry %s volume = %d, want %d", e.Metric, e.Buckets[0].Volume, wantSeries[e.Metric.String()])
		}
	}

	// In aggregateBy=labels mode presence applies to the row before names are
	// collected: a row with only one of two targets contributes nothing.
	got, err = r.Volume(t.Context(), metaVolumeRequest{
		Query: window, TargetLabels: []string{"env", "service_name"}, AggregateByLabels: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]uint64{`{env=""}`: 5, `{service_name=""}`: 5}
	if len(got) != len(want) {
		t.Fatalf("labels-mode presence volume = %v, want %v", got, want)
	}
	for _, e := range got {
		if e.Buckets[0].Volume != want[e.Metric.String()] {
			t.Fatalf("labels-mode entry %s volume = %d, want %d", e.Metric, e.Buckets[0].Volume, want[e.Metric.String()])
		}
	}
}

// TestMetaSQLVolumeORGrouping covers private OR-group volumes: the projection
// is the union of matcher names across arms, a match-all arm keeps full-label
// grouping, and target presence is added per arm where that arm does not name
// the target, never duplicating rows.
func TestMetaSQLVolumeORGrouping(t *testing.T) {
	start := plantest.T0
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "s",
		Rows: []plan.Row{
			metaRow(start, "aa", "service_name", "s"),
			metaRow(start, "b", "job", "batch", "env", "prod"),
			metaRow(start, "c", "service_name", "s", "env", "prod"),
		},
	}}
	r := openMetaReader(t, files)
	window := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(time.Hour)}
	window.Groups = [][]plan.Matcher{
		{metaEq("service_name", "s")},
		{metaEq("env", "prod")},
	}

	got, err := r.Volume(t.Context(), metaVolumeRequest{Query: window})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]uint64{
		`{env="prod", service_name="s"}`: 1,
		`{env="prod"}`:                   1,
		`{service_name="s"}`:             2,
	}
	if len(got) != len(want) {
		t.Fatalf("OR-group projected volume = %v, want %v", got, want)
	}
	for _, e := range got {
		if e.Buckets[0].Volume != want[e.Metric.String()] {
			t.Fatalf("OR-group entry %s volume = %d, want %d", e.Metric, e.Buckets[0].Volume, want[e.Metric.String()])
		}
	}

	// A match-all arm keeps the approved full-label grouping.
	full := window
	full.Groups = [][]plan.Matcher{{metaEq("service_name", "s")}, nil}
	shape := metaTestVolumeShapeOf(full, nil)
	if shape.projected != nil {
		t.Fatalf("match-all arm projection = %v, want full labels", shape.projected)
	}
	rows := metaMatched(files, tenant.Default, full.Start, full.End, shape.groups)
	got, err = r.Volume(t.Context(), metaVolumeRequest{Query: full})
	if err != nil {
		t.Fatal(err)
	}
	metaAssertVolumes(t, got, metavolumeWant(rows, newVolumeGrid(full.Start, full.End, 0), metaVolumeRequest{Query: full}, shape))
	if len(got) != 3 {
		t.Fatalf("full-label OR volume returned %d entries, want 3 distinct label sets", len(got))
	}

	// Presence is per arm: each arm names service_name, so env is added to
	// both arms and rows without env never match.
	presence := window
	presence.Groups = [][]plan.Matcher{{metaEq("service_name", "s")}, {metaEq("service_name", "t")}}
	pr := openMetaReader(t, []plantest.File{{
		Tenant: tenant.Default, Stream: "p",
		Rows: []plan.Row{
			metaRow(start, "aa", "service_name", "s", "env", "prod"),
			metaRow(start, "b", "service_name", "s"),
			metaRow(start, "cc", "service_name", "t", "env", "dev"),
			metaRow(start, "d", "service_name", "t"),
		},
	}})
	got, err = pr.Volume(t.Context(), metaVolumeRequest{Query: presence, TargetLabels: []string{"env"}})
	if err != nil {
		t.Fatal(err)
	}
	wantPresence := map[string]uint64{`{env="prod"}`: 2, `{env="dev"}`: 2}
	if len(got) != len(wantPresence) {
		t.Fatalf("OR presence volume = %v, want %v", got, wantPresence)
	}
	for _, e := range got {
		if e.Buckets[0].Volume != wantPresence[e.Metric.String()] {
			t.Fatalf("OR presence entry %s volume = %d, want %d", e.Metric, e.Buckets[0].Volume, wantPresence[e.Metric.String()])
		}
	}
}

// TestMetaSQLVolumeORIdenticalRows updates the superseded raw-multiplicity
// expectation: two physically identical, legitimate log entries delivered at
// least once are one stored row even when overlapping OR arms both match.
// Stats and volume count the row once. TestMetaSQLORDuplicateRowsKeepIdentity
// adds the survivors: rows differing only in metadata or full labels.
func TestMetaSQLVolumeORIdenticalRows(t *testing.T) {
	start := plantest.T0
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "dup",
		Rows: []plan.Row{
			metaRow(start, "same", "service_name", "dup", "env", "prod"),
			metaRow(start, "same", "service_name", "dup", "env", "prod"),
		},
	}}
	r := openMetaReader(t, files)
	q := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(time.Hour)}
	q.Groups = [][]plan.Matcher{{metaEq("service_name", "dup")}, {metaEq("env", "prod")}}

	stats, err := r.Stats(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Entries != 1 || stats.Bytes != 4 || stats.Streams != 1 {
		t.Fatalf("Stats over identical rows = %+v, want 1 entry, 4 bytes, 1 stream", stats)
	}
	sets, err := r.Series(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 1 {
		t.Fatalf("Series over identical rows = %v, want one distinct label set", sets)
	}
	vol, err := r.Volume(t.Context(), metaVolumeRequest{Query: q})
	if err != nil {
		t.Fatal(err)
	}
	if len(vol) != 1 || vol[0].Buckets[0].Volume != 4 {
		t.Fatalf("Volume over identical rows = %v, want one entry of 4 bytes (the exact copy counts once)", vol)
	}
}

// TestMetaSQLVolumeShapeImmutability proves presence matchers are appended to
// cloned arms: the caller's groups and their matcher slices are unchanged, and
// compiling the statement does not mutate them either.
func TestMetaSQLVolumeShapeImmutability(t *testing.T) {
	inner := []plan.Matcher{metaEq("service_name", "s")}
	before := slices.Clone(inner)
	q := metaQuery{
		Tenant: tenant.Default, Start: plantest.T0, End: plantest.T0.Add(time.Hour),
		Groups: [][]plan.Matcher{inner},
	}
	shape, err := newVolumeShape(q, []string{"env"})
	if err != nil {
		t.Fatal(err)
	}
	if len(inner) != len(before) || !slices.Equal(inner, before) {
		t.Fatalf("caller matcher slice mutated: %v, want %v", inner, before)
	}
	if len(q.Groups[0]) != len(before) {
		t.Fatalf("caller group mutated to %v", q.Groups[0])
	}
	if len(shape.matcherGroups[0]) != len(before)+1 || shape.matcherGroups[0][1].Name != "env" {
		t.Fatalf("effective arm = %v, want the selector matcher plus env presence", shape.matcherGroups[0])
	}
	// Mutating the effective arm must not reach the caller's arm.
	shape.matcherGroups[0][0] = metaEq("service_name", "changed")
	if !slices.Equal(q.Groups[0], before) {
		t.Fatalf("effective arm aliases the caller's group: %v", q.Groups[0])
	}

	// The no-targets path may reuse q.Groups, but compiling must not mutate it.
	full, err := newVolumeShape(q, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = metaVolumeSQL(metaVolumeRequest{Query: q}, newVolumeGrid(q.Start, q.End, time.Second), full,
		[]fileGroup{{paths: []string{"/a.parquet"}}})
	if !slices.Equal(q.Groups[0], before) {
		t.Fatalf("compiling mutated the caller's group: %v", q.Groups[0])
	}
}

// TestMetaSQLS3Backend is a compact smoke over the real S3 backend and cache:
// every reader method works through the object cache, tenants stay isolated
// and every lease is released.
func TestMetaSQLS3Backend(t *testing.T) {
	start := plantest.T0
	files := []plantest.File{
		{Tenant: tenant.Default, Stream: "s", Promoted: []string{"level"}, Rows: []plan.Row{
			metaRow(start, "aa", "service_name", "s", "env", "prod", "level", "info"),
			metaRow(start, "aa", "service_name", "s", "env", "prod", "level", "info"),
			metaRow(start.Add(time.Second), "b", "service_name", "s", "env", "dev"),
			metaRow(start.Add(2*time.Second), "ccc", "job", "batch"),
		}},
		{Tenant: plantest.OtherTenant, Stream: "s", Rows: []plan.Row{
			metaRow(start, "other", "service_name", "s", "env", "prod"),
		}},
	}
	env := newBackendEnv(t, backendS3, files)
	r := newMetadataReader(env.src)
	q := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(time.Hour), Groups: [][]plan.Matcher{{metaEq("service_name", "s")}}}
	rows := metaMatched(files, tenant.Default, q.Start, q.End, q.Groups)

	names, err := r.LabelNames(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names, metaRefNames(rows)) {
		t.Fatalf("S3 LabelNames = %v, want %v", names, metaRefNames(rows))
	}
	values, err := r.LabelValues(t.Context(), q, "env")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(values, metaRefValues(rows, "env")) {
		t.Fatalf("S3 LabelValues = %v, want %v", values, metaRefValues(rows, "env"))
	}
	sets, err := r.Series(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 2 {
		t.Fatalf("S3 Series = %v, want the two matching label sets", sets)
	}
	stats, err := r.Stats(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	wantStats := metaRefStats(rows)
	if stats.Entries != wantStats.Entries || stats.Bytes != wantStats.Bytes || stats.Chunks != 1 {
		t.Fatalf("S3 Stats = %+v, want %+v with 1 chunk", stats, wantStats)
	}
	vol, err := r.Volume(t.Context(), metaVolumeRequest{Query: q})
	if err != nil {
		t.Fatal(err)
	}
	if len(vol) != 1 || vol[0].Metric.String() != `{service_name="s"}` || vol[0].Buckets[0].Volume != wantStats.Bytes {
		t.Fatalf("S3 Volume = %v, want one {service_name=\"s\"} of %d bytes", vol, wantStats.Bytes)
	}

	other := metaQuery{Tenant: plantest.OtherTenant, Start: q.Start, End: q.End}
	otherStats, err := r.Stats(t.Context(), other)
	if err != nil {
		t.Fatal(err)
	}
	if otherStats.Entries != 1 || otherStats.Chunks != 1 {
		t.Fatalf("S3 other-tenant Stats = %+v, want its own single row", otherStats)
	}
	if leaks := env.res.liveLeases(); leaks != 0 {
		t.Fatalf("%d S3 leases still held after metadata reads", leaks)
	}
}

// TestMetaSQLVolumePlaceholders pins that the volume statement's placeholders and
// arguments stay in lockstep for every request shape. The draft wrote literal
// '?' next to compiler.bind (which prints its own), so this is the direct
// regression for that class of bug.
func TestMetaSQLVolumePlaceholders(t *testing.T) {
	groups := []fileGroup{{promoted: []string{"level"}, paths: []string{"/a.parquet", "/b.parquet"}}}
	start, end := plantest.T0, plantest.T0.Add(3*time.Second)
	base := metaQuery{Tenant: tenant.Default, Start: start, End: end}
	selected := metaQuery{Tenant: tenant.Default, Start: start, End: end, Groups: [][]plan.Matcher{{metaEq("service_name", "api")}}}
	grid := newVolumeGrid(start, end, time.Second)
	instant := newVolumeGrid(start, start, 0)
	for _, tc := range []struct {
		name string
		req  metaVolumeRequest
		grid volumeGrid
	}{
		{"series instant", metaVolumeRequest{Query: base}, instant},
		{"series range", metaVolumeRequest{Query: base}, grid},
		{"series matcher projection", metaVolumeRequest{Query: selected}, grid},
		{"series targets", metaVolumeRequest{Query: base, TargetLabels: []string{"level"}}, grid},
		{"series targets present", metaVolumeRequest{Query: selected, TargetLabels: []string{"level"}}, grid},
		{"labels range", metaVolumeRequest{Query: base, AggregateByLabels: true}, grid},
		{"labels targets", metaVolumeRequest{Query: base, AggregateByLabels: true, TargetLabels: []string{"level"}}, grid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shape, err := newVolumeShape(tc.req.Query, tc.req.TargetLabels)
			if err != nil {
				t.Fatalf("newVolumeShape: %v", err)
			}
			q := metaVolumeSQL(tc.req, tc.grid, shape, groups)
			if n, want := strings.Count(q.sql, "?"), len(q.args); n != want {
				t.Fatalf("%d placeholders but %d bound args in:\n%s", n, want, q.sql)
			}
		})
	}

	// A stepped range binds exactly its three bucket bounds more than the
	// instant shape over the same selector.
	full, err := newVolumeShape(base, nil)
	if err != nil {
		t.Fatal(err)
	}
	instantArgs := len(metaVolumeSQL(metaVolumeRequest{Query: base}, instant, full, groups).args)
	rangeArgs := len(metaVolumeSQL(metaVolumeRequest{Query: base}, grid, full, groups).args)
	if rangeArgs != instantArgs+3 {
		t.Fatalf("range shape binds %d args, instant %d, want exactly 3 more", rangeArgs, instantArgs)
	}
}

// TestMetaSQLVolumeGrid unit-tests the window arithmetic directly: epoch-aligned
// steps, the Start==End point bucket, and the pre-epoch truncation quirk.
func TestMetaSQLVolumeGrid(t *testing.T) {
	for _, tc := range []struct {
		name         string
		start, end   time.Time
		step         time.Duration
		wantInstant  bool
		wantAligned  int64
		wantCount    int64
		wantLastEnd  time.Time
		wantFirstEnd time.Time
	}{
		{
			name: "aligned", start: plantest.T0, end: plantest.T0.Add(3 * time.Second),
			step: time.Second, wantAligned: plantest.T0.UnixMilli(), wantCount: 3,
			wantFirstEnd: plantest.T0.Add(time.Second - time.Millisecond),
			wantLastEnd:  plantest.T0.Add(3 * time.Second),
		},
		{
			name: "partial first window", start: plantest.T0.Add(250 * time.Millisecond), end: plantest.T0.Add(3 * time.Second),
			step: time.Second, wantAligned: plantest.T0.UnixMilli(), wantCount: 3,
			wantFirstEnd: plantest.T0.Add(time.Second - time.Millisecond),
			wantLastEnd:  plantest.T0.Add(3 * time.Second),
		},
		{
			name: "start equals end", start: plantest.T0, end: plantest.T0,
			step: time.Second, wantAligned: plantest.T0.UnixMilli(), wantCount: 1,
			wantFirstEnd: plantest.T0, wantLastEnd: plantest.T0,
		},
		{
			name: "instant", start: plantest.T0, end: plantest.T0.Add(3 * time.Second),
			step: 0, wantInstant: true, wantCount: 1,
			wantFirstEnd: plantest.T0.Add(3 * time.Second), wantLastEnd: plantest.T0.Add(3 * time.Second),
		},
		{
			name: "sub-millisecond step truncates to instant", start: plantest.T0, end: plantest.T0.Add(3 * time.Second),
			step: 500 * time.Microsecond, wantInstant: true, wantCount: 1,
			wantFirstEnd: plantest.T0.Add(3 * time.Second), wantLastEnd: plantest.T0.Add(3 * time.Second),
		},
		{
			name: "pre-epoch truncates toward zero", start: time.UnixMilli(-1500).UTC(), end: time.UnixMilli(2500).UTC(),
			step: time.Second, wantAligned: -1000, wantCount: 4,
			wantFirstEnd: time.UnixMilli(-1).UTC(),
			wantLastEnd:  time.UnixMilli(2500).UTC(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newVolumeGrid(tc.start, tc.end, tc.step)
			if g.instant != tc.wantInstant || g.aligned != tc.wantAligned || g.count != tc.wantCount {
				t.Fatalf("grid = %+v, want instant=%v aligned=%d count=%d", g, tc.wantInstant, tc.wantAligned, tc.wantCount)
			}
			if got := g.sampleEnd(0, tc.end); !got.Equal(tc.wantFirstEnd) {
				t.Fatalf("first sample End = %s, want %s", got, tc.wantFirstEnd)
			}
			if got := g.sampleEnd(g.count-1, tc.end); !got.Equal(tc.wantLastEnd) {
				t.Fatalf("last sample End = %s, want %s", got, tc.wantLastEnd)
			}
		})
	}
}

// TestMetaSQLListingSelection pins the catalog listing choice: tenant and the
// inclusive-end window always, matchers only where every OR group proves the
// same dataset.
func TestMetaSQLListingSelection(t *testing.T) {
	env, r := openMetaEnv(t, plantest.Dataset())
	q := metaWindow(tenant.Default, []plan.Matcher{metaEq("service_name", "api")})
	if _, err := r.LabelNames(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	f := env.cat.lastFilter()
	if f.Tenant != tenant.Default || !f.From.Equal(q.Start) || !f.Through.Equal(q.End.Add(time.Nanosecond)) {
		t.Fatalf("listing filter = %+v, want tenant/start/end+1ns", f)
	}
	if f.Stream != "api" {
		t.Fatalf("listing stream = %q, want api (sole selector forces it)", f.Stream)
	}

	sameStream := q
	sameStream.Groups = [][]plan.Matcher{{metaEq("service_name", "api")}, {metaEq("service_name", "api"), metaEq("env", "prod")}}
	if _, err := r.LabelNames(t.Context(), sameStream); err != nil {
		t.Fatal(err)
	}
	if f := env.cat.lastFilter(); f.Stream != "api" {
		t.Fatalf("listing stream for same-stream groups = %q, want api", f.Stream)
	}

	mixed := q
	mixed.Groups = [][]plan.Matcher{{metaEq("service_name", "api")}, {metaEq("service_name", "web")}}
	if _, err := r.LabelNames(t.Context(), mixed); err != nil {
		t.Fatal(err)
	}
	if f := env.cat.lastFilter(); f.Stream != "" {
		t.Fatalf("listing stream for mixed groups = %q, want unpruned", f.Stream)
	}

	if _, err := r.LabelNames(t.Context(), metaWindow(tenant.Default)); err != nil {
		t.Fatal(err)
	}
	if f := env.cat.lastFilter(); f.Stream != "" {
		t.Fatalf("listing stream without selectors = %q, want unpruned", f.Stream)
	}
}

// TestMetaSQLRetryAfterReplacement proves the metadata reader goes through the
// shared attempt boundary: a file replaced between listing and first row is
// read again from a fresh listing, with no partial answer and no held lease.
func TestMetaSQLRetryAfterReplacement(t *testing.T) {
	files := retryFiles()
	env := newBackendEnv(t, backendFS, files)
	oldKey := env.files[0].Key
	_, replCatalog := storeFixture(t, env.store, replacementFile())
	env.cat.setHook(func() {
		if err := env.store.Delete(t.Context(), oldKey); err != nil {
			t.Fatalf("delete replaced object: %v", err)
		}
		if err := env.cat.Catalog.ReplaceFiles(t.Context(), tenant.Default, []string{oldKey}, []catalog.File{replCatalog}); err != nil {
			t.Fatalf("replace catalog row: %v", err)
		}
	})

	r := newMetadataReader(env.src)
	q := metaQuery{Tenant: tenant.Default, Start: plantest.T0.Add(-time.Hour), End: plantest.T0.Add(time.Hour)}
	got, err := r.Series(t.Context(), q)
	if err != nil {
		t.Fatalf("Series after replacement: %v", err)
	}
	want := metaRefSeries(metaMatched([]plantest.File{files[1], replacementFile()}, tenant.Default, q.Start, q.End, nil))
	if len(got) != len(want) {
		t.Fatalf("Series returned %d sets, want %d (stale file must not survive)", len(got), len(want))
	}
	for i := range want {
		if got[i].String() != want[i].String() {
			t.Fatalf("Series[%d] = %s, want %s", i, got[i], want[i])
		}
	}
	if n := env.cat.listings(); n != 2 {
		t.Fatalf("catalog listings = %d, want 2 (one stale plus one fresh)", n)
	}
	if leaks := env.res.liveLeases(); leaks != 0 {
		t.Fatalf("%d leases still held after a successful retry", leaks)
	}
}

// TestMetaSQLEmptyStoreEveryEndpoint pins empty answers (not errors, not
// invented zeros) over a store with no listed files.
func TestMetaSQLEmptyStoreEveryEndpoint(t *testing.T) {
	r := openMetaReader(t, nil)
	q := metaWindow(tenant.Default)
	if got, err := r.LabelNames(t.Context(), q); err != nil || len(got) != 0 {
		t.Fatalf("LabelNames = %v, %v; want empty", got, err)
	}
	if got, err := r.LabelValues(t.Context(), q, "any"); err != nil || len(got) != 0 {
		t.Fatalf("LabelValues = %v, %v; want empty", got, err)
	}
	if got, err := r.Series(t.Context(), q); err != nil || len(got) != 0 {
		t.Fatalf("Series = %v, %v; want empty", got, err)
	}
	if got, err := r.Volume(t.Context(), metaVolumeRequest{Query: q, Step: time.Second}); err != nil || len(got) != 0 {
		t.Fatalf("Volume = %v, %v; want empty", got, err)
	}
}

// TestMetaSQLMergedMapFallback regresses compaction's additive schema union:
// a merged file keeps rows whose promoted column is NULL and whose label lives
// in the map, so metadata must recover that value through the map fallback
// rather than trusting the file's promoted summary.
func TestMetaSQLMergedMapFallback(t *testing.T) {
	base := plantest.T0
	colFile := plantest.File{Tenant: tenant.Default, Stream: "merged", Promoted: []string{"level"}, Rows: []plan.Row{
		metaRow(base, "column row", "service_name", "merged", "level", "info"),
	}}
	mapFile := plantest.File{Tenant: tenant.Default, Stream: "merged", Rows: []plan.Row{
		metaRow(base.Add(time.Second), "map row", "service_name", "merged", "level", "warn"),
	}}
	var readers []*storage.Reader
	for _, f := range []plantest.File{colFile, mapFile} {
		data, _, err := plantest.Encode(f)
		if err != nil {
			t.Fatal(err)
		}
		rd, err := storage.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { rd.Close() })
		readers = append(readers, rd)
	}
	var merged bytes.Buffer
	st, err := storage.Merge(t.Context(), &merged, readers, []string{"level"})
	if err != nil {
		t.Fatal(err)
	}

	env := newBackendEnv(t, backendFS, nil)
	key := objstore.DataKey(tenant.Default, "merged", st.MinTS, uuid.NewV7())
	if err := env.store.Put(t.Context(), key, bytes.NewReader(merged.Bytes()), int64(merged.Len())); err != nil {
		t.Fatalf("put merged object: %v", err)
	}
	if err := env.cat.AddFiles(t.Context(), catalog.File{
		Tenant: tenant.Default, Stream: "merged", Key: key,
		MinTS: st.MinTS, MaxTS: st.MaxTS, Rows: st.Rows, Bytes: int64(merged.Len()),
		LabelValues: st.LabelValues, CreatedAt: time.Now().Truncate(time.Microsecond),
	}); err != nil {
		t.Fatal(err)
	}
	r := newMetadataReader(env.src)
	q := metaWindow(tenant.Default)

	values, err := r.LabelValues(t.Context(), q, "level")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(values, []string{"info", "warn"}) {
		t.Fatalf("LabelValues(level) over a merged file = %v, want both column and map values", values)
	}
	sets, err := r.Series(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 2 {
		t.Fatalf("Series over a merged file = %v, want both label sets", sets)
	}

	warnOnly := metaWindow(tenant.Default, []plan.Matcher{metaEq("level", "warn")})
	vol, err := r.Volume(t.Context(), metaVolumeRequest{Query: warnOnly})
	if err != nil {
		t.Fatal(err)
	}
	if len(vol) != 1 || len(vol[0].Buckets) != 1 || vol[0].Buckets[0].Volume != uint64(len("map row")) {
		t.Fatalf("Volume over the map-fallback row = %v, want one entry with its bytes", vol)
	}
}

// TestMetaSQLCancellation reports cancellation instead of a partial answer.
func TestMetaSQLCancellation(t *testing.T) {
	r := openMetaReader(t, plantest.Dataset())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := r.Series(ctx, metaWindow(tenant.Default)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Series on a canceled context = %v, want context.Canceled", err)
	}
	if _, err := r.Stats(ctx, metaWindow(tenant.Default)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stats on a canceled context = %v, want context.Canceled", err)
	}
}

// metaTestVolumeShape is the independent oracle for the pinned volume
// grouping: the effective OR arms (selector matchers plus target presence) and
// the series projection names (nil keeps the full label set).
type metaTestVolumeShape struct {
	groups       [][]plan.Matcher
	projected    []string
	labelsFilter []string
}

// metaTestVolumeShapeOf applies Loki's PrepareLabelsAndMatchers rules: with
// targets the projection is those names and each arm gains a target=~".+"
// presence matcher unless a matcher in that arm already names the target;
// without targets the projection is the union of matcher names, keeping full
// labels for a selector-free or match-all request.
func metaTestVolumeShapeOf(q metaQuery, targets []string) metaTestVolumeShape {
	targets = slices.Compact(slices.Sorted(slices.Values(targets)))
	named := func(ms []plan.Matcher, name string) bool {
		return slices.ContainsFunc(ms, func(m plan.Matcher) bool { return m.Name == name })
	}
	addPresence := func(group []plan.Matcher) []plan.Matcher {
		augmented := slices.Clone(group)
		for _, target := range targets {
			if !named(group, target) {
				// Loki's DotNL-parsed =~".+" accepts newlines too.
				augmented = append(augmented, plan.MustMatcher(plan.MatchRegexp, target, "(?s:.+)"))
			}
		}
		return augmented
	}
	if len(targets) > 0 {
		shape := metaTestVolumeShape{projected: targets, labelsFilter: targets}
		if len(q.Groups) == 0 {
			shape.groups = [][]plan.Matcher{addPresence(nil)}
			return shape
		}
		for _, group := range q.Groups {
			shape.groups = append(shape.groups, addPresence(group))
		}
		return shape
	}
	shape := metaTestVolumeShape{groups: q.Groups}
	if len(q.Groups) == 0 {
		return shape
	}
	var names []string
	for _, group := range q.Groups {
		if len(group) == 0 {
			return shape // a match-all arm keeps full-label grouping
		}
		for _, m := range group {
			names = append(names, m.Name)
		}
	}
	shape.projected = slices.Compact(slices.Sorted(slices.Values(names)))
	return shape
}

// metavolumeWant is the reference per-bucket totals: the continuous window
// partition (bucket 0 begins at Start, later buckets at whole boundaries, the
// final one includes End), the request's grouping, the per-bucket Limit and
// Loki's sample timestamps. Result names are the metric strings (that is what
// the reader returns); keys of the map are sample Ends in UTC.
func metavolumeWant(rows []plan.Row, g volumeGrid, req metaVolumeRequest, shape metaTestVolumeShape) map[time.Time]map[string]uint64 {
	type agg struct {
		name   string
		metric logs.Labels
		volume uint64
	}
	byBucket := make(map[int64][]agg)
	index := make(map[int64]map[string]int)
	bucketOf := func(ts time.Time) int64 {
		if g.instant {
			return 0
		}
		us := ts.UnixMicro()
		for i := int64(1); i < g.count; i++ {
			if us < (g.aligned+i*g.step)*1000 {
				return i - 1
			}
		}
		return g.count - 1
	}
	for _, r := range rows {
		idx := bucketOf(r.TS)
		for _, group := range metavolumeGroups(r, req, shape) {
			if index[idx] == nil {
				index[idx] = make(map[string]int)
			}
			if at, ok := index[idx][group.name]; ok {
				byBucket[idx][at].volume += uint64(len(r.Line))
				continue
			}
			index[idx][group.name] = len(byBucket[idx])
			byBucket[idx] = append(byBucket[idx], agg{
				name: group.name, metric: group.metric, volume: uint64(len(r.Line)),
			})
		}
	}
	out := make(map[time.Time]map[string]uint64)
	for idx, aggs := range byBucket {
		slices.SortFunc(aggs, func(a, b agg) int {
			if c := cmp.Compare(b.volume, a.volume); c != 0 {
				return c
			}
			return strings.Compare(a.name, b.name)
		})
		if req.Limit > 0 && len(aggs) > req.Limit {
			aggs = aggs[:req.Limit]
		}
		end := g.sampleEnd(idx, req.Query.End).UTC()
		if out[end] == nil {
			out[end] = make(map[string]uint64)
		}
		for _, a := range aggs {
			out[end][a.metric.String()] = a.volume
		}
	}
	return out
}

// metaVolGroup is one grouping key a row contributes to, named the way the
// reader names its results.
type metaVolGroup struct {
	name   string
	metric logs.Labels
}

// metavolumeGroups returns the grouping keys a row contributes to: the raw
// label name for aggregateBy=labels (a raw name sorts differently from its
// {name=""} metric string), the canonical projected labelset otherwise.
func metavolumeGroups(r plan.Row, req metaVolumeRequest, shape metaTestVolumeShape) []metaVolGroup {
	if req.AggregateByLabels {
		var out []metaVolGroup
		for _, l := range r.Labels {
			if shape.labelsFilter != nil && !slices.Contains(shape.labelsFilter, l.Name) {
				continue
			}
			out = append(out, metaVolGroup{name: l.Name, metric: logs.FromMap(map[string]string{l.Name: ""})})
		}
		return out
	}
	m := r.Labels.Map()
	if shape.projected != nil {
		projected := make(map[string]string, len(shape.projected))
		for _, name := range shape.projected {
			if v, ok := m[name]; ok {
				projected[name] = v
			}
		}
		m = projected
	}
	ls := logs.FromMap(m)
	return []metaVolGroup{{name: ls.String(), metric: ls}}
}

// metaAssertVolumes compares the reader's buckets with the reference map keyed
// by sample End (UTC) and result name.
func metaAssertVolumes(t *testing.T, got []metaVolumeEntry, want map[time.Time]map[string]uint64) {
	t.Helper()
	flat := make(map[time.Time]map[string]uint64)
	for _, e := range got {
		for _, s := range e.Buckets {
			end := s.End.UTC()
			if flat[end] == nil {
				flat[end] = make(map[string]uint64)
			}
			flat[end][e.Metric.String()] = s.Volume
		}
	}
	if len(flat) != len(want) {
		t.Fatalf("volume buckets = %v, want %v", flat, want)
	}
	for end, wantNames := range want {
		gotNames, ok := flat[end]
		if !ok {
			t.Fatalf("missing volume bucket ending %s (got %v)", end, flat)
		}
		if len(gotNames) != len(wantNames) {
			t.Fatalf("bucket %s = %v, want %v", end, gotNames, wantNames)
		}
		for name, wantVol := range wantNames {
			if gotNames[name] != wantVol {
				t.Fatalf("bucket %s name %s volume = %d, want %d", end, name, gotNames[name], wantVol)
			}
		}
	}
}

// metaAssertVolumeOrder checks the result order: first surviving bucket volume
// descending, then the result's canonical name ascending.
func metaAssertVolumeOrder(t *testing.T, entries []metaVolumeEntry) {
	t.Helper()
	for i := 1; i < len(entries); i++ {
		a, b := entries[i-1], entries[i]
		if len(a.Buckets) == 0 || len(b.Buckets) == 0 {
			t.Fatalf("entry without buckets in %v", entries)
		}
		if a.Buckets[0].Volume < b.Buckets[0].Volume ||
			a.Buckets[0].Volume == b.Buckets[0].Volume && a.Metric.String() > b.Metric.String() {
			t.Fatalf("volume order broken at %d: %s then %s", i, a.Metric, b.Metric)
		}
	}
}
