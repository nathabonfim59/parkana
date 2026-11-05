// Metadata identity for bucket aggregation (the milestone-1 IncludeMeta
// amendment): with BucketAgg.IncludeMeta, logically equal structured
// metadata must form one bucket whatever key order the Parquet map stored,
// distinct metadata must split, and an absent or empty map is the empty set
// while a present empty value is a value. The disabled path must keep
// merging by stream labels only and leave Bucket.Meta nil.
package query

import (
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// metaAggregateFixtures returns the same rows twice: physically, with the
// shared metadata written in three different map orders across three
// promotion layouts (promoted level, all-map, promoted env), and logically,
// with every Labels and Meta sorted for the MemSource oracle. One row has no
// metadata, one an empty map, one a distinct second trace id, and one a
// present empty trace id.
func metaAggregateFixtures() (physical, logical []plantest.File) {
	base := plantest.T0
	labels := func() logs.Labels {
		return logs.FromMap(map[string]string{"service_name": "s", "env": "prod", "level": "info"})
	}
	shared := map[string]string{"region": "r1", "trace_id": "t1", "zone": "z1"}
	other := map[string]string{"region": "r1", "trace_id": "t2", "zone": "z1"}
	emptyValue := map[string]string{"trace_id": ""}
	// Same logical metadata, three physical key orders. logs.Labels is
	// normally sorted; these literals deliberately are not, so the writer
	// stores each order verbatim and the raw meta_json differs per file.
	orderZoneRegionTrace := logs.Labels{
		{Name: "zone", Value: "z1"},
		{Name: "region", Value: "r1"},
		{Name: "trace_id", Value: "t1"},
	}
	orderTraceZoneRegion := logs.Labels{
		{Name: "trace_id", Value: "t1"},
		{Name: "zone", Value: "z1"},
		{Name: "region", Value: "r1"},
	}
	orderRegionTraceZone := logs.Labels{
		{Name: "region", Value: "r1"},
		{Name: "trace_id", Value: "t1"},
		{Name: "zone", Value: "z1"},
	}
	physical = []plantest.File{
		{Tenant: tenant.Default, Stream: "s", Promoted: []string{"level"}, Rows: []plan.Row{
			{TS: base, Labels: labels(), Meta: orderZoneRegionTrace, Line: "order-a"},
			{TS: base.Add(time.Second), Labels: labels(), Meta: logs.FromMap(other), Line: "distinct-meta"},
		}},
		{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{
			{TS: base.Add(2 * time.Second), Labels: labels(), Meta: orderTraceZoneRegion, Line: "order-b"},
			{TS: base.Add(3 * time.Second), Labels: labels(), Meta: nil, Line: "absent"},
		}},
		{Tenant: tenant.Default, Stream: "s", Promoted: []string{"env"}, Rows: []plan.Row{
			{TS: base.Add(4 * time.Second), Labels: labels(), Meta: orderRegionTraceZone, Line: "order-c"},
			{TS: base.Add(5 * time.Second), Labels: labels(), Meta: logs.Labels{}, Line: "empty-map"},
			{TS: base.Add(6 * time.Second), Labels: labels(), Meta: logs.FromMap(emptyValue), Line: "empty-value"},
		}},
	}
	logical = []plantest.File{
		{Tenant: tenant.Default, Stream: "s", Promoted: []string{"level"}, Rows: []plan.Row{
			{TS: base, Labels: labels(), Meta: logs.FromMap(shared), Line: "order-a"},
			{TS: base.Add(time.Second), Labels: labels(), Meta: logs.FromMap(other), Line: "distinct-meta"},
		}},
		{Tenant: tenant.Default, Stream: "s", Rows: []plan.Row{
			{TS: base.Add(2 * time.Second), Labels: labels(), Meta: logs.FromMap(shared), Line: "order-b"},
			{TS: base.Add(3 * time.Second), Labels: labels(), Meta: nil, Line: "absent"},
		}},
		{Tenant: tenant.Default, Stream: "s", Promoted: []string{"env"}, Rows: []plan.Row{
			{TS: base.Add(4 * time.Second), Labels: labels(), Meta: logs.FromMap(shared), Line: "order-c"},
			{TS: base.Add(5 * time.Second), Labels: labels(), Meta: logs.Labels{}, Line: "empty-map"},
			{TS: base.Add(6 * time.Second), Labels: labels(), Meta: logs.FromMap(emptyValue), Line: "empty-value"},
		}},
	}
	return physical, logical
}

// metaAggregateSelection matches every fixture row in one minute bucket.
func metaAggregateSelection() plan.Selection {
	base := plantest.T0
	return plan.Selection{
		Tenant:   tenant.Default,
		Matchers: []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "service_name", "s")},
		From:     base.Add(-time.Hour),
		Through:  base.Add(time.Hour),
	}
}

// metaBucketKey includes every field of a bucket, Meta included, and pins
// starts with seconds plus nanoseconds so far or sub-µs starts cannot wrap
// or hide.
func metaBucketKey(b plan.Bucket) string {
	return fmt.Sprintf("%d.%09d|%s|%s|%d|%d", b.Start.Unix(), b.Start.Nanosecond(),
		b.Labels.String(), b.Meta.String(), b.Count, b.Bytes)
}

// sortedMeta reports whether ls is strictly sorted by label name.
func sortedMeta(ls logs.Labels) bool {
	for i := 1; i < len(ls); i++ {
		if ls[i-1].Name >= ls[i].Name {
			return false
		}
	}
	return true
}

// TestAggregateMetaSQLBranch pins the compiled statement shapes and arities
// for both IncludeMeta values through the existing aggregateSQL entrypoint:
// disabled stays the four-column GROUP BY 1, 2 statement, enabled adds the
// canonical metadata projection and groups by it as position 3. No new
// bind is introduced either way, and real DuckDB returns four or five
// columns respectively.
func TestAggregateMetaSQLBranch(t *testing.T) {
	physical, _ := metaAggregateFixtures()
	env := openSourceEnv(t, physical, defaultStreamLabels)
	paths := make([]string, len(env.files))
	for i, f := range env.files {
		paths[i] = env.path(f.Key)
	}
	groups := groupFiles(env.files, paths)
	sel := metaAggregateSelection()
	base := plantest.T0

	off := aggregateSQL(sel, plan.BucketAgg{Width: time.Minute, Origin: base}, groups)
	on := aggregateSQL(sel, plan.BucketAgg{Width: time.Minute, Origin: base, IncludeMeta: true}, groups)

	if !strings.HasSuffix(off.sql, ")) GROUP BY 1, 2") {
		t.Errorf("disabled statement does not end with GROUP BY 1, 2: %s", off.sql)
	}
	if strings.Contains(off.sql, "GROUP BY 1, 2, 3") {
		t.Errorf("disabled statement grouped by metadata: %s", off.sql)
	}
	if !strings.HasSuffix(on.sql, ")) GROUP BY 1, 2, 3") {
		t.Errorf("enabled statement does not end with GROUP BY 1, 2, 3: %s", on.sql)
	}
	if !strings.Contains(on.sql, "labels_json, meta_json, count(*)") {
		t.Errorf("enabled statement does not return meta_json before the aggregates: %s", on.sql)
	}
	// The identity metadata is canonicalized inside every group from the
	// stored MAP; TestResidualIdentityDirectMapSQL pins that shape (no
	// JSON read-back).
	for _, want := range []string{
		"list_sort(map_entries(coalesce(",
		"map([], [])",
	} {
		if !strings.Contains(on.sql, want) {
			t.Errorf("enabled statement is missing %q: %s", want, on.sql)
		}
	}
	for name, q := range map[string]selectQuery{"disabled": off, "enabled": on} {
		if n, args := strings.Count(q.sql, "?"), len(q.args); n != args {
			t.Errorf("%s: %d placeholders for %d arguments: %s", name, n, args, q.sql)
		}
	}

	for _, tc := range []struct {
		name string
		q    selectQuery
		cols int
	}{
		{name: "disabled", q: off, cols: 4},
		{name: "enabled", q: on, cols: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := env.source.db.QueryContext(t.Context(), tc.q.sql, tc.q.args...)
			if err != nil {
				t.Fatalf("execute: %v\nSQL: %s", err, tc.q.sql)
			}
			defer rows.Close()
			cols, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			if len(cols) != tc.cols {
				t.Fatalf("%d columns, want %d: %v", len(cols), tc.cols, cols)
			}
			for rows.Next() {
				var k, count, bytes int64
				var labelsJSON, metaJSON sql.NullString
				dest := []any{&k, &labelsJSON}
				if tc.cols == 5 {
					dest = append(dest, &metaJSON)
				}
				dest = append(dest, &count, &bytes)
				if err := rows.Scan(dest...); err != nil {
					t.Fatalf("scan %d columns: %v", tc.cols, err)
				}
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestAggregateMetaRawKeyOrderIsPhysical proves the range this test covers
// is real: compiling the raw normRelation directly (before the row-identity
// DISTINCT wrapper) keeps the stored map order, so logically equal metadata
// from the three promotion layouts has three different raw strings before
// canonicalization.
func TestAggregateMetaRawKeyOrderIsPhysical(t *testing.T) {
	physical, _ := metaAggregateFixtures()
	env := openSourceEnv(t, physical, defaultStreamLabels)
	paths := make([]string, len(env.files))
	for i, f := range env.files {
		paths[i] = env.path(f.Key)
	}
	c := compiler{}
	normRelation(&c, metaAggregateSelection(), groupFiles(env.files, paths))
	q := selectQuery{sql: c.b.String(), args: c.args}
	rows, err := env.source.db.QueryContext(t.Context(), q.sql, q.args...)
	if err != nil {
		t.Fatalf("execute: %v\nSQL: %s", err, q.sql)
	}
	defer rows.Close()
	raw := make(map[string]string)
	for rows.Next() {
		var ts time.Time
		var line string
		var labelsJSON, metaJSON sql.NullString
		if err := rows.Scan(&ts, &line, &labelsJSON, &metaJSON); err != nil {
			t.Fatal(err)
		}
		raw[line] = metaJSON.String
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	lines := []string{"order-a", "order-b", "order-c"}
	decoded := make([]map[string]string, len(lines))
	for i, line := range lines {
		s, ok := raw[line]
		if !ok {
			t.Fatalf("no raw meta_json for %q; got %v", line, maps.Keys(raw))
		}
		m, err := decodeMap(sql.NullString{String: s, Valid: true})
		if err != nil {
			t.Fatalf("decode raw meta_json %q: %v", s, err)
		}
		decoded[i] = m
	}
	for i := range lines {
		for j := i + 1; j < len(lines); j++ {
			if raw[lines[i]] == raw[lines[j]] {
				t.Fatalf("raw meta_json for %q and %q is the same %q; the fixture must store different physical key orders",
					lines[i], lines[j], raw[lines[i]])
			}
			if !maps.Equal(decoded[i], decoded[j]) {
				t.Fatalf("metadata for %q and %q is not logically equal: %v vs %v", lines[i], lines[j], decoded[i], decoded[j])
			}
		}
	}
}

// TestAggregateMetaIdentity drives the PUBLIC source.Aggregate against the
// MemSource oracle with exact start/labels/meta/count/bytes identity. With
// IncludeMeta, the three physical key orders and the absent/empty rows
// collapse into the empty-set or shared-set bucket, a distinct trace id and
// a present empty value stay separate, and Meta is sorted. With it disabled
// every row merges into one label-only bucket and Meta is nil.
func TestAggregateMetaIdentity(t *testing.T) {
	physical, logical := metaAggregateFixtures()
	env := openSourceEnv(t, physical, defaultStreamLabels)
	ref := plantest.Mem(logical)
	sel := metaAggregateSelection()
	base := plantest.T0

	keys := func(bs []plan.Bucket) []string {
		out := make([]string, len(bs))
		for i, b := range bs {
			out[i] = metaBucketKey(b)
			if b.Start.Location() != time.UTC {
				t.Fatalf("bucket start %s is not UTC", b.Start)
			}
		}
		slices.Sort(out)
		return out
	}
	compare := func(t *testing.T, agg plan.BucketAgg) []plan.Bucket {
		t.Helper()
		got, err := collectBuckets(env.source.Aggregate(t.Context(), sel, agg))
		if err != nil {
			t.Fatalf("source.Aggregate: %v", err)
		}
		want, err := collectBuckets(ref.Aggregate(t.Context(), sel, agg))
		if err != nil {
			t.Fatalf("reference Aggregate: %v", err)
		}
		g, w := keys(got), keys(want)
		if !slices.Equal(g, w) {
			t.Fatalf("buckets differ:\n got  %v\n want %v", g, w)
		}
		return got
	}

	t.Run("include-meta", func(t *testing.T) {
		agg := plan.BucketAgg{Width: time.Minute, Origin: base, IncludeMeta: true}
		got := compare(t, agg)
		if len(got) != 4 {
			t.Fatalf("%d buckets, want 4 (shared, distinct trace id, empty value, empty set): %v", len(got), keys(got))
		}
		find := func(meta logs.Labels) (plan.Bucket, bool) {
			for _, b := range got {
				if b.Meta.String() == meta.String() {
					return b, true
				}
			}
			return plan.Bucket{}, false
		}
		shared := logs.FromMap(map[string]string{"region": "r1", "trace_id": "t1", "zone": "z1"})
		b, ok := find(shared)
		if !ok {
			t.Fatalf("no bucket with shared metadata %s: %v", shared, keys(got))
		}
		wantSharedBytes := int64(len("order-a") + len("order-b") + len("order-c"))
		if b.Count != 3 || b.Bytes != wantSharedBytes {
			t.Errorf("shared metadata bucket = count %d bytes %d, want 3 and %d (three physical key orders)", b.Count, b.Bytes, wantSharedBytes)
		}
		empty := logs.Labels{}
		b, ok = find(empty)
		if !ok {
			t.Fatalf("no empty-metadata bucket %s: %v", empty, keys(got))
		}
		wantEmptyBytes := int64(len("absent") + len("empty-map"))
		if b.Count != 2 || b.Bytes != wantEmptyBytes {
			t.Errorf("empty-metadata bucket = count %d bytes %d, want 2 and %d (absent and empty map together)", b.Count, b.Bytes, wantEmptyBytes)
		}
		b, ok = find(logs.FromMap(map[string]string{"trace_id": ""}))
		if !ok {
			t.Fatalf("no present-empty-value bucket: %v", keys(got))
		}
		if b.Count != 1 || b.Bytes != int64(len("empty-value")) {
			t.Errorf("present empty value bucket = count %d bytes %d, want 1 and %d", b.Count, b.Bytes, len("empty-value"))
		}
		b, ok = find(logs.FromMap(map[string]string{"region": "r1", "trace_id": "t2", "zone": "z1"}))
		if !ok {
			t.Fatalf("no distinct-metadata bucket: %v", keys(got))
		}
		if b.Count != 1 || b.Bytes != int64(len("distinct-meta")) {
			t.Errorf("distinct metadata bucket = count %d bytes %d, want 1 and %d", b.Count, b.Bytes, len("distinct-meta"))
		}
		for _, b := range got {
			if !sortedMeta(b.Meta) {
				t.Errorf("bucket metadata is not sorted by name: %s", b.Meta)
			}
		}
	})

	t.Run("without-meta", func(t *testing.T) {
		agg := plan.BucketAgg{Width: time.Minute, Origin: base}
		got := compare(t, agg)
		if len(got) != 1 {
			t.Fatalf("%d buckets, want 1 label-only bucket: %v", len(got), keys(got))
		}
		wantBytes := int64(len("order-a") + len("order-b") + len("order-c") + len("absent") + len("empty-map") + len("empty-value") + len("distinct-meta"))
		if got[0].Count != 7 || got[0].Bytes != wantBytes {
			t.Errorf("label-only bucket = count %d bytes %d, want 7 and %d", got[0].Count, got[0].Bytes, wantBytes)
		}
		if got[0].Meta != nil {
			t.Errorf("disabled aggregation returned Meta %s, want nil", got[0].Meta)
		}
	})
}
