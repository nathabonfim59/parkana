package compactor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

var testHour = time.Date(2026, 11, 3, 14, 0, 0, 0, time.UTC)

// at returns the timestamp us microseconds after testHour.
func at(us int64) time.Time { return testHour.Add(time.Duration(us) * time.Microsecond) }

// date returns January 1 of year in UTC.
func date(year int) time.Time { return time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC) }

func baseSchema(t *testing.T, promoted ...string) *arrow.Schema {
	t.Helper()
	s, err := schema.ForStream(promoted)
	if err != nil {
		t.Fatalf("ForStream: %v", err)
	}
	return s
}

func TestKeyHour(t *testing.T) {
	cases := []struct {
		key  string
		want string // empty: expect an error
	}{
		{"default/logs/svc/2026/11/03/14/f.parquet", "2026/11/03/14"},
		{"default/logs/svc/2024/02/29/00/f.parquet", "2024/02/29/00"}, // leap day
		{"other/logs/svc/2026/11/03/14/f.parquet", ""},                // cross-tenant
		{"default/logs/other/2026/11/03/14/f.parquet", ""},            // cross-stream
		{"default/logs/svc/2026/11/03/f.parquet", ""},                 // missing hour
		{"default/logs/svc/2026/11/03/14/aa/f.parquet", ""},           // extra segment
		{"default/logs/svc/2026/1/03/14/f.parquet", ""},               // short month
		{"default/logs/svc/2026/xx/03/14/f.parquet", ""},              // non-digits
		{"default/logs/svc/2026/11/03/14/f.idx", ""},                  // sidecar name
		{"default/logs/svc/2026/11/03/14/.parquet", ""},               // empty name
		{"default/logs/svc/2026/13/03/14/f.parquet", ""},              // month 13
		{"default/logs/svc/2026/11/31/14/f.parquet", ""},              // Nov 31
		{"default/logs/svc/2025/02/29/14/f.parquet", ""},              // not a leap year
		{"default/logs/svc/2026/11/03/24/f.parquet", ""},              // hour 24
		{"default/logs/svc/2026/99/99/99/f.parquet", ""},              // all invalid
		{"default//logs/svc/2026/11/03/14/f.parquet", ""},             // invalid key
		{"default/logs/svc/2026/11/03/14/../f.parquet", ""},           // dot segment
	}
	for _, tc := range cases {
		got, err := keyHour(tenant.Default, "svc", tc.key)
		if tc.want == "" {
			if err == nil {
				t.Fatalf("keyHour(%q) = %q, want error", tc.key, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("keyHour(%q): %v", tc.key, err)
		}
		if got != tc.want {
			t.Fatalf("keyHour(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}

func TestPromotedUnion(t *testing.T) {
	batch := []catalog.File{
		{LabelValues: map[string][]string{"level": {"info"}, "app": {"web"}}},
		{LabelValues: map[string][]string{"level": {"warn"}, "trace": {"x"}}},
		{LabelValues: map[string][]string{"_bad": {"y"}, "": {"z"}, "line": {"q"}}},
		{LabelValues: nil},
	}
	got := promotedUnion(batch)
	want := []string{"app", "level", "trace"}
	if !slices.Equal(got, want) {
		t.Fatalf("promotedUnion = %v, want %v", got, want)
	}
}

// TestCycleMergesSameHour verifies a merged object preserves rows exactly:
// sorted, ties stable by input order, base columns, maps, promoted values and
// typed additive fields, plus correct catalog stats.
func TestCycleMergesSameHour(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	base := schema.TS(testHour)

	f1 := putFile(t, d, tenant.Default, "svc", testHour, baseSchema(t, "level"), []string{"level"},
		tss(base+10, base+30, base+50),
		strs("a0", "a1", "a2"),
		mapVals(map[string]string{"app": "web"}, map[string]string{"app": "web"}, map[string]string{"app": "web"}),
		mapVals(map[string]string{"m": "1"}, map[string]string{"m": "2"}, map[string]string{"m": "3"}),
		strs("info", "warn", "info"),
	)
	f3 := putFile(t, d, tenant.Default, "svc", testHour,
		arrow.NewSchema(append(schema.Base(), arrow.Field{Name: "trace", Type: arrow.PrimitiveTypes.Int64, Nullable: true}), nil),
		nil,
		tss(base+15, base+50),
		strs("c0", "c1"),
		mapVals(map[string]string{"app": "cache"}, map[string]string{"app": "cache"}),
		mapVals(map[string]string{}, map[string]string{}),
		[]any{int64(7), int64(9)},
	)
	f2 := putFile(t, d, tenant.Default, "svc", testHour, baseSchema(t, "level", "svc_name"), []string{"level", "svc_name"},
		tss(base+20, base+40, base+50),
		strs("b0", "b1", "b2"),
		mapVals(map[string]string{"app": "db"}, map[string]string{"app": "db"}, map[string]string{"app": "db"}),
		mapVals(map[string]string{}, map[string]string{}, map[string]string{}),
		strs("debug", "info", "warn"),
		strs("api", "api", "api"),
	)

	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cycle: %v", err)
	}

	live := liveFiles(t, d.cat, tenant.Default, "svc")
	if len(live) != 1 {
		t.Fatalf("live files = %d, want 1", len(live))
	}
	out := live[0]
	if !strings.Contains(out.Key, "/2026/11/03/14/") {
		t.Fatalf("merged key %q not in hour partition", out.Key)
	}
	if out.Rows != 8 || !out.MinTS.Equal(at(10)) || !out.MaxTS.Equal(at(50)) {
		t.Fatalf("merged stats = %+v", out)
	}
	if info, err := d.fs.Stat(t.Context(), out.Key); err != nil || info.Size != out.Bytes || out.Bytes <= 0 {
		t.Fatalf("merged object stat = %+v, %v; catalog bytes %d", info, err, out.Bytes)
	}
	if !out.HasSidecar {
		t.Fatal("merged file not marked with sidecar")
	}
	if !out.LabelsComplete {
		t.Fatal("merged file lost LabelsComplete from FileStats")
	}
	if got := metricValue(t, d.reg, "parkana_index_built_total"); got != 1 {
		t.Fatalf("index_built_total = %v, want 1 (compaction)", got)
	}
	if got := metricValue(t, d.reg, "parkana_index_skipped_too_many_grams_total"); got != 0 {
		t.Fatalf("index_skipped_too_many_grams_total = %v, want 0", got)
	}
	requireSidecar(t, d, out.Key, []string{"a0", "a1", "a2", "b0", "b1", "b2", "c0", "c1"})
	if out.CreatedAt.IsZero() || time.Since(out.CreatedAt) > time.Minute {
		t.Fatalf("merged CreatedAt = %v", out.CreatedAt)
	}
	// f3 has neither promoted column and no map entries for them, so the
	// complete summaries record the empty string for its rows.
	if got, want := out.LabelValues["level"], []string{"", "debug", "info", "warn"}; !slices.Equal(got, want) {
		t.Fatalf("LabelValues[level] = %v, want %v", got, want)
	}
	if got, want := out.LabelValues["svc_name"], []string{"", "api"}; !slices.Equal(got, want) {
		t.Fatalf("LabelValues[svc_name] = %v, want %v", got, want)
	}
	if _, ok := out.LabelValues["trace"]; ok {
		t.Fatal("non-promoted typed field in LabelValues")
	}

	sch, rows := readObjectRows(t, d, tenant.Default, out.Key)
	if got, want := fieldNames(sch), []string{schema.ColTS, schema.ColLine, schema.ColLabels, schema.ColMeta, "level", "trace", "svc_name"}; !slices.Equal(got, want) {
		t.Fatalf("merged fields = %v, want %v", got, want)
	}
	want := [][]any{
		{base + 10, "a0", map[string]string{"app": "web"}, map[string]string{"m": "1"}, "info", nil, nil},
		{base + 15, "c0", map[string]string{"app": "cache"}, map[string]string{}, nil, int64(7), nil},
		{base + 20, "b0", map[string]string{"app": "db"}, map[string]string{}, "debug", nil, "api"},
		{base + 30, "a1", map[string]string{"app": "web"}, map[string]string{"m": "2"}, "warn", nil, nil},
		{base + 40, "b1", map[string]string{"app": "db"}, map[string]string{}, "info", nil, "api"},
		{base + 50, "a2", map[string]string{"app": "web"}, map[string]string{"m": "3"}, "info", nil, nil},
		{base + 50, "c1", map[string]string{"app": "cache"}, map[string]string{}, nil, int64(9), nil},
		{base + 50, "b2", map[string]string{"app": "db"}, map[string]string{}, "warn", nil, "api"},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("merged rows =\n%v\nwant\n%v", rows, want)
	}

	// Sources are marked deleted but their objects are intact.
	all := listFiles(t, d.cat, catalog.Filter{Tenant: tenant.Default, Stream: "svc", IncludeDeleted: true})
	if len(all) != 4 {
		t.Fatalf("catalog rows = %d, want 4", len(all))
	}
	for _, src := range []catalog.File{f1, f2, f3} {
		if _, err := d.fs.Stat(t.Context(), src.Key); err != nil {
			t.Fatalf("source object %s gone: %v", src.Key, err)
		}
	}
}

// TestCycleGroupsIsolation merges only files sharing tenant, stream and
// object-key hour.
func TestCycleGroupsIsolation(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	h14 := testHour
	h15 := testHour.Add(time.Hour)
	base14, base15 := schema.TS(h14), schema.TS(h15)

	putFile(t, d, tenant.Default, "svc", h14, sch, nil,
		tss(base14), strs("h14a"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "svc", h14, sch, nil,
		tss(base14+1), strs("h14b"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "svc", h15, sch, nil,
		tss(base15), strs("h15a"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "svc", h15, sch, nil,
		tss(base15+1), strs("h15b"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "other", h14, sch, nil,
		tss(base14), strs("oa"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "other", h14, sch, nil,
		tss(base14+1), strs("ob"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	// A file belonging to another tenant: the phase-1 single-tenant scan
	// never touches it.
	otherFile := putFile(t, d, tenant.ID("other"), "svc", h14, sch, nil,
		tss(base14), strs("ta"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cycle: %v", err)
	}

	svcLive := liveFiles(t, d.cat, tenant.Default, "svc")
	if len(svcLive) != 2 {
		t.Fatalf("svc live files = %d, want 2", len(svcLive))
	}
	for _, f := range svcLive {
		schOut, rows := readObjectRows(t, d, tenant.Default, f.Key)
		if got := fieldNames(schOut); !slices.Equal(got, []string{schema.ColTS, schema.ColLine, schema.ColLabels, schema.ColMeta}) {
			t.Fatalf("fields = %v", got)
		}
		var want [][]any
		switch {
		case f.MinTS.Equal(h14):
			want = [][]any{
				{base14, "h14a", map[string]string{}, map[string]string{}},
				{base14 + 1, "h14b", map[string]string{}, map[string]string{}},
			}
		case f.MinTS.Equal(h15):
			want = [][]any{
				{base15, "h15a", map[string]string{}, map[string]string{}},
				{base15 + 1, "h15b", map[string]string{}, map[string]string{}},
			}
		default:
			t.Fatalf("merged file %s has MinTS %v outside both hours", f.Key, f.MinTS)
		}
		if !reflect.DeepEqual(rows, want) {
			t.Fatalf("merged %s rows = %v, want %v", f.Key, rows, want)
		}
	}

	otherLive := liveFiles(t, d.cat, tenant.Default, "other")
	if len(otherLive) != 1 || otherLive[0].Rows != 2 {
		t.Fatalf("other-stream live = %+v, want one merged 2-row file", otherLive)
	}
	if got := liveFiles(t, d.cat, tenant.ID("other"), "svc"); len(got) != 1 || got[0].Key != otherFile.Key {
		t.Fatalf("other-tenant files = %+v, want only %s", got, otherFile.Key)
	}
}

// TestCycleTargetBounds exercises the packing rules: files at or over the
// target are never candidates, the batch sums under the target, and a smaller
// pair is merged instead of being stranded behind a big candidate.
func TestCycleTargetBounds(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	put := func(us int64, line string, n int) catalog.File {
		ts := make([]any, n)
		lines := make([]any, n)
		lbls := make([]any, n)
		meta := make([]any, n)
		for i := range n {
			ts[i] = base + us + int64(i)
			lines[i] = fmt.Sprintf("%s-%d", line, i)
			lbls[i] = map[string]string{}
			meta[i] = map[string]string{}
		}
		return putFile(t, d, tenant.Default, "svc", testHour, sch, nil, ts, lines, lbls, meta)
	}

	small1 := put(0, "s1", 1)
	small2 := put(100, "s2", 1)
	small3 := put(200, "s3", 1)
	big := put(300, "big", 2000)

	// The big file alone exceeds the target; each small pair fits.
	target := config.ByteSize(small1.Bytes + small2.Bytes)
	if big.Bytes < int64(target) {
		t.Fatalf("test setup: big file %d < target %d", big.Bytes, target)
	}
	s.cfg.TargetSize = target

	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cycle: %v", err)
	}

	live := liveFiles(t, d.cat, tenant.Default, "svc")
	// small1+small2 merge (they pack within target); small3 has no partner
	// left that fits and big was never a candidate.
	if len(live) != 3 {
		t.Fatalf("live files = %d, want 3", len(live))
	}
	var merged *catalog.File
	keys := make(map[string]bool)
	for i := range live {
		keys[live[i].Key] = true
		if live[i].Key != small3.Key && live[i].Key != big.Key {
			merged = &live[i]
		}
	}
	if merged == nil {
		t.Fatalf("no merged output; live = %v", live)
	}
	if keys[small1.Key] || keys[small2.Key] {
		t.Fatal("merged inputs still live")
	}
	if merged.Rows != 2 || !merged.MinTS.Equal(testHour) || !merged.MaxTS.Equal(at(100)) {
		t.Fatalf("merged stats = %+v", merged)
	}
}

// TestCycleSmallPairBehindBigCandidate is the 90/20/20 regression: sorted
// ascending by size, the two 20s merge even though the 90 is listed first in
// catalog order and would strand them under catalog-order first-fit.
func TestCycleSmallPairBehindBigCandidate(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)

	put := func(us int64, line string, n int) catalog.File {
		ts := make([]any, n)
		lines := make([]any, n)
		lbls := make([]any, n)
		meta := make([]any, n)
		for i := range n {
			ts[i] = base + us + int64(i)
			lines[i] = fmt.Sprintf("%s-%d", line, i)
			lbls[i] = map[string]string{}
			meta[i] = map[string]string{}
		}
		return putFile(t, d, tenant.Default, "svc", testHour, sch, nil, ts, lines, lbls, meta)
	}
	small1 := put(0, "s1", 5)
	small2 := put(100, "s2", 5)
	// big stays a candidate (below target) but fits with neither small.
	big := put(200, "b", 12)

	// Force the scenario directly through mergeHour with big first and a
	// target that fits only the small pair.
	target := config.ByteSize(small1.Bytes + small2.Bytes + 1)
	if big.Bytes >= int64(target) {
		t.Fatalf("test setup: big %d must stay a candidate below target %d", big.Bytes, target)
	}
	if big.Bytes+small1.Bytes <= int64(target) || big.Bytes+small2.Bytes <= int64(target) {
		t.Fatalf("test setup: big %d must fit with neither small under target %d", big.Bytes, target)
	}
	s.cfg.TargetSize = target
	if err := s.mergeHour(t.Context(), tenant.Default, "svc", []catalog.File{big, small1, small2}); err != nil {
		t.Fatalf("mergeHour: %v", err)
	}

	live := liveFiles(t, d.cat, tenant.Default, "svc")
	if len(live) != 2 {
		t.Fatalf("live files = %d, want 2", len(live))
	}
	var merged, survivor *catalog.File
	for i := range live {
		if live[i].Key == big.Key {
			survivor = &live[i]
		} else {
			merged = &live[i]
		}
	}
	if merged == nil || survivor == nil {
		t.Fatalf("live = %v, want merged output plus the big file", live)
	}
	if merged.Rows != 10 || !merged.MinTS.Equal(testHour) || !merged.MaxTS.Equal(at(104)) {
		t.Fatalf("merged stats = %+v", merged)
	}
}

func TestCycleSingleFileNoOp(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	f := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(schema.TS(testHour)), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cycle: %v", err)
	}
	live := liveFiles(t, d.cat, tenant.Default, "svc")
	if len(live) != 1 || live[0].Key != f.Key {
		t.Fatalf("live = %+v, want unchanged %s", live, f.Key)
	}
	if got := d.store.putCalls.Load(); got != 0 {
		t.Fatalf("service Put calls = %d, want 0", got)
	}
}

// TestCycleSkipsMalformedKeys: files whose keys fail parsing are left live
// and untouched while valid files in the same stream still merge.
func TestCycleSkipsMalformedKeys(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)

	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base+1), strs("a1"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	bad := []catalog.File{
		{Tenant: tenant.Default, Stream: "svc", Key: "default/logs/svc/badkey.parquet", Bytes: 10, MinTS: testHour, MaxTS: testHour},
		{Tenant: tenant.Default, Stream: "svc", Key: "other/logs/svc/2026/11/03/14/x.parquet", Bytes: 10, MinTS: testHour, MaxTS: testHour},
		{Tenant: tenant.Default, Stream: "svc", Key: "default/logs/other/2026/11/03/14/x.parquet", Bytes: 10, MinTS: testHour, MaxTS: testHour},
		{Tenant: tenant.Default, Stream: "svc", Key: "default/logs/svc/2026/13/03/14/x.parquet", Bytes: 10, MinTS: testHour, MaxTS: testHour},
	}
	if err := d.cat.AddFiles(t.Context(), bad...); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}

	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cycle: %v", err)
	}
	live := liveFiles(t, d.cat, tenant.Default, "svc")
	// One merged output plus the four untouched malformed rows.
	if len(live) != 5 {
		t.Fatalf("live files = %d, want 5", len(live))
	}
	for _, f := range live {
		if f.Key == bad[0].Key || f.Key == bad[1].Key || f.Key == bad[2].Key || f.Key == bad[3].Key {
			continue
		}
		if f.Rows != 2 {
			t.Fatalf("merged file = %+v, want 2 rows", f)
		}
	}
}

func TestCyclePutFailurePreservesSources(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	f1 := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	f2 := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base+1), strs("a1"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	d.store.putErr = errSpy
	d.store.putReadLimit = -1 // fail before consuming any bytes

	if err := s.cycle(t.Context()); err == nil {
		t.Fatal("cycle succeeded, want put failure")
	}
	live := liveFiles(t, d.cat, tenant.Default, "svc")
	if len(live) != 2 {
		t.Fatalf("live files = %d, want sources intact", len(live))
	}
	for _, f := range []catalog.File{f1, f2} {
		if _, err := d.fs.Stat(t.Context(), f.Key); err != nil {
			t.Fatalf("source %s: %v", f.Key, err)
		}
	}
	if got := metricValue(t, d.reg, "parkana_compactor_merge_errors_total"); got != 1 {
		t.Fatalf("merge_errors_total = %v, want 1", got)
	}
}

// TestCycleEarlyPutFailureJoinsProducer: Put fails after reading a few bytes
// while the producer is mid-merge; the merge must still terminate (pipe read
// side closed) with no leaked goroutine, and sources stay intact.
func TestCycleEarlyPutFailureJoinsProducer(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base+1), strs("a1"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	d.store.putErr = errSpy
	d.store.putReadLimit = 64

	before := runtime.NumGoroutine()
	if err := s.cycle(t.Context()); err == nil {
		t.Fatal("cycle succeeded, want put failure")
	}
	waitGoroutines(t, before)
	if got := len(liveFiles(t, d.cat, tenant.Default, "svc")); got != 2 {
		t.Fatalf("live files = %d, want sources intact", got)
	}
}

// TestCyclePutFailureUnblocksInputReads is the regression for a producer
// stuck in a ranged input read when Put fails: the merge context must be
// canceled before joining the producer, or the cycle hangs forever. The spy
// holds Put until a ranged read is provably in flight, so the producer is
// pinned inside GetRange when the upload fails.
func TestCyclePutFailureUnblocksInputReads(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)

	// Files larger than the object reader's read-ahead force several
	// merge-phase GetRange calls, so the producer is guaranteed to issue a
	// ranged read after Put starts; small files can be cached whole during
	// open and never read again.
	const n = 20000
	ts := make([]any, n)
	lines := make([]any, n)
	lbls := make([]any, n)
	meta := make([]any, n)
	for i := range n {
		ts[i] = base + int64(i)
		lines[i] = fmt.Sprintf("line-%d-%060d", i, i)
		lbls[i] = map[string]string{"app": "web"}
		meta[i] = map[string]string{}
	}
	f1 := putFile(t, d, tenant.Default, "svc", testHour, sch, nil, ts, lines, lbls, meta)
	for i := range n {
		ts[i] = base + n + int64(i)
	}
	f2 := putFile(t, d, tenant.Default, "svc", testHour, sch, nil, ts, lines, lbls, meta)
	for _, f := range []catalog.File{f1, f2} {
		if info, err := d.fs.Stat(t.Context(), f.Key); err != nil || info.Size <= 64*1024 {
			t.Fatalf("test setup: input %s size = %d, %v; want > 64KiB", f.Key, info.Size, err)
		}
	}

	d.store.getRangeDelay = 10 * time.Millisecond // Put lands mid-reads
	d.store.blockRanges = true                    // merge-phase reads wait for ctx
	d.store.waitBlocked = true                    // Put fails with a read in flight
	d.store.putErr = errSpy

	before := runtime.NumGoroutine()
	done := make(chan error, 1)
	go func() { done <- s.cycle(t.Context()) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cycle succeeded, want error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cycle hung: producer not unblocked by Put failure")
	}
	if got := d.store.blockedRanges.Load(); got == 0 {
		t.Fatal("producer never entered a blocked ranged read; regression path not exercised")
	}
	waitGoroutines(t, before)
	if got := len(liveFiles(t, d.cat, tenant.Default, "svc")); got != 2 {
		t.Fatalf("live files = %d, want sources intact", got)
	}
}

// TestCycleStatFailureLeavesOrphan: when Stat fails after a successful Put,
// sources stay live and the merged object remains in the store for a later
// cleanup pass.
func TestCycleStatFailureLeavesOrphan(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base+1), strs("a1"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	d.store.statFailPut = true

	if err := s.cycle(t.Context()); err == nil {
		t.Fatal("cycle succeeded, want stat failure")
	}
	orphan, _ := d.store.lastPut.Load().(string)
	if orphan == "" {
		t.Fatal("no Put recorded")
	}
	if _, err := d.fs.Stat(t.Context(), orphan); err != nil {
		t.Fatalf("orphan object %s missing: %v", orphan, err)
	}
	if got := len(liveFiles(t, d.cat, tenant.Default, "svc")); got != 2 {
		t.Fatalf("live files = %d, want sources intact", got)
	}
}

// TestCycleReplaceFailureLeavesOrphan: when the atomic catalog swap fails,
// the uploaded object stays unreferenced and sources keep serving.
func TestCycleReplaceFailureLeavesOrphan(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base+1), strs("a1"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	d.cat.replaceErr = errSpy

	if err := s.cycle(t.Context()); err == nil {
		t.Fatal("cycle succeeded, want replace failure")
	}
	added := d.cat.captured()
	if len(added) != 1 {
		t.Fatalf("ReplaceFiles add args = %+v, want one file", added)
	}
	if _, err := d.fs.Stat(t.Context(), added[0].Key); err != nil {
		t.Fatalf("orphan object %s missing: %v", added[0].Key, err)
	}
	if added[0].Rows != 2 || added[0].Tenant != tenant.Default || added[0].Stream != "svc" || added[0].Bytes <= 0 {
		t.Fatalf("replacement row = %+v", added[0])
	}
	if got := len(liveFiles(t, d.cat, tenant.Default, "svc")); got != 2 {
		t.Fatalf("live files = %d, want sources intact", got)
	}
}

// TestCycleSidecarOperationOrder proves the merge publication sequence
// through the shared store/catalog op log: the merged object is fully Put
// first, its sidecar is built by rereading that final object, the sidecar is
// Put, and only then does the catalog swap run. The reread runs on the live
// outer context (the merge producer's context is canceled as soon as it
// joins), so a successful swap with a sidecar also proves the reread escaped
// that canceled context.
func TestCycleSidecarOperationOrder(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base+1), strs("a1"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cycle: %v", err)
	}
	live := liveFiles(t, d.cat, tenant.Default, "svc")
	if len(live) != 1 || !live[0].HasSidecar {
		t.Fatalf("live = %+v, want one merged file with a sidecar", live)
	}
	key := live[0].Key
	sidecar := objstore.SidecarKey(key)

	putData := d.store.ops.index("put:" + key)
	reread := d.store.ops.indexFunc(func(op string) bool { return op == "range:"+key })
	putSidecar := d.store.ops.index("put:" + sidecar)
	replace := d.store.ops.indexFunc(func(op string) bool { return strings.HasPrefix(op, "replace:") })
	for _, pos := range []struct {
		name string
		idx  int
	}{{"data put", putData}, {"final-object reread", reread}, {"sidecar put", putSidecar}, {"catalog replace", replace}} {
		if pos.idx < 0 {
			t.Fatalf("operation %s missing from log %v", pos.name, d.store.ops.snapshot())
		}
	}
	if !(putData < reread && reread < putSidecar && putSidecar < replace) {
		t.Fatalf("operation order wrong: data put @%d, final-object reread @%d, sidecar put @%d, replace @%d; log %v",
			putData, reread, putSidecar, replace, d.store.ops.snapshot())
	}
}

// TestCycleSidecarPutFailurePreservesSources: a sidecar Put failure after a
// successful merge upload leaves the original catalog inputs serving, no
// catalog swap and no sidecar object; the merged object stays as an orphan
// for cleanup. The build metrics still count the successful encode: the
// build work was consumed even though publication failed.
func TestCycleSidecarPutFailurePreservesSources(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	f1 := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	f2 := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base+1), strs("a1"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	d.store.sidecarPutErr = errSpy

	err := s.cycle(t.Context())
	if err == nil || !strings.Contains(err.Error(), "sidecar") {
		t.Fatalf("cycle error = %v, want a sidecar failure", err)
	}
	if got := len(liveFiles(t, d.cat, tenant.Default, "svc")); got != 2 {
		t.Fatalf("live files = %d, want sources intact", got)
	}
	if got := d.cat.replaceCalls.Load(); got != 0 {
		t.Fatalf("ReplaceFiles calls = %d, want 0", got)
	}
	for _, f := range []catalog.File{f1, f2} {
		if _, err := d.fs.Stat(t.Context(), f.Key); err != nil {
			t.Fatalf("source object %s: %v", f.Key, err)
		}
	}
	// The uploaded merged object survives unreferenced (one data orphan, no
	// sidecar), keeping the exact store content cleanup expects.
	dataObjs, idxObjs := 0, 0
	for info, err := range d.fs.List(t.Context(), objstore.LogPrefix(tenant.Default, "svc")) {
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		switch {
		case strings.HasSuffix(info.Key, objstore.ExtData):
			if info.Key != f1.Key && info.Key != f2.Key {
				dataObjs++
			}
		case strings.HasSuffix(info.Key, objstore.ExtSidecar):
			idxObjs++
		}
	}
	if dataObjs != 1 || idxObjs != 0 {
		t.Fatalf("orphans = %d data, %d sidecar; want 1 data, 0 sidecar", dataObjs, idxObjs)
	}
	if got := metricValue(t, d.reg, "parkana_index_built_total"); got != 1 {
		t.Fatalf("index_built_total = %v, want 1: the encode succeeded and was observed despite the Put failure", got)
	}
	if got := metricValue(t, d.reg, "parkana_index_skipped_too_many_grams_total"); got != 0 {
		t.Fatalf("index_skipped_too_many_grams_total = %v, want 0", got)
	}
	if got := metricValue(t, d.reg, "parkana_index_build_duration_seconds"); got != 1 {
		t.Fatalf("index_build_duration_seconds count = %v, want 1", got)
	}
	if got := metricValue(t, d.reg, "parkana_compactor_merge_errors_total"); got != 1 {
		t.Fatalf("merge_errors_total = %v, want 1", got)
	}
}

// TestCycleSidecarBuildFailurePreservesSources: when reading the final object
// back for its sidecar fails (here: every ranged read of the just-uploaded
// key, which the merge's input reads never touch), the merge is abandoned
// before the catalog swap. No build metric fires: the failure belongs to the
// compactor's error accounting, not the build counters.
func TestCycleSidecarBuildFailurePreservesSources(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base+1), strs("a1"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	d.store.failRangesAfterPut = true

	err := s.cycle(t.Context())
	if err == nil || !strings.Contains(err.Error(), "sidecar for") {
		t.Fatalf("cycle error = %v, want a sidecar reread failure", err)
	}
	if got := len(liveFiles(t, d.cat, tenant.Default, "svc")); got != 2 {
		t.Fatalf("live files = %d, want sources intact", got)
	}
	if got := d.cat.replaceCalls.Load(); got != 0 {
		t.Fatalf("ReplaceFiles calls = %d, want 0", got)
	}
	if got := countObjects(t, d, tenant.Default, "svc"); got != 3 {
		t.Fatalf("stored objects = %d, want 2 sources plus the data orphan", got)
	}
	for _, name := range []string{
		"parkana_index_built_total",
		"parkana_index_skipped_too_many_grams_total",
		"parkana_index_build_duration_seconds",
	} {
		if got := metricValue(t, d.reg, name); got != 0 {
			t.Fatalf("%s = %v, want 0: a failed build is caller error traffic", name, got)
		}
	}
	if got := metricValue(t, d.reg, "parkana_compactor_merge_errors_total"); got != 1 {
		t.Fatalf("merge_errors_total = %v, want 1", got)
	}
}

// TestCycleOverCapOutputQueryableWithoutSidecar: a merged output whose grams
// exceed the cap is published without a sidecar (flag false, one compaction
// skip observation, key remembered so a later backfill pass does not rebuild
// it), and its stored Parquet and catalog visibility stay exactly what a
// query needs.
func TestCycleOverCapOutputQueryableWithoutSidecar(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	// The gram length is pinned so the fixture survives a default change:
	// the two 8-byte lines hold 5 distinct grams each at N=4 (10 total,
	// clearing the cap of 8), but only 4 each at N=5 (8 total, exactly at the
	// cap and not over it).
	d.deps.Config.Index.N = 4
	d.deps.Config.Index.MaxGrams = 8
	s := mustService(t, d)
	// Backfill stays off for the first cycle so the skip observation can
	// only come from the compaction path.
	sch := baseSchema(t)
	base := schema.TS(testHour)
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("abcdefgh"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base+1), strs("ijklmnop"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cycle: %v", err)
	}
	live := liveFiles(t, d.cat, tenant.Default, "svc")
	if len(live) != 1 {
		t.Fatalf("live files = %d, want the merged output", len(live))
	}
	out := live[0]
	if out.HasSidecar {
		t.Fatal("over-cap output published with a sidecar flag")
	}
	if !out.LabelsComplete {
		t.Fatal("over-cap output lost LabelsComplete")
	}
	if _, err := d.fs.Stat(t.Context(), objstore.SidecarKey(out.Key)); !errors.Is(err, objstore.ErrNotExist) {
		t.Fatalf("over-cap sidecar object = %v, want ErrNotExist", err)
	}
	if got := metricLabeledValue(t, d.reg, "parkana_index_skipped_too_many_grams_total", "compaction"); got != 1 {
		t.Fatalf("index_skipped_too_many_grams_total{compaction} = %v, want 1", got)
	}
	if got := metricLabeledValue(t, d.reg, "parkana_index_skipped_too_many_grams_total", "backfill"); got != 0 {
		t.Fatalf("index_skipped_too_many_grams_total{backfill} = %v, want 0", got)
	}
	// The stored Parquet is untouched and readable, and the row keeps
	// serving queries with full stats.
	schOut, rows := readObjectRows(t, d, tenant.Default, out.Key)
	if got := fieldNames(schOut); !slices.Equal(got, []string{schema.ColTS, schema.ColLine, schema.ColLabels, schema.ColMeta}) {
		t.Fatalf("fields = %v", got)
	}
	want := [][]any{
		{base, "abcdefgh", map[string]string{}, map[string]string{}},
		{base + 1, "ijklmnop", map[string]string{}, map[string]string{}},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}

	// Enabling backfill afterwards must not re-read the remembered over-cap
	// key: no new skip observation and no flag update.
	enableBackfill(s)
	before := d.store.rangeCalls.Load()
	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("backfill cycle: %v", err)
	}
	if got := metricLabeledValue(t, d.reg, "parkana_index_skipped_too_many_grams_total", "backfill"); got != 0 {
		t.Fatalf("index_skipped_too_many_grams_total{backfill} = %v after backfill cycle, want 0", got)
	}
	if got := d.cat.setSidecarCalls.Load(); got != 0 {
		t.Fatalf("SetSidecar calls = %d, want 0", got)
	}
	if got := d.store.rangeCalls.Load(); got != before {
		t.Fatalf("range calls grew from %d to %d; the over-cap key should be skipped without reading", before, got)
	}
}

// TestCycleContextCancelStopsMerge: cancellation while Put is in flight ends
// the merge promptly and returns the context error; sources stay intact.
func TestCycleContextCancelStopsMerge(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base), strs("a0"), mapVals(map[string]string{}), mapVals(map[string]string{}))
	putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
		tss(base+1), strs("a1"), mapVals(map[string]string{}), mapVals(map[string]string{}))

	d.store.blockPut = true

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.cycle(ctx) }()
	waitFor(t, "merge to reach Put", func() bool { return d.store.putCalls.Load() > 0 })
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cycle error = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cycle did not return after cancel")
	}
	if got := len(liveFiles(t, d.cat, tenant.Default, "svc")); got != 2 {
		t.Fatalf("live files = %d, want sources intact", got)
	}
}

// TestMergeInputCap: a group larger than maxMergeInputs merges only the
// smallest maxMergeInputs candidates and leaves the rest for a later cycle.
func TestMergeInputCap(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)

	files := make([]catalog.File, maxMergeInputs+1)
	for i := range files {
		files[i] = putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
			tss(base+int64(i)), strs(fmt.Sprintf("r%d", i)),
			mapVals(map[string]string{}), mapVals(map[string]string{}))
	}
	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cycle: %v", err)
	}

	live := liveFiles(t, d.cat, tenant.Default, "svc")
	if len(live) != 2 {
		t.Fatalf("live files = %d, want 2 (merged + leftover)", len(live))
	}
	var merged, leftover *catalog.File
	for i := range live {
		if live[i].Rows == maxMergeInputs {
			merged = &live[i]
		} else {
			leftover = &live[i]
		}
	}
	if merged == nil || leftover == nil {
		t.Fatalf("live = %+v, want a %d-row merge plus one leftover", live, maxMergeInputs)
	}
	if leftover.Rows != 1 {
		t.Fatalf("leftover = %+v", leftover)
	}
}

// TestCheckMergeStats covers the verification performed before a merge may
// replace its inputs. Reversed output bounds cannot be produced by real
// storage.Merge, so the helper is the only place that branch is exercised.
func TestCheckMergeStats(t *testing.T) {
	input := func(rows int64, minTS, maxTS time.Time) catalog.File {
		return catalog.File{Rows: rows, MinTS: minTS, MaxTS: maxTS}
	}
	batch := []catalog.File{input(3, at(10), at(30)), input(3, at(20), at(50))}

	cases := []struct {
		name    string
		batch   []catalog.File
		stats   storage.FileStats
		wantErr string // empty: valid
	}{
		{
			name:  "exact bounds",
			batch: batch,
			stats: storage.FileStats{Rows: 6, MinTS: at(10), MaxTS: at(50)},
		},
		{
			name:  "output narrower than inputs",
			batch: batch,
			stats: storage.FileStats{Rows: 6, MinTS: at(15), MaxTS: at(45)},
		},
		{
			name:    "row count mismatch",
			batch:   batch,
			stats:   storage.FileStats{Rows: 5, MinTS: at(10), MaxTS: at(50)},
			wantErr: "rows",
		},
		{
			name:  "exact deduplicated accounting",
			batch: batch,
			stats: storage.FileStats{Rows: 5, DeduplicatedRows: 1, MinTS: at(10), MaxTS: at(50)},
		},
		{
			name:  "all duplicates collapse to one row",
			batch: []catalog.File{input(3, at(10), at(20))},
			stats: storage.FileStats{Rows: 1, DeduplicatedRows: 2, MinTS: at(10), MaxTS: at(20)},
		},
		{
			name:    "missing rows without removed count",
			batch:   batch,
			stats:   storage.FileStats{Rows: 5, MinTS: at(10), MaxTS: at(50)},
			wantErr: "rows",
		},
		{
			name:    "removed count too small",
			batch:   batch,
			stats:   storage.FileStats{Rows: 5, DeduplicatedRows: 0, MinTS: at(10), MaxTS: at(50)},
			wantErr: "rows",
		},
		{
			name:    "removed count too large",
			batch:   batch,
			stats:   storage.FileStats{Rows: 6, DeduplicatedRows: 1, MinTS: at(10), MaxTS: at(50)},
			wantErr: "rows",
		},
		{
			name:    "negative removed count",
			batch:   batch,
			stats:   storage.FileStats{Rows: 6, DeduplicatedRows: -1, MinTS: at(10), MaxTS: at(50)},
			wantErr: "nonnegative",
		},
		{
			name:    "negative output rows",
			batch:   batch,
			stats:   storage.FileStats{Rows: -1, DeduplicatedRows: 7, MinTS: at(10), MaxTS: at(50)},
			wantErr: "nonnegative",
		},
		{
			name:    "output plus removed overflows",
			batch:   batch,
			stats:   storage.FileStats{Rows: 1, DeduplicatedRows: math.MaxInt64, MinTS: at(10), MaxTS: at(50)},
			wantErr: "overflow",
		},
		{
			name:    "zero output claiming the whole input as duplicates",
			batch:   []catalog.File{input(3, at(10), at(20))},
			stats:   storage.FileStats{Rows: 0, DeduplicatedRows: 3, MinTS: at(10), MaxTS: at(20)},
			wantErr: "zero rows",
		},
		{
			name:    "negative input rows",
			batch:   []catalog.File{input(-1, at(10), at(20))},
			stats:   storage.FileStats{Rows: 0, DeduplicatedRows: 0},
			wantErr: "input rows",
		},
		{
			name:    "input rows overflow",
			batch:   []catalog.File{input(math.MaxInt64, at(10), at(20)), input(1, at(10), at(20))},
			stats:   storage.FileStats{Rows: math.MaxInt64, MinTS: at(10), MaxTS: at(20)},
			wantErr: "overflow",
		},
		{
			name:    "output starts before inputs",
			batch:   batch,
			stats:   storage.FileStats{Rows: 6, MinTS: at(9), MaxTS: at(50)},
			wantErr: "min ts",
		},
		{
			name:    "output ends after inputs",
			batch:   batch,
			stats:   storage.FileStats{Rows: 6, MinTS: at(10), MaxTS: at(51)},
			wantErr: "max ts",
		},
		{
			name:    "reversed output range",
			batch:   batch,
			stats:   storage.FileStats{Rows: 6, MinTS: at(50), MaxTS: at(10)},
			wantErr: "reversed",
		},
		{
			name:  "zero-row inputs",
			batch: []catalog.File{input(0, time.Time{}, time.Time{})},
			stats: storage.FileStats{},
		},
		{
			name:  "zero-row input ignored for bounds",
			batch: []catalog.File{input(0, time.Time{}, time.Time{}), input(2, at(10), at(20))},
			stats: storage.FileStats{Rows: 2, MinTS: at(10), MaxTS: at(20)},
		},
		{
			name:  "zero-valued timestamp on non-empty input is a real bound",
			batch: []catalog.File{input(1, time.Time{}, at(10))},
			stats: storage.FileStats{Rows: 1, MinTS: time.Time{}, MaxTS: at(10)},
		},
		{
			name:    "pre-epoch output before input",
			batch:   []catalog.File{input(1, date(1960), date(1961))},
			stats:   storage.FileStats{Rows: 1, MinTS: date(1950), MaxTS: date(1961)},
			wantErr: "min ts",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkMergeStats(tc.batch, tc.stats)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("checkMergeStats: %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("checkMergeStats = nil, want %q error", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("checkMergeStats = %v, want error containing %q", err, tc.wantErr)
			}
		})
	}
}

// withCatalogRows replaces the memory catalog backing d with rows that keep
// pointing at already-stored objects. Tests use it to simulate a catalog whose
// metadata disagrees with its Parquet data without touching production merge
// code.
func withCatalogRows(t *testing.T, d *testDeps, files ...catalog.File) {
	t.Helper()
	mem := catalog.NewMemory()
	if err := mem.AddFiles(t.Context(), files...); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	d.cat.Catalog = mem
}

// TestCycleRejectsWrongCatalogMetadata is the real-cycle regression for a
// catalog row that disagrees with its object. The merge must refuse to swap
// the catalog: every source row and object stays live, the successful upload
// remains as one unreferenced orphan, and ReplaceFiles is never called.
func TestCycleRejectsWrongCatalogMetadata(t *testing.T) {
	base := schema.TS(testHour)
	cases := []struct {
		name    string
		corrupt func(f1, f2 *catalog.File)
		wantErr string
	}{
		{
			name:    "row count",
			corrupt: func(f1, f2 *catalog.File) { f1.Rows-- },
			wantErr: "rows",
		},
		{
			name:    "narrowed min timestamp",
			corrupt: func(f1, f2 *catalog.File) { f1.MinTS = at(20) },
			wantErr: "min ts",
		},
		{
			name:    "narrowed max timestamp",
			corrupt: func(f1, f2 *catalog.File) { f2.MaxTS = at(40) },
			wantErr: "max ts",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeps(t, time.Hour, 512*config.MiB)
			s := mustService(t, d)
			sch := baseSchema(t)

			f1 := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
				tss(base+10, base+20, base+30), strs("a0", "a1", "a2"),
				mapVals(map[string]string{}, map[string]string{}, map[string]string{}),
				mapVals(map[string]string{}, map[string]string{}, map[string]string{}))
			f2 := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
				tss(base+20, base+40, base+50), strs("b0", "b1", "b2"),
				mapVals(map[string]string{}, map[string]string{}, map[string]string{}),
				mapVals(map[string]string{}, map[string]string{}, map[string]string{}))
			tc.corrupt(&f1, &f2)
			withCatalogRows(t, d, f1, f2)

			err := s.cycle(t.Context())
			if err == nil {
				t.Fatal("cycle succeeded, want metadata verification failure")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("cycle error = %v, want it to contain %q", err, tc.wantErr)
			}
			if got := d.cat.replaceCalls.Load(); got != 0 {
				t.Fatalf("ReplaceFiles calls = %d, want 0", got)
			}

			srcs := []catalog.File{f1, f2}
			live := liveFiles(t, d.cat, tenant.Default, "svc")
			if len(live) != len(srcs) {
				t.Fatalf("live files = %d, want both sources intact", len(live))
			}
			liveKeys := make([]string, len(live))
			for i, f := range live {
				liveKeys[i] = f.Key
				if _, err := d.fs.Stat(t.Context(), f.Key); err != nil {
					t.Fatalf("source object %s: %v", f.Key, err)
				}
			}
			for _, src := range srcs {
				if !slices.Contains(liveKeys, src.Key) {
					t.Fatalf("source row %s missing from live catalog %v", src.Key, liveKeys)
				}
			}

			orphan, _ := d.store.lastPut.Load().(string)
			if orphan == "" {
				t.Fatal("no uploaded object recorded")
			}
			if _, err := d.fs.Stat(t.Context(), orphan); err != nil {
				t.Fatalf("orphan object %s missing: %v", orphan, err)
			}
			if slices.Contains(liveKeys, orphan) {
				t.Fatalf("orphan %s is referenced by the catalog", orphan)
			}
			if got := countObjects(t, d, tenant.Default, "svc"); got != len(srcs)+1 {
				t.Fatalf("stored objects = %d, want %d sources plus one orphan", got, len(srcs))
			}
			if got := metricValue(t, d.reg, "parkana_compactor_merge_errors_total"); got != 1 {
				t.Fatalf("merge_errors_total = %v, want 1", got)
			}
		})
	}
}

// countObjects returns the number of objects under a stream's log prefix.
func countObjects(t *testing.T, d *testDeps, tid tenant.ID, stream string) int {
	t.Helper()
	n := 0
	for _, err := range d.fs.List(t.Context(), objstore.LogPrefix(tid, stream)) {
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		n++
	}
	return n
}

// requireSidecar fetches the sidecar object of key through the service's
// store view, decodes it and checks that no substring window of any line is
// proven absent: a sidecar that misses a contained needle would make queries
// drop matching files. The fixed absent probe prunes for these deterministic
// fixtures and the pinned Bloom implementation; false positives remain
// permitted.
func requireSidecar(t *testing.T, d *testDeps, key string, lines []string) {
	t.Helper()
	sk := objstore.SidecarKey(key)
	body, err := d.store.Get(t.Context(), sk)
	if err != nil {
		t.Fatalf("get sidecar %s: %v", sk, err)
	}
	data, readErr := io.ReadAll(body)
	closeErr := body.Close()
	if readErr != nil {
		t.Fatalf("read sidecar %s: %v", sk, readErr)
	}
	if closeErr != nil {
		t.Fatalf("close sidecar %s: %v", sk, closeErr)
	}
	f, err := index.Decode(data)
	if err != nil {
		t.Fatalf("decode sidecar %s: %v", sk, err)
	}
	if got := f.GramLen(); got != index.DefaultParams.N {
		t.Fatalf("sidecar %s gram length = %d, want %d", sk, got, index.DefaultParams.N)
	}
	for _, line := range lines {
		for i := range len(line) {
			for j := i + 1; j <= len(line); j++ {
				if !f.MayContain(line[i:j]) {
					t.Fatalf("sidecar %s: false negative for %q (window %d:%d of %q)", sk, line[i:j], i, j, line)
				}
			}
		}
	}
	if f.MayContain("ZZZZnot-a-merged-line") {
		t.Fatalf("sidecar %s: absent needle %q does not prune", sk, "ZZZZnot-a-merged-line")
	}
}

// TestMergeHourKeepsCaseCollidingPromotionsApart: a file promoting "Level"
// must not be merged with files promoting "level" (DuckDB would read both as
// one column); the compatible pair still merges.
func TestMergeHourKeepsCaseCollidingPromotionsApart(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	s := mustService(t, d)
	base := schema.TS(testHour)
	put := func(us int64, col, value string) catalog.File {
		sch := baseSchema(t, col)
		return putFile(t, d, tenant.Default, "svc", testHour, sch, []string{col},
			[]any{base + us}, []any{"line"}, []any{map[string]string{}}, []any{map[string]string{}}, []any{value})
	}
	a := put(0, "level", "info")
	upper := put(1, "Level", "High")
	b := put(2, "level", "warn")
	if err := s.mergeHour(t.Context(), tenant.Default, "svc", []catalog.File{a, upper, b}); err != nil {
		t.Fatalf("mergeHour: %v", err)
	}
	live := liveFiles(t, d.cat, tenant.Default, "svc")
	if len(live) != 2 {
		t.Fatalf("live files = %d, want 2 (merged level pair + the Level file)", len(live))
	}
	for _, f := range live {
		_, lower := f.LabelValues["level"]
		_, cap := f.LabelValues["Level"]
		if lower && cap {
			t.Fatalf("file %s mixes level and Level columns", f.Key)
		}
	}
}

// mergeLogCapture keeps the compactor's structured log records so a test can
// assert the deduplicated count reported at the merge seam. It is per-test
// state, not a package hook.
type mergeLogCapture struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (h *mergeLogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (h *mergeLogCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.recs = append(h.recs, r.Clone())
	return nil
}

func (h *mergeLogCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *mergeLogCapture) WithGroup(string) slog.Handler      { return h }

// find returns the first captured record with msg and the requested attr.
func (h *mergeLogCapture) find(msg, key string) (slog.Value, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.recs {
		if r.Message != msg {
			continue
		}
		var v slog.Value
		found := false
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key {
				v = a.Value
				found = true
				return false
			}
			return true
		})
		if found {
			return v, true
		}
	}
	return slog.Value{}, false
}

// TestCycleMergeDedupAccounting runs a real compaction over two files holding
// exact duplicates, one all-duplicate cross-input row and metadata-distinct
// rows at the same timestamp. The published output must hold exactly the
// surviving physical rows, report the removed count at the seam, keep complete
// summaries, a sidecar covering every survivor, and leave the source objects
// intact for cleanup.
func TestCycleMergeDedupAccounting(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	logs := &mergeLogCapture{}
	d.deps.Logger = slog.New(logs)
	s := mustService(t, d)
	sch := baseSchema(t, "level")
	base := schema.TS(testHour)

	f1 := putFile(t, d, tenant.Default, "svc", testHour, sch, []string{"level"},
		tss(base+10, base+20, base+40),
		strs("keep-a", "dup", "same-ts"),
		mapVals(map[string]string{"app": "web"}, map[string]string{"app": "web"}, map[string]string{"app": "db"}),
		mapVals(map[string]string{"m": "1"}, map[string]string{}, map[string]string{"m": "2"}),
		strs("info", "warn", "info"),
	)
	f2 := putFile(t, d, tenant.Default, "svc", testHour, sch, []string{"level"},
		tss(base+30, base+40, base+40, base+50),
		strs("keep-b", "same-ts", "same-ts", "dup"),
		mapVals(map[string]string{"app": "db"}, map[string]string{"app": "db"}, map[string]string{"app": "db"}, map[string]string{"app": "web"}),
		mapVals(map[string]string{}, map[string]string{"m": "2"}, map[string]string{"m": "9"}, map[string]string{}),
		// The second same-ts row differs from the first ONLY in metadata, so
		// the survivor proves metadata participates in the identity.
		strs("error", "info", "info", "warn"),
	)

	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cycle: %v", err)
	}

	live := liveFiles(t, d.cat, tenant.Default, "svc")
	if len(live) != 1 {
		t.Fatalf("live files = %d, want one merged output", len(live))
	}
	out := live[0]
	if out.Rows != 6 {
		t.Fatalf("merged catalog Rows = %d, want 6 physical rows (7 inputs, 1 duplicate)", out.Rows)
	}
	if !out.LabelsComplete || !out.HasSidecar {
		t.Fatalf("merged output flags = complete=%v sidecar=%v, want both true", out.LabelsComplete, out.HasSidecar)
	}
	if got, want := out.LabelValues["level"], []string{"error", "info", "warn"}; !slices.Equal(got, want) {
		t.Fatalf("LabelValues[level] = %v, want %v", got, want)
	}
	if v, ok := logs.find("compacted files", "deduplicated"); !ok || v.Int64() != 1 {
		t.Fatalf("logged deduplicated = %v (found=%v), want 1", v, ok)
	}
	if v, ok := logs.find("compacted files", "rows"); !ok || v.Int64() != 6 {
		t.Fatalf("logged rows = %v (found=%v), want 6", v, ok)
	}

	_, rows := readObjectRows(t, d, tenant.Default, out.Key)
	want := [][]any{
		{base + 10, "keep-a", map[string]string{"app": "web"}, map[string]string{"m": "1"}, "info"},
		{base + 20, "dup", map[string]string{"app": "web"}, map[string]string{}, "warn"},
		{base + 30, "keep-b", map[string]string{"app": "db"}, map[string]string{}, "error"},
		{base + 40, "same-ts", map[string]string{"app": "db"}, map[string]string{"m": "2"}, "info"},
		{base + 40, "same-ts", map[string]string{"app": "db"}, map[string]string{"m": "9"}, "info"},
		{base + 50, "dup", map[string]string{"app": "web"}, map[string]string{}, "warn"},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("merged rows =\n%v\nwant\n%v", rows, want)
	}
	requireSidecar(t, d, out.Key, []string{"keep-a", "dup", "keep-b", "same-ts"})

	for _, src := range []catalog.File{f1, f2} {
		if _, err := d.fs.Stat(t.Context(), src.Key); err != nil {
			t.Fatalf("source object %s gone: %v", src.Key, err)
		}
	}
}

// TestCycleAllDuplicatesCollapseToOneRow is the legitimate extreme: every
// physical input row is an exact duplicate, so the merge keeps the first
// survivor and still publishes a nonempty one-row file.
func TestCycleAllDuplicatesCollapseToOneRow(t *testing.T) {
	d := newTestDeps(t, time.Hour, 512*config.MiB)
	logs := &mergeLogCapture{}
	d.deps.Logger = slog.New(logs)
	s := mustService(t, d)
	sch := baseSchema(t)
	base := schema.TS(testHour)

	put := func() catalog.File {
		return putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
			tss(base+10), strs("only"), mapVals(map[string]string{"app": "web"}), mapVals(map[string]string{"m": "1"}))
	}
	put()
	put()

	if err := s.cycle(t.Context()); err != nil {
		t.Fatalf("cycle: %v", err)
	}
	live := liveFiles(t, d.cat, tenant.Default, "svc")
	if len(live) != 1 {
		t.Fatalf("live files = %d, want the merged output", len(live))
	}
	if live[0].Rows != 1 {
		t.Fatalf("merged Rows = %d, want 1 survivor", live[0].Rows)
	}
	if v, ok := logs.find("compacted files", "deduplicated"); !ok || v.Int64() != 1 {
		t.Fatalf("logged deduplicated = %v (found=%v), want 1", v, ok)
	}
	_, rows := readObjectRows(t, d, tenant.Default, live[0].Key)
	want := [][]any{{base + 10, "only", map[string]string{"app": "web"}, map[string]string{"m": "1"}}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
	requireSidecar(t, d, live[0].Key, []string{"only"})
}

// TestCycleRejectsLyingMergeStats injects malformed accounting at the merge
// seam and proves publication is refused at the row guard: sources and their
// objects stay live, ReplaceFiles never runs, and the upload is left as an
// unreferenced orphan. A wrong removed count must not be allowed to hide
// dropped rows.
func TestCycleRejectsLyingMergeStats(t *testing.T) {
	base := schema.TS(testHour)
	cases := []struct {
		name    string
		mutate  func(*storage.FileStats)
		wantErr string
	}{
		{
			name:    "missing rows without removed count",
			mutate:  func(st *storage.FileStats) { st.Rows-- },
			wantErr: "deduplicated",
		},
		{
			name:    "removed count too large",
			mutate:  func(st *storage.FileStats) { st.DeduplicatedRows++ },
			wantErr: "deduplicated",
		},
		{
			name:    "negative removed count",
			mutate:  func(st *storage.FileStats) { st.DeduplicatedRows = -1 },
			wantErr: "nonnegative",
		},
		{
			name:    "overflowing removed count",
			mutate:  func(st *storage.FileStats) { st.DeduplicatedRows = math.MaxInt64 },
			wantErr: "overflows",
		},
		{
			name: "zero output claiming every row duplicated",
			mutate: func(st *storage.FileStats) {
				st.DeduplicatedRows = st.Rows + st.DeduplicatedRows
				st.Rows = 0
			},
			wantErr: "zero rows",
		},
		{
			name:    "negative output rows",
			mutate:  func(st *storage.FileStats) { st.Rows = -1 },
			wantErr: "nonnegative",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestDeps(t, time.Hour, 512*config.MiB)
			s := mustService(t, d)
			// Keep the failure probe on the merge path: a later backfill
			// pass would overwrite lastPut with a source sidecar and make an
			// orphan-key assertion meaningless.
			s.cfg.SidecarBackfill = false
			sch := baseSchema(t)

			f1 := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
				tss(base+10, base+20), strs("a0", "a1"),
				mapVals(map[string]string{}, map[string]string{}), mapVals(map[string]string{}, map[string]string{}))
			f2 := putFile(t, d, tenant.Default, "svc", testHour, sch, nil,
				tss(base+30, base+40), strs("b0", "b1"),
				mapVals(map[string]string{}, map[string]string{}), mapVals(map[string]string{}, map[string]string{}))

			s.merge = func(ctx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error) {
				stats, err := storage.Merge(ctx, w, inputs, promoted)
				if err == nil {
					tc.mutate(&stats)
				}
				return stats, err
			}

			err := s.cycle(t.Context())
			if err == nil {
				t.Fatal("cycle succeeded, want row accounting rejection")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("cycle error = %v, want the %q row-accounting guard", err, tc.wantErr)
			}
			if got := d.cat.replaceCalls.Load(); got != 0 {
				t.Fatalf("ReplaceFiles calls = %d, want 0", got)
			}
			if got := metricValue(t, d.reg, "parkana_compactor_merge_errors_total"); got != 1 {
				t.Fatalf("merge_errors_total = %v, want 1", got)
			}

			live := liveFiles(t, d.cat, tenant.Default, "svc")
			if len(live) != 2 {
				t.Fatalf("live files = %d, want both sources intact", len(live))
			}
			liveKeys := make([]string, len(live))
			for i, f := range live {
				liveKeys[i] = f.Key
				if _, err := d.fs.Stat(t.Context(), f.Key); err != nil {
					t.Fatalf("source object %s: %v", f.Key, err)
				}
			}
			for _, src := range []catalog.File{f1, f2} {
				if !slices.Contains(liveKeys, src.Key) {
					t.Fatalf("source row %s missing from live catalog %v", src.Key, liveKeys)
				}
			}

			orphan, _ := d.store.lastPut.Load().(string)
			if orphan == "" {
				t.Fatal("no uploaded object recorded")
			}
			if !strings.HasSuffix(orphan, objstore.ExtData) || strings.HasSuffix(orphan, objstore.ExtSidecar) {
				t.Fatalf("last Put = %q, want the fresh merge data key, not a sidecar", orphan)
			}
			if _, err := d.fs.Stat(t.Context(), orphan); err != nil {
				t.Fatalf("orphan object %s missing: %v", orphan, err)
			}
			if slices.Contains(liveKeys, orphan) {
				t.Fatalf("orphan %s is referenced by the catalog", orphan)
			}
			// The orphan holds the real merge output: its row guard failed on
			// the reported stats, not on corrupt or missing bytes.
			_, orphanRows := readObjectRows(t, d, tenant.Default, orphan)
			wantOrphan := [][]any{
				{base + 10, "a0", map[string]string{}, map[string]string{}},
				{base + 20, "a1", map[string]string{}, map[string]string{}},
				{base + 30, "b0", map[string]string{}, map[string]string{}},
				{base + 40, "b1", map[string]string{}, map[string]string{}},
			}
			if !reflect.DeepEqual(orphanRows, wantOrphan) {
				t.Fatalf("orphan rows = %v, want the complete merge output %v", orphanRows, wantOrphan)
			}
		})
	}
}
