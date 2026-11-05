// Stored-row identity for the SQL metadata endpoints: stats and volume count
// exact stored copies once, including when overlapping OR selector arms both
// match a row, while rows differing only in metadata or full labels survive.
package query

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// TestMetaSQLDedupStatsAndVolume checks the physical FS backend: stats entries
// and bytes follow the distinct stored rows across promotion layouts, physical
// metadata key orders and nil/empty metadata; chunks stays the physical file
// count. Volume totals match stats bytes under every projection, and full
// labels outside the env projection still contribute their own rows.
func TestMetaSQLDedupStatsAndVolume(t *testing.T) {
	physical, _ := dedupFixtures()
	env := openSourceEnv(t, physical, defaultStreamLabels)
	r := newMetadataReader(env.source)
	q := metaWindow(tenant.Default, []plan.Matcher{metaEq("service_name", "dedup")})

	stats, err := r.Stats(t.Context(), q)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	wantEntries := uint64(dedupDistinctRows())
	wantBytes := uint64(dedupDistinctBytes())
	if stats.Entries != wantEntries || stats.Bytes != wantBytes || stats.Streams != 2 || stats.Chunks != uint64(len(physical)) {
		t.Fatalf("Stats = %+v, want %d distinct entries, %d bytes, 2 streams and %d physical chunks",
			stats, wantEntries, wantBytes, len(physical))
	}

	volumeTotal := func(req metaVolumeRequest) map[string]uint64 {
		t.Helper()
		entries, err := r.Volume(t.Context(), req)
		if err != nil {
			t.Fatalf("Volume: %v", err)
		}
		out := make(map[string]uint64)
		for _, e := range entries {
			var total uint64
			for _, b := range e.Buckets {
				total += b.Volume
			}
			out[e.Metric.String()] = total
		}
		return out
	}

	instant := metaVolumeRequest{Query: q}
	if got := volumeTotal(instant); len(got) != 1 || got[`{service_name="dedup"}`] != wantBytes {
		t.Fatalf("instant volume = %v, want one {service_name=\"dedup\"} of %d distinct bytes", got, wantBytes)
	}

	byService := instant
	byService.TargetLabels = []string{"service_name"}
	if got := volumeTotal(byService); len(got) != 1 || got[`{service_name="dedup"}`] != wantBytes {
		t.Fatalf("service_name volume = %v, want %d: rows differing outside the projection still count", got, wantBytes)
	}

	byEnv := instant
	byEnv.TargetLabels = []string{"env"}
	wantEnv := map[string]uint64{
		`{env="prod"}`: wantBytes - uint64(len("survivor labels")),
		`{env="dev"}`:  uint64(len("survivor labels")),
	}
	if got := volumeTotal(byEnv); !maps.Equal(got, wantEnv) {
		t.Fatalf("env volume = %v, want %v", got, wantEnv)
	}

	// aggregateBy=labels groups by every label name, still after row identity.
	// Its Metric carries the raw name with an empty value ({name=""}), so the
	// totals are keyed by that shape.
	byLabels := instant
	byLabels.AggregateByLabels = true
	wantByLabels := map[string]uint64{
		`{env=""}`:          wantBytes,
		`{level=""}`:        wantBytes,
		`{service_name=""}`: wantBytes,
	}
	if got := volumeTotal(byLabels); !maps.Equal(got, wantByLabels) {
		t.Fatalf("labels volume = %v, want %v", got, wantByLabels)
	}

	// A positive step partitions the same distinct rows over time: the sum of
	// every range bucket must equal the instant total, over more than one
	// bucket.
	byRange := instant
	byRange.Step = time.Second
	rangeEntries, err := r.Volume(t.Context(), byRange)
	if err != nil {
		t.Fatalf("range Volume: %v", err)
	}
	var rangeTotal uint64
	rangeBuckets := 0
	for _, e := range rangeEntries {
		rangeBuckets += len(e.Buckets)
		for _, b := range e.Buckets {
			rangeTotal += b.Volume
		}
	}
	if rangeTotal != wantBytes {
		t.Fatalf("range volume total = %d, want the %d distinct instant bytes", rangeTotal, wantBytes)
	}
	if rangeBuckets <= 1 {
		t.Fatalf("range volume used %d buckets, want the rows spread over more than one", rangeBuckets)
	}
}

// TestMetaSQLORDuplicateRowsKeepIdentity updates the old raw-multiplicity
// expectation: two exact copies count once even when both OR arms match them,
// but a row differing only in metadata and a row differing only in full labels
// survive. It also drives IncludeMeta=false Aggregate, which must deduplicate
// on the complete row identity (metadata included), not on labels alone.
func TestMetaSQLORDuplicateRowsKeepIdentity(t *testing.T) {
	start := plantest.T0
	base := metaRow(start, "same", "service_name", "dup", "env", "prod")
	metaVariant := base
	metaVariant.Meta = logs.FromMap(map[string]string{"trace_id": "a"})
	labelVariant := base
	labelVariant.Labels = logs.FromMap(map[string]string{"service_name": "dup", "env": "dev"})
	files := []plantest.File{{
		Tenant: tenant.Default, Stream: "dup",
		Rows: []plan.Row{base, base, metaVariant, labelVariant},
	}}
	env := openSourceEnv(t, files, defaultStreamLabels)
	r := newMetadataReader(env.source)
	q := metaQuery{Tenant: tenant.Default, Start: start, End: start.Add(time.Hour)}
	q.Groups = [][]plan.Matcher{
		{metaEq("service_name", "dup"), metaEq("env", "prod")},
		{plan.MustMatcher(plan.MatchRegexp, "env", "prod|dev")},
	}

	// Arm coverage guard: the prod row must match both arms (the overlap the
	// test is about), the dev row only the second. An equality literal such as
	// env=prod|dev matches neither value, which would make the overlap vacuous.
	matchesArm := func(r plan.Row, arm int) bool {
		return metaGroupMatches(r.Labels, [][]plan.Matcher{q.Groups[arm]})
	}
	if !matchesArm(base, 0) || !matchesArm(base, 1) {
		t.Fatalf("prod row must match both OR arms (arm1=%v arm2=%v): %v",
			matchesArm(base, 0), matchesArm(base, 1), q.Groups)
	}
	if matchesArm(labelVariant, 0) || !matchesArm(labelVariant, 1) {
		t.Fatalf("dev row must match only the second OR arm (arm1=%v arm2=%v): %v",
			matchesArm(labelVariant, 0), matchesArm(labelVariant, 1), q.Groups)
	}

	stats, err := r.Stats(t.Context(), q)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Entries != 3 || stats.Bytes != 12 || stats.Streams != 2 || stats.Chunks != 1 {
		t.Fatalf("Stats = %+v, want 3 distinct rows (exact copy collapsed, metadata and label survivors), 12 bytes, 2 streams, 1 chunk", stats)
	}
	sets, err := r.Series(t.Context(), q)
	if err != nil {
		t.Fatalf("Series: %v", err)
	}
	if len(sets) != 2 {
		t.Fatalf("Series = %v, want the two distinct label sets", sets)
	}

	vol, err := r.Volume(t.Context(), metaVolumeRequest{Query: q})
	if err != nil {
		t.Fatalf("Volume: %v", err)
	}
	wantVolume := map[string]uint64{
		`{env="prod", service_name="dup"}`: 8,
		`{env="dev", service_name="dup"}`:  4,
	}
	gotVolume := make(map[string]uint64)
	for _, e := range vol {
		gotVolume[e.Metric.String()] = e.Buckets[0].Volume
	}
	if !maps.Equal(gotVolume, wantVolume) {
		t.Fatalf("Volume = %v, want %v (projected union of both matcher names)", gotVolume, wantVolume)
	}
	byLabels, err := r.Volume(t.Context(), metaVolumeRequest{Query: q, AggregateByLabels: true})
	if err != nil {
		t.Fatalf("aggregateBy=labels Volume: %v", err)
	}
	wantLabels := map[string]uint64{`{env=""}`: 12, `{service_name=""}`: 12}
	gotLabels := make(map[string]uint64)
	for _, e := range byLabels {
		gotLabels[e.Metric.String()] = e.Buckets[0].Volume
	}
	if !maps.Equal(gotLabels, wantLabels) {
		t.Fatalf("aggregateBy=labels Volume = %v, want %v", gotLabels, wantLabels)
	}

	sel := plan.Selection{
		Tenant:   tenant.Default,
		Matchers: []plan.Matcher{metaEq("service_name", "dup")},
		From:     start,
		Through:  start.Add(time.Hour),
	}
	for _, tc := range []struct {
		name string
		agg  plan.BucketAgg
		want map[string]string
	}{
		{
			name: "include-meta-false",
			agg:  plan.BucketAgg{Width: time.Hour, Origin: start},
			want: map[string]string{
				`{env="prod", service_name="dup"}`: "2|8",
				`{env="dev", service_name="dup"}`:  "1|4",
			},
		},
		{
			name: "include-meta-true",
			agg:  plan.BucketAgg{Width: time.Hour, Origin: start, IncludeMeta: true},
			want: map[string]string{
				`{env="prod", service_name="dup"}|{}`:             "1|4",
				`{env="prod", service_name="dup"}|{trace_id="a"}`: "1|4",
				`{env="dev", service_name="dup"}|{}`:              "1|4",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buckets, err := collectBuckets(env.source.Aggregate(t.Context(), sel, tc.agg))
			if err != nil {
				t.Fatalf("Aggregate: %v", err)
			}
			got := make(map[string]string)
			for _, b := range buckets {
				key := b.Labels.String()
				if tc.agg.IncludeMeta {
					key += "|" + b.Meta.String()
				}
				got[key] = fmtIntPair(b.Count, b.Bytes)
			}
			if !slices.Equal(sortedKeys(got), sortedKeys(tc.want)) {
				t.Fatalf("buckets = %v, want %v", got, tc.want)
			}
			for k, want := range tc.want {
				if got[k] != want {
					t.Errorf("bucket %s = %s, want %s", k, got[k], want)
				}
			}
		})
	}
}

func fmtIntPair(count, bytes int64) string {
	return fmt.Sprintf("%d|%d", count, bytes)
}

func sortedKeys(m map[string]string) []string {
	return slices.Sorted(maps.Keys(m))
}
