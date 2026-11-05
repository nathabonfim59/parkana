package plantest

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// RunSource checks a Source against MemSource on the Dataset. newSource gets
// the dataset and returns a Source reading it (Load it into a store and
// catalog first); Run calls it once.
func RunSource(t *testing.T, newSource func(t *testing.T, files []File) plan.Source) {
	files := Dataset()
	src := newSource(t, files)
	ref := Mem(files)

	eq := func(name, v string) plan.Matcher { return plan.MustMatcher(plan.MatchEqual, name, v) }
	ne := func(name, v string) plan.Matcher { return plan.MustMatcher(plan.MatchNotEqual, name, v) }
	re := func(name, v string) plan.Matcher { return plan.MustMatcher(plan.MatchRegexp, name, v) }
	nre := func(name, v string) plan.Matcher { return plan.MustMatcher(plan.MatchNotRegexp, name, v) }
	contains := func(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineContains, v) }
	notContains := func(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineNotContains, v) }
	lre := func(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineRegexp, v) }
	lnre := func(v string) plan.LineFilter { return plan.MustLineFilter(plan.LineNotRegexp, v) }

	from, through := T0.Add(-time.Hour), T0.Add(2*time.Hour)
	matcherSets := map[string][]plan.Matcher{
		"stream":                   {eq("service_name", "api")},
		"promoted-and-map-label":   {eq("service_name", "api"), eq("level", "error")},
		"not-equal":                {eq("service_name", "api"), ne("level", "info")},
		"regexp-alternation":       {re("service_name", "api|web")},
		"regexp-anchored":          {re("service_name", "ap")},
		"regexp-case":              {re("env", "PROD")},
		"regexp-case-insensitive":  {re("env", "(?i)PROD")},
		"promoted-pod":             {eq("service_name", "web"), eq("pod", "w2")},
		"job-only":                 {eq("job", "batch")},
		"missing-equals-empty":     {eq("service_name", "api"), eq("foo", "")},
		"label-absent":             {eq("service_name", "api"), eq("level", "")},
		"label-present":            {eq("service_name", "api"), re("level", ".+")},
		"not-regexp-missing-label": {re("service_name", ".+"), nre("env", "prod|dev")},
		"no-such-stream":           {eq("service_name", "nope")},
		"odd-label-name":           {eq("Weird_Label9", "x")},
		"not-equal-on-stream":      {re("service_name", ".*"), ne("service_name", "api"), ne("job", "")},
		"case-promoted":            {eq("Level", "High")},
		"case-promoted-absent":     {eq("service_name", "api"), eq("Level", ""), eq("level", "info")},
		"case-both":                {eq("Level", "Low"), eq("level", "info")},
		"case-base-names":          {eq("LINE", "x"), eq("Labels", "y")},
		"case-lower-only":          {re("level", "(?i)HIGH|low")},
	}
	filterSets := map[string][]plan.LineFilter{
		"none":              nil,
		"contains":          {contains("error")},
		"percent":           {contains("%")},
		"underscore":        {contains("_")},
		"quote":             {contains("'")},
		"backslash":         {contains(`\`)},
		"unicode":           {contains("café")},
		"newline":           {contains("\n")},
		"not-contains":      {notContains("error")},
		"regexp-ci":         {lre("(?i)error")},
		"regexp-json":       {lre(`"status":5\d\d`)},
		"not-regexp-anchor": {lnre("^GET")},
		"empty-line":        {lre("^$")},
		"combined":          {contains("error"), notContains("timeout"), lre("[a-z]")},
		// Needles for sidecar pruning (phase 3): in one file only, in none,
		// shorter than a gram, in two files that must both be read, and two
		// filters that each match a different file (no row has both).
		"needle-one-file":  {contains("boundary exact")},
		"needle-absent":    {contains("no-such-needle-zq")},
		"needle-short":     {contains("GE")},
		"needle-two-files": {contains("POST /api/orders")},
		"needle-and-split": {contains("boundary"), contains("bulk 12")},
		"needle-in-dups":   {contains("timeout"), contains("conn")},
	}

	t.Run("Select", func(t *testing.T) {
		for mname, ms := range sortedMap(matcherSets) {
			for _, fname := range []string{"none", "contains"} {
				for _, dir := range []plan.Direction{plan.Forward, plan.Backward} {
					sel := plan.Selection{Tenant: tenant.Default, Matchers: ms, LineFilters: filterSets[fname], From: from, Through: through, Direction: dir}
					t.Run(fmt.Sprintf("%s/%s/%s", mname, fname, dir), func(t *testing.T) { compareSelect(t, src, ref, sel) })
				}
			}
		}
		for fname, fs := range sortedMap(filterSets) {
			sel := plan.Selection{Tenant: tenant.Default, Matchers: matcherSets["regexp-alternation"], LineFilters: fs, From: from, Through: through, Direction: plan.Forward}
			t.Run("filter/"+fname, func(t *testing.T) { compareSelect(t, src, ref, sel) })
			sel.Matchers = []plan.Matcher{re("job", ".+")}
			t.Run("filter-batch/"+fname, func(t *testing.T) { compareSelect(t, src, ref, sel) })
		}
	})

	t.Run("TimeRange", func(t *testing.T) {
		b := T0.Add(60 * time.Second)
		ranges := map[string][2]time.Time{
			"from-inclusive-through-exclusive": {b, T0.Add(120 * time.Second)},
			"from-after-boundary":              {b.Add(time.Microsecond), T0.Add(120 * time.Second)},
			"one-microsecond":                  {b, b.Add(time.Microsecond)},
			"through-at-boundary":              {T0, b},
			"through-after-boundary":           {T0, b.Add(time.Microsecond)},
			"empty-window":                     {b, b},
			"no-files":                         {T0.Add(-48 * time.Hour), T0.Add(-47 * time.Hour)},
			"inside-bulk":                      {T0.Add(401 * time.Second), T0.Add(401*time.Second + 7*time.Millisecond)},
			"across-files":                     {T0.Add(150 * time.Second), T0.Add(250 * time.Second)},
		}
		for name, r := range sortedMap(ranges) {
			for _, dir := range []plan.Direction{plan.Forward, plan.Backward} {
				sel := plan.Selection{Tenant: tenant.Default, Matchers: matcherSets["stream"], From: r[0], Through: r[1], Direction: dir}
				t.Run(name+"/"+dir.String(), func(t *testing.T) { compareSelect(t, src, ref, sel) })
			}
		}
	})

	t.Run("Limit", func(t *testing.T) {
		for _, limit := range []int{1, 7, 150, 2500, 100000} {
			for _, dir := range []plan.Direction{plan.Forward, plan.Backward} {
				for _, mname := range []string{"stream", "regexp-alternation", "promoted-and-map-label"} {
					sel := plan.Selection{Tenant: tenant.Default, Matchers: matcherSets[mname], From: from, Through: through, Direction: dir, Limit: limit}
					t.Run(fmt.Sprintf("%d/%s/%s", limit, dir, mname), func(t *testing.T) { compareSelect(t, src, ref, sel) })
				}
				sel := plan.Selection{Tenant: tenant.Default, Matchers: matcherSets["stream"], LineFilters: filterSets["contains"],
					From: T0.Add(100 * time.Second), Through: T0.Add(500 * time.Second), Direction: dir, Limit: limit}
				t.Run(fmt.Sprintf("%d/%s/filtered-window", limit, dir), func(t *testing.T) { compareSelect(t, src, ref, sel) })
			}
		}
	})

	t.Run("Tenant", func(t *testing.T) {
		sel := plan.Selection{Tenant: OtherTenant, Matchers: matcherSets["stream"], From: from, Through: through, Direction: plan.Forward}
		compareSelect(t, src, ref, sel)
		sel.Tenant = "nobody"
		compareSelect(t, src, ref, sel)
		agg := plan.BucketAgg{Width: time.Minute, Origin: T0}
		sel.Tenant = OtherTenant
		compareAggregate(t, src, ref, sel, agg)
	})

	t.Run("Aggregate", func(t *testing.T) {
		aggs := map[string]plan.BucketAgg{
			"minute":            {Width: time.Minute, Origin: T0},
			"odd-width-shifted": {Width: 7 * time.Second, Origin: T0.Add(time.Microsecond)},
			"hour-origin-early": {Width: time.Hour, Origin: T0.Add(-30 * time.Minute)},
			"origin-after-data": {Width: 10 * time.Second, Origin: T0.Add(3 * time.Hour)},
			"tiny":              {Width: 3 * time.Millisecond, Origin: T0.Add(400 * time.Second)},
		}
		for aname, agg := range sortedMap(aggs) {
			for _, mname := range []string{"stream", "promoted-and-map-label", "regexp-alternation", "job-only", "label-absent", "no-such-stream", "case-promoted", "case-both"} {
				for _, fname := range []string{"none", "contains", "unicode", "regexp-json"} {
					sel := plan.Selection{Tenant: tenant.Default, Matchers: matcherSets[mname], LineFilters: filterSets[fname], From: from, Through: through}
					t.Run(fmt.Sprintf("%s/%s/%s", aname, mname, fname), func(t *testing.T) { compareAggregate(t, src, ref, sel, agg) })
				}
			}
			sel := plan.Selection{Tenant: tenant.Default, Matchers: matcherSets["stream"], From: T0.Add(60 * time.Second), Through: T0.Add(60*time.Second + time.Microsecond)}
			t.Run(aname+"/boundary-window", func(t *testing.T) { compareAggregate(t, src, ref, sel, agg) })
			// IncludeMeta: the api rows carry trace_id on some rows and none on
			// others, batch rows carry run=r1 on all of them.
			metaAgg := agg
			metaAgg.IncludeMeta = true
			for _, mname := range []string{"stream", "job-only", "regexp-alternation", "case-promoted"} {
				sel := plan.Selection{Tenant: tenant.Default, Matchers: matcherSets[mname], From: from, Through: through}
				t.Run(fmt.Sprintf("%s/meta/%s", aname, mname), func(t *testing.T) { compareAggregate(t, src, ref, sel, metaAgg) })
			}
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		bad := map[string]plan.Selection{
			"no-matchers": {Tenant: tenant.Default, From: from, Through: through},
			"no-range":    {Tenant: tenant.Default, Matchers: matcherSets["stream"]},
			"bad-tenant":  {Tenant: "a/b", Matchers: matcherSets["stream"], From: from, Through: through},
			"raw-matcher": {Tenant: tenant.Default, Matchers: []plan.Matcher{{Type: plan.MatchRegexp, Name: "a", Value: "x"}}, From: from, Through: through},
		}
		for name, sel := range sortedMap(bad) {
			t.Run(name, func(t *testing.T) {
				if _, err := collect(src.Select(t.Context(), sel)); err == nil {
					t.Error("Select: no error")
				}
				if _, err := collect(src.Aggregate(t.Context(), sel, plan.BucketAgg{Width: time.Minute, Origin: T0})); err == nil {
					t.Error("Aggregate: no error")
				}
			})
		}
		sel := plan.Selection{Tenant: tenant.Default, Matchers: matcherSets["stream"], From: from, Through: through}
		if _, err := collect(src.Aggregate(t.Context(), sel, plan.BucketAgg{Origin: T0})); err == nil {
			t.Error("Aggregate with zero width: no error")
		}
	})

	t.Run("Canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		sel := plan.Selection{Tenant: tenant.Default, Matchers: matcherSets["stream"], From: from, Through: through}
		if _, err := collect(src.Select(ctx, sel)); !errors.Is(err, context.Canceled) {
			t.Errorf("Select on canceled ctx: err = %v, want context.Canceled", err)
		}
		if _, err := collect(src.Aggregate(ctx, sel, plan.BucketAgg{Width: time.Minute, Origin: T0})); !errors.Is(err, context.Canceled) {
			t.Errorf("Aggregate on canceled ctx: err = %v, want context.Canceled", err)
		}
	})

	t.Run("EarlyBreak", func(t *testing.T) {
		// The iterator must stop when yield returns false (the runtime
		// panics otherwise) and release its resources.
		sel := plan.Selection{Tenant: tenant.Default, Matchers: matcherSets["stream"], From: from, Through: through}
		for range 20 {
			for _, err := range src.Select(t.Context(), sel) {
				if err != nil {
					t.Fatal(err)
				}
				break
			}
			for _, err := range src.Aggregate(t.Context(), sel, plan.BucketAgg{Width: time.Minute, Origin: T0}) {
				if err != nil {
					t.Fatal(err)
				}
				break
			}
		}
	})
}

func sortedMap[V any](m map[string]V) iter.Seq2[string, V] {
	return func(yield func(string, V) bool) {
		for _, k := range slices.Sorted(maps.Keys(m)) {
			if !yield(k, m[k]) {
				return
			}
		}
	}
}

func collect[T any](seq iter.Seq2[T, error]) ([]T, error) {
	var out []T
	for v, err := range seq {
		if err != nil {
			return out, err
		}
		out = append(out, v)
	}
	return out, nil
}

func rowKey(r plan.Row) string {
	return fmt.Sprintf("%d|%s|%s|%q", r.TS.UnixMicro(), r.Labels, r.Meta, r.Line)
}

// compareSelect checks src against ref for sel: same rows (as a multiset),
// in Direction order, and with a Limit the same timestamps (rows with equal
// TS may differ at the cut).
func compareSelect(t *testing.T, src, ref plan.Source, sel plan.Selection) {
	t.Helper()
	got, err := collect(src.Select(t.Context(), sel))
	if err != nil {
		t.Fatalf("Select: %v", err)
	}
	want, err := collect(ref.Select(t.Context(), sel))
	if err != nil {
		t.Fatalf("reference Select: %v", err)
	}
	for i := 1; i < len(got); i++ {
		c := got[i-1].TS.Compare(got[i].TS)
		if sel.Direction == plan.Forward && c > 0 || sel.Direction == plan.Backward && c < 0 {
			t.Fatalf("row %d out of %s order: %s then %s", i, sel.Direction, got[i-1].TS, got[i].TS)
		}
	}
	for _, r := range got {
		if r.TS.Location() != time.UTC || r.TS.Nanosecond()%1000 != 0 {
			t.Fatalf("row TS %s is not UTC with µs precision", r.TS)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d\nfirst got:  %s\nfirst want: %s", len(got), len(want), first(got), first(want))
	}
	if sel.Limit == 0 {
		diffRows(t, got, want)
		return
	}
	// With a limit: same TS sequence, and every row exists in the
	// unlimited reference result, without duplicates.
	for i := range got {
		if !got[i].TS.Equal(want[i].TS) {
			t.Fatalf("row %d TS = %s, want %s", i, got[i].TS, want[i].TS)
		}
	}
	all := sel
	all.Limit = 0
	full, err := collect(ref.Select(t.Context(), all))
	if err != nil {
		t.Fatal(err)
	}
	avail := make(map[string]int)
	for _, r := range full {
		avail[rowKey(r)]++
	}
	for _, r := range got {
		k := rowKey(r)
		if avail[k] == 0 {
			t.Fatalf("unexpected or duplicate row %s", k)
		}
		avail[k]--
	}
}

func first(rs []plan.Row) string {
	if len(rs) == 0 {
		return "(none)"
	}
	return rowKey(rs[0])
}

func diffRows(t *testing.T, got, want []plan.Row) {
	t.Helper()
	g := make([]string, len(got))
	for i, r := range got {
		g[i] = rowKey(r)
	}
	w := make([]string, len(want))
	for i, r := range want {
		w[i] = rowKey(r)
	}
	slices.Sort(g)
	slices.Sort(w)
	var missing, extra []string
	i, j := 0, 0
	for i < len(g) || j < len(w) {
		switch {
		case j == len(w) || i < len(g) && g[i] < w[j]:
			extra = append(extra, g[i])
			i++
		case i == len(g) || w[j] < g[i]:
			missing = append(missing, w[j])
			j++
		default:
			i++
			j++
		}
	}
	if len(missing)+len(extra) > 0 {
		t.Fatalf("rows differ: %d missing, %d extra\nmissing: %s\nextra: %s",
			len(missing), len(extra), head(missing), head(extra))
	}
}

func head(s []string) string {
	if len(s) > 5 {
		s = append(s[:5:5], "...")
	}
	return strings.Join(s, "\n  ")
}

func compareAggregate(t *testing.T, src, ref plan.Source, sel plan.Selection, agg plan.BucketAgg) {
	t.Helper()
	got, err := collect(src.Aggregate(t.Context(), sel, agg))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	want, err := collect(ref.Aggregate(t.Context(), sel, agg))
	if err != nil {
		t.Fatalf("reference Aggregate: %v", err)
	}
	key := func(b plan.Bucket) string {
		meta := "-"
		if agg.IncludeMeta {
			meta = b.Meta.String()
		} else if b.Meta != nil {
			meta = "unexpected " + b.Meta.String()
		}
		return fmt.Sprintf("%d|%s|%s|%d|%d", b.Start.UnixMicro(), b.Labels, meta, b.Count, b.Bytes)
	}
	order := func(a, b plan.Bucket) int {
		return cmp.Or(a.Start.Compare(b.Start), cmp.Compare(a.Labels.String(), b.Labels.String()),
			cmp.Compare(a.Meta.String(), b.Meta.String()))
	}
	slices.SortFunc(got, order)
	slices.SortFunc(want, order)
	for i := range max(len(got), len(want)) {
		var g, w string
		if i < len(got) {
			g = key(got[i])
			if got[i].Start.Location() != time.UTC {
				t.Fatalf("bucket start %s is not UTC", got[i].Start)
			}
		}
		if i < len(want) {
			w = key(want[i])
		}
		if g != w {
			t.Fatalf("bucket %d (of %d got, %d want):\n got  %s\n want %s", i, len(got), len(want), g, w)
		}
	}
}
