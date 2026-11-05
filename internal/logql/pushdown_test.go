package logql

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// fakeSource is a plan.Source over preset rows with the hooks the pushdown and
// aliasing tests need: it records every Selection, can reuse one Labels/Meta
// backing array across yields, can fail after N rows, and reports whether the
// consumer stopped iteration early.
type fakeSource struct {
	rows []plan.Row

	// mutate rewrites one Labels/Meta backing array before every yield, the
	// way an executor that decodes into scratch buffers may.
	mutate bool

	// onYield runs before yielding row i (0-based).
	onYield func(i int)
	// errAfter yields that many rows and then err.
	errAfter int
	err      error

	selectCalls  int
	selections   []plan.Selection
	yields       int
	stoppedEarly bool
}

func (s *fakeSource) Select(ctx context.Context, sel plan.Selection) iter.Seq2[plan.Row, error] {
	return func(yield func(plan.Row, error) bool) {
		s.selectCalls++
		s.selections = append(s.selections, sel)
		if err := ctx.Err(); err != nil {
			yield(plan.Row{}, fmt.Errorf("fake: %w", err))
			return
		}
		var scratchLabels, scratchMeta logs.Labels
		for i, r := range s.rows {
			if s.errAfter > 0 && i == s.errAfter {
				yield(plan.Row{}, s.err)
				return
			}
			if s.onYield != nil {
				s.onYield(i)
			}
			if s.mutate {
				scratchLabels = append(scratchLabels[:0], r.Labels...)
				scratchMeta = append(scratchMeta[:0], r.Meta...)
				r.Labels, r.Meta = scratchLabels, scratchMeta
			}
			s.yields++
			if !yield(r, nil) {
				s.stoppedEarly = true
				return
			}
		}
	}
}

func (s *fakeSource) Aggregate(context.Context, plan.Selection, plan.BucketAgg) iter.Seq2[plan.Bucket, error] {
	return func(yield func(plan.Bucket, error) bool) {
		yield(plan.Bucket{}, errors.New("fakeSource: Aggregate not expected"))
	}
}

func sameLineFilters(a, b []plan.LineFilter) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type || a[i].Value != b[i].Value {
			return false
		}
	}
	return true
}

func lastSelection(t *testing.T, src *fakeSource) plan.Selection {
	t.Helper()
	if src.selectCalls != 1 {
		t.Fatalf("Source.Select calls = %d, want 1", src.selectCalls)
	}
	return src.selections[0]
}

func TestSelectionPushdown(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := &fakeSource{}
	p := testParams(`{app="api", env=~"prod|dev"} |= "err" != "debug"`, base.Add(500*time.Nanosecond), base.Add(time.Microsecond+500*time.Nanosecond))
	p.Direction = plan.Forward
	p.Limit = 7
	if _, err := testEngine().Query(t.Context(), src, p); err != nil {
		t.Fatalf("Query: %v", err)
	}
	sel := lastSelection(t, src)
	if sel.Tenant != tenant.Default {
		t.Fatalf("Tenant = %q", sel.Tenant)
	}
	if len(sel.Matchers) != 2 || sel.Matchers[0].Name != "app" || sel.Matchers[1].Name != "env" {
		t.Fatalf("Matchers = %v", sel.Matchers)
	}
	wantFilters := []plan.LineFilter{
		plan.MustLineFilter(plan.LineContains, "err"),
		plan.MustLineFilter(plan.LineNotContains, "debug"),
	}
	if !sameLineFilters(sel.LineFilters, wantFilters) {
		t.Fatalf("LineFilters = %v, want %v", sel.LineFilters, wantFilters)
	}
	if want := base.Add(time.Microsecond); !sel.From.Equal(want) {
		t.Fatalf("From = %s, want %s", sel.From, want)
	}
	if want := base.Add(2 * time.Microsecond); !sel.Through.Equal(want) {
		t.Fatalf("Through = %s, want %s", sel.Through, want)
	}
	if sel.Direction != plan.Forward {
		t.Fatalf("Direction = %s", sel.Direction)
	}
	if sel.Limit != 7 {
		t.Fatalf("Limit = %d, want 7 (fully pushed query)", sel.Limit)
	}
}

// TestSelectionNoUnsafeLimit proves the Source limit is only pushed when every
// selected row reaches the result unchanged.
func TestSelectionNoUnsafeLimit(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name       string
		query      string
		interval   time.Duration
		wantLimit  int
		wantFilter []plan.LineFilter
	}{
		{
			name:      "positive or stays in Go",
			query:     `{app="api"} |= "err" or "fail"`,
			wantLimit: 0,
		},
		{
			name:      "regexp exists stays in Go",
			query:     `{app="api"} |~ ".+"`,
			wantLimit: 0,
		},
		{
			name:      "regexp alternate stays in Go",
			query:     `{app="api"} |~ "err|fail"`,
			wantLimit: 0,
		},
		{
			name:      "interval discards rows",
			query:     `{app="api"} |= "err"`,
			interval:  time.Minute,
			wantLimit: 0,
			wantFilter: []plan.LineFilter{
				plan.MustLineFilter(plan.LineContains, "err"),
			},
		},
		{
			name:      "true filter is safe",
			query:     `{app="api"} |= ""`,
			wantLimit: 3,
		},
		{
			name:      "false filter is not pushable",
			query:     `{app="api"} != ""`,
			wantLimit: 0,
		},
		{
			name:      "negative or is an AND and is safe",
			query:     `{app="api"} != "err" or "fail"`,
			wantLimit: 3,
			wantFilter: []plan.LineFilter{
				plan.MustLineFilter(plan.LineNotContains, "err"),
				plan.MustLineFilter(plan.LineNotContains, "fail"),
			},
		},
		{
			name:      "negative regexp or is an AND and is safe",
			query:     `{app="api"} !~ "err|fail"`,
			wantLimit: 3,
			wantFilter: []plan.LineFilter{
				plan.MustLineFilter(plan.LineNotContains, "err"),
				plan.MustLineFilter(plan.LineNotContains, "fail"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &fakeSource{}
			p := testParams(tt.query, base, base.Add(time.Second))
			p.Limit = 3
			p.Interval = tt.interval
			if _, err := testEngine().Query(t.Context(), src, p); err != nil {
				t.Fatalf("Query: %v", err)
			}
			sel := lastSelection(t, src)
			if sel.Limit != tt.wantLimit {
				t.Fatalf("Selection.Limit = %d, want %d", sel.Limit, tt.wantLimit)
			}
			if !sameLineFilters(sel.LineFilters, tt.wantFilter) {
				t.Fatalf("LineFilters = %v, want %v", sel.LineFilters, tt.wantFilter)
			}
		})
	}
}

// TestSelectionRegexpOptimizer pins the pushdown side of the Loki regex
// simplifier: only language-equivalent plan filters go down.
func TestSelectionRegexpOptimizer(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name        string
		query       string
		want        []plan.LineFilter
		wantMatches map[string]bool // asserted against the single pushed regexp
	}{
		{
			name:  "literal becomes contains",
			query: `{app="api"} |~ "literal"`,
			want:  []plan.LineFilter{plan.MustLineFilter(plan.LineContains, "literal")},
		},
		{
			name:  "literal plus star becomes contains",
			query: `{app="api"} |~ "foo.*"`,
			want:  []plan.LineFilter{plan.MustLineFilter(plan.LineContains, "foo")},
		},
		{
			// Loki re-renders the simplified syntax tree, so the pushed value
			// is the tree's spelling; only the language must match the input.
			name:        "plain regexp falls back",
			query:       `{app="api"} |~ "a.b"`,
			wantMatches: map[string]bool{"a b": true, "a\nb": false, "x": false},
		},
		{
			name:        "explicit dotall falls back",
			query:       `{app="api"} |~ "(?s:a.b)"`,
			wantMatches: map[string]bool{"a b": true, "a\nb": true, "x": false},
		},
		{
			name:  "plus is exists, not pushable",
			query: `{app="api"} |~ ".+"`,
		},
		{
			name:  "captured plus is exists, not pushable",
			query: `{app="api"} |~ "(.+)"`,
		},
		{
			name:  "star is true, nothing to push",
			query: `{app="api"} |~ ".*"`,
		},
		{
			name:  "alternate is OR, not pushable",
			query: `{app="api"} |~ "foo|bar"`,
		},
		{
			name:  "case-insensitive contains is not pushable",
			query: `{app="api"} |~ "(?i)foo"`,
		},
		{
			name:  "negative alternate is AND of negatives",
			query: `{app="api"} !~ "foo|bar"`,
			want: []plan.LineFilter{
				plan.MustLineFilter(plan.LineNotContains, "foo"),
				plan.MustLineFilter(plan.LineNotContains, "bar"),
			},
		},
		{
			name:  "negative exists is not pushable",
			query: `{app="api"} !~ ".+"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src := &fakeSource{}
			if _, err := testEngine().Query(t.Context(), src, testParams(tt.query, base, base.Add(time.Second))); err != nil {
				t.Fatalf("Query: %v", err)
			}
			sel := lastSelection(t, src)
			if tt.wantMatches != nil {
				if len(sel.LineFilters) != 1 || sel.LineFilters[0].Type != plan.LineRegexp {
					t.Fatalf("LineFilters = %v, want one pushed regexp", sel.LineFilters)
				}
				for line, want := range tt.wantMatches {
					if got := sel.LineFilters[0].Matches(line); got != want {
						t.Fatalf("pushed regexp %s matches %q = %v, want %v", sel.LineFilters[0], line, got, want)
					}
				}
				return
			}
			if !sameLineFilters(sel.LineFilters, tt.want) {
				t.Fatalf("LineFilters = %v, want %v", sel.LineFilters, tt.want)
			}
		})
	}
}

// TestMutatingSourceClonesRetainedRows feeds a Source that reuses its Labels
// and Meta backing arrays between yields. Every result row must keep the
// values it was yielded with.
func TestMutatingSourceClonesRetainedRows(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	var rows []plan.Row
	for i := range 3 {
		id := fmt.Sprintf("v%d", i)
		rows = append(rows, plan.Row{
			TS:     base.Add(time.Duration(i) * time.Second),
			Line:   id,
			Labels: logs.New(logs.Label{Name: "app", Value: "api"}, logs.Label{Name: "id", Value: id}),
			Meta:   logs.New(logs.Label{Name: "trace", Value: id}),
		})
	}
	src := &fakeSource{rows: rows, mutate: true}
	res := mustQuery(t, testEngine(), src, testParams(`{app="api"}`, base, base.Add(2*time.Second)))
	if len(res.Streams) != 3 {
		t.Fatalf("streams = %d, want 3 distinct label sets", len(res.Streams))
	}
	for _, s := range res.Streams {
		id, ok := s.Labels.Get("id")
		if !ok {
			t.Fatalf("stream labels missing id: %s", s.Labels)
		}
		if len(s.Entries) != 1 {
			t.Fatalf("stream %s entries = %d, want 1", id, len(s.Entries))
		}
		if trace, _ := s.Entries[0].Meta.Get("trace"); trace != id {
			t.Fatalf("entry %s meta trace = %q, aliased to another row", id, trace)
		}
	}
}

// TestLokiRegexpLabelSemantics freezes the shared regexp optimizer for A5's
// pipeline label filters (isLabel=true): literals are whole-value equality,
// alternation/capture propagate label semantics, concat always uses contains
// (pinned filter.go:749-806), and the fallback is anchored ^(?:...)$.
func TestLokiRegexpLabelSemantics(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		cases   map[string]bool
	}{
		{"literal is whole-value equality", "foo", map[string]bool{"foo": true, "xfoo": false, "foox": false, "": false}},
		{"folded literal is equality", "(?i)foo", map[string]bool{"foo": true, "FOO": true, "xfoo": false}},
		{"unicode fold", "(?i)é", map[string]bool{"é": true, "É": true, "e": false, "Éx": false}},
		{"capture is equality", "(foo)", map[string]bool{"foo": true, "xfoo": false}},
		{"alternation is equality per branch", "foo|bar", map[string]bool{"foo": true, "bar": true, "xfoo": false, "foobar": false}},
		{"concat is contains", "foo.*", map[string]bool{"foo": true, "xfooy": true, "bar": false}},
		{"capture concat is cleared", "(foo).*", map[string]bool{"xfooy": true, "bar": false}},
		{"concat alternate is contains", "c(foo|bar)", map[string]bool{"cfoo": true, "xcfooy": true, "bar": false}},
		{"char class fallback is anchored", "c([ab])", map[string]bool{"ca": true, "cb": true, "xcbz": false}},
		{"plus is exists", ".+", map[string]bool{"\n": true, "x": true, "": false}},
		{"star is true", ".*", map[string]bool{"": true, "\n": true, "x": true}},
		{"or drops true", "foo|.*", map[string]bool{"foo": true, "bar": false, "": false}},
		{"fallback is anchored", "a.b", map[string]bool{"a b": true, "xa b": false, "a bz": false, "a\nb": false}},
		{"fallback dotall is anchored", "(?s:a.b)", map[string]bool{"a\nb": true, "xa\nb": false}},
		{"fallback no dotall", "(?-s:a.b)", map[string]bool{"a b": true, "a\nb": false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := lokiRegexp(tt.pattern, true)
			if err != nil {
				t.Fatalf("lokiRegexp(%q, true): %v", tt.pattern, err)
			}
			for value, want := range tt.cases {
				if got := m.match(value); got != want {
					t.Fatalf("lokiRegexp(%q, true) matches %q = %v, want %v", tt.pattern, value, got, want)
				}
			}
		})
	}
}

// TestContainsLowerPinnedEdges pins the exact containsLower port, including
// its Unicode rune path and the raw-byte path for invalid UTF-8.
func TestContainsLowerPinnedEdges(t *testing.T) {
	tests := []struct {
		name   string
		line   string
		substr string
		want   bool
	}{
		{"unicode upper line", "É", "é", true},
		{"unicode lower line", "é", "é", true},
		{"byte length mismatch", "É", "e", false},
		{"ascii fold", "AbC", "abc", true},
		{"invalid utf8 line raw byte", "\xffA", "a", true},
		{"invalid utf8 line no match", "\xff", "a", false},
		{"first byte fast path misses dotted I", "İ", "i", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := containsLower(tt.line, tt.substr); got != tt.want {
				t.Fatalf("containsLower(%q, %q) = %v, want %v", tt.line, tt.substr, got, tt.want)
			}
		})
	}
}

func TestCompileLogQueryMatchersCopy(t *testing.T) {
	c, err := testEngine().CompileLogQuery(`{app="api"}`)
	if err != nil {
		t.Fatalf("CompileLogQuery: %v", err)
	}
	got := c.Matchers()
	if len(got) != 1 || got[0].Value != "api" {
		t.Fatalf("Matchers = %v", got)
	}
	got[0] = plan.MustMatcher(plan.MatchEqual, "app", "changed")
	if again := c.Matchers(); again[0].Value != "api" {
		t.Fatalf("Matchers returned an aliased slice: %v", again)
	}
}

func TestCompileLogQueryRejectsNonLog(t *testing.T) {
	for _, q := range []string{
		`count_over_time({app="api"}[5m])`,
		`sum(rate({app="api"}[1m]))`,
		`1 + 1`,
		`{app="api"}[5m]`,
	} {
		if _, err := testEngine().CompileLogQuery(q); !errors.Is(err, plan.ErrBadQuery) {
			t.Fatalf("CompileLogQuery(%q) err = %v, want ErrBadQuery", q, err)
		}
	}
}

// TestProcessAppliesSelectorForTail pins the tail contract: private
// subscriptions carry every row of a tenant, so Process must apply the stream
// selector itself even though no Selection was involved.
func TestProcessAppliesSelectorForTail(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	c, err := testEngine().CompileLogQuery(`{app="api"} |= "err"`)
	if err != nil {
		t.Fatalf("CompileLogQuery: %v", err)
	}
	matching := plan.Row{
		TS:     base,
		Line:   "an err",
		Labels: logs.New(logs.Label{Name: "app", Value: "api"}),
		Meta:   logs.New(logs.Label{Name: "trace", Value: "1"}),
	}
	beforeLabels := slices.Clone(matching.Labels)
	beforeMeta := slices.Clone(matching.Meta)
	labels, entry, keep, err := c.Process(t.Context(), tenant.Default, matching)
	if err != nil || !keep {
		t.Fatalf("Process = keep %v, err %v; want keep", keep, err)
	}
	if got := labels.String(); got != `{app="api"}` {
		t.Fatalf("labels = %s", got)
	}
	if entry.Line != "an err" || !entry.TS.Equal(base) {
		t.Fatalf("entry = %+v", entry)
	}
	if got, _ := entry.Meta.Get("trace"); got != "1" {
		t.Fatalf("entry meta trace = %q", got)
	}
	if entry.Parsed != nil {
		t.Fatalf("Parsed = %v, want nil", entry.Parsed)
	}
	if !slices.Equal(matching.Labels, beforeLabels) || !slices.Equal(matching.Meta, beforeMeta) {
		t.Fatalf("Process mutated the source row")
	}

	otherStream := plan.Row{TS: base, Line: "an err", Labels: logs.New(logs.Label{Name: "app", Value: "web"})}
	if _, _, keep, err := c.Process(t.Context(), tenant.Default, otherStream); err != nil || keep {
		t.Fatalf("non-matching stream: keep %v, err %v; want filtered out", keep, err)
	}
	noMatch := plan.Row{TS: base, Line: "clean", Labels: logs.New(logs.Label{Name: "app", Value: "api"})}
	if _, _, keep, err := c.Process(t.Context(), tenant.Default, noMatch); err != nil || keep {
		t.Fatalf("non-matching line: keep %v, err %v; want filtered out", keep, err)
	}
}

func TestProcessValidatesTenantAndContext(t *testing.T) {
	c, err := testEngine().CompileLogQuery(`{app="api"}`)
	if err != nil {
		t.Fatalf("CompileLogQuery: %v", err)
	}
	row := plan.Row{TS: time.Now(), Line: "x", Labels: logs.New(logs.Label{Name: "app", Value: "api"})}
	if _, _, _, err := c.Process(t.Context(), tenant.ID("bad tenant"), row); !errors.Is(err, tenant.ErrInvalid) || !errors.Is(err, plan.ErrBadQuery) {
		t.Fatalf("invalid tenant err = %v, want tenant.ErrInvalid and ErrBadQuery", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, _, err := c.Process(ctx, tenant.Default, row); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx err = %v, want context.Canceled", err)
	}
}

func TestCompiledLogQueryConcurrentProcess(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	c, err := testEngine().CompileLogQuery(`{app="api"} |= "err"`)
	if err != nil {
		t.Fatalf("CompileLogQuery: %v", err)
	}
	row := plan.Row{TS: base, Line: "err", Labels: logs.New(logs.Label{Name: "app", Value: "api"})}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 50 {
				labels, _, keep, err := c.Process(t.Context(), tenant.Default, row)
				if err != nil || !keep || labels.String() != `{app="api"}` {
					t.Errorf("Process = labels %v, keep %v, err %v", labels, keep, err)
					return
				}
			}
		})
	}
	wg.Wait()
}
