package main

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

func testQueries(t *testing.T) ([]queryCase, fixtureManifest, manifestTimes, fixturePlan) {
	t.Helper()
	m, plan := testManifest(t)
	times, err := m.parse()
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	queries, err := fixedQueries(m, times)
	if err != nil {
		t.Fatalf("fixed queries: %v", err)
	}
	return queries, m, times, plan
}

func TestFixedQuerySetCoversTheContract(t *testing.T) {
	queries, m, times, plan := testQueries(t)
	wantNames := []string{
		"entries-backward", "entries-forward", "limit-newest", "limit-oldest",
		"selector-service-present", "selector-service-empty", "selector-service-unknown",
		"selector-service-discovered", "selector-container-label", "metadata-job",
		"selector-k8s-pod",
		"collision-meta-resource", "collision-meta-scope", "collision-meta-log",
		"collision-http-get", "collision-http-scope", "collision-http-log",
		"collision-label-stream", "collision-label-metadata", "negative-label-selector",
		"unusual-name-dash", "unusual-name-space", "unusual-name-dot",
		"fixed-severity-attr", "fixed-severity-field", "fixed-trace-attr", "fixed-trace-field",
		"fixed-span-attr", "fixed-span-field", "fixed-observed-attr", "fixed-observed-field",
		"fixed-flags-attr", "fixed-flags-field", "fixed-dropped-attr", "fixed-dropped-field",
		"fixed-scope-name-attr", "fixed-scope-name-field", "fixed-scope-version-attr", "fixed-scope-version-field",
		"substring-present", "substring-absent", "substring-negated",
		"boundary-first", "boundary-last", "boundary-before-first",
		"boundary-micro-first", "boundary-micro-adjacent", "boundary-micro-before",
		"series",
	}
	var got []string
	for _, q := range queries {
		got = append(got, q.name)
	}
	if !slices.Equal(got, wantNames) {
		t.Fatalf("query names = %v, want %v", got, wantNames)
	}

	byName := make(map[string]queryCase, len(queries))
	for _, q := range queries {
		byName[q.name] = q
		if q.kind == queryRange && q.expectEntries == nil &&
			!strings.HasPrefix(q.name, "collision-") && !strings.HasPrefix(q.name, "unusual-name-") &&
			!strings.HasPrefix(q.name, "fixed-") {
			t.Fatalf("range query %q has no expected entry count", q.name)
		}
		if q.start.After(q.end) {
			t.Fatalf("query %q start %s is after end %s", q.name, q.start, q.end)
		}
	}
	if want := plan.total; *byName["entries-backward"].expectEntries != want {
		t.Fatalf("entries-backward wants %d, want %d", *byName["entries-backward"].expectEntries, want)
	}
	if want := 0; *byName["substring-absent"].expectEntries != want {
		t.Fatalf("substring-absent wants %d", *byName["substring-absent"].expectEntries)
	}
	if want := plan.total - 1; *byName["substring-negated"].expectEntries != want {
		t.Fatalf("substring-negated wants %d, want %d", *byName["substring-negated"].expectEntries, want)
	}
	if !slices.Equal(byName["limit-newest"].expectTimestamps, []int64{times.last.UnixNano()}) {
		t.Fatalf("limit-newest timestamps = %v", byName["limit-newest"].expectTimestamps)
	}
	if !slices.Equal(byName["limit-oldest"].expectTimestamps, []int64{times.first.UnixNano()}) {
		t.Fatalf("limit-oldest timestamps = %v", byName["limit-oldest"].expectTimestamps)
	}
	if got := byName["boundary-before-first"].expectTimestamps; len(got) != 0 || got == nil {
		t.Fatalf("boundary-before-first timestamps = %v (want empty non-nil)", got)
	}
	if !byName["series"].requireMatch {
		t.Fatal("series query does not require a match")
	}
	if got := m.Namespace; got != "ns-1" {
		t.Fatalf("namespace = %q", got)
	}
}

func TestQueryRequestsUseHalfOpenWindowsAndTenant(t *testing.T) {
	queries, _, _, _ := testQueries(t)
	for _, q := range queries {
		req, err := q.request(t.Context(), "http://localhost:3100", tenant.Default)
		if err != nil {
			t.Fatalf("%s: request: %v", q.name, err)
		}
		if got := req.Header.Get(tenant.Header); got != string(tenant.Default) {
			t.Fatalf("%s: tenant header = %q", q.name, got)
		}
		values := req.URL.Query()
		// Both endpoints are sent unshifted. Pinned Loki 3.7.9 uses inclusive
		// mint and exclusive maxt (pkg/iter/entry_iterator.go), matching
		// Parkana's From <= ts < Through; any shift here would silently move
		// the boundary.
		if got, want := values.Get("start"), formatTime(q.start); got != want {
			t.Fatalf("%s: start = %q, want %q", q.name, got, want)
		}
		if got, want := values.Get("end"), formatTime(q.end); got != want {
			t.Fatalf("%s: end = %q, want %q", q.name, got, want)
		}
		switch q.kind {
		case queryRange:
			if req.URL.Path != apiPrefix+"/query_range" {
				t.Fatalf("%s: path = %q", q.name, req.URL.Path)
			}
			if got := req.Header.Get(encodingFlagsHeader); got != categorizeLabels {
				t.Fatalf("%s: encoding flags = %q", q.name, got)
			}
			if got := values.Get("query"); got != q.expr {
				t.Fatalf("%s: query = %q, want %q", q.name, got, q.expr)
			}
			if got := values.Get("direction"); got != q.direction {
				t.Fatalf("%s: direction = %q", q.name, got)
			}
		case querySeries:
			if req.URL.Path != apiPrefix+"/series" {
				t.Fatalf("%s: path = %q", q.name, req.URL.Path)
			}
			if got := values["match[]"]; !slices.Equal(got, []string{q.expr}) {
				t.Fatalf("%s: match[] = %v", q.name, got)
			}
		}
	}
}

func TestCollisionFiltersAreExactAndNotCountAssumed(t *testing.T) {
	queries, _, _, _ := testQueries(t)
	byName := make(map[string]queryCase, len(queries))
	for _, q := range queries {
		byName[q.name] = q
	}
	// Every contender gets an exact equality filter. A regex alternation over
	// the contenders cannot distinguish converter precedence, so it must not
	// appear here, and no per-contender count may be assumed: the comparison
	// itself is the empirical gate.
	filters := map[string]string{
		"collision-meta-resource":  `| collide_name="resource"`,
		"collision-meta-scope":     `| collide_name="scope"`,
		"collision-meta-log":       `| collide_name="log"`,
		"collision-http-get":       `| http_request_method="GET"`,
		"collision-http-scope":     `| http_request_method="SCOPE"`,
		"collision-http-log":       `| http_request_method="LOG"`,
		"collision-label-stream":   `| service_name="label-svc"`,
		"collision-label-metadata": `| service_name="shadow-log"`,
		"unusual-name-dash":        `| a_b="dash"`,
		"unusual-name-space":       `| a_b="space"`,
		"unusual-name-dot":         `| a_b="dot"`,
		"fixed-severity-attr":      `| severity_text="attr-severity"`,
		"fixed-severity-field":     `| severity_text="ERROR"`,
		"fixed-trace-attr":         `| trace_id="attr-trace"`,
		"fixed-span-attr":          `| span_id="attr-span"`,
		"fixed-observed-attr":      `| observed_timestamp="attr-observed"`,
		"fixed-flags-attr":         `| flags="attr-flags"`,
		"fixed-dropped-attr":       `| dropped_attributes_count="attr-dropped"`,
		"fixed-scope-name-attr":    `| scope_name="attr-scope-name"`,
		"fixed-scope-version-attr": `| scope_version="attr-scope-version"`,
	}
	for name, fragment := range filters {
		q, ok := byName[name]
		if !ok {
			t.Fatalf("missing collision filter %q", name)
		}
		if !strings.Contains(q.expr, fragment) {
			t.Fatalf("%s expr = %q, want %q", name, q.expr, fragment)
		}
		if strings.Contains(q.expr, "=~") {
			t.Fatalf("%s uses a regex alternation: %q", name, q.expr)
		}
		if q.expectEntries != nil {
			t.Fatalf("%s assumes an entry count %d; collision precedence is empirical", name, *q.expectEntries)
		}
		if q.requireMatch {
			t.Fatalf("%s requires a match; a losing contender legitimately matches nothing", name)
		}
	}
	// The two label-vs-metadata filters must run under the same stream selector.
	if byName["collision-label-stream"].expr == byName["collision-label-metadata"].expr {
		t.Fatal("label and metadata collision filters are identical")
	}
	for name, q := range byName {
		empirical := strings.HasPrefix(name, "collision-") || strings.HasPrefix(name, "unusual-name-") || strings.HasPrefix(name, "fixed-")
		if !empirical {
			continue
		}
		if q.expectEntries != nil || q.requireMatch {
			t.Fatalf("%s assumes a per-contender outcome (count=%v require=%v)", name, q.expectEntries, q.requireMatch)
		}
		if strings.Contains(q.expr, "=~") {
			t.Fatalf("%s uses a regex alternation: %q", name, q.expr)
		}
	}
}

func TestBoundaryWindowsAreExact(t *testing.T) {
	queries, _, times, plan := testQueries(t)
	byName := make(map[string]queryCase, len(queries))
	for _, q := range queries {
		byName[q.name] = q
	}
	edges, ok := plan.caseByName(caseTimeEdges)
	if !ok || len(edges.records) != 2 {
		t.Fatalf("time-edges case = %+v", edges)
	}
	microFirst := edges.records[0].timestamp
	microSecond := edges.records[1].timestamp
	cases := []struct {
		name           string
		wantStart      time.Time
		wantEnd        time.Time
		wantCount      int
		wantTimestamps []int64
	}{
		{"boundary-first", times.first, times.first.Add(time.Second), 1, []int64{times.first.UnixNano()}},
		{"boundary-last", times.last, times.last.Add(time.Second), 1, []int64{times.last.UnixNano()}},
		{"boundary-before-first", times.first.Add(-time.Second), times.first, 0, []int64{}},
		{"boundary-micro-first", microFirst, microFirst.Add(time.Microsecond), 1, []int64{microFirst.UnixNano()}},
		{"boundary-micro-adjacent", microSecond, microSecond.Add(time.Microsecond), 1, []int64{microSecond.UnixNano()}},
		{"boundary-micro-before", microFirst.Add(-time.Microsecond), microFirst, 0, []int64{}},
	}
	for _, tc := range cases {
		q := byName[tc.name]
		if !q.start.Equal(tc.wantStart) || !q.end.Equal(tc.wantEnd) {
			t.Fatalf("%s window = [%s, %s), want [%s, %s)", tc.name, q.start, q.end, tc.wantStart, tc.wantEnd)
		}
		if q.expectEntries == nil || *q.expectEntries != tc.wantCount {
			t.Fatalf("%s expectEntries = %v", tc.name, q.expectEntries)
		}
		if !slices.Equal(q.expectTimestamps, tc.wantTimestamps) {
			t.Fatalf("%s timestamps = %v, want %v", tc.name, q.expectTimestamps, tc.wantTimestamps)
		}
		// The wire request must carry the unshifted bounds exactly. A canned
		// fake response cannot prove endpoint semantics, so inspect the request.
		req, err := q.request(t.Context(), "http://localhost:3100", tenant.Default)
		if err != nil {
			t.Fatalf("%s: request: %v", tc.name, err)
		}
		values := req.URL.Query()
		if got, want := values.Get("start"), formatTime(tc.wantStart); got != want {
			t.Fatalf("%s: wire start = %q, want unshifted %q", tc.name, got, want)
		}
		if got, want := values.Get("end"), formatTime(tc.wantEnd); got != want {
			t.Fatalf("%s: wire end = %q, want unshifted %q", tc.name, got, want)
		}
	}
	if got := microSecond.Sub(microFirst); got != time.Microsecond {
		t.Fatalf("time-edges pair gap = %s, want 1µs", got)
	}
}
