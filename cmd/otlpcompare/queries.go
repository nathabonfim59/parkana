package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	// apiPrefix is the Loki-compatible read API prefix on a server base URL.
	apiPrefix = "/loki/api/v1"
	// encodingFlagsHeader asks Loki and Parkana to return structured metadata
	// per entry, which the comparison requires.
	encodingFlagsHeader = "X-Loki-Response-Encoding-Flags"
	categorizeLabels    = "categorize-labels"

	// maxEntriesPerQuery bounds every range query. The corpus is far smaller;
	// the +1 lets the precheck detect extra rows instead of hiding them behind
	// the limit.
	maxEntriesPerQuery = 1000
)

// timeBounds are an inclusive start and exclusive end.
type timeBounds struct {
	start time.Time
	end   time.Time
}

// parseTime accepts RFC3339 (optionally with nanoseconds) or integer Unix
// nanoseconds.
func parseTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("empty timestamp")
	}
	if ns, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(0, ns).UTC(), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("timestamp %q: want RFC3339 or integer Unix nanoseconds", s)
	}
	return t.UTC(), nil
}

// formatTime renders an exact UTC timestamp.
func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// queryKind selects the Loki endpoint and the result shape.
type queryKind string

const (
	queryRange  queryKind = "range"
	querySeries queryKind = "series"
)

// queryCase is one fixed comparison query. ExpectEntries and RequireMatch are
// structural assertions derived from the fixture plan, never from the server
// responses under comparison: a filter query that matches nothing on either
// side must not pass as a meaningful equality.
type queryCase struct {
	name          string
	kind          queryKind
	expr          string
	direction     string
	limit         int
	start         time.Time
	end           time.Time
	expectEntries *int
	requireMatch  bool
	// expectTimestamps, when non-nil, is the exact multiset of entry
	// timestamps the fixture plan requires. An empty non-nil slice means the
	// query must return no entries; nil means no timestamp assertion.
	expectTimestamps []int64
}

// selector renders a stream selector for the manifest namespace.
func namespaceSelector(namespace string) string {
	return "{service_namespace=" + logqlQuote(namespace) + "}"
}

// withLabels adds label matchers to a namespace selector.
func withLabels(namespace string, matchers ...string) string {
	var b strings.Builder
	b.WriteString("{service_namespace=")
	b.WriteString(logqlQuote(namespace))
	for _, m := range matchers {
		b.WriteString(", ")
		b.WriteString(m)
	}
	b.WriteString("}")
	return b.String()
}

func labelMatcher(name, value string) string {
	return name + "=" + logqlQuote(value)
}

// logqlQuote quotes a LogQL string literal. Fixture values are ASCII
// identifiers and fixed markers, so the Go quote form is also a valid LogQL
// double-quoted literal.
func logqlQuote(s string) string { return strconv.Quote(s) }

// fixedQueries builds the documented query set for one manifest. Case counts
// come from the compiled fixture definitions checked against the manifest.
func fixedQueries(m fixtureManifest, t manifestTimes) ([]queryCase, error) {
	plan := buildPlan(t.anchor, m.RunID, m.Namespace)
	if err := checkManifestMatchesPlan(m, plan); err != nil {
		return nil, err
	}
	counts := plan.recordCounts()
	need := func(name string) int {
		count, ok := counts[name]
		if !ok {
			// Checked by checkManifestMatchesPlan, so this is unreachable.
			return 0
		}
		return count
	}

	selector := namespaceSelector(m.Namespace)
	fullLimit := min(plan.total+1, maxEntriesPerQuery)
	total := plan.total
	one, zero := 1, 0
	presentCount := need(caseServicePresent)
	unknownServiceCount := need(caseServiceEmpty) + need(caseServiceMissing) + need(caseServiceMissingJob)
	discoveredCount := need(caseServiceDiscovered)
	missingJobCount := need(caseServiceMissingJob)
	negatedCount := max(total-1, 0)

	rangeCase := func(name, expr, direction string, limit int, start, end time.Time) queryCase {
		return queryCase{
			name:      name,
			kind:      queryRange,
			expr:      expr,
			direction: direction,
			limit:     limit,
			start:     start,
			end:       end,
		}
	}
	withCount := func(q queryCase, count int) queryCase {
		q.expectEntries = new(count)
		q.requireMatch = count > 0
		return q
	}
	wantNone := func(q queryCase) queryCase {
		q.expectTimestamps = []int64{}
		return q
	}
	wantTS := func(q queryCase, ts ...int64) queryCase {
		q.expectTimestamps = slices.Clone(ts)
		return q
	}
	firstTS, lastTS := t.first.UnixNano(), t.last.UnixNano()
	alphaTS := int64(0)
	if c, ok := plan.caseByName(caseServicePresent); ok && len(c.records) > 0 {
		alphaTS = c.records[0].timestamp.UnixNano()
	}
	fixedTraceHex := hex.EncodeToString(fixedFieldTraceID[:])
	fixedSpanHex := hex.EncodeToString(fixedFieldSpanID[:])
	fixedObserved := ""
	if c, ok := plan.caseByName(caseFixedField); ok && len(c.records) > 0 {
		fixedObserved = strconv.FormatInt(c.records[0].timestamp.Add(fixedFieldObservedOffset).UnixNano(), 10)
	}
	microFirst, microSecond := time.Time{}, time.Time{}
	if c, ok := plan.caseByName(caseTimeEdges); ok && len(c.records) >= 2 {
		microFirst = c.records[0].timestamp
		microSecond = c.records[1].timestamp
	}
	exactFilter := func(scope, name, key, value string) queryCase {
		return rangeCase(name, scope+` | `+key+`=`+logqlQuote(value),
			"backward", fullLimit, t.window.start, t.window.end)
	}

	queries := []queryCase{
		withCount(rangeCase("entries-backward", selector, "backward", fullLimit, t.window.start, t.window.end), total),
		withCount(rangeCase("entries-forward", selector, "forward", fullLimit, t.window.start, t.window.end), total),
		wantTS(withCount(rangeCase("limit-newest", selector, "backward", 1, t.window.start, t.window.end), one), lastTS),
		wantTS(withCount(rangeCase("limit-oldest", selector, "forward", 1, t.window.start, t.window.end), one), firstTS),
		withCount(rangeCase("selector-service-present",
			withLabels(m.Namespace, labelMatcher("service_name", serviceNamePresent)),
			"backward", fullLimit, t.window.start, t.window.end), presentCount),
		// service.name present but empty: with native OTLP defaults, discovery
		// replaces the empty label with unknown_service, so this selector must
		// match nothing. Unknown is asserted separately.
		withCount(rangeCase("selector-service-empty",
			withLabels(m.Namespace, labelMatcher("service_name", "")),
			"backward", fullLimit, t.window.start, t.window.end), zero),
		// service.name missing (with or without non-indexed job) and empty both
		// fall back to unknown_service under the default discovery list.
		withCount(rangeCase("selector-service-unknown",
			withLabels(m.Namespace, labelMatcher("service_name", discoveredServiceNameUnknown)),
			"backward", fullLimit, t.window.start, t.window.end), unknownServiceCount),
		// container.name is an indexed default resource attribute and a
		// configured discovery label, so it supplies service_name.
		withCount(rangeCase("selector-service-discovered",
			withLabels(m.Namespace, labelMatcher("service_name", discoveredServiceName)),
			"backward", fullLimit, t.window.start, t.window.end), discoveredCount),
		withCount(rangeCase("selector-container-label",
			withLabels(m.Namespace, labelMatcher("container_name", discoveredServiceName)),
			"backward", fullLimit, t.window.start, t.window.end), discoveredCount),
		// job is not an indexed resource attribute: it is structured metadata.
		withCount(rangeCase("metadata-job",
			selector+` | job=`+logqlQuote(missingServiceJob),
			"backward", fullLimit, t.window.start, t.window.end), missingJobCount),
		withCount(rangeCase("selector-k8s-pod",
			withLabels(m.Namespace, labelMatcher("k8s_pod_name", "checkout-0")),
			"backward", fullLimit, t.window.start, t.window.end), presentCount),
		// Normalization collisions. Upstream Loki appends entry metadata first,
		// then resource, then scope, but which value a metadata filter observes
		// is the empirical gate for the converter. Each contender gets its own
		// exact-value filter; no count is assumed for them, so both stores are
		// compared directly and a converter that picks a different winner shows
		// up as a difference on at least one query. The global count gate still
		// proves the corpus exists.
		exactFilter(selector, "collision-meta-resource", collisionMetaName, "resource"),
		exactFilter(selector, "collision-meta-scope", collisionMetaName, "scope"),
		exactFilter(selector, "collision-meta-log", collisionMetaName, "log"),
		exactFilter(selector, "collision-http-get", collisionHTTPName, "GET"),
		exactFilter(selector, "collision-http-scope", collisionHTTPName, "SCOPE"),
		exactFilter(selector, "collision-http-log", collisionHTTPName, "LOG"),
		// The stream label is service_name=label-svc while the entry's metadata
		// carries service_name=shadow-log. These two filters distinguish label
		// and metadata precedence without assuming which one wins.
		exactFilter(withLabels(m.Namespace, labelMatcher("service_name", serviceNameCollision)),
			"collision-label-stream", "service_name", serviceNameCollision),
		exactFilter(withLabels(m.Namespace, labelMatcher("service_name", serviceNameCollision)),
			"collision-label-metadata", "service_name", "shadow-log"),
		withCount(rangeCase("negative-label-selector",
			withLabels(m.Namespace, labelMatcher("service_name", "shadow-log")),
			"backward", fullLimit, t.window.start, t.window.end), zero),
		// a-b, a b and with.dot all normalize to a_b; each contender is its own
		// exact filter, again without a count assumption.
		exactFilter(selector, "unusual-name-dash", collisionUnusualName, "dash"),
		exactFilter(selector, "unusual-name-space", collisionUnusualName, "space"),
		exactFilter(selector, "unusual-name-dot", collisionUnusualName, "dot"),
		// Attributes that normalize onto names Loki writes from fixed OTLP
		// fields. Each fixed field and each attribute value gets its own exact
		// filter; the winning side is determined by real Loki, not assumed.
		exactFilter(selector, "fixed-severity-attr", "severity_text", "attr-severity"),
		exactFilter(selector, "fixed-severity-field", "severity_text", "ERROR"),
		exactFilter(selector, "fixed-trace-attr", "trace_id", "attr-trace"),
		exactFilter(selector, "fixed-trace-field", "trace_id", fixedTraceHex),
		exactFilter(selector, "fixed-span-attr", "span_id", "attr-span"),
		exactFilter(selector, "fixed-span-field", "span_id", fixedSpanHex),
		exactFilter(selector, "fixed-observed-attr", "observed_timestamp", "attr-observed"),
		exactFilter(selector, "fixed-observed-field", "observed_timestamp", fixedObserved),
		exactFilter(selector, "fixed-flags-attr", "flags", "attr-flags"),
		exactFilter(selector, "fixed-flags-field", "flags", "1"),
		exactFilter(selector, "fixed-dropped-attr", "dropped_attributes_count", "attr-dropped"),
		exactFilter(selector, "fixed-dropped-field", "dropped_attributes_count", "4"),
		exactFilter(selector, "fixed-scope-name-attr", "scope_name", "attr-scope-name"),
		exactFilter(selector, "fixed-scope-name-field", "scope_name", "fixed.scope"),
		exactFilter(selector, "fixed-scope-version-attr", "scope_version", "attr-scope-version"),
		exactFilter(selector, "fixed-scope-version-field", "scope_version", "9.9.9"),
		wantTS(withCount(rangeCase("substring-present",
			selector+` |= `+logqlQuote(markerAlpha),
			"backward", fullLimit, t.window.start, t.window.end), one), alphaTS),
		withCount(rangeCase("substring-absent",
			selector+` |= `+logqlQuote("not-present-anywhere"),
			"backward", fullLimit, t.window.start, t.window.end), zero),
		withCount(rangeCase("substring-negated",
			selector+` != `+logqlQuote(markerAlpha),
			"backward", fullLimit, t.window.start, t.window.end), negatedCount),
		// From <= ts < Through: these pin the boundary semantics with exact
		// counts derived from the fixture timestamps.
		wantTS(withCount(rangeCase("boundary-first",
			selector, "forward", 2, t.first, t.first.Add(time.Second)), one), firstTS),
		wantTS(withCount(rangeCase("boundary-last",
			selector, "forward", 2, t.last, t.last.Add(time.Second)), one), lastTS),
		wantNone(withCount(rangeCase("boundary-before-first",
			selector, "forward", 2, t.first.Add(-time.Second), t.first), zero)),
		// Adjacent microsecond points: any endpoint shift or sub-microsecond
		// rounding changes these counts or timestamps.
		wantTS(withCount(rangeCase("boundary-micro-first",
			selector, "forward", 2, microFirst, microFirst.Add(time.Microsecond)), one), microFirst.UnixNano()),
		wantTS(withCount(rangeCase("boundary-micro-adjacent",
			selector, "forward", 2, microSecond, microSecond.Add(time.Microsecond)), one), microSecond.UnixNano()),
		wantNone(withCount(rangeCase("boundary-micro-before",
			selector, "forward", 2, microFirst.Add(-time.Microsecond), microFirst), zero)),
		{
			name:         "series",
			kind:         querySeries,
			expr:         selector,
			start:        t.window.start,
			end:          t.window.end,
			requireMatch: true,
		},
	}
	for i := range queries {
		if queries[i].expectEntries != nil && *queries[i].expectEntries > maxEntriesPerQuery {
			return nil, fmt.Errorf("query %q expects %d entries, above the %d limit",
				queries[i].name, *queries[i].expectEntries, maxEntriesPerQuery)
		}
	}
	return queries, nil
}

// request binds one query to a server base URL. Every request carries the
// tenant header, and range queries ask for categorized metadata.
//
// Query windows are half-open, [start, end), and are sent unshifted: pinned
// Loki 3.7.9 (pkg/iter/entry_iterator.go uses inclusive mint and exclusive
// maxt) and Parkana's plan contract (From <= ts < Through) already agree, so
// both stores evaluate exactly the same instants.
func (q queryCase) request(ctx context.Context, base string, id tenant.ID) (*http.Request, error) {
	params := url.Values{}
	params.Set("start", formatTime(q.start))
	params.Set("end", formatTime(q.end))
	var path string
	switch q.kind {
	case queryRange:
		path = apiPrefix + "/query_range"
		params.Set("query", q.expr)
		params.Set("direction", q.direction)
		params.Set("limit", strconv.Itoa(q.limit))
	case querySeries:
		path = apiPrefix + "/series"
		params["match[]"] = []string{q.expr}
	default:
		return nil, fmt.Errorf("query %q has unknown kind %q", q.name, q.kind)
	}
	target := base + path + "?" + params.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set(tenant.Header, string(id))
	if q.kind == queryRange {
		req.Header.Set(encodingFlagsHeader, categorizeLabels)
	}
	return req, nil
}

// endpoint renders a query without its parameter values for reports.
func (q queryCase) endpoint() string {
	if q.kind == querySeries {
		return "series " + q.expr
	}
	return fmt.Sprintf("range %s (direction %s, limit %d)", q.expr, q.direction, q.limit)
}
