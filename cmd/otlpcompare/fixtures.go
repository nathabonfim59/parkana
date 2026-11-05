package main

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

const (
	// fixtureID and fixtureVersion identify the compiled fixture corpus. They
	// are copied into the manifest and every report so an artifact can always
	// be tied to the fixture definitions that produced it.
	fixtureID      = "phase4-otlp-v1"
	fixtureVersion = 1
	// pdataModule and pdataVersion pin the OTLP data model used to encode the
	// fixtures. They are recorded next to the fixture identity.
	pdataModule  = "go.opentelemetry.io/collector/pdata"
	pdataVersion = "v1.59.0"
)

// Case names. They are stable identifiers: the manifest, the fixed query set
// and the reports all refer to them.
const (
	caseServicePresent    = "service-present"
	caseServiceEmpty      = "service-empty"
	caseServiceMissing    = "service-missing"
	caseServiceDiscovered = "service-discovered"
	caseServiceMissingJob = "service-missing-job"
	caseScopeMetadata     = "scope-metadata"
	caseCollisionMeta     = "collision-meta"
	caseCollisionLabel    = "collision-label"
	caseFixedField        = "fixed-field"
	caseNestedValues      = "nested-values"
	caseSeverityFlags     = "severity-flags"
	caseTraceSpan         = "trace-span"
	caseUnusualNames      = "unusual-names"
	caseBodyShapes        = "body-shapes"
	caseTimeEdges         = "time-edges"
	caseTimeEdge          = "time-edge"
)

const (
	// serviceNamePresent is the explicit service.name carried by the selected
	// fixture stream. Queries select it by label.
	serviceNamePresent = "checkout"
	// serviceNameCollision is the stream label of the normalization collision
	// fixture; its log attribute service.name becomes metadata of the same
	// name, so label and metadata filters exercise different values.
	serviceNameCollision = "label-svc"
	// markerAlpha appears in exactly one line, for |= and != substring
	// queries with exact expected counts.
	markerAlpha = "fixture-marker-alpha"
	// markerCollide appears in the metadata collision line so its substring
	// query is independent of label filters.
	markerCollide = "fixture-marker-collide"
	// discoveredServiceName is found by upstream Loki's default
	// discover_service_name list through the indexed container.name resource
	// attribute (container_name).
	discoveredServiceName = "discovered-container"
	// discoveredServiceNameUnknown is upstream Loki's ServiceUnknown fallback
	// when no service.name or configured discovery label is present.
	discoveredServiceNameUnknown = "unknown_service"
	// missingServiceJob is resource metadata, not an index label, so the
	// default OTLP resource mapping must not discover service_name from it.
	missingServiceJob = "legacy"
	// fixedFieldObservedOffset is the observed-timestamp offset used by the
	// converter's fixed-field collision fixture and its query expectation.
	fixedFieldObservedOffset = 250 * time.Millisecond
)

// fixedFieldTraceID and fixedFieldSpanID are the ids of the fixed-field
// collision record. Query expectations reuse them as lowercase hex.
var (
	fixedFieldTraceID = traceIDFor(0x21)
	fixedFieldSpanID  = spanIDFor(0x71)
)

// Fixed attribute names whose normalizations are the point of the collision
// fixtures.
const (
	collisionMetaName    = "collide_name"
	collisionHTTPName    = "http_request_method"
	collisionUnusualName = "a_b"
)

// fixtureScope is the instrumentation scope of one fixture export.
type fixtureScope struct {
	name    string
	version string
	dropped uint32
	attrs   attrList
}

// fixtureRecord is one OTLP log record before timestamps are assigned. body,
// attrs and the scalar fields map directly onto the OTLP data model.
type fixtureRecord struct {
	body           any
	attrs          attrList
	severity       plog.SeverityNumber
	severityText   string
	flags          plog.LogRecordFlags
	dropped        uint32
	traceID        pcommon.TraceID
	spanID         pcommon.SpanID
	observedOffset time.Duration
	// observedFallback leaves Timestamp unset and puts the entry time in
	// ObservedTimestamp, which upstream Loki uses as the entry timestamp and
	// then omits from structured metadata.
	observedFallback bool
	// gap is the delay after this record before the next corpus record. Zero
	// means one second; time-edge uses one microsecond to place adjacent
	// boundary points.
	gap time.Duration

	// timestamp is assigned by buildPlan.
	timestamp time.Time
}

// fixtureCase is one OTLP export: one resource, one scope and their records.
type fixtureCase struct {
	name     string
	instance string
	attrs    attrList
	scope    fixtureScope
	records  []fixtureRecord
}

// attr is one attribute in insertion order. val is one of nil, string, bool,
// int64, float64, []byte, attrList (an ordered nested map) or []any (a slice).
type attr struct {
	key string
	val any
}

type attrList []attr

// fixturePlan is the deterministic corpus bound to an anchor time. Every
// record is one second after the previous one, so time-boundary queries are
// exact and no two records share an entry timestamp.
type fixturePlan struct {
	anchor    time.Time
	runID     string
	namespace string
	cases     []fixtureCase
	total     int
	first     time.Time
	last      time.Time
}

// buildPlan binds the compiled fixture definitions to anchor. Timestamps are
// truncated to the microsecond grid Parkana stores; records are one second
// apart unless a record carries a narrower gap (the time-edge case uses 1µs).
func buildPlan(anchor time.Time, runID, namespace string) fixturePlan {
	anchor = anchor.UTC().Truncate(time.Microsecond)
	plan := fixturePlan{
		anchor:    anchor,
		runID:     runID,
		namespace: namespace,
		cases:     fixtureCases(),
		first:     anchor,
	}
	at := anchor
	slot := 0
	for i := range plan.cases {
		c := &plan.cases[i]
		c.instance = c.name
		for j := range c.records {
			record := &c.records[j]
			record.timestamp = at
			plan.last = at
			gap := record.gap
			if gap == 0 {
				gap = time.Second
			}
			at = at.Add(gap)
			slot++
		}
	}
	plan.total = slot
	return plan
}

// windowStart and windowEnd are the inclusive/exclusive bounds the fixed query
// set uses for full-corpus queries.
func (p fixturePlan) windowStart() time.Time { return p.first.Add(-time.Second) }
func (p fixturePlan) windowEnd() time.Time   { return p.last.Add(time.Second) }

// resourceAttrs returns the resource attributes in wire order: the two
// isolation labels first, then the case's own attributes.
func (c fixtureCase) resourceAttrs(namespace string) attrList {
	out := make(attrList, 0, 2+len(c.attrs))
	out = append(out,
		attr{key: "service.namespace", val: namespace},
		attr{key: "service.instance.id", val: c.instance},
	)
	return append(out, c.attrs...)
}

// logs encodes one case as an OTLP Logs export.
func (p fixturePlan) logs(c fixtureCase) plog.Logs {
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	setAttrs(resourceLogs.Resource().Attributes(), c.resourceAttrs(p.namespace))

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	scope := scopeLogs.Scope()
	if c.scope.name != "" {
		scope.SetName(c.scope.name)
	}
	if c.scope.version != "" {
		scope.SetVersion(c.scope.version)
	}
	if c.scope.dropped != 0 {
		scope.SetDroppedAttributesCount(c.scope.dropped)
	}
	setAttrs(scope.Attributes(), c.scope.attrs)

	for _, rec := range c.records {
		entry := scopeLogs.LogRecords().AppendEmpty()
		if rec.observedFallback {
			entry.SetObservedTimestamp(pcommon.Timestamp(rec.timestamp.UnixNano()))
		} else {
			entry.SetTimestamp(pcommon.Timestamp(rec.timestamp.UnixNano()))
			if rec.observedOffset != 0 {
				entry.SetObservedTimestamp(pcommon.Timestamp(rec.timestamp.Add(rec.observedOffset).UnixNano()))
			}
		}
		setValue(entry.Body(), rec.body)
		setAttrs(entry.Attributes(), rec.attrs)
		if rec.severity != plog.SeverityNumberUnspecified {
			entry.SetSeverityNumber(rec.severity)
		}
		if rec.severityText != "" {
			entry.SetSeverityText(rec.severityText)
		}
		if rec.flags != 0 {
			entry.SetFlags(rec.flags)
		}
		if rec.dropped != 0 {
			entry.SetDroppedAttributesCount(rec.dropped)
		}
		if !rec.traceID.IsEmpty() {
			entry.SetTraceID(rec.traceID)
		}
		if !rec.spanID.IsEmpty() {
			entry.SetSpanID(rec.spanID)
		}
	}
	return logs
}

func setAttrs(m pcommon.Map, attrs attrList) {
	for _, a := range attrs {
		setValue(m.PutEmpty(a.key), a.val)
	}
}

// setValue fills v from the fixture value model. Unknown values are a fixture
// programming error, not user input.
func setValue(v pcommon.Value, value any) {
	switch x := value.(type) {
	case nil:
		v.SetEmptyMap()
	case string:
		v.SetStr(x)
	case bool:
		v.SetBool(x)
	case int:
		v.SetInt(int64(x))
	case int64:
		v.SetInt(x)
	case float64:
		v.SetDouble(x)
	case []byte:
		v.SetEmptyBytes().FromRaw(x)
	case attrList:
		m := v.SetEmptyMap()
		setAttrs(m, x)
	case []any:
		s := v.SetEmptySlice()
		for _, item := range x {
			setValue(s.AppendEmpty(), item)
		}
	default:
		panic(fmt.Sprintf("otlpcompare: unsupported fixture value type %T", value))
	}
}

// hexID builds a deterministic trace or span id. n is the id length in bytes.
func hexID(seed byte, n int) []byte {
	id := make([]byte, n)
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func traceIDFor(seed byte) pcommon.TraceID {
	var id pcommon.TraceID
	copy(id[:], hexID(seed, len(id)))
	return id
}

func spanIDFor(seed byte) pcommon.SpanID {
	var id pcommon.SpanID
	copy(id[:], hexID(seed, len(id)))
	return id
}

// fixtureCases is the fixed, deterministic corpus. Case order defines
// timestamps, so never reorder or remove a case without bumping fixtureVersion.
func fixtureCases() []fixtureCase {
	return []fixtureCase{
		{
			name: caseServicePresent,
			attrs: attrList{
				{key: "service.name", val: serviceNamePresent},
				{key: "deployment.environment", val: "staging"},
				{key: "host.name", val: "host-a"},
				{key: "service.version", val: "1.2.3"},
				{key: "k8s.pod.name", val: "checkout-0"},
			},
			records: []fixtureRecord{
				{body: caseServicePresent + " plain " + markerAlpha},
				{body: caseServicePresent + " second line"},
			},
		},
		{
			name:    caseServiceEmpty,
			attrs:   attrList{{key: "service.name", val: ""}},
			records: []fixtureRecord{{body: caseServiceEmpty + " line"}},
		},
		{
			name:    caseServiceMissing,
			records: []fixtureRecord{{body: caseServiceMissing + " line"}},
		},
		{
			// container.name is one of the default indexed resource attributes,
			// so upstream Loki's default service-name discovery maps it to
			// service_name=discovered-container.
			name:    caseServiceDiscovered,
			attrs:   attrList{{key: "container.name", val: discoveredServiceName}},
			records: []fixtureRecord{{body: caseServiceDiscovered + " line"}},
		},
		{
			// job is not in the default indexed resource attributes, so it
			// becomes structured metadata and must not feed service-name
			// discovery. service_name stays unknown_service.
			name:    caseServiceMissingJob,
			attrs:   attrList{{key: "job", val: missingServiceJob}},
			records: []fixtureRecord{{body: caseServiceMissingJob + " line"}},
		},
		{
			name: caseScopeMetadata,
			attrs: attrList{
				{key: "service.name", val: "scope-svc"},
				{key: "resource.meta", val: "resource-value"},
			},
			scope: fixtureScope{
				name:    "fixture.scope",
				version: "2.0.0",
				dropped: 3,
				attrs:   attrList{{key: "scope.attr", val: "scope-value"}},
			},
			records: []fixtureRecord{{
				body: "scope metadata line",
				attrs: attrList{
					{key: "log.attr", val: "log-value"},
					{key: "log.count", val: int64(42)},
				},
			}},
		},
		{
			// Resource attribute collide.name, scope attribute collide_name
			// and log attribute collide.name all normalize to collide_name.
			// http.request.method, a nested resource map, and log/scope
			// attributes collide on http_request_method. Upstream Loki appends
			// entry metadata first, then resource, then scope, so these detect
			// first-wins vs last-wins before any Parkana converter runs.
			name: caseCollisionMeta,
			attrs: attrList{
				{key: "service.name", val: "collision-svc"},
				{key: "collide.name", val: "resource"},
				{key: "http.request", val: attrList{
					{key: "method", val: "GET"},
					{key: "status", val: int64(200)},
				}},
			},
			scope: fixtureScope{
				name:    "fixture.scope.collision",
				version: "1.0.0",
				attrs: attrList{
					{key: "collide_name", val: "scope"},
					{key: "http_request_method", val: "SCOPE"},
				},
			},
			records: []fixtureRecord{{
				body: "collision metadata line " + markerCollide,
				attrs: attrList{
					{key: "collide.name", val: "log"},
					{key: "http.request.method", val: "LOG"},
				},
			}},
		},
		{
			// The stream label is service_name=label-svc while the entry's
			// metadata carries service_name=shadow-log. Label selectors and
			// metadata filters must not borrow each other's value.
			name:    caseCollisionLabel,
			attrs:   attrList{{key: "service.name", val: serviceNameCollision}},
			records: []fixtureRecord{{body: "collision label line", attrs: attrList{{key: "service.name", val: "shadow-log"}}}},
		},
		{
			// Log attributes normalize onto names Loki's converter writes from
			// fixed OTLP fields: severity_text, trace_id, span_id,
			// observed_timestamp, flags, dropped_attributes_count. Resource and
			// scope attributes add scope_name/scope_version collisions. The
			// exact-value filters decide which value survives.
			name: caseFixedField,
			attrs: attrList{
				{key: "service.name", val: "fixed-svc"},
				{key: "scope.name", val: "attr-scope-name"},
			},
			scope: fixtureScope{name: "fixed.scope", version: "9.9.9"},
			records: []fixtureRecord{{
				body: "fixed field collision line",
				attrs: attrList{
					{key: "severity.text", val: "attr-severity"},
					{key: "trace.id", val: "attr-trace"},
					{key: "observed.timestamp", val: "attr-observed"},
					{key: "flags", val: "attr-flags"},
					{key: "span.id", val: "attr-span"},
					{key: "scope.version", val: "attr-scope-version"},
					{key: "dropped.attributes.count", val: "attr-dropped"},
				},
				severity:       plog.SeverityNumberError,
				severityText:   "ERROR",
				flags:          plog.LogRecordFlags(1),
				dropped:        4,
				traceID:        fixedFieldTraceID,
				spanID:         fixedFieldSpanID,
				observedOffset: fixedFieldObservedOffset,
			}},
		},
		{
			name: caseNestedValues,
			attrs: attrList{
				{key: "service.name", val: "nested-svc"},
				{key: "nested.map", val: attrList{
					{key: "alpha", val: "one"},
					{key: "beta", val: int64(2)},
					{key: "gamma", val: true},
				}},
				{key: "nested.slice", val: []any{"x", int64(3), false}},
				{key: "nested.bytes", val: []byte{0x01, 0x02, 0xff}},
				{key: "nested.int", val: int64(-7)},
				{key: "nested.double", val: 1.5},
				{key: "nested.bool", val: true},
				{key: "nested.empty", val: nil},
				{key: "nested.deep", val: attrList{{key: "outer", val: attrList{{key: "inner", val: "value"}}}}},
			},
			records: []fixtureRecord{{body: "nested values line"}},
		},
		{
			name:  caseSeverityFlags,
			attrs: attrList{{key: "service.name", val: "severity-svc"}},
			records: []fixtureRecord{
				{
					body:           "severity warn line",
					severity:       plog.SeverityNumberWarn,
					severityText:   "WARN",
					flags:          plog.LogRecordFlags(1),
					dropped:        2,
					observedOffset: 500 * time.Millisecond,
				},
				{body: "severity error line", severity: plog.SeverityNumberError, severityText: "ERROR"},
				{body: "severity observed fallback line", observedFallback: true},
			},
		},
		{
			name:  caseTraceSpan,
			attrs: attrList{{key: "service.name", val: "trace-svc"}},
			records: []fixtureRecord{
				{body: "trace span full line", traceID: traceIDFor(0x01), spanID: spanIDFor(0x81)},
				{body: "trace span only span line", spanID: spanIDFor(0x82)},
				{body: "trace span empty line"},
			},
		},
		{
			name:  caseUnusualNames,
			attrs: attrList{{key: "service.name", val: "names-svc"}},
			records: []fixtureRecord{{
				body: "unusual names line",
				attrs: attrList{
					{key: "9lives", val: "nine"},
					{key: "_hidden", val: "under"},
					{key: "__reserved__", val: "double"},
					{key: "a-b", val: "dash"},
					{key: "a b", val: "space"},
					{key: "a.b", val: "dot"},
					{key: "with.dot", val: "with-dot"},
					{key: "emoji😀", val: "smile"},
				},
			}},
		},
		{
			// Two records one microsecond apart: a boundary query of width 1µs
			// catches any endpoint shift or sub-microsecond rounding that an
			// inclusive/exclusive mistake would introduce.
			name:  caseTimeEdges,
			attrs: attrList{{key: "service.name", val: "edge-svc"}},
			records: []fixtureRecord{
				{body: "time edges microsecond first line", gap: time.Microsecond},
				{body: "time edges microsecond adjacent line"},
			},
		},
		{
			name:  caseBodyShapes,
			attrs: attrList{{key: "service.name", val: "body-svc"}},
			records: []fixtureRecord{
				{body: attrList{{key: "kind", val: "map"}, {key: "n", val: int64(1)}}},
				{body: []any{"a", int64(1), true}},
				{body: int64(42)},
				{body: ""},
				{body: []byte{0xde, 0xad, 0xbe, 0xef}},
			},
		},
		{
			name:  caseTimeEdge,
			attrs: attrList{{key: "service.name", val: "edge-svc"}},
			records: []fixtureRecord{
				{body: "time edge first boundary line"},
				{body: "time edge final line"},
			},
		},
	}
}

// caseByName returns the planned case with the given name.
func (p fixturePlan) caseByName(name string) (fixtureCase, bool) {
	for _, c := range p.cases {
		if c.name == name {
			return c, true
		}
	}
	return fixtureCase{}, false
}

// recordCounts returns each case's record count in plan order.
func (p fixturePlan) recordCounts() map[string]int {
	counts := make(map[string]int, len(p.cases))
	for _, c := range p.cases {
		counts[c.name] = len(c.records)
	}
	return counts
}

const (
	codecMixed    = "mixed"
	codecProtobuf = "protobuf"
	codecJSON     = "json"
)

// validCodec reports whether codec is an accepted payload codec mode.
func validCodec(codec string) bool {
	switch codec {
	case codecMixed, codecProtobuf, codecJSON:
		return true
	default:
		return false
	}
}

// codecFor picks the payload codec for case index i. mixed alternates so one
// run exercises both encodings deterministically.
func codecFor(mode string, i int) string {
	switch mode {
	case codecProtobuf, codecJSON:
		return mode
	default:
		if i%2 == 0 {
			return codecProtobuf
		}
		return codecJSON
	}
}
