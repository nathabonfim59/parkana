package main

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
)

func testAnchor() time.Time {
	return time.Date(2026, 11, 5, 12, 0, 0, 123_456_000, time.UTC)
}

func TestBuildPlanIsDeterministicAndMicrosecondAligned(t *testing.T) {
	plan := buildPlan(testAnchor(), "run-1", "ns-1")
	if got, want := plan.total, 27; got != want {
		t.Fatalf("total records = %d, want %d", got, want)
	}
	if got, want := len(plan.cases), len(fixtureCases()); got != want {
		t.Fatalf("cases = %d, want %d", got, want)
	}
	if plan.anchor.Nanosecond()%1000 != 0 {
		t.Fatalf("anchor %s is not microsecond aligned", plan.anchor)
	}
	readAt := plan.anchor
	last := plan.anchor
	for _, c := range plan.cases {
		if len(c.records) == 0 {
			t.Fatalf("case %q has no records", c.name)
		}
		if c.instance != c.name {
			t.Fatalf("case %q instance = %q", c.name, c.instance)
		}
		for _, rec := range c.records {
			if !rec.timestamp.Equal(readAt) {
				t.Fatalf("case %q record timestamp = %s, want %s", c.name, rec.timestamp, readAt)
			}
			if rec.timestamp.Nanosecond()%1000 != 0 {
				t.Fatalf("case %q record timestamp %s is not microsecond aligned", c.name, rec.timestamp)
			}
			last = rec.timestamp
			gap := rec.gap
			if gap == 0 {
				gap = time.Second
			}
			readAt = readAt.Add(gap)
		}
	}
	if !plan.first.Equal(testAnchor().Truncate(time.Microsecond)) {
		t.Fatalf("first = %s", plan.first)
	}
	if !plan.last.Equal(last) {
		t.Fatalf("last = %s, want %s", plan.last, last)
	}
	if !plan.windowStart().Equal(plan.first.Add(-time.Second)) || !plan.windowEnd().Equal(plan.last.Add(time.Second)) {
		t.Fatalf("window = [%s, %s)", plan.windowStart(), plan.windowEnd())
	}
	edges, ok := plan.caseByName(caseTimeEdges)
	if !ok || len(edges.records) != 2 {
		t.Fatalf("time-edges case = %+v", edges)
	}
	if got := edges.records[1].timestamp.Sub(edges.records[0].timestamp); got != time.Microsecond {
		t.Fatalf("time-edges pair gap = %s, want 1µs", got)
	}

	again := buildPlan(testAnchor(), "run-1", "ns-1")
	if got, want := again.total, plan.total; got != want {
		t.Fatalf("rebuild total = %d, want %d", got, want)
	}
	for i := range plan.cases {
		if plan.cases[i].name != again.cases[i].name {
			t.Fatalf("case %d name changed: %q vs %q", i, plan.cases[i].name, again.cases[i].name)
		}
		for j := range plan.cases[i].records {
			if !plan.cases[i].records[j].timestamp.Equal(again.cases[i].records[j].timestamp) {
				t.Fatalf("case %q record %d timestamp changed", plan.cases[i].name, j)
			}
		}
	}
}

func TestFixturePlanIsolationLabels(t *testing.T) {
	plan := buildPlan(testAnchor(), "run-1", "isolated-ns")
	for _, c := range plan.cases {
		logs := plan.logs(c)
		attrs := logs.ResourceLogs().At(0).Resource().Attributes()
		if got, ok := attrs.Get("service.namespace"); !ok || got.Str() != "isolated-ns" {
			t.Fatalf("case %q service.namespace = %v (present %v)", c.name, got.AsString(), ok)
		}
		if got, ok := attrs.Get("service.instance.id"); !ok || got.Str() != c.instance {
			t.Fatalf("case %q service.instance.id = %v (present %v)", c.name, got.AsString(), ok)
		}
	}
}

func TestFixtureServiceNameVariants(t *testing.T) {
	plan := buildPlan(testAnchor(), "run-1", "ns")
	checks := []struct {
		caseName string
		key      string
		want     string
		present  bool
	}{
		{caseServicePresent, "service.name", serviceNamePresent, true},
		{caseServiceEmpty, "service.name", "", true},
		{caseServiceMissing, "service.name", "", false},
		{caseServiceDiscovered, "container.name", discoveredServiceName, true},
		{caseServiceMissingJob, "job", missingServiceJob, true},
	}
	for _, tc := range checks {
		c, ok := plan.caseByName(tc.caseName)
		if !ok {
			t.Fatalf("case %q missing", tc.caseName)
		}
		attrs := plan.logs(c).ResourceLogs().At(0).Resource().Attributes()
		value, present := attrs.Get(tc.key)
		if present != tc.present {
			t.Fatalf("case %q attribute %q present = %v, want %v", tc.caseName, tc.key, present, tc.present)
		}
		if present && value.Str() != tc.want {
			t.Fatalf("case %q attribute %q = %q, want %q", tc.caseName, tc.key, value.Str(), tc.want)
		}
		if tc.caseName == caseServiceMissingJob {
			if _, ok := attrs.Get("service.name"); ok {
				t.Fatal("service-missing-job unexpectedly carries service.name")
			}
		}
	}
}

func TestCollisionFixtureHasSourceAttributes(t *testing.T) {
	plan := buildPlan(testAnchor(), "run-1", "ns")
	c, ok := plan.caseByName(caseCollisionMeta)
	if !ok {
		t.Fatal("collision-meta case missing")
	}
	logs := plan.logs(c)
	resource := logs.ResourceLogs().At(0).Resource().Attributes()
	if got, ok := resource.Get("collide.name"); !ok || got.Str() != "resource" {
		t.Fatalf("resource collide.name = %v (present %v)", got.AsString(), ok)
	}
	nested, ok := resource.Get("http.request")
	if !ok || nested.Type() != pcommon.ValueTypeMap {
		t.Fatalf("resource http.request is %v", nested.Type())
	}
	scope := logs.ResourceLogs().At(0).ScopeLogs().At(0)
	scopeAttrs := scope.Scope().Attributes()
	if got, ok := scopeAttrs.Get("collide_name"); !ok || got.Str() != "scope" {
		t.Fatalf("scope collide_name = %v (present %v)", got.AsString(), ok)
	}
	recordAttrs := scope.LogRecords().At(0).Attributes()
	if got, ok := recordAttrs.Get("collide.name"); !ok || got.Str() != "log" {
		t.Fatalf("record collide.name = %v (present %v)", got.AsString(), ok)
	}
	if got, ok := recordAttrs.Get("http.request.method"); !ok || got.Str() != "LOG" {
		t.Fatalf("record http.request.method = %v (present %v)", got.AsString(), ok)
	}
}

func TestFixtureEncodingRoundTrip(t *testing.T) {
	plan := buildPlan(testAnchor(), "run-1", "ns")
	for _, codec := range []string{codecProtobuf, codecJSON} {
		t.Run(codec, func(t *testing.T) {
			for _, c := range plan.cases {
				body, contentType, err := encodeCase(plan, c, codec)
				if err != nil {
					t.Fatalf("%s: encode: %v", c.name, err)
				}
				wantType := contentTypeProtobuf
				if codec == codecJSON {
					wantType = contentTypeJSON
				}
				if contentType != wantType {
					t.Fatalf("%s: content type = %q, want %q", c.name, contentType, wantType)
				}
				decoded := plogotlp.NewExportRequest()
				if codec == codecProtobuf {
					err = decoded.UnmarshalProto(body)
				} else {
					err = decoded.UnmarshalJSON(body)
				}
				if err != nil {
					t.Fatalf("%s: decode: %v", c.name, err)
				}
				if got, want := decoded.Logs().LogRecordCount(), len(c.records); got != want {
					t.Fatalf("%s: decoded records = %d, want %d", c.name, got, want)
				}
			}
		})
	}
}

func TestFixtureRecordFieldsSurviveEncoding(t *testing.T) {
	plan := buildPlan(testAnchor(), "run-1", "ns")
	severity, ok := plan.caseByName(caseSeverityFlags)
	if !ok {
		t.Fatal("severity-flags case missing")
	}
	logs := plan.logs(severity)
	records := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if records.Len() != 3 {
		t.Fatalf("severity records = %d, want 3", records.Len())
	}
	first := records.At(0)
	if first.SeverityNumber() != plog.SeverityNumberWarn || first.SeverityText() != "WARN" {
		t.Fatalf("severity = %v %q", first.SeverityNumber(), first.SeverityText())
	}
	if first.Flags() != plog.LogRecordFlags(1) || first.DroppedAttributesCount() != 2 {
		t.Fatalf("flags/dropped = %v/%d", first.Flags(), first.DroppedAttributesCount())
	}
	if first.ObservedTimestamp() != pcommon.Timestamp(severity.records[0].timestamp.Add(500*time.Millisecond).UnixNano()) {
		t.Fatalf("observed timestamp = %d", first.ObservedTimestamp())
	}
	fallback := records.At(2)
	if fallback.Timestamp() != 0 {
		t.Fatalf("observed fallback record has timestamp %d", fallback.Timestamp())
	}
	if fallback.ObservedTimestamp() != pcommon.Timestamp(severity.records[2].timestamp.UnixNano()) {
		t.Fatalf("observed fallback timestamp = %d, want %d",
			fallback.ObservedTimestamp(), severity.records[2].timestamp.UnixNano())
	}

	trace, ok := plan.caseByName(caseTraceSpan)
	if !ok {
		t.Fatal("trace-span case missing")
	}
	traceRecords := plan.logs(trace).ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if traceRecords.At(0).TraceID().IsEmpty() || traceRecords.At(0).SpanID().IsEmpty() {
		t.Fatal("full trace/span record is missing ids")
	}
	if !traceRecords.At(1).TraceID().IsEmpty() || traceRecords.At(1).SpanID().IsEmpty() {
		t.Fatal("span-only record ids are wrong")
	}
	if !traceRecords.At(2).TraceID().IsEmpty() || !traceRecords.At(2).SpanID().IsEmpty() {
		t.Fatal("empty trace/span record has ids")
	}
}

func TestFixtureNestedAndBodyValues(t *testing.T) {
	plan := buildPlan(testAnchor(), "run-1", "ns")
	nested, ok := plan.caseByName(caseNestedValues)
	if !ok {
		t.Fatal("nested-values case missing")
	}
	attrs := plan.logs(nested).ResourceLogs().At(0).Resource().Attributes()
	raw := attrs.AsRaw()
	wantNested := map[string]any{
		"nested.map": map[string]any{"alpha": "one", "beta": int64(2), "gamma": true},
	}
	if got := raw["nested.map"]; !equalAny(got, wantNested["nested.map"]) {
		t.Fatalf("nested.map = %#v", got)
	}
	bytesValue, ok := attrs.Get("nested.bytes")
	if !ok || bytesValue.Type() != pcommon.ValueTypeBytes {
		t.Fatalf("nested.bytes type = %v", bytesValue.Type())
	}
	shapes, ok := plan.caseByName(caseBodyShapes)
	if !ok {
		t.Fatal("body-shapes case missing")
	}
	bodyValues := plan.logs(shapes).ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	if got := bodyValues.At(0).Body().Type(); got != pcommon.ValueTypeMap {
		t.Fatalf("body-shapes 0 type = %v", got)
	}
	if got := bodyValues.At(1).Body().Type(); got != pcommon.ValueTypeSlice {
		t.Fatalf("body-shapes 1 type = %v", got)
	}
	if got := bodyValues.At(2).Body().AsString(); got != "42" {
		t.Fatalf("body-shapes 2 = %q", got)
	}
	if got := bodyValues.At(3).Body().AsString(); got != "" {
		t.Fatalf("body-shapes 3 = %q", got)
	}
}

// equalAny compares the small value model produced by pcommon.Map.AsRaw.
func equalAny(a, b any) bool {
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			if !equalAny(v, bv[k]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
