package otlpcodec

import (
	"context"
	"errors"
	"maps"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/schema"
)

var testNow = time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)

type attr struct{ k, v string }

func strAttrs(m pcommon.Map, attrs ...attr) {
	for _, a := range attrs {
		m.PutStr(a.k, a.v)
	}
}

func addResource(ld plog.Logs, attrs ...attr) plog.ResourceLogs {
	rl := ld.ResourceLogs().AppendEmpty()
	strAttrs(rl.Resource().Attributes(), attrs...)
	return rl
}

func addScope(rl plog.ResourceLogs) plog.ScopeLogs {
	return rl.ScopeLogs().AppendEmpty()
}

func addEntry(sl plog.ScopeLogs, line string, ns uint64) plog.LogRecord {
	rec := sl.LogRecords().AppendEmpty()
	rec.Body().SetStr(line)
	rec.SetTimestamp(pcommon.Timestamp(ns))
	return rec
}

func fixedDecoder() Decoder {
	return Decoder{now: func() time.Time { return testNow }}
}

func decodeLogs(t *testing.T, ld plog.Logs) *logs.PushRequest {
	t.Helper()
	req, err := fixedDecoder().Decode(t.Context(), plogotlp.NewExportRequestFromLogs(ld))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	validatePush(t, req)
	return req
}

// validatePush asserts the output contract every decoder call must satisfy:
// sorted, unique and valid stream labels and entry metadata.
func validatePush(t *testing.T, req *logs.PushRequest) {
	t.Helper()
	for si := range req.Streams {
		s := &req.Streams[si]
		if err := s.Labels.Validate(); err != nil {
			t.Fatalf("stream %d labels: %v", si, err)
		}
		for ei := range s.Entries {
			if err := s.Entries[ei].Metadata.Validate(); err != nil {
				t.Fatalf("stream %d entry %d metadata: %v", si, ei, err)
			}
		}
	}
}

func metaMap(t *testing.T, e logs.Entry) map[string]string {
	t.Helper()
	if err := e.Metadata.Validate(); err != nil {
		t.Fatalf("metadata: %v", err)
	}
	return e.Metadata.Map()
}

func oneStream(t *testing.T, req *logs.PushRequest) logs.Stream {
	t.Helper()
	if len(req.Streams) != 1 {
		t.Fatalf("got %d streams, want 1: %+v", len(req.Streams), req.Streams)
	}
	return req.Streams[0]
}

func TestLokiDefaultLists(t *testing.T) {
	// Exact copies of otlp_config.go:56-75 and validation/limits.go:329-343.
	index := []string{
		"service.name",
		"service.namespace",
		"service.instance.id",
		"deployment.environment",
		"deployment.environment.name",
		"cloud.region",
		"cloud.availability_zone",
		"k8s.cluster.name",
		"k8s.namespace.name",
		"k8s.pod.name",
		"k8s.container.name",
		"container.name",
		"k8s.replicaset.name",
		"k8s.deployment.name",
		"k8s.statefulset.name",
		"k8s.daemonset.name",
		"k8s.cronjob.name",
		"k8s.job.name",
	}
	if len(defaultIndexAttributes) != len(index) {
		t.Fatalf("index attributes = %d, upstream default has %d", len(defaultIndexAttributes), len(index))
	}
	for _, name := range index {
		if _, ok := defaultIndexAttributes[name]; !ok {
			t.Errorf("index attribute %q is missing", name)
		}
	}

	discover := []string{
		"service",
		"app",
		"application",
		"app_name",
		"name",
		"app_kubernetes_io_name",
		"container",
		"container_name",
		"k8s_container_name",
		"component",
		"workload",
		"job",
		"k8s_job_name",
	}
	if !slices.Equal(defaultDiscoverAttributes, discover) {
		t.Fatalf("discovery attributes = %v, want %v", defaultDiscoverAttributes, discover)
	}
}

// TestDefaultResourceIndexLabels covers every attribute in Loki's
// distributor.otlp.default_resource_attributes_as_index_labels default
// (otlp_config.go:56-75).
func TestDefaultResourceIndexLabels(t *testing.T) {
	attrs := []attr{
		{"service.name", "api"},
		{"service.namespace", "checkout"},
		{"service.instance.id", "i-1"},
		{"deployment.environment", "prod"},
		{"deployment.environment.name", "production"},
		{"cloud.region", "eu-west-1"},
		{"cloud.availability_zone", "eu-west-1a"},
		{"k8s.cluster.name", "cluster"},
		{"k8s.namespace.name", "ns"},
		{"k8s.pod.name", "pod"},
		{"k8s.container.name", "k8s-app"},
		{"container.name", "app"},
		{"k8s.replicaset.name", "rs"},
		{"k8s.deployment.name", "deploy"},
		{"k8s.statefulset.name", "sts"},
		{"k8s.daemonset.name", "ds"},
		{"k8s.cronjob.name", "cj"},
		{"k8s.job.name", "job"},
	}
	want := map[string]string{
		"service_name":                "api",
		"service_namespace":           "checkout",
		"service_instance_id":         "i-1",
		"deployment_environment":      "prod",
		"deployment_environment_name": "production",
		"cloud_region":                "eu-west-1",
		"cloud_availability_zone":     "eu-west-1a",
		"k8s_cluster_name":            "cluster",
		"k8s_namespace_name":          "ns",
		"k8s_pod_name":                "pod",
		"k8s_container_name":          "k8s-app",
		"container_name":              "app",
		"k8s_replicaset_name":         "rs",
		"k8s_deployment_name":         "deploy",
		"k8s_statefulset_name":        "sts",
		"k8s_daemonset_name":          "ds",
		"k8s_cronjob_name":            "cj",
		"k8s_job_name":                "job",
	}

	ld := plog.NewLogs()
	rl := addResource(ld, attrs...)
	addEntry(addScope(rl), "line", 1)

	s := oneStream(t, decodeLogs(t, ld))
	if got := s.Labels.Map(); !maps.Equal(got, want) {
		t.Fatalf("stream labels = %v, want %v", got, want)
	}
	if got := len(s.Entries); got != 1 {
		t.Fatalf("got %d entries, want 1", got)
	}
	if meta := s.Entries[0].Metadata; len(meta) != 0 {
		t.Fatalf("indexed attributes leaked into metadata: %v", meta)
	}
}

// TestResourceScopeLogMetadata covers non-indexed resource attributes and
// every scope and log-record field mapped by otlp.go:298-339 and :482-573.
func TestResourceScopeLogMetadata(t *testing.T) {
	ld := plog.NewLogs()
	rl := addResource(ld,
		attr{"service.name", "api"},
		attr{"deployment.environment", "prod"},
		attr{"host.name", "node-1"},
	)
	rl.Resource().Attributes().PutInt("cloud.account.id", 42)

	sl := addScope(rl)
	scope := sl.Scope()
	scope.SetName("instrumentation/lib")
	scope.SetVersion("1.2.3")
	scope.SetDroppedAttributesCount(2)
	strAttrs(scope.Attributes(), attr{"scope.attr", "sv1"})

	rec := addEntry(sl, "hello", 1000)
	strAttrs(rec.Attributes(), attr{"http.method", "GET"})
	rec.Attributes().PutInt("http.status_code", 200)
	request := rec.Attributes().PutEmptyMap("http.request")
	request.PutStr("method", "GET")
	strAttrs(request.PutEmptyMap("headers"), attr{"x-trace-id", "t1"})

	s := oneStream(t, decodeLogs(t, ld))
	wantLabels := map[string]string{
		"service_name":           "api",
		"deployment_environment": "prod",
	}
	if got := s.Labels.Map(); !maps.Equal(got, wantLabels) {
		t.Fatalf("stream labels = %v, want %v", got, wantLabels)
	}

	wantMeta := map[string]string{
		"http_method":                     "GET",
		"http_status_code":                "200",
		"http_request_method":             "GET",
		"http_request_headers_x_trace_id": "t1",
		"host_name":                       "node-1",
		"cloud_account_id":                "42",
		"scope_attr":                      "sv1",
		"scope_name":                      "instrumentation/lib",
		"scope_version":                   "1.2.3",
		"scope_dropped_attributes_count":  "2",
	}
	if got := metaMap(t, s.Entries[0]); !maps.Equal(got, wantMeta) {
		t.Fatalf("metadata = %v, want %v", got, wantMeta)
	}
}

func TestBodyConversion(t *testing.T) {
	cases := []struct {
		name string
		set  func(pcommon.Value)
		want string
	}{
		{"empty", func(pcommon.Value) {}, ""},
		{"string", func(v pcommon.Value) { v.SetStr("hello") }, "hello"},
		{"int", func(v pcommon.Value) { v.SetInt(42) }, "42"},
		{"double", func(v pcommon.Value) { v.SetDouble(1.25) }, "1.25"},
		{"bool", func(v pcommon.Value) { v.SetBool(true) }, "true"},
		{"bytes", func(v pcommon.Value) { v.SetEmptyBytes().FromRaw([]byte("hi")) }, "aGk="},
		{"slice", func(v pcommon.Value) {
			s := v.SetEmptySlice()
			s.AppendEmpty().SetStr("a")
			s.AppendEmpty().SetInt(2)
		}, `["a",2]`},
		{"map", func(v pcommon.Value) {
			m := v.SetEmptyMap()
			m.PutStr("k", "v")
			m.PutInt("n", 1)
		}, `{"k":"v","n":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ld := plog.NewLogs()
			rl := addResource(ld, attr{"service.name", "api"})
			sl := addScope(rl)
			rec := sl.LogRecords().AppendEmpty()
			rec.SetTimestamp(1)
			tc.set(rec.Body())

			s := oneStream(t, decodeLogs(t, ld))
			if got := s.Entries[0].Line; got != tc.want {
				t.Fatalf("line = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAttributeValues(t *testing.T) {
	ld := plog.NewLogs()
	rl := addResource(ld, attr{"service.name", "api"})
	rec := addEntry(addScope(rl), "x", 1)
	rec.Attributes().PutStr("s", "v")
	rec.Attributes().PutInt("i", 7)
	rec.Attributes().PutDouble("d", 2.5)
	rec.Attributes().PutBool("b", true)
	rec.Attributes().PutEmptyBytes("by").FromRaw([]byte{1, 2})
	arr := rec.Attributes().PutEmptySlice("arr")
	arr.AppendEmpty().SetInt(1)
	arr.AppendEmpty().SetInt(2)
	m := rec.Attributes().PutEmptyMap("m")
	m.PutStr("a", "b")
	rec.Attributes().PutEmptyMap("empty_map")
	rec.Attributes().PutEmpty("empty_value")

	want := map[string]string{
		"s":           "v",
		"i":           "7",
		"d":           "2.5",
		"b":           "true",
		"by":          "AQI=",
		"arr":         "[1,2]",
		"m_a":         "b",
		"empty_value": "",
	}
	got := metaMap(t, oneStream(t, decodeLogs(t, ld)).Entries[0])
	if !maps.Equal(got, want) {
		t.Fatalf("metadata = %v, want %v", got, want)
	}
}

// TestLabelNameMapping uses upstream label_namer_test.go vectors through the
// full decoder, so normalization is exercised on real OTLP attributes.
func TestLabelNameMapping(t *testing.T) {
	cases := []struct{ in, want string }{
		{"label:with:colons", "label_with_colons"},
		{"LabelWithCapitalLetters", "LabelWithCapitalLetters"},
		{"label_with_foreign_characters_字符", "label_with_foreign_characters_"},
		{"label.with.dots", "label_with_dots"},
		{"123label", "key_123label"},
		{"_label_starting_with_underscore", "_label_starting_with_underscore"},
		{"__label_starting_with_2underscores", "_label_starting_with_2underscores"},
		{"__reserved__label__name__", "__reserved_label_name__"},
		{"trailing_underscores___", "trailing_underscores_"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			ld := plog.NewLogs()
			rl := addResource(ld, attr{"service.name", "api"})
			rec := addEntry(addScope(rl), "x", 1)
			rec.Attributes().PutStr(tc.in, "v")

			got := metaMap(t, oneStream(t, decodeLogs(t, ld)).Entries[0])
			if v, ok := got[tc.want]; !ok || v != "v" {
				t.Fatalf("metadata = %v, want %q = %q", got, tc.want, "v")
			}
		})
	}
}

func TestInvalidAttributeNames(t *testing.T) {
	for _, name := range []string{"", "__", "___", "...", "ようこそ"} {
		t.Run(name, func(t *testing.T) {
			ld := plog.NewLogs()
			rl := addResource(ld, attr{"service.name", "api"})
			rec := addEntry(addScope(rl), "x", 1)
			rec.Attributes().PutStr(name, "v")

			_, err := fixedDecoder().Decode(t.Context(), plogotlp.NewExportRequestFromLogs(ld))
			if err == nil {
				t.Fatalf("Decode accepted invalid attribute name %q", name)
			}
			if !strings.Contains(err.Error(), "otlpcodec: log attribute") {
				t.Fatalf("error = %v, want log attribute context", err)
			}
		})
	}
}

func TestInvalidAttributeErrorLocations(t *testing.T) {
	cases := []struct {
		name  string
		build func(rl plog.ResourceLogs, sl plog.ScopeLogs, rec plog.LogRecord)
		want  string
	}{
		{
			name: "scope attribute",
			build: func(_ plog.ResourceLogs, sl plog.ScopeLogs, _ plog.LogRecord) {
				sl.Scope().Attributes().PutStr("!!!", "v")
			},
			want: "otlpcodec: scope attribute",
		},
		{
			name: "resource attribute",
			build: func(rl plog.ResourceLogs, _ plog.ScopeLogs, _ plog.LogRecord) {
				rl.Resource().Attributes().PutStr("!!!", "v")
			},
			want: "otlpcodec: resource attribute",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ld := plog.NewLogs()
			rl := addResource(ld, attr{"service.name", "api"})
			sl := addScope(rl)
			rec := addEntry(sl, "x", 1)
			tc.build(rl, sl, rec)

			_, err := fixedDecoder().Decode(t.Context(), plogotlp.NewExportRequestFromLogs(ld))
			if err == nil {
				t.Fatalf("Decode accepted invalid attribute")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestMetadataCollisionLastWins documents last-value resolution for small
// metadata sets. Loki sorts converter metadata with slices.SortFunc and the
// query pipeline keeps the last value; short runs retain their append order,
// so the fixed fields win here. TestFixedFieldMetadataFixture covers the
// larger live fixture where the unstable permutation flips trace_id.
func TestMetadataCollisionLastWins(t *testing.T) {
	t.Run("log attribute order", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			first     string
			second    string
			wantValue string
		}{
			{"dotted then underscore", "a.b", "a_b", "second"},
			{"underscore then dotted", "a_b", "a.b", "second"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ld := plog.NewLogs()
				rl := addResource(ld, attr{"service.name", "api"})
				rec := addEntry(addScope(rl), "x", 1)
				rec.Attributes().PutStr(tc.first, "first")
				rec.Attributes().PutStr(tc.second, "second")

				got := metaMap(t, oneStream(t, decodeLogs(t, ld)).Entries[0])
				if got["a_b"] != tc.wantValue || len(got) != 1 {
					t.Fatalf("metadata = %v, want a_b=%q and nothing else", got, tc.wantValue)
				}
			})
		}
	})

	t.Run("fixed field beats log attribute", func(t *testing.T) {
		ld := plog.NewLogs()
		rl := addResource(ld, attr{"service.name", "api"})
		rec := addEntry(addScope(rl), "x", 1)
		rec.Attributes().PutStr("severity_text", "from-attribute")
		rec.SetSeverityText("fixed")

		got := metaMap(t, oneStream(t, decodeLogs(t, ld)).Entries[0])
		if got["severity_text"] != "fixed" {
			t.Fatalf("severity_text = %q, want fixed", got["severity_text"])
		}
	})

	t.Run("resource beats log attribute", func(t *testing.T) {
		ld := plog.NewLogs()
		rl := addResource(ld, attr{"service.name", "api"}, attr{"env", "resource"})
		rec := addEntry(addScope(rl), "x", 1)
		rec.Attributes().PutStr("env", "log")

		got := metaMap(t, oneStream(t, decodeLogs(t, ld)).Entries[0])
		if got["env"] != "resource" {
			t.Fatalf("env = %q, want resource", got["env"])
		}
	})

	t.Run("scope beats resource", func(t *testing.T) {
		ld := plog.NewLogs()
		rl := addResource(ld, attr{"service.name", "api"}, attr{"env", "resource"})
		sl := addScope(rl)
		sl.Scope().Attributes().PutStr("env", "scope")
		addEntry(sl, "x", 1)

		got := metaMap(t, oneStream(t, decodeLogs(t, ld)).Entries[0])
		if got["env"] != "scope" {
			t.Fatalf("env = %q, want scope", got["env"])
		}
	})

	t.Run("resource beats fixed field", func(t *testing.T) {
		ld := plog.NewLogs()
		rl := addResource(ld, attr{"service.name", "api"}, attr{"observed_timestamp", "resource"})
		rec := addEntry(addScope(rl), "x", 1000)
		rec.SetObservedTimestamp(2000)

		got := metaMap(t, oneStream(t, decodeLogs(t, ld)).Entries[0])
		if got["observed_timestamp"] != "resource" {
			t.Fatalf("observed_timestamp = %q, want resource", got["observed_timestamp"])
		}
	})
}

// TestDuplicateIndexedResourceAttribute exercises the only way a pinned
// default index label can collide: the wire carries the same resource
// attribute twice. Loki's map assignment keeps the last value.
func TestDuplicateIndexedResourceAttribute(t *testing.T) {
	body := `{"resourceLogs":[{"resource":{"attributes":[` +
		`{"key":"service.name","value":{"stringValue":"first"}},` +
		`{"key":"service.name","value":{"stringValue":"second"}}]},` +
		`"scopeLogs":[{"logRecords":[{"timeUnixNano":"1000","body":{"stringValue":"x"}}]}]}]}`

	req, err := Decoder{}.DecodeJSON(t.Context(), []byte(body))
	if err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	validatePush(t, req)
	s := oneStream(t, req)
	if got := s.Labels.Map()["service_name"]; got != "second" {
		t.Fatalf("service_name = %q, want last value second", got)
	}
}

// TestIndexedResourceMapValue matches otlp.go:186-207: a map-valued index
// attribute becomes one label per leaf, so service.name is not present even
// though the raw attribute is non-empty and suppresses discovery.
func TestIndexedResourceMapValue(t *testing.T) {
	ld := plog.NewLogs()
	rl := addResource(ld)
	service := rl.Resource().Attributes().PutEmptyMap("service.name")
	service.PutStr("a", "1")
	service.PutStr("b", "2")
	addEntry(addScope(rl), "x", 1)

	s := oneStream(t, decodeLogs(t, ld))
	want := map[string]string{"service_name_a": "1", "service_name_b": "2"}
	if got := s.Labels.Map(); !maps.Equal(got, want) {
		t.Fatalf("labels = %v, want %v", got, want)
	}
}

// TestDiscoveryOnlyForIndexedAttributes matches otlp.go:191-207: discovery
// only inspects attributes whose action is IndexLabel. "job" is in the
// discovery list but not in the default index list, so it stays metadata and
// service_name falls back to unknown_service.
func TestDiscoveryOnlyForIndexedAttributes(t *testing.T) {
	ld := plog.NewLogs()
	rl := addResource(ld, attr{"job", "batch"}, attr{"host.name", "node"})
	addEntry(addScope(rl), "x", 1)

	s := oneStream(t, decodeLogs(t, ld))
	if got := s.Labels.Map()["service_name"]; got != serviceUnknown {
		t.Fatalf("service_name = %q, want %q", got, serviceUnknown)
	}
	if got := metaMap(t, s.Entries[0])["job"]; got != "batch" {
		t.Fatalf("job metadata = %q, want batch", got)
	}
}

// TestFixedFieldMetadataFixture reproduces the live Loki v3.7.9 fixed-field
// fixture (cmd/otlpcompare fixtures.go caseFixedField, report
// report-direct-protobuf.json). Loki exposes the log attribute
// trace_id=attr-trace while the other fixed fields keep their fixed values:
// the converter's metadata slice is sorted by an unstable sort before the
// query pipeline's last-value-wins dedupe, and that permutation flips only
// the trace_id pair for this metadata shape.
func TestFixedFieldMetadataFixture(t *testing.T) {
	ts := time.Date(2026, 11, 5, 5, 17, 10, 824423000, time.UTC)
	observed := ts.Add(250 * time.Millisecond)

	ld := plog.NewLogs()
	rl := addResource(ld, attr{"service.name", "fixed-svc"}, attr{"scope.name", "attr-scope-name"})
	sl := addScope(rl)
	scope := sl.Scope()
	scope.SetName("fixed.scope")
	scope.SetVersion("9.9.9")

	rec := addEntry(sl, "fixed field collision line", uint64(ts.UnixNano()))
	strAttrs(rec.Attributes(),
		attr{"severity.text", "attr-severity"},
		attr{"trace.id", "attr-trace"},
		attr{"observed.timestamp", "attr-observed"},
		attr{"flags", "attr-flags"},
		attr{"span.id", "attr-span"},
		attr{"scope.version", "attr-scope-version"},
		attr{"dropped.attributes.count", "attr-dropped"},
	)
	rec.SetSeverityNumber(plog.SeverityNumberError)
	rec.SetSeverityText("ERROR")
	rec.SetFlags(plog.LogRecordFlags(1))
	rec.SetDroppedAttributesCount(4)
	rec.SetObservedTimestamp(pcommon.Timestamp(observed.UnixNano()))
	rec.SetTraceID(traceIDFor(0x21))
	rec.SetSpanID(spanIDFor(0x71))

	s := oneStream(t, decodeLogs(t, ld))
	want := map[string]string{
		"dropped_attributes_count": "4",
		"flags":                    "1",
		"observed_timestamp":       strconv.FormatInt(observed.UnixNano(), 10),
		"scope_name":               "fixed.scope",
		"scope_version":            "9.9.9",
		"severity_number":          "17",
		"severity_text":            "ERROR",
		"span_id":                  "7172737475767778",
		"trace_id":                 "attr-trace",
	}
	if got := metaMap(t, s.Entries[0]); !maps.Equal(got, want) {
		t.Fatalf("metadata = %v, want %v", got, want)
	}
}

// traceIDFor mirrors the deterministic ids in cmd/otlpcompare fixtures.go.
func traceIDFor(seed byte) pcommon.TraceID {
	var id pcommon.TraceID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func spanIDFor(seed byte) pcommon.SpanID {
	var id pcommon.SpanID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func TestServiceNameDiscovery(t *testing.T) {
	cases := []struct {
		name string
		// attrs are inserted in order; only some are index labels.
		attrs []attr
		want  string
	}{
		{
			name:  "service name present",
			attrs: []attr{{"service.name", "api"}, {"k8s.job.name", "job"}},
			want:  "api",
		},
		{
			name:  "missing uses k8s job",
			attrs: []attr{{"k8s.job.name", "job"}},
			want:  "job",
		},
		{
			name:  "missing uses k8s container",
			attrs: []attr{{"k8s.container.name", "k8s-ctr"}},
			want:  "k8s-ctr",
		},
		{
			name:  "missing without candidate",
			attrs: []attr{{"host.name", "node"}},
			want:  serviceUnknown,
		},
		{
			name:  "empty service name uses candidate",
			attrs: []attr{{"service.name", ""}, {"k8s.job.name", "job"}},
			want:  "job",
		},
		{
			name:  "empty service name without candidate",
			attrs: []attr{{"service.name", ""}, {"host.name", "node"}},
			want:  serviceUnknown,
		},
		{
			name:  "first candidate in attribute order",
			attrs: []attr{{"k8s.job.name", "job"}, {"container.name", "ctr"}},
			want:  "job",
		},
		{
			name:  "reversed attribute order",
			attrs: []attr{{"container.name", "ctr"}, {"k8s.job.name", "job"}},
			want:  "ctr",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ld := plog.NewLogs()
			rl := addResource(ld, tc.attrs...)
			addEntry(addScope(rl), "x", 1)

			s := oneStream(t, decodeLogs(t, ld))
			if got := s.Labels.Map()["service_name"]; got != tc.want {
				t.Fatalf("service_name = %q, want %q (labels %v)", got, tc.want, s.Labels)
			}
		})
	}
}

func TestRecordFields(t *testing.T) {
	ld := plog.NewLogs()
	rl := addResource(ld, attr{"service.name", "api"})
	rec := addEntry(addScope(rl), "x", 1000)
	rec.SetObservedTimestamp(2000)
	rec.SetSeverityNumber(plog.SeverityNumberError)
	rec.SetSeverityText("error")
	rec.SetDroppedAttributesCount(3)
	rec.SetFlags(plog.LogRecordFlags(1))
	rec.SetTraceID(pcommon.TraceID{0xde, 0xad, 0xbe, 0xef, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	rec.SetSpanID(pcommon.SpanID{1, 2, 3, 4, 5, 6, 7, 8})

	want := map[string]string{
		"observed_timestamp":       "2000",
		"severity_number":          "17",
		"severity_text":            "error",
		"dropped_attributes_count": "3",
		"flags":                    "1",
		"trace_id":                 "deadbeef000102030405060708090a0b",
		"span_id":                  "0102030405060708",
	}
	got := metaMap(t, oneStream(t, decodeLogs(t, ld)).Entries[0])
	if !maps.Equal(got, want) {
		t.Fatalf("metadata = %v, want %v", got, want)
	}
}

func TestRecordFieldsOmittedWhenZero(t *testing.T) {
	ld := plog.NewLogs()
	rl := addResource(ld, attr{"service.name", "api"})
	addEntry(addScope(rl), "x", 1000)

	if got := oneStream(t, decodeLogs(t, ld)).Entries[0].Metadata; len(got) != 0 {
		t.Fatalf("metadata = %v, want empty", got)
	}
}

func TestTimestampFallback(t *testing.T) {
	cases := []struct {
		name         string
		ts           uint64
		observed     uint64
		want         time.Time
		wantObserved string
	}{
		{"timestamp", 1000, 0, time.Unix(0, 1000).UTC(), ""},
		{"observed fallback", 0, 2000, time.Unix(0, 2000).UTC(), ""},
		{"clock fallback", 0, 0, testNow, ""},
		{"both", 1000, 2000, time.Unix(0, 1000).UTC(), "2000"},
		{"max uint64 wraps", math.MaxUint64, 0, time.Unix(0, -1).UTC(), ""},
		{"max int64", math.MaxInt64, 0, time.Unix(0, math.MaxInt64).UTC(), ""},
		{"observed max uint64 wraps", 0, math.MaxUint64, time.Unix(0, -1).UTC(), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ld := plog.NewLogs()
			rl := addResource(ld, attr{"service.name", "api"})
			rec := addEntry(addScope(rl), "x", tc.ts)
			rec.SetObservedTimestamp(pcommon.Timestamp(tc.observed))

			e := oneStream(t, decodeLogs(t, ld)).Entries[0]
			if !e.Timestamp.Equal(tc.want) {
				t.Fatalf("timestamp = %s, want %s", e.Timestamp, tc.want)
			}
			if got := metaMap(t, e)["observed_timestamp"]; got != tc.wantObserved {
				t.Fatalf("observed_timestamp = %q, want %q", got, tc.wantObserved)
			}
		})
	}
}

func TestTimestampMicrosecondNormalization(t *testing.T) {
	const ns = 1_700_000_000_123_456_789
	ld := plog.NewLogs()
	rl := addResource(ld, attr{"service.name", "api"})
	addEntry(addScope(rl), "x", ns)

	e := oneStream(t, decodeLogs(t, ld)).Entries[0]
	if got := e.Timestamp.UnixNano(); got != ns {
		t.Fatalf("UnixNano = %d, want %d", got, ns)
	}
	if got := schema.TS(e.Timestamp); got != ns/1000 {
		t.Fatalf("schema.TS = %d, want %d (truncate, do not round)", got, ns/1000)
	}
}

func TestGroupsIdenticalLabelSets(t *testing.T) {
	ld := plog.NewLogs()

	first := addResource(ld, attr{"service.name", "api"}, attr{"env", "prod"})
	rec := addEntry(addScope(first), "one", 1)
	rec.Attributes().PutStr("kind", "a")

	second := addResource(ld, attr{"service.name", "api"}, attr{"env", "canary"})
	rec = addEntry(addScope(second), "two", 2)
	rec.Attributes().PutStr("kind", "b")

	other := addResource(ld, attr{"service.name", "worker"})
	addEntry(addScope(other), "three", 3)

	req := decodeLogs(t, ld)
	if len(req.Streams) != 2 {
		t.Fatalf("got %d streams, want 2: %+v", len(req.Streams), req.Streams)
	}

	api := req.Streams[0]
	if got := api.Labels.Map()["service_name"]; got != "api" {
		t.Fatalf("first stream service_name = %q, want api", got)
	}
	if len(api.Entries) != 2 || api.Entries[0].Line != "one" || api.Entries[1].Line != "two" {
		t.Fatalf("grouped entries = %+v, want one,two", api.Entries)
	}
	if got := metaMap(t, api.Entries[0])["env"]; got != "prod" {
		t.Fatalf("first entry env = %q, want prod", got)
	}
	if got := metaMap(t, api.Entries[1])["env"]; got != "canary" {
		t.Fatalf("second entry env = %q, want canary", got)
	}

	worker := req.Streams[1]
	if got := worker.Labels.Map()["service_name"]; got != "worker" {
		t.Fatalf("second stream service_name = %q, want worker", got)
	}
}

// TestResourceWithoutRecordsKeepsStream matches otlp.go:254-261: a resource
// with labels creates a stream even when its scopes carry no records.
func TestResourceWithoutRecordsKeepsStream(t *testing.T) {
	ld := plog.NewLogs()
	addResource(ld, attr{"service.name", "empty"})
	withRecord := addResource(ld, attr{"service.name", "api"})
	addEntry(addScope(withRecord), "x", 1)

	req := decodeLogs(t, ld)
	if len(req.Streams) != 2 {
		t.Fatalf("got %d streams, want 2", len(req.Streams))
	}
	if got := len(req.Streams[0].Entries); got != 0 {
		t.Fatalf("label-only stream has %d entries, want 0", got)
	}
	if got := req.Entries(); got != 1 {
		t.Fatalf("Entries() = %d, want 1", got)
	}
}

func TestZeroRecordsAndEmptyBodies(t *testing.T) {
	req, err := Decoder{}.Decode(t.Context(), plogotlp.NewExportRequest())
	if err != nil {
		t.Fatalf("Decode empty: %v", err)
	}
	if len(req.Streams) != 0 || req.Entries() != 0 {
		t.Fatalf("empty request = %+v, want no streams", req)
	}

	req, err = Decoder{}.DecodeProto(t.Context(), nil)
	if err != nil || len(req.Streams) != 0 {
		t.Fatalf("DecodeProto(nil) = %+v, %v; want empty request", req, err)
	}

	req, err = Decoder{}.DecodeJSON(t.Context(), []byte("{}"))
	if err != nil || len(req.Streams) != 0 {
		t.Fatalf("DecodeJSON({}) = %+v, %v; want empty request", req, err)
	}

	if _, err := (Decoder{}).DecodeProto(t.Context(), []byte{0xff, 0xff}); err == nil {
		t.Fatal("DecodeProto accepted invalid protobuf")
	}
	if _, err := (Decoder{}).DecodeJSON(t.Context(), []byte("{")); err == nil {
		t.Fatal("DecodeJSON accepted invalid JSON")
	}
}

func TestCancellation(t *testing.T) {
	ld := plog.NewLogs()
	rl := addResource(ld, attr{"service.name", "api"})
	addEntry(addScope(rl), "x", 1)
	body, err := plogotlp.NewExportRequestFromLogs(ld).MarshalProto()
	if err != nil {
		t.Fatalf("MarshalProto: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := fixedDecoder().DecodeProto(ctx, body); !errors.Is(err, context.Canceled) {
		t.Fatalf("DecodeProto error = %v, want context.Canceled", err)
	}
}

func TestProtoJSONEquivalence(t *testing.T) {
	ld := plog.NewLogs()

	rl := addResource(ld, attr{"service.name", "api"}, attr{"host.name", "node"})
	rl.Resource().Attributes().PutInt("cloud.account.id", 42)
	rl.Resource().Attributes().PutBool("sampled", true)
	rl.Resource().Attributes().PutDouble("ratio", 1.5)
	rl.Resource().Attributes().PutEmptyBytes("key").FromRaw([]byte{1, 2, 3})
	attrs := rl.Resource().Attributes().PutEmptyMap("resource.map")
	attrs.PutStr("a", "b")
	arr := rl.Resource().Attributes().PutEmptySlice("resource.array")
	arr.AppendEmpty().SetStr("x")

	sl := addScope(rl)
	sl.Scope().SetName("scope")
	sl.Scope().SetVersion("1")
	sl.Scope().Attributes().PutStr("scope.attr", "v")

	rec := addEntry(sl, "first", 1234567890)
	rec.SetObservedTimestamp(1234567999)
	rec.SetSeverityNumber(plog.SeverityNumberError)
	rec.SetSeverityText("error")
	rec.SetDroppedAttributesCount(1)
	rec.SetFlags(plog.LogRecordFlags(1))
	rec.SetTraceID(pcommon.TraceID{1, 2, 3, 4})
	rec.SetSpanID(pcommon.SpanID{5, 6, 7, 8})
	rec.Attributes().PutStr("http.method", "GET")
	rec.Attributes().PutInt("http.status_code", 500)
	body := rec.Body().SetEmptyMap()
	body.PutStr("msg", "boom")

	seconds := addResource(ld, attr{"service.name", "worker"})
	rec = addEntry(addScope(seconds), "second", 42)
	rec.Body().SetEmptyBytes().FromRaw([]byte("raw"))

	exp := plogotlp.NewExportRequestFromLogs(ld)
	protoBody, err := exp.MarshalProto()
	if err != nil {
		t.Fatalf("MarshalProto: %v", err)
	}
	jsonBody, err := exp.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	fromProto, err := fixedDecoder().DecodeProto(t.Context(), protoBody)
	if err != nil {
		t.Fatalf("DecodeProto: %v", err)
	}
	fromJSON, err := fixedDecoder().DecodeJSON(t.Context(), jsonBody)
	if err != nil {
		t.Fatalf("DecodeJSON: %v", err)
	}
	validatePush(t, fromProto)
	validatePush(t, fromJSON)

	if !reflect.DeepEqual(fromProto, fromJSON) {
		t.Fatalf("protobuf and JSON differ:\nproto: %+v\njson:  %+v", fromProto, fromJSON)
	}
}

func TestDecodeDoesNotMutateInputOrShareMetadata(t *testing.T) {
	ld := plog.NewLogs()
	api := addResource(ld, attr{"service.name", "api"})
	sl := addScope(api)
	for i, line := range []string{"one", "two"} {
		rec := addEntry(sl, line, uint64(i+1))
		rec.Attributes().PutStr("k", "v")
	}
	worker := addResource(ld, attr{"service.name", "worker"})
	addEntry(addScope(worker), "three", 3).Attributes().PutStr("k", "v")

	exp := plogotlp.NewExportRequestFromLogs(ld)
	before, err := exp.MarshalProto()
	if err != nil {
		t.Fatalf("MarshalProto: %v", err)
	}

	first := decodeLogs(t, ld)
	second := decodeLogs(t, ld)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("repeated decodes differ:\nfirst:  %+v\nsecond: %+v", first, second)
	}

	after, err := exp.MarshalProto()
	if err != nil {
		t.Fatalf("MarshalProto: %v", err)
	}
	if !slices.Equal(before, after) {
		t.Fatal("decoder mutated the input request")
	}

	// Metadata slices are per entry; mutating one must not touch a sibling
	// entry or an entry in another stream.
	first.Streams[0].Entries[0].Metadata[0].Value = "mutated"
	if got := first.Streams[0].Entries[1].Metadata[0].Value; got != "v" {
		t.Fatalf("sibling entry metadata = %q, want v", got)
	}
	if got := first.Streams[1].Entries[0].Metadata[0].Value; got != "v" {
		t.Fatalf("other stream metadata = %q, want v", got)
	}
}
