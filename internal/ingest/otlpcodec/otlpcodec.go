// Package otlpcodec converts OTLP log export requests into Parkana's push
// model (logs.PushRequest).
//
// It mirrors the conversion in Loki v3.7.9, pkg/loghttp/push/otlp.go and
// pkg/loghttp/push/otlp_config.go, with Loki's pinned defaults
// (https://github.com/grafana/loki/blob/v3.7.9/pkg/loghttp/push/otlp.go):
//
//   - The resource attributes listed in defaultIndexAttributes become stream
//     labels (otlp_config.go:56-75, action IndexLabel).
//   - Every other resource, scope and log-record attribute becomes structured
//     metadata (otlp_config.go:103, action StructuredMetadata).
//   - severity_text_as_label is off (otlp_config.go:47).
//   - Service name discovery uses validation/limits.go:329-343 and falls back
//     to unknown_service (otlp.go:218-220). Discovery is enabled because these
//     are upstream defaults; a Loki configured with an empty
//     validation.discover-service-name list does not add the label.
//
// Attribute names are normalized exactly like
// github.com/prometheus/otlptranslator v1.0.0 LabelNamer.Build with its
// default options. Only the pinned defaults are implemented; callers can't
// configure OTLP attribute actions here.
//
// Decoding has no I/O: the HTTP handler owns limits, compression and content
// type dispatch. DecodeProto, DecodeJSON and Decode take an already bounded
// body or pdata request and return an error on malformed data, invalid
// attribute names, or context cancellation.
//
// # Duplicate names
//
// OTLP attributes are free-form and two names can normalize to the same
// label or metadata name. Loki resolves index-label collisions through map
// assignment (otlp.go:193, last attribute wins). Structured metadata takes a
// longer path: the converter's slice is sorted by
// logproto.FromLabelAdaptersToLabels with slices.SortFunc
// (pkg/logproto/compat.go:81, called from pkg/chunkenc/memchunk.go:880),
// then the query pipeline's LabelsBuilder.Add overwrites an existing name
// (pkg/logql/log/pipeline.go:104,224 and pkg/logql/log/labels.go:344-410).
// The last value in that sorted order wins.
//
// That sort is not stable, so the surviving value of a duplicate-name run is
// the permutation produced by pdqsort. metadataLabels reproduces the same
// sort and last-value rule. The live Loki v3.7.9 fixed-field fixture depends
// on it: trace_id="attr-trace" survives while severity_text, span_id, flags,
// observed_timestamp, dropped_attributes_count, scope_name and scope_version
// keep their fixed values.
//
// Other collision shapes are only covered by real-Loki differential tests;
// this package makes no broader compatibility claim about duplicate names.
package otlpcodec

import (
	"cmp"
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"

	"github.com/nathabonfim59/parkana/internal/logs"
)

// Fixed metadata names Loki adds from log-record and scope fields
// (otlp.go:519-573, 322-339).
const (
	nameObservedTimestamp      = "observed_timestamp"
	nameSeverityNumber         = "severity_number"
	nameSeverityText           = "severity_text"
	nameDroppedAttributesCount = "dropped_attributes_count"
	nameFlags                  = "flags"
	nameTraceID                = "trace_id"
	nameSpanID                 = "span_id"
	nameScopeName              = "scope_name"
	nameScopeVersion           = "scope_version"
	nameScopeDroppedAttrsCount = "scope_dropped_attributes_count"
	nameService                = "service_name"
	attrServiceName            = "service.name"
	serviceUnknown             = "unknown_service"
)

// defaultIndexAttributes are the resource attributes Loki stores as index
// labels by default, in otlp_config.go:56-75. The list is compared with the
// raw OTLP attribute key.
var defaultIndexAttributes = map[string]struct{}{
	"service.name":                {},
	"service.namespace":           {},
	"service.instance.id":         {},
	"deployment.environment":      {},
	"deployment.environment.name": {},
	"cloud.region":                {},
	"cloud.availability_zone":     {},
	"k8s.cluster.name":            {},
	"k8s.namespace.name":          {},
	"k8s.pod.name":                {},
	"k8s.container.name":          {},
	"container.name":              {},
	"k8s.replicaset.name":         {},
	"k8s.deployment.name":         {},
	"k8s.statefulset.name":        {},
	"k8s.daemonset.name":          {},
	"k8s.cronjob.name":            {},
	"k8s.job.name":                {},
}

// defaultDiscoverAttributes is Loki's validation/limits.go:329-343 list,
// compared against the normalized label name produced from every indexed
// resource attribute.
var defaultDiscoverAttributes = []string{
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

// Decoder maps OTLP logs to the Parkana push model. The zero value is ready
// to use; now is a test seam for records that carry no timestamp.
type Decoder struct {
	now func() time.Time
}

// DecodeProto decodes an OTLP protobuf ExportLogsServiceRequest body.
func (d Decoder) DecodeProto(ctx context.Context, body []byte) (*logs.PushRequest, error) {
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalProto(body); err != nil {
		return nil, fmt.Errorf("otlpcodec: decode protobuf: %w", err)
	}
	return d.Decode(ctx, req)
}

// DecodeJSON decodes an OTLP JSON ExportLogsServiceRequest body.
func (d Decoder) DecodeJSON(ctx context.Context, body []byte) (*logs.PushRequest, error) {
	req := plogotlp.NewExportRequest()
	if err := req.UnmarshalJSON(body); err != nil {
		return nil, fmt.Errorf("otlpcodec: decode JSON: %w", err)
	}
	return d.Decode(ctx, req)
}

// Decode converts an already decoded OTLP export request. An empty request
// yields an empty push request, matching Loki's early return at otlp.go:144.
func (d Decoder) Decode(ctx context.Context, req plogotlp.ExportRequest) (*logs.PushRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("otlpcodec: %w", err)
	}
	ld := req.Logs()
	rls := ld.ResourceLogs()
	if ld.LogRecordCount() == 0 {
		return &logs.PushRequest{}, nil
	}

	b := builder{req: &logs.PushRequest{}, byLabels: make(map[string]int, rls.Len())}
	for i := range rls.Len() {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("otlpcodec: %w", err)
		}
		if err := d.resourceLogs(ctx, &b, rls.At(i)); err != nil {
			return nil, err
		}
	}
	return b.req, nil
}

// builder groups records by their sorted label set while preserving the
// first-seen stream order. Loki returns streams in map order (otlp.go:453),
// which is not observable in query results.
type builder struct {
	req      *logs.PushRequest
	byLabels map[string]int
}

func (b *builder) stream(labels logs.Labels) *logs.Stream {
	key := labels.String()
	if i, ok := b.byLabels[key]; ok {
		return &b.req.Streams[i]
	}
	b.byLabels[key] = len(b.req.Streams)
	b.req.Streams = append(b.req.Streams, logs.Stream{Labels: labels})
	return &b.req.Streams[len(b.req.Streams)-1]
}

// resourceLogs maps one OTLP ResourceLogs item (otlp.go:161-444).
func (d Decoder) resourceLogs(ctx context.Context, b *builder, rl plog.ResourceLogs) error {
	resAttrs := rl.Resource().Attributes()

	var streamLabels []logs.Label
	var resourceMeta []logs.Label
	hasServiceName := false
	if v, ok := resAttrs.Get(attrServiceName); ok && v.AsString() != "" {
		hasServiceName = true
	}

	var rangeErr error
	resAttrs.Range(func(k string, v pcommon.Value) bool {
		pairs, err := attributeLabels(k, v, "")
		if err != nil {
			rangeErr = fmt.Errorf("otlpcodec: resource attribute %q: %w", k, err)
			return false
		}
		if _, ok := defaultIndexAttributes[k]; ok {
			streamLabels = append(streamLabels, pairs...)
			if !hasServiceName {
				for _, l := range pairs {
					if slices.Contains(defaultDiscoverAttributes, l.Name) {
						streamLabels = append(streamLabels, logs.Label{Name: nameService, Value: l.Value})
						hasServiceName = true
						break
					}
				}
			}
			return true
		}
		resourceMeta = append(resourceMeta, pairs...)
		return true
	})
	if rangeErr != nil {
		return rangeErr
	}
	if !hasServiceName {
		streamLabels = append(streamLabels, logs.Label{Name: nameService, Value: serviceUnknown})
	}

	// Sorting happens once per resource; every entry in the stream shares
	// one immutable labels slice, as logs.Labels expects.
	labels := logs.New(streamLabels...)
	stream := b.stream(labels)

	sls := rl.ScopeLogs()
	for j := range sls.Len() {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("otlpcodec: %w", err)
		}
		scopeMeta, err := scopeMetadata(sls.At(j).Scope())
		if err != nil {
			return err
		}

		lrs := sls.At(j).LogRecords()
		for k := range lrs.Len() {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("otlpcodec: %w", err)
			}
			rec := lrs.At(k)
			logMeta, err := recordMetadata(rec)
			if err != nil {
				return err
			}

			// Loki appends log metadata first, then resource metadata, then
			// scope metadata (otlp.go:406-407); duplicate names keep the last
			// value in Loki's sorted order (see metadataLabels).
			meta := make([]logs.Label, 0, len(logMeta)+len(resourceMeta)+len(scopeMeta))
			meta = append(meta, logMeta...)
			meta = append(meta, resourceMeta...)
			meta = append(meta, scopeMeta...)

			stream.Entries = append(stream.Entries, logs.Entry{
				Timestamp: timestamp(rec, d.nowTime()),
				Line:      rec.Body().AsString(),
				Metadata:  metadataLabels(meta),
			})
		}
	}
	return nil
}

// scopeMetadata maps instrumentation scope attributes and fields
// (otlp.go:298-339). Scope attributes are always structured metadata under
// the pinned defaults.
func scopeMetadata(scope pcommon.InstrumentationScope) ([]logs.Label, error) {
	attrs := scope.Attributes()
	out := make([]logs.Label, 0, attrs.Len()+3)
	var rangeErr error
	attrs.Range(func(k string, v pcommon.Value) bool {
		pairs, err := attributeLabels(k, v, "")
		if err != nil {
			rangeErr = fmt.Errorf("otlpcodec: scope attribute %q: %w", k, err)
			return false
		}
		out = append(out, pairs...)
		return true
	})
	if rangeErr != nil {
		return nil, rangeErr
	}
	if name := scope.Name(); name != "" {
		out = append(out, logs.Label{Name: nameScopeName, Value: name})
	}
	if version := scope.Version(); version != "" {
		out = append(out, logs.Label{Name: nameScopeVersion, Value: version})
	}
	if n := scope.DroppedAttributesCount(); n != 0 {
		out = append(out, logs.Label{Name: nameScopeDroppedAttrsCount, Value: strconv.FormatUint(uint64(n), 10)})
	}
	return out, nil
}

// recordMetadata maps log-record attributes and fields (otlp.go:482-580).
// Log attributes are always structured metadata under the pinned defaults;
// severity_text_as_label is off and no log attribute is an index label.
func recordMetadata(rec plog.LogRecord) ([]logs.Label, error) {
	attrs := rec.Attributes()
	out := make([]logs.Label, 0, attrs.Len()+7)
	var rangeErr error
	attrs.Range(func(k string, v pcommon.Value) bool {
		pairs, err := attributeLabels(k, v, "")
		if err != nil {
			rangeErr = fmt.Errorf("otlpcodec: log attribute %q: %w", k, err)
			return false
		}
		out = append(out, pairs...)
		return true
	})
	if rangeErr != nil {
		return nil, rangeErr
	}

	// otlp.go:519-525: only when both are present; the fallback case already
	// used observed as the entry timestamp.
	if rec.Timestamp() != 0 && rec.ObservedTimestamp() != 0 {
		out = append(out, logs.Label{Name: nameObservedTimestamp, Value: timestampString(rec.ObservedTimestamp())})
	}
	if sev := rec.SeverityNumber(); sev != plog.SeverityNumberUnspecified {
		out = append(out, logs.Label{Name: nameSeverityNumber, Value: strconv.FormatInt(int64(sev), 10)})
	}
	if text := rec.SeverityText(); text != "" {
		out = append(out, logs.Label{Name: nameSeverityText, Value: text})
	}
	if n := rec.DroppedAttributesCount(); n != 0 {
		out = append(out, logs.Label{Name: nameDroppedAttributesCount, Value: strconv.FormatUint(uint64(n), 10)})
	}
	if flags := rec.Flags(); flags != 0 {
		out = append(out, logs.Label{Name: nameFlags, Value: strconv.FormatUint(uint64(flags), 10)})
	}
	if traceID := rec.TraceID(); !traceID.IsEmpty() {
		out = append(out, logs.Label{Name: nameTraceID, Value: hex.EncodeToString(traceID[:])})
	}
	if spanID := rec.SpanID(); !spanID.IsEmpty() {
		out = append(out, logs.Label{Name: nameSpanID, Value: hex.EncodeToString(spanID[:])})
	}
	return out, nil
}

// attributeLabels maps one OTLP attribute to labels, recursing into maps with
// an underscore-joined prefix (otlp.go:602-640). Scalar values use
// pcommon.Value.AsString, exactly like Loki.
func attributeLabels(key string, v pcommon.Value, prefix string) ([]logs.Label, error) {
	name := key
	if prefix != "" {
		name = prefix + "_" + key
	}
	name, err := labelName(name)
	if err != nil {
		return nil, err
	}

	if v.Type() != pcommon.ValueTypeMap {
		return []logs.Label{{Name: name, Value: v.AsString()}}, nil
	}

	m := v.Map()
	out := make([]logs.Label, 0, m.Len())
	var rangeErr error
	m.Range(func(k string, child pcommon.Value) bool {
		pairs, err := attributeLabels(k, child, name)
		if err != nil {
			rangeErr = fmt.Errorf("attribute %q: %w", k, err)
			return false
		}
		out = append(out, pairs...)
		return true
	})
	if rangeErr != nil {
		return nil, rangeErr
	}
	return out, nil
}

// metadataLabels resolves duplicate metadata names the way Loki's query path
// exposes them. Loki stores the converter's slice after
// logproto.FromLabelAdaptersToLabels sorts it (pkg/logproto/compat.go:81,
// called from pkg/chunkenc/memchunk.go:880). That sort is slices.SortFunc,
// which is not stable, so a duplicate-name run keeps the permutation produced
// by pdqsort. The query pipeline then passes the stored order through
// LabelsBuilder.Add (pkg/logql/log/pipeline.go:104,224); its Set
// (pkg/logql/log/labels.go:344-410) overwrites an existing name, so the last
// value in that order wins.
//
// Reproducing the unstable sort is deliberate. In the live v3.7.9
// fixed-field fixture it exposes trace_id="attr-trace" while every other
// fixed-field collision keeps Loki's fixed value; that is the permutation
// slices.SortFunc produces for the fixture's 17-label metadata set, and a
// stable sort would pick the fixed trace id instead.
func metadataLabels(meta []logs.Label) logs.Labels {
	slices.SortFunc(meta, func(a, b logs.Label) int { return cmp.Compare(a.Name, b.Name) })
	out := meta[:0]
	for i, l := range meta {
		if i+1 < len(meta) && meta[i+1].Name == l.Name {
			continue
		}
		out = append(out, l)
	}
	return out
}

// timestamp implements otlp.go:642-652: timestamp, then observed timestamp,
// then the clock. OTLP nanoseconds are uint64; the int64 conversion wraps for
// values past 2262 exactly as Loki does.
func timestamp(rec plog.LogRecord, now time.Time) time.Time {
	if ts := rec.Timestamp(); ts != 0 {
		return time.Unix(0, int64(ts)).UTC()
	}
	if ts := rec.ObservedTimestamp(); ts != 0 {
		return time.Unix(0, int64(ts)).UTC()
	}
	return now.UTC()
}

func timestampString(ts pcommon.Timestamp) string {
	return strconv.FormatInt(int64(ts), 10)
}

func (d Decoder) nowTime() time.Time {
	if d.now != nil {
		return d.now()
	}
	return time.Now()
}
