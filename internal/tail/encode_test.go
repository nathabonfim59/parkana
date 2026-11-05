package tail

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

func TestBuildResponseFlatMergesCategories(t *testing.T) {
	s := &session{categorized: false}
	base := logs.New(logs.Label{Name: "job", Value: "api"})
	meta := logs.New(logs.Label{Name: "trace_id", Value: "abc"})
	parsed := logs.New(logs.Label{Name: "level", Value: "error"})
	entry := queuedEntry{
		ts:     time.Unix(1, 123456000).UTC(),
		labels: s.responseLabels(base, meta, parsed),
		entry: plan.Entry{
			TS:     time.Unix(1, 123456000).UTC(),
			Line:   `{"level":"error"}`,
			Meta:   meta,
			Parsed: parsed,
		},
	}
	b, err := json.Marshal(buildResponse([]queuedEntry{entry}, nil, false))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got decodedTail
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	if len(got.Streams) != 1 {
		t.Fatalf("streams = %+v", got.Streams)
	}
	stream := got.Streams[0].Stream
	if stream["job"] != "api" || stream["trace_id"] != "abc" || stream["level"] != "error" {
		t.Fatalf("flat stream labels = %v, want merged categories", stream)
	}
	if len(got.Streams[0].Values) != 1 {
		t.Fatalf("values = %+v", got.Streams[0].Values)
	}
	var parts []jsontext.Value
	if err := json.Unmarshal(got.Streams[0].Values[0], &parts); err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 {
		t.Fatalf("flat tuple has %d elements, want 2: %s", len(parts), got.Streams[0].Values[0])
	}
	if !strings.Contains(string(parts[0]), "1123456000") {
		t.Fatalf("timestamp tuple = %s", parts[0])
	}
}

func TestBuildResponseCategorizedKeepsCategories(t *testing.T) {
	s := &session{categorized: true}
	base := logs.New(
		logs.Label{Name: "job", Value: "api"},
		logs.Label{Name: "trace_id", Value: "stale"},
	)
	meta := logs.New(logs.Label{Name: "trace_id", Value: "abc"})
	parsed := logs.New(logs.Label{Name: "level", Value: "error"})
	entry := queuedEntry{
		ts:     time.Unix(2, 0).UTC(),
		labels: s.responseLabels(base, meta, parsed),
		entry: plan.Entry{
			TS:     time.Unix(2, 0).UTC(),
			Line:   `{"level":"error"}`,
			Meta:   meta,
			Parsed: parsed,
		},
	}
	b, err := json.Marshal(buildResponse([]queuedEntry{entry}, nil, true))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got decodedTail
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	decoded := got.entries(t)
	if len(decoded) != 1 {
		t.Fatalf("entries = %+v", decoded)
	}
	if _, ok := decoded[0].labels["trace_id"]; ok {
		t.Fatalf("categorized stream labels kept a category: %v", decoded[0].labels)
	}
	if decoded[0].metadata["trace_id"] != "abc" || decoded[0].parsed["level"] != "error" {
		t.Fatalf("categories = %v / %v", decoded[0].metadata, decoded[0].parsed)
	}
}

func TestBuildResponseEmptyLabelsUseObject(t *testing.T) {
	e := queuedEntry{
		ts:     time.Unix(4, 0).UTC(),
		labels: nil,
		entry:  plan.Entry{TS: time.Unix(4, 0).UTC(), Line: "no labels"},
	}
	b, err := json.Marshal(buildResponse([]queuedEntry{e}, nil, false))
	if err != nil {
		t.Fatal(err)
	}
	var got decodedTail
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Streams[0].Stream == nil {
		t.Fatalf("empty stream labels serialized as null: %s", b)
	}
	if len(got.Streams[0].Stream) != 0 {
		t.Fatalf("stream labels = %v, want {}", got.Streams[0].Stream)
	}
	if !strings.Contains(string(b), `"stream":{}`) {
		t.Fatalf("wire = %s, want stream:{}", b)
	}
}

func TestBuildFramesBoundsEncodedSizeAndSplits(t *testing.T) {
	base := time.Unix(5, 0).UTC()
	// JSON escaping doubles each quote, and both line and labels contribute
	// to the encoded size. 50 x ~120 KiB frames are well over one frame.
	line := strings.Repeat(`"`, 60_000)
	labels := []logs.Label{{Name: "job", Value: strings.Repeat("v", 20_000)}}
	entries := make([]queuedEntry, 50)
	for i := range entries {
		entries[i] = queuedEntry{
			ts:     base.Add(time.Duration(i) * time.Millisecond),
			labels: logs.New(labels...),
			entry:  plan.Entry{TS: base.Add(time.Duration(i) * time.Millisecond), Line: line},
		}
	}
	frames, err := buildFrames(entries, nil, false)
	if err != nil {
		t.Fatalf("buildFrames: %v", err)
	}
	if len(frames) < 2 {
		t.Fatalf("frames = %d, want the batch split", len(frames))
	}
	total := 0
	for i, frame := range frames {
		if len(frame) > maxFrameBytes {
			t.Fatalf("frame %d is %d bytes, max %d", i, len(frame), maxFrameBytes)
		}
		var m decodedTail
		if err := json.Unmarshal(frame, &m); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		total += len(m.entries(t))
	}
	if total != len(entries) {
		t.Fatalf("entries across frames = %d, want %d", total, len(entries))
	}
}

func TestBuildFramesRejectsSingleOversizedEntry(t *testing.T) {
	e := queuedEntry{
		ts:     time.Unix(6, 0).UTC(),
		labels: logs.New(logs.Label{Name: "job", Value: "api"}),
		entry:  plan.Entry{TS: time.Unix(6, 0).UTC(), Line: strings.Repeat(`"`, maxFrameBytes)},
	}
	if _, err := buildFrames([]queuedEntry{e}, nil, false); !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("error = %v, want errFrameTooLarge", err)
	}
}

func TestBuildFramesBoundsDropDescriptors(t *testing.T) {
	base := time.Unix(7, 0).UTC()
	long := strings.Repeat("x", 100_000)
	drops := make([]droppedEntry, 50)
	for i := range drops {
		drops[i] = droppedEntry{
			ts:     base.Add(time.Duration(i) * time.Millisecond),
			labels: logs.New(logs.Label{Name: "pod", Value: long}),
		}
	}
	frames, err := buildFrames(nil, drops, false)
	if err != nil {
		t.Fatalf("buildFrames: %v", err)
	}
	if len(frames) < 2 {
		t.Fatalf("frames = %d, want drop descriptors split", len(frames))
	}
	total := 0
	for i, frame := range frames {
		if len(frame) > maxFrameBytes {
			t.Fatalf("frame %d is %d bytes, max %d", i, len(frame), maxFrameBytes)
		}
		var m decodedTail
		if err := json.Unmarshal(frame, &m); err != nil {
			t.Fatal(err)
		}
		total += len(m.Dropped)
	}
	if total != len(drops) {
		t.Fatalf("descriptors across frames = %d, want %d", total, len(drops))
	}
}

func TestBuildResponseDroppedEntries(t *testing.T) {
	drops := []droppedEntry{{
		ts:     time.Unix(3, 5000).UTC(),
		labels: logs.New(logs.Label{Name: "job", Value: "api"}),
	}}
	b, err := json.Marshal(buildResponse(nil, drops, false))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got decodedTail
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	if len(got.Dropped) != 1 || got.Dropped[0].Timestamp != "3000005000" {
		t.Fatalf("dropped = %+v", got.Dropped)
	}
	if got.Dropped[0].Labels["job"] != "api" {
		t.Fatalf("dropped labels = %+v", got.Dropped[0].Labels)
	}
	if !strings.Contains(string(b), `"streams":[]`) {
		t.Fatalf("response must always carry streams: %s", b)
	}
}
