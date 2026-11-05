package pushcodec_test

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/golang/snappy"

	"github.com/nathabonfim59/parkana/internal/ingest/pushcodec"
	"github.com/nathabonfim59/parkana/internal/logs"
)

// protoFixture is a canonical logproto.PushRequest written byte-for-byte
// from pkg/push/push.proto (provenance: grafana/loki push.proto):
// PushRequest.streams=1, StreamAdapter.labels=1/entries=2/hash=3,
// EntryAdapter.timestamp=1/line=2/structuredMetadata=3, Timestamp
// seconds=1/nanos=2, LabelPairAdapter name=1/value=2. It deliberately
// carries fields the decoder must skip: StreamAdapter.hash and an unknown
// top-level field 99.
var protoFixture = []byte{
	0x0a, 0x5b, // PushRequest.streams (LEN), 91 bytes
	0x0a, 0x16, // StreamAdapter.labels (LEN), 22 bytes
	'{', 'z', 'o', 'n', 'e', '=', '"', 'u', 's', '"', ',', ' ',
	'j', 'o', 'b', '=', '"', 'a', 'p', 'i', '"', '}',
	0x18, 0x07, // StreamAdapter.hash (varint) = 7, ignored
	0x12, 0x23, // StreamAdapter.entries (LEN), 35 bytes
	0x0a, 0x0c, // EntryAdapter.timestamp (LEN), 12 bytes
	0x08, 0x80, 0xa0, 0xf8, 0xfa, 0x05, // Timestamp.seconds = 1600000000
	0x10, 0x81, 0xca, 0xb5, 0xee, 0x01, // Timestamp.nanos = 500000001
	0x12, 0x02, 'h', 'i', // EntryAdapter.line = "hi"
	0x1a, 0x0f, // EntryAdapter.structuredMetadata (LEN), 15 bytes
	0x0a, 0x08, 't', 'r', 'a', 'c', 'e', '_', 'i', 'd', // name = "trace_id"
	0x12, 0x03, 'a', 'b', 'c', // value = "abc"
	0x12, 0x1a, // StreamAdapter.entries (LEN), 26 bytes
	0x0a, 0x11, // EntryAdapter.timestamp (LEN), 17 bytes
	0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, // seconds = -1
	0x10, 0xff, 0x93, 0xeb, 0xdc, 0x03, // nanos = 999999999
	0x12, 0x05, 'w', 'o', 'r', 'l', 'd', // line = "world"
	0x98, 0x06, 0x01, // unknown field 99 (varint) = 1, skipped
}

// jsonFixture is the same push in Loki's JSON form.
var jsonFixture = []byte(`{"streams":[{"stream":{"job":"api","zone":"us"},` +
	`"values":[["1600000000500000001","hi",{"trace_id":"abc"}],["-1","world"]]}]}`)

func fixtureRequest() *logs.PushRequest {
	return &logs.PushRequest{Streams: []logs.Stream{{
		Labels: logs.New(logs.Label{Name: "zone", Value: "us"}, logs.Label{Name: "job", Value: "api"}),
		Entries: []logs.Entry{
			{
				Timestamp: time.Unix(1600000000, 500000001).UTC(),
				Line:      "hi",
				Metadata:  logs.New(logs.Label{Name: "trace_id", Value: "abc"}),
			},
			{Timestamp: time.Unix(-1, 999999999).UTC(), Line: "world"},
		},
	}}}
}

func TestDecodeProtoFixture(t *testing.T) {
	got, err := pushcodec.DecodeProto(t.Context(), protoFixture)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, fixtureRequest()) {
		t.Fatalf("DecodeProto = %+v, want %+v", got, fixtureRequest())
	}
}

func TestDecodeJSONFixtureEquivalence(t *testing.T) {
	got, err := pushcodec.DecodeJSON(t.Context(), jsonFixture)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, fixtureRequest()) {
		t.Fatalf("DecodeJSON = %+v, want %+v", got, fixtureRequest())
	}
}

func TestProtoRoundTrip(t *testing.T) {
	want := fixtureRequest()
	b, err := pushcodec.Marshal(t.Context(), want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pushcodec.DecodeProto(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestMarshalSnappyRoundTrip(t *testing.T) {
	want := fixtureRequest()
	b, err := pushcodec.MarshalSnappy(t.Context(), want)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := snappy.Decode(nil, b)
	if err != nil {
		t.Fatalf("MarshalSnappy output is not block snappy: %v", err)
	}
	got, err := pushcodec.DecodeProto(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestJSONRoundTrip(t *testing.T) {
	want := fixtureRequest()
	b, err := pushcodec.MarshalJSON(t.Context(), want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pushcodec.DecodeJSON(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestDecodeJSONValidExtras(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want *logs.PushRequest
	}{
		{"empty body object", `{}`, &logs.PushRequest{Streams: []logs.Stream{}}},
		{"empty streams", `{"streams":[]}`, &logs.PushRequest{Streams: []logs.Stream{}}},
		{"null stream labels", `{"streams":[{"stream":null,"values":[]}]}`, &logs.PushRequest{
			Streams: []logs.Stream{{Labels: logs.Labels{}, Entries: []logs.Entry{}}},
		}},
		{"missing stream labels", `{"streams":[{"values":[["1","x"]]}]}`, &logs.PushRequest{
			Streams: []logs.Stream{{
				Labels:  logs.Labels{},
				Entries: []logs.Entry{{Timestamp: time.Unix(0, 1).UTC(), Line: "x"}},
			}},
		}},
	}
	for _, c := range cases {
		got, err := pushcodec.DecodeJSON(t.Context(), []byte(c.in))
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
}

func TestDecodeJSONRFC3339(t *testing.T) {
	got, err := pushcodec.DecodeJSON(t.Context(),
		[]byte(`{"streams":[{"stream":{},"values":[["2020-09-13T12:26:40.123456789Z","x"]]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2020, 9, 13, 12, 26, 40, 123456789, time.UTC)
	if ts := got.Streams[0].Entries[0].Timestamp; !ts.Equal(want) {
		t.Fatalf("timestamp = %s, want %s", ts, want)
	}
}

func TestDecodeProtoInvalid(t *testing.T) {
	wire := func(num int, typ int, val ...byte) []byte {
		return append([]byte{byte(num<<3 | typ)}, val...)
	}
	stream := func(labelText string, entries ...byte) []byte {
		b := append([]byte{0x0a, byte(len(labelText))}, labelText...)
		return append(b, entries...)
	}
	push := func(streamBytes []byte) []byte {
		return append([]byte{0x0a, byte(len(streamBytes))}, streamBytes...)
	}
	cases := []struct {
		name string
		b    []byte
	}{
		{"truncated tag", []byte{0x0a}},
		{"truncated streams value", []byte{0x0a, 0x10, 0x0a}},
		{"streams wrong wire type", wire(1, 0, 0x01)},
		{"bad stream tag", push([]byte{0x0a})},
		{"labels wrong wire type", push(wire(1, 0, 0x01))},
		{"unparseable labels", push(stream(`{a!="b"}`))},
		{"missing labels", push([]byte{0x12, 0x02, 0x12, 0x00})},
		{"entry wrong wire type", push(append(stream(`{}`), wire(2, 0, 0x01)...))},
		{"entry missing timestamp", push(append(stream(`{}`), 0x12, 0x03, 0x12, 0x01, 'x'))},
		{"timestamp wrong wire type", push(append(stream(`{}`), 0x12, 0x03, 0x08, 0x01, 'x'))},
		// Timestamp.seconds = 253402300800, just past the protobuf range.
		{"timestamp seconds out of range", push(append(stream(`{}`), 0x12, 0x0c,
			0x0a, 0x07, 0x08, 0x80, 0x83, 0xd1, 0xff, 0xaf, 0x07, 0x12, 0x01, 'x'))},
		// Timestamp.nanos = 1000000000, out of range.
		{"nanos out of range", push(append(stream(`{}`), 0x12, 0x0a,
			0x0a, 0x06, 0x10, 0x80, 0x94, 0xeb, 0xdc, 0x03, 0x12, 0x00))},
		{"bad varint", []byte{0x0a, 0x02, 0x18, 0xff}},
		{"bad nested varint", push(append(stream(`{}`), 0x18, 0xff))},
	}
	for _, c := range cases {
		if got, err := pushcodec.DecodeProto(t.Context(), c.b); err == nil {
			t.Errorf("%s: DecodeProto = %+v, want error", c.name, got)
		}
	}
}

func TestDecodeJSONInvalid(t *testing.T) {
	cases := []string{
		`{`,
		`"x"`,
		`{"streams":[{"stream":{"a":"1","a":"2"},"values":[]}]}`,             // duplicate member
		`{"streams":[{"stream":{"bad-name":"x"},"values":[]}]}`,              // invalid label name
		`{"streams":[{"stream":{"job":null},"values":[]}]}`,                  // null label value
		`{"streams":[{"stream":{"job":5},"values":[]}]}`,                     // numeric label value
		`{"streams":[{"stream":{"job":{}},"values":[]}]}`,                    // object label value
		`{"streams":[{"stream":{},"values":[[]]}]}`,                          // tuple too short
		`{"streams":[{"stream":{},"values":[["1","x","y","z"]]}]}`,           // tuple too long
		`{"streams":[{"stream":{},"values":[["abc","x"]]}]}`,                 // bad timestamp
		`{"streams":[{"stream":{},"values":[[null,"x"]]}]}`,                  // null timestamp
		`{"streams":[{"stream":{},"values":[[5,"x"]]}]}`,                     // numeric timestamp
		`{"streams":[{"stream":{},"values":[["1",null]}]}`,                   // null line
		`{"streams":[{"stream":{},"values":[["1",5]]}]}`,                     // numeric line
		`{"streams":[{"stream":{},"values":[["1","x",null]}]}`,               // null metadata
		`{"streams":[{"stream":{},"values":[["1","x",{"k":null}]]}]}`,        // null metadata value
		`{"streams":[{"stream":{},"values":[["1","x",{"k":5}]]}]}`,           // numeric metadata value
		`{"streams":[{"stream":{},"values":[["1","x",{"k":{}}]]}]}`,          // object metadata value
		`{"streams":[{"stream":{},"values":[["1","x",{"a":"1","a":"2"}]]}]}`, // dup metadata
		`{"streams":[{"stream":{},"values":[["1","x",{"bad-name":"v"}]]}]}`,  // bad meta name
	}
	for _, in := range cases {
		if got, err := pushcodec.DecodeJSON(t.Context(), []byte(in)); err == nil {
			t.Errorf("DecodeJSON(%s) = %+v, want error", in, got)
		}
	}
}

// A metadata value that is an array is valid JSON but the wrong type; the
// fixture must be syntactically valid so the error comes from type checking.
func TestDecodeJSONMetadataArray(t *testing.T) {
	in := `{"streams":[{"stream":{},"values":[["1","x",[1]]]}]}`
	if !jsontext.Value(in).IsValid() {
		t.Fatalf("fixture is not valid JSON: %s", in)
	}
	if got, err := pushcodec.DecodeJSON(t.Context(), []byte(in)); err == nil {
		t.Errorf("DecodeJSON(%s) = %+v, want error", in, got)
	}
}

// TestDecodeMalformedCorpus feeds arbitrary corruptions of the fixture; every
// input must return without a panic (an error is expected, a decode is also
// acceptable where the corruption happens to be valid wire data).
func TestDecodeMalformedCorpus(t *testing.T) {
	for i := range protoFixture {
		mut := bytes.Clone(protoFixture)
		mut[i] ^= 0xff
		_, _ = pushcodec.DecodeProto(t.Context(), mut)
	}
	for i := 0; i < len(protoFixture); i++ {
		_, _ = pushcodec.DecodeProto(t.Context(), protoFixture[:i])
	}
}

func TestMarshalRejectsOutOfRange(t *testing.T) {
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels: logs.New(),
		Entries: []logs.Entry{
			{Timestamp: time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), Line: "x"},
		},
	}}}
	if _, err := pushcodec.Marshal(t.Context(), req); err == nil {
		t.Error("Marshal accepted a timestamp outside the protobuf range")
	}
}

func TestMarshalJSONRejectsOutOfRange(t *testing.T) {
	req := &logs.PushRequest{Streams: []logs.Stream{{
		Labels: logs.New(),
		Entries: []logs.Entry{
			{Timestamp: time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC), Line: "x"},
		},
	}}}
	if _, err := pushcodec.MarshalJSON(t.Context(), req); err == nil {
		t.Error("MarshalJSON accepted a timestamp outside int64 nanoseconds")
	}
}

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	req := fixtureRequest()
	for name, fn := range map[string]func() error{
		"Marshal":       func() error { _, err := pushcodec.Marshal(ctx, req); return err },
		"MarshalSnappy": func() error { _, err := pushcodec.MarshalSnappy(ctx, req); return err },
		"MarshalJSON":   func() error { _, err := pushcodec.MarshalJSON(ctx, req); return err },
		"DecodeProto":   func() error { _, err := pushcodec.DecodeProto(ctx, protoFixture); return err },
		"DecodeJSON":    func() error { _, err := pushcodec.DecodeJSON(ctx, jsonFixture); return err },
	} {
		if err := fn(); err == nil {
			t.Errorf("%s succeeded with a canceled context", name)
		}
	}
}

// failAfterCtx is a context whose Err starts failing after n calls, letting a
// test cancel deterministically mid-stream.
type failAfterCtx struct {
	context.Context
	calls, failAt int
}

func (c *failAfterCtx) Err() error {
	c.calls++
	if c.calls > c.failAt {
		return context.Canceled
	}
	return c.Context.Err()
}

func TestContextCancellationEmpty(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	empty := &logs.PushRequest{}
	if _, err := pushcodec.Marshal(ctx, empty); err == nil {
		t.Error("Marshal accepted a canceled context with zero streams")
	}
	if _, err := pushcodec.MarshalJSON(ctx, empty); err == nil {
		t.Error("MarshalJSON accepted a canceled context with zero streams")
	}
	if _, err := pushcodec.DecodeProto(ctx, nil); err == nil {
		t.Error("DecodeProto accepted a canceled context on empty input")
	}
	if _, err := pushcodec.DecodeJSON(ctx, []byte(`{}`)); err == nil {
		t.Error("DecodeJSON accepted a canceled context on empty input")
	}
}

func TestContextCancellationMidStream(t *testing.T) {
	// failAt is tuned per function so at least one entry is processed
	// before cancellation is observed.
	cases := []struct {
		name   string
		failAt int
		fn     func(context.Context) error
	}{
		{"DecodeProto", 3, func(ctx context.Context) error {
			_, err := pushcodec.DecodeProto(ctx, protoFixture)
			return err
		}},
		{"DecodeJSON", 3, func(ctx context.Context) error {
			_, err := pushcodec.DecodeJSON(ctx, jsonFixture)
			return err
		}},
		{"Marshal", 2, func(ctx context.Context) error {
			_, err := pushcodec.Marshal(ctx, fixtureRequest())
			return err
		}},
		{"MarshalJSON", 2, func(ctx context.Context) error {
			_, err := pushcodec.MarshalJSON(ctx, fixtureRequest())
			return err
		}},
	}
	for _, c := range cases {
		ctx := &failAfterCtx{Context: t.Context(), failAt: c.failAt}
		if err := c.fn(ctx); !errors.Is(err, context.Canceled) {
			t.Errorf("%s: err = %v, want context.Canceled", c.name, err)
		}
	}
}
