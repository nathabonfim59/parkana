package wire_test

import (
	"encoding/json/v2"
	"reflect"
	"testing"

	"github.com/nathabonfim59/parkana/internal/tail/wire"
)

func TestMessageRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   wire.Message
	}{
		{"ready", wire.Message{Type: wire.TypeReady, Tenant: "default", BufferEntries: 1024}},
		{"batch", wire.Message{Type: wire.TypeBatch, Entries: []wire.Entry{{
			Timestamp: "1700000000123456000",
			Line:      "hello",
			Labels:    []wire.Label{{Name: "job", Value: "api"}},
			Metadata:  []wire.Label{{Name: "trace_id", Value: "abc"}},
		}}}},
		{"dropped", wire.Message{Type: wire.TypeDropped, Dropped: 3, Reason: "slow subscriber"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			var got wire.Message
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.in) {
				t.Fatalf("round trip = %+v, want %+v", got, tc.in)
			}
		})
	}
}

func TestUnsetFieldsAreOmitted(t *testing.T) {
	b, err := json.Marshal(wire.Message{Type: wire.TypeReady})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"type":"ready"}`
	if string(b) != want {
		t.Fatalf("Marshal = %s, want %s", b, want)
	}
}
