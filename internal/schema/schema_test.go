package schema

import (
	"testing"
	"time"
)

func TestForStream(t *testing.T) {
	s, err := ForStream([]string{"level", "namespace"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{ColTS, ColLine, ColLabels, ColMeta, "level", "namespace"}
	if s.NumFields() != len(want) {
		t.Fatalf("got %d fields, want %d", s.NumFields(), len(want))
	}
	for i, name := range want {
		if got := s.Field(i).Name; got != name {
			t.Errorf("field %d = %q, want %q", i, got, name)
		}
	}
	for _, bad := range [][]string{{"line"}, {"LINE"}, {"Labels"}, {"META"}, {"_x"}, {""}, {"a", "a"}, {"level", "Level"}} {
		if _, err := ForStream(bad); err == nil {
			t.Errorf("ForStream(%q) accepted", bad)
		}
	}
}

func TestTSRoundTrip(t *testing.T) {
	now := time.Date(2026, 11, 3, 20, 1, 2, 123456789, time.UTC)
	if got := Time(TS(now)); !got.Equal(now.Truncate(time.Microsecond)) {
		t.Fatalf("round trip = %v", got)
	}
}
