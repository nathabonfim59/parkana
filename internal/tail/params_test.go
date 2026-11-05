package tail

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseTailRequestDefaults(t *testing.T) {
	r := httptest.NewRequest("GET", `/loki/api/v1/tail?query=%7Bjob%3D%22api%22%7D`, nil)
	req, err := parseTailRequest(r)
	if err != nil {
		t.Fatalf("parseTailRequest: %v", err)
	}
	if req.query != `{job="api"}` {
		t.Fatalf("query = %q", req.query)
	}
	if req.limit != defaultTailLimit {
		t.Fatalf("limit = %d, want %d", req.limit, defaultTailLimit)
	}
	if req.delayFor != 0 {
		t.Fatalf("delayFor = %v, want 0", req.delayFor)
	}
	if !req.start.IsZero() {
		t.Fatalf("start = %v, want no lower bound", req.start)
	}
}

func TestParseTailRequestValues(t *testing.T) {
	r := httptest.NewRequest("GET", `/loki/api/v1/tail?query=x&limit=7&delay_for=5&start=1700000000123456789`, nil)
	req, err := parseTailRequest(r)
	if err != nil {
		t.Fatalf("parseTailRequest: %v", err)
	}
	if req.limit != 7 || req.delayFor != 5*time.Second {
		t.Fatalf("limit/delay = %d/%v", req.limit, req.delayFor)
	}
	if want := time.Unix(0, 1700000000123456789).UTC(); !req.start.Equal(want) {
		t.Fatalf("start = %v, want %v", req.start, want)
	}
}

func TestParseTailRequestRejects(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		want  string
	}{
		{"missing query", ``, "missing query"},
		{"blank query", `query=+`, "missing query"},
		{"duplicate query", `query=a&query=b`, "at most once"},
		{"duplicate limit", `query=a&limit=1&limit=2`, "at most once"},
		{"duplicate delay", `query=a&delay_for=1&delay_for=2`, "at most once"},
		{"duplicate start", `query=a&start=1&start=2`, "at most once"},
		{"limit zero", `query=a&limit=0`, "limit must be a positive value"},
		{"limit negative", `query=a&limit=-1`, "limit must be a positive value"},
		{"limit malformed", `query=a&limit=x`, "invalid limit"},
		{"limit overflow", `query=a&limit=999999999999999999999`, "invalid limit"},
		{"delay overflow", `query=a&delay_for=999999999999999999999`, "invalid delay_for"},
		{"delay negative", `query=a&delay_for=-1`, "delay_for must be a positive value"},
		{"delay too large", `query=a&delay_for=6`, "delay_for can't be greater than 5"},
		{"delay malformed", `query=a&delay_for=x`, "invalid delay_for"},
		{"start malformed", `query=a&start=nope`, "invalid start"},
		{"start overflow", `query=a&start=999999999999999999999999`, "invalid start"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/loki/api/v1/tail?"+tc.query, nil)
			_, err := parseTailRequest(r)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want %q", err, tc.want)
			}
		})
	}
}

func TestParseTailTimestampForms(t *testing.T) {
	rfc := time.Date(2026, 11, 4, 5, 6, 7, 890123456, time.UTC)
	for _, tc := range []struct {
		raw  string
		want time.Time
	}{
		{"1700000000", time.Unix(1700000000, 0).UTC()},
		{"-5", time.Unix(-5, 0).UTC()},
		{"1700000000123456789", time.Unix(0, 1700000000123456789).UTC()},
		{"1700000000.123456789", time.Unix(0, 1700000000123000000).UTC()},
		{"1700000000.1239", time.Unix(0, 1700000000124000000).UTC()},
		{"1700000000.1234", time.Unix(0, 1700000000123000000).UTC()},
		{"2026-11-04T05:06:07.890123456Z", rfc},
	} {
		got, err := parseTailTimestamp(tc.raw)
		if err != nil {
			t.Fatalf("parseTailTimestamp(%q): %v", tc.raw, err)
		}
		if !got.Equal(tc.want) {
			t.Fatalf("parseTailTimestamp(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
	for _, raw := range []string{
		"", "abc", "NaN", "Inf", "1e999",
		"2026-13-40T99:99:99Z",
		// 2^63 seconds is exactly representable as float64 and would wrap
		// to MinInt64 under a naive > MaxInt64 check.
		"9.223372036854776e18",
		"-9.223372036854776e18",
		// 2^62 is the explicit range boundary and is rejected too.
		"4.611686018427388e18",
		"-4.611686018427388e18",
	} {
		if _, err := parseTailTimestamp(raw); err == nil {
			t.Fatalf("parseTailTimestamp(%q) accepted invalid input", raw)
		}
	}
}

func TestParseTailTimestampSignedBoundary(t *testing.T) {
	// Just inside the float range must cast without wrapping and stay on the
	// intended side of the epoch.
	pos, err := parseTailTimestamp("4.611686018427387e18")
	if err != nil {
		t.Fatalf("positive boundary: %v", err)
	}
	if pos.Unix() <= 0 {
		t.Fatalf("positive boundary wrapped to %v", pos)
	}
	neg, err := parseTailTimestamp("-4.611686018427387e18")
	if err != nil {
		t.Fatalf("negative boundary: %v", err)
	}
	if neg.Unix() >= 0 {
		t.Fatalf("negative boundary wrapped to %v", neg)
	}
	// Nanosecond integers use the full int64 range through time.Unix's
	// normalization and must not wrap around the epoch.
	minNS, err := parseTailTimestamp("-9223372036854775808")
	if err != nil {
		t.Fatalf("minimum nanosecond integer: %v", err)
	}
	if minNS.Unix() >= 0 {
		t.Fatalf("minimum nanosecond value wrapped to %v", minNS)
	}
	maxNS, err := parseTailTimestamp("9223372036854775807")
	if err != nil {
		t.Fatalf("maximum nanosecond integer: %v", err)
	}
	if maxNS.Unix() <= 0 {
		t.Fatalf("maximum nanosecond value wrapped to %v", maxNS)
	}
}

func TestParseWireTimestampUsesStorageGrid(t *testing.T) {
	// 1.9µs must land on the stored 1µs point, matching UnixMicro.
	got, err := parseWireTimestamp("1900")
	if err != nil {
		t.Fatalf("parseWireTimestamp: %v", err)
	}
	if want := time.UnixMicro(1).UTC(); !got.Equal(want) {
		t.Fatalf("timestamp = %v, want %v", got, want)
	}
	if _, err := parseWireTimestamp("999999999999999999999"); err == nil {
		t.Fatal("expected overflow error")
	}
}
