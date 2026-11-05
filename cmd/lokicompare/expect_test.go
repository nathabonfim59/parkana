package main

import (
	"math/big"
	"strings"
	"testing"
)

func TestValidDecimalNumber(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{input: "0", want: true},
		{input: "1200", want: true},
		{input: "-1", want: true},
		{input: "-0", want: true},
		{input: "1200.0", want: true},
		{input: "-1.5", want: true},
		{input: "1e3", want: true},
		{input: "1.5E-3", want: true},
		{input: "1e+3", want: true},
		{input: "", want: false},
		{input: "1/1", want: false},
		{input: "0x1", want: false},
		{input: "1_000", want: false},
		{input: "NaN", want: false},
		{input: "Inf", want: false},
		{input: "-Inf", want: false},
		{input: "+1", want: false},
		{input: " 1", want: false},
		{input: "1 ", want: false},
		{input: "01", want: false},
		{input: "1.", want: false},
		{input: ".5", want: false},
		{input: "1e", want: false},
		{input: "1e+", want: false},
		{input: "-", want: false},
		{input: "--1", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			if got := validDecimalNumber(tc.input); got != tc.want {
				t.Errorf("validDecimalNumber(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestParseExpectationInvalidShapes(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{"null", `null`, "must be an object"},
		{"array", `[]`, "must be an object"},
		{"stats array", `{"stats":[]}`, "stats must be an object"},
		{"stats null field", `{"stats":{"streams":null}}`, "number or decimal string"},
		{"stats boolean field", `{"stats":{"streams":true}}`, "number or decimal string"},
		{"series object", `{"series":{}}`, "series must be an array"},
		{"series item array", `{"series":[[]]}`, "must be an object"},
		{"metric null", `{"series":[{"metric":null,"value":1}]}`, "metric must be an object"},
		{"value null", `{"series":[{"metric":{},"value":null}]}`, "number or decimal string"},
		{"values object", `{"series":[{"metric":{},"values":{}}]}`, "must be an array"},
		{"short tuple", `{"series":[{"metric":{},"values":[[]]}]}`, "[numeric seconds, value]"},
		{"null sample value", `{"series":[{"metric":{},"values":[[1700000000,null]]}]}`, "number or decimal string"},
		{"both value forms", `{"series":[{"metric":{},"values":[[1700000000,1]],"value":1}]}`, "both value and values"},
		{"empty points", `{"series":[{"metric":{},"values":[]}]}`, "must not be empty"},
		{"status null", `{"status":null}`, "must not be null"},
		{"status string", `{"status":"400"}`, "JSON integer"},
		{"status bool", `{"status":false}`, "JSON integer"},
		{"status object", `{"status":{}}`, "JSON integer"},
		{"status array", `{"status":[400]}`, "JSON integer"},
		{"status fraction", `{"status":400.5}`, "integer"},
		{"status below range", `{"status":399}`, "[400, 599]"},
		{"status above range", `{"status":600}`, "[400, 599]"},
		{"status with stats", `{"status":400,"stats":{"streams":1}}`, "must not be combined"},
		{"status with empty series", `{"status":400,"series":[]}`, "must not be combined"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseExpectation([]byte(tc.input))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("parseExpectation(%s) error = %v, want containing %q", tc.input, err, tc.wantErr)
			}
		})
	}
}

func TestParseExactNumberRejectsNonDecimalSpellings(t *testing.T) {
	for _, input := range []string{`"1/1"`, `"0x1"`, `"1_000"`, `"NaN"`, `"+1"`, `" 1"`} {
		if _, err := parseExactNumber([]byte(input)); err == nil {
			t.Errorf("parseExactNumber(%s) succeeded, want a decimal grammar error", input)
		}
	}
	for _, input := range []string{`1200`, `"1200"`, `"1200.0"`, `"1.5e3"`} {
		if _, err := parseExactNumber([]byte(input)); err != nil {
			t.Errorf("parseExactNumber(%s): %v", input, err)
		}
	}
}

func TestValidateExpectationBranches(t *testing.T) {
	tests := []struct {
		name    string
		kind    queryType
		exp     *expectation
		wantErr string
	}{
		{name: "nil", kind: queryStats, exp: nil},
		{name: "stats requires stats", kind: queryStats, exp: &expectation{}, wantErr: "requires stats"},
		{name: "stats rejects series", kind: queryStats, exp: &expectation{stats: &statsExpectation{}, seriesSet: true}, wantErr: "must not define series"},
		{name: "volume requires series", kind: queryVolume, exp: &expectation{}, wantErr: "requires series"},
		{name: "volume rejects stats", kind: queryVolume, exp: &expectation{stats: &statsExpectation{}}, wantErr: "must not define stats"},
		{
			name: "volume rejects range values",
			kind: queryVolume,
			exp: &expectation{seriesSet: true, series: []seriesExpectation{{
				metric: map[string]string{}, points: []expectedPoint{{ts: 1, value: big.NewRat(1, 1)}},
			}}},
			wantErr: "expects value",
		},
		{name: "volume_range requires series", kind: queryVolumeRange, exp: &expectation{}, wantErr: "requires series"},
		{name: "volume_range rejects stats", kind: queryVolumeRange, exp: &expectation{stats: &statsExpectation{}}, wantErr: "must not define stats"},
		{
			name:    "volume_range rejects instant value",
			kind:    queryVolumeRange,
			exp:     &expectation{seriesSet: true, series: []seriesExpectation{{metric: map[string]string{}, value: big.NewRat(1, 1)}}},
			wantErr: "expects values",
		},
		{name: "incompatible endpoint", kind: queryRange, exp: &expectation{stats: &statsExpectation{}}, wantErr: "only valid"},
		{name: "valid stats", kind: queryStats, exp: &expectation{stats: &statsExpectation{}}},
		{name: "valid empty series", kind: queryVolume, exp: &expectation{seriesSet: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateExpectation(tc.kind, tc.exp)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateExpectation: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validateExpectation error = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateExpectationStatusAllTypes(t *testing.T) {
	exp, err := parseExpectation([]byte(`{"status":400}`))
	if err != nil {
		t.Fatalf("parseExpectation: %v", err)
	}
	for _, kind := range queryTypeList() {
		if err := validateExpectation(kind, exp); err != nil {
			t.Errorf("validateExpectation(%s, status): %v", kind, err)
		}
	}
}

func TestFormatPointValuesBounded(t *testing.T) {
	points := make([]normPoint, 12)
	for i := range points {
		points[i] = normPoint{rawTS: int64(i), value: "1"}
	}
	rendered := formatPointValues(points, true)
	if !strings.Contains(rendered, "... (2 more)") {
		t.Errorf("formatPointValues = %q, want a bounded suffix", rendered)
	}
	if got := formatPointValues(nil, false); got != "<missing>" {
		t.Errorf("missing side = %q, want <missing>", got)
	}
	if got := formatPointValues(nil, true); got != "[]" {
		t.Errorf("present empty side = %q, want []", got)
	}
}
