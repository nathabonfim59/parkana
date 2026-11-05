package logql

import (
	"testing"

	"github.com/nathabonfim59/parkana/internal/logs"
)

func TestStringLabelFilters(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		set    string
		keep   bool
		labels logs.Labels
		meta   logs.Labels
	}{
		{name: "missing equals empty keeps", query: `{app="api"} | foo = ""`, keep: true},
		{name: "missing not equal empty drops", query: `{app="api"} | foo != ""`, keep: false},
		{name: "missing matches .*", query: `{app="api"} | foo =~ ".*"`, keep: true},
		{name: "missing does not match .+", query: `{app="api"} | foo =~ ".+"`, keep: false},
		{name: "missing not matches .*", query: `{app="api"} | foo !~ ".*"`, keep: false},
		{
			name:  "newline-only value passes .+ because the optimizer tests existence",
			query: `{app="api"} | foo =~ ".+"`,
			set:   "\n",
			// The pinned optimizer turns .+ into len(value)>0, so a newline-only
			// value passes even though Go's . does not match a newline.
			keep: true,
		},
		{
			name:  "literal plus .* becomes a substring test",
			query: `{app="api"} | foo =~ "bar.*"`,
			set:   "xxbarxx",
			// simplifyConcat yields a contains filter, so the anchored semantics
			// of the source regexp are not what runs.
			keep: true,
		},
		{
			name:  "an OR with .* drops the true branch",
			query: `{app="api"} | foo =~ "foo|.*"`,
			set:   "xxfoo",
			// newOrFilter treats TrueFilter as an identity: foo|.* is foo.
			keep: false,
		},
		{
			name:  "an OR with .* still matches the literal branch",
			query: `{app="api"} | foo =~ "foo|.*"`,
			set:   "foo",
			keep:  true,
		},
		{
			name:  "dot does not match a newline",
			query: `{app="api"} | foo =~ "a.b"`,
			set:   "a\nb",
			keep:  false,
		},
		{
			name:  "inline (?s) makes dot match a newline",
			query: `{app="api"} | foo =~ "(?s)a.b"`,
			set:   "a\nb",
			keep:  true,
		},
		{
			name:   "parsed wins over stream and metadata",
			query:  `{app="api"} | foo = "parsed"`,
			set:    "parsed",
			keep:   true,
			labels: labelPairs("foo", "stream"),
			meta:   labelPairs("foo", "meta"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := mustStages(t, tt.query)
			e := newTestEntry("", tt.labels, tt.meta)
			if tt.set != "" {
				e.setParsed("foo", tt.set)
			}
			if got := mustRun(t, stages, e); got != tt.keep {
				t.Fatalf("keep = %v, want %v", got, tt.keep)
			}
		})
	}
}

func TestLabelFilterCategoryPrecedence(t *testing.T) {
	stages := mustStages(t, `{app="api"} | foo = "meta"`)
	e := newTestEntry("", labelPairs("foo", "stream"), labelPairs("foo", "meta"))
	if !mustRun(t, stages, e) {
		t.Fatal("metadata value should win over the stream label")
	}
	stages = mustStages(t, `{app="api"} | foo = "stream"`)
	if mustRun(t, stages, newTestEntry("", labelPairs("foo", "stream"), labelPairs("foo", "meta"))) {
		t.Fatal("stream value should not match once metadata owns the name")
	}
}

func TestTypedLabelFilterMissingDropsWithoutError(t *testing.T) {
	for _, query := range []string{
		`{app="api"} | n > 1`,
		`{app="api"} | d > 1s`,
		`{app="api"} | b > 1KB`,
	} {
		t.Run(query, func(t *testing.T) {
			e := newTestEntry("", nil, nil)
			if mustRun(t, mustStages(t, query), e) {
				t.Fatal("missing typed label kept the row")
			}
			assertErr(t, e, "")
		})
	}
}

func TestTypedLabelFilterInvalidValueKeepsRowWithError(t *testing.T) {
	tests := []struct {
		query string
		name  string
	}{
		{query: `{app="api"} | n > 1`, name: "n"},
		{query: `{app="api"} | d > 1s`, name: "d"},
		{query: `{app="api"} | b > 1KB`, name: "b"},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			e := newTestEntry("", labelPairs(tt.name, "not-a-value"), nil)
			if !mustRun(t, mustStages(t, tt.query), e) {
				t.Fatal("conversion error dropped the row")
			}
			assertErr(t, e, labelFilterErr)
			if e.errDetailsValue() == "" {
				t.Fatal("LabelFilterErr without details")
			}
		})
	}
}

func TestNumericLabelFilter(t *testing.T) {
	tests := []struct {
		name  string
		query string
		value string
		keep  bool
	}{
		{name: "eq", query: `{app="api"} | n == 2`, value: "2", keep: true},
		{name: "neq", query: `{app="api"} | n != 2`, value: "2", keep: false},
		{name: "gt", query: `{app="api"} | n > 2`, value: "3", keep: true},
		{name: "gte", query: `{app="api"} | n >= 2`, value: "2", keep: true},
		{name: "lt", query: `{app="api"} | n < 2`, value: "1.5", keep: true},
		{name: "lte", query: `{app="api"} | n <= 2`, value: "3", keep: false},
		{name: "NaN is never ordered", query: `{app="api"} | n > 0`, value: "NaN", keep: false},
		{name: "NaN is not equal", query: `{app="api"} | n == 0`, value: "NaN", keep: false},
		{name: "NaN is not equal to itself", query: `{app="api"} | n != 0`, value: "NaN", keep: true},
		{name: "Inf compares", query: `{app="api"} | n > 1`, value: "Inf", keep: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestEntry("", labelPairs("n", tt.value), nil)
			if got := mustRun(t, mustStages(t, tt.query), e); got != tt.keep {
				t.Fatalf("keep = %v, want %v", got, tt.keep)
			}
		})
	}
}

func TestDurationLabelFilter(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		value   string
		keep    bool
		wantErr string
	}{
		{name: "less", query: `{app="api"} | d > 1s`, value: "500ms", keep: false},
		{name: "more", query: `{app="api"} | d > 1s`, value: "2s", keep: true},
		{name: "equal", query: `{app="api"} | d == 1500ms`, value: "1.5s", keep: true},
		{
			// The right-hand literal accepts a day component, the field value
			// does not: time.ParseDuration has no "d" unit.
			name:    "day field value is a conversion error",
			query:   `{app="api"} | d > 1d`,
			value:   "1d",
			keep:    true,
			wantErr: labelFilterErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestEntry("", labelPairs("d", tt.value), nil)
			if got := mustRun(t, mustStages(t, tt.query), e); got != tt.keep {
				t.Fatalf("keep = %v, want %v", got, tt.keep)
			}
			assertErr(t, e, tt.wantErr)
		})
	}
}

func TestBytesLabelFilter(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		value   string
		keep    bool
		wantErr string
	}{
		{name: "bare number is bytes", query: `{app="api"} | b > 1KB`, value: "1500", keep: true},
		{name: "bare number below", query: `{app="api"} | b > 1KB`, value: "999", keep: false},
		{name: "SI unit", query: `{app="api"} | b == 1KB`, value: "1kb", keep: true},
		{name: "IEC unit", query: `{app="api"} | b > 1000B`, value: "1KiB", keep: true},
		{name: "commas are removed", query: `{app="api"} | b > 1KB`, value: "2,000", keep: true},
		{name: "fractional unit scales", query: `{app="api"} | b > 1KB`, value: "1.5KB", keep: true},
		{name: "fractional bare truncates", query: `{app="api"} | b > 1B`, value: "1.5", keep: false},
		{name: "uint64 beyond int64", query: `{app="api"} | b > 1KB`, value: "9223372036854775808", keep: true},
		{
			name:    "2^64 is too large",
			query:   `{app="api"} | b > 1KB`,
			value:   "18446744073709551616",
			keep:    true,
			wantErr: labelFilterErr,
		},
		{
			name:    "unknown unit",
			query:   `{app="api"} | b > 1KB`,
			value:   "1XB",
			keep:    true,
			wantErr: labelFilterErr,
		},
		{
			name:    "negative is not a byte count",
			query:   `{app="api"} | b > 1KB`,
			value:   "-1",
			keep:    true,
			wantErr: labelFilterErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestEntry("", labelPairs("b", tt.value), nil)
			if got := mustRun(t, mustStages(t, tt.query), e); got != tt.keep {
				t.Fatalf("keep = %v, want %v", got, tt.keep)
			}
			assertErr(t, e, tt.wantErr)
		})
	}
}

func TestBooleanFilterEvaluationIsObservable(t *testing.T) {
	// (false comparison AND invalid conversion) OR true comparison: the AND
	// still evaluates its right side, so the conversion error is recorded and
	// the row survives through the OR.
	query := `{app="api"} | (n > 100 and m > 500) or n >= 0`
	e := newTestEntry("", labelPairs("n", "0", "m", "bad"), nil)
	if !mustRun(t, mustStages(t, query), e) {
		t.Fatal("row should survive the true OR branch")
	}
	assertErr(t, e, labelFilterErr)

	// OR short-circuits when the left side is true, so the right conversion
	// never runs and no error is recorded.
	query = `{app="api"} | n >= 0 or m > 500`
	e = newTestEntry("", labelPairs("n", "0", "m", "bad"), nil)
	if !mustRun(t, mustStages(t, query), e) {
		t.Fatal("row should survive the true left side")
	}
	assertErr(t, e, "")
}

func TestErrorLabelFilters(t *testing.T) {
	badJSON := `not json`
	tests := []struct {
		name  string
		query string
		keep  bool
		want  string
	}{
		{
			name:  "error label is not empty",
			query: `{app="api"} | json | __error__ != ""`,
			keep:  true,
			want:  jsonParserErr,
		},
		{
			name:  "empty error excludes failed rows",
			query: `{app="api"} | json | __error__ = ""`,
			keep:  false,
			want:  jsonParserErr,
		},
		{
			name:  "regexp on the error label",
			query: `{app="api"} | json | __error__ =~ "JSONParser.*"`,
			keep:  true,
			want:  jsonParserErr,
		},
		{
			name: "error details are invisible to Get, so the empty filter matches",
			// Only __error__ is special-cased by labelValue; __error_details__
			// goes through Get, which never sees the builder's error state.
			query: `{app="api"} | json | __error_details__ = ""`,
			keep:  true,
			want:  jsonParserErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestEntry(badJSON, nil, nil)
			stages := mustStages(t, tt.query)
			if got := mustRun(t, stages, e); got != tt.keep {
				t.Fatalf("keep = %v, want %v", got, tt.keep)
			}
			assertErr(t, e, tt.want)
		})
	}
}

func TestErrorLabelDropsRowOnlyThroughAStage(t *testing.T) {
	// A parser error alone never rejects a row.
	for _, query := range []string{
		`{app="api"} | json`,
		`{app="api"} | logfmt --strict`,
	} {
		e := newTestEntry("not json or logfmt =", nil, nil)
		if !mustRun(t, mustStages(t, query), e) {
			t.Fatalf("%q dropped the row", query)
		}
	}
}

func TestTypedFilterOnEmptyParsedLabel(t *testing.T) {
	// The named JSON parser materializes missing paths as ""; a typed filter
	// then sees a present but unparsable value and keeps the row with an error.
	query := `{app="api"} | json x="missing" | x > 1`
	e := newTestEntry(`{}`, nil, nil)
	if !mustRun(t, mustStages(t, query), e) {
		t.Fatal("row with an empty extracted value should survive")
	}
	assertErr(t, e, labelFilterErr)
}

func TestTypedFilterIgnoresEmptyStreamLabel(t *testing.T) {
	e := newTestEntry("", labelPairs("n", ""), nil)
	if mustRun(t, mustStages(t, `{app="api"} | n > 1`), e) {
		t.Fatal("empty stream label should count as missing and drop the row")
	}
	assertErr(t, e, "")
}
