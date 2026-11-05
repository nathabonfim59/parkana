package logql

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

func TestJSONFlatten(t *testing.T) {
	tests := []struct {
		name        string
		line        string
		labels      logs.Labels
		meta        logs.Labels
		want        map[string]string
		wantErr     string
		wantDetails string
	}{
		{
			name: "flat values",
			line: `{"s":"v","n":12.5,"t":true,"f":false}`,
			want: map[string]string{"s": "v", "n": "12.5", "t": "true", "f": "false"},
		},
		{
			name: "nested objects join with underscore",
			line: `{"o":{"i":{"x":"v"},"y":2}}`,
			want: map[string]string{"o_i_x": "v", "o_y": "2"},
		},
		{
			name: "arrays and nulls are ignored",
			line: `{"a":[1,{"b":2}],"n":null,"c":3}`,
			want: map[string]string{"c": "3"},
		},
		{
			name: "nested arrays inside arrays ignored",
			line: `{"a":[[1,2],{"b":3}],"c":"d"}`,
			want: map[string]string{"c": "d"},
		},
		{
			name: "keys are sanitized and trimmed",
			line: `{"a b":{"c d":1},"1x":2,"--":3}`,
			want: map[string]string{"a_b_c_d": "1", "_1x": "2", "__": "3"},
		},
		{
			name: "escaped key is unescaped before sanitizing",
			line: `{"a\"b":1}`,
			want: map[string]string{"a_b": "1"},
		},
		{
			name: "string escapes are decoded",
			line: `{"a":"x\ny\tz"}`,
			want: map[string]string{"a": "x\ny\tz"},
		},
		{
			name: "invalid escape in a value becomes empty without an error",
			line: `{"a":"\q"}`,
			want: map[string]string{"a": ""},
		},
		{
			name: "invalid utf8 in a value becomes a space",
			line: "{\"a\":\"x\xffy\"}",
			want: map[string]string{"a": "x y"},
		},
		{
			name: "duplicate keys keep the first value",
			line: `{"a":1,"a":2}`,
			want: map[string]string{"a": "1"},
		},
		{
			name:   "collision with a stream label takes the suffix",
			line:   `{"a":1}`,
			labels: labelPairs("a", "stream"),
			want:   map[string]string{"a" + duplicateSuffix: "1"},
		},
		{
			name: "collision with metadata takes the suffix",
			line: `{"a":1}`,
			meta: labelPairs("a", "meta"),
			want: map[string]string{"a" + duplicateSuffix: "1"},
		},
		{
			name:   "collision keeps the stream label",
			line:   `{"a":1}`,
			labels: labelPairs("a", "stream", "b", "keep"),
			want:   map[string]string{"a" + duplicateSuffix: "1"},
		},
		{
			name: "trailing comma is accepted",
			line: `{"a":1,}`,
			want: map[string]string{"a": "1"},
		},
		{
			name:        "empty line",
			line:        "",
			wantErr:     jsonParserErr,
			wantDetails: errMalformedObject.Error(),
		},
		{
			name:        "not an object",
			line:        "not json",
			wantErr:     jsonParserErr,
			wantDetails: errMalformedObject.Error(),
		},
		{
			name:        "truncated object",
			line:        `{"a":1`,
			want:        map[string]string{"a": "1"},
			wantErr:     jsonParserErr,
			wantDetails: errMalformedArray.Error(),
		},
		{
			name:        "missing comma",
			line:        `{"a":1 "b":2}`,
			want:        map[string]string{"a": "1"},
			wantErr:     jsonParserErr,
			wantDetails: errMalformedObject.Error(),
		},
		{
			name:        "missing value",
			line:        `{"a":}`,
			wantErr:     jsonParserErr,
			wantDetails: errUnknownValueType.Error(),
		},
		{
			name:        "unterminated string",
			line:        `{"a":"x`,
			wantErr:     jsonParserErr,
			wantDetails: errMalformedString.Error(),
		},
		{
			name:        "bad literal",
			line:        `{"a":tru}`,
			wantErr:     jsonParserErr,
			wantDetails: errUnknownValueType.Error(),
		},
		{
			name:        "opening brace only",
			line:        "{",
			wantErr:     jsonParserErr,
			wantDetails: errMalformedJSON.Error(),
		},
		{
			name: "labels before the error survive",
			line: `{"a":1,"b":}`,
			want: map[string]string{"a": "1"},
			// The failing value aborts the walk; the error kind is set.
			wantErr:     jsonParserErr,
			wantDetails: errUnknownValueType.Error(),
		},
	}
	stages := mustStages(t, `{app="api"} | json`)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestEntry(tt.line, tt.labels, tt.meta)
			if keep := mustRun(t, stages, e); !keep {
				t.Fatal("json stage dropped the row")
			}
			assertParsed(t, e, tt.want)
			if tt.wantErr == "" {
				if got := e.errValue(); got != "" {
					t.Fatalf("unexpected __error__ = %q (details %q)", got, e.errDetailsValue())
				}
				return
			}
			assertErr(t, e, tt.wantErr)
			if tt.wantDetails != "" && e.errDetailsValue() != tt.wantDetails {
				t.Fatalf("details = %q, want %q", e.errDetailsValue(), tt.wantDetails)
			}
		})
	}
}

func TestJSONNamed(t *testing.T) {
	tests := []struct {
		name        string
		extractions string
		line        string
		labels      logs.Labels
		meta        logs.Labels
		want        map[string]string
		wantErr     string
		wantDetails string
	}{
		{
			name:        "nested path",
			extractions: `x="a.b"`,
			line:        `{"a":{"b":"v"}}`,
			want:        map[string]string{"x": "v"},
		},
		{
			name:        "quoted key with a dot",
			extractions: `x="[\"a.b\"]"`,
			line:        `{"a.b":"v"}`,
			want:        map[string]string{"x": "v"},
		},
		{
			name:        "bracket key",
			extractions: `x="a[\"b c\"]"`,
			line:        `{"a":{"b c":"v"}}`,
			want:        map[string]string{"x": "v"},
		},
		{
			name:        "array index",
			extractions: `x="l[1].y"`,
			line:        `{"l":[{"y":1},{"y":2}]}`,
			want:        map[string]string{"x": "2"},
		},
		{
			name:        "root array index",
			extractions: `x="[1]"`,
			line:        `[10,20]`,
			want:        map[string]string{"x": "20"},
		},
		{
			name:        "object is kept raw",
			extractions: `x="o"`,
			line:        `{"o":{"a": 1}}`,
			want:        map[string]string{"x": `{"a": 1}`},
		},
		{
			name:        "array is kept raw with escapes decoded",
			extractions: `x="l"`,
			line:        `{"l":["a\"b"]}`,
			want:        map[string]string{"x": `["a"b"]`},
		},
		{
			name:        "null is empty",
			extractions: `x="a"`,
			line:        `{"a":null}`,
			want:        map[string]string{"x": ""},
		},
		{
			name:        "number and boolean",
			extractions: `n="n", b="b"`,
			line:        `{"n":12,"b":false}`,
			want:        map[string]string{"n": "12", "b": "false"},
		},
		{
			name:        "missing path becomes empty",
			extractions: `x="b"`,
			line:        `{"a":1}`,
			want:        map[string]string{"x": ""},
		},
		{
			name:        "empty line creates nothing",
			extractions: `x="a"`,
			line:        "",
			want:        map[string]string{},
		},
		{
			name:        "not json reports the kind without details",
			extractions: `x="a"`,
			line:        "not json",
			want:        map[string]string{},
			wantErr:     jsonParserErr,
			wantDetails: "",
		},
		{
			name:        "bad value reports details",
			extractions: `x="a"`,
			line:        `{"a":}`,
			want:        map[string]string{"x": ""},
			wantErr:     jsonParserErr,
			wantDetails: errUnknownValueType.Error(),
		},
		{
			name:        "invalid escape in a value is empty without error",
			extractions: `x="a"`,
			line:        `{"a":"\q"}`,
			want:        map[string]string{"x": ""},
		},
		{
			name:        "duplicate destinations overwrite in order",
			extractions: `x="a", x="b"`,
			line:        `{"a":"1","b":"2"}`,
			want:        map[string]string{"x": "2"},
		},
		{
			name:        "collision with a stream label takes the suffix",
			extractions: `x="a"`,
			line:        `{"a":"v"}`,
			labels:      labelPairs("x", "stream"),
			want:        map[string]string{"x" + duplicateSuffix: "v"},
		},
		{
			name:        "missing value and an empty stream label leave an empty parsed label",
			extractions: `x="a"`,
			line:        `{"a":"v"}`,
			labels:      labelPairs("x", ""),
			want:        map[string]string{"x": "", "x" + duplicateSuffix: "v"},
		},
		{
			name:        "metadata collision takes the suffix",
			extractions: `x="a"`,
			line:        `{"a":"v"}`,
			meta:        labelPairs("x", "meta"),
			want:        map[string]string{"x" + duplicateSuffix: "v"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := mustStages(t, `{app="api"} | json `+tt.extractions)
			e := newTestEntry(tt.line, tt.labels, tt.meta)
			if keep := mustRun(t, stages, e); !keep {
				t.Fatal("json stage dropped the row")
			}
			assertParsed(t, e, tt.want)
			if tt.wantErr == "" {
				if got := e.errValue(); got != "" {
					t.Fatalf("unexpected __error__ = %q (details %q)", got, e.errDetailsValue())
				}
				return
			}
			assertErr(t, e, tt.wantErr)
			if got := e.errDetailsValue(); got != tt.wantDetails {
				t.Fatalf("details = %q, want %q", got, tt.wantDetails)
			}
		})
	}
}

func TestJSONNamedOverwritesExtracted(t *testing.T) {
	stages := mustStages(t, `{app="api"} | json | json b="a"`)
	e := newTestEntry(`{"a":1,"b":2}`, nil, nil)
	mustRun(t, stages, e)
	// The flattening stage wrote a=1, b=2; the named stage deliberately
	// overwrites b with a's value instead of skipping the extracted name.
	assertParsed(t, e, map[string]string{"a": "1", "b": "1"})
}

func TestJSONNamedPathCompileErrors(t *testing.T) {
	for _, query := range []string{
		`{app="api"} | json x="a..b"`,
		`{app="api"} | json x="a."`,
		`{app="api"} | json x="a[1.5]"`,
		`{app="api"} | json x="a[x]"`,
		`{app="api"} | json x="[1"`,
		`{app="api"} | json x="-a"`,
	} {
		t.Run(query, func(t *testing.T) {
			_, err := testEngine().CompileLogQuery(query)
			if !errors.Is(err, plan.ErrBadQuery) {
				t.Fatalf("CompileLogQuery(%q) = %v, want ErrBadQuery", query, err)
			}
		})
	}
}

func TestLogfmtPlain(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		line      string
		want      map[string]string
		wantErr   string
		wantDetal string
	}{
		{
			name:  "basic",
			query: `{app="api"} | logfmt`,
			line:  `a=1 b="two words" c`,
			want:  map[string]string{"a": "1", "b": "two words"},
		},
		{
			name:  "keep empty",
			query: `{app="api"} | logfmt --keep-empty`,
			line:  `a= b=""`,
			want:  map[string]string{"a": "", "b": ""},
		},
		{
			name:  "quoted escapes",
			query: `{app="api"} | logfmt`,
			line:  `a="x\ny"`,
			want:  map[string]string{"a": "x\ny"},
		},
		{
			name:  "duplicate keys keep the first value",
			query: `{app="api"} | logfmt`,
			line:  `a=1 a=2`,
			want:  map[string]string{"a": "1"},
		},
		{
			name:  "invalid utf8 value becomes a space",
			query: `{app="api"} | logfmt`,
			line:  "a=x\xffy",
			want:  map[string]string{"a": "x y"},
		},
		{
			name:    "strict malformed pair",
			query:   `{app="api"} | logfmt --strict`,
			line:    `a=1 =bad`,
			want:    map[string]string{"a": "1"},
			wantErr: logfmtParserErr,
			// SyntaxError.Pos is 1-based.
			wantDetal: `logfmt syntax error at pos 5 : unexpected '='`,
		},
		{
			name:  "permissive malformed pair keeps scanning",
			query: `{app="api"} | logfmt`,
			line:  `a=1 =bad b=2`,
			want:  map[string]string{"a": "1", "b": "2"},
		},
		{
			name:      "strict unterminated quote",
			query:     `{app="api"} | logfmt --strict`,
			line:      `a=1 b="oops`,
			want:      map[string]string{"a": "1"},
			wantErr:   logfmtParserErr,
			wantDetal: `logfmt syntax error at pos 12 : unterminated quoted value`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := mustStages(t, tt.query)
			e := newTestEntry(tt.line, nil, nil)
			if keep := mustRun(t, stages, e); !keep {
				t.Fatal("logfmt stage dropped the row")
			}
			assertParsed(t, e, tt.want)
			if tt.wantErr == "" {
				if got := e.errValue(); got != "" {
					t.Fatalf("unexpected __error__ = %q (details %q)", got, e.errDetailsValue())
				}
				return
			}
			assertErr(t, e, tt.wantErr)
			if tt.wantDetal != "" && e.errDetailsValue() != tt.wantDetal {
				t.Fatalf("details = %q, want %q", e.errDetailsValue(), tt.wantDetal)
			}
		})
	}
}

func TestLogfmtNamed(t *testing.T) {
	tests := []struct {
		name        string
		extractions string
		line        string
		labels      logs.Labels
		meta        logs.Labels
		want        map[string]string
		wantErr     string
	}{
		{
			name:        "rename",
			extractions: `out="src"`,
			line:        `src=1 other=2`,
			want:        map[string]string{"out": "1"},
		},
		{
			name:        "identity extraction",
			extractions: `a`,
			line:        `a=1 b=2`,
			want:        map[string]string{"a": "1"},
		},
		{
			name:        "missing source stays empty",
			extractions: `out="src"`,
			line:        `other=2`,
			want:        map[string]string{"out": ""},
		},
		{
			name:        "duplicate destination keeps the last path",
			extractions: `out="a", out="b"`,
			line:        `a=1 b=2`,
			want:        map[string]string{"out": "2"},
		},
		{
			name:        "collision with a stream label takes the suffix",
			extractions: `out="src"`,
			line:        `src=1`,
			labels:      labelPairs("out", "stream"),
			want:        map[string]string{"out" + duplicateSuffix: "1"},
		},
		{
			name:        "collision with metadata takes the suffix",
			extractions: `out="src"`,
			line:        `src=1`,
			meta:        labelPairs("out", "meta"),
			want:        map[string]string{"out" + duplicateSuffix: "1"},
		},
		{
			name:        "invalid utf8 value is cleared",
			extractions: `out="src"`,
			line:        "src=x\xffy",
			want:        map[string]string{"out": ""},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := mustStages(t, `{app="api"} | logfmt `+tt.extractions)
			e := newTestEntry(tt.line, tt.labels, tt.meta)
			if keep := mustRun(t, stages, e); !keep {
				t.Fatal("logfmt stage dropped the row")
			}
			assertParsed(t, e, tt.want)
			if tt.wantErr != "" {
				assertErr(t, e, tt.wantErr)
			}
		})
	}
}

func TestLogfmtNamedOverwritesAndPreFills(t *testing.T) {
	stages := mustStages(t, `{app="api"} | logfmt out="src"`)
	e := newTestEntry(`out=bogus src=1`, nil, nil)
	mustRun(t, stages, e)
	// The destination is pre-created, but the source key overwrites it.
	assertParsed(t, e, map[string]string{"out": "1"})
}

func TestRegexpParser(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		line    string
		labels  logs.Labels
		want    map[string]string
		wantErr string
	}{
		{
			name:  "named captures",
			query: `{app="api"} | regexp "(?P<level>\\w+) (?P<msg>.+)"`,
			line:  "WARN something broke",
			want:  map[string]string{"level": "WARN", "msg": "something broke"},
		},
		{
			name:  "unnamed groups are skipped",
			query: `{app="api"} | regexp "(\\w+) (?P<x>\\w+)"`,
			line:  "a b",
			want:  map[string]string{"x": "b"},
		},
		{
			name:  "unmatched optional group is empty",
			query: `{app="api"} | regexp "(?P<a>x)?(?P<b>y)"`,
			line:  "y",
			want:  map[string]string{"a": "", "b": "y"},
		},
		{
			name:  "no match keeps the row without labels",
			query: `{app="api"} | regexp "(?P<n>\\d+)"`,
			line:  "abc",
			want:  map[string]string{},
		},
		{
			name:   "collision takes the suffix",
			query:  `{app="api"} | regexp "(?P<n>.*)"`,
			line:   "v",
			labels: labelPairs("n", "stream"),
			want:   map[string]string{"n" + duplicateSuffix: "v"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := mustStages(t, tt.query)
			e := newTestEntry(tt.line, tt.labels, nil)
			if keep := mustRun(t, stages, e); !keep {
				t.Fatal("regexp stage dropped the row")
			}
			assertParsed(t, e, tt.want)
		})
	}
}

func TestRegexpParserSkipsAlreadyExtracted(t *testing.T) {
	stages := mustStages(t, `{app="api"} | regexp "(?P<x>a)" | regexp "(?P<x>b)"`)
	e := newTestEntry("ab", nil, nil)
	mustRun(t, stages, e)
	assertParsed(t, e, map[string]string{"x": "a"})
}

func TestPatternParser(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		line    string
		want    map[string]string
	}{
		{
			name:    "anchored literal and capture",
			pattern: `foo <foo> bar`,
			line:    `foo buzz bar`,
			want:    map[string]string{"foo": "buzz"},
		},
		{
			name:    "must start with the leading literal",
			pattern: `foo <x>`,
			line:    `xx foo bar`,
			want:    map[string]string{},
		},
		{
			name:    "leading wildcard makes it unanchored",
			pattern: `<_> foo <x>`,
			line:    `xx foo bar`,
			want:    map[string]string{"x": "bar"},
		},
		{
			name:    "missing separator gives the rest to the capture",
			pattern: `<path>?<_>`,
			line:    `/api/plugins/status`,
			want:    map[string]string{"path": "/api/plugins/status"},
		},
		{
			name:    "adjacent literals",
			pattern: `<foo>bar<baz>`,
			line:    ` bar `,
			want:    map[string]string{"foo": " ", "baz": " "},
		},
		{
			name:    "anonymous captures are skipped",
			pattern: `<a>-<_>-<b>`,
			line:    `1-x-2`,
			want:    map[string]string{"a": "1", "b": "2"},
		},
		{
			name:    "no match keeps the row",
			pattern: `foo <x> bar`,
			line:    `nope`,
			want:    map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := mustStages(t, `{app="api"} | pattern "`+tt.pattern+`"`)
			e := newTestEntry(tt.line, nil, nil)
			if keep := mustRun(t, stages, e); !keep {
				t.Fatal("pattern stage dropped the row")
			}
			assertParsed(t, e, tt.want)
		})
	}
}

func TestPatternCompileErrors(t *testing.T) {
	for _, pattern := range []string{"<_>", "foo bar", "<a><b>", "<a> <a>", ""} {
		t.Run(pattern, func(t *testing.T) {
			_, err := compileStage(&PatternParser{Pattern: pattern})
			if !errors.Is(err, plan.ErrBadQuery) {
				t.Fatalf("compileStage(pattern %q) = %v, want ErrBadQuery", pattern, err)
			}
		})
	}
}

func TestParserErrorReplacesEarlierParserError(t *testing.T) {
	// The line fails both parsers. addErrLabel always replaces the kind, so
	// the later logfmt stage owns the final __error__.
	stages := mustStages(t, `{app="api"} | json | logfmt --strict`)
	e := newTestEntry(`not json =bad`, nil, nil)
	mustRun(t, stages, e)
	assertErr(t, e, logfmtParserErr)
}

func TestTypedFilterPreservesParserError(t *testing.T) {
	// The truncated line sets n=x and JSONParserErr; the numeric conversion of
	// n fails too, but a typed filter must not overwrite the parser error.
	stages := mustStages(t, `{app="api"} | json | n > 1`)
	e := newTestEntry(`{"n":"x"`, nil, nil)
	if keep := mustRun(t, stages, e); !keep {
		t.Fatal("row dropped; a conversion error keeps the row")
	}
	assertErr(t, e, jsonParserErr)
	if got := e.errDetailsValue(); got != errMalformedArray.Error() {
		t.Fatalf("details = %q, want %q", got, errMalformedArray.Error())
	}
}

func TestSanitizeLabelKey(t *testing.T) {
	tests := map[string]string{
		"":        "",
		"  a b ":  "a_b",
		"1x":      "_1x",
		"a.b-c":   "a_b_c",
		"é":       "_",
		"a\tb":    "a_b",
		"__error": "__error",
	}
	for in, want := range tests {
		if got := sanitizeLabelKey(in); got != want {
			t.Errorf("sanitizeLabelKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestJSONUnicodeEscapes(t *testing.T) {
	tests := []struct {
		name string
		line string
		want map[string]string
	}{
		{
			name: "bmp escape",
			line: `{"a":"\u0041"}`,
			want: map[string]string{"a": "A"},
		},
		{
			name: "surrogate pair",
			line: `{"a":"\uD83D\uDE00"}`,
			want: map[string]string{"a": "\U0001F600"},
		},
		{
			name: "unpaired surrogate is an invalid escape",
			line: `{"a":"\uD800"}`,
			want: map[string]string{"a": ""},
		},
		{
			name: "invalid hex is an invalid escape",
			line: `{"a":"\uZZZZ"}`,
			want: map[string]string{"a": ""},
		},
		{
			name: "escaped key",
			line: `{"\u0061":1}`,
			want: map[string]string{"a": "1"},
		},
	}
	stages := mustStages(t, `{app="api"} | json`)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestEntry(tt.line, nil, nil)
			mustRun(t, stages, e)
			assertParsed(t, e, tt.want)
		})
	}
}

func TestJSONFlattenStructuralEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		line        string
		wantDetails string
	}{
		{name: "missing colon", line: `{"a" 1}`, wantDetails: errMalformedJSON.Error()},
		{name: "bad key escape", line: `{"\q":1}`, wantDetails: errMalformedStringEscape.Error()},
		{name: "eof after comma", line: `{"a":1,`, wantDetails: errMalformedArray.Error()},
		{name: "nested non-ascii key", line: `{"a":{"é":1,"b":2}}`, wantDetails: ""},
	}
	stages := mustStages(t, `{app="api"} | json`)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestEntry(tt.line, nil, nil)
			mustRun(t, stages, e)
			if tt.wantDetails == "" {
				assertErr(t, e, "")
				return
			}
			assertErr(t, e, jsonParserErr)
			if got := e.errDetailsValue(); got != tt.wantDetails {
				t.Fatalf("details = %q, want %q", got, tt.wantDetails)
			}
		})
	}
}

func TestJSONNamedArrayEdges(t *testing.T) {
	tests := []struct {
		name        string
		extractions string
		line        string
		want        map[string]string
	}{
		{
			name:        "index past the end",
			extractions: `x="l[5]"`,
			line:        `{"l":[1]}`,
			want:        map[string]string{"x": ""},
		},
		{
			name:        "index on a non-array",
			extractions: `x="a[0]"`,
			line:        `{"a":1}`,
			want:        map[string]string{"x": ""},
		},
		{
			name:        "key lookup on an array",
			extractions: `x="l.a"`,
			line:        `{"l":[{"a":1}]}`,
			want:        map[string]string{"x": ""},
		},
		{
			name:        "malformed array element stops the scan",
			extractions: `x="l[1]"`,
			line:        `{"l":[1,]}`,
			want:        map[string]string{"x": ""},
		},
		{
			name:        "nested array index",
			extractions: `x="l[0][1]"`,
			line:        `{"l":[[1,2]]}`,
			want:        map[string]string{"x": "2"},
		},
		{
			name:        "root string has no paths",
			extractions: `x="a"`,
			line:        `"just a string"`,
			want:        map[string]string{"x": ""},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := mustStages(t, `{app="api"} | json `+tt.extractions)
			e := newTestEntry(tt.line, nil, nil)
			mustRun(t, stages, e)
			assertParsed(t, e, tt.want)
			assertErr(t, e, "")
		})
	}
}

func TestRegexpCompileErrors(t *testing.T) {
	for _, pattern := range []string{`(a)`, `(?P<a>x)(?P<a>y)`, `(`} {
		t.Run(pattern, func(t *testing.T) {
			_, err := compileStage(&RegexpParser{Pattern: pattern})
			if !errors.Is(err, plan.ErrBadQuery) {
				t.Fatalf("compileStage(%q) = %v, want ErrBadQuery", pattern, err)
			}
		})
	}
}

func TestLogfmtNamedCompileErrors(t *testing.T) {
	for _, query := range []string{
		`{app="api"} | logfmt x="a.b"`,
		`{app="api"} | logfmt x="a" "b"`,
		`{app="api"} | logfmt x="[a]"`,
		`{app="api"} | logfmt x=""`,
	} {
		t.Run(query, func(t *testing.T) {
			_, err := testEngine().CompileLogQuery(query)
			if !errors.Is(err, plan.ErrBadQuery) {
				t.Fatalf("CompileLogQuery(%q) = %v, want ErrBadQuery", query, err)
			}
		})
	}
}

func TestLogfmtQuotedEscapes(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		line      string
		want      map[string]string
		wantErr   string
		wantDetal string
	}{
		{
			name:  "quote and backslash",
			query: `{app="api"} | logfmt`,
			line:  `a="q\"uote" b="back\\slash"`,
			want:  map[string]string{"a": `q"uote`, "b": `back\slash`},
		},
		{
			name:  "unicode escape",
			query: `{app="api"} | logfmt`,
			line:  `a="uni\u0041"`,
			want:  map[string]string{"a": "uniA"},
		},
		{
			name:      "invalid escape is a strict error",
			query:     `{app="api"} | logfmt --strict`,
			line:      `a="bad\q"`,
			want:      map[string]string{},
			wantErr:   logfmtParserErr,
			wantDetal: `logfmt syntax error at pos 10 : invalid quoted value`,
		},
		{
			name:  "invalid escape is silent in permissive mode",
			query: `{app="api"} | logfmt`,
			line:  `a="bad\q" b=2`,
			want:  map[string]string{"b": "2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := mustStages(t, tt.query)
			e := newTestEntry(tt.line, nil, nil)
			mustRun(t, stages, e)
			assertParsed(t, e, tt.want)
			if tt.wantErr != "" {
				assertErr(t, e, tt.wantErr)
				if got := e.errDetailsValue(); got != tt.wantDetal {
					t.Fatalf("details = %q, want %q", got, tt.wantDetal)
				}
			}
		})
	}
}

func TestJSONNestingLimit(t *testing.T) {
	const depth = maxJSONNesting + 100
	line := strings.Repeat(`{"a":`, depth) + "1" + strings.Repeat("}", depth)
	stages := mustStages(t, `{app="api"} | json`)
	e := newTestEntry(line, nil, nil)
	if !mustRun(t, stages, e) {
		t.Fatal("nesting failure dropped the row")
	}
	assertErr(t, e, jsonParserErr)
	if got := e.errDetailsValue(); got != errJSONNesting.Error() {
		t.Fatalf("details = %q, want %q", got, errJSONNesting.Error())
	}
}

func TestJSONLargeFlatObject(t *testing.T) {
	var b strings.Builder
	b.WriteByte('{')
	for i := range 5000 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `"k%d":%d`, i, i)
	}
	b.WriteByte('}')
	stages := mustStages(t, `{app="api"} | json`)
	e := newTestEntry(b.String(), nil, nil)
	if !mustRun(t, stages, e) {
		t.Fatal("large object dropped the row")
	}
	assertErr(t, e, "")
	if got := len(parsedExceptErrors(e)); got != 5000 {
		t.Fatalf("parsed labels = %d, want 5000", got)
	}
	if e.parsed[0].Name != "k0" {
		t.Fatalf("first label = %q, want k0", e.parsed[0].Name)
	}
}

// deferredCancelContext lets the stage's entry check pass and cancels the next
// time the parser walks a node, proving the mid-parse checks are real.
type deferredCancelContext struct {
	context.Context
	calls int
}

func (c *deferredCancelContext) Err() error {
	c.calls++
	if c.calls > 1 {
		return context.Canceled
	}
	return nil
}

func TestJSONCancellationDuringWalk(t *testing.T) {
	var b strings.Builder
	b.WriteByte('{')
	for i := range 600 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `"k%d":%d`, i, i)
	}
	b.WriteByte('}')
	stages := mustStages(t, `{app="api"} | json`)
	e := newTestEntry(b.String(), nil, nil)
	keep, err := stages[0].process(&deferredCancelContext{Context: t.Context()}, e)
	if keep {
		t.Fatal("canceled parse reported keep=true")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestLogfmtUnquoteEscapes(t *testing.T) {
	tests := []struct {
		name string
		line string
		want map[string]string
	}{
		{name: "control escapes", line: `a="t\tb\bc\fd\re"`, want: map[string]string{"a": "t\tb\bc\fd\re"}},
		{name: "slash and single quote", line: `a="x\/y\'z"`, want: map[string]string{"a": `x/y'z`}},
		{name: "uppercase hex", line: `a="\u00AF"`, want: map[string]string{"a": "\u00af"}},
		{name: "surrogate pair", line: `a="\uD83D\uDE00"`, want: map[string]string{"a": "\U0001F600"}},
		{name: "unpaired surrogate becomes the replacement rune", line: `a="\uD83Dx"`, want: map[string]string{"a": " x"}},
	}
	stages := mustStages(t, `{app="api"} | logfmt`)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newTestEntry(tt.line, nil, nil)
			mustRun(t, stages, e)
			assertParsed(t, e, tt.want)
		})
	}
}

func TestDurationAndBytesComparisonOps(t *testing.T) {
	tests := []struct {
		name  string
		query string
		value string
		keep  bool
	}{
		{name: "duration gte", query: `{app="api"} | d >= 1s`, value: "1s", keep: true},
		{name: "duration lt", query: `{app="api"} | d < 2s`, value: "1s", keep: true},
		{name: "duration lte", query: `{app="api"} | d <= 1s`, value: "1s", keep: true},
		{name: "duration neq", query: `{app="api"} | d != 1s`, value: "1s", keep: false},
		{name: "bytes gte", query: `{app="api"} | b >= 1KB`, value: "1000", keep: true},
		{name: "bytes lt", query: `{app="api"} | b < 1KB`, value: "999", keep: true},
		{name: "bytes lte", query: `{app="api"} | b <= 1KB`, value: "1KB", keep: true},
		{name: "bytes neq", query: `{app="api"} | b != 1KB`, value: "1KB", keep: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name := "d"
			if strings.HasPrefix(tt.name, "bytes") {
				name = "b"
			}
			e := newTestEntry("", labelPairs(name, tt.value), nil)
			if got := mustRun(t, mustStages(t, tt.query), e); got != tt.keep {
				t.Fatalf("keep = %v, want %v", got, tt.keep)
			}
		})
	}
}

func TestJSONFieldsNamedLikeErrorState(t *testing.T) {
	const line = `{"__error__":"user","__error_details__":"detail"}`
	stages := mustStages(t, `{app="api"} | json`)
	e := newTestEntry(line, nil, nil)
	mustRun(t, stages, e)
	// The fields are ordinary parsed labels.
	if v, cat, ok := e.get("__error__"); !ok || v != "user" || cat != parsedLabels {
		t.Fatalf("get(__error__) = %q, %v, %v; want the parsed field", v, cat, ok)
	}
	if v, _, ok := e.get("__error_details__"); !ok || v != "detail" {
		t.Fatalf("get(__error_details__) = %q, %v; want the parsed field", v, ok)
	}
	// The builder error state stays empty.
	assertErr(t, e, "")

	// The string __error__ filter reads the state, not the field.
	e = newTestEntry(line, nil, nil)
	if !mustRun(t, stages, e) {
		t.Fatal("json dropped the row")
	}
	stringStage := mustStages(t, `{app="api"} | __error__ = ""`)
	if !mustRun(t, stringStage, e) {
		t.Fatal("__error__=\"\" should match when the state is empty even with a parsed field of that name")
	}
	// __error_details__ has no special case in labelValue, so Get exposes the
	// parsed field to a string filter.
	detailStage := mustStages(t, `{app="api"} | __error_details__ = "detail"`)
	e = newTestEntry(line, nil, nil)
	mustRun(t, stages, e)
	if !mustRun(t, detailStage, e) {
		t.Fatal("__error_details__ string filter should see the parsed field")
	}
	// A typed filter uses Get, so it sees the parsed field and records a
	// conversion error.
	e = newTestEntry(line, nil, nil)
	mustRun(t, stages, e)
	typed := mustStages(t, `{app="api"} | __error__ > 1`)
	if !mustRun(t, typed, e) {
		t.Fatal("typed filter on a parsed __error__ field should keep the row")
	}
	assertErr(t, e, labelFilterErr)
}

func TestJSONParserErrorStateSeparateFromField(t *testing.T) {
	// Flattening extracts __error__ before the malformed member fails, then
	// the parser failure sets the state. Both survive separately.
	stages := mustStages(t, `{app="api"} | json`)
	e := newTestEntry(`{"__error__":"user","x":}`, nil, nil)
	mustRun(t, stages, e)
	if v, _, ok := e.get("__error__"); !ok || v != "user" {
		t.Fatalf("parsed __error__ field lost: %q, %v", v, ok)
	}
	assertErr(t, e, jsonParserErr)

	// Output cannot carry duplicate names: the state is appended last and
	// replaces the parsed field.
	entry := entryParsed(e.parsed, e.err, e.errDetails)
	if v, ok := entry.Get(errorLabel); !ok || v != jsonParserErr {
		t.Fatalf("output __error__ = %q, %v; want the state", v, ok)
	}
}

func TestJSONConcatenatedRoots(t *testing.T) {
	tests := []struct {
		name        string
		extractions string
		line        string
		want        map[string]string
		wantErr     string
	}{
		{
			name:        "second root",
			extractions: `x="b"`,
			line:        `{"a":1}{"b":2}`,
			want:        map[string]string{"x": "2"},
		},
		{
			name:        "first root wins",
			extractions: `x="a"`,
			line:        `{"a":1} {"a":2}`,
			want:        map[string]string{"x": "1"},
		},
		{
			name:        "array root then object root",
			extractions: `x="a"`,
			line:        `[1]{"a":2}`,
			want:        map[string]string{"x": "2"},
		},
		{
			name:        "trailing garbage after a match",
			extractions: `x="a"`,
			line:        `{"a":1} garbage`,
			want:        map[string]string{"x": "1"},
		},
		{
			name:        "resync after a malformed root",
			extractions: `x="b"`,
			line:        `{"a":}{"b":2}`,
			want:        map[string]string{"x": "2"},
		},
		{
			name:        "matched malformed value errors",
			extractions: `x="a"`,
			line:        `{"a":}{"b":2}`,
			want:        map[string]string{"x": ""},
			wantErr:     jsonParserErr,
		},
		{
			name:        "invalid first byte wins",
			extractions: `x="a"`,
			line:        `garbage {"a":1}`,
			want:        map[string]string{},
			wantErr:     jsonParserErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := mustStages(t, `{app="api"} | json `+tt.extractions)
			e := newTestEntry(tt.line, nil, nil)
			mustRun(t, stages, e)
			assertParsed(t, e, tt.want)
			assertErr(t, e, tt.wantErr)
		})
	}
}

func TestJSONNamedNestedObjectUsesBalanceOnly(t *testing.T) {
	// Flattening walks the nested object, so its corruption is a parse error.
	flat := mustStages(t, `{app="api"} | json`)
	e := newTestEntry(`{"a":{"b":}}`, nil, nil)
	mustRun(t, flat, e)
	assertErr(t, e, jsonParserErr)

	// A named path that returns the object only needs balanced braces (Get).
	named := mustStages(t, `{app="api"} | json x="a"`)
	e = newTestEntry(`{"a":{"b":}}`, nil, nil)
	mustRun(t, named, e)
	assertErr(t, e, "")
	assertParsed(t, e, map[string]string{"x": `{"b":}`})

	// Descending into it hits the malformed value.
	deep := mustStages(t, `{app="api"} | json x="a.b"`)
	e = newTestEntry(`{"a":{"b":}}`, nil, nil)
	mustRun(t, deep, e)
	assertErr(t, e, jsonParserErr)
	assertParsed(t, e, map[string]string{"x": ""})
}

func TestJSONNestedWhitespaceKeysAreSkipped(t *testing.T) {
	stages := mustStages(t, `{app="api"} | json`)
	e := newTestEntry(`{"a":{" ":{"b":1}}}`, nil, nil)
	mustRun(t, stages, e)
	assertParsed(t, e, map[string]string{"a_b": "1"})
}

func TestLogfmtNamedDestinationKeyStillExtracts(t *testing.T) {
	// A decoded key that is itself a destination is exempt from the extracted
	// check, so it overwrites the pre-created empty label even though the
	// expression's source path is different.
	stages := mustStages(t, `{app="api"} | logfmt out="src"`)
	e := newTestEntry(`out=bogus`, nil, nil)
	mustRun(t, stages, e)
	assertParsed(t, e, map[string]string{"out": "bogus"})
}

func TestJSONNamedMalformedString(t *testing.T) {
	// A matched value that is an unterminated string: jsonparser.Get returns
	// MalformedStringError, EachKey forwards it to the callback, and the label
	// is still filled with "" by the post-loop pass.
	const query = `{app="api"} | json x="x"`
	const line = `{"x":"unterminated`
	stages := mustStages(t, query)
	e := newTestEntry(line, nil, nil)
	mustRun(t, stages, e)
	assertParsed(t, e, map[string]string{"x": ""})
	assertErr(t, e, jsonParserErr)
	if got := e.errDetailsValue(); got != errMalformedString.Error() {
		t.Fatalf("details = %q, want %q", got, errMalformedString.Error())
	}

	// The same path through the tail-facing compiled API.
	c, err := testEngine().CompileLogQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	_, entry, keep, err := c.Process(t.Context(), tenant.Default, plan.Row{
		Line: line, Labels: labelPairs("app", "api"),
	})
	if err != nil || !keep {
		t.Fatalf("Process = keep %v, err %v; want kept", keep, err)
	}
	parsed := entry.Parsed.Map()
	if parsed["x"] != "" || parsed[errorLabel] != jsonParserErr {
		t.Fatalf("parsed = %v, want x=\"\" and __error__=%s", parsed, jsonParserErr)
	}
}

func TestJSONNamedDuplicateDestinationDocumentOrder(t *testing.T) {
	tests := []struct {
		name string
		line string
		want string
	}{
		// EachKey invokes callbacks in document order, so the last write wins
		// by position, not by extraction declaration order.
		{name: "b before a", line: `{"b":"2","a":"1"}`, want: "1"},
		{name: "a before b", line: `{"a":"1","b":"2"}`, want: "2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := mustStages(t, `{app="api"} | json x="a", x="b"`)
			e := newTestEntry(tt.line, nil, nil)
			mustRun(t, stages, e)
			assertParsed(t, e, map[string]string{"x": tt.want})
		})
	}
}

func TestJSONNamedBracketLookalikeKeys(t *testing.T) {
	tests := []struct {
		name   string
		extras string
		line   string
		want   map[string]string
	}{
		{
			name:   "bracket key on an empty array",
			extras: `x="[\"[\"]"`,
			line:   `[]`,
			want:   map[string]string{"x": ""},
		},
		{
			name:   "bracket-looking object key",
			extras: `x="[\"[foo\"]"`,
			line:   `{"[foo":"v"}`,
			want:   map[string]string{"x": "v"},
		},
		{
			name:   "quoted numeric key is not an index",
			extras: `x="[\"0\"]"`,
			line:   `{"0":"v"}`,
			want:   map[string]string{"x": "v"},
		},
		{
			name:   "nested bracket key",
			extras: `x="o[\"[\"]"`,
			line:   `{"o":{"[":"v"}}`,
			want:   map[string]string{"x": "v"},
		},
		{
			name:   "root index still works",
			extras: `x="[0]"`,
			line:   `["v"]`,
			want:   map[string]string{"x": "v"},
		},
		{
			name:   "nested array index still works",
			extras: `x="o[1]"`,
			line:   `{"o":[1,2]}`,
			want:   map[string]string{"x": "2"},
		},
		{
			name:   "index past the array end",
			extras: `x="[3]"`,
			line:   `[1,2]`,
			want:   map[string]string{"x": ""},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages := mustStages(t, `{app="api"} | json `+tt.extras)
			e := newTestEntry(tt.line, nil, nil)
			mustRun(t, stages, e)
			assertParsed(t, e, tt.want)
		})
	}
}

func FuzzPipelineJSON(f *testing.F) {
	seeds := []struct{ path, line string }{
		{`a.b`, `{"a":{"b":"1"}}`},
		{`a`, `{"b":"2","a":"1"}`},
		{`["["]`, `[]`},
		{`["[foo"]`, `{"[foo":"v"}`},
		{`[0]`, `[1,2]`},
		{`a[1].b`, `{"a":[{"b":"x"},{"b":"y"}]}`},
		{`x`, `{"x":"unterminated`},
		{`a`, `{"a":[[1,2],[3]]}`},
		{`a..b`, `{"a":1}`},
		{`a`, `{"__error__":5,"x":}`},
	}
	for _, s := range seeds {
		f.Add(s.path, s.line)
	}
	eng := testEngine()
	f.Fuzz(func(t *testing.T, pathExpr, line string) {
		if len(pathExpr) > 256 || len(line) > 4096 {
			t.Skip()
		}
		query := `{app="api"} | json x=` + strconv.Quote(pathExpr)
		c, err := eng.CompileLogQuery(query)
		if err == nil {
			if _, _, _, err := c.Process(t.Context(), tenant.Default, plan.Row{
				Line: line, Labels: labelPairs("app", "api"),
			}); err != nil {
				t.Fatalf("named json Process(%q, %q): %v", pathExpr, line, err)
			}
		} else if !errors.Is(err, plan.ErrBadQuery) && !errors.Is(err, plan.ErrUnsupported) {
			t.Fatalf("CompileLogQuery(%q) = %v", query, err)
		}
		flat, err := eng.CompileLogQuery(`{app="api"} | json`)
		if err != nil {
			t.Fatalf("compile flatten: %v", err)
		}
		if _, _, _, err := flat.Process(t.Context(), tenant.Default, plan.Row{
			Line: line, Labels: labelPairs("app", "api"),
		}); err != nil {
			t.Fatalf("flatten Process(%q): %v", line, err)
		}
	})
}
