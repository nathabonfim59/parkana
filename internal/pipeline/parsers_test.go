package pipeline

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"slices"
	"strings"
	"testing"
)

// parseJSON is parseJSONFields bound to the test context.
func parseJSON(t *testing.T, src string, maxTotal, maxValue int64) ([]extractedField, error) {
	t.Helper()
	return parseJSONFields(t.Context(), src, maxTotal, maxValue)
}

// nestedPunct builds {"root":{".":"1","!":"1",...}} where every one-character
// leaf key sanitizes to "_".
func nestedPunct(root string, keys []string) string {
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = `"` + k + `":"1"`
	}
	return `{"` + root + `":{` + strings.Join(parts, ",") + `}}`
}

// fieldNames returns the ordered (name, value) pairs of fields, for readable
// failures.
func fieldNames(fields []extractedField) []string {
	out := make([]string, len(fields))
	for i, f := range fields {
		out[i] = f.name + "=" + f.value
	}
	return out
}

func TestParseJSONFields(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want []extractedField
		err  string
	}{
		{name: "empty object", src: `{}`},
		{
			name: "scalars keep spelling",
			src:  `{"s":"x","i":42,"f":1e3,"neg":-0.5,"t":true,"f2":false,"n":null}`,
			want: []extractedField{{"s", "x"}, {"i", "42"}, {"f", "1e3"}, {"neg", "-0.5"}, {"t", "true"}, {"f2", "false"}, {"n", "null"}},
		},
		{
			name: "nested objects flatten in source order",
			src:  `{"n":{"b":2,"a":1},"arr":[3,1]}`,
			want: []extractedField{{"n_b", "2"}, {"n_a", "1"}, {"arr", `[3,1]`}},
		},
		{
			name: "nested arrays canonicalize but keep raw numbers",
			src:  `{"o":[{"z":12345678901234567890,"a":0.10000000000000000001}]}`,
			want: []extractedField{{"o", `[{"a":0.10000000000000000001,"z":12345678901234567890}]`}},
		},
		{
			name: "raw numbers preserved beyond 2^53",
			src:  `{"big":12345678901234567890,"prec":0.1234567890123456789,"exp":1e300,"neg":-1e-7}`,
			want: []extractedField{
				{"big", "12345678901234567890"},
				{"prec", "0.1234567890123456789"},
				{"exp", "1e300"},
				{"neg", "-1e-7"},
			},
		},
		{
			name: "string escapes resolve",
			src:  `{"s":"a\"b\\c\ndé"}`,
			want: []extractedField{{"s", "a\"b\\c\ndé"}},
		},
		{
			name: "empty value",
			src:  `{ "a" : "" }`,
			want: []extractedField{{"a", ""}},
		},
		{
			name: "dotted hyphen and at-sign keys sanitize",
			src:  `{"user.id":7,"http-status":"200","@timestamp":"t"}`,
			want: []extractedField{{"user_id", "7"}, {"http_status", "200"}, {"_timestamp", "t"}},
		},
		{
			name: "keys trim whitespace",
			src:  `{" a ":"1","\tb":"2"}`,
			want: []extractedField{{"a", "1"}, {"b", "2"}},
		},
		{
			name: "leading digit gets underscore",
			src:  `{"1a":"x"}`,
			want: []extractedField{{"_1a", "x"}},
		},
		{
			name: "non-ASCII runes become one underscore each",
			src:  `{"é":"x","aé":"y"}`,
			want: []extractedField{{"_", "x"}, {"a_", "y"}},
		},
		{
			name: "nested digit child joins without a new prefix underscore",
			src:  `{"user":{"1id":7}}`,
			want: []extractedField{{"user_1id", "7"}},
		},
		{
			name: "blank nested segment joins parent",
			src:  `{"user":{"":1}}`,
			want: []extractedField{{"user", "1"}},
		},
		{
			name: "sanitized collisions keep the later source occurrence",
			src:  `{"user.id":1,"user_id":2}`,
			want: []extractedField{{"user_id", "2"}},
		},
		{
			name: "sanitized collisions are order sensitive",
			src:  `{"user_id":1,"user.id":2}`,
			want: []extractedField{{"user_id", "2"}},
		},
		{
			name: "protected prefix",
			src:  `{"__error__":"x"}`,
			want: []extractedField{{"extracted__error__", "x"}},
		},
		{
			name: "protected collision interleaving keeps last source value",
			src:  `{"__x":"first","extracted__x":"middle","..x":"last"}`,
			want: []extractedField{{"extracted__x", "last"}},
		},
		{
			name: "protected collision reverse order keeps last source value",
			src:  `{"extracted__x":"middle","__x":"first","..x":"last"}`,
			want: []extractedField{{"extracted__x", "last"}},
		},
		{
			name: "protected collision last name wins",
			src:  `{"..x":"last","__x":"first","extracted__x":"middle"}`,
			want: []extractedField{{"extracted__x", "middle"}},
		},
		{name: "duplicate member names rejected", src: `{"a":1,"a":2}`, err: "duplicate"},
		{name: "duplicate nested member names rejected", src: `{"o":{"a":1,"a":2}}`, err: "duplicate"},
		{name: "duplicate member names in array object rejected", src: `{"a":[{"x":1,"x":2}]}`, err: "duplicate"},
		{name: "empty member name", src: `{"":1}`, err: "empty extracted field name"},
		{name: "whitespace-only member name", src: `{"  ":1}`, err: "empty extracted field name"},
		{name: "top-level array", src: `[1,2]`, err: "not a JSON object"},
		{name: "top-level string", src: `"x"`, err: "not a JSON object"},
		{name: "top-level number", src: `42`, err: "not a JSON object"},
		{name: "top-level null", src: `null`, err: "not a JSON object"},
		{name: "empty input", src: ``, err: "not a JSON object"},
		{name: "trailing garbage", src: `{"a":1} x`, err: "not a JSON object"},
		{name: "missing brace", src: `{"a":1`, err: "not a JSON object"},
		{name: "invalid utf8", src: "{\"a\":\"\xff\"}", err: "not a JSON object"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseJSON(t, tc.src, DefaultMetaBytes, DefaultMetaValueBytes)
			if tc.err != "" {
				if err == nil {
					t.Fatalf("parse(%q) = %v, want error %q", tc.src, got, tc.err)
				}
				if !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("parse(%q) error %q does not contain %q", tc.src, err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse(%q): %v", tc.src, err)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("parse(%q) = %v, want %v", tc.src, fieldNames(got), fieldNames(tc.want))
			}
		})
	}
}

// TestParseJSONFieldsBounds: the parser stops at the configured metadata
// bounds before materializing an amplified flattened result.
func TestParseJSONFieldsBounds(t *testing.T) {
	t.Run("value bound", func(t *testing.T) {
		_, err := parseJSON(t, `{"a":"123456"}`, 100, 5)
		if err == nil || !strings.Contains(err.Error(), "value exceeds 5") {
			t.Fatalf("err = %v, want value bound", err)
		}
	})
	t.Run("flattened prefix amplification", func(t *testing.T) {
		prefix := strings.Repeat("p", 20)
		src := `{"` + prefix + `":{"a":"1","b":"2","c":"3"}}`
		// One leaf costs 22 name bytes + 1 value byte; a 30-byte budget
		// admits only the first, so the second trips the bound.
		_, err := parseJSON(t, src, 30, 100)
		if err == nil || !strings.Contains(err.Error(), "would exceed 30 bytes") {
			t.Fatalf("err = %v, want flattened total bound", err)
		}
	})
	t.Run("key growth", func(t *testing.T) {
		_, err := parseJSON(t, `{"aaa":{"bbb":{"ccc":1}}}`, 8, 100)
		if err == nil || !strings.Contains(err.Error(), "flattened key exceeds 8 bytes") {
			t.Fatalf("err = %v, want key bound", err)
		}
	})
	t.Run("collision work budget", func(t *testing.T) {
		// Every one-character raw key sanitizes to "_", so the unique
		// total stays under 64 bytes while each leaf still rebuilds and
		// hashes the 60-byte prefix. The derived work budget is
		// 4*MetaBytes = 256, reached on the fifth leaf.
		prefix := strings.Repeat("p", 60)
		keys := []string{".", "!", "#", "$", "%", "&", "*", "+", "-", "/"}
		_, err := parseJSON(t, nestedPunct(prefix, keys), 64, 100)
		if err == nil || !strings.Contains(err.Error(), "flattened key work exceeds 256 bytes") {
			t.Fatalf("err = %v, want derived work bound", err)
		}
	})
	t.Run("protected collision charges protected name bytes", func(t *testing.T) {
		// The raw root "..ppp..." sanitizes to a __-prefixed prefix, so
		// every leaf's final name gains the 9-byte "extracted" protection.
		// A leaf costs 52 final-name bytes (42 prefix + '_' + '-' + 9);
		// 5 leaves reach the 256-byte work budget while the unique total
		// (53 bytes) stays under 64.
		root := ".." + strings.Repeat("p", 40)
		keys := []string{".", "!", "#", "$", "%"}
		_, err := parseJSON(t, nestedPunct(root, keys), 64, 100)
		if err == nil || !strings.Contains(err.Error(), "flattened key work exceeds 256 bytes") {
			t.Fatalf("err = %v, want protected work bound", err)
		}
	})
	t.Run("long root key rejected before prefix growth", func(t *testing.T) {
		src := `{"` + strings.Repeat("a", 100<<10) + `":1}`
		_, err := parseJSON(t, src, 64, 100)
		if err == nil || !strings.Contains(err.Error(), "flattened key exceeds 64 bytes") {
			t.Fatalf("err = %v, want pre-growth key bound", err)
		}
	})
	t.Run("long nested key rejected before prefix growth", func(t *testing.T) {
		src := `{"a":{"` + strings.Repeat("b", 100<<10) + `":1}}`
		_, err := parseJSON(t, src, 64, 100)
		if err == nil || !strings.Contains(err.Error(), "flattened key exceeds 64 bytes") {
			t.Fatalf("err = %v, want pre-growth nested key bound", err)
		}
	})
	t.Run("protected prefix counted in key bound", func(t *testing.T) {
		// 57 sanitized key bytes pass the growth precheck; the 9-byte
		// protected prefix then pushes the final name past 60.
		src := `{"__` + strings.Repeat("p", 55) + `":1}`
		_, err := parseJSON(t, src, 60, 100)
		if err == nil || !strings.Contains(err.Error(), "flattened key exceeds 60 bytes") {
			t.Fatalf("err = %v, want protected key bound", err)
		}
	})
	t.Run("adversarial nesting is bounded deterministically", func(t *testing.T) {
		const depth = 10001 // above jsontext's 10000 nesting limit
		src := strings.Repeat(`{"a":`, depth) + "1" + strings.Repeat("}", depth)
		for range 3 {
			if _, err := parseJSON(t, src, DefaultMetaBytes, DefaultMetaValueBytes); err == nil {
				t.Fatal("deep nesting: want error")
			}
		}
	})
}

// countingContext wraps a real cancellable context and cancels it on the
// (n+1)th Err call, so a test can cancel exactly between two JSON fields
// while the downstream context stays real (Done closes, Err stays canceled).
type countingContext struct {
	context.Context
	cancel context.CancelFunc
	n      int
}

// newCountingContext returns a child of t.Context that cancels after n
// successful Err observations.
func newCountingContext(t *testing.T, n int) *countingContext {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return &countingContext{Context: ctx, cancel: cancel, n: n}
}

func (c *countingContext) Err() error {
	if err := c.Context.Err(); err != nil {
		return err
	}
	if c.n <= 0 {
		c.cancel()
		return c.Context.Err()
	}
	c.n--
	return nil
}

// TestParseJSONFieldsCancellation: cancellation between members is returned
// as an operation error, never as a data error and never with partial output.
func TestParseJSONFieldsCancellation(t *testing.T) {
	// parseJSONFields checks once before the object, then once per member.
	// Two ticks leave the check before "b" canceled.
	ctx := newCountingContext(t, 2)
	fields, err := parseJSONFields(ctx, `{"a":1,"b":2,"c":3}`, DefaultMetaBytes, DefaultMetaValueBytes)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if fields != nil {
		t.Fatalf("fields = %v, want no partial output", fieldNames(fields))
	}
}

// TestParseJSONFieldsArrayErrors covers the array branch's decode error path:
// the member-name token must have been copied before ReadValue invalidates it,
// so the error stays a clean data error.
func TestParseJSONFieldsArrayErrors(t *testing.T) {
	for _, src := range []string{
		`{"a":[{"x":1,"x":2}]}`,
		`{"a":[1,`,
		`{"a":[01]}`,
	} {
		fields, err := parseJSON(t, src, DefaultMetaBytes, DefaultMetaValueBytes)
		if err == nil {
			t.Fatalf("parseJSONFields(%q) = %v, want error", src, fieldNames(fields))
		}
		if fields != nil {
			t.Fatalf("parseJSONFields(%q) returned partial fields %v", src, fieldNames(fields))
		}
	}
}

// TestRenderJSONContainerRejectsInvalid covers the render error branch itself
// (unreachable from the decoder for well-formed input, but kept as defense).
func TestRenderJSONContainerRejectsInvalid(t *testing.T) {
	for _, raw := range []string{`[{"x":1,"x":2}]`, `[1,`, `["\x"]`} {
		if _, err := renderJSONContainer(jsontext.Value(raw)); err == nil {
			t.Fatalf("renderJSONContainer(%q) succeeded, want error", raw)
		}
	}
}

// TestParseJSONFieldsDeterministic pins source order across runs: Go map
// iteration must never decide which flattened name wins or their order.
func TestParseJSONFieldsDeterministic(t *testing.T) {
	src := `{"z":{"y":2,"x":1},"a":1}`
	want := []extractedField{{"z_y", "2"}, {"z_x", "1"}, {"a", "1"}}
	first, err := parseJSON(t, src, DefaultMetaBytes, DefaultMetaValueBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(first, want) {
		t.Fatalf("fields = %v, want %v", fieldNames(first), fieldNames(want))
	}
	for range 100 {
		again, err := parseJSON(t, src, DefaultMetaBytes, DefaultMetaValueBytes)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(again, want) {
			t.Fatalf("nondeterministic: %v vs %v", fieldNames(again), fieldNames(want))
		}
	}
}

func TestSanitizeName(t *testing.T) {
	tests := []struct{ in, want string }{
		{"user.id", "user_id"},
		{"http-status", "http_status"},
		{"@timestamp", "_timestamp"},
		{" a ", "a"},
		{"1a", "_1a"},
		{"é", "_"},
		{"aé", "a_"},
		{"日本", "__"},
		{"__error__", "__error__"}, // rewritten to extracted__error__ by execExtract
		{"", ""},
		{"   ", ""},
		{"a_b", "a_b"},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			if got := sanitizeName(tc.in); got != tc.want {
				t.Fatalf("sanitizeName(%q) = %q, want %q", tc.in, got, tc.want)
			}
			// Sanitized names are fixed points: sanitizeName(ok) == ok.
			if out := sanitizeName(tc.want); tc.want != "" && out != tc.want {
				t.Fatalf("sanitizeName(%q) = %q, not a fixed point", tc.want, out)
			}
		})
	}
}

func TestAppendSanitizedSegment(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		key    string
		want   string
	}{
		{"plain", "", "user", "user"},
		{"digit first", "", "1id", "_1id"},
		{"digit child", "user", "1id", "user_1id"},
		{"dot", "user", "a.b", "user_a_b"},
		{"blank segment keeps prefix", "user", "  ", "user"},
		{"blank first segment", "", "  ", ""},
		{"digit after blank first", "", "1a", "_1a"},
		{"non-ASCII runes", "user", "aé", "user_a_"},
		{"whitespace trimmed", "", "  x  ", "x"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prefix := []byte(tc.prefix)
			got := string(appendSanitizedSegment(append([]byte(nil), prefix...), tc.key))
			if got != tc.want {
				t.Fatalf("appendSanitizedSegment(%q, %q) = %q, want %q", tc.prefix, tc.key, got, tc.want)
			}
			// The pre-growth length must match what the append produces.
			if n := projectedKeyLen(prefix, tc.key); n != len(got) {
				t.Fatalf("projectedKeyLen(%q, %q) = %d, append produced %d bytes", tc.prefix, tc.key, n, len(got))
			}
		})
	}
}

func TestParseLogfmt(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want []logfmtPair
		err  string
	}{
		{name: "empty", src: "", want: nil},
		{name: "spaces only", src: "  \t ", want: nil},
		{
			name: "unquoted",
			src:  "a=1 b=two",
			want: []logfmtPair{{"a", "1"}, {"b", "two"}},
		},
		{
			name: "quoted with escapes",
			src:  `a="x y" b="he said \"hi\"" c="tab\tnl\nr\rbs\\"`,
			want: []logfmtPair{{"a", "x y"}, {"b", `he said "hi"`}, {"c", "tab\tnl\nr\rbs\\"}},
		},
		{
			name: "empty value",
			src:  "a= b=2",
			want: []logfmtPair{{"a", ""}, {"b", "2"}},
		},
		{
			name: "crlf separators",
			src:  "a=1\rb=2\nc=3\r\n d=4",
			want: []logfmtPair{{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}},
		},
		{
			name: "duplicate keys in order",
			src:  "a=1 a=2",
			want: []logfmtPair{{"a", "1"}, {"a", "2"}},
		},
		{
			name: "value with equals",
			src:  "a=b=c",
			want: []logfmtPair{{"a", "b=c"}},
		},
		{name: "bare word", src: "novalue", err: "no '='"},
		{name: "empty key", src: "=1", err: "empty key"},
		{name: "key no equals", src: "a=1 b", err: "no '='"},
		{name: "unterminated quote", src: `a="x`, err: "unterminated"},
		{name: "trailing backslash", src: `a="x\`, err: "unterminated"},
		{name: "bad escape", src: `a="x\q"`, err: "invalid escape"},
		{name: "no whitespace after quote", src: `a="x"b=2`, err: "whitespace after"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLogfmt(tc.src)
			if tc.err != "" {
				if err == nil {
					t.Fatalf("parse(%q) = %v, want error %q", tc.src, got, tc.err)
				}
				if !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("parse(%q) error %q does not contain %q", tc.src, err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parse(%q): %v", tc.src, err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("parse(%q) = %v, want %v", tc.src, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("pair %d = %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}
