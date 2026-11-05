package pipeline

import (
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

// FuzzParseLogfmt: parsing never panics; well-formed input re-parses to the
// same pairs.
func FuzzParseLogfmt(f *testing.F) {
	f.Add("a=1 b=two")
	f.Add(`k="quoted \"x\""`)
	f.Add("crlf\r\nsep\tsep")
	f.Add("no-equals")
	f.Add(`unterminated="`)
	f.Add("=empty-key")
	f.Add("")
	f.Fuzz(func(t *testing.T, s string) {
		pairs, err := parseLogfmt(s)
		if err != nil {
			return
		}
		// Re-parse the rendered form of the same pairs: values without
		// whitespace/quotes must round-trip.
		simple := true
		for _, p := range pairs {
			if strings.ContainsAny(p.Value, " \t\r\n\"\\") {
				simple = false
				break
			}
		}
		if !simple {
			return
		}
		var b strings.Builder
		for _, p := range pairs {
			b.WriteString(p.Key)
			b.WriteByte('=')
			b.WriteString(p.Value)
			b.WriteByte(' ')
		}
		again, err := parseLogfmt(b.String())
		if err != nil {
			t.Fatalf("reparse of %q: %v", b.String(), err)
		}
		if len(again) != len(pairs) {
			t.Fatalf("reparse count %d != %d for %q", len(again), len(pairs), s)
		}
		for i := range again {
			if again[i] != pairs[i] {
				t.Fatalf("reparse pair %d = %+v, want %+v (src %q)", i, again[i], pairs[i], s)
			}
		}
	})
}

// FuzzParseJSONFields: extraction never panics, names stay valid metadata
// names, and the flattened output stays inside the parser's configured
// bounds (nested objects can repeat a prefix over many leaves, so the bound,
// not the raw input size, is what keeps output small).
func FuzzParseJSONFields(f *testing.F) {
	f.Add(`{"a":1}`)
	f.Add(`{"nested":{"z":1,"a":[1,2]}}`)
	f.Add(`{"user.id":{"1id":"x"}}`)
	f.Add(`{"s":"é\""}`)
	f.Add(`{"replacement":"� is valid"}`)
	f.Add(`not json`)
	f.Add(`[]`)
	f.Add(`{"big":12345678901234567890}`)
	const maxTotal, maxValue = 1 << 20, 1 << 16
	f.Fuzz(func(t *testing.T, s string) {
		fields, err := parseJSONFields(t.Context(), s, maxTotal, maxValue)
		if err != nil {
			return
		}
		var total int64
		seen := make(map[string]struct{}, len(fields))
		for _, f := range fields {
			total += int64(len(f.name) + len(f.value))
			if f.name == "" {
				t.Fatalf("empty field name")
			}
			if _, dup := seen[f.name]; dup {
				t.Fatalf("duplicate field %q in ordered output", f.name)
			}
			seen[f.name] = struct{}{}
			if strings.HasPrefix(f.name, "__") {
				t.Fatalf("field %q kept a reserved __ prefix", f.name)
			}
			if err := validLabelNameErr(f.name); err != nil {
				t.Fatalf("field %q is not a valid name: %v", f.name, err)
			}
			// Every value must itself be valid UTF-8: it ends up in
			// metadata that must marshal back to JSON. utf8.ValidString
			// (a literal U+FFFD in the input stays valid).
			if !utf8.ValidString(f.value) {
				t.Fatalf("field %q value not valid UTF-8: %q", f.name, f.value)
			}
		}
		if total > maxTotal {
			t.Fatalf("output %d bytes exceeds parser bound %d", total, maxTotal)
		}
	})
}

// FuzzSanitizeName: sanitizing always yields either an empty string or a
// valid metadata name, and is idempotent.
func FuzzSanitizeName(f *testing.F) {
	f.Add("user.id")
	f.Add("1a")
	f.Add("é")
	f.Add(" a ")
	f.Add("__x")
	f.Fuzz(func(t *testing.T, s string) {
		got := sanitizeName(s)
		if got == "" {
			return
		}
		if err := validLabelNameErr(got); err != nil {
			t.Fatalf("sanitizeName(%q) = %q: %v", s, got, err)
		}
		if again := sanitizeName(got); again != got {
			t.Fatalf("sanitizeName not idempotent: %q -> %q -> %q", s, got, again)
		}
	})
}

// FuzzExpandLen: the redact sizer must equal len(ExpandString) exactly.
func FuzzExpandLen(f *testing.F) {
	f.Add("(a)(b)?", "$1$2$x", "ab")
	f.Add("a", "$é$", "a")
	f.Add("(?P<n>a)", "${n}${bad", "a")
	f.Add("a*", "$0", "")
	f.Add("a", "$$1", "aaa")
	f.Fuzz(func(t *testing.T, pattern, template, src string) {
		re, err := regexp.Compile(pattern)
		if err != nil {
			t.Skip()
		}
		for _, m := range re.FindAllStringSubmatchIndex(src, 10) {
			want := len(re.ExpandString(nil, template, src, m))
			if got := int(expandLen(re, template, src, m)); got != want {
				t.Fatalf("expandLen(%q, %q) = %d, len(ExpandString) = %d", template, src, got, want)
			}
		}
	})
}
