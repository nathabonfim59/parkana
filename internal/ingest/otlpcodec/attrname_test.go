package otlpcodec

import "testing"

// TestLabelName uses the test vectors from
// github.com/prometheus/otlptranslator v1.0.0 label_namer_test.go with the
// default options (UTF8Allowed=false, PreserveMultipleUnderscores=false).
func TestLabelName(t *testing.T) {
	cases := []struct{ name, want string }{
		{"label:with:colons", "label_with_colons"},
		{"LabelWithCapitalLetters", "LabelWithCapitalLetters"},
		{"label!with&special$chars)", "label_with_special_chars_"},
		{"label_with_foreign_characters_字符", "label_with_foreign_characters_"},
		{"label.with.dots", "label_with_dots"},
		{"123label", "key_123label"},
		{"_label_starting_with_underscore", "_label_starting_with_underscore"},
		{"__label_starting_with_2underscores", "_label_starting_with_2underscores"},
		{"label__with__double__underscores", "label_with_double_underscores"},
		{"label.name__with&&special##chars", "label_name_with_special_chars"},
		{"__reserved__label__name__", "__reserved_label_name__"},
		{"trailing_underscores___", "trailing_underscores_"},
		// Extra cases for digits, ASCII-only normalization and reserved names.
		{"9", "key_9"},
		{"__a", "_a"},
		{"a__", "a_"},
		{"a.😀.b", "a_b"},
		{"héllo", "h_llo"},
		{"aé", "a_"},
		{"__a__b__", "__a_b__"},
		{"http.server.duration", "http_server_duration"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := labelName(tc.name)
			if err != nil {
				t.Fatalf("labelName(%q) returned error: %v", tc.name, err)
			}
			if got != tc.want {
				t.Fatalf("labelName(%q) = %q, want %q", tc.name, got, tc.want)
			}
		})
	}
}

func TestLabelNameInvalid(t *testing.T) {
	// Upstream rejects empty names and names that sanitize to only
	// underscores (label_namer.go:66-68, :86-88).
	for _, name := range []string{"", "__", "___", "____", "...", "ようこそ", "!!!", "😀"} {
		t.Run(name, func(t *testing.T) {
			if got, err := labelName(name); err == nil {
				t.Fatalf("labelName(%q) = %q, want an error", name, got)
			}
		})
	}
}
