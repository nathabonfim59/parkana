package otlpcodec

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// labelName normalizes an OTLP attribute name like
// github.com/prometheus/otlptranslator v1.0.0 LabelNamer.Build with the
// options Loki uses: UTF8Allowed=false, UnderscoreLabelSanitization=false,
// PreserveMultipleUnderscores=false (label_namer.go:65-91, strconv.go:28-67).
//
// The rules are:
//   - Replace invalid characters with '_' and collapse consecutive
//     underscores, including underscores already in the name.
//   - Preserve names of the form __x__ as "__" + sanitized(x) + "__".
//   - Prefix a leading digit with "key_".
//   - Reject empty names and names that sanitize to only underscores.
func labelName(name string) (string, error) {
	if name == "" {
		return "", errors.New("label name is empty")
	}

	normalized := sanitizeLabelName(name)
	if unicode.IsDigit(rune(normalized[0])) {
		normalized = "key_" + normalized
	}
	if onlyUnderscores(normalized) {
		return "", fmt.Errorf("normalization for label name %q resulted in invalid name %q", name, normalized)
	}
	return normalized, nil
}

// sanitizeLabelName mirrors the upstream function of the same name.
func sanitizeLabelName(name string) string {
	inner, reserved := reservedLabel(name)

	var b strings.Builder
	b.Grow(len(name) + 2)
	prevUnderscore := false
	for _, r := range inner {
		if isLabelChar(r) {
			b.WriteRune(r)
			prevUnderscore = false
			continue
		}
		if prevUnderscore {
			continue
		}
		b.WriteByte('_')
		prevUnderscore = true
	}

	if reserved {
		return "__" + b.String() + "__"
	}
	return b.String()
}

// reservedLabel reports whether name starts and ends with exactly "__" and
// returns the name without that prefix and suffix.
func reservedLabel(name string) (string, bool) {
	if len(name) < 4 || !strings.HasPrefix(name, "__") || !strings.HasSuffix(name, "__") {
		return name, false
	}
	return name[2 : len(name)-2], true
}

// isLabelChar reports whether r is valid in a classical Prometheus label
// name. Upstream accepts ASCII letters and digits here; underscores are
// handled as invalid so runs of them collapse.
func isLabelChar(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}

func onlyUnderscores(s string) bool {
	for _, r := range s {
		if r != '_' {
			return false
		}
	}
	return true
}
