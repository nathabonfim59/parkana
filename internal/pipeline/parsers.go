package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"encoding/json/jsontext"
)

// extractedField is one metadata assignment produced by a parser, in source
// order. Names are sanitized, and parseJSONFields already applies protectName
// to its final names; execExtract applies protectName again (idempotently) and
// merges in this order, so a later occurrence always wins.
type extractedField struct {
	name  string
	value string
}

// extractedPrefix replaces a leading "__" on an extracted name: Parkana
// reserves __-prefixed metadata, so a JSON key "__error__" becomes
// "extracted__error__" instead of being rejected or written reserved. This is
// an extra Parkana protection, not Loki behavior (Loki has no reserved prefix
// and its duplicate suffix is "_extracted").
const extractedPrefix = "extracted"

// protectName rewrites a reserved __-prefixed name to its safe form. It is
// idempotent: an already-protected name starts with "extracted__", not "__".
func protectName(name string) string {
	if strings.HasPrefix(name, "__") {
		return extractedPrefix + name
	}
	return name
}

// sanitizeName maps a flat extracted key to a metadata name the way Loki
// v3.7.9 does (pkg/logql/log/util.go:22-38 sanitizeLabelKey): trim surrounding
// whitespace, prefix '_' when the first byte is an ASCII digit, and replace
// every rune outside [A-Za-z0-9_] with '_' (one '_' per rune, not per byte).
// An empty result means the key has no usable name.
func sanitizeName(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(key) + 1)
	if key[0] >= '0' && key[0] <= '9' {
		b.WriteByte('_')
	}
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// appendSanitizedSegment appends one nested JSON object member name to the
// flattened prefix. Blank (after trimming) segments are skipped; a non-blank
// segment is underscore-joined when the prefix already has a part, and a
// leading ASCII digit gets a '_' prefix only while the buffer is still empty.
// This is Loki v3.7.9's buildSanitizedPrefixFromBuffer (pkg/logql/log/
// parser.go:213-228) over appendSanitized (util.go:42-66), applied
// incrementally so one prefix buffer serves the whole traversal.
func appendSanitizedSegment(dst []byte, key string) []byte {
	key = strings.TrimSpace(key)
	if key == "" {
		return dst
	}
	hasParts := len(dst) > 0
	if !hasParts && key[0] >= '0' && key[0] <= '9' {
		dst = append(dst, '_')
	}
	if hasParts {
		dst = append(dst, '_')
	}
	for _, r := range key {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
			dst = append(dst, byte(r))
		default:
			dst = append(dst, '_')
		}
	}
	return dst
}

// sanitizedSegmentLen returns the sanitized output length of one path segment
// and whether it starts with an ASCII digit (which needs a leading '_' only
// while the prefix is empty). It never allocates: every rune maps to exactly
// one output byte.
func sanitizedSegmentLen(key string) (n int, leadingDigit bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return 0, false
	}
	return utf8.RuneCountInString(key), key[0] >= '0' && key[0] <= '9'
}

// projectedKeyLen returns the flattened key length appendSanitizedSegment
// would produce, without growing the buffer. parseObject checks it against
// maxTotal first, so an oversized raw member name is rejected before any
// prefix allocation or copy.
func projectedKeyLen(prefix []byte, key string) int {
	seg, digit := sanitizedSegmentLen(key)
	if seg == 0 {
		return len(prefix)
	}
	n := len(prefix)
	if n > 0 {
		n++ // underscore separator between path parts
	} else if digit {
		n++ // leading-digit guard while the buffer is empty
	}
	return n + seg
}

// jsonFlattener is the single-pass state of parseJSONFields: one streaming
// jsontext decoder, a prefix buffer reused along the current object path, and
// the ordered output. index lets a repeated flattened name replace its first
// occurrence (later source occurrence wins) while keeping the bounded total
// exact, so duplicate names cannot inflate the accounting.
type jsonFlattener struct {
	ctx      context.Context
	dec      *jsontext.Decoder
	fields   []extractedField
	index    map[string]int
	total    int64
	maxTotal int64
	maxValue int64
	work     int64
	maxWork  int64
}

// flattenWorkFactor derives the cumulative flattened-key work budget from
// MetaBytes: maxWork = 4 * maxTotal (saturating). Every leaf charges the
// length of its final protected name (including the "extracted" prefix) before
// that name is copied and hashed for dedupe. Distinct keys already pay their
// full name bytes through maxTotal, so the 3x headroom exists only for
// repeated collisions/overwrites: many distinct raw names that sanitize or
// protect to the same key can otherwise rehash an enormous prefix over and
// over while the unique total stays small. Exceeding the work budget is a
// data error, so on_error applies and the step rolls back atomically.
const flattenWorkFactor = 4

// parseJSONFields parses src as a JSON object and flattens it into metadata
// fields in source member order, following Loki v3.7.9's recursive
// parseObject (pkg/logql/log/parser.go:103-116) and per-segment sanitizer
// (buildSanitizedPrefixFromBuffer, parser.go:213-228; the flat-name call site
// is parser.go:141). Strings stay strings (unescaped); numbers, booleans and
// null keep their exact JSON spelling; arrays keep their compact canonical
// JSON (RFC 8785 member ordering, raw number spelling); nested objects recurse
// to underscore-joined leaf names, so {"user":{"id":7}} yields user_id=7.
// Names are sanitized per segment (sanitizeName and appendSanitizedSegment).
// Invalid UTF-8, duplicate member names, non-object documents and trailing
// data are errors.
//
// maxTotal caps the names plus values this parse may produce and maxValue
// caps one value. Both are checked before a leaf is stored: a small nested
// prefix repeated over many leaves therefore fails at the bound instead of
// materializing an amplified result. ctx is checked between members and
// before descending into a nested object; its error is returned unwrapped so
// callers can distinguish cancellation from a data error.
func parseJSONFields(ctx context.Context, src string, maxTotal, maxValue int64) ([]extractedField, error) {
	f := &jsonFlattener{
		ctx:      ctx,
		dec:      jsontext.NewDecoder(strings.NewReader(src)),
		index:    make(map[string]int),
		maxTotal: maxTotal,
		maxValue: maxValue,
		maxWork:  satMul(maxTotal, flattenWorkFactor),
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tok, err := f.dec.ReadToken()
	if err != nil {
		return nil, fmt.Errorf("not a JSON object: %v", err)
	}
	if tok.Kind() != jsontext.KindBeginObject {
		return nil, fmt.Errorf("not a JSON object: %s", tok.Kind())
	}
	if err := f.parseObject(nil); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := f.dec.ReadToken(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("not a JSON object: trailing data")
		}
		return nil, fmt.Errorf("not a JSON object: %v", err)
	}
	return f.fields, nil
}

// parseObject consumes one object, starting after its '{' and including its
// '}', flattening every member under prefix.
func (f *jsonFlattener) parseObject(prefix []byte) error {
	for {
		if err := f.ctx.Err(); err != nil {
			return err
		}
		tok, err := f.dec.ReadToken()
		if err != nil {
			return fmt.Errorf("not a JSON object: %v", err)
		}
		if tok.Kind() == jsontext.KindEndObject {
			return nil
		}
		if tok.Kind() != jsontext.KindString {
			return fmt.Errorf("not a JSON object: %v", jsontext.ErrNonStringName)
		}
		// Decoder tokens are only valid until the next decoder call
		// (jsontext decode.go:466-468): copy the member name before
		// PeekKind/ReadValue can invalidate it.
		member := tok.String()
		if int64(projectedKeyLen(prefix, member)) > f.maxTotal {
			return fmt.Errorf("flattened key exceeds %d bytes", f.maxTotal)
		}
		key := appendSanitizedSegment(prefix, member)
		switch f.dec.PeekKind() {
		case jsontext.KindBeginObject:
			if err := f.ctx.Err(); err != nil {
				return err
			}
			if _, err := f.dec.ReadToken(); err != nil {
				return fmt.Errorf("not a JSON object: %v", err)
			}
			if err := f.parseObject(key); err != nil {
				return err
			}
		case jsontext.KindBeginArray:
			v, err := f.dec.ReadValue()
			if err != nil {
				return fmt.Errorf("not a JSON object: %v", err)
			}
			s, err := renderJSONContainer(v)
			if err != nil {
				return fmt.Errorf("member %q: %w", member, err)
			}
			if err := f.add(key, s); err != nil {
				return err
			}
		default:
			tok, err := f.dec.ReadToken()
			if err != nil {
				return fmt.Errorf("not a JSON object: %v", err)
			}
			s, ok := jsonTokenValue(tok)
			if !ok {
				return fmt.Errorf("not a JSON object: invalid value")
			}
			if err := f.add(key, s); err != nil {
				return err
			}
		}
	}
}

// add stores one flattened leaf, replacing an earlier occurrence of the same
// final name. The reserved "__" rewrite happens before indexing, so dedupe and
// later-source-wins see the name that will actually be written (for example
// "__x", "extracted__x" and "..x" all resolve to extracted__x and keep source
// order). The protected name's bytes are part of the key, work and total
// bounds, and all three are charged before the key is copied into a string or
// hashed, so an over-budget leaf is rejected without growing or rehashing the
// output.
func (f *jsonFlattener) add(key []byte, value string) error {
	if len(key) == 0 {
		return fmt.Errorf("empty extracted field name")
	}
	if int64(len(value)) > f.maxValue {
		return fmt.Errorf("extracted field value exceeds %d bytes", f.maxValue)
	}
	protected := len(key) >= 2 && key[0] == '_' && key[1] == '_'
	finalLen := len(key)
	if protected {
		finalLen += len(extractedPrefix)
	}
	if int64(finalLen) > f.maxTotal {
		return fmt.Errorf("flattened key exceeds %d bytes", f.maxTotal)
	}
	f.work += int64(finalLen)
	if f.work > f.maxWork {
		return fmt.Errorf("flattened key work exceeds %d bytes", f.maxWork)
	}
	var name string
	if protected {
		name = extractedPrefix + string(key)
	} else {
		name = string(key)
	}
	// A repeated final name reuses the one stored copy; new names keep the
	// string built above.
	i, seen := f.index[name]
	size := int64(len(name) + len(value))
	var prev int64
	if seen {
		prev = int64(len(f.fields[i].name) + len(f.fields[i].value))
	}
	if f.total-prev+size > f.maxTotal {
		return fmt.Errorf("flattened metadata would exceed %d bytes", f.maxTotal)
	}
	if seen {
		f.fields[i].value = value
		f.total += size - prev
		return nil
	}
	f.index[name] = len(f.fields)
	f.fields = append(f.fields, extractedField{name: name, value: value})
	f.total += size
	return nil
}

// jsonTokenValue renders a scalar JSON token as its metadata string: strings
// are unescaped, other literals keep their exact JSON spelling (1e3 stays 1e3,
// null stays null).
func jsonTokenValue(tok jsontext.Token) (string, bool) {
	switch tok.Kind() {
	case jsontext.KindString:
		return tok.String(), true
	case jsontext.KindNumber, jsontext.KindTrue, jsontext.KindFalse, jsontext.KindNull:
		return tok.String(), true
	}
	return "", false
}

// renderJSONContainer renders a JSON array as compact canonical JSON with raw
// number spelling preserved (JCS rounding would reword integers beyond 2^53
// and high-precision floats).
func renderJSONContainer(v jsontext.Value) (string, error) {
	c := v.Clone()
	if err := c.Canonicalize(jsontext.CanonicalizeRawInts(false), jsontext.CanonicalizeRawFloats(false)); err != nil {
		return "", fmt.Errorf("invalid nested value: %v", err)
	}
	return c.String(), nil
}

// logfmtPair is one key=value pair of a logfmt line.
type logfmtPair struct {
	Key, Value string
}

// parseLogfmt parses src as logfmt: whitespace-separated key=value pairs
// where the value is unquoted (until whitespace) or double-quoted with \",
// \\, \n, \t and \r escapes. Whitespace is space, tab, CR and LF, and a
// closing quote must be followed by whitespace or end of input. Keys are
// syntax-checked only ([^ \t\r\n=]+, the metadata name rule is enforced by
// the caller); duplicate keys are preserved in order and the caller's merge
// lets later values win. Malformed input — a bare word, an empty key, an
// unterminated quote or an unknown escape — is an explicit error, never a
// silent drop.
func parseLogfmt(src string) ([]logfmtPair, error) {
	var pairs []logfmtPair
	i := 0
	skipSpace := func() {
		for i < len(src) && isLogfmtSpace(src[i]) {
			i++
		}
	}
	skipSpace()
	for i < len(src) {
		start := i
		for i < len(src) && !isLogfmtSpace(src[i]) && src[i] != '=' {
			i++
		}
		key := src[start:i]
		if key == "" {
			return nil, fmt.Errorf("empty key at offset %d", start)
		}
		if i >= len(src) || src[i] != '=' {
			return nil, fmt.Errorf("key %q has no '='", key)
		}
		i++ // '='
		value, quoted, err := parseLogfmtValue(src, key, &i)
		if err != nil {
			return nil, err
		}
		if quoted && i < len(src) && !isLogfmtSpace(src[i]) {
			return nil, fmt.Errorf("no whitespace after quoted value of key %q", key)
		}
		pairs = append(pairs, logfmtPair{Key: key, Value: value})
		skipSpace()
	}
	return pairs, nil
}

// isLogfmtSpace reports whether c separates logfmt pairs.
func isLogfmtSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}

// parseLogfmtValue reads one value at *i, quoted or unquoted; quoted says
// which syntax was found.
func parseLogfmtValue(src, key string, i *int) (string, bool, error) {
	if *i < len(src) && src[*i] == '"' {
		*i++
		var b strings.Builder
		for *i < len(src) {
			c := src[*i]
			if c == '"' {
				*i++
				return b.String(), true, nil
			}
			if c == '\\' {
				*i++
				if *i >= len(src) {
					return "", false, fmt.Errorf("unterminated escape in value of key %q", key)
				}
				switch src[*i] {
				case '"':
					b.WriteByte('"')
				case '\\':
					b.WriteByte('\\')
				case 'n':
					b.WriteByte('\n')
				case 't':
					b.WriteByte('\t')
				case 'r':
					b.WriteByte('\r')
				default:
					return "", false, fmt.Errorf("invalid escape \\%c in value of key %q", src[*i], key)
				}
				*i++
				continue
			}
			b.WriteByte(c)
			*i++
		}
		return "", false, fmt.Errorf("unterminated quoted value for key %q", key)
	}
	start := *i
	for *i < len(src) && !isLogfmtSpace(src[*i]) {
		*i++
	}
	return src[start:*i], false, nil
}
