// Pipeline parsers: json, logfmt, regexp and pattern. Each compiled stage is
// immutable and safe for concurrent Process calls; all mutable state is the
// per-call *logEntry.
//
// The behaviour is ported from the pinned Loki 3.7.9 sources in
// pkg/logql/log/parser.go and the dependencies pinned by its go.mod:
// grafana/jsonparser v0.0.0-20241004153430 (JSON object walking, path
// extraction, escapes, error strings), Loki's logfmt decoder
// (pkg/logql/log/logfmt, a fork of go-logfmt) and pkg/logql/log/pattern.
// Parser failures are data: they set __error__/__error_details__ and keep the
// row. Only context cancellation is a Go error.
//
// Matched pinned behaviour: named paths scan every concatenated top-level
// value (first match in document order, including after a malformed value);
// callbacks and therefore duplicate-destination writes run in document order,
// not declaration order; a quoted bracket key such as "[" is always a literal
// object key, never an index; an object value returned by a named path only
// needs balanced braces (Get's
// blockEnd), while flattening walks its members and reports internal
// corruption (ObjectEach); a malformed array stops yielding elements
// (EachKey ignores ArrayEach's error); a bad escape is fatal in a key but
// yields "" in a value; a missing named path is silent while a malformed
// matched value sets JSONParserErr; duplicate keys are first-wins through the
// extracted record; invalid UTF-8 becomes spaces; trailing commas, key-level
// checks and detail strings are jsonparser's.
//
// Why a scanner instead of encoding/json/v2 or jsontext (the json_v2
// guideline was checked with explain): jsontext does allow duplicate names,
// invalid UTF-8 and raw value reads (AllowDuplicateNames, AllowInvalidUTF8,
// ReadValue), so the exception is not about missing decoder options. It is
// about the pinned parser's permissive, stateful grammar and output: EachKey
// walks raw bytes across concatenated and malformed top-level values, Get
// reports errors only for the matched value, ObjectEach aborts mid-object but
// keeps the labels already extracted, and __error_details__ carries
// jsonparser's exact message text. Reproducing those rules on jsontext would
// place this same scan on top of its token stream, so the exception to the
// json_v2 guideline rests on matching the pinned parser's malformed-input
// behaviour, not on jsontext lacking the options.
//
// Remaining differences from jsonparser: object recursion is capped at
// maxJSONNesting; concatenated-root resynchronisation finds the next
// brace/bracket rather than tracking EachKey's exact byte levels; a quoted
// bracket-looking key such as "[" or "[foo" is a literal object key here, with
// no accidental array-index coercion, whereas EachKey's array branch uses a
// string-prefix heuristic and ignores Atoi errors around its path elements; an
// extraction whose sanitized nested name is empty is skipped; and logs.Labels
// cannot hold the duplicate names Loki emits for a parsed field called
// __error__/__error_details__, so the builder error state appended at output
// wins (see entryParsed).
package logql

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// Loki's parser error kinds (pkg/logql/log/error.go).
const (
	jsonParserErr   = "JSONParserErr"
	logfmtParserErr = "LogfmtParserErr"
	labelFilterErr  = "LabelFilterErr"
)

// JSON scanner failures, copied from grafana/jsonparser so the details label
// carries the same text Loki produces.
// The message text is the __error_details__ value Loki's jsonparser produces,
// so it stays capitalized.
var (
	//lint:ignore ST1005 mirrors grafana/jsonparser MalformedJsonError
	errMalformedJSON = errors.New("Malformed JSON error")
	//lint:ignore ST1005 mirrors grafana/jsonparser MalformedStringError
	errMalformedString = errors.New("Value is string, but can't find closing '\"' symbol")
	//lint:ignore ST1005 mirrors grafana/jsonparser MalformedArrayError
	errMalformedArray = errors.New("Value is array, but can't find closing ']' symbol")
	//lint:ignore ST1005 mirrors grafana/jsonparser MalformedObjectError
	errMalformedObject = errors.New("Value looks like object, but can't find closing '}' symbol")
	//lint:ignore ST1005 mirrors grafana/jsonparser MalformedStringEscapeError
	errMalformedStringEscape = errors.New("Encountered an invalid escape sequence in a string")
	//lint:ignore ST1005 mirrors grafana/jsonparser UnknownValueTypeError
	errUnknownValueType = errors.New("Unknown value type")
)

// sanitizeLabelKey is Loki's sanitizeLabelKey(key, true): trim spaces, prefix a
// leading digit with '_', and replace every non [A-Za-z0-9_] rune with '_'.
func sanitizeLabelKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	if key[0] >= '0' && key[0] <= '9' {
		key = "_" + key
	}
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == '_' || r >= '0' && r <= '9' {
			return r
		}
		return '_'
	}, key)
}

// appendSanitized is Loki's appendSanitized: append key to dst with the same
// sanitization, adding the leading '_' only when dst is still empty.
func appendSanitized(dst []byte, key string) []byte {
	key = strings.TrimSpace(key)
	if key == "" {
		return dst
	}
	if len(dst) == 0 && key[0] >= '0' && key[0] <= '9' {
		dst = append(dst, '_')
	}
	for _, r := range key {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == '_' || r >= '0' && r <= '9' {
			dst = append(dst, byte(r))
			continue
		}
		dst = append(dst, '_')
	}
	return dst
}

// setExtractedLabel runs the default extraction path shared by the flattening
// JSON parser, regexp, pattern and unnamed logfmt: collision suffix first, then
// skip a name an earlier stage already extracted, then write.
func (e *logEntry) setExtractedLabel(name, value string) {
	if name == "" {
		return
	}
	key := e.parserKey(name)
	if e.alreadyExtracted(key) {
		return
	}
	e.setParsed(key, value)
}

// replaceInvalidRune reproduces removeInvalidUtf: every utf8.RuneError rune
// (including one decoded from an invalid byte) becomes a space.
func replaceInvalidRune(s string) string {
	if !strings.ContainsRune(s, utf8.RuneError) {
		return s
	}
	return strings.Map(func(r rune) rune {
		if r == utf8.RuneError {
			return ' '
		}
		return r
	}, s)
}

// --- JSON ------------------------------------------------------------------

type jsonKind uint8

const (
	jsonInvalid jsonKind = iota
	jsonString
	jsonNumber
	jsonBool
	jsonNull
	jsonObject
	jsonArray
)

// jsonValue is one parsed JSON value. Arrays keep only their raw span because
// the flattening parser ignores them and named paths index into them lazily.
type jsonValue struct {
	kind jsonKind
	raw  string // exact source text of the value
	str  string // decoded content, jsonString only
	obj  []jsonMember
	err  error // malformed value, reported when the value is consumed
}

type jsonMember struct {
	key   string
	value jsonValue
}

// jsonScanner walks one JSON value. ctx is optional (unit tests may leave it
// nil); every 256 parsed nodes it checks cancellation so a huge malformed line
// cannot postpone a canceled request until the walk finishes.
type jsonScanner struct {
	src      string
	ctx      context.Context
	nodes    int
	canceled error
}

// maxJSONNesting bounds object recursion. The pinned jsonparser recurses
// through ObjectEach with no bound; a hostile line must produce a parse error
// instead of exhausting the goroutine stack.
const maxJSONNesting = 1000

// errJSONNesting is a parse failure, not a Go error from the pipeline.
var errJSONNesting = errors.New("JSON nesting limit exceeded")

// step counts parsed nodes and periodically observes context cancellation.
func (s *jsonScanner) step() error {
	s.nodes++
	if s.ctx == nil || s.nodes%256 != 0 {
		return nil
	}
	if err := s.ctx.Err(); err != nil {
		s.canceled = err
		return err
	}
	return nil
}

func isJSONSpace(c byte) bool {
	return c == ' ' || c == '\n' || c == '\r' || c == '\t'
}

func (s *jsonScanner) skipSpace(i int) int {
	for i < len(s.src) && isJSONSpace(s.src[i]) {
		i++
	}
	return i
}

// tokenEnd mirrors jsonparser.tokenEnd: the end of a bare number/bool/null.
func (s *jsonScanner) tokenEnd(i int) int {
	for ; i < len(s.src); i++ {
		switch s.src[i] {
		case ' ', '\n', '\r', '\t', ',', '}', ']':
			return i
		}
	}
	return len(s.src)
}

// scanString returns the raw content between the quotes at i and the index
// after the closing quote.
func (s *jsonScanner) scanString(i int) (raw string, next int, err error) {
	j := i + 1
	start := j
	for j < len(s.src) {
		switch s.src[j] {
		case '\\':
			j += 2
		case '"':
			return s.src[start:j], j + 1, nil
		default:
			j++
		}
	}
	return "", len(s.src), errMalformedString
}

// blockEnd returns the index just past the object or array starting at i, or
// -1 when unbalanced, mirroring jsonparser.blockEnd.
func (s *jsonScanner) blockEnd(i int) int {
	open := s.src[i]
	closeSym := byte(']')
	if open == '{' {
		closeSym = '}'
	}
	level := 0
	for ; i < len(s.src); i++ {
		switch s.src[i] {
		case '"':
			_, next, err := s.scanString(i)
			if err != nil {
				return -1
			}
			i = next - 1
		case open:
			level++
		case closeSym:
			level--
			if level == 0 {
				return i + 1
			}
		}
	}
	return -1
}

// parseValue parses one JSON value at i. A malformed value comes back as a
// node carrying err with kind jsonInvalid, so callers can decide whether the
// value is consumed (named path) or aborts the walk (flatten).
func (s *jsonScanner) parseValue(i int, depth int) (jsonValue, int) {
	if err := s.step(); err != nil {
		return jsonValue{err: err}, i
	}
	if depth > maxJSONNesting {
		return jsonValue{err: errJSONNesting}, i
	}
	i = s.skipSpace(i)
	if i >= len(s.src) {
		return jsonValue{err: errMalformedJSON}, i
	}
	switch s.src[i] {
	case '"':
		raw, next, err := s.scanString(i)
		if err != nil {
			return jsonValue{kind: jsonString, err: err}, next
		}
		return jsonValue{kind: jsonString, raw: s.src[i:next], str: unescapeJSONString(raw)}, next
	case '{':
		return s.parseObject(i, depth)
	case '[':
		end := s.blockEnd(i)
		if end == -1 {
			return jsonValue{kind: jsonArray, raw: s.src[i:], err: errMalformedArray}, len(s.src)
		}
		return jsonValue{kind: jsonArray, raw: s.src[i:end]}, end
	}
	end := s.tokenEnd(i)
	val := s.src[i:end]
	switch c := s.src[i]; {
	case c == 't' || c == 'f':
		if val == "true" || val == "false" {
			return jsonValue{kind: jsonBool, raw: val}, end
		}
	case c == 'u' || c == 'n':
		if val == "null" {
			return jsonValue{kind: jsonNull, raw: val}, end
		}
	case c == '-' || c >= '0' && c <= '9':
		return jsonValue{kind: jsonNumber, raw: val}, end
	}
	return jsonValue{err: errUnknownValueType}, end
}

// parseObject walks an object the way jsonparser.ObjectEach does: keys are
// unescaped (a bad escape is fatal), values use Get's rules, and structural
// errors abort after the members parsed so far.
func (s *jsonScanner) parseObject(i int, depth int) (jsonValue, int) {
	if err := s.step(); err != nil {
		return jsonValue{err: err}, i
	}
	if depth > maxJSONNesting {
		return jsonValue{err: errJSONNesting}, i
	}
	start := i
	v := jsonValue{kind: jsonObject, raw: s.src[start:]}
	i++
	i = s.skipSpace(i)
	if i >= len(s.src) {
		v.err = errMalformedJSON
		return v, i
	}
	if s.src[i] == '}' {
		v.raw = s.src[start : i+1]
		return v, i + 1
	}
	for {
		if i >= len(s.src) {
			v.err = errMalformedObject
			return v, i
		}
		if s.src[i] == '}' {
			// A trailing comma is accepted: ObjectEach restarts at '}'.
			v.raw = s.src[start : i+1]
			return v, i + 1
		}
		if s.src[i] != '"' {
			v.err = errMalformedObject
			return v, i
		}
		keyRaw, next, err := s.scanString(i)
		if err != nil {
			v.err = errMalformedJSON
			return v, next
		}
		key, kerr := unescapeJSON(keyRaw)
		if kerr != nil {
			v.err = errMalformedStringEscape
			return v, next
		}
		i = s.skipSpace(next)
		if i >= len(s.src) {
			v.err = errMalformedJSON
			return v, i
		}
		if s.src[i] != ':' {
			v.err = errMalformedJSON
			return v, i
		}
		val, next := s.parseValue(i+1, depth+1)
		v.obj = append(v.obj, jsonMember{key: key, value: val})
		if val.err != nil {
			v.err = val.err
			return v, next
		}
		i = s.skipSpace(next)
		if i >= len(s.src) {
			v.err = errMalformedArray
			return v, i
		}
		switch s.src[i] {
		case '}':
			v.raw = s.src[start : i+1]
			return v, i + 1
		case ',':
			i = s.skipSpace(i + 1)
			if i >= len(s.src) {
				v.err = errMalformedArray
				return v, i
			}
		default:
			v.err = errMalformedObject
			return v, i
		}
	}
}

// flattenJSON walks a parsed document like JSONParser.parseObject: strings,
// numbers and booleans become labels, nulls and arrays are ignored, and nested
// objects contribute a "_"-joined path. Labels parsed before an error stay
// set, exactly as the incremental Loki walk leaves them.
func (s *jsonScanner) flattenJSON(e *logEntry, v *jsonValue, prefix []string) error {
	if v.kind != jsonObject {
		return v.err
	}
	for _, m := range v.obj {
		if err := s.step(); err != nil {
			return err
		}
		parts := append(prefix[:len(prefix):len(prefix)], m.key)
		name := ""
		if len(prefix) == 0 {
			name = sanitizeLabelKey(m.key)
		} else {
			var b []byte
			for _, part := range parts {
				if strings.TrimSpace(part) == "" {
					continue
				}
				if len(b) > 0 {
					b = append(b, '_')
				}
				b = appendSanitized(b, part)
			}
			name = string(b)
		}
		if m.value.kind == jsonObject {
			if err := s.flattenJSON(e, &m.value, parts); err != nil {
				return err
			}
			continue
		}
		if m.value.err != nil {
			return m.value.err
		}
		switch m.value.kind {
		case jsonString:
			e.setExtractedLabel(name, m.value.str)
		case jsonNumber:
			e.setExtractedLabel(name, m.value.raw)
		case jsonBool:
			e.setExtractedLabel(name, m.value.raw)
		case jsonNull, jsonArray:
			// Loki's flattening parser ignores nulls and arrays.
		}
	}
	// Members parsed before a structural error keep their labels; the error is
	// reported afterwards, like the incremental ObjectEach walk.
	return v.err
}

// jsonPathElem is one step of a parsed JSON extraction path: either a literal
// object key (isIndex false) or an array index. Keeping the kind explicit means
// a quoted key such as "[" or "[foo" is never mistaken for an index.
type jsonPathElem struct {
	key     string
	index   int
	isIndex bool
}

// jsonExtraction is one name="path" pair of a | json stage.
type jsonExtraction struct {
	name string
	path []jsonPathElem
}

// jsonPending is one extraction still walking down a document.
type jsonPending struct {
	index int
	path  []jsonPathElem
}

type jsonParserStage struct {
	extractions []jsonExtraction // empty for the flattening form
}

func compileJSONParser(p *JSONParser) (pipelineStage, error) {
	s := &jsonParserStage{}
	for _, ex := range p.Extractions {
		path, err := parseJSONPath(ex.Path)
		if err != nil {
			return nil, fmt.Errorf("logql: %w: json extraction %s=%q: %w", plan.ErrBadQuery, ex.Name, ex.Path, err)
		}
		s.extractions = append(s.extractions, jsonExtraction{name: ex.Name, path: path})
	}
	return s, nil
}

func (s *jsonParserStage) process(ctx context.Context, e *logEntry) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	if len(s.extractions) == 0 {
		if err := s.processFlatten(ctx, e); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := s.processNamed(ctx, e); err != nil {
		return false, err
	}
	return true, nil
}

func (s *jsonParserStage) processFlatten(ctx context.Context, e *logEntry) error {
	sc := &jsonScanner{src: e.line, ctx: ctx}
	i := sc.skipSpace(0)
	if i >= len(e.line) || e.line[i] != '{' {
		e.setParseError(jsonParserErr, errMalformedObject)
		return nil
	}
	root, _ := sc.parseObject(i, 0)
	err := sc.flattenJSON(e, &root, nil)
	if sc.canceled != nil {
		return fmt.Errorf("logql: json: %w", sc.canceled)
	}
	if err != nil {
		e.setParseError(jsonParserErr, err)
	}
	return nil
}

func (s *jsonParserStage) processNamed(ctx context.Context, e *logEntry) error {
	// JSONExpressionParser.Process: empty lines are ignored, and a line that
	// does not start with a JSON value sets the error kind without details.
	if e.line == "" {
		return nil
	}
	switch e.line[0] {
	case '"', '{', '[':
	default:
		e.setParseError(jsonParserErr, nil)
		return nil
	}
	sc := &jsonScanner{src: e.line, ctx: ctx}
	roots := sc.parseRoots()
	matched := make([]bool, len(s.extractions))
	pending := make([]jsonPending, len(s.extractions))
	for i := range s.extractions {
		pending[i] = jsonPending{index: i, path: s.extractions[i].path}
	}
	// EachKey invokes its callbacks in document order, not extraction
	// declaration order, so duplicate destinations are resolved by position.
	apply := func(index int, node *jsonValue) {
		ex := s.extractions[index]
		key := ex.name
		if e.baseHas(key) || e.hasMeta(key) {
			key += duplicateSuffix
		}
		var val string
		switch node.kind {
		case jsonObject:
			// Get only requires balanced braces for an object value: internal
			// corruption is not its error, so return the raw text.
			end := jsonObjectEnd(node.raw)
			if end == -1 {
				e.setParseError(jsonParserErr, errMalformedObject)
				return
			}
			val = node.raw[:end]
		case jsonNull:
			val = ""
		case jsonString:
			if node.err != nil {
				e.setParseError(jsonParserErr, node.err)
				return
			}
			val = node.str
		default:
			if node.err != nil {
				e.setParseError(jsonParserErr, node.err)
				return
			}
			val = unescapeJSONString(node.raw)
		}
		// The named JSON parser deliberately overwrites: it does not consult
		// the extracted record.
		e.setParsed(key, val)
	}
	for i := range roots {
		if sc.canceled != nil {
			break
		}
		jsonWalk(sc, &roots[i], pending, matched, 0, apply)
	}
	// Loki fills a label for every extraction that produced no value, looking
	// the original destination name up after the collision suffix was applied.
	for _, ex := range s.extractions {
		if _, _, ok := e.get(ex.name); !ok {
			e.setParsed(ex.name, "")
		}
	}
	if sc.canceled != nil {
		return fmt.Errorf("logql: json: %w", sc.canceled)
	}
	return nil
}

// parseRoots parses every concatenated top-level JSON value on the line, which
// is the shape EachKey's linear scan matches against. A malformed value is
// appended and the scan resynchronises on the next brace or bracket, the way
// EachKey keeps walking raw bytes; a remainder with no opener ends the scan.
func (s *jsonScanner) parseRoots() []jsonValue {
	var roots []jsonValue
	i := s.skipSpace(0)
	for i < len(s.src) {
		v, next := s.parseValue(i, 0)
		roots = append(roots, v)
		if next <= i {
			break
		}
		i = s.skipSpace(next)
		if v.err == nil {
			continue
		}
		// Resynchronise on the next value opener: EachKey keeps scanning the
		// raw bytes after a malformed value instead of stopping.
		resync := -1
		for k := next; k < len(s.src); k++ {
			if s.src[k] == '{' || s.src[k] == '[' {
				resync = k
				break
			}
		}
		if resync == -1 {
			break
		}
		i = resync
	}
	return roots
}

// jsonObjectEnd returns the index just past the balanced object starting at
// raw[0], or -1. It checks only brace balance, like jsonparser.blockEnd.
func jsonObjectEnd(raw string) int {
	if raw == "" || raw[0] != '{' {
		return -1
	}
	sc := &jsonScanner{src: raw}
	return sc.blockEnd(0)
}

// jsonWalk walks one parsed document in source order and applies every
// extraction whose path is satisfied, mirroring EachKey's callback order. Each
// path fires at most once (its first match).
func jsonWalk(sc *jsonScanner, node *jsonValue, pending []jsonPending, matched []bool, depth int, apply func(int, *jsonValue)) {
	if len(pending) == 0 || depth > maxJSONNesting || sc.canceled != nil {
		return
	}
	if err := sc.step(); err != nil {
		return
	}
	switch node.kind {
	case jsonObject:
		for mi := range node.obj {
			m := &node.obj[mi]
			var next []jsonPending
			for _, p := range pending {
				if matched[p.index] || p.path[0].isIndex || p.path[0].key != m.key {
					continue
				}
				if len(p.path) == 1 {
					matched[p.index] = true
					apply(p.index, &m.value)
					continue
				}
				next = append(next, jsonPending{index: p.index, path: p.path[1:]})
			}
			if len(next) > 0 {
				jsonWalk(sc, &m.value, next, matched, depth+1, apply)
			}
			if sc.canceled != nil {
				return
			}
		}
	case jsonArray:
		jsonWalkArray(sc, node.raw, pending, matched, depth+1, apply)
	}
}

// jsonWalkArray yields the array's elements in order. EachKey ignores
// ArrayEach's error, so a malformed element ends the walk silently.
func jsonWalkArray(sc *jsonScanner, raw string, pending []jsonPending, matched []bool, depth int, apply func(int, *jsonValue)) {
	wanted := false
	for _, p := range pending {
		if !matched[p.index] && p.path[0].isIndex {
			wanted = true
			break
		}
	}
	if !wanted {
		return
	}
	inner := &jsonScanner{src: raw, ctx: sc.ctx}
	i := inner.skipSpace(0)
	if i >= len(raw) || raw[i] != '[' {
		return
	}
	i++
	idx := 0
	for {
		i = inner.skipSpace(i)
		if i >= len(raw) || raw[i] == ']' {
			return
		}
		node, next := inner.parseValue(i, depth)
		if inner.canceled != nil {
			sc.canceled = inner.canceled
			return
		}
		if node.err != nil {
			return
		}
		var nextPending []jsonPending
		for _, p := range pending {
			if matched[p.index] || !p.path[0].isIndex || p.path[0].index != idx {
				continue
			}
			if len(p.path) == 1 {
				matched[p.index] = true
				apply(p.index, &node)
				continue
			}
			nextPending = append(nextPending, jsonPending{index: p.index, path: p.path[1:]})
		}
		if len(nextPending) > 0 {
			jsonWalk(inner, &node, nextPending, matched, depth+1, apply)
		}
		if inner.canceled != nil {
			sc.canceled = inner.canceled
			return
		}
		idx++
		i = inner.skipSpace(next)
		if i >= len(raw) {
			return
		}
		switch raw[i] {
		case ']':
			return
		case ',':
			i++
		default:
			return
		}
	}
}

// --- JSON path expressions -------------------------------------------------

func isJSExprSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' }

func skipJSExprSpace(s string, i int) int {
	for i < len(s) && isJSExprSpace(s[i]) {
		i++
	}
	return i
}

func isJSIdentStart(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_'
}

func isJSIdent(c byte) bool {
	return isJSIdentStart(c) || c >= '0' && c <= '9'
}

// parseJSONPath parses Loki's jsonexpr grammar:
//
//	values: field | key_access | index_access
//	      | values key_access | values index_access | values DOT field
//	key_access: "[" STRING "]"; index_access: "[" INDEX "]"
//
// A quoted key is always a literal object key here, even when it looks like an
// index ("[" or "[0]"): the typed step prevents accidental index coercion. The
// pinned EachKey array branch instead uses a string-prefix heuristic and
// ignores Atoi errors, so unusual quoted bracket-looking paths can differ; see
// the package doc.
func parseJSONPath(expr string) ([]jsonPathElem, error) {
	i := skipJSExprSpace(expr, 0)
	if i >= len(expr) {
		return nil, errors.New("syntax error: unexpected end of expression")
	}
	var out []jsonPathElem
	elem, next, err := parseJSONPathElem(expr, i, false)
	if err != nil {
		return nil, err
	}
	out = append(out, elem)
	i = next
	for {
		i = skipJSExprSpace(expr, i)
		if i >= len(expr) {
			return out, nil
		}
		switch expr[i] {
		case '.':
			i = skipJSExprSpace(expr, i+1)
			if i >= len(expr) {
				return nil, errors.New("syntax error: unexpected end of expression")
			}
			if !isJSIdentStart(expr[i]) {
				return nil, fmt.Errorf("unexpected char %c", expr[i])
			}
			start := i
			for i < len(expr) && isJSIdent(expr[i]) {
				i++
			}
			out = append(out, jsonPathElem{key: expr[start:i]})
		case '[':
			elem, next, err := parseJSONPathElem(expr, i, true)
			if err != nil {
				return nil, err
			}
			out = append(out, elem)
			i = next
		default:
			return nil, fmt.Errorf("unexpected char %c", expr[i])
		}
	}
}

// parseJSONPathElem parses one field, key access or index access at i and
// returns the index after it. bracketOnly forces the bracket forms.
func parseJSONPathElem(expr string, i int, bracketOnly bool) (jsonPathElem, int, error) {
	if expr[i] != '[' {
		if bracketOnly || !isJSIdentStart(expr[i]) {
			return jsonPathElem{}, i, fmt.Errorf("unexpected char %c", expr[i])
		}
		start := i
		for i < len(expr) && isJSIdent(expr[i]) {
			i++
		}
		return jsonPathElem{key: expr[start:i]}, i, nil
	}
	i = skipJSExprSpace(expr, i+1)
	if i >= len(expr) {
		return jsonPathElem{}, i, errors.New("syntax error: unexpected end of expression")
	}
	var elem jsonPathElem
	switch {
	case expr[i] == '"':
		key, next, err := scanJSExprString(expr, i)
		if err != nil {
			return jsonPathElem{}, i, err
		}
		elem, i = jsonPathElem{key: key}, next
	case expr[i] >= '0' && expr[i] <= '9':
		start := i
		for i < len(expr) && expr[i] >= '0' && expr[i] <= '9' {
			i++
		}
		if i < len(expr) {
			switch expr[i] {
			case '.':
				return jsonPathElem{}, i, errors.New("cannot use float as array index")
			case ' ', '\t', '\n', ']':
			default:
				return jsonPathElem{}, i, fmt.Errorf("non-integer value: %c", expr[i])
			}
		}
		n, err := strconv.Atoi(expr[start:i])
		if err != nil {
			return jsonPathElem{}, i, err
		}
		elem = jsonPathElem{index: n, isIndex: true}
	default:
		return jsonPathElem{}, i, fmt.Errorf("unexpected char %c", expr[i])
	}
	i = skipJSExprSpace(expr, i)
	if i >= len(expr) || expr[i] != ']' {
		return jsonPathElem{}, i, errors.New("syntax error: expecting ']'")
	}
	return elem, i + 1, nil
}

// scanJSExprString mirrors the jsonexpr lexer's string token: it stops at the
// closing quote or at ']', and tolerates end of input.
func scanJSExprString(expr string, i int) (string, int, error) {
	j := i + 1
	start := j
	for j < len(expr) {
		switch expr[j] {
		case '"', ']':
			return expr[start:j], j + 1, nil
		}
		j++
	}
	return expr[start:], len(expr), nil
}

// --- JSON escapes ----------------------------------------------------------

// unescapeJSONString is Loki's unescapeJSONString: an invalid escape yields ""
// (never an error), and RuneError runes become spaces.
func unescapeJSONString(raw string) string {
	s, err := unescapeJSON(raw)
	if err != nil {
		return ""
	}
	return replaceInvalidRune(s)
}

// unescapeJSON is a port of grafana/jsonparser's Unescape.
func unescapeJSON(in string) (string, error) {
	first := strings.IndexByte(in, '\\')
	if first == -1 {
		return in, nil
	}
	var b strings.Builder
	b.Grow(len(in))
	b.WriteString(in[:first])
	for i := first; i < len(in); {
		if in[i] != '\\' {
			b.WriteByte(in[i])
			i++
			continue
		}
		if i+1 >= len(in) {
			return "", errMalformedStringEscape
		}
		switch e := in[i+1]; e {
		case 'u':
			r, n := decodeUnicodeEscape(in[i:])
			if n == -1 {
				return "", errMalformedStringEscape
			}
			b.WriteRune(r)
			i += n
		default:
			v, ok := backslashEscape(e)
			if !ok {
				return "", errMalformedStringEscape
			}
			b.WriteByte(v)
			i += 2
		}
	}
	return b.String(), nil
}

// backslashEscape returns the byte a two-character JSON escape stands for.
func backslashEscape(c byte) (byte, bool) {
	switch c {
	case '"', '\\', '/':
		return c, true
	case 'b':
		return '\b', true
	case 'f':
		return '\f', true
	case 'n':
		return '\n', true
	case 'r':
		return '\r', true
	case 't':
		return '\t', true
	}
	return 0, false
}

func h2i(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	}
	return -1
}

func decodeSingleUnicodeEscape(in string) (rune, bool) {
	if len(in) < 6 {
		return utf8.RuneError, false
	}
	h1, h2, h3, h4 := h2i(in[2]), h2i(in[3]), h2i(in[4]), h2i(in[5])
	if h1 < 0 || h2 < 0 || h3 < 0 || h4 < 0 {
		return utf8.RuneError, false
	}
	return rune(h1<<12 + h2<<8 + h3<<4 + h4), true
}

func decodeUnicodeEscape(in string) (rune, int) {
	r, ok := decodeSingleUnicodeEscape(in)
	if !ok {
		return utf8.RuneError, -1
	}
	if r <= 0xFFFF && !(r >= 0xD800 && r <= 0xDFFF) {
		return r, 6
	}
	r2, ok := decodeSingleUnicodeEscape(in[6:])
	if !ok || r2 < 0xDC00 {
		return utf8.RuneError, -1
	}
	return 0x10000 + (r-0xD800)<<10 + (r2 - 0xDC00), 12
}

// --- regexp ----------------------------------------------------------------

type regexpParserStage struct {
	re    *regexp.Regexp
	names []string // sanitized capture name by submatch index, "" when unnamed
}

func compileRegexpParser(p *RegexpParser) (pipelineStage, error) {
	re, err := regexp.Compile(p.Pattern)
	if err != nil {
		return nil, fmt.Errorf("logql: %w: regexp %q: %w", plan.ErrBadQuery, p.Pattern, err)
	}
	seen := make(map[string]bool, re.NumSubexp())
	names := make([]string, len(re.SubexpNames()))
	named := 0
	for i, name := range re.SubexpNames() {
		if i == 0 || name == "" {
			continue
		}
		if seen[name] {
			return nil, fmt.Errorf("logql: %w: regexp %q repeats capture name %q", plan.ErrBadQuery, p.Pattern, name)
		}
		seen[name] = true
		names[i] = sanitizeLabelKey(name)
		named++
	}
	if named == 0 {
		return nil, fmt.Errorf("logql: %w: regexp %q must contain at least one named capture group", plan.ErrBadQuery, p.Pattern)
	}
	return &regexpParserStage{re: re, names: names}, nil
}

func (s *regexpParserStage) process(ctx context.Context, e *logEntry) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	m := s.re.FindStringSubmatch(e.line)
	for i := 1; i < len(m); i++ {
		if name := s.names[i]; name != "" {
			e.setExtractedLabel(name, m[i])
		}
	}
	return true, nil
}

// --- pattern ---------------------------------------------------------------

type patternNode struct {
	capture bool
	name    string // "_" for an anonymous capture
	lit     string
}

type patternParserStage struct {
	nodes []patternNode
	names []string // named captures in order
}

const (
	patternAnonymous = "_"
	patternNoCapture = "at least one capture is required"
)

// parsePattern ports pkg/logql/log/pattern's parser and validate. Captures are
// <identifier> with an ASCII letter or '_' first; everything else, including
// malformed capture-looking text, is a literal.
func parsePattern(src string) (*patternParserStage, error) {
	if src == "" {
		return nil, errors.New("syntax error: unexpected $end, expecting IDENTIFIER or LITERAL")
	}
	var (
		nodes []patternNode
		lit   strings.Builder
		names []string
	)
	flush := func() {
		if lit.Len() > 0 {
			nodes = append(nodes, patternNode{lit: lit.String()})
			lit.Reset()
		}
	}
	for i := 0; i < len(src); {
		if src[i] == '<' {
			if j, ok := patternIdentifier(src, i); ok {
				flush()
				name := src[i+1 : j]
				if name != patternAnonymous {
					names = append(names, name)
				}
				nodes = append(nodes, patternNode{capture: true, name: name})
				i = j + 1
				continue
			}
		}
		r, size := utf8.DecodeRuneInString(src[i:])
		if r == utf8.RuneError && size == 1 {
			return nil, fmt.Errorf("invalid UTF-8 byte %#x", src[i])
		}
		lit.WriteString(src[i : i+size])
		i += size
	}
	flush()

	if len(names) == 0 {
		return nil, errors.New(patternNoCapture)
	}
	for i, n := range nodes {
		if !n.capture || i+1 >= len(nodes) {
			continue
		}
		if nodes[i+1].capture {
			return nil, fmt.Errorf("found consecutive capture '%s%s': invalid expression", n.render(), nodes[i+1].render())
		}
	}
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if seen[name] {
			return nil, fmt.Errorf("duplicate capture name (%s): invalid expression", name)
		}
		seen[name] = true
	}
	return &patternParserStage{nodes: nodes, names: names}, nil
}

// patternIdentifier reports whether a capture starts at i and returns the index
// of its closing '>'.
func patternIdentifier(src string, i int) (int, bool) {
	j := i + 1
	if j >= len(src) || !isPatternIdentStart(src[j]) {
		return 0, false
	}
	j++
	for j < len(src) && isPatternIdent(src[j]) {
		j++
	}
	if j < len(src) && src[j] == '>' {
		return j, true
	}
	return 0, false
}

func isPatternIdentStart(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_'
}

func isPatternIdent(c byte) bool {
	return isPatternIdentStart(c) || c >= '0' && c <= '9'
}

func (n patternNode) render() string {
	if n.capture {
		return "<" + n.name + ">"
	}
	return n.lit
}

func compilePatternParser(p *PatternParser) (pipelineStage, error) {
	m, err := parsePattern(p.Pattern)
	if err != nil {
		return nil, fmt.Errorf("logql: %w: pattern %q: %w", plan.ErrBadQuery, p.Pattern, err)
	}
	return m, nil
}

func (s *patternParserStage) process(ctx context.Context, e *logEntry) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	for i, value := range s.capture(e.line) {
		e.setExtractedLabel(s.names[i], value)
	}
	return true, nil
}

// capture ports pattern.Matcher.Matches. A leading literal must match at the
// start of the line; a missing later literal makes the current capture take
// the whole remainder. There is no full-match requirement, so partial matches
// still yield labels.
func (s *patternParserStage) capture(line string) []string {
	if line == "" || len(s.nodes) == 0 {
		return nil
	}
	nodes := s.nodes
	if !nodes[0].capture {
		if !strings.HasPrefix(line, nodes[0].lit) {
			return nil
		}
		line = line[len(nodes[0].lit):]
		nodes = nodes[1:]
	}
	if len(nodes) == 0 {
		return nil
	}
	var out []string
	for len(nodes) > 0 {
		if len(nodes) == 1 {
			if nodes[0].name != patternAnonymous {
				out = append(out, line)
			}
			return out
		}
		capt := nodes[0]
		lit := nodes[1].lit
		nodes = nodes[2:]
		i := strings.Index(line, lit)
		if i == -1 {
			if capt.name != patternAnonymous {
				out = append(out, line)
			}
			return out
		}
		if capt.name != patternAnonymous {
			out = append(out, line[:i])
		}
		line = line[i+len(lit):]
	}
	return out
}

// --- logfmt ----------------------------------------------------------------

type logfmtExtraction struct {
	name string
	orig string
}

type logfmtParserStage struct {
	strict      bool
	keepEmpty   bool
	extractions []logfmtExtraction
}

// compileLogfmtParser validates the named extraction expressions. A duplicate
// destination keeps the last path, as the pinned map does.
func compileLogfmtParser(p *LogfmtParser) (pipelineStage, error) {
	s := &logfmtParserStage{strict: p.Strict, keepEmpty: p.KeepEmpty}
	for _, ex := range p.Extractions {
		orig, err := parseLogfmtExpr(ex.Path)
		if err != nil {
			return nil, fmt.Errorf("logql: %w: logfmt extraction %s=%q: %w", plan.ErrBadQuery, ex.Name, ex.Path, err)
		}
		replaced := false
		for i := range s.extractions {
			if s.extractions[i].name == ex.Name {
				s.extractions[i].orig = orig
				replaced = true
				break
			}
		}
		if !replaced {
			s.extractions = append(s.extractions, logfmtExtraction{name: ex.Name, orig: orig})
		}
	}
	return s, nil
}

func (s *logfmtParserStage) isExtraction(name string) bool {
	for _, ex := range s.extractions {
		if ex.name == name {
			return true
		}
	}
	return false
}

// destination maps a decoded logfmt key to the destination of an extraction
// whose parsed expression matches it.
func (s *logfmtParserStage) destination(key string) (string, bool) {
	for _, ex := range s.extractions {
		if ex.orig == key {
			return ex.name, true
		}
	}
	return "", false
}

func (s *logfmtParserStage) process(ctx context.Context, e *logEntry) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	if len(s.extractions) == 0 {
		s.processPlain(e)
		return true, nil
	}
	s.processNamed(e)
	return true, nil
}

// processPlain is LogfmtParser.Process.
func (s *logfmtParserStage) processPlain(e *logEntry) {
	var dec logfmtDecoder
	dec.reset(e.line)
	for !dec.eol() {
		if !dec.scanKeyval() {
			if s.strict {
				break
			}
			continue
		}
		name := sanitizeLabelKey(dec.key)
		if name == "" {
			continue
		}
		key := e.parserKey(name)
		if e.alreadyExtracted(key) {
			continue
		}
		val := replaceInvalidRune(dec.value)
		if !s.keepEmpty && val == "" {
			continue
		}
		e.setParsed(key, val)
	}
	if s.strict && dec.err != nil {
		e.setParseError(logfmtParserErr, dec.err)
	}
}

// processNamed is LogfmtExpressionParser.Process: every destination starts as
// an empty parsed label, matching keys overwrite it, and collisions with stream
// or metadata labels take the duplicate suffix.
func (s *logfmtParserStage) processNamed(e *logEntry) {
	for _, ex := range s.extractions {
		if !e.baseHas(ex.name) && !e.hasMeta(ex.name) {
			e.setParsed(ex.name, "")
		}
	}
	var dec logfmtDecoder
	dec.reset(e.line)
	for !dec.eol() {
		if !dec.scanKeyval() {
			if s.strict {
				break
			}
			continue
		}
		key := sanitizeLabelKey(dec.key)
		if key == "" {
			continue
		}
		// Only a key that is itself a destination is exempt from the
		// extracted check (Loki's alwaysExtract).
		if !s.isExtraction(key) && e.alreadyExtracted(key) {
			continue
		}
		val := dec.value
		if !utf8.ValidString(val) {
			// The expression parser clears any value containing invalid UTF-8.
			val = ""
		}
		if dest, ok := s.destination(key); ok {
			key = dest
		}
		if !s.isExtraction(key) {
			continue
		}
		if e.baseHas(key) || e.hasMeta(key) {
			key += duplicateSuffix
			if e.alreadyExtracted(key) {
				break
			}
		}
		e.setParsed(key, val)
	}
	if s.strict && dec.err != nil {
		e.setParseError(logfmtParserErr, dec.err)
	}
}

func isLogfmtSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' }

// parseLogfmtExpr ports pkg/logql/log/logfmt.Parse for the single-token
// expressions the LogQL grammar can express: a bare identifier or a quoted
// string, optionally padded with whitespace.
func parseLogfmtExpr(expr string) (string, error) {
	i := 0
	for i < len(expr) && isLogfmtSpace(expr[i]) {
		i++
	}
	if i >= len(expr) {
		return "", errors.New("syntax error: unexpected end of expression")
	}
	var tok string
	switch c := expr[i]; {
	case isPatternIdentStart(c):
		start := i
		for i < len(expr) && isPatternIdent(expr[i]) {
			i++
		}
		tok = expr[start:i]
	case c == '"':
		j := i + 1
		start := j
		found := false
		for j < len(expr) {
			if expr[j] == '"' || expr[j] == ']' {
				tok = expr[start:j]
				j++
				found = true
				break
			}
			j++
		}
		if !found {
			tok = expr[start:]
		}
		i = j
	default:
		return "", fmt.Errorf("unexpected char %c", c)
	}
	for i < len(expr) && isLogfmtSpace(expr[i]) {
		i++
	}
	if i < len(expr) {
		return "", fmt.Errorf("unexpected char %c", expr[i])
	}
	return tok, nil
}

// logfmtDecoder is a port of Loki's pkg/logql/log/logfmt Decoder (itself
// adapted from go-logfmt). It decodes one line in place.
type logfmtDecoder struct {
	pos   int
	line  string
	key   string
	value string
	err   error
}

func (d *logfmtDecoder) reset(line string) {
	d.pos = 0
	d.line = line
	d.err = nil
}

func (d *logfmtDecoder) eol() bool { return d.pos >= len(d.line) }

func (d *logfmtDecoder) syntaxError(msg string) {
	d.err = &logfmtSyntaxError{Msg: msg, Pos: d.pos + 1}
}

func (d *logfmtDecoder) unexpectedByte(c byte) {
	d.err = &logfmtSyntaxError{Msg: fmt.Sprintf("unexpected %q", c), Pos: d.pos + 1}
}

// logfmtSyntaxError is the decoder's SyntaxError.
type logfmtSyntaxError struct {
	Msg string
	Pos int
}

func (e *logfmtSyntaxError) Error() string {
	return fmt.Sprintf("logfmt syntax error at pos %d : %s", e.Pos, e.Msg)
}

func (d *logfmtDecoder) scanKeyval() bool {
	d.key, d.value = "", ""
	line := d.line
	var (
		start     int
		multibyte bool
		hasEsc    bool
		esc       bool
		c         byte
		p         int
	)

	for p = d.pos; p < len(line); p++ {
		if line[p] > ' ' {
			d.pos = p
			goto key
		}
	}
	d.pos = len(line)
	return false

key:
	const invalidKey = "invalid key"

	start, multibyte = d.pos, false
	for p = d.pos; p < len(line); p++ {
		c = line[p]
		switch {
		case c == '=':
			d.pos = p
			if d.pos > start {
				d.key = line[start:d.pos]
				if multibyte && strings.ContainsRune(d.key, utf8.RuneError) {
					d.syntaxError(invalidKey)
					goto skipValue
				}
			}
			if d.key == "" {
				d.unexpectedByte(c)
				goto skipValue
			}
			goto equal
		case c == '"':
			d.pos = p
			d.unexpectedByte(c)
			goto skipValue
		case c <= ' ':
			d.pos = p
			if d.pos > start {
				d.key = line[start:d.pos]
				if multibyte && strings.ContainsRune(d.key, utf8.RuneError) {
					d.syntaxError(invalidKey)
					return false
				}
			}
			return true
		case c >= utf8.RuneSelf:
			multibyte = true
		}
	}
	d.pos = len(line)
	if d.pos > start {
		d.key = line[start:d.pos]
		if multibyte && strings.ContainsRune(d.key, utf8.RuneError) {
			d.syntaxError(invalidKey)
			return false
		}
	}
	return true

equal:
	d.pos++
	if d.pos >= len(line) {
		return true
	}
	switch c := line[d.pos]; {
	case c <= ' ':
		return true
	case c == '"':
		goto qvalue
	}
	start = d.pos
	for p = d.pos; p < len(line); p++ {
		c = line[p]
		switch {
		case c == '=' || c == '"':
			d.pos = p
			d.unexpectedByte(c)
			goto skipValue
		case c <= ' ':
			d.pos = p
			if d.pos > start {
				d.value = line[start:d.pos]
			}
			return true
		}
	}
	d.pos = len(line)
	if d.pos > start {
		d.value = line[start:d.pos]
	}
	return true

skipValue:
	for p = d.pos; p < len(line); p++ {
		if line[p] <= ' ' {
			d.pos = p
			return false
		}
	}
	d.pos = len(line)
	return false

qvalue:
	const (
		untermQuote  = "unterminated quoted value"
		invalidQuote = "invalid quoted value"
	)

	hasEsc, esc = false, false
	start = d.pos
	for p = d.pos + 1; p < len(line); p++ {
		c = line[p]
		switch {
		case esc:
			esc = false
		case c == '\\':
			hasEsc, esc = true, true
		case c == '"':
			d.pos = p + 1
			if hasEsc {
				v, ok := logfmtUnquote(line[start:d.pos])
				if !ok {
					d.syntaxError(invalidQuote)
					return false
				}
				d.value = v
			} else if p-1 > start {
				d.value = line[start+1 : p]
			}
			return true
		}
	}
	d.pos = len(line)
	d.syntaxError(untermQuote)
	return false
}

// logfmtUnquote is a port of Loki's logfmt unquoteBytes (from encoding/json).
func logfmtUnquote(s string) (string, bool) {
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return "", false
	}
	s = s[1 : len(s)-1]
	if !strings.ContainsAny(s, "\\\"") && utf8.ValidString(s) {
		return s, true
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		switch c := s[i]; {
		case c == '\\':
			i++
			if i >= len(s) {
				return "", false
			}
			switch s[i] {
			case '"', '\\', '/', '\'':
				b.WriteByte(s[i])
				i++
			case 'b':
				b.WriteByte('\b')
				i++
			case 'f':
				b.WriteByte('\f')
				i++
			case 'n':
				b.WriteByte('\n')
				i++
			case 'r':
				b.WriteByte('\r')
				i++
			case 't':
				b.WriteByte('\t')
				i++
			case 'u':
				rr := logfmtGetu4(s[i-1:])
				if rr < 0 {
					return "", false
				}
				i += 5
				if rr >= 0xD800 && rr <= 0xDBFF {
					if r2 := logfmtGetu4(s[i:]); r2 >= 0xDC00 && r2 <= 0xDFFF {
						rr = 0x10000 + (rr-0xD800)<<10 + (r2 - 0xDC00)
						i += 6
					} else {
						rr = utf8.RuneError
					}
				}
				b.WriteRune(rr)
			default:
				return "", false
			}
		case c == '"':
			return "", false
		case c < utf8.RuneSelf:
			b.WriteByte(c)
			i++
		default:
			r, size := utf8.DecodeRuneInString(s[i:])
			b.WriteRune(r)
			i += size
		}
	}
	return b.String(), true
}

func logfmtGetu4(s string) rune {
	if len(s) < 6 || s[0] != '\\' || s[1] != 'u' {
		return -1
	}
	v, err := strconv.ParseUint(s[2:6], 16, 64)
	if err != nil {
		return -1
	}
	return rune(v)
}
