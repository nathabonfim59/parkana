// This file implements the formatting stages: | line_format and | label_format.
// Both run Go text/templates with a bounded subset of Loki's template helpers
// (pkg/logql/log/fmt.go), missingkey=zero, and per-row binding for the reserved
// functions __line__ and __timestamp__. A helper outside the subset is rejected
// at compile time rather than silently rendering something else.
//
// The compiled stages hold only read-only parse trees. Every Process call
// builds a fresh template over those trees with a function map that closes over
// that call's *logEntry, so the compiled query stays immutable and safe for
// concurrent Process calls even though text/template function values cannot be
// rebound in place.

package logql

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"text/template"
	"text/template/parse"
	"time"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// templateFormatErr is Loki's errTemplateFormat: the __error__ kind set when a
// line or label template fails at execution time (pkg/logql/log/parser.go).
const templateFormatErr = "TemplateFormatErr"

// The reserved template function names. Like Loki's functionLineName and
// functionTimestampName they shadow any label of the same name.
const (
	functionLineName      = "__line__"
	functionTimestampName = "__timestamp__"
)

// logTemplate is one compiled text/template: the root template name plus every
// parse tree the template defines (the root and any {{define}} bodies). The
// trees are immutable after compilation and are shared by every Process call.
type logTemplate struct {
	name  string
	trees []logTemplateTree
}

// logTemplateTree is one named parse tree of a compiled template.
type logTemplateTree struct {
	name string
	tree *parse.Tree
}

// compileLogTemplate parses text with the helper function map and
// missingkey=zero, the way Loki builds line and label templates. Template
// syntax errors are bad queries, even when an unknown helper appears earlier in
// the text. A syntactically valid template that uses an unknown or unimplemented
// helper is explicitly unsupported, so a query never silently renders something
// else.
func compileLogTemplate(name, text string) (*logTemplate, error) {
	// The parse-time binding is never executed: compilation keeps only the
	// parse trees and rebinds real values per row in bind.
	t, err := template.New(name).
		Option("missingkey=zero").
		Funcs(logTemplateFuncs(nil)).
		Parse(text)
	if err != nil {
		if undefinedFunction(err) {
			// template.Parse stops at the first undefined function, so a
			// later syntax error may still be hiding behind it. Re-parse the
			// whole text with function checking disabled: only a template
			// that is otherwise syntactically valid is an unsupported helper.
			if syntaxErr := syntaxOnlyError(name, text); syntaxErr != nil {
				return nil, fmt.Errorf("%w: %v", plan.ErrBadQuery, syntaxErr)
			}
			return nil, fmt.Errorf("%w: template helper: %v", plan.ErrUnsupported, err)
		}
		return nil, fmt.Errorf("%w: %v", plan.ErrBadQuery, err)
	}
	lt := &logTemplate{name: name}
	for _, tmpl := range t.Templates() {
		if tmpl.Tree == nil {
			continue
		}
		lt.trees = append(lt.trees, logTemplateTree{name: tmpl.Name(), tree: tmpl.Tree})
	}
	return lt, nil
}

// undefinedFunction reports whether text/template rejected an action because it
// names an unknown function. That is the only parse failure caused by a helper
// outside the allowlist; text/template exposes it only as message text.
func undefinedFunction(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "function \"") && strings.Contains(msg, "not defined")
}

// syntaxOnlyError re-parses text with function checking disabled, returning only
// a grammar error. It is how compileLogTemplate looks past an undefined helper
// to validate the rest of the template; parse.SkipFuncCheck is the stdlib mode
// that turns the function lookup off without binding any function.
func syntaxOnlyError(name, text string) error {
	t := parse.New(name)
	t.Mode = parse.SkipFuncCheck
	_, err := t.Parse(text, "", "", make(map[string]*parse.Tree))
	return err
}

// bind returns a fresh template over the compiled trees whose function map is
// bound to e. Trees are never mutated by execution, so bindings from concurrent
// Process calls do not interfere.
func (t *logTemplate) bind(e *logEntry) (*template.Template, error) {
	bound := template.New(t.name).Option("missingkey=zero").Funcs(logTemplateFuncs(e))
	for _, tt := range t.trees {
		if _, err := bound.AddParseTree(tt.name, tt.tree); err != nil {
			return nil, fmt.Errorf("logql: bind template %q: %w", tt.name, err)
		}
	}
	return bound, nil
}

// logTemplateFuncs returns the supported subset of Loki's template helpers. It
// is rebuilt for every execution so the package holds no mutable function-map
// state. A nil entry binds __line__ and __timestamp__ to zero values, which is
// only used while parsing (the parse-time template is never executed).
func logTemplateFuncs(e *logEntry) template.FuncMap {
	line := func() string {
		if e == nil {
			return ""
		}
		return e.line
	}
	timestamp := func() time.Time {
		if e == nil {
			return time.Time{}
		}
		// Loki hands templates time.Unix(0, tsNanos), which renders in the
		// local zone; rows carry microsecond precision.
		return time.Unix(0, e.ts.UnixNano())
	}
	return template.FuncMap{
		functionLineName:      line,
		functionTimestampName: timestamp,

		// The supported subset of Loki's legacy helpers, which map directly
		// onto the strings package.
		"ToLower":    strings.ToLower,
		"ToUpper":    strings.ToUpper,
		"Trim":       strings.Trim,
		"TrimLeft":   strings.TrimLeft,
		"TrimRight":  strings.TrimRight,
		"TrimPrefix": strings.TrimPrefix,
		"TrimSuffix": strings.TrimSuffix,
		"TrimSpace":  strings.TrimSpace,
		"Replace":    strings.Replace,

		"regexReplaceAll":        regexReplaceAll,
		"regexReplaceAllLiteral": regexReplaceAllLiteral,
		"count":                  countMatches,
		"urlencode":              url.QueryEscape,
		"urldecode":              url.QueryUnescape,
		"bytes":                  convertBytes,
		"duration":               convertDuration,
		"duration_seconds":       convertDuration,

		// Sprig helpers, restricted to the string and default helpers whose
		// semantics this file pins. Spelled exactly as sprig v3.3.0.
		"lower":      strings.ToLower,
		"upper":      strings.ToUpper,
		"contains":   func(substr, s string) bool { return strings.Contains(s, substr) },
		"hasPrefix":  func(prefix, s string) bool { return strings.HasPrefix(s, prefix) },
		"hasSuffix":  func(suffix, s string) bool { return strings.HasSuffix(s, suffix) },
		"trim":       strings.TrimSpace,
		"trimPrefix": func(prefix, s string) string { return strings.TrimPrefix(s, prefix) },
		"trimSuffix": func(suffix, s string) string { return strings.TrimSuffix(s, suffix) },
		"replace":    func(old, new, src string) string { return strings.ReplaceAll(src, old, new) },
		"default":    dfault,
	}
}

// regexReplaceAll is Loki's regexReplaceAll: RE2 replacement syntax, where $1
// and ${name} expand capture groups.
func regexReplaceAll(regex, s, repl string) (string, error) {
	r, err := regexp.Compile(regex)
	if err != nil {
		return "", err
	}
	return r.ReplaceAllString(s, repl), nil
}

// regexReplaceAllLiteral is Loki's regexReplaceAllLiteral: the replacement is
// inserted verbatim, without expanding $ sequences.
func regexReplaceAllLiteral(regex, s, repl string) (string, error) {
	r, err := regexp.Compile(regex)
	if err != nil {
		return "", err
	}
	return r.ReplaceAllLiteralString(s, repl), nil
}

// countMatches is Loki's count template helper: the number of non-overlapping
// RE2 matches.
func countMatches(regex, s string) (int, error) {
	r, err := regexp.Compile(regex)
	if err != nil {
		return 0, err
	}
	return len(r.FindAllStringIndex(s, -1)), nil
}

// convertDuration is Loki's duration/duration_seconds helper: a Go duration
// string converted to seconds.
func convertDuration(v string) (float64, error) {
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, err
	}
	return d.Seconds(), nil
}

// convertBytes is Loki's bytes helper: a human-readable size converted to a
// float64 byte count. Parsing is A5's go-humanize port, which is the exact
// library Loki calls.
func convertBytes(v string) (float64, error) {
	b, err := parseBytesField(v)
	if err != nil {
		return 0, err
	}
	return float64(b), nil
}

// dfault is sprig's default: the fallback d when given is unset (zero for its
// type), otherwise given.
func dfault(d any, given ...any) any {
	if len(given) == 0 || isEmptyValue(given[0]) {
		return d
	}
	return given[0]
}

// isEmptyValue is sprig's empty: zeros, empty strings/collections and nil
// pointers/interfaces count as unset; structs never do.
func isEmptyValue(given any) bool {
	g := reflect.ValueOf(given)
	if !g.IsValid() {
		return true
	}
	switch g.Kind() {
	case reflect.Array, reflect.Slice, reflect.Map, reflect.String:
		return g.Len() == 0
	case reflect.Bool:
		return !g.Bool()
	case reflect.Complex64, reflect.Complex128:
		return g.Complex() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return g.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return g.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return g.Float() == 0
	case reflect.Struct:
		return false
	default:
		return g.IsNil()
	}
}

// lineFormatStage is the compiled | line_format stage. simpleKey is Loki's
// fast path: a template that is exactly one field lookup, like {{.message}},
// copies the label value directly instead of executing a template.
type lineFormatStage struct {
	tmpl      *logTemplate
	simpleKey string
}

// compileLineFormat compiles a | line_format stage.
func compileLineFormat(f *LineFormat) (pipelineStage, error) {
	tmpl, err := compileLogTemplate("line", f.Template)
	if err != nil {
		return nil, fmt.Errorf("logql: line_format: %w", err)
	}
	return &lineFormatStage{tmpl: tmpl, simpleKey: simpleLineKey(tmpl)}, nil
}

// simpleLineKey ports Loki's LineFormatter simple-key detection: the root
// template must be a single action whose only command has exactly one argument,
// a one-identifier field node. It returns "" when the template is not of that
// shape.
func simpleLineKey(t *logTemplate) string {
	for _, tt := range t.trees {
		// The root tree is registered under the template name; every
		// {{define}} body has a different name.
		if tt.name != t.name || tt.tree.Root == nil || len(tt.tree.Root.Nodes) != 1 {
			continue
		}
		action, ok := tt.tree.Root.Nodes[0].(*parse.ActionNode)
		if !ok || len(action.Pipe.Cmds) != 1 || len(action.Pipe.Cmds[0].Args) != 1 {
			return ""
		}
		field, ok := action.Pipe.Cmds[0].Args[0].(*parse.FieldNode)
		if !ok || len(field.Ident) != 1 {
			return ""
		}
		return field.Ident[0]
	}
	return ""
}

// process implements pipelineStage. A line template failure keeps the original
// line and records TemplateFormatErr; it never drops the row.
func (s *lineFormatStage) process(ctx context.Context, e *logEntry) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	if s.simpleKey != "" {
		if v, _, ok := e.get(s.simpleKey); ok {
			e.line = v
		} else {
			e.line = ""
		}
		return true, nil
	}
	tmpl, err := s.tmpl.bind(e)
	if err != nil {
		return false, err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, e.templateMap()); err != nil {
		e.setParseError(templateFormatErr, err)
		return true, nil
	}
	e.line = buf.String()
	return true, nil
}

// labelFormatEntry is one compiled label_format assignment: either a rename
// (rename true, reading src) or a template assignment writing dest.
type labelFormatEntry struct {
	dest   string
	src    string
	tmpl   *logTemplate
	rename bool
}

// labelFormatStage is the compiled | label_format stage.
type labelFormatStage struct {
	entries []labelFormatEntry
}

// compileLabelFormat compiles a | label_format stage. Loki validates the
// destination list when it builds the pipeline: __error__ can never be
// formatted and one stage may not write the same destination twice.
func compileLabelFormat(f *LabelFormat) (pipelineStage, error) {
	// Loki validates every destination before it parses any template.
	seen := make(map[string]struct{}, len(f.Replacements))
	for _, r := range f.Replacements {
		if r.Dest == errorLabel {
			return nil, badQuery(f.Pos, "%s cannot be formatted", r.Dest)
		}
		if _, ok := seen[r.Dest]; ok {
			return nil, badQuery(f.Pos, "multiple label name '%s' not allowed in a single format operation", r.Dest)
		}
		seen[r.Dest] = struct{}{}
	}
	entries := make([]labelFormatEntry, 0, len(f.Replacements))
	for _, r := range f.Replacements {
		if !r.IsTemplate {
			entries = append(entries, labelFormatEntry{dest: r.Dest, src: r.Src, rename: true})
			continue
		}
		tmpl, err := compileLogTemplate("label", r.Template)
		if err != nil {
			return nil, fmt.Errorf("logql: label_format label %q: %w", r.Dest, err)
		}
		entries = append(entries, labelFormatEntry{dest: r.Dest, tmpl: tmpl})
	}
	return &labelFormatStage{entries: entries}, nil
}

// process implements pipelineStage. label_format never drops a row and never
// changes the line: a rename moves a label into the parsed category, a template
// writes its destination there, and a template failure only records
// TemplateFormatErr while leaving the destination untouched.
func (s *labelFormatStage) process(ctx context.Context, e *logEntry) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	// Loki builds one label-map snapshot per stage, lazily at the first
	// template assignment, and reuses it for the remaining templates. Earlier
	// renames are visible in that snapshot; later writes are not.
	var snapshot map[string]string
	for _, entry := range s.entries {
		if entry.rename {
			v, _, ok := e.get(entry.src)
			if !ok {
				continue
			}
			e.setParsed(entry.dest, v)
			e.deleteLabel(entry.src)
			continue
		}
		if len(snapshot) == 0 {
			snapshot = e.templateMap()
		}
		tmpl, err := entry.tmpl.bind(e)
		if err != nil {
			return false, err
		}
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, snapshot); err != nil {
			e.setParseError(templateFormatErr, err)
			continue
		}
		e.setParsed(entry.dest, buf.String())
	}
	return true, nil
}

// templateMap returns the map a template sees: the stream, structured-metadata
// and parsed categories followed by the builder error state, in Loki's
// UnsortedLabels order so later writes win. It is a fresh map, so template
// execution can never mutate the entry's label slices.
func (e *logEntry) templateMap() map[string]string {
	m := make(map[string]string, len(e.labels)+len(e.meta)+len(e.parsed)+2)
	for _, l := range e.labels {
		m[l.Name] = l.Value
	}
	for _, l := range e.meta {
		m[l.Name] = l.Value
	}
	for _, l := range e.parsed {
		m[l.Name] = l.Value
	}
	if e.err != "" {
		m[errorLabel] = e.err
	}
	if e.errDetails != "" {
		m[errorDetailsLabel] = e.errDetails
	}
	return m
}
