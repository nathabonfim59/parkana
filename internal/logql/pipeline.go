// Pipeline compilation and the mutable per-entry label state the Go-side
// stages share. A4 owns the compiled query and Process; this file is the A5
// extension point: compileStage turns parser and label-filter AST nodes into
// pipelineStage implementations, and the *logEntry methods below mirror the
// parts of Loki's LabelsBuilder the parsers, filters and the later A6
// formatting stages need.
//
// The category precedence is Loki's (pkg/logql/log/labels.go Set): parsed
// labels win over structured metadata, which wins over stream labels. A parsed
// write therefore removes the same name from the other two categories. Stream
// labels with an empty value count as missing, like LabelsBuilder.Get's
// base.Get lookup. The builder error state (__error__/__error_details__) lives
// in logEntry.err/errDetails, not in a label category: get and the typed
// filters do not see it, while a parsed field that happens to carry one of
// those names is an ordinary label. Process appends the state at output.
package logql

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// Error and metadata label names, pinned to Loki's logqlmodel constants.
const (
	errorLabel        = "__error__"
	errorDetailsLabel = "__error_details__"

	// duplicateSuffix is appended when an extracted label would collide with
	// a stream or structured-metadata label of the same name
	// (pkg/logql/log/parser.go).
	duplicateSuffix = "_extracted"
)

// labelCategory names one of the three label sets a log entry carries.
type labelCategory int

const (
	streamLabels labelCategory = iota
	metaLabels
	parsedLabels
)

// isErrorLabel reports whether name is the parser-error label.
func isErrorLabel(name string) bool { return name == errorLabel }

// compileStage compiles one Go-side pipeline stage. Line filters are compiled
// by compileLogQuery; every other stage reaches this dispatch. Stages owned by
// later phases return plan.ErrUnsupported rather than being dropped.
func compileStage(st Stage) (pipelineStage, error) {
	switch st := st.(type) {
	case *JSONParser:
		return compileJSONParser(st)
	case *LogfmtParser:
		return compileLogfmtParser(st)
	case *RegexpParser:
		return compileRegexpParser(st)
	case *PatternParser:
		return compilePatternParser(st)
	case *LabelFilterLeaf:
		return compileLabelFilterLeaf(st)
	case *LabelFilterBinary:
		return compileLabelFilterBinary(st)
	case *Drop:
		return compileDrop(st)
	case *Keep:
		return compileKeep(st)
	case *LineFormat:
		return compileLineFormat(st)
	case *LabelFormat:
		return compileLabelFormat(st)
	case *Unwrap:
		return nil, fmt.Errorf("logql: %w: pipeline stage %q is not implemented yet", plan.ErrUnsupported, st.String())
	default:
		return nil, fmt.Errorf("logql: %w: pipeline stage %q is not supported", plan.ErrUnsupported, st.String())
	}
}

// checkContext returns a wrapped cancellation error, the only runtime error a
// pipeline stage reports. Parser failures are data, not Go errors: they set
// the error labels and keep the row.
func checkContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("logql: pipeline: %w", err)
	}
	return nil
}

// baseHas reports whether the entry's original stream labels contain name.
// Loki's LabelsBuilder.BaseHas consults its immutable base set, so collisions
// are decided against the stored stream labels even after a later drop or
// rename changed the final set.
func (e *logEntry) baseHas(name string) bool {
	_, ok := e.base.Get(name)
	return ok
}

// hasMeta reports whether the structured metadata holds name.
func (e *logEntry) hasMeta(name string) bool {
	_, ok := e.meta.Get(name)
	return ok
}

// get looks up name the way LabelsBuilder.GetWithCategory does: parsed first,
// then structured metadata, then the stream labels. A stream label with an
// empty value is reported missing. The builder error state is not a category,
// so it is invisible here even though a parsed field may share its name.
func (e *logEntry) get(name string) (string, labelCategory, bool) {
	if v, ok := e.parsed.Get(name); ok {
		return v, parsedLabels, true
	}
	if v, ok := e.meta.Get(name); ok {
		return v, metaLabels, true
	}
	if v, ok := e.labels.Get(name); ok && v != "" {
		return v, streamLabels, true
	}
	return "", 0, false
}

// parserKey returns the name an extraction with the given (already sanitized)
// name must write: Loki appends "_extracted" when the original stream labels
// or the structured metadata already own the name.
func (e *logEntry) parserKey(name string) string {
	if e.baseHas(name) || e.hasMeta(name) {
		return name + duplicateSuffix
	}
	return name
}

// alreadyExtracted reports whether an earlier stage wrote a parsed label with
// this name. Parsers consult it after parserKey, mirroring
// LabelsBuilder.ParserLabelHints().Extracted.
func (e *logEntry) alreadyExtracted(name string) bool {
	_, ok := e.extracted[name]
	return ok
}

// setParsed writes a parsed label unconditionally, mirroring
// LabelsBuilder.Set(ParsedLabel, …): it removes the name from the stream and
// metadata categories, replaces or inserts the sorted parsed entry and records
// the name as extracted. Callers that skip already-extracted names must check
// alreadyExtracted themselves; some Loki parsers (the named JSON path and
// logfmt variants) deliberately overwrite.
func (e *logEntry) setParsed(name, value string) {
	e.labels = removeLabel(e.labels, name)
	e.meta = removeLabel(e.meta, name)
	e.parsed = upsertLabel(e.parsed, name, value)
	if e.extracted == nil {
		e.extracted = make(map[string]struct{}, 4)
	}
	e.extracted[name] = struct{}{}
	e.builderMutated = true
}

// deleteLabel removes name from every category. It is Loki's LabelsBuilder.Del:
// only the drop, keep and label_format-rename stages call it, never Set shadow
// removal (setParsed and addEntryMetadata use removeLabel directly). It does
// not clear the extracted record, matching Del, and records the deletion for
// GroupedLabels' modified path and withResult's del-before-add check.
func (e *logEntry) deleteLabel(name string) {
	e.labels = removeLabel(e.labels, name)
	e.meta = removeLabel(e.meta, name)
	e.parsed = removeLabel(e.parsed, name)
	e.builderMutated = true
	if e.builderDeleted == nil {
		e.builderDeleted = make(map[string]struct{}, 4)
	}
	e.builderDeleted[name] = struct{}{}
}

// errValue returns the builder's current __error__ state, "" when no stage has
// set one.
func (e *logEntry) errValue() string { return e.err }

// errDetailsValue returns the builder's current __error_details__ state.
func (e *logEntry) errDetailsValue() string { return e.errDetails }

// setErr replaces the __error__ state unconditionally, as Loki's addErrLabel
// does when a parser fails. It is not a parsed label and is not recorded as
// extracted.
func (e *logEntry) setErr(kind string) { e.err = kind }

// setErrorDetails replaces the __error_details__ state unconditionally.
func (e *logEntry) setErrorDetails(details string) { e.errDetails = details }

// setParseError is Loki's addErrLabel: it always replaces the error kind, and
// sets the details only when the underlying error is non-nil (some parser
// failures carry a type but no detail).
func (e *logEntry) setParseError(kind string, err error) {
	e.setErr(kind)
	if err != nil {
		e.setErrorDetails(err.Error())
	}
}

// setErrIfAbsent sets both error labels only when no error is set yet. Typed
// label filters use it so a conversion failure cannot hide a more useful
// parser error.
func (e *logEntry) setErrIfAbsent(kind, details string) {
	if e.err != "" {
		return
	}
	e.setErr(kind)
	e.setErrorDetails(details)
}

// entryParsed returns the parsed labels an entry exposes, with the builder
// error state appended last like Loki's UnsortedLabels. logs.Labels cannot
// hold duplicate names: if a parser extracted a field literally named
// __error__ or __error_details__, the appended state value replaces it, which
// is what a JSON encoder keeping the last duplicate does in Loki. The input
// slice is returned unchanged when there is no error state.
func entryParsed(parsed logs.Labels, err, details string) logs.Labels {
	if err == "" && details == "" {
		return parsed
	}
	out := slices.Clone(parsed)
	if err != "" {
		out = upsertLabel(out, errorLabel, err)
	}
	if details != "" {
		out = upsertLabel(out, errorDetailsLabel, details)
	}
	return out
}

// upsertLabel replaces the value of name or inserts it, keeping ls sorted.
// Callers own ls, so no copy is made.
func upsertLabel(ls logs.Labels, name, value string) logs.Labels {
	i, found := slices.BinarySearchFunc(ls, name, func(l logs.Label, name string) int {
		return cmp.Compare(l.Name, name)
	})
	if found {
		ls[i].Value = value
		return ls
	}
	return slices.Insert(ls, i, logs.Label{Name: name, Value: value})
}

// removeLabel deletes name from a sorted label set, if present.
func removeLabel(ls logs.Labels, name string) logs.Labels {
	i, found := slices.BinarySearchFunc(ls, name, func(l logs.Label, name string) int {
		return cmp.Compare(l.Name, name)
	})
	if !found {
		return ls
	}
	return slices.Delete(ls, i, i+1)
}
