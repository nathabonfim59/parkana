// Drop and keep stages: | drop foo, bar=~"x.*" and | keep foo, bar=~"x.*"
// select which labels survive the pipeline. They never drop the row itself —
// Loki's DropLabels and KeepLabels always return the line and true — the
// targets select labels, not rows.
//
// Drop removes each target wherever it appears: stream, structured metadata
// or parsed. A name or matcher that hits no label is harmless. The builder
// error state is reachable only through the special names __error__ and
// __error_details__: a name target resets it unconditionally and a matcher
// compares against it, while a parsed field that happens to carry one of
// those literal names stays untouched (dropLabelNames/dropLabelMatches).
//
// Keep deletes every label no target matches, traversing the effective set
// once so the parsed value wins over metadata, which wins over the stream
// value, and always preserves __error__, __error_details__ and
// __preserve_error__ regardless of the targets (Loki's isSpecialLabel). An
// empty target list is a no-op: the parser never produces one, but a directly
// built stage keeps every label.
package logql

import (
	"context"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// preserveErrorLabel is logqlmodel.PreserveErrorLabel: keep stages never
// delete a label with this name.
const preserveErrorLabel = "__preserve_error__"

// isErrorDetailsLabel reports whether name is the error-details label.
func isErrorDetailsLabel(name string) bool { return name == errorDetailsLabel }

// isSpecialLabel reports whether a keep stage must always preserve name: the
// error labels and __preserve_error__ (Loki's isSpecialLabel).
func isSpecialLabel(name string) bool {
	switch name {
	case errorLabel, errorDetailsLabel, preserveErrorLabel:
		return true
	}
	return false
}

// addEntryMetadata loads a row's structured metadata into the entry the way
// Loki's LabelsBuilder.Add(StructuredMetadataLabel, meta) does, in source
// order: a name the immutable base owns is suffixed with _extracted first,
// then __error__/__error_details__ feed the builder error state instead of
// the metadata category, and ordinary names are upserted into the metadata
// set and removed from the mutable stream labels. Metadata is not recorded
// as parser-extracted, so a later parser still sees the base collision.
func addEntryMetadata(e *logEntry, meta logs.Labels) {
	for _, l := range meta {
		name := l.Name
		if e.baseHas(name) {
			name += duplicateSuffix
		}
		if isErrorLabel(name) {
			e.setErr(l.Value)
			continue
		}
		if isErrorDetailsLabel(name) {
			e.setErrorDetails(l.Value)
			continue
		}
		e.labels = removeLabel(e.labels, name)
		e.meta = upsertLabel(e.meta, name, l.Value)
		e.builderMutated = true
	}
}

// compileDrop compiles a | drop stage. The parser has already validated the
// targets, so the only work is freezing them for the compiled stage's
// lifetime.
func compileDrop(d *Drop) (pipelineStage, error) {
	return dropStage{targets: cloneDropTargets(d.Targets)}, nil
}

// compileKeep compiles a | keep stage; see compileDrop.
func compileKeep(k *Keep) (pipelineStage, error) {
	return keepStage{targets: cloneDropTargets(k.Targets)}, nil
}

// cloneDropTargets copies the target list and each matcher so the compiled
// stage shares no mutable state with the AST.
func cloneDropTargets(ts []DropTarget) []DropTarget {
	out := make([]DropTarget, len(ts))
	for i, t := range ts {
		out[i] = t
		if t.Matcher != nil {
			m := *t.Matcher
			out[i].Matcher = &m
		}
	}
	return out
}

// dropStage is Loki's DropLabels: a compiled | drop stage.
type dropStage struct {
	targets []DropTarget
}

func (s dropStage) process(ctx context.Context, e *logEntry) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	for _, t := range s.targets {
		if t.Matcher != nil {
			dropLabelMatch(e, t.Matcher)
			continue
		}
		dropLabelName(e, t.Name)
	}
	return true, nil
}

// dropLabelName is Loki's dropLabelNames: the error names reset the builder
// error state and nothing else; every other name is deleted from all
// categories, but only when Get can see it.
func dropLabelName(e *logEntry, name string) {
	if isErrorLabel(name) {
		e.setErr("")
		return
	}
	if isErrorDetailsLabel(name) {
		e.setErrorDetails("")
		return
	}
	if _, _, ok := e.get(name); ok {
		e.deleteLabel(name)
	}
}

// dropLabelMatch is Loki's dropLabelMatches: the error names compare against
// the builder error state; every other name compares against Get's effective
// value, where a missing label reads "".
func dropLabelMatch(e *logEntry, m *plan.Matcher) {
	switch {
	case isErrorLabel(m.Name):
		if m.Matches(e.errValue()) {
			e.setErr("")
		}
	case isErrorDetailsLabel(m.Name):
		if m.Matches(e.errDetailsValue()) {
			e.setErrorDetails("")
		}
	default:
		v, _, _ := e.get(m.Name)
		if m.Matches(v) {
			e.deleteLabel(m.Name)
		}
	}
}

// keepStage is Loki's KeepLabels: a compiled | keep stage.
type keepStage struct {
	targets []DropTarget
}

func (s keepStage) process(ctx context.Context, e *logEntry) (bool, error) {
	if err := checkContext(ctx); err != nil {
		return false, err
	}
	if len(s.targets) == 0 {
		return true, nil
	}
	// Traverse the effective label set once, parsed first so a shadowed name
	// is judged on the value UnsortedLabels would emit. Deletions collect
	// first so the iteration never sees a mutated slice.
	var drops []string
	seen := make(map[string]struct{}, len(e.parsed)+len(e.meta)+len(e.labels))
	for _, ls := range []logs.Labels{e.parsed, e.meta, e.labels} {
		for _, l := range ls {
			if _, dup := seen[l.Name]; dup {
				continue
			}
			seen[l.Name] = struct{}{}
			if isSpecialLabel(l.Name) || s.keeps(l.Name, l.Value) {
				continue
			}
			drops = append(drops, l.Name)
		}
	}
	for _, name := range drops {
		e.deleteLabel(name)
	}
	return true, nil
}

// keeps reports whether any target retains the label: a name target matches
// the name, a matcher target must share the name and match the effective
// value (Loki's KeepLabels inner loop).
func (s keepStage) keeps(name, value string) bool {
	for _, t := range s.targets {
		if t.Matcher != nil {
			if t.Matcher.Name == name && t.Matcher.Matches(value) {
				return true
			}
			continue
		}
		if t.Name == name {
			return true
		}
	}
	return false
}
