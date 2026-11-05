// Drop/keep stage tests. The stage predicates select labels, never rows:
// every check goes through the compiled pipeline (Parse -> compileStage via
// CompileLogQuery.Process or Engine.Query) so the dispatch itself is
// exercised.
package logql

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// processQuery compiles query and runs it on row through the tail path.
func processQuery(t *testing.T, query string, row plan.Row) (logs.Labels, plan.Entry, bool) {
	t.Helper()
	c, err := testEngine().CompileLogQuery(query)
	if err != nil {
		t.Fatalf("CompileLogQuery(%q): %v", query, err)
	}
	labels, entry, keep, err := c.Process(t.Context(), tenant.Default, row)
	if err != nil {
		t.Fatalf("Process(%q): %v", query, err)
	}
	return labels, entry, keep
}

func TestDropRemovesAcrossCategories(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	row := plan.Row{
		TS:     base,
		Line:   `{"p":"1","n":"2"}`,
		Labels: labelPairs("app", "api", "x", "stream"),
		Meta:   labelPairs("trace", "t1", "x", "meta"),
	}
	beforeLabels, beforeMeta := slices.Clone(row.Labels), slices.Clone(row.Meta)

	labels, entry, keep := processQuery(t, `{app="api"} | json | drop x, p, trace`, row)
	if !keep {
		t.Fatal("drop removed the row; its targets select labels, never rows")
	}
	if got := labels.String(); got != `{app="api"}` {
		t.Fatalf("stream labels = %s; drop x removed it from stream and meta", got)
	}
	// The metadata x arrived as x_extracted (base collision), so drop x and
	// drop trace leave exactly it behind.
	if got := entry.Meta.Map(); len(got) != 1 || got["x_extracted"] != "meta" {
		t.Fatalf("meta = %v, want only x_extracted=meta", got)
	}
	parsed := entry.Parsed.Map()
	if _, ok := parsed["p"]; ok {
		t.Fatalf("parsed = %v; drop p left the parsed label", parsed)
	}
	if parsed["n"] != "2" {
		t.Fatalf("parsed = %v, want n=2", parsed)
	}
	if !slices.Equal(row.Labels, beforeLabels) || !slices.Equal(row.Meta, beforeMeta) {
		t.Fatalf("Process mutated the source row: %v %v", row.Labels, row.Meta)
	}
}

func TestDropStreamLabelRegroupsStreams(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	src := plan.NewMemSource()
	src.Add(tenant.Default,
		logRow(base, "a", logs.Label{Name: "app", Value: "api"}, logs.Label{Name: "shard", Value: "s1"}),
		logRow(base.Add(time.Second), "b", logs.Label{Name: "app", Value: "api"}, logs.Label{Name: "shard", Value: "s2"}),
	)
	res := mustQuery(t, testEngine(), src, testParams(`{app="api"} | drop shard`, base, base.Add(time.Minute)))
	if len(res.Streams) != 1 {
		t.Fatalf("streams = %+v, want one merged stream", res.Streams)
	}
	if got := res.Streams[0].Labels.String(); got != `{app="api"}` {
		t.Fatalf("stream labels = %s, want shard gone", got)
	}
	if got := len(res.Streams[0].Entries); got != 2 {
		t.Fatalf("entries = %d, want both rows in the merged stream", got)
	}
}

func TestDropMatcherRegexpSemantics(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	row := func(v string) plan.Row {
		return plan.Row{TS: base, Line: "x", Labels: labelPairs("app", "api", "lvl", v)}
	}
	dropped := func(t *testing.T, query string, r plan.Row) bool {
		labels, _, keep := processQuery(t, query, r)
		if !keep {
			t.Fatal("drop removed the row")
		}
		_, ok := labels.Get("lvl")
		return !ok
	}

	// Direct matchers are anchored: "erro" does not match "error".
	if dropped(t, `{app="api"} | drop lvl=~"erro"`, row("error")) {
		t.Fatal("unanchored match dropped lvl; matchers are fully anchored")
	}
	if !dropped(t, `{app="api"} | drop lvl=~"err.*"`, row("error")) {
		t.Fatal(`lvl=~"err.*" did not drop lvl="error"`)
	}
	// Drop/keep matchers use the DotNL stream-matcher semantics: . matches a
	// newline inside the value unless the pattern turns it off.
	if !dropped(t, `{app="api"} | drop lvl=~"a.b"`, row("a\nb")) {
		t.Fatal(`lvl=~"a.b" did not drop "a\nb"; drop matchers use DotNL`)
	}
	if dropped(t, `{app="api"} | drop lvl=~"(?-s:a.b)"`, row("a\nb")) {
		t.Fatal(`lvl=~"(?-s:a.b)" dropped "a\nb"; explicit (?-s) must win`)
	}
	// !~ drops when the pattern does not match.
	if !dropped(t, `{app="api"} | drop lvl!~"warn.*"`, row("error")) {
		t.Fatal(`lvl!~"warn.*" did not drop lvl="error"`)
	}
	if dropped(t, `{app="api"} | drop lvl!~"err.*"`, row("error")) {
		t.Fatal(`lvl!~"err.*" dropped lvl="error"`)
	}
}

func TestDropEqualityMissingAndEmpty(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	hasLabel := func(ls logs.Labels, name string) bool {
		_, ok := ls.Get(name)
		return ok
	}
	// ="" drops a stream label whose value is empty, and is a harmless no-op
	// when the label is missing entirely.
	labels, _, _ := processQuery(t, `{app="api"} | drop x=""`, plan.Row{
		TS: base, Line: "l", Labels: labelPairs("app", "api", "x", ""),
	})
	if hasLabel(labels, "x") {
		t.Fatal(`drop x="" kept an empty stream label`)
	}
	labels, _, keep := processQuery(t, `{app="api"} | drop x=""`, plan.Row{
		TS: base, Line: "l", Labels: labelPairs("app", "api"),
	})
	if !keep || !hasLabel(labels, "app") {
		t.Fatalf(`drop x="" on a row without x: keep=%v labels=%v`, keep, labels)
	}
	// !="" drops every present non-empty value and is a no-op on a missing or
	// empty label.
	labels, _, _ = processQuery(t, `{app="api"} | drop x!=""`, plan.Row{
		TS: base, Line: "l", Labels: labelPairs("app", "api", "x", "v"),
	})
	if hasLabel(labels, "x") {
		t.Fatal(`drop x!="" kept x="v"`)
	}
	labels, _, _ = processQuery(t, `{app="api"} | drop x!=""`, plan.Row{
		TS: base, Line: "l", Labels: labelPairs("app", "api", "x", ""),
	})
	if !hasLabel(labels, "x") {
		t.Fatal(`drop x!="" removed an empty x`)
	}
	// !="v" matches the missing value "" and deletes the name wherever it is;
	// on a present "v" it leaves the label.
	labels, _, _ = processQuery(t, `{app="api"} | drop x!="v"`, plan.Row{
		TS: base, Line: "l", Labels: labelPairs("app", "api", "x", "v"),
	})
	if !hasLabel(labels, "x") {
		t.Fatal(`drop x!="v" removed x="v"`)
	}
	labels, _, _ = processQuery(t, `{app="api"} | drop x!="v"`, plan.Row{
		TS: base, Line: "l", Labels: labelPairs("app", "api", "x", "other"),
	})
	if hasLabel(labels, "x") {
		t.Fatal(`drop x!="v" kept x="other"`)
	}
}

func TestDropErrorState(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	errRow := plan.Row{TS: base, Line: `not json`, Labels: labelPairs("app", "api")}

	// The name target resets __error__ only: the details survive.
	_, entry, keep := processQuery(t, `{app="api"} | json | drop __error__`, errRow)
	if !keep {
		t.Fatal("dropping __error__ dropped the row")
	}
	parsed := entry.Parsed.Map()
	if _, ok := parsed[errorLabel]; ok {
		t.Fatalf("parsed = %v; drop __error__ left the error label", parsed)
	}
	if _, ok := parsed[errorDetailsLabel]; !ok {
		t.Fatalf("parsed = %v; drop __error__ must not reset __error_details__", parsed)
	}

	// __error_details__ is reset independently.
	_, entry, _ = processQuery(t, `{app="api"} | json | drop __error_details__`, errRow)
	parsed = entry.Parsed.Map()
	if _, ok := parsed[errorDetailsLabel]; ok {
		t.Fatalf("parsed = %v; drop __error_details__ left the details label", parsed)
	}
	if parsed[errorLabel] != jsonParserErr {
		t.Fatalf("parsed = %v; drop __error_details__ must not reset __error__", parsed)
	}

	// A matcher on __error__ compares against the error state.
	_, entry, _ = processQuery(t, `{app="api"} | json | drop __error__="JSONParserErr"`, errRow)
	if _, ok := entry.Parsed.Map()[errorLabel]; ok {
		t.Fatal(`drop __error__="JSONParserErr" left the error state`)
	}
	_, entry, _ = processQuery(t, `{app="api"} | json | drop __error__="LogfmtParserErr"`, errRow)
	if got := entry.Parsed.Map()[errorLabel]; got != jsonParserErr {
		t.Fatalf(`drop __error__="LogfmtParserErr" cleared %q state`, got)
	}
	_, entry, _ = processQuery(t, `{app="api"} | json | drop __error__!="JSONParserErr"`, errRow)
	if got := entry.Parsed.Map()[errorLabel]; got != jsonParserErr {
		t.Fatalf(`drop __error__!="JSONParserErr" cleared %q state`, got)
	}
	_, entry, _ = processQuery(t, `{app="api"} | json | drop __error__!="LogfmtParserErr"`, errRow)
	if _, ok := entry.Parsed.Map()[errorLabel]; ok {
		t.Fatal(`drop __error__!="LogfmtParserErr" left the JSONParserErr state`)
	}

	// The matcher form on __error_details__ compares against the details
	// state the same way. A non-object line fails with errMalformedObject,
	// "Value looks like object, but can't find closing '}' symbol".
	_, entry, _ = processQuery(t, `{app="api"} | json | drop __error_details__=~"Value.*"`, errRow)
	parsed = entry.Parsed.Map()
	if _, ok := parsed[errorDetailsLabel]; ok {
		t.Fatal(`drop __error_details__=~"Value.*" left the details state`)
	}
	if parsed[errorLabel] != jsonParserErr {
		t.Fatal(`drop __error_details__ cleared __error__ too`)
	}
	_, entry, _ = processQuery(t, `{app="api"} | json | drop __error_details__=~"nomatch"`, errRow)
	if _, ok := entry.Parsed.Map()[errorDetailsLabel]; !ok {
		t.Fatal(`drop __error_details__=~"nomatch" cleared the details state`)
	}

	// Without an error, drop __error__ is an exact no-op.
	_, entry, _ = processQuery(t, `{app="api"} | json | drop __error__`, plan.Row{
		TS: base, Line: `{"n":1}`, Labels: labelPairs("app", "api"),
	})
	parsed = entry.Parsed.Map()
	if parsed["n"] != "1" || len(parsed) != 1 {
		t.Fatalf("parsed = %v; no-error drop __error__ must be a no-op", parsed)
	}
}

func TestDropParsedErrorLiteralVsState(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	// The parsed field named __error__ is an ordinary parsed label; a name
	// drop of __error__ clears only the builder error state, which is what the
	// appended output label owns. The parsed literal then shows through.
	row := plan.Row{TS: base, Line: `{"__error__":5,"x":}`, Labels: labelPairs("app", "api")}
	_, entry, keep := processQuery(t, `{app="api"} | json | drop __error__`, row)
	if !keep {
		t.Fatal("drop removed the row")
	}
	parsed := entry.Parsed.Map()
	if got := parsed[errorLabel]; got != "5" {
		t.Fatalf("__error__ = %q, want the surviving parsed literal %q", got, "5")
	}
	if _, ok := parsed[errorDetailsLabel]; !ok {
		t.Fatalf("parsed = %v; the error details must survive a __error__ drop", parsed)
	}
}

func TestDropExtractedCollisionAndRecord(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	// Dropping the stream label first must not change the collision rule:
	// parserKey consults the immutable base, so json still writes x_extracted.
	_, entry, _ := processQuery(t, `{app="api"} | drop x | json`, plan.Row{
		TS:     base,
		Line:   `{"x":1}`,
		Labels: labelPairs("app", "api", "x", "s"),
	})
	parsed := entry.Parsed.Map()
	if got := parsed["x"+duplicateSuffix]; got != "1" {
		t.Fatalf("parsed = %v, want x_extracted=1 (base still owns x)", parsed)
	}
	if _, ok := parsed["x"]; ok {
		t.Fatalf("parsed = %v; extraction must not take the dropped base name", parsed)
	}

	// Dropping a parsed label does not clear the extracted record: a second
	// json must not re-extract lvl.
	_, entry, _ = processQuery(t, `{app="api"} | json | drop lvl | json`, plan.Row{
		TS: base, Line: `{"lvl":"a","n":1}`, Labels: labelPairs("app", "api"),
	})
	parsed = entry.Parsed.Map()
	if _, ok := parsed["lvl"]; ok {
		t.Fatalf("parsed = %v; second json re-extracted a dropped label", parsed)
	}
	if parsed["n"] != "1" {
		t.Fatalf("parsed = %v, want n=1", parsed)
	}
}

func TestKeepRemovesUnmatched(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	row := plan.Row{
		TS:     base,
		Line:   `{"p":"1","n":"2"}`,
		Labels: labelPairs("a", "1", "app", "api", "b", "2"),
		Meta:   labelPairs("trace", "t1"),
	}
	labels, entry, keep := processQuery(t, `{app="api"} | json | keep a, b, p`, row)
	if !keep {
		t.Fatal("keep removed the row")
	}
	// Keep traverses every category, so the stream label app is dropped too.
	if got := labels.String(); got != `{a="1", b="2"}` {
		t.Fatalf("stream labels = %s, want only kept a and b", got)
	}
	if len(entry.Meta) != 0 {
		t.Fatalf("meta = %v; keep removed trace", entry.Meta)
	}
	parsed := entry.Parsed.Map()
	if parsed["p"] != "1" || len(parsed) != 1 {
		t.Fatalf("parsed = %v, want only kept p", parsed)
	}
}

func TestKeepMatcherTargets(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	kept := func(t *testing.T, query, name string, r plan.Row) bool {
		labels, entry, keep := processQuery(t, query, r)
		if !keep {
			t.Fatal("keep removed the row")
		}
		if _, ok := labels.Get(name); ok {
			return true
		}
		_, ok := entry.Parsed.Get(name)
		_, okm := entry.Meta.Get(name)
		return ok || okm
	}

	// A matcher target must share the label's name and match its value.
	r := plan.Row{TS: base, Line: "l", Labels: labelPairs("app", "api", "x", "v1", "y", "v1")}
	if !kept(t, `{app="api"} | keep x=~"v.*"`, "x", r) {
		t.Fatal(`keep x=~"v.*" removed x="v1"`)
	}
	if kept(t, `{app="api"} | keep x=~"v.*"`, "y", r) {
		t.Fatal(`keep x=~"v.*" kept y="v1"; the matcher name must equal the label name`)
	}
	if kept(t, `{app="api"} | keep x=~"other"`, "x", r) {
		t.Fatal(`keep x=~"other" kept x="v1"`)
	}
	// An equality target keeps only the equal value.
	if !kept(t, `{app="api"} | keep x="v1"`, "x", r) {
		t.Fatal(`keep x="v1" removed x="v1"`)
	}
	if kept(t, `{app="api"} | keep x!="v1"`, "x", r) {
		t.Fatal(`keep x!="v1" kept x="v1"`)
	}
	if !kept(t, `{app="api"} | keep x!="v2"`, "x", r) {
		t.Fatal(`keep x!="v2" removed x="v1"`)
	}
}

func TestKeepPreservesSpecialLabels(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	// The error state survives keep even though no target names it.
	_, entry, _ := processQuery(t, `{app="api"} | json | keep lvl`, plan.Row{
		TS: base, Line: `not json`, Labels: labelPairs("app", "api"),
	})
	parsed := entry.Parsed.Map()
	if parsed[errorLabel] != jsonParserErr {
		t.Fatalf("parsed = %v; keep removed the error state", parsed)
	}
	if _, ok := parsed[errorDetailsLabel]; !ok {
		t.Fatalf("parsed = %v; keep removed the error details", parsed)
	}

	// Parsed fields literally named __error__/__error_details__/
	// __preserve_error__ are special and survive keep too.
	row := plan.Row{
		TS:     base,
		Line:   `{"lvl":"x","__preserve_error__":"y","dropme":1}`,
		Labels: labelPairs("app", "api"),
	}
	_, entry, _ = processQuery(t, `{app="api"} | json | keep lvl`, row)
	parsed = entry.Parsed.Map()
	if parsed["lvl"] != "x" || parsed[preserveErrorLabel] != "y" {
		t.Fatalf("parsed = %v, want lvl and __preserve_error__ kept", parsed)
	}
	if _, ok := parsed["dropme"]; ok {
		t.Fatalf("parsed = %v; dropme must be removed", parsed)
	}

	// A keep target can name __error__ without gaining power over the state:
	// the state is preserved by the special-label rule, not the target.
	_, entry, _ = processQuery(t, `{app="api"} | json | keep __error__`, plan.Row{
		TS: base, Line: `not json`, Labels: labelPairs("app", "api"),
	})
	if got := entry.Parsed.Map()[errorLabel]; got != jsonParserErr {
		t.Fatalf("parsed = %v; keep __error__ lost the state", entry.Parsed.Map())
	}
}

func TestKeepVisitsEmptyValuedLabels(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	// A label with an empty value is a real label: keep must visit it even
	// though get reports it missing. A name target keeps it.
	row := plan.Row{
		TS:     base,
		Line:   `{"p":""}`,
		Labels: labelPairs("app", "api", "x", ""),
		Meta:   labelPairs("m", ""),
	}
	labels, entry, _ := processQuery(t, `{app="api"} | json | keep x, m, p`, row)
	if _, ok := labels.Get("x"); !ok {
		t.Fatalf("keep x removed an empty-valued stream label: %v", labels)
	}
	if _, ok := entry.Meta.Get("m"); !ok {
		t.Fatalf("keep m removed an empty-valued meta label: %v", entry.Meta)
	}
	if v, ok := entry.Parsed.Get("p"); !ok || v != "" {
		t.Fatalf("keep p removed an empty-valued parsed label: %v", entry.Parsed)
	}
	// A matcher compares the empty value: x="" keeps it, x="other" drops it.
	labels, _, _ = processQuery(t, `{app="api"} | keep x=""`, plan.Row{
		TS: base, Line: "l", Labels: labelPairs("app", "api", "x", ""),
	})
	if _, ok := labels.Get("x"); !ok {
		t.Fatal(`keep x="" removed an empty-valued stream label`)
	}
	labels, _, _ = processQuery(t, `{app="api"} | keep x="other"`, plan.Row{
		TS: base, Line: "l", Labels: labelPairs("app", "api", "x", ""),
	})
	if _, ok := labels.Get("x"); ok {
		t.Fatal(`keep x="other" kept an empty-valued stream label`)
	}
	// An unkept empty label is still visited and deleted.
	labels, _, _ = processQuery(t, `{app="api"} | keep y`, plan.Row{
		TS: base, Line: "l", Labels: labelPairs("app", "api", "x", "", "y", "1"),
	})
	if _, ok := labels.Get("x"); ok {
		t.Fatal("keep y kept the unrelated empty label x")
	}
	if _, ok := labels.Get("y"); !ok {
		t.Fatal("keep y removed y")
	}
}

func TestKeepEmptyTargetsNoOp(t *testing.T) {
	// The parser rejects | keep with no targets, but a directly built stage
	// keeps every label (Loki's len(kl.labels)==0 fast path).
	st, err := compileKeep(&Keep{})
	if err != nil {
		t.Fatalf("compileKeep: %v", err)
	}
	e := newTestEntry("l", labelPairs("a", "1", "b", "2"), labelPairs("m", "3"))
	keep, err := st.process(t.Context(), e)
	if err != nil || !keep {
		t.Fatalf("process = keep %v, err %v", keep, err)
	}
	if len(e.labels) != 2 || len(e.meta) != 1 {
		t.Fatalf("empty keep changed labels: %v %v", e.labels, e.meta)
	}
}

func TestKeepSuffixedMetadataName(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	// A metadata label whose name collides with the immutable base is exposed
	// under the _extracted suffix (Loki's LabelsBuilder.Add), not as a
	// shadowing second x. Stages address it by the suffixed name.
	row := plan.Row{
		TS:     base,
		Line:   "l",
		Labels: labelPairs("app", "api", "x", "s"),
		Meta:   labelPairs("x", "meta"),
	}
	labels, entry, _ := processQuery(t, `{app="api"} | keep x`, row)
	if got := labels.String(); got != `{x="s"}` {
		t.Fatalf(`keep x labels = %s; x_extracted is a different name`, got)
	}
	if len(entry.Meta) != 0 {
		t.Fatalf(`keep x left meta %v; x_extracted is unkept`, entry.Meta)
	}
	labels, entry, _ = processQuery(t, `{app="api"} | keep x_extracted`, row)
	if len(labels) != 0 {
		t.Fatalf(`keep x_extracted left stream labels %v`, labels)
	}
	if v, ok := entry.Meta.Get("x_extracted"); !ok || v != "meta" {
		t.Fatalf(`keep x_extracted meta = %v`, entry.Meta)
	}
	// Matching by value still needs the suffixed name.
	labels, entry, _ = processQuery(t, `{app="api"} | keep x_extracted="meta"`, row)
	if v, ok := entry.Meta.Get("x_extracted"); !ok || v != "meta" {
		t.Fatalf(`keep x_extracted="meta" meta = %v`, entry.Meta)
	}
	if len(labels) != 0 {
		t.Fatalf(`keep x_extracted="meta" kept stream labels %v`, labels)
	}
	labels, entry, _ = processQuery(t, `{app="api"} | keep x="meta"`, row)
	if _, ok := entry.Meta.Get("x_extracted"); ok {
		t.Fatal(`keep x="meta" kept x_extracted; the matcher name must equal the label name`)
	}
	if len(labels) != 0 {
		t.Fatalf(`keep x="meta" kept stream labels %v (x="s" != "meta")`, labels)
	}
}

func TestEntryMetadataBaseCollision(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	row := plan.Row{
		TS:     base,
		Line:   "l",
		Labels: labelPairs("app", "api", "x", "s"),
		Meta:   labelPairs("trace", "t1", "x", "meta"),
	}
	// Plain query: the colliding metadata name arrives suffixed, the stream
	// label stays as-is and non-colliding metadata keeps its name.
	labels, entry, keep := processQuery(t, `{app="api"}`, row)
	if !keep {
		t.Fatal("row dropped")
	}
	if got := labels.String(); got != `{app="api", x="s"}` {
		t.Fatalf("stream labels = %s", got)
	}
	meta := entry.Meta.Map()
	if meta["x_extracted"] != "meta" || meta["trace"] != "t1" || len(meta) != 2 {
		t.Fatalf("meta = %v, want x_extracted=meta and trace=t1", meta)
	}
	if _, ok := meta["x"]; ok {
		t.Fatalf("meta = %v; raw x must not shadow the base name", meta)
	}
	// The base name still resolves to the stream value for label filters.
	_, _, keep = processQuery(t, `{app="api"} | x="s" | x_extracted="meta"`, row)
	if !keep {
		t.Fatal(`x="s" and x_extracted="meta" must both match`)
	}
	_, _, keep = processQuery(t, `{app="api"} | x="meta"`, row)
	if keep {
		t.Fatal(`x="meta" must not see the metadata value`)
	}
	// Drop addresses the suffixed name.
	_, entry, _ = processQuery(t, `{app="api"} | drop x_extracted`, row)
	if meta := entry.Meta.Map(); meta["x_extracted"] != "" || meta["trace"] != "t1" || len(meta) != 1 {
		t.Fatalf("meta = %v after drop x_extracted", meta)
	}
	labels, _, _ = processQuery(t, `{app="api"} | drop x`, row)
	if got := labels.String(); got != `{app="api"}` {
		t.Fatalf("labels = %s after drop x; stream x must go, meta suffix stays", got)
	}
}

func TestEntryMetadataEmptyBaseValueCollides(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	// BaseHas is a presence check, not a value check: an empty-valued stream
	// label still owns the name.
	_, entry, _ := processQuery(t, `{app="api"}`, plan.Row{
		TS:     base,
		Line:   "l",
		Labels: labelPairs("app", "api", "x", ""),
		Meta:   labelPairs("x", "meta"),
	})
	meta := entry.Meta.Map()
	if meta["x_extracted"] != "meta" || len(meta) != 1 {
		t.Fatalf("meta = %v; empty base x must still force x_extracted", meta)
	}
}

func TestEntryMetadataErrorDiversion(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	// Metadata labels named __error__/__error_details__ with no base
	// collision feed the builder error state, not the metadata category.
	row := plan.Row{
		TS:     base,
		Line:   "l",
		Labels: labelPairs("app", "api"),
		Meta:   labelPairs(errorDetailsLabel, "det", errorLabel, "boom"),
	}
	_, entry, _ := processQuery(t, `{app="api"}`, row)
	if len(entry.Meta) != 0 {
		t.Fatalf("meta = %v; error names must divert into state", entry.Meta)
	}
	parsed := entry.Parsed.Map()
	if parsed[errorLabel] != "boom" || parsed[errorDetailsLabel] != "det" {
		t.Fatalf("parsed = %v; metadata error names must surface as state", parsed)
	}
	// The diverted state is reset by the error names, like a parser error.
	_, entry, _ = processQuery(t, `{app="api"} | drop __error__`, row)
	parsed = entry.Parsed.Map()
	if _, ok := parsed[errorLabel]; ok {
		t.Fatalf("parsed = %v; drop __error__ must clear diverted state", parsed)
	}
	if parsed[errorDetailsLabel] != "det" {
		t.Fatalf("parsed = %v; drop __error__ must keep details", parsed)
	}
}

func TestEntryMetadataBaseCollidingErrorName(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	// When the base itself owns __error__, the metadata name is suffixed
	// first and never reaches the error-state diversion: it stays an ordinary
	// metadata label.
	row := plan.Row{
		TS:     base,
		Line:   "l",
		Labels: labelPairs("app", "api", errorLabel, "stream"),
		Meta:   labelPairs(errorLabel, "meta"),
	}
	labels, entry, _ := processQuery(t, `{app="api"}`, row)
	if v, ok := entry.Meta.Get("__error___extracted"); !ok || v != "meta" {
		t.Fatalf("meta = %v; colliding __error__ must become __error___extracted", entry.Meta)
	}
	if v, ok := labels.Get(errorLabel); !ok || v != "stream" {
		t.Fatalf("labels = %v; stream __error__ stays", labels)
	}
	if _, ok := entry.Parsed.Get(errorLabel); ok {
		t.Fatal("no error state may be set; the diversion ran on the suffixed name")
	}
	// drop __error__ still resets only the builder state; the stream literal
	// is unreachable by the special name, the suffixed one is ordinary.
	labels, entry, _ = processQuery(t, `{app="api"} | drop __error__`, row)
	if v, ok := labels.Get(errorLabel); !ok || v != "stream" {
		t.Fatalf("labels = %v; drop __error__ must not touch the stream literal", labels)
	}
	if _, ok := entry.Meta.Get("__error___extracted"); !ok {
		t.Fatalf("meta = %v; drop __error__ must not touch the suffixed label", entry.Meta)
	}
	labels, entry, _ = processQuery(t, `{app="api"} | drop __error___extracted`, row)
	if len(entry.Meta) != 0 {
		t.Fatalf("meta = %v; drop must address the suffixed name", entry.Meta)
	}
	if v, ok := labels.Get(errorLabel); !ok || v != "stream" {
		t.Fatalf("labels = %v; the stream __error__ literal survives the suffixed drop", labels)
	}
}

func TestEntryMetadataExtractedCollisionOrder(t *testing.T) {
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	// Meta [x, x_extracted] with base x: x is rewritten to x_extracted first,
	// then the literal x_extracted overwrites it in place — the last write in
	// source order wins, like LabelsBuilder.Set.
	_, entry, _ := processQuery(t, `{app="api"}`, plan.Row{
		TS:     base,
		Line:   "l",
		Labels: labelPairs("app", "api", "x", "s"),
		Meta:   labelPairs("x", "1", "x_extracted", "2"),
	})
	meta := entry.Meta.Map()
	if meta["x_extracted"] != "2" || len(meta) != 1 {
		t.Fatalf("meta = %v; the literal x_extracted must win in source order", meta)
	}
	// If the base also owns x_extracted, the metadata literal is suffixed
	// again: names never collide after rewriting.
	_, entry, _ = processQuery(t, `{app="api"}`, plan.Row{
		TS:     base,
		Line:   "l",
		Labels: labelPairs("app", "api", "x", "s", "x_extracted", "b"),
		Meta:   labelPairs("x", "1", "x_extracted", "2"),
	})
	meta = entry.Meta.Map()
	if meta["x_extracted"] != "1" || meta["x_extracted_extracted"] != "2" || len(meta) != 2 {
		t.Fatalf("meta = %v, want x_extracted=1 and x_extracted_extracted=2", meta)
	}
}

func TestDropKeepProcessConcurrent(t *testing.T) {
	c, err := testEngine().CompileLogQuery(`{app="api"} | json | drop n | keep lvl, app`)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	rows := []plan.Row{
		{TS: base, Line: `{"lvl":"a","n":1}`, Labels: labelPairs("app", "api", "z", "1")},
		{TS: base, Line: `{"lvl":"b","n":2}`, Labels: labelPairs("app", "api")},
		{TS: base, Line: `not json`, Labels: labelPairs("app", "api")},
	}
	var wg sync.WaitGroup
	errs := make(chan string, len(rows)*8)
	for range 8 {
		for i, row := range rows {
			wg.Go(func() {
				labels, entry, keep, err := c.Process(t.Context(), tenant.Default, row)
				if err != nil {
					errs <- err.Error()
					return
				}
				if !keep {
					errs <- fmt.Sprintf("row %d: dropped; drop/keep never drop rows", i)
					return
				}
				if labels.String() != `{app="api"}` {
					errs <- fmt.Sprintf("row %d: labels = %s", i, labels)
					return
				}
				if i < 2 && entry.Parsed.Map()["lvl"] == "" {
					errs <- fmt.Sprintf("row %d: parsed = %v", i, entry.Parsed)
				}
				if i < 2 {
					if _, ok := entry.Parsed.Map()["n"]; ok {
						errs <- fmt.Sprintf("row %d: dropped label n survived", i)
					}
				} else if entry.Parsed.Map()[errorLabel] != jsonParserErr {
					errs <- fmt.Sprintf("row %d: error state = %v", i, entry.Parsed)
				}
			})
		}
	}
	wg.Wait()
	close(errs)
	for msg := range errs {
		t.Error(msg)
	}
}

func TestDropKeepContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, query := range []string{
		`{app="api"} | drop x`,
		`{app="api"} | keep x`,
	} {
		t.Run(query, func(t *testing.T) {
			for _, st := range mustStages(t, query) {
				if _, err := st.process(ctx, newTestEntry("l", labelPairs("x", "1"), nil)); err == nil {
					t.Fatalf("%T: canceled context returned no error", st)
				}
			}
		})
	}
}

func TestDropKeepDispatchFromRealParser(t *testing.T) {
	// The dispatch must come from Parse -> compileStage, not a hand-built AST.
	stages := mustStages(t, `{app="api"} | drop a, b="1", c!="2", d=~"x.*", e!~"y.*" | keep z`)
	if len(stages) != 2 {
		t.Fatalf("stages = %d, want drop + keep", len(stages))
	}
	d, ok := stages[0].(dropStage)
	if !ok {
		t.Fatalf("stage 0 = %T, want dropStage", stages[0])
	}
	if len(d.targets) != 5 {
		t.Fatalf("drop targets = %v, want 5", d.targets)
	}
	k, ok := stages[1].(keepStage)
	if !ok || len(k.targets) != 1 {
		t.Fatalf("stage 1 = %T targets %v, want keepStage with 1 target", stages[1], k)
	}
}

func TestDropKeepQueryLogsAccepted(t *testing.T) {
	// These stages moved out of TestQueryLogsUnsupportedStages: they must now
	// evaluate instead of returning ErrUnsupported.
	base := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, q := range []string{`{app="api"} | drop x`, `{app="api"} | keep x`} {
		src := plan.NewMemSource()
		src.Add(tenant.Default, appRow(base, "l"))
		res := mustQuery(t, testEngine(), src, testParams(q, base, base.Add(time.Minute)))
		if got := len(resultLines(res)); got != 1 {
			t.Fatalf("%s: lines = %d, want the row kept", q, got)
		}
	}
}
