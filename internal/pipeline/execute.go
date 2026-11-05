package pipeline

import (
	"cmp"
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/expr-lang/expr/vm"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/schema"
)

// Entry is one log entry moving through a pipeline: the unit adapters
// translate logs.Entry (ingest) and plan.Row (read) to and from. TS is
// carried for round-tripping; pipelines never change it.
type Entry struct {
	TS     time.Time
	Line   string
	Labels logs.Labels
	Meta   logs.Labels
}

// Outcome is what Apply did to an entry.
type Outcome uint8

const (
	// OutcomeKeep passes the entry through; matched pipelines may continue
	// with the returned Entry.
	OutcomeKeep Outcome = iota
	// OutcomeStop keeps the entry but an on_error keep policy fired: the
	// entry carries pipeline_error/pipeline_error_details metadata and its
	// remaining steps were skipped. Callers must not run further matched
	// pipelines on it either.
	OutcomeStop
	// OutcomeDrop removes the entry; the returned Entry is meaningless.
	OutcomeDrop
)

func (o Outcome) String() string {
	switch o {
	case OutcomeKeep:
		return "keep"
	case OutcomeStop:
		return "stop"
	case OutcomeDrop:
		return "drop"
	}
	return "Outcome"
}

// Marker metadata names written by the keep policy. They are not
// '__'-prefixed, so LogQL metric queries over transformed streams stay valid;
// filter errored entries with | pipeline_error="" or | pipeline_error!="".
const (
	ErrField        = "pipeline_error"
	ErrDetailsField = "pipeline_error_details"
)

// RejectError marks an explicit on_error reject. The ingest adapter wraps it
// as ingest.ErrRejected (the whole push fails before the WAL); read
// execution reports it as a query error.
type RejectError struct {
	Pipeline string
	Step     string
	Details  string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("pipeline %s rejected at %s: %s", e.Pipeline, e.Step, e.Details)
}

// dataError is a data-dependent failure (parse failure, regex no-match,
// expression runtime error, exceeded bound). Only data errors reach the
// on_error policy; everything else aborts the call.
type dataError struct{ msg string }

func (e *dataError) Error() string { return e.msg }

func dataErrf(format string, args ...any) error {
	return &dataError{msg: fmt.Sprintf(format, args...)}
}

// Snapshot is an immutable view of a tenant's compiled pipelines, held for
// the entire execution of one request or query. Implementations must be safe
// for concurrent use.
type Snapshot interface {
	// For returns the pipelines of stage matching dataset (the entry's
	// original sanitized stream name), in name order: selector "*" matches
	// every dataset. The result is a fresh slice the caller may keep.
	For(stage Stage, dataset string) []*Compiled
	// Has reports whether the tenant has any pipeline of stage at all. A
	// read wrapper uses it to distinguish "no read pipelines anywhere"
	// (exact pass-through of the original call) from the conservative
	// tenant-wide transformed path.
	Has(stage Stage) bool
}

// fixedSnapshot is the trivial Snapshot over an explicit list, for tests,
// dry-run and registry bootstrap.
type fixedSnapshot struct {
	byStage map[Stage][]*Compiled
}

// NewFixedSnapshot builds a Snapshot over defs. Pipeline names are unique
// per tenant (the catalog's key), so the same name twice is an error even
// across stages.
func NewFixedSnapshot(defs ...*Compiled) (Snapshot, error) {
	s := &fixedSnapshot{byStage: make(map[Stage][]*Compiled)}
	seen := make(map[string]struct{})
	for _, c := range defs {
		if c == nil {
			return nil, fmt.Errorf("pipeline: nil compiled pipeline")
		}
		if _, dup := seen[c.name]; dup {
			return nil, fmt.Errorf("pipeline: duplicate pipeline %q", c.name)
		}
		seen[c.name] = struct{}{}
		s.byStage[c.stage] = append(s.byStage[c.stage], c)
	}
	for stage := range s.byStage {
		slices.SortStableFunc(s.byStage[stage], func(a, b *Compiled) int { return cmp.Compare(a.name, b.name) })
	}
	return s, nil
}

// For implements Snapshot.
func (s *fixedSnapshot) For(stage Stage, dataset string) []*Compiled {
	return Match(s.byStage[stage], stage, dataset)
}

// Has implements Snapshot.
func (s *fixedSnapshot) Has(stage Stage) bool { return len(s.byStage[stage]) > 0 }

// Match returns the subset of defs of the given stage whose selector matches
// dataset, in name order. It is the selection rule plan §1 defines and every
// Snapshot implements; the result is a fresh slice.
func Match(defs []*Compiled, stage Stage, dataset string) []*Compiled {
	out := make([]*Compiled, 0, len(defs))
	for _, c := range defs {
		if c.stage == stage && (c.selector == "*" || c.selector == dataset) {
			out = append(out, c)
		}
	}
	slices.SortStableFunc(out, func(a, b *Compiled) int { return cmp.Compare(a.name, b.name) })
	return out
}

// work is the mutable per-Apply state. It is rebuilt from the input and
// committed to step by step; a step's writes land only after its bounds
// checks pass, which is what makes "restore the entry to its state before
// the failing step" a no-op rollback.
type work struct {
	line string
	// labels and meta hold the current state, including seeded keys for the
	// names the definition's expressions reference (absent ones read "").
	// Seeded keys are not real metadata: labelSeeds/metaSeeds track them so
	// they never reach the output unless a step writes them for real.
	labels     map[string]string
	meta       map[string]string
	labelSeeds map[string]struct{}
	metaSeeds  map[string]struct{}
	// envMeta is the set of metadata names the definition's expressions
	// reference; deleting such a key (rename) must restore its seed so a
	// later expression still reads "".
	envMeta   map[string]struct{}
	metaTotal int64
	opts      CompileOptions
}

func newWork(c *Compiled, in Entry) *work {
	w := &work{
		line:       in.Line,
		labels:     make(map[string]string, len(in.Labels)+len(c.envLabels)),
		meta:       make(map[string]string, len(in.Meta)+len(c.envMeta)),
		labelSeeds: make(map[string]struct{}, len(c.envLabels)),
		metaSeeds:  make(map[string]struct{}, len(c.envMeta)),
		envMeta:    c.envMeta,
		opts:       c.opts,
	}
	for _, l := range in.Labels {
		w.labels[l.Name] = l.Value
	}
	for k := range c.envLabels {
		if _, ok := w.labels[k]; !ok {
			w.labels[k] = ""
			w.labelSeeds[k] = struct{}{}
		}
	}
	for _, m := range in.Meta {
		w.meta[m.Name] = m.Value
		w.metaTotal += int64(len(m.Name) + len(m.Value))
	}
	for k := range c.envMeta {
		if _, ok := w.meta[k]; !ok {
			w.meta[k] = ""
			w.metaSeeds[k] = struct{}{}
		}
	}
	return w
}

// metaCost returns the bytes a metadata key currently counts toward
// metaTotal (0 for absent or seeded keys).
func (w *work) metaCost(k string) int64 {
	if _, seeded := w.metaSeeds[k]; seeded {
		return 0
	}
	if v, ok := w.meta[k]; ok {
		return int64(len(k) + len(v))
	}
	return 0
}

// setMeta writes one metadata field, updating the running total. Bounds are
// the caller's (commit) responsibility.
func (w *work) setMeta(k, v string) {
	w.metaTotal -= w.metaCost(k)
	delete(w.metaSeeds, k)
	w.meta[k] = v
	w.metaTotal += int64(len(k) + len(v))
}

// checkInputBounds verifies the entry Apply received: line, total metadata
// and every label value. Oversized input is a data error so it flows through
// the first step's on_error policy instead of being amplified.
func (w *work) checkInputBounds() error {
	if int64(len(w.line)) > int64(w.opts.LineBytes) {
		return dataErrf("line exceeds %d bytes", w.opts.LineBytes)
	}
	if w.metaTotal > int64(w.opts.MetaBytes) {
		return dataErrf("metadata exceeds %d bytes", w.opts.MetaBytes)
	}
	for k, v := range w.meta {
		if _, seeded := w.metaSeeds[k]; seeded {
			continue
		}
		if int64(len(v)) > int64(w.opts.MetaValueBytes) {
			return dataErrf("metadata value %q exceeds %d bytes", k, w.opts.MetaValueBytes)
		}
	}
	for k, v := range w.labels {
		if _, seeded := w.labelSeeds[k]; seeded {
			continue
		}
		if int64(len(v)) > int64(w.opts.LabelValueBytes) {
			return dataErrf("label value %q exceeds %d bytes", k, w.opts.LabelValueBytes)
		}
	}
	return nil
}

// markError writes the keep-policy markers. Marker writes are exempt from
// the metadata bounds: an error report must not itself fail. Details are
// truncated to one line of at most MaxErrorDetails bytes on a UTF-8 rune
// boundary, so the value can always be re-encoded as JSON.
func (w *work) markError(step StepType, err error) {
	details := err.Error()
	if i := strings.IndexByte(details, '\n'); i >= 0 {
		details = details[:i]
	}
	if len(details) > MaxErrorDetails {
		details = truncateUTF8(details, MaxErrorDetails)
	}
	w.setMeta(ErrField, string(step))
	w.setMeta(ErrDetailsField, details)
}

// truncateUTF8 cuts s to at most n bytes without splitting a rune.
func truncateUTF8(s string, n int) string {
	if n >= len(s) {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// entry builds the output Entry: fresh Labels and Meta (seeded keys never
// materialize), the current line and the input's timestamp.
func (w *work) entry(ts time.Time) Entry {
	labels := make(logs.Labels, 0, len(w.labels))
	for k, v := range w.labels {
		if _, seeded := w.labelSeeds[k]; seeded {
			continue
		}
		labels = append(labels, logs.Label{Name: k, Value: v})
	}
	slices.SortFunc(labels, func(a, b logs.Label) int { return strings.Compare(a.Name, b.Name) })
	meta := make(logs.Labels, 0, len(w.meta))
	for k, v := range w.meta {
		if _, seeded := w.metaSeeds[k]; seeded {
			continue
		}
		meta = append(meta, logs.Label{Name: k, Value: v})
	}
	slices.SortFunc(meta, func(a, b logs.Label) int { return strings.Compare(a.Name, b.Name) })
	return Entry{TS: ts, Line: w.line, Labels: labels, Meta: meta}
}

// source returns the parse step's input: the line or a metadata field.
func (w *work) source(st *compiledStep) (string, error) {
	if st.fromMeta == "" {
		return w.line, nil
	}
	v, ok := w.meta[st.fromMeta]
	if !ok {
		return "", dataErrf("source field %q is missing", st.fromMeta)
	}
	if _, seeded := w.metaSeeds[st.fromMeta]; seeded {
		return "", dataErrf("source field %q is missing", st.fromMeta)
	}
	return v, nil
}

// env builds the per-evaluation environment: the current state plus the
// original dataset as stream. Maps are shared with work; expressions only
// read them.
func (w *work) env(dataset string) map[string]any {
	return map[string]any{"line": w.line, "labels": w.labels, "meta": w.meta, "stream": dataset}
}

// run evaluates prog and converts every failure into a data error.
func (w *work) run(prog *vm.Program, dataset string) (any, error) {
	v := vm.VM{MemoryBudget: VMMemoryBudget}
	out, err := v.Run(prog, w.env(dataset))
	if err != nil {
		return nil, dataErrf("expression failed: %v", err)
	}
	return out, nil
}

// Apply runs in through the pipeline's steps in order. dataset is the
// entry's ORIGINAL sanitized stream name — adapters compute it once (for
// example with DatasetOf) and pass the same value to every matched
// pipeline; set_stream never re-selects and expressions see the original
// stream. Apply never modifies in or any array it references.
//
// The returned Entry shares no mutable state with in: Labels and Meta are
// fresh slices (unless a label step changed them, out.Labels is rebuilt
// anyway) and strings are immutable. On OutcomeDrop the returned Entry is
// meaningless. A *RejectError means an explicit on_error reject; any other
// error aborts the call (cancellation or an internal fault), and callers
// must check err before using the outcome.
func (c *Compiled) Apply(ctx context.Context, dataset string, in Entry) (out Entry, outcome Outcome, err error) {
	if c.opts.ObserveExecution != nil {
		start := time.Now()
		defer func() {
			c.opts.ObserveExecution(c.stage, outcome, err, time.Since(start))
		}()
	}
	if dataset == "" {
		return Entry{}, OutcomeKeep, fmt.Errorf("pipeline: Apply requires a dataset")
	}
	// Cancellation wins over everything, including oversized input: a
	// canceled call must never answer keep/drop.
	if err := ctx.Err(); err != nil {
		return Entry{}, OutcomeKeep, fmt.Errorf("pipeline: %w", err)
	}
	w := newWork(c, in)

	// Policy applies the first step's on_error to pre-step input faults.
	policy := func(cs *compiledStep, err error) (Entry, Outcome, error) {
		switch cs.onErr {
		case OnErrorKeep:
			w.markError(cs.spec.Type, err)
			return w.entry(in.TS), OutcomeStop, nil
		case OnErrorDrop:
			return Entry{}, OutcomeDrop, nil
		default:
			return Entry{}, OutcomeKeep, &RejectError{Pipeline: c.name, Step: string(cs.spec.Type), Details: err.Error()}
		}
	}
	fail := func(i int, err error) (Entry, Outcome, error) {
		return policy(&c.steps[i], err)
	}

	if err := w.checkInputBounds(); err != nil {
		return fail(0, err)
	}
	for i := range c.steps {
		if err := ctx.Err(); err != nil {
			return Entry{}, OutcomeKeep, fmt.Errorf("pipeline: %w", err)
		}
		cs := &c.steps[i]
		if cs.when != nil {
			gate, err := w.run(cs.when, dataset)
			if err != nil {
				return fail(i, err)
			}
			b, ok := gate.(bool)
			if !ok {
				return fail(i, dataErrf("when did not evaluate to a boolean"))
			}
			if !b {
				continue
			}
		}
		var err error
		switch cs.spec.Type {
		case StepParseJSON:
			err = w.execParseJSON(ctx, cs)
		case StepParseLogfmt:
			err = w.execParseLogfmt(ctx, cs)
		case StepRegexExtract:
			err = w.execRegexExtract(cs)
		case StepRename:
			err = w.execRename(cs)
		case StepRedact:
			err = w.execRedact(cs)
		case StepDropIf:
			out, evalErr := w.run(cs.dropIf, dataset)
			if evalErr == nil {
				b, ok := out.(bool)
				switch {
				case !ok:
					evalErr = dataErrf("drop_if did not evaluate to a boolean")
				case b:
					return Entry{}, OutcomeDrop, nil
				}
			}
			err = evalErr
		case StepPromote:
			err = w.execPromote(cs, dataset)
		case StepSetStream:
			err = w.execSetStream(cs, dataset)
		default:
			return Entry{}, OutcomeKeep, fmt.Errorf("pipeline: unknown compiled step %q", cs.spec.Type)
		}
		if err != nil {
			// Cancellation is never a data error: it must not reach
			// on_error (keep/drop/reject). The parse steps check ctx
			// between fields and containers, so a canceled call that got
			// this far still answers with an operation error.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return Entry{}, OutcomeKeep, fmt.Errorf("pipeline: %w", ctxErr)
			}
			return fail(i, err)
		}
	}
	return w.entry(in.TS), OutcomeKeep, nil
}

// execParseJSON parses the step's source as a JSON object, flattening nested
// objects to underscore-joined leaves. The parser itself enforces the metadata
// total and value bounds while it walks, so an amplified nested prefix fails
// at the bound instead of being materialized first, and it checks ctx between
// fields and containers.
func (w *work) execParseJSON(ctx context.Context, cs *compiledStep) error {
	return w.execParse(ctx, cs, func(ctx context.Context, src string) ([]extractedField, error) {
		return parseJSONFields(ctx, src, int64(w.opts.MetaBytes), int64(w.opts.MetaValueBytes))
	})
}

// execParseLogfmt parses the step's source as logfmt. Pairs keep their source
// order, so when two keys sanitize to the same name the later pair wins
// (Loki sanitizes logfmt keys the same way, pkg/logql/log/parser.go:398).
func (w *work) execParseLogfmt(ctx context.Context, cs *compiledStep) error {
	return w.execParse(ctx, cs, func(_ context.Context, src string) ([]extractedField, error) {
		pairs, err := parseLogfmt(src)
		if err != nil {
			return nil, err
		}
		fields := make([]extractedField, 0, len(pairs))
		for _, p := range pairs {
			fields = append(fields, extractedField{name: sanitizeName(p.Key), value: p.Value})
		}
		return fields, nil
	})
}

// execParse reads the step's source and runs parse over it. Parse failures are
// data errors (on_error applies); a canceled context is returned as-is so it
// is never mistaken for a data error.
func (w *work) execParse(ctx context.Context, cs *compiledStep, parse func(context.Context, string) ([]extractedField, error)) error {
	src, err := w.source(cs)
	if err != nil {
		return err
	}
	fields, err := parse(ctx, src)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return dataErrf("%v", err)
	}
	// A cancellation that lands after a successful parse but before the
	// commit must still be an operation error, not a data error.
	if err := ctx.Err(); err != nil {
		return err
	}
	return w.execExtract(cs, fields)
}

// execExtract merges parsed fields into metadata. Names arrive sanitized and
// ordered; a name that still starts with "__" is rewritten to
// "extracted__..." because pipelines must never write reserved metadata (an
// extra Parkana protection, not Loki behavior). A repeated name is merged
// later-occurrence-wins in the parser's source order (logfmt pair order, regex
// capture order, JSON member order), never Go map order. Everything is
// validated and size-checked before any field is written: on failure the
// metadata is exactly what it was before the step.
func (w *work) execExtract(cs *compiledStep, fields []extractedField) error {
	order := make([]string, 0, len(fields))
	values := make(map[string]string, len(fields))
	for _, f := range fields {
		name := protectName(f.name)
		if err := validLabelNameErr(name); err != nil {
			return dataErrf("extracted field %q is not a valid metadata name", f.name)
		}
		if int64(len(f.value)) > int64(w.opts.MetaValueBytes) {
			return dataErrf("extracted field %q exceeds %d bytes", name, w.opts.MetaValueBytes)
		}
		if _, ok := values[name]; !ok {
			order = append(order, name)
		}
		values[name] = f.value
	}
	var delta int64
	for _, name := range order {
		delta += int64(len(name)+len(values[name])) - w.metaCost(name)
	}
	if w.metaTotal+delta > int64(w.opts.MetaBytes) {
		return dataErrf("metadata would exceed %d bytes", w.opts.MetaBytes)
	}
	for _, name := range order {
		w.setMeta(name, values[name]) // extracted values overwrite existing ones
	}
	return nil
}

// execRegexExtract merges the named captures of the FIRST match into
// metadata; no match is a data error. Captures never remove metadata. Capture
// names are Go's [A-Za-z0-9_]+ (leading digits and "__" are legal in Go), so
// they are sanitized rather than compile-rejected (Loki's regexp parser does
// the same at pkg/logql/log/parser.go:331).
func (w *work) execRegexExtract(cs *compiledStep) error {
	src, err := w.source(cs)
	if err != nil {
		return err
	}
	loc := cs.re.FindSubmatchIndex([]byte(src))
	if loc == nil {
		return dataErrf("pattern does not match")
	}
	fields := make([]extractedField, 0, len(cs.re.SubexpNames()))
	for i, name := range cs.re.SubexpNames() {
		if name == "" || 2*i+1 >= len(loc) || loc[2*i] < 0 {
			continue
		}
		fields = append(fields, extractedField{name: sanitizeName(name), value: src[loc[2*i]:loc[2*i+1]]})
	}
	return w.execExtract(cs, fields)
}

// execRename moves one metadata field to another; a missing source is a
// no-op. The move can grow the total (a longer destination name), so it is
// bounded like every other metadata write.
func (w *work) execRename(cs *compiledStep) error {
	from, to := cs.spec.From, cs.spec.To
	if from == to {
		return nil
	}
	if _, seeded := w.metaSeeds[from]; seeded {
		return nil
	}
	v, ok := w.meta[from]
	if !ok {
		return nil
	}
	delta := int64(len(to)+len(v)) - w.metaCost(from) - w.metaCost(to)
	if w.metaTotal+delta > int64(w.opts.MetaBytes) {
		return dataErrf("metadata would exceed %d bytes", w.opts.MetaBytes)
	}
	w.metaTotal += delta
	delete(w.meta, from)
	delete(w.metaSeeds, to)
	w.meta[to] = v
	// A referenced source key must keep reading "" after its deletion:
	// restore the seed.
	if _, ref := w.envMeta[from]; ref {
		w.meta[from] = ""
		w.metaSeeds[from] = struct{}{}
	}
	return nil
}

// execRedact replaces every pattern match with the replacement template
// (Go regexp semantics) in the line or one metadata field. A missing
// metadata target or no match is a no-op. The output size is computed
// exactly from the actual matches before anything is materialized, so an
// expanding replacement fails the bound cheaply.
func (w *work) execRedact(cs *compiledStep) error {
	if cs.targetMeta != "" {
		if _, seeded := w.metaSeeds[cs.targetMeta]; seeded {
			return nil // absent field: nothing to redact
		}
		v, ok := w.meta[cs.targetMeta]
		if !ok {
			return nil
		}
		out, err := w.redactString(cs, v, int64(w.opts.MetaValueBytes))
		if err != nil {
			return err
		}
		delta := int64(len(cs.targetMeta)+len(out)) - w.metaCost(cs.targetMeta)
		if w.metaTotal+delta > int64(w.opts.MetaBytes) {
			return dataErrf("metadata would exceed %d bytes", w.opts.MetaBytes)
		}
		w.setMeta(cs.targetMeta, out)
		return nil
	}
	out, err := w.redactString(cs, w.line, int64(w.opts.LineBytes))
	if err != nil {
		return err
	}
	w.line = out
	return nil
}

// redactString applies the replacement to src with limit as the exact output
// bound. The match search itself is capped: the total capture-index budget
// bounds the FindAllSubmatchIndex allocation before the size precheck runs.
func (w *work) redactString(cs *compiledStep, src string, limit int64) (string, error) {
	perMatch := 2 * (1 + len(cs.re.SubexpNames())) // ints per match
	maxMatches := MaxMatchIndexes / perMatch
	matches := cs.re.FindAllSubmatchIndex([]byte(src), maxMatches+1)
	if len(matches) > maxMatches {
		return "", dataErrf("redact match budget of %d indexes exceeded", MaxMatchIndexes)
	}
	if len(matches) == 0 {
		return src, nil
	}
	// Retained bytes + per-match expanded sizes; early-exit keeps the sum
	// small even for pathological patterns.
	var kept, out int64
	prev := 0
	for _, m := range matches {
		kept += int64(m[0] - prev)
		prev = m[1]
		out += expandLen(cs.re, cs.replace, src, m)
		if kept+out > limit {
			return "", dataErrf("redacted output would exceed %d bytes", limit)
		}
	}
	kept += int64(len(src) - prev)
	if kept+out > limit {
		return "", dataErrf("redacted output would exceed %d bytes", limit)
	}
	res := cs.re.ReplaceAllString(src, cs.replace)
	if int64(len(res)) > limit {
		// The sizer's safety net: it must overestimate, never underestimate.
		return "", dataErrf("redacted output would exceed %d bytes", limit)
	}
	return res, nil
}

// expandLen returns the length of re.Expand applied to template with match m
// of src, mirroring Go's $name/${name}/$1/$$ expansion exactly.
func expandLen(re *regexp.Regexp, template, src string, match []int) int64 {
	var n int64
	for len(template) > 0 {
		before, after, ok := strings.Cut(template, "$")
		if !ok {
			break
		}
		n += int64(len(before))
		template = after
		if template != "" && template[0] == '$' {
			n++
			template = template[1:]
			continue
		}
		name, num, rest, ok := extractRef(template)
		if !ok {
			n++ // malformed ref: '$' is literal
			continue
		}
		template = rest
		if num >= 0 {
			if 2*num+1 < len(match) && match[2*num] >= 0 {
				n += int64(match[2*num+1] - match[2*num])
			}
			continue
		}
		for i, namei := range re.SubexpNames() {
			if name == namei && 2*i+1 < len(match) && match[2*i] >= 0 {
				n += int64(match[2*i+1] - match[2*i])
				break
			}
		}
	}
	return n + int64(len(template))
}

// extractRef is Go's regexp extract: the leading name or {name} of a
// reference, with its group number when the name is all digits. Names may
// contain any Unicode letter or digit (plus '_'), exactly like Go.
func extractRef(str string) (name string, num int, rest string, ok bool) {
	if str == "" {
		return
	}
	brace := false
	if str[0] == '{' {
		brace = true
		str = str[1:]
	}
	i := 0
	for i < len(str) {
		r, size := utf8.DecodeRuneInString(str[i:])
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			break
		}
		i += size
	}
	if i == 0 {
		return
	}
	name = str[:i]
	if brace {
		if i >= len(str) || str[i] != '}' {
			return
		}
		i++
	}
	num = 0
	for j := 0; j < len(name); j++ {
		if name[j] < '0' || '9' < name[j] || num >= 1e8 {
			num = -1
			break
		}
		num = num*10 + int(name[j]-'0')
	}
	// Go disallows leading zeros: "$00" is the name "00", not group 0.
	if name[0] == '0' && len(name) > 1 {
		num = -1
	}
	rest = str[i:]
	ok = true
	return
}

// execPromote copies a string into a label: a literal metadata field (a
// missing one is a no-op) or a string expression. An empty result copies an
// empty string; the label is never removed.
func (w *work) execPromote(cs *compiledStep, dataset string) error {
	v, err := w.valueOrField(cs, dataset)
	if err != nil || v == nil {
		return err // nil = no-op: source field missing
	}
	if int64(len(*v)) > int64(w.opts.LabelValueBytes) {
		return dataErrf("promoted value exceeds %d bytes", w.opts.LabelValueBytes)
	}
	// Case collision against a live label of this entry (the compile-time
	// check only sees the definition's own targets) would create two labels
	// that differ only in case.
	for name := range w.labels {
		if _, seeded := w.labelSeeds[name]; seeded {
			continue
		}
		if schema.Collide(name, cs.spec.To) {
			return dataErrf("label %q differs only in case from %q", name, cs.spec.To)
		}
	}
	w.setLabel(cs.spec.To, *v)
	return nil
}

// execSetStream ALWAYS writes the sanitized value to the first configured
// routing label; an empty value routes to logs.DefaultStream, so a later
// routing label can never take over by accident.
func (w *work) execSetStream(cs *compiledStep, dataset string) error {
	v, err := w.valueOrField(cs, dataset)
	if err != nil || v == nil {
		return err
	}
	w.setLabel(w.opts.StreamLabels[0], logs.SanitizeStream(*v))
	return nil
}

// valueOrField resolves promote/set_stream's source: a string from the
// compiled expression or a metadata literal. A missing literal field is a
// no-op (nil, nil).
func (w *work) valueOrField(cs *compiledStep, dataset string) (*string, error) {
	if cs.value != nil {
		out, err := w.run(cs.value, dataset)
		if err != nil {
			return nil, err
		}
		s, ok := out.(string)
		if !ok {
			return nil, dataErrf("value did not evaluate to a string")
		}
		return &s, nil
	}
	from := cs.spec.From
	if _, seeded := w.metaSeeds[from]; seeded {
		return nil, nil
	}
	if v, ok := w.meta[from]; ok {
		return &v, nil
	}
	return nil, nil
}

// setLabel writes one label value (bounds already checked by the caller).
func (w *work) setLabel(name, value string) {
	delete(w.labelSeeds, name)
	w.labels[name] = value
}
