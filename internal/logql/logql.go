// Package logql parses LogQL and evaluates it over a plan.Source (PLAN.md §5):
// stream selectors and line filters are pushed down as a plan.Selection,
// parsers, label filters and the step evaluation of metric queries run here.
//
// The compiled log-query API is shared by stored queries and live tail:
//
//	(*Engine).CompileLogQuery(q string) (*CompiledLogQuery, error)
//	(*CompiledLogQuery).Matchers() []plan.Matcher
//	(*CompiledLogQuery).Process(ctx context.Context, t tenant.ID, r plan.Row) (logs.Labels, plan.Entry, bool, error)
//
// A4 implements stored log execution and line filters. Metric and scalar
// expressions, parsers, label filters and formatting stages are later phases
// (A5-A9) and return plan.ErrUnsupported until they land; no stage is ever
// silently skipped.
package logql

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Engine is the LogQL plan.Engine.
type Engine struct {
	cfg config.LogQL
}

var _ plan.Engine = (*Engine)(nil)

// New returns a LogQL engine with the given limits.
func New(cfg config.LogQL) *Engine { return &Engine{cfg: cfg} }

// Query implements plan.Engine. Log queries run here; metric expressions go to
// evalMetric, which evaluates the supported range aggregations and rejects the
// later phases' families without touching the Source.
func (e *Engine) Query(ctx context.Context, src plan.Source, p plan.Params) (plan.Result, error) {
	if err := p.Validate(); err != nil {
		return plan.Result{}, err
	}
	expr, err := Parse(p.Query)
	if err != nil {
		return plan.Result{}, err
	}
	q, ok := expr.(*LogQuery)
	if !ok {
		// Metric range queries are normalized once, here, before any
		// recursion or window arithmetic: Loki's frontend truncates the step
		// to milliseconds and aligns start/end to its step grid. Log queries
		// and instant (Step==0) requests are untouched.
		p, err = normalizeMetricParams(p)
		if err != nil {
			return plan.Result{}, err
		}
		return e.evalMetric(ctx, src, expr, p)
	}
	// Loki's public instant handler rejects a log selector before building or
	// executing the pipeline: pkg/querier/http.go:122-128 requires a
	// syntax.SampleExpr. Params.Instant is Step==0; the HTTP frontend always
	// sends a positive step for range queries, so a point range (Start==End,
	// Step>0) still works while /query log requests fail as bad queries.
	if p.Instant() {
		return plan.Result{}, fmt.Errorf("logql: %w: log queries cannot be instant queries", plan.ErrBadQuery)
	}
	c, err := compileLogQuery(q)
	if err != nil {
		return plan.Result{}, err
	}
	return e.queryLogs(ctx, src, c, p)
}

// ParseSelector implements plan.Engine: it parses a bare stream selector,
// such as {app="api"}, as the /series match[] parameter and label endpoints
// use. Errors wrap plan.ErrBadQuery.
func (e *Engine) ParseSelector(s string) ([]plan.Matcher, error) {
	sel, err := ParseSelector(s)
	if err != nil {
		return nil, err
	}
	return slices.Clone(sel.Matchers), nil
}

// CompiledLogQuery is a parsed log query compiled for execution. It is
// immutable after CompileLogQuery returns and safe for concurrent Process
// calls. The unexported fields hold the pushed-down Selection and the Go-side
// pipeline, both read-only.
type CompiledLogQuery struct {
	// matchers is the stream selector, cloned from the AST.
	matchers []plan.Matcher
	// pushdown are the pre-barrier line filters plan.Selection can express.
	pushdown []plan.LineFilter
	// stages is the ordered Go-side pipeline; it always includes every line
	// filter, including the ones also pushed down.
	stages []pipelineStage
	// exact reports that no Go-side stage can drop a row the Selection
	// admits, so a Source limit may be pushed down safely.
	exact bool
}

// logEntry is the mutable row a Go-side pipeline stage sees: the final stream
// labels plus the two per-entry label categories and the line. Process owns
// the slices, so stages may mutate them without touching the Source's row.
//
// base is an independent clone of the original Row.Labels: A5/A6 need Loki's
// BaseHas semantics, where a drop or rename of a mutable label must not erase
// the fact that the label existed on the stored row. extracted is A5's
// bookkeeping set of labels a parser produced; it is nil until a parser
// lazily initializes it. err and errDetails are the builder's error state
// (LabelsBuilder.err/errDetails): it is not a label category, so Get and the
// typed filters do not see it, while a parsed field that happens to be named
// __error__ stays an ordinary parsed label. builderMutated records that any
// label was ever added to a category or deleted, the history Loki's builder
// keeps in its add/del lists: GroupedLabels only appends the retained error
// state on the modified paths, never when the builder is unchanged.
type logEntry struct {
	ts         time.Time
	line       string
	labels     logs.Labels
	base       logs.Labels
	meta       logs.Labels
	parsed     logs.Labels
	err        string
	errDetails string
	extracted  map[string]struct{}
	// builderMutated is true after any ordinary category add (metadata,
	// parser or label_format write) or any delete (drop, keep, rename). Loki's
	// hasAdd/hasDel remain true for the rest of the entry's life even when the
	// added label is later deleted, because Del never clears the del list.
	builderMutated bool
	// builderDeleted is the lazy set of names a Del removed. Loki keeps its
	// del list for the whole entry and withResult checks it before additions,
	// so a delete followed by a re-add of the same name stays hidden from a by
	// grouping while raw/without paths still see the re-added parsed label.
	builderDeleted map[string]struct{}
}

// pipelineStage is one compiled Go-side pipeline stage. A4 compiles line
// filters; A5 (parsers and label filters) and A6 (formatting, drop/keep)
// implement the rest through compileStage. process returns keep=false when
// the row is filtered out.
type pipelineStage interface {
	process(ctx context.Context, e *logEntry) (keep bool, err error)
}

// compileStage is implemented by A5's pipeline.go, which compiles every stage
// that is not a line filter. A8's unwrap returns plan.ErrUnsupported there
// until it lands.

// compileLogQuery compiles a parsed log query. Line filters before a
// line-mutating stage are also pushed into the Selection when plan can express
// them; the Go-side stages always keep every filter so Process is complete on
// its own for tail.
func compileLogQuery(q *LogQuery) (*CompiledLogQuery, error) {
	c := &CompiledLogQuery{
		matchers: slices.Clone(q.Selector.Matchers),
		exact:    true,
	}
	lineChanged := false
	for _, st := range q.Stages {
		if lf, ok := st.(*LineFilter); ok {
			m, err := compileLineFilter(lf)
			if err != nil {
				return nil, err
			}
			c.stages = append(c.stages, lineFilterStage{m: m})
			if !lineChanged {
				if fs, ok := pushableLineFilters(m); ok {
					c.pushdown = append(c.pushdown, fs...)
					continue
				}
			}
			c.exact = false
			continue
		}
		if _, ok := st.(*LineFormat); ok {
			// A line filter after line_format sees the formatted line, so
			// nothing after this barrier may be pushed down.
			lineChanged = true
		}
		c.exact = false
		ps, err := compileStage(st)
		if err != nil {
			return nil, err
		}
		c.stages = append(c.stages, ps)
	}
	return c, nil
}

// CompileLogQuery parses q and compiles it for log execution. It rejects
// metric, scalar and bare-range inputs because tail and stored log queries are
// log-only. The returned query is immutable and safe for concurrent callers.
func (e *Engine) CompileLogQuery(q string) (*CompiledLogQuery, error) {
	expr, err := Parse(q)
	if err != nil {
		return nil, err
	}
	lq, ok := expr.(*LogQuery)
	if !ok {
		return nil, fmt.Errorf("logql: %w: not a log query", plan.ErrBadQuery)
	}
	return compileLogQuery(lq)
}

// Matchers returns a copy of the compiled stream selector's matchers. Tail
// uses it to ask the ingester for matching streams; the copy keeps the
// compiled query immutable.
func (c *CompiledLogQuery) Matchers() []plan.Matcher {
	return slices.Clone(c.matchers)
}

// Process applies the compiled query to one row and reports whether it is
// kept. It is the single evaluator shared by stored queries and live tail: the
// selector is checked against Row.Labels (tail subscriptions carry all rows of
// one tenant, so the Source's Selection cannot filter them), then the pipeline
// runs over copies of the row's label slices. The returned labels are the
// final stream labels; Meta and Parsed stay per entry. The source row is never
// modified.
func (c *CompiledLogQuery) Process(ctx context.Context, t tenant.ID, r plan.Row) (logs.Labels, plan.Entry, bool, error) {
	e, keep, err := c.process(ctx, t, r)
	if err != nil || !keep {
		return nil, plan.Entry{}, keep, err
	}
	return e.labels, plan.Entry{TS: e.ts, Line: e.line, Meta: e.meta, Parsed: entryParsed(e.parsed, e.err, e.errDetails)}, true, nil
}

// process is Process without flattening the builder error state into the
// returned entry's parsed labels. Metric evaluation shares it: it needs the
// final labels, the line and the separate __error__ state, which the public
// Process API (kept frozen for tail) encodes as labels.
func (c *CompiledLogQuery) process(ctx context.Context, t tenant.ID, r plan.Row) (*logEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, fmt.Errorf("logql: process: %w", err)
	}
	if err := t.Validate(); err != nil {
		return nil, false, fmt.Errorf("logql: process: %w: %w", plan.ErrBadQuery, err)
	}
	for _, m := range c.matchers {
		if !m.MatchesLabels(r.Labels) {
			return nil, false, nil
		}
	}
	e := &logEntry{
		ts:     r.TS,
		line:   r.Line,
		labels: slices.Clone(r.Labels),
		base:   slices.Clone(r.Labels),
	}
	// Structured metadata enters through Loki's LabelsBuilder.Add, not a raw
	// copy: base collisions become _extracted and the error names feed the
	// builder state.
	addEntryMetadata(e, r.Meta)
	for _, st := range c.stages {
		keep, err := st.process(ctx, e)
		if err != nil {
			return nil, false, err
		}
		if !keep {
			return nil, false, nil
		}
	}
	return e, true, nil
}

// queryLogs runs a compiled log query. It enforces the request fields the
// shared plan validation leaves to the engine (direction and MaxEntries), then
// applies Loki's global interval sampling and result limit to the rows Process
// emits, and groups the survivors by their final stream labels.
//
// Limit 0 means "no client limit": the HTTP frontend rejects an explicit zero
// and supplies Loki's default 100, so the engine only has to cap positive
// requests against cfg.MaxEntries. Only emitted rows consume the limit;
// interval-skipped and pipeline-rejected rows do not.
func (e *Engine) queryLogs(ctx context.Context, src plan.Source, c *CompiledLogQuery, p plan.Params) (plan.Result, error) {
	// A canceled request must not open the Source at all: checking here keeps
	// every Source implementation (and any side effect it has on Select) out
	// of the canceled path.
	if err := ctx.Err(); err != nil {
		return plan.Result{}, fmt.Errorf("logql: query: %w", err)
	}
	if p.Direction != plan.Backward && p.Direction != plan.Forward {
		return plan.Result{}, fmt.Errorf("logql: invalid direction %d: %w", p.Direction, plan.ErrBadQuery)
	}
	if e.cfg.MaxEntries > 0 && p.Limit > e.cfg.MaxEntries {
		return plan.Result{}, fmt.Errorf("logql: limit %d exceeds maximum %d: %w", p.Limit, e.cfg.MaxEntries, plan.ErrLimit)
	}
	if src == nil {
		return plan.Result{}, errors.New("logql: nil source")
	}
	sel := c.selection(p)
	var (
		streams  streamSet
		emitted  int
		last     time.Time
		haveLast bool
	)
	for row, err := range src.Select(ctx, sel) {
		if err != nil {
			return plan.Result{}, fmt.Errorf("logql: select: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return plan.Result{}, fmt.Errorf("logql: select: %w", err)
		}
		labels, entry, keep, err := c.Process(ctx, p.Tenant, row)
		if err != nil {
			return plan.Result{}, err
		}
		if !keep {
			continue
		}
		// Interval sampling uses the previous emitted row, globally across
		// streams and ignoring rows the pipeline rejected.
		if p.Interval > 0 && haveLast && !intervalPass(last, entry.TS, p.Interval, p.Direction) {
			continue
		}
		streams.add(labels, entry)
		last, haveLast = entry.TS, true
		emitted++
		// Only emitted rows consume the limit.
		if p.Limit > 0 && emitted >= p.Limit {
			break
		}
	}
	return plan.Result{Type: plan.TypeStreams, Streams: streams.result()}, nil
}
