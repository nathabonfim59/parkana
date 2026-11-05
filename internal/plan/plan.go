// Package plan is the contract between Parkana's query language front ends
// and the executors that read stored logs (PLAN.md §5, decision 4).
//
// A front end (internal/logql today, SQL later) decides what to read and
// hands the executor a Selection; the executor (internal/query, DuckDB over
// Parquet) returns matching Rows, or pre-aggregated Buckets, through the
// Source interface. Everything that can be pushed down to storage is
// expressed here, and nothing else: parsers, label filters, formatting and
// the step evaluation of metric queries stay in the front end.
//
// The semantics of every Selection field are pinned down by the conformance
// suite in plantest, which every Source must pass. MemSource is the reference
// implementation the suite compares against.
package plan

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Errors a front end returns for queries it can't run. HTTP handlers map all
// three to 400 with the error text, Loki style; anything else is a 500.
var (
	// ErrBadQuery means the query doesn't parse or is invalid LogQL.
	ErrBadQuery = errors.New("bad query")
	// ErrUnsupported means valid LogQL that Parkana doesn't implement yet.
	ErrUnsupported = errors.New("unsupported")
	// ErrLimit means the query exceeds a configured limit (series, range, ...).
	ErrLimit = errors.New("limit exceeded")
)

// MatchType is a label matcher's operator.
type MatchType int

// Label matcher operators: =, !=, =~ and !~.
const (
	MatchEqual MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

func (t MatchType) String() string {
	switch t {
	case MatchEqual:
		return "="
	case MatchNotEqual:
		return "!="
	case MatchRegexp:
		return "=~"
	case MatchNotRegexp:
		return "!~"
	}
	return "MatchType(" + strconv.Itoa(int(t)) + ")"
}

// Matcher is one label matcher of a stream selector, e.g. env=~"prod|dev".
//
// Semantics follow Loki/Prometheus: a missing label has the value "", so
// {foo=""} matches rows without foo and {foo=~".*"} matches every row.
// Regexps are RE2 and fully anchored: env=~"pro" does not match "prod".
// Build matchers with NewMatcher; the zero value is not usable.
type Matcher struct {
	Type  MatchType
	Name  string
	Value string
	re    *regexp.Regexp
}

// NewMatcher validates and compiles a matcher.
func NewMatcher(t MatchType, name, value string) (Matcher, error) {
	m := Matcher{Type: t, Name: name, Value: value}
	if name == "" {
		return Matcher{}, fmt.Errorf("plan: matcher with empty label name: %w", ErrBadQuery)
	}
	switch t {
	case MatchEqual, MatchNotEqual:
	case MatchRegexp, MatchNotRegexp:
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return Matcher{}, fmt.Errorf("plan: matcher %s%s%q: %w: %w", name, t, value, ErrBadQuery, err)
		}
		m.re = re
	default:
		return Matcher{}, fmt.Errorf("plan: unknown match type %d: %w", t, ErrBadQuery)
	}
	return m, nil
}

// MustMatcher is NewMatcher for tests and constants; it panics on error.
func MustMatcher(t MatchType, name, value string) Matcher {
	m, err := NewMatcher(t, name, value)
	if err != nil {
		panic(err)
	}
	return m
}

// Matches reports whether a label value matches; pass "" for a missing label.
func (m Matcher) Matches(v string) bool {
	switch m.Type {
	case MatchEqual:
		return v == m.Value
	case MatchNotEqual:
		return v != m.Value
	case MatchRegexp:
		return m.re.MatchString(v)
	case MatchNotRegexp:
		return !m.re.MatchString(v)
	}
	return false
}

// MatchesLabels reports whether ls passes the matcher.
func (m Matcher) MatchesLabels(ls logs.Labels) bool {
	v, _ := ls.Get(m.Name)
	return m.Matches(v)
}

// MatchesEmpty reports whether the matcher accepts a missing label. Loki
// rejects selectors where every matcher does.
func (m Matcher) MatchesEmpty() bool { return m.Matches("") }

func (m Matcher) String() string { return m.Name + m.Type.String() + strconv.Quote(m.Value) }

// LineFilterType is a line filter's operator.
type LineFilterType int

// Line filter operators: |=, !=, |~ and !~.
const (
	LineContains LineFilterType = iota
	LineNotContains
	LineRegexp
	LineNotRegexp
)

func (t LineFilterType) String() string {
	switch t {
	case LineContains:
		return "|="
	case LineNotContains:
		return "!="
	case LineRegexp:
		return "|~"
	case LineNotRegexp:
		return "!~"
	}
	return "LineFilterType(" + strconv.Itoa(int(t)) + ")"
}

// LineFilter is one pushed-down line filter.
//
// Contains is a case-sensitive byte substring test (strings.Contains): no
// wildcards, so an executor using SQL LIKE must escape % and _. Regexps are
// RE2 and unanchored (Loki's |~), the same dialect DuckDB's regexp_matches
// uses. Build filters with NewLineFilter.
type LineFilter struct {
	Type  LineFilterType
	Value string
	re    *regexp.Regexp
}

// NewLineFilter validates and compiles a line filter.
func NewLineFilter(t LineFilterType, value string) (LineFilter, error) {
	f := LineFilter{Type: t, Value: value}
	switch t {
	case LineContains, LineNotContains:
	case LineRegexp, LineNotRegexp:
		re, err := regexp.Compile(value)
		if err != nil {
			return LineFilter{}, fmt.Errorf("plan: line filter %s%q: %w: %w", t, value, ErrBadQuery, err)
		}
		f.re = re
	default:
		return LineFilter{}, fmt.Errorf("plan: unknown line filter type %d: %w", t, ErrBadQuery)
	}
	return f, nil
}

// MustLineFilter is NewLineFilter for tests and constants; it panics on error.
func MustLineFilter(t LineFilterType, value string) LineFilter {
	f, err := NewLineFilter(t, value)
	if err != nil {
		panic(err)
	}
	return f
}

// Matches reports whether line passes the filter.
func (f LineFilter) Matches(line string) bool {
	switch f.Type {
	case LineContains:
		return strings.Contains(line, f.Value)
	case LineNotContains:
		return !strings.Contains(line, f.Value)
	case LineRegexp:
		return f.re.MatchString(line)
	case LineNotRegexp:
		return !f.re.MatchString(line)
	}
	return false
}

func (f LineFilter) String() string { return f.Type.String() + " " + strconv.Quote(f.Value) }

// Direction orders rows by timestamp.
type Direction int

const (
	// Backward is newest first, Loki's default for log queries.
	Backward Direction = iota
	// Forward is oldest first.
	Forward
)

func (d Direction) String() string {
	if d == Forward {
		return "forward"
	}
	return "backward"
}

// Selection says which rows to read. All conditions are ANDed.
type Selection struct {
	Tenant tenant.ID
	// Matchers select streams by label. At least one is required.
	Matchers []Matcher
	// LineFilters apply to the line, in any order (they commute).
	LineFilters []LineFilter
	// From is inclusive, Through exclusive: From <= TS < Through. Both are
	// required. Timestamps are stored in µs; front ends that need Loki's
	// left-open windows (t-range, t] shift by a microsecond themselves.
	From, Through time.Time
	// Direction orders Select's rows. Rows with equal TS come in an
	// unspecified order. Aggregate ignores it.
	Direction Direction
	// Limit caps the rows Select yields; 0 means no limit. With a limit the
	// rows are the first Limit in Direction order. Aggregate ignores it.
	Limit int
}

// Validate checks the fields every Source relies on.
func (s Selection) Validate() error {
	var errs []error
	if err := s.Tenant.Validate(); err != nil {
		errs = append(errs, err)
	}
	if len(s.Matchers) == 0 {
		errs = append(errs, errors.New("no matchers"))
	}
	for _, m := range s.Matchers {
		if m.Name == "" || (m.Type == MatchRegexp || m.Type == MatchNotRegexp) && m.re == nil {
			errs = append(errs, fmt.Errorf("matcher %s not built with NewMatcher", m))
		}
	}
	for _, f := range s.LineFilters {
		if (f.Type == LineRegexp || f.Type == LineNotRegexp) && f.re == nil {
			errs = append(errs, fmt.Errorf("line filter %s not built with NewLineFilter", f))
		}
	}
	if s.From.IsZero() || s.Through.IsZero() {
		errs = append(errs, errors.New("from and through are required"))
	}
	if s.Limit < 0 {
		errs = append(errs, errors.New("negative limit"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("plan: invalid selection: %w", err)
	}
	return nil
}

// Matches reports whether a row of tenant t passes the selection's tenant,
// matchers, line filters and time range.
func (s Selection) Matches(t tenant.ID, r Row) bool {
	if t != s.Tenant || r.TS.Before(s.From) || !r.TS.Before(s.Through) {
		return false
	}
	for _, m := range s.Matchers {
		if !m.MatchesLabels(r.Labels) {
			return false
		}
	}
	for _, f := range s.LineFilters {
		if !f.Matches(r.Line) {
			return false
		}
	}
	return true
}

// Row is one stored log entry.
type Row struct {
	// TS is the entry timestamp, µs precision, UTC.
	TS time.Time
	// Labels are the stream labels: promoted label columns (non-null) and
	// the labels map merged, sorted by name.
	Labels logs.Labels
	// Meta is the entry's structured metadata (the meta column), sorted.
	Meta logs.Labels
	Line string
}

// BucketAgg asks Aggregate for per-bucket totals instead of rows, so count
// and bytes range aggregations don't stream every row to the front end.
//
// Bucket k covers [Origin + k*Width, Origin + (k+1)*Width). Origin can be any
// instant (before, inside or after the selection); rows before it land in
// negative buckets. Only rows inside the Selection are counted.
type BucketAgg struct {
	Width  time.Duration // > 0, a whole number of µs
	Origin time.Time
	// IncludeMeta also groups by the rows' structured metadata (Row.Meta)
	// and returns it in Bucket.Meta. Loki keeps structured metadata in a
	// metric series' identity unless the query projects it away, so a front
	// end sets this when the metadata can reach the result. Default false:
	// buckets group by stream labels only and Bucket.Meta is nil.
	IncludeMeta bool
}

// Validate checks the bucket parameters.
func (b BucketAgg) Validate() error {
	if b.Width <= 0 || b.Width%time.Microsecond != 0 {
		return fmt.Errorf("plan: bucket width %s must be a positive whole number of µs", b.Width)
	}
	return nil
}

// Start returns the start of the bucket holding ts.
func (b BucketAgg) Start(ts time.Time) time.Time {
	d := ts.Sub(b.Origin)
	k := d / b.Width
	if d%b.Width < 0 {
		k--
	}
	return b.Origin.Add(k * b.Width)
}

// Bucket is one Aggregate result: the rows of one label set in one bucket.
type Bucket struct {
	Start  time.Time
	Labels logs.Labels
	// Meta is the rows' structured metadata when BucketAgg.IncludeMeta is
	// set (sorted; empty for rows without metadata), else nil.
	Meta logs.Labels
	// Count is the number of rows; Bytes the sum of len(Line) in bytes.
	Count, Bytes int64
}

// Source reads stored rows. Implementations must pass plantest.RunSource.
//
// Duplicate rows count once: rows with the same TS, Labels (the complete
// stream label set, however it was stored), Meta and Line are one row for
// Select (before ordering and Limit) and for Aggregate (before grouping, so
// Count and Bytes count each distinct row once). Collectors deliver at least
// once (an Alloy retry after a timeout re-sends a batch the ingester already
// made durable); Loki hides those copies at ingest, Parkana at read time.
// Rows that differ in any field, including Meta, are distinct.
//
// Both methods validate their arguments and yield a single error for an
// invalid one. A canceled ctx ends iteration with an error wrapping ctx.Err().
// Yielded Labels and Meta may share backing arrays between rows: treat them
// as read-only.
type Source interface {
	// Select yields the selected rows in Direction order, at most Limit.
	Select(ctx context.Context, sel Selection) iter.Seq2[Row, error]
	// Aggregate yields one Bucket per (bucket, label set) holding at least
	// one selected row, in no particular order; with agg.IncludeMeta, one per
	// (bucket, label set, metadata set).
	Aggregate(ctx context.Context, sel Selection, agg BucketAgg) iter.Seq2[Bucket, error]
}
