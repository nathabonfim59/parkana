package plan

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Params are one query request, as the Loki HTTP API receives it.
type Params struct {
	Tenant tenant.ID
	Query  string
	// Start and End bound a range query. Log queries select [Start, End), as
	// Loki does; metric queries evaluate steps from Start through End
	// inclusive. An instant query has Start == End == its time.
	Start, End time.Time
	// Step is the range query resolution; 0 for instant queries.
	Step time.Duration
	// Interval is Loki's optional log query sampling interval; 0 for none.
	Interval time.Duration
	// Limit caps entries for log queries (Loki's default is 100).
	Limit     int
	Direction Direction
}

// Instant reports whether p is an instant query.
func (p Params) Instant() bool { return p.Step == 0 }

// Validate checks the request-level fields.
func (p Params) Validate() error {
	var errs []error
	if err := p.Tenant.Validate(); err != nil {
		errs = append(errs, err)
	}
	if p.Query == "" {
		errs = append(errs, errors.New("empty query"))
	}
	if p.Start.IsZero() || p.End.IsZero() || p.End.Before(p.Start) {
		errs = append(errs, errors.New("start and end are required, end >= start"))
	}
	if p.Step < 0 || p.Step == 0 && !p.Start.Equal(p.End) {
		errs = append(errs, errors.New("step must be positive for range queries"))
	}
	if p.Limit < 0 || p.Interval < 0 {
		errs = append(errs, errors.New("negative limit or interval"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("plan: invalid params: %w: %w", ErrBadQuery, err)
	}
	return nil
}

// Engine evaluates queries over a Source. internal/logql implements it for
// LogQL; HTTP handlers call it and encode the Result.
type Engine interface {
	// Query runs a log or metric query. Errors wrapping ErrBadQuery,
	// ErrUnsupported or ErrLimit are the client's fault (HTTP 400).
	Query(ctx context.Context, src Source, p Params) (Result, error)
	// ParseSelector parses a bare stream selector such as {app="api"}, as
	// used by /series match[] and the query parameter of the label
	// endpoints. Errors wrap ErrBadQuery or ErrUnsupported.
	ParseSelector(s string) ([]Matcher, error)
}

// ResultType names a Result's payload, with Loki's resultType values.
type ResultType string

// Result types.
const (
	TypeStreams ResultType = "streams"
	TypeMatrix  ResultType = "matrix"
	TypeVector  ResultType = "vector"
	TypeScalar  ResultType = "scalar"
)

// Result is a query result; only the field named by Type is set.
type Result struct {
	Type    ResultType
	Streams []Stream
	Matrix  []Series
	Vector  []Sample
	Scalar  Point
}

// Stream is a log query result stream: entries sharing a label set.
type Stream struct {
	// Labels are the stream labels after the pipeline, without the
	// structured metadata and parsed labels kept per entry. Encoders
	// that don't categorize labels (no X-Loki-Response-Encoding-Flags:
	// categorize-labels) merge all three and regroup.
	Labels  logs.Labels
	Entries []Entry
}

// Entry is one log line in a result.
type Entry struct {
	TS   time.Time
	Line string
	// Meta is the entry's structured metadata; Parsed holds labels the
	// pipeline extracted (json, logfmt, regexp, pattern). Both sorted.
	Meta, Parsed logs.Labels
}

// Series is one matrix series.
type Series struct {
	Metric logs.Labels
	Points []Point
}

// Point is one sample value at a time.
type Point struct {
	T time.Time
	V float64
}

// Sample is one vector element.
type Sample struct {
	Metric logs.Labels
	Point
}
