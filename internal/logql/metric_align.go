// Metric range grid normalization. Loki's frontend does not evaluate a metric
// range query on the raw request times: the query codec truncates the step to
// whole milliseconds (pkg/querier/queryrange/codec.go, Step.Milliseconds()),
// the splitter is enabled by default (limits.go QuerySplitDuration /
// split_queries), and metricQuerySplitter.alignStartEnd
// (pkg/querier/queryrange/splitters.go 308-318) rounds start down and end up
// onto that millisecond step grid before the engine sees them:
//
//	stepNs  = stepMs * 1e6
//	startNs = startNs - startNs%stepNs
//	endNs  += stepNs - endNs%stepNs        (when the remainder is non-zero)
//
// Go's % is a signed remainder, so negative timestamps align toward zero, not
// toward minus infinity. Instants (Step==0) and log queries bypass this
// entirely, and the engine's own window/offset arithmetic keeps full
// precision after normalization.
//
// A positive step below 1ms truncates to zero milliseconds. Loki's own splitter
// then divides by a zero step; Parkana rejects the request explicitly instead
// of panicking.
package logql

import (
	"fmt"
	"math"
	"time"

	"github.com/nathabonfim59/parkana/internal/plan"
)

// normalizeMetricParams applies Loki's metric range alignment once. Callers
// must not apply it to instants or log queries.
func normalizeMetricParams(p plan.Params) (plan.Params, error) {
	if p.Instant() {
		return p, nil
	}
	stepMs := p.Step.Milliseconds()
	if stepMs <= 0 {
		return p, fmt.Errorf("logql: %w: metric step %s is below the 1ms resolution Loki evaluates metric ranges at", plan.ErrBadQuery, p.Step)
	}
	if stepMs > math.MaxInt64/int64(time.Millisecond) {
		return p, fmt.Errorf("logql: %w: metric step %s is too large", plan.ErrBadQuery, p.Step)
	}
	startNs, ok := exactUnixNano(p.Start)
	if !ok {
		return p, fmt.Errorf("logql: %w: start %s is outside the nanosecond range", plan.ErrBadQuery, p.Start)
	}
	endNs, ok := exactUnixNano(p.End)
	if !ok {
		return p, fmt.Errorf("logql: %w: end %s is outside the nanosecond range", plan.ErrBadQuery, p.End)
	}
	stepNs := stepMs * int64(time.Millisecond)
	alignedStart := startNs - startNs%stepNs
	alignedEnd := endNs
	if mod := endNs % stepNs; mod != 0 {
		delta := stepNs - mod
		if alignedEnd > math.MaxInt64-delta {
			return p, fmt.Errorf("logql: %w: aligned end overflows the nanosecond range", plan.ErrBadQuery)
		}
		alignedEnd += delta
	}
	p.Step = time.Duration(stepMs) * time.Millisecond
	p.Start = time.Unix(0, alignedStart).UTC()
	p.End = time.Unix(0, alignedEnd).UTC()
	return p, nil
}

// exactUnixNano reports whether t has an exact UnixNano representation.
// UnixNano is undefined outside 1678-2262, and the alignment formula (like
// Loki's) works in int64 nanoseconds.
func exactUnixNano(t time.Time) (int64, bool) {
	ns := t.UnixNano()
	if !time.Unix(0, ns).Equal(t) {
		return 0, false
	}
	return ns, true
}
