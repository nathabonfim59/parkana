package query

import (
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Loki's own defaults and bounds, from pkg/loghttp in Loki 3.7.
const (
	defaultQueryLimit = 100
	defaultSince      = time.Hour
	// stepDivisor and minStep give Loki's dynamic default step
	// (defaultQueryRangeStep): floor(range/stepDivisor) whole seconds,
	// at least minStep.
	stepDivisor    = 250
	minStep        = time.Second
	maxRangePoints = 11000 // Loki's per-series point cap (errStepTooSmall)
)

// queryKind selects which endpoint's parameter set parseQueryParams applies.
type queryKind int

const (
	// instantQuery is GET/POST /loki/api/v1/query: query, limit, time,
	// direction.
	instantQuery queryKind = iota
	// rangeQuery is GET/POST /loki/api/v1/query_range: query, start, end,
	// since, step, interval, limit, direction.
	rangeQuery
)

// parseQueryParams turns r's Loki query parameters into plan.Params for the
// query handlers. kind selects the instant or range parameter set, now is the
// single request time every default derives from, and the tenant comes from
// the request context (tenant.Middleware). GET query strings and POST
// application/x-www-form-urlencoded bodies both land in r.Form.
//
// The accepted values mirror Loki 3.7's pkg/loghttp (ParseInstantQuery,
// ParseRangeQuery, determineBounds and their helpers); every parse and
// consistency failure returns an error wrapping plan.ErrBadQuery.
func parseQueryParams(r *http.Request, kind queryKind, now time.Time) (plan.Params, error) {
	if err := r.ParseForm(); err != nil {
		return plan.Params{}, fmt.Errorf("query: malformed request parameters: %w: %w", plan.ErrBadQuery, err)
	}
	p := plan.Params{Tenant: tenant.From(r.Context()), Query: r.Form.Get("query")}
	var err error
	if p.Limit, err = parseLimit(r.Form.Get("limit")); err != nil {
		return plan.Params{}, err
	}
	if p.Direction, err = parseDirection(r.Form.Get("direction")); err != nil {
		return plan.Params{}, err
	}
	if kind == instantQuery {
		t, err := parseTimestamp(r.Form.Get("time"), now)
		if err != nil {
			return plan.Params{}, fmt.Errorf("query: could not parse 'time' parameter: %w", err)
		}
		p.Start, p.End = t, t // Step stays 0: plan.Params is instant.
	} else {
		start, end, err := parseBounds(r.Form, now)
		if err != nil {
			return plan.Params{}, err
		}
		if end.Before(start) {
			return plan.Params{}, fmt.Errorf("query: 'end' must not be before 'start': %w", plan.ErrBadQuery)
		}
		p.Start, p.End = start, end
		if p.Step, err = parseStep(r.Form.Get("step"), start, end); err != nil {
			return plan.Params{}, err
		}
		if p.Step <= 0 {
			return plan.Params{}, fmt.Errorf("query: zero or negative 'step' is not accepted: %w", plan.ErrBadQuery)
		}
		if end.Sub(start)/p.Step > maxRangePoints {
			return plan.Params{}, fmt.Errorf("query: 'step' too small: more than %d points per time series: %w", maxRangePoints, plan.ErrBadQuery)
		}
		if p.Interval, err = parseSecondsOrDuration(r.Form.Get("interval"), "interval"); err != nil {
			return plan.Params{}, err
		}
		if p.Interval < 0 {
			return plan.Params{}, fmt.Errorf("query: 'interval' must be >= 0: %w", plan.ErrBadQuery)
		}
	}
	if err := p.Validate(); err != nil {
		return plan.Params{}, err
	}
	return p, nil
}

// parseBounds mirrors Loki's determineBounds: end defaults to now, start to
// min(end, now)-since, and an explicit start supersedes since.
func parseBounds(f url.Values, now time.Time) (start, end time.Time, err error) {
	since := defaultSince
	if s := f.Get("since"); s != "" {
		d, err := parsePromDuration(s)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("query: could not parse 'since' parameter: %w: %w", plan.ErrBadQuery, err)
		}
		since = d
	}
	if end, err = parseTimestamp(f.Get("end"), now); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("query: could not parse 'end' parameter: %w", err)
	}
	// An end in the future anchors since at now, not at end.
	endOrNow := end
	if end.After(now) {
		endOrNow = now
	}
	if start, err = parseTimestamp(f.Get("start"), endOrNow.Add(-since)); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("query: could not parse 'start' parameter: %w", err)
	}
	return start, end, nil
}

// parseTimestamp parses a Loki API timestamp. An empty value returns def.
// Integer strings are Unix seconds when at most 10 characters long
// (Loki's heuristic, sign included), otherwise Unix nanoseconds; decimal
// numbers with a fraction are seconds; anything else must be
// RFC3339/RFC3339Nano.
func parseTimestamp(value string, def time.Time) (time.Time, error) {
	if value == "" {
		return def, nil
	}
	if strings.Contains(value, ".") {
		if t, err := parseFloatTimestamp(value); err == nil {
			return t, nil
		}
	}
	nanos, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return t, nil
		}
		return time.Time{}, fmt.Errorf("%w: invalid timestamp %q", plan.ErrBadQuery, value)
	}
	if len(value) <= 10 {
		return time.Unix(nanos, 0), nil
	}
	return time.Unix(0, nanos), nil
}

// Bounds on decimal timestamp input: a real timestamp fits in 64 bytes, and
// bigger exponents or fractions only serve to grow big.Rat's denominator.
// Loki accepts them (it goes through a float64 anyway); rejecting keeps the
// exact parse bounded.
const (
	maxTimestampLen = 64
	maxTimestampExp = 1024
)

// parseFloatTimestamp parses a decimal number of seconds, e.g.
// "1699000000.123456789". Loki routes these through a float64 and rounds the
// fraction to milliseconds, losing precision; this parses the decimal exactly
// and rounds half away from zero to the nanosecond plan.Params keeps.
func parseFloatTimestamp(value string) (time.Time, error) {
	if len(value) > maxTimestampLen {
		return time.Time{}, fmt.Errorf("timestamp %q too long", value)
	}
	f, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return time.Time{}, err // not a float Loki would take; caller retries
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return time.Time{}, fmt.Errorf("invalid timestamp %q", value)
	}
	if i := strings.IndexAny(value, "eE"); i >= 0 {
		if e, err := strconv.Atoi(value[i+1:]); err != nil || e < -maxTimestampExp || e > maxTimestampExp {
			return time.Time{}, fmt.Errorf("timestamp %q out of range", value)
		}
	}
	// Loki timestamps are decimal; keep the exact parse to those. ParseFloat
	// and big.Rat also accept hex floats, fractions and underscores.
	if strings.IndexFunc(value, func(r rune) bool {
		return !strings.ContainsRune("0123456789.eE+-", r)
	}) >= 0 {
		return time.Time{}, fmt.Errorf("invalid timestamp %q", value)
	}
	r, ok := new(big.Rat).SetString(value)
	if !ok {
		return time.Time{}, fmt.Errorf("invalid timestamp %q", value)
	}
	r.Mul(r, big.NewRat(int64(time.Second), 1))
	num, denom := r.Num(), r.Denom()
	ns, rem := new(big.Int).QuoRem(num, denom, new(big.Int))
	if twice := new(big.Int).Lsh(new(big.Int).Abs(rem), 1); twice.Cmp(denom) >= 0 {
		if r.Sign() < 0 {
			ns.Sub(ns, big.NewInt(1))
		} else {
			ns.Add(ns, big.NewInt(1))
		}
	}
	if !ns.IsInt64() {
		return time.Time{}, fmt.Errorf("timestamp %q overflows int64 nanoseconds", value)
	}
	return time.Unix(0, ns.Int64()), nil
}

// parseLimit is Loki's limit helper: empty means the default, anything else
// must be a positive integer.
func parseLimit(value string) (int, error) {
	if value == "" {
		return defaultQueryLimit, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("query: 'limit' must be a positive integer, got %q: %w", value, plan.ErrBadQuery)
	}
	return n, nil
}

// parseDirection is Loki's direction helper: empty means backward, values are
// case-insensitive forward or backward.
func parseDirection(value string) (plan.Direction, error) {
	switch strings.ToUpper(value) {
	case "":
		return plan.Backward, nil
	case "FORWARD":
		return plan.Forward, nil
	case "BACKWARD":
		return plan.Backward, nil
	}
	return 0, fmt.Errorf("query: invalid 'direction' %q: %w", value, plan.ErrBadQuery)
}

// parseStep is Loki's step helper: empty gives the dynamic default based on
// the range, anything else is a float number of seconds or a Prometheus
// duration.
func parseStep(value string, start, end time.Time) (time.Duration, error) {
	if value == "" {
		secs := max(int64(math.Floor(end.Sub(start).Seconds()/stepDivisor)), int64(minStep/time.Second))
		return time.Duration(secs) * time.Second, nil
	}
	return parseSecondsOrDuration(value, "step")
}

// parseSecondsOrDuration parses an empty value as 0, a float as seconds and
// anything else as a Prometheus duration, like Loki's helper.
func parseSecondsOrDuration(value, name string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return 0, fmt.Errorf("query: cannot parse '%s' %q to a valid duration: %w", name, value, plan.ErrBadQuery)
		}
		ns := f * float64(time.Second)
		if ns > float64(math.MaxInt64) || ns < float64(math.MinInt64) {
			return 0, fmt.Errorf("query: '%s' %q overflows int64: %w", name, value, plan.ErrBadQuery)
		}
		return time.Duration(ns), nil
	}
	d, err := parsePromDuration(value)
	if err != nil {
		return 0, fmt.Errorf("query: cannot parse '%s' %q to a valid duration: %w: %w", name, value, plan.ErrBadQuery, err)
	}
	return d, nil
}

// promUnit looks up a Prometheus duration unit, in strictly decreasing order
// (y=365d, w=7d, d=24h), exactly the unit table prometheus/common's
// model.ParseDuration uses, which Loki's since parameter accepts — kept here
// so the querier doesn't take a new dependency.
func promUnit(s string) (pos int, mult uint64, ok bool) {
	switch s {
	case "ms":
		return 7, uint64(time.Millisecond), true
	case "s":
		return 6, uint64(time.Second), true
	case "m":
		return 5, uint64(time.Minute), true
	case "h":
		return 4, uint64(time.Hour), true
	case "d":
		return 3, uint64(24 * time.Hour), true
	case "w":
		return 2, uint64(7 * 24 * time.Hour), true
	case "y":
		return 1, uint64(365 * 24 * time.Hour), true
	}
	return 0, 0, false
}

// parsePromDuration parses "[0-9]+(y|w|d|h|m|s|ms)+" with units in decreasing
// order, exactly prometheus/common's model.ParseDuration grammar.
func parsePromDuration(s string) (time.Duration, error) {
	if s == "0" {
		return 0, nil
	}
	orig := s
	if orig == "" {
		return 0, fmt.Errorf("empty duration string")
	}
	var total uint64
	lastPos := 0
	for s != "" {
		i := 0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == 0 {
			return 0, fmt.Errorf("invalid duration %q", orig)
		}
		v, err := strconv.ParseUint(s[:i], 10, 0)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", orig)
		}
		s = s[i:]
		i = 0
		for i < len(s) && (s[i] < '0' || s[i] > '9') {
			i++
		}
		if i == 0 {
			return 0, fmt.Errorf("invalid duration %q", orig)
		}
		pos, mult, ok := promUnit(s[:i])
		if !ok {
			return 0, fmt.Errorf("unknown unit %q in duration %q", s[:i], orig)
		}
		s = s[i:]
		if pos <= lastPos {
			return 0, fmt.Errorf("invalid duration %q: units must go in order from biggest to smallest", orig)
		}
		lastPos = pos
		if v > math.MaxInt64/mult || total+v*mult > math.MaxInt64 {
			return 0, fmt.Errorf("duration %q out of range", orig)
		}
		total += v * mult
	}
	return time.Duration(total), nil
}
