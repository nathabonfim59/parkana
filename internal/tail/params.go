package tail

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Public tail parameters. The defaults are pinned to Loki v3.7.9:
// pkg/loghttp/params.go defaultQueryLimit = 100, tailDelay defaults to 0 and
// ParseTailQuery rejects delay_for > maxDelayForInTailing = 5 seconds
// (pkg/loghttp/tail.go).
const (
	defaultTailLimit = 100
	maxDelayFor      = 5 // seconds
)

// tailRequest is one validated public tail request.
type tailRequest struct {
	query string
	limit int
	// delayFor holds back entries until now >= timestamp + delayFor, which
	// lets a querier merge timestamp order across ingesters.
	delayFor time.Duration
	// start filters incoming rows; it is zero when the client did not ask
	// for a lower bound. Parkana does not backfill history, so Loki's
	// implied now-1h default (which only bounds its store query) has no
	// observable effect and is not applied to live rows.
	start time.Time
}

// parseTailRequest validates the public query string before any upgrade.
// Duplicate values are rejected even though Loki's parser takes the first
// one: silently ignoring a second limit or start would hide client bugs.
func parseTailRequest(r *http.Request) (tailRequest, error) {
	if err := r.ParseForm(); err != nil {
		return tailRequest{}, errors.New("invalid query parameters")
	}
	for _, name := range []string{"query", "limit", "delay_for", "start"} {
		if len(r.Form[name]) > 1 {
			return tailRequest{}, fmt.Errorf("parameter %q must be specified at most once", name)
		}
	}
	req := tailRequest{query: r.Form.Get("query")}
	if strings.TrimSpace(req.query) == "" {
		return tailRequest{}, errors.New("missing query parameter")
	}
	limit, err := parseTailLimit(r.Form.Get("limit"))
	if err != nil {
		return tailRequest{}, err
	}
	req.limit = limit
	delay, err := parseTailDelay(r.Form.Get("delay_for"))
	if err != nil {
		return tailRequest{}, err
	}
	req.delayFor = delay
	if raw := r.Form.Get("start"); raw != "" {
		start, err := parseTailTimestamp(raw)
		if err != nil {
			return tailRequest{}, fmt.Errorf("invalid start: %w", err)
		}
		req.start = start
	}
	return req, nil
}

// parseTailLimit applies Loki's limit rules: default 100 and a positive
// integer. The caller separately caps it with the configured MaxEntries.
func parseTailLimit(raw string) (int, error) {
	if raw == "" {
		return defaultTailLimit, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid limit %q", raw)
	}
	if n <= 0 {
		return 0, errors.New("limit must be a positive value")
	}
	return n, nil
}

// parseTailDelay parses the delay in whole seconds. Loki's uint32 conversion
// turns a negative value into a huge one and then reports the maximum; a
// direct "must be positive" error is clearer and still a Loki-style 400.
func parseTailDelay(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid delay_for %q", raw)
	}
	if n < 0 {
		return 0, errors.New("delay_for must be a positive value")
	}
	if n > maxDelayFor {
		return 0, fmt.Errorf("delay_for can't be greater than %d", maxDelayFor)
	}
	return time.Duration(n) * time.Second, nil
}

// maxUnixSeconds bounds a float start timestamp's integral seconds so
// time.Unix cannot overflow its internal int64 representation. It is far
// outside any real log window, and the float path rejects anything at or
// beyond it (including the unrepresentable 2^63 float boundary) before
// casting. Integer inputs are already bounded: at most ten digits means
// seconds, and longer values are nanoseconds, both safe for time.Unix.
const maxUnixSeconds = int64(1) << 62

// parseTailTimestamp mirrors Loki's loghttp.parseTimestamp: a decimal
// fraction is Unix seconds rounded to MILLISECONDS (Loki does
// math.Round(fraction*1000)/1000), an integer of at most 10 digits is Unix
// seconds, a longer integer is Unix nanoseconds, and anything else must be
// RFC3339Nano. Overflow, NaN and infinities are rejected as invalid input
// rather than clamped.
func parseTailTimestamp(value string) (time.Time, error) {
	if strings.Contains(value, ".") {
		if f, err := strconv.ParseFloat(value, 64); err == nil {
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return time.Time{}, fmt.Errorf("invalid timestamp %q", value)
			}
			seconds, fraction := math.Modf(f)
			// Compare in float space before the int64 cast: MaxInt64
			// rounds to 2^63 as a float64, so a naive > MaxInt64 check
			// misses exactly that boundary and int64(2^63) wraps.
			if seconds >= float64(maxUnixSeconds) || seconds <= -float64(maxUnixSeconds) {
				return time.Time{}, fmt.Errorf("timestamp %q is out of range", value)
			}
			nanos := math.Round(fraction*1000) / 1000 * float64(time.Second)
			return time.Unix(int64(seconds), int64(nanos)).UTC(), nil
		}
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		if ts, rfcErr := time.Parse(time.RFC3339Nano, value); rfcErr == nil {
			return ts.UTC(), nil
		}
		return time.Time{}, fmt.Errorf("invalid timestamp %q", value)
	}
	if len(value) <= 10 {
		return time.Unix(n, 0).UTC(), nil
	}
	return time.Unix(0, n).UTC(), nil
}
