// Metadata endpoints of Loki's read API (PLAN.md §8, part B2c): label
// names, label values, series, index stats and index volume. The types here
// are the private contract between the HTTP handlers in metadata_http.go and
// the reader that answers them from stored data; the SQL implementation
// arrives with the normalized relation shared with the row Source.

package query

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// metaQuery bounds and filters one metadata read.
//
// Unlike plan.Selection, whose window is [From, Through), metadata requests
// keep Loki's index semantics: Start and End are both inclusive. The label
// and series endpoints accept an inverted range (End before Start) and simply
// find nothing, exactly as Loki's index lookups do; the stats and volume
// handlers reject it before getting here. Groups OR-combined stream selector
// groups, AND within a group; nil means no selector, so the request stays
// tenant and time scoped without ever building an invalid plan.Selection.
type metaQuery struct {
	Tenant tenant.ID
	Start  time.Time
	End    time.Time
	// Groups are already parsed and validated (plan.NewMatcher built).
	Groups [][]plan.Matcher
}

// metaStats is the result of /index/stats, shaped like Loki's
// IndexStatsResponse (logproto) but filled with the parent-approved Parquet
// meanings, which deliberately deviate from Loki's: Streams counts the
// distinct full label sets with at least one entry in range, Entries the
// matching rows, Bytes the sum of len(line) in UTF-8 bytes (the same
// definition as plan.Bucket.Bytes) and Chunks the Parquet files the query
// touched. Loki instead estimates its chunks and bytes from index chunk
// statistics; Parkana measures rows and files and invents no numbers.
type metaStats struct {
	Streams, Chunks, Bytes, Entries uint64
}

// metaVolumeRequest is /index/volume and /index/volume_range. Volume sums the
// exact line bytes with the same parent-approved definition as
// metaStats.Bytes, so a volume total over a range equals the stats bytes for
// the same selector.
type metaVolumeRequest struct {
	Query metaQuery
	// Limit caps the names kept per time bucket, Loki's default 100.
	Limit int
	// Step is the range resolution, normalized to whole milliseconds after
	// the range validation, exactly as Loki's frontend codec converts it
	// (Step.Milliseconds()); 0 means an instant request, whose single
	// bucket covers the whole range.
	Step time.Duration
	// TargetLabels projects the grouping onto these labels only and adds a
	// presence filter for each one the selector does not already name; nil
	// projects series onto the selector's matcher names (the full label set
	// without a selector) or groups labels mode by every label name.
	TargetLabels []string
	// AggregateByLabels selects Loki's aggregateBy=labels; false is the
	// aggregateBy=series default.
	AggregateByLabels bool
}

// metaVolumeSample is one bucket's volume for one grouped name.
type metaVolumeSample struct {
	// End is the bucket's reported timestamp, not its counting window's
	// end: one millisecond before the next bucket boundary, or the
	// request's Through for the final bucket, the values Loki pins on the
	// wire (queryrange/volume.go's ForInterval end alignment with its 1ms
	// split gap).
	End time.Time
	// Volume is the summed line bytes of the bucket, over the approved
	// partition: non-final buckets count [bucketStart, nextBoundary) and
	// the final bucket counts [bucketStart, Through], so every row in the
	// request range is counted exactly once, with no gap and no whole-
	// second truncation of the kind Loki's child requests introduce.
	Volume uint64
}

// metaVolumeEntry is one grouped name across the request's buckets.
type metaVolumeEntry struct {
	// Metric is the result series' label set: the (projected) stream label
	// set for aggregateBy=series, or the single label name with an empty
	// value for aggregateBy=labels, as Loki re-labels those results.
	Metric logs.Labels
	// Buckets are ordered by End and hold only the buckets where the name
	// survived the per-bucket Limit; names may skip buckets.
	Buckets []metaVolumeSample
}

// metaReader answers the metadata endpoints from stored data. It is private
// to the querier role; the DuckDB implementation shares file listing, leases,
// SQL predicates and retry behavior with the row Source, and never scans rows
// into Go to aggregate them. LabelNames, LabelValues and Series return sorted
// distinct results; an inverted time range yields empty results, not an
// error. Errors wrapping plan.ErrBadQuery or plan.ErrUnsupported are the
// client's fault; anything else fails the request.
type metaReader interface {
	// LabelNames returns the sorted distinct label names of every stream
	// with an entry in [Start, End] matching Groups.
	LabelNames(ctx context.Context, q metaQuery) ([]string, error)
	// LabelValues returns the sorted distinct values of name across the
	// matching streams.
	LabelValues(ctx context.Context, q metaQuery, name string) ([]string, error)
	// Series returns the distinct matching label sets, sorted by their
	// canonical string.
	Series(ctx context.Context, q metaQuery) ([]logs.Labels, error)
	// Stats counts the matching data (see metaStats for the field
	// meanings).
	Stats(ctx context.Context, q metaQuery) (metaStats, error)
	// Volume returns the grouped volumes of metaVolumeRequest. Entries are
	// sorted by the first bucket's volume descending, then by the metric
	// string ascending (Loki's result order), and the per-bucket Limit is
	// already applied. Buckets partition the ms-rounded request range
	// exactly (see metaVolumeSample), start at the request's Start for the
	// first bucket, then at whole step boundaries, and report Loki's pinned
	// sample timestamps. Grouping follows Loki's PrepareLabelsAndMatchers:
	// series volume projects onto the selector's matcher names (the full
	// label set only without a selector), targetLabels project onto those
	// names with a target=~".+" presence filter wherever the selector does
	// not name the target, and labels volume groups every label name (or
	// only the targets when given). An empty targetLabels element (trailing
	// or consecutive comma) yields a successful empty result, as pinned live
	// Loki 3.7.9 does.
	Volume(ctx context.Context, req metaVolumeRequest) ([]metaVolumeEntry, error)
}

// parseMetaQuerySelector parses the single query selector that the labels,
// index stats and volume endpoints take. An empty query means no selector on
// the labels endpoints and is rejected for stats and volume, where Loki's
// ParseMatchers fails on it. allowEmpty treats the bare "{}" (spaces ignored)
// as no selector, which Loki's volume path tolerates while its label and
// stats paths let the parser reject it.
func parseMetaQuerySelector(parse func(string) ([]plan.Matcher, error), query string, allowEmpty bool) ([][]plan.Matcher, error) {
	if query == "" {
		return nil, nil
	}
	if allowEmpty && isEmptySelector(query) {
		return nil, nil
	}
	ms, err := parse(query)
	if err != nil {
		return nil, err
	}
	return [][]plan.Matcher{ms}, nil
}

// parseMetaMatchSelectors parses the match[] parameters of the series
// endpoint. Loki accepts both match and match[], deduplicates them, and
// treats a sole "{}" (spaces ignored) as if no selector was supplied at all.
// The returned groups are OR-combined.
func parseMetaMatchSelectors(parse func(string) ([]plan.Matcher, error), matches []string) ([][]plan.Matcher, error) {
	if len(matches) == 0 {
		return nil, nil
	}
	deduped := sortedUniqueStrings(matches)
	if len(deduped) == 1 && isEmptySelector(deduped[0]) {
		return nil, nil
	}
	groups := make([][]plan.Matcher, 0, len(deduped))
	for _, m := range deduped {
		ms, err := parse(m)
		if err != nil {
			return nil, err
		}
		groups = append(groups, ms)
	}
	return groups, nil
}

// isEmptySelector reports whether s is "{}" ignoring spaces, the bare empty
// selector Loki special-cases in its series and volume paths.
func isEmptySelector(s string) bool {
	return strings.ReplaceAll(s, " ", "") == "{}"
}

// sortedUniqueStrings returns the sorted distinct values of in, mirroring the
// deduplication Loki's ParseSeriesQuery applies to match[] before sorting
// them.
func sortedUniqueStrings(in []string) []string {
	return slices.Compact(slices.Sorted(slices.Values(in)))
}

// roundToMilliseconds widens a [start, end] range to whole milliseconds
// before index stats and volume queries, with the exact arithmetic of Loki's
// util.RoundToMilliseconds — compatibility quirks included, because Loki's
// endpoint computes the same expressions. For a post-epoch sub-millisecond
// start the division floors and the end ceilings (the end only when it has
// sub-millisecond precision); both bounds stay inclusive. Before the Unix
// epoch the division truncates toward zero instead, so the widening runs the
// other way and the end's +1ms can overshoot by up to a millisecond; and
// outside the years UnixNano can represent (about 1678 to 2262) the
// nanosecond value wraps and the result is meaningless. Those inputs pin
// this function to Loki's behavior rather than to true floor/ceiling.
func roundToMilliseconds(start, end time.Time) (time.Time, time.Time) {
	from := time.UnixMilli(start.UnixNano() / int64(time.Millisecond))
	through := time.UnixMilli(end.UnixNano() / int64(time.Millisecond))
	if end.Nanosecond()%int(time.Millisecond) != 0 {
		through = through.Add(time.Millisecond)
	}
	return from, through
}
