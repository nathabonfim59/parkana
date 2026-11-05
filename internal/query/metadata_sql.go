// The SQL implementation of the metaReader contract (PLAN.md §8, part B2c):
// label names, label values, series, index stats and index volume become one
// fully aggregated DuckDB statement each, compiled over the same normalized
// relation the row Source reads (normRelation) and run through the same
// listing/lease/retry attempt boundary (openRows). Go never scans rows to
// aggregate them; it only assembles and sorts the preaggregated results.
//
// Approved semantics pinned here (lead/orchestrator review of this package):
//
//   - Inclusive index bounds: a row exactly at q.End is matched (Through is
//     End plus one nanosecond, which the µs-ceiling comparison turns into
//     ts <= End); nanosecond endpoints are exact over the whole-microsecond
//     stored timestamps.
//   - Several selector groups never multiply rows: a sole group's matchers
//     push down into the group subqueries, while several groups share one
//     matcher-free distinct relation filtered by a boolean OR across groups
//     (Loki's union semantics without the duplicate reads). The identity
//     DISTINCT runs before the OR projection, so an exact stored row counts
//     once even when both arms match it, while rows differing only in metadata
//     survive for stats and volume.
//   - Rows with the same TS, complete canonical labels, canonical metadata and
//     line are one stored row (the Source contract): at-least-once delivery
//     duplicates count once in stats entries/bytes and volume, before any
//     target-label or name projection.
//   - Stats chunks counts the object paths the successful attempt acquired
//     (the Parquet files the statement read), including when zero rows match.
//   - Bytes are octet_length(encode(line)): len(line) in UTF-8 bytes. DuckDB
//     has no VARCHAR overload of octet_length, so the line is encoded to a
//     BLOB first (the B1c Aggregate finding).
//   - Volume windows follow the pinned util.ForInterval grid (epoch-aligned,
//     computed in Unix milliseconds), partitioned continuously: [Start, G1),
//     [Gi, Gi+1) and a final [G(n-1), Through] that includes Through, with
//     Loki's boundary-minus-1ms sample timestamps for non-final buckets. A
//     Start==End request keeps exactly one final POINT bucket at End — the
//     approved exception where Loki's ForInterval emits no bucket at all.
package query

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/schema"
)

// metaLabelsMapSQL re-reads the canonical labels_json of the normalized
// relation as a case-sensitive MAP(VARCHAR, VARCHAR): labels_json is built by
// normRelation as the complete key-sorted label map (promoted columns merged
// over the labels map), so the cast recovers exactly that map for name/value
// access and grouping without any JSON path parsing.
const metaLabelsMapSQL = "CAST(CAST(labels_json AS JSON) AS MAP(VARCHAR, VARCHAR))"

// metaSQLReader answers the metadata endpoints from the Parquet objects the
// shared source machinery reads. It owns nothing: the role that owns the
// *sql.DB, the catalog and the resolver owns the wrapped source, and closing
// them closes the reader with it.
type metaSQLReader struct {
	src *source
}

// newMetadataReader returns the metaReader answering metadata queries over
// src. The wrapper holds no resources of its own, does no I/O and has no
// Close: src's owner's lifetime is its lifetime.
func newMetadataReader(src *source) metaReader { return metaSQLReader{src: src} }

// LabelNames implements metaReader: the sorted distinct label names of every
// stream with an entry in q's inclusive window matching q's selectors. The
// DISTINCT name sort over the normalized relation block-reads every input
// file before the first row, so openRows' disappearance retry holds.
func (r metaSQLReader) LabelNames(ctx context.Context, q metaQuery) ([]string, error) {
	rows, err := r.open(ctx, q, func(groups []fileGroup) selectQuery {
		c := compiler{}
		c.printf("SELECT DISTINCT name FROM (SELECT unnest(map_keys(%s)) AS name FROM (", metaLabelsMapSQL)
		metaRelation(&c, q, q.Groups, groups)
		c.printf(")) ORDER BY name")
		return selectQuery{sql: c.b.String(), args: c.args}
	})
	if err != nil {
		return nil, err
	}
	return rows.collectAll(func(rows *primedRows) (string, error) {
		var name string
		if err := rows.Scan(&name); err != nil {
			return "", fmt.Errorf("query: scan label name: %w", err)
		}
		return name, nil
	})
}

// LabelValues implements metaReader: the sorted distinct values of name over
// the same matched rows. An absent label yields no value (NULL reads are
// filtered); an empty-string value is a value, not an absence. A name DuckDB
// cannot bind as a VARCHAR is looked up by its bytes, exactly as the label
// matchers are (see metaMapValueSQL).
func (r metaSQLReader) LabelValues(ctx context.Context, q metaQuery, name string) ([]string, error) {
	rows, err := r.open(ctx, q, func(groups []fileGroup) selectQuery {
		c := compiler{}
		c.printf("SELECT DISTINCT value FROM (SELECT ")
		metaMapValueSQL(&c, metaLabelsMapSQL, name)
		c.printf(" AS value FROM (")
		metaRelation(&c, q, q.Groups, groups)
		c.printf(")) WHERE value IS NOT NULL ORDER BY value")
		return selectQuery{sql: c.b.String(), args: c.args}
	})
	if err != nil {
		return nil, err
	}
	return rows.collectAll(func(rows *primedRows) (string, error) {
		var value string
		if err := rows.Scan(&value); err != nil {
			return "", fmt.Errorf("query: scan label value: %w", err)
		}
		return value, nil
	})
}

// Series implements metaReader: the distinct matching label sets, sorted by
// their canonical string. The canonical labels_json is injective over label
// sets, so DISTINCT deduplicates exactly the sets, never raw rows; the sort
// runs in Go on logs.Labels' canonical form, which is Loki's ordering key.
func (r metaSQLReader) Series(ctx context.Context, q metaQuery) ([]logs.Labels, error) {
	rows, err := r.open(ctx, q, func(groups []fileGroup) selectQuery {
		c := compiler{}
		c.printf("SELECT DISTINCT labels_json FROM (")
		metaRelation(&c, q, q.Groups, groups)
		c.printf(") ORDER BY labels_json")
		return selectQuery{sql: c.b.String(), args: c.args}
	})
	if err != nil {
		return nil, err
	}
	sets, err := rows.collectAll(func(rows *primedRows) (logs.Labels, error) {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("query: scan series: %w", err)
		}
		return decodeLabelsJSON(key)
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(sets, func(a, b logs.Labels) int { return cmp.Compare(a.String(), b.String()) })
	return sets, nil
}

// Stats implements metaReader with the parent-approved Parquet meanings:
// streams is COUNT(DISTINCT labels_json) over the matched distinct rows,
// entries the count of those rows, bytes SUM(octet_length(encode(line))), and
// chunks the number of object paths the successful attempt acquired — the
// Parquet files the statement read, including when zero rows match the
// selectors (chunks stays 0 when the listing is empty, because then no
// statement runs at all). Exact stored copies (the Source identity) count
// once; metadata-only differences stay distinct.
func (r metaSQLReader) Stats(ctx context.Context, q metaQuery) (metaStats, error) {
	var chunks int
	rows, err := r.open(ctx, q, func(groups []fileGroup) selectQuery {
		chunks = 0
		for _, g := range groups {
			chunks += len(g.paths)
		}
		c := compiler{}
		c.printf("SELECT COUNT(*), COUNT(DISTINCT labels_json), CAST(COALESCE(SUM(octet_length(encode(%s))), 0) AS BIGINT) FROM (", quoteIdent(schema.ColLine))
		metaRelation(&c, q, q.Groups, groups)
		c.printf(")")
		return selectQuery{sql: c.b.String(), args: c.args}
	})
	if err != nil {
		return metaStats{}, err
	}
	if rows == nil {
		return metaStats{}, nil // empty listing: nothing was read or touched
	}
	var entries, streams, bytes int64
	for rows.Next() {
		if err := rows.Scan(&entries, &streams, &bytes); err != nil {
			return metaStats{}, errors.Join(fmt.Errorf("query: scan stats: %w", err), rows.Close())
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return metaStats{}, err
	}
	return metaStats{Streams: uint64(streams), Chunks: uint64(chunks), Bytes: uint64(bytes), Entries: uint64(entries)}, nil
}

// Volume implements metaReader: per-bucket line-byte sums grouped by the
// pinned projection (the selector's matcher names by default, the target
// labels when given) or by label name, the per-bucket Limit already applied.
// SQL aggregates every bucket × group total; Go only trims, orders and stamps
// the buckets (see volumeGrid for the window partition and sample
// timestamps).
func (r metaSQLReader) Volume(ctx context.Context, req metaVolumeRequest) ([]metaVolumeEntry, error) {
	q := req.Query
	if q.End.Before(q.Start) {
		return nil, nil // handlers reject this; a defensive empty answer, not an error
	}
	grid := newVolumeGrid(q.Start, q.End, req.Step)
	shape, err := newVolumeShape(q, req.TargetLabels)
	if err != nil {
		return nil, err
	}
	if shape.empty {
		return nil, nil // Loki keeps empty CSV elements and answers an empty result
	}

	rows, err := r.open(ctx, q, func(groups []fileGroup) selectQuery {
		return metaVolumeSQL(req, grid, shape, groups)
	})
	if err != nil {
		return nil, err
	}

	// volAgg is one preaggregated bucket total; name is Loki's ordering key:
	// the raw label name for aggregateBy=labels (whose Metric.String(),
	// {name=""}, orders differently), the label-set string otherwise.
	type volAgg struct {
		name   string
		metric logs.Labels
		volume uint64
	}
	byBucket := make(map[int64][]volAgg)
	if rows != nil {
		for rows.Next() {
			var idx int64
			var key string
			var volume int64
			if err := rows.Scan(&idx, &key, &volume); err != nil {
				return nil, errors.Join(fmt.Errorf("query: scan volume: %w", err), rows.Close())
			}
			var agg volAgg
			if req.AggregateByLabels {
				agg = volAgg{name: key, metric: logs.FromMap(map[string]string{key: ""}), volume: uint64(volume)}
			} else {
				metric, err := decodeLabelsJSON(key)
				if err != nil {
					return nil, errors.Join(err, rows.Close())
				}
				agg = volAgg{name: metric.String(), metric: metric, volume: uint64(volume)}
			}
			byBucket[idx] = append(byBucket[idx], agg)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return nil, err
		}
	}

	// Per bucket: Loki's order (volume desc, name asc) and the Limit trim,
	// so a name survives only the buckets where it stayed within the top
	// Limit; names may skip buckets.
	idxs := slices.Sorted(maps.Keys(byBucket))
	for _, idx := range idxs {
		aggs := byBucket[idx]
		slices.SortFunc(aggs, func(a, b volAgg) int {
			if c := cmp.Compare(b.volume, a.volume); c != 0 {
				return c
			}
			return cmp.Compare(a.name, b.name)
		})
		if req.Limit > 0 && len(aggs) > req.Limit {
			aggs = aggs[:req.Limit]
		}
		byBucket[idx] = aggs
	}

	// Assemble entries in bucket order (sample timestamps grow with the
	// index), then order them by their first surviving bucket's volume
	// descending and name ascending, Loki's result order.
	type volEntry struct {
		entry    metaVolumeEntry
		name     string
		firstVol uint64
	}
	byName := make(map[string]*volEntry, len(byBucket))
	var ordered []*volEntry
	for _, idx := range idxs {
		for _, agg := range byBucket[idx] {
			e := byName[agg.name]
			if e == nil {
				e = &volEntry{entry: metaVolumeEntry{Metric: agg.metric}, name: agg.name, firstVol: agg.volume}
				byName[agg.name] = e
				ordered = append(ordered, e)
			}
			e.entry.Buckets = append(e.entry.Buckets, metaVolumeSample{End: grid.sampleEnd(idx, q.End), Volume: agg.volume})
		}
	}
	slices.SortFunc(ordered, func(a, b *volEntry) int {
		if c := cmp.Compare(b.firstVol, a.firstVol); c != 0 {
			return c
		}
		return cmp.Compare(a.name, b.name)
	})
	entries := make([]metaVolumeEntry, 0, len(ordered))
	for _, e := range ordered {
		entries = append(entries, e.entry)
	}
	return entries, nil
}

// volumeShape is one volume request's pinned grouping (Loki's
// util.PrepareLabelsAndMatchers): matcherGroups are the effective OR arms with
// target presence matchers added where needed, projected is the label-name
// projection for aggregateBy=series (and, when targets are given, for
// aggregateBy=labels), and labelsFilter restricts aggregateBy=labels only when
// target labels were given explicitly. A nil projected keeps the full label
// set — the approved bare-{} behavior and an OR arm that matches everything.
type volumeShape struct {
	matcherGroups [][]plan.Matcher
	projected     []string
	labelsFilter  []string
	// empty marks a request whose targetLabels CSV held an empty element. The
	// live pinned Loki answers such a request (trailing, consecutive or sole
	// empty elements) with HTTP 200 and an empty result: its appended presence
	// matcher has an empty label name, which no series satisfies.
	empty bool
}

// newVolumeShape derives the pinned grouping for req.
//
// Without targetLabels, series volume projects onto the union of the selector
// matcher names, exactly as Loki's index does; a selector-free request or one
// with a match-all arm (a private empty group) keeps full-label grouping, the
// approved Parkana exception. With targetLabels, the projection is those names
// and every target not already named by a matcher in an arm gets Loki's
// `target=~".+"` presence matcher in that arm: a series missing the target is
// excluded unless some matcher in its arm already names it (Loki only checks
// that a matcher names the target, not its type). Presence matchers go into
// cloned arms, so the caller's selector and its matcher slices stay immutable.
// labelsFilter stays nil without explicit targets: aggregateBy=labels groups
// every label name of the matched rows, and the selector's matcher names are a
// series-mode projection only. An empty targetLabels element (from "env," or a
// leading/consecutive comma) marks the shape empty: the pinned Loki answers
// those with a successful empty result, which Volume returns without listing.
func newVolumeShape(q metaQuery, targets []string) (volumeShape, error) {
	targets = sortedUniqueStrings(targets)
	if slices.Contains(targets, "") {
		return volumeShape{empty: true}, nil
	}
	if len(targets) == 0 {
		return volumeShape{matcherGroups: q.Groups, projected: volumeMatcherNames(q.Groups)}, nil
	}
	presence := func(group []plan.Matcher) ([]plan.Matcher, error) {
		augmented := slices.Clone(group)
		for _, target := range targets {
			if slices.ContainsFunc(group, func(m plan.Matcher) bool { return m.Name == target }) {
				continue
			}
			// Loki's pinned Prometheus matcher parses regexps with syntax.DotNL
			// and wraps them in ^(?s:...)$, so target=~".+" accepts any non-empty
			// value, newlines included. This is a PRIVATE implicit predicate:
			// spell the dot-all form explicitly instead of widening plan
			// Matcher's ordinary Go regexp semantics.
			m, err := plan.NewMatcher(plan.MatchRegexp, target, "(?s:.+)")
			if err != nil {
				return nil, fmt.Errorf("query: volume target %q: %w", target, err)
			}
			augmented = append(augmented, m)
		}
		return augmented, nil
	}
	shape := volumeShape{projected: targets, labelsFilter: targets}
	if len(q.Groups) == 0 {
		arm, err := presence(nil)
		if err != nil {
			return volumeShape{}, err
		}
		shape.matcherGroups = [][]plan.Matcher{arm}
		return shape, nil
	}
	for _, group := range q.Groups {
		arm, err := presence(group)
		if err != nil {
			return volumeShape{}, err
		}
		shape.matcherGroups = append(shape.matcherGroups, arm)
	}
	return shape, nil
}

// volumeMatcherNames returns the sorted union of matcher names across arms, or
// nil when series volume keeps full-label grouping: no selector at all, or any
// match-all arm (a private empty group matches every row, whose projected
// label set is not restricted by the other arms).
func volumeMatcherNames(matcherGroups [][]plan.Matcher) []string {
	if len(matcherGroups) == 0 {
		return nil
	}
	var names []string
	for _, ms := range matcherGroups {
		if len(ms) == 0 {
			return nil
		}
		for _, m := range ms {
			names = append(names, m.Name)
		}
	}
	return sortedUniqueStrings(names)
}

// metaVolumeSQL compiles the volume statement req over groups: one
// bucket × grouping-key aggregate over the normalized relation. It is a
// separate function so placeholder/argument alignment is testable without a
// database. The bucket expression's three bounds are bound where their
// placeholders are written (compiler.bind prints and appends together).
func metaVolumeSQL(req metaVolumeRequest, grid volumeGrid, shape volumeShape, groups []fileGroup) selectQuery {
	q := req.Query
	c := compiler{}
	line := quoteIdent(schema.ColLine)
	c.printf("SELECT bucket, key, CAST(SUM(octet_length(encode(%s))) AS BIGINT) AS volume FROM (SELECT ", line)
	if grid.instant {
		c.printf("0 AS bucket")
	} else {
		// Row partition onto the grid: floor((ts - aligned)/step) capped at
		// the final bucket. The greatest(..., 0) guard covers the negative-
		// time first partial window, where ts precedes the epoch-aligned grid
		// start and truncating division could land on -1; those rows belong to
		// bucket 0, whose window begins at the request's Start, not at the
		// grid's.
		c.printf("greatest(least((epoch_us(%s) - ", quoteIdent(schema.ColTS))
		c.bind(grid.aligned * 1000)
		c.printf(") // ")
		c.bind(grid.step * 1000)
		c.printf(", ")
		c.bind(grid.count - 1)
		c.printf("), 0) AS bucket")
	}
	if req.AggregateByLabels {
		c.printf(", unnest(")
		if len(shape.labelsFilter) > 0 {
			c.printf("list_filter(map_keys(%s), lambda k: list_contains(", metaLabelsMapSQL)
			c.bind(metaNameHex(shape.labelsFilter))
			c.printf(", lower(hex(k))))")
		} else {
			c.printf("map_keys(%s)", metaLabelsMapSQL)
		}
		c.printf(") AS key")
	} else if len(shape.projected) > 0 {
		// The projection of the canonical label map onto the grouping names,
		// rebuilt as canonical JSON so equal projections group together. Names
		// reach SQL as lowercase hex (see metaNameHex), so an invalid-UTF8
		// target or matcher name never reaches DuckDB as a VARCHAR.
		c.printf(", CAST(to_json(coalesce(map_from_entries(list_sort(list_filter(map_entries(%s), lambda e: list_contains(", metaLabelsMapSQL)
		c.bind(metaNameHex(shape.projected))
		c.printf(", lower(hex(e.key)))))), map([], []))) AS VARCHAR) AS key")
	} else {
		c.printf(", labels_json AS key")
	}
	c.printf(", %s FROM (", line)
	metaRelation(&c, q, shape.matcherGroups, groups)
	c.printf(")) GROUP BY bucket, key ORDER BY bucket, key")
	return selectQuery{sql: c.b.String(), args: c.args}
}

// open runs one metadata statement through the shared attempt boundary with
// q's listing selection and compile closure.
func (r metaSQLReader) open(ctx context.Context, q metaQuery, compile func([]fileGroup) selectQuery) (*primedRows, error) {
	return r.src.openRows(ctx, metaListingSelection(r.src.streamLabels, q), compile)
}

// metaListingSelection builds the Selection openRows lists files with: the
// tenant and the inclusive-end window always, and matchers only where they
// let the listing prune by proven stream, exactly as Select's listing does.
// A sole selector group prunes with its own matchers; several OR-combined
// groups prove a stream only when every group forces the same dataset, and a
// selector-free query prunes by tenant and time alone. The listing choice is
// deliberately separate from the row filters (metaRelation): listing
// matchers never filter rows, and the SQL predicates re-check every matcher
// exactly, so a wider listing only reads more files, never answers wrong.
func metaListingSelection(streamLabels []string, q metaQuery) plan.Selection {
	sel := plan.Selection{Tenant: q.Tenant, From: q.Start, Through: metaThrough(q.End)}
	if len(q.Groups) == 0 {
		return sel
	}
	if len(q.Groups) > 1 {
		stream, ok := deriveStream(streamLabels, q.Groups[0])
		if !ok {
			return sel
		}
		for _, g := range q.Groups[1:] {
			s, ok := deriveStream(streamLabels, g)
			if !ok || s != stream {
				return sel
			}
		}
	}
	sel.Matchers = q.Groups[0]
	return sel
}

// metaRelation compiles q's normalized relation over groups into c. With at
// most one effective selector group it is one distinctNormRelation pass whose
// matchers push down into every group subquery (the fast path Select uses).
// With several OR-combined groups a single matcher-free distinct relation is
// wrapped with a boolean OR across groups — AND within a group — over the
// canonical label map aliased as the labels column, so matcherPred compiles
// unchanged against the complete case-sensitive label set and a row matching
// several selectors is read and counted exactly once: no UNION ALL
// duplication. Identity DISTINCT runs inside that relation, before this
// projection drops meta_json, so rows differing only in metadata stay separate
// for stats and volume while an overlapping arm cannot multiply an exact row.
// matcherGroups is q.Groups for every read endpoint; volume passes its
// effective arms (selector matchers plus target presence matchers) so presence
// applies per OR arm exactly as Loki adds it.
func metaRelation(c *compiler, q metaQuery, matcherGroups [][]plan.Matcher, groups []fileGroup) {
	if len(matcherGroups) <= 1 {
		var matchers []plan.Matcher
		if len(matcherGroups) == 1 {
			matchers = matcherGroups[0]
		}
		distinctNormRelation(c, plan.Selection{Tenant: q.Tenant, From: q.Start, Through: metaThrough(q.End), Matchers: matchers}, groups)
		return
	}
	c.printf("SELECT %s, %s, labels_json, %s AS %s FROM (",
		quoteIdent(schema.ColTS), quoteIdent(schema.ColLine), metaLabelsMapSQL, quoteIdent(schema.ColLabels))
	distinctNormRelation(c, plan.Selection{Tenant: q.Tenant, From: q.Start, Through: metaThrough(q.End)}, groups)
	c.printf(") WHERE ")
	for gi, ms := range matcherGroups {
		if gi > 0 {
			c.printf(" OR ")
		}
		c.printf("(")
		if len(ms) == 0 {
			c.printf("TRUE") // an empty selector group matches everything
		} else {
			for mi, m := range ms {
				if mi > 0 {
					c.printf(" AND ")
				}
				c.matcherPred(m, fileGroup{})
			}
		}
		c.printf(")")
	}
}

// metaThrough converts q's inclusive End into plan's exclusive Through: one
// nanosecond past End. normRelation compares ts < ceilµs(Through), and for
// every nanosecond offset of End that ceiling is floorµs(End)+1µs, so the
// stored whole-microsecond timestamps satisfy ts <= End exactly — a row
// precisely at End is matched, and nanosecond endpoints stay exact.
func metaThrough(end time.Time) time.Time { return end.Add(time.Nanosecond) }

// volumeGrid is one volume request's bucket partition: the pinned
// util.ForInterval grid over the millisecond-rounded request range. Bound
// arithmetic runs in Unix milliseconds (int64), never time.Sub, so wide
// ranges cannot overflow a duration.
type volumeGrid struct {
	// instant marks the whole-range single bucket (step 0).
	instant bool
	// aligned is the grid origin in Unix milliseconds: Start floored to the
	// epoch-aligned step grid with Go's truncated %, the exact arithmetic of
	// Loki's ForInterval (which floors only post-epoch starts; pre-epoch it
	// rounds toward zero, and this grid inherits that).
	aligned int64
	// step is the bucket width in whole milliseconds; count is the number of
	// buckets, always >= 1.
	step, count int64
}

// newVolumeGrid partitions [start, end] for step. It expects the
// millisecond-rounded bounds and whole-millisecond step the accepted handlers
// always provide (roundToMilliseconds plus the codec's step truncation).
//
// Buckets: bucket 0 starts at Start itself (not at the aligned grid origin,
// which can precede it), buckets 1..count-1 start at aligned+i*step. The
// final bucket includes Through. When Start==End the count would be zero —
// Loki's ForInterval then emits no bucket at all, but the approved Parkana
// behavior is exactly one final POINT bucket covering [Start, Through].
func newVolumeGrid(start, end time.Time, step time.Duration) volumeGrid {
	if step <= 0 {
		return volumeGrid{instant: true, count: 1}
	}
	stepMs := step.Milliseconds() // the handler codec truncates Step to whole ms
	if stepMs <= 0 {
		// A sub-millisecond step truncates to zero, exactly as the handler's
		// codec does, and zero means an instant whole-range request.
		return volumeGrid{instant: true, count: 1}
	}
	startMs, endMs := start.UnixMilli(), end.UnixMilli()
	aligned := startMs - startMs%stepMs
	count := (endMs - aligned + stepMs - 1) / stepMs
	if count < 1 {
		count = 1 // the Start==End point-bucket exception
	}
	return volumeGrid{aligned: aligned, step: stepMs, count: count}
}

// sampleEnd returns bucket idx's reported timestamp: one millisecond before
// the next grid boundary for non-final buckets, the request's Through for
// the final one (and for the instant whole-range bucket) — Loki's pinned
// volume sample timestamps.
func (g volumeGrid) sampleEnd(idx int64, through time.Time) time.Time {
	if g.instant || idx == g.count-1 {
		return through
	}
	return time.UnixMilli(g.aligned + (idx+1)*g.step - 1).UTC()
}

// metaMapValueSQL appends the value of label name from the canonical labels
// map expression mapSQL. A valid UTF-8 name binds directly. DuckDB refuses to
// bind a VARCHAR that is not valid UTF-8, so an invalid name compares the map
// key bytes instead: lower(hex(key)) against the bound lowercase hex of the
// name (the same byte lookup sql.go uses for matchers). An absent key yields
// NULL, which callers filter; a present empty value stays a value.
func metaMapValueSQL(c *compiler, mapSQL, name string) {
	if utf8.ValidString(name) {
		c.printf("(%s)[", mapSQL)
		c.bind(name)
		c.printf("]")
		return
	}
	c.printf("list_extract(map_values(%s), list_position(list_transform(map_keys(%s), lambda k: lower(hex(k))), ", mapSQL, mapSQL)
	c.bind(hex.EncodeToString([]byte(name)))
	c.printf("))")
}

// metaNameHex encodes label names as lowercase hex for DuckDB parameters, so a
// projection list never carries invalid UTF-8: the SQL compares them against
// lower(hex(label name)) instead. Hex is injective, so set membership is exact
// for valid names too.
func metaNameHex(names []string) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = hex.EncodeToString([]byte(name))
	}
	return out
}

// decodeLabelsJSON decodes one canonical labels_json into sorted labels.
func decodeLabelsJSON(s string) (logs.Labels, error) {
	m, err := decodeMap(sql.NullString{String: s, Valid: s != ""})
	if err != nil {
		return nil, fmt.Errorf("query: decode label set %q: %w", s, err)
	}
	return logs.FromMap(m), nil
}

// collectAll collects this result set's rows. A nil receiver (empty listing or
// empty result) yields nil; iteration and close errors are joined so cleanup
// failures never mask the real one, matching Select's pattern.
func (r *primedRows) collectAll[T any](scan func(*primedRows) (T, error)) ([]T, error) {
	if r == nil {
		return nil, nil
	}
	var out []T
	for r.Next() {
		v, err := scan(r)
		if err != nil {
			return nil, errors.Join(err, r.Close())
		}
		out = append(out, v)
	}
	if err := errors.Join(r.Err(), r.Close()); err != nil {
		return nil, err
	}
	return out, nil
}
