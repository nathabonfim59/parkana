// SQL aggregation for the querier's plan.Source: a Selection and BucketAgg
// over the listed object groups become one GROUP BY statement over the same
// normalized relation Select compiles, with all bucket arithmetic in exact
// wide integers and Go time.Duration saturation semantics.
package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/schema"
)

// The saturation bounds of Go's time.Duration — the values time.Sub clamps
// an out-of-range difference to. The SQL distance is clamped to exactly
// these, so MemSource's buckets and Aggregate's agree even for rows more
// than the ~292 years an int64 nanosecond count can span.
const (
	nsSatMin = -1 << 63
	nsSatMax = 1<<63 - 1
)

// aggregateSQL compiles a validated sel and agg over groups into the
// Aggregate statement:
//
//	SELECT CAST((d - ((d % w + w) % w)) // w AS BIGINT) AS bucket,
//	       labels_json, count(*),
//	       CAST(sum(octet_length(encode("line"))) AS BIGINT) AS bytes
//	FROM (
//	  SELECT greatest(<min>, least(<max>,
//	         CAST(epoch_us("_ts") AS HUGEINT) * 1000
//	         - (CAST(? AS HUGEINT) * 1000000000 + ?))) AS d,
//	         ? AS w, "line", labels_json
//	  FROM ( <distinct normalized group subqueries, UNION ALL'd> )
//	) GROUP BY 1, 2
//
// With agg.IncludeMeta the projection also carries the distinct relation's
// already-canonical meta_json, the outer SELECT returns it after labels_json,
// and the GROUP BY adds its position:
//
//	SELECT ..., labels_json, meta_json, count(*), ...
//	FROM ( SELECT ..., labels_json, meta_json FROM ( <distinct relation> ) )
//	GROUP BY 1, 2, 3
//
// The inner subquery is exactly Select's distinctNormRelation — matchers, line
// filters and time range pushed into every group, labels_json the complete
// canonical label map, meta_json canonical and part of the row identity even
// when IncludeMeta is false — so Aggregate counts precisely the distinct rows
// Select would return, and rows group together no matter which file promoted
// which label or in which order the map stores it. The middle subquery turns
// every whole-microsecond _ts into d, the row's nanosecond distance from the
// bucket origin: epoch_us is µs since epoch, the origin binds as Unix()
// seconds plus its full 0..999999999ns remainder — two int64s every
// time.Time carries by construction, so every origin BucketAgg.Validate
// accepts is bindable, however far from the epoch — and everything runs in
// HUGEINT, where the exact products and differences of any two instants
// fit; in BIGINT the same products overflow loudly past the ~292 years an
// int64 nanosecond count spans (pinned by TestDuckDBAggregateArithmetic).
// The clamp to nsSatMin/nsSatMax is bit-identical to time.Sub's saturation,
// MemSource's bucketing included. w carries the width in ns as a constant
// column so the outer floor can reference it four times from one bind:
// DuckDB's // truncates toward zero (probed), so the adjustment
// (d - ((d % w + w) % w)) // w subtracts the non-negative remainder first,
// making the dividend an exact multiple of w and the quotient Go's floored
// k of plan.BucketAgg.Start — negative for origins after the data, however
// % signs its operands. The CAST is safe because k = d/width with width a
// whole µs always fits an int64. The scan rebuilds each start as
// agg.Origin.Add(k * agg.Width), wrapped multiplication included, the exact
// arithmetic the reference runs. Aggregate ignores Selection.Direction and
// Limit: neither ORDER BY nor LIMIT is compiled. octet_length has no
// VARCHAR overload, so encode turns the line into a BLOB whose byte count
// is Go's len(Line); sum comes back as HUGEINT, so its CAST keeps the scan
// an int64. Client strings reach SQL only as the bound parameters
// normRelation already binds, plus the three bucket values in text order:
// origin seconds, origin nanoseconds, width.
func aggregateSQL(sel plan.Selection, agg plan.BucketAgg, groups []fileGroup) selectQuery {
	c := compiler{}
	ts, line := quoteIdent(schema.ColTS), quoteIdent(schema.ColLine)
	c.printf("SELECT CAST((d - ((d %% w + w) %% w)) // w AS BIGINT) AS bucket, labels_json")
	if agg.IncludeMeta {
		c.printf(", meta_json")
	}
	c.printf(", count(*), CAST(sum(octet_length(encode(%s))) AS BIGINT) AS bytes FROM (", line)
	c.printf("SELECT greatest(%d, least(%d, CAST(epoch_us(%s) AS HUGEINT) * 1000 - (CAST(", nsSatMin, nsSatMax, ts)
	c.bind(agg.Origin.Unix())
	c.printf(" AS HUGEINT) * 1000000000 + ")
	c.bind(int64(agg.Origin.Nanosecond()))
	c.printf("))) AS d, ")
	c.bind(agg.Width.Nanoseconds())
	c.printf(" AS w, %s, labels_json", line)
	if agg.IncludeMeta {
		c.printf(", meta_json")
	}
	c.printf(" FROM (")
	distinctNormRelation(&c, sel, groups)
	c.printf(")) GROUP BY 1, 2")
	if agg.IncludeMeta {
		c.printf(", 3")
	}
	return selectQuery{sql: c.b.String(), args: c.args}
}

// Aggregate implements plan.Source.
//
// It validates sel and agg, then hands aggregateSQL over the shared
// normalized relation to openRows: one catalog listing, the same label and
// sidecar pruning Select applies, one acquisition of what remains, one GROUP
// BY statement with its first row primed before anything is yielded, and the
// same exactly-once-after-fresh-listing disappearance retry Select documents
// (source.Select's attempt boundary; GROUP BY reads every input file before
// the first group, so the boundary holds). Every origin BucketAgg.Validate
// accepts is served — the seconds+nanoseconds binding covers any time.Time.
// With IncludeMeta the statement groups by the canonical metadata set too and
// Bucket.Meta carries it; without it buckets group by stream labels only and
// Meta stays nil. Buckets stream in no particular order; Selection.Direction
// and Limit are ignored.
func (s *source) Aggregate(ctx context.Context, sel plan.Selection, agg plan.BucketAgg) iter.Seq2[plan.Bucket, error] {
	return func(yield func(plan.Bucket, error) bool) {
		if err := errors.Join(sel.Validate(), agg.Validate()); err != nil {
			yield(plan.Bucket{}, err)
			return
		}
		if err := ctx.Err(); err != nil {
			yield(plan.Bucket{}, fmt.Errorf("query: aggregate: %w", err))
			return
		}
		rows, err := s.openRows(ctx, sel, func(groups []fileGroup) selectQuery {
			return aggregateSQL(sel, agg, groups)
		})
		if err != nil {
			yield(plan.Bucket{}, err)
			return
		}
		if rows == nil {
			return // nothing listed, or no group matched any row
		}
		defer rows.Close()
		for rows.Next() {
			b, err := scanBucket(rows, agg)
			if err != nil {
				yield(plan.Bucket{}, errors.Join(err, rows.Close()))
				return
			}
			if !yield(b, nil) {
				return
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			yield(plan.Bucket{}, fmt.Errorf("query: aggregate: %w", err))
		}
	}
}

// scanBucket decodes the current row of an Aggregate statement into a
// plan.Bucket. The SQL bucket index k becomes the start exactly as
// plan.BucketAgg.Start does: origin plus k widths, preserving a sub-µs
// origin's nanoseconds and the reference's wrapped multiplication beyond
// the saturation span. labels_json is the complete canonical label map
// built in SQL inside the row's group (see normRelation); with
// BucketAgg.IncludeMeta the statement also returns the canonical metadata
// set, decoded and sorted here, and the disabled path keeps the four-column
// scan unchanged.
func scanBucket(rows *primedRows, agg plan.BucketAgg) (plan.Bucket, error) {
	var k, count, bytes int64
	var labelsJSON, metaJSON sql.NullString
	dest := []any{&k, &labelsJSON}
	if agg.IncludeMeta {
		dest = append(dest, &metaJSON)
	}
	dest = append(dest, &count, &bytes)
	if err := rows.Scan(dest...); err != nil {
		return plan.Bucket{}, fmt.Errorf("query: scan bucket: %w", err)
	}
	labels, err := decodeMap(labelsJSON)
	if err != nil {
		return plan.Bucket{}, fmt.Errorf("query: scan bucket: decode labels: %w", err)
	}
	b := plan.Bucket{
		Start:  agg.Origin.Add(time.Duration(k) * agg.Width),
		Labels: logs.FromMap(labels),
		Count:  count,
		Bytes:  bytes,
	}
	if agg.IncludeMeta {
		meta, err := decodeMap(metaJSON)
		if err != nil {
			return plan.Bucket{}, fmt.Errorf("query: scan bucket: decode meta: %w", err)
		}
		b.Meta = logs.FromMap(meta)
	}
	return b, nil
}
