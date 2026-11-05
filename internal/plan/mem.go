package plan

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"slices"
	"sync"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// MemSource is the reference Source: a plain slice of rows per tenant. It
// defines the expected results of the plantest conformance suite and backs
// front-end unit tests. It is safe for concurrent use.
type MemSource struct {
	mu   sync.RWMutex
	rows map[tenant.ID][]Row
}

// NewMemSource returns an empty MemSource.
func NewMemSource() *MemSource { return &MemSource{rows: make(map[tenant.ID][]Row)} }

// Add appends rows for tenant t. TS is truncated to µs, as storage does.
func (m *MemSource) Add(t tenant.ID, rows ...Row) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range rows {
		r.TS = r.TS.Truncate(time.Microsecond).UTC()
		m.rows[t] = append(m.rows[t], r)
	}
}

func (m *MemSource) selected(sel Selection) []Row {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []Row
	seen := make(map[rowIdentity]struct{})
	for _, r := range m.rows[sel.Tenant] {
		if !sel.Matches(sel.Tenant, r) {
			continue
		}
		id := rowIdentity{r.TS.UnixMicro(), r.Labels.String(), r.Meta.String(), r.Line}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, r)
	}
	return out
}

// rowIdentity is what makes two stored rows the same row (Source doc):
// duplicates delivered twice by an at-least-once collector count once.
type rowIdentity struct {
	us                 int64
	labels, meta, line string
}

// Select implements Source. Rows with equal TS keep insertion order
// (reversed for Backward).
func (m *MemSource) Select(ctx context.Context, sel Selection) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		if err := sel.Validate(); err != nil {
			yield(Row{}, err)
			return
		}
		if err := ctx.Err(); err != nil {
			yield(Row{}, fmt.Errorf("plan: select: %w", err))
			return
		}
		rows := m.selected(sel)
		slices.SortStableFunc(rows, func(a, b Row) int { return a.TS.Compare(b.TS) })
		if sel.Direction == Backward {
			slices.Reverse(rows)
		}
		if sel.Limit > 0 && len(rows) > sel.Limit {
			rows = rows[:sel.Limit]
		}
		for _, r := range rows {
			if err := ctx.Err(); err != nil {
				yield(Row{}, fmt.Errorf("plan: select: %w", err))
				return
			}
			if !yield(r, nil) {
				return
			}
		}
	}
}

// Aggregate implements Source. Buckets come sorted by Start, then labels.
func (m *MemSource) Aggregate(ctx context.Context, sel Selection, agg BucketAgg) iter.Seq2[Bucket, error] {
	return func(yield func(Bucket, error) bool) {
		if err := sel.Validate(); err != nil {
			yield(Bucket{}, err)
			return
		}
		if err := agg.Validate(); err != nil {
			yield(Bucket{}, err)
			return
		}
		if err := ctx.Err(); err != nil {
			yield(Bucket{}, fmt.Errorf("plan: aggregate: %w", err))
			return
		}
		type key struct {
			start        time.Time
			labels, meta string
		}
		sums := make(map[key]*Bucket)
		for _, r := range m.selected(sel) {
			k := key{start: agg.Start(r.TS), labels: r.Labels.String()}
			if agg.IncludeMeta {
				k.meta = r.Meta.String()
			}
			b := sums[k]
			if b == nil {
				b = &Bucket{Start: k.start, Labels: r.Labels}
				if agg.IncludeMeta {
					b.Meta = r.Meta
				}
				sums[k] = b
			}
			b.Count++
			b.Bytes += int64(len(r.Line))
		}
		out := make([]Bucket, 0, len(sums))
		for _, b := range sums {
			out = append(out, *b)
		}
		slices.SortFunc(out, func(a, b Bucket) int {
			return cmp.Or(a.Start.Compare(b.Start), cmp.Compare(a.Labels.String(), b.Labels.String()),
				cmp.Compare(a.Meta.String(), b.Meta.String()))
		})
		for _, b := range out {
			if err := ctx.Err(); err != nil {
				yield(Bucket{}, fmt.Errorf("plan: aggregate: %w", err))
				return
			}
			if !yield(b, nil) {
				return
			}
		}
	}
}
