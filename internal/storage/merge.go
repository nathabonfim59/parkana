package storage

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"iter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/nathabonfim59/parkana/internal/schema"
)

// mergeBatchRows bounds the rows accumulated into one output record batch.
const mergeBatchRows = 1024

// Merge k-way-merges sorted input files by ascending _ts into w, unioning
// their schemas additively (columns missing in an input are filled with nulls)
// and collecting FileStats for the promoted label columns.
//
// Exact duplicates are removed: within one global equal-timestamp run
// (spanning every input, reader record batch and output batch), a row whose
// timestamp, complete effective label set, structured metadata and exact line
// bytes match an earlier row is dropped, keeping the first row in the stable
// merge order, pipeline column values included (see mergeDedup for the
// identity rules). The removed count is reported in FileStats.DeduplicatedRows.
// Deduplication state is released when the global timestamp advances, so the
// extra memory is proportional to the largest equal-timestamp run, never to
// the whole file.
//
// Rows with equal timestamps retain input order, then their order within each
// input. Merge never closes w or any of inputs; callers retain ownership of
// both. On error the caller must discard the partial output in w regardless of
// how many footer bytes were written.
func Merge(ctx context.Context, w io.Writer, inputs []*Reader, promoted []string) (FileStats, error) {
	if err := ctx.Err(); err != nil {
		return FileStats{}, fmt.Errorf("storage: merge: %w", err)
	}
	if w == nil {
		return FileStats{}, fmt.Errorf("storage: merge: nil output writer")
	}

	merged, err := mergeSchemas(inputs, promoted)
	if err != nil {
		return FileStats{}, err
	}
	writer, err := NewWriter(w, merged.schema, promoted)
	if err != nil {
		return FileStats{}, fmt.Errorf("storage: merge create writer: %w", err)
	}
	// Close best-effort on every return after this point so the wrapped
	// Parquet writer releases its resources; the primary error is preserved.
	closed := false
	defer func() {
		if !closed {
			_, _ = writer.Close()
		}
	}()

	cursors := make([]*mergeCursor, len(inputs))
	defer func() {
		for _, cursor := range cursors {
			if cursor != nil {
				cursor.stop()
			}
		}
	}()
	heads := make(mergeHeap, 0, len(inputs))
	for i, input := range inputs {
		next, stop := iter.Pull2(input.Records(ctx))
		cursor := &mergeCursor{
			input:    i,
			tsCol:    merged.timestamp[i],
			needCols: merged.needCols[i],
			lookup:   merged.columns[i],
			coerce:   merged.coerce[i],
			next:     next,
			stop:     stop,
		}
		cursors[i] = cursor
		if err := cursor.advance(); err != nil {
			return FileStats{}, err
		}
		if cursor.rec != nil {
			heap.Push(&heads, cursor)
		}
	}

	batch := newMergeBatch(merged.schema)
	defer batch.release()
	dedup := newMergeDedup(merged, promoted)
	for heads.Len() > 0 {
		cursor := heap.Pop(&heads).(*mergeCursor)
		dedup.beginRun(cursor.timestamp)

		// Emit the cursor's current run of equal timestamps. Runs may exceed
		// the output batch capacity, so they are emitted in bounded chunks
		// while the cursor stays out of the heap. Exact duplicates of earlier
		// rows in the global run are skipped, keeping each contiguous span of
		// survivors one batch.add slice.
		runEnd := cursor.row + 1
		for runEnd < cursor.rec.NumRows() && cursor.ts.IsValid(int(runEnd)) && cursor.ts.Value(int(runEnd)) == cursor.timestamp {
			runEnd++
		}
		for cursor.row < runEnd {
			if err := ctx.Err(); err != nil {
				return FileStats{}, fmt.Errorf("storage: merge: %w", err)
			}
			if batch.rows >= mergeBatchRows {
				if err := batch.flush(writer); err != nil {
					return FileStats{}, err
				}
			}
			limit := min(runEnd, cursor.row+mergeBatchRows-batch.rows)
			spanStart := cursor.row
			for row := cursor.row; row < limit; row++ {
				dup, err := dedup.check(cursor, row)
				if err != nil {
					return FileStats{}, err
				}
				if !dup {
					continue
				}
				if row > spanStart {
					if err := batch.add(cursor, spanStart, row); err != nil {
						return FileStats{}, err
					}
				}
				spanStart = row + 1
			}
			if limit > spanStart {
				if err := batch.add(cursor, spanStart, limit); err != nil {
					return FileStats{}, err
				}
			}
			cursor.row = limit
		}
		if cursor.row == cursor.rec.NumRows() {
			if err := cursor.advance(); err != nil {
				return FileStats{}, err
			}
		} else if err := cursor.setTimestamp(); err != nil {
			return FileStats{}, err
		}
		if cursor.rec != nil {
			heap.Push(&heads, cursor)
		}
	}
	if batch.rows > 0 {
		if err := batch.flush(writer); err != nil {
			return FileStats{}, err
		}
	}

	stats, err := writer.Close()
	closed = true
	if err != nil {
		return FileStats{}, fmt.Errorf("storage: merge close output: %w", err)
	}
	stats.DeduplicatedRows = dedup.removed
	return stats, nil
}

// mergeSchemaMap is the output schema plus, for every input, the output-to-
// input column mapping and the column index of _ts. A -1 mapping denotes a
// missing column filled with typed nulls; needCols is the minimum record width
// the merge reads from that input.
type mergeSchemaMap struct {
	schema    *arrow.Schema
	columns   [][]int
	coerce    [][]bool
	timestamp []int
	needCols  []int64
}

func mergeSchemas(inputs []*Reader, promoted []string) (*mergeSchemaMap, error) {
	if len(inputs) == 0 {
		s, err := schema.ForStream(promoted)
		if err != nil {
			return nil, fmt.Errorf("storage: merge schema: %w", err)
		}
		return &mergeSchemaMap{schema: s}, nil
	}

	// The union starts with the canonical base columns in their fixed order;
	// promoted label and pipeline columns follow in first-seen input order.
	// Inputs may store their fields in any order and are remapped by name.
	fields := schema.Base()
	seen := make([]int, len(fields))
	indices := make(map[string]int, len(fields))
	for i, field := range fields {
		indices[field.Name] = i
	}
	local := make([]map[string]int, len(inputs))
	for i, input := range inputs {
		if input == nil {
			return nil, fmt.Errorf("storage: merge input %d: nil reader", i)
		}
		s := input.Schema()
		if s == nil {
			return nil, fmt.Errorf("storage: merge input %d: nil schema", i)
		}
		local[i] = make(map[string]int, len(s.Fields()))
		for j, field := range s.Fields() {
			if field.Name == "" {
				return nil, fmt.Errorf("storage: merge input %d: empty field name at column %d", i, j)
			}
			if _, exists := local[i][field.Name]; exists {
				return nil, fmt.Errorf("storage: merge input %d: duplicate field %q", i, field.Name)
			}
			local[i][field.Name] = j
			if k, exists := indices[field.Name]; exists {
				switch {
				case arrow.TypeEqual(fields[k].Type, field.Type):
				case k < len(schema.Base()) && mergeCompatibleType(fields[k].Type, field.Type):
					// A base column with identical value layout but harmless
					// annotation differences (for example a foreign writer's
					// map KeysSorted flag or item nullability): normalize to
					// the canonical type and coerce slices during the merge.
				default:
					return nil, fmt.Errorf("storage: merge field %q: incompatible types %s and %s", field.Name, fields[k].Type, field.Type)
				}
				fields[k].Nullable = fields[k].Nullable || field.Nullable
				seen[k]++
				continue
			}
			indices[field.Name] = len(fields)
			fields = append(fields, field)
			seen = append(seen, 1)
		}
	}

	for i := range fields {
		if seen[i] < len(inputs) {
			fields[i].Nullable = true
		}
	}
	metadata := inputs[0].Schema().Metadata()
	mapped := &mergeSchemaMap{
		schema:    arrow.NewSchema(fields, &metadata),
		columns:   make([][]int, len(inputs)),
		coerce:    make([][]bool, len(inputs)),
		timestamp: make([]int, len(inputs)),
		needCols:  make([]int64, len(inputs)),
	}
	for i := range inputs {
		ts, ok := local[i][schema.ColTS]
		if !ok {
			return nil, fmt.Errorf("storage: merge input %d: missing %q column", i, schema.ColTS)
		}
		mapped.timestamp[i] = ts
		mapped.needCols[i] = int64(ts + 1)
		mapped.columns[i] = make([]int, len(fields))
		mapped.coerce[i] = make([]bool, len(fields))
		for j, field := range fields {
			source, ok := local[i][field.Name]
			if !ok {
				mapped.columns[i][j] = -1
				continue
			}
			mapped.columns[i][j] = source
			mapped.coerce[i][j] = !arrow.TypeEqual(inputs[i].Schema().Field(source).Type, field.Type)
			mapped.needCols[i] = max(mapped.needCols[i], int64(source+1))
		}
	}
	return mapped, nil
}

// mergeCompatibleType reports whether got can be normalized to want without
// copying values. It accepts maps whose key and item types match, tolerating
// differences in the KeysSorted flag or field nullability that TypeEqual
// treats as incompatible; the physical layout is identical.
func mergeCompatibleType(want, got arrow.DataType) bool {
	w, ok1 := want.(*arrow.MapType)
	g, ok2 := got.(*arrow.MapType)
	if !ok1 || !ok2 {
		return false
	}
	return !g.KeyField().Nullable &&
		arrow.TypeEqual(w.KeyType(), g.KeyType()) &&
		arrow.TypeEqual(w.ItemType(), g.ItemType())
}

// mergeCoerceArray returns col retyped as want, sharing all value buffers. It
// is valid only for types mergeCompatibleType accepts; it returns nil
// otherwise.
func mergeCoerceArray(col arrow.Array, want arrow.DataType) arrow.Array {
	target, ok := want.(*arrow.MapType)
	if !ok || !mergeCompatibleType(want, col.DataType()) {
		return nil
	}
	d := col.Data()
	if len(d.Children()) != 1 {
		return nil
	}
	entries := d.Children()[0]
	child := array.NewData(target.Elem(), entries.Len(), entries.Buffers(), entries.Children(), entries.NullN(), entries.Offset())
	data := array.NewData(target, d.Len(), d.Buffers(), []arrow.ArrayData{child}, d.NullN(), d.Offset())
	child.Release()
	out := array.MakeFromData(data)
	data.Release()
	return out
}

// mergeCursor holds one borrowed record per input. It never retains or
// releases that record; output runs retain their own array slices.
type mergeCursor struct {
	input     int
	tsCol     int
	needCols  int64
	lookup    []int
	coerce    []bool
	next      func() (arrow.RecordBatch, error, bool)
	stop      func()
	rec       arrow.RecordBatch
	ts        *array.Timestamp
	row       int64
	timestamp arrow.Timestamp
	last      arrow.Timestamp
	hasLast   bool
}

func (c *mergeCursor) advance() error {
	for {
		rec, err, ok := c.next()
		if !ok {
			c.rec = nil
			c.ts = nil
			return nil
		}
		if err != nil {
			return fmt.Errorf("storage: merge input %d: %w", c.input, err)
		}
		if rec == nil {
			return fmt.Errorf("storage: merge input %d: iterator yielded a nil record", c.input)
		}
		if rec.NumRows() == 0 {
			continue
		}
		if rec.NumCols() < c.needCols {
			return fmt.Errorf("storage: merge input %d: record has %d columns, want at least %d", c.input, rec.NumCols(), c.needCols)
		}
		ts, ok := rec.Column(c.tsCol).(*array.Timestamp)
		if !ok {
			return fmt.Errorf("storage: merge input %d: record column %q has type %T, want *array.Timestamp", c.input, schema.ColTS, rec.Column(c.tsCol))
		}
		c.rec = rec
		c.ts = ts
		c.row = 0
		if err := c.setTimestamp(); err != nil {
			return err
		}
		return nil
	}
}

func (c *mergeCursor) setTimestamp() error {
	row := int(c.row)
	if !c.ts.IsValid(row) {
		return fmt.Errorf("storage: merge input %d: null %q at record row %d", c.input, schema.ColTS, c.row)
	}
	ts := c.ts.Value(row)
	if c.hasLast && ts < c.last {
		return fmt.Errorf("storage: merge input %d: %q is not ascending", c.input, schema.ColTS)
	}
	c.timestamp = ts
	c.last = ts
	c.hasLast = true
	return nil
}

// mergeHeap is a min-heap of cursors ordered by current timestamp, then by
// input index so rows with equal timestamps stay in input order.
type mergeHeap []*mergeCursor

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	if h[i].timestamp == h[j].timestamp {
		return h[i].input < h[j].input
	}
	return h[i].timestamp < h[j].timestamp
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(*mergeCursor)) }
func (h *mergeHeap) Pop() any {
	old := *h
	last := len(old) - 1
	x := old[last]
	old[last] = nil
	*h = old[:last]
	return x
}

// mergeBatch owns retained array slices until flush or release.
type mergeBatch struct {
	schema  *arrow.Schema
	columns [][]arrow.Array
	rows    int64
}

func newMergeBatch(s *arrow.Schema) *mergeBatch {
	return &mergeBatch{schema: s, columns: make([][]arrow.Array, len(s.Fields()))}
}

func (b *mergeBatch) add(cursor *mergeCursor, start, end int64) error {
	length := int(end - start)
	for i, source := range cursor.lookup {
		var col arrow.Array
		if source < 0 {
			col = array.MakeArrayOfNull(memory.DefaultAllocator, b.schema.Field(i).Type, length)
		} else {
			col = array.NewSlice(cursor.rec.Column(source), start, end)
			if cursor.coerce[i] {
				coerced := mergeCoerceArray(col, b.schema.Field(i).Type)
				col.Release()
				if coerced == nil {
					return fmt.Errorf("storage: merge input %d: column %q type %s cannot be normalized to %s",
						cursor.input, b.schema.Field(i).Name, cursor.rec.Column(source).DataType(), b.schema.Field(i).Type)
				}
				col = coerced
			}
		}
		b.columns[i] = append(b.columns[i], col)
	}
	b.rows += int64(length)
	return nil
}

func (b *mergeBatch) flush(writer *Writer) error {
	defer b.release()
	cols := make([]arrow.Array, len(b.columns))
	defer func() {
		for _, col := range cols {
			if col != nil {
				col.Release()
			}
		}
	}()
	for i, runs := range b.columns {
		col, err := array.Concatenate(runs, memory.DefaultAllocator)
		if err != nil {
			return fmt.Errorf("storage: merge column %q: %w", b.schema.Field(i).Name, err)
		}
		cols[i] = col
	}
	rec := array.NewRecordBatch(b.schema, cols, b.rows)
	defer rec.Release()
	if err := writer.Write(rec); err != nil {
		return fmt.Errorf("storage: merge write output: %w", err)
	}
	return nil
}

func (b *mergeBatch) release() {
	for _, runs := range b.columns {
		for _, col := range runs {
			col.Release()
		}
	}
	clear(b.columns)
	b.rows = 0
}
