package storage_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
)

// mergeField is a shorthand for a nullable arrow field.
func mergeField(name string, dt arrow.DataType) arrow.Field {
	return arrow.Field{Name: name, Type: dt, Nullable: true}
}

// mergeTSs and mergeStrs build base column values: Unix-microsecond timestamps
// and log lines. A negative timestamp emits a null.
func mergeTSs(us ...int64) []any {
	vals := make([]any, len(us))
	for i, u := range us {
		if u < 0 {
			vals[i] = nil
			continue
		}
		vals[i] = schema.Time(u)
	}
	return vals
}

func mergeStrs(ss ...string) []any {
	vals := make([]any, len(ss))
	for i, s := range ss {
		vals[i] = s
	}
	return vals
}

// mergeMaps builds label/meta column values; a nil map emits a null.
func mergeMaps(ms ...map[string]string) []any {
	vals := make([]any, len(ms))
	for i, m := range ms {
		if m != nil {
			vals[i] = m
		}
	}
	return vals
}

// mergeEmptyMaps fills n rows with empty maps, matching non-null label/meta
// columns as produced by the writer.
func mergeEmptyMaps(n int) []any {
	vals := make([]any, n)
	for i := range vals {
		vals[i] = map[string]string{}
	}
	return vals
}

// mergeColumn builds one array for dt from vals; nil entries become nulls.
func mergeColumn(t *testing.T, dt arrow.DataType, vals []any) arrow.Array {
	t.Helper()
	b := array.NewBuilder(memory.DefaultAllocator, dt)
	defer b.Release()
	for _, v := range vals {
		if v == nil {
			b.AppendNull()
			continue
		}
		switch bb := b.(type) {
		case *array.TimestampBuilder:
			switch ts := v.(type) {
			case int64:
				bb.Append(arrow.Timestamp(ts))
			default:
				bb.Append(arrow.Timestamp(schema.TS(v.(time.Time))))
			}
		case *array.StringBuilder:
			bb.Append(v.(string))
		case *array.Int64Builder:
			bb.Append(v.(int64))
		case *array.Float64Builder:
			bb.Append(v.(float64))
		case *array.BooleanBuilder:
			bb.Append(v.(bool))
		case *array.MapBuilder:
			bb.Append(true)
			kb := bb.KeyBuilder().(*array.StringBuilder)
			ib := bb.ItemBuilder().(*array.StringBuilder)
			for _, k := range slices.Sorted(maps.Keys(v.(map[string]string))) {
				kb.Append(k)
				ib.Append(v.(map[string]string)[k])
			}
		default:
			t.Fatalf("mergeColumn: unsupported type %s", dt)
		}
	}
	return b.NewArray()
}

// mergeRecord builds one record batch for s; cols are positional and must
// match the field count.
func mergeRecord(t *testing.T, s *arrow.Schema, cols ...[]any) arrow.RecordBatch {
	t.Helper()
	fields := s.Fields()
	if len(cols) != len(fields) {
		t.Fatalf("mergeRecord: %d columns for %d fields", len(cols), len(fields))
	}
	arrays := make([]arrow.Array, len(fields))
	rows := -1
	for i, field := range fields {
		arrays[i] = mergeColumn(t, field.Type, cols[i])
		defer arrays[i].Release()
		if rows < 0 {
			rows = arrays[i].Len()
		} else if arrays[i].Len() != rows {
			t.Fatalf("mergeRecord: column %d has %d rows, want %d", i, arrays[i].Len(), rows)
		}
	}
	return array.NewRecordBatch(s, arrays, int64(rows))
}

// mergeParquetE writes batches as one Parquet file without any of Writer's
// validation, so invalid inputs (unsorted or null _ts, missing or reordered
// base columns, conflicting types) are expressible. storeSchema controls
// whether the ARROW:schema metadata is embedded; foreign writers may not
// produce it.
func mergeParquetE(s *arrow.Schema, storeSchema bool, recs ...arrow.RecordBatch) ([]byte, error) {
	var buf bytes.Buffer
	props := parquet.NewWriterProperties()
	arrprops := pqarrow.NewArrowWriterProperties()
	if storeSchema {
		arrprops = pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	}
	fw, err := pqarrow.NewFileWriter(s, &buf, props, arrprops)
	if err != nil {
		return nil, err
	}
	for _, rec := range recs {
		if err := fw.Write(rec); err != nil {
			_ = fw.Close()
			return nil, err
		}
	}
	if err := fw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func mergeParquet(t *testing.T, s *arrow.Schema, recs ...arrow.RecordBatch) []byte {
	t.Helper()
	data, err := mergeParquetE(s, true, recs...)
	if err != nil {
		t.Fatalf("mergeParquet: %v", err)
	}
	return data
}

// mergeInput writes each batch (a positional list of column value slices) as
// one Parquet file and opens it as a Reader.
func mergeInput(t *testing.T, fields []arrow.Field, batches ...[][]any) *storage.Reader {
	t.Helper()
	return mergeInputStore(t, fields, true, batches...)
}

// mergeInputStore is mergeInput with control over ARROW:schema embedding.
func mergeInputStore(t *testing.T, fields []arrow.Field, storeSchema bool, batches ...[][]any) *storage.Reader {
	t.Helper()
	s := arrow.NewSchema(fields, nil)
	recs := make([]arrow.RecordBatch, len(batches))
	for i, cols := range batches {
		recs[i] = mergeRecord(t, s, cols...)
		defer recs[i].Release()
	}
	data, err := mergeParquetE(s, storeSchema, recs...)
	if err != nil {
		t.Fatalf("mergeInputStore parquet: %v", err)
	}
	r, err := storage.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("mergeInputStore NewReader: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// mergeRows reads a Parquet file and extracts rows as positionally indexed Go
// values: int64 micros for timestamps, string, int64, float64, bool,
// map[string]string for maps, nil for nulls.
func mergeRows(t *testing.T, data []byte) (*arrow.Schema, [][]any) {
	t.Helper()
	r, err := storage.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("mergeRows NewReader: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	var rows [][]any
	for rec, err := range r.Records(t.Context()) {
		if err != nil {
			t.Fatalf("mergeRows Records: %v", err)
		}
		for i := range int(rec.NumRows()) {
			row := make([]any, rec.NumCols())
			for c := range int(rec.NumCols()) {
				row[c] = mergeValue(t, rec.Column(c), i)
			}
			rows = append(rows, row)
		}
	}
	return r.Schema(), rows
}

func mergeValue(t *testing.T, col arrow.Array, i int) any {
	t.Helper()
	if !col.IsValid(i) {
		return nil
	}
	switch a := col.(type) {
	case *array.Timestamp:
		return int64(a.Value(i))
	case *array.String:
		return a.Value(i)
	case *array.Int64:
		return a.Value(i)
	case *array.Float64:
		return a.Value(i)
	case *array.Boolean:
		return a.Value(i)
	case *array.Map:
		start, end := a.ValueOffsets(i)
		keys := a.Keys().(*array.String)
		items := a.Items().(*array.String)
		m := make(map[string]string, end-start)
		for k := start; k < end; k++ {
			m[keys.Value(int(k))] = items.Value(int(k))
		}
		return m
	default:
		t.Fatalf("mergeValue: unsupported array %T", col)
		return nil
	}
}

func mergeFieldNames(s *arrow.Schema) []string {
	names := make([]string, len(s.Fields()))
	for i, f := range s.Fields() {
		names[i] = f.Name
	}
	return names
}

// mergeBaseFields returns the canonical four base fields.
func mergeBaseFields() []arrow.Field { return schema.Base() }

// mergeRowOf composes an expected output row with empty labels/meta maps.
func mergeRowOf(us int64, line string, extras ...any) []any {
	return append([]any{us, line, map[string]string{}, map[string]string{}}, extras...)
}

// mergeFailWriter fails every Write.
type mergeFailWriter struct{ err error }

func (w *mergeFailWriter) Write([]byte) (int, error) { return 0, w.err }

// mergeFailRAS fails ReadAt once enabled, simulating an object that goes bad
// after the Parquet footer was read.
type mergeFailRAS struct {
	*bytes.Reader
	fail atomic.Bool
	err  error
}

func (r *mergeFailRAS) ReadAt(p []byte, off int64) (int, error) {
	if r.fail.Load() {
		return 0, r.err
	}
	return r.Reader.ReadAt(p, off)
}

// mergeCountdownCtx cancels itself after n calls to Err or Done-adjacent
// checks, giving deterministic mid-merge cancellation however deep into the
// pull/write pipeline the check lands.
type mergeCountdownCtx struct {
	context.Context
	remaining atomic.Int64
	done      chan struct{}
	once      sync.Once
}

func newMergeCountdownCtx(parent context.Context, n int64) *mergeCountdownCtx {
	c := &mergeCountdownCtx{Context: parent, done: make(chan struct{})}
	c.remaining.Store(n)
	return c
}

func (c *mergeCountdownCtx) Done() <-chan struct{} { return c.done }

func (c *mergeCountdownCtx) Err() error {
	if c.remaining.Add(-1) < 0 {
		c.once.Do(func() { close(c.done) })
	}
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

// mergeCloserSpy records Close on a caller-owned io.Writer.
type mergeCloserSpy struct {
	bytes.Buffer
	closed bool
}

func (w *mergeCloserSpy) Close() error {
	w.closed = true
	return nil
}

func mergeBaseCols(ts, line []any, labels, meta []any) [][]any {
	return [][]any{ts, line, labels, meta}
}

func TestMergeInterleavedInputs(t *testing.T) {
	in0 := mergeInput(t, mergeBaseFields(), mergeBaseCols(
		mergeTSs(10, 30, 50),
		mergeStrs("a0", "a1", "a2"),
		mergeEmptyMaps(3), mergeEmptyMaps(3),
	))
	in1 := mergeInput(t, mergeBaseFields(), mergeBaseCols(
		mergeTSs(20, 40, 60),
		mergeStrs("b0", "b1", "b2"),
		mergeEmptyMaps(3), mergeEmptyMaps(3),
	))
	in2 := mergeInput(t, mergeBaseFields(), mergeBaseCols(
		mergeTSs(5, 50, 70),
		mergeStrs("c0", "c1", "c2"),
		mergeEmptyMaps(3), mergeEmptyMaps(3),
	))

	out := &bytes.Buffer{}
	stats, err := storage.Merge(t.Context(), out, []*storage.Reader{in0, in1, in2}, nil)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	sch, rows := mergeRows(t, out.Bytes())

	want := [][]any{
		mergeRowOf(5, "c0"),
		mergeRowOf(10, "a0"),
		mergeRowOf(20, "b0"),
		mergeRowOf(30, "a1"),
		mergeRowOf(40, "b1"),
		mergeRowOf(50, "a2"),
		mergeRowOf(50, "c1"),
		mergeRowOf(60, "b2"),
		mergeRowOf(70, "c2"),
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
	if got := mergeFieldNames(sch); !slices.Equal(got, []string{schema.ColTS, schema.ColLine, schema.ColLabels, schema.ColMeta}) {
		t.Fatalf("fields = %v", got)
	}
	if stats.Rows != 9 || !stats.MinTS.Equal(schema.Time(5)) || !stats.MaxTS.Equal(schema.Time(70)) {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestMergeEqualTimestampStability(t *testing.T) {
	in0 := mergeInput(t, mergeBaseFields(), mergeBaseCols(
		mergeTSs(10, 20, 20),
		mergeStrs("a0", "a1", "a2"),
		mergeEmptyMaps(3), mergeEmptyMaps(3),
	))
	in1 := mergeInput(t, mergeBaseFields(), mergeBaseCols(
		mergeTSs(20, 20, 30),
		mergeStrs("b0", "b1", "b2"),
		mergeEmptyMaps(3), mergeEmptyMaps(3),
	))
	in2 := mergeInput(t, mergeBaseFields(), mergeBaseCols(
		mergeTSs(20),
		mergeStrs("c0"),
		mergeEmptyMaps(1), mergeEmptyMaps(1),
	))

	out := &bytes.Buffer{}
	if _, err := storage.Merge(t.Context(), out, []*storage.Reader{in0, in1, in2}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	_, rows := mergeRows(t, out.Bytes())
	want := [][]any{
		mergeRowOf(10, "a0"),
		mergeRowOf(20, "a1"),
		mergeRowOf(20, "a2"),
		mergeRowOf(20, "b0"),
		mergeRowOf(20, "b1"),
		mergeRowOf(20, "c0"),
		mergeRowOf(30, "b2"),
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
}

// TestMergeLongEqualRun forces one equal-timestamp run past both the reader's
// per-file batch boundary (8192 rows) and the merge output batch bound, across
// two inputs.
func TestMergeLongEqualRun(t *testing.T) {
	const n0, n1 = 9000, 1500
	us := make([]int64, n0)
	lines := make([]string, n0)
	for i := range n0 {
		us[i] = 42
		lines[i] = "a" + strconv.Itoa(i)
	}
	in0 := mergeInput(t, mergeBaseFields(), mergeBaseCols(
		mergeTSs(us...), mergeStrs(lines...), mergeEmptyMaps(n0), mergeEmptyMaps(n0),
	))
	us1 := make([]int64, n1)
	lines1 := make([]string, n1)
	for i := range n1 {
		us1[i] = 42
		lines1[i] = "b" + strconv.Itoa(i)
	}
	in1 := mergeInput(t, mergeBaseFields(), mergeBaseCols(
		mergeTSs(us1...), mergeStrs(lines1...), mergeEmptyMaps(n1), mergeEmptyMaps(n1),
	))

	out := &bytes.Buffer{}
	stats, err := storage.Merge(t.Context(), out, []*storage.Reader{in0, in1}, nil)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if stats.Rows != int64(n0+n1) {
		t.Fatalf("stats.Rows = %d, want %d", stats.Rows, n0+n1)
	}
	_, rows := mergeRows(t, out.Bytes())
	if len(rows) != n0+n1 {
		t.Fatalf("rows = %d, want %d", len(rows), n0+n1)
	}
	for i, row := range rows {
		if row[0] != int64(42) {
			t.Fatalf("row %d ts = %v, want 42", i, row[0])
		}
		want := "a" + strconv.Itoa(i)
		if i >= n0 {
			want = "b" + strconv.Itoa(i-n0)
		}
		if row[1] != want {
			t.Fatalf("row %d line = %v, want %v", i, row[1], want)
		}
	}
}

func TestMergeSchemaEvolution(t *testing.T) {
	in0 := mergeInput(t,
		append(mergeBaseFields(), mergeField("level", arrow.BinaryTypes.String)),
		[][]any{
			mergeTSs(10, 30),
			mergeStrs("a0", "a1"),
			mergeEmptyMaps(2), mergeEmptyMaps(2),
			mergeStrs("info", "warn"),
		})
	in1 := mergeInput(t,
		append(mergeBaseFields(), mergeField("trace", arrow.PrimitiveTypes.Int64)),
		[][]any{
			mergeTSs(20, 40),
			mergeStrs("b0", "b1"),
			mergeEmptyMaps(2), mergeEmptyMaps(2),
			[]any{int64(7), int64(9)},
		})

	out := &bytes.Buffer{}
	stats, err := storage.Merge(t.Context(), out, []*storage.Reader{in0, in1}, []string{"level"})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	sch, rows := mergeRows(t, out.Bytes())
	want := [][]any{
		mergeRowOf(10, "a0", "info", nil),
		mergeRowOf(20, "b0", nil, int64(7)),
		mergeRowOf(30, "a1", "warn", nil),
		mergeRowOf(40, "b1", nil, int64(9)),
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
	wantNames := []string{schema.ColTS, schema.ColLine, schema.ColLabels, schema.ColMeta, "level", "trace"}
	if got := mergeFieldNames(sch); !slices.Equal(got, wantNames) {
		t.Fatalf("fields = %v, want %v", got, wantNames)
	}
	if got := stats.LabelValues["level"]; !slices.Equal(got, []string{"", "info", "warn"}) {
		t.Fatalf("LabelValues[level] = %v", got)
	}
}

// TestMergeReorderedBaseColumns merges a foreign file whose base columns are
// stored in a different order; the union still emits canonical order.
func TestMergeReorderedBaseColumns(t *testing.T) {
	base := mergeBaseFields()
	reordered := []arrow.Field{base[1], base[3], base[0], base[2]} // line, meta, _ts, labels
	in0 := mergeInput(t, reordered,
		[][]any{
			mergeStrs("x0", "x1"),
			mergeEmptyMaps(2),
			mergeTSs(20, 40),
			mergeEmptyMaps(2),
		})
	in1 := mergeInput(t, mergeBaseFields(), mergeBaseCols(
		mergeTSs(10, 30),
		mergeStrs("c0", "c1"),
		mergeEmptyMaps(2), mergeEmptyMaps(2),
	))

	out := &bytes.Buffer{}
	if _, err := storage.Merge(t.Context(), out, []*storage.Reader{in0, in1}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	sch, rows := mergeRows(t, out.Bytes())
	want := [][]any{
		mergeRowOf(10, "c0"),
		mergeRowOf(20, "x0"),
		mergeRowOf(30, "c1"),
		mergeRowOf(40, "x1"),
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
	if got := mergeFieldNames(sch); !slices.Equal(got, []string{schema.ColTS, schema.ColLine, schema.ColLabels, schema.ColMeta}) {
		t.Fatalf("fields = %v", got)
	}
}

// TestMergeForeignSchemaVariations merges inputs whose schemas are
// reconstructed without ARROW:schema metadata or carry harmless annotation
// differences: a map with non-nullable items is normalized to the canonical
// nullable-item map type instead of rejected.
func TestMergeForeignSchemaVariations(t *testing.T) {
	// A compatible file written without embedded ARROW:schema metadata.
	noSchema := mergeInputStore(t, mergeBaseFields(), false, mergeBaseCols(
		mergeTSs(20, 40),
		mergeStrs("x0", "x1"),
		mergeMaps(map[string]string{"a": "1"}, map[string]string{}),
		mergeEmptyMaps(2),
	))

	// A map whose items are non-nullable: identical layout, different
	// annotation. Stored with ARROW:schema so the variant survives the
	// round trip.
	variant := mergeBaseFields()
	variant[2].Type = arrow.MapOfFields(
		arrow.Field{Name: "key", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false},
	)
	nonNullItems := mergeInputStore(t, variant, true, mergeBaseCols(
		mergeTSs(10, 30),
		mergeStrs("a0", "a1"),
		mergeMaps(map[string]string{"b": "2"}, map[string]string{"c": "3"}),
		mergeEmptyMaps(2),
	))

	out := &bytes.Buffer{}
	if _, err := storage.Merge(t.Context(), out, []*storage.Reader{noSchema, nonNullItems}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	sch, rows := mergeRows(t, out.Bytes())

	// The union normalized both inputs to canonical base fields.
	wantNames := []string{schema.ColTS, schema.ColLine, schema.ColLabels, schema.ColMeta}
	if got := mergeFieldNames(sch); !slices.Equal(got, wantNames) {
		t.Fatalf("fields = %v, want %v", got, wantNames)
	}
	if got := sch.Field(2).Type; !arrow.TypeEqual(got, schema.LabelMapType) {
		t.Fatalf("labels type = %s, want %s", got, schema.LabelMapType)
	}
	want := [][]any{
		{int64(10), "a0", map[string]string{"b": "2"}, map[string]string{}},
		{int64(20), "x0", map[string]string{"a": "1"}, map[string]string{}},
		{int64(30), "a1", map[string]string{"c": "3"}, map[string]string{}},
		{int64(40), "x1", map[string]string{}, map[string]string{}},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
}

func TestMergeNullAndMapValues(t *testing.T) {
	fields := mergeBaseFields()
	fields[2].Nullable = true // labels
	fields[3].Nullable = true // meta
	in0 := mergeInput(t, fields, mergeBaseCols(
		mergeTSs(10, 20),
		mergeStrs("a0", "a1"),
		mergeMaps(map[string]string{"app": "web"}, nil),
		mergeMaps(nil, map[string]string{"k": "v"}),
	))

	out := &bytes.Buffer{}
	if _, err := storage.Merge(t.Context(), out, []*storage.Reader{in0}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	_, rows := mergeRows(t, out.Bytes())
	want := [][]any{
		{int64(10), "a0", map[string]string{"app": "web"}, nil},
		{int64(20), "a1", nil, map[string]string{"k": "v"}},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
}

func TestMergeEmptyInputs(t *testing.T) {
	t.Run("no inputs", func(t *testing.T) {
		out := &bytes.Buffer{}
		stats, err := storage.Merge(t.Context(), out, nil, []string{"level"})
		if err != nil {
			t.Fatalf("Merge: %v", err)
		}
		if stats.Rows != 0 || !stats.MinTS.IsZero() || !stats.MaxTS.IsZero() {
			t.Fatalf("stats = %+v", stats)
		}
		sch, rows := mergeRows(t, out.Bytes())
		if len(rows) != 0 {
			t.Fatalf("rows = %v", rows)
		}
		wantNames := []string{schema.ColTS, schema.ColLine, schema.ColLabels, schema.ColMeta, "level"}
		if got := mergeFieldNames(sch); !slices.Equal(got, wantNames) {
			t.Fatalf("fields = %v, want %v", got, wantNames)
		}
	})

	t.Run("empty and populated", func(t *testing.T) {
		empty := mergeInput(t, mergeBaseFields())
		full := mergeInput(t, mergeBaseFields(), mergeBaseCols(
			mergeTSs(10), mergeStrs("a0"), mergeEmptyMaps(1), mergeEmptyMaps(1),
		))
		out := &bytes.Buffer{}
		stats, err := storage.Merge(t.Context(), out, []*storage.Reader{empty, full}, nil)
		if err != nil {
			t.Fatalf("Merge: %v", err)
		}
		_, rows := mergeRows(t, out.Bytes())
		if stats.Rows != 1 || !reflect.DeepEqual(rows, [][]any{mergeRowOf(10, "a0")}) {
			t.Fatalf("rows = %v stats = %+v", rows, stats)
		}
	})
}

func TestMergeValidationErrors(t *testing.T) {
	valid := mergeInput(t, mergeBaseFields(), mergeBaseCols(
		mergeTSs(10), mergeStrs("a0"), mergeEmptyMaps(1), mergeEmptyMaps(1),
	))

	t.Run("nil writer", func(t *testing.T) {
		if _, err := storage.Merge(t.Context(), nil, []*storage.Reader{valid}, nil); err == nil {
			t.Fatal("Merge succeeded with nil writer")
		}
	})
	t.Run("canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := storage.Merge(ctx, &bytes.Buffer{}, []*storage.Reader{valid}, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("Merge error = %v, want context.Canceled", err)
		}
	})
	t.Run("nil reader", func(t *testing.T) {
		if _, err := storage.Merge(t.Context(), &bytes.Buffer{}, []*storage.Reader{nil}, nil); err == nil {
			t.Fatal("Merge succeeded with nil reader")
		}
	})
	t.Run("uninitialized reader", func(t *testing.T) {
		if _, err := storage.Merge(t.Context(), &bytes.Buffer{}, []*storage.Reader{{}}, nil); err == nil {
			t.Fatal("Merge succeeded with uninitialized reader")
		}
	})
	t.Run("reserved promoted name", func(t *testing.T) {
		if _, err := storage.Merge(t.Context(), &bytes.Buffer{}, nil, []string{"line"}); err == nil {
			t.Fatal("Merge succeeded with reserved promoted name")
		}
	})
	t.Run("incompatible extra types", func(t *testing.T) {
		str := mergeInput(t,
			append(mergeBaseFields(), mergeField("level", arrow.BinaryTypes.String)),
			[][]any{mergeTSs(10), mergeStrs("a0"), mergeEmptyMaps(1), mergeEmptyMaps(1), mergeStrs("x")})
		num := mergeInput(t,
			append(mergeBaseFields(), mergeField("level", arrow.PrimitiveTypes.Int64)),
			[][]any{mergeTSs(20), mergeStrs("b0"), mergeEmptyMaps(1), mergeEmptyMaps(1), []any{int64(1)}})
		if _, err := storage.Merge(t.Context(), &bytes.Buffer{}, []*storage.Reader{str, num}, nil); err == nil {
			t.Fatal("Merge succeeded with incompatible field types")
		}
	})
	t.Run("duplicate field", func(t *testing.T) {
		dupFields := append(mergeBaseFields(), mergeField("level", arrow.BinaryTypes.String), mergeField("level", arrow.BinaryTypes.String))
		s := arrow.NewSchema(dupFields, nil)
		rec := mergeRecord(t, s, mergeTSs(10), mergeStrs("a0"), mergeEmptyMaps(1), mergeEmptyMaps(1), mergeStrs("x"), mergeStrs("y"))
		defer rec.Release()
		data, err := mergeParquetE(s, true, rec)
		if err != nil {
			t.Skipf("parquet writer rejects duplicate field names: %v", err)
		}
		r, err := storage.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Skipf("reader rejects duplicate field names: %v", err)
		}
		t.Cleanup(func() { _ = r.Close() })
		if _, err := storage.Merge(t.Context(), &bytes.Buffer{}, []*storage.Reader{r}, nil); err == nil {
			t.Fatal("Merge succeeded with duplicate field names")
		}
	})
}

func TestMergeNullAndUnsortedTS(t *testing.T) {
	nullableTS := mergeBaseFields()
	nullableTS[0].Nullable = true

	t.Run("null first row", func(t *testing.T) {
		in := mergeInput(t, nullableTS, mergeBaseCols(
			mergeTSs(-1, 20), mergeStrs("a0", "a1"), mergeEmptyMaps(2), mergeEmptyMaps(2),
		))
		if _, err := storage.Merge(t.Context(), &bytes.Buffer{}, []*storage.Reader{in}, nil); err == nil {
			t.Fatal("Merge succeeded with null _ts")
		}
	})
	t.Run("null mid record", func(t *testing.T) {
		in := mergeInput(t, nullableTS, mergeBaseCols(
			mergeTSs(10, -1, 30), mergeStrs("a0", "a1", "a2"), mergeEmptyMaps(3), mergeEmptyMaps(3),
		))
		if _, err := storage.Merge(t.Context(), &bytes.Buffer{}, []*storage.Reader{in}, nil); err == nil {
			t.Fatal("Merge succeeded with mid-record null _ts")
		}
	})
	t.Run("descending within record", func(t *testing.T) {
		in := mergeInput(t, mergeBaseFields(), mergeBaseCols(
			mergeTSs(30, 10), mergeStrs("a0", "a1"), mergeEmptyMaps(2), mergeEmptyMaps(2),
		))
		if _, err := storage.Merge(t.Context(), &bytes.Buffer{}, []*storage.Reader{in}, nil); err == nil {
			t.Fatal("Merge succeeded with descending _ts")
		}
	})
	t.Run("descending across records", func(t *testing.T) {
		in := mergeInput(t, mergeBaseFields(),
			mergeBaseCols(mergeTSs(10, 20), mergeStrs("a0", "a1"), mergeEmptyMaps(2), mergeEmptyMaps(2)),
			mergeBaseCols(mergeTSs(15), mergeStrs("a2"), mergeEmptyMaps(1), mergeEmptyMaps(1)))
		if _, err := storage.Merge(t.Context(), &bytes.Buffer{}, []*storage.Reader{in}, nil); err == nil {
			t.Fatal("Merge succeeded with _ts descending across records")
		}
	})
}

func TestMergeInputReadFailure(t *testing.T) {
	want := errors.New("storage test: injected read failure")
	s := arrow.NewSchema(mergeBaseFields(), nil)
	rec := mergeRecord(t, s, mergeTSs(10, 20), mergeStrs("a0", "a1"), mergeEmptyMaps(2), mergeEmptyMaps(2))
	defer rec.Release()
	data := mergeParquet(t, s, rec)

	ras := &mergeFailRAS{Reader: bytes.NewReader(data), err: want}
	r, err := storage.NewReader(ras, int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	ras.fail.Store(true)
	if _, err := storage.Merge(t.Context(), io.Discard, []*storage.Reader{r}, nil); !errors.Is(err, want) {
		t.Fatalf("Merge error = %v, want wrapped %v", err, want)
	}
}

func TestMergeOutputWriteFailure(t *testing.T) {
	want := errors.New("storage test: injected write failure")
	in := mergeInput(t, mergeBaseFields(), mergeBaseCols(
		mergeTSs(10), mergeStrs("a0"), mergeEmptyMaps(1), mergeEmptyMaps(1),
	))
	if _, err := storage.Merge(t.Context(), &mergeFailWriter{err: want}, []*storage.Reader{in}, nil); !errors.Is(err, want) {
		t.Fatalf("Merge error = %v, want wrapped %v", err, want)
	}
}

func TestMergeCancellationMidMerge(t *testing.T) {
	const n = 3 * 8192
	us := make([]int64, n)
	lines := make([]string, n)
	for i := range n {
		us[i] = int64(i)
		lines[i] = "x"
	}
	in := mergeInput(t, mergeBaseFields(), mergeBaseCols(
		mergeTSs(us...), mergeStrs(lines...), mergeEmptyMaps(n), mergeEmptyMaps(n),
	))
	ctx := newMergeCountdownCtx(t.Context(), 64)
	if _, err := storage.Merge(ctx, io.Discard, []*storage.Reader{in}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Merge error = %v, want context.Canceled", err)
	}
}

func TestMergeOwnership(t *testing.T) {
	out := &mergeCloserSpy{}
	in := mergeInput(t, mergeBaseFields(), mergeBaseCols(
		mergeTSs(10), mergeStrs("a0"), mergeEmptyMaps(1), mergeEmptyMaps(1),
	))
	if _, err := storage.Merge(t.Context(), out, []*storage.Reader{in}, nil); err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if out.closed {
		t.Fatal("Merge closed the caller's output writer")
	}
}

func TestMergeStatsLabelValues(t *testing.T) {
	in0 := mergeInput(t,
		append(mergeBaseFields(), mergeField("level", arrow.BinaryTypes.String), mergeField("ns", arrow.BinaryTypes.String)),
		[][]any{
			mergeTSs(10, 30),
			mergeStrs("a0", "a1"),
			mergeEmptyMaps(2), mergeEmptyMaps(2),
			mergeStrs("warn", "info"),
			mergeStrs("b", "a"),
		})
	in1 := mergeInput(t,
		append(mergeBaseFields(), mergeField("level", arrow.BinaryTypes.String)),
		[][]any{
			mergeTSs(20, 40),
			mergeStrs("b0", "b1"),
			mergeEmptyMaps(2), mergeEmptyMaps(2),
			mergeStrs("info", "error"),
		})

	stats, err := storage.Merge(t.Context(), &bytes.Buffer{}, []*storage.Reader{in0, in1}, []string{"level", "ns"})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if stats.Rows != 4 || !stats.MinTS.Equal(schema.Time(10)) || !stats.MaxTS.Equal(schema.Time(40)) {
		t.Fatalf("stats = %+v", stats)
	}
	// in1 has no ns column, so ns is absent there: complete summaries record
	// absent promoted labels as the empty string.
	want := map[string][]string{"level": {"error", "info", "warn"}, "ns": {"", "a", "b"}}
	if !reflect.DeepEqual(stats.LabelValues, want) {
		t.Fatalf("LabelValues = %v, want %v", stats.LabelValues, want)
	}
	if !stats.LabelsComplete {
		t.Fatal("LabelsComplete = false, want true after Merge's Writer closes")
	}
}

func TestMergeStatsLabelValuesSchemaEvolution(t *testing.T) {
	oldInput := mergeInput(t, mergeBaseFields(), [][]any{
		mergeTSs(10, 20),
		mergeStrs("old-a", "old-b"),
		mergeMaps(
			map[string]string{"level": "old-map"},
			map[string]string{},
		),
		mergeEmptyMaps(2),
	})
	newInput := mergeInput(t,
		append(mergeBaseFields(), mergeField("level", arrow.BinaryTypes.String)),
		[][]any{
			mergeTSs(15, 25),
			mergeStrs("new-a", "new-b"),
			mergeMaps(
				map[string]string{"level": "new-map"},
				map[string]string{"level": "new-map"},
			),
			mergeEmptyMaps(2),
			[]any{nil, "new-col"},
		})

	stats, err := storage.Merge(t.Context(), &bytes.Buffer{}, []*storage.Reader{oldInput, newInput}, []string{"level"})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if stats.Rows != 4 {
		t.Fatalf("Rows = %d, want 4", stats.Rows)
	}
	if !stats.LabelsComplete {
		t.Fatal("LabelsComplete = false, want true after Merge's Writer closes")
	}
	// Old rows have no level column, so their map values are the effective
	// ones; the new row with a null column falls back to its map value, and
	// the new row with a value keeps the column over the conflicting entry.
	if want := []string{"", "new-col", "new-map", "old-map"}; !slices.Equal(stats.LabelValues["level"], want) {
		t.Fatalf("LabelValues[level] = %v, want %v", stats.LabelValues["level"], want)
	}
}
