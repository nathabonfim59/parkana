package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"reflect"
	"slices"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	parquetschema "github.com/apache/arrow-go/v18/parquet/schema"

	parkschema "github.com/nathabonfim59/parkana/internal/schema"
)

// fixtureRow is one row of a test Parquet file. It has the Parkana base
// columns plus one promoted label (level) and one pipeline field (score).
type fixtureRow struct {
	ts        int64
	line      string
	labels    map[string]string
	meta      map[string]string
	level     string
	levelNull bool
	score     int64
	scoreNull bool
}

func fixtureSchema() *arrow.Schema {
	fields := parkschema.Base()
	fields = append(fields,
		arrow.Field{Name: "level", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "score", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	)
	return arrow.NewSchema(fields, nil)
}

func writeFixture(t *testing.T, mem memory.Allocator, rows []fixtureRow, storeSchema bool) []byte {
	t.Helper()
	sc := fixtureSchema()
	rec := fixtureBatch(t, mem, sc, rows)
	defer rec.Release()
	return writeRecords(t, mem, sc, []arrow.RecordBatch{rec}, storeSchema)
}

func writeEmptyFixture(t *testing.T, mem memory.Allocator, sc *arrow.Schema, storeSchema bool) []byte {
	t.Helper()
	rb := array.NewRecordBuilder(mem, sc)
	defer rb.Release()
	rec := rb.NewRecordBatch()
	defer rec.Release()
	return writeRecords(t, mem, sc, []arrow.RecordBatch{rec}, storeSchema)
}

func writeRecords(t *testing.T, mem memory.Allocator, sc *arrow.Schema, recs []arrow.RecordBatch, storeSchema bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	opts := []pqarrow.WriterOption{pqarrow.WithAllocator(mem)}
	if storeSchema {
		opts = append(opts, pqarrow.WithStoreSchema())
	}
	fw, err := pqarrow.NewFileWriter(sc, &buf,
		parquet.NewWriterProperties(
			parquet.WithAllocator(mem),
			parquet.WithCompression(compress.Codecs.Zstd),
		),
		pqarrow.NewArrowWriterProperties(opts...),
	)
	if err != nil {
		t.Fatalf("pqarrow.NewFileWriter: %v", err)
	}
	for _, rec := range recs {
		if err := fw.Write(rec); err != nil {
			_ = fw.Close()
			t.Fatalf("FileWriter.Write: %v", err)
		}
	}
	if err := fw.Close(); err != nil {
		t.Fatalf("FileWriter.Close: %v", err)
	}
	return buf.Bytes()
}

func fixtureBatch(t *testing.T, mem memory.Allocator, sc *arrow.Schema, rows []fixtureRow) arrow.RecordBatch {
	t.Helper()
	rb := array.NewRecordBuilder(mem, sc)
	defer rb.Release()

	ts := rb.Field(0).(*array.TimestampBuilder)
	line := rb.Field(1).(*array.StringBuilder)
	labels := rb.Field(2).(*array.MapBuilder)
	meta := rb.Field(3).(*array.MapBuilder)
	level := rb.Field(4).(*array.StringBuilder)
	score := rb.Field(5).(*array.Int64Builder)

	for _, row := range rows {
		ts.Append(arrow.Timestamp(row.ts))
		line.Append(row.line)
		appendStringMap(labels, row.labels)
		appendStringMap(meta, row.meta)
		if row.levelNull {
			level.AppendNull()
		} else {
			level.Append(row.level)
		}
		if row.scoreNull {
			score.AppendNull()
		} else {
			score.Append(row.score)
		}
	}
	return rb.NewRecordBatch()
}

func appendStringMap(b *array.MapBuilder, m map[string]string) {
	if m == nil {
		b.AppendNull()
		return
	}
	b.Append(true)
	kb := b.KeyBuilder().(*array.StringBuilder)
	vb := b.ItemBuilder().(*array.StringBuilder)
	for _, k := range slices.Sorted(maps.Keys(m)) {
		kb.Append(k)
		vb.Append(m[k])
	}
}

func readRows(t *testing.T, r *Reader) ([]fixtureRow, error) {
	t.Helper()
	var rows []fixtureRow
	for rec, err := range r.Records(t.Context()) {
		if err != nil {
			return rows, err
		}
		for i := range int(rec.NumRows()) {
			rows = append(rows, fixtureRowAt(t, rec, i))
		}
	}
	return rows, nil
}

func fixtureRowAt(t *testing.T, rec arrow.RecordBatch, i int) fixtureRow {
	t.Helper()
	ts := rec.Column(0).(*array.Timestamp)
	line := rec.Column(1).(*array.String)
	labels := rec.Column(2).(*array.Map)
	meta := rec.Column(3).(*array.Map)
	level := rec.Column(4).(*array.String)
	score := rec.Column(5).(*array.Int64)

	row := fixtureRow{
		ts:     int64(ts.Value(i)),
		line:   line.Value(i),
		labels: stringMapAt(t, labels, i),
		meta:   stringMapAt(t, meta, i),
	}
	if level.IsNull(i) {
		row.levelNull = true
	} else {
		row.level = level.Value(i)
	}
	if score.IsNull(i) {
		row.scoreNull = true
	} else {
		row.score = score.Value(i)
	}
	return row
}

func stringMapAt(t *testing.T, a *array.Map, i int) map[string]string {
	t.Helper()
	if a.IsNull(i) {
		return nil
	}
	start, end := a.ValueOffsets(i)
	keys := a.Keys().(*array.String)
	items := a.Items().(*array.String)
	out := make(map[string]string, end-start)
	for j := start; j < end; j++ {
		out[keys.Value(int(j))] = items.Value(int(j))
	}
	return out
}

func fixtureRows() []fixtureRow {
	const base = int64(1_700_000_000_000_000)
	return []fixtureRow{
		{
			ts:     base,
			line:   "first line",
			labels: map[string]string{"app": "api", "env": "prod"},
			meta:   map[string]string{"trace_id": "abc"},
			level:  "info",
			score:  7,
		},
		{
			ts:        base + 1_000_000,
			line:      "second line",
			labels:    map[string]string{},
			meta:      map[string]string{},
			levelNull: true,
			scoreNull: true,
		},
		{
			ts:     base + 2_500_000,
			line:   "third line",
			labels: map[string]string{"app": "api"},
			meta:   map[string]string{},
			level:  "warn",
			score:  42,
		},
	}
}

func loadRows(n int) []fixtureRow {
	const base = int64(1_700_000_000_000_000)
	rows := make([]fixtureRow, n)
	for i := range n {
		rows[i] = fixtureRow{
			ts:     base + int64(i),
			line:   fmt.Sprintf("line-%d", i),
			labels: map[string]string{"app": "load"},
			meta:   map[string]string{"i": fmt.Sprint(i)},
			level:  "info",
			score:  int64(i),
		}
	}
	return rows
}

func TestReaderZeroValue(t *testing.T) {
	for _, tc := range []struct {
		name string
		r    *Reader
	}{
		{name: "nil", r: nil},
		{name: "zero", r: &Reader{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.r.Schema(); got != nil {
				t.Fatalf("Schema() = %v, want nil", got)
			}
			var gotErr error
			for rec, err := range tc.r.Records(t.Context()) {
				if rec != nil {
					t.Fatalf("Records yielded batch %v, want nil", rec)
				}
				gotErr = err
			}
			if !errors.Is(gotErr, errReaderUninitialized) {
				t.Fatalf("Records error = %v, want %v", gotErr, errReaderUninitialized)
			}
			if err := tc.r.Close(); err != nil {
				t.Fatalf("Close error = %v, want nil", err)
			}
		})
	}
}

func TestReaderRoundTrip(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	want := fixtureRows()
	data := writeFixture(t, mem, want, true)

	r, err := newReader(bytes.NewReader(data), int64(len(data)), mem)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Errorf("Reader.Close: %v", err)
		}
	}()

	sch := r.Schema()
	if sch == nil {
		t.Fatal("Schema() = nil")
	}
	if got, wantN := sch.NumFields(), 6; got != wantN {
		t.Fatalf("Schema() has %d fields, want %d", got, wantN)
	}
	if got := sch.Field(4).Name; got != "level" {
		t.Fatalf("extras not retained: field 4 = %q", got)
	}
	if got := sch.Field(5).Name; got != "score" {
		t.Fatalf("extras not retained: field 5 = %q", got)
	}
	tsType, ok := sch.Field(0).Type.(*arrow.TimestampType)
	if !ok || tsType.Unit != arrow.Microsecond || tsType.TimeZone != "UTC" {
		t.Fatalf("_ts type = %v, want timestamp[us, UTC]", sch.Field(0).Type)
	}

	got, err := readRows(t, r)
	if err != nil {
		t.Fatalf("Records: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows mismatch:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestReaderSchemaWithoutArrowMetadata(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	want := fixtureRows()
	data := writeFixture(t, mem, want, false)

	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("file.NewParquetReader: %v", err)
	}
	defer func() {
		if err := pf.Close(); err != nil {
			t.Errorf("parquet reader Close: %v", err)
		}
	}()
	if got := pf.MetaData().KeyValueMetadata().FindValue("ARROW:schema"); got != nil {
		t.Fatalf("fixture unexpectedly has ARROW:schema metadata: %q", *got)
	}

	r, err := newReader(bytes.NewReader(data), int64(len(data)), mem)
	if err != nil {
		t.Fatalf("newReader without ARROW:schema: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Errorf("Reader.Close: %v", err)
		}
	}()

	got, err := readRows(t, r)
	if err != nil {
		t.Fatalf("Records: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows mismatch:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestReaderParquetTimestampType(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	data := writeFixture(t, mem, fixtureRows(), true)
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("file.NewParquetReader: %v", err)
	}
	defer func() {
		if err := pf.Close(); err != nil {
			t.Errorf("parquet reader Close: %v", err)
		}
	}()

	col := pf.MetaData().Schema.Column(0)
	if got := col.Name(); got != parkschema.ColTS {
		t.Fatalf("column 0 = %q, want %q", got, parkschema.ColTS)
	}
	lt, ok := col.LogicalType().(parquetschema.TimestampLogicalType)
	if !ok {
		t.Fatalf("column 0 logical type = %T, want timestamp", col.LogicalType())
	}
	if got := lt.TimeUnit(); got != parquetschema.TimeUnitMicros {
		t.Fatalf("timestamp unit = %v, want micros", got)
	}
	if !lt.IsAdjustedToUTC() {
		t.Fatal("timestamp is not adjusted to UTC")
	}
}

func TestReaderInvalidInput(t *testing.T) {
	mem := memory.NewGoAllocator()

	valid := writeFixture(t, mem, fixtureRows(), true)

	cases := []struct {
		name    string
		data    []byte
		size    int64
		wantErr error
	}{
		{name: "nil", data: nil, size: 0, wantErr: errInvalidFileSize},
		{name: "tiny", data: []byte("PAR1"), size: 4, wantErr: errInvalidFileSize},
		{name: "size below footer", data: valid, size: 7, wantErr: errInvalidFileSize},
		{name: "not parquet", data: []byte("this is not a parquet file"), size: 27},
		{name: "truncated footer", data: valid[:len(valid)-10], size: int64(len(valid) - 10)},
		{name: "truncated data", data: valid[:len(valid)/2], size: int64(len(valid) / 2)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewReader(bytes.NewReader(tc.data), tc.size)
			if err == nil {
				_ = r.Close()
				t.Fatal("NewReader succeeded, want error")
			}
			if r != nil {
				t.Fatalf("NewReader returned %v with an error", r)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("NewReader error = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestReaderSizeMismatch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	data := writeFixture(t, mem, fixtureRows(), true)
	for _, delta := range []int64{-1, 1, 8} {
		t.Run(fmt.Sprintf("delta %+d", delta), func(t *testing.T) {
			size := int64(len(data)) + delta
			r, err := NewReader(bytes.NewReader(data), size)
			if err == nil {
				_ = r.Close()
				t.Fatal("NewReader succeeded with a mismatched size")
			}
			if !errors.Is(err, errInvalidFileSize) {
				t.Fatalf("NewReader error = %v, want %v", err, errInvalidFileSize)
			}
		})
	}
}

func TestReaderPreservesCallerSeekPosition(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	data := writeFixture(t, mem, fixtureRows(), true)
	br := bytes.NewReader(data)
	const pos = 7
	if _, err := br.Seek(pos, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	r, err := NewReader(br, int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Errorf("Reader.Close: %v", err)
		}
	}()

	if got, err := br.Seek(0, io.SeekCurrent); err != nil {
		t.Fatalf("SeekCurrent: %v", err)
	} else if got != pos {
		t.Fatalf("caller position = %d, want %d", got, pos)
	}
	if _, err := readRows(t, r); err != nil {
		t.Fatalf("Records: %v", err)
	}
}

func TestReaderNilContext(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	data := writeFixture(t, mem, fixtureRows(), true)
	r, err := newReader(bytes.NewReader(data), int64(len(data)), mem)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Errorf("Reader.Close: %v", err)
		}
	}()

	// Records must reject a nil context instead of substituting
	// context.Background, which would drop tenant scope from the context.
	var nilCtx context.Context
	var gotErr error
	for rec, err := range r.Records(nilCtx) {
		if rec != nil {
			t.Fatal("Records yielded a batch with a nil context")
		}
		gotErr = err
	}
	if !errors.Is(gotErr, errNilContext) {
		t.Fatalf("Records(nil) error = %v, want %v", gotErr, errNilContext)
	}
}

func TestReaderCorruptPage(t *testing.T) {
	mem := memory.NewGoAllocator()

	data := writeFixture(t, mem, fixtureRows(), true)

	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("file.NewParquetReader: %v", err)
	}
	chunk, err := pf.MetaData().RowGroup(0).ColumnChunk(0)
	if err != nil {
		t.Fatalf("ColumnChunk: %v", err)
	}
	offset := chunk.DataPageOffset()
	if err := pf.Close(); err != nil {
		t.Fatalf("parquet reader Close: %v", err)
	}

	corrupt := bytes.Clone(data)
	corrupt[offset] ^= 0xff

	r, err := NewReader(bytes.NewReader(corrupt), int64(len(corrupt)))
	if err != nil {
		t.Fatalf("NewReader on corrupt page: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Errorf("Reader.Close: %v", err)
		}
	}()

	var gotErr error
	for _, err := range r.Records(t.Context()) {
		if err != nil {
			gotErr = err
			break
		}
	}
	if gotErr == nil {
		t.Fatal("Records succeeded on a corrupt page, want decode error")
	}
}

func TestReaderCancellation(t *testing.T) {
	t.Run("before first batch", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)

		data := writeFixture(t, mem, fixtureRows(), true)
		r, err := newReader(bytes.NewReader(data), int64(len(data)), mem)
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		defer func() {
			if err := r.Close(); err != nil {
				t.Errorf("Reader.Close: %v", err)
			}
		}()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		var batches int
		var gotErr error
		for rec, err := range r.Records(ctx) {
			if err != nil {
				gotErr = err
				continue
			}
			if rec == nil {
				t.Fatal("Records yielded a nil batch without an error")
			}
			batches++
		}
		if batches != 0 {
			t.Fatalf("Records yielded %d batches after cancellation, want 0", batches)
		}
		if !errors.Is(gotErr, context.Canceled) {
			t.Fatalf("Records error = %v, want context.Canceled", gotErr)
		}
	})

	t.Run("between batches", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)

		data := writeFixture(t, mem, loadRows(readerBatchSize+100), true)
		r, err := newReader(bytes.NewReader(data), int64(len(data)), mem)
		if err != nil {
			t.Fatalf("newReader: %v", err)
		}
		defer func() {
			if err := r.Close(); err != nil {
				t.Errorf("Reader.Close: %v", err)
			}
		}()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var rows int
		var gotErr error
		for rec, err := range r.Records(ctx) {
			if err != nil {
				gotErr = err
				break
			}
			rows += int(rec.NumRows())
			if rows >= readerBatchSize {
				cancel()
			}
		}
		if rows != readerBatchSize {
			t.Fatalf("read %d rows before cancellation, want %d", rows, readerBatchSize)
		}
		if !errors.Is(gotErr, context.Canceled) {
			t.Fatalf("Records error = %v, want context.Canceled", gotErr)
		}
	})
}

func TestReaderEarlyBreakReleasesBatch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	data := writeFixture(t, mem, loadRows(readerBatchSize+100), true)
	r, err := newReader(bytes.NewReader(data), int64(len(data)), mem)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Errorf("Reader.Close: %v", err)
		}
	}()

	baseline := mem.CurrentAlloc()

	var rows int
	for rec, err := range r.Records(t.Context()) {
		if err != nil {
			t.Fatalf("Records: %v", err)
		}
		rows += int(rec.NumRows())
		break
	}
	if rows != readerBatchSize {
		t.Fatalf("first batch has %d rows, want %d", rows, readerBatchSize)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Reader.Close: %v", err)
	}
	if got := mem.CurrentAlloc(); got != baseline {
		t.Fatalf("allocator holds %d bytes after early break, want %d", got, baseline)
	}
}

func TestReaderCloseDoesNotCloseCallerIO(t *testing.T) {
	mem := memory.NewGoAllocator()

	data := writeFixture(t, mem, fixtureRows(), true)

	spy := &readerAtSeekerCloserSpy{Reader: bytes.NewReader(data)}
	r, err := NewReader(spy, int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if _, err := readRows(t, r); err != nil {
		t.Fatalf("Records: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Reader.Close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Reader.Close: %v", err)
	}
	if spy.closed {
		t.Fatal("Reader.Close closed caller-owned I/O")
	}

	var gotErr error
	for _, err := range r.Records(t.Context()) {
		gotErr = err
	}
	if !errors.Is(gotErr, errReaderClosed) {
		t.Fatalf("Records after Close error = %v, want %v", gotErr, errReaderClosed)
	}
}

func TestReaderNewReaderErrorDoesNotCloseCallerIO(t *testing.T) {
	spy := &readerAtSeekerCloserSpy{Reader: bytes.NewReader([]byte("not parquet at all"))}
	if _, err := NewReader(spy, int64(spy.Len())); err == nil {
		t.Fatal("NewReader succeeded, want error")
	}
	if spy.closed {
		t.Fatal("NewReader closed caller-owned I/O")
	}
}

func TestReaderEmptyFile(t *testing.T) {
	mem := memory.NewGoAllocator()

	data := writeFixture(t, mem, nil, true)
	r, err := NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Errorf("Reader.Close: %v", err)
		}
	}()

	rows, err := readRows(t, r)
	if err != nil {
		t.Fatalf("Records: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("read %d rows from an empty file, want 0", len(rows))
	}
}

func TestReaderRejectsInvalidBaseSchema(t *testing.T) {
	mem := memory.NewGoAllocator()

	replace := func(name string, typ arrow.DataType) *arrow.Schema {
		fields := parkschema.Base()
		for i := range fields {
			if fields[i].Name == name {
				fields[i].Type = typ
			}
		}
		return arrow.NewSchema(fields, nil)
	}
	without := func(name string) *arrow.Schema {
		fields := slices.DeleteFunc(parkschema.Base(), func(f arrow.Field) bool {
			return f.Name == name
		})
		return arrow.NewSchema(fields, nil)
	}

	cases := []struct {
		name   string
		schema *arrow.Schema
	}{
		{name: "missing _ts", schema: without(parkschema.ColTS)},
		{name: "missing line", schema: without(parkschema.ColLine)},
		{name: "missing labels", schema: without(parkschema.ColLabels)},
		{name: "missing meta", schema: without(parkschema.ColMeta)},
		{name: "ts int64", schema: replace(parkschema.ColTS, arrow.PrimitiveTypes.Int64)},
		{name: "ts millis", schema: replace(parkschema.ColTS, &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"})},
		{name: "ts no timezone", schema: replace(parkschema.ColTS, &arrow.TimestampType{Unit: arrow.Microsecond})},
		{name: "ts local timezone", schema: replace(parkschema.ColTS, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Europe/Paris"})},
		{name: "line int64", schema: replace(parkschema.ColLine, arrow.PrimitiveTypes.Int64)},
		{name: "labels wrong value", schema: replace(parkschema.ColLabels, arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64))},
		{name: "meta list", schema: replace(parkschema.ColMeta, arrow.ListOf(arrow.BinaryTypes.String))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := writeEmptyFixture(t, mem, tc.schema, true)
			r, err := NewReader(bytes.NewReader(data), int64(len(data)))
			if err == nil {
				_ = r.Close()
				t.Fatal("NewReader succeeded, want schema error")
			}
			if !errors.Is(err, errInvalidSchema) {
				t.Fatalf("NewReader error = %v, want %v", err, errInvalidSchema)
			}
		})
	}
}

// readerAtSeekerCloserSpy implements io.Closer to prove that Reader never
// closes caller-owned I/O, even when the caller's type implements Close.
type readerAtSeekerCloserSpy struct {
	*bytes.Reader
	closed bool
}

func (r *readerAtSeekerCloserSpy) Close() error {
	r.closed = true
	return nil
}
