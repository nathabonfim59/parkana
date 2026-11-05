package storage_test

import (
	"bytes"
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	parquetschema "github.com/apache/arrow-go/v18/parquet/schema"

	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
)

type writerRow struct {
	ts         time.Time
	line       string
	labels     map[string]string
	meta       map[string]string
	promoted   map[string]*string
	statusCode *int64
	// nullLabelValues makes the labels map entry for that key null while the
	// key itself is still present.
	nullLabelValues map[string]bool
	// nullLabels makes the whole labels map null for the row.
	nullLabels bool
}

type writerCloseSpy struct {
	bytes.Buffer
	closed bool
}

func (w *writerCloseSpy) Close() error {
	w.closed = true
	return nil
}

type writerFailingSink struct {
	limit      int
	shortWrite bool
	written    int
	closed     bool
	err        error
}

func (w *writerFailingSink) Write(p []byte) (int, error) {
	if w.written+len(p) > w.limit {
		avail := max(0, w.limit-w.written)
		w.written += avail
		if w.shortWrite && avail < len(p) {
			return avail, nil
		}
		return avail, w.err
	}
	w.written += len(p)
	return len(p), nil
}

func (w *writerFailingSink) Close() error {
	w.closed = true
	return nil
}

func writerSchemaWithPipelineCol(t *testing.T, promoted []string) *arrow.Schema {
	t.Helper()
	base, err := schema.ForStream(promoted)
	if err != nil {
		t.Fatal(err)
	}
	fields := append(slices.Clone(base.Fields()), arrow.Field{
		Name:     "status_code",
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: true,
	})
	return arrow.NewSchema(fields, nil)
}

func writerBuildBatch(t *testing.T, mem memory.Allocator, s *arrow.Schema, rows []writerRow) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(mem, s)
	defer b.Release()

	tsBuilder := b.Field(0).(*array.TimestampBuilder)
	lineBuilder := b.Field(1).(*array.StringBuilder)
	labelsBuilder := b.Field(2).(*array.MapBuilder)
	metaBuilder := b.Field(3).(*array.MapBuilder)

	for _, r := range rows {
		tsBuilder.Append(arrow.Timestamp(schema.TS(r.ts)))
		lineBuilder.Append(r.line)
		if r.nullLabels {
			labelsBuilder.Append(false)
		} else {
			writerAppendMapNulls(labelsBuilder, r.labels, r.nullLabelValues)
		}
		writerAppendMap(metaBuilder, r.meta)

		for i := 4; i < s.NumFields(); i++ {
			f := s.Field(i)
			switch f.Name {
			case "status_code":
				ib := b.Field(i).(*array.Int64Builder)
				if r.statusCode == nil {
					ib.AppendNull()
				} else {
					ib.Append(*r.statusCode)
				}
			default:
				sb := b.Field(i).(*array.StringBuilder)
				val, ok := r.promoted[f.Name]
				if !ok || val == nil {
					sb.AppendNull()
				} else {
					sb.Append(*val)
				}
			}
		}
	}
	return b.NewRecordBatch()
}

func writerAppendMap(mb *array.MapBuilder, m map[string]string) {
	writerAppendMapNulls(mb, m, nil)
}

func writerAppendMapNulls(mb *array.MapBuilder, m map[string]string, nullValues map[string]bool) {
	mb.Append(true)
	kb := mb.KeyBuilder().(*array.StringBuilder)
	ib := mb.ItemBuilder().(*array.StringBuilder)
	for _, k := range slices.Sorted(maps.Keys(m)) {
		kb.Append(k)
		if nullValues[k] {
			ib.AppendNull()
			continue
		}
		ib.Append(m[k])
	}
}

func writerAssertSchemaMatches(t *testing.T, fr *pqarrow.FileReader, want *arrow.Schema) {
	t.Helper()
	if fr.Manifest.OriginSchema == nil || !fr.Manifest.OriginSchema.Equal(want) {
		t.Fatalf("OriginSchema mismatch:\n got: %v\nwant: %v", fr.Manifest.OriginSchema, want)
	}
	got, err := fr.Schema()
	if err != nil {
		t.Fatalf("fr.Schema: %v", err)
	}
	if got.NumFields() != want.NumFields() {
		t.Fatalf("NumFields = %d, want %d\n got: %s\nwant: %s", got.NumFields(), want.NumFields(), got, want)
	}
	for i := range want.NumFields() {
		gf, wf := got.Field(i), want.Field(i)
		if gf.Name != wf.Name || gf.Nullable != wf.Nullable || !arrow.TypeEqual(gf.Type, wf.Type) {
			t.Fatalf("field %d mismatch:\n got: %v\nwant: %v", i, gf, wf)
		}
	}
}

func writerReadMapRow(mc *array.Map, row int) map[string]string {
	beg, end := mc.ValueOffsets(row)
	keys := mc.Keys().(*array.String)
	items := mc.Items().(*array.String)
	out := make(map[string]string, int(end-beg))
	for i := int(beg); i < int(end); i++ {
		out[keys.Value(i)] = items.Value(i)
	}
	return out
}

func TestWriterRoundTripAndMetadata(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	promoted := []string{"level", "env"}
	s := writerSchemaWithPipelineCol(t, promoted)

	// Use non-UTC input Locations to verify UTC normalization via schema.TS/schema.Time.
	tzMinus3 := time.FixedZone("UTC-3", -3*3600)
	t0 := time.Date(2026, 11, 3, 18, 0, 0, 123456000, tzMinus3)
	t1 := t0.Add(250 * time.Microsecond)
	t2 := t0.Add(2 * time.Second)

	batch1Rows := []writerRow{
		{
			ts:         t0,
			line:       "first line",
			labels:     map[string]string{"service_name": "ingester", "instance": "i-1"},
			meta:       map[string]string{"trace_id": "abc123"},
			promoted:   map[string]*string{"level": new("warn")},
			statusCode: new(int64(200)),
		},
		{
			ts:         t0, // equal timestamp within batch
			line:       "second line same microsecond",
			labels:     nil,
			meta:       map[string]string{},
			promoted:   map[string]*string{"level": new("error")},
			statusCode: nil,
		},
	}
	batch2Rows := []writerRow{
		{
			ts:         t0, // equal timestamp across Write calls
			line:       "third line across batch",
			labels:     map[string]string{"region": "us-east"},
			meta:       map[string]string{"span_id": "s1", "user": "u1"},
			promoted:   map[string]*string{"level": nil},
			statusCode: new(int64(500)),
		},
		{
			ts:         t1,
			line:       "fourth line",
			labels:     map[string]string{"job": "api"},
			meta:       nil,
			promoted:   map[string]*string{"level": new("info")},
			statusCode: new(int64(204)),
		},
		{
			ts:         t2,
			line:       "fifth line",
			labels:     map[string]string{"job": "api"},
			meta:       map[string]string{"trace_id": "xyz789"},
			promoted:   map[string]*string{"level": new("warn")},
			statusCode: nil,
		},
	}

	sink := &writerCloseSpy{}
	promotedArg := []string{"level", "env"}
	w, err := storage.NewWriter(sink, s, promotedArg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// Mutating caller's slice after NewWriter must not affect Writer.
	promotedArg[0] = "mutated"

	rec1 := writerBuildBatch(t, alloc, s, batch1Rows)
	defer rec1.Release()
	if err := w.Write(rec1); err != nil {
		t.Fatalf("Write batch 1: %v", err)
	}

	rec2 := writerBuildBatch(t, alloc, s, batch2Rows)
	defer rec2.Release()
	if err := w.Write(rec2); err != nil {
		t.Fatalf("Write batch 2: %v", err)
	}

	stats, err := w.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
	if sink.closed {
		t.Fatal("Close closed caller-owned io.Writer")
	}

	if stats.Rows != 5 {
		t.Fatalf("stats.Rows = %d, want 5", stats.Rows)
	}
	if stats.MinTS.Location() != time.UTC || !stats.MinTS.Equal(t0.UTC()) {
		t.Fatalf("stats.MinTS = %v (%v), want %v (UTC)", stats.MinTS, stats.MinTS.Location(), t0.UTC())
	}
	if stats.MaxTS.Location() != time.UTC || !stats.MaxTS.Equal(t2.UTC()) {
		t.Fatalf("stats.MaxTS = %v (%v), want %v (UTC)", stats.MaxTS, stats.MaxTS.Location(), t2.UTC())
	}
	if want := []string{"", "error", "info", "warn"}; !slices.Equal(stats.LabelValues["level"], want) {
		t.Fatalf("LabelValues[level] = %v, want %v", stats.LabelValues["level"], want)
	}
	envVals, hasEnv := stats.LabelValues["env"]
	if !hasEnv || !slices.Equal(envVals, []string{""}) {
		t.Fatalf("LabelValues[env] = %#v (present=%v), want []string{\"\"}", envVals, hasEnv)
	}
	if len(stats.LabelValues) != 2 {
		t.Fatalf("len(LabelValues) = %d, want 2: %v", len(stats.LabelValues), stats.LabelValues)
	}
	if !stats.LabelsComplete {
		t.Fatal("LabelsComplete = false, want true after successful Close")
	}

	// Inspect low-level Parquet metadata, logical types, zstd, sorting, stats, and page indexes.
	pqReader, err := file.NewParquetReader(bytes.NewReader(sink.Bytes()))
	if err != nil {
		t.Fatalf("NewParquetReader: %v", err)
	}
	defer pqReader.Close()

	fileMeta := pqReader.MetaData()
	if fileMeta.NumRows != 5 {
		t.Fatalf("Parquet NumRows = %d, want 5", fileMeta.NumRows)
	}
	if fileMeta.KeyValueMetadata().FindValue("ARROW:schema") == nil {
		t.Fatal("missing ARROW:schema key-value metadata (WithStoreSchema)")
	}

	// Check _ts Parquet physical and logical type: INT64 + TIMESTAMP(MICROS, isAdjustedToUTC=true).
	tsDescr := fileMeta.Schema.Column(0)
	if tsDescr.Name() != schema.ColTS {
		t.Fatalf("leaf col 0 name = %q, want %q", tsDescr.Name(), schema.ColTS)
	}
	if tsDescr.PhysicalType() != parquet.Types.Int64 {
		t.Fatalf("_ts physical type = %s, want INT64", tsDescr.PhysicalType())
	}
	tsLogical, ok := tsDescr.LogicalType().(parquetschema.TimestampLogicalType)
	if !ok {
		t.Fatalf("_ts logical type = %T (%v), want TimestampLogicalType", tsDescr.LogicalType(), tsDescr.LogicalType())
	}
	if tsLogical.TimeUnit() != parquetschema.TimeUnitMicros || !tsLogical.IsAdjustedToUTC() {
		t.Fatalf("_ts logical = unit %v adjustedToUTC=%v, want MICROS and true", tsLogical.TimeUnit(), tsLogical.IsAdjustedToUTC())
	}

	if pqReader.NumRowGroups() != 1 {
		t.Fatalf("NumRowGroups = %d, want 1", pqReader.NumRowGroups())
	}
	rgMeta := fileMeta.RowGroup(0)
	sortCols := rgMeta.SortingColumns()
	if len(sortCols) != 1 || sortCols[0].ColumnIdx != 0 || sortCols[0].Descending || sortCols[0].NullsFirst {
		t.Fatalf("SortingColumns = %+v, want [{ColumnIdx:0 Descending:false NullsFirst:false}]", sortCols)
	}

	for colIdx := range rgMeta.NumColumns() {
		cc, err := rgMeta.ColumnChunk(colIdx)
		if err != nil {
			t.Fatalf("ColumnChunk(%d): %v", colIdx, err)
		}
		if cc.Compression() != compress.Codecs.Zstd {
			t.Fatalf("col %d (%s) compression = %s, want ZSTD", colIdx, cc.PathInSchema(), cc.Compression())
		}
		statsSet, err := cc.StatsSet()
		if err != nil || !statsSet {
			t.Fatalf("col %d (%s) StatsSet = %v, %v, want true", colIdx, cc.PathInSchema(), statsSet, err)
		}
		if cc.GetOffsetIndexLocation() == nil {
			t.Fatalf("col %d (%s) missing OffsetIndexLocation", colIdx, cc.PathInSchema())
		}
		if cc.GetColumnIndexLocation() == nil {
			t.Fatalf("col %d (%s) missing ColumnIndexLocation", colIdx, cc.PathInSchema())
		}
	}

	// Check _ts column chunk min/max statistics match exact microseconds.
	tsChunk, err := rgMeta.ColumnChunk(0)
	if err != nil {
		t.Fatal(err)
	}
	rawStats, err := tsChunk.Statistics()
	if err != nil {
		t.Fatal(err)
	}
	i64Stats := rawStats.(*metadata.Int64Statistics)
	if i64Stats.Min() != schema.TS(t0) || i64Stats.Max() != schema.TS(t2) {
		t.Fatalf("_ts chunk stats = [%d, %d], want [%d, %d]", i64Stats.Min(), i64Stats.Max(), schema.TS(t0), schema.TS(t2))
	}

	// Read page index structures back through PageIndexReader.
	pir := pqReader.GetPageIndexReader()
	if pir == nil {
		t.Fatal("GetPageIndexReader returned nil")
	}
	rgPir, err := pir.RowGroup(0)
	if err != nil || rgPir == nil {
		t.Fatalf("PageIndexReader.RowGroup(0) = %v, %v", rgPir, err)
	}
	colIdx0, err := rgPir.GetColumnIndex(0)
	if err != nil || colIdx0 == nil {
		t.Fatalf("GetColumnIndex(0) = %v, %v", colIdx0, err)
	}
	offIdx0, err := rgPir.GetOffsetIndex(0)
	if err != nil || offIdx0 == nil || len(offIdx0.GetPageLocations()) == 0 {
		t.Fatalf("GetOffsetIndex(0) = %v, %v", offIdx0, err)
	}

	// Round-trip Arrow schema and data through pqarrow.NewFileReader.
	fr, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{BatchSize: 8192}, alloc)
	if err != nil {
		t.Fatalf("pqarrow.NewFileReader: %v", err)
	}
	writerAssertSchemaMatches(t, fr, s)

	tbl, err := fr.ReadTable(t.Context())
	if err != nil {
		t.Fatalf("ReadTable: %v", err)
	}
	defer tbl.Release()
	if tbl.NumRows() != 5 {
		t.Fatalf("table NumRows = %d, want 5", tbl.NumRows())
	}

	allRows := slices.Concat(batch1Rows, batch2Rows)
	tr := array.NewTableReader(tbl, 8192)
	defer tr.Release()
	rowIdx := 0
	for tr.Next() {
		rec := tr.RecordBatch()
		tsCol := rec.Column(0).(*array.Timestamp)
		lineCol := rec.Column(1).(*array.String)
		labelsCol := rec.Column(2).(*array.Map)
		metaCol := rec.Column(3).(*array.Map)
		levelCol := rec.Column(4).(*array.String)
		envCol := rec.Column(5).(*array.String)
		statusCol := rec.Column(6).(*array.Int64)

		for r := range int(rec.NumRows()) {
			want := allRows[rowIdx]
			if gotTS := int64(tsCol.Value(r)); gotTS != schema.TS(want.ts) {
				t.Fatalf("row %d _ts = %d, want %d", rowIdx, gotTS, schema.TS(want.ts))
			}
			if gotLine := lineCol.Value(r); gotLine != want.line {
				t.Fatalf("row %d line = %q, want %q", rowIdx, gotLine, want.line)
			}
			if gotLabels := writerReadMapRow(labelsCol, r); !maps.Equal(gotLabels, want.labels) {
				t.Fatalf("row %d labels = %v, want %v", rowIdx, gotLabels, want.labels)
			}
			if gotMeta := writerReadMapRow(metaCol, r); !maps.Equal(gotMeta, want.meta) {
				t.Fatalf("row %d meta = %v, want %v", rowIdx, gotMeta, want.meta)
			}
			wantLevel := want.promoted["level"]
			if wantLevel == nil {
				if !levelCol.IsNull(r) {
					t.Fatalf("row %d level = %q, want null", rowIdx, levelCol.Value(r))
				}
			} else if levelCol.IsNull(r) || levelCol.Value(r) != *wantLevel {
				t.Fatalf("row %d level = null:%v val:%q, want %q", rowIdx, levelCol.IsNull(r), levelCol.Value(r), *wantLevel)
			}
			if !envCol.IsNull(r) {
				t.Fatalf("row %d env = %q, want null", rowIdx, envCol.Value(r))
			}
			if want.statusCode == nil {
				if !statusCol.IsNull(r) {
					t.Fatalf("row %d status_code = %d, want null", rowIdx, statusCol.Value(r))
				}
			} else if statusCol.IsNull(r) || statusCol.Value(r) != *want.statusCode {
				t.Fatalf("row %d status_code = null:%v val:%d, want %d", rowIdx, statusCol.IsNull(r), statusCol.Value(r), *want.statusCode)
			}
			rowIdx++
		}
	}
	if err := tr.Err(); err != nil {
		t.Fatalf("TableReader: %v", err)
	}
	if rowIdx != len(allRows) {
		t.Fatalf("read %d rows, want %d", rowIdx, len(allRows))
	}
}

func TestWriterRowGroupSizing(t *testing.T) {
	s, err := schema.ForStream([]string{"level"})
	if err != nil {
		t.Fatal(err)
	}
	linePayload := strings.Repeat("x", 1024)
	t0 := time.Date(2026, 11, 3, 20, 0, 0, 0, time.UTC)

	t.Run("coalesce_small_batches", func(t *testing.T) {
		alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer alloc.AssertSize(t, 0)

		var buf bytes.Buffer
		w, err := storage.NewWriter(&buf, s, []string{"level"})
		if err != nil {
			t.Fatal(err)
		}

		// 50 batches * 200 rows * ~1 KiB/row ≈ 10 MiB total uncompressed -> should coalesce into ~3 row groups, not 50.
		const batches = 50
		const rowsPerBatch = 200
		var seq int
		for range batches {
			rows := make([]writerRow, rowsPerBatch)
			for i := range rowsPerBatch {
				rows[i] = writerRow{
					ts:       t0.Add(time.Duration(seq) * time.Microsecond),
					line:     linePayload,
					labels:   map[string]string{"app": "svc"},
					promoted: map[string]*string{"level": new("info")},
				}
				seq++
			}
			rec := writerBuildBatch(t, alloc, s, rows)
			err := w.Write(rec)
			rec.Release()
			if err != nil {
				t.Fatalf("Write: %v", err)
			}
		}

		stats, err := w.Close()
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
		if stats.Rows != batches*rowsPerBatch {
			t.Fatalf("Rows = %d, want %d", stats.Rows, batches*rowsPerBatch)
		}

		pqReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		defer pqReader.Close()

		numRG := pqReader.NumRowGroups()
		if numRG < 2 || numRG > 5 {
			t.Fatalf("NumRowGroups = %d, want coalesced 2..5 (not %d)", numRG, batches)
		}
		for i := range numRG {
			rg := pqReader.MetaData().RowGroup(i)
			if rg.NumRows() <= 0 {
				t.Fatalf("row group %d is empty", i)
			}
		}
	})

	t.Run("split_large_batch", func(t *testing.T) {
		alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer alloc.AssertSize(t, 0)

		var buf bytes.Buffer
		w, err := storage.NewWriter(&buf, s, []string{"level"})
		if err != nil {
			t.Fatal(err)
		}

		// Single ~10 MiB batch -> should split into multiple ~4 MiB row groups.
		const totalRows = 10000
		rows := make([]writerRow, totalRows)
		for i := range totalRows {
			rows[i] = writerRow{
				ts:       t0.Add(time.Duration(i) * time.Microsecond),
				line:     linePayload,
				labels:   map[string]string{"app": "svc"},
				promoted: map[string]*string{"level": new("info")},
			}
		}
		rec := writerBuildBatch(t, alloc, s, rows)
		err = w.Write(rec)
		rec.Release()
		if err != nil {
			t.Fatalf("Write large batch: %v", err)
		}

		stats, err := w.Close()
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
		if stats.Rows != totalRows {
			t.Fatalf("Rows = %d, want %d", stats.Rows, totalRows)
		}

		pqReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		defer pqReader.Close()

		numRG := pqReader.NumRowGroups()
		if numRG < 2 || numRG > 5 {
			t.Fatalf("NumRowGroups = %d, want 2..5 for ~10 MiB batch", numRG)
		}
		var sumRows int64
		for i := range numRG {
			rg := pqReader.MetaData().RowGroup(i)
			if rg.NumRows() <= 0 {
				t.Fatalf("row group %d is empty", i)
			}
			sumRows += rg.NumRows()
		}
		if sumRows != totalRows {
			t.Fatalf("sum of row group rows = %d, want %d", sumRows, totalRows)
		}
	})
}

func TestWriterEmptyFile(t *testing.T) {
	s, err := schema.ForStream([]string{"level"})
	if err != nil {
		t.Fatal(err)
	}

	for _, writeEmptyBatch := range []bool{false, true} {
		name := "no_writes"
		if writeEmptyBatch {
			name = "zero_row_batch"
		}
		t.Run(name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer alloc.AssertSize(t, 0)

			var buf bytes.Buffer
			w, err := storage.NewWriter(&buf, s, []string{"level"})
			if err != nil {
				t.Fatal(err)
			}
			if writeEmptyBatch {
				empty := writerBuildBatch(t, alloc, s, nil)
				err := w.Write(empty)
				empty.Release()
				if err != nil {
					t.Fatalf("Write empty batch: %v", err)
				}
			}

			stats, err := w.Close()
			if err != nil {
				t.Fatalf("Close: %v", err)
			}
			if stats.Rows != 0 || !stats.MinTS.IsZero() || !stats.MaxTS.IsZero() {
				t.Fatalf("stats = %+v, want 0 rows and zero timestamps", stats)
			}
			vals, ok := stats.LabelValues["level"]
			if !ok || len(vals) != 0 {
				t.Fatalf("LabelValues[level] = %v (ok=%v), want empty slice", vals, ok)
			}
			if !stats.LabelsComplete {
				t.Fatal("LabelsComplete = false for a successfully closed empty file, want true")
			}

			pqReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
			if err != nil {
				t.Fatalf("NewParquetReader on empty file: %v", err)
			}
			defer pqReader.Close()

			if pqReader.NumRows() != 0 || pqReader.NumRowGroups() != 0 {
				t.Fatalf("Parquet empty file rows=%d rgs=%d, want 0, 0", pqReader.NumRows(), pqReader.NumRowGroups())
			}
			fr, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, alloc)
			if err != nil {
				t.Fatalf("pqarrow.NewFileReader: %v", err)
			}
			writerAssertSchemaMatches(t, fr, s)
		})
	}
}

func TestWriterCompleteLabelSummaries(t *testing.T) {
	s, err := schema.ForStream([]string{"level", "env"})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)

	writeBatches := func(t *testing.T, batches ...[]writerRow) storage.FileStats {
		t.Helper()
		alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer alloc.AssertSize(t, 0)

		var buf bytes.Buffer
		w, err := storage.NewWriter(&buf, s, []string{"level", "env"})
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		for i, rows := range batches {
			rec := writerBuildBatch(t, alloc, s, rows)
			if err := w.Write(rec); err != nil {
				rec.Release()
				t.Fatalf("Write batch %d: %v", i, err)
			}
			rec.Release()
		}
		stats, err := w.Close()
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
		if !stats.LabelsComplete {
			t.Fatal("LabelsComplete = false, want true after successful Close")
		}
		return stats
	}

	t.Run("precedence_fallback_and_empty", func(t *testing.T) {
		stats := writeBatches(t,
			[]writerRow{
				{
					ts:       t0,
					line:     "column wins over conflicting map entry",
					labels:   map[string]string{"level": "map-info", "env": "map-dev"},
					promoted: map[string]*string{"level": new("col-warn")},
				},
				{
					ts:       t0.Add(time.Microsecond),
					line:     "null column falls back to map",
					labels:   map[string]string{"level": "map-error"},
					promoted: map[string]*string{"level": nil},
				},
				{
					ts:       t0.Add(2 * time.Microsecond),
					line:     "absent from column and map",
					labels:   map[string]string{},
					promoted: map[string]*string{"level": nil},
				},
			},
			[]writerRow{
				{
					ts:       t0.Add(3 * time.Microsecond),
					line:     "repeated column value across batches",
					labels:   map[string]string{"level": "map-debug"},
					promoted: map[string]*string{"level": new("col-warn")},
				},
				{
					ts:              t0.Add(4 * time.Microsecond),
					line:            "null map item is empty",
					labels:          map[string]string{"level": ""},
					nullLabelValues: map[string]bool{"level": true},
					promoted:        map[string]*string{"level": nil},
				},
				{
					ts:       t0.Add(5 * time.Microsecond),
					line:     "present empty map value stays empty",
					labels:   map[string]string{"level": "", "env": ""},
					promoted: map[string]*string{"level": nil},
				},
				{
					ts:       t0.Add(6 * time.Microsecond),
					line:     "null column falls back to a later map value",
					labels:   map[string]string{"level": "map-debug"},
					promoted: map[string]*string{"level": nil},
				},
				{
					ts:         t0.Add(7 * time.Microsecond),
					line:       "null labels map is empty",
					nullLabels: true,
					promoted:   map[string]*string{"level": nil},
				},
				{
					ts:       t0.Add(8 * time.Microsecond),
					line:     "present empty column wins over a nonempty map value",
					labels:   map[string]string{"level": "map-ignored"},
					promoted: map[string]*string{"level": new("")},
				},
			},
		)

		if want := []string{"", "col-warn", "map-debug", "map-error"}; !slices.Equal(stats.LabelValues["level"], want) {
			t.Fatalf("LabelValues[level] = %v, want %v", stats.LabelValues["level"], want)
		}
		if want := []string{"", "map-dev"}; !slices.Equal(stats.LabelValues["env"], want) {
			t.Fatalf("LabelValues[env] = %v, want %v", stats.LabelValues["env"], want)
		}
		if stats.Rows != 9 {
			t.Fatalf("Rows = %d, want 9", stats.Rows)
		}
	})

	t.Run("sliced_arrays", func(t *testing.T) {
		alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer alloc.AssertSize(t, 0)

		full := writerBuildBatch(t, alloc, s, []writerRow{
			{ts: t0, line: "outside slice", labels: map[string]string{"level": "excluded", "env": "excluded"}, promoted: map[string]*string{"level": new("excluded-col")}},
			{ts: t0.Add(time.Microsecond), line: "s0", labels: map[string]string{"level": "map-1", "env": "env-1"}},
			{ts: t0.Add(2 * time.Microsecond), line: "s1", labels: map[string]string{"level": "map-2"}, promoted: map[string]*string{"level": new("col-2")}},
			{ts: t0.Add(3 * time.Microsecond), line: "s2", labels: map[string]string{"level": ""}, nullLabelValues: map[string]bool{"level": true}},
			{ts: t0.Add(4 * time.Microsecond), line: "s3", labels: map[string]string{"env": "env-4"}},
		})
		defer full.Release()
		rec := full.NewSlice(1, 5)
		defer rec.Release()

		var buf bytes.Buffer
		w, err := storage.NewWriter(&buf, s, []string{"level", "env"})
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		if err := w.Write(rec); err != nil {
			t.Fatalf("Write sliced batch: %v", err)
		}
		stats, err := w.Close()
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
		if !stats.LabelsComplete {
			t.Fatal("LabelsComplete = false, want true after successful Close")
		}
		if want := []string{"", "col-2", "map-1"}; !slices.Equal(stats.LabelValues["level"], want) {
			t.Fatalf("LabelValues[level] = %v, want %v", stats.LabelValues["level"], want)
		}
		if want := []string{"", "env-1", "env-4"}; !slices.Equal(stats.LabelValues["env"], want) {
			t.Fatalf("LabelValues[env] = %v, want %v", stats.LabelValues["env"], want)
		}
	})
}

func TestWriterRejectsUnsortedAndNullTimestamps(t *testing.T) {
	s, err := schema.ForStream(nil)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 11, 3, 20, 0, 0, 0, time.UTC)

	t.Run("unsorted_within_batch", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := storage.NewWriter(&buf, s, nil)
		if err != nil {
			t.Fatal(err)
		}
		bad := writerBuildBatch(t, memory.DefaultAllocator, s, []writerRow{
			{ts: t0.Add(10 * time.Microsecond), line: "a"},
			{ts: t0.Add(9 * time.Microsecond), line: "b"},
		})
		defer bad.Release()
		if err := w.Write(bad); err == nil {
			t.Fatal("expected error for descending _ts within batch")
		}
	})

	t.Run("unsorted_across_calls", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := storage.NewWriter(&buf, s, nil)
		if err != nil {
			t.Fatal(err)
		}
		b1 := writerBuildBatch(t, memory.DefaultAllocator, s, []writerRow{
			{ts: t0.Add(10 * time.Microsecond), line: "a"},
			{ts: t0.Add(20 * time.Microsecond), line: "b"},
		})
		defer b1.Release()
		if err := w.Write(b1); err != nil {
			t.Fatalf("first Write: %v", err)
		}

		b2 := writerBuildBatch(t, memory.DefaultAllocator, s, []writerRow{
			{ts: t0.Add(19 * time.Microsecond), line: "c"},
		})
		defer b2.Release()
		if err := w.Write(b2); err == nil {
			t.Fatal("expected error for descending _ts across Write calls")
		}
	})

	t.Run("null_in_non_nullable_column", func(t *testing.T) {
		var buf bytes.Buffer
		w, err := storage.NewWriter(&buf, s, nil)
		if err != nil {
			t.Fatal(err)
		}
		// Manually build an array with a null in non-nullable _ts.
		tsB := array.NewTimestampBuilder(memory.DefaultAllocator, schema.TSType)
		defer tsB.Release()
		tsB.AppendNull()
		tsArr := tsB.NewArray()
		defer tsArr.Release()

		lineB := array.NewStringBuilder(memory.DefaultAllocator)
		defer lineB.Release()
		lineB.Append("x")
		lineArr := lineB.NewArray()
		defer lineArr.Release()

		mapB := array.NewMapBuilder(memory.DefaultAllocator, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
		defer mapB.Release()
		mapB.Append(true)
		mapArr := mapB.NewArray()
		defer mapArr.Release()

		rec := array.NewRecordBatch(s, []arrow.Array{tsArr, lineArr, mapArr, mapArr}, 1)
		defer rec.Release()

		if err := w.Write(rec); err == nil {
			t.Fatal("expected error for null in _ts")
		}
		if _, err := w.Close(); err != nil {
			t.Fatalf("Close after rejected batch: %v", err)
		}
	})

	t.Run("nullable_ts_schema", func(t *testing.T) {
		alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer alloc.AssertSize(t, 0)

		base := schema.Base()
		nullableTSSchema := arrow.NewSchema([]arrow.Field{
			{Name: schema.ColTS, Type: schema.TSType, Nullable: true},
			base[1],
			base[2],
			base[3],
		}, nil)

		// Non-null timestamps on a nullable _ts schema must succeed (needed for third-party/Merge inputs).
		sink := &writerCloseSpy{}
		w, err := storage.NewWriter(sink, nullableTSSchema, nil)
		if err != nil {
			t.Fatalf("NewWriter with nullable _ts: %v", err)
		}
		good := writerBuildBatch(t, alloc, nullableTSSchema, []writerRow{
			{ts: t0, line: "ok-1"},
			{ts: t0.Add(5 * time.Microsecond), line: "ok-2"},
		})
		if err := w.Write(good); err != nil {
			good.Release()
			t.Fatalf("Write non-null rows with nullable _ts schema: %v", err)
		}
		good.Release()
		stats, err := w.Close()
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
		if stats.Rows != 2 || !stats.MinTS.Equal(t0) || !stats.MaxTS.Equal(t0.Add(5*time.Microsecond)) {
			t.Fatalf("stats = %+v, want 2 rows [%v, %v]", stats, t0, t0.Add(5*time.Microsecond))
		}

		// Actual null _ts value on a nullable _ts schema must still be rejected by Write.
		sink2 := &writerCloseSpy{}
		w2, err := storage.NewWriter(sink2, nullableTSSchema, nil)
		if err != nil {
			t.Fatalf("NewWriter 2: %v", err)
		}
		b := array.NewRecordBuilder(alloc, nullableTSSchema)
		b.Field(0).(*array.TimestampBuilder).Append(arrow.Timestamp(schema.TS(t0)))
		b.Field(1).(*array.StringBuilder).Append("first")
		writerAppendMap(b.Field(2).(*array.MapBuilder), nil)
		writerAppendMap(b.Field(3).(*array.MapBuilder), nil)

		b.Field(0).(*array.TimestampBuilder).AppendNull()
		b.Field(1).(*array.StringBuilder).Append("second-null-ts")
		writerAppendMap(b.Field(2).(*array.MapBuilder), nil)
		writerAppendMap(b.Field(3).(*array.MapBuilder), nil)

		nullRec := b.NewRecordBatch()
		b.Release()
		if err := w2.Write(nullRec); err == nil {
			nullRec.Release()
			t.Fatal("expected Write error when nullable _ts column contains null value")
		}
		nullRec.Release()
		if _, err := w2.Close(); err != nil {
			t.Fatalf("Close after rejected null _ts batch: %v", err)
		}
		if sink2.closed {
			t.Fatal("Close closed caller io.Writer")
		}
	})
}

func TestWriterValidationAndLifecycleErrors(t *testing.T) {
	validSchema, err := schema.ForStream([]string{"level"})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := storage.NewWriter(nil, validSchema, nil); err == nil {
		t.Fatal("expected error for nil io.Writer")
	}
	if _, err := storage.NewWriter(&bytes.Buffer{}, nil, nil); err == nil {
		t.Fatal("expected error for nil schema")
	}

	base := schema.Base()
	badSchemas := []struct {
		name     string
		fields   []arrow.Field
		promoted []string
	}{
		{
			name:   "too_few_fields",
			fields: base[:3],
		},
		{
			name: "wrong_ts_name",
			fields: []arrow.Field{
				{Name: "ts", Type: schema.TSType},
				base[1], base[2], base[3],
			},
		},
		{
			name: "wrong_ts_unit",
			fields: []arrow.Field{
				{Name: schema.ColTS, Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}},
				base[1], base[2], base[3],
			},
		},
		{
			name: "wrong_ts_tz",
			fields: []arrow.Field{
				{Name: schema.ColTS, Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
				base[1], base[2], base[3],
			},
		},
		{
			name: "wrong_line_type",
			fields: []arrow.Field{
				base[0],
				{Name: schema.ColLine, Type: arrow.PrimitiveTypes.Int64},
				base[2], base[3],
			},
		},
		{
			name: "wrong_labels_type",
			fields: []arrow.Field{
				base[0], base[1],
				{Name: schema.ColLabels, Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)},
				base[3],
			},
		},
		{
			name: "reserved_extra_field",
			fields: append(slices.Clone(base), arrow.Field{
				Name: "_secret", Type: arrow.BinaryTypes.String, Nullable: true,
			}),
		},
		{
			name:     "promoted_missing_from_schema",
			fields:   slices.Clone(base),
			promoted: []string{"level"},
		},
		{
			name: "promoted_non_string",
			fields: append(slices.Clone(base), arrow.Field{
				Name: "level", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
			}),
			promoted: []string{"level"},
		},
		{
			name: "promoted_duplicate",
			fields: append(slices.Clone(base), arrow.Field{
				Name: "level", Type: arrow.BinaryTypes.String, Nullable: true,
			}),
			promoted: []string{"level", "level"},
		},
		{
			name:     "promoted_reserved",
			fields:   slices.Clone(base),
			promoted: []string{"line"},
		},
	}

	for _, tc := range badSchemas {
		t.Run(tc.name, func(t *testing.T) {
			sc := arrow.NewSchema(tc.fields, nil)
			if _, err := storage.NewWriter(&bytes.Buffer{}, sc, tc.promoted); err == nil {
				t.Fatalf("NewWriter(%s) succeeded, want error", tc.name)
			}
		})
	}

	// Nil and zero-value Writer receivers must return errors and never panic.
	for _, badW := range []*storage.Writer{nil, {}} {
		if err := badW.Write(nil); err == nil {
			t.Fatal("expected Write error on uninitialized Writer")
		}
		if _, err := badW.Close(); err == nil {
			t.Fatal("expected Close error on uninitialized Writer")
		}
	}

	// Lifecycle and incompatible batch schema checks on an active Writer.
	var buf bytes.Buffer
	w, err := storage.NewWriter(&buf, validSchema, []string{"level"})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(nil); err == nil {
		t.Fatal("expected error for nil RecordBatch")
	}

	otherSchema, err := schema.ForStream(nil)
	if err != nil {
		t.Fatal(err)
	}
	otherBatch := writerBuildBatch(t, memory.DefaultAllocator, otherSchema, []writerRow{
		{ts: time.Date(2026, 11, 3, 20, 0, 0, 0, time.UTC), line: "x"},
	})
	defer otherBatch.Release()
	if err := w.Write(otherBatch); err == nil {
		t.Fatal("expected error for incompatible RecordBatch schema")
	}

	if _, err := w.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if _, err := w.Close(); err == nil {
		t.Fatal("expected error on second Close")
	}
	validBatch := writerBuildBatch(t, memory.DefaultAllocator, validSchema, nil)
	defer validBatch.Release()
	if err := w.Write(validBatch); err == nil {
		t.Fatal("expected error on Write after Close")
	}
}

func TestWriterOwnershipAndIOFailures(t *testing.T) {
	s, err := schema.ForStream([]string{"level"})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 11, 3, 20, 0, 0, 0, time.UTC)

	t.Run("batch_and_writer_ownership", func(t *testing.T) {
		alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
		defer alloc.AssertSize(t, 0)

		sink := &writerCloseSpy{}
		w, err := storage.NewWriter(sink, s, []string{"level"})
		if err != nil {
			t.Fatal(err)
		}

		rec := writerBuildBatch(t, alloc, s, []writerRow{
			{ts: t0, line: "hello", promoted: map[string]*string{"level": new("info")}},
		})
		if err := w.Write(rec); err != nil {
			rec.Release()
			t.Fatal(err)
		}
		if _, err := w.Close(); err != nil {
			rec.Release()
			t.Fatal(err)
		}
		if sink.closed {
			rec.Release()
			t.Fatal("Writer closed caller io.Writer")
		}
		// Caller still owns rec and can read its columns before releasing once.
		if got := rec.Column(1).(*array.String).Value(0); got != "hello" {
			rec.Release()
			t.Fatalf("caller batch corrupted after Write: got %q", got)
		}
		rec.Release()
	})

	t.Run("fail_on_header_write", func(t *testing.T) {
		sink := &writerFailingSink{limit: 0, err: io.ErrClosedPipe}
		if _, err := storage.NewWriter(sink, s, nil); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("NewWriter header failure = %v, want io.ErrClosedPipe", err)
		}
		if sink.closed {
			t.Fatal("NewWriter closed caller io.Writer on error")
		}
	})

	t.Run("short_write_on_header", func(t *testing.T) {
		sink := &writerFailingSink{limit: 2, shortWrite: true}
		if _, err := storage.NewWriter(sink, s, nil); err == nil {
			t.Fatal("NewWriter short header write succeeded, want error")
		}
	})

	t.Run("fail_on_close_flush", func(t *testing.T) {
		sink := &writerFailingSink{limit: 16, err: io.ErrShortBuffer}
		w, err := storage.NewWriter(sink, s, []string{"level"})
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		rec := writerBuildBatch(t, memory.DefaultAllocator, s, []writerRow{
			{ts: t0, line: "payload", promoted: map[string]*string{"level": new("info")}},
		})
		defer rec.Release()

		writeErr := w.Write(rec)
		_, closeErr := w.Close()
		if writeErr == nil && closeErr == nil {
			t.Fatal("expected error from Write or Close when sink fails")
		}
		if sink.closed {
			t.Fatal("Writer closed caller io.Writer on flush error")
		}
	})

	t.Run("fail_during_write_row_group_flush", func(t *testing.T) {
		sink := &writerFailingSink{limit: 64, err: io.ErrClosedPipe}
		w, err := storage.NewWriter(sink, s, []string{"level"})
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		// Large batch (~10 MiB > 8 MiB maxRowGroupBytes) forces row-group rotation and flush during Write.
		bigLine := strings.Repeat("z", 2048)
		rows := make([]writerRow, 5000)
		for i := range rows {
			rows[i] = writerRow{
				ts:       t0.Add(time.Duration(i) * time.Microsecond),
				line:     bigLine,
				promoted: map[string]*string{"level": new("info")},
			}
		}
		rec := writerBuildBatch(t, memory.DefaultAllocator, s, rows)
		defer rec.Release()

		writeErr := w.Write(rec)
		if !errors.Is(writeErr, io.ErrClosedPipe) {
			t.Fatalf("Write error = %v, want wrapping io.ErrClosedPipe", writeErr)
		}
		// Subsequent Write and Close must preserve the sticky write error and never close sink.
		small := writerBuildBatch(t, memory.DefaultAllocator, s, []writerRow{
			{ts: t0.Add(time.Second), line: "after"},
		})
		defer small.Release()
		if err := w.Write(small); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("subsequent Write error = %v, want io.ErrClosedPipe", err)
		}
		if _, closeErr := w.Close(); !errors.Is(closeErr, io.ErrClosedPipe) {
			t.Fatalf("Close after Write failure = %v, want io.ErrClosedPipe", closeErr)
		}
		if sink.closed {
			t.Fatal("Writer closed caller io.Writer after Write failure")
		}
	})

	t.Run("fail_on_delayed_row_group_rotation", func(t *testing.T) {
		// A ~6 MiB batch is below maxRowGroupBytes, so it is buffered without
		// writing a row group. The next Write rotates the row group, flushes the
		// buffered data and surfaces the sink error there (not in Close).
		sink := &writerFailingSink{limit: 4, err: io.ErrClosedPipe} // PAR1 header only
		w, err := storage.NewWriter(sink, s, []string{"level"})
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		bigLine := strings.Repeat("q", 2048)
		rows := make([]writerRow, 3000) // ~6 MiB, below the 8 MiB row-group max
		for i := range rows {
			rows[i] = writerRow{
				ts:       t0.Add(time.Duration(i) * time.Microsecond),
				line:     bigLine,
				promoted: map[string]*string{"level": new("info")},
			}
		}
		rec := writerBuildBatch(t, memory.DefaultAllocator, s, rows)
		defer rec.Release()

		if err := w.Write(rec); err != nil {
			t.Fatalf("first (~6 MiB) Write = %v, want nil (batch buffered)", err)
		}
		if sink.written != 4 {
			t.Fatalf("first Write wrote %d bytes to sink, want only the 4-byte header", sink.written)
		}

		small := writerBuildBatch(t, memory.DefaultAllocator, s, []writerRow{
			{ts: t0.Add(time.Hour), line: "after", promoted: map[string]*string{"level": new("info")}},
		})
		defer small.Release()
		if err := w.Write(small); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("next Write error = %v, want wrapping io.ErrClosedPipe", err)
		}
		if err := w.Write(small); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("subsequent Write error = %v, want sticky io.ErrClosedPipe", err)
		}
		if _, closeErr := w.Close(); !errors.Is(closeErr, io.ErrClosedPipe) {
			t.Fatalf("Close after rotation failure = %v, want sticky io.ErrClosedPipe", closeErr)
		}
		if sink.closed {
			t.Fatal("Writer closed caller io.Writer after rotation failure")
		}
	})
}
