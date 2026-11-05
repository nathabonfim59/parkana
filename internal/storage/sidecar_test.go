package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"

	"github.com/nathabonfim59/parkana/internal/index"
	parkschema "github.com/nathabonfim59/parkana/internal/schema"
)

// sidecarRows returns rows whose lines exercise the builder through a real
// Parquet round-trip: empty, shorter than the gram length, plain ASCII,
// multibyte UTF-8, invalid UTF-8 bytes and repeated grams.
func sidecarRows() []fixtureRow {
	const base = int64(1_700_000_000_000_000)
	lines := []string{
		"",
		"a",
		"abc",
		"user=nathanael connected from 10.0.0.1",
		"héllo wörld ünïcode ☃",
		string([]byte{0xff, 0x00, 0xfe, 0xff, 0x01}),
		"repeat repeat repeat",
	}
	rows := make([]fixtureRow, len(lines))
	for i, line := range lines {
		rows[i] = fixtureRow{
			ts:     base + int64(i),
			line:   line,
			labels: map[string]string{"app": "sidecar"},
			meta:   map[string]string{},
			level:  "info",
			score:  int64(i),
		}
	}
	return rows
}

// buildSidecarFor writes rows to a Parquet file, reads it back and returns
// the decoded sidecar filter, the encoded bytes and the still-open reader.
func buildSidecarFor(t *testing.T, params index.Params, rows []fixtureRow) (*index.Filter, []byte, *Reader) {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { mem.AssertSize(t, 0) })

	data := writeFixture(t, mem, rows, true)
	r, err := newReader(bytes.NewReader(data), int64(len(data)), mem)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("Reader.Close: %v", err)
		}
	})
	enc, err := BuildSidecar(t.Context(), r, params)
	if err != nil {
		t.Fatalf("BuildSidecar: %v", err)
	}
	f, err := index.Decode(enc)
	if err != nil {
		t.Fatalf("index.Decode: %v", err)
	}
	return f, enc, r
}

// assertNoFalseNegatives checks MayContain for every byte-substring window of
// every line: the sidecar must never prove a contained needle absent.
func assertNoFalseNegatives(t *testing.T, f *index.Filter, lines []string) {
	t.Helper()
	for _, line := range lines {
		for i := range len(line) {
			for j := i + 1; j <= len(line); j++ {
				if !f.MayContain(line[i:j]) {
					t.Fatalf("false negative for %q (window %d:%d of %q)", line[i:j], i, j, line)
				}
			}
		}
	}
}

func TestBuildSidecarRoundTrip(t *testing.T) {
	rows := sidecarRows()
	lines := make([]string, len(rows))
	for i, row := range rows {
		lines[i] = row.line
	}
	f, enc, _ := buildSidecarFor(t, index.DefaultParams, rows)

	if f.GramLen() != index.DefaultParams.N {
		t.Fatalf("GramLen = %d, want %d", f.GramLen(), index.DefaultParams.N)
	}
	if len(enc) == 0 {
		t.Fatal("BuildSidecar returned no bytes")
	}
	assertNoFalseNegatives(t, f, lines)

	// Needles shorter than the gram length never prune.
	for _, short := range []string{"", "a", "ab", "abc"} {
		if !f.MayContain(short) {
			t.Fatalf("MayContain(%q) = false, want true for a short needle", short)
		}
	}
	// Fixed absent needles must prune: none of their grams appears in any
	// line. These fixed probes prune for this deterministic fixture and the
	// pinned Bloom implementation; false positives remain permitted.
	for _, absent := range []string{"ZZZZabsent", "qqqq", "november"} {
		if f.MayContain(absent) {
			t.Fatalf("MayContain(%q) = true, want false", absent)
		}
	}
}

func TestBuildSidecarGramLengthEight(t *testing.T) {
	params := index.DefaultParams
	params.N = 8
	rows := sidecarRows()
	f, _, _ := buildSidecarFor(t, params, rows)

	if f.GramLen() != 8 {
		t.Fatalf("GramLen = %d, want 8", f.GramLen())
	}
	lines := make([]string, len(rows))
	for i, row := range rows {
		lines[i] = row.line
	}
	assertNoFalseNegatives(t, f, lines)
	if f.MayContain("abcdefgh") {
		t.Fatal("MayContain(abcdefgh) = true, want false")
	}
}

func TestBuildSidecarEmptyFile(t *testing.T) {
	f, enc, _ := buildSidecarFor(t, index.DefaultParams, nil)

	if len(enc) == 0 {
		t.Fatal("empty file produced no sidecar bytes")
	}
	if f.MayContain("anything") {
		t.Fatal("MayContain on an empty file = true, want false")
	}
	// Short needles still never prune.
	if !f.MayContain("abc") {
		t.Fatal("MayContain(abc) = false on an empty file, want true")
	}
}

func TestBuildSidecarCrossesReaderBatches(t *testing.T) {
	rows := loadRows(readerBatchSize + 100)
	f, _, _ := buildSidecarFor(t, index.DefaultParams, rows)

	// Rows from the first batch, the batch boundary and the last batch.
	for _, needle := range []string{"line-0", "line-8191", "line-8192", "line-8291"} {
		if !f.MayContain(needle) {
			t.Fatalf("MayContain(%q) = false, want true", needle)
		}
	}
	// line-99999 shares its line- prefix with every row, but its four-nines
	// gram appears in no row number. This fixed probe prunes for this
	// deterministic fixture and the pinned Bloom implementation; false
	// positives remain permitted.
	if f.MayContain("line-99999") {
		t.Fatal("MayContain(line-99999) = true, want false")
	}
	assertNoFalseNegatives(t, f, []string{rows[0].line, rows[readerBatchSize].line, rows[len(rows)-1].line})
}

func TestBuildSidecarReaderStaysOwned(t *testing.T) {
	rows := sidecarRows()
	_, _, r := buildSidecarFor(t, index.DefaultParams, rows)

	// BuildSidecar must not close the reader or leave it unusable; every row
	// still reads back exactly.
	got, err := readRows(t, r)
	if err != nil {
		t.Fatalf("readRows after BuildSidecar: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("read %d rows after BuildSidecar, want %d", len(got), len(rows))
	}
	for i := range rows {
		if got[i].line != rows[i].line {
			t.Fatalf("row %d line = %q, want %q", i, got[i].line, rows[i].line)
		}
	}
}

// reorderedFixtureSchema returns the fixture schema with its fields shuffled,
// so the base columns are not in canonical positions (line is not field 1).
func reorderedFixtureSchema(t *testing.T) *arrow.Schema {
	t.Helper()
	byName := make(map[string]arrow.Field)
	for _, f := range fixtureSchema().Fields() {
		byName[f.Name] = f
	}
	order := []string{
		parkschema.ColLabels, parkschema.ColTS, "score",
		parkschema.ColMeta, parkschema.ColLine, "level",
	}
	fields := make([]arrow.Field, len(order))
	for i, name := range order {
		f, ok := byName[name]
		if !ok {
			t.Fatalf("fixture schema has no field %q", name)
		}
		fields[i] = f
	}
	return arrow.NewSchema(fields, nil)
}

// reorderedBatch builds one batch in sc's field order, filling each column
// from rows by name; the line of row nullLineAt is null.
func reorderedBatch(t *testing.T, mem memory.Allocator, sc *arrow.Schema, rows []fixtureRow, nullLineAt int) arrow.RecordBatch {
	t.Helper()
	rb := array.NewRecordBuilder(mem, sc)
	defer rb.Release()
	for i, row := range rows {
		for j, f := range sc.Fields() {
			switch f.Name {
			case parkschema.ColTS:
				rb.Field(j).(*array.TimestampBuilder).Append(arrow.Timestamp(row.ts))
			case parkschema.ColLine:
				line := rb.Field(j).(*array.StringBuilder)
				if i == nullLineAt {
					line.AppendNull()
				} else {
					line.Append(row.line)
				}
			case parkschema.ColLabels:
				appendStringMap(rb.Field(j).(*array.MapBuilder), row.labels)
			case parkschema.ColMeta:
				appendStringMap(rb.Field(j).(*array.MapBuilder), row.meta)
			case "level":
				level := rb.Field(j).(*array.StringBuilder)
				if row.levelNull {
					level.AppendNull()
				} else {
					level.Append(row.level)
				}
			case "score":
				score := rb.Field(j).(*array.Int64Builder)
				if row.scoreNull {
					score.AppendNull()
				} else {
					score.Append(row.score)
				}
			default:
				t.Fatalf("unexpected field %q", f.Name)
			}
		}
	}
	return rb.NewRecordBatch()
}

func TestBuildSidecarReorderedColumnsAndNullLine(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	rows := sidecarRows()
	const nullLineAt = 3 // the "user=nathanael connected from 10.0.0.1" row
	sc := reorderedFixtureSchema(t)
	rec := reorderedBatch(t, mem, sc, rows, nullLineAt)
	defer rec.Release()
	data := writeRecords(t, mem, sc, []arrow.RecordBatch{rec}, true)

	r, err := newReader(bytes.NewReader(data), int64(len(data)), mem)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			t.Errorf("Reader.Close: %v", err)
		}
	}()
	if got := r.Schema().FieldIndices(parkschema.ColLine); len(got) != 1 || got[0] == 1 {
		t.Fatalf("line column found at %v, want it away from base position 1", got)
	}

	enc, err := BuildSidecar(t.Context(), r, index.DefaultParams)
	if err != nil {
		t.Fatalf("BuildSidecar: %v", err)
	}
	f, err := index.Decode(enc)
	if err != nil {
		t.Fatalf("index.Decode: %v", err)
	}

	lines := make([]string, 0, len(rows)-1)
	for i, row := range rows {
		if i != nullLineAt {
			lines = append(lines, row.line)
		}
	}
	assertNoFalseNegatives(t, f, lines)

	// The null line contributed no grams: "nath" appears only in the null
	// row's text. This fixed probe prunes for this deterministic fixture and
	// the pinned Bloom implementation; false positives remain permitted.
	if f.MayContain("nath") {
		t.Fatal("MayContain(nath) = true, want false for a needle only in the null line")
	}
}

func TestBuildSidecarTooManyGrams(t *testing.T) {
	params := index.DefaultParams
	params.MaxGrams = 8

	enc, err := BuildSidecar(t.Context(), mustReader(t, sidecarRows()), params)
	if enc != nil {
		t.Fatalf("BuildSidecar returned %d bytes over the gram cap, want nil", len(enc))
	}
	if !errors.Is(err, index.ErrTooManyGrams) {
		t.Fatalf("BuildSidecar error = %v, want index.ErrTooManyGrams", err)
	}
}

func TestBuildSidecarInvalidParams(t *testing.T) {
	params := index.DefaultParams
	params.N = 9

	if _, err := BuildSidecar(t.Context(), mustReader(t, sidecarRows()), params); err == nil {
		t.Fatal("BuildSidecar accepted N=9")
	}
}

func TestBuildSidecarCanceledContext(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	data := writeFixture(t, mem, sidecarRows(), true)
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
	if _, err := BuildSidecar(ctx, r, index.DefaultParams); !errors.Is(err, context.Canceled) {
		t.Fatalf("BuildSidecar with canceled context error = %v, want context.Canceled", err)
	}
}

func TestBuildSidecarReaderFailures(t *testing.T) {
	t.Run("nil reader", func(t *testing.T) {
		if _, err := BuildSidecar(t.Context(), nil, index.DefaultParams); err == nil {
			t.Fatal("BuildSidecar(nil) succeeded, want error")
		}
	})
	t.Run("zero value reader", func(t *testing.T) {
		r := &Reader{}
		if _, err := BuildSidecar(t.Context(), r, index.DefaultParams); !errors.Is(err, errReaderUninitialized) {
			t.Fatalf("BuildSidecar(zero reader) error = %v, want %v", err, errReaderUninitialized)
		}
	})
	t.Run("closed reader", func(t *testing.T) {
		r := mustReader(t, sidecarRows())
		if err := r.Close(); err != nil {
			t.Fatalf("Reader.Close: %v", err)
		}
		if _, err := BuildSidecar(t.Context(), r, index.DefaultParams); !errors.Is(err, errReaderClosed) {
			t.Fatalf("BuildSidecar(closed reader) error = %v, want %v", err, errReaderClosed)
		}
	})
	t.Run("corrupt page", func(t *testing.T) {
		mem := memory.NewGoAllocator()

		data := writeFixture(t, mem, sidecarRows(), true)
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

		enc, err := BuildSidecar(t.Context(), r, index.DefaultParams)
		if err == nil {
			t.Fatalf("BuildSidecar succeeded on a corrupt page (%d bytes), want error", len(enc))
		}
		if errors.Is(err, index.ErrTooManyGrams) {
			t.Fatalf("BuildSidecar error = %v, want a read failure, not the gram cap", err)
		}
	})
}

// mustReader returns an open Reader over a Parquet file holding rows.
func mustReader(t *testing.T, rows []fixtureRow) *Reader {
	t.Helper()
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { mem.AssertSize(t, 0) })

	data := writeFixture(t, mem, rows, true)
	r, err := newReader(bytes.NewReader(data), int64(len(data)), mem)
	if err != nil {
		t.Fatalf("newReader: %v", err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("Reader.Close: %v", err)
		}
	})
	return r
}
