package storage

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/util"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/nathabonfim59/parkana/internal/schema"
)

const (
	minRowGroupBytes    int64 = 1 << 20   // 1 MiB
	targetRowGroupBytes int64 = 4 << 20   // 4 MiB
	maxRowGroupBytes    int64 = 8 << 20   // 8 MiB
	maxRowGroupRows     int64 = 256 << 10 // 262,144 rows safety cap
)

// writerOnly hides io.Closer on the caller's io.Writer so pqarrow.FileWriter
// never closes caller-owned writers on Close.
type writerOnly struct {
	w io.Writer
}

func (wo writerOnly) Write(p []byte) (int, error) {
	return wo.w.Write(p)
}

type promotedCol struct {
	name   string
	index  int
	values map[string]struct{}
}

// Writer writes one Parquet log file with zstd compression, ~1–8 MiB row
// groups, column statistics and page indexes enabled, sorted by ascending _ts.
//
// The caller owns the underlying io.Writer and any arrow.RecordBatch values
// passed to Write; neither Write nor Close closes the io.Writer or releases
// caller batches.
type Writer struct {
	fw       *pqarrow.FileWriter
	schema   *arrow.Schema
	promoted []promotedCol

	rows    int64
	minTS   int64
	maxTS   int64
	rgRows  int64
	rgBytes int64

	closed bool
	err    error
}

// NewWriter opens a Parquet writer to w for batches matching s, tracking
// distinct values of the named promoted label columns. The caller retains
// ownership of w.
func NewWriter(w io.Writer, s *arrow.Schema, promoted []string) (*Writer, error) {
	if w == nil {
		return nil, errors.New("storage: nil io.Writer")
	}
	cols, err := validateWriterSchema(s, promoted)
	if err != nil {
		return nil, err
	}

	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithStats(true),
		parquet.WithPageIndexEnabled(true),
		parquet.WithMaxRowGroupLength(maxRowGroupRows),
		parquet.WithSortingColumns([]parquet.SortingColumn{
			{ColumnIdx: 0, Descending: false, NullsFirst: false},
		}),
	)
	arrProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

	fw, err := pqarrow.NewFileWriter(s, writerOnly{w: w}, props, arrProps)
	if err != nil {
		return nil, fmt.Errorf("storage: open parquet writer: %w", err)
	}

	return &Writer{
		fw:       fw,
		schema:   s,
		promoted: cols,
	}, nil
}

func validateWriterSchema(s *arrow.Schema, promoted []string) ([]promotedCol, error) {
	if s == nil {
		return nil, errors.New("storage: nil schema")
	}
	base := schema.Base()
	if s.NumFields() < len(base) {
		return nil, fmt.Errorf("storage: schema has %d fields, want at least %d base columns", s.NumFields(), len(base))
	}

	tsField := s.Field(0)
	if tsField.Name != schema.ColTS {
		return nil, fmt.Errorf("storage: base field 0 = %q, want %q", tsField.Name, schema.ColTS)
	}
	if !arrow.TypeEqual(tsField.Type, schema.TSType) {
		return nil, fmt.Errorf("storage: field %q has type %s, want %s", schema.ColTS, tsField.Type, schema.TSType)
	}

	lineField := s.Field(1)
	if lineField.Name != schema.ColLine {
		return nil, fmt.Errorf("storage: base field 1 = %q, want %q", lineField.Name, schema.ColLine)
	}
	if !arrow.TypeEqual(lineField.Type, arrow.BinaryTypes.String) {
		return nil, fmt.Errorf("storage: field %q has type %s, want %s", schema.ColLine, lineField.Type, arrow.BinaryTypes.String)
	}

	for _, idx := range []int{2, 3} {
		f := s.Field(idx)
		if f.Name != base[idx].Name {
			return nil, fmt.Errorf("storage: base field %d = %q, want %q", idx, f.Name, base[idx].Name)
		}
		if !isStringMapType(f.Type) {
			return nil, fmt.Errorf("storage: field %q has type %s, want map<utf8, utf8>", f.Name, f.Type)
		}
	}

	fieldIdx := make(map[string]int, s.NumFields())
	for i := range len(base) {
		fieldIdx[s.Field(i).Name] = i
	}
	for i := len(base); i < s.NumFields(); i++ {
		f := s.Field(i)
		if f.Name == "" || schema.IsReserved(f.Name) {
			return nil, fmt.Errorf("storage: field %d (%q) is reserved or empty", i, f.Name)
		}
		if _, dup := fieldIdx[f.Name]; dup {
			return nil, fmt.Errorf("storage: duplicate field %q in schema", f.Name)
		}
		if f.Type == nil {
			return nil, fmt.Errorf("storage: field %q has nil type", f.Name)
		}
		fieldIdx[f.Name] = i
	}

	cols := make([]promotedCol, 0, len(promoted))
	for i, name := range promoted {
		if name == "" || schema.IsReserved(name) {
			return nil, fmt.Errorf("storage: promoted label %q is reserved or empty", name)
		}
		if slices.Contains(promoted[:i], name) {
			return nil, fmt.Errorf("storage: duplicate promoted label %q", name)
		}
		idx, ok := fieldIdx[name]
		if !ok {
			return nil, fmt.Errorf("storage: promoted label %q not found in schema", name)
		}
		f := s.Field(idx)
		if !arrow.TypeEqual(f.Type, arrow.BinaryTypes.String) {
			return nil, fmt.Errorf("storage: promoted label %q has type %s, want utf8 string", name, f.Type)
		}
		cols = append(cols, promotedCol{
			name:   name,
			index:  idx,
			values: make(map[string]struct{}),
		})
	}
	return cols, nil
}

func isStringMapType(dt arrow.DataType) bool {
	mt, ok := dt.(*arrow.MapType)
	if !ok {
		return false
	}
	return !mt.KeyField().Nullable &&
		arrow.TypeEqual(mt.KeyType(), arrow.BinaryTypes.String) &&
		arrow.TypeEqual(mt.ItemType(), arrow.BinaryTypes.String)
}

// Write appends rec to the file. Rows must arrive in non-decreasing _ts order
// within rec and across calls. The caller retains ownership of rec.
func (w *Writer) Write(rec arrow.RecordBatch) error {
	if w == nil || w.fw == nil {
		return errors.New("storage: writer is not initialized")
	}
	if w.closed {
		return errors.New("storage: write on closed writer")
	}
	if w.err != nil {
		return w.err
	}
	if rec == nil {
		return errors.New("storage: nil record batch")
	}
	if rec.Schema() == nil || !rec.Schema().Equal(w.schema) {
		return errors.New("storage: record batch schema does not match writer schema")
	}

	n := rec.NumRows()
	if n == 0 {
		return nil
	}

	tsCol, ok := rec.Column(0).(*array.Timestamp)
	if !ok {
		return fmt.Errorf("storage: expected *array.Timestamp for %q, got %T", schema.ColTS, rec.Column(0))
	}
	if tsCol.NullN() > 0 {
		return fmt.Errorf("storage: %q column contains null values", schema.ColTS)
	}
	tsVals := tsCol.TimestampValues()
	firstTS := int64(tsVals[0])
	if w.rows > 0 && firstTS < w.maxTS {
		return fmt.Errorf("storage: out-of-order %s: %d < previous max %d", schema.ColTS, firstTS, w.maxTS)
	}
	for i := 1; i < len(tsVals); i++ {
		if tsVals[i] < tsVals[i-1] {
			return fmt.Errorf("storage: out-of-order %s at row %d: %d < %d", schema.ColTS, i, int64(tsVals[i]), int64(tsVals[i-1]))
		}
	}

	if err := w.recordPromotedValues(rec); err != nil {
		w.err = err
		return err
	}

	if err := w.writeBatchSlices(rec); err != nil {
		return err
	}

	if w.rows == 0 {
		w.minTS = firstTS
	}
	w.maxTS = int64(tsVals[len(tsVals)-1])
	w.rows += n

	return nil
}

// recordPromotedValues adds every row's effective value for each configured
// promoted label to its summary set. A non-null column value wins; otherwise
// the labels map entry is used; a label absent from both is the empty string.
//
// The declared schema guarantees the labels column is map<utf8, utf8>, but a
// caller can still hand over a batch whose runtime arrays differ. Such a batch
// cannot yield a trustworthy complete summary, so a non-map labels column or
// non-string keys/items are reported as errors (poisoning the writer through
// Write) instead of silently falling back to "".
func (w *Writer) recordPromotedValues(rec arrow.RecordBatch) error {
	labels, ok := rec.Column(2).(*array.Map)
	if !ok {
		return fmt.Errorf("storage: expected *array.Map for %q, got %T", schema.ColLabels, rec.Column(2))
	}
	keys, ok := labels.Keys().(*array.String)
	if !ok {
		return fmt.Errorf("storage: expected *array.String for %q map keys, got %T", schema.ColLabels, labels.Keys())
	}
	items, ok := labels.Items().(*array.String)
	if !ok {
		return fmt.Errorf("storage: expected *array.String for %q map items, got %T", schema.ColLabels, labels.Items())
	}
	for i := range w.promoted {
		col, ok := rec.Column(w.promoted[i].index).(*array.String)
		if !ok {
			return fmt.Errorf("storage: expected *array.String for promoted label %q, got %T", w.promoted[i].name, rec.Column(w.promoted[i].index))
		}
		vals := w.promoted[i].values
		for r := range col.Len() {
			v := ""
			switch {
			case !col.IsNull(r):
				v = col.Value(r)
			default:
				v = labelsValue(labels, keys, items, r, w.promoted[i].name)
			}
			if _, exists := vals[v]; !exists {
				vals[strings.Clone(v)] = struct{}{}
			}
		}
	}
	return nil
}

// labelsValue returns the labels map entry for name in row, or "" when the
// row has no such entry or its value is null. labels, keys and items come from
// one validated map array.
func labelsValue(labels *array.Map, keys, items *array.String, row int, name string) string {
	if labels.IsNull(row) {
		return ""
	}
	beg, end := labels.ValueOffsets(row)
	for i := int(beg); i < int(end); i++ {
		if keys.Value(i) != name {
			continue
		}
		if items.IsNull(i) {
			return ""
		}
		return items.Value(i)
	}
	return ""
}

func (w *Writer) rotateRowGroup() error {
	if err := w.fw.NewBufferedRowGroupChecked(); err != nil {
		w.err = fmt.Errorf("storage: flush row group: %w", err)
		_ = w.fw.Close()
		return w.err
	}
	w.rgRows = 0
	w.rgBytes = 0
	return nil
}

func (w *Writer) writeBufferedBatch(rec arrow.RecordBatch, estBytes int64) error {
	if err := w.fw.WriteBuffered(rec); err != nil {
		w.err = fmt.Errorf("storage: write parquet batch: %w", err)
		_ = w.fw.Close()
		return w.err
	}
	w.rgRows += rec.NumRows()
	w.rgBytes = max(w.rgBytes+estBytes, w.fw.RowGroupTotalBytesWritten())
	return nil
}

func (w *Writer) writeBatchSlices(rec arrow.RecordBatch) error {
	n := rec.NumRows()
	batchBytes := max(util.TotalRecordSize(rec), n*8)

	if w.rgRows > 0 && (w.rgBytes >= targetRowGroupBytes || w.rgRows >= maxRowGroupRows ||
		(w.rgBytes+batchBytes > maxRowGroupBytes && w.rgBytes >= minRowGroupBytes)) {
		if err := w.rotateRowGroup(); err != nil {
			return err
		}
	}
	if w.rgBytes+batchBytes <= maxRowGroupBytes && w.rgRows+n <= maxRowGroupRows {
		return w.writeBufferedBatch(rec, batchBytes)
	}

	for offset := int64(0); offset < n; {
		if w.rgRows > 0 && (w.rgBytes >= targetRowGroupBytes || w.rgRows >= maxRowGroupRows) {
			if err := w.rotateRowGroup(); err != nil {
				return err
			}
		}

		remRows := n - offset
		remBytes := (batchBytes * remRows) / n

		if w.rgRows > 0 && w.rgBytes+remBytes > maxRowGroupBytes && (w.rgBytes >= minRowGroupBytes || remRows == 1) {
			if err := w.rotateRowGroup(); err != nil {
				return err
			}
		}

		if (w.rgBytes+remBytes <= maxRowGroupBytes && w.rgRows+remRows <= maxRowGroupRows) || remRows == 1 {
			if err := w.writeSlice(rec, offset, n, remBytes); err != nil {
				return err
			}
			break
		}

		budget := targetRowGroupBytes - w.rgBytes
		if budget < minRowGroupBytes {
			budget = maxRowGroupBytes - w.rgBytes
		}
		takeByBytes := max(int64(1), (n*budget)/max(batchBytes, 1))
		takeByRows := max(int64(1), maxRowGroupRows-w.rgRows)
		take := min(remRows, min(takeByBytes, takeByRows))
		subBytes := (batchBytes * take) / n

		if err := w.writeSlice(rec, offset, offset+take, subBytes); err != nil {
			return err
		}
		offset += take
	}
	return nil
}

func (w *Writer) writeSlice(rec arrow.RecordBatch, start, end, estBytes int64) error {
	sub := rec.NewSlice(start, end)
	defer sub.Release()
	return w.writeBufferedBatch(sub, estBytes)
}

// Close flushes Parquet metadata and footer bytes to the underlying io.Writer
// without closing it, and returns statistics for the written file. LabelValues
// maps each configured promoted column to its sorted distinct effective
// values: the non-null column value, else the labels map entry, else "".
// LabelsComplete is set only when Close succeeds, and is true even for an
// empty file.
func (w *Writer) Close() (FileStats, error) {
	if w == nil || w.fw == nil {
		return FileStats{}, errors.New("storage: writer is not initialized")
	}
	if w.closed {
		return FileStats{}, errors.New("storage: writer already closed")
	}
	w.closed = true

	closeErr := w.fw.Close()
	if w.err != nil {
		return FileStats{}, w.err
	}
	if closeErr != nil {
		w.err = fmt.Errorf("storage: close parquet writer: %w", closeErr)
		return FileStats{}, w.err
	}

	lv := make(map[string][]string, len(w.promoted))
	for _, p := range w.promoted {
		if len(p.values) == 0 {
			lv[p.name] = []string{}
		} else {
			lv[p.name] = slices.Sorted(maps.Keys(p.values))
		}
	}

	var minTS, maxTS time.Time
	if w.rows > 0 {
		minTS = schema.Time(w.minTS)
		maxTS = schema.Time(w.maxTS)
	}

	return FileStats{
		MinTS:          minTS,
		MaxTS:          maxTS,
		Rows:           w.rows,
		LabelValues:    lv,
		LabelsComplete: true,
	}, nil
}
