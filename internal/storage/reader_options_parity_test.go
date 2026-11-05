package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
)

// parityRow is one physical row of a merge-parity fixture.
type parityRow struct {
	ts       int64
	line     string
	labels   map[string]string
	meta     map[string]string
	level    string
	score    string
	hasScore bool
}

// parityFile writes one Parquet file with the Parkana base columns and a
// "level" candidate promoted column. withScore adds a "score" pipeline column,
// so different inputs exercise additive schema evolution.
func parityFile(t *testing.T, withScore bool, rows []parityRow) []byte {
	t.Helper()

	fields := append([]arrow.Field{}, schema.Base()...)
	fields = append(fields, mergeField("level", arrow.BinaryTypes.String))
	if withScore {
		fields = append(fields, mergeField("score", arrow.BinaryTypes.String))
	}
	s := arrow.NewSchema(fields, nil)

	n := len(rows)
	ts := make([]any, n)
	lines := make([]any, n)
	labels := make([]any, n)
	meta := make([]any, n)
	levels := make([]any, n)
	scores := make([]any, n)
	for i, row := range rows {
		ts[i] = row.ts
		lines[i] = row.line
		labels[i] = row.labels
		meta[i] = row.meta
		levels[i] = row.level
		if row.hasScore {
			scores[i] = row.score
		}
	}
	cols := [][]any{ts, lines, labels, meta, levels}
	if withScore {
		cols = append(cols, scores)
	}

	rec := mergeRecord(t, s, cols...)
	defer rec.Release()
	return mergeParquet(t, s, rec)
}

// parityMerge opens inputs with opts, merges them with "level" promoted and
// returns the encoded output plus stats.
func parityMerge(t *testing.T, inputs [][]byte, opts storage.ReaderOptions) ([]byte, storage.FileStats) {
	t.Helper()

	readers := make([]*storage.Reader, len(inputs))
	for i, data := range inputs {
		r, err := storage.NewReaderWithOptions(bytes.NewReader(data), int64(len(data)), opts)
		if err != nil {
			t.Fatalf("NewReaderWithOptions input %d: %v", i, err)
		}
		readers[i] = r
		t.Cleanup(func() { _ = r.Close() })
	}

	var buf bytes.Buffer
	stats, err := storage.Merge(t.Context(), &buf, readers, []string{"level"})
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	return buf.Bytes(), stats
}

// TestReaderOptionsMergeParity proves the production buffered1024 merge reader
// decodes exactly like the default reader. The inputs exceed one output batch,
// carry a 2,000-row equal-timestamp duplicate run that spans the 1,024-row
// batch boundary, and evolve the schema additively (one input has no "score"
// column). The reader path cancelled before reading is also checked.
func TestReaderOptionsMergeParity(t *testing.T) {
	const (
		duplicates = 2000
		tailRows   = 300
		firstTS    = int64(1000)
	)

	head := make([]parityRow, duplicates)
	for i := range head {
		head[i] = parityRow{
			ts:       firstTS,
			line:     fmt.Sprintf("dup-%04d", i),
			labels:   map[string]string{"app": "api", "instance": strconv.Itoa(i % 16)},
			meta:     map[string]string{"trace_id": fmt.Sprintf("t-%04d", i)},
			level:    "info",
			score:    strconv.Itoa(i % 7),
			hasScore: true,
		}
	}
	tail := make([]parityRow, tailRows)
	for i := range tail {
		tail[i] = parityRow{
			ts:     firstTS + 1000 + int64(i),
			line:   fmt.Sprintf("tail-%03d", i),
			labels: map[string]string{"app": "worker"},
			meta:   map[string]string{},
			level:  "warn",
		}
	}

	// Two byte-identical inputs make every row in the second an exact
	// duplicate of the first; the third has no "score" column.
	inputs := [][]byte{
		parityFile(t, true, head),
		parityFile(t, true, head),
		parityFile(t, false, tail),
	}

	defaultOut, defaultStats := parityMerge(t, inputs, storage.ReaderOptions{})
	scopedOut, scopedStats := parityMerge(t, inputs, storage.MergeReaderOptions())

	if scopedStats.Rows != duplicates+tailRows {
		t.Fatalf("merged rows = %d, want %d", scopedStats.Rows, duplicates+tailRows)
	}
	if scopedStats.DeduplicatedRows != duplicates {
		t.Fatalf("deduplicated rows = %d, want %d", scopedStats.DeduplicatedRows, duplicates)
	}
	if !reflect.DeepEqual(defaultStats, scopedStats) {
		t.Fatalf("stats differ:\ndefault: %+v\nscoped:  %+v", defaultStats, scopedStats)
	}

	defaultSchema, defaultRows := mergeRows(t, defaultOut)
	scopedSchema, scopedRows := mergeRows(t, scopedOut)
	if !reflect.DeepEqual(mergeFieldNames(defaultSchema), mergeFieldNames(scopedSchema)) {
		t.Fatalf("schema differs: default %v, scoped %v",
			mergeFieldNames(defaultSchema), mergeFieldNames(scopedSchema))
	}
	if !reflect.DeepEqual(defaultRows, scopedRows) {
		t.Fatalf("decoded merge output differs: default %d rows, scoped %d rows",
			len(defaultRows), len(scopedRows))
	}
	if len(scopedRows) != duplicates+tailRows {
		t.Fatalf("decoded rows = %d, want %d", len(scopedRows), duplicates+tailRows)
	}

	// Prove the fixture really carries non-empty label/meta maps and both
	// promoted and pipeline fields through the scoped reader, so the parity
	// above is not a comparison of empty values.
	first := scopedRows[0]
	if got, want := first[2], map[string]string{"app": "api", "instance": "0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("first row labels = %v, want %v", got, want)
	}
	if got, want := first[3], map[string]string{"trace_id": "t-0000"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("first row meta = %v, want %v", got, want)
	}
	if got, want := first[4], "info"; got != want {
		t.Fatalf("first row promoted level = %v, want %v", got, want)
	}
	if got, want := first[5], "0"; got != want {
		t.Fatalf("first row pipeline score = %v, want %v", got, want)
	}
	last := scopedRows[len(scopedRows)-1]
	if got, want := last[4], "warn"; got != want {
		t.Fatalf("last row promoted level = %v, want %v", got, want)
	}
	if last[5] != nil {
		t.Fatalf("last row pipeline score = %v, want nil (input without the column)", last[5])
	}

	t.Run("cancelled", func(t *testing.T) {
		readers := make([]*storage.Reader, len(inputs))
		for i, data := range inputs {
			r, err := storage.NewReaderWithOptions(bytes.NewReader(data), int64(len(data)), storage.MergeReaderOptions())
			if err != nil {
				t.Fatalf("NewReaderWithOptions input %d: %v", i, err)
			}
			readers[i] = r
			t.Cleanup(func() { _ = r.Close() })
		}

		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := storage.Merge(ctx, &bytes.Buffer{}, readers, []string{"level"}); !errors.Is(err, context.Canceled) {
			t.Fatalf("Merge error = %v, want context.Canceled", err)
		}
	})
}
