package storage_test

// Regression tests for malformed runtime arrays in the labels column: the
// writer's declared schema can say map<utf8, utf8> while a caller hands over a
// record batch whose labels column, or whose map keys/items, is another type.
// Such a batch cannot produce a trustworthy complete label summary, so Write
// must fail (poisoning the writer like the promoted-column mismatch does) and
// Close must never return LabelsComplete=true for it.
//
// Every fixture uses a checked allocator: the malformed Data trees are
// assembled directly, so their Retain/Release balance is part of the test.

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
)

// malformedRecord overrides one column's runtime array while keeping the
// declared schema of the wrapped batch.
type malformedRecord struct {
	arrow.RecordBatch
	col         int
	replacement arrow.Array
}

func (r malformedRecord) Column(i int) arrow.Array {
	if i == r.col {
		return r.replacement
	}
	return r.RecordBatch.Column(i)
}

// malformedLabelsMap builds a real *array.Map whose declared type is
// map<utf8, utf8> but whose keys or items child array has another type. Arrow
// constructors normally refuse that combination, so the Data tree is assembled
// directly: one list entry pointing at one struct<key, item> row. The returned
// map owns the whole tree; releasing it frees every checked allocation.
func malformedLabelsMap(t *testing.T, alloc memory.Allocator, badKeys, badItems bool) *array.Map {
	t.Helper()
	mapType := arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)

	keyData := malformedStringData(t, alloc)
	if badKeys {
		keyData.Release()
		keyData = malformedInt64Data(t, alloc)
	}
	itemData := malformedStringData(t, alloc)
	if badItems {
		itemData.Release()
		itemData = malformedInt64Data(t, alloc)
	}

	structType := arrow.StructOf(
		arrow.Field{Name: "key", Type: keyData.DataType(), Nullable: false},
		arrow.Field{Name: "entries", Type: itemData.DataType()},
	)
	structData := array.NewData(structType, 1, nil, []arrow.ArrayData{keyData, itemData}, 0, 0)
	keyData.Release()
	itemData.Release()

	offsets := memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, 1}))
	mapData := array.NewData(mapType, 1, []*memory.Buffer{nil, offsets}, []arrow.ArrayData{structData}, 0, 0)
	offsets.Release()
	structData.Release()

	m := array.NewMapData(mapData)
	mapData.Release()
	return m
}

// malformedStringData is a one-row utf8 array holding "level"; the caller owns
// the returned reference.
func malformedStringData(t *testing.T, alloc memory.Allocator) arrow.ArrayData {
	t.Helper()
	b := array.NewStringBuilder(alloc)
	defer b.Release()
	b.Append("level")
	arr := b.NewArray()
	data := arr.Data()
	data.Retain()
	arr.Release()
	return data
}

// malformedInt64Data is a one-row int64 array; the caller owns the returned
// reference.
func malformedInt64Data(t *testing.T, alloc memory.Allocator) arrow.ArrayData {
	t.Helper()
	b := array.NewInt64Builder(alloc)
	defer b.Release()
	b.Append(7)
	arr := b.NewArray()
	data := arr.Data()
	data.Retain()
	arr.Release()
	return data
}

// malformedPromotedBatch builds one row whose promoted level column is null, so
// the writer must consult the labels map, and replaces the labels column.
func malformedPromotedBatch(t *testing.T, alloc memory.Allocator, replacement func() arrow.Array) arrow.RecordBatch {
	t.Helper()
	s, err := schema.ForStream([]string{"level"})
	if err != nil {
		t.Fatalf("schema.ForStream: %v", err)
	}
	b := array.NewRecordBuilder(alloc, s)
	defer b.Release()
	tsB := b.Field(0).(*array.TimestampBuilder)
	lineB := b.Field(1).(*array.StringBuilder)
	labelsB := b.Field(2).(*array.MapBuilder)
	metaB := b.Field(3).(*array.MapBuilder)
	levelB := b.Field(4).(*array.StringBuilder)
	tsB.Append(arrow.Timestamp(schema.TS(time.Date(2026, 11, 4, 10, 0, 0, 0, time.UTC))))
	lineB.Append("malformed")
	labelsB.Append(true)
	labelsB.KeyBuilder().(*array.StringBuilder).Append("level")
	labelsB.ItemBuilder().(*array.StringBuilder).Append("info")
	metaB.Append(true)
	levelB.AppendNull()
	rec := b.NewRecordBatch()
	t.Cleanup(rec.Release)

	repl := replacement()
	t.Cleanup(repl.Release)
	return malformedRecord{RecordBatch: rec, col: 2, replacement: repl}
}

func TestWriterRejectsMalformedLabelsArrays(t *testing.T) {
	tests := []struct {
		name    string
		replace func(t *testing.T, alloc memory.Allocator) arrow.Array
		want    string
	}{
		{
			name: "labels column is not a map",
			replace: func(t *testing.T, alloc memory.Allocator) arrow.Array {
				b := array.NewStringBuilder(alloc)
				defer b.Release()
				b.Append("not a map")
				return b.NewArray()
			},
			want: "labels",
		},
		{
			name: "map keys are not strings",
			replace: func(t *testing.T, alloc memory.Allocator) arrow.Array {
				return malformedLabelsMap(t, alloc, true, false)
			},
			want: "key",
		},
		{
			name: "map items are not strings",
			replace: func(t *testing.T, alloc memory.Allocator) arrow.Array {
				return malformedLabelsMap(t, alloc, false, true)
			},
			want: "item",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
			t.Cleanup(func() { alloc.AssertSize(t, 0) })

			var out bytes.Buffer
			w, err := storage.NewWriter(&out, malformedSchema(t), []string{"level"})
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			rec := malformedPromotedBatch(t, alloc, func() arrow.Array { return tt.replace(t, alloc) })
			if err := w.Write(rec); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Write() error = %v, want mention of %q", err, tt.want)
			}
			stats, err := w.Close()
			if err == nil {
				t.Fatal("Close() after malformed batch = nil, want the poisoned write error")
			}
			if stats.LabelsComplete || stats.Rows != 0 || len(stats.LabelValues) != 0 {
				t.Fatalf("Close() stats = %+v, want zero stats without a complete summary", stats)
			}
		})
	}
}

// malformedSchema returns the writer schema matching malformedPromotedBatch.
func malformedSchema(t *testing.T) *arrow.Schema {
	t.Helper()
	s, err := schema.ForStream([]string{"level"})
	if err != nil {
		t.Fatalf("schema.ForStream: %v", err)
	}
	return s
}
