package storage

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestMergeReaderOptions(t *testing.T) {
	got := MergeReaderOptions()
	want := ReaderOptions{BatchSize: mergeReaderBatchSize, BufferedStream: true}
	if got != want {
		t.Fatalf("MergeReaderOptions() = %+v, want %+v", got, want)
	}
	if mergeReaderBatchSize != 1024 {
		t.Fatalf("mergeReaderBatchSize = %d, want 1024", mergeReaderBatchSize)
	}
}

func TestReaderOptionsRejectNegativeBatchSize(t *testing.T) {
	data := writeFixture(t, memory.NewGoAllocator(), fixtureRows(), true)
	for _, batch := range []int{-1, -readerBatchSize} {
		opts := ReaderOptions{BatchSize: batch}
		r, err := NewReaderWithOptions(bytes.NewReader(data), int64(len(data)), opts)
		if !errors.Is(err, errInvalidReaderOptions) {
			t.Errorf("NewReaderWithOptions(%+v) error = %v, want %v", opts, err, errInvalidReaderOptions)
		}
		if r != nil {
			t.Errorf("NewReaderWithOptions(%+v) returned a reader with the error", opts)
		}
	}
}

func TestReaderOptionsZeroValueMatchesNewReader(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	want := loadRows(readerBatchSize + 123)
	data := writeFixture(t, mem, want, true)

	open := map[string]func() (*Reader, error){
		"NewReader": func() (*Reader, error) {
			return NewReader(bytes.NewReader(data), int64(len(data)))
		},
		"NewReaderWithOptions zero": func() (*Reader, error) {
			return NewReaderWithOptions(bytes.NewReader(data), int64(len(data)), ReaderOptions{})
		},
	}
	for name, openReader := range open {
		t.Run(name, func(t *testing.T) {
			r, err := openReader()
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() {
				if err := r.Close(); err != nil {
					t.Errorf("Reader.Close: %v", err)
				}
			}()

			got, sizes := readBatchRows(t, r)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("rows differ from fixture (got %d, want %d)", len(got), len(want))
			}
			if wantSizes := []int{readerBatchSize, 123}; !reflect.DeepEqual(sizes, wantSizes) {
				t.Fatalf("batch sizes = %v, want %v", sizes, wantSizes)
			}
		})
	}
}

// TestReaderOptionsMergeReaderBatches checks the merge reader's decoded batch
// size and release behaviour. It cannot observe Arrow's internal buffered
// stream switch from the rows; that setting is applied when the constructor
// builds the Parquet reader and is certified end to end by the buffered1024
// allocation/profile arm in upload_memory_bench_test.go.
func TestReaderOptionsMergeReaderBatches(t *testing.T) {
	t.Run("production constructor", func(t *testing.T) {
		want := loadRows(mergeReaderBatchSize*2 + 77)
		data := writeFixture(t, memory.NewGoAllocator(), want, true)

		r, err := NewReaderWithOptions(bytes.NewReader(data), int64(len(data)), MergeReaderOptions())
		if err != nil {
			t.Fatalf("NewReaderWithOptions: %v", err)
		}
		defer func() {
			if err := r.Close(); err != nil {
				t.Errorf("Reader.Close: %v", err)
			}
		}()

		got, sizes := readBatchRows(t, r)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("rows differ from fixture (got %d, want %d)", len(got), len(want))
		}
		wantSizes := []int{mergeReaderBatchSize, mergeReaderBatchSize, 77}
		if !reflect.DeepEqual(sizes, wantSizes) {
			t.Fatalf("batch sizes = %v, want %v", sizes, wantSizes)
		}
	})

	t.Run("checked allocator cancellation releases", func(t *testing.T) {
		mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
		defer mem.AssertSize(t, 0)

		data := writeFixture(t, mem, loadRows(mergeReaderBatchSize*3), true)
		r, err := newReaderWithOptions(bytes.NewReader(data), int64(len(data)), mem, MergeReaderOptions())
		if err != nil {
			t.Fatalf("newReaderWithOptions: %v", err)
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
			cancel()
		}
		if rows != mergeReaderBatchSize {
			t.Fatalf("read %d rows before cancellation, want %d", rows, mergeReaderBatchSize)
		}
		if !errors.Is(gotErr, context.Canceled) {
			t.Fatalf("Records error = %v, want context.Canceled", gotErr)
		}
	})
}

// readBatchRows returns every row of r plus the row count of each yielded
// batch, so tests can assert both decoded values and batching.
func readBatchRows(t *testing.T, r *Reader) ([]fixtureRow, []int) {
	t.Helper()
	var rows []fixtureRow
	var sizes []int
	for rec, err := range r.Records(t.Context()) {
		if err != nil {
			t.Fatalf("Records: %v", err)
		}
		sizes = append(sizes, int(rec.NumRows()))
		for i := range int(rec.NumRows()) {
			rows = append(rows, fixtureRowAt(t, rec, i))
		}
	}
	return rows, sizes
}
