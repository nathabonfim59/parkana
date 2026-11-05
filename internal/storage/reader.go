package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/nathabonfim59/parkana/internal/schema"
)

// readerBatchSize is the default maximum number of rows in each batch yielded
// by Reader.Records.
const readerBatchSize = 8192

// mergeReaderBatchSize is the record-batch size used for merge inputs. Smaller
// batches bound the decoded input footprint of a k-way merge, where every
// input holds at least one batch live.
const mergeReaderBatchSize = 1024

// ReaderOptions tunes how NewReaderWithOptions decodes a Parquet file. The
// zero value is the default used by NewReader: readerBatchSize rows per batch
// with Arrow's buffered stream reader disabled.
type ReaderOptions struct {
	// BatchSize is the maximum number of rows in each batch yielded by
	// Reader.Records. Zero means readerBatchSize; negative values are rejected.
	BatchSize int
	// BufferedStream enables Arrow's buffered stream reader for the file's
	// column chunks. It changes read-ahead and allocation behaviour, never the
	// decoded rows.
	BufferedStream bool
}

// MergeReaderOptions returns the reader options for files feeding storage.Merge:
// 1024-row batches with Arrow's buffered stream reader enabled.
//
// Every call returns a fresh value. The scope is deliberately limited to merge
// inputs: every other reader keeps the default configuration, so sidecar
// builds and general reads are unchanged.
func MergeReaderOptions() ReaderOptions {
	return ReaderOptions{BatchSize: mergeReaderBatchSize, BufferedStream: true}
}

var (
	// errNilReaderInput is returned when NewReader is given a nil
	// parquet.ReaderAtSeeker.
	errNilReaderInput = errors.New("storage: nil parquet reader input")
	// errInvalidFileSize is returned when the supplied file size cannot hold a
	// Parquet footer.
	errInvalidFileSize = errors.New("storage: invalid parquet file size")
	// errInvalidSchema is returned when a Parquet file does not have compatible
	// base columns.
	errInvalidSchema = errors.New("storage: invalid parquet schema")
	// errReaderClosed is returned by Records after Close.
	errReaderClosed = errors.New("storage: reader is closed")
	// errReaderUninitialized is returned by a zero-value or nil Reader.
	errReaderUninitialized = errors.New("storage: reader is not initialized")
	// errNilContext is returned when Records is called without a context.
	errNilContext = errors.New("storage: nil context")
	// errInvalidReaderOptions is returned when ReaderOptions cannot be used.
	errInvalidReaderOptions = errors.New("storage: invalid reader options")
)

// Reader reads Arrow record batches from a Parquet log file containing the
// Parkana base columns (plus any promoted label or pipeline columns).
//
// The caller retains ownership of the underlying parquet.ReaderAtSeeker; Close
// releases internal Parquet and Arrow resources without closing it.
type Reader struct {
	fr     *pqarrow.FileReader
	schema *arrow.Schema
	closed atomic.Bool

	// closeOnce closes the underlying Parquet reader exactly once. Reads go
	// through an io.SectionReader, so that close never touches caller-owned
	// I/O.
	closeOnce func() error
}

// NewReader opens a Parquet log file of the given byte size from r. The caller
// retains ownership of r; Reader.Close does not close it.
//
// NewReader validates the base columns (_ts as timestamp[us, UTC], line as
// string, and labels/meta as map<string, string>) and caches the full Arrow
// schema. Additional promoted-label or pipeline columns are retained.
func NewReader(r parquet.ReaderAtSeeker, size int64) (*Reader, error) {
	return newReader(r, size, memory.NewGoAllocator())
}

// NewReaderWithOptions opens a Parquet log file like NewReader, using opts to
// tune batching and buffering. The caller retains ownership of r;
// Reader.Close does not close it.
//
// The options affect decoding only: the yielded rows, schema and ownership
// rules are identical to NewReader's.
func NewReaderWithOptions(r parquet.ReaderAtSeeker, size int64, opts ReaderOptions) (*Reader, error) {
	return newReaderWithOptions(r, size, memory.NewGoAllocator(), opts)
}

// newReader is NewReader with an explicit Arrow allocator, primarily so tests
// can use a memory.CheckedAllocator.
func newReader(r parquet.ReaderAtSeeker, size int64, mem memory.Allocator) (*Reader, error) {
	return newReaderWithOptions(r, size, mem, ReaderOptions{})
}

// newReaderWithOptions is NewReaderWithOptions with an explicit Arrow
// allocator, primarily so tests can use a memory.CheckedAllocator.
func newReaderWithOptions(r parquet.ReaderAtSeeker, size int64, mem memory.Allocator, opts ReaderOptions) (*Reader, error) {
	if r == nil {
		return nil, errNilReaderInput
	}
	if size < 8 {
		return nil, fmt.Errorf("%w: %d bytes cannot contain a parquet footer", errInvalidFileSize, size)
	}
	if mem == nil {
		return nil, errors.New("storage: nil arrow allocator")
	}
	if opts.BatchSize < 0 {
		return nil, fmt.Errorf("%w: batch size %d", errInvalidReaderOptions, opts.BatchSize)
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = readerBatchSize
	}

	// Trust but verify the caller's size, then restore the caller's seek
	// position. Reading through a SectionReader keeps file.Reader from closing
	// the caller's I/O even when it implements io.Closer, and bounds reads to
	// the declared object size.
	pos, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("storage: determine parquet position: %w", err)
	}
	end, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		_, _ = r.Seek(pos, io.SeekStart)
		return nil, fmt.Errorf("storage: determine parquet size: %w", err)
	}
	if _, err := r.Seek(pos, io.SeekStart); err != nil {
		return nil, fmt.Errorf("storage: restore parquet position: %w", err)
	}
	if end != size {
		return nil, fmt.Errorf("%w: supplied size %d, reader size %d", errInvalidFileSize, size, end)
	}

	section := io.NewSectionReader(r, 0, size)
	props := parquet.NewReaderProperties(mem)
	props.BufferedStreamEnabled = opts.BufferedStream
	pf, err := file.NewParquetReader(section, file.WithReadProps(props))
	if err != nil {
		return nil, fmt.Errorf("storage: open parquet: %w", err)
	}

	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{
		BatchSize: int64(opts.BatchSize),
		Parallel:  false,
	}, mem)
	if err != nil {
		_ = pf.Close()
		return nil, fmt.Errorf("storage: open arrow reader: %w", err)
	}

	sch, err := fr.Schema()
	if err != nil {
		_ = pf.Close()
		return nil, fmt.Errorf("storage: read parquet schema: %w", err)
	}
	if err := validateBaseSchema(sch); err != nil {
		_ = pf.Close()
		return nil, err
	}

	closeOnce := sync.OnceValue(func() error { return pf.Close() })
	return &Reader{fr: fr, schema: sch, closeOnce: closeOnce}, nil
}

// validateBaseSchema checks that sch has the required Parkana base columns with
// the expected logical types. Extra fields are allowed and retained.
func validateBaseSchema(sch *arrow.Schema) error {
	if sch == nil {
		return fmt.Errorf("%w: missing schema", errInvalidSchema)
	}

	for _, name := range []string{schema.ColTS, schema.ColLine, schema.ColLabels, schema.ColMeta} {
		fields, ok := sch.FieldsByName(name)
		if !ok {
			return fmt.Errorf("%w: missing base column %q", errInvalidSchema, name)
		}
		if len(fields) != 1 {
			return fmt.Errorf("%w: base column %q appears %d times", errInvalidSchema, name, len(fields))
		}
		if err := validateBaseField(fields[0]); err != nil {
			return err
		}
	}
	return nil
}

func validateBaseField(field arrow.Field) error {
	switch field.Name {
	case schema.ColTS:
		ts, ok := field.Type.(*arrow.TimestampType)
		if !ok || ts.Unit != arrow.Microsecond || ts.TimeZone != "UTC" {
			return fmt.Errorf("%w: %s must be timestamp[us, UTC], got %s", errInvalidSchema, field.Name, field.Type)
		}
	case schema.ColLine:
		if !arrow.TypeEqual(field.Type, arrow.BinaryTypes.String) {
			return fmt.Errorf("%w: %s must be string, got %s", errInvalidSchema, field.Name, field.Type)
		}
	case schema.ColLabels, schema.ColMeta:
		if !isStringMap(field.Type) {
			return fmt.Errorf("%w: %s must be map<string, string>, got %s", errInvalidSchema, field.Name, field.Type)
		}
	}
	return nil
}

// isStringMap reports whether t is a map with string keys and values. Arrow's
// KeysSorted hint and item nullability vary between Parquet writers and do not
// change the decoded key/value types, so they are ignored here.
func isStringMap(t arrow.DataType) bool {
	m, ok := t.(*arrow.MapType)
	if !ok {
		return false
	}
	return arrow.TypeEqual(m.KeyType(), arrow.BinaryTypes.String) &&
		arrow.TypeEqual(m.ItemType(), arrow.BinaryTypes.String)
}

// Schema returns the Arrow schema of the Parquet file, or nil if the reader is
// uninitialized.
func (r *Reader) Schema() *arrow.Schema {
	if r == nil {
		return nil
	}
	return r.schema
}

// Records streams Arrow record batches from the file until EOF, an error, or
// context cancellation.
//
// Each yielded arrow.RecordBatch is borrowed from the iterator and is valid
// only until the iterator advances or terminates; callers that need a batch
// longer must call Retain on it and later Release it.
func (r *Reader) Records(ctx context.Context) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		if ctx == nil {
			yield(nil, errNilContext)
			return
		}
		if err := ctx.Err(); err != nil {
			yield(nil, err)
			return
		}
		if r == nil || r.fr == nil {
			yield(nil, errReaderUninitialized)
			return
		}
		if r.closed.Load() {
			yield(nil, errReaderClosed)
			return
		}

		rr, err := r.fr.GetRecordReader(ctx, nil, nil)
		if err != nil {
			yield(nil, fmt.Errorf("storage: open record reader: %w", err))
			return
		}
		// Release the record reader, and through it the last borrowed batch,
		// on every exit path including an early stop by the caller.
		defer rr.Release()

		for {
			if err := ctx.Err(); err != nil {
				yield(nil, err)
				return
			}
			if r.closed.Load() {
				yield(nil, errReaderClosed)
				return
			}

			if !rr.Next() {
				if err := rr.Err(); err != nil {
					yield(nil, fmt.Errorf("storage: read parquet: %w", err))
				}
				return
			}
			rec := rr.RecordBatch()
			// Next released the previous batch. Check cancellation before
			// handing this one to the caller so a cancelled query stops
			// promptly; the deferred Release frees rec.
			if err := ctx.Err(); err != nil {
				yield(nil, err)
				return
			}
			if !yield(rec, nil) {
				return
			}
		}
	}
}

// Close releases internal Parquet and Arrow resources held by r without
// closing the caller's parquet.ReaderAtSeeker. It is safe to call more than
// once.
func (r *Reader) Close() error {
	if r == nil {
		return nil
	}
	r.closed.Store(true)
	if r.closeOnce == nil {
		return nil
	}
	if err := r.closeOnce(); err != nil {
		return fmt.Errorf("storage: close parquet reader: %w", err)
	}
	return nil
}
