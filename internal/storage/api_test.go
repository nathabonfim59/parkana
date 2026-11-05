package storage_test

import (
	"context"
	"io"
	"iter"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"

	"github.com/nathabonfim59/parkana/internal/storage"
)

// Compile-time checks against the Phase 1 storage API contract.
var (
	_ error = storage.ErrNotImplemented
	_       = storage.FileStats{
		MinTS:       time.Time{},
		MaxTS:       time.Time{},
		Rows:        int64(0),
		LabelValues: map[string][]string(nil),
	}

	_ func(io.Writer, *arrow.Schema, []string) (*storage.Writer, error)                        = storage.NewWriter
	_ func(*storage.Writer, arrow.RecordBatch) error                                           = (*storage.Writer).Write
	_ func(*storage.Writer) (storage.FileStats, error)                                         = (*storage.Writer).Close
	_ func(parquet.ReaderAtSeeker, int64) (*storage.Reader, error)                             = storage.NewReader
	_ func(*storage.Reader) *arrow.Schema                                                      = (*storage.Reader).Schema
	_ func(*storage.Reader, context.Context) iter.Seq2[arrow.RecordBatch, error]               = (*storage.Reader).Records
	_ func(*storage.Reader) error                                                              = (*storage.Reader).Close
	_ func(context.Context, io.Writer, []*storage.Reader, []string) (storage.FileStats, error) = storage.Merge
)
