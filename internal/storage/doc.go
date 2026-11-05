// Package storage reads, writes and merges Parkana's Parquet log files using
// Apache Arrow record batches.
//
// Ownership rules:
//   - Callers own all supplied I/O objects (the io.Writer passed to NewWriter
//     and Merge, and the parquet.ReaderAtSeeker passed to NewReader). Writer.Close,
//     Reader.Close and Merge never close caller-supplied I/O objects, and Merge
//     never closes its input Readers.
//   - Callers own the arrow.RecordBatch values passed to Writer.Write; Write
//     does not release them.
//   - Reader.Records yields borrowed arrow.RecordBatch values that remain valid
//     only until the iterator advances or stops. Callers that keep a batch past
//     the current iteration step must call Retain on it and Release it when done.
//   - Reader.Close releases internal Parquet and Arrow reader resources without
//     closing the underlying parquet.ReaderAtSeeker.
package storage

import (
	"errors"
	"time"
)

// ErrNotImplemented is returned by unimplemented storage stubs.
var ErrNotImplemented = errors.New("storage: not implemented")

// FileStats summarizes the rows written to a Parquet file, suitable for
// populating a catalog.File entry.
type FileStats struct {
	// MinTS and MaxTS bound the file's _ts values (UTC, inclusive). Both are
	// zero when Rows is 0.
	MinTS, MaxTS time.Time
	// Rows is the total number of rows written.
	Rows int64
	// DeduplicatedRows counts the physical input rows that Merge removed as
	// exact duplicates: rows whose timestamp, complete effective label set,
	// metadata and exact line bytes matched an earlier row of the same
	// equal-timestamp run (see Merge for the identity rules). It is set only
	// by Merge; an ordinary Writer always leaves it zero, and for a merge the
	// input row total is Rows + DeduplicatedRows.
	DeduplicatedRows int64
	// LabelValues maps each promoted label column to its sorted distinct
	// effective values in the file: the non-null column value, else the labels
	// map entry, else "" for a label absent from both.
	LabelValues map[string][]string
	// LabelsComplete reports that LabelValues is the complete summary
	// catalog.File.LabelsComplete describes. Writer (and therefore Merge, which
	// writes through it) sets it only after a successful Close; an empty file
	// has no values but is still complete.
	LabelsComplete bool
}
