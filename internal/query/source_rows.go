package query

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// primedRows is one query attempt's result set with its first row already
// current, plus the object leases backing it. openRows returns it only after
// the first Next succeeded, so every consuming statement (Select's ORDER BY,
// Aggregate's GROUP BY, metadata's DISTINCT sort) has read every input file
// before the caller can expose a row.
//
// Callers iterate with Next and Scan; the primed first row is returned by the
// first Next, exactly like *sql.Rows. Err reports iteration failures. Close is
// idempotent: it closes the *sql.Rows and then calls the object lease release,
// once, and returns the close error to every caller.
type primedRows struct {
	rows  *sql.Rows
	first bool // the primed first row has not been handed out by Next yet

	close func() error
}

// newPrimedRows wraps an already-primed result set and its release function.
// release must not be nil; it runs exactly once, after rows is closed.
func newPrimedRows(rows *sql.Rows, release func()) *primedRows {
	r := &primedRows{rows: rows, first: true}
	r.close = sync.OnceValue(func() error {
		err := r.rows.Close()
		release()
		return err
	})
	return r
}

// Next advances to the next row, returning the primed first row before
// delegating to the underlying *sql.Rows.
func (r *primedRows) Next() bool {
	if r.first {
		r.first = false
		return true
	}
	return r.rows.Next()
}

// Scan copies the current row into dest, like *sql.Rows.Scan.
func (r *primedRows) Scan(dest ...any) error { return r.rows.Scan(dest...) }

// Err returns the error, if any, that ended iteration.
func (r *primedRows) Err() error { return r.rows.Err() }

// Close closes the result set and releases the object leases behind it. The
// first call does the work; later calls return the same error.
func (r *primedRows) Close() error { return r.close() }

// scanRow decodes the current row of a Select statement into a plan.Row.
// labels_json is the complete canonical label map built in SQL inside the
// row's group (see normRelation), so no label merging happens here.
func scanRow(rows *primedRows) (plan.Row, error) {
	var ts time.Time
	var line string
	var labelsJSON, metaJSON sql.NullString
	if err := rows.Scan(&ts, &line, &labelsJSON, &metaJSON); err != nil {
		return plan.Row{}, fmt.Errorf("query: scan row: %w", err)
	}
	labels, err := decodeMap(labelsJSON)
	if err != nil {
		return plan.Row{}, fmt.Errorf("query: scan row: decode labels: %w", err)
	}
	meta, err := decodeMap(metaJSON)
	if err != nil {
		return plan.Row{}, fmt.Errorf("query: scan row: decode meta: %w", err)
	}
	return plan.Row{TS: ts.UTC(), Line: line, Labels: logs.FromMap(labels), Meta: logs.FromMap(meta)}, nil
}
