package query

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/plan"
)

// errObjectDisappeared marks an attempt failure caused by an input object
// vanishing between the catalog listing and the first row. Only such errors
// are retried, and only once, after a fresh listing.
var errObjectDisappeared = errors.New("input object disappeared")

// duckDBMissingPatternNeedle and duckDBMissingErrnoNeedle are the DuckDB IO
// messages that can mean an input file was deleted. They are disappearance
// phrases only: corrupt Parquet and generic IO failures use different text and
// never match. A textual match is never enough on its own; disappearedObject
// also requires the named path to be physically gone.
const (
	duckDBMissingPatternNeedle = "no files found that match the pattern"
	duckDBMissingErrnoNeedle   = "no such file or directory"
)

// openRows runs one whole attempt for sel (list the catalog, prune the
// listing by label summaries and search sidecars, lease every remaining
// object, compile with compile, query and prime the first row), retrying that
// whole attempt exactly once after a fresh listing and fresh pruning when an
// input object disappeared between listing and the first row. A nil result
// with a nil error means there was nothing to read: no file matched, every
// file was pruned, or the query matched no rows. Nothing is exposed before
// the first row is ready, so a retry can never replay or duplicate output.
//
// compile's statement MUST block-read every input before its first row: an
// outer ORDER BY (Select), GROUP BY (Aggregate) or DISTINCT sort (metadata) is
// what makes the attempt boundary real. With such a blocking operator DuckDB
// opens and reads every input file before the primed first row, so a file
// disappearing earlier fails the attempt cleanly; a streaming statement would
// let a late failure happen after rows were already yielded, where no retry is
// possible.
//
// openRows does not validate sel: both Select and Aggregate validate their
// public arguments first, and the metadata reader may legitimately have no
// matchers. Callers that expose rows must still validate their own requests.
//
// Classification is deliberately narrow (see disappearedObject): catalog-list
// failures, corrupt Parquet, generic IO failures, cancellation and joined
// failures are never retried. When an attempt fails, its rows and leases are
// released before the retry; both failures are joined into the reported error
// so neither the first disappearance nor the retry's real cause is lost.
func (s *source) openRows(ctx context.Context, sel plan.Selection, compile func([]fileGroup) selectQuery) (*primedRows, error) {
	rows, err := s.attemptRows(ctx, sel, compile)
	if err == nil {
		return rows, nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, fmt.Errorf("query: open rows: %w", errors.Join(err, ctxErr))
	}
	if !errors.Is(err, errObjectDisappeared) {
		return nil, err
	}
	retry, retryErr := s.attemptRows(ctx, sel, compile)
	if retryErr == nil {
		return retry, nil
	}
	cause := errors.Join(err, retryErr)
	if ctxErr := ctx.Err(); ctxErr != nil {
		cause = errors.Join(cause, ctxErr)
	}
	return nil, fmt.Errorf("query: open rows: retry after a fresh listing failed: %w", cause)
}

// attemptRows runs one whole attempt: list, prune, acquire every lease,
// compile, query and prime the first row before returning. A nil result with
// a nil error means the listing was empty, every file was pruned, or the query
// matched no rows; in every case the rows and leases are already released and
// no statement was compiled for an empty candidate list.
func (s *source) attemptRows(ctx context.Context, sel plan.Selection, compile func([]fileGroup) selectQuery) (*primedRows, error) {
	files, err := s.listObjects(ctx, sel)
	if err != nil {
		// A catalog failure is not a disappearance: never retry it.
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}
	files = s.pruneIndex(ctx, sel, files)
	if len(files) == 0 {
		// Every candidate was pruned: nothing to scan, no SQL to run.
		return nil, nil
	}
	paths, release, err := s.acquire(ctx, sel.Tenant, files)
	if err != nil {
		if key, ok := disappearedObject(ctx, err, files, nil); ok {
			return nil, disappearedError(key, err)
		}
		return nil, err
	}
	q := compile(groupFiles(files, paths))
	s.countScan(files)
	rows, err := s.db.QueryContext(ctx, q.sql, q.args...)
	if err != nil {
		release()
		err = fmt.Errorf("query: execute rows: %w", err)
		if key, ok := disappearedObject(ctx, err, files, paths); ok {
			return nil, disappearedError(key, err)
		}
		return nil, err
	}
	if !rows.Next() {
		// nil error: an empty result set, released like any other attempt.
		err := errors.Join(rows.Err(), rows.Close())
		release()
		if err == nil {
			return nil, nil
		}
		err = fmt.Errorf("query: prime rows: %w", err)
		if key, ok := disappearedObject(ctx, err, files, paths); ok {
			return nil, disappearedError(key, err)
		}
		return nil, err
	}
	return newPrimedRows(rows, release), nil
}

// disappearedError marks cause as a confirmed disappearance of key (empty when
// the object could not be identified) so openRows retries exactly once.
func disappearedError(key string, cause error) error {
	if key == "" {
		return fmt.Errorf("query: %w: %w", errObjectDisappeared, cause)
	}
	return fmt.Errorf("query: %w: %s: %w", errObjectDisappeared, key, cause)
}

// disappearedObject reports whether err is a single, unambiguous disappearance
// of one of the attempt's input objects, and returns that object's catalog key
// when it can be identified. paths are the acquired local paths in files
// order; nil paths means the failure happened during acquisition.
//
// The rules are conservative on purpose:
//   - context cancellation is never a disappearance;
//   - a tree with two or more genuine failures is never one, even when one
//     branch is fs.ErrNotExist: retrying would hide the others. Single-child
//     join wrappers are transparent, so errors.Join(err, nil) still classifies;
//   - an acquisition failure qualifies only when it is fs.ErrNotExist, which
//     the resolver contract reserves for a missing object;
//   - a query/open failure qualifies only when its message carries a
//     disappearance phrase and names an acquired path that is now physically
//     missing. Paths are checked exactly as acquired, so relative paths from
//     injected resolvers work too.
//
// Corrupt Parquet, malformed SQL results and generic IO errors therefore never
// retry, even when an unrelated file happens to be missing at the same time.
func disappearedObject(ctx context.Context, err error, files []catalog.File, paths []string) (string, bool) {
	if err == nil || ctx.Err() != nil || !singleFailure(err) {
		return "", false
	}
	if errors.Is(err, fs.ErrNotExist) {
		return keyNamed(err, files), true
	}
	msg := objectErrorMessage(err)
	if !hasMissingNeedle(msg) {
		return "", false
	}
	for i, p := range paths {
		if p == "" || !strings.Contains(msg, p) {
			continue
		}
		if _, statErr := os.Stat(p); errors.Is(statErr, fs.ErrNotExist) {
			if i < len(files) {
				return files[i].Key, true
			}
			return "", true
		}
	}
	return "", false
}

// keyNamed returns the first catalog key that err's message names.
func keyNamed(err error, files []catalog.File) string {
	msg := err.Error()
	for _, f := range files {
		if strings.Contains(msg, f.Key) {
			return f.Key
		}
	}
	return ""
}

// objectErrorMessage returns the DuckDB driver's structured message when err
// carries one, so classification sees the raw IO text without wrapper noise.
func objectErrorMessage(err error) string {
	if derr, ok := errors.AsType[*duckdb.Error](err); ok {
		return derr.Msg
	}
	return err.Error()
}

// hasMissingNeedle reports whether msg carries one of the disappearance
// phrases DuckDB uses for a deleted input file.
func hasMissingNeedle(msg string) bool {
	lower := strings.ToLower(msg)
	return strings.Contains(lower, duckDBMissingPatternNeedle) ||
		strings.Contains(lower, duckDBMissingErrnoNeedle)
}

// singleFailure reports whether err is one failure chain with no genuine
// multiple failure and no context failure anywhere in it. errors.Join produces
// a tree whose node may hold several children; a single child is just another
// wrapper (errors.Join(err, nil) drops the nil), so it is walked through, but
// two or more children mean several genuine failures: retrying because one
// looks like a disappearance would discard the others.
func singleFailure(err error) bool {
	ok := true
	var walk func(error)
	walk = func(e error) {
		if e == nil || !ok {
			return
		}
		if errors.Is(e, context.Canceled) || errors.Is(e, context.DeadlineExceeded) {
			ok = false
			return
		}
		switch u := e.(type) {
		case interface{ Unwrap() []error }:
			children := u.Unwrap()
			if len(children) != 1 {
				ok = false
				return
			}
			walk(children[0])
		case interface{ Unwrap() error }:
			walk(u.Unwrap())
		}
	}
	walk(err)
	return ok
}
