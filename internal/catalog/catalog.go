// Package catalog is the metadata store: the file list every query prunes
// against, pipeline definitions, and (later) stream schemas, views and
// watermarks.
//
// Roles share state only through the catalog and the object store, never
// in-process, so they can run in separate processes later. The in-memory
// implementation here is for tests and `-catalog.driver=memory`; SQLite (and
// later Postgres) implement the same interface and must pass catalogtest.Run.
package catalog

import (
	"cmp"
	"context"
	"errors"
	"iter"
	"slices"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

var (
	// ErrExists is returned when adding a file whose key is already listed.
	ErrExists = errors.New("catalog: file already exists")
	// ErrNotFound is returned when replacing a file that isn't listed or is
	// already deleted.
	ErrNotFound = errors.New("catalog: file not found")
)

// File is one Parquet data file in the object store.
type File struct {
	Tenant tenant.ID
	Stream string
	// Key is the object key; unique per tenant.
	Key string
	// MinTS and MaxTS bound the file's _ts values, inclusive.
	MinTS, MaxTS time.Time
	Rows, Bytes  int64
	// LabelValues maps each promoted label name to the distinct values it
	// takes in this file (sorted), for pruning by stream selector. The keys
	// are also the file's promoted columns (the querier groups files by them).
	LabelValues map[string][]string
	// LabelsComplete reports that LabelValues is a complete summary: for
	// every promoted name, the sorted distinct values of
	// coalesce(column, labels[name], "") over ALL rows, including the
	// map-only values a schema-evolution merge leaves beside a NULL column
	// and "" for rows without the label (PLAN.md decision 13). Only complete
	// summaries prune in Filter.Match; files cataloged before phase 3 have it
	// false and are never pruned by label.
	LabelsComplete bool
	// HasSidecar reports that the search sidecar (objstore.SidecarKey)
	// exists. Writers put the sidecar object before they catalog the file
	// with HasSidecar (or call SetSidecar), so a true flag never points at a
	// missing object; readers must still treat an unreadable sidecar as
	// "cannot prune".
	HasSidecar bool
	CreatedAt  time.Time
	// DeletedAt is set when compaction or retention replaced the file. Deleted
	// files stay listed until PurgeDeleted so in-flight queries can finish.
	DeletedAt time.Time
}

// Deleted reports whether the file was logically deleted.
func (f File) Deleted() bool { return !f.DeletedAt.IsZero() }

// Clone returns a deep copy of f.
func (f File) Clone() File {
	if f.LabelValues != nil {
		lv := make(map[string][]string, len(f.LabelValues))
		for k, v := range f.LabelValues {
			lv[k] = slices.Clone(v)
		}
		f.LabelValues = lv
	}
	return f
}

// Filter selects files. Tenant is required; zero values of the other fields
// mean "no constraint".
type Filter struct {
	Tenant tenant.ID
	Stream string
	// From and Through select files whose [MinTS, MaxTS] overlaps [From, Through].
	From, Through time.Time
	// Match keeps files whose LabelValues[name] contains the value. Files
	// without LabelsComplete, or with no LabelValues entry for a name, are
	// kept (they can't be pruned).
	Match          map[string]string
	IncludeDeleted bool
}

// Matches reports whether f passes the filter. Implementations may push the
// filter down to their store, but must return exactly the files Matches
// accepts.
func (flt Filter) Matches(f File) bool {
	if f.Tenant != flt.Tenant || flt.Stream != "" && f.Stream != flt.Stream {
		return false
	}
	if f.Deleted() && !flt.IncludeDeleted {
		return false
	}
	if !flt.From.IsZero() && f.MaxTS.Before(flt.From) || !flt.Through.IsZero() && f.MinTS.After(flt.Through) {
		return false
	}
	for name, want := range flt.Match {
		if !f.LabelsComplete {
			break
		}
		if vals, ok := f.LabelValues[name]; ok && !slices.Contains(vals, want) {
			return false
		}
	}
	return true
}

// Catalog is safe for concurrent use.
type Catalog interface {
	// AddFiles lists new files atomically: all or none. It fails with
	// ErrExists if any key is already listed (deleted or not).
	AddFiles(ctx context.Context, files ...File) error
	// ReplaceFiles atomically marks the remove keys deleted and adds the new
	// files: the compactor's swap. It fails with ErrNotFound if any remove
	// key is missing or already deleted, and changes nothing then.
	ReplaceFiles(ctx context.Context, t tenant.ID, remove []string, add []File) error
	// ListFiles yields matching files ordered by MinTS, then Key.
	ListFiles(ctx context.Context, f Filter) iter.Seq2[File, error]
	// Streams returns the tenant's stream names that have live files, sorted.
	Streams(ctx context.Context, t tenant.ID) ([]string, error)
	// SetSidecar marks a live file as having its search sidecar: the
	// backfill path for files cataloged before their sidecar was built. The
	// caller puts the sidecar object first. It is idempotent and fails with
	// ErrNotFound if the key is missing or deleted.
	SetSidecar(ctx context.Context, t tenant.ID, key string) error
	// PurgeDeleted drops files deleted before cutoff from the catalog and
	// returns their keys so the caller can delete the objects.
	PurgeDeleted(ctx context.Context, t tenant.ID, cutoff time.Time) ([]string, error)

	// GetPipeline returns the tenant's live pipeline definition named name.
	// It fails with ErrNotFound when the name has no live row (never created
	// or tombstoned).
	GetPipeline(ctx context.Context, t tenant.ID, name string) (PipelineDefinition, error)
	// ListPipelines returns the tenant's live pipeline definitions sorted by
	// name.
	ListPipelines(ctx context.Context, t tenant.ID) ([]PipelineDefinition, error)
	// PutPipeline creates, replaces or recreates a pipeline with optimistic
	// concurrency. A create passes expectedVersion zero, including when the
	// name is tombstoned (recreation continues above the tombstone version
	// and gets a fresh CreatedAt); a replace passes the live version. It
	// returns the stored definition with the catalog-assigned version and
	// timestamps, ignoring the caller's Version and timestamp fields. It
	// fails with ErrNotFound when there is no live row and expectedVersion is
	// nonzero, and with ErrVersionConflict when expectedVersion does not match
	// the live version (so concurrent creates have exactly one winner). Spec
	// bytes are stored verbatim. A successful write advances the tenant's
	// change token.
	PutPipeline(ctx context.Context, def PipelineDefinition, expectedVersion int64) (PipelineDefinition, error)
	// DeletePipeline tombstones a live pipeline whose version equals
	// expectedVersion and returns the tombstone. It fails with ErrNotFound
	// when no live row exists and ErrVersionConflict on a stale version. A
	// successful delete advances the tenant's change token.
	DeletePipeline(ctx context.Context, t tenant.ID, name string, expectedVersion int64) (PipelineDefinition, error)
	// PipelineChangeToken returns the tenant's durable monotonic change token,
	// zero before its first successful pipeline write. Pollers compare it to
	// their last observed value.
	PipelineChangeToken(ctx context.Context, t tenant.ID) (int64, error)

	Close() error
}

// SortFiles orders files by MinTS, then Key: the ListFiles order.
func SortFiles(files []File) {
	slices.SortFunc(files, func(a, b File) int {
		if c := a.MinTS.Compare(b.MinTS); c != 0 {
			return c
		}
		return cmp.Compare(a.Key, b.Key)
	})
}
