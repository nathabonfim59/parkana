package ingest

// This file is the flush half of local publication: it turns a sealed
// memtable generation into committed local Parquet files and one atomic
// tenant checkpoint, then lets the WAL reclaim what the manifest durably
// covers.
//
// Crash semantics: a generation is visible only through its manifest commit.
// Files written and renamed but not committed are quarantined on the next
// recovery; a committed generation whose WAL reclaim failed is replayed from
// the checkpoint and republished idempotently.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/tenant"
	"github.com/nathabonfim59/parkana/internal/wal"
)

const (
	// defaultFlushConcurrency is used when FlushOptions.Concurrency is zero.
	defaultFlushConcurrency = 4
	// maxFlushConcurrency bounds the part writers of one generation.
	maxFlushConcurrency = 64
)

// SealedGeneration is the flush-facing slice of the memtable contract: one
// sealed, immutable tenant generation. *Generation satisfies it. Parts are
// lazily materialized and borrowed until the caller releases the generation;
// Flush never calls Release.
type SealedGeneration interface {
	// Tenant is the tenant the generation belongs to.
	Tenant() tenant.ID
	// Frontier is the inclusive WAL sequence range the generation covers.
	Frontier() (first, last uint64)
	// Parts materializes sorted per-stream/UTC-hour batches.
	Parts(ctx context.Context) ([]Part, error)
}

// ReclaimFunc drops WAL segments durably covered by a published frontier;
// wired to wal.WAL.Reclaim in production.
type ReclaimFunc func(ctx context.Context, id tenant.ID, frontier wal.Position) error

// FlushOptions configures a Flusher.
type FlushOptions struct {
	// Store is the rooted local publication state. Required.
	Store *LocalStore
	// NewWriter builds the Parquet writer for one part file; nil uses
	// storage.NewWriter. It may be called concurrently from multiple part
	// writers when Concurrency > 1, so it must be safe for concurrent use.
	NewWriter WriterFactory
	// Reclaim truncates covered WAL segments after a committed publication;
	// nil disables reclaim (tests without a WAL).
	Reclaim ReclaimFunc
	// PromotedFor returns the stream's promoted label columns; nil promotes
	// nothing. It may be called concurrently from multiple part writers when
	// Concurrency > 1, so it must be safe for concurrent use.
	PromotedFor func(stream string) []string
	// Concurrency bounds how many parts of one generation are written at
	// once. Zero selects defaultFlushConcurrency (4); valid values are 1
	// through 64. One writes parts strictly serially in generation order.
	Concurrency int
}

// Flusher publishes sealed generations to local Parquet under
// <data.dir>/parquet/<tenant>/.
type Flusher struct {
	store       *LocalStore
	nf          WriterFactory
	reclaim     ReclaimFunc
	promotedFor func(stream string) []string
	concurrency int
}

// NewFlusher validates o and returns a Flusher.
func NewFlusher(o FlushOptions) (*Flusher, error) {
	if o.Store == nil {
		return nil, errors.New("ingest: FlushOptions.Store is required")
	}
	if o.NewWriter == nil {
		o.NewWriter = defaultWriterFactory
	}
	if o.PromotedFor == nil {
		o.PromotedFor = func(string) []string { return nil }
	}
	switch {
	case o.Concurrency == 0:
		o.Concurrency = defaultFlushConcurrency
	case o.Concurrency < 0 || o.Concurrency > maxFlushConcurrency:
		return nil, fmt.Errorf("ingest: FlushOptions.Concurrency %d must be between 1 and %d", o.Concurrency, maxFlushConcurrency)
	}
	return &Flusher{
		store:       o.Store,
		nf:          o.NewWriter,
		reclaim:     o.Reclaim,
		promotedFor: o.PromotedFor,
		concurrency: o.Concurrency,
	}, nil
}

// Flush publishes gen: every part becomes a fsynced local Parquet file, then
// one manifest commit records the files and advances the tenant checkpoint,
// then the WAL is reclaimed through the new frontier. A generation already
// covered by the committed frontier is a no-op (exactly-once across
// restarts); gaps and overlaps are rejected. On any failure before the
// manifest commit nothing is durable and a retry rewrites cleanly; on a
// Reclaim failure publication is still durable and the error is returned so
// the caller can retry or let the next flush cover it.
func (f *Flusher) Flush(ctx context.Context, gen SealedGeneration) error {
	if gen == nil {
		return errors.New("ingest: nil generation")
	}
	id := gen.Tenant()
	if err := id.Validate(); err != nil {
		return fmt.Errorf("ingest: flush: %w", err)
	}
	first, last := gen.Frontier()
	if first == 0 || first > last {
		return fmt.Errorf("ingest: invalid generation frontier %d-%d", first, last)
	}
	ts, err := f.store.Tenant(ctx, id)
	if err != nil {
		return err
	}
	cur, err := ts.Frontier(ctx)
	if err != nil {
		return err
	}
	switch {
	case last <= cur.Last:
		// Covered generations retry the post-publish reclaim that a crash or
		// error may have lost, at the committed frontier, without touching
		// parts or files.
		if f.reclaim != nil {
			if err := f.reclaim(ctx, id, wal.Position{Tenant: id, Seq: cur.Last}); err != nil {
				return fmt.Errorf("ingest: reclaim %s at committed frontier %d: %w", id, cur.Last, err)
			}
		}
		return nil
	case first > cur.Last+1:
		return fmt.Errorf("%w: generation starts at %d, committed frontier is %d", ErrGenerationGap, first, cur.Last)
	case first <= cur.Last:
		return fmt.Errorf("%w: generation %d-%d overlaps committed frontier %d", ErrGenerationOverlap, first, last, cur.Last)
	}
	parts, err := gen.Parts(ctx)
	if err != nil {
		return fmt.Errorf("ingest: materialize generation %d-%d: %w", first, last, err)
	}
	files, err := f.writeParts(ctx, ts, parts)
	if err != nil {
		return err
	}
	if err := ts.Publish(ctx, Frontier{First: first, Last: last}, files); err != nil {
		return err
	}
	if f.reclaim != nil {
		if err := f.reclaim(ctx, id, wal.Position{Tenant: id, Seq: last}); err != nil {
			return fmt.Errorf("ingest: reclaim %s after publishing %d-%d: %w", id, first, last, err)
		}
	}
	return nil
}

// writeParts writes one generation's parts, at most f.concurrency at a time,
// and returns the resulting files in part order. Concurrency one runs the
// original serial loop. Above one, a fixed worker pool consumes a bounded
// index queue; every result and error is stored in its part's index slot, so
// output and error joins stay deterministic regardless of completion order.
// The first failed part stops further scheduling, every worker is joined
// before returning (the borrowed part records are never used past Flush), and
// all worker errors are preserved: a fatal error recorded concurrently with a
// cancellation or transient failure still survives errors.Join.
func (f *Flusher) writeParts(ctx context.Context, ts *TenantStore, parts []Part) ([]LocalFile, error) {
	if len(parts) == 0 {
		return nil, nil
	}
	written := make([]LocalFile, len(parts))
	if f.concurrency == 1 {
		for i, p := range parts {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			lf, err := f.writePart(ctx, ts, p)
			if err != nil {
				return nil, err
			}
			written[i] = lf
		}
		return collectFiles(written), nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := min(f.concurrency, len(parts))
	jobs := make(chan int, workers)
	errs := make([]error, len(parts))
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for i := range jobs {
				// Stop dispatching once any part failed; parts already in
				// flight still record their own error below.
				if runCtx.Err() != nil {
					return
				}
				lf, err := f.writePart(runCtx, ts, parts[i])
				if err != nil {
					errs[i] = err
					cancel()
					return
				}
				written[i] = lf
			}
		})
	}
feed:
	for i := range parts {
		select {
		case jobs <- i:
		case <-runCtx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	if err := joinPartErrors(errs); err != nil {
		return nil, err
	}
	// Cancellation before or during scheduling may have left parts
	// undispatched without any worker error; report it rather than publishing
	// an incomplete set. A canceled context after every part completed is
	// also surfaced, keeping Flush consistent with its serial path.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return collectFiles(written), nil
}

// writePart writes one part through the tenant store.
func (f *Flusher) writePart(ctx context.Context, ts *TenantStore, p Part) (LocalFile, error) {
	return ts.writeRecord(ctx, p.Stream, p.Hour, p.Record, f.nf, f.promotedFor(p.Stream))
}

// collectFiles drops the empty slots left by empty parts, preserving part
// order.
func collectFiles(written []LocalFile) []LocalFile {
	files := make([]LocalFile, 0, len(written))
	for _, lf := range written {
		if lf.ID != uuid.Nil() {
			files = append(files, lf)
		}
	}
	return files
}

// joinPartErrors joins every recorded part error in stable part order.
func joinPartErrors(errs []error) error {
	kept := make([]error, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			kept = append(kept, err)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return errors.Join(kept...)
}

// ReclaimAll re-runs WAL reclaim for every tenant's committed frontier after
// a restart, covering reclaims lost to a crash between the manifest commit
// and the reclaim call.
func (f *Flusher) ReclaimAll(ctx context.Context) error {
	if f.reclaim == nil {
		return nil
	}
	ids, err := f.store.Tenants(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, id := range ids {
		fr, err := f.store.Frontier(ctx, id)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if fr.Last == 0 {
			continue
		}
		if err := f.reclaim(ctx, id, wal.Position{Tenant: id, Seq: fr.Last}); err != nil {
			errs = append(errs, fmt.Errorf("ingest: reclaim %s at %d: %w", id, fr.Last, err))
		}
	}
	return errors.Join(errs...)
}
