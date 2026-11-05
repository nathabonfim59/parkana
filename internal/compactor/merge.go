package compactor

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"time"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// mergeFunc is the Parquet merge seam. Production uses storage.Merge; tests
// replace it to inject malformed stats without a package-level hook.
type mergeFunc func(context.Context, io.Writer, []*storage.Reader, []string) (storage.FileStats, error)

// mergeHour picks the batch of files to merge. Candidates are packed by
// ascending size (ties by key): picking the biggest first could strand every
// eligible pair behind one file that fits no partner. The selected batch is
// re-sorted into catalog order (MinTS, then Key) so merge output keeps
// chronological order on equal timestamps. Fewer than two files or a pair
// over the target is a no-op.
func (s *service) mergeHour(ctx context.Context, t tenant.ID, stream string, files []catalog.File) error {
	bySize := slices.Clone(files)
	slices.SortFunc(bySize, func(a, b catalog.File) int {
		if c := cmp.Compare(a.Bytes, b.Bytes); c != 0 {
			return c
		}
		return cmp.Compare(a.Key, b.Key)
	})
	var batch []catalog.File
	var sum int64
	promoted := make(map[string]struct{})
	for _, f := range bySize {
		if len(batch) >= maxMergeInputs || sum+f.Bytes > int64(s.cfg.TargetSize) {
			continue
		}
		// Files whose promoted columns differ only in case (the promotion
		// config changed between them) can't share one Parquet schema that a
		// case-insensitive reader resolves unambiguously: leave them apart.
		if collidesWith(promoted, f) {
			continue
		}
		batch = append(batch, f)
		sum += f.Bytes
		for name := range f.LabelValues {
			promoted[name] = struct{}{}
		}
	}
	if len(batch) < 2 {
		return nil
	}
	catalog.SortFiles(batch)
	return s.mergeFiles(ctx, t, stream, batch)
}

// mergeFiles merges batch into one new object and swaps the catalog
// atomically: the merged Parquet is streamed to a fresh key, its stats are
// checked against the catalog rows (physical output plus removed duplicates
// must equal the input total), stat'ed for its byte size, its sidecar is
// built from the final stored object and put under objstore.SidecarKey, and
// only then does Catalog.ReplaceFiles mark the inputs deleted and list the
// output with HasSidecar describing what was published. A failure at any
// point before the swap (including the sidecar build or its Put) returns
// with the original catalog inputs still serving. Source objects are never
// deleted here, so a failure after the upload leaves an orphan object for a
// later cleanup pass and sources keep serving.
func (s *service) mergeFiles(ctx context.Context, t tenant.ID, stream string, batch []catalog.File) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// mctx covers everything the producer touches, ranged input reads and
	// the merge itself, so an early Put failure can abort a blocked object
	// read, not just the next pipe write.
	mctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Input readers stay open until the producer goroutine has finished
	// reading them; Close releases Parquet/Arrow resources only, never the
	// underlying object readers or the store.
	readers := make([]*storage.Reader, 0, len(batch))
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	for _, f := range batch {
		or, err := storage.NewObjectReader(mctx, s.store, t, f.Key)
		if err != nil {
			return fmt.Errorf("open %s: %w", f.Key, err)
		}
		r, err := storage.NewReaderWithOptions(or, or.Size(), storage.MergeReaderOptions())
		if err != nil {
			return fmt.Errorf("read %s: %w", f.Key, err)
		}
		readers = append(readers, r)
	}

	key := objstore.NewDataKey(t, stream, batch[0].MinTS)

	// The merged file's size is unknown until its footer is written, so it
	// streams through a pipe into Put. The producer must be joined on every
	// path: when Put fails early, closing the read side unblocks the
	// producer's next write and cancellation stops its reads.
	pr, pw := io.Pipe()
	type mergeResult struct {
		stats storage.FileStats
		err   error
	}
	resc := make(chan mergeResult, 1)
	go func() {
		stats, err := s.merge(mctx, pw, readers, promotedUnion(batch))
		_ = pw.CloseWithError(err)
		resc <- mergeResult{stats, err}
	}()

	putErr := s.store.Put(ctx, key, pr, -1)
	if putErr != nil {
		_ = pr.CloseWithError(putErr)
	}
	cancel()
	out := <-resc
	_ = pr.Close()

	if putErr != nil {
		return fmt.Errorf("put %s: %w", key, putErr)
	}
	if out.err != nil {
		return fmt.Errorf("merge %s: %w", key, out.err)
	}

	remove := make([]string, len(batch))
	var inBytes int64
	for i, f := range batch {
		remove[i] = f.Key
		inBytes += f.Bytes
	}
	// Never publish an output that does not account for exactly the catalog
	// rows it replaces. The upload stays unreferenced and the sources keep
	// serving, so a metadata bug surfaces as a retried merge plus an orphan
	// for cleanup instead of lost or duplicated log rows.
	if err := checkMergeStats(batch, out.stats); err != nil {
		return fmt.Errorf("verify merge %s: %w", key, err)
	}

	info, err := s.store.Stat(ctx, key)
	if err != nil {
		return fmt.Errorf("stat %s: %w", key, err)
	}

	// The sidecar is built from the final stored object, not from the merge
	// inputs: it must describe exactly the bytes the catalog is about to list.
	// This reread runs on ctx, not mctx: the merge producer's context was
	// canceled once it joined, but the outer context keeps the reads live.
	// Any failure here leaves the sources serving and the output orphaned.
	hasSidecar, err := s.buildSidecar(ctx, t, key, index.SourceCompaction)
	if err != nil {
		return fmt.Errorf("sidecar for %s: %w", key, err)
	}

	repl := catalog.File{
		Tenant:         t,
		Stream:         stream,
		Key:            key,
		MinTS:          out.stats.MinTS,
		MaxTS:          out.stats.MaxTS,
		Rows:           out.stats.Rows,
		Bytes:          info.Size,
		LabelValues:    out.stats.LabelValues,
		LabelsComplete: out.stats.LabelsComplete,
		HasSidecar:     hasSidecar,
		CreatedAt:      time.Now(),
	}
	if err := s.cat.ReplaceFiles(ctx, t, remove, []catalog.File{repl}); err != nil {
		return fmt.Errorf("replace catalog for %s: %w", key, err)
	}

	s.filesMerged.Add(float64(len(batch)))
	s.bytesMerged.Add(float64(inBytes))
	s.log.Info("compacted files",
		"tenant", t, "stream", stream, "inputs", len(batch),
		"in_bytes", inBytes, "rows", out.stats.Rows,
		"deduplicated", out.stats.DeduplicatedRows, "out_bytes", info.Size,
		"sidecar", hasSidecar, "key", key)
	return nil
}

// buildSidecar builds the search sidecar of the object stored at key by
// rereading its final bytes, puts it under objstore.SidecarKey(key) and
// reports whether the file now has a sidecar. src labels the write path on
// the shared parkana_index_* metrics.
//
// The build metrics count successful encodes, not publications: ObserveBuild
// fires as soon as BuildSidecar returns bytes, before the Put, and its
// duration covers only the build, so a failed Put still counts the encode it
// consumed. ObserveSkip likewise records the real read-and-build duration of
// an index.ErrTooManyGrams result. Over the gram cap is the one outcome that
// is not an error: the file is published sidecar-less, and its key is
// remembered in the bounded over-cap skip set so the backfill pass does not
// re-read the same file every cycle. Every other failure means the caller
// must not publish a sidecar flag.
func (s *service) buildSidecar(ctx context.Context, t tenant.ID, key string, src index.Source) (bool, error) {
	or, err := storage.NewObjectReader(ctx, s.store, t, key)
	if err != nil {
		return false, fmt.Errorf("compactor: open %s: %w", key, err)
	}
	r, err := storage.NewReader(or, or.Size())
	if err != nil {
		return false, fmt.Errorf("compactor: read %s: %w", key, err)
	}
	defer func() { _ = r.Close() }()

	start := time.Now()
	enc, err := storage.BuildSidecar(ctx, r, s.params)
	d := time.Since(start)
	if errors.Is(err, index.ErrTooManyGrams) {
		s.idx.ObserveSkip(src, d)
		s.skips.Add(key)
		s.log.Info("compactor sidecar skipped: file over gram cap",
			"tenant", t, "key", key, "source", string(src))
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("compactor: build sidecar %s: %w", key, err)
	}
	s.idx.ObserveBuild(src, d, len(enc))

	sidecarKey := objstore.SidecarKey(key)
	if err := s.store.Put(ctx, sidecarKey, bytes.NewReader(enc), int64(len(enc))); err != nil {
		return false, fmt.Errorf("compactor: put %s: %w", sidecarKey, err)
	}
	return true, nil
}

// checkMergeStats verifies that stats describe exactly the rows in batch.
// Merge may drop exact duplicates, so the physical output rows plus the
// reported deduplicated rows must equal the sum of the catalog input rows:
// accepting a smaller output alone would let dedup hide lost records. Both
// counts must be nonnegative and their sum must not overflow, a nonempty input
// must not collapse to a zero-row output, and a negative or overflowed input
// total is rejected. The timestamp range must be ordered and both ends must
// stay within the aggregate input range. Non-empty inputs with zero-valued
// timestamps keep their bounds: the Unix epoch and pre-epoch times are valid.
// Zero-row inputs carry no range and are ignored for the bounds, only their
// (zero) row count matters.
func checkMergeStats(batch []catalog.File, stats storage.FileStats) error {
	var wantRows int64
	var minTS, maxTS time.Time
	haveBounds := false
	for _, f := range batch {
		if f.Rows < 0 {
			return fmt.Errorf("input rows: catalog file %s has %d rows", f.Key, f.Rows)
		}
		if wantRows > math.MaxInt64-f.Rows {
			return fmt.Errorf("input rows: catalog totals overflow int64")
		}
		wantRows += f.Rows
		if f.Rows == 0 {
			continue
		}
		if !haveBounds {
			minTS, maxTS = f.MinTS, f.MaxTS
			haveBounds = true
			continue
		}
		if f.MinTS.Before(minTS) {
			minTS = f.MinTS
		}
		if f.MaxTS.After(maxTS) {
			maxTS = f.MaxTS
		}
	}

	if stats.MaxTS.Before(stats.MinTS) {
		return fmt.Errorf("timestamp range reversed: min %s after max %s", stats.MinTS, stats.MaxTS)
	}
	if stats.Rows < 0 || stats.DeduplicatedRows < 0 {
		return fmt.Errorf("rows: merged %d plus %d deduplicated must be nonnegative", stats.Rows, stats.DeduplicatedRows)
	}
	if stats.Rows > math.MaxInt64-stats.DeduplicatedRows {
		return fmt.Errorf("rows: merged %d plus %d deduplicated overflows int64", stats.Rows, stats.DeduplicatedRows)
	}
	// Dedup always retains the first row of an equal-timestamp run, so a
	// nonempty input can never legitimately produce zero rows even when the
	// claimed deduplicated count equals the whole input.
	if wantRows > 0 && stats.Rows == 0 {
		return fmt.Errorf("rows: merged zero rows from %d physical input rows", wantRows)
	}
	if accounted := stats.Rows + stats.DeduplicatedRows; accounted != wantRows {
		return fmt.Errorf("rows: merged %d plus %d deduplicated = %d, want %d from catalog inputs",
			stats.Rows, stats.DeduplicatedRows, accounted, wantRows)
	}
	if haveBounds && stats.MinTS.Before(minTS) {
		return fmt.Errorf("min ts: merged %s, before catalog inputs' min %s", stats.MinTS, minTS)
	}
	if haveBounds && stats.MaxTS.After(maxTS) {
		return fmt.Errorf("max ts: merged %s, after catalog inputs' max %s", stats.MaxTS, maxTS)
	}
	return nil
}

// collidesWith reports whether any of f's promoted columns differs only in
// case from a name in promoted.
func collidesWith(promoted map[string]struct{}, f catalog.File) bool {
	for name := range f.LabelValues {
		for p := range promoted {
			if schema.Collide(p, name) {
				return true
			}
		}
	}
	return false
}

// promotedUnion returns the sorted union of promoted label names over the
// inputs' catalog LabelValues. The promoted set comes from what the inputs
// actually track, not from ingest config.
func promotedUnion(batch []catalog.File) []string {
	set := make(map[string]struct{})
	for _, f := range batch {
		for name := range f.LabelValues {
			if name != "" && !schema.IsReserved(name) {
				set[name] = struct{}{}
			}
		}
	}
	return slices.Sorted(maps.Keys(set))
}
