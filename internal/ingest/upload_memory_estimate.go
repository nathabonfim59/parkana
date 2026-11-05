package ingest

// Upload memory estimator. It plans one receipt's peak resident footprint
// before admission: the fan-in-bound decoder state of the merge readers, the
// footer-derived decoded batches, the output writer, the sidecar build and an
// allocation/GC allowance. It is a planning estimate, never a hard RSS cap:
// real peaks can exceed it on skewed rows, a very long equal-timestamp dedup
// run, foreign file metadata, allocator fragmentation or GC timing.
//
// The merge reader batch size comes from storage.MergeReaderOptions. The
// sidecar build keeps the default 8192-row reader, so it has its own batch and
// overhead terms. Other reader configurations are not covered by these
// constants.
//
// The constants below are calibrated from one 512MiB corpus at fan-in
// 16/32/64/128; the tests pin that arithmetic. A single sample is not proof of
// a universal memory bound.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"

	"github.com/apache/arrow-go/v18/parquet/file"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/storage"
)

const (
	// uploadEstimateSidecarBatchRows is the default reader batch used by
	// storage.BuildSidecar.
	uploadEstimateSidecarBatchRows int64 = 8192
	// uploadEstimateInputFixedBytes is the per-source decoder, dictionary,
	// page and buffered-stream allowance. The calibration arms measured
	// about 2.0-2.1MiB of input Arrow allocation per source.
	uploadEstimateInputFixedBytes int64 = 2 << 20
	// uploadEstimateOutputWriterBytes covers one output row group, the merge
	// batch slices and the concatenate/writer transient.
	uploadEstimateOutputWriterBytes int64 = 24 << 20
	// uploadEstimateSidecarOverheadBytes covers the sidecar reader's fixed
	// decoder state plus bloom/encode buffers.
	uploadEstimateSidecarOverheadBytes int64 = 8 << 20
	// uploadEstimateCleanupBytes covers a published receipt's cleanup, which
	// clones the manifest and removes files without opening any source.
	uploadEstimateCleanupBytes int64 = 16 << 20
	// uploadEstimateFooterMaxBytes bounds one source footer before the Parquet
	// reader allocates its metadata.
	uploadEstimateFooterMaxBytes int64 = 4 << 20
	// uploadEstimateFooterBudgetBytes bounds one receipt's total footer
	// preflight. The 128-source calibration corpus had about 1.1MiB of
	// footers.
	uploadEstimateFooterBudgetBytes int64 = 32 << 20
)

const (
	// uploadEstimateDecodeAmplification turns footer uncompressed column bytes
	// into an Arrow-decoded batch allowance (offsets, validity, maps and
	// alignment).
	uploadEstimateDecodeAmplification = 3.0
	// uploadEstimateGCSlack is the allocation/GC headroom multiplier.
	uploadEstimateGCSlack = 2.5
)

// newUploadMemoryModel returns a fresh estimator model. It is a function
// rather than a package variable so no mutable package state exists.
func newUploadMemoryModel() uploadMemoryModel {
	return uploadMemoryModel{
		SidecarBatchRows:     uploadEstimateSidecarBatchRows,
		InputFixedBytes:      uploadEstimateInputFixedBytes,
		OutputWriterBytes:    uploadEstimateOutputWriterBytes,
		SidecarOverheadBytes: uploadEstimateSidecarOverheadBytes,
		CleanupBytes:         uploadEstimateCleanupBytes,
		DecodeAmplification:  uploadEstimateDecodeAmplification,
		GCSlack:              uploadEstimateGCSlack,
	}
}

// uploadMemoryModel holds the estimator coefficients. A zero value is not a
// usable model; NewUploader installs newUploadMemoryModel().
type uploadMemoryModel struct {
	SidecarBatchRows     int64
	InputFixedBytes      int64
	OutputWriterBytes    int64
	SidecarOverheadBytes int64
	CleanupBytes         int64
	DecodeAmplification  float64
	GCSlack              float64
}

// uploadMemorySourceMeta is the footer data read from one Parquet file.
type uploadMemorySourceMeta struct {
	Rows                    int64
	RowGroups               int64
	CompressedBytes         int64
	UncompressedBytes       int64
	FooterBytes             int64
	MaxRowGroupUncompressed int64
	MaxRowGroupRows         int64
	// WorstRowBytes is max over row groups of ceil(groupUncompressed/groupRows),
	// falling back to the whole-file average only when no row group is usable.
	WorstRowBytes int64
}

// uploadMemorySourceSummary aggregates a merge's source footers.
type uploadMemorySourceSummary struct {
	FanIn             int
	Rows              int64
	CompressedBytes   int64
	UncompressedBytes int64
	FooterBytes       int64
	// DecodedBytes is the pre-amplification sum over sources of
	// min(mergeBatchRows, sourceRows) * sourceWorstRowBytes.
	DecodedBytes int64
	// WorstRowBytes is the largest per-source worst row-group average.
	WorstRowBytes int64
}

// uploadMemoryEstimate is one job's admission plan. Bytes == 0 means unknown,
// so the core charges the full budget; it is never a permanent rejection.
type uploadMemoryEstimate struct {
	Bytes       int64
	Known       bool
	Merge       bool
	FanIn       int
	SourceBytes int64
	SourceRows  int64
	OutputBytes int64
}

// uploadEstimateFile is the narrow file contract the footer survey needs. It
// is satisfied by *os.File; tests wrap it to prove bounded reads and closes.
type uploadEstimateFile interface {
	io.ReaderAt
	io.Seeker
	io.Closer
	Stat() (fs.FileInfo, error)
}

// uploadEstimateOpener opens one source or merge-output file. Production uses
// the read-only TenantStore root snapshot helpers (openEstimateSource and
// openEstimateTemp), not a localFileOps or ensureSynced path.
type uploadEstimateOpener func(context.Context, uuid.UUID) (uploadEstimateFile, error)

// estimateUploadMemory classifies a receipt's remaining work and returns its
// admission estimate. It returns an error only for context cancellation; every
// metadata problem becomes an unknown estimate so the actual job stage reports
// its own error and the receipt stays resumable.
func (u *Uploader) estimateUploadMemory(ctx context.Context, ts *TenantStore, up Upload) (uploadMemoryEstimate, error) {
	if err := ctx.Err(); err != nil {
		return uploadMemoryEstimate{}, err
	}
	switch up.Stage {
	case UploadPublished:
		// Cleanup only: never stat or open a source, because a partially
		// cleaned published receipt may already have removed some of them.
		return u.memoryModel.estimateCleanup(), nil
	case UploadReserved:
		return u.estimateMergeUpload(ctx, ts, up)
	case UploadMerged, UploadStored:
		size, err := estimateTempSize(ctx, ts, up.ID)
		switch {
		case err == nil && up.Bytes > 0 && size == up.Bytes:
			return u.estimateExistingTemp(ctx, ts, up)
		case err != nil && !errors.Is(err, fs.ErrNotExist):
			if ctxErr := ctx.Err(); ctxErr != nil {
				return uploadMemoryEstimate{}, ctxErr
			}
			return uploadMemoryEstimate{}, nil
		default:
			// Missing or stale temp: the next attempt re-merges the sources.
			return u.estimateMergeUpload(ctx, ts, up)
		}
	default:
		// An unknown stage is the stage machine's error to report.
		return uploadMemoryEstimate{}, nil
	}
}

// estimateMergeUpload surveys the live sources and plans a fresh merge.
func (u *Uploader) estimateMergeUpload(ctx context.Context, ts *TenantStore, up Upload) (uploadMemoryEstimate, error) {
	if len(up.Sources) == 0 {
		return uploadMemoryEstimate{}, nil
	}
	open := func(ctx context.Context, id uuid.UUID) (uploadEstimateFile, error) {
		return ts.openEstimateSource(ctx, id)
	}
	summary, ok, err := surveyUploadSources(ctx, up.Sources, open, uploadEstimateFooterBudgetBytes)
	if err != nil || !ok {
		return uploadMemoryEstimate{}, err
	}
	return u.memoryModel.estimateMerge(summary, u.indexParams), nil
}

// estimateExistingTemp reads only the finalized merge output's footer. The
// next attempt reuses the temp, so no source is opened.
func (u *Uploader) estimateExistingTemp(ctx context.Context, ts *TenantStore, up Upload) (uploadMemoryEstimate, error) {
	f, err := ts.openEstimateTemp(ctx, up.ID)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return uploadMemoryEstimate{}, ctxErr
		}
		return uploadMemoryEstimate{}, nil
	}
	return u.estimateFromFile(ctx, f)
}

// estimateTempSize returns the finalized temp size without completing a
// pending manifest directory sync. A missing temp reports fs.ErrNotExist; any
// other problem stays an unknown estimate in the caller.
func estimateTempSize(ctx context.Context, ts *TenantStore, id uuid.UUID) (int64, error) {
	f, err := ts.openEstimateTemp(ctx, id)
	if err != nil {
		return 0, err
	}
	info, statErr := f.Stat()
	closeErr := f.Close()
	if statErr != nil {
		return 0, statErr
	}
	if closeErr != nil {
		return 0, closeErr
	}
	return info.Size(), nil
}

// errUploadEstimateUncertain reports that the estimator refused to use state
// whose manifest-directory sync is still pending. The real processing path
// retries that sync; the estimate simply charges the full budget.
var errUploadEstimateUncertain = errors.New("ingest: upload memory estimate: source state is not durable")

// openEstimateSource snapshots one committed source's path under the tenant
// lock and opens it directly from the rooted filesystem. It deliberately does
// not call ensureSyncedLocked: estimate preflight is read-only and must never
// complete or retry a pending manifest/directory sync on behalf of the real
// processing path. A pending sync makes the snapshot uncertain, so the
// estimate becomes unknown and the actual stage decides.
func (t *TenantStore) openEstimateSource(ctx context.Context, id uuid.UUID) (uploadEstimateFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.mu.Lock()
	if t.closed.Load() {
		t.mu.Unlock()
		return nil, ErrLocalClosed
	}
	if t.pendingSync {
		t.mu.Unlock()
		return nil, errUploadEstimateUncertain
	}
	i := findFile(t.state.Files, id)
	if i < 0 {
		t.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, id)
	}
	rel := t.state.Files[i].RelPath()
	t.mu.Unlock()
	f, err := t.root.Open(rel)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrFileMissing, rel, err)
	}
	return f, nil
}

// openEstimateTemp snapshots a receipt's temp path under the tenant lock and
// opens it directly, with the same read-only/no-sync rule as
// openEstimateSource.
func (t *TenantStore) openEstimateTemp(ctx context.Context, id uuid.UUID) (uploadEstimateFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.mu.Lock()
	if t.closed.Load() {
		t.mu.Unlock()
		return nil, ErrLocalClosed
	}
	if t.pendingSync {
		t.mu.Unlock()
		return nil, errUploadEstimateUncertain
	}
	if findUpload(t.state.Uploads, id) < 0 {
		t.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", ErrUploadNotFound, id)
	}
	t.mu.Unlock()
	f, err := t.root.Open(uploadTempName(id))
	if err != nil {
		return nil, fmt.Errorf("ingest: open upload %s temp: %w", id, err)
	}
	return f, nil
}

// estimateFromFile reads one finalized output's footer and plans the put and
// sidecar work. It always closes f.
func (u *Uploader) estimateFromFile(ctx context.Context, f uploadEstimateFile) (uploadMemoryEstimate, error) {
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return uploadMemoryEstimate{}, ctxErr
		}
		return uploadMemoryEstimate{}, nil
	}
	meta, _, ok := readUploadSourceMeta(f, info.Size(), uploadEstimateFooterBudgetBytes)
	closeErr := f.Close()
	if err := ctx.Err(); err != nil {
		return uploadMemoryEstimate{}, err
	}
	if closeErr != nil || !ok {
		return uploadMemoryEstimate{}, nil
	}
	est := u.memoryModel.estimatePutOnly(meta, u.indexParams)
	est.OutputBytes = info.Size()
	return est, nil
}

// surveyUploadSources reads every source's footer sequentially and aggregates
// the metadata. It returns ok=false for any metadata problem (the real stage
// then reports its own error) and an error only for context cancellation.
func surveyUploadSources(ctx context.Context, ids []uuid.UUID, open uploadEstimateOpener, footerBudget int64) (uploadMemorySourceSummary, bool, error) {
	var summary uploadMemorySourceSummary
	remaining := footerBudget
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return uploadMemorySourceSummary{}, false, err
		}
		f, err := open(ctx, id)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return uploadMemorySourceSummary{}, false, ctxErr
			}
			return uploadMemorySourceSummary{}, false, nil
		}
		info, statErr := f.Stat()
		if statErr != nil {
			_ = f.Close()
			if ctxErr := ctx.Err(); ctxErr != nil {
				return uploadMemorySourceSummary{}, false, ctxErr
			}
			return uploadMemorySourceSummary{}, false, nil
		}
		meta, used, ok := readUploadSourceMeta(f, info.Size(), remaining)
		closeErr := f.Close()
		if err := ctx.Err(); err != nil {
			return uploadMemorySourceSummary{}, false, err
		}
		if closeErr != nil || !ok {
			return uploadMemorySourceSummary{}, false, nil
		}
		remaining = uploadMemorySub(remaining, used)
		summary.FanIn++
		summary.Rows = uploadMemoryAdd(summary.Rows, meta.Rows)
		summary.CompressedBytes = uploadMemoryAdd(summary.CompressedBytes, meta.CompressedBytes)
		summary.UncompressedBytes = uploadMemoryAdd(summary.UncompressedBytes, meta.UncompressedBytes)
		summary.FooterBytes = uploadMemoryAdd(summary.FooterBytes, meta.FooterBytes)
		summary.WorstRowBytes = max(summary.WorstRowBytes, meta.WorstRowBytes)
		batchRows := min(int64(storage.MergeReaderOptions().BatchSize), meta.Rows)
		summary.DecodedBytes = uploadMemoryAdd(summary.DecodedBytes, uploadMemoryMul(batchRows, meta.WorstRowBytes))
	}
	if summary.FanIn == 0 || summary.Rows <= 0 || summary.UncompressedBytes <= 0 {
		return uploadMemorySourceSummary{}, false, nil
	}
	return summary, true, nil
}

// uploadMemoryRowGroup is the per-row-group input to the worst-average helper.
type uploadMemoryRowGroup struct {
	Rows              int64
	UncompressedBytes int64
}

// uploadMemoryWorstRowBytes returns max over row groups of
// ceil(groupUncompressed/groupRows), falling back to the whole-file average
// only when no group has rows. It deliberately does not divide the largest
// group bytes by the largest group rows: those maxima can come from different
// groups and would invent a data shape that never existed.
func uploadMemoryWorstRowBytes(groups []uploadMemoryRowGroup) int64 {
	var worst, totalBytes, totalRows int64
	for _, g := range groups {
		totalBytes = uploadMemoryAdd(totalBytes, g.UncompressedBytes)
		totalRows = uploadMemoryAdd(totalRows, g.Rows)
		if g.Rows > 0 {
			worst = max(worst, uploadMemoryCeilDiv(g.UncompressedBytes, g.Rows))
		}
	}
	if worst <= 0 && totalRows > 0 {
		worst = uploadMemoryCeilDiv(totalBytes, totalRows)
	}
	return worst
}

// readUploadSourceMeta reads one Parquet footer after checking its declared
// length against both the per-file cap and the receipt's remaining budget. The
// caller owns f and must close it.
func readUploadSourceMeta(f uploadEstimateFile, size, remaining int64) (uploadMemorySourceMeta, int64, bool) {
	if f == nil || size < 8 || remaining < 8 {
		return uploadMemorySourceMeta{}, 0, false
	}
	var trailer [8]byte
	if _, err := f.ReadAt(trailer[:], size-8); err != nil {
		return uploadMemorySourceMeta{}, 0, false
	}
	if string(trailer[4:]) != "PAR1" {
		return uploadMemorySourceMeta{}, 0, false
	}
	footerLen := int64(binary.LittleEndian.Uint32(trailer[:4]))
	if footerLen <= 0 || footerLen > size-8 || footerLen > uploadEstimateFooterMaxBytes || footerLen > remaining-8 {
		return uploadMemorySourceMeta{}, 0, false
	}
	pf, err := file.NewParquetReader(io.NewSectionReader(f, 0, size))
	if err != nil {
		return uploadMemorySourceMeta{}, 0, false
	}
	defer pf.Close()
	md := pf.MetaData()
	if md == nil {
		return uploadMemorySourceMeta{}, 0, false
	}
	used := int64(md.Size())
	if used <= 0 || used > remaining {
		return uploadMemorySourceMeta{}, 0, false
	}
	meta := uploadMemorySourceMeta{RowGroups: int64(md.NumRowGroups()), FooterBytes: used}
	groups := make([]uploadMemoryRowGroup, 0, md.NumRowGroups())
	for i := range md.NumRowGroups() {
		rg := md.RowGroup(i)
		rows := rg.NumRows()
		uncompressed := rg.TotalByteSize()
		compressed := rg.TotalCompressedSize()
		if rows < 0 || uncompressed < 0 || compressed < 0 {
			return uploadMemorySourceMeta{}, 0, false
		}
		meta.Rows = uploadMemoryAdd(meta.Rows, rows)
		meta.CompressedBytes = uploadMemoryAdd(meta.CompressedBytes, compressed)
		meta.UncompressedBytes = uploadMemoryAdd(meta.UncompressedBytes, uncompressed)
		meta.MaxRowGroupUncompressed = max(meta.MaxRowGroupUncompressed, uncompressed)
		meta.MaxRowGroupRows = max(meta.MaxRowGroupRows, rows)
		groups = append(groups, uploadMemoryRowGroup{Rows: rows, UncompressedBytes: uncompressed})
	}
	meta.WorstRowBytes = uploadMemoryWorstRowBytes(groups)
	if meta.Rows <= 0 || meta.UncompressedBytes <= 0 {
		return uploadMemorySourceMeta{}, 0, false
	}
	return meta, used, true
}

// estimateCleanup plans a published receipt's cleanup work.
func (m uploadMemoryModel) estimateCleanup() uploadMemoryEstimate {
	return uploadMemoryEstimate{Bytes: m.CleanupBytes, Known: true}
}

// estimateMerge plans a fresh merge plus its output writer and sidecar build.
func (m uploadMemoryModel) estimateMerge(s uploadMemorySourceSummary, params index.Params) uploadMemoryEstimate {
	input := uploadMemoryMul(int64(s.FanIn), m.InputFixedBytes)
	input = uploadMemoryAdd(input, uploadMemoryMulFloat(s.DecodedBytes, m.DecodeAmplification))
	sidecar := m.estimateSidecar(s.UncompressedBytes, s.Rows, s.WorstRowBytes, params)
	live := uploadMemoryAdd(input, m.OutputWriterBytes)
	live = uploadMemoryAdd(live, sidecar)
	return uploadMemoryEstimate{
		Bytes:       uploadMemoryMulFloat(live, m.GCSlack),
		Known:       true,
		Merge:       true,
		FanIn:       s.FanIn,
		SourceBytes: s.CompressedBytes,
		SourceRows:  s.Rows,
	}
}

// estimatePutOnly plans the sidecar build for an already-finalized temp.
func (m uploadMemoryModel) estimatePutOnly(out uploadMemorySourceMeta, params index.Params) uploadMemoryEstimate {
	sidecar := m.estimateSidecar(out.UncompressedBytes, out.Rows, out.WorstRowBytes, params)
	return uploadMemoryEstimate{
		Bytes:      uploadMemoryMulFloat(sidecar, m.GCSlack),
		Known:      true,
		FanIn:      1,
		SourceRows: out.Rows,
	}
}

// estimateSidecar bounds the sidecar build: its gram set transient, its fixed
// reader/encode state and one 8192-row output decode.
func (m uploadMemoryModel) estimateSidecar(uncompressedBytes, rows, worstRowBytes int64, params index.Params) int64 {
	grams := min(int64(params.MaxGrams), max(uncompressedBytes, 0))
	sidecar := uploadMemoryGramBytes(grams, int64(params.MaxGrams))
	sidecar = uploadMemoryAdd(sidecar, m.SidecarOverheadBytes)
	batchRows := min(m.SidecarBatchRows, max(rows, 0))
	batch := uploadMemoryMul(batchRows, max(worstRowBytes, 0))
	return uploadMemoryAdd(sidecar, uploadMemoryMulFloat(batch, m.DecodeAmplification))
}

// uploadMemoryGramBytes is the conservative peak for the sidecar gram set. The
// open-addressing table holds at most MaxGrams entries at 0.5 load (8 bytes per
// slot). The (MaxGrams+1)th insertion grows before the cap resets the set, so
// the transient peak is the old table plus its doubled replacement: 3x the
// steady table. This is a bound for this component only; the model's GC slack
// is applied afterwards.
func uploadMemoryGramBytes(grams, maxGrams int64) int64 {
	if grams <= 0 || maxGrams <= 0 {
		return 0
	}
	grams = min(grams, maxGrams)
	slots := uploadMemoryNextPow2(max(int64(4096), uploadMemoryMul(grams, 2)))
	old := uploadMemoryMul(slots, 8)
	return uploadMemoryAdd(old, uploadMemoryMul(old, 2))
}

// uploadMemoryAdd returns a+b saturating at MaxInt64.
func uploadMemoryAdd(a, b int64) int64 {
	if a < 0 || b < 0 {
		return 0
	}
	if b > math.MaxInt64-a {
		return math.MaxInt64
	}
	return a + b
}

// uploadMemorySub returns a-b saturating at zero.
func uploadMemorySub(a, b int64) int64 {
	if b >= a {
		return 0
	}
	return a - b
}

// uploadMemoryMul returns a*b saturating at MaxInt64.
func uploadMemoryMul(a, b int64) int64 {
	if a <= 0 || b <= 0 {
		return 0
	}
	if a > math.MaxInt64/b {
		return math.MaxInt64
	}
	return a * b
}

// uploadMemoryMulFloat returns ceil(a*f) saturating at MaxInt64.
func uploadMemoryMulFloat(a int64, f float64) int64 {
	if a <= 0 || f <= 0 || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	if f > float64(math.MaxInt64)/float64(a) {
		return math.MaxInt64
	}
	v := math.Ceil(float64(a) * f)
	if v >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(v)
}

// uploadMemoryCeilDiv returns ceil(a/b) for nonnegative a and positive b.
func uploadMemoryCeilDiv(a, b int64) int64 {
	if a <= 0 {
		return 0
	}
	if b <= 0 {
		return math.MaxInt64
	}
	q, r := a/b, a%b
	if r != 0 {
		q++
	}
	return q
}

// uploadMemoryNextPow2 returns the smallest power of two >= n, saturating at
// MaxInt64.
func uploadMemoryNextPow2(n int64) int64 {
	if n <= 1 {
		return 1
	}
	slots := int64(1)
	for slots < n {
		if slots > math.MaxInt64/2 {
			return math.MaxInt64
		}
		slots *= 2
	}
	return slots
}
