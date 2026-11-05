package ingest

// Focused A3b coverage: real duplicate removal through storage.Merge on the
// upload path, exact source-row accounting in RecordMerge, lying merge results
// that must never publish, and retry-idempotent resumed output.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/storage"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// dedupRowKey formats one row's query-visible identity: timestamp, line,
// effective labels (the labels map plus the promoted level column), and
// structured metadata. Labels and metadata are sorted so map iteration order
// cannot affect equality.
func dedupRowKey(ts time.Time, line string, labels, meta map[string]string, level *string) string {
	parts := []string{fmt.Sprintf("ts=%d", ts.UnixMicro()), "line=" + line}
	for k, v := range labels {
		parts = append(parts, "l:"+k+"="+v)
	}
	if level != nil {
		parts = append(parts, "level="+*level)
	}
	for k, v := range meta {
		parts = append(parts, "m:"+k+"="+v)
	}
	slices.Sort(parts)
	return strings.Join(parts, "|")
}

// dedupRowSet returns the identities of rows read back from an object, sorted
// so assertions do not depend on the stable but key-tied input order of
// equal-timestamp survivors.
func dedupRowSet(f *uploadFixture, id tenant.ID, key string) []string {
	f.t.Helper()
	rows := f.readObject(id, key)
	out := make([]string, len(rows))
	for i, row := range rows {
		out[i] = dedupRowKey(row.ts, row.line, row.labels, row.meta, row.level)
	}
	slices.Sort(out)
	return out
}

// TestUploaderDedupAccountingPublishesUniqueRows merges two real source files
// through the uploader. Exact cross-input duplicates collapse, metadata-distinct
// rows at the same timestamp survive, the catalog records the physical output
// rows, complete summaries cover the survivors, and the sidecar describes
// exactly the stored lines.
func TestUploaderDedupAccountingPublishesUniqueRows(t *testing.T) {
	hour := time.Date(2026, 11, 4, 13, 0, 0, 0, time.UTC)
	for _, kind := range uploadCatalogKinds() {
		t.Run(string(kind), func(t *testing.T) {
			f := uploadNewFixtureKind(t, kind, nil)
			f.promote = func(string) []string { return []string{"level"} }

			f.seed(tenant.Default, "checkout", hour, []uploadRow{
				{ts: hour.Add(10 * time.Minute), line: "dup-line", labels: map[string]string{"app": "web"}, meta: map[string]string{"m": "1"}, level: uploadText("info")},
				{ts: hour.Add(20 * time.Minute), line: "only-first", labels: map[string]string{"app": "web"}},
			})
			f.seed(tenant.Default, "checkout", hour, []uploadRow{
				{ts: hour.Add(10 * time.Minute), line: "dup-line", labels: map[string]string{"app": "web"}, meta: map[string]string{"m": "1"}, level: uploadText("info")},
				{ts: hour.Add(10 * time.Minute), line: "dup-line", labels: map[string]string{"app": "web"}, meta: map[string]string{"m": "2"}, level: uploadText("info")},
				{ts: hour.Add(30 * time.Minute), line: "only-second", labels: map[string]string{"app": "db"}},
			})

			u := f.uploader(UploaderOptions{MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20})
			merges := 0
			realMerge := u.merge
			u.merge = func(ctx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error) {
				merges++
				return realMerge(ctx, w, inputs, promoted)
			}

			report, err := u.RunOnce(t.Context())
			if err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if merges != 1 {
				t.Fatalf("merge calls = %d, want 1", merges)
			}
			if report.Uploads != 1 || report.Completed[0].Sources != 2 || report.Completed[0].Rows != 4 {
				t.Fatalf("report = %+v, want one upload of 2 sources and 4 surviving rows", report)
			}

			files := f.liveCatalogFiles(tenant.Default)
			if len(files) != 1 {
				t.Fatalf("catalog rows = %d, want 1", len(files))
			}
			file := files[0]
			if file.Rows != 4 || report.Completed[0].Bytes != file.Bytes {
				t.Fatalf("catalog row = %+v, want 4 physical rows and the reported bytes", file)
			}
			if !file.LabelsComplete {
				t.Fatal("catalog row LabelsComplete = false, want the merge's complete summary")
			}
			if !file.HasSidecar {
				t.Fatal("catalog row HasSidecar = false after a successful sidecar Put")
			}
			if got, want := file.LabelValues["level"], []string{"", "info"}; !slices.Equal(got, want) {
				t.Fatalf("LabelValues[level] = %v, want %v", got, want)
			}
			// Only the surviving rows are query-visible, with their exact
			// timestamps, effective labels (including the promoted level) and
			// metadata: the duplicate is gone exactly once and the
			// metadata-distinct row remains.
			wantRows := []string{
				dedupRowKey(hour.Add(10*time.Minute), "dup-line", map[string]string{"app": "web"}, map[string]string{"m": "1"}, uploadText("info")),
				dedupRowKey(hour.Add(10*time.Minute), "dup-line", map[string]string{"app": "web"}, map[string]string{"m": "2"}, uploadText("info")),
				dedupRowKey(hour.Add(20*time.Minute), "only-first", map[string]string{"app": "web"}, map[string]string{}, nil),
				dedupRowKey(hour.Add(30*time.Minute), "only-second", map[string]string{"app": "db"}, map[string]string{}, nil),
			}
			slices.Sort(wantRows)
			if got := dedupRowSet(f, tenant.Default, file.Key); !slices.Equal(got, wantRows) {
				t.Fatalf("stored rows = %v, want %v", got, wantRows)
			}
			if rows := f.readObject(tenant.Default, file.Key); len(rows) != 4 {
				t.Fatalf("stored physical rows = %d, want 4", len(rows))
			}

			filter, err := index.Decode(uploadSidecarBytes(t, f, file.Key))
			if err != nil {
				t.Fatalf("Decode sidecar: %v", err)
			}
			for _, needle := range []string{"dup-line", "only-first", "only-second"} {
				if !filter.MayContain(needle) {
					t.Fatalf("sidecar MayContain(%q) = false, want true", needle)
				}
			}
			// Bloom filters may report false positives, so this fixed absent
			// probe is a fixture expectation for the current encoding, not
			// proof of exact membership. A false positive here is permitted.
			if filter.MayContain("never-a-surviving-line") {
				t.Fatal("fixed absent probe did not prune (unexpected for this fixture, but a Bloom false positive is valid)")
			}

			if got := f.receipts(tenant.Default); len(got) != 0 {
				t.Fatalf("receipts after publication = %+v, want none", got)
			}
		})
	}
}

// TestUploaderRecordMergeRejectsLyingDedupCounts injects a merge result that
// drops rows without a matching removed count, or reports a wrong, negative or
// overflowing one. RecordMerge must reject each at the row guard before any
// catalog AddFiles or object Put, leaving the receipt and local sources intact.
func TestUploaderRecordMergeRejectsLyingDedupCounts(t *testing.T) {
	hour := time.Date(2026, 11, 4, 14, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		// mutate changes the real merge's stats before RecordMerge sees them.
		mutate func(*storage.FileStats)
		// short merges only the first source, producing a genuinely short
		// output while reporting its own (smaller) stats.
		short   bool
		wantErr string
	}{
		{
			name:    "missing rows without removed count",
			mutate:  func(st *storage.FileStats) { st.Rows-- },
			wantErr: "deduplicated",
		},
		{
			name:    "removed count too large",
			mutate:  func(st *storage.FileStats) { st.DeduplicatedRows++ },
			wantErr: "deduplicated",
		},
		{
			name:    "negative removed count",
			mutate:  func(st *storage.FileStats) { st.DeduplicatedRows = -1 },
			wantErr: "nonnegative",
		},
		{
			name:    "overflowing removed count",
			mutate:  func(st *storage.FileStats) { st.DeduplicatedRows = math.MaxInt64 },
			wantErr: "overflows",
		},
		{
			name: "zero output claiming every row duplicated",
			mutate: func(st *storage.FileStats) {
				st.DeduplicatedRows = st.Rows + st.DeduplicatedRows
				st.Rows = 0
			},
			// RecordMerge's established invalid-stats guard rejects a zero
			// output before the new source-total guard; the helper's matching
			// branch is covered by TestCheckMergeRows.
			wantErr: "invalid stats",
		},
		{name: "merge over only some sources", short: true, wantErr: "deduplicated"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := uploadNewFixture(t)
			f.seed(tenant.Default, "checkout", hour, []uploadRow{
				{ts: hour.Add(10 * time.Minute), line: "a0"},
				{ts: hour.Add(20 * time.Minute), line: "a1"},
			})
			f.seed(tenant.Default, "checkout", hour, []uploadRow{
				{ts: hour.Add(30 * time.Minute), line: "b0"},
				{ts: hour.Add(40 * time.Minute), line: "b1"},
			})

			cat := &uploadCountingCatalog{Catalog: f.cat}
			puts := &uploadKeyListPut{Store: f.objects}
			u := f.uploader(UploaderOptions{
				Objects: puts, Catalog: cat, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
			})
			u.merge = func(ctx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error) {
				if tc.short {
					// A genuinely short output: the first source only.
					return storage.Merge(ctx, w, inputs[:1], promoted)
				}
				stats, err := storage.Merge(ctx, w, inputs, promoted)
				if err == nil {
					tc.mutate(&stats)
				}
				return stats, err
			}

			_, err := u.RunOnce(t.Context())
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("RunOnce error = %v, want the %q guard", err, tc.wantErr)
			}
			if cat.calls != 0 {
				t.Fatalf("AddFiles calls = %d, want 0", cat.calls)
			}
			if len(puts.keys) != 0 {
				t.Fatalf("Store.Put calls = %v, want none before a verified merge result", puts.keys)
			}
			if got := len(f.liveCatalogFiles(tenant.Default)); got != 0 {
				t.Fatalf("catalog rows = %d, want 0", got)
			}
			receipt := f.onlyReceipt(tenant.Default)
			if receipt.Stage != UploadReserved {
				t.Fatalf("receipt stage = %s, want reserved after a rejected merge", receipt.Stage)
			}
			if _, err := f.objects.Stat(t.Context(), receipt.Key); !errors.Is(err, objstore.ErrNotExist) {
				t.Fatalf("object %s after rejected merge = %v, want ErrNotExist", receipt.Key, err)
			}
			pending := mustPending(t, f.tenantStore(tenant.Default))
			if len(pending) != 2 {
				t.Fatalf("pending source files = %d, want 2", len(pending))
			}
			for _, src := range pending {
				if _, err := f.objects.Stat(t.Context(), objstore.DataKey(tenant.Default, src.Stream, src.Hour, src.ID)); !errors.Is(err, objstore.ErrNotExist) {
					t.Fatalf("rejected merge uploaded a source object: %v", err)
				}
				if _, err := os.Stat(f.localPath(tenant.Default, src)); err != nil {
					t.Fatalf("source %s removed by rejected merge: %v", src.ID, err)
				}
			}
		})
	}
}

// TestUploaderDedupResumeIsIdempotent fails the first catalog AddFiles after a
// real deduplicating merge, reopens the stores, and resumes. The finalized temp
// is reused without re-merging, the retry rotates to a fresh key (M2.1), the
// old bytes stay untouched, and the catalog publishes the same unique rows
// exactly once.
func TestUploaderDedupResumeIsIdempotent(t *testing.T) {
	f := uploadNewFixture(t)
	hour := time.Date(2026, 11, 4, 15, 0, 0, 0, time.UTC)
	f.seed(tenant.Default, "api", hour, []uploadRow{
		{ts: hour.Add(10 * time.Minute), line: "dup", labels: map[string]string{"app": "web"}, meta: map[string]string{"m": "1"}},
		{ts: hour.Add(20 * time.Minute), line: "keep-a"},
	})
	f.seed(tenant.Default, "api", hour, []uploadRow{
		{ts: hour.Add(10 * time.Minute), line: "dup", labels: map[string]string{"app": "web"}, meta: map[string]string{"m": "1"}},
		{ts: hour.Add(30 * time.Minute), line: "keep-b"},
	})

	catErr := errors.New("catalog unavailable")
	broken := &uploadFailingCatalog{Catalog: f.cat, err: catErr}
	firstPuts := &uploadKeyListPut{Store: f.objects}
	if _, err := f.uploader(UploaderOptions{
		Objects: firstPuts, Catalog: broken, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	}).RunOnce(t.Context()); !errors.Is(err, catErr) {
		t.Fatalf("first RunOnce error = %v, want %v", err, catErr)
	}
	receipt := f.onlyReceipt(tenant.Default)
	if receipt.Stage != UploadStored || receipt.Rows != 3 {
		t.Fatalf("receipt = %+v, want stored with 3 unique rows", receipt)
	}
	oldKey := receipt.Key
	oldBytes := uploadObjectBytes(t, f, oldKey)

	f.reopen()

	retryPuts := &uploadKeyListPut{Store: f.objects}
	u := f.uploader(UploaderOptions{
		Objects: retryPuts, MinBytes: 1, MaxAge: time.Hour, MergeMax: 1 << 20,
	})
	merges := 0
	realMerge := u.merge
	u.merge = func(ctx context.Context, w io.Writer, inputs []*storage.Reader, promoted []string) (storage.FileStats, error) {
		merges++
		return realMerge(ctx, w, inputs, promoted)
	}
	report, err := u.RunOnce(t.Context())
	if err != nil {
		t.Fatalf("resume RunOnce: %v", err)
	}
	if merges != 0 {
		t.Fatalf("resume merge calls = %d, want 0: the finalized dedup output is reused", merges)
	}
	if report.Uploads != 1 || !report.Completed[0].Resumed || report.Completed[0].Rows != 3 {
		t.Fatalf("resume report = %+v, want one resumed 3-row upload", report)
	}
	newKey := report.Completed[0].Key
	if newKey == oldKey {
		t.Fatalf("resume rewrote the stored key %q", oldKey)
	}
	if got := uploadObjectBytes(t, f, oldKey); !slices.Equal(got, oldBytes) {
		t.Fatal("resume rewrote the previously completed object")
	}
	dataKeys, _ := uploadSplitPutKeys(t, slices.Concat(firstPuts.keys, retryPuts.keys))
	if len(dataKeys) != 2 || dataKeys[0] != oldKey || dataKeys[1] != newKey {
		t.Fatalf("data Put keys = %v, want old %q then fresh %q", dataKeys, oldKey, newKey)
	}

	files := f.liveCatalogFiles(tenant.Default)
	if len(files) != 1 {
		t.Fatalf("catalog rows after resume = %d, want 1", len(files))
	}
	if files[0].Rows != 3 {
		t.Fatalf("published Rows = %d, want 3", files[0].Rows)
	}
	if rows := f.readObject(tenant.Default, files[0].Key); len(rows) != 3 {
		t.Fatalf("published physical rows = %d, want 3", len(rows))
	}
	if got := f.receipts(tenant.Default); len(got) != 0 {
		t.Fatalf("receipts after resume = %+v, want none", got)
	}
}

// TestCheckMergeRows covers the RecordMerge guard directly, including the
// legacy shape (no deduplicated rows) that must keep resuming.
func TestCheckMergeRows(t *testing.T) {
	src1, src2 := uuid.NewV7(), uuid.NewV7()
	files := []LocalFile{
		{ID: src1, Rows: 2},
		{ID: src2, Rows: 3},
	}
	sources := []uuid.UUID{src1, src2}
	other := uuid.NewV7()

	cases := []struct {
		name    string
		files   []LocalFile
		sources []uuid.UUID
		stats   storage.FileStats
		wantErr string
	}{
		{name: "legacy exact rows", files: files, sources: sources, stats: storage.FileStats{Rows: 5}},
		{name: "deduplicated exact rows", files: files, sources: sources, stats: storage.FileStats{Rows: 3, DeduplicatedRows: 2}},
		{
			name: "all duplicates collapse to one row", files: files, sources: sources,
			stats: storage.FileStats{Rows: 1, DeduplicatedRows: 4},
		},
		{name: "missing rows without removed count", files: files, sources: sources, stats: storage.FileStats{Rows: 4}, wantErr: "rows"},
		{name: "wrong removed count", files: files, sources: sources, stats: storage.FileStats{Rows: 4, DeduplicatedRows: 2}, wantErr: "rows"},
		{name: "negative removed count", files: files, sources: sources, stats: storage.FileStats{Rows: 6, DeduplicatedRows: -1}, wantErr: "nonnegative"},
		{name: "negative output rows", files: files, sources: sources, stats: storage.FileStats{Rows: -1, DeduplicatedRows: 6}, wantErr: "nonnegative"},
		{name: "output plus removed overflows", files: files, sources: sources, stats: storage.FileStats{Rows: 1, DeduplicatedRows: math.MaxInt64}, wantErr: "overflow"},
		{
			name: "zero output claiming every row duplicated", files: files, sources: sources,
			stats: storage.FileStats{Rows: 0, DeduplicatedRows: 5}, wantErr: "zero rows",
		},
		{
			name: "negative source rows", files: []LocalFile{{ID: src1, Rows: -1}}, sources: []uuid.UUID{src1},
			stats: storage.FileStats{Rows: 0}, wantErr: "source",
		},
		{
			name: "source rows overflow", files: []LocalFile{{ID: src1, Rows: math.MaxInt64}, {ID: src2, Rows: 1}},
			sources: sources, stats: storage.FileStats{Rows: math.MaxInt64}, wantErr: "overflow",
		},
		{
			name: "unknown source", files: files, sources: []uuid.UUID{src1, other},
			stats: storage.FileStats{Rows: 2}, wantErr: "source",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkMergeRows(tc.files, tc.sources, tc.stats)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("checkMergeRows: %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("checkMergeRows = %v, want error containing %q", err, tc.wantErr)
			}
		})
	}
}
