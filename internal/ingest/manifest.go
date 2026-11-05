package ingest

// This file defines the per-tenant durable local-publication state: one
// versioned manifest.json per tenant under <data.dir>/parquet/<tenant>/.
// The manifest is the sole authority for the durable WAL checkpoint
// (frontier), the committed local Parquet files awaiting upload, and the
// upload reservation/retirement receipts. Every mutation is committed as one
// atomic snapshot: write manifest.json.tmp, fsync, rename over manifest.json,
// fsync the tenant directory. There is no separate watermark to race.

import (
	"cmp"
	"encoding/json/v2"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	// manifestVersion is the tenantManifest format version.
	manifestVersion = 1
	// manifestName is the committed per-tenant manifest file name.
	manifestName = "manifest.json"
	// manifestTempName is the in-progress manifest write.
	manifestTempName = manifestName + ".tmp"
	// parquetExt is the extension of committed local Parquet files.
	parquetExt = ".parquet"
	// tempSuffix marks an uncommitted file write; such files are quarantined
	// during recovery.
	tempSuffix = ".tmp"
	// uploadsDir holds finalized upload merge outputs under the tenant root.
	// The leading dot cannot collide with a sanitized stream name.
	uploadsDir = ".uploads"
	// quarantineDir receives unreferenced or unexpected files during recovery.
	quarantineDir = ".quarantine"
)

var (
	// ErrManifestCorrupt reports invalid or inconsistent manifest content.
	ErrManifestCorrupt = errors.New("ingest: corrupt local manifest")
	// ErrManifestVersion reports a manifest version this build cannot read.
	ErrManifestVersion = errors.New("ingest: unsupported local manifest version")
	// ErrGenerationGap reports a generation frontier that skips sequences.
	ErrGenerationGap = errors.New("ingest: generation frontier gap")
	// ErrGenerationOverlap reports a generation frontier partially covered by
	// the committed checkpoint.
	ErrGenerationOverlap = errors.New("ingest: generation frontier overlap")
	// ErrFileMissing reports a manifest-referenced live file absent from disk.
	ErrFileMissing = errors.New("ingest: manifest file missing")
	// ErrFileNotFound reports an unknown local file id.
	ErrFileNotFound = errors.New("ingest: local file not found")
	// ErrUploadNotFound reports an unknown upload receipt id.
	ErrUploadNotFound = errors.New("ingest: upload not found")
	// ErrUploadConflict reports an upload reservation or source that collides
	// with existing durable state.
	ErrUploadConflict = errors.New("ingest: upload conflicts with durable state")
	// ErrUploadStage reports an illegal upload stage transition.
	ErrUploadStage = errors.New("ingest: illegal upload stage transition")
	// ErrNotRetired reports an operation allowed only on retired files.
	ErrNotRetired = errors.New("ingest: file is not retired")
	// ErrTenantMismatch reports data belonging to another tenant.
	ErrTenantMismatch = errors.New("ingest: tenant mismatch")
	// ErrKeyRotationOverflow reports a key-rotation counter that cannot be
	// incremented without wrapping.
	ErrKeyRotationOverflow = errors.New("ingest: key rotation counter overflow")
)

// Frontier is the tenant WAL sequence range of one generation. Frontier{1, N}
// is the first publishable range; the committed frontier is the durable
// checkpoint every replay and reclaim decision is based on.
type Frontier struct {
	First uint64 `json:"first"`
	Last  uint64 `json:"last"`
}

// LocalFile is one committed local Parquet file: every row of one stream for
// one UTC hour. Its rooted relative path is derived from Stream/Hour/ID and is
// never stored. Retired marks a file whose upload committed; it awaits
// removal by CompleteUpload.
type LocalFile struct {
	Tenant tenant.ID `json:"tenant"`
	ID     uuid.UUID `json:"id"`
	Stream string    `json:"stream"`
	// Hour is the UTC partition start (truncated to the hour).
	Hour time.Time `json:"hour"`
	// Bytes is the full file size including the Parquet footer.
	Bytes       int64               `json:"bytes"`
	Rows        int64               `json:"rows"`
	MinTS       time.Time           `json:"minTS"`
	MaxTS       time.Time           `json:"maxTS"`
	LabelValues map[string][]string `json:"labelValues,omitempty"`
	// LabelsComplete reports that LabelValues is the complete promoted-label
	// summary storage.Writer computed. It is false for files committed by
	// builds that predate complete summaries; such a file is never relabeled
	// complete without recomputing it.
	LabelsComplete bool `json:"labelsComplete,omitzero"`
	// PublishedAt is when the file's generation was committed to the
	// manifest. Upload age limits compare against this, not MinTS.
	PublishedAt time.Time `json:"publishedAt"`
	// Retired marks a file uploaded through a published receipt; it is
	// excluded from new batches and awaits removal by CompleteUpload.
	Retired bool `json:"retired,omitempty"`
}

// RelPath returns the file's path relative to the tenant root:
// <stream>/<yyyy>/<mm>/<dd>/<hh>/<id>.parquet.
func (f LocalFile) RelPath() string {
	return localFileRelPath(f.Stream, f.Hour, f.ID)
}

// Clone returns a deep copy of f.
func (f LocalFile) Clone() LocalFile {
	return LocalFile{
		Tenant: f.Tenant, ID: f.ID, Stream: f.Stream, Hour: f.Hour,
		Bytes: f.Bytes, Rows: f.Rows, MinTS: f.MinTS, MaxTS: f.MaxTS,
		LabelValues: cloneLabelValues(f.LabelValues), LabelsComplete: f.LabelsComplete,
		Retired: f.Retired, PublishedAt: f.PublishedAt,
	}
}

// UploadStage is the durable progress marker of an upload receipt. Stages
// advance forward only: reserved → merged → stored → published, after which
// CompleteUpload drops the receipt.
type UploadStage string

// Upload stages.
const (
	// UploadReserved records the upload identity (object key and sources)
	// before any merge or object write.
	UploadReserved UploadStage = "reserved"
	// UploadMerged records that the merged bytes and stats are durable in the
	// finalized upload temp.
	UploadMerged UploadStage = "merged"
	// UploadStored records that Store.Put returned success. It is a retry
	// hint, not proof the object still exists: orphan GC may have removed it,
	// so the uploader must rotate to a fresh key and Put before
	// Catalog.AddFiles.
	UploadStored UploadStage = "stored"
	// UploadPublished records that the catalog rows are committed and the
	// source files are retired; only cleanup remains.
	UploadPublished UploadStage = "published"
)

// Upload is a durable upload reservation/receipt. The original object Key is
// derived from Tenant/Stream/Hour/ID via objstore.DataKey; RefreshUploadKey
// replaces it with a fresh canonical key before any retry Puts a receipt that
// may already name a stored object, so a data or sidecar key is never written
// twice. Temp is the finalized merge output path relative to the tenant root,
// set by RecordMerge.
type Upload struct {
	Tenant tenant.ID `json:"tenant"`
	ID     uuid.UUID `json:"id"`
	Stream string    `json:"stream"`
	// Hour is the UTC partition start (truncated to the hour). Uploads never
	// merge across streams or hours.
	Hour time.Time `json:"hour"`
	// Key is the object key the merged temp is Put under. The original
	// reservation derives it as objstore.DataKey(tenant, stream, hour, ID),
	// but RefreshUploadKey durably rekeys a stale or unverifiable receipt
	// with a fresh UUIDv7 while retaining the receipt ID, so Key's UUID is
	// not necessarily the receipt ID.
	Key     string      `json:"key"`
	Sources []uuid.UUID `json:"sources"`
	Temp    string      `json:"temp,omitempty"`
	Stage   UploadStage `json:"stage"`
	// Rows, Bytes, MinTS, MaxTS and LabelValues are the merged file's stats,
	// recorded by RecordMerge for the catalog row.
	Rows        int64               `json:"rows,omitempty"`
	Bytes       int64               `json:"bytes,omitempty"`
	MinTS       time.Time           `json:"minTS,omitzero"`
	MaxTS       time.Time           `json:"maxTS,omitzero"`
	LabelValues map[string][]string `json:"labelValues,omitempty"`
	// LabelsComplete is the merged file's summary completeness, recorded by
	// RecordMerge for the catalog row. A resume reuses the durable merge
	// output, so this value (true only when the merge's Writer closed
	// successfully) survives reopen; receipts written before the field existed
	// decode false and are never upgraded without a recomputation.
	LabelsComplete bool `json:"labelsComplete,omitzero"`
	// CreatedAt is stamped when the reservation commits and stays stable
	// across retries, so it is reused for catalog.File.CreatedAt.
	CreatedAt time.Time `json:"createdAt"`
	// PutCompletedAt is stamped by RecordPut when the object store Put
	// completed for the CURRENT key. A nonzero value proves that key was
	// already Put, and zero means the Put outcome is unknown (a merge never
	// Put, or an unacknowledged Put). Either way the uploader rotates the key
	// before any later Put and never writes the current key twice; the time
	// also bounds how long the resulting object may wait for
	// Catalog.AddFiles before the orphan GC could collect it.
	PutCompletedAt time.Time `json:"putCompletedAt,omitzero"`
}

// Clone returns a deep copy of u.
func (u Upload) Clone() Upload {
	return Upload{
		Tenant: u.Tenant, ID: u.ID, Stream: u.Stream, Hour: u.Hour, Key: u.Key,
		Sources: slices.Clone(u.Sources), Temp: u.Temp, Stage: u.Stage,
		Rows: u.Rows, Bytes: u.Bytes, MinTS: u.MinTS, MaxTS: u.MaxTS,
		LabelValues: cloneLabelValues(u.LabelValues), LabelsComplete: u.LabelsComplete,
		CreatedAt: u.CreatedAt, PutCompletedAt: u.PutCompletedAt,
	}
}

// tenantManifest is the versioned on-disk snapshot for one tenant.
type tenantManifest struct {
	Version int       `json:"version"`
	Seq     uint64    `json:"seq"`
	Tenant  tenant.ID `json:"tenant"`
	// KeyRotations counts every durable object-key rotation (RefreshUploadKey)
	// this tenant has committed. It is cumulative: upload cleanup, restarts and
	// reconciliation never reset it, and manifests written before the field
	// existed decode as zero.
	KeyRotations uint64      `json:"keyRotations,omitzero"`
	Frontier     Frontier    `json:"frontier"`
	Files        []LocalFile `json:"files"`
	Uploads      []Upload    `json:"uploads"`
}

func newManifest(id tenant.ID) tenantManifest {
	return tenantManifest{Version: manifestVersion, Tenant: id}
}

func encodeManifest(m tenantManifest) ([]byte, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("ingest: encode manifest: %w", err)
	}
	return data, nil
}

// decodeManifest parses and fully validates a manifest for the tenant carried
// by the directory it lives in.
func decodeManifest(data []byte, id tenant.ID) (tenantManifest, error) {
	var m tenantManifest
	if err := json.Unmarshal(data, &m, json.RejectUnknownMembers(true)); err != nil {
		return tenantManifest{}, fmt.Errorf("%w: %v", ErrManifestCorrupt, err)
	}
	if m.Version != manifestVersion {
		return tenantManifest{}, fmt.Errorf("%w: %d", ErrManifestVersion, m.Version)
	}
	if m.Tenant != id {
		return tenantManifest{}, fmt.Errorf("%w: manifest for %q in directory for %q", ErrManifestCorrupt, m.Tenant, id)
	}
	if err := validateManifest(m); err != nil {
		return tenantManifest{}, err
	}
	return m, nil
}

// validateManifest checks the structural invariants of a decoded or candidate
// manifest, including referential integrity between uploads and files.
func validateManifest(m tenantManifest) error {
	if m.Seq == ^uint64(0) {
		return fmt.Errorf("%w: sequence overflow", ErrManifestCorrupt)
	}
	switch {
	case m.Frontier == (Frontier{}):
	case m.Frontier.First == 0 || m.Frontier.Last == 0:
		return fmt.Errorf("%w: frontier %+v has a zero bound", ErrManifestCorrupt, m.Frontier)
	case m.Frontier.First > m.Frontier.Last:
		return fmt.Errorf("%w: frontier first %d > last %d", ErrManifestCorrupt, m.Frontier.First, m.Frontier.Last)
	}
	// Files and receipts can only exist behind a committed frontier;
	// otherwise replay would duplicate their rows.
	if m.Frontier == (Frontier{}) && (len(m.Files) > 0 || len(m.Uploads) > 0) {
		return fmt.Errorf("%w: %d files and %d uploads without a frontier", ErrManifestCorrupt, len(m.Files), len(m.Uploads))
	}
	files := make(map[uuid.UUID]*LocalFile, len(m.Files))
	for i := range m.Files {
		f := &m.Files[i]
		if err := f.validate(m.Tenant); err != nil {
			return err
		}
		if _, dup := files[f.ID]; dup {
			return fmt.Errorf("%w: duplicate file id %s", ErrManifestCorrupt, f.ID)
		}
		files[f.ID] = f
	}
	uploads := make(map[uuid.UUID]bool, len(m.Uploads))
	reserved := make(map[uuid.UUID]uuid.UUID) // source file id → upload id
	for i := range m.Uploads {
		u := &m.Uploads[i]
		if err := u.validate(m.Tenant); err != nil {
			return err
		}
		if uploads[u.ID] {
			return fmt.Errorf("%w: duplicate upload id %s", ErrManifestCorrupt, u.ID)
		}
		uploads[u.ID] = true
		for _, src := range u.Sources {
			srcFile, ok := files[src]
			if !ok {
				return fmt.Errorf("%w: upload %s references unknown file %s", ErrManifestCorrupt, u.ID, src)
			}
			if srcFile.Stream != u.Stream || !srcFile.Hour.Equal(u.Hour) {
				return fmt.Errorf("%w: upload %s mixes stream/hour with file %s", ErrManifestCorrupt, u.ID, src)
			}
			if other, dup := reserved[src]; dup {
				return fmt.Errorf("%w: file %s is a source of uploads %s and %s", ErrManifestCorrupt, src, other, u.ID)
			}
			reserved[src] = u.ID
			// Retirement commits atomically with publication, so a retired
			// source on an unpublished receipt (or a live one on a published
			// receipt) is corrupt.
			if u.Stage == UploadPublished && !srcFile.Retired {
				return fmt.Errorf("%w: published upload %s has unretired source %s", ErrManifestCorrupt, u.ID, src)
			}
			if u.Stage != UploadPublished && srcFile.Retired {
				return fmt.Errorf("%w: %s upload %s has retired source %s", ErrManifestCorrupt, u.Stage, u.ID, src)
			}
		}
	}
	return nil
}

func (f *LocalFile) validate(id tenant.ID) error {
	if f.Tenant != id {
		return fmt.Errorf("%w: file %s belongs to %q: %w", ErrManifestCorrupt, f.ID, f.Tenant, ErrTenantMismatch)
	}
	if f.ID == uuid.Nil() {
		return fmt.Errorf("%w: file with nil id", ErrManifestCorrupt)
	}
	if err := validateStreamName(f.Stream); err != nil {
		return err
	}
	if err := validateHour(f.Hour); err != nil {
		return err
	}
	if f.Rows <= 0 || f.Bytes <= 0 {
		return fmt.Errorf("%w: file %s has rows=%d bytes=%d", ErrManifestCorrupt, f.ID, f.Rows, f.Bytes)
	}
	if f.MaxTS.Before(f.MinTS) {
		return fmt.Errorf("%w: file %s minTS after maxTS", ErrManifestCorrupt, f.ID)
	}
	end := f.Hour.Add(time.Hour)
	if f.MinTS.Before(f.Hour) || !f.MaxTS.Before(end) {
		return fmt.Errorf("%w: file %s stats outside hour %s", ErrManifestCorrupt, f.ID, f.Hour)
	}
	if f.PublishedAt.IsZero() {
		return fmt.Errorf("%w: file %s has no publication time", ErrManifestCorrupt, f.ID)
	}
	return validateLabelValues(f.LabelValues, f.ID.String())
}

func (u *Upload) validate(id tenant.ID) error {
	if u.Tenant != id {
		return fmt.Errorf("%w: upload %s belongs to %q: %w", ErrManifestCorrupt, u.ID, u.Tenant, ErrTenantMismatch)
	}
	if u.ID == uuid.Nil() {
		return fmt.Errorf("%w: upload with nil id", ErrManifestCorrupt)
	}
	if err := validateStreamName(u.Stream); err != nil {
		return err
	}
	if err := validateHour(u.Hour); err != nil {
		return err
	}
	// The key is validated against the canonical layout, not the receipt ID:
	// RefreshUploadKey rekeys a receipt with a fresh UUIDv7 for retry.
	if err := checkDataKey(u.Tenant, u.Stream, u.Hour, u.Key); err != nil {
		return fmt.Errorf("%w: upload %s: %v", ErrManifestCorrupt, u.ID, err)
	}
	switch u.Stage {
	case UploadReserved:
		if u.Temp != "" || u.Rows != 0 || u.Bytes != 0 || !u.MinTS.IsZero() || !u.MaxTS.IsZero() ||
			u.LabelValues != nil || u.LabelsComplete {
			return fmt.Errorf("%w: reserved upload %s carries merge output", ErrManifestCorrupt, u.ID)
		}
	case UploadMerged, UploadStored, UploadPublished:
		if u.Temp != uploadTempName(u.ID) {
			return fmt.Errorf("%w: %s upload %s temp %q", ErrManifestCorrupt, u.Stage, u.ID, u.Temp)
		}
		if u.Rows <= 0 || u.Bytes <= 0 {
			return fmt.Errorf("%w: %s upload %s has rows=%d bytes=%d", ErrManifestCorrupt, u.Stage, u.ID, u.Rows, u.Bytes)
		}
		end := u.Hour.Add(time.Hour)
		if u.MaxTS.Before(u.MinTS) || u.MinTS.Before(u.Hour) || !u.MaxTS.Before(end) {
			return fmt.Errorf("%w: %s upload %s stats outside hour %s", ErrManifestCorrupt, u.Stage, u.ID, u.Hour)
		}
	default:
		return fmt.Errorf("%w: upload %s stage %q", ErrManifestCorrupt, u.ID, u.Stage)
	}
	// A Put completion only exists once the receipt reached stored; merged
	// receipts and fresh RefreshUploadKey results must carry zero.
	if u.Stage != UploadStored && u.Stage != UploadPublished && !u.PutCompletedAt.IsZero() {
		return fmt.Errorf("%w: %s upload %s carries a Put completion before stored", ErrManifestCorrupt, u.Stage, u.ID)
	}
	if len(u.Sources) == 0 {
		return fmt.Errorf("%w: upload %s has no sources", ErrManifestCorrupt, u.ID)
	}
	for i, src := range u.Sources {
		if src == uuid.Nil() {
			return fmt.Errorf("%w: upload %s has nil source", ErrManifestCorrupt, u.ID)
		}
		if slices.Contains(u.Sources[:i], src) {
			return fmt.Errorf("%w: upload %s repeats source %s", ErrManifestCorrupt, u.ID, src)
		}
	}
	if u.CreatedAt.IsZero() {
		return fmt.Errorf("%w: upload %s has no creation time", ErrManifestCorrupt, u.ID)
	}
	return validateLabelValues(u.LabelValues, u.ID.String())
}

func validateStreamName(stream string) error {
	if err := checkStreamName(stream); err != nil {
		return fmt.Errorf("%w: %v", ErrManifestCorrupt, err)
	}
	return nil
}

func validateHour(h time.Time) error {
	if err := checkHour(h); err != nil {
		return fmt.Errorf("%w: %v", ErrManifestCorrupt, err)
	}
	return nil
}

// checkStreamName verifies stream is a sanitized stream name (the output of
// logs.SanitizeStream), as object and local paths require.
func checkStreamName(stream string) error {
	if stream == "" || logs.SanitizeStream(stream) != stream {
		return fmt.Errorf("ingest: stream %q is not a sanitized name", stream)
	}
	return nil
}

// checkHour accepts every UTC hour boundary. The zero time
// (0001-01-01T00:00:00Z) is a legal protobuf timestamp and a valid hour, not
// a missing field.
func checkHour(h time.Time) error {
	if !h.Equal(h.UTC().Truncate(time.Hour)) {
		return fmt.Errorf("ingest: hour %s is not a UTC hour boundary", h)
	}
	return nil
}

// checkDataKey verifies key is a canonical object key for tenant/stream/hour:
// {tenant}/logs/{stream}/{yyyy}/{mm}/{dd}/{hh}/{uuidv7}.parquet. The UUID is
// checked independently of any receipt ID because RefreshUploadKey rekeys a
// receipt with a fresh UUIDv7.
func checkDataKey(tid tenant.ID, stream string, hour time.Time, key string) error {
	wantDir := objstore.LogPrefix(tid, stream) + hour.UTC().Format("2006/01/02/15")
	base := path.Base(key)
	if path.Dir(key) != wantDir || !strings.HasSuffix(base, objstore.ExtData) {
		return fmt.Errorf("key %q is not a %s data key for hour %s", key, stream, hour.Format(time.RFC3339))
	}
	id, err := uuid.Parse(strings.TrimSuffix(base, objstore.ExtData))
	if err != nil || id[6]>>4 != 7 || id[8]&0xC0 != 0x80 {
		return fmt.Errorf("key %q does not end with an RFC UUIDv7 basename", key)
	}
	// Reconstruct the canonical key: path.Dir cleaning lets double slashes
	// or a /../ component through, so the whole string must match.
	if want := objstore.DataKey(tid, stream, hour, id); key != want {
		return fmt.Errorf("key %q is not canonical (want %q)", key, want)
	}
	return nil
}

func validateLabelValues(lv map[string][]string, what string) error {
	for name, values := range lv {
		if !slices.IsSorted(values) {
			return fmt.Errorf("%w: %s label %q values not sorted", ErrManifestCorrupt, what, name)
		}
	}
	return nil
}

func cloneLabelValues(lv map[string][]string) map[string][]string {
	if lv == nil {
		return nil
	}
	out := make(map[string][]string, len(lv))
	for k, v := range lv {
		out[k] = slices.Clone(v)
	}
	return out
}

func cloneManifest(m tenantManifest) tenantManifest {
	out := m
	out.Files = make([]LocalFile, len(m.Files))
	for i := range m.Files {
		out.Files[i] = m.Files[i].Clone()
	}
	out.Uploads = make([]Upload, len(m.Uploads))
	for i := range m.Uploads {
		out.Uploads[i] = m.Uploads[i].Clone()
	}
	return out
}

// sortFiles orders files by stream, hour, then id; the order Files returns
// and the manifest stores.
func sortFiles(files []LocalFile) {
	slices.SortFunc(files, func(a, b LocalFile) int {
		if c := cmp.Compare(a.Stream, b.Stream); c != 0 {
			return c
		}
		if c := a.Hour.Compare(b.Hour); c != 0 {
			return c
		}
		return a.ID.Compare(b.ID)
	})
}

func sortUploads(uploads []Upload) {
	slices.SortFunc(uploads, func(a, b Upload) int {
		if c := cmp.Compare(a.Stream, b.Stream); c != 0 {
			return c
		}
		if c := a.Hour.Compare(b.Hour); c != 0 {
			return c
		}
		return a.ID.Compare(b.ID)
	})
}

// localFileRelPath derives a file's rooted relative path.
func localFileRelPath(stream string, hour time.Time, id uuid.UUID) string {
	return hourDirRelPath(stream, hour) + "/" + id.String() + parquetExt
}

// hourDirRelPath is the rooted relative directory of one stream/hour
// partition: <stream>/<yyyy>/<mm>/<dd>/<hh>.
func hourDirRelPath(stream string, hour time.Time) string {
	return stream + hour.UTC().Format("/2006/01/02/15")
}

// uploadTempName is the finalized merge output path of an upload, relative to
// the tenant root. The same name with tempSuffix is the in-progress write.
func uploadTempName(id uuid.UUID) string {
	return uploadsDir + "/" + id.String() + parquetExt
}

func uploadTempTmpName(id uuid.UUID) string {
	return uploadsDir + "/" + id.String() + parquetExt + tempSuffix
}
