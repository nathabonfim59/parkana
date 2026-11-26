package catalog

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// This file holds the driver-neutral parts of the SQL catalogs (SQLite and
// Postgres): timestamp and label encoding, migration loading, batch checks,
// the ListFiles query and the row decoders. Both backends store identical
// values, so a catalog row means the same thing whichever backend wrote it.

// dialect is what differs between the SQL backends in the shared helpers.
type dialect struct {
	// name prefixes error messages: "catalog: <name>: ...".
	name string
	// placeholder returns the bind marker for the n-th argument (1-based).
	placeholder func(n int) string
}

var (
	sqliteDialect   = dialect{name: "sqlite", placeholder: func(int) string { return "?" }}
	postgresDialect = dialect{name: "postgres", placeholder: func(n int) string { return "$" + strconv.Itoa(n) }}
)

// microsFloor returns t in whole Unix microseconds rounded toward negative
// infinity. It is the representation stored in the database.
func microsFloor(t time.Time) int64 { return t.UnixMicro() }

// microsCeil returns t in whole Unix microseconds rounded toward positive
// infinity. Stored timestamps are integer microseconds, so for such a value m:
//
//	m >= t   iff  m >= microsCeil(t)
//	m < t    iff  m <  microsCeil(t)
//
// That keeps sub-microsecond filter bounds and purge cutoffs exact instead of
// silently widening or narrowing them when t is truncated to microseconds.
func microsCeil(t time.Time) int64 {
	us := t.UnixMicro()
	if t.Nanosecond()%1000 != 0 {
		us++
	}
	return us
}

// fromMicros decodes a stored Unix-microsecond timestamp.
func fromMicros(us int64) time.Time { return time.UnixMicro(us).UTC() }

// encodeLabelValues encodes File.LabelValues for the label_values column.
func encodeLabelValues(lv map[string][]string) (string, error) {
	b, err := json.Marshal(lv)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// decodeLabelValues decodes a label_values column; "" and "null" are nil.
func decodeLabelValues(s string) (map[string][]string, error) {
	if s == "" || s == "null" {
		return nil, nil
	}
	var lv map[string][]string
	if err := json.Unmarshal([]byte(s), &lv); err != nil {
		return nil, err
	}
	return lv, nil
}

// migration is one embedded, ordered schema change.
type migration struct {
	version int
	name    string
	stmt    string
}

// readMigrations reads the migrations in dir of fsys, ordered by version.
// Each file is named {version}_{description}.sql; stmt is its trimmed body.
// Errors carry no backend prefix: callers add it.
func readMigrations(fsys fs.FS, dir string) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	migrations := make([]migration, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(name, "_")
		if !ok {
			return nil, fmt.Errorf("migration %s: name must be {version}_{description}.sql", name)
		}
		version, err := strconv.Atoi(prefix)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("migration %s: invalid version prefix %q", name, prefix)
		}
		body, err := fs.ReadFile(fsys, path.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", name, err)
		}
		stmt := strings.TrimSpace(string(body))
		if stmt == "" {
			return nil, fmt.Errorf("migration %s is empty", name)
		}
		migrations = append(migrations, migration{version: version, name: name, stmt: stmt})
	}
	if len(migrations) == 0 {
		return nil, errors.New("no migrations embedded")
	}
	slices.SortFunc(migrations, func(a, b migration) int {
		return cmp.Compare(a.version, b.version)
	})
	for i := 1; i < len(migrations); i++ {
		if migrations[i].version == migrations[i-1].version {
			return nil, fmt.Errorf("duplicate migration version %d", migrations[i].version)
		}
	}
	return migrations, nil
}

// checkSchemaVersion returns an error wrapping ErrSchemaTooNew when the
// ledger's current version is above the highest of migrations (sorted, as
// readMigrations returns them): a newer binary migrated the catalog, and this
// one must not write to a schema it does not know. The error carries no
// backend prefix: callers add it.
func checkSchemaVersion(current int, migrations []migration) error {
	if latest := migrations[len(migrations)-1].version; current > latest {
		return &schemaTooNewError{current: current, latest: latest}
	}
	return nil
}

// schemaTooNewError reports a ledger version above the embedded migrations.
// It matches ErrSchemaTooNew with errors.Is.
type schemaTooNewError struct {
	current, latest int
}

func (e *schemaTooNewError) Error() string {
	return fmt.Sprintf("schema version %d is newer than this binary supports (%d): "+
		"it was migrated by a newer Parkana; run that version or restore a catalog backup",
		e.current, e.latest)
}

func (e *schemaTooNewError) Unwrap() error { return ErrSchemaTooNew }

// batchKeys checks the files of one AddFiles or ReplaceFiles batch: a valid
// tenant and no (tenant, key) twice in the batch. Existence in the catalog is
// each backend's own check.
type batchKeys map[string]struct{}

// check validates f and records its key; a repeated key is ErrExists.
func (seen batchKeys) check(f File) error {
	if err := f.Tenant.Validate(); err != nil {
		return err
	}
	id := string(f.Tenant) + "\x00" + f.Key
	if _, ok := seen[id]; ok {
		return fmt.Errorf("%w: %s", ErrExists, f.Key)
	}
	seen[id] = struct{}{}
	return nil
}

// checkBatch runs batchKeys.check over every file of a batch.
func checkBatch(files []File) error {
	seen := make(batchKeys, len(files))
	for _, f := range files {
		if err := seen.check(f); err != nil {
			return err
		}
	}
	return nil
}

// fileColumns is the files column list, in fileInsertValues and scanFile
// order.
const fileColumns = `"tenant", "stream", "key", "min_ts", "max_ts", "rows", "bytes", "label_values", "labels_complete", "has_sidecar", "created_at", "deleted_at"`

// fileInsertValues returns f's column values in fileColumns order. A zero
// CreatedAt is replaced with now, matching Memory. A zero DeletedAt is stored
// as SQL NULL ("live"); a nonzero one is preserved, so an explicitly
// pre-deleted file keeps its deletion time (including Unix epoch, which is
// distinct from zero). flag converts a boolean to the backend's column value.
func fileInsertValues(f File, now time.Time, flag func(bool) any) ([]any, error) {
	labels, err := encodeLabelValues(f.LabelValues)
	if err != nil {
		return nil, fmt.Errorf("encode labels for %s: %w", f.Key, err)
	}
	created := f.CreatedAt
	if created.IsZero() {
		created = now
	}
	var deleted any
	if !f.DeletedAt.IsZero() {
		deleted = microsFloor(f.DeletedAt)
	}
	return []any{
		string(f.Tenant), f.Stream, f.Key,
		microsFloor(f.MinTS), microsFloor(f.MaxTS),
		f.Rows, f.Bytes, labels, flag(f.LabelsComplete), flag(f.HasSidecar), created.UnixMicro(), deleted,
	}, nil
}

// listQuery builds the ListFiles statement. Time bounds are exact for stored
// microsecond timestamps: a file's MaxTS must be >= From, its MinTS <= Through.
// The label Match constraint is not pushed down; see listFiles.
func listQuery(flt Filter, d dialect) (string, []any) {
	var b strings.Builder
	args := []any{string(flt.Tenant)}
	b.WriteString(`SELECT ` + fileColumns + ` FROM files WHERE "tenant" = ` + d.placeholder(len(args)))
	arg := func(cond string, v any) {
		args = append(args, v)
		b.WriteString(cond + d.placeholder(len(args)))
	}
	if flt.Stream != "" {
		arg(` AND "stream" = `, flt.Stream)
	}
	if !flt.IncludeDeleted {
		b.WriteString(` AND "deleted_at" IS NULL`)
	}
	if !flt.From.IsZero() {
		arg(` AND "max_ts" >= `, microsCeil(flt.From))
	}
	if !flt.Through.IsZero() {
		arg(` AND "min_ts" <= `, microsFloor(flt.Through))
	}
	b.WriteString(` ORDER BY "min_ts", "key"`)
	return b.String(), args
}

// listFiles implements Catalog.ListFiles over db. Stream, tenant and time
// filtering run in SQL; the label Match constraint is applied with
// Filter.Matches so its "files without the label are kept" rule stays exact.
// The context is checked before the query and per row, and the rows are
// closed when the caller stops early.
func listFiles(ctx context.Context, db *sql.DB, flt Filter, d dialect) iter.Seq2[File, error] {
	return func(yield func(File, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(File{}, err)
			return
		}
		query, args := listQuery(flt, d)
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			yield(File{}, fmt.Errorf("catalog: %s: list files: %w", d.name, err))
			return
		}
		defer rows.Close()
		for rows.Next() {
			if err := ctx.Err(); err != nil {
				yield(File{}, err)
				return
			}
			f, err := scanFile(rows)
			if err != nil {
				yield(File{}, fmt.Errorf("catalog: %s: list files: %w", d.name, err))
				return
			}
			if !flt.Matches(f) {
				continue
			}
			if !yield(f, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(File{}, fmt.Errorf("catalog: %s: list files: %w", d.name, err))
		}
	}
}

// rowScanner is satisfied by *sql.Row and *sql.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanFile decodes one files row selected with fileColumns.
func scanFile(row rowScanner) (File, error) {
	var (
		f          File
		tenantID   string
		labels     string
		sidecar    any
		complete   any
		minTS      int64
		maxTS      int64
		createdTS  int64
		deletedTSP sql.NullInt64
	)
	if err := row.Scan(&tenantID, &f.Stream, &f.Key, &minTS, &maxTS, &f.Rows, &f.Bytes,
		&labels, &complete, &sidecar, &createdTS, &deletedTSP); err != nil {
		return File{}, err
	}
	f.Tenant = tenant.ID(tenantID)
	f.MinTS = fromMicros(minTS)
	f.MaxTS = fromMicros(maxTS)
	f.CreatedAt = fromMicros(createdTS)
	if deletedTSP.Valid {
		f.DeletedAt = fromMicros(deletedTSP.Int64)
	}
	var err error
	if f.HasSidecar, err = scanFlag(sidecar); err != nil {
		return File{}, fmt.Errorf("decode has_sidecar for %s: %w", f.Key, err)
	}
	if f.LabelsComplete, err = scanFlag(complete); err != nil {
		return File{}, fmt.Errorf("decode labels_complete for %s: %w", f.Key, err)
	}
	if f.LabelValues, err = decodeLabelValues(labels); err != nil {
		return File{}, fmt.Errorf("decode labels for %s: %w", f.Key, err)
	}
	return f, nil
}

// scanFlag decodes a boolean column: a Postgres BOOLEAN or a SQLite INTEGER
// (nonzero is true).
func scanFlag(v any) (bool, error) {
	switch x := v.(type) {
	case bool:
		return x, nil
	case int64:
		return x != 0, nil
	default:
		return false, fmt.Errorf("unexpected flag value %T", v)
	}
}

// pipelineColumns is the pipelines column list in scanPipeline order.
const pipelineColumns = `"tenant", "name", "stage", "selector", "spec", "version", "created_at", "updated_at", "deleted_at"`

// scanPipeline decodes one pipelines row selected with pipelineColumns.
func scanPipeline(row rowScanner) (PipelineDefinition, error) {
	var (
		def                  PipelineDefinition
		tenantID, stage      string
		version              int64
		createdAt, updatedAt int64
		deletedAt            sql.NullInt64
	)
	if err := row.Scan(&tenantID, &def.Name, &stage, &def.Selector, &def.Spec,
		&version, &createdAt, &updatedAt, &deletedAt); err != nil {
		return PipelineDefinition{}, err
	}
	def.Tenant = tenant.ID(tenantID)
	def.Stage = PipelineStage(stage)
	def.Version = version
	def.CreatedAt = fromMicros(createdAt)
	def.UpdatedAt = fromMicros(updatedAt)
	if def.Spec == nil {
		def.Spec = []byte{}
	}
	if deletedAt.Valid {
		def.DeletedAt = fromMicros(deletedAt.Int64)
	}
	return def, nil
}

// pipelinePut decides a PutPipeline for the SQL backends. existing is the
// tenant's stored row for def.Name (live or tombstone) when found, maxToken
// the tenant's current change token. It returns the row to store and its
// change token, or the error the write must fail with; it mirrors Memory's
// PutPipeline.
func pipelinePut(def PipelineDefinition, expectedVersion int64, existing PipelineDefinition, found bool, maxToken int64, now time.Time) (PipelineDefinition, int64, error) {
	now = pipelineMicros(now)
	out := PipelineDefinition{
		Tenant:    def.Tenant,
		Name:      def.Name,
		Stage:     def.Stage,
		Selector:  def.Selector,
		Spec:      clonePipelineSpec(def.Spec),
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	switch {
	case !found:
		if expectedVersion != 0 {
			return PipelineDefinition{}, 0, pipelineNotFound(def.Name)
		}
	case existing.Deleted():
		// Create expects version zero even over a tombstone; the new
		// version continues above it so recreation never resets it. A
		// nonzero expected version names no live row.
		if expectedVersion != 0 {
			return PipelineDefinition{}, 0, pipelineNotFound(def.Name)
		}
		if existing.Version == maxInt64 {
			return PipelineDefinition{}, 0, errVersionExhausted
		}
		out.Version = existing.Version + 1
	default:
		if expectedVersion != existing.Version {
			return PipelineDefinition{}, 0, versionConflict(def.Name, expectedVersion, existing.Version)
		}
		if existing.Version == maxInt64 {
			return PipelineDefinition{}, 0, errVersionExhausted
		}
		out.Version = existing.Version + 1
		out.CreatedAt = existing.CreatedAt
	}
	if maxToken == maxInt64 {
		return PipelineDefinition{}, 0, errVersionExhausted
	}
	return out, maxToken + 1, nil
}

// pipelineDelete decides a DeletePipeline for the SQL backends, with the
// arguments of pipelinePut. It returns the tombstone to store and its change
// token; it mirrors Memory's DeletePipeline.
func pipelineDelete(name string, expectedVersion int64, existing PipelineDefinition, found bool, maxToken int64, now time.Time) (PipelineDefinition, int64, error) {
	if !found || existing.Deleted() {
		return PipelineDefinition{}, 0, pipelineNotFound(name)
	}
	if expectedVersion != existing.Version {
		return PipelineDefinition{}, 0, versionConflict(name, expectedVersion, existing.Version)
	}
	if existing.Version == maxInt64 || maxToken == maxInt64 {
		return PipelineDefinition{}, 0, errVersionExhausted
	}
	now = pipelineMicros(now)
	existing.Version++
	existing.UpdatedAt = now
	existing.DeletedAt = now
	return existing, maxToken + 1, nil
}

// queryRower is satisfied by *sql.DB, *sql.Conn and *sql.Tx.
type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// listStreams implements Catalog.Streams over db.
func listStreams(ctx context.Context, db *sql.DB, t tenant.ID, d dialect) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT "stream" FROM files WHERE "tenant" = `+d.placeholder(1)+` AND "deleted_at" IS NULL ORDER BY "stream"`,
		string(t))
	if err != nil {
		return nil, fmt.Errorf("catalog: %s: streams: %w", d.name, err)
	}
	defer rows.Close()
	var streams []string
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var stream string
		if err := rows.Scan(&stream); err != nil {
			return nil, fmt.Errorf("catalog: %s: streams: %w", d.name, err)
		}
		streams = append(streams, stream)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: %s: streams: %w", d.name, err)
	}
	return streams, nil
}

// getPipeline implements Catalog.GetPipeline over db.
func getPipeline(ctx context.Context, db *sql.DB, t tenant.ID, name string, d dialect) (PipelineDefinition, error) {
	if err := ctx.Err(); err != nil {
		return PipelineDefinition{}, err
	}
	if err := t.Validate(); err != nil {
		return PipelineDefinition{}, err
	}
	def, err := scanPipeline(db.QueryRowContext(ctx,
		`SELECT `+pipelineColumns+` FROM pipelines WHERE "tenant" = `+d.placeholder(1)+` AND "name" = `+d.placeholder(2)+` AND "deleted_at" IS NULL`,
		string(t), name))
	if errors.Is(err, sql.ErrNoRows) {
		return PipelineDefinition{}, pipelineNotFound(name)
	}
	if err != nil {
		return PipelineDefinition{}, fmt.Errorf("catalog: %s: get pipeline %q: %w", d.name, name, err)
	}
	return def, nil
}

// listPipelines implements Catalog.ListPipelines over db.
func listPipelines(ctx context.Context, db *sql.DB, t tenant.ID, d dialect) ([]PipelineDefinition, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := t.Validate(); err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx,
		`SELECT `+pipelineColumns+` FROM pipelines WHERE "tenant" = `+d.placeholder(1)+` AND "deleted_at" IS NULL ORDER BY "name"`,
		string(t))
	if err != nil {
		return nil, fmt.Errorf("catalog: %s: list pipelines: %w", d.name, err)
	}
	defer rows.Close()
	var out []PipelineDefinition
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		def, err := scanPipeline(rows)
		if err != nil {
			return nil, fmt.Errorf("catalog: %s: list pipelines: %w", d.name, err)
		}
		out = append(out, def)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: %s: list pipelines: %w", d.name, err)
	}
	return out, nil
}

// pipelineChangeToken implements Catalog.PipelineChangeToken over db.
func pipelineChangeToken(ctx context.Context, db *sql.DB, t tenant.ID, d dialect) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := t.Validate(); err != nil {
		return 0, err
	}
	return readPipelineToken(ctx, db, t, d)
}

// readPipelineToken reads the tenant's change token: the maximum over its
// rows, tombstones included, which are never purged.
func readPipelineToken(ctx context.Context, q queryRower, t tenant.ID, d dialect) (int64, error) {
	var token int64
	if err := q.QueryRowContext(ctx,
		`SELECT COALESCE(MAX("change_token"), 0) FROM pipelines WHERE "tenant" = `+d.placeholder(1),
		string(t)).Scan(&token); err != nil {
		return 0, fmt.Errorf("catalog: %s: read pipeline token: %w", d.name, err)
	}
	return token, nil
}

// readPipelineState reads what pipelinePut and pipelineDelete decide on: the
// stored row for name (live or tombstone) if any, and the tenant's change
// token. q must be the write transaction (or connection) that then writes.
func readPipelineState(ctx context.Context, q queryRower, t tenant.ID, name string, d dialect) (existing PipelineDefinition, found bool, maxToken int64, err error) {
	existing, err = scanPipeline(q.QueryRowContext(ctx,
		`SELECT `+pipelineColumns+` FROM pipelines WHERE "tenant" = `+d.placeholder(1)+` AND "name" = `+d.placeholder(2),
		string(t), name))
	found = err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return PipelineDefinition{}, false, 0, fmt.Errorf("catalog: %s: read pipeline %q: %w", d.name, name, err)
	}
	maxToken, err = readPipelineToken(ctx, q, t, d)
	if err != nil {
		return PipelineDefinition{}, false, 0, err
	}
	return existing, found, maxToken, nil
}
