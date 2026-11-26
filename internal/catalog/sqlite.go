package catalog

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
	_ "modernc.org/sqlite" // registers the "sqlite" database/sql driver
)

const (
	// sqliteDriver is the database/sql driver registered by modernc.org/sqlite.
	sqliteDriver = "sqlite"
	// sqliteBusyTimeoutMS bounds how long a connection waits for a lock.
	sqliteBusyTimeoutMS = 10_000
	// sqliteMaxReaders bounds the read pool. Writes always use a single
	// connection, so readers never contend with each other for SQLite locks.
	sqliteMaxReaders = 8
	// migrationsDir is the embedded directory holding ordered migrations.
	migrationsDir = "sqlite_migrations"
)

//go:embed sqlite_migrations/*.sql
var sqliteMigrations embed.FS

// SQLite is a Catalog stored in a SQLite database file. It is safe for
// concurrent use.
//
// Writes are serialized through a single connection, so concurrent AddFiles and
// ReplaceFiles calls queue up instead of returning SQLITE_BUSY. Reads use a
// separate pool: an in-flight ListFiles iterator never blocks a write, and a
// caller that invokes another catalog method while iterating cannot deadlock.
//
// Timestamps are stored as INTEGER Unix microseconds; ListFiles returns them
// truncated toward negative infinity to whole microseconds. Sub-microsecond
// filter bounds and purge cutoffs are compared exactly (see microsCeil).
type SQLite struct {
	write *sql.DB
	read  *sql.DB
	now   func() time.Time

	closeOnce sync.Once
	closeErr  error
}

var _ Catalog = (*SQLite)(nil)

// NewSQLite opens (creating if needed) the SQLite database at the plain
// filesystem path and applies any pending embedded migrations. The parent
// directory is created and a relative path is resolved against the current
// directory first, so a file literally named ":memory:" works. path is a file
// path, not a URI or DSN: connection pragmas are added internally.
//
// The database uses WAL journaling. The caller must Close it.
func NewSQLite(path string) (*SQLite, error) {
	if path == "" {
		return nil, errors.New("catalog: sqlite: empty database path")
	}
	// Resolve to an absolute path before building the file: URI. Otherwise a
	// relative name that is literally ":memory:" would select SQLite's special
	// in-memory database instead of a file named ":memory:".
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("catalog: sqlite: resolve %s: %w", path, err)
	}
	if dir := filepath.Dir(abs); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("catalog: sqlite: create directory %s: %w", dir, err)
		}
	}

	ctx := context.Background()
	write, err := sql.Open(sqliteDriver, sqliteDSN(abs,
		"busy_timeout("+strconv.Itoa(sqliteBusyTimeoutMS)+")",
		"journal_mode(WAL)",
		"synchronous(FULL)",
		"foreign_keys(1)",
	))
	if err != nil {
		return nil, fmt.Errorf("catalog: sqlite: open %s: %w", path, err)
	}
	write.SetMaxOpenConns(1)
	write.SetMaxIdleConns(1)
	if err := write.PingContext(ctx); err != nil {
		write.Close()
		return nil, fmt.Errorf("catalog: sqlite: open %s: %w", path, err)
	}

	// Migrations run before the read pool opens so readers never see a
	// half-migrated schema.
	if err := migrate(ctx, write); err != nil {
		write.Close()
		return nil, err
	}

	read, err := sql.Open(sqliteDriver, sqliteDSN(abs,
		"busy_timeout("+strconv.Itoa(sqliteBusyTimeoutMS)+")",
		"synchronous(FULL)",
		"foreign_keys(1)",
	))
	if err != nil {
		write.Close()
		return nil, fmt.Errorf("catalog: sqlite: open read pool %s: %w", path, err)
	}
	read.SetMaxOpenConns(sqliteMaxReaders)
	read.SetMaxIdleConns(sqliteMaxReaders)
	if err := read.PingContext(ctx); err != nil {
		read.Close()
		write.Close()
		return nil, fmt.Errorf("catalog: sqlite: open read pool %s: %w", path, err)
	}

	return &SQLite{write: write, read: read, now: time.Now}, nil
}

// sqliteDSN builds a file: URI for the absolute path with the given connection
// pragmas. NewSQLite resolves configured paths first, so a relative name like
// ":memory:" names a real file. Building a URI (rather than appending a query
// string to the raw path) keeps '?' and '#' characters that are part of the
// file name from being parsed as DSN syntax.
func sqliteDSN(filePath string, pragmas ...string) string {
	q := url.Values{}
	for _, p := range pragmas {
		q.Add("_pragma", p)
	}
	return "file:" + (&url.URL{Path: filePath}).EscapedPath() + "?" + q.Encode()
}

// loadMigrations reads and validates the embedded SQLite migrations, ordered
// by version. Each file is named {version}_{description}.sql and holds exactly
// one SQL statement: modernc.org/sqlite prepares only the first statement of a
// multi-statement string, so a second statement would silently never run.
func loadMigrations() ([]migration, error) {
	migrations, err := readMigrations(sqliteMigrations, migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("catalog: sqlite: %w", err)
	}
	for i, m := range migrations {
		if migrations[i].stmt, err = migrationStatement(m.name, m.stmt); err != nil {
			return nil, err
		}
	}
	return migrations, nil
}

// migrationStatement strips a trailing semicolon and rejects extra statements.
func migrationStatement(name, body string) (string, error) {
	stmt := strings.TrimSpace(body)
	stmt = strings.TrimSpace(strings.TrimSuffix(stmt, ";"))
	if stmt == "" {
		return "", fmt.Errorf("catalog: sqlite: migration %s is empty", name)
	}
	if strings.Contains(stmt, ";") {
		return "", fmt.Errorf("catalog: sqlite: migration %s must contain exactly one statement", name)
	}
	return stmt, nil
}

// migrate creates the migration ledger and applies every migration whose
// version is newer than the recorded maximum, one transaction per migration.
// A recorded maximum above every embedded migration fails with
// ErrSchemaTooNew before anything is written.
func migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		"version" INTEGER PRIMARY KEY,
		"name" TEXT NOT NULL,
		"applied_at" INTEGER NOT NULL
	)`); err != nil {
		return fmt.Errorf("catalog: sqlite: create schema_migrations: %w", err)
	}
	var current int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX("version"), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("catalog: sqlite: read schema version: %w", err)
	}
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}
	if err := checkSchemaVersion(current, migrations); err != nil {
		return fmt.Errorf("catalog: sqlite: %w", err)
	}
	for _, m := range migrations {
		if m.version <= current {
			continue
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("catalog: sqlite: begin migration %s: %w", m.name, err)
		}
		if _, err := tx.ExecContext(ctx, m.stmt); err != nil {
			tx.Rollback()
			return fmt.Errorf("catalog: sqlite: apply migration %s: %w", m.name, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_migrations ("version", "name", "applied_at") VALUES (?, ?, ?)`,
			m.version, m.name, time.Now().UnixMicro()); err != nil {
			tx.Rollback()
			return fmt.Errorf("catalog: sqlite: record migration %s: %w", m.name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("catalog: sqlite: commit migration %s: %w", m.name, err)
		}
	}
	return nil
}

// AddFiles implements Catalog.
func (s *SQLite) AddFiles(ctx context.Context, files ...File) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: sqlite: add files: %w", err)
	}
	defer tx.Rollback()

	check, err := tx.PrepareContext(ctx, `SELECT EXISTS(SELECT 1 FROM files WHERE "tenant" = ? AND "key" = ?)`)
	if err != nil {
		return fmt.Errorf("catalog: sqlite: add files: %w", err)
	}
	defer check.Close()
	seen := make(batchKeys, len(files))
	for _, f := range files {
		if err := seen.check(f); err != nil {
			return err
		}
		var exists int64
		if err := check.QueryRowContext(ctx, string(f.Tenant), f.Key).Scan(&exists); err != nil {
			return fmt.Errorf("catalog: sqlite: check %s: %w", f.Key, err)
		}
		if exists != 0 {
			return fmt.Errorf("%w: %s", ErrExists, f.Key)
		}
	}
	if err := insertFiles(ctx, tx, files, s.now()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: sqlite: add files: %w", err)
	}
	return nil
}

// ReplaceFiles implements Catalog.
func (s *SQLite) ReplaceFiles(ctx context.Context, t tenant.ID, remove []string, add []File) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(remove) == 0 && len(add) == 0 {
		return nil
	}

	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: sqlite: replace files: %w", err)
	}
	defer tx.Rollback()

	checkLive, err := tx.PrepareContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM files WHERE "tenant" = ? AND "key" = ? AND "deleted_at" IS NULL)`)
	if err != nil {
		return fmt.Errorf("catalog: sqlite: replace files: %w", err)
	}
	defer checkLive.Close()
	live := make(map[string]struct{}, len(remove))
	for _, key := range remove {
		if _, ok := live[key]; ok {
			continue
		}
		var exists int64
		if err := checkLive.QueryRowContext(ctx, string(t), key).Scan(&exists); err != nil {
			return fmt.Errorf("catalog: sqlite: check %s: %w", key, err)
		}
		if exists == 0 {
			return fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		live[key] = struct{}{}
	}

	checkAny, err := tx.PrepareContext(ctx, `SELECT EXISTS(SELECT 1 FROM files WHERE "tenant" = ? AND "key" = ?)`)
	if err != nil {
		return fmt.Errorf("catalog: sqlite: replace files: %w", err)
	}
	defer checkAny.Close()
	seen := make(batchKeys, len(add))
	for _, f := range add {
		if err := seen.check(f); err != nil {
			return err
		}
		var exists int64
		if err := checkAny.QueryRowContext(ctx, string(f.Tenant), f.Key).Scan(&exists); err != nil {
			return fmt.Errorf("catalog: sqlite: check %s: %w", f.Key, err)
		}
		if exists != 0 {
			return fmt.Errorf("%w: %s", ErrExists, f.Key)
		}
	}

	now := s.now()
	mark, err := tx.PrepareContext(ctx,
		`UPDATE files SET "deleted_at" = ? WHERE "tenant" = ? AND "key" = ? AND "deleted_at" IS NULL`)
	if err != nil {
		return fmt.Errorf("catalog: sqlite: replace files: %w", err)
	}
	defer mark.Close()
	for key := range live {
		res, err := mark.ExecContext(ctx, now.UnixMicro(), string(t), key)
		if err != nil {
			return fmt.Errorf("catalog: sqlite: delete %s: %w", key, err)
		}
		if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("catalog: sqlite: delete %s: %w", key, err)
		} else if n != 1 {
			return fmt.Errorf("%w: %s", ErrNotFound, key)
		}
	}
	if err := insertFiles(ctx, tx, add, now); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: sqlite: replace files: %w", err)
	}
	return nil
}

// insertFiles inserts files inside tx; see fileInsertValues for the stored
// values.
func insertFiles(ctx context.Context, tx *sql.Tx, files []File, now time.Time) error {
	if len(files) == 0 {
		return nil
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO files (`+fileColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("catalog: sqlite: prepare insert: %w", err)
	}
	defer stmt.Close()
	for _, f := range files {
		values, err := fileInsertValues(f, now, sqliteFlag)
		if err != nil {
			return fmt.Errorf("catalog: sqlite: %w", err)
		}
		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("catalog: sqlite: insert %s: %w", f.Key, err)
		}
	}
	return nil
}

// sqliteFlag stores a boolean as INTEGER 0 or 1.
func sqliteFlag(b bool) any {
	if b {
		return int64(1)
	}
	return int64(0)
}

// ListFiles implements Catalog. Stream, tenant and time filtering run in SQL;
// the label Match constraint is applied with Filter.Matches so its
// "files without the label are kept" rule stays exact.
func (s *SQLite) ListFiles(ctx context.Context, flt Filter) iter.Seq2[File, error] {
	return listFiles(ctx, s.read, flt, sqliteDialect)
}

// Streams implements Catalog.
func (s *SQLite) Streams(ctx context.Context, t tenant.ID) ([]string, error) {
	return listStreams(ctx, s.read, t, sqliteDialect)
}

// SetSidecar implements Catalog.
func (s *SQLite) SetSidecar(ctx context.Context, t tenant.ID, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	res, err := s.write.ExecContext(ctx,
		`UPDATE files SET "has_sidecar" = 1 WHERE "tenant" = ? AND "key" = ? AND "deleted_at" IS NULL`,
		string(t), key)
	if err != nil {
		return fmt.Errorf("catalog: sqlite: set sidecar %s: %w", key, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: sqlite: set sidecar %s: %w", key, err)
	}
	if n != 1 {
		return fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	return nil
}

// PurgeDeleted implements Catalog. Only files deleted strictly before cutoff
// are removed; see microsCeil.
func (s *SQLite) PurgeDeleted(ctx context.Context, t tenant.ID, cutoff time.Time) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("catalog: sqlite: purge deleted: %w", err)
	}
	defer tx.Rollback()

	bound := microsCeil(cutoff)
	rows, err := tx.QueryContext(ctx,
		`SELECT "key" FROM files WHERE "tenant" = ? AND "deleted_at" IS NOT NULL AND "deleted_at" < ? ORDER BY "key"`,
		string(t), bound)
	if err != nil {
		return nil, fmt.Errorf("catalog: sqlite: purge deleted: %w", err)
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return nil, fmt.Errorf("catalog: sqlite: purge deleted: %w", err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("catalog: sqlite: purge deleted: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("catalog: sqlite: purge deleted: %w", err)
	}
	if len(keys) > 0 {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM files WHERE "tenant" = ? AND "deleted_at" IS NOT NULL AND "deleted_at" < ?`,
			string(t), bound); err != nil {
			return nil, fmt.Errorf("catalog: sqlite: purge deleted: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("catalog: sqlite: purge deleted: %w", err)
	}
	return keys, nil
}

// Close implements Catalog. It is safe to call more than once.
func (s *SQLite) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = errors.Join(s.read.Close(), s.write.Close())
	})
	return s.closeErr
}
