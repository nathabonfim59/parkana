package catalog

import (
	"cmp"
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"iter"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	// postgresMaxOpenConns bounds the connection pool.
	postgresMaxOpenConns = 16
	// postgresMaxIdleConns is how many idle connections the pool keeps.
	postgresMaxIdleConns = 4
	// postgresConnMaxIdleTime closes connections idle for longer.
	postgresConnMaxIdleTime = 5 * time.Minute
	// postgresConnMaxLifetime recycles connections older than this.
	postgresConnMaxLifetime = 30 * time.Minute
	// postgresTxAttempts bounds how often a write transaction runs when
	// Postgres aborts it with a serialization failure or deadlock.
	postgresTxAttempts = 3
	// postgresCancelDeadline is how long a connection whose context ended
	// waits for the server to answer the cancel request before it gives up
	// on the connection.
	postgresCancelDeadline = time.Second
	// postgresMigrationsDir is the embedded directory holding ordered
	// migrations.
	postgresMigrationsDir = "postgres_migrations"
	// postgresMigrateLock and postgresPipelinesLock prefix the advisory lock
	// keys that serialize migrations (per schema) and pipeline writes (per
	// tenant) across processes.
	postgresMigrateLock   = "parkana.catalog.migrate:"
	postgresPipelinesLock = "parkana.catalog.pipelines:"
	// redacted replaces secrets in DSNs and error messages.
	redacted = "xxxxx"
)

// Postgres error codes the catalog branches on.
const (
	pgUniqueViolation      = "23505"
	pgSerializationFailure = "40001"
	pgDeadlockDetected     = "40P01"
)

//go:embed postgres_migrations/*.sql
var postgresMigrations embed.FS

// Postgres is a Catalog stored in a PostgreSQL database, so catalogs in
// several processes (on several nodes) can share one file list. It is safe
// for concurrent use, also against other *Postgres instances on the same
// database and schema.
//
// The schema is SQLite's: timestamps are BIGINT Unix microseconds with the
// same floor/ceil rules, label values the same JSON text, and the text
// columns that order or identify rows use COLLATE "C" so ordering is bytewise
// like Go's whatever the database locale. Writes run in READ COMMITTED
// transactions that take their locks in one order (sorted keys, FOR UPDATE,
// per-tenant advisory locks for pipelines), and are retried a bounded number
// of times on serialization failures and deadlocks.
type Postgres struct {
	db    *sql.DB
	now   func() time.Time
	close func() error
}

var _ Catalog = (*Postgres)(nil)

// NewPostgres connects to the PostgreSQL database named by dsn (a
// postgres:// URL or a keyword/value string, as pgx accepts) and applies any
// pending embedded migrations to the current schema (the first existing
// schema of the search_path). Concurrent callers on one schema migrate it
// exactly once. The DSN may hold a password: it never appears in a returned
// error. The caller must Close the catalog.
func NewPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	if dsn == "" {
		return nil, errors.New("catalog: postgres: empty DSN")
	}
	secrets := dsnSecrets(dsn)
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, redactError("catalog: postgres: parse DSN", err, secrets)
	}
	// cfg.Password may also come from PGPASSWORD or a passfile.
	secrets = append(secrets, cfg.Password)
	// pgx's default only closes the socket when a context ends, and the
	// server backend then keeps running (or waiting for a lock) with the
	// locks it holds, and may even commit an autocommit statement after the
	// caller saw the context error. A cancel request stops the statement on
	// the server; it fails with query_canceled, which ctxError ties back to
	// the context.
	cfg.BuildContextWatcherHandler = func(conn *pgconn.PgConn) ctxwatch.Handler {
		return &pgconn.CancelRequestContextWatcherHandler{Conn: conn, DeadlineDelay: postgresCancelDeadline}
	}

	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(postgresMaxOpenConns)
	db.SetMaxIdleConns(postgresMaxIdleConns)
	db.SetConnMaxIdleTime(postgresConnMaxIdleTime)
	db.SetConnMaxLifetime(postgresConnMaxLifetime)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, redactError("catalog: postgres: connect to "+scrub(redactDSN(dsn), secrets), err, secrets)
	}
	if err := migratePostgres(ctx, db); err != nil {
		db.Close()
		return nil, redactError("catalog: postgres", err, secrets)
	}
	return &Postgres{db: db, now: time.Now, close: sync.OnceValue(db.Close)}, nil
}

// migratePostgres applies every pending migration in one transaction under
// an advisory lock keyed on the current schema, so concurrent openers of one
// schema serialize (and the later ones find nothing to do) while openers of
// other schemas don't wait.
func migratePostgres(ctx context.Context, db *sql.DB) error {
	migrations, err := readMigrations(postgresMigrations, postgresMigrationsDir)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("migrate: begin: %w", err)
	}
	defer tx.Rollback()

	// The lock key is computed here, not with current_schema() in SQL: a NULL
	// schema would make pg_advisory_xact_lock(NULL) return without locking.
	var schema sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		return fmt.Errorf("migrate: read current schema: %w", err)
	}
	if !schema.Valid {
		return errors.New("migrate: no current schema: the search_path names no existing schema")
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		postgresMigrateLock+schema.String); err != nil {
		return fmt.Errorf("migrate: lock schema %q: %w", schema.String, err)
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		"version" BIGINT PRIMARY KEY,
		"name" TEXT NOT NULL,
		"applied_at" BIGINT NOT NULL
	)`); err != nil {
		return fmt.Errorf("migrate: create schema_migrations: %w", err)
	}
	var current int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX("version"), 0) FROM schema_migrations`).Scan(&current); err != nil {
		return fmt.Errorf("migrate: read schema version: %w", err)
	}
	// A newer binary migrated this schema: return before writing anything,
	// and the deferred Rollback undoes the CREATE TABLE IF NOT EXISTS.
	if err := checkSchemaVersion(current, migrations); err != nil {
		return err
	}
	for _, m := range migrations {
		if m.version <= current {
			continue
		}
		// No arguments: pgx sends the body with the simple query protocol,
		// which runs every statement of a multi-statement migration.
		if _, err := tx.ExecContext(ctx, m.stmt); err != nil {
			return fmt.Errorf("migrate: apply %s: %w", m.name, err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO schema_migrations ("version", "name", "applied_at") VALUES ($1, $2, $3)`,
			m.version, m.name, time.Now().UnixMicro()); err != nil {
			return fmt.Errorf("migrate: record %s: %w", m.name, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("migrate: commit: %w", err)
	}
	return nil
}

// inTx runs fn in a write transaction, committing when fn returns nil. A
// transaction Postgres aborted with a serialization failure or deadlock runs
// again, at most postgresTxAttempts times in all; fn must be safe to rerun.
func (p *Postgres) inTx(ctx context.Context, op string, fn func(*sql.Tx) error) error {
	var err error
	for range postgresTxAttempts {
		err = p.tryTx(ctx, op, fn)
		if !retryable(err) || ctx.Err() != nil {
			return ctxError(ctx, err)
		}
	}
	return err
}

// ctxError returns err, also matching ctx's error with errors.Is when ctx
// has ended: a statement the server canceled for an ended context fails
// with a query_canceled *pgconn.PgError, not the context error.
func ctxError(ctx context.Context, err error) error {
	if err == nil || ctx.Err() == nil || errors.Is(err, ctx.Err()) {
		return err
	}
	return fmt.Errorf("%w (%w)", err, ctx.Err())
}

// tryTx runs fn in one transaction attempt.
func (p *Postgres) tryTx(ctx context.Context, op string, fn func(*sql.Tx) error) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: postgres: %s: begin: %w", op, err)
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: postgres: %s: commit: %w", op, err)
	}
	return nil
}

// retryable reports whether err aborted a transaction that may succeed when
// run again.
func retryable(err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && (pgErr.Code == pgSerializationFailure || pgErr.Code == pgDeadlockDetected)
}

// isUniqueViolation reports whether err is a unique-constraint violation.
func isUniqueViolation(err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && pgErr.Code == pgUniqueViolation
}

// AddFiles implements Catalog. The batch is inserted in (tenant, key) order,
// so concurrent inserters of overlapping batches lock index entries in one
// order; an existing key (live or deleted) fails the insert with a unique
// violation, reported as ErrExists, and the rollback leaves nothing added.
func (p *Postgres) AddFiles(ctx context.Context, files ...File) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(files) == 0 {
		return nil
	}
	if err := checkBatch(files); err != nil {
		return err
	}
	sorted := sortedFiles(files)
	return p.inTx(ctx, "add files", func(tx *sql.Tx) error {
		return insertPostgresFiles(ctx, tx, sorted, p.now())
	})
}

// ReplaceFiles implements Catalog. The live remove rows are locked in key
// order (FOR UPDATE), so two compactors replacing overlapping sets never
// deadlock, and the second sees the first's deletions and fails with
// ErrNotFound. As in SQLite and Memory, a missing remove key is reported
// before any problem with the added files.
func (p *Postgres) ReplaceFiles(ctx context.Context, t tenant.ID, remove []string, add []File) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(remove) == 0 && len(add) == 0 {
		return nil
	}
	keys := slices.Compact(slices.Sorted(slices.Values(remove)))
	sorted := sortedFiles(add)
	return p.inTx(ctx, "replace files", func(tx *sql.Tx) error {
		if len(keys) > 0 {
			live, err := lockLiveFiles(ctx, tx, t, keys)
			if err != nil {
				return err
			}
			for _, key := range remove {
				if _, ok := live[key]; !ok {
					return fmt.Errorf("%w: %s", ErrNotFound, key)
				}
			}
		}
		if err := checkBatch(add); err != nil {
			return err
		}
		now := p.now()
		if len(keys) > 0 {
			res, err := tx.ExecContext(ctx,
				`UPDATE files SET "deleted_at" = $1 WHERE "tenant" = $2 AND "key" = ANY($3) AND "deleted_at" IS NULL`,
				microsFloor(now), string(t), keys)
			if err != nil {
				return fmt.Errorf("catalog: postgres: replace files: delete: %w", err)
			}
			n, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("catalog: postgres: replace files: delete: %w", err)
			}
			// The rows are locked, so this only fails if the lock is broken.
			if n != int64(len(keys)) {
				return fmt.Errorf("%w: deleted %d of %d remove keys", ErrNotFound, n, len(keys))
			}
		}
		// An add key that already exists (live, deleted, or one of the remove
		// keys just marked deleted) fails the insert with a unique violation:
		// ErrExists, and the rollback undoes the deletions above.
		return insertPostgresFiles(ctx, tx, sorted, now)
	})
}

// lockLiveFiles locks the tenant's live files among keys (sorted) in key
// order and returns the keys it found.
func lockLiveFiles(ctx context.Context, tx *sql.Tx, t tenant.ID, keys []string) (map[string]struct{}, error) {
	rows, err := tx.QueryContext(ctx,
		`SELECT "key" FROM files WHERE "tenant" = $1 AND "key" = ANY($2) AND "deleted_at" IS NULL ORDER BY "key" FOR UPDATE`,
		string(t), keys)
	if err != nil {
		return nil, fmt.Errorf("catalog: postgres: replace files: lock: %w", err)
	}
	defer rows.Close()
	live := make(map[string]struct{}, len(keys))
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("catalog: postgres: replace files: lock: %w", err)
		}
		live[key] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: postgres: replace files: lock: %w", err)
	}
	return live, nil
}

// sortedFiles returns a copy of files ordered by (tenant, key) bytewise.
func sortedFiles(files []File) []File {
	sorted := slices.Clone(files)
	slices.SortFunc(sorted, func(a, b File) int {
		return cmp.Or(cmp.Compare(a.Tenant, b.Tenant), cmp.Compare(a.Key, b.Key))
	})
	return sorted
}

// insertPostgresFiles inserts files inside tx; see fileInsertValues for the
// stored values. A key that exists is ErrExists.
func insertPostgresFiles(ctx context.Context, tx *sql.Tx, files []File, now time.Time) error {
	for _, f := range files {
		values, err := fileInsertValues(f, now, postgresFlag)
		if err != nil {
			return fmt.Errorf("catalog: postgres: %w", err)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO files (`+fileColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			values...); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("%w: %s", ErrExists, f.Key)
			}
			return fmt.Errorf("catalog: postgres: insert %s: %w", f.Key, err)
		}
	}
	return nil
}

// postgresFlag stores a boolean as BOOLEAN.
func postgresFlag(b bool) any { return b }

// ListFiles implements Catalog. Stream, tenant and time filtering run in SQL;
// the label Match constraint is applied with Filter.Matches.
func (p *Postgres) ListFiles(ctx context.Context, flt Filter) iter.Seq2[File, error] {
	return func(yield func(File, error) bool) {
		for f, err := range listFiles(ctx, p.db, flt, postgresDialect) {
			if !yield(f, ctxError(ctx, err)) {
				return
			}
		}
	}
}

// Streams implements Catalog.
func (p *Postgres) Streams(ctx context.Context, t tenant.ID) ([]string, error) {
	streams, err := listStreams(ctx, p.db, t, postgresDialect)
	return streams, ctxError(ctx, err)
}

// SetSidecar implements Catalog.
func (p *Postgres) SetSidecar(ctx context.Context, t tenant.ID, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// A transaction, not an autocommit statement, so an update the caller
	// gave up on (its context ended) never takes effect.
	return p.inTx(ctx, "set sidecar", func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE files SET "has_sidecar" = TRUE WHERE "tenant" = $1 AND "key" = $2 AND "deleted_at" IS NULL`,
			string(t), key)
		if err != nil {
			return fmt.Errorf("catalog: postgres: set sidecar %s: %w", key, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("catalog: postgres: set sidecar %s: %w", key, err)
		}
		if n != 1 {
			return fmt.Errorf("%w: %s", ErrNotFound, key)
		}
		return nil
	})
}

// PurgeDeleted implements Catalog. Only files deleted strictly before cutoff
// are removed; see microsCeil. The keys are returned sorted.
func (p *Postgres) PurgeDeleted(ctx context.Context, t tenant.ID, cutoff time.Time) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var keys []string
	err := p.inTx(ctx, "purge deleted", func(tx *sql.Tx) error {
		keys = nil
		rows, err := tx.QueryContext(ctx,
			`DELETE FROM files WHERE "tenant" = $1 AND "deleted_at" IS NOT NULL AND "deleted_at" < $2 RETURNING "key"`,
			string(t), microsCeil(cutoff))
		if err != nil {
			return fmt.Errorf("catalog: postgres: purge deleted: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				return fmt.Errorf("catalog: postgres: purge deleted: %w", err)
			}
			keys = append(keys, key)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("catalog: postgres: purge deleted: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(keys)
	return keys, nil
}

// Close implements Catalog. It is safe to call more than once.
func (p *Postgres) Close() error { return p.close() }

// redactError wraps err as "prefix: message" with every secret in the
// message replaced. errors.Is and errors.AsType still reach err.
func redactError(prefix string, err error, secrets []string) error {
	return &redactedError{msg: prefix + ": " + scrub(err.Error(), secrets), err: err}
}

// redactedError is an error whose message has its secrets removed.
type redactedError struct {
	msg string
	err error
}

func (e *redactedError) Error() string { return e.msg }
func (e *redactedError) Unwrap() error { return e.err }

// scrub replaces every nonempty secret in s.
func scrub(s string, secrets []string) string {
	for _, secret := range secrets {
		if secret != "" {
			s = strings.ReplaceAll(s, secret, redacted)
		}
	}
	return s
}

// dsnPassword matches a password or sslpassword keyword and its value
// (single-quoted with backslash escapes, or bare) in a keyword/value DSN.
// A quoted value missing its closing quote runs to the end of the DSN; the
// third group is the closing quote, empty when there is none.
var dsnPassword = regexp.MustCompile(`(?i)(\b(?:ssl)?password\s*=\s*)('(?:[^'\\]|\\.)*('|$)|[^\s']*)`)

// isURLDSN reports whether dsn is in URL form.
func isURLDSN(dsn string) bool {
	return strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")
}

// redactDSN returns dsn with its passwords masked, for error messages. A URL
// that does not parse is replaced entirely.
func redactDSN(dsn string) string {
	if !isURLDSN(dsn) {
		return dsnPassword.ReplaceAllString(dsn, "${1}"+redacted)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "postgres://" + redacted
	}
	q := u.Query()
	for _, k := range []string{"password", "sslpassword"} {
		if q.Has(k) {
			q.Set(k, redacted)
		}
	}
	u.RawQuery = q.Encode()
	return u.Redacted()
}

// dsnSecrets returns the passwords written in dsn, both as written and
// decoded, so they can be scrubbed from messages that echo them, including
// errors for a DSN that does not parse.
func dsnSecrets(dsn string) []string {
	var secrets []string
	if !isURLDSN(dsn) {
		for _, m := range dsnPassword.FindAllStringSubmatch(dsn, -1) {
			v, closing := m[2], m[3]
			secrets = append(secrets, v)
			if inner, ok := strings.CutPrefix(v, "'"); ok {
				inner = strings.TrimSuffix(inner, closing)
				secrets = append(secrets, inner, unescapeDSNValue(inner))
				if closing == "" {
					// Unterminated: pgx's parse error masks the value only up
					// to its first space (redactPW), so the value never appears
					// whole and each space-separated piece is scrubbed.
					for piece := range strings.FieldsSeq(inner) {
						secrets = append(secrets, piece, unescapeDSNValue(piece))
					}
				}
			}
		}
		return secrets
	}
	u, err := url.Parse(dsn)
	if err != nil {
		// Best effort on a URL that doesn't parse: whatever sits between the
		// first ':' of the userinfo and the last '@' of the authority.
		// Query parameters are scrubbed the same way.
		_, rest, _ := strings.Cut(dsn, "://")
		authority, _, _ := strings.Cut(rest, "/")
		authority, _, _ = strings.Cut(authority, "?")
		if userinfo, _, ok := strings.CutLast(authority, "@"); ok {
			if _, pw, ok := strings.Cut(userinfo, ":"); ok {
				secrets = append(secrets, pw)
			}
		}
		if _, query, ok := strings.Cut(dsn, "?"); ok {
			q, _ := url.ParseQuery(query) // partial values on error are what we want
			for _, k := range []string{"password", "sslpassword"} {
				secrets = append(secrets, q[k]...)
			}
		}
		return secrets
	}
	if pw, ok := u.User.Password(); ok {
		secrets = append(secrets, pw)
		if _, raw, ok := strings.Cut(u.User.String(), ":"); ok {
			secrets = append(secrets, raw)
		}
	}
	q := u.Query()
	for _, k := range []string{"password", "sslpassword"} {
		for _, v := range q[k] {
			secrets = append(secrets, v, url.QueryEscape(v))
		}
	}
	return secrets
}

// unescapeDSNValue removes the backslash escapes of a quoted keyword value.
func unescapeDSNValue(s string) string {
	var b strings.Builder
	escaped := false
	for _, r := range s {
		if !escaped && r == '\\' {
			escaped = true
			continue
		}
		escaped = false
		b.WriteRune(r)
	}
	return b.String()
}
