// The SQL API's executor: the internal contract the HTTP endpoint calls. One
// Query call is one sandboxed request — per-request DuckDB database,
// tenant-and-time materialization, lockdown, single-SELECT validation and a
// typed, bounded result. No routes are registered here.
//
// Result contract: typed columns
// {name, type, databaseType}, rows of JSON-encodable cells and a truncated
// flag. A cell is nil (SQL NULL), bool, string, a UTC time.Time, an exact
// JSON number token (jsontext.Value) for every numeric type, or a nested
// map/list of those for composite types. Numbers keep full precision:
// integers (including HUGEINT and friends) are exact decimal digits,
// DECIMAL is the driver's exact decimal text (trailing-zero padding
// dropped: DECIMAL 3.0 encodes as 3), FLOAT/DOUBLE are shortest round-trip
// and non-finite floats fail the request; consumers must decode with
// json.Number semantics (the plugin does) instead of float64 when precision
// matters. INTERVAL encodes as a {"months","days","micros"} JSON object,
// TIME/TIME WITH TIME ZONE as clock strings, BLOB/GEOMETRY as base64 and
// UUID as canonical text. Instant values outside years 0-9999 (DuckDB
// infinities and over-range dates) fail as client SQL errors, because RFC 3339
// cannot carry them.
package query

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Executor policy errors. The HTTP layer maps errSQLRecursive to its dedicated
// 400 before the generic errSQLStatement, and maps errSQLStatement and
// errSQLLimits to 400 (client SQL/limit failures), errSQLRejected to 400 for
// failures DuckDB reports about the user's statement, and errSQLMemory to a
// 400 naming sql.memory-limit; anything else is a store or internal fault.
var (
	// errSQLStatement rejects queries that are not exactly one side-effect-free
	// SELECT (batches, other statement types, PRAGMA, parameters, empty SQL).
	errSQLStatement = errors.New("query: sql: the SQL API runs exactly one SELECT statement")
	// errSQLRecursive rejects explicit recursive CTEs. The API does not support
	// them, and sqlPreflight rejects the WITH RECURSIVE token pair lexically
	// before DuckDB parses the text (see sqlHasExplicitRecursive), so the ban
	// does not depend on the CTE name surviving the AST relation policy. The
	// returned error also wraps errSQLStatement for existing callers.
	errSQLRecursive = errors.New("query: sql: recursive CTEs are not supported")
	// errSQLLimits rejects malformed request limits (negative or above MaxRows).
	errSQLLimits = errors.New("query: sql: invalid result limit")
	// errSQLRejected marks a DuckDB-reported failure of the user's own
	// statement (parse, bind, permission inside the sandbox, runtime).
	errSQLRejected = errors.New("query: sql: statement rejected")
)

// sqlOptions are the executor's resource limits, mirroring the approved
// defaults until the configuration flags land: 1 GiB memory, one DuckDB
// thread, 10000 max rows, 1000 default. Each request's sandbox applies them
// to its own database only.
type sqlOptions struct {
	// MemoryLimitBytes is the per-request DuckDB memory budget, in bytes.
	MemoryLimitBytes int64
	// Threads is the per-request DuckDB thread count.
	Threads int
	// MaxRows is the hard cap on returned rows.
	MaxRows int
	// DefaultLimit is the row limit used when a request carries none (0).
	DefaultLimit int
}

// validate checks the options' own invariants.
func (o sqlOptions) validate() error {
	switch {
	case o.MemoryLimitBytes <= 0:
		return fmt.Errorf("query: sql: memory limit must be positive, got %d", o.MemoryLimitBytes)
	case o.Threads < 1:
		return fmt.Errorf("query: sql: threads must be at least 1, got %d", o.Threads)
	case o.MaxRows < 1:
		return fmt.Errorf("query: sql: max rows must be at least 1, got %d", o.MaxRows)
	case o.DefaultLimit < 1 || o.DefaultLimit > o.MaxRows:
		return fmt.Errorf("query: sql: default limit %d out of [1, %d]", o.DefaultLimit, o.MaxRows)
	}
	return nil
}

// defaultSQLOptions returns the approved defaults.
func defaultSQLOptions() sqlOptions {
	return sqlOptions{MemoryLimitBytes: 1 << 30, Threads: 1, MaxRows: 10000, DefaultLimit: 1000}
}

// sqlExec runs sandboxed SQL requests over the shared catalog and object
// cache. It holds no per-request state: every Query call builds and tears
// down its own DuckDB database, so concurrent requests with different tenants
// cannot observe each other.
type sqlExec struct {
	cat     catalog.Catalog
	resolve fileResolver
	opts    sqlOptions
	log     *slog.Logger
}

// newSQLExec builds an executor over cat's file list and the object cache (or
// any file resolver). opts must satisfy its invariants; log may be nil.
func newSQLExec(opts sqlOptions, cat catalog.Catalog, resolve fileResolver, log *slog.Logger) (*sqlExec, error) {
	if cat == nil {
		return nil, errors.New("query: sql: catalog is required")
	}
	if resolve == nil {
		return nil, errors.New("query: sql: file resolver is required")
	}
	if err := opts.validate(); err != nil {
		return nil, err
	}
	return &sqlExec{cat: cat, resolve: resolve, opts: opts, log: cmpLogger(log)}, nil
}

// sqlRequest is one SQL API request. From and To bound the materialized
// window half-open, [From, To), in UTC; Limit 0 means the default limit,
// values outside [1, MaxRows] are rejected.
type sqlRequest struct {
	Tenant tenant.ID
	Query  string
	From   time.Time
	To     time.Time
	Limit  int
}

// sqlColumn is one result column: its name, its response type (time, number,
// string, boolean or json) and DuckDB's own type name. The JSON tags are the
// frozen lowercase response contract.
type sqlColumn struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	DatabaseType string `json:"databaseType"`
}

// sqlResult is a bounded response: columns, up to limit rows, and whether
// more rows existed. Cells are the JSON-encodable values documented on the
// package's executor contract, and the JSON tags are the frozen response
// contract: {"columns":[...],"rows":[[...]],"truncated":bool}.
type sqlResult struct {
	Columns   []sqlColumn `json:"columns"`
	Rows      [][]any     `json:"rows"`
	Truncated bool        `json:"truncated"`
}

// Query runs one sandboxed request: list the tenant's files overlapping the
// window, lease and materialize their in-range rows, lock the sandbox down,
// validate the statement and execute it under the context. The context
// covers every phase, including statement parsing and planning (the driver's
// QueryContext interruption); ctx cancellation aborts with its own error.
func (e *sqlExec) Query(ctx context.Context, req sqlRequest) (res sqlResult, err error) {
	if err := e.checkRequest(ctx, req); err != nil {
		return sqlResult{}, err
	}
	limit := req.Limit
	if limit == 0 {
		limit = e.opts.DefaultLimit
	}
	if limit < 0 || limit > e.opts.MaxRows {
		return sqlResult{}, fmt.Errorf("%w: %d is negative or above the %d-row maximum", errSQLLimits, req.Limit, e.opts.MaxRows)
	}
	files, err := e.listFiles(ctx, req)
	if err != nil {
		return sqlResult{}, err
	}
	paths, release, err := e.acquire(ctx, req.Tenant, files)
	if err != nil {
		return sqlResult{}, err
	}
	defer release() // idempotent; the real release follows materialization

	sb, err := openSQLSandbox(ctx)
	if err != nil {
		return sqlResult{}, err
	}
	defer func() { err = errors.Join(err, sb.close()) }()

	if err := sb.applyLimits(ctx, e.opts.MemoryLimitBytes, e.opts.Threads); err != nil {
		return sqlResult{}, err
	}
	if err := sb.materialize(ctx, paths, req.From, req.To); err != nil {
		return sqlResult{}, err
	}
	e.log.Debug("query: sql: materialized",
		"tenant", req.Tenant, "files", len(paths), "from", req.From, "to", req.To)
	// Every file read is done: the leases go back before the sandbox closes
	// its filesystem, exactly one release no matter which path failed.
	release()
	if err := sb.lockdown(ctx); err != nil {
		return sqlResult{}, err
	}
	if err := sb.validate(ctx, req.Query); err != nil {
		return sqlResult{}, err
	}
	rows, err := sb.query(ctx, req.Query)
	if err != nil {
		return sqlResult{}, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	return sqlScanRows(ctx, rows, limit)
}

// checkRequest validates the request's invariants before any resource is
// touched: no catalog listing, object lease or DuckDB database exists until
// it passes. The HTTP layer rejects malformed requests itself with precise
// messages; these checks keep the internal contract honest even without it.
func (e *sqlExec) checkRequest(ctx context.Context, req sqlRequest) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("query: sql: %w", err)
	}
	if err := req.Tenant.Validate(); err != nil {
		return fmt.Errorf("query: sql: %w", err)
	}
	if req.From.IsZero() || req.To.IsZero() || !req.From.Before(req.To) {
		return fmt.Errorf("query: sql: time bounds must satisfy From < To, got [%s, %s)", req.From, req.To)
	}
	if strings.TrimSpace(req.Query) == "" {
		return fmt.Errorf("%w: empty query", errSQLStatement)
	}
	if err := sqlPreflight(req.Query); err != nil {
		return err
	}
	return nil
}

// listFiles returns the tenant's live catalog files overlapping the request
// window: tenant and time only, no stream or label pruning — the SQL relation
// spans every stream the tenant stored in the window.
func (e *sqlExec) listFiles(ctx context.Context, req sqlRequest) ([]catalog.File, error) {
	var files []catalog.File
	for f, err := range e.cat.ListFiles(ctx, catalog.Filter{
		Tenant:  req.Tenant,
		From:    req.From,
		Through: req.To,
	}) {
		if err != nil {
			return nil, fmt.Errorf("query: sql: list files: %w", err)
		}
		files = append(files, f)
	}
	return files, nil
}

// acquire leases every listed object and returns the local paths DuckDB
// reads, plus an idempotent release for all of them. A failed acquisition
// releases what it already holds, as the LogQL source does.
func (e *sqlExec) acquire(ctx context.Context, t tenant.ID, files []catalog.File) ([]string, func(), error) {
	paths := make([]string, 0, len(files))
	var releases []func()
	release := sync.OnceFunc(func() {
		for _, r := range releases {
			r()
		}
	})
	for _, f := range files {
		p, rel, err := e.resolve.Acquire(ctx, t, f.Key)
		if err != nil {
			release()
			return nil, sync.OnceFunc(func() {}), fmt.Errorf("query: sql: acquire %s: %w", f.Key, err)
		}
		paths = append(paths, p)
		releases = append(releases, rel)
	}
	return paths, release, nil
}

// sqlScanRows reads up to limit+1 rows from rows, converts them to the
// response contract and reports truncation: the limit+1th row, when it
// exists, only sets Truncated and is dropped.
func sqlScanRows(ctx context.Context, rows *sql.Rows, limit int) (sqlResult, error) {
	types, err := rows.ColumnTypes()
	if err != nil {
		return sqlResult{}, fmt.Errorf("query: sql: column types: %w", err)
	}
	cols := make([]sqlColumn, len(types))
	for i, ct := range types {
		kind, err := sqlColumnKind(ct.DatabaseTypeName(), ct.ScanType())
		if err != nil {
			// An unsupported result type is the user's query shape: 400.
			return sqlResult{}, fmt.Errorf("%w: column %q: %w", errSQLRejected, ct.Name(), err)
		}
		cols[i] = sqlColumn{Name: ct.Name(), Type: kind, DatabaseType: ct.DatabaseTypeName()}
	}
	// Rows starts as an empty slice, not nil: the frozen response contract
	// carries an array, so an empty result encodes as "rows":[] rather than
	// "rows":null.
	res := sqlResult{Columns: cols, Rows: [][]any{}}
	cells := make([]any, len(types))
	scan := make([]any, len(types))
	for i := range scan {
		scan[i] = &cells[i]
	}
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return sqlResult{}, fmt.Errorf("query: sql: scan: %w", err)
		}
		// The row past the limit only proves truncation: it is never scanned
		// or converted, so a discarded non-finite or unsupported cell cannot
		// fail data the response actually returns.
		if len(res.Rows) == limit {
			res.Truncated = true
			break
		}
		if err := rows.Scan(scan...); err != nil {
			return sqlResult{}, fmt.Errorf("query: sql: scan row: %w", err)
		}
		row := make([]any, len(cells))
		for i, v := range cells {
			c, err := sqlCellValue(v, cols[i].DatabaseType)
			if err != nil {
				return sqlResult{}, fmt.Errorf("query: sql: column %q: %w", cols[i].Name, err)
			}
			row[i] = c
		}
		res.Rows = append(res.Rows, row)
	}
	if err := rows.Err(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return sqlResult{}, fmt.Errorf("query: sql: scan: %w", errors.Join(err, ctxErr))
		}
		if isOutOfMemory(err) {
			return sqlResult{}, fmt.Errorf("%w %w", errSQLMemory, err)
		}
		return sqlResult{}, fmt.Errorf("%w: %w", errSQLRejected, err)
	}
	return res, nil
}

// sqlColumnKind maps one column's DuckDB type name and driver scan type to
// the response type: time, number, string, boolean or json. The scan types
// are the ones the pinned driver hands back per column type (duckdb-go
// rows.go getScanType), so columns classify without parsing type names. An
// unmappable column is an error, not a guess.
func sqlColumnKind(dbType string, scanType reflect.Type) (string, error) {
	// SQLNULL columns (SELECT NULL) report no scan type; every value is nil,
	// so any kind works and string is the documented answer.
	if scanType == nil && dbType == "SQLNULL" {
		return "string", nil
	}
	switch scanType {
	case reflect.TypeFor[bool]():
		return "boolean", nil
	case reflect.TypeFor[int8](), reflect.TypeFor[int16](), reflect.TypeFor[int32](), reflect.TypeFor[int64](),
		reflect.TypeFor[uint8](), reflect.TypeFor[uint16](), reflect.TypeFor[uint32](), reflect.TypeFor[uint64](),
		reflect.TypeFor[float32](), reflect.TypeFor[float64](),
		reflect.TypeFor[*big.Int](), reflect.TypeFor[duckdb.Decimal]():
		return "number", nil
	case reflect.TypeFor[time.Time]():
		// Instant types encode as UTC RFC3339Nano strings; TIME and TIME
		// WITH TIME ZONE are clock values, not instants, and classify as
		// strings.
		switch dbType {
		case "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS", "DATE":
			return "time", nil
		}
		return "string", nil
	case reflect.TypeFor[string](), reflect.TypeFor[[]byte]():
		return "string", nil
	case reflect.TypeFor[duckdb.Interval](), reflect.TypeFor[duckdb.Union](),
		reflect.TypeFor[duckdb.OrderedMap](), reflect.TypeFor[map[string]any](),
		reflect.TypeFor[[]any](), reflect.TypeFor[any]():
		return "json", nil
	}
	return "", fmt.Errorf("unsupported column type %s (scan %v)", dbType, scanType)
}

// sqlCellValue converts one scanned driver value into the response contract.
// dbType is the column's DuckDB type name, needed only to tell UUID bytes
// from blobs. Composite values convert recursively with the same scalar
// rules.
func sqlCellValue(v any, dbType string) (any, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case bool:
		return x, nil
	case string:
		return x, nil
	case time.Time:
		// TIME and TIME WITH TIME ZONE are clock values, not instants: the
		// driver scans them as a year-1 datetime, so only the clock part
		// (and offset, for TIMETZ) encodes.
		switch dbType {
		case "TIME":
			return x.Format("15:04:05.999999"), nil
		case "TIMETZ":
			return x.Format("15:04:05.999999Z07:00"), nil
		}
		return sqlTimeValue(x)
	case int8:
		return jsontext.Value(strconv.FormatInt(int64(x), 10)), nil
	case int16:
		return jsontext.Value(strconv.FormatInt(int64(x), 10)), nil
	case int32:
		return jsontext.Value(strconv.FormatInt(int64(x), 10)), nil
	case int64:
		return jsontext.Value(strconv.FormatInt(x, 10)), nil
	case uint8:
		return jsontext.Value(strconv.FormatUint(uint64(x), 10)), nil
	case uint16:
		return jsontext.Value(strconv.FormatUint(uint64(x), 10)), nil
	case uint32:
		return jsontext.Value(strconv.FormatUint(uint64(x), 10)), nil
	case uint64:
		return jsontext.Value(strconv.FormatUint(x, 10)), nil
	case float32:
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return nil, fmt.Errorf("%w: non-finite FLOAT value", errSQLRejected)
		}
		return jsontext.Value(strconv.FormatFloat(float64(x), 'g', -1, 32)), nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, fmt.Errorf("%w: non-finite DOUBLE value", errSQLRejected)
		}
		return jsontext.Value(strconv.FormatFloat(x, 'g', -1, 64)), nil
	case *big.Int:
		return jsontext.Value(x.String()), nil
	case duckdb.Decimal:
		return jsontext.Value(x.String()), nil
	case duckdb.Interval:
		return map[string]any{
			"months": jsontext.Value(strconv.FormatInt(int64(x.Months), 10)),
			"days":   jsontext.Value(strconv.FormatInt(int64(x.Days), 10)),
			"micros": jsontext.Value(strconv.FormatInt(x.Micros, 10)),
		}, nil
	case duckdb.Union:
		val, err := sqlCellValue(x.Value, dbType)
		if err != nil {
			return nil, err
		}
		return map[string]any{"tag": x.Tag, "value": val}, nil
	case duckdb.OrderedMap:
		keys, vals := x.Keys(), x.Values()
		m := make(map[string]any, x.Len())
		for i, k := range keys {
			key, err := sqlMapKey(k)
			if err != nil {
				return nil, err
			}
			val, err := sqlCellValue(vals[i], dbType)
			if err != nil {
				return nil, err
			}
			m[key] = val
		}
		return m, nil
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, val := range x {
			c, err := sqlCellValue(val, dbType)
			if err != nil {
				return nil, err
			}
			m[k] = c
		}
		return m, nil
	case []any:
		l := make([]any, len(x))
		for i, val := range x {
			c, err := sqlCellValue(val, dbType)
			if err != nil {
				return nil, err
			}
			l[i] = c
		}
		return l, nil
	case []byte:
		if dbType == "UUID" && len(x) == 16 {
			return fmt.Sprintf("%x-%x-%x-%x-%x", x[0:4], x[4:6], x[6:8], x[8:10], x[10:16]), nil
		}
		// BLOB and GEOMETRY encode as standard base64 strings; JSON cannot
		// carry raw bytes.
		return base64.StdEncoding.EncodeToString(x), nil
	}
	return nil, fmt.Errorf("%w: unsupported value type %T", errSQLRejected, v)
}

// sqlTimeValue normalizes one instant to UTC and rejects years the frozen
// response contract cannot encode. RFC 3339 (and therefore the JSON encoder)
// only carries four-digit years, so 0..9999 is the accepted range. DuckDB's
// infinities scan as far-out years (DATE infinity is year 5881580; TIMESTAMPTZ
// infinity and -infinity are years 294247 and -290308) and a finite DATE can
// exceed 9999, so these are the user's query shape and fail as errSQLRejected
// before the encoder instead of becoming a 500. Composite values reach this
// guard through sqlCellValue's recursion.
func sqlTimeValue(t time.Time) (time.Time, error) {
	u := t.UTC()
	if y := u.Year(); y < 0 || y > 9999 {
		return time.Time{}, fmt.Errorf("%w: temporal value year %d is outside the JSON RFC 3339 range 0-9999", errSQLRejected, y)
	}
	return u, nil
}

// sqlMapKey stringifies one DuckDB map key for a JSON object: JSON object
// keys are strings, so scalars use their text form and anything else is
// rejected rather than silently reformatted.
func sqlMapKey(k any) (string, error) {
	switch x := k.(type) {
	case string:
		return x, nil
	case bool:
		return strconv.FormatBool(x), nil
	case int8:
		return strconv.FormatInt(int64(x), 10), nil
	case int16:
		return strconv.FormatInt(int64(x), 10), nil
	case int32:
		return strconv.FormatInt(int64(x), 10), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case uint8:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(x), 10), nil
	case uint64:
		return strconv.FormatUint(x, 10), nil
	}
	return "", fmt.Errorf("%w: map key type %T is not representable", errSQLRejected, k)
}
