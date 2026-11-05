// The SQL API's HTTP endpoint (POST /api/v1/sql): a strict JSON request, the
// tenant from the request context, a bounded wait for an execution slot, one
// sandboxed query through the executor and a fully encoded, size-bounded JSON
// response.
//
// Errors are Loki-style plain text: 400 for anything the caller can fix (JSON,
// bounds, limits, policy, the user's SQL and the memory budget), 504/499 for
// deadlines and cancellations as the rest of the querier reports them, and a
// sanitized 500 for catalog, cache and other internal faults. Details of an
// internal failure go to the log, never to the response.
package query

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	// sqlAPIPattern is the SQL endpoint's route. It is not a Loki pattern, so
	// it lives here with the handler that owns it.
	sqlAPIPattern = "POST /api/v1/sql"
	// sqlAPIMaxBodyBytes bounds the request body before it is parsed. The SQL
	// text itself has its own, smaller cap in the executor's preflight.
	sqlAPIMaxBodyBytes = 1 << 20 // 1 MiB
	// sqlAPIErrorWriteGrace bounds the plain-text error response after the
	// request deadline has already expired. The handler's read deadline stays
	// expired (a stalled body is never drained) but the write gets one finite
	// second so the caller sees the 504/499/400 instead of a reset connection.
	sqlAPIErrorWriteGrace = time.Second
)

// sqlInternalMessage is the only body an internal SQL failure says: no object
// path, catalog detail, SQL text or other tenant information.
const sqlInternalMessage = "parkana: sql: internal error"

// Bounded public texts for the client-error classes. They name the actionable
// class without echoing parser/binder snippets, SQL text, relation names or
// anything else the sandbox saw; the full error goes to the log.
const (
	sqlPublicStatement = "parkana: sql: the SQL API runs exactly one read-only SELECT statement over the logs relation"
	sqlPublicRejected  = "parkana: sql: query rejected: check the SQL syntax, column names and types"
	sqlPublicLimits    = "parkana: sql: invalid result limit; limit must be between 1 and sql.max-rows"
	sqlPublicText      = "parkana: sql: query text exceeds the 65536-byte SQL text cap or the nesting depth limit of 1000"
	sqlPublicRecursive = "parkana: sql: recursive CTEs are not supported"
	sqlPublicTooLarge  = "parkana: sql: encoded result exceeds sql.max-result-bytes"
)

var (
	// errSQLRequest rejects a request the HTTP layer itself can see is
	// malformed: JSON shape, required fields, timestamps and limits.
	errSQLRequest = errors.New("query: sql api: invalid request")
	// errSQLResultTooLarge rejects an encoded response over sql.max-result-bytes.
	errSQLResultTooLarge = errors.New("query: sql api: encoded result exceeds sql.max-result-bytes")
)

// sqlRunner executes one validated request. *sqlExec is the production
// implementation; the HTTP layer's tests supply a fake.
type sqlRunner interface {
	Query(context.Context, sqlRequest) (sqlResult, error)
}

// sqlAPIOptions are the endpoint's request-level limits, distinct from the
// executor's DuckDB budgets.
type sqlAPIOptions struct {
	MaxRows        int           // most rows an explicit request limit may ask for
	MaxResultBytes int64         // largest encoded response body
	Timeout        time.Duration // per-request SQL deadline
	MaxConcurrent  int           // most requests executing at once
}

// validate checks the endpoint's invariants.
func (o sqlAPIOptions) validate() error {
	switch {
	case o.MaxRows < 1:
		return fmt.Errorf("query: sql api: max rows must be at least 1, got %d", o.MaxRows)
	case o.MaxResultBytes < 1:
		return fmt.Errorf("query: sql api: max result bytes must be positive, got %d", o.MaxResultBytes)
	case o.Timeout <= 0:
		return fmt.Errorf("query: sql api: timeout must be positive, got %s", o.Timeout)
	case o.MaxConcurrent < 1:
		return fmt.Errorf("query: sql api: max concurrent requests must be at least 1, got %d", o.MaxConcurrent)
	}
	return nil
}

// sqlAPI is the SQL endpoint. It owns its metrics and the admission semaphore;
// the executor owns no request state and is safe for concurrent use. slots is
// sized once at construction and never mutated.
type sqlAPI struct {
	exec    sqlRunner
	metrics *sqlMetrics
	log     *slog.Logger
	opts    sqlAPIOptions
	slots   chan struct{}
}

// sqlConfigWithDefaults supplies the approved defaults only when the whole
// SQL config is zero, which is how tests build config.Config directly without
// going through config.Load. A partially configured SQL struct is returned
// unchanged so a zero limit is rejected by validation instead of silently
// becoming a default; production always loads the full defaults.
func sqlConfigWithDefaults(c config.SQL) config.SQL {
	if c != (config.SQL{}) {
		return c
	}
	return config.SQL{
		MaxRows:        10000,
		DefaultLimit:   1000,
		MaxResultBytes: 8 * config.MiB,
		Timeout:        30 * time.Second,
		MemoryLimit:    config.GiB,
		Threads:        1,
		MaxConcurrent:  2,
	}
}

// newSQLExecFromConfig builds the request executor with the endpoint's
// configured DuckDB budgets. It is separate from newSQLAPI so the HTTP layer
// can be constructed and tested over any sqlRunner.
func newSQLExecFromConfig(cfg config.SQL, cat catalog.Catalog, resolve fileResolver, log *slog.Logger) (*sqlExec, error) {
	cfg = sqlConfigWithDefaults(cfg)
	return newSQLExec(sqlOptions{
		MemoryLimitBytes: int64(cfg.MemoryLimit),
		Threads:          cfg.Threads,
		MaxRows:          cfg.MaxRows,
		DefaultLimit:     cfg.DefaultLimit,
	}, cat, resolve, log)
}

// validateSQLConfig checks every SQL setting, not only the ones the HTTP layer
// uses. newSQLAPI receives the same config as the executor, so a partially
// zero struct must fail here instead of reaching the executor or silently
// taking the fully-zero compatibility defaults.
func validateSQLConfig(c config.SQL) error {
	opts := sqlAPIOptions{
		MaxRows:        c.MaxRows,
		MaxResultBytes: int64(c.MaxResultBytes),
		Timeout:        c.Timeout,
		MaxConcurrent:  c.MaxConcurrent,
	}
	if err := opts.validate(); err != nil {
		return err
	}
	switch {
	case c.DefaultLimit < 1:
		return fmt.Errorf("query: sql api: default limit must be at least 1, got %d", c.DefaultLimit)
	case c.DefaultLimit > c.MaxRows:
		return fmt.Errorf("query: sql api: default limit %d exceeds max rows %d", c.DefaultLimit, c.MaxRows)
	case c.MemoryLimit < 1:
		return fmt.Errorf("query: sql api: memory limit must be positive, got %d", c.MemoryLimit)
	case c.Threads < 1:
		return fmt.Errorf("query: sql api: threads must be at least 1, got %d", c.Threads)
	}
	return nil
}

// newSQLAPI builds the SQL endpoint over exec and registers its metrics. exec
// comes from newSQLExecFromConfig in the querier (or a fake in tests); the
// catalog, object cache and DuckDB handle it reads are not owned here. A nil
// registerer leaves the metrics unregistered, as everywhere else in the
// querier.
func newSQLAPI(cfg config.SQL, exec sqlRunner, reg prometheus.Registerer, log *slog.Logger) (*sqlAPI, error) {
	if exec == nil {
		return nil, errors.New("query: sql api: executor is required")
	}
	cfg = sqlConfigWithDefaults(cfg)
	if err := validateSQLConfig(cfg); err != nil {
		return nil, err
	}
	opts := sqlAPIOptions{
		MaxRows:        cfg.MaxRows,
		MaxResultBytes: int64(cfg.MaxResultBytes),
		Timeout:        cfg.Timeout,
		MaxConcurrent:  cfg.MaxConcurrent,
	}
	metrics, err := newSQLMetrics(reg)
	if err != nil {
		return nil, err
	}
	return &sqlAPI{
		exec:    exec,
		metrics: metrics,
		log:     cmpLogger(log),
		opts:    opts,
		slots:   make(chan struct{}, opts.MaxConcurrent),
	}, nil
}

// close unregisters the endpoint's metrics. It is called from the querier's
// shutdown and from every construction rollback after newSQLAPI succeeded;
// calling it more than once is harmless.
func (a *sqlAPI) close() {
	if a != nil {
		a.metrics.unregister()
	}
}

// ServeHTTP implements http.Handler. The querier registers it through the
// shared read wrapper (readRoute), so service cancellation, the read timeout,
// admission bookkeeping and shutdown waiting cover SQL exactly like LogQL.
func (a *sqlAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	outcome := "internal"
	var rows, resultBytes int64
	defer func() {
		a.metrics.observe(outcome, time.Since(start), rows, resultBytes)
	}()

	// The read wrapper already joined service cancellation and the querier
	// timeout; this adds the SQL deadline, and WithTimeout keeps whichever
	// deadline is sooner. The deadline starts before the body is read, and the
	// connection's read and write deadlines are pinned to it so a slow client
	// cannot hold the handler (or a shutdown) open.
	ctx, cancel := context.WithTimeout(r.Context(), a.opts.Timeout)
	defer cancel()
	deadlines := armSQLDeadlines(w, ctx)
	defer deadlines.close()
	fail := func(err error, bodyComplete bool) {
		// net/http cancels the request context when the connection read
		// deadline (pinned to this same deadline) expires, so a real SQL
		// deadline can race with a parent context reporting Canceled. Once
		// the deadline has elapsed, the request is a deadline, not a client
		// cancellation; a genuine cancellation before the deadline stays 499.
		deadlinePassed := sqlDeadlinePassed(ctx)
		outcome = sqlOutcome(err, deadlinePassed)
		// A request whose body was not fully read (deadline, cancellation,
		// shutdown or the body cap) must not reuse the connection: net/http
		// would block draining the rest, and an expired read deadline makes
		// the response undeliverable. Closing after the bounded error is both
		// correct and cheap.
		if !bodyComplete || ctx.Err() != nil {
			w.Header().Set("Connection", "close")
		}
		// The cancellation hook may have advanced the write deadline to now.
		// Stop it and wait for it, then give only the error response a finite
		// write grace: clearing the deadlines here would let a stalled client
		// hold the handler open forever. The deferred close clears both
		// deadlines once the response is complete.
		deadlines.boundErrorWrite()
		a.respondError(w, ctx, err, outcome, deadlinePassed)
	}

	req, bodyComplete, err := decodeSQLAPIRequest(ctx, r, a.opts.MaxRows)
	if err != nil {
		fail(err, bodyComplete)
		return
	}

	release, err := a.acquire(ctx)
	if err != nil {
		fail(err, bodyComplete)
		return
	}
	defer release()
	a.metrics.admit()
	defer a.metrics.release()

	req.Tenant = tenant.From(ctx)
	res, err := a.exec.Query(ctx, req)
	if err != nil {
		fail(err, bodyComplete)
		return
	}

	body, err := encodeSQLResult(res, a.opts.MaxResultBytes)
	if err != nil {
		fail(err, bodyComplete)
		return
	}
	// Encoding can outlive the deadline; a canceled or expired request must
	// never turn into a 200 afterwards.
	if err := ctx.Err(); err != nil {
		fail(err, bodyComplete)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	n, werr := w.Write(body)
	if werr == nil && n != len(body) {
		// A writer that reports a short write without an error still produced
		// a truncated response; treat it exactly like a failed write.
		werr = io.ErrShortWrite
	}
	if werr != nil {
		// The success header is already on the wire, so this cannot become an
		// error response. Record what actually happened: a deadline/canceled
		// context keeps that outcome, otherwise the peer went away or the
		// connection failed. Only fully written responses count as successes
		// and contribute rows and bytes.
		outcome = sqlWriteOutcome(ctx)
		a.log.Debug("sql response write failed", "outcome", outcome, "written", n, "err", werr)
		return
	}
	outcome = "success"
	rows = int64(len(res.Rows))
	resultBytes = int64(n)
}

// sqlWriteOutcome classifies a failed response write. A canceled or expired
// request keeps its context outcome; any other write failure means the peer
// stopped reading, which is a client cancellation, not a server fault.
func sqlWriteOutcome(ctx context.Context) string {
	if sqlDeadlinePassed(ctx) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "deadline"
	}
	return "canceled"
}

// sqlDeadlines owns the connection's read and write deadlines for one SQL
// request. The deadlines start at the request deadline and the cancellation
// hook advances both to now, so a stalled body read or a blocked write cannot
// outlive the request or hold shutdown open. boundErrorWrite replaces the
// expired write deadline with a finite grace for the error response; close
// clears both deadlines after the response so a keep-alive connection is
// reusable. Response writers without deadline support (httptest.ResponseRecorder,
// wrapped writers without Unwrap) report errors that are deliberately ignored.
type sqlDeadlines struct {
	rc   *http.ResponseController
	stop func() bool
	done chan struct{}
	wait func()
}

// armSQLDeadlines installs the request deadlines and the cancellation hook.
func armSQLDeadlines(w http.ResponseWriter, ctx context.Context) *sqlDeadlines {
	d := &sqlDeadlines{rc: http.NewResponseController(w), done: make(chan struct{})}
	if deadline, ok := ctx.Deadline(); ok {
		_ = d.rc.SetReadDeadline(deadline)
		_ = d.rc.SetWriteDeadline(deadline)
	}
	if ctx.Err() != nil {
		// An already-canceled request must not depend on the hook goroutine
		// being scheduled before the handler writes its error response.
		now := time.Now()
		_ = d.rc.SetReadDeadline(now)
		_ = d.rc.SetWriteDeadline(now)
	}
	d.stop = context.AfterFunc(ctx, func() {
		defer close(d.done)
		now := time.Now()
		_ = d.rc.SetReadDeadline(now)
		_ = d.rc.SetWriteDeadline(now)
	})
	// The wait runs at most once, so a later close after boundErrorWrite
	// cannot block on a hook that was stopped before it ever ran.
	d.wait = sync.OnceFunc(func() {
		if !d.stop() {
			<-d.done
		}
	})
	return d
}

// boundErrorWrite stops the cancellation hook and gives the error response a
// finite write grace. The read deadline is left where it is (expired after a
// canceled request) so a stalled body is never drained.
func (d *sqlDeadlines) boundErrorWrite() {
	d.wait()
	_ = d.rc.SetWriteDeadline(time.Now().Add(sqlAPIErrorWriteGrace))
}

// close clears both deadlines after the response is complete.
func (d *sqlDeadlines) close() {
	d.wait()
	_ = d.rc.SetReadDeadline(time.Time{})
	_ = d.rc.SetWriteDeadline(time.Time{})
}

// acquire waits for one execution slot. The wait is bounded by ctx, which
// carries the request, service and SQL deadlines, so a queued request can
// never outlive the deadline that covers it.
func (a *sqlAPI) acquire(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case a.slots <- struct{}{}:
		return func() { <-a.slots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// respondError logs a failed request at a level that matches whose fault it is
// and writes the mapped plain-text response. Detailed errors, including any
// parser or binder text, stay in the log; the response carries only the
// bounded public classification.
func (a *sqlAPI) respondError(w http.ResponseWriter, ctx context.Context, err error, outcome string, deadlinePassed bool) {
	level := slog.LevelDebug
	if outcome == "internal" {
		level = slog.LevelError
	}
	a.log.Log(ctx, level, "sql request failed", "outcome", outcome, "err", err)
	writeSQLError(w, err, deadlinePassed)
}

// sqlOutcome maps a request failure to one bounded metrics outcome.
func sqlOutcome(err error, deadlinePassed bool) string {
	switch {
	case errors.Is(err, errSQLRequest),
		errors.Is(err, errSQLRecursive),
		errors.Is(err, errSQLStatement),
		errors.Is(err, errSQLLimits),
		errors.Is(err, errSQLRejected),
		errors.Is(err, errSQLText),
		errors.Is(err, errSQLMemory),
		errors.Is(err, errSQLResultTooLarge):
		return "client_error"
	}
	deadline, canceled, other := classifySQLContextAt(err, deadlinePassed)
	switch {
	case other || (deadline && canceled):
		return "internal"
	case deadline:
		return "deadline"
	case canceled:
		return "canceled"
	case sqlParserError(err):
		return "client_error"
	default:
		return "internal"
	}
}

// writeSQLError maps an error to the SQL endpoint's HTTP response. SQL policy
// and request failures are 400 and say exactly what the caller can fix; the
// memory budget reports the public sentinel text, never the raw driver error.
// Beyond those, only an unambiguous context outcome becomes a timeout or a
// cancellation; a genuine failure joined with one keeps a sanitized 500 so the
// real fault is not masked and no internal detail leaks.
func writeSQLError(w http.ResponseWriter, err error, deadlinePassed bool) {
	switch {
	case errors.Is(err, errSQLRequest):
		// Request-validation messages are built here and carry no SQL text.
		lokiapi.Error(w, http.StatusBadRequest, err.Error())
		return
	case errors.Is(err, errSQLMemory):
		lokiapi.Error(w, http.StatusBadRequest, errSQLMemory.Error())
		return
	case errors.Is(err, errSQLText):
		lokiapi.Error(w, http.StatusBadRequest, sqlPublicText)
		return
	case errors.Is(err, errSQLRecursive):
		// Checked before errSQLStatement: the executor wraps the dedicated
		// sentinel in the generic statement error, and recursion has its own
		// actionable text.
		lokiapi.Error(w, http.StatusBadRequest, sqlPublicRecursive)
		return
	case errors.Is(err, errSQLStatement):
		lokiapi.Error(w, http.StatusBadRequest, sqlPublicStatement)
		return
	case errors.Is(err, errSQLLimits):
		lokiapi.Error(w, http.StatusBadRequest, sqlPublicLimits)
		return
	case errors.Is(err, errSQLRejected):
		lokiapi.Error(w, http.StatusBadRequest, sqlPublicRejected)
		return
	case errors.Is(err, errSQLResultTooLarge):
		lokiapi.Error(w, http.StatusBadRequest, sqlPublicTooLarge)
		return
	}
	deadline, canceled, other := classifySQLContextAt(err, deadlinePassed)
	switch {
	case other || (deadline && canceled):
		lokiapi.Error(w, http.StatusInternalServerError, sqlInternalMessage)
	case deadline:
		lokiapi.Error(w, http.StatusGatewayTimeout, lokiErrDeadlineExceeded)
	case canceled:
		lokiapi.Error(w, statusClientClosedRequest, lokiErrClientCanceled)
	case sqlParserError(err):
		// The executor normally wraps this in errSQLRejected; the raw
		// preparation path only loses that wrapper when a context is joined.
		lokiapi.Error(w, http.StatusBadRequest, sqlPublicRejected)
	default:
		lokiapi.Error(w, http.StatusInternalServerError, sqlInternalMessage)
	}
}

// classifySQLContext is classifyContext for the SQL sandbox. The executor
// intentionally joins DuckDB's typed interrupt error with the context error:
// when a deadline or cancellation stops a query, the driver reports
// ErrorTypeInterrupt, which does not wrap context.Canceled/DeadlineExceeded.
// A typed parser error can be joined the same way when preparation outlasts the
// deadline: the parser is still a user-SQL failure, so that leaf is neutral too
// and the context outcome decides (504/499). A genuine store or catalog failure
// joined into the same tree still reports other and keeps the sanitized 500.
func classifySQLContext(err error) (deadline, canceled, other bool) {
	return classifySQLContextAt(err, false)
}

// classifySQLContextAt is classifySQLContext with the request deadline's
// observed state. net/http cancels the request context when the connection read
// deadline expires, and that deadline is pinned to the SQL deadline; a
// context.Canceled leaf can therefore be the local deadline racing with the
// parent cancellation. Once the deadline has passed, a Canceled leaf is the
// deadline, while a genuine cancellation before it stays 499. A genuine store
// or catalog failure in the same tree still reports other and stays 500.
func classifySQLContextAt(err error, deadlinePassed bool) (deadline, canceled, other bool) {
	if err == nil {
		return false, false, false
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		for _, inner := range e.Unwrap() {
			d, c, o := classifySQLContextAt(inner, deadlinePassed)
			deadline, canceled, other = deadline || d, canceled || c, other || o
		}
		return deadline, canceled, other
	case interface{ Unwrap() error }:
		return classifySQLContextAt(e.Unwrap(), deadlinePassed)
	}
	switch {
	case errors.Is(err, errMetaTimeout), errors.Is(err, context.DeadlineExceeded):
		return true, false, false
	case errors.Is(err, context.Canceled):
		if deadlinePassed {
			return true, false, false
		}
		return false, true, false
	}
	if derr, ok := errors.AsType[*duckdb.Error](err); ok {
		switch derr.Type {
		case duckdb.ErrorTypeInterrupt, duckdb.ErrorTypeParser:
			return false, false, false
		}
	}
	return false, false, true
}

// sqlDeadlinePassed reports whether ctx's deadline has elapsed. It is the
// tie-breaker for the read-deadline/parent-cancellation race: the handler sets
// the connection read deadline to this same deadline, and net/http cancels the
// request context when that read fails, so ctx.Err() can be Canceled even
// though the SQL deadline is what actually ended the request.
func sqlDeadlinePassed(ctx context.Context) bool {
	deadline, ok := ctx.Deadline()
	return ok && !time.Now().Before(deadline)
}

// sqlContextError returns the context outcome for a failed body read. A read
// that timed out on the handler's own deadline is a deadline even when net/http
// has already canceled the parent context; a real client cancellation before
// the deadline stays Canceled.
func sqlContextError(ctx context.Context, readErr error) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) && (sqlDeadlinePassed(ctx) || sqlReadTimeout(readErr)) {
		return context.DeadlineExceeded
	}
	return err
}

// sqlReadTimeout reports whether err is a network timeout, which is how the
// connection read deadline set by armSQLDeadlines surfaces.
func sqlReadTimeout(err error) bool {
	ne, ok := errors.AsType[net.Error](err)
	return ok && ne.Timeout()
}

// sqlParserError reports whether err carries DuckDB's typed parser error. The
// executor normally wraps a parser failure in errSQLRejected, but the raw
// preparation path joins the driver error with the context when preparation is
// interrupted. Without a context error the parser failure is still the user's
// SQL and maps to 400, never to the sanitized 500.
func sqlParserError(err error) bool {
	derr, ok := errors.AsType[*duckdb.Error](err)
	return ok && derr.Type == duckdb.ErrorTypeParser
}

// decodeSQLAPIRequest reads and validates one request. The body is capped
// before parsing; encoding/json/v2 rejects duplicate object member names and
// trailing data, and the field walk rejects unknown members, JSON null,
// required-field omissions and wrong JSON types with a precise message. Parse
// and read failures are reported generically: the raw decoder error can carry
// JSON content or I/O detail, and an unknown member name is bounded before it
// is echoed. bodyComplete reports whether the whole body was consumed; a
// capped or failed read may leave bytes on the connection, so the handler must
// not keep it alive.
func decodeSQLAPIRequest(ctx context.Context, r *http.Request, maxRows int) (req sqlRequest, bodyComplete bool, err error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, sqlAPIMaxBodyBytes+1))
	if err != nil {
		// A canceled or expired request that was blocked reading a slow body
		// is a deadline/cancellation, not a malformed request.
		if ctxErr := sqlContextError(ctx, err); ctxErr != nil {
			return sqlRequest{}, false, ctxErr
		}
		return sqlRequest{}, false, badSQLRequestf("could not read request body")
	}
	if len(body) > sqlAPIMaxBodyBytes {
		return sqlRequest{}, false, badSQLRequestf("request body exceeds the %d-byte limit", sqlAPIMaxBodyBytes)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return sqlRequest{}, true, badSQLRequestf("request body is empty")
	}

	var fields map[string]jsontext.Value
	if err := json.Unmarshal(body, &fields); err != nil {
		return sqlRequest{}, true, badSQLRequestf("request body must be a valid JSON object")
	}
	if fields == nil {
		return sqlRequest{}, true, badSQLRequestf("request body must be a JSON object, not null")
	}
	for _, name := range slices.Sorted(maps.Keys(fields)) {
		switch name {
		case "query", "from", "to", "limit":
		default:
			// The name is caller-controlled and can be as large as the body
			// cap; echo at most 64 bytes of it.
			return sqlRequest{}, true, badSQLRequestf("unknown field %.64q", name)
		}
	}

	query, err := sqlStringField(fields, "query")
	if err != nil {
		return sqlRequest{}, true, err
	}
	if strings.TrimSpace(query) == "" {
		return sqlRequest{}, true, badSQLRequestf("field %q must not be empty", "query")
	}
	from, err := sqlTimeField(fields, "from")
	if err != nil {
		return sqlRequest{}, true, err
	}
	to, err := sqlTimeField(fields, "to")
	if err != nil {
		return sqlRequest{}, true, err
	}
	if !from.Before(to) {
		return sqlRequest{}, true, badSQLRequestf("field %q must be before %q", "from", "to")
	}
	limit, err := sqlLimitField(fields, maxRows)
	if err != nil {
		return sqlRequest{}, true, err
	}
	return sqlRequest{Query: query, From: from, To: to, Limit: limit}, true, nil
}

// badSQLRequestf wraps one request-validation message in errSQLRequest.
func badSQLRequestf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errSQLRequest, fmt.Sprintf(format, args...))
}

// sqlField returns one member's raw value. A present JSON null is rejected:
// null is not an absent optional field, and every SQL field has one type.
func sqlField(fields map[string]jsontext.Value, name string) (jsontext.Value, bool, error) {
	raw, ok := fields[name]
	if !ok {
		return nil, false, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, true, badSQLRequestf("field %q must not be null", name)
	}
	return raw, true, nil
}

// sqlStringField returns a required string member.
func sqlStringField(fields map[string]jsontext.Value, name string) (string, error) {
	raw, ok, err := sqlField(fields, name)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", badSQLRequestf("missing required field %q", name)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", badSQLRequestf("field %q must be a string", name)
	}
	return s, nil
}

// sqlTimeField returns a required RFC3339Nano timestamp member, normalized to
// UTC. The zero time (year 1) is not a usable bound and is rejected.
func sqlTimeField(fields map[string]jsontext.Value, name string) (time.Time, error) {
	s, err := sqlStringField(fields, name)
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, badSQLRequestf("field %q must be an RFC3339Nano timestamp", name)
	}
	if t.IsZero() {
		return time.Time{}, badSQLRequestf("field %q must not be the zero timestamp", name)
	}
	return t.UTC(), nil
}

// sqlLimitField returns the optional row limit: 0 when the member is absent,
// meaning the executor applies the configured default. An explicit limit must
// be a JSON integer in [1, maxRows].
func sqlLimitField(fields map[string]jsontext.Value, maxRows int) (int, error) {
	raw, ok, err := sqlField(fields, "limit")
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	var limit int
	if err := json.Unmarshal(raw, &limit); err != nil {
		return 0, badSQLRequestf("field %q must be an integer", "limit")
	}
	if limit < 1 || limit > maxRows {
		return 0, badSQLRequestf("field %q must be between 1 and %d", "limit", maxRows)
	}
	return limit, nil
}

// encodeSQLResult renders the response into memory while a bounded writer
// stops it as soon as it would exceed maxBytes, so an oversized result fails
// before any success header or partial body is written. The returned bytes are
// the complete response body.
func encodeSQLResult(res sqlResult, maxBytes int64) ([]byte, error) {
	bounded := &sqlLimitWriter{max: maxBytes}
	if err := json.MarshalEncode(jsontext.NewEncoder(bounded), res); err != nil {
		if errors.Is(err, errSQLResultTooLarge) {
			return nil, fmt.Errorf("%w: encoded result exceeds the %d-byte limit", errSQLResultTooLarge, maxBytes)
		}
		return nil, fmt.Errorf("query: sql api: encode result: %w", err)
	}
	return bounded.buf.Bytes(), nil
}

// sqlLimitWriter buffers JSON output up to max bytes and fails the write that
// would cross it, so the encoder never buffers more than the response cap.
type sqlLimitWriter struct {
	buf bytes.Buffer
	max int64
}

// Write appends p or fails with errSQLResultTooLarge when it would overflow.
func (w *sqlLimitWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.max-int64(w.buf.Len()) {
		return 0, errSQLResultTooLarge
	}
	return w.buf.Write(p)
}
