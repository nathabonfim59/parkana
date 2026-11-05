package query

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// sqlFakeRunner is a sqlRunner test double: it records every request and
// either returns canned values or runs a hook.
type sqlFakeRunner struct {
	mu     sync.Mutex
	calls  []sqlRequest
	result sqlResult
	err    error
	hook   func(context.Context, sqlRequest) (sqlResult, error)
}

func (f *sqlFakeRunner) Query(ctx context.Context, req sqlRequest) (sqlResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, req)
	hook := f.hook
	f.mu.Unlock()
	if hook != nil {
		return hook(ctx, req)
	}
	return f.result, f.err
}

func (f *sqlFakeRunner) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *sqlFakeRunner) lastCall(t *testing.T) sqlRequest {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		t.Fatal("sql runner was not called")
	}
	return f.calls[len(f.calls)-1]
}

// sqlTestConfig is a small explicit SQL config: room for a couple of
// requests, a bounded response and a short deadline.
func sqlTestConfig() config.SQL {
	return config.SQL{
		MaxRows:        100,
		DefaultLimit:   10,
		MaxResultBytes: 4096,
		Timeout:        time.Second,
		MemoryLimit:    config.GiB,
		Threads:        1,
		MaxConcurrent:  1,
	}
}

// newSQLTestAPI builds the endpoint under test. A nil registry leaves the
// metrics unregistered.
func newSQLTestAPI(t *testing.T, runner sqlRunner, reg prometheus.Registerer, mutate func(*config.SQL)) *sqlAPI {
	t.Helper()
	cfg := sqlTestConfig()
	if mutate != nil {
		mutate(&cfg)
	}
	api, err := newSQLAPI(cfg, runner, reg, nil)
	if err != nil {
		t.Fatalf("newSQLAPI: %v", err)
	}
	return api
}

// sqlValidBody returns a valid request body for query.
func sqlValidBody(query string) string {
	return fmt.Sprintf(`{"query":%q,"from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z"}`, query)
}

// serveSQL posts body to h with ctx and returns the recorder.
func serveSQL(t *testing.T, h http.Handler, ctx context.Context, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sql", strings.NewReader(body)).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// sqlMetricValue returns one sample's value from a gathered counter or gauge,
// or fails when the sample is absent. Histograms are not used by these tests.
func sqlMetricValue(t *testing.T, g prometheus.Gatherer, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if len(m.GetLabel()) != len(labels) {
				continue
			}
			match := true
			for _, lp := range m.GetLabel() {
				if labels[lp.GetName()] != lp.GetValue() {
					match = false
				}
			}
			if match {
				switch {
				case m.GetCounter() != nil:
					return m.GetCounter().GetValue()
				case m.GetGauge() != nil:
					return m.GetGauge().GetValue()
				}
				t.Fatalf("metric %s sample is neither a counter nor a gauge", name)
			}
		}
	}
	t.Fatalf("metric %s with labels %v not found", name, labels)
	return 0
}

// TestSQLAPIDecodeRejectsInvalidRequests covers the strict request contract:
// every malformed body is a 400, the runner is never entered and the response
// contains the actionable class.
func TestSQLAPIDecodeRejectsInvalidRequests(t *testing.T) {
	valid := sqlValidBody("SELECT 1")
	tests := []struct {
		name string
		body string
		want string
	}{
		{"empty body", "", "empty"},
		{"top-level null", "null", "must be a JSON object"},
		{"top-level array", "[]", "must be a valid JSON object"},
		{"malformed", `{"query":`, "must be a valid JSON object"},
		{"unknown field", `{"query":"SELECT 1","from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z","extra":1}`, `unknown field "extra"`},
		{"duplicate field", `{"query":"SELECT 1","query":"SELECT 2","from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z"}`, "must be a valid JSON object"},
		{"trailing data", valid + ` {}`, "must be a valid JSON object"},
		{"null query", `{"query":null,"from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z"}`, `field "query" must not be null`},
		{"missing query", `{"from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z"}`, `missing required field "query"`},
		{"missing from", `{"query":"SELECT 1","to":"2024-01-02T00:00:00Z"}`, `missing required field "from"`},
		{"missing to", `{"query":"SELECT 1","from":"2024-01-01T00:00:00Z"}`, `missing required field "to"`},
		{"query wrong type", `{"query":1,"from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z"}`, `field "query" must be a string`},
		{"from wrong type", `{"query":"SELECT 1","from":1,"to":"2024-01-02T00:00:00Z"}`, `field "from" must be a string`},
		{"from not RFC3339Nano", `{"query":"SELECT 1","from":"yesterday","to":"2024-01-02T00:00:00Z"}`, "RFC3339Nano"},
		{"to not RFC3339Nano", `{"query":"SELECT 1","from":"2024-01-01T00:00:00Z","to":"2024-13-02T00:00:00Z"}`, "RFC3339Nano"},
		{"zero from", `{"query":"SELECT 1","from":"0001-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z"}`, "zero timestamp"},
		{"equal bounds", `{"query":"SELECT 1","from":"2024-01-01T00:00:00Z","to":"2024-01-01T00:00:00Z"}`, `field "from" must be before "to"`},
		{"reversed bounds", `{"query":"SELECT 1","from":"2024-01-02T00:00:00Z","to":"2024-01-01T00:00:00Z"}`, `field "from" must be before "to"`},
		{"empty query", sqlValidBody("   "), `field "query" must not be empty`},
		{"limit zero", `{"query":"SELECT 1","from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z","limit":0}`, `field "limit" must be between 1 and 100`},
		{"limit negative", `{"query":"SELECT 1","from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z","limit":-1}`, `field "limit" must be between 1 and 100`},
		{"limit above max", `{"query":"SELECT 1","from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z","limit":101}`, `field "limit" must be between 1 and 100`},
		{"limit fractional", `{"query":"SELECT 1","from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z","limit":1.5}`, `field "limit" must be an integer`},
		{"limit null", `{"query":"SELECT 1","from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z","limit":null}`, `field "limit" must not be null`},
		{"limit string", `{"query":"SELECT 1","from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z","limit":"1"}`, `field "limit" must be an integer`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner := new(sqlFakeRunner)
			api := newSQLTestAPI(t, runner, nil, nil)
			rec := serveSQL(t, api, t.Context(), tc.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body = %q", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.want) {
				t.Errorf("body = %q, want it to contain %q", rec.Body.String(), tc.want)
			}
			if got := runner.callCount(); got != 0 {
				t.Errorf("runner called %d times for an invalid request", got)
			}
		})
	}
}

// TestSQLAPIBodyCap checks the 1 MiB body cap is applied before parsing.
func TestSQLAPIBodyCap(t *testing.T) {
	runner := new(sqlFakeRunner)
	api := newSQLTestAPI(t, runner, nil, nil)
	body := `{"query":"` + strings.Repeat("x", sqlAPIMaxBodyBytes) + `"}`
	rec := serveSQL(t, api, t.Context(), body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "body exceeds") {
		t.Errorf("body = %q, want a body-size error", rec.Body.String())
	}
	if got := len(rec.Body.String()); got > 256 {
		t.Errorf("oversized-body error is %d bytes, want a bounded message", got)
	}
	if got := rec.Header().Get("Connection"); got != "close" {
		t.Errorf("Connection = %q, want close for a body left unread", got)
	}
	if runner.callCount() != 0 {
		t.Error("runner ran for an oversized body")
	}
}

// TestSQLAPIRequestContext pins what the handler hands the executor: the
// tenant from the context, the parsed half-open bounds, the explicit limit and
// an RFC3339Nano response for the numeric token and NULL cells.
func TestSQLAPIRequestContext(t *testing.T) {
	runner := &sqlFakeRunner{result: sqlResult{
		Columns: []sqlColumn{
			{Name: "n", Type: "number", DatabaseType: "BIGINT"},
			{Name: "s", Type: "string", DatabaseType: "VARCHAR"},
		},
		Rows: [][]any{
			{jsontext.Value("12345678901234567890"), nil},
		},
		Truncated: true,
	}}
	api := newSQLTestAPI(t, runner, nil, nil)
	ctx := tenant.With(t.Context(), tenant.ID("acme"))
	body := `{"query":"SELECT n, s FROM logs","from":"2024-05-06T07:08:09.123456789Z","to":"2024-05-06T08:00:00Z","limit":7}`
	rec := serveSQL(t, api, ctx, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %q", rec.Code, rec.Body.String())
	}
	called := runner.lastCall(t)
	if called.Tenant != tenant.ID("acme") {
		t.Errorf("tenant = %q, want acme", called.Tenant)
	}
	if want := time.Date(2024, 5, 6, 7, 8, 9, 123456789, time.UTC); !called.From.Equal(want) {
		t.Errorf("from = %s, want %s", called.From, want)
	}
	if want := time.Date(2024, 5, 6, 8, 0, 0, 0, time.UTC); !called.To.Equal(want) {
		t.Errorf("to = %s, want %s", called.To, want)
	}
	if called.Limit != 7 {
		t.Errorf("limit = %d, want 7", called.Limit)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("content type = %q, want application/json", got)
	}
	if !strings.Contains(rec.Body.String(), `"rows":[[12345678901234567890,null]]`) {
		t.Errorf("body lost the exact numeric token or NULL: %q", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"truncated":true`) {
		t.Errorf("body does not report truncation: %q", rec.Body.String())
	}
}

// TestSQLAPIDefaultLimit checks that an omitted limit reaches the executor as
// 0, which is how the executor applies sql.default-limit.
func TestSQLAPIDefaultLimit(t *testing.T) {
	runner := &sqlFakeRunner{result: sqlResult{Rows: [][]any{}}}
	api := newSQLTestAPI(t, runner, nil, nil)
	rec := serveSQL(t, api, t.Context(), sqlValidBody("SELECT 1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := runner.lastCall(t).Limit; got != 0 {
		t.Errorf("limit = %d, want 0 (executor default)", got)
	}
}

// TestSQLAPIResultByteOverflow proves an oversized encoded result is a 400
// before any success body is written: no partial JSON with "columns" reaches
// the recorder.
func TestSQLAPIResultByteOverflow(t *testing.T) {
	runner := &sqlFakeRunner{result: sqlResult{
		Columns: []sqlColumn{{Name: "s", Type: "string", DatabaseType: "VARCHAR"}},
		Rows:    [][]any{{strings.Repeat("x", 512)}},
	}}
	api := newSQLTestAPI(t, runner, nil, func(c *config.SQL) { c.MaxResultBytes = 256 })
	rec := serveSQL(t, api, t.Context(), sqlValidBody("SELECT s FROM logs"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "sql.max-result-bytes") {
		t.Errorf("body = %q, want the flag name", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), `"columns"`) {
		t.Errorf("partial success JSON leaked into the error body: %q", rec.Body.String())
	}
}

// TestSQLAPIUnknownFieldNameIsBounded checks a huge unknown member name is not
// echoed in full into the error body.
func TestSQLAPIUnknownFieldNameIsBounded(t *testing.T) {
	runner := new(sqlFakeRunner)
	api := newSQLTestAPI(t, runner, nil, nil)
	name := strings.Repeat("z", 4096)
	body := `{"query":"SELECT 1","from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z","` + name + `":1}`
	rec := serveSQL(t, api, t.Context(), body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if strings.Contains(rec.Body.String(), strings.Repeat("z", 100)) {
		t.Errorf("unknown field name was echoed beyond the bound: %q", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "unknown field") {
		t.Errorf("body = %q, want an unknown-field error", rec.Body.String())
	}
}

// TestSQLAPIConfigDefaults pins the compatibility fallback: only a fully zero
// SQL config gets the approved defaults; a partial config keeps its zero
// fields and fails validation instead of silently defaulting them.
func TestSQLAPIConfigDefaults(t *testing.T) {
	zero := sqlConfigWithDefaults(config.SQL{})
	want := config.SQL{
		MaxRows:        10000,
		DefaultLimit:   1000,
		MaxResultBytes: 8 * config.MiB,
		Timeout:        30 * time.Second,
		MemoryLimit:    config.GiB,
		Threads:        1,
		MaxConcurrent:  2,
	}
	if zero != want {
		t.Fatalf("zero config defaults = %+v, want %+v", zero, want)
	}
	partial := config.SQL{MaxRows: 10}
	if got := sqlConfigWithDefaults(partial); got != partial {
		t.Errorf("partial config = %+v, want it unchanged", got)
	}
	if _, err := newSQLAPI(partial, new(sqlFakeRunner), nil, nil); err == nil {
		t.Fatal("newSQLAPI accepted a partial config with zero limits")
	}
	// The HTTP options are all valid here; the executor budget is not. A
	// partial struct must still be rejected rather than silently defaulted.
	partialBudget := config.SQL{
		MaxRows:        10,
		DefaultLimit:   5,
		MaxResultBytes: 1024,
		Timeout:        time.Second,
		MaxConcurrent:  1,
	}
	if _, err := newSQLAPI(partialBudget, new(sqlFakeRunner), nil, nil); err == nil {
		t.Fatal("newSQLAPI accepted a partial config with zero memory and threads")
	}
	if _, err := newSQLAPI(config.SQL{}, new(sqlFakeRunner), nil, nil); err != nil {
		t.Fatalf("newSQLAPI rejected the fully zero compatibility config: %v", err)
	}
}

// TestSQLAPIAdmissionAndRelease proves the concurrent-execution cap: a second
// request waits, its own deadline bounds the wait, and the slot is released
// for the next request.
func TestSQLAPIAdmissionAndRelease(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	runner := &sqlFakeRunner{hook: func(ctx context.Context, _ sqlRequest) (sqlResult, error) {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return sqlResult{Rows: [][]any{}}, nil
		case <-ctx.Done():
			return sqlResult{}, ctx.Err()
		}
	}}
	api := newSQLTestAPI(t, runner, nil, func(c *config.SQL) {
		c.MaxConcurrent = 1
		c.Timeout = 5 * time.Second
	})

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- serveSQL(t, api, context.WithoutCancel(t.Context()), sqlValidBody("SELECT 1"))
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first request never reached the runner")
	}

	// The second request must not enter the runner: it queues and its own
	// short context deadline ends the wait with a timeout response.
	waitCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	rec := serveSQL(t, api, waitCtx, sqlValidBody("SELECT 1"))
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("queued request status = %d, want 504; body = %q", rec.Code, rec.Body.String())
	}
	if runner.callCount() != 1 {
		t.Fatalf("runner called %d times, want only the admitted request", runner.callCount())
	}

	close(release)
	select {
	case rec := <-firstDone:
		if rec.Code != http.StatusOK {
			t.Fatalf("first request status = %d, want 200; body = %q", rec.Code, rec.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not finish")
	}

	// The slot is free again.
	rec = serveSQL(t, api, t.Context(), sqlValidBody("SELECT 1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("post-release status = %d, want 200; body = %q", rec.Code, rec.Body.String())
	}
}

// TestSQLAPICancellation proves a canceled request context ends the handler
// promptly with the repository's client-cancel status.
func TestSQLAPICancellation(t *testing.T) {
	entered := make(chan struct{})
	runner := &sqlFakeRunner{hook: func(ctx context.Context, _ sqlRequest) (sqlResult, error) {
		close(entered)
		<-ctx.Done()
		return sqlResult{}, ctx.Err()
	}}
	api := newSQLTestAPI(t, runner, nil, nil)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- serveSQL(t, api, ctx, sqlValidBody("SELECT 1")) }()
	<-entered
	cancel()
	select {
	case rec := <-done:
		if rec.Code != statusClientClosedRequest {
			t.Fatalf("status = %d, want %d; body = %q", rec.Code, statusClientClosedRequest, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "cancelled by the client") {
			t.Errorf("body = %q, want the client-cancel text", rec.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled request did not return")
	}
}

// TestSQLAPICanceledAfterDeadlineIs504 pins the deadline/cancellation race
// tie-breaker: once the request deadline has elapsed, a context.Canceled
// returned by the executor (as net/http's parent cancellation can produce when
// the connection read deadline fires) is a deadline, not a client cancellation.
func TestSQLAPICanceledAfterDeadlineIs504(t *testing.T) {
	runner := &sqlFakeRunner{hook: func(ctx context.Context, _ sqlRequest) (sqlResult, error) {
		<-ctx.Done()
		return sqlResult{}, context.Canceled
	}}
	api := newSQLTestAPI(t, runner, nil, func(c *config.SQL) {
		c.Timeout = 100 * time.Millisecond
	})
	rec := serveSQL(t, api, t.Context(), sqlValidBody("SELECT 1"))
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504 after the deadline elapsed; body = %q", rec.Code, rec.Body.String())
	}
	if runner.callCount() != 1 {
		t.Fatalf("runner called %d times, want the deadline override path", runner.callCount())
	}
}

// timeoutError is a net.Error whose Timeout is true, matching the connection
// read deadline's error.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// TestSQLAPIContextErrorDeadlineRace proves a timed-out body read is a deadline
// even when the parent request context already reports Canceled, while a real
// client read failure before the deadline stays a cancellation.
func TestSQLAPIContextErrorDeadlineRace(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if got := sqlContextError(canceled, timeoutError{}); !errors.Is(got, context.DeadlineExceeded) {
		t.Errorf("timed-out read with canceled parent = %v, want DeadlineExceeded", got)
	}
	if got := sqlContextError(canceled, io.ErrUnexpectedEOF); !errors.Is(got, context.Canceled) {
		t.Errorf("client read failure = %v, want Canceled", got)
	}
}

// TestSQLAPIClassifyContextAtDeadline covers the deadline override itself,
// including that a genuine catalog fault joined with it still stays internal.
func TestSQLAPIClassifyContextAtDeadline(t *testing.T) {
	tests := []struct {
		name                      string
		err                       error
		deadlinePassed            bool
		deadline, canceled, other bool
	}{
		{"canceled after deadline", context.Canceled, true, true, false, false},
		{"canceled before deadline", context.Canceled, false, false, true, false},
		{"canceled+catalog after deadline", errors.Join(context.Canceled, errors.New("catalog failed")), true, true, false, true},
		{"deadline", context.DeadlineExceeded, false, true, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deadline, canceled, other := classifySQLContextAt(tc.err, tc.deadlinePassed)
			if deadline != tc.deadline || canceled != tc.canceled || other != tc.other {
				t.Fatalf("classifySQLContextAt = (%t, %t, %t), want (%t, %t, %t)",
					deadline, canceled, other, tc.deadline, tc.canceled, tc.other)
			}
		})
	}
}

// TestSQLAPIErrorMapping pins every status class and, for SQL failures, that
// the public body is the bounded class text rather than raw parser, binder or
// path detail.
func TestSQLAPIErrorMapping(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode int
		wantBody string
		leak     string
	}{
		{"statement", fmt.Errorf("%w: relation \"secret_relation\" is not allowed", errSQLStatement), 400, sqlPublicStatement, "secret_relation"},
		{"rejected", fmt.Errorf("%w: Parser Error: syntax error near /etc/passwd", errSQLRejected), 400, sqlPublicRejected, "/etc/passwd"},
		{"limits", fmt.Errorf("%w: 500 is above the maximum", errSQLLimits), 400, sqlPublicLimits, "500"},
		{"text", fmt.Errorf("%w: 70000 bytes exceeds the cap", errSQLText), 400, sqlPublicText, "70000"},
		{"text names both bounds", fmt.Errorf("%w: 65537 bytes", errSQLText), 400, "65536-byte", "65537"},
		{"recursive", fmt.Errorf("%w: %w", errSQLStatement, errSQLRecursive), 400, sqlPublicRecursive, "SELECT statement"},
		{"memory", fmt.Errorf("%w driver_detail=/secret/path", errSQLMemory), 400, errSQLMemory.Error(), "driver_detail"},
		{"deadline", context.DeadlineExceeded, 504, lokiErrDeadlineExceeded, ""},
		{"canceled", context.Canceled, statusClientClosedRequest, lokiErrClientCanceled, ""},
		{"interrupt joined deadline", errors.Join(sqlInterruptError(), context.DeadlineExceeded), 504, lokiErrDeadlineExceeded, "INTERRUPT"},
		{"interrupt joined canceled", errors.Join(sqlInterruptError(), context.Canceled), statusClientClosedRequest, lokiErrClientCanceled, "INTERRUPT"},
		{"interrupt joined catalog", errors.Join(sqlInterruptError(), context.DeadlineExceeded, errors.New("catalog /secret/path broke")), 500, sqlInternalMessage, "/secret/path"},
		{"interrupt alone", sqlInterruptError(), 500, sqlInternalMessage, "INTERRUPT"},
		{"parser joined deadline", errors.Join(sqlParserTestError(), context.DeadlineExceeded), 504, lokiErrDeadlineExceeded, "Parser Error"},
		{"parser joined canceled", errors.Join(sqlParserTestError(), context.Canceled), statusClientClosedRequest, lokiErrClientCanceled, "Parser Error"},
		{"parser joined catalog", errors.Join(sqlParserTestError(), context.DeadlineExceeded, errors.New("catalog /secret/path broke")), 500, sqlInternalMessage, "/secret/path"},
		{"parser alone", sqlParserTestError(), 400, sqlPublicRejected, "Parser Error"},
		{"internal", errors.New("catalog: /secret/path broke"), 500, sqlInternalMessage, "/secret/path"},
		{"mixed", errors.Join(context.DeadlineExceeded, errors.New("cache /secret/path")), 500, sqlInternalMessage, "/secret/path"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner := &sqlFakeRunner{err: tc.err}
			api := newSQLTestAPI(t, runner, nil, nil)
			rec := serveSQL(t, api, t.Context(), sqlValidBody("SELECT 1"))
			if rec.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d; body = %q", rec.Code, tc.wantCode, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tc.wantBody) {
				t.Errorf("body = %q, want it to contain %q", rec.Body.String(), tc.wantBody)
			}
			if tc.leak != "" && strings.Contains(rec.Body.String(), tc.leak) {
				t.Errorf("body leaked %q: %q", tc.leak, rec.Body.String())
			}
		})
	}
}

// TestSQLAPIMetrics checks the success and client-error counters, the row and
// byte totals and that inflight returns to zero.
func TestSQLAPIMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	runner := &sqlFakeRunner{result: sqlResult{
		Columns: []sqlColumn{{Name: "n", Type: "number", DatabaseType: "INTEGER"}},
		Rows:    [][]any{{jsontext.Value("1")}},
	}}
	api := newSQLTestAPI(t, runner, reg, nil)
	if rec := serveSQL(t, api, t.Context(), sqlValidBody("SELECT 1")); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := sqlMetricValue(t, reg, "parkana_sql_requests_total", map[string]string{"outcome": "success"}); got != 1 {
		t.Errorf("success requests = %v, want 1", got)
	}
	if got := sqlMetricValue(t, reg, "parkana_sql_rows_total", nil); got != 1 {
		t.Errorf("rows = %v, want 1", got)
	}
	if got := sqlMetricValue(t, reg, "parkana_sql_result_bytes_total", nil); got <= 0 {
		t.Errorf("result bytes = %v, want positive", got)
	}
	if got := sqlMetricValue(t, reg, "parkana_sql_inflight", nil); got != 0 {
		t.Errorf("inflight = %v, want 0 after the request", got)
	}

	runner.err = errSQLRejected
	runner.result = sqlResult{}
	if rec := serveSQL(t, api, t.Context(), sqlValidBody("SELECT 1")); rec.Code != http.StatusBadRequest {
		t.Fatalf("failure status = %d, want 400", rec.Code)
	}
	if got := sqlMetricValue(t, reg, "parkana_sql_requests_total", map[string]string{"outcome": "client_error"}); got != 1 {
		t.Errorf("client_error requests = %v, want 1", got)
	}
	if got := sqlMetricValue(t, reg, "parkana_sql_inflight", nil); got != 0 {
		t.Errorf("inflight = %v, want 0 after a failed request", got)
	}
}

// TestSQLAPIMetricsRegistrationRollback proves a partial registration failure
// leaves no SQL collector behind.
func TestSQLAPIMetricsRegistrationRollback(t *testing.T) {
	reg := prometheus.NewRegistry()
	squatter := prometheus.NewCounter(prometheus.CounterOpts{Name: "parkana_sql_result_bytes_total", Help: "already here"})
	if err := reg.Register(squatter); err != nil {
		t.Fatal(err)
	}
	if _, err := newSQLMetrics(reg); err == nil {
		t.Fatal("newSQLMetrics succeeded despite a name collision")
	}
	if metricPresent(t, reg, "parkana_sql_requests_total") {
		t.Error("the requests counter registered before the collision was not rolled back")
	}
	if !metricPresent(t, reg, "parkana_sql_result_bytes_total") {
		t.Error("the pre-registered squatter was unregistered")
	}
}

// TestSQLAPIRoute and method checks go through a ServeMux, so the POST-only
// pattern is what rejects other methods.
func TestSQLAPIMethodAndRoute(t *testing.T) {
	api := newSQLTestAPI(t, new(sqlFakeRunner), nil, nil)
	mux := http.NewServeMux()
	mux.Handle(sqlAPIPattern, api)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/sql", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", rec.Code)
	}
}

// TestSQLAPIServiceWiring proves the real constructor registers the SQL route
// once through the read wrapper, serves a real SELECT and rejects non-POST.
func TestSQLAPIServiceWiring(t *testing.T) {
	deps := newTestDeps(t, nil)
	newTestService(t, deps, &recordingEngine{})

	if !deps.HTTP.Registered(sqlAPIPattern) {
		t.Fatalf("route %q is not registered", sqlAPIPattern)
	}
	handler := deps.HTTP.Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/sql", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405; body = %q", rec.Code, rec.Body.String())
	}

	rec = serveSQL(t, handler, t.Context(), sqlValidBody("SELECT 1 AS n"))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST status = %d, want 200; body = %q", rec.Code, rec.Body.String())
	}
	var body struct {
		Columns   []sqlColumn `json:"columns"`
		Rows      [][]any     `json:"rows"`
		Truncated bool        `json:"truncated"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Columns) != 1 || body.Columns[0].Name != "n" || body.Columns[0].Type != "number" {
		t.Errorf("columns = %+v, want one number column n", body.Columns)
	}
	if len(body.Rows) != 1 || len(body.Rows[0]) != 1 {
		t.Errorf("rows = %v, want one row with one cell", body.Rows)
	}
}

// TestSQLAPIRealRecursiveRejection proves the reviewed sandbox's dedicated
// errSQLRecursive sentinel reaches the HTTP layer as its own 400, before the
// generic single-statement text.
func TestSQLAPIRealRecursiveRejection(t *testing.T) {
	deps := newTestDeps(t, nil)
	newTestService(t, deps, &recordingEngine{})
	rec := serveSQL(t, deps.HTTP.Handler(), t.Context(),
		sqlValidBody("WITH RECURSIVE t AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM t WHERE n < 3) SELECT * FROM t"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body = %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), sqlPublicRecursive) {
		t.Errorf("body = %q, want the dedicated recursive text", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), sqlPublicStatement) {
		t.Errorf("body = %q, want the generic statement text suppressed", rec.Body.String())
	}
}

// TestSQLAPIRealDeadlineClassification runs an interrupted real DuckDB query
// through the registered endpoint: the executor joins the driver's typed
// INTERRUPT error with the context deadline, and the HTTP layer must still
// answer 504, not 500. A later request proves the deadline cleaned up.
func TestSQLAPIRealDeadlineClassification(t *testing.T) {
	deps := newTestDeps(t, func(c *config.Config) {
		c.SQL = config.SQL{
			MaxRows:        10000,
			DefaultLimit:   1000,
			MaxResultBytes: 8 * config.MiB,
			Timeout:        100 * time.Millisecond,
			MemoryLimit:    512 * config.MiB,
			Threads:        1,
			MaxConcurrent:  2,
		}
	})
	newTestService(t, deps, &recordingEngine{})
	handler := deps.HTTP.Handler()

	rec := serveSQL(t, handler, t.Context(), sqlValidBody("SELECT count(*) FROM range(100000000000)"))
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("timed-out status = %d, want 504; body = %q", rec.Code, rec.Body.String())
	}

	rec = serveSQL(t, handler, t.Context(), sqlValidBody("SELECT 1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("follow-up status = %d, want 200; body = %q", rec.Code, rec.Body.String())
	}
}

// TestSQLAPISlowBodyDeadline drives a real HTTP server with a body that stops
// mid-JSON: the SQL deadline covers the body read, so the handler answers 504
// instead of hanging and the client's read completes.
func TestSQLAPISlowBodyDeadline(t *testing.T) {
	api := newSQLTestAPI(t, new(sqlFakeRunner), nil, func(c *config.SQL) {
		c.Timeout = 100 * time.Millisecond
	})
	srv := httptest.NewServer(api)
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "POST /api/v1/sql HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: 200\r\n\r\n{\"query\":\""); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatalf("reading deadline response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", resp.StatusCode)
	}
}

// TestSQLAPIShutdownUnblocksStalledBody proves service shutdown is not held by
// a request blocked reading a slow body. A listener wrapper reports when the
// server has read the request headers and when it then reads body bytes, so the
// test observes the body read instead of sleeping and hoping the request was
// admitted; only after that barrier does it cancel the service and require
// shutdown to return.
func TestSQLAPIShutdownUnblocksStalledBody(t *testing.T) {
	deps := newTestDeps(t, func(c *config.Config) {
		c.SQL = config.SQL{
			MaxRows:        10000,
			DefaultLimit:   1000,
			MaxResultBytes: 8 * config.MiB,
			Timeout:        10 * time.Second,
			MemoryLimit:    512 * config.MiB,
			Threads:        1,
			MaxConcurrent:  2,
		}
	})
	svc := newTestService(t, deps, &recordingEngine{})
	srv := httptest.NewUnstartedServer(deps.HTTP.Handler())
	signals := &sqlReadSignalListener{
		Listener: srv.Listener,
		headers:  make(chan struct{}),
		body:     make(chan struct{}),
	}
	srv.Listener = signals
	srv.Start()
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Send only the headers first: the server cannot read body bytes until the
	// test explicitly sends them below, so each signal identifies one phase.
	if _, err := fmt.Fprintf(conn, "POST /api/v1/sql HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: 4096\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	waitSQLSignal(t, signals.headers, "request headers")
	if _, err := fmt.Fprint(conn, `{"query":"`); err != nil {
		t.Fatal(err)
	}
	waitSQLSignal(t, signals.body, "request body")

	done := make(chan error, 1)
	go func() { done <- svc.shutdown() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not unblock the stalled body read")
	}
}

// sqlReadSignalListener wraps a listener so a test can observe that the server
// read the request headers and then read body bytes.
type sqlReadSignalListener struct {
	net.Listener
	headers chan struct{}
	body    chan struct{}
	hOnce   sync.Once
	bOnce   sync.Once
}

// Accept wraps each connection.
func (l *sqlReadSignalListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &sqlReadSignalConn{Conn: conn, listener: l}, nil
}

// sqlReadSignalConn reports header and body reads on the wrapped connection.
type sqlReadSignalConn struct {
	net.Conn
	listener *sqlReadSignalListener
	mu       sync.Mutex
	header   []byte
	sawHead  bool
}

// Read observes the bytes net/http reads without changing them.
func (c *sqlReadSignalConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n == 0 {
		return n, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sawHead {
		c.listener.bOnce.Do(func() { close(c.listener.body) })
		return n, err
	}
	c.header = append(c.header, p[:n]...)
	if i := bytes.Index(c.header, []byte("\r\n\r\n")); i >= 0 {
		c.sawHead = true
		c.listener.hOnce.Do(func() { close(c.listener.headers) })
		if i+4 < len(c.header) {
			c.listener.bOnce.Do(func() { close(c.listener.body) })
		}
		c.header = nil
	}
	return n, err
}

// waitSQLSignal waits for one observable phase with a test timeout.
func waitSQLSignal(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("did not observe %s", name)
	}
}

// TestSQLAPIShutdownReleasesRouteAndMetrics proves shutdown unregisters the
// SQL metrics, so the registry is reusable, and the read wrapper refuses new
// SQL requests.
func TestSQLAPIShutdownReleasesRouteAndMetrics(t *testing.T) {
	deps := newTestDeps(t, nil)
	svc := newTestService(t, deps, &recordingEngine{})
	if err := svc.shutdown(); err != nil {
		t.Fatal(err)
	}
	rec := serveSQL(t, deps.HTTP.Handler(), t.Context(), sqlValidBody("SELECT 1"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status after shutdown = %d, want 503; body = %q", rec.Code, rec.Body.String())
	}
	// A fresh collector group can register: shutdown unregistered the old one.
	metrics, err := newSQLMetrics(deps.Metrics)
	if err != nil {
		t.Fatalf("SQL metrics still registered after shutdown: %v", err)
	}
	metrics.unregister()
}

// TestSQLAPIConstructionMetricCollision proves a SQL metric collision fails
// construction, registers no route and closes the caches and DuckDB handle
// cleanly: the same registry can build every collector group again.
func TestSQLAPIConstructionMetricCollision(t *testing.T) {
	deps := newTestDeps(t, nil)
	squatter := prometheus.NewCounter(prometheus.CounterOpts{Name: "parkana_sql_result_bytes_total", Help: "already here"})
	if err := deps.Metrics.Register(squatter); err != nil {
		t.Fatal(err)
	}
	s, err := newService(t.Context(), deps, &recordingEngine{})
	if s != nil {
		s.shutdown()
		t.Fatal("newService succeeded despite a SQL metric collision")
	}
	if err == nil || !strings.Contains(err.Error(), "register") {
		t.Fatalf("newService error = %v, want a registration failure", err)
	}
	for _, pattern := range allReadPatterns() {
		if deps.HTTP.Registered(pattern) {
			t.Errorf("route %q registered despite construction failure", pattern)
		}
	}
	gather, ok := deps.Metrics.(prometheus.Gatherer)
	if !ok {
		t.Fatalf("deps.Metrics (%T) cannot be gathered", deps.Metrics)
	}
	if seriesPresent(t, gather, "parkana_sql_requests_total", map[string]string{"outcome": "success"}) {
		t.Error("the requests counter registered before the collision was not rolled back")
	}
	// Every other collector group is reusable.
	cache, err := newObjectCache(cacheOptions{store: deps.Store, local: true, register: deps.Metrics})
	if err != nil {
		t.Fatalf("cache metrics leaked from the failed constructor: %v", err)
	}
	if err := closeCache(cache); err != nil {
		t.Fatal(err)
	}
	sidecars, err := newSidecarCache(sidecarCacheOptions{store: deps.Store, budget: 64 << 20, register: deps.Metrics})
	if err != nil {
		t.Fatalf("sidecar metrics leaked from the failed constructor: %v", err)
	}
	if err := sidecars.Close(); err != nil {
		t.Fatal(err)
	}
	scan, err := newScanMetrics(deps.Metrics)
	if err != nil {
		t.Fatalf("scan metrics leaked from the failed constructor: %v", err)
	}
	scan.unregister()
	// client_golang keeps a name's dimension hash forever, even after
	// Unregister, so the failed collision permanently prevents that specific
	// name from registering again in this registry. The rollback proof is the
	// absent requests series above; other groups already proved reusable.
	deps.Metrics.Unregister(squatter)
}

// TestSQLAPILateCanceledConstruction proves a creator canceled while the SQL
// metrics register rolls back the metrics, the DuckDB handle and both caches.
func TestSQLAPILateCanceledConstruction(t *testing.T) {
	deps := newTestDeps(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	deps.Metrics = &cancelOnMetric{Registerer: deps.Metrics, name: "parkana_sql_requests_total", cancel: cancel}

	s, err := newService(ctx, deps, &recordingEngine{})
	if s != nil {
		s.shutdown()
		t.Fatal("newService returned a service after a canceled construction")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("newService error = %v, want context.Canceled", err)
	}
	for _, pattern := range allReadPatterns() {
		if deps.HTTP.Registered(pattern) {
			t.Errorf("route %q registered despite construction failure", pattern)
		}
	}
	cache, err := newObjectCache(cacheOptions{store: deps.Store, local: true, register: deps.Metrics})
	if err != nil {
		t.Fatalf("cache metrics leaked from the failed constructor: %v", err)
	}
	if err := closeCache(cache); err != nil {
		t.Fatal(err)
	}
	sidecars, err := newSidecarCache(sidecarCacheOptions{store: deps.Store, budget: 64 << 20, register: deps.Metrics})
	if err != nil {
		t.Fatalf("sidecar metrics leaked from the failed constructor: %v", err)
	}
	if err := sidecars.Close(); err != nil {
		t.Fatal(err)
	}
	scan, err := newScanMetrics(deps.Metrics)
	if err != nil {
		t.Fatalf("scan metrics leaked from the failed constructor: %v", err)
	}
	scan.unregister()
	metrics, err := newSQLMetrics(deps.Metrics)
	if err != nil {
		t.Fatalf("SQL metrics leaked from the failed constructor: %v", err)
	}
	metrics.unregister()
}

// sqlInterruptError returns the driver's typed interruption error, which does
// not wrap context.DeadlineExceeded or context.Canceled.
func sqlInterruptError() error {
	return &duckdb.Error{Type: duckdb.ErrorTypeInterrupt, Msg: "INTERRUPT Error: query interrupted"}
}

// sqlParserTestError returns the driver's typed parser error, as the raw
// preparation path reports it.
func sqlParserTestError() error {
	return &duckdb.Error{Type: duckdb.ErrorTypeParser, Msg: "Parser Error: Max expression depth limit of 1000 exceeded"}
}

// TestSQLAPIClassifyContextInterrupt pins the interrupt/context classification:
// the driver's typed interrupt is a side effect of the joined context, while a
// genuine catalog fault joined into the same tree still wins as internal.
func TestSQLAPIClassifyContextInterrupt(t *testing.T) {
	tests := []struct {
		name                      string
		err                       error
		deadline, canceled, other bool
	}{
		{"interrupt joined deadline", errors.Join(sqlInterruptError(), context.DeadlineExceeded), true, false, false},
		{"interrupt joined canceled", errors.Join(sqlInterruptError(), context.Canceled), false, true, false},
		{"interrupt joined catalog", errors.Join(sqlInterruptError(), context.DeadlineExceeded, errors.New("catalog failed")), true, false, true},
		{"interrupt alone", sqlInterruptError(), false, false, false},
		{"wrapped interrupt joined deadline", fmt.Errorf("execute: %w", errors.Join(sqlInterruptError(), context.DeadlineExceeded)), true, false, false},
		{"parser joined deadline", errors.Join(sqlParserTestError(), context.DeadlineExceeded), true, false, false},
		{"parser joined canceled", errors.Join(sqlParserTestError(), context.Canceled), false, true, false},
		{"parser joined catalog", errors.Join(sqlParserTestError(), context.DeadlineExceeded, errors.New("catalog failed")), true, false, true},
		{"parser alone", sqlParserTestError(), false, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deadline, canceled, other := classifySQLContext(tc.err)
			if deadline != tc.deadline || canceled != tc.canceled || other != tc.other {
				t.Fatalf("classifySQLContext = (%t, %t, %t), want (%t, %t, %t)",
					deadline, canceled, other, tc.deadline, tc.canceled, tc.other)
			}
		})
	}
}

// sqlFailingBodyReader fails the body read with an internal-looking message.
type sqlFailingBodyReader struct{}

func (sqlFailingBodyReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed near /secret/io")
}

// TestSQLAPIJSONErrorsAreBounded proves parser, read and unknown-member errors
// never echo raw JSON content, I/O detail or a body-sized field name.
func TestSQLAPIJSONErrorsAreBounded(t *testing.T) {
	runner := new(sqlFakeRunner)
	api := newSQLTestAPI(t, runner, nil, nil)

	// The malformed JSON contains a secret; only the generic class is public.
	rec := serveSQL(t, api, t.Context(), `{"query":"SELECT '/etc/passwd'","from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z"`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed status = %d, want 400", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "/etc/passwd") {
		t.Errorf("malformed body leaked JSON content: %q", rec.Body.String())
	}
	if got := len(rec.Body.String()); got > 256 {
		t.Errorf("malformed error body is %d bytes, want a bounded message", got)
	}

	// A read failure's I/O detail stays in the log, not the response.
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sql", nil).WithContext(t.Context())
	req.Body = io.NopCloser(sqlFailingBodyReader{})
	readRec := httptest.NewRecorder()
	api.ServeHTTP(readRec, req)
	if readRec.Code != http.StatusBadRequest {
		t.Fatalf("read-failure status = %d, want 400; body = %q", readRec.Code, readRec.Body.String())
	}
	if !strings.Contains(readRec.Body.String(), "could not read request body") {
		t.Errorf("read-failure body = %q, want the generic read message", readRec.Body.String())
	}
	if strings.Contains(readRec.Body.String(), "/secret/io") {
		t.Errorf("read-failure body leaked I/O detail: %q", readRec.Body.String())
	}

	// A near-cap unknown member name is echoed at most 64 bytes.
	name := strings.Repeat("z", 500_000)
	body := `{"query":"SELECT 1","from":"2024-01-01T00:00:00Z","to":"2024-01-02T00:00:00Z","` + name + `":1}`
	rec = serveSQL(t, api, t.Context(), body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown-field status = %d, want 400", rec.Code)
	}
	if strings.Contains(rec.Body.String(), strings.Repeat("z", 100)) {
		t.Errorf("unknown field name was echoed beyond the bound: %q", rec.Body.String())
	}
	if got := len(rec.Body.String()); got > 512 {
		t.Errorf("unknown-field error body is %d bytes, want a bounded message", got)
	}
}

// sqlDeadlineRecorder captures ResponseController deadline calls.
type sqlDeadlineRecorder struct {
	header   http.Header
	read     time.Time
	write    time.Time
	readSet  int
	writeSet int
}

func (d *sqlDeadlineRecorder) Header() http.Header {
	if d.header == nil {
		d.header = make(http.Header)
	}
	return d.header
}

func (d *sqlDeadlineRecorder) WriteHeader(int) {}

func (d *sqlDeadlineRecorder) Write(p []byte) (int, error) { return len(p), nil }

func (d *sqlDeadlineRecorder) SetReadDeadline(t time.Time) error {
	d.read, d.readSet = t, d.readSet+1
	return nil
}

func (d *sqlDeadlineRecorder) SetWriteDeadline(t time.Time) error {
	d.write, d.writeSet = t, d.writeSet+1
	return nil
}

// TestSQLAPIDeadlineGrace proves a failed request gets a finite write grace
// instead of having every deadline cleared before the error body is written,
// and that the final cleanup clears both deadlines for keep-alive reuse.
func TestSQLAPIDeadlineGrace(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	rec := &sqlDeadlineRecorder{}
	deadlines := armSQLDeadlines(rec, ctx)
	deadlines.boundErrorWrite()

	if rec.write.IsZero() {
		t.Fatal("boundErrorWrite left the write deadline unset")
	}
	if grace := time.Until(rec.write); grace <= 0 || grace > 2*time.Second {
		t.Fatalf("error write grace = %s, want a finite value near %s", grace, sqlAPIErrorWriteGrace)
	}
	if rec.read.IsZero() {
		t.Error("boundErrorWrite cleared the read deadline; a stalled body would be drained")
	}
	deadlines.close()
	if !rec.read.IsZero() || !rec.write.IsZero() {
		t.Errorf("close left deadlines set: read=%s write=%s", rec.read, rec.write)
	}
	deadlines.close() // idempotent, and must not block after boundErrorWrite
}

// sqlBlockingJSON blocks the response encoder until the test releases it.
type sqlBlockingJSON struct {
	entered chan struct{}
	release chan struct{}
}

func (b *sqlBlockingJSON) MarshalJSON() ([]byte, error) {
	close(b.entered)
	<-b.release
	return []byte(`"late"`), nil
}

// TestSQLAPIContextCheckedAfterEncode proves a request canceled while the
// response is being encoded never becomes a 200 or leaks the encoded body.
func TestSQLAPIContextCheckedAfterEncode(t *testing.T) {
	cell := &sqlBlockingJSON{entered: make(chan struct{}), release: make(chan struct{})}
	runner := &sqlFakeRunner{result: sqlResult{
		Columns: []sqlColumn{{Name: "s", Type: "string", DatabaseType: "VARCHAR"}},
		Rows:    [][]any{{cell}},
	}}
	api := newSQLTestAPI(t, runner, nil, nil)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- serveSQL(t, api, ctx, sqlValidBody("SELECT s FROM logs")) }()
	select {
	case <-cell.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("encoder never started")
	}
	cancel()
	close(cell.release)
	select {
	case rec := <-done:
		if rec.Code != statusClientClosedRequest {
			t.Fatalf("status = %d, want %d; body = %q", rec.Code, statusClientClosedRequest, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "late") {
			t.Errorf("encoded body leaked into the cancellation response: %q", rec.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled encode did not return")
	}
}

// sqlCaptureRunner records the executor's error so a test can prove the real
// interruption path was reached.
type sqlCaptureRunner struct {
	inner sqlRunner
	mu    sync.Mutex
	err   error
}

func (c *sqlCaptureRunner) Query(ctx context.Context, req sqlRequest) (sqlResult, error) {
	res, err := c.inner.Query(ctx, req)
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
	return res, err
}

func (c *sqlCaptureRunner) lastErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// TestSQLAPIRealExecutorInterruptAndRecovery runs a real, heavy DuckDB query
// through the real executor and the HTTP layer. The one-second deadline is far
// longer than startup over an empty catalog, and the captured error must carry
// the driver's typed interrupt joined with the context deadline: that only
// happens once the query is executing. A later request proves recovery.
func TestSQLAPIRealExecutorInterruptAndRecovery(t *testing.T) {
	deps := newTestDeps(t, func(c *config.Config) {
		c.SQL = config.SQL{
			MaxRows:        10000,
			DefaultLimit:   1000,
			MaxResultBytes: 8 * config.MiB,
			Timeout:        time.Second,
			MemoryLimit:    512 * config.MiB,
			Threads:        1,
			MaxConcurrent:  1,
		}
	})
	svc := newTestService(t, deps, &recordingEngine{})
	capture := &sqlCaptureRunner{inner: svc.sql.exec}
	api, err := newSQLAPI(deps.Config.SQL, capture, nil, deps.Logger)
	if err != nil {
		t.Fatalf("newSQLAPI: %v", err)
	}

	rec := serveSQL(t, api, t.Context(), sqlValidBody("SELECT sum(i) FROM range(10000000000) t(i)"))
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("timed-out status = %d, want 504; body = %q", rec.Code, rec.Body.String())
	}
	execErr := capture.lastErr()
	driverErr, ok := errors.AsType[*duckdb.Error](execErr)
	if !ok || driverErr.Type != duckdb.ErrorTypeInterrupt {
		t.Fatalf("executor error = %v, want a typed DuckDB interrupt from the executing query", execErr)
	}
	if !errors.Is(execErr, context.DeadlineExceeded) {
		t.Fatalf("executor error = %v, want the joined context deadline", execErr)
	}

	rec = serveSQL(t, api, t.Context(), sqlValidBody("SELECT 1"))
	if rec.Code != http.StatusOK {
		t.Fatalf("follow-up status = %d, want 200; body = %q", rec.Code, rec.Body.String())
	}
}

// TestSQLAPICappedParserClassificationProbe exercises the observed worst case:
// a cap-sized deep-left expression whose trusted parser path outlasts a tiny
// SQL deadline. It reports the executor error type and classifySQLContext's
// result so the HTTP mapping can be checked against a real parser failure.
func TestSQLAPICappedParserClassificationProbe(t *testing.T) {
	query := "SELECT " + strings.Repeat("1+", ((64<<10)-8)/2) + "1"
	for _, budget := range []time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond} {
		t.Run(budget.String(), func(t *testing.T) {
			deps := newTestDeps(t, func(c *config.Config) {
				c.SQL = config.SQL{
					MaxRows:        10000,
					DefaultLimit:   1000,
					MaxResultBytes: 8 * config.MiB,
					Timeout:        budget,
					MemoryLimit:    512 * config.MiB,
					Threads:        1,
					MaxConcurrent:  1,
				}
			})
			svc := newTestService(t, deps, &recordingEngine{})
			capture := &sqlCaptureRunner{inner: svc.sql.exec}
			api, err := newSQLAPI(deps.Config.SQL, capture, nil, deps.Logger)
			if err != nil {
				t.Fatalf("newSQLAPI: %v", err)
			}
			rec := serveSQL(t, api, t.Context(), sqlValidBody(query))
			execErr := capture.lastErr()
			deadline, canceled, other := classifySQLContext(execErr)
			driverType := duckdb.ErrorTypeInvalid
			if driverErr, ok := errors.AsType[*duckdb.Error](execErr); ok {
				driverType = driverErr.Type
			}
			t.Logf("status=%d classify=(deadline=%t canceled=%t other=%t) driverType=%v err=%v",
				rec.Code, deadline, canceled, other, driverType, execErr)
			if rec.Code != http.StatusBadRequest && rec.Code != http.StatusGatewayTimeout {
				t.Fatalf("capped deep-left status = %d, want 400 (user SQL) or 504 (deadline); err=%v", rec.Code, execErr)
			}
			if execErr != nil && other && (deadline || canceled) {
				t.Fatalf("parser/interrupt joined with a context was classified as internal: %v", execErr)
			}
		})
	}
}

// TestSQLAPIRawPrepareParserProbe exercises the measurement path directly: the
// raw execution-path prepare (no preceding validation) over the cap-sized
// deep-left expression with a short deadline. It reports the error shape and
// classification so the HTTP mapping can be checked against the observed
// parser/deadline join.
func TestSQLAPIRawPrepareParserProbe(t *testing.T) {
	query := "SELECT " + strings.Repeat("1+", ((64<<10)-8)/2) + "1"
	for _, budget := range []time.Duration{25 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond} {
		t.Run(budget.String(), func(t *testing.T) {
			sb, err := openSQLSandbox(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer sb.close()
			if err := sb.applyLimits(t.Context(), 1<<30, 1); err != nil {
				t.Fatal(err)
			}
			if err := sb.lockdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), budget)
			defer cancel()
			rows, err := sb.query(ctx, query)
			if rows != nil {
				rows.Close()
			}
			deadline, canceled, other := classifySQLContext(err)
			driverErr, typed := errors.AsType[*duckdb.Error](err)
			driverType := duckdb.ErrorTypeInvalid
			if typed {
				driverType = driverErr.Type
			}
			t.Logf("classify=(deadline=%t canceled=%t other=%t) driverType=%v err=%v",
				deadline, canceled, other, driverType, err)
			if err != nil {
				parser := typed && driverErr.Type == duckdb.ErrorTypeParser
				if parser && errors.Is(err, context.DeadlineExceeded) {
					if !deadline || canceled || other {
						t.Fatalf("raw parser+deadline classified as (%t, %t, %t), want (true, false, false)", deadline, canceled, other)
					}
					t.Log("observed raw parser+deadline -> 504")
				}
			}
		})
	}
}

// sqlWriteSignalWriter signals the first response-body write and delegates
// deadline support to the underlying writer.
type sqlWriteSignalWriter struct {
	http.ResponseWriter
	started chan struct{}
	once    *sync.Once
}

func (w *sqlWriteSignalWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	return w.ResponseWriter.Write(p)
}

func (w *sqlWriteSignalWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// sqlSmallWriteListener shrinks the accepted connection's send buffer so a
// large response blocks once the client stops reading.
type sqlSmallWriteListener struct {
	net.Listener
}

func (l *sqlSmallWriteListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetWriteBuffer(1024)
	}
	return conn, nil
}

// TestSQLAPIShutdownUnblocksBlockedWrite proves shutdown unblocks a handler
// stuck writing a large response to a client that stopped reading. The handler
// wrapper signals that the body write began, and the small send buffer keeps
// the write blocked until the shutdown hook advances the write deadline.
func TestSQLAPIShutdownUnblocksBlockedWrite(t *testing.T) {
	deps := newTestDeps(t, nil)
	svc := newTestService(t, deps, &recordingEngine{})
	big := strings.Repeat("x", 16<<20)
	runner := &sqlFakeRunner{result: sqlResult{
		Columns: []sqlColumn{{Name: "s", Type: "string", DatabaseType: "VARCHAR"}},
		Rows:    [][]any{{big}},
	}}
	api, err := newSQLAPI(config.SQL{
		MaxRows:        10,
		DefaultLimit:   10,
		MaxResultBytes: 32 << 20,
		Timeout:        10 * time.Second,
		MemoryLimit:    config.GiB,
		Threads:        1,
		MaxConcurrent:  1,
	}, runner, nil, deps.Logger)
	if err != nil {
		t.Fatalf("newSQLAPI: %v", err)
	}
	started := make(chan struct{})
	var once sync.Once
	handler := svc.readRoute(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.ServeHTTP(&sqlWriteSignalWriter{ResponseWriter: w, started: started, once: &once}, r)
	}))
	srv := httptest.NewUnstartedServer(handler)
	srv.Listener = &sqlSmallWriteListener{Listener: srv.Listener}
	srv.Start()
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	body := sqlValidBody("SELECT s FROM logs")
	if _, err := fmt.Fprintf(conn, "POST /api/v1/sql HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body); err != nil {
		t.Fatal(err)
	}
	waitSQLSignal(t, started, "response body write")

	done := make(chan error, 1)
	go func() { done <- svc.shutdown() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not unblock the blocked response write")
	}
}

// sqlFailingWriter accepts headers and then fails the body write.
type sqlFailingWriter struct {
	header http.Header
	code   int
	wrote  int
}

func (w *sqlFailingWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *sqlFailingWriter) WriteHeader(code int) { w.code = code }

func (w *sqlFailingWriter) Write(p []byte) (int, error) {
	w.wrote = min(len(p), 64)
	return w.wrote, errors.New("peer went away")
}

// TestSQLAPIWriteFailureIsNotCountedAsSuccess proves a failed body write is
// classified and does not contribute rows or bytes to the success metrics.
func TestSQLAPIWriteFailureIsNotCountedAsSuccess(t *testing.T) {
	reg := prometheus.NewRegistry()
	runner := &sqlFakeRunner{result: sqlResult{
		Columns: []sqlColumn{{Name: "s", Type: "string", DatabaseType: "VARCHAR"}},
		Rows:    [][]any{{"x"}},
	}}
	api := newSQLTestAPI(t, runner, reg, nil)
	rec := &sqlFailingWriter{}
	api.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/sql", strings.NewReader(sqlValidBody("SELECT s FROM logs"))).WithContext(t.Context()))

	if rec.code != http.StatusOK {
		t.Fatalf("status = %d, want the already-sent 200", rec.code)
	}
	if got := sqlMetricValue(t, reg, "parkana_sql_requests_total", map[string]string{"outcome": "canceled"}); got != 1 {
		t.Errorf("canceled requests = %v, want 1 for the failed write", got)
	}
	if got := sqlMetricValue(t, reg, "parkana_sql_rows_total", nil); got != 0 {
		t.Errorf("rows = %v, want 0 for a failed write", got)
	}
	if got := sqlMetricValue(t, reg, "parkana_sql_result_bytes_total", nil); got != 0 {
		t.Errorf("result bytes = %v, want 0 for a failed write", got)
	}
}
