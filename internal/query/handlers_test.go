package query

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// queryResponseJSON is the subset of Loki's query response the handler tests
// inspect.
type queryResponseJSON struct {
	Status string `json:"status"`
	Data   struct {
		ResultType    plan.ResultType `json:"resultType"`
		EncodingFlags []string        `json:"encodingFlags"`
		Result        []struct {
			Stream map[string]string `json:"stream"`
			Values [][]any           `json:"values"`
		} `json:"result"`
		Stats struct {
			Summary struct {
				ExecTime             float64 `json:"execTime"`
				TotalEntriesReturned int64   `json:"totalEntriesReturned"`
			} `json:"summary"`
		} `json:"stats"`
	} `json:"data"`
}

// unmarshalJSON decodes a response body, adding context to the error.
func unmarshalJSON(t *testing.T, data []byte, v any) error {
	t.Helper()
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("decode response %q: %w", data, err)
	}
	return nil
}

// newQueryRequest builds a GET query-string or POST form request carrying the
// tenant in its context, as the server middleware does in production.
func newQueryRequest(t *testing.T, method, target string, form url.Values, id tenant.ID) *http.Request {
	t.Helper()
	var r *http.Request
	if method == http.MethodPost {
		r = httptest.NewRequest(method, target, strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		r = httptest.NewRequest(method, target+"?"+form.Encode(), nil)
	}
	return r.WithContext(tenant.With(r.Context(), id))
}

func TestServiceQueryGET(t *testing.T) {
	deps := newTestDeps(t, nil)
	ts := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	engine := &recordingEngine{result: plan.Result{
		Type: plan.TypeStreams,
		Streams: []plan.Stream{{
			Labels: logs.FromMap(map[string]string{"service_name": "api"}),
			Entries: []plan.Entry{
				{TS: ts, Line: "one"},
				{TS: ts.Add(time.Second), Line: "two"},
			},
		}},
	}}
	svc := newTestService(t, deps, engine)

	form := url.Values{
		"query":     {`{service_name="api"}`},
		"time":      {"1700000000"},
		"limit":     {"7"},
		"direction": {"forward"},
	}
	r := newQueryRequest(t, http.MethodGet, "/loki/api/v1/query", form, "acme")
	r.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels,foo,categorize-labels")
	w := httptest.NewRecorder()
	svc.readRoute(http.HandlerFunc(svc.handleQuery)).ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body)
	}
	call := engine.call(t, 0)
	want := plan.Params{
		Tenant:    "acme",
		Query:     `{service_name="api"}`,
		Start:     time.Unix(1700000000, 0),
		End:       time.Unix(1700000000, 0),
		Limit:     7,
		Direction: plan.Forward,
	}
	if call.params != want {
		t.Errorf("params = %+v, want %+v", call.params, want)
	}
	if call.src != svc.src {
		t.Error("engine did not receive the service's source")
	}
	if call.hasDeadline {
		t.Error("engine context has a deadline, want none without a configured timeout")
	}

	var body queryResponseJSON
	if err := unmarshalJSON(t, w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "success" {
		t.Errorf("status = %q, want success", body.Status)
	}
	if body.Data.ResultType != plan.TypeStreams {
		t.Errorf("resultType = %q, want streams", body.Data.ResultType)
	}
	if want := []string{"categorize-labels", "foo"}; !slices.Equal(body.Data.EncodingFlags, want) {
		t.Errorf("encodingFlags = %v, want %v", body.Data.EncodingFlags, want)
	}
	if len(body.Data.Result) != 1 || len(body.Data.Result[0].Values) != 2 {
		t.Fatalf("result = %+v, want one stream with two entries", body.Data.Result)
	}
	if got := body.Data.Stats.Summary.TotalEntriesReturned; got != 2 {
		t.Errorf("totalEntriesReturned = %d, want 2", got)
	}
	if got := body.Data.Stats.Summary.ExecTime; got <= 0 {
		t.Errorf("execTime = %v, want a positive measured duration", got)
	}
}

func TestServiceQueryRangePOST(t *testing.T) {
	deps := newTestDeps(t, nil)
	engine := &recordingEngine{result: plan.Result{
		Type: plan.TypeMatrix,
		Matrix: []plan.Series{{
			Metric: logs.FromMap(map[string]string{"service_name": "api"}),
			Points: []plan.Point{{T: time.Unix(1700000000, 0), V: 1}, {T: time.Unix(1700000060, 0), V: 2}},
		}},
	}}
	svc := newTestService(t, deps, engine)

	form := url.Values{
		"query":     {`{service_name="api"}`},
		"start":     {"2026-11-01T09:00:00Z"},
		"end":       {"2026-11-01T11:00:00Z"},
		"step":      {"90"},
		"interval":  {"30s"},
		"limit":     {"50"},
		"direction": {"backward"},
	}
	r := newQueryRequest(t, http.MethodPost, "/loki/api/v1/query_range", form, "acme")
	w := httptest.NewRecorder()
	svc.readRoute(http.HandlerFunc(svc.handleQueryRange)).ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body)
	}

	call := engine.call(t, 0)
	want := plan.Params{
		Tenant:    "acme",
		Query:     `{service_name="api"}`,
		Start:     time.Date(2026, 11, 1, 9, 0, 0, 0, time.UTC),
		End:       time.Date(2026, 11, 1, 11, 0, 0, 0, time.UTC),
		Step:      90 * time.Second,
		Interval:  30 * time.Second,
		Limit:     50,
		Direction: plan.Backward,
	}
	if call.params != want {
		t.Errorf("params = %+v, want %+v", call.params, want)
	}
	var body queryResponseJSON
	if err := unmarshalJSON(t, w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.ResultType != plan.TypeMatrix {
		t.Errorf("resultType = %q, want matrix", body.Data.ResultType)
	}
	if got := body.Data.Stats.Summary.TotalEntriesReturned; got != 2 {
		t.Errorf("totalEntriesReturned = %d, want 2", got)
	}
}

func TestServiceQueryErrors(t *testing.T) {
	valid := url.Values{"query": {`{service_name="api"}`}}
	tests := []struct {
		name    string
		form    url.Values
		err     error
		want    int
		wantRun int
	}{
		{name: "bad parameter", form: url.Values{"query": {`{service_name="api"}`}, "limit": {"0"}}, want: http.StatusBadRequest},
		{name: "bad query", form: valid, err: fmt.Errorf("logql: %w", plan.ErrBadQuery), want: http.StatusBadRequest, wantRun: 1},
		{name: "unsupported", form: valid, err: plan.ErrUnsupported, want: http.StatusBadRequest, wantRun: 1},
		{name: "limit", form: valid, err: fmt.Errorf("query: %w", plan.ErrLimit), want: http.StatusBadRequest, wantRun: 1},
		{name: "client canceled", form: valid, err: fmt.Errorf("query: select: %w", context.Canceled), want: statusClientClosedRequest, wantRun: 1},
		{name: "deadline", form: valid, err: fmt.Errorf("query: select: %w", context.DeadlineExceeded), want: http.StatusGatewayTimeout, wantRun: 1},
		{name: "joined genuine failure", form: valid, err: errors.Join(context.Canceled, errors.New("disk on fire")), want: http.StatusInternalServerError, wantRun: 1},
		{name: "server failure", form: valid, err: errors.New("boom"), want: http.StatusInternalServerError, wantRun: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deps := newTestDeps(t, nil)
			engine := &recordingEngine{err: tc.err}
			svc := newTestService(t, deps, engine)
			r := newQueryRequest(t, http.MethodGet, "/loki/api/v1/query", tc.form, tenant.Default)
			w := httptest.NewRecorder()
			svc.readRoute(http.HandlerFunc(svc.handleQuery)).ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Errorf("status = %d, want %d; body = %s", w.Code, tc.want, w.Body)
			}
			if got := engine.callCount(); got != tc.wantRun {
				t.Errorf("engine calls = %d, want %d", got, tc.wantRun)
			}
		})
	}
}

func TestServiceQueryTimeout(t *testing.T) {
	deps := newTestDeps(t, func(c *config.Config) { c.Querier.Timeout = 25 * time.Millisecond })
	engine := &recordingEngine{query: func(ctx context.Context, _ plan.Source, _ plan.Params) (plan.Result, error) {
		<-ctx.Done()
		return plan.Result{}, ctx.Err()
	}}
	svc := newTestService(t, deps, engine)

	r := newQueryRequest(t, http.MethodGet, "/loki/api/v1/query", url.Values{"query": {`{service_name="api"}`}}, tenant.Default)
	w := httptest.NewRecorder()
	svc.readRoute(http.HandlerFunc(svc.handleQuery)).ServeHTTP(w, r)
	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d; body = %s", w.Code, http.StatusGatewayTimeout, w.Body)
	}
	call := engine.call(t, 0)
	if !call.hasDeadline {
		t.Error("engine context has no deadline, want the configured timeout")
	}
	if call.deadline.IsZero() {
		t.Error("engine context deadline is zero")
	}
}

// TestServiceQueryTimeoutWinsOverCallerDeadline proves the configured querier
// timeout, not a longer caller deadline, bounds the engine. The fake returns
// immediately, so the assertion compares the engine's observed deadline with
// the serve window plus the configured timeout: a caller deadline reaching the
// engine instead would land about ten minutes later.
func TestServiceQueryTimeoutWinsOverCallerDeadline(t *testing.T) {
	const serviceTimeout = 30 * time.Second
	deps := newTestDeps(t, func(c *config.Config) { c.Querier.Timeout = serviceTimeout })
	engine := &recordingEngine{result: plan.Result{Type: plan.TypeStreams}}
	newTestService(t, deps, engine)

	r := newQueryRequest(t, http.MethodGet, "/loki/api/v1/query", url.Values{"query": {`{service_name="api"}`}}, tenant.Default)
	callerCtx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	r = r.WithContext(callerCtx)

	w := httptest.NewRecorder()
	before := time.Now()
	deps.HTTP.Handler().ServeHTTP(w, r)
	after := time.Now()
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", w.Code, w.Body)
	}
	call := engine.call(t, 0)
	if !call.hasDeadline {
		t.Fatal("engine context has no deadline, want the configured timeout")
	}
	if low := before.Add(serviceTimeout); call.deadline.Before(low) {
		t.Errorf("engine deadline %s is before the service timeout window start %s", call.deadline, low)
	}
	if high := after.Add(serviceTimeout); call.deadline.After(high) {
		t.Errorf("engine deadline %s is after the service timeout window end %s; the caller deadline won", call.deadline, high)
	}
}

func TestResultEntries(t *testing.T) {
	tests := []struct {
		name string
		res  plan.Result
		want int64
	}{
		{"streams", plan.Result{Type: plan.TypeStreams, Streams: []plan.Stream{{Entries: make([]plan.Entry, 3)}}}, 3},
		{"matrix", plan.Result{Type: plan.TypeMatrix, Matrix: []plan.Series{{Points: make([]plan.Point, 2)}}}, 2},
		{"vector", plan.Result{Type: plan.TypeVector, Vector: []plan.Sample{{}}}, 1},
		{"scalar", plan.Result{Type: plan.TypeScalar}, 1},
		{"unknown", plan.Result{}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := resultEntries(tc.res); got != tc.want {
				t.Errorf("resultEntries = %d, want %d", got, tc.want)
			}
		})
	}
}
