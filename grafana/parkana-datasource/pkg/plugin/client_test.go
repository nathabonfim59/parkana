package plugin

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

var (
	testFrom = time.Date(2026, 11, 5, 12, 0, 0, 123456789, time.UTC)
	testTo   = time.Date(2026, 11, 5, 13, 0, 0, 0, time.UTC)
)

// settingsFor builds Grafana data source settings pointing at serverURL.
func settingsFor(t *testing.T, serverURL, tenant string) backend.DataSourceInstanceSettings {
	t.Helper()
	jsonData, err := json.Marshal(jsonSettings{URL: serverURL, Tenant: tenant})
	if err != nil {
		t.Fatalf("marshal settings: %v", err)
	}
	return backend.DataSourceInstanceSettings{URL: serverURL, JSONData: jsonData}
}

// newTestDatasource starts a fixture server and builds a Datasource for it.
func newTestDatasource(t *testing.T, handler http.HandlerFunc, opts options, tenant string) (*Datasource, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	ds, err := newDatasource(settingsFor(t, server.URL, tenant), opts)
	if err != nil {
		t.Fatalf("newDatasource: %v", err)
	}
	return ds, server
}

// dataQuery builds one Grafana query with the standard test time range.
func dataQuery(refID, body string) backend.DataQuery {
	return backend.DataQuery{
		RefID:         refID,
		JSON:          []byte(body),
		TimeRange:     backend.TimeRange{From: testFrom, To: testTo},
		Interval:      15 * time.Second,
		MaxDataPoints: 100,
	}
}

func TestQueryDataRequestAndResponse(t *testing.T) {
	var gotMethod, gotPath, gotTenant, gotContentType string
	var gotBody []byte

	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotTenant = r.Header.Get("X-Scope-OrgID")
		gotContentType = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{
			"columns": [
				{"name":"time","type":"time","databaseType":"TIMESTAMP"},
				{"name":"value","type":"number","databaseType":"DOUBLE"},
				{"name":"label","type":"string","databaseType":"VARCHAR"},
				{"name":"attrs","type":"json","databaseType":"JSON"}
			],
			"rows": [
				["2026-11-05T12:00:00.123456789Z", 1.5, "hello", {"a":1}],
				["2026-11-05T12:01:00Z", null, null, null]
			],
			"truncated": true
		}`)
	}, defaultOptions(), "tenant-a")

	req := &backend.QueryDataRequest{Queries: []backend.DataQuery{
		dataQuery("A", `{"rawSQL":"SELECT * FROM logs WHERE _ts >= $__timeFrom AND _ts < $__timeTo AND _ts > now() - $__interval","format":"table","limit":10}`),
	}}
	response, err := ds.QueryData(t.Context(), req)
	if err != nil {
		t.Fatalf("QueryData: %v", err)
	}

	if gotMethod != http.MethodPost || gotPath != "/api/v1/sql" {
		t.Fatalf("request = %s %s, want POST /api/v1/sql", gotMethod, gotPath)
	}
	if gotTenant != "tenant-a" {
		t.Fatalf("tenant header = %q, want tenant-a", gotTenant)
	}
	if gotContentType != "application/json" {
		t.Fatalf("content type = %q", gotContentType)
	}

	var sent sqlRequest
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatalf("decode sent body: %v", err)
	}
	wantSQL := "SELECT * FROM logs WHERE _ts >= TIMESTAMP '2026-11-05 12:00:00.123457' AND _ts < TIMESTAMP '2026-11-05 13:00:00.000000' AND _ts > now() - INTERVAL '15000 milliseconds'"
	if sent.Query != wantSQL {
		t.Fatalf("sent query = %q, want %q", sent.Query, wantSQL)
	}
	if sent.From != "2026-11-05T12:00:00.123456789Z" || sent.To != "2026-11-05T13:00:00Z" {
		t.Fatalf("sent bounds = %q .. %q", sent.From, sent.To)
	}
	if sent.Limit != 10 {
		t.Fatalf("sent limit = %d, want 10", sent.Limit)
	}

	result := response.Responses["A"]
	if result.Error != nil {
		t.Fatalf("query error: %v", result.Error)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(result.Frames))
	}
	frame := result.Frames[0]
	if frame.Meta == nil || len(frame.Meta.Notices) != 1 || !strings.Contains(frame.Meta.Notices[0].Text, "truncated") {
		t.Fatalf("truncation notice missing: %#v", frame.Meta)
	}
	if got, ok := frame.Fields[1].ConcreteAt(0); !ok || got.(float64) != 1.5 {
		t.Fatalf("value = %#v, %v", got, ok)
	}
	if _, ok := frame.Fields[1].ConcreteAt(1); ok {
		t.Fatal("null value not preserved")
	}
}

func TestQueryDataNormalizesBoundsToUTC(t *testing.T) {
	var got sqlRequest
	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		if err := json.UnmarshalRead(r.Body, &got); err != nil {
			t.Errorf("decode body: %v", err)
		}
		io.WriteString(w, `{"columns":[],"rows":[],"truncated":false}`)
	}, defaultOptions(), "")

	zone := time.FixedZone("CEST", 2*60*60)
	from := time.Date(2026, 11, 5, 14, 0, 0, 500000000, zone)
	to := time.Date(2026, 11, 5, 15, 0, 0, 0, zone)
	response, err := ds.QueryData(t.Context(), &backend.QueryDataRequest{Queries: []backend.DataQuery{{
		RefID:     "A",
		JSON:      []byte(`{"rawSQL":"SELECT 1"}`),
		TimeRange: backend.TimeRange{From: from, To: to},
	}}})
	if err != nil {
		t.Fatalf("QueryData: %v", err)
	}
	if response.Responses["A"].Error != nil {
		t.Fatalf("query error: %v", response.Responses["A"].Error)
	}
	if got.From != "2026-11-05T12:00:00.5Z" || got.To != "2026-11-05T13:00:00Z" {
		t.Fatalf("bounds = %q .. %q, want UTC", got.From, got.To)
	}
}

func TestQueryDataIntervalFromMaxDataPoints(t *testing.T) {
	var got sqlRequest
	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		if err := json.UnmarshalRead(r.Body, &got); err != nil {
			t.Errorf("decode body: %v", err)
		}
		io.WriteString(w, `{"columns":[],"rows":[],"truncated":false}`)
	}, defaultOptions(), "")

	from := testFrom
	to := from.Add(100 * time.Second)
	response, err := ds.QueryData(t.Context(), &backend.QueryDataRequest{Queries: []backend.DataQuery{{
		RefID:         "A",
		JSON:          []byte(`{"rawSQL":"SELECT $__interval"}`),
		TimeRange:     backend.TimeRange{From: from, To: to},
		MaxDataPoints: 10,
	}}})
	if err != nil {
		t.Fatalf("QueryData: %v", err)
	}
	if response.Responses["A"].Error != nil {
		t.Fatalf("query error: %v", response.Responses["A"].Error)
	}
	if want := "SELECT INTERVAL '10000 milliseconds'"; got.Query != want {
		t.Fatalf("query = %q, want %q", got.Query, want)
	}
}

func TestQueryDataMultipleQueries(t *testing.T) {
	var calls atomic.Int32
	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		io.WriteString(w, `{"columns":[{"name":"x","type":"string"}],"rows":[["ok"]],"truncated":false}`)
	}, defaultOptions(), "")

	response, err := ds.QueryData(t.Context(), &backend.QueryDataRequest{Queries: []backend.DataQuery{
		dataQuery("bad", `{"rawSQL":""}`),
		dataQuery("good", `{"rawSQL":"SELECT 'ok' AS x"}`),
	}})
	if err != nil {
		t.Fatalf("QueryData: %v", err)
	}
	if response.Responses["bad"].Error == nil {
		t.Fatal("bad query did not fail")
	}
	good := response.Responses["good"]
	if good.Error != nil || len(good.Frames) != 1 {
		t.Fatalf("good query = %#v", good)
	}
	if calls.Load() != 1 {
		t.Fatalf("endpoint calls = %d, want 1", calls.Load())
	}
}

func TestQueryDataInfersTimeSeries(t *testing.T) {
	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{
			"columns": [
				{"name":"time","type":"time","databaseType":"TIMESTAMP"},
				{"name":"value","type":"number","databaseType":"DOUBLE"}
			],
			"rows": [
				["2026-11-05T12:01:00Z", 2.0],
				["2026-11-05T12:00:00Z", 1.0]
			],
			"truncated": false
		}`)
	}, defaultOptions(), "")

	response, err := ds.QueryData(t.Context(), &backend.QueryDataRequest{Queries: []backend.DataQuery{
		dataQuery("A", `{"rawSQL":"SELECT _ts AS time, value FROM logs"}`),
	}})
	if err != nil {
		t.Fatalf("QueryData: %v", err)
	}
	result := response.Responses["A"]
	if result.Error != nil {
		t.Fatalf("query error: %v", result.Error)
	}
	frame := result.Frames[0]
	if frame.Meta == nil || frame.Meta.Type != data.FrameTypeTimeSeriesWide {
		t.Fatalf("inferred frame type = %#v", frame.Meta)
	}
	if got, _ := frame.Fields[0].ConcreteAt(0); !got.(time.Time).Equal(time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("inferred time series not sorted: %v", got)
	}
}

func TestQueryDataExplicitTable(t *testing.T) {
	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{
			"columns": [
				{"name":"time","type":"time","databaseType":"TIMESTAMP"},
				{"name":"value","type":"number","databaseType":"DOUBLE"}
			],
			"rows": [["2026-11-05T12:00:00Z", 1.0]],
			"truncated": false
		}`)
	}, defaultOptions(), "")

	response, err := ds.QueryData(t.Context(), &backend.QueryDataRequest{Queries: []backend.DataQuery{
		dataQuery("A", `{"rawSQL":"SELECT _ts AS time, value FROM logs","format":"table"}`),
	}})
	if err != nil {
		t.Fatalf("QueryData: %v", err)
	}
	frame := response.Responses["A"].Frames[0]
	if frame.Meta == nil || frame.Meta.Type != data.FrameTypeTable {
		t.Fatalf("explicit table frame type = %#v", frame.Meta)
	}
}

func TestQueryDataValidation(t *testing.T) {
	tests := []struct {
		name  string
		query backend.DataQuery
		want  string
	}{
		{
			name:  "malformed JSON",
			query: dataQuery("A", `{`),
			want:  "parse query",
		},
		{
			name:  "missing SQL",
			query: dataQuery("A", `{"rawSQL":"  ","format":"table"}`),
			want:  "rawSQL is required",
		},
		{
			name:  "unknown format",
			query: dataQuery("A", `{"rawSQL":"SELECT 1","format":"chart"}`),
			want:  "unsupported format",
		},
		{
			name: "missing range",
			query: func() backend.DataQuery {
				q := dataQuery("A", `{"rawSQL":"SELECT 1"}`)
				q.TimeRange = backend.TimeRange{}
				return q
			}(),
			want: "time range is required",
		},
		{
			name: "reversed range",
			query: func() backend.DataQuery {
				q := dataQuery("A", `{"rawSQL":"SELECT 1"}`)
				q.TimeRange = backend.TimeRange{From: testTo, To: testFrom}
				return q
			}(),
			want: "start must be before end",
		},
		{
			name:  "negative limit",
			query: dataQuery("A", `{"rawSQL":"SELECT 1","limit":-1}`),
			want:  "limit must not be negative",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
			}, defaultOptions(), "")
			response, err := ds.QueryData(t.Context(), &backend.QueryDataRequest{Queries: []backend.DataQuery{tt.query}})
			if err != nil {
				t.Fatalf("QueryData returned top-level error: %v", err)
			}
			result := response.Responses["A"]
			if result.Error == nil || !strings.Contains(result.Error.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", result.Error, tt.want)
			}
			if result.Status != backend.StatusBadRequest || result.ErrorSource != backend.ErrorSourcePlugin {
				t.Fatalf("status = %v source = %v", result.Status, result.ErrorSource)
			}
			if len(result.Frames) != 0 {
				t.Fatalf("frames = %d, want 0", len(result.Frames))
			}
			if calls.Load() != 0 {
				t.Fatalf("endpoint called %d times", calls.Load())
			}
		})
	}
}

func TestQueryDataLimitZeroOmitted(t *testing.T) {
	var body []byte
	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		io.WriteString(w, `{"columns":[],"rows":[],"truncated":false}`)
	}, defaultOptions(), "")

	response, err := ds.QueryData(t.Context(), &backend.QueryDataRequest{Queries: []backend.DataQuery{
		dataQuery("A", `{"rawSQL":"SELECT 1","limit":0}`),
	}})
	if err != nil {
		t.Fatalf("QueryData: %v", err)
	}
	if response.Responses["A"].Error != nil {
		t.Fatalf("query error: %v", response.Responses["A"].Error)
	}
	var fields map[string]jsontext.Value
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if _, ok := fields["limit"]; ok {
		t.Fatalf("limit was sent for zero: %s", body)
	}
}

func TestQueryDataUpstreamError(t *testing.T) {
	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "query is not read-only")
	}, defaultOptions(), "")

	response, err := ds.QueryData(t.Context(), &backend.QueryDataRequest{Queries: []backend.DataQuery{
		dataQuery("A", `{"rawSQL":"DROP TABLE logs"}`),
	}})
	if err != nil {
		t.Fatalf("QueryData: %v", err)
	}
	result := response.Responses["A"]
	if result.Error == nil || !strings.Contains(result.Error.Error(), "query is not read-only") {
		t.Fatalf("error = %v", result.Error)
	}
	if result.Status != backend.StatusBadRequest || result.ErrorSource != backend.ErrorSourceDownstream {
		t.Fatalf("status = %v source = %v", result.Status, result.ErrorSource)
	}
}

func TestQueryDataCancellation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
	}, defaultOptions(), "")

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan backend.DataResponse, 1)
	go func() {
		response, _ := ds.QueryData(ctx, &backend.QueryDataRequest{Queries: []backend.DataQuery{
			dataQuery("A", `{"rawSQL":"SELECT 1"}`),
		}})
		done <- response.Responses["A"]
	}()

	<-started
	cancel()
	select {
	case result := <-done:
		if result.Error == nil {
			t.Fatal("canceled request did not fail")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled request did not return")
	}
	close(release)
}

func TestQueryDataTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		<-release
	}, options{timeout: 50 * time.Millisecond}, "")

	started := time.Now()
	response, err := ds.QueryData(t.Context(), &backend.QueryDataRequest{Queries: []backend.DataQuery{
		dataQuery("A", `{"rawSQL":"SELECT 1"}`),
	}})
	if err != nil {
		t.Fatalf("QueryData: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("timeout took %s", elapsed)
	}
	result := response.Responses["A"]
	if result.Error == nil || result.Status != backend.StatusTimeout {
		t.Fatalf("error = %v status = %v", result.Error, result.Status)
	}
}

func TestExecuteResponseLimits(t *testing.T) {
	t.Run("oversized response", func(t *testing.T) {
		ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"columns":[{"name":"x","type":"string"}],"rows":[["`+strings.Repeat("a", 100)+`"]],"truncated":false}`)
		}, options{maxResponseBytes: 64}, "")
		_, err := ds.execute(t.Context(), sqlRequest{Query: "SELECT 1"})
		if err == nil || !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("error = %v, want size error", err)
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `not json`)
		}, defaultOptions(), "")
		_, err := ds.execute(t.Context(), sqlRequest{Query: "SELECT 1"})
		if err == nil || !strings.Contains(err.Error(), "decode SQL response") {
			t.Fatalf("error = %v, want decode error", err)
		}
	})

	t.Run("trailing JSON", func(t *testing.T) {
		ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, `{"columns":[],"rows":[],"truncated":false} trailing`)
		}, defaultOptions(), "")
		_, err := ds.execute(t.Context(), sqlRequest{Query: "SELECT 1"})
		if err == nil || !strings.Contains(err.Error(), "decode SQL response") {
			t.Fatalf("error = %v, want decode error", err)
		}
	})

	t.Run("long error text bounded", func(t *testing.T) {
		ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, strings.Repeat("x", maxErrorTextBytes*2))
		}, defaultOptions(), "")
		_, err := ds.execute(t.Context(), sqlRequest{Query: "SELECT 1"})
		if err == nil {
			t.Fatal("expected error")
		}
		if len(err.Error()) > maxErrorTextBytes+200 {
			t.Fatalf("error text not bounded: %d bytes", len(err.Error()))
		}
	})
}

func TestCheckHealth(t *testing.T) {
	fixedNow := time.Date(2026, 11, 5, 12, 30, 0, 123456789, time.UTC)
	var gotQuery string
	var gotFrom, gotTo time.Time
	var gotLimit int
	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		var request sqlRequest
		if err := json.UnmarshalRead(r.Body, &request); err != nil {
			t.Errorf("decode health request: %v", err)
		}
		gotQuery = request.Query
		gotLimit = request.Limit
		gotFrom, _ = time.Parse(time.RFC3339Nano, request.From)
		gotTo, _ = time.Parse(time.RFC3339Nano, request.To)
		if r.Header.Get("X-Scope-OrgID") != "tenant-a" {
			t.Errorf("tenant header = %q", r.Header.Get("X-Scope-OrgID"))
		}
		io.WriteString(w, `{"columns":[{"name":"1","type":"number","databaseType":"INTEGER"}],"rows":[[1]],"truncated":false}`)
	}, defaultOptions(), "tenant-a")
	ds.now = func() time.Time { return fixedNow }

	result, err := ds.CheckHealth(t.Context(), &backend.CheckHealthRequest{})
	if err != nil {
		t.Fatalf("CheckHealth: %v", err)
	}
	if result.Status != backend.HealthStatusOk {
		t.Fatalf("health = %v: %s", result.Status, result.Message)
	}
	if gotQuery != "SELECT 1" || gotLimit != 1 {
		t.Fatalf("health request = %q limit %d", gotQuery, gotLimit)
	}
	if !gotFrom.Equal(fixedNow) || !gotTo.Equal(fixedNow.Add(time.Microsecond)) {
		t.Fatalf("health bounds = %s .. %s, want %s .. %s", gotFrom, gotTo, fixedNow, fixedNow.Add(time.Microsecond))
	}
}

func TestCheckHealthUnexpectedResponse(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "empty result",
			body: `{"columns":[],"rows":[],"truncated":false}`,
			want: "expected 1 column",
		},
		{
			name: "wrong column type",
			body: `{"columns":[{"name":"x","type":"string"}],"rows":[["1"]],"truncated":false}`,
			want: "number column",
		},
		{
			name: "wrong cell count",
			body: `{"columns":[{"name":"1","type":"number"}],"rows":[],"truncated":false}`,
			want: "1 row with 1 cell",
		},
		{
			name: "wrong value",
			body: `{"columns":[{"name":"1","type":"number"}],"rows":[[2]],"truncated":false}`,
			want: "numeric 1",
		},
		{
			name: "non-numeric cell",
			body: `{"columns":[{"name":"1","type":"number"}],"rows":[["1"]],"truncated":false}`,
			want: "numeric 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, tt.body)
			}, defaultOptions(), "")
			result, err := ds.CheckHealth(t.Context(), &backend.CheckHealthRequest{})
			if err != nil {
				t.Fatalf("CheckHealth: %v", err)
			}
			if result.Status != backend.HealthStatusError || !strings.Contains(result.Message, tt.want) {
				t.Fatalf("health = %v: %s, want error containing %q", result.Status, result.Message, tt.want)
			}
		})
	}
}

func TestCheckHealthError(t *testing.T) {
	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "storage unavailable")
	}, defaultOptions(), "")

	result, err := ds.CheckHealth(t.Context(), &backend.CheckHealthRequest{})
	if err != nil {
		t.Fatalf("CheckHealth returned top-level error: %v", err)
	}
	if result.Status != backend.HealthStatusError || !strings.Contains(result.Message, "storage unavailable") {
		t.Fatalf("health = %v: %s", result.Status, result.Message)
	}
}

func TestCheckHealthTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		<-release
	}, options{healthTimeout: 30 * time.Millisecond}, "")

	started := time.Now()
	result, err := ds.CheckHealth(t.Context(), &backend.CheckHealthRequest{})
	if err != nil {
		t.Fatalf("CheckHealth: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("health timeout took %s", elapsed)
	}
	if result.Status != backend.HealthStatusError {
		t.Fatalf("health = %v: %s", result.Status, result.Message)
	}
}

// closeSpyTransport records calls to CloseIdleConnections.
type closeSpyTransport struct {
	closed atomic.Int32
}

func (s *closeSpyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"columns":[],"rows":[],"truncated":false}`)),
		Request:    r,
	}, nil
}

func (s *closeSpyTransport) CloseIdleConnections() {
	s.closed.Add(1)
}

func TestDisposeClosesOwnedTransport(t *testing.T) {
	transport := new(closeSpyTransport)
	ds, _ := newTestDatasource(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"columns":[],"rows":[],"truncated":false}`)
	}, options{transport: transport}, "")
	ds.Dispose()
	ds.Dispose()
	if got := transport.closed.Load(); got != 2 {
		t.Fatalf("CloseIdleConnections calls = %d, want 2", got)
	}
}

func TestDatasourceClonesDefaultTransport(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(server.Close)
	ds, err := newDatasource(settingsFor(t, server.URL, ""), defaultOptions())
	if err != nil {
		t.Fatalf("newDatasource: %v", err)
	}
	if ds.client.Transport == http.DefaultTransport {
		t.Fatal("datasource reused the process-wide default transport")
	}
	if _, ok := ds.client.Transport.(*http.Transport); !ok {
		t.Fatalf("transport type = %T, want *http.Transport", ds.client.Transport)
	}
}

func TestRejectRedirect(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		io.WriteString(w, `{"columns":[],"rows":[],"truncated":false}`)
	}))
	t.Cleanup(target.Close)

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/api/v1/sql", http.StatusFound)
	}))
	t.Cleanup(redirector.Close)

	ds, err := newDatasource(settingsFor(t, redirector.URL, "tenant-a"), defaultOptions())
	if err != nil {
		t.Fatalf("newDatasource: %v", err)
	}
	if _, err := ds.execute(t.Context(), sqlRequest{Query: "SELECT 1"}); err == nil ||
		!strings.Contains(err.Error(), "refusing SQL endpoint redirect") {
		t.Fatalf("error = %v, want redirect rejection", err)
	}
	if targetCalls.Load() != 0 {
		t.Fatalf("redirect target was called %d times", targetCalls.Load())
	}
}

func TestNewInstance(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(server.Close)

	instance, err := NewInstance(t.Context(), settingsFor(t, server.URL, "tenant-a"))
	if err != nil {
		t.Fatalf("NewInstance: %v", err)
	}
	if _, ok := instance.(*Datasource); !ok {
		t.Fatalf("instance type = %T", instance)
	}
	if _, err := NewInstance(t.Context(), backend.DataSourceInstanceSettings{}); err == nil {
		t.Fatal("NewInstance accepted empty settings")
	}
}

func TestResponseCellDecoding(t *testing.T) {
	var response sqlResponse
	raw := fmt.Sprintf(`{"columns":[{"name":"n","type":"number","databaseType":"BIGINT"}],"rows":[[%s]],"truncated":false}`, "9007199254740993")
	if err := json.Unmarshal([]byte(raw), &response); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := string(response.Rows[0][0])
	if got != "9007199254740993" {
		t.Fatalf("raw cell = %q, want exact token", got)
	}
	if response.Rows[0][0].Kind() != jsontext.KindNumber {
		t.Fatalf("kind = %v", response.Rows[0][0].Kind())
	}
}
