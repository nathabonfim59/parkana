package query

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// testNow is the single captured request time every default derives from:
// 2026-11-04T12:00:00Z, Unix 1793793600.
var testNow = time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)

func getRequest(t *testing.T, path, rawQuery string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, path+"?"+rawQuery, nil)
	return r
}

func postRequest(t *testing.T, path, body string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}

func checkParams(t *testing.T, got, want plan.Params) {
	t.Helper()
	if got.Tenant != want.Tenant || got.Query != want.Query || got.Limit != want.Limit ||
		got.Direction != want.Direction || got.Step != want.Step || got.Interval != want.Interval {
		t.Errorf("params mismatch:\n got %+v\nwant %+v", got, want)
	}
	if !got.Start.Equal(want.Start) || !got.End.Equal(want.End) {
		t.Errorf("time mismatch:\n got %s - %s\nwant %s - %s", got.Start, got.End, want.Start, want.End)
	}
}

func unix(sec, nsec int64) time.Time { return time.Unix(sec, nsec) }

// TestRangeParams exercises GET and POST query_range requests through
// parseQueryParams, asserting the full plan.Params, not just status codes.
func TestRangeParams(t *testing.T) {
	const path = "/loki/api/v1/query_range"
	for _, tc := range []struct {
		name   string
		params string // URL query for GET, form body for POST
		want   plan.Params
	}{
		{
			name:   "defaults: last hour, limit 100, backward, dynamic step",
			params: `query={app="api"}`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{app="api"}`,
				Start: testNow.Add(-time.Hour), End: testNow,
				Step:  14 * time.Second, // floor(3600/250)
				Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "every parameter",
			params: `query={app="api"}&start=1793790000&end=1793793600&step=15&interval=2m&limit=5&direction=forward`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{app="api"}`,
				Start: unix(1793790000, 0), End: unix(1793793600, 0),
				Step: 15 * time.Second, Interval: 2 * time.Minute,
				Limit: 5, Direction: plan.Forward,
			},
		},
		{
			name:   "mixed case direction",
			params: `query={a="b"}&start=0&end=10&step=1&direction=Forward`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(0, 0), End: unix(10, 0),
				Step: time.Second, Limit: 100, Direction: plan.Forward,
			},
		},
		{
			name:   "rfc3339 timestamps",
			params: `query={a="b"}&start=2026-11-04T11:00:00Z&end=2026-11-04T12:00:00Z`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: testNow.Add(-time.Hour), End: testNow,
				Step: 14 * time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "rfc3339nano with zone offset",
			params: `query={a="b"}&start=2026-11-04T13:30:00.25%2B02:00&end=2026-11-04T14:00:00%2B02:00`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: testNow.Add(-30 * time.Minute).Add(250 * time.Millisecond),
				End:   testNow,
				Step:  7 * time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "fractional seconds keep nanoseconds, no float rounding",
			params: `query={a="b"}&start=1793793600.123456789&end=1793793600.987654321`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(1793793600, 123456789), End: unix(1793793600, 987654321),
				Step: time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "fraction rounds half away from zero past ns",
			params: `query={a="b"}&start=1793793600.0000000004&end=1793793600.0000000005`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(1793793600, 0), End: unix(1793793600, 1),
				Step: time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "scientific decimal seconds",
			params: `query={a="b"}&start=1.7937936e9&end=1793793600`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(1793793600, 0), End: unix(1793793600, 0),
				Step: time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "negative fractional seconds before epoch",
			params: `query={a="b"}&start=-1.5&end=-1`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(0, -1500000000), End: unix(-1, 0),
				Step: time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "ten digit integer is seconds",
			params: `query={a="b"}&start=9999999999&end=9999999999`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(9999999999, 0), End: unix(9999999999, 0),
				Step: time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "eleven digit integer is nanoseconds",
			params: `query={a="b"}&start=10000000000&end=20000000000`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(0, 10000000000), End: unix(0, 20000000000),
				Step: time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "nanosecond integers",
			params: `query={a="b"}&start=1793790000000000000&end=1793793600000000000`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(1793790000, 0), End: unix(1793793600, 0),
				Step: 14 * time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "since only",
			params: `query={a="b"}&since=30m`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: testNow.Add(-30 * time.Minute), End: testNow,
				Step: 7 * time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "end plus since",
			params: `query={a="b"}&end=1793793600&since=15m`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(1793793600, 0).Add(-15 * time.Minute), End: unix(1793793600, 0),
				Step: 3 * time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "future end anchors since at now",
			params: `query={a="b"}&end=1793800800&since=30m`, // end = now + 2h
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: testNow.Add(-30 * time.Minute), End: unix(1793800800, 0),
				Step: 36 * time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "explicit start supersedes since",
			params: `query={a="b"}&start=100&end=200&since=5s`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(100, 0), End: unix(200, 0),
				Step: time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "since compound duration and zero",
			params: `query={a="b"}&end=1793793600&since=1h30m`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(1793793600, 0).Add(-90 * time.Minute), End: unix(1793793600, 0),
				Step: 21 * time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "since zero",
			params: `query={a="b"}&end=1793793600&since=0`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(1793793600, 0), End: unix(1793793600, 0),
				Step: time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "equal start and end allowed",
			params: `query={a="b"}&start=100&end=100`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(100, 0), End: unix(100, 0),
				Step: time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "step float seconds",
			params: `query={a="b"}&start=0&end=6000&step=1.5`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(0, 0), End: unix(6000, 0),
				Step: 1500 * time.Millisecond, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "step prometheus duration",
			params: `query={a="b"}&start=0&end=60000&step=1m30s`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(0, 0), End: unix(60000, 0),
				Step: 90 * time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "step milliseconds",
			params: `query={a="b"}&start=0&end=5000&step=500ms`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(0, 0), End: unix(5000, 0),
				Step: 500 * time.Millisecond, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "interval float seconds",
			params: `query={a="b"}&start=0&end=100&step=10&interval=0.5`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(0, 0), End: unix(100, 0),
				Step: 10 * time.Second, Interval: 500 * time.Millisecond,
				Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "interval zero explicit",
			params: `query={a="b"}&start=0&end=100&step=10&interval=0`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(0, 0), End: unix(100, 0),
				Step: 10 * time.Second, Interval: 0,
				Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "11000 points exactly accepted",
			params: `query={a="b"}&start=0&end=11000&step=1`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(0, 0), End: unix(11000, 0),
				Step: time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "default step floors sub-second ranges to one second",
			params: `query={a="b"}&start=0&end=100`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(0, 0), End: unix(100, 0),
				Step: time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "limit minimum positive",
			params: `query={a="b"}&start=0&end=100&step=10&limit=1`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(0, 0), End: unix(100, 0),
				Step: 10 * time.Second, Limit: 1, Direction: plan.Backward,
			},
		},
		{
			name:   "time parameter ignored on range queries",
			params: `query={a="b"}&start=0&end=100&step=10&time=5`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `{a="b"}`,
				Start: unix(0, 0), End: unix(100, 0),
				Step: 10 * time.Second, Limit: 100, Direction: plan.Backward,
			},
		},
	} {
		t.Run("GET/"+tc.name, func(t *testing.T) {
			got, err := parseQueryParams(getRequest(t, path, tc.params), rangeQuery, testNow)
			if err != nil {
				t.Fatalf("parseQueryParams: %v", err)
			}
			checkParams(t, got, tc.want)
		})
		t.Run("POST/"+tc.name, func(t *testing.T) {
			got, err := parseQueryParams(postRequest(t, path, tc.params), rangeQuery, testNow)
			if err != nil {
				t.Fatalf("parseQueryParams: %v", err)
			}
			checkParams(t, got, tc.want)
		})
	}
}

// TestInstantParams exercises query (instant) requests.
func TestInstantParams(t *testing.T) {
	const path = "/loki/api/v1/query"
	for _, tc := range []struct {
		name   string
		params string
		want   plan.Params
	}{
		{
			name:   "defaults to now",
			params: `query=sum(rate({a="b"}[5m]))`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `sum(rate({a="b"}[5m]))`,
				Start: testNow, End: testNow,
				Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "explicit time and options",
			params: `query=count_over_time({a="b"}[1h])&time=1793793600&limit=7&direction=forward`,
			want: plan.Params{
				Tenant: tenant.Default, Query: `count_over_time({a="b"}[1h])`,
				Start: unix(1793793600, 0), End: unix(1793793600, 0),
				Limit: 7, Direction: plan.Forward,
			},
		},
		{
			name:   "fractional time keeps nanoseconds",
			params: `query=q&time=1793793600.000000001`,
			want: plan.Params{
				Tenant: tenant.Default, Query: "q",
				Start: unix(1793793600, 1), End: unix(1793793600, 1),
				Limit: 100, Direction: plan.Backward,
			},
		},
		{
			name:   "range-only parameters are ignored",
			params: `query=q&time=60&start=garbage&end=garbage&step=-5&interval=x&since=bad`,
			want: plan.Params{
				Tenant: tenant.Default, Query: "q",
				Start: unix(60, 0), End: unix(60, 0),
				Limit: 100, Direction: plan.Backward,
			},
		},
	} {
		t.Run("GET/"+tc.name, func(t *testing.T) {
			got, err := parseQueryParams(getRequest(t, path, tc.params), instantQuery, testNow)
			if err != nil {
				t.Fatalf("parseQueryParams: %v", err)
			}
			checkParams(t, got, tc.want)
			if !got.Instant() {
				t.Errorf("expected instant params, got step %s", got.Step)
			}
		})
		t.Run("POST/"+tc.name, func(t *testing.T) {
			got, err := parseQueryParams(postRequest(t, path, tc.params), instantQuery, testNow)
			if err != nil {
				t.Fatalf("parseQueryParams: %v", err)
			}
			checkParams(t, got, tc.want)
			if !got.Instant() {
				t.Errorf("expected instant params, got step %s", got.Step)
			}
		})
	}
}

// TestParamErrors rejects malformed and inconsistent values; every error must
// wrap plan.ErrBadQuery so handlers can map it to a Loki-style 400.
func TestParamErrors(t *testing.T) {
	const path = "/loki/api/v1/query_range"
	bad := []struct {
		name   string
		kind   queryKind
		params string
	}{
		{"empty query", rangeQuery, `start=0&end=100&step=10`},
		{"start after end", rangeQuery, `query=q&start=200&end=100`},
		{"bad start", rangeQuery, `query=q&start=oops&end=100`},
		{"bad end", rangeQuery, `query=q&start=0&end=oops`},
		{"end overflows int64", rangeQuery, `query=q&end=99999999999999999999`},
		{"start overflows int64", rangeQuery, `query=q&start=99999999999999999999&end=1`},
		{"hostile long fraction rejected", rangeQuery, `query=q&end=0.00000000000000000000000000000000000000000000000000000000000000001`},
		{"since without unit", rangeQuery, `query=q&since=3600`},
		{"since negative", rangeQuery, `query=q&since=-1h`},
		{"since bad unit", rangeQuery, `query=q&since=5x`},
		{"since units out of order", rangeQuery, `query=q&since=30m1h`},
		{"since not a number", rangeQuery, `query=q&since=many`},
		{"step zero", rangeQuery, `query=q&start=0&end=100&step=0`},
		{"step negative", rangeQuery, `query=q&start=0&end=100&step=-5`},
		{"step overflows int64", rangeQuery, `query=q&start=0&end=100&step=1e300`},
		{"step not a duration", rangeQuery, `query=q&start=0&end=100&step=abc`},
		{"step nan rejected", rangeQuery, `query=q&start=0&end=100&step=NaN`},
		{"step infinite rejected", rangeQuery, `query=q&start=0&end=100&step=Inf`},
		{"11001 points rejected", rangeQuery, `query=q&start=0&end=11001&step=1`},
		{"step too small for range", rangeQuery, `query=q&start=0&end=3600&step=0.3`},
		{"interval negative", rangeQuery, `query=q&start=0&end=100&step=10&interval=-1`},
		{"interval not a duration", rangeQuery, `query=q&start=0&end=100&step=10&interval=two`},
		{"limit zero", rangeQuery, `query=q&start=0&end=100&step=10&limit=0`},
		{"limit negative", rangeQuery, `query=q&start=0&end=100&step=10&limit=-5`},
		{"limit not an int", rangeQuery, `query=q&start=0&end=100&step=10&limit=abc`},
		{"limit float rejected", rangeQuery, `query=q&start=0&end=100&step=10&limit=5.5`},
		{"limit overflows int", rangeQuery, `query=q&start=0&end=100&step=10&limit=99999999999999999999`},
		{"bad direction", rangeQuery, `query=q&start=0&end=100&step=10&direction=sideways`},
		{"instant bad time", instantQuery, `query=q&time=oops`},
		{"instant hex float time", instantQuery, `query=q&time=0x1.fp0`},
		{"instant hex huge p exponent", instantQuery, `query=q&time=0x0p-999999999`},
		{"range hex start", rangeQuery, `query=q&start=0x1.8p2&end=10`},
		{"range hex end", rangeQuery, `query=q&start=0&end=0x1.8p1`},
		{"instant bad limit", instantQuery, `query=q&limit=0`},
		{"instant bad direction", instantQuery, `query=q&direction=down`},
		{"instant empty query", instantQuery, `time=0`},
	}
	for _, tc := range bad {
		t.Run("GET/"+tc.name, func(t *testing.T) {
			_, err := parseQueryParams(getRequest(t, path, tc.params), tc.kind, testNow)
			if err == nil {
				t.Fatal("expected error, got none")
			}
			if !errors.Is(err, plan.ErrBadQuery) {
				t.Errorf("error %v does not wrap plan.ErrBadQuery", err)
			}
		})
		t.Run("POST/"+tc.name, func(t *testing.T) {
			_, err := parseQueryParams(postRequest(t, path, tc.params), tc.kind, testNow)
			if err == nil {
				t.Fatal("expected error, got none")
			}
			if !errors.Is(err, plan.ErrBadQuery) {
				t.Errorf("error %v does not wrap plan.ErrBadQuery", err)
			}
		})
	}
}

// TestMalformedForm rejects requests whose percent-encoding is broken, for
// both the URL query string and a POST form body.
func TestMalformedForm(t *testing.T) {
	const path = "/loki/api/v1/query_range"
	for _, r := range []*http.Request{
		getRequest(t, path, `query=q&step=%zz`),
		postRequest(t, path, `query=q&step=%zz`),
	} {
		if _, err := parseQueryParams(r, rangeQuery, testNow); !errors.Is(err, plan.ErrBadQuery) {
			t.Errorf("%s: expected ErrBadQuery, got %v", r.Method, err)
		}
	}
}

// TestPostBodyPrecedence checks POST form values win over URL query values,
// matching net/http's r.Form ordering.
func TestPostBodyPrecedence(t *testing.T) {
	r := postRequest(t, "/loki/api/v1/query_range?limit=5&query=q", `start=0&end=100&step=10&limit=10`)
	got, err := parseQueryParams(r, rangeQuery, testNow)
	if err != nil {
		t.Fatalf("parseQueryParams: %v", err)
	}
	if got.Limit != 10 || got.Query != "q" {
		t.Errorf("got limit %d query %q, want 10 and q", got.Limit, got.Query)
	}
}

// TestTenantFromContext checks the tenant id rides the request context.
func TestTenantFromContext(t *testing.T) {
	r := getRequest(t, "/loki/api/v1/query", `query=q&time=60`)
	r = r.WithContext(tenant.With(r.Context(), "acme"))
	got, err := parseQueryParams(r, instantQuery, testNow)
	if err != nil {
		t.Fatalf("parseQueryParams: %v", err)
	}
	if got.Tenant != "acme" {
		t.Errorf("got tenant %q, want acme", got.Tenant)
	}
}

// captureEngine is a plan.Engine that records the Params it was called with.
type captureEngine struct{ got plan.Params }

func (e *captureEngine) Query(_ context.Context, _ plan.Source, p plan.Params) (plan.Result, error) {
	e.got = p
	return plan.Result{Type: plan.TypeStreams}, nil
}

func (e *captureEngine) ParseSelector(string) ([]plan.Matcher, error) { return nil, nil }

// TestHandlerPassesParams proves the parser plugs into an HTTP handler that
// feeds a plan.Engine: the engine sees exactly the parsed Params.
func TestHandlerPassesParams(t *testing.T) {
	engine := &captureEngine{}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := parseQueryParams(r, rangeQuery, testNow)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := engine.Query(r.Context(), nil, p); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	r := getRequest(t, "/loki/api/v1/query_range", `query={a="b"}&end=1793793600&since=30m`)
	r = r.WithContext(tenant.With(r.Context(), "acme"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("got status %d body %q", rec.Code, rec.Body)
	}
	checkParams(t, engine.got, plan.Params{
		Tenant: "acme", Query: `{a="b"}`,
		Start: unix(1793793600, 0).Add(-30 * time.Minute), End: unix(1793793600, 0),
		Step: 7 * time.Second, Limit: 100, Direction: plan.Backward,
	})
}
