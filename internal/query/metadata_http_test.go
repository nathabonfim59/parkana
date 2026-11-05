package query

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/lokiapi"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// recordingRegistrar registers handlers behind the real tenant middleware
// (multi-tenant, so tests can set X-Scope-OrgID) and records patterns.
type recordingRegistrar struct {
	mux  *http.ServeMux
	seen map[string]bool
}

func (r *recordingRegistrar) Handle(pattern string, h http.Handler) {
	r.seen[pattern] = true
	r.mux.Handle(pattern, tenant.Middleware(true, h))
}

func (r *recordingRegistrar) Registered(pattern string) bool { return r.seen[pattern] }

// newMetaHTTP builds the handlers with fakes: fixed clock testNow, a
// comfortable default timeout and the given reader behavior.
func newMetaHTTP(reader *fakeMetaReader, parsed *[]string) *metadataHTTP {
	return &metadataHTTP{
		reader:  reader,
		engine:  &fakeEngine{parsed: parsed},
		timeout: 30 * time.Second,
		now:     func() time.Time { return testNow },
	}
}

func metaTestServer(t *testing.T, h *metadataHTTP) (*httptest.Server, *recordingRegistrar) {
	t.Helper()
	reg := &recordingRegistrar{mux: http.NewServeMux(), seen: map[string]bool{}}
	h.register(reg)
	srv := httptest.NewServer(reg.mux)
	t.Cleanup(srv.Close)
	return srv, reg
}

func metaDo(t *testing.T, srv *httptest.Server, method, path, rawQuery, form string, header http.Header) *http.Response {
	t.Helper()
	var body io.Reader
	if form != "" {
		body = strings.NewReader(form)
	}
	u := srv.URL + path
	if rawQuery != "" {
		u += "?" + rawQuery
	}
	r, err := http.NewRequest(method, u, body)
	if err != nil {
		t.Fatal(err)
	}
	if form != "" {
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	for k, vs := range header {
		for _, v := range vs {
			r.Header.Add(k, v)
		}
	}
	resp, err := srv.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func metaBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func mustEqual(t *testing.T, what string, got, want any) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

// referenceStats returns the data.stats object of a zero-stat query
// response encoded by the shared lokiapi encoder, so volume tests assert the
// envelope without duplicating the stats shape.
func referenceStats(t *testing.T) map[string]any {
	t.Helper()
	b, err := lokiapi.EncodeQueryResult(plan.Result{Type: plan.TypeVector}, lokiapi.ResponseOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]any
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatal(err)
	}
	data, _ := env["data"].(map[string]any)
	stats, _ := data["stats"].(map[string]any)
	if stats == nil {
		t.Fatal("reference encoding has no data.stats")
	}
	return stats
}

func TestMetaRegister(t *testing.T) {
	var parsed []string
	_, reg := metaTestServer(t, newMetaHTTP(&fakeMetaReader{}, &parsed))
	for _, pattern := range []string{
		lokiapi.Labels, lokiapi.LabelsPost, lokiapi.LabelValues,
		lokiapi.Series, lokiapi.SeriesPost, lokiapi.IndexStats,
		lokiapi.IndexVolume, lokiapi.IndexVolumeRng,
	} {
		if !reg.Registered(pattern) {
			t.Errorf("pattern %s not registered", pattern)
		}
	}
}

func TestMetaLabels(t *testing.T) {
	appAPI := plan.MustMatcher(plan.MatchEqual, "app", "api")

	t.Run("selector, bounds and tenant reach the reader", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{names: []string{"app", "env"}}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/labels",
			"start=1793790000&end=1793793600&query="+url.QueryEscape(`{app="api"}`), "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		mustEqual(t, "content type", resp.Header.Get("Content-Type"), "application/json")
		mustEqual(t, "body", metaBody(t, resp), `{"status":"success","data":["app","env"]}`)

		want := metaQuery{
			Tenant: tenant.Default,
			Start:  unix(1793790000, 0),
			End:    unix(1793793600, 0),
			Groups: [][]plan.Matcher{{appAPI}},
		}
		if len(fr.gotNames) != 1 || !metaQueryEqual(fr.gotNames[0], want) {
			t.Errorf("reader got %+v, want %+v", fr.gotNames, want)
		}
	})

	t.Run("defaults cover the last hour without a selector", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/labels", "", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		if len(fr.gotNames) != 1 {
			t.Fatalf("reader calls = %d, want 1", len(fr.gotNames))
		}
		q := fr.gotNames[0]
		if q.Groups != nil {
			t.Errorf("groups = %v, want nil", q.Groups)
		}
		if !q.Start.Equal(testNow.Add(-time.Hour)) || !q.End.Equal(testNow) {
			t.Errorf("bounds = %s - %s, want last hour ending at testNow", q.Start, q.End)
		}
	})

	t.Run("empty result omits the data field", func(t *testing.T) {
		var parsed []string
		srv, _ := metaTestServer(t, newMetaHTTP(&fakeMetaReader{}, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/labels", "", "", nil)
		mustEqual(t, "body", metaBody(t, resp), `{"status":"success"}`)
	})

	t.Run("inverted range is accepted and read", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/labels",
			"start=1793793600&end=1793790000", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		if len(fr.gotNames) != 1 {
			t.Errorf("reader calls = %d, want 1", len(fr.gotNames))
		}
	})

	t.Run("future end anchors since at now", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/labels",
			"end=1793800800&since=30m", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		q := fr.gotNames[0]
		if !q.Start.Equal(testNow.Add(-30 * time.Minute)) {
			t.Errorf("start = %s, want now-30m", q.Start)
		}
	})

	t.Run("bad selector answers 400 without reading", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/labels", "query=app", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusBadRequest)
		mustEqual(t, "content type", resp.Header.Get("Content-Type"), "text/plain; charset=utf-8")
		metaBody(t, resp)
		if len(fr.gotNames) != 0 {
			t.Errorf("reader was called %d times, want 0", len(fr.gotNames))
		}
	})

	t.Run("bare empty selector is a client error here", func(t *testing.T) {
		var parsed []string
		srv, _ := metaTestServer(t, newMetaHTTP(&fakeMetaReader{}, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/labels", "query=%7B%7D", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusBadRequest)
		metaBody(t, resp)
	})

	t.Run("bad since answers 400", func(t *testing.T) {
		var parsed []string
		srv, _ := metaTestServer(t, newMetaHTTP(&fakeMetaReader{}, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/labels", "since=nope", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusBadRequest)
		metaBody(t, resp)
	})

	t.Run("POST form matches GET", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{names: []string{"app"}}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		form := "start=1793790000&end=1793793600&query=" + url.QueryEscape(`{app="api"}`)
		resp := metaDo(t, srv, http.MethodPost, "/loki/api/v1/labels", "", form, nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		mustEqual(t, "body", metaBody(t, resp), `{"status":"success","data":["app"]}`)
		if len(fr.gotNames) != 1 || !fr.gotNames[0].Start.Equal(unix(1793790000, 0)) {
			t.Errorf("reader got %+v", fr.gotNames)
		}
	})

	t.Run("tenant comes from the request context", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/labels", "", "",
			http.Header{tenant.Header: {"acme"}})
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		metaBody(t, resp)
		if len(fr.gotNames) != 1 || fr.gotNames[0].Tenant != tenant.ID("acme") {
			t.Errorf("tenant = %v, want acme", fr.gotNames)
		}
	})
}

func TestMetaLabelValues(t *testing.T) {
	t.Run("name comes from the route", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{values: []string{"api", "web"}}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/label/app/values",
			"start=1793790000&end=1793793600", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		mustEqual(t, "body", metaBody(t, resp), `{"status":"success","data":["api","web"]}`)
		if len(fr.gotValues) != 1 || fr.gotValues[0].name != "app" {
			t.Errorf("reader got %+v", fr.gotValues)
		}
		if len(fr.gotValues) == 1 && !fr.gotValues[0].q.Start.Equal(unix(1793790000, 0)) {
			t.Errorf("bounds = %s - %s", fr.gotValues[0].q.Start, fr.gotValues[0].q.End)
		}
	})

	t.Run("empty values omit data", func(t *testing.T) {
		var parsed []string
		srv, _ := metaTestServer(t, newMetaHTTP(&fakeMetaReader{}, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/label/missing/values", "", "", nil)
		mustEqual(t, "body", metaBody(t, resp), `{"status":"success"}`)
	})

	t.Run("POST is not routed for label values", func(t *testing.T) {
		var parsed []string
		srv, _ := metaTestServer(t, newMetaHTTP(&fakeMetaReader{}, &parsed))
		resp := metaDo(t, srv, http.MethodPost, "/loki/api/v1/label/app/values", "", "x=1", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusMethodNotAllowed)
		metaBody(t, resp)
	})
}

func TestMetaSeries(t *testing.T) {
	apiSeries := logs.New(logs.Label{Name: "app", Value: "api"})
	bSeries := logs.New(logs.Label{Name: "b", Value: "2"})

	t.Run("no match selects every stream", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{series: []logs.Labels{apiSeries}}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/series",
			"start=1793790000&end=1793793600", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		mustEqual(t, "body", metaBody(t, resp), `{"status":"success","data":[{"app":"api"}]}`)
		if len(fr.gotSeries) != 1 || fr.gotSeries[0].Groups != nil || len(parsed) != 0 {
			t.Errorf("reader got %+v, parsed %v", fr.gotSeries, parsed)
		}
	})

	t.Run("lone empty selector means every stream", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/series", "match=%7B%7D", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		metaBody(t, resp)
		if len(fr.gotSeries) != 1 || fr.gotSeries[0].Groups != nil || len(parsed) != 0 {
			t.Errorf("reader got %+v, parsed %v", fr.gotSeries, parsed)
		}
	})

	t.Run("duplicate selectors are deduplicated", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "match%5B%5D=" + url.QueryEscape(`{a="1"}`) + "&match%5B%5D=" + url.QueryEscape(`{a="1"}`)
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/series", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		metaBody(t, resp)
		if len(parsed) != 1 {
			t.Errorf("selector parsed %d times, want 1: %v", len(parsed), parsed)
		}
		if len(fr.gotSeries) != 1 || len(fr.gotSeries[0].Groups) != 1 {
			t.Errorf("reader got %+v", fr.gotSeries)
		}
	})

	t.Run("match and match[] both count, multiple selectors OR", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{series: []logs.Labels{apiSeries, bSeries}}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "match=" + url.QueryEscape(`{b="2"}`) + "&match%5B%5D=" + url.QueryEscape(`{a="1"}`)
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/series", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		mustEqual(t, "body", metaBody(t, resp), `{"status":"success","data":[{"app":"api"},{"b":"2"}]}`)
		if len(fr.gotSeries) != 1 || len(fr.gotSeries[0].Groups) != 2 {
			t.Errorf("reader got %+v", fr.gotSeries)
		}
	})

	t.Run("empty result keeps an empty array", func(t *testing.T) {
		var parsed []string
		srv, _ := metaTestServer(t, newMetaHTTP(&fakeMetaReader{}, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/series", "", "", nil)
		mustEqual(t, "body", metaBody(t, resp), `{"status":"success","data":[]}`)
	})

	t.Run("inverted range is accepted", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/series",
			"start=200&end=100", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		metaBody(t, resp)
		if len(fr.gotSeries) != 1 {
			t.Errorf("reader calls = %d, want 1", len(fr.gotSeries))
		}
	})

	t.Run("bad selector answers 400", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/series", "match=bogus", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusBadRequest)
		metaBody(t, resp)
		if len(fr.gotSeries) != 0 {
			t.Errorf("reader was called, want not")
		}
	})

	t.Run("POST form matches GET", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{series: []logs.Labels{apiSeries}}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		form := url.Values{"match[]": []string{`{a="1"}`}}.Encode()
		resp := metaDo(t, srv, http.MethodPost, "/loki/api/v1/series", "", form, nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		mustEqual(t, "body", metaBody(t, resp), `{"status":"success","data":[{"app":"api"}]}`)
	})
}

func TestMetaIndexStats(t *testing.T) {
	t.Run("counts are answered without an envelope", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{stats: metaStats{Streams: 2, Chunks: 1, Bytes: 10, Entries: 3}}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`) + "&start=1793790000&end=1793793600"
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/stats", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		mustEqual(t, "body", metaBody(t, resp), `{"streams":2,"chunks":1,"bytes":10,"entries":3}`)
		want := metaQuery{
			Tenant: tenant.Default,
			Start:  unix(1793790000, 0),
			End:    unix(1793793600, 0),
			Groups: [][]plan.Matcher{{plan.MustMatcher(plan.MatchEqual, "app", "api")}},
		}
		if len(fr.gotStats) != 1 || !metaQueryEqual(fr.gotStats[0], want) {
			t.Errorf("reader got %+v, want %+v", fr.gotStats, want)
		}
	})

	t.Run("bounds widen to whole milliseconds", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`) +
			"&start=1793793600123456789&end=1793793660999999999"
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/stats", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		metaBody(t, resp)
		got := fr.gotStats[0]
		if !got.Start.Equal(time.UnixMilli(1793793600123)) || !got.End.Equal(time.UnixMilli(1793793661000)) {
			t.Errorf("bounds = %s - %s", got.Start, got.End)
		}
	})

	t.Run("query is required", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/stats", "", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusBadRequest)
		metaBody(t, resp)
		if len(fr.gotStats) != 0 {
			t.Errorf("reader was called, want not")
		}
	})

	t.Run("bare empty selector is rejected", func(t *testing.T) {
		var parsed []string
		srv, _ := metaTestServer(t, newMetaHTTP(&fakeMetaReader{}, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/stats", "query=%7B%7D", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusBadRequest)
		metaBody(t, resp)
	})

	t.Run("inverted range is rejected", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`) + "&start=200&end=100"
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/stats", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusBadRequest)
		metaBody(t, resp)
		if len(fr.gotStats) != 0 {
			t.Errorf("reader was called, want not")
		}
	})

	t.Run("range query parameters stay validated", func(t *testing.T) {
		var parsed []string
		srv, _ := metaTestServer(t, newMetaHTTP(&fakeMetaReader{}, &parsed))
		for _, q := range []string{
			"query=" + url.QueryEscape(`{app="api"}`) + "&step=-1",
			"query=" + url.QueryEscape(`{app="api"}`) + "&limit=0",
			"query=" + url.QueryEscape(`{app="api"}`) + "&direction=sideways",
		} {
			resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/stats", q, "", nil)
			mustEqual(t, "status for "+q, resp.StatusCode, http.StatusBadRequest)
			metaBody(t, resp)
		}
	})
}

func TestMetaVolumeInstant(t *testing.T) {
	volEntry := func(name, value string, end time.Time, volume uint64) metaVolumeEntry {
		return metaVolumeEntry{
			Metric:  logs.New(logs.Label{Name: name, Value: value}),
			Buckets: []metaVolumeSample{{End: end, Volume: volume}},
		}
	}

	t.Run("vector answer with seconds timestamps and zero stats", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{volume: []metaVolumeEntry{volEntry("app", "api", testNow, 10)}}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`) + "&start=1793790000&end=1793793600"
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)

		var env map[string]any
		if err := json.Unmarshal([]byte(metaBody(t, resp)), &env); err != nil {
			t.Fatal(err)
		}
		mustEqual(t, "status field", env["status"], "success")
		data, _ := env["data"].(map[string]any)
		if data == nil {
			t.Fatalf("no data object: %v", env)
		}
		mustEqual(t, "resultType", data["resultType"], "vector")
		if !reflect.DeepEqual(data["stats"], referenceStats(t)) {
			t.Errorf("data.stats = %v, want the encoder's zero stats", data["stats"])
		}
		result, _ := data["result"].([]any)
		if len(result) != 1 {
			t.Fatalf("result = %v, want one sample", result)
		}
		obj, _ := result[0].(map[string]any)
		sample, _ := obj["value"].([]any)
		if len(sample) != 2 {
			t.Fatalf("value = %v, want [ts, value]", obj["value"])
		}
		mustEqual(t, "timestamp seconds", sample[0], float64(1793793600))
		mustEqual(t, "volume string", sample[1], "10")
		metric, _ := obj["metric"].(map[string]any)
		mustEqual(t, "metric label", metric["app"], "api")
	})

	t.Run("request parameters reach the reader", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`) +
			"&start=1793790000&end=1793793600&limit=7&aggregateBy=labels&targetLabels=level,env"
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		metaBody(t, resp)
		req := fr.gotVolume[0]
		if req.Limit != 7 || req.Step != 0 || !req.AggregateByLabels {
			t.Errorf("volume request = %+v", req)
		}
		if !reflect.DeepEqual(req.TargetLabels, []string{"level", "env"}) {
			t.Errorf("targetLabels = %v", req.TargetLabels)
		}
		if !req.Query.Start.Equal(unix(1793790000, 0)) || !req.Query.End.Equal(unix(1793793600, 0)) {
			t.Errorf("bounds = %s - %s", req.Query.Start, req.Query.End)
		}
	})

	t.Run("bare empty selector selects everything", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume",
			"query=%7B%7D&start=1793790000&end=1793793600", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		metaBody(t, resp)
		if len(fr.gotVolume) != 1 || fr.gotVolume[0].Query.Groups != nil {
			t.Errorf("reader got %+v", fr.gotVolume)
		}
	})

	t.Run("query is required", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume", "", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusBadRequest)
		metaBody(t, resp)
		if len(fr.gotVolume) != 0 {
			t.Errorf("reader was called, want not")
		}
	})

	t.Run("limit zero resets to the default", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`) + "&limit=0"
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		metaBody(t, resp)
		if fr.gotVolume[0].Limit != defaultQueryLimit {
			t.Errorf("limit = %d, want %d", fr.gotVolume[0].Limit, defaultQueryLimit)
		}
	})

	t.Run("invalid parameters are client errors", func(t *testing.T) {
		var parsed []string
		srv, _ := metaTestServer(t, newMetaHTTP(&fakeMetaReader{}, &parsed))
		query := "query=" + url.QueryEscape(`{app="api"}`)
		for _, q := range []string{
			query + "&limit=-2",
			query + "&limit=abc",
			query + "&aggregateBy=bogus",
			query + "&start=200&end=100",
			query + "&direction=sideways",
			query + "&time=notatime",
			"query=bogus",
		} {
			resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume", q, "", nil)
			mustEqual(t, "status for "+q, resp.StatusCode, http.StatusBadRequest)
			metaBody(t, resp)
		}
	})

	t.Run("instant parameters are validated then ignored", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`) +
			"&time=1793793600&direction=forward&start=1793790000&end=1793793600"
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		metaBody(t, resp)
		req := fr.gotVolume[0]
		if !req.Query.Start.Equal(unix(1793790000, 0)) || !req.Query.End.Equal(unix(1793793600, 0)) {
			t.Errorf("bounds = %s - %s", req.Query.Start, req.Query.End)
		}
	})

	t.Run("empty volumes answer an empty vector", func(t *testing.T) {
		var parsed []string
		srv, _ := metaTestServer(t, newMetaHTTP(&fakeMetaReader{}, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`)
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		var env map[string]any
		if err := json.Unmarshal([]byte(metaBody(t, resp)), &env); err != nil {
			t.Fatal(err)
		}
		data, _ := env["data"].(map[string]any)
		mustEqual(t, "resultType", data["resultType"], "vector")
		result, _ := data["result"].([]any)
		mustEqual(t, "result length", len(result), 0)
	})
}

func TestMetaVolumeRange(t *testing.T) {
	multiBucket := metaVolumeEntry{
		Metric: logs.New(logs.Label{Name: "app", Value: "api"}),
		Buckets: []metaVolumeSample{
			{End: time.UnixMilli(1793793540000), Volume: 30},
			{End: time.UnixMilli(1793793600000), Volume: 10},
		},
	}

	t.Run("multiple buckets answer a matrix", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{volume: []metaVolumeEntry{
			multiBucket,
			{
				Metric:  logs.New(logs.Label{Name: "level", Value: "info"}),
				Buckets: []metaVolumeSample{{End: time.UnixMilli(1793793600000), Volume: 5}},
			},
		}}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`) +
			"&start=1793790000&end=1793793600&step=60"
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume_range", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)

		var env map[string]any
		if err := json.Unmarshal([]byte(metaBody(t, resp)), &env); err != nil {
			t.Fatal(err)
		}
		data, _ := env["data"].(map[string]any)
		mustEqual(t, "resultType", data["resultType"], "matrix")
		result, _ := data["result"].([]any)
		mustEqual(t, "series count", len(result), 2)
		first, _ := result[0].(map[string]any)
		values, _ := first["values"].([]any)
		if len(values) != 2 {
			t.Fatalf("values = %v, want two samples", values)
		}
		sample, _ := values[0].([]any)
		mustEqual(t, "first timestamp seconds", sample[0], float64(1793793540))
		mustEqual(t, "first volume", sample[1], "30")
		if !reflect.DeepEqual(data["stats"], referenceStats(t)) {
			t.Errorf("data.stats = %v, want the encoder's zero stats", data["stats"])
		}
	})

	t.Run("single buckets answer a vector", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{volume: []metaVolumeEntry{{
			Metric:  logs.New(logs.Label{Name: "app", Value: "api"}),
			Buckets: []metaVolumeSample{{End: time.UnixMilli(1793793600000), Volume: 10}},
		}}}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`) + "&start=1793790000&end=1793793600&step=60"
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume_range", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		var env map[string]any
		if err := json.Unmarshal([]byte(metaBody(t, resp)), &env); err != nil {
			t.Fatal(err)
		}
		data, _ := env["data"].(map[string]any)
		mustEqual(t, "resultType", data["resultType"], "vector")
	})

	t.Run("default step and bounds reach the reader", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`)
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume_range", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		metaBody(t, resp)
		req := fr.gotVolume[0]
		if req.Step != 14*time.Second {
			t.Errorf("step = %s, want the dynamic default 14s", req.Step)
		}
		if !req.Query.Start.Equal(testNow.Add(-time.Hour)) || !req.Query.End.Equal(testNow) {
			t.Errorf("bounds = %s - %s", req.Query.Start, req.Query.End)
		}
	})

	t.Run("too many buckets are rejected", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`) +
			"&start=0&end=1793793600&step=1"
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume_range", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusBadRequest)
		metaBody(t, resp)
		if len(fr.gotVolume) != 0 {
			t.Errorf("reader was called, want not")
		}
	})

	// Regressions for the step normalization Loki's frontend codec applies
	// (queryrange/codec.go converts Step.Milliseconds()): the range
	// validation runs on the parsed nanosecond step, then the reader's step
	// truncates to whole milliseconds.
	t.Run("fractional-millisecond step truncates to whole milliseconds", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`) + "&start=0&end=1&step=0.0015"
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume_range", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		metaBody(t, resp)
		mustEqual(t, "step", fr.gotVolume[0].Step, time.Millisecond)
	})

	t.Run("sub-millisecond step becomes the instant whole-range path", func(t *testing.T) {
		var parsed []string
		fr := &fakeMetaReader{}
		srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
		q := "query=" + url.QueryEscape(`{app="api"}`) + "&start=0&end=1&step=0.0005"
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/index/volume_range", q, "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusOK)
		metaBody(t, resp)
		mustEqual(t, "step", fr.gotVolume[0].Step, time.Duration(0))
	})
}

func TestMetaErrorMapping(t *testing.T) {
	newReaderErr := func(err error) *fakeMetaReader { return &fakeMetaReader{err: err} }

	t.Run("reader client faults answer 400", func(t *testing.T) {
		for _, err := range []error{
			fmt.Errorf("metadata: %w", plan.ErrBadQuery),
			fmt.Errorf("metadata: %w", plan.ErrUnsupported),
			fmt.Errorf("metadata: %w", plan.ErrLimit),
		} {
			var parsed []string
			srv, _ := metaTestServer(t, newMetaHTTP(newReaderErr(err), &parsed))
			resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/labels", "", "", nil)
			mustEqual(t, "status", resp.StatusCode, http.StatusBadRequest)
			metaBody(t, resp)
		}
	})

	t.Run("other reader faults answer 500", func(t *testing.T) {
		var parsed []string
		srv, _ := metaTestServer(t, newMetaHTTP(newReaderErr(errors.New("boom")), &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/labels", "", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusInternalServerError)
		body := metaBody(t, resp)
		if !strings.Contains(body, "boom") {
			t.Errorf("body %q does not carry the error", body)
		}
	})

	t.Run("expired deadline answers 504 with Loki's text", func(t *testing.T) {
		var parsed []string
		h := newMetaHTTP(&fakeMetaReader{hang: true}, &parsed)
		h.timeout = 50 * time.Millisecond
		srv, _ := metaTestServer(t, h)
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/labels", "", "", nil)
		mustEqual(t, "status", resp.StatusCode, http.StatusGatewayTimeout)
		mustEqual(t, "body", strings.TrimSpace(metaBody(t, resp)), lokiErrDeadlineExceeded)
	})

	t.Run("client cancellation answers 499 with Loki's text", func(t *testing.T) {
		var parsed []string
		h := newMetaHTTP(&fakeMetaReader{hang: true}, &parsed)
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/loki/api/v1/labels", nil)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		h.labels(w, r.WithContext(ctx))
		mustEqual(t, "status", w.Code, statusClientClosedRequest)
		mustEqual(t, "body", strings.TrimSpace(w.Body.String()), lokiErrClientCanceled)
	})
}

// TestMetaVolumeMalformedForm is a regression for normalizeVolumeLimit
// swallowing the first ParseForm error: a second ParseForm over the already
// populated form returns nil, so a malformed percent escape used to be
// silently dropped and the read ran with defaulted bounds instead of a 400.
func TestMetaVolumeMalformedForm(t *testing.T) {
	for _, route := range []string{"/loki/api/v1/index/volume", "/loki/api/v1/index/volume_range"} {
		t.Run(route, func(t *testing.T) {
			var parsed []string
			fr := &fakeMetaReader{}
			srv, _ := metaTestServer(t, newMetaHTTP(fr, &parsed))
			raw := "query=" + url.QueryEscape(`{app="api"}`) + "&start=%zz"
			resp := metaDo(t, srv, http.MethodGet, route, raw, "", nil)
			mustEqual(t, "status", resp.StatusCode, http.StatusBadRequest)
			metaBody(t, resp)
			if len(fr.gotVolume) != 0 {
				t.Errorf("reader was called %d times, want 0", len(fr.gotVolume))
			}
		})
	}
}

// TestMetaErrorContextClassification pins the approved policy for context
// errors inside larger failures: only a unanimous deadline (or a unanimous
// cancellation) is a 504 (or 499); any genuine error joined with context
// errors, and any deadline/cancellation mix, keeps its 500.
func TestMetaErrorContextClassification(t *testing.T) {
	statusFor := func(t *testing.T, err error) int {
		t.Helper()
		var parsed []string
		srv, _ := metaTestServer(t, newMetaHTTP(&fakeMetaReader{err: err}, &parsed))
		resp := metaDo(t, srv, http.MethodGet, "/loki/api/v1/labels", "", "", nil)
		metaBody(t, resp)
		return resp.StatusCode
	}

	t.Run("genuine errors joined with context errors stay 500", func(t *testing.T) {
		for _, err := range []error{
			errors.Join(errors.New("boom"), context.DeadlineExceeded),
			errors.Join(errors.New("boom"), context.Canceled),
			fmt.Errorf("read files: %w", errors.Join(errors.New("boom"), context.DeadlineExceeded)),
		} {
			mustEqual(t, fmt.Sprintf("status for %v", err), statusFor(t, err), http.StatusInternalServerError)
		}
	})

	t.Run("mixed cancellation and deadline answers 500", func(t *testing.T) {
		err := errors.Join(context.Canceled, context.DeadlineExceeded)
		mustEqual(t, "status", statusFor(t, err), http.StatusInternalServerError)
	})

	t.Run("pure joined deadlines answer 504", func(t *testing.T) {
		err := errors.Join(context.DeadlineExceeded, fmt.Errorf("scan: %w", context.DeadlineExceeded))
		mustEqual(t, "status", statusFor(t, err), http.StatusGatewayTimeout)
	})

	t.Run("wrapped timeout cause answers 504", func(t *testing.T) {
		err := fmt.Errorf("query: %w", errMetaTimeout)
		mustEqual(t, "status", statusFor(t, err), http.StatusGatewayTimeout)
	})

	t.Run("wrapped cancellation answers 499", func(t *testing.T) {
		err := fmt.Errorf("query: %w", context.Canceled)
		mustEqual(t, "status", statusFor(t, err), statusClientClosedRequest)
	})
}
