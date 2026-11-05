package tail

import (
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/nathabonfim59/parkana/internal/app"
	"github.com/nathabonfim59/parkana/internal/config"
	"github.com/nathabonfim59/parkana/internal/server"
)

func TestTruncateReason(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want int
	}{
		{"short", "boom", 4},
		{"exact", strings.Repeat("a", 123), 123},
		{"long ascii", strings.Repeat("a", 500), 123},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateReason(tc.in)
			if len(got) != tc.want {
				t.Fatalf("len = %d, want %d", len(got), tc.want)
			}
		})
	}
	// A multibyte rune straddling the limit must be dropped whole.
	reason := strings.Repeat("a", 120) + "é" + strings.Repeat("b", 10)
	got := truncateReason(reason)
	if len(got) > 123 || !utf8.ValidString(got) {
		t.Fatalf("truncated reason len %d valid=%t", len(got), utf8.ValidString(got))
	}
	if strings.HasSuffix(got, "\xc3") {
		t.Fatal("truncation split a UTF-8 rune")
	}
}

func TestNewQuerierValidatesDeps(t *testing.T) {
	reg := prometheus.NewRegistry()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := server.New(server.Options{Logger: log, Registry: reg})
	good := app.Deps{Logger: log, HTTP: srv, Metrics: reg}

	for _, tc := range []struct {
		name string
		deps app.Deps
	}{
		{"no logger", app.Deps{HTTP: srv, Metrics: reg}},
		{"no http", app.Deps{Logger: log, Metrics: reg}},
		{"no metrics", app.Deps{Logger: log, HTTP: srv}},
		{"bad ingester url", app.Deps{Logger: log, HTTP: srv, Metrics: reg,
			Config: config.Config{LogQL: config.LogQL{Tail: config.Tail{
				IngesterURLs: []string{"ftp://example.com"},
			}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewQuerier(tc.deps); err == nil {
				t.Fatal("expected an error")
			}
		})
	}

	first, err := NewQuerier(good)
	if err != nil {
		t.Fatalf("NewQuerier: %v", err)
	}
	if first == nil {
		t.Fatal("NewQuerier returned nil")
	}
	if _, err := NewQuerier(good); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("duplicate route error = %v", err)
	}
}

func TestNewTailMetricsRollsBackPartialRegistration(t *testing.T) {
	reg := prometheus.NewRegistry()
	// Occupy the third collector's name so newTailMetrics registers two
	// collectors and then fails; the rollback must free those two names.
	blocker := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "parkana_tail_dropped_entries_total",
		Help: "test blocker",
	}, []string{"tenant"})
	reg.MustRegister(blocker)
	if _, err := newTailMetrics(reg); err == nil {
		t.Fatal("expected the blocked registration to fail")
	}
	for _, probe := range []struct {
		name string
		help string
	}{
		{"parkana_tail_subscribers", "Live-tail WebSocket subscribers currently connected, by tenant."},
		{"parkana_tail_entries_total", "Entries written to live-tail WebSocket subscribers, by tenant."},
	} {
		c := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: probe.name, Help: probe.help}, []string{"tenant"})
		if err := reg.Register(c); err != nil {
			t.Fatalf("%s was not rolled back: %v", probe.name, err)
		}
	}
}

func TestParseIngestersRejectsMalformedURL(t *testing.T) {
	if _, err := parseIngesters([]string{"http://%zz"}); err == nil {
		t.Fatal("malformed URL accepted")
	}
	if _, err := parseIngesters(nil); err != nil {
		t.Fatalf("empty list: %v", err)
	}
}

func TestWantsCategorizedLabels(t *testing.T) {
	for _, tc := range []struct {
		header string
		want   bool
	}{
		{"", false},
		{"categorize-labels", true},
		{"other, categorize-labels", true},
		{"categorize-labels,other", true},
	} {
		r, err := http.NewRequest("GET", "/loki/api/v1/tail", nil)
		if err != nil {
			t.Fatal(err)
		}
		if tc.header != "" {
			r.Header.Set("X-Loki-Response-Encoding-Flags", tc.header)
		}
		if got := wantsCategorizedLabels(r); got != tc.want {
			t.Fatalf("%q -> %t, want %t", tc.header, got, tc.want)
		}
	}
}
