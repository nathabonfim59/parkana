// Package gen implements pushgen's send loop: it batches synthetic lines into
// Loki push requests, paces them at a configurable line rate and POSTs them to
// a push URL. The wire format is behind the Encoder interface, so the loop can
// be tested without a codec and run against any request representation.
package gen

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	// DefaultStreamName is the stream pushgen fills in when none is configured.
	DefaultStreamName = "pushgen"
	// DefaultStreamLabel is the label carrying the stream name, so parkana's
	// stream selector (first present of service_name, job) picks the dataset.
	DefaultStreamLabel = "service_name"
)

// Encoder renders a push request in one wire format. Implementations must be
// safe for sequential use and must respect the context.
type Encoder interface {
	// ContentType is the request's Content-Type header value.
	ContentType() string
	// Encode renders req as the request body.
	Encode(ctx context.Context, req *logs.PushRequest) ([]byte, error)
}

// Config is one pushgen run. The zero value is invalid; Run validates it.
type Config struct {
	// URL is the push endpoint, e.g. http://localhost:3100/loki/api/v1/push.
	URL string
	// Tenant is the tenant of the pushed data: Run puts it in the context
	// handed to Encoder.Encode and sends it as X-Scope-OrgID. Empty uses
	// tenant.Default in the context and omits the header.
	Tenant tenant.ID
	// Streams are the stream names; line i goes to Streams[i % len(Streams)].
	// Empty selects DefaultStreamName.
	Streams []string
	// StreamLabel is the label carrying the stream name (default
	// DefaultStreamLabel).
	StreamLabel string
	// Labels are extra labels set on every stream.
	Labels logs.Labels
	// Profile selects the line generator: empty sends the default synthetic
	// lines, ProfileCorpus sends the deterministic comparison corpus. The
	// corpus adds its own format/level/pod labels (winning collisions with
	// Labels) and structured metadata. Every profile aligns timestamps to
	// the microsecond storage grid.
	Profile string
	// Rate is the target line rate in lines per second; 0 sends without
	// pacing. Pacing is counted in lines, not requests.
	Rate float64
	// BatchSize is the number of lines per push request.
	BatchSize int
	// Total is the exact number of lines to send; the last batch may be
	// partial.
	Total int
	// Gzip wraps the encoded body in gzip (Content-Encoding: gzip).
	Gzip bool
	// Encoder renders the requests.
	Encoder Encoder
	// Client sends the requests; nil uses http.DefaultClient. Set one with a
	// Timeout for bounded requests.
	Client *http.Client
	// Output receives a one-line summary after the run; nil discards it.
	Output io.Writer
}

// Run sends cfg.Total synthetic lines to cfg.URL. Each line carries a
// deterministic identifier ("id=<n> stream=<name>") and a timestamp one
// millisecond after the previous line's, so runs are reproducible given the
// same configuration. It fails on the first non-204 response or transport
// error, and returns the context error when ctx is canceled mid-run.
func Run(ctx context.Context, cfg Config) error {
	if err := cfg.fill(); err != nil {
		return err
	}
	// The context always carries a resolved tenant, so Encoders and any
	// handler-side verification see one; an empty Tenant keeps the header
	// off but still defaults the context.
	tid := cfg.Tenant
	if tid == "" {
		tid = tenant.Default
	}
	if err := tid.Validate(); err != nil {
		return fmt.Errorf("pushgen: -tenant: %w", err)
	}
	ctx = tenant.With(ctx, tid)

	labels := streamLabels(cfg)
	for i, ls := range labels {
		if err := ls.Validate(); err != nil {
			return fmt.Errorf("pushgen: stream %q: %w", cfg.Streams[i], err)
		}
	}

	out := cfg.Output
	if out == nil {
		out = io.Discard
	}
	client := cfg.Client
	if client == nil {
		client = http.DefaultClient
	}
	// Generated timestamps align to the microsecond storage grid so
	// Loki/Parkana comparisons see identical ranges in every profile.
	start := time.Now().UTC().Truncate(time.Microsecond)

	var requests int
	var next time.Time // earliest send time of the next batch when pacing
	for lo := 0; lo < cfg.Total; lo += cfg.BatchSize {
		hi := min(lo+cfg.BatchSize, cfg.Total)
		if !next.IsZero() {
			if err := sleepUntil(ctx, next); err != nil {
				return fmt.Errorf("pushgen: canceled after %d of %d lines: %w", lo, cfg.Total, err)
			}
		}
		req, err := batchRequest(start, cfg, labels, lo, hi)
		if err != nil {
			return fmt.Errorf("pushgen: lines %d-%d of %d: %w", lo, hi, cfg.Total, err)
		}
		if err := send(ctx, client, &cfg, req); err != nil {
			return fmt.Errorf("pushgen: lines %d-%d of %d: %w", lo, hi, cfg.Total, err)
		}
		requests++
		if cfg.Rate > 0 {
			if next.IsZero() {
				next = time.Now()
			}
			next = next.Add(time.Duration(float64(hi-lo) / cfg.Rate * float64(time.Second)))
		}
	}
	fmt.Fprintf(out, "pushgen: sent %d lines in %d requests to %s\n", cfg.Total, requests, cfg.URL)
	return nil
}

// fill applies defaults and checks the required fields.
func (c *Config) fill() error {
	if c.URL == "" {
		return fmt.Errorf("pushgen: no -url given")
	}
	if c.Total <= 0 {
		return fmt.Errorf("pushgen: -total must be positive, got %d", c.Total)
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("pushgen: -batch must be positive, got %d", c.BatchSize)
	}
	if math.IsNaN(c.Rate) || math.IsInf(c.Rate, 0) {
		return fmt.Errorf("pushgen: -rate must be finite, got %v", c.Rate)
	}
	if c.Rate < 0 {
		return fmt.Errorf("pushgen: -rate must not be negative, got %v", c.Rate)
	}
	if c.Encoder == nil {
		return fmt.Errorf("pushgen: no wire format encoder configured")
	}
	if c.Profile != "" && c.Profile != ProfileCorpus {
		return fmt.Errorf("pushgen: -profile must be empty or %q, got %q", ProfileCorpus, c.Profile)
	}
	c.Streams = dedupe(c.Streams)
	if len(c.Streams) == 0 {
		c.Streams = []string{DefaultStreamName}
	}
	if c.StreamLabel == "" {
		c.StreamLabel = DefaultStreamLabel
	}
	return nil
}

// streamLabels builds the label set of every configured stream: the extra
// labels plus the stream name under cfg.StreamLabel (last, so it wins over a
// duplicate extra label).
func streamLabels(cfg Config) []logs.Labels {
	labels := make([]logs.Labels, len(cfg.Streams))
	for i, name := range cfg.Streams {
		pairs := append(slices.Clone(cfg.Labels), logs.Label{Name: cfg.StreamLabel, Value: name})
		labels[i] = logs.New(pairs...)
	}
	return labels
}

// batchRequest builds the push request for lines [lo, hi), round-robin over
// the streams, preserving line order within each stream.
func batchRequest(start time.Time, cfg Config, labels []logs.Labels, lo, hi int) (*logs.PushRequest, error) {
	if cfg.Profile == ProfileCorpus {
		return corpusBatchRequest(start, cfg, labels, lo, hi)
	}
	entries := make([][]logs.Entry, len(cfg.Streams))
	for i := lo; i < hi; i++ {
		s := i % len(cfg.Streams)
		entries[s] = append(entries[s], logs.Entry{
			Timestamp: start.Add(time.Duration(i) * time.Millisecond),
			Line:      fmt.Sprintf("id=%d stream=%s", i, cfg.Streams[s]),
		})
	}
	req := new(logs.PushRequest)
	for s, es := range entries {
		if len(es) == 0 {
			continue
		}
		req.Streams = append(req.Streams, logs.Stream{Labels: labels[s], Entries: es})
	}
	return req, nil
}

// send encodes and POSTs one push request, expecting 204.
func send(ctx context.Context, client *http.Client, cfg *Config, req *logs.PushRequest) error {
	body, err := cfg.Encoder.Encode(ctx, req)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	if cfg.Gzip {
		if body, err = gzipBytes(body); err != nil {
			return err
		}
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", cfg.Encoder.ContentType())
	if cfg.Tenant != "" {
		r.Header.Set(tenant.Header, string(cfg.Tenant))
	}
	if cfg.Gzip {
		r.Header.Set("Content-Encoding", "gzip")
	}
	resp, err := client.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		_, _ = io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("unexpected status %s: %s", resp.Status, bytes.TrimSpace(snippet))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// gzipBytes returns b gzip-compressed.
func gzipBytes(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		return nil, fmt.Errorf("pushgen: gzip: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("pushgen: gzip: %w", err)
	}
	return buf.Bytes(), nil
}

// sleepUntil waits until t or ctx is done, whichever comes first.
func sleepUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// dedupe removes duplicate stream names, keeping the first occurrence.
func dedupe(names []string) []string {
	seen := make(map[string]bool, len(names))
	out := make([]string, 0, len(names))
	for _, n := range names {
		if !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	return out
}
