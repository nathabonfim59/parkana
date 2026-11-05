// Command pushgen generates synthetic Loki push traffic for tests,
// benchmarks and end-to-end runs: it batches deterministic lines ("id=<n>
// stream=<name>"), encodes them with the shared push codec (JSON, or
// protobuf compressed with block-snappy) and POSTs them to a push URL.
//
// Examples:
//
//	pushgen -url=http://localhost:3100/loki/api/v1/push -total=10000 \
//	        -format=protobuf -stream=checkout -rate=5000
//	pushgen -url=http://localhost:3100/loki/api/v1/push -format=json -gzip \
//	        -stream=app1 -stream=app2 -label=env=dev -total=100000 -batch=1000
//
// The -profile=corpus profile emits the deterministic comparison corpus
// used to diff Parkana against Loki:
//
//	pushgen -url=http://localhost:3100/loki/api/v1/push -profile=corpus \
//	        -format=json -stream=corpus -label=job=corpus -label=env=dev \
//	        -total=1200 -rate=1000 -batch=100
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nathabonfim59/parkana/cmd/pushgen/gen"
	"github.com/nathabonfim59/parkana/internal/ingest/pushcodec"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "pushgen:", err)
		os.Exit(1)
	}
}

// run parses args and performs one pushgen run, printing a summary on out.
func run(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("pushgen", flag.ContinueOnError)
	fs.SetOutput(out)
	url := fs.String("url", "", "push endpoint, e.g. http://localhost:3100/loki/api/v1/push")
	tenantFlag := fs.String("tenant", "", "tenant id sent as X-Scope-OrgID (empty: no header, tenant 'default')")
	format := fs.String("format", "json", "wire format: json or protobuf (block-snappy)")
	gzipIt := fs.Bool("gzip", false, "wrap the request body in gzip (Content-Encoding: gzip)")
	streamLabel := fs.String("stream-label", gen.DefaultStreamLabel, "label that carries the stream name")
	var streamNames []string
	fs.Var(repeatList{p: &streamNames}, "stream", "stream name (repeatable)")
	var labelPairs []string
	fs.Var(repeatList{p: &labelPairs}, "label", "extra stream label name=value (repeatable)")
	rate := fs.Float64("rate", 0, "lines per second; 0 sends without pacing")
	batch := fs.Int("batch", 100, "lines per push request")
	total := fs.Int("total", 1000, "total lines to send")
	profile := fs.String("profile", "", "line generator profile: empty for default synthetic lines, corpus for the deterministic comparison corpus")
	timeout := fs.Duration("timeout", 30*time.Second, "per-request HTTP timeout (0 disables the timeout)")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // help was printed
		}
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("pushgen: unexpected arguments %q", fs.Args())
	}
	if *url == "" {
		return fmt.Errorf("pushgen: no -url given")
	}
	enc, err := encoderFor(*format)
	if err != nil {
		return err
	}
	labels, err := parseLabels(labelPairs)
	if err != nil {
		return err
	}
	tid := tenant.ID(*tenantFlag)
	if tid != "" {
		if err := tid.Validate(); err != nil {
			return fmt.Errorf("pushgen: -tenant: %w", err)
		}
	}
	if *timeout < 0 {
		return fmt.Errorf("pushgen: -timeout must not be negative, got %v", *timeout)
	}
	for _, name := range streamNames {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("pushgen: -stream must not be empty")
		}
	}

	return gen.Run(ctx, gen.Config{
		URL: *url, Tenant: tid, Streams: streamNames, StreamLabel: *streamLabel,
		Labels: labels, Rate: *rate, BatchSize: *batch, Total: *total,
		Profile: *profile,
		Gzip:    *gzipIt, Encoder: enc, Client: &http.Client{Timeout: *timeout}, Output: out,
	})
}

// encoderFor picks the wire encoder for a -format value.
func encoderFor(format string) (gen.Encoder, error) {
	switch strings.ToLower(format) {
	case "json":
		return codecEncoder{"application/json", pushcodec.MarshalJSON}, nil
	case "protobuf", "proto":
		return codecEncoder{"application/x-protobuf", pushcodec.MarshalSnappy}, nil
	default:
		return nil, fmt.Errorf("pushgen: -format must be json or protobuf, got %q", format)
	}
}

// codecEncoder adapts a pushcodec marshal function to gen.Encoder.
type codecEncoder struct {
	contentType string
	encode      func(context.Context, *logs.PushRequest) ([]byte, error)
}

func (e codecEncoder) ContentType() string { return e.contentType }

func (e codecEncoder) Encode(ctx context.Context, req *logs.PushRequest) ([]byte, error) {
	return e.encode(ctx, req)
}

// parseLabels parses repeated name=value flags into a sorted label set.
func parseLabels(pairs []string) (logs.Labels, error) {
	ls := make([]logs.Label, 0, len(pairs))
	for _, p := range pairs {
		name, value, ok := strings.Cut(p, "=")
		if !ok {
			return nil, fmt.Errorf("pushgen: -label %q is not name=value", p)
		}
		ls = append(ls, logs.Label{Name: strings.TrimSpace(name), Value: strings.TrimSpace(value)})
	}
	out := logs.New(ls...)
	if err := out.Validate(); err != nil {
		return nil, fmt.Errorf("pushgen: -label: %w", err)
	}
	return out, nil
}

// repeatList is a flag.Value appending every occurrence to a string list.
type repeatList struct{ p *[]string }

// String is nil-safe: flag help builds a zero repeatList to render defaults.
func (f repeatList) String() string {
	if f.p == nil {
		return ""
	}
	return strings.Join(*f.p, ",")
}

func (f repeatList) Set(s string) error {
	*f.p = append(*f.p, s)
	return nil
}
