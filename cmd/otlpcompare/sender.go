package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"google.golang.org/protobuf/encoding/protowire"
)

const (
	contentTypeProtobuf = "application/x-protobuf"
	contentTypeJSON     = "application/json"

	// Loki 3.7.9 answers an empty OTLP export with HTTP 422 and a marshaled
	// google.rpc.Status (pkg/loghttp/push/push.go OTLPError uses
	// grpcstatus.New(0, errorStr).Proto(); the message comes from
	// pkg/validation/validate.go MissingStreamsErrorMsg). An empty readiness
	// probe produces no streams, so this exact binary Status proves the
	// endpoint is up even though it is not 2xx. No other 4xx status is
	// accepted, and the body must carry this exact message.
	lokiMissingStreamsStatus  = http.StatusUnprocessableEntity
	lokiMissingStreamsMessage = "error at least one valid stream is required for ingestion"

	// defaultRetryBackoff is the first retry delay; it doubles per attempt up
	// to maxRetryBackoff.
	defaultRetryBackoff = 200 * time.Millisecond
	maxRetryBackoff     = 2 * time.Second
)

// senderConfig is the validated invocation of the send subcommand.
type senderConfig struct {
	target       string
	manifestPath string
	namespace    string
	runID        string
	anchor       time.Time
	codec        string
	gzip         bool
	tenant       tenant.ID
	timeout      time.Duration
	retries      int
	readyTimeout time.Duration
	backoff      time.Duration
}

// sendUsage documents the send subcommand.
const sendUsage = `Usage:
  otlpcompare send -target URL -manifest PATH [flags]

send encodes one deterministic OTLP export per fixture case and posts it to a
single OTLP/HTTP logs endpoint. A collector between the target and the two
stores (Alloy in the e2e stack) is expected to fan the same request out to
Loki's native OTLP receiver and Parkana's /v1/logs.

Every case is one resource + one scope + its records. Timestamps are anchored
to -anchor (default: now, truncated to microseconds) and spaced one second
apart. Every request carries X-Scope-OrgID. The manifest records the fixture
identity, the namespace, every case's codec and expected record count, and the
half-open query window; compare requires it and never guesses counts.

Flags:
`

func parseSendFlags(args []string, stderr io.Writer) (senderConfig, error, bool) {
	fs := flag.NewFlagSet("otlpcompare send", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		target       = fs.String("target", "", "OTLP/HTTP logs endpoint, e.g. http://127.0.0.1:4318/v1/logs")
		manifest     = fs.String("manifest", "", "write the fixture manifest to this path")
		namespace    = fs.String("namespace", "", "service.namespace isolation value (default otlpcompare-<anchor unix microseconds>)")
		runID        = fs.String("run-id", "", "fixture run identifier (default phase4-otlp-v1-<anchor unix microseconds>)")
		anchorRaw    = fs.String("anchor", "", "RFC3339 timestamp anchoring fixture records (default now, truncated to microseconds)")
		codec        = fs.String("codec", codecMixed, "payload codec: mixed (alternates protobuf/json per case), protobuf or json")
		gzipOn       = fs.Bool("gzip", false, "send gzip-compressed bodies")
		tenantRaw    = fs.String("tenant", string(tenant.Default), "tenant id sent as X-Scope-OrgID")
		timeout      = fs.Duration("timeout", 30*time.Second, "per-request HTTP timeout")
		retries      = fs.Int("retries", 5, "bounded attempts per case")
		readyTimeout = fs.Duration("ready-timeout", 30*time.Second, "wait this long for the target to answer a readiness probe (0 skips the probe)")
		backoff      = fs.Duration("retry-backoff", defaultRetryBackoff, "first retry delay; doubles per attempt")
	)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), sendUsage)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return senderConfig{}, err, errors.Is(err, flag.ErrHelp)
	}
	if fs.NArg() > 0 {
		return senderConfig{}, fmt.Errorf("unexpected arguments %q", fs.Args()), false
	}
	if *target == "" {
		return senderConfig{}, errors.New("-target is required"), false
	}
	if *manifest == "" {
		return senderConfig{}, errors.New("-manifest is required"), false
	}
	targetURL, err := normalizeTarget(*target)
	if err != nil {
		return senderConfig{}, fmt.Errorf("-target: %w", err), false
	}
	anchor := time.Now().UTC().Truncate(time.Microsecond)
	if *anchorRaw != "" {
		anchor, err = parseTime(*anchorRaw)
		if err != nil {
			return senderConfig{}, fmt.Errorf("-anchor: %w", err), false
		}
		anchor = anchor.Truncate(time.Microsecond)
	}
	id := tenant.ID(*tenantRaw)
	if err := id.Validate(); err != nil {
		return senderConfig{}, fmt.Errorf("-tenant: %w", err), false
	}
	if !validCodec(*codec) {
		return senderConfig{}, fmt.Errorf("-codec must be mixed, protobuf or json, got %q", *codec), false
	}
	if !validNamespace(*namespace) {
		return senderConfig{}, fmt.Errorf("-namespace %q must be 1-128 characters from [A-Za-z0-9._-]", *namespace), false
	}
	if *retries < 1 {
		return senderConfig{}, fmt.Errorf("-retries must be positive, got %d", *retries), false
	}
	if *timeout <= 0 {
		return senderConfig{}, fmt.Errorf("-timeout must be positive, got %v", *timeout), false
	}
	if *readyTimeout < 0 {
		return senderConfig{}, fmt.Errorf("-ready-timeout must not be negative, got %v", *readyTimeout), false
	}
	if *backoff < 0 {
		return senderConfig{}, fmt.Errorf("-retry-backoff must not be negative, got %v", *backoff), false
	}
	ns := *namespace
	if ns == "" {
		ns = fmt.Sprintf("otlpcompare-%d", anchor.UnixMicro())
	}
	run := *runID
	if run == "" {
		run = fmt.Sprintf("%s-%d", fixtureID, anchor.UnixMicro())
	}
	return senderConfig{
		target:       targetURL,
		manifestPath: *manifest,
		namespace:    ns,
		runID:        run,
		anchor:       anchor,
		codec:        *codec,
		gzip:         *gzipOn,
		tenant:       id,
		timeout:      *timeout,
		retries:      *retries,
		readyTimeout: *readyTimeout,
		backoff:      *backoff,
	}, nil, false
}

// validNamespace keeps the isolation label easy to quote in LogQL selectors.
func validNamespace(namespace string) bool {
	if namespace == "" {
		return true // the default is derived from the anchor
	}
	if len(namespace) > 128 {
		return false
	}
	for _, c := range []byte(namespace) {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// normalizeTarget validates the -target URL. A query or fragment is rejected
// because the endpoint is used as given.
func normalizeTarget(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("%q: scheme must be http or https", raw)
	}
	if u.Host == "" {
		return "", fmt.Errorf("%q: missing host", raw)
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", fmt.Errorf("%q: query and fragment are not allowed", raw)
	}
	return u.String(), nil
}

// runSend sends the whole fixture corpus and writes the manifest.
func runSend(ctx context.Context, cfg senderConfig, stdout, stderr io.Writer) int {
	plan := buildPlan(cfg.anchor, cfg.runID, cfg.namespace)
	m := plan.buildManifest(cfg.target, cfg.codec, cfg.gzip)
	client := &http.Client{}

	if cfg.readyTimeout > 0 {
		if err := waitForTarget(ctx, client, cfg); err != nil {
			fmt.Fprintf(stderr, "otlpcompare: %v\n", err)
			return exitFailure
		}
	}

	for i, c := range plan.cases {
		codec := codecFor(cfg.codec, i)
		body, contentType, err := encodeCase(plan, c, codec)
		if err != nil {
			fmt.Fprintf(stderr, "otlpcompare: %s: %v\n", c.name, err)
			return exitFailure
		}
		if cfg.gzip {
			body, err = gzipBytes(body)
			if err != nil {
				fmt.Fprintf(stderr, "otlpcompare: %s: gzip: %v\n", c.name, err)
				return exitFailure
			}
		}
		if err := postWithRetry(ctx, client, cfg, contentType, body, stderr); err != nil {
			fmt.Fprintf(stderr, "otlpcompare: %s: %v\n", c.name, err)
			return exitFailure
		}
	}

	if err := writeManifest(cfg.manifestPath, m); err != nil {
		fmt.Fprintf(stderr, "otlpcompare: %v\n", err)
		return exitFailure
	}
	fmt.Fprintf(stdout, "sent %d records in %d requests to %s\n", plan.total, len(plan.cases), cfg.target)
	fmt.Fprintf(stdout, "manifest %s: fixture=%s v%d namespace=%s run=%s anchor=%s\n",
		cfg.manifestPath, fixtureID, fixtureVersion, cfg.namespace, cfg.runID, formatTime(cfg.anchor))
	return exitEqual
}

// encodeCase marshals one fixture export in the requested codec.
func encodeCase(plan fixturePlan, c fixtureCase, codec string) ([]byte, string, error) {
	request := plogotlp.NewExportRequestFromLogs(plan.logs(c))
	switch codec {
	case codecProtobuf:
		body, err := request.MarshalProto()
		if err != nil {
			return nil, "", fmt.Errorf("marshal protobuf: %w", err)
		}
		return body, contentTypeProtobuf, nil
	case codecJSON:
		body, err := request.MarshalJSON()
		if err != nil {
			return nil, "", fmt.Errorf("marshal JSON: %w", err)
		}
		return body, contentTypeJSON, nil
	default:
		return nil, "", fmt.Errorf("unknown codec %q", codec)
	}
}

func gzipBytes(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write(body); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// otlpResponse is the status line and bounded body of one OTLP HTTP response.
type otlpResponse struct {
	status     int
	statusLine string
	body       []byte
}

// waitForTarget probes the OTLP endpoint with an empty export until it answers
// 2xx or the pinned Loki empty-push Status, the readiness timeout expires, or
// the context is canceled.
func waitForTarget(ctx context.Context, client *http.Client, cfg senderConfig) error {
	probe, err := plogotlp.NewExportRequest().MarshalProto()
	if err != nil {
		return fmt.Errorf("readiness probe: %w", err)
	}
	if cfg.gzip {
		probe, err = gzipBytes(probe)
		if err != nil {
			return fmt.Errorf("readiness probe: gzip: %w", err)
		}
	}
	deadline := time.Now().Add(cfg.readyTimeout)
	var lastErr error
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("target %s not ready after %s: %w", cfg.target, cfg.readyTimeout, lastErr)
		}
		resp, err := postOnce(ctx, client, cfg, contentTypeProtobuf, probe, min(cfg.timeout, remaining))
		if err == nil && readinessAccepted(resp) {
			return nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("HTTP %s: %s", resp.statusLine, bodySnippet(resp.body))
		}
		if ctx.Err() != nil {
			return fmt.Errorf("readiness probe canceled: %w", ctx.Err())
		}
		remaining = time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("target %s not ready after %s: %w", cfg.target, cfg.readyTimeout, lastErr)
		}
		if err := sleepContext(ctx, min(cfg.backoff, remaining)); err != nil {
			return fmt.Errorf("readiness probe canceled: %w", err)
		}
	}
}

// readinessAccepted accepts any 2xx, plus the pinned Loki 422 whose body is a
// google.rpc.Status with code 0 and the MissingStreamsErrorMsg message that an
// empty OTLP export produces on Loki's native endpoint.
func readinessAccepted(resp otlpResponse) bool {
	if resp.status >= 200 && resp.status < 300 {
		return true
	}
	return resp.status == lokiMissingStreamsStatus && isLokiMissingStreamsStatus(resp.body)
}

// isLokiMissingStreamsStatus validates the protobuf google.rpc.Status shape
// Loki writes. Proto3 omits code 0, so the canonical body is just the message
// field; an explicit code 0 is also accepted. Malformed wire data, a different
// message, a nonzero code and unknown fields are rejected.
func isLokiMissingStreamsStatus(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	code := uint64(0)
	message := ""
	haveMessage := false
	rest := body
	for len(rest) > 0 {
		number, wireType, n := protowire.ConsumeTag(rest)
		if n < 0 {
			return false
		}
		rest = rest[n:]
		switch number {
		case 1: // google.rpc.Status.code
			if wireType != protowire.VarintType {
				return false
			}
			value, n := protowire.ConsumeVarint(rest)
			if n < 0 {
				return false
			}
			code = value
			rest = rest[n:]
		case 2: // google.rpc.Status.message
			if wireType != protowire.BytesType {
				return false
			}
			value, n := protowire.ConsumeBytes(rest)
			if n < 0 {
				return false
			}
			message = string(value)
			haveMessage = true
			rest = rest[n:]
		case 3: // google.rpc.Status.details, allowed but unused
			if wireType != protowire.BytesType {
				return false
			}
			_, n := protowire.ConsumeBytes(rest)
			if n < 0 {
				return false
			}
			rest = rest[n:]
		default:
			return false
		}
	}
	return haveMessage && code == 0 && message == lokiMissingStreamsMessage
}

// postWithRetry posts one encoded case with a bounded number of attempts. 4xx
// responses are terminal; transport failures, 429 and 5xx are retried.
func postWithRetry(ctx context.Context, client *http.Client, cfg senderConfig, contentType string, body []byte, stderr io.Writer) error {
	var lastErr error
	for attempt := 0; attempt < cfg.retries; attempt++ {
		resp, err := postOnce(ctx, client, cfg, contentType, body, cfg.timeout)
		if err == nil && resp.status >= 200 && resp.status < 300 {
			return nil
		}
		switch {
		case err != nil:
			lastErr = err
		case retryableStatus(resp.status):
			lastErr = fmt.Errorf("HTTP %s: %s", resp.statusLine, bodySnippet(resp.body))
		default:
			return fmt.Errorf("HTTP %s is not retryable: %s", resp.statusLine, bodySnippet(resp.body))
		}
		if attempt+1 < cfg.retries {
			fmt.Fprintf(stderr, "otlpcompare: attempt %d/%d: %v\n", attempt+1, cfg.retries, lastErr)
			delay := configBackoff(cfg.backoff, attempt)
			if err := sleepContext(ctx, delay); err != nil {
				return fmt.Errorf("canceled after %d attempts: %w", attempt+1, err)
			}
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", cfg.retries, lastErr)
}

// postOnce performs exactly one HTTP request with the given timeout. Only
// transport and read failures are errors; any HTTP status is returned to the
// caller.
func postOnce(ctx context.Context, client *http.Client, cfg senderConfig, contentType string, body []byte, timeout time.Duration) (otlpResponse, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, cfg.target, bytes.NewReader(body))
	if err != nil {
		return otlpResponse{}, err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set(tenant.Header, string(cfg.tenant))
	if cfg.gzip {
		req.Header.Set("Content-Encoding", "gzip")
	}
	resp, err := client.Do(req)
	if err != nil {
		return otlpResponse{}, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	if err != nil {
		return otlpResponse{status: resp.StatusCode, statusLine: resp.Status}, fmt.Errorf("read response: %w", err)
	}
	return otlpResponse{status: resp.StatusCode, statusLine: resp.Status, body: respBody}, nil
}

func retryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

// configBackoff doubles the configured delay per attempt, capped.
func configBackoff(base time.Duration, attempt int) time.Duration {
	delay := base
	for range attempt {
		if delay >= maxRetryBackoff {
			return maxRetryBackoff
		}
		delay *= 2
	}
	return min(delay, maxRetryBackoff)
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
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

// bodySnippet bounds an error body included in a failure message.
func bodySnippet(body []byte) string {
	const limit = 300
	s := string(body)
	if len(s) > limit {
		return s[:limit] + "..."
	}
	if s == "" {
		return "(empty body)"
	}
	return s
}
