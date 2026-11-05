package tail

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tail/wire"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// readSubscription reads one ingester's NDJSON tail stream. The first record
// must be a ready that names the request's tenant; only then is the
// subscription considered established. Afterwards every batch is processed
// through the query pipeline, and every deviation (HTTP error, bad or
// oversized record, unknown type, EOF, TypeDropped) is returned as an error so
// the session can surface an explicit gap.
func (s *session) readSubscription(ctx context.Context, target ingester) error {
	u := target.url.Clone()
	u.Path = path.Join(u.Path, internalTailPath())
	u.RawPath = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return fmt.Errorf("tail: build ingester request for %s: %w", target.display, err)
	}
	req.Header.Set("Accept", wire.ContentType)
	req.Header.Set(tenant.Header, string(s.tenant))

	resp, err := s.svc.client.Do(req)
	if err != nil {
		return fmt.Errorf("tail: subscribe to ingester %s: %w", target.display, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("tail: ingester %s returned HTTP %d: %s", target.display, resp.StatusCode, errorSnippet(resp.Body))
	}
	if ct := resp.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(strings.ToLower(ct), wire.ContentType) {
		return protocolError(target, "unexpected content type %q", ct)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64<<10), maxRecordBytes)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("tail: read ready from ingester %s: %w", target.display, err)
		}
		return fmt.Errorf("tail: ingester %s closed the tail stream before ready", target.display)
	}
	var first wire.Message
	if err := json.Unmarshal(scanner.Bytes(), &first); err != nil {
		return protocolError(target, "malformed ready record")
	}
	if first.Type != wire.TypeReady {
		return protocolError(target, "first record is %q, want %q", first.Type, wire.TypeReady)
	}
	if first.Tenant != string(s.tenant) {
		return protocolError(target, "ready tenant %q does not match request tenant", first.Tenant)
	}
	select {
	case s.ready <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	for scanner.Scan() {
		var msg wire.Message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			return protocolError(target, "malformed record")
		}
		switch msg.Type {
		case wire.TypeBatch:
			entries, err := s.processBatch(ctx, target, msg.Entries)
			if err != nil {
				return err
			}
			if len(entries) > 0 {
				s.queue.push(entries)
			}
		case wire.TypeDropped:
			return fmt.Errorf("tail: ingester %s dropped %d entries (%s); the live tail is incomplete", target.display, msg.Dropped, strings.TrimSpace(msg.Reason))
		case wire.TypeReady:
			return protocolError(target, "unexpected second ready record")
		default:
			return protocolError(target, "unknown record type %q", msg.Type)
		}
	}
	if err := scanner.Err(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("tail: read ingester %s: %w", target.display, err)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("tail: ingester %s closed the tail stream; the live tail is incomplete", target.display)
}

// protocolError wraps a wire-contract violation with the ingester that sent
// it, without echoing the offending record (it may hold log content).
func protocolError(target ingester, format string, args ...any) error {
	return fmt.Errorf("tail: ingester %s protocol error: "+format, append([]any{target.display}, args...)...)
}

// errorSnippet returns a short single-line description of an upstream error
// body, enough to diagnose an HTTP failure without copying an arbitrary body
// into logs or a close reason.
func errorSnippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 256))
	s := strings.Join(strings.Fields(string(b)), " ")
	if s == "" {
		return "(empty body)"
	}
	return s
}

// parseWireTimestamp parses the wire's decimal Unix nanoseconds and returns
// the stored microsecond grid point, matching schema.Time(schema.TS(t)).
func parseWireTimestamp(raw string) (time.Time, error) {
	ns, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMicro(time.Unix(0, ns).UnixMicro()).UTC(), nil
}

// wireLabels validates and copies one wire label slice. The wire contract
// guarantees sorted, unique names; a violation is a protocol error because
// binary-search lookups would otherwise silently misbehave.
func wireLabels(in []wire.Label) (logs.Labels, error) {
	ls := make(logs.Labels, len(in))
	for i, l := range in {
		ls[i] = logs.Label{Name: l.Name, Value: l.Value}
	}
	if err := ls.Validate(); err != nil {
		return nil, err
	}
	return ls, nil
}
