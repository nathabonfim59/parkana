// Corpus fixture: ProfileCorpus replaces the default synthetic lines with a
// deterministic comparison corpus used to diff Parkana against real Loki.
// Every attribute of record n derives from n through a fixed splitmix64
// stream, so the same index always yields the same record regardless of
// batch size, pacing or send order, and changing the fixture is a deliberate
// seed change.

package gen

import (
	"encoding/json/v2"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
)

// ProfileCorpus selects the deterministic corpus line generator.
const ProfileCorpus = "corpus"

// corpusSeed fixes the fixture. Changing it regenerates every record.
const corpusSeed uint64 = 0x7061_726b_616e_6101 // "parkana" + 1

// CorpusRecord is one deterministic corpus fixture record: the fields the
// line encodes, the varying stream labels and the structured metadata.
type CorpusRecord struct {
	// ID is the line index, unique across a run.
	ID int
	// Stream is the stream name the record was assigned to.
	Stream string
	// Format is the line encoding and the format label: json, logfmt or plain.
	Format string
	// Level is the level label and the level inside the line: info, warn or
	// error (plain lines render it upper-case).
	Level string
	// Pod is the pod label and the pod inside the line: pod-a or pod-b.
	Pod string
	// Status is an HTTP-like status: nested as response.status in JSON lines,
	// flat status= elsewhere.
	Status int
	// Latency is a numeric millisecond value for unwrap.
	Latency float64
	// Bytes is a numeric byte count for unwrap.
	Bytes int64
	// Duration is a Go duration string ("15ms", "1.5s") for
	// duration_seconds unwrap.
	Duration string
	// Size is a byte-unit string ("4KiB") for bytes unwrap.
	Size string
	// Msg is a free-text message with words for line filters.
	Msg string
	// TraceID and RequestID go out as structured metadata.
	TraceID, RequestID string
}

// corpusJSONLine is the JSON line layout; field order is the wire order.
type corpusJSONLine struct {
	ID       int     `json:"id"`
	Stream   string  `json:"stream"`
	Level    string  `json:"level"`
	Pod      string  `json:"pod"`
	Latency  float64 `json:"latency"`
	Bytes    int64   `json:"bytes"`
	Duration string  `json:"duration"`
	Size     string  `json:"size"`
	Response struct {
		Status int `json:"status"`
	} `json:"response"`
	Msg string `json:"msg"`
}

// corpusRNG is a splitmix64 stream seeded per record; next() is called once
// per attribute, in declaration order below.
type corpusRNG uint64

func (r *corpusRNG) next() uint64 {
	*r += 0x9e37_79b9_7f4a_7c15
	z := uint64(*r)
	z = (z ^ (z >> 30)) * 0xbf58_476d_1ce4_e5b9
	z = (z ^ (z >> 27)) * 0x94d0_49bb_1331_11eb
	return z ^ (z >> 31)
}

// pick returns choices[n % len(choices)].
func pick[T any](choices []T, n uint64) T { return choices[n%uint64(len(choices))] }

// NewCorpusRecord returns the corpus record for line n on stream name
// stream. It is a pure function: the same (n, stream) always yields the
// same record, so generation does not depend on batch size or order.
func NewCorpusRecord(n int, stream string) CorpusRecord {
	// The choice tables live here, not at package level, so nothing can
	// mutate the fixture between calls.
	formats := [...]string{"json", "logfmt", "plain"}
	levels := [...]string{"info", "warn", "error"}
	pods := [...]string{"pod-a", "pod-b"}
	statuses := [...]int{200, 201, 204, 301, 400, 401, 403, 404, 500, 502, 503}
	sizes := [...]string{"128B", "512B", "1KiB", "4KiB", "64KiB", "256KiB", "1MiB", "4MiB"}
	msgs := [...]string{
		"request completed",
		"connection timed out",
		"cache miss",
		"user login succeeded",
		"disk quota exceeded",
		"retrying upstream",
		"invalid token",
		"slow query",
	}
	r := corpusRNG(corpusSeed + uint64(n))
	latency := float64(r.next()%5000)/10 + 0.5 // 0.5..500.4 ms, one decimal
	return CorpusRecord{
		ID:        n,
		Stream:    stream,
		Format:    pick(formats[:], r.next()),
		Level:     pick(levels[:], r.next()),
		Pod:       pick(pods[:], r.next()),
		Status:    pick(statuses[:], r.next()),
		Latency:   latency,
		Bytes:     int64(64 + r.next()%65536),
		Duration:  (time.Duration(1+r.next()%120_000) * time.Millisecond).String(),
		Size:      pick(sizes[:], r.next()),
		Msg:       pick(msgs[:], r.next()),
		TraceID:   fmt.Sprintf("%016x", r.next()),
		RequestID: fmt.Sprintf("req-%08x", r.next()&0xffff_ffff),
	}
}

// Labels returns the per-record stream labels: format, level and pod. They
// are appended after the configured labels, so the fixture wins collisions.
func (r CorpusRecord) Labels() logs.Labels {
	return logs.Labels{
		{Name: "format", Value: r.Format},
		{Name: "level", Value: r.Level},
		{Name: "pod", Value: r.Pod},
	}
}

// Metadata returns the structured metadata attached to the entry.
func (r CorpusRecord) Metadata() logs.Labels {
	return logs.Labels{
		{Name: "request_id", Value: r.RequestID},
		{Name: "trace_id", Value: r.TraceID},
	}
}

// Line renders the record in its format. JSON nests status under
// response.status; logfmt and plain carry it flat. All formats embed the id,
// numeric latency and bytes, a duration string and a size string.
// A non-nil error means the record could not be encoded (e.g. a non-finite
// or unsupported field value); records from NewCorpusRecord never fail.
func (r CorpusRecord) Line() (string, error) {
	lat := strconv.FormatFloat(r.Latency, 'f', -1, 64)
	switch r.Format {
	case "json":
		line := corpusJSONLine{
			ID: r.ID, Stream: r.Stream, Level: r.Level, Pod: r.Pod,
			Latency: r.Latency, Bytes: r.Bytes, Duration: r.Duration,
			Size: r.Size, Msg: r.Msg,
		}
		line.Response.Status = r.Status
		b, err := json.Marshal(line)
		if err != nil {
			return "", fmt.Errorf("pushgen: corpus record %d: marshal json line: %w", r.ID, err)
		}
		return string(b), nil
	case "logfmt":
		return fmt.Sprintf("id=%d stream=%s level=%s pod=%s status=%d latency=%s bytes=%d duration=%s size=%s msg=%q",
			r.ID, r.Stream, r.Level, r.Pod, r.Status, lat, r.Bytes, r.Duration, r.Size, r.Msg), nil
	default: // plain: parseable free text for regexp and pattern stages
		return fmt.Sprintf("%s %s status=%d latency=%sms bytes=%d duration=%s size=%s msg=%q id=%d",
			strings.ToUpper(r.Level), r.Pod, r.Status, lat, r.Bytes, r.Duration, r.Size, r.Msg, r.ID), nil
	}
}

// corpusBatchRequest builds the push request for corpus lines [lo, hi),
// round-robin over the stream names. Records carry varying format/level/pod
// labels, so entries are grouped by complete label identity into one stream
// per distinct label set, in first-seen order.
func corpusBatchRequest(start time.Time, cfg Config, base []logs.Labels, lo, hi int) (*logs.PushRequest, error) {
	req := new(logs.PushRequest)
	group := make(map[string]int) // canonical label set -> req.Streams index
	for i := lo; i < hi; i++ {
		s := i % len(cfg.Streams)
		rec := NewCorpusRecord(i, cfg.Streams[s])
		line, err := rec.Line()
		if err != nil {
			return nil, err
		}
		ls := logs.New(append(slices.Clone(base[s]), rec.Labels()...)...)
		gi, ok := group[ls.String()]
		if !ok {
			gi = len(req.Streams)
			group[ls.String()] = gi
			req.Streams = append(req.Streams, logs.Stream{Labels: ls})
		}
		req.Streams[gi].Entries = append(req.Streams[gi].Entries, logs.Entry{
			Timestamp: start.Add(time.Duration(i) * time.Millisecond),
			Line:      line,
			Metadata:  rec.Metadata(),
		})
	}
	return req, nil
}
