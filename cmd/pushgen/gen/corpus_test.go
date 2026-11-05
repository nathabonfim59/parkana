package gen_test

import (
	"encoding/json/v2"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/cmd/pushgen/gen"
)

// corpusLineID extracts the embedded record id from any corpus line format.
func corpusLineID(t *testing.T, line string) int {
	t.Helper()
	switch {
	case strings.HasPrefix(line, "{"): // json
		var v struct {
			ID int `json:"id"`
		}
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			t.Fatalf("corpus json line %q: %v", line, err)
		}
		return v.ID
	case strings.HasPrefix(line, "id="): // logfmt
		rest := line[len("id="):]
		if i := strings.IndexByte(rest, ' '); i >= 0 {
			rest = rest[:i]
		}
		n, err := strconv.Atoi(rest)
		if err != nil {
			t.Fatalf("corpus logfmt line %q: %v", line, err)
		}
		return n
	default: // plain ends with " id=<n>"
		_, tail, ok := strings.CutLast(line, " id=")
		if !ok {
			t.Fatalf("corpus plain line %q has no id", line)
		}
		n, err := strconv.Atoi(tail)
		if err != nil {
			t.Fatalf("corpus plain line %q: %v", line, err)
		}
		return n
	}
}

// corpusLineFormat detects the line encoding from its shape: the same
// classes the format label carries.
func corpusLineFormat(line string) string {
	switch {
	case strings.HasPrefix(line, "{"):
		return "json"
	case strings.HasPrefix(line, "id="):
		return "logfmt"
	default:
		return "plain"
	}
}

func TestCorpusRecordDeterministic(t *testing.T) {
	for _, n := range []int{0, 1, 5, 42, 999, 65536} {
		a := gen.NewCorpusRecord(n, "corpus")
		b := gen.NewCorpusRecord(n, "corpus")
		if a != b {
			t.Errorf("NewCorpusRecord(%d) differs between calls: %+v vs %+v", n, a, b)
		}
		la, _ := a.Line()
		lb, _ := b.Line()
		if la != lb {
			t.Errorf("record %d line differs between calls: %q vs %q", n, la, lb)
		}
	}
	// The stream name only changes where the line carries it; the fixture
	// labels and metadata come from n alone.
	a := gen.NewCorpusRecord(7, "one")
	b := gen.NewCorpusRecord(7, "two")
	if !slices.Equal(a.Labels(), b.Labels()) || !slices.Equal(a.Metadata(), b.Metadata()) {
		t.Errorf("stream name changed labels/metadata: %v/%v vs %v/%v",
			a.Labels(), a.Metadata(), b.Labels(), b.Metadata())
	}
	if a.Stream != "one" || b.Stream != "two" {
		t.Errorf("Stream = %q, %q, want one, two", a.Stream, b.Stream)
	}
}

func TestCorpusCoverage(t *testing.T) {
	formats := map[string]int{}
	levels := map[string]int{}
	pods := map[string]int{}
	statuses := map[int]int{}
	msgs := map[string]int{}
	for i := range 1200 {
		r := gen.NewCorpusRecord(i, "corpus")
		formats[r.Format]++
		levels[r.Level]++
		pods[r.Pod]++
		statuses[r.Status]++
		msgs[r.Msg]++
		if err := r.Labels().Validate(); err != nil {
			t.Fatalf("record %d labels: %v", i, err)
		}
		if err := r.Metadata().Validate(); err != nil {
			t.Fatalf("record %d metadata: %v", i, err)
		}
		if r.Latency <= 0 || r.Latency > 500.4 {
			t.Errorf("record %d latency %v out of fixture range", i, r.Latency)
		}
		if r.Bytes < 64 {
			t.Errorf("record %d bytes %d out of fixture range", i, r.Bytes)
		}
		if _, err := time.ParseDuration(r.Duration); err != nil {
			t.Errorf("record %d duration %q does not parse: %v", i, r.Duration, err)
		}
		if tid := r.TraceID; len(tid) != 16 {
			t.Errorf("record %d trace_id %q, want 16 hex chars", i, tid)
		} else if _, err := strconv.ParseUint(tid, 16, 64); err != nil {
			t.Errorf("record %d trace_id %q not hex: %v", i, tid, err)
		}
		if !strings.HasPrefix(r.RequestID, "req-") {
			t.Errorf("record %d request_id %q, want req- prefix", i, r.RequestID)
		}
		line, err := r.Line()
		if err != nil {
			t.Fatalf("record %d Line(): %v", i, err)
		}
		if got := corpusLineFormat(line); got != r.Format {
			t.Errorf("record %d format %s rendered a %s-looking line: %q", i, r.Format, got, line)
		}
		if id := corpusLineID(t, line); id != i {
			t.Errorf("record %d line embeds id %d", i, id)
		}
	}
	for _, want := range []string{"json", "logfmt", "plain"} {
		if formats[want] == 0 {
			t.Errorf("format %s has no records in 1200", want)
		}
	}
	for _, want := range []string{"info", "warn", "error"} {
		if levels[want] == 0 {
			t.Errorf("level %s has no records in 1200", want)
		}
	}
	for _, want := range []string{"pod-a", "pod-b"} {
		if pods[want] == 0 {
			t.Errorf("pod %s has no records in 1200", want)
		}
	}
	if len(statuses) < 5 {
		t.Errorf("only %d distinct statuses in 1200 records, want variety", len(statuses))
	}
	if len(msgs) < 5 {
		t.Errorf("only %d distinct messages in 1200 records, want variety", len(msgs))
	}
	t.Logf("1200-record mix: formats=%v levels=%v pods=%v", formats, levels, pods)
}

// TestCorpusLineShapes parses each format the way a LogQL pipeline would.
func TestCorpusLineShapes(t *testing.T) {
	byFormat := map[string]gen.CorpusRecord{}
	for i := range 1200 {
		r := gen.NewCorpusRecord(i, "corpus")
		if _, ok := byFormat[r.Format]; !ok {
			byFormat[r.Format] = r
		}
		if len(byFormat) == 3 {
			break
		}
	}

	jsonRec := byFormat["json"]
	line, err := jsonRec.Line()
	if err != nil {
		t.Fatalf("json Line(): %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(line), &doc); err != nil {
		t.Fatalf("json line does not parse: %v", err)
	}
	for _, key := range []string{"id", "stream", "level", "pod", "latency", "bytes", "duration", "size", "response", "msg"} {
		if _, ok := doc[key]; !ok {
			t.Errorf("json line missing field %q: %s", key, line)
		}
	}
	resp, ok := doc["response"].(map[string]any)
	if !ok || resp["status"] != float64(jsonRec.Status) {
		t.Errorf("json line response.status = %v, want %d", doc["response"], jsonRec.Status)
	}
	if lat, ok := doc["latency"].(float64); !ok || lat != jsonRec.Latency {
		t.Errorf("json latency = %v, want numeric %v", doc["latency"], jsonRec.Latency)
	}

	logfmtRec := byFormat["logfmt"]
	line, err = logfmtRec.Line()
	if err != nil {
		t.Fatalf("logfmt Line(): %v", err)
	}
	fields := map[string]string{}
	for tok := range strings.FieldsSeq(line) {
		if k, v, ok := strings.Cut(tok, "="); ok {
			fields[k] = v
		}
	}
	for _, key := range []string{"id", "stream", "level", "pod", "status", "latency", "bytes", "duration", "size"} {
		if fields[key] == "" {
			t.Errorf("logfmt line missing field %q: %s", key, line)
		}
	}
	if fields["status"] != strconv.Itoa(logfmtRec.Status) {
		t.Errorf("logfmt status = %q, want %d", fields["status"], logfmtRec.Status)
	}
	if _, err := strconv.ParseFloat(fields["latency"], 64); err != nil {
		t.Errorf("logfmt latency %q not numeric: %v", fields["latency"], err)
	}
	if fields["level"] != logfmtRec.Level || fields["pod"] != logfmtRec.Pod {
		t.Errorf("logfmt level/pod = %q/%q, want %q/%q",
			fields["level"], fields["pod"], logfmtRec.Level, logfmtRec.Pod)
	}

	plainRec := byFormat["plain"]
	line, err = plainRec.Line()
	if err != nil {
		t.Fatalf("plain Line(): %v", err)
	}
	parts := strings.Fields(line)
	if len(parts) < 8 {
		t.Fatalf("plain line too short: %q", line)
	}
	if parts[0] != strings.ToUpper(plainRec.Level) || parts[1] != plainRec.Pod {
		t.Errorf("plain level/pod = %q/%q, want %q/%q",
			parts[0], parts[1], strings.ToUpper(plainRec.Level), plainRec.Pod)
	}
	pfields := map[string]string{}
	for _, tok := range parts[2:] {
		if k, v, ok := strings.Cut(tok, "="); ok {
			pfields[k] = v
		}
	}
	if _, err := strconv.Atoi(pfields["status"]); err != nil {
		t.Errorf("plain status %q not numeric: %v", pfields["status"], err)
	}
	lat, ok := strings.CutSuffix(pfields["latency"], "ms")
	if !ok {
		t.Errorf("plain latency %q missing ms suffix", pfields["latency"])
	} else if _, err := strconv.ParseFloat(lat, 64); err != nil {
		t.Errorf("plain latency %q not numeric: %v", lat, err)
	}
	if _, err := strconv.ParseInt(pfields["bytes"], 10, 64); err != nil {
		t.Errorf("plain bytes %q not numeric: %v", pfields["bytes"], err)
	}
}

// TestCorpusSnapshot freezes the first records so any fixture change is a
// deliberate, reviewable diff. Expected lines are literal strings, not
// derived from the generator.
func TestCorpusSnapshot(t *testing.T) {
	want := []struct {
		labels string
		line   string
		meta   string
	}{
		{
			`{format="json", level="info", pod="pod-a"}`,
			`{"id":0,"stream":"corpus","level":"info","pod":"pod-a","latency":176.3,"bytes":25995,"duration":"18.093s","size":"4KiB","response":{"status":400},"msg":"cache miss"}`,
			`{request_id="req-d672eb18", trace_id="7b8fdd306eb4b244"}`,
		},
		{
			`{format="json", level="warn", pod="pod-b"}`,
			`{"id":1,"stream":"corpus","level":"warn","pod":"pod-b","latency":137.9,"bytes":3569,"duration":"38.66s","size":"1MiB","response":{"status":201},"msg":"request completed"}`,
			`{request_id="req-d6997456", trace_id="506776892383ba64"}`,
		},
		{
			`{format="json", level="error", pod="pod-a"}`,
			`{"id":2,"stream":"corpus","level":"error","pod":"pod-a","latency":398.3,"bytes":27481,"duration":"1m31.211s","size":"4KiB","response":{"status":204},"msg":"invalid token"}`,
			`{request_id="req-20011bf3", trace_id="0d6770d5b1e24d5b"}`,
		},
		{
			`{format="logfmt", level="warn", pod="pod-a"}`,
			`id=3 stream=corpus level=warn pod=pod-a status=401 latency=118.9 bytes=46174 duration=1m58.305s size=4MiB msg="request completed"`,
			`{request_id="req-f8be96a1", trace_id="5c06a104de649058"}`,
		},
		{
			`{format="json", level="info", pod="pod-a"}`,
			`{"id":4,"stream":"corpus","level":"info","pod":"pod-a","latency":238.2,"bytes":29742,"duration":"55.347s","size":"128B","response":{"status":400},"msg":"slow query"}`,
			`{request_id="req-43247f69", trace_id="9ee2ba84b0bd1514"}`,
		},
		{
			`{format="logfmt", level="info", pod="pod-a"}`,
			`id=5 stream=corpus level=info pod=pod-a status=502 latency=178.1 bytes=30986 duration=1m48.858s size=1MiB msg="cache miss"`,
			`{request_id="req-a02ac11b", trace_id="f9cc4ba06810e4e7"}`,
		},
		{
			`{format="plain", level="warn", pod="pod-a"}`,
			`WARN pod-a status=400 latency=54.2ms bytes=43274 duration=41.811s size=512B msg="cache miss" id=6`,
			`{request_id="req-299027dd", trace_id="96a0d044cb50e2d0"}`,
		},
	}
	for i, w := range want {
		r := gen.NewCorpusRecord(i, "corpus")
		line, err := r.Line()
		if err != nil {
			t.Fatalf("record %d Line(): %v", i, err)
		}
		if line != w.line {
			t.Errorf("record %d line changed:\n got: %s\nwant: %s", i, line, w.line)
		}
		if got := r.Labels().String(); got != w.labels {
			t.Errorf("record %d labels = %s, want %s", i, got, w.labels)
		}
		if got := r.Metadata().String(); got != w.meta {
			t.Errorf("record %d metadata = %s, want %s", i, got, w.meta)
		}
	}
}

// TestCorpusLineError makes sure a record that cannot be encoded reports an
// error instead of silently pushing a bad line.
func TestCorpusLineError(t *testing.T) {
	rec := gen.CorpusRecord{ID: 1, Format: "json", Latency: math.NaN()}
	if _, err := rec.Line(); err == nil {
		t.Error("Line() with NaN latency = nil error, want failure")
	}
}
