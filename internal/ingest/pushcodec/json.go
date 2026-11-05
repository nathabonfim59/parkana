package pushcodec

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"math"
	"strconv"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
)

// jsonPush is the Loki JSON push body. Unknown members are ignored so newer
// clients keep working; jsontext already rejects duplicate member names.
type jsonPush struct {
	Streams []jsonStream `json:"streams"`
}

type jsonStream struct {
	// Pointer values make a null member distinguishable from "": null and
	// non-string label values are rejected rather than zeroed.
	Stream map[string]*string `json:"stream"`
	Values [][]jsontext.Value `json:"values"`
}

// jsonOutPush/jsonOutStream are the marshal-side shapes; plain string values
// suffice here.
type jsonOutPush struct {
	Streams []jsonOutStream `json:"streams"`
}

type jsonOutStream struct {
	Stream map[string]string  `json:"stream"`
	Values [][]jsontext.Value `json:"values"`
}

// Bounds of a timestamp encodable as int64 nanoseconds (1678..2262).
var (
	jsonTSMin = time.Unix(0, math.MinInt64).UTC()
	jsonTSMax = time.Unix(0, math.MaxInt64).UTC()
)

// DecodeJSON decodes the Loki JSON push format. Each value tuple is
// ["<ns>", "<line>"] or ["<ns>", "<line>", {metadata}]; the timestamp is
// nanoseconds since epoch as a string (RFC 3339 is also accepted, as in
// Loki).
func DecodeJSON(ctx context.Context, b []byte) (*logs.PushRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var in jsonPush
	if err := json.Unmarshal(b, &in); err != nil {
		return nil, invalidf("JSON: %v", err)
	}
	req := &logs.PushRequest{Streams: make([]logs.Stream, 0, len(in.Streams))}
	for i, js := range in.Streams {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ls, err := labelMap(js.Stream)
		if err != nil {
			return nil, invalidf("stream %d labels: %v", i, err)
		}
		if err := ls.Validate(); err != nil {
			return nil, invalidf("stream %d labels: %v", i, err)
		}
		s := logs.Stream{Labels: ls, Entries: make([]logs.Entry, 0, len(js.Values))}
		for j, tuple := range js.Values {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			e, err := decodeTuple(tuple)
			if err != nil {
				return nil, invalidf("stream %d value %d: %v", i, j, err)
			}
			s.Entries = append(s.Entries, *e)
		}
		req.Streams = append(req.Streams, s)
	}
	return req, nil
}

func decodeTuple(tuple []jsontext.Value) (*logs.Entry, error) {
	if len(tuple) != 2 && len(tuple) != 3 {
		return nil, invalidf("value tuple has %d elements, want 2 or 3", len(tuple))
	}
	// Pointer targets reject null and non-string members instead of silently
	// yielding a zero value.
	var ts, line *string
	if err := json.Unmarshal(tuple[0], &ts); err != nil || ts == nil {
		return nil, invalidf("timestamp must be a string")
	}
	if err := json.Unmarshal(tuple[1], &line); err != nil || line == nil {
		return nil, invalidf("line must be a string")
	}
	t, err := parseJSONTS(*ts)
	if err != nil {
		return nil, err
	}
	e := &logs.Entry{Timestamp: t, Line: *line}
	if len(tuple) == 3 {
		var meta *map[string]*string
		if err := json.Unmarshal(tuple[2], &meta); err != nil || meta == nil {
			return nil, invalidf("metadata must be an object")
		}
		ls, err := labelMap(*meta)
		if err != nil {
			return nil, invalidf("metadata: %v", err)
		}
		e.Metadata = ls
		if err := e.Metadata.Validate(); err != nil {
			return nil, invalidf("metadata: %v", err)
		}
	}
	return e, nil
}

// labelMap converts a decoded name→*string object into a sorted label set,
// rejecting null values.
func labelMap(m map[string]*string) (logs.Labels, error) {
	flat := make(map[string]string, len(m))
	for name, v := range m {
		if v == nil {
			return nil, invalidf("label %q has a null value", name)
		}
		flat[name] = *v
	}
	return logs.FromMap(flat), nil
}

// parseJSONTS parses a Loki JSON timestamp: nanoseconds since the Unix epoch
// as a decimal string, or RFC 3339 (with nanoseconds) as Loki also accepts.
func parseJSONTS(s string) (time.Time, error) {
	if ns, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(0, ns).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC(), nil
	}
	return time.Time{}, invalidf("timestamp %q is neither nanoseconds nor RFC 3339", s)
}

// MarshalJSON encodes req in the Loki JSON push format: nanosecond string
// timestamps, with a metadata object only on entries that have one.
func MarshalJSON(ctx context.Context, req *logs.PushRequest) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := jsonOutPush{Streams: make([]jsonOutStream, 0, len(req.Streams))}
	for i := range req.Streams {
		s := &req.Streams[i]
		js := jsonOutStream{Stream: s.Labels.Map(), Values: make([][]jsontext.Value, 0, len(s.Entries))}
		for j := range s.Entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			e := &s.Entries[j]
			if e.Timestamp.Before(jsonTSMin) || e.Timestamp.After(jsonTSMax) {
				return nil, invalidf("entry timestamp %s cannot be encoded as int64 nanoseconds", e.Timestamp)
			}
			tuple := make([]jsontext.Value, 2, 3)
			tuple[0], _ = json.Marshal(strconv.FormatInt(e.Timestamp.UnixNano(), 10))
			tuple[1], _ = json.Marshal(e.Line)
			if len(e.Metadata) > 0 {
				m, _ := json.Marshal(e.Metadata.Map())
				tuple = append(tuple, m)
			}
			js.Values = append(js.Values, tuple)
		}
		out.Streams = append(out.Streams, js)
	}
	return json.Marshal(out)
}
