package tail

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"strconv"

	"github.com/nathabonfim59/parkana/internal/logs"
)

// errFrameTooLarge reports that one entry or dropped descriptor cannot be
// encoded into a WebSocket message under maxFrameBytes.
var errFrameTooLarge = errors.New("tail: encoded message exceeds the maximum WebSocket frame size")

// tailResponse is one WebSocket text message, matching Loki's
// loghttp.TailResponse / marshal.EncodeTailResult: "streams" is always
// present and dropped_entries only when non-empty. There is no status
// envelope on the tail endpoint.
type tailResponse struct {
	Streams []tailStream  `json:"streams"`
	Dropped []tailDropped `json:"dropped_entries,omitempty"`
}

// tailStream is one label set and its entries. Loki groups tail entries by
// final stream labels; values keep frame order inside a stream.
type tailStream struct {
	Stream map[string]string `json:"stream"`
	Values []tailValue       `json:"values"`
}

// tailDropped is one dropped_entries descriptor.
type tailDropped struct {
	Timestamp string            `json:"timestamp"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// tailValue is one ["<decimal Unix ns>", "<line>"] tuple, with Loki's optional
// third categorized element when the client asked for it.
type tailValue struct {
	timestamp   string
	line        string
	categorized bool
	meta        logs.Labels
	parsed      logs.Labels
}

// MarshalJSON implements json.Marshaler. The categorized form is
// ["<ns>", "<line>", {"structuredMetadata":{...},"parsed":{...}}], exactly
// what Loki's encodeStream writes when categorize-labels is set; without the
// flag metadata and parsed labels have already been flattened into the stream
// labels and the tuple stays two elements.
func (v tailValue) MarshalJSON() ([]byte, error) {
	if !v.categorized {
		return json.Marshal([2]string{v.timestamp, v.line})
	}
	categories := struct {
		StructuredMetadata map[string]string `json:"structuredMetadata,omitempty"`
		Parsed             map[string]string `json:"parsed,omitempty"`
	}{
		StructuredMetadata: labelMap(v.meta),
		Parsed:             labelMap(v.parsed),
	}
	return json.Marshal([3]any{v.timestamp, v.line, categories})
}

// buildResponse groups one flush's entries and descriptors into Loki's wire
// shape. Entries are already timestamp ordered and duplicates are preserved.
func buildResponse(entries []queuedEntry, drops []droppedEntry, categorized bool) tailResponse {
	resp := tailResponse{Streams: make([]tailStream, 0)}
	index := make(map[string]int)
	for _, e := range entries {
		key := e.labels.String()
		i, ok := index[key]
		if !ok {
			i = len(resp.Streams)
			index[key] = i
			resp.Streams = append(resp.Streams, tailStream{Stream: labelMap(e.labels)})
		}
		resp.Streams[i].Values = append(resp.Streams[i].Values, tailValue{
			timestamp:   strconv.FormatInt(e.entry.TS.UnixNano(), 10),
			line:        e.entry.Line,
			categorized: categorized,
			meta:        e.entry.Meta,
			parsed:      e.entry.Parsed,
		})
	}
	if len(drops) > 0 {
		resp.Dropped = make([]tailDropped, 0, len(drops))
		for _, d := range drops {
			var labels map[string]string
			if len(d.labels) > 0 {
				labels = labelMap(d.labels)
			}
			resp.Dropped = append(resp.Dropped, tailDropped{
				Timestamp: strconv.FormatInt(d.ts.UnixNano(), 10),
				Labels:    labels,
			})
		}
	}
	return resp
}

// buildFrames encodes a flush into one or more WebSocket messages whose actual
// JSON size is at most maxFrameBytes. It splits the entry batch (and, when
// needed, the drop descriptors) until each frame fits. A single entry or drop
// descriptor that cannot fit is an explicit error: the caller closes the
// stream rather than silently losing or truncating it.
func buildFrames(entries []queuedEntry, drops []droppedEntry, categorized bool) ([][]byte, error) {
	var frames [][]byte
	for len(entries) > 0 {
		n := len(entries)
		for {
			data, err := json.Marshal(buildResponse(entries[:n], nil, categorized))
			if err != nil {
				return nil, err
			}
			if len(data) <= maxFrameBytes {
				frames = append(frames, data)
				entries = entries[n:]
				break
			}
			if n == 1 {
				return nil, fmt.Errorf("%w: one entry", errFrameTooLarge)
			}
			n /= 2
		}
	}
	for len(drops) > 0 {
		n := len(drops)
		for {
			data, err := json.Marshal(buildResponse(nil, drops[:n], categorized))
			if err != nil {
				return nil, err
			}
			if len(data) <= maxFrameBytes {
				frames = append(frames, data)
				drops = drops[n:]
				break
			}
			if n == 1 {
				return nil, fmt.Errorf("%w: one dropped entry", errFrameTooLarge)
			}
			n /= 2
		}
	}
	return frames, nil
}

// labelMap converts a sorted label set to the JSON object Loki uses for
// stream labels. An empty set becomes an empty object, never null:
// drop/keep stages may legitimately remove every label, and Loki writes
// "stream":{} for those rows.
func labelMap(ls logs.Labels) map[string]string {
	if len(ls) == 0 {
		return map[string]string{}
	}
	return ls.Map()
}
