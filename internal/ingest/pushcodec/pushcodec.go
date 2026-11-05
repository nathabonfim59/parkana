// Package pushcodec encodes and decodes Loki push request bodies without
// importing Loki itself. The protobuf layout mirrors
// github.com/grafana/loki/pkg/push/push.proto (logproto.PushRequest):
//
//	PushRequest:    streams = 1 (repeated StreamAdapter), format = 2
//	StreamAdapter:  labels = 1, entries = 2 (repeated EntryAdapter), hash = 3
//	EntryAdapter:   timestamp = 1 (google.protobuf.Timestamp:
//	                seconds = 1, nanos = 2), line = 2,
//	                structuredMetadata = 3 (repeated LabelPairAdapter),
//	                parsed = 4 (query path only, ignored here)
//	LabelPairAdapter: name = 1, value = 2
//
// Loki clients snappy-compress the protobuf message with the block format
// (snappy.Encode / snappy.Decode, not the framed format); the JSON format is
// {"streams":[{"stream":{...},"values":[["<ns>","line",{meta}], ...]}]}.
//
// Decoders keep full nanosecond timestamp precision; microsecond truncation
// for Parquet happens later at the schema boundary. Valid unknown protobuf
// fields are skipped so newer clients keep working; malformed wire data,
// malformed JSON tuples and invalid label sets return errors.
package pushcodec

import "fmt"

// invalidf returns a decode error. Callers report it to clients as a
// Loki-style 400.
func invalidf(format string, args ...any) error {
	return fmt.Errorf("pushcodec: "+format, args...)
}
