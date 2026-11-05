package pushcodec

import (
	"context"
	"slices"
	"time"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/nathabonfim59/parkana/internal/logs"
)

// Field numbers and wire types of the Loki push proto (see package doc).
const (
	fPushStreams    = 1 // PushRequest.streams
	fStreamLabels   = 1
	fStreamEntries  = 2
	fEntryTimestamp = 1
	fEntryLine      = 2
	fEntryMetadata  = 3
	fTSSeconds      = 1
	fTSNanos        = 2
	fPairName       = 1
	fPairValue      = 2
)

// Bounds of a valid google.protobuf.Timestamp (years 1 and 9999).
const (
	tsMinSeconds = -62135596800
	tsMaxSeconds = 253402300799
)

// Marshal encodes req as a Loki logproto.PushRequest wire message, without
// snappy compression. Use MarshalSnappy for the wire form clients send.
func Marshal(ctx context.Context, req *logs.PushRequest) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var b []byte
	for i := range req.Streams {
		s := &req.Streams[i]
		var sb []byte
		sb = protowire.AppendTag(sb, fStreamLabels, protowire.BytesType)
		sb = protowire.AppendString(sb, s.Labels.String())
		for j := range s.Entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			e := &s.Entries[j]
			if sec := e.Timestamp.Unix(); sec < tsMinSeconds || sec > tsMaxSeconds {
				return nil, invalidf("entry timestamp %s out of protobuf range", e.Timestamp)
			}
			var eb []byte
			eb = protowire.AppendTag(eb, fEntryTimestamp, protowire.BytesType)
			eb = protowire.AppendBytes(eb, marshalTimestamp(e.Timestamp))
			eb = protowire.AppendTag(eb, fEntryLine, protowire.BytesType)
			eb = protowire.AppendString(eb, e.Line)
			for _, m := range e.Metadata {
				var pb []byte
				pb = protowire.AppendTag(pb, fPairName, protowire.BytesType)
				pb = protowire.AppendString(pb, m.Name)
				pb = protowire.AppendTag(pb, fPairValue, protowire.BytesType)
				pb = protowire.AppendString(pb, m.Value)
				eb = protowire.AppendTag(eb, fEntryMetadata, protowire.BytesType)
				eb = protowire.AppendBytes(eb, pb)
			}
			sb = protowire.AppendTag(sb, fStreamEntries, protowire.BytesType)
			sb = protowire.AppendBytes(sb, eb)
		}
		b = protowire.AppendTag(b, fPushStreams, protowire.BytesType)
		b = protowire.AppendBytes(b, sb)
	}
	return b, nil
}

// MarshalSnappy encodes req as the body of a Loki protobuf push: the wire
// message compressed with snappy's block format.
func MarshalSnappy(ctx context.Context, req *logs.PushRequest) ([]byte, error) {
	b, err := Marshal(ctx, req)
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, b), nil
}

// marshalTimestamp encodes a google.protobuf.Timestamp at full precision.
func marshalTimestamp(t time.Time) []byte {
	var b []byte
	if sec := t.Unix(); sec != 0 {
		b = protowire.AppendTag(b, fTSSeconds, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(sec))
	}
	if ns := t.Nanosecond(); ns != 0 {
		b = protowire.AppendTag(b, fTSNanos, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(int64(ns)))
	}
	return b
}

// DecodeProto decodes a raw (uncompressed) logproto.PushRequest. Stream
// labels must parse with logs.ParseLabels; unknown fields are skipped.
func DecodeProto(ctx context.Context, b []byte) (*logs.PushRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var req logs.PushRequest
	for len(b) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, invalidf("malformed tag: %v", protowire.ParseError(n))
		}
		b = b[n:]
		if num != fPushStreams {
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return nil, invalidf("malformed field %d: %v", num, protowire.ParseError(m))
			}
			b = b[m:]
			continue
		}
		if typ != protowire.BytesType {
			return nil, invalidf("field streams has wire type %s, want bytes", typ)
		}
		v, m := protowire.ConsumeBytes(b)
		if m < 0 {
			return nil, invalidf("malformed streams value: %v", protowire.ParseError(m))
		}
		s, err := decodeStream(ctx, v)
		if err != nil {
			return nil, err
		}
		req.Streams = append(req.Streams, *s)
		b = b[m:]
	}
	return &req, nil
}

func decodeStream(ctx context.Context, b []byte) (*logs.Stream, error) {

	var s logs.Stream
	var labelText string
	for len(b) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, invalidf("malformed stream tag: %v", protowire.ParseError(n))
		}
		b = b[n:]
		switch num {
		case fStreamLabels:
			v, m := protowire.ConsumeString(b)
			if typ != protowire.BytesType || m < 0 {
				return nil, invalidf("malformed stream labels")
			}
			labelText = v
			b = b[m:]
		case fStreamEntries:
			v, m := protowire.ConsumeBytes(b)
			if typ != protowire.BytesType || m < 0 {
				return nil, invalidf("malformed stream entry")
			}
			e, err := decodeEntry(ctx, v)
			if err != nil {
				return nil, err
			}
			s.Entries = append(s.Entries, *e)
			b = b[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return nil, invalidf("malformed stream field %d: %v", num, protowire.ParseError(m))
			}
			b = b[m:]
		}
	}
	ls, err := logs.ParseLabels(labelText)
	if err != nil {
		return nil, invalidf("stream labels: %v", err)
	}
	s.Labels = ls
	return &s, nil
}

func decodeEntry(ctx context.Context, b []byte) (*logs.Entry, error) {
	var e logs.Entry
	var meta []logs.Label
	seenTS := false
	for len(b) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, invalidf("malformed entry tag: %v", protowire.ParseError(n))
		}
		b = b[n:]
		switch num {
		case fEntryTimestamp:
			v, m := protowire.ConsumeBytes(b)
			if typ != protowire.BytesType || m < 0 {
				return nil, invalidf("malformed entry timestamp")
			}
			t, err := decodeTimestamp(v)
			if err != nil {
				return nil, err
			}
			e.Timestamp, seenTS = t, true
			b = b[m:]
		case fEntryLine:
			v, m := protowire.ConsumeString(b)
			if typ != protowire.BytesType || m < 0 {
				return nil, invalidf("malformed entry line")
			}
			e.Line = v
			b = b[m:]
		case fEntryMetadata:
			v, m := protowire.ConsumeBytes(b)
			if typ != protowire.BytesType || m < 0 {
				return nil, invalidf("malformed structured metadata")
			}
			l, err := decodeLabelPair(v)
			if err != nil {
				return nil, err
			}
			meta = append(meta, *l)
			b = b[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return nil, invalidf("malformed entry field %d: %v", num, protowire.ParseError(m))
			}
			b = b[m:]
		}
	}
	if !seenTS {
		return nil, invalidf("entry is missing its timestamp")
	}
	names := make([]string, len(meta))
	for i, l := range meta {
		names[i] = l.Name
	}
	slices.Sort(names)
	for i := 1; i < len(names); i++ {
		if names[i-1] == names[i] {
			return nil, invalidf("duplicate structured metadata %q", names[i])
		}
	}
	e.Metadata = logs.New(meta...)
	if err := e.Metadata.Validate(); err != nil {
		return nil, invalidf("structured metadata: %v", err)
	}
	return &e, nil
}

func decodeTimestamp(b []byte) (time.Time, error) {
	var sec, nsec int64
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return time.Time{}, invalidf("malformed timestamp tag: %v", protowire.ParseError(n))
		}
		b = b[n:]
		switch num {
		case fTSSeconds:
			v, m := protowire.ConsumeVarint(b)
			if typ != protowire.VarintType || m < 0 {
				return time.Time{}, invalidf("malformed timestamp seconds")
			}
			sec = int64(v)
			b = b[m:]
		case fTSNanos:
			v, m := protowire.ConsumeVarint(b)
			if typ != protowire.VarintType || m < 0 {
				return time.Time{}, invalidf("malformed timestamp nanos")
			}
			nsec = int64(v)
			b = b[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return time.Time{}, invalidf("malformed timestamp field %d: %v", num, protowire.ParseError(m))
			}
			b = b[m:]
		}
	}
	if sec < tsMinSeconds || sec > tsMaxSeconds {
		return time.Time{}, invalidf("timestamp seconds %d out of range", sec)
	}
	if nsec < 0 || nsec > 999999999 {
		return time.Time{}, invalidf("timestamp nanos %d out of range", nsec)
	}
	return time.Unix(sec, nsec).UTC(), nil
}

func decodeLabelPair(b []byte) (*logs.Label, error) {
	var l logs.Label
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, invalidf("malformed label pair tag: %v", protowire.ParseError(n))
		}
		b = b[n:]
		switch num {
		case fPairName:
			v, m := protowire.ConsumeString(b)
			if typ != protowire.BytesType || m < 0 {
				return nil, invalidf("malformed label pair name")
			}
			l.Name = v
			b = b[m:]
		case fPairValue:
			v, m := protowire.ConsumeString(b)
			if typ != protowire.BytesType || m < 0 {
				return nil, invalidf("malformed label pair value")
			}
			l.Value = v
			b = b[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return nil, invalidf("malformed label pair field %d: %v", num, protowire.ParseError(m))
			}
			b = b[m:]
		}
	}
	return &l, nil
}
