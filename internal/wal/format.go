package wal

import (
	"context"
	"encoding/binary"
	"encoding/json/v2"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

const (
	payloadVersion = 1
	segmentVersion = 1
	frameVersion   = 2

	segmentFixedHeaderSize = 20

	// frameHeaderSize holds 16 bytes of fields followed by a CRC32C of those
	// fields and a CRC32C of the payload. The header checksum is verified
	// before the length or kind fields are trusted, so a corrupted length can
	// never masquerade as a torn tail.
	frameHeaderSize = 24

	frameRecord = 1
	frameSeal   = 2
)

var (
	segmentMagic = [8]byte{'P', 'K', 'W', 'A', 'L', 'S', 'E', 'G'}
	crcTable     = crc32.MakeTable(crc32.Castagnoli)
)

type payloadEnvelope struct {
	Version int             `json:"version"`
	Tenant  string          `json:"tenant"`
	Streams []payloadStream `json:"streams"`
}

type payloadStream struct {
	Labels  []payloadLabel `json:"labels"`
	Entries []payloadEntry `json:"entries"`
}

type payloadEntry struct {
	Seconds     int64          `json:"seconds"`
	Nanoseconds int32          `json:"nanoseconds"`
	Line        string         `json:"line"`
	Metadata    []payloadLabel `json:"metadata"`
}

type payloadLabel struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Encode serializes a request for the tenant carried by ctx. Timestamps use
// seconds plus nanoseconds so the full protobuf timestamp range is preserved.
// Encoding is CPU-only and intended to happen before the ingestion tenant lock.
func Encode(ctx context.Context, request logs.PushRequest) (Payload, error) {
	if err := ctx.Err(); err != nil {
		return Payload{}, fmt.Errorf("wal: encode: %w", err)
	}
	id := tenant.From(ctx)
	if err := id.Validate(); err != nil {
		return Payload{}, fmt.Errorf("wal: encode: %w", err)
	}
	envelope := payloadEnvelope{Version: payloadVersion, Tenant: string(id)}
	if request.Streams != nil {
		envelope.Streams = make([]payloadStream, len(request.Streams))
	}
	for i, stream := range request.Streams {
		if err := stream.Labels.Validate(); err != nil {
			return Payload{}, fmt.Errorf("wal: encode stream %d: %w", i, err)
		}
		encoded := payloadStream{Labels: encodeLabels(stream.Labels)}
		if stream.Entries != nil {
			encoded.Entries = make([]payloadEntry, len(stream.Entries))
		}
		for j, entry := range stream.Entries {
			if err := entry.Metadata.Validate(); err != nil {
				return Payload{}, fmt.Errorf("wal: encode stream %d entry %d metadata: %w", i, j, err)
			}
			encoded.Entries[j] = payloadEntry{
				Seconds:     entry.Timestamp.Unix(),
				Nanoseconds: int32(entry.Timestamp.Nanosecond()),
				Line:        entry.Line,
				Metadata:    encodeLabels(entry.Metadata),
			}
		}
		envelope.Streams[i] = encoded
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return Payload{}, fmt.Errorf("wal: encode %s request: %w", id, err)
	}
	return Payload{tenant: id, data: data}, nil
}

func encodeLabels(labels logs.Labels) []payloadLabel {
	if labels == nil {
		return nil
	}
	encoded := make([]payloadLabel, len(labels))
	for i, label := range labels {
		encoded[i] = payloadLabel{Name: label.Name, Value: label.Value}
	}
	return encoded
}

func decodePayload(id tenant.ID, data []byte) (logs.PushRequest, error) {
	var envelope payloadEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return logs.PushRequest{}, fmt.Errorf("wal: decode %s payload: %w", id, errorsJoinCorrupt(err))
	}
	if envelope.Version != payloadVersion {
		return logs.PushRequest{}, fmt.Errorf("wal: %s payload version %d: %w", id, envelope.Version, ErrUnsupportedVersion)
	}
	if envelope.Tenant != string(id) {
		return logs.PushRequest{}, fmt.Errorf("wal: payload for %s replayed as %s: %w", envelope.Tenant, id, ErrTenantMismatch)
	}
	request := logs.PushRequest{}
	if envelope.Streams != nil {
		request.Streams = make([]logs.Stream, len(envelope.Streams))
	}
	for i, stream := range envelope.Streams {
		labels, err := decodeLabels(stream.Labels)
		if err != nil {
			return logs.PushRequest{}, fmt.Errorf("wal: decode %s stream %d: %w", id, i, err)
		}
		decoded := logs.Stream{Labels: labels}
		if stream.Entries != nil {
			decoded.Entries = make([]logs.Entry, len(stream.Entries))
		}
		for j, entry := range stream.Entries {
			if entry.Nanoseconds < 0 || entry.Nanoseconds >= int32(time.Second) {
				return logs.PushRequest{}, corruptf("%s stream %d entry %d nanoseconds %d", id, i, j, entry.Nanoseconds)
			}
			metadata, err := decodeLabels(entry.Metadata)
			if err != nil {
				return logs.PushRequest{}, fmt.Errorf("wal: decode %s stream %d entry %d: %w", id, i, j, err)
			}
			decoded.Entries[j] = logs.Entry{
				Timestamp: time.Unix(entry.Seconds, int64(entry.Nanoseconds)).UTC(),
				Line:      entry.Line,
				Metadata:  metadata,
			}
		}
		request.Streams[i] = decoded
	}
	return request, nil
}

func decodeLabels(encoded []payloadLabel) (logs.Labels, error) {
	if encoded == nil {
		return nil, nil
	}
	labels := make(logs.Labels, len(encoded))
	for i, label := range encoded {
		labels[i] = logs.Label{Name: label.Name, Value: label.Value}
	}
	if err := labels.Validate(); err != nil {
		return nil, errorsJoinCorrupt(err)
	}
	return labels, nil
}

func errorsJoinCorrupt(err error) error {
	return fmt.Errorf("%w: %v", ErrCorrupt, err)
}

func segmentHeaderSize(tenantLen int) int64 {
	return int64(segmentFixedHeaderSize + tenantLen + 4)
}

func marshalSegmentHeader(id tenant.ID, first uint64) ([]byte, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	if first == 0 {
		return nil, fmt.Errorf("wal: segment first sequence must be positive")
	}
	header := make([]byte, segmentHeaderSize(len(id)))
	copy(header, segmentMagic[:])
	binary.BigEndian.PutUint16(header[8:10], segmentVersion)
	binary.BigEndian.PutUint16(header[10:12], uint16(len(id)))
	binary.BigEndian.PutUint64(header[12:20], first)
	copy(header[20:], id)
	checksum := crc32.Checksum(header[:len(header)-4], crcTable)
	binary.BigEndian.PutUint32(header[len(header)-4:], checksum)
	return header, nil
}

func parseSegmentHeader(header []byte, id tenant.ID, first uint64) error {
	if len(header) != int(segmentHeaderSize(len(id))) {
		return corruptf("segment header size %d", len(header))
	}
	if string(header[:8]) != string(segmentMagic[:]) {
		return corruptf("bad segment magic")
	}
	if version := binary.BigEndian.Uint16(header[8:10]); version != segmentVersion {
		return fmt.Errorf("wal: segment version %d: %w", version, ErrUnsupportedVersion)
	}
	if int(binary.BigEndian.Uint16(header[10:12])) != len(id) || string(header[20:20+len(id)]) != string(id) {
		return fmt.Errorf("wal: segment tenant does not match %s: %w", id, ErrTenantMismatch)
	}
	if got := binary.BigEndian.Uint64(header[12:20]); got != first {
		return corruptf("segment first sequence %d, filename says %d", got, first)
	}
	want := binary.BigEndian.Uint32(header[len(header)-4:])
	if got := crc32.Checksum(header[:len(header)-4], crcTable); got != want {
		return corruptf("segment header checksum mismatch")
	}
	return nil
}

func segmentName(first uint64) string {
	return fmt.Sprintf("%020d.wal", first)
}

func parseSegmentName(name string) (uint64, error) {
	digits, ok := strings.CutSuffix(name, ".wal")
	if !ok || len(digits) != 20 {
		return 0, corruptf("invalid segment filename %q", name)
	}
	first, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || first == 0 || segmentName(first) != name {
		return 0, corruptf("invalid segment filename %q", name)
	}
	return first, nil
}

func marshalFrame(kind byte, seq uint64, payload []byte) ([]byte, error) {
	if seq == 0 || (kind != frameRecord && kind != frameSeal) {
		return nil, fmt.Errorf("wal: invalid frame kind %d or sequence %d", kind, seq)
	}
	if kind == frameSeal && len(payload) != 0 {
		return nil, fmt.Errorf("wal: seal frame has payload")
	}
	if len(payload) > int(^uint32(0)) {
		return nil, fmt.Errorf("wal: frame payload too large")
	}
	frame := make([]byte, frameHeaderSize+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint16(frame[4:6], frameVersion)
	frame[6] = kind
	binary.BigEndian.PutUint64(frame[8:16], seq)
	binary.BigEndian.PutUint32(frame[16:20], crc32.Checksum(frame[:16], crcTable))
	binary.BigEndian.PutUint32(frame[20:24], crc32.Checksum(payload, crcTable))
	copy(frame[frameHeaderSize:], payload)
	return frame, nil
}

func parseFrameHeader(header []byte, maxRecordBytes int) (kind byte, seq uint64, length int, err error) {
	if len(header) != frameHeaderSize {
		return 0, 0, 0, corruptf("frame header size %d", len(header))
	}
	if crc32.Checksum(header[:16], crcTable) != binary.BigEndian.Uint32(header[16:20]) {
		return 0, 0, 0, corruptf("frame header checksum mismatch")
	}
	if version := binary.BigEndian.Uint16(header[4:6]); version != frameVersion {
		return 0, 0, 0, fmt.Errorf("wal: frame version %d: %w", version, ErrUnsupportedVersion)
	}
	kind = header[6]
	if kind != frameRecord && kind != frameSeal || header[7] != 0 {
		return 0, 0, 0, corruptf("invalid frame kind %d or flags %d", kind, header[7])
	}
	seq = binary.BigEndian.Uint64(header[8:16])
	if seq == 0 {
		return 0, 0, 0, corruptf("zero frame sequence")
	}
	length64 := uint64(binary.BigEndian.Uint32(header[0:4]))
	if kind == frameRecord && (length64 == 0 || length64 > uint64(maxRecordBytes)) {
		return 0, 0, 0, corruptf("record length %d outside 1..%d", length64, maxRecordBytes)
	}
	if kind == frameSeal && length64 != 0 {
		return 0, 0, 0, corruptf("seal payload length %d", length64)
	}
	return kind, seq, int(length64), nil
}

func validFrameChecksum(header, payload []byte) bool {
	return crc32.Checksum(payload, crcTable) == binary.BigEndian.Uint32(header[20:24])
}

func corruptf(format string, args ...any) error {
	return fmt.Errorf("wal: %s: %w", fmt.Sprintf(format, args...), ErrCorrupt)
}
