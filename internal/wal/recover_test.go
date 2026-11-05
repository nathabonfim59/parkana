package wal

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

func TestActiveTailRepairAndCorruption(t *testing.T) {
	validPayload := func(t *testing.T, id tenant.ID) Payload {
		t.Helper()
		payload, err := Encode(tenant.With(t.Context(), id), logs.PushRequest{Streams: []logs.Stream{{
			Labels:  logs.New(logs.Label{Name: "job", Value: "tail"}),
			Entries: []logs.Entry{{Line: "next-payload"}},
		}}})
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	tests := []struct {
		name    string
		mutate  func(t *testing.T, dir string, id tenant.ID)
		wantErr error
	}{
		{
			name: "partial final header",
			mutate: func(t *testing.T, dir string, id tenant.ID) {
				appendFile(t, newestSegment(t, dir, id), []byte{1, 2, 3, 4, 5})
			},
		},
		{
			name: "partial final payload",
			mutate: func(t *testing.T, dir string, id tenant.ID) {
				frame, err := marshalFrame(frameRecord, 3, validPayload(t, id).data)
				if err != nil {
					t.Fatal(err)
				}
				appendFile(t, newestSegment(t, dir, id), frame[:frameHeaderSize+len(frame[frameHeaderSize:])/2])
			},
		},
		{
			name: "complete active checksum mismatch",
			mutate: func(t *testing.T, dir string, id tenant.ID) {
				flipLastByte(t, newestSegment(t, dir, id))
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "bounded corrupt length",
			mutate: func(t *testing.T, dir string, id tenant.ID) {
				frame, err := marshalFrame(frameRecord, 3, validPayload(t, id).data)
				if err != nil {
					t.Fatal(err)
				}
				binary.BigEndian.PutUint32(frame[:4], uint32(defaultMaxRecordBytes)+1)
				appendFile(t, newestSegment(t, dir, id), frame[:frameHeaderSize])
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "unsupported frame version",
			mutate: func(t *testing.T, dir string, id tenant.ID) {
				path := newestSegment(t, dir, id)
				data := readFile(t, path)
				offset := segmentHeaderSize(len(id))
				binary.BigEndian.PutUint16(data[offset+4:offset+6], frameVersion+1)
				// Keep the header checksum valid, as a future-version frame
				// written by a newer build would have.
				crc := crc32.Checksum(data[offset:offset+16], crcTable)
				binary.BigEndian.PutUint32(data[offset+16:offset+20], crc)
				writeFile(t, path, data)
			},
			wantErr: ErrUnsupportedVersion,
		},
		{
			name: "trailing bytes after seal in active segment",
			mutate: func(t *testing.T, dir string, id tenant.ID) {
				path := newestSegment(t, dir, id)
				seal, err := marshalFrame(frameSeal, 3, nil)
				if err != nil {
					t.Fatal(err)
				}
				appendFile(t, path, append(seal, 0xde, 0xad))
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "older complete checksum mismatch",
			mutate: func(t *testing.T, dir string, id tenant.ID) {
				flipLastByte(t, segmentPaths(t, dir, id)[0])
			},
			wantErr: ErrCorrupt,
		},
		{
			name: "older torn seal",
			mutate: func(t *testing.T, dir string, id tenant.ID) {
				path := segmentPaths(t, dir, id)[0]
				stat, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(path, stat.Size()-1); err != nil {
					t.Fatal(err)
				}
			},
			wantErr: ErrCorrupt,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			id := tenant.ID("tail")
			writeRecords(t, dir, id, 2, Config{MaxRecordBytes: 4096, SegmentMaxBytes: 220})
			tt.mutate(t, dir, id)
			w, err := Open(t.Context(), dir, Config{MaxRecordBytes: 4096, SegmentMaxBytes: 220})
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Open error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close(context.Background())
			var positions []uint64
			for record, err := range w.Replay(t.Context(), id, Position{}) {
				if err != nil {
					t.Fatal(err)
				}
				positions = append(positions, record.Position.Seq)
			}
			if !slices.Equal(positions, []uint64{1, 2}) {
				t.Fatalf("repaired replay positions = %v, want [1 2]", positions)
			}
			if pos := appendInternal(t, w, id); pos.Seq != 3 {
				t.Fatalf("append after repair sequence = %d, want 3", pos.Seq)
			}
		})
	}
}

func TestCorruptRecordLengthFailsOpenWithoutTruncation(t *testing.T) {
	dir := t.TempDir()
	id := tenant.ID("middle")
	cfg := Config{MaxRecordBytes: 4096, SegmentMaxBytes: 1 << 20}
	writeRecords(t, dir, id, 2, cfg)
	path := newestSegment(t, dir, id)
	before := readFile(t, path)

	// Corrupt the first record's length: still within MaxRecordBytes but
	// larger than every byte that follows its header. A format that trusted
	// the unverified length would truncate both acknowledged records.
	offset := segmentHeaderSize(len(id))
	binary.BigEndian.PutUint32(before[offset:offset+4], uint32(cfg.MaxRecordBytes))
	writeFile(t, path, before)

	w, err := Open(t.Context(), dir, cfg)
	if err == nil {
		w.Close(context.Background())
		t.Fatal("Open succeeded with a corrupted record length")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open error = %v, want ErrCorrupt", err)
	}
	if after := readFile(t, path); len(after) != len(before) {
		t.Fatalf("segment size changed on failed open: %d, want %d", len(after), len(before))
	}
}

func TestSealedRecoveryReclaimRestartPreservesNextSequence(t *testing.T) {
	dir := t.TempDir()
	id := tenant.ID("next")
	cfg := Config{MaxRecordBytes: 4096, SegmentMaxBytes: 1 << 20}
	writeRecords(t, dir, id, 2, cfg)

	// Simulate a crash after the old segment's seal was synced but before its
	// successor was created. The seal persists next=3 without a replay checkpoint.
	path := newestSegment(t, dir, id)
	seal, err := marshalFrame(frameSeal, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendFile(t, path, seal)
	w, err := Open(t.Context(), dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(segmentPaths(t, dir, id)); got != 2 {
		t.Fatalf("segments after sealed recovery = %d, want 2", got)
	}
	if err := w.Reclaim(t.Context(), id, Position{Tenant: id, Seq: 2}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(segmentPaths(t, dir, id)); got != 1 {
		t.Fatalf("segments after reclaim = %d, want empty active only", got)
	}

	w, err = Open(t.Context(), dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(context.Background())
	for _, err := range w.Replay(t.Context(), id, Position{Tenant: id, Seq: 2}) {
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("reclaimed frontier replay returned a record")
	}
	if pos := appendInternal(t, w, id); pos.Seq != 3 {
		t.Fatalf("append after empty-active reclaim sequence = %d, want 3", pos.Seq)
	}
}

func writeRecords(t *testing.T, dir string, id tenant.ID, count int, cfg Config) {
	t.Helper()
	w, err := Open(t.Context(), dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for range count {
		appendInternal(t, w, id)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func appendInternal(t *testing.T, w *WAL, id tenant.ID) Position {
	t.Helper()
	payload, err := Encode(tenant.With(t.Context(), id), logs.PushRequest{Streams: []logs.Stream{{
		Labels:  logs.New(logs.Label{Name: "job", Value: "recovery"}),
		Entries: []logs.Entry{{Line: "a sufficiently long log line for segment rotation"}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := w.Enqueue(t.Context(), id, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := receipt.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	return receipt.Position
}

func segmentPaths(t *testing.T, dir string, id tenant.ID) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "wal", string(id), "*.wal"))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(paths)
	return paths
}

func newestSegment(t *testing.T, dir string, id tenant.ID) string {
	t.Helper()
	paths := segmentPaths(t, dir, id)
	if len(paths) == 0 {
		t.Fatal("no WAL segments")
	}
	return paths[len(paths)-1]
}

func appendFile(t *testing.T, path string, data []byte) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func flipLastByte(t *testing.T, path string) {
	t.Helper()
	data := readFile(t, path)
	if len(data) == 0 {
		t.Fatal("cannot flip empty file")
	}
	data[len(data)-1] ^= 0xff
	writeFile(t, path, data)
}
