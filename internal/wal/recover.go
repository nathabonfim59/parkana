package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"slices"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

var errReplayStopped = errors.New("wal: replay stopped")

type segmentInfo struct {
	first   uint64
	last    uint64
	next    uint64
	size    int64
	records uint64
	sealed  bool
}

func (w *WAL) recoverTenant(ctx context.Context, id tenant.ID) (_ *tenantLog, retErr error) {
	root, err := w.root.OpenRoot(string(id))
	if err != nil {
		return nil, fmt.Errorf("wal: open tenant %s: %w", id, err)
	}
	defer func() {
		if retErr != nil {
			root.Close()
		}
	}()
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return nil, fmt.Errorf("wal: list tenant %s segments: %w", id, err)
	}
	segments := make([]segmentInfo, 0, len(entries))
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			return nil, fmt.Errorf("wal: tenant %s contains non-segment %q", id, entry.Name())
		}
		first, err := parseSegmentName(entry.Name())
		if err != nil {
			return nil, fmt.Errorf("wal: tenant %s: %w", id, err)
		}
		segments = append(segments, segmentInfo{first: first})
	}
	slices.SortFunc(segments, func(a, b segmentInfo) int {
		switch {
		case a.first < b.first:
			return -1
		case a.first > b.first:
			return 1
		default:
			return 0
		}
	})
	for i := range segments {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("wal: recover %s: %w", id, err)
		}
		repair := i == len(segments)-1
		scanned, err := scanSegment(ctx, root, id, segments[i].first, w.cfg.MaxRecordBytes, repair, nil)
		if err != nil {
			return nil, fmt.Errorf("wal: recover %s segment %s: %w", id, segmentName(segments[i].first), err)
		}
		if !repair && !scanned.sealed {
			return nil, corruptf("tenant %s older segment %s has no seal", id, segmentName(scanned.first))
		}
		if i > 0 && segments[i-1].next != scanned.first {
			return nil, corruptf("tenant %s sequence gap between segments %d and %d", id, segments[i-1].next, scanned.first)
		}
		segments[i] = scanned
	}
	if len(segments) == 0 {
		return newTenantLog(id, root, 1), nil
	}
	active := segments[len(segments)-1]
	if active.sealed {
		file, err := w.ops.create(root, segmentName(active.next))
		if err != nil {
			return nil, fmt.Errorf("wal: recreate successor for %s: %w", id, err)
		}
		header, err := marshalSegmentHeader(id, active.next)
		if err == nil {
			_, err = file.Write(header)
		}
		if err == nil {
			err = w.syncFile(file)
		}
		if err == nil {
			err = syncRoot(root)
		}
		closeErr := file.Close()
		if err = errors.Join(err, closeErr); err != nil {
			return nil, fmt.Errorf("wal: initialize successor for %s: %w", id, err)
		}
		segments = append(segments, segmentInfo{
			first: active.next,
			last:  active.next - 1,
			next:  active.next,
			size:  segmentHeaderSize(len(id)),
		})
		active = segments[len(segments)-1]
	}
	log := newTenantLog(id, root, active.next)
	log.durable = active.next - 1
	log.segments = segments
	return log, nil
}

func scanSegment(
	ctx context.Context,
	root *os.Root,
	id tenant.ID,
	first uint64,
	maxRecordBytes int,
	repair bool,
	visit func(seq uint64, payload []byte) error,
) (_ segmentInfo, retErr error) {
	flags := os.O_RDONLY
	if repair {
		flags = os.O_RDWR
	}
	file, err := root.OpenFile(segmentName(first), flags, 0)
	if err != nil {
		return segmentInfo{}, err
	}
	defer func() { retErr = errors.Join(retErr, file.Close()) }()
	stat, err := file.Stat()
	if err != nil {
		return segmentInfo{}, err
	}
	info := segmentInfo{first: first, last: first - 1, next: first}
	headerSize := segmentHeaderSize(len(id))
	if stat.Size() < headerSize {
		if !repair {
			return segmentInfo{}, corruptf("partial segment header")
		}
		if err := repairSegmentHeader(file, root, id, first); err != nil {
			return segmentInfo{}, err
		}
		info.size = headerSize
		return info, nil
	}
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(io.NewSectionReader(file, 0, headerSize), header); err != nil {
		return segmentInfo{}, err
	}
	if err := parseSegmentHeader(header, id, first); err != nil {
		return segmentInfo{}, err
	}
	offset := headerSize
	for offset < stat.Size() {
		if err := ctx.Err(); err != nil {
			return segmentInfo{}, err
		}
		remaining := stat.Size() - offset
		if remaining < frameHeaderSize {
			if !repair {
				return segmentInfo{}, corruptf("partial frame header at offset %d", offset)
			}
			if err := repairSegmentTail(file, root, offset); err != nil {
				return segmentInfo{}, err
			}
			break
		}
		frameHeader := make([]byte, frameHeaderSize)
		if _, err := io.ReadFull(io.NewSectionReader(file, offset, frameHeaderSize), frameHeader); err != nil {
			return segmentInfo{}, err
		}
		kind, seq, length, err := parseFrameHeader(frameHeader, maxRecordBytes)
		if err != nil {
			return segmentInfo{}, fmt.Errorf("offset %d: %w", offset, err)
		}
		frameSize := int64(frameHeaderSize + length)
		if remaining < frameSize {
			if !repair {
				return segmentInfo{}, corruptf("partial frame payload at offset %d", offset)
			}
			if err := repairSegmentTail(file, root, offset); err != nil {
				return segmentInfo{}, err
			}
			break
		}
		payload := make([]byte, length)
		if length > 0 {
			if _, err := io.ReadFull(io.NewSectionReader(file, offset+frameHeaderSize, int64(length)), payload); err != nil {
				return segmentInfo{}, err
			}
		}
		if !validFrameChecksum(frameHeader, payload) {
			return segmentInfo{}, corruptf("frame checksum mismatch at offset %d", offset)
		}
		if info.sealed {
			return segmentInfo{}, corruptf("frame after seal at offset %d", offset)
		}
		if seq != info.next || seq == ^uint64(0) {
			return segmentInfo{}, corruptf("frame sequence %d, expected %d", seq, info.next)
		}
		switch kind {
		case frameRecord:
			if visit != nil {
				if err := visit(seq, payload); err != nil {
					return segmentInfo{}, err
				}
			}
			info.last = seq
			info.next++
			info.records++
			offset += frameSize
		case frameSeal:
			if info.records == 0 {
				return segmentInfo{}, corruptf("seal of empty segment at offset %d", offset)
			}
			info.sealed = true
			offset += frameSize
			if offset < stat.Size() {
				// A seal is the final write of a segment; nothing legitimate can
				// follow it, even in the newest segment.
				return segmentInfo{}, corruptf("trailing bytes after seal at offset %d", offset)
			}
		}
		if info.sealed {
			break
		}
	}
	info.size = offset
	return info, nil
}

func repairSegmentHeader(file *os.File, root *os.Root, id tenant.ID, first uint64) error {
	header, err := marshalSegmentHeader(id, first)
	if err != nil {
		return err
	}
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("wal: truncate torn segment header: %w", err)
	}
	if _, err := file.WriteAt(header, 0); err != nil {
		return fmt.Errorf("wal: rewrite torn segment header: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("wal: sync repaired segment header: %w", err)
	}
	if err := syncRoot(root); err != nil {
		return fmt.Errorf("wal: sync repaired segment directory: %w", err)
	}
	return nil
}

func repairSegmentTail(file *os.File, root *os.Root, offset int64) error {
	if err := file.Truncate(offset); err != nil {
		return fmt.Errorf("wal: truncate torn tail at %d: %w", offset, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("wal: sync repaired tail: %w", err)
	}
	if err := syncRoot(root); err != nil {
		return fmt.Errorf("wal: sync repaired segment directory: %w", err)
	}
	return nil
}

// Replay yields records strictly after the caller's durable A7 generation
// manifest frontier. The WAL never advances or persists that frontier itself.
// Replay and Enqueue for the same tenant are mutually exclusive.
func (w *WAL) Replay(ctx context.Context, id tenant.ID, after Position) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		if err := w.beginOperation(); err != nil {
			yield(Record{}, err)
			return
		}
		defer w.operations.Done()
		if err := ctx.Err(); err != nil {
			yield(Record{}, fmt.Errorf("wal: replay %s: %w", id, err))
			return
		}
		if err := id.Validate(); err != nil {
			yield(Record{}, fmt.Errorf("wal: replay: %w", err))
			return
		}
		if err := validatePosition(id, after); err != nil {
			yield(Record{}, err)
			return
		}
		log, err := w.existingLog(id)
		if err != nil {
			yield(Record{}, err)
			return
		}
		if err := log.beginReplay(); err != nil {
			yield(Record{}, err)
			return
		}
		defer log.endReplay()
		log.mu.Lock()
		segments := slices.Clone(log.segments)
		durable := log.durable
		root := log.root
		log.mu.Unlock()
		if err := validateCoverage(id, segments, durable, after); err != nil {
			yield(Record{}, err)
			return
		}
		for _, segment := range segments {
			visit := func(seq uint64, payload []byte) error {
				if seq <= after.Seq {
					return nil
				}
				request, err := decodePayload(id, payload)
				if err != nil {
					return err
				}
				record := Record{Position: Position{Tenant: id, Seq: seq}, Request: request}
				if !yield(record, nil) {
					return errReplayStopped
				}
				return nil
			}
			_, err := scanSegment(ctx, root, id, segment.first, w.cfg.MaxRecordBytes, false, visit)
			if errors.Is(err, errReplayStopped) {
				return
			}
			if err != nil {
				yield(Record{}, fmt.Errorf("wal: replay %s segment %s: %w", id, segmentName(segment.first), err))
				return
			}
		}
	}
}

func validateCoverage(id tenant.ID, segments []segmentInfo, durable uint64, after Position) error {
	first := uint64(1)
	if len(segments) > 0 {
		first = segments[0].first
	}
	if after.Seq > durable {
		return fmt.Errorf("wal: %s frontier %d exceeds durable sequence %d: %w", id, after.Seq, durable, ErrInvalidFrontier)
	}
	if after.Seq < first-1 {
		return fmt.Errorf("wal: %s frontier %d precedes retained first sequence %d: %w", id, after.Seq, first, ErrInvalidFrontier)
	}
	return nil
}

// Reclaim removes sealed segments completely covered by an already-committed
// A7 generation manifest. It never removes the active segment and never persists
// an independent replay checkpoint. Each removal is synced to the tenant
// directory before the next one is attempted, so a crash can only ever leave a
// reclaimed prefix, never a gap in checkpointed segments.
func (w *WAL) Reclaim(ctx context.Context, id tenant.ID, manifestFrontier Position) error {
	if err := w.beginOperation(); err != nil {
		return err
	}
	defer w.operations.Done()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("wal: reclaim %s: %w", id, err)
	}
	if err := id.Validate(); err != nil {
		return fmt.Errorf("wal: reclaim: %w", err)
	}
	if err := validatePosition(id, manifestFrontier); err != nil {
		return err
	}
	log, err := w.existingLog(id)
	if err != nil {
		return err
	}
	if err := log.beginReclaim(); err != nil {
		return err
	}
	defer log.endReclaim()
	log.mu.Lock()
	segments := slices.Clone(log.segments)
	durable := log.durable
	root := log.root
	pending := log.dirSyncPending
	log.mu.Unlock()
	if pending {
		// A previous reclaim removed a segment whose directory sync failed.
		// Persist it before removing anything else — and even when this call
		// removes nothing — so a crash can never persist a non-prefix subset.
		if root == nil {
			return fmt.Errorf("wal: reclaim %s: %w", id, ErrCorrupt)
		}
		if err := w.ops.syncDir(root); err != nil {
			return fmt.Errorf("wal: sync %s pending reclaim: %w", id, err)
		}
		log.mu.Lock()
		log.dirSyncPending = false
		log.mu.Unlock()
	}
	if manifestFrontier.Seq == 0 {
		return nil
	}
	if err := validateCoverage(id, segments, durable, manifestFrontier); err != nil {
		return err
	}
	remove := 0
	for remove < len(segments)-1 && segments[remove].sealed && segments[remove].next-1 <= manifestFrontier.Seq {
		remove++
	}
	if remove == 0 {
		return nil
	}
	if root == nil {
		return fmt.Errorf("wal: reclaim %s: %w", id, ErrCorrupt)
	}
	for i := range remove {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("wal: reclaim %s: %w", id, err)
		}
		name := segmentName(segments[i].first)
		if err := w.ops.remove(root, name); err != nil {
			return fmt.Errorf("wal: reclaim %s segment %s: %w", id, name, err)
		}
		// The file is gone from the directory now; keep memory in sync and mark
		// the removal as needing a directory sync before reporting any failure.
		log.mu.Lock()
		log.dirSyncPending = true
		if len(log.segments) > 0 && log.segments[0].first == segments[i].first {
			log.segments = log.segments[1:]
		}
		log.mu.Unlock()
		if err := w.ops.syncDir(root); err != nil {
			return fmt.Errorf("wal: sync %s reclaim of %s: %w", id, name, err)
		}
		log.mu.Lock()
		log.dirSyncPending = false
		log.mu.Unlock()
	}
	return nil
}

func (w *WAL) beginOperation() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closing {
		return fmt.Errorf("wal: operation: %w", ErrClosed)
	}
	w.operations.Add(1)
	return nil
}
