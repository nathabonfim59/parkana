package wal_test

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
	"github.com/nathabonfim59/parkana/internal/wal"
)

func TestWALRoundTripMultiTenantAndSegments(t *testing.T) {
	dir := t.TempDir()
	cfg := wal.Config{MaxRecordBytes: 4096, SegmentMaxBytes: 256}
	w := openWAL(t, dir, cfg)
	requests := map[tenant.ID]logs.PushRequest{
		"beta":  testRequest("beta", 3),
		"alpha": testRequest("alpha", 4),
	}
	next := make(map[tenant.ID]int)
	for _, id := range []tenant.ID{"alpha", "beta", "alpha", "beta", "alpha", "beta", "alpha"} {
		index := next[id]
		next[id]++
		request := oneEntryRequest(requests[id], index)
		pos := appendRequest(t, w, id, request)
		if pos.Tenant != id {
			t.Fatalf("position tenant = %q, want %q", pos.Tenant, id)
		}
	}
	gotTenants, err := w.Tenants(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if want := []tenant.ID{"alpha", "beta"}; !slices.Equal(gotTenants, want) {
		t.Fatalf("Tenants() = %v, want %v", gotTenants, want)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	w = openWAL(t, dir, cfg)
	for _, id := range []tenant.ID{"alpha", "beta"} {
		got := collectReplay(t, w.Replay(t.Context(), id, wal.Position{}))
		want := flattenRequests(id, requests[id])
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("tenant %s replay mismatch\ngot:  %#v\nwant: %#v", id, got, want)
		}
		for i, record := range got {
			if record.Position.Seq != uint64(i+1) {
				t.Errorf("tenant %s record %d sequence = %d", id, i, record.Position.Seq)
			}
		}
	}

	frontier := wal.Position{Tenant: "alpha", Seq: 2}
	got := collectReplay(t, w.Replay(t.Context(), "alpha", frontier))
	if len(got) != 2 || got[0].Position.Seq != 3 || got[1].Position.Seq != 4 {
		t.Fatalf("replay after frontier positions = %v", recordPositions(got))
	}
}

func TestWALPreservesTimestampRangeAndMetadata(t *testing.T) {
	w := openWAL(t, t.TempDir(), wal.Config{})
	id := tenant.ID("time")
	request := logs.PushRequest{Streams: []logs.Stream{{
		Labels: logs.New(logs.Label{Name: "service_name", Value: "clock"}),
		Entries: []logs.Entry{
			{Timestamp: time.Date(1, 1, 1, 0, 0, 0, 1, time.UTC), Line: "minimum", Metadata: logs.New(logs.Label{Name: "trace", Value: "min"})},
			{Timestamp: time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC), Line: "maximum", Metadata: logs.New(logs.Label{Name: "trace", Value: "max"})},
		},
	}}}
	appendRequest(t, w, id, request)
	got := collectReplay(t, w.Replay(t.Context(), id, wal.Position{}))
	if len(got) != 1 || !reflect.DeepEqual(got[0].Request, request) {
		t.Fatalf("timestamp replay mismatch\ngot:  %#v\nwant: %#v", got, request)
	}
}

func TestWALReclaimAndRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := wal.Config{MaxRecordBytes: 4096, SegmentMaxBytes: 220}
	w := openWAL(t, dir, cfg)
	id := tenant.ID("reclaim")
	for i := range 4 {
		appendRequest(t, w, id, oneEntryRequest(testRequest("reclaim", 4), i))
	}
	frontier := wal.Position{Tenant: id, Seq: 2}
	if err := w.Reclaim(t.Context(), id, frontier); err != nil {
		t.Fatal(err)
	}
	if err := w.Reclaim(t.Context(), id, wal.Position{Tenant: id, Seq: 1}); !errors.Is(err, wal.ErrInvalidFrontier) {
		t.Fatalf("regressive reclaim error = %v, want ErrInvalidFrontier", err)
	}
	if err := w.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	w = openWAL(t, dir, cfg)
	got := collectReplay(t, w.Replay(t.Context(), id, frontier))
	if positions := recordPositions(got); !slices.Equal(positions, []uint64{3, 4}) {
		t.Fatalf("replay after reclaim = %v, want [3 4]", positions)
	}
	if err := replayError(w.Replay(t.Context(), id, wal.Position{})); !errors.Is(err, wal.ErrInvalidFrontier) {
		t.Fatalf("replay without frontier error = %v, want ErrInvalidFrontier", err)
	}
	if err := w.Reclaim(t.Context(), id, wal.Position{Tenant: id, Seq: 5}); !errors.Is(err, wal.ErrInvalidFrontier) {
		t.Fatalf("future reclaim error = %v, want ErrInvalidFrontier", err)
	}
	if pos := appendRequest(t, w, id, oneEntryRequest(testRequest("reclaim", 5), 4)); pos.Seq != 5 {
		t.Fatalf("append after reclaim sequence = %d, want 5", pos.Seq)
	}
}

func TestWALRejectsTenantAndFrontierMismatch(t *testing.T) {
	w := openWAL(t, t.TempDir(), wal.Config{})
	payload, err := wal.Encode(tenant.With(t.Context(), tenant.Default), logs.PushRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Enqueue(t.Context(), "other", payload); !errors.Is(err, wal.ErrTenantMismatch) {
		t.Fatalf("cross-tenant enqueue error = %v, want ErrTenantMismatch", err)
	}
	if _, err := w.Enqueue(t.Context(), "../escape", wal.Payload{}); !errors.Is(err, tenant.ErrInvalid) {
		t.Fatalf("invalid tenant error = %v, want tenant.ErrInvalid", err)
	}
	if _, err := wal.Encode(tenant.With(t.Context(), "../escape"), logs.PushRequest{}); !errors.Is(err, tenant.ErrInvalid) {
		t.Fatalf("invalid encode tenant error = %v, want tenant.ErrInvalid", err)
	}
	pos := wal.Position{Tenant: "other", Seq: 1}
	if err := replayError(w.Replay(t.Context(), tenant.Default, pos)); !errors.Is(err, wal.ErrTenantMismatch) {
		t.Fatalf("cross-tenant replay error = %v, want ErrTenantMismatch", err)
	}
}

func TestWALCanceledEnqueueHasNoReceipt(t *testing.T) {
	w := openWAL(t, t.TempDir(), wal.Config{})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ctx = tenant.With(ctx, tenant.Default)
	payload, err := wal.Encode(tenant.With(t.Context(), tenant.Default), testRequest("cancel", 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Enqueue(ctx, tenant.Default, payload); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled enqueue error = %v, want context.Canceled", err)
	}
	if got := collectReplay(t, w.Replay(t.Context(), tenant.Default, wal.Position{})); len(got) != 0 {
		t.Fatalf("canceled enqueue replayed %d records", len(got))
	}
	if pos := appendRequest(t, w, tenant.Default, testRequest("cancel", 1)); pos.Seq != 1 {
		t.Fatalf("sequence after canceled enqueue = %d, want 1", pos.Seq)
	}
}

func openWAL(t *testing.T, dir string, cfg wal.Config) *wal.WAL {
	t.Helper()
	w, err := wal.Open(t.Context(), dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Close(context.Background()); err != nil {
			t.Errorf("close WAL: %v", err)
		}
	})
	return w
}

func appendRequest(t *testing.T, w *wal.WAL, id tenant.ID, request logs.PushRequest) wal.Position {
	t.Helper()
	payload, err := wal.Encode(tenant.With(t.Context(), id), request)
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

func collectReplay(t *testing.T, seq iter.Seq2[wal.Record, error]) []wal.Record {
	t.Helper()
	var records []wal.Record
	for record, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	return records
}

func replayError(seq iter.Seq2[wal.Record, error]) error {
	for _, err := range seq {
		return err
	}
	return nil
}

func testRequest(prefix string, count int) logs.PushRequest {
	request := logs.PushRequest{Streams: []logs.Stream{{
		Labels: logs.New(logs.Label{Name: "service_name", Value: prefix}),
	}}}
	for i := range count {
		request.Streams[0].Entries = append(request.Streams[0].Entries, logs.Entry{
			Timestamp: time.Date(2026, 11, 3, 12, 0, i, i+1, time.UTC),
			Line:      prefix + "-line",
			Metadata:  logs.New(logs.Label{Name: "index", Value: string(rune('a' + i))}),
		})
	}
	return request
}

func oneEntryRequest(request logs.PushRequest, index int) logs.PushRequest {
	return logs.PushRequest{Streams: []logs.Stream{{
		Labels:  request.Streams[0].Labels,
		Entries: []logs.Entry{request.Streams[0].Entries[index]},
	}}}
}

func flattenRequests(id tenant.ID, request logs.PushRequest) []wal.Record {
	records := make([]wal.Record, len(request.Streams[0].Entries))
	for i, entry := range request.Streams[0].Entries {
		records[i] = wal.Record{
			Position: wal.Position{Tenant: id, Seq: uint64(i + 1)},
			Request: logs.PushRequest{Streams: []logs.Stream{{
				Labels:  request.Streams[0].Labels,
				Entries: []logs.Entry{entry},
			}}},
		}
	}
	return records
}

func recordPositions(records []wal.Record) []uint64 {
	positions := make([]uint64, len(records))
	for i, record := range records {
		positions[i] = record.Position.Seq
	}
	return positions
}
