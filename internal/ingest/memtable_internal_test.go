package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/nathabonfim59/parkana/internal/schema"
)

// cancelAfter is a context whose Err fails after at calls.
type cancelAfter struct {
	context.Context
	n, at int
}

func (c *cancelAfter) Err() error {
	c.n++
	if c.n > c.at {
		return context.Canceled
	}
	return nil
}

// splitHours returns its accumulated parts on a mid-scan cancellation so the
// caller can release them; this test pins that ownership contract plus the
// allocator-leak regression: >4096 rows across an hour boundary, cancel at
// the second checkpoint after a slice exists.
func TestSplitHoursCancelReleasesParts(t *testing.T) {
	ca := memory.NewCheckedAllocator(memory.NewGoAllocator())
	sc, err := schema.ForStream(nil)
	if err != nil {
		t.Fatal(err)
	}
	const h = int64(time.Hour / time.Microsecond)
	rb := array.NewRecordBuilder(ca, sc)
	fields := rb.Fields()
	tsb := fields[0].(*array.TimestampBuilder)
	lb := fields[1].(*array.StringBuilder)
	rows := 9000
	for i := range rows {
		us := int64(i) + 1
		if i >= 5000 {
			us = h + int64(i-5000) + 1 // second hour
		}
		tsb.Append(arrow.Timestamp(us))
		lb.Append("x")
		appendMap(fields[2].(*array.MapBuilder), nil)
		appendMap(fields[3].(*array.MapBuilder), nil)
	}
	rec := rb.NewRecordBatch()
	rb.Release()

	// Checkpoints at rows 4096 and 8192; the hour boundary at row 5000 makes
	// the first slice before the second checkpoint fires.
	ctx := &cancelAfter{Context: t.Context(), at: 1}
	parts, err := splitHours(ctx, rec, "s", nil)
	if err == nil {
		t.Fatal("expected cancellation")
	}
	if len(parts) != 1 {
		t.Fatalf("accumulated parts = %d, want 1", len(parts))
	}
	releaseParts(parts)
	rec.Release()
	ca.AssertSize(t, 0)
}
