package ingest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exec"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/schema"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// ErrSequence is returned by Append when a tenant's sequences are not
// strictly contiguous (each must equal the previous + 1; the first may be
// any value after a checkpointed replay).
var ErrSequence = errors.New("ingest: non-contiguous tenant sequence")

// ErrClosed is returned when the memtable was closed and cannot be used.
var ErrClosed = errors.New("ingest: memtable closed")

// ErrPreparedMismatch is returned by AppendPrepared for a push prepared for
// a different memtable or tenant.
var ErrPreparedMismatch = errors.New("ingest: prepared push for another memtable or tenant")

// MemtableOptions configures NewMemtable.
type MemtableOptions struct {
	// Allocator accounts for all Arrow memory; nil uses memory.DefaultAllocator.
	Allocator memory.Allocator
	// StreamLabels selects the dataset name via Labels.StreamName.
	StreamLabels []string
	// PromotedFor returns the promoted label columns of a stream
	// (config.Ingest.PromotedFor). Required. Its mapping must be stable for
	// the lifetime of the table: a stream's schema is resolved once, at its
	// first row.
	PromotedFor func(stream string) []string
	// MemtableMaxBytes seals the active generation past this admission-size
	// estimate; zero means unlimited.
	MemtableMaxBytes int64
	// WALMaxBytes seals when accumulated WAL payload bytes reach this;
	// zero means unlimited.
	WALMaxBytes int64
}

// NewMemtable returns an empty memtable holding one generation of Arrow
// builders per tenant.
func NewMemtable(o MemtableOptions) (*Memtable, error) {
	if o.PromotedFor == nil {
		return nil, errors.New("ingest: MemtableOptions.PromotedFor is required")
	}
	if o.MemtableMaxBytes < 0 || o.WALMaxBytes < 0 {
		return nil, fmt.Errorf("ingest: negative memtable limit (%d, %d)", o.MemtableMaxBytes, o.WALMaxBytes)
	}
	if o.Allocator == nil {
		o.Allocator = memory.DefaultAllocator
	}
	return &Memtable{
		alloc:        o.Allocator,
		streamLabels: slices.Clone(o.StreamLabels),
		promotedFor:  o.PromotedFor,
		memMax:       o.MemtableMaxBytes,
		walMax:       o.WALMaxBytes,
		tenants:      make(map[tenant.ID]*tenantBuf),
	}, nil
}

// Memtable buffers decoded pushes per tenant in Arrow builders. Each tenant
// accumulates rows for all its streams in one active generation covering a
// contiguous sequence range; sealing detaches the builders into an immutable
// Generation that materializes sorted, hour-partitioned batches lazily.
type Memtable struct {
	alloc        memory.Allocator
	streamLabels []string
	promotedFor  func(string) []string
	memMax       int64
	walMax       int64

	mu      sync.Mutex // guards tenants and closed
	tenants map[tenant.ID]*tenantBuf
	closed  bool
}

// streamBuf holds the Arrow builder state of one stream in the active
// generation.
type streamBuf struct {
	schema   *arrow.Schema
	promoted []string
	rb       *array.RecordBuilder
}

// tenantBuf is one tenant's append state.
type tenantBuf struct {
	mu          sync.Mutex
	hasNext     bool   // a sequence has been accepted
	next        uint64 // expected next sequence
	genStarted  bool   // the active generation consumed at least one sequence
	first, last uint64 // active generation sequence range
	wal, mem    int64  // accumulated WAL payload bytes / admission estimate
	streams     map[string]*streamBuf
	sealed      []*Generation // FIFO awaiting flush
	closed      bool
}

// admissionOverhead is the estimated per-entry cost beyond the copied bytes:
// column slots, map rows and validity bits.
const admissionOverhead = 96

// preparedStream is a request's entries for one stream, resolved outside the
// tenant lock.
type preparedStream struct {
	name     string
	schema   *arrow.Schema // resolved for new streams
	promoted []string
	rows     []preparedRow
	bytes    int64
}

type preparedRow struct {
	us   int64
	line string
	// Stream labels are per Loki stream: several request streams may share
	// one dataset with different label sets, so each row carries its own
	// promoted values (aligned to preparedStream.promoted) and rest labels.
	promoted []promotedVal
	rest     logs.Labels
	meta     logs.Labels
}

type promotedVal struct {
	v  string
	ok bool
}

// PreparedPush is a push request validated and resolved for one memtable and
// tenant. Prepare does all of the fallible work — stream grouping, label
// splits and schema resolution — so AppendPrepared only admits rows inside
// the service's serialize-WAL-plus-memtable critical section.
type PreparedPush struct {
	m       *Memtable
	id      tenant.ID
	streams []preparedStream
}

// Prepare validates req and resolves its per-stream schemas without
// mutating the memtable; input-owned values are copied. The result is bound
// to this memtable and tenant and can only be admitted by AppendPrepared.
func (m *Memtable) Prepare(ctx context.Context, id tenant.ID, req *logs.PushRequest) (PreparedPush, error) {
	if err := ctx.Err(); err != nil {
		return PreparedPush{}, err
	}
	if err := id.Validate(); err != nil {
		return PreparedPush{}, err
	}
	if req == nil {
		return PreparedPush{}, errors.New("ingest: nil push request")
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return PreparedPush{}, ErrClosed
	}
	m.mu.Unlock()
	streams, err := m.prepare(ctx, req)
	if err != nil {
		return PreparedPush{}, err
	}
	return PreparedPush{m: m, id: id, streams: streams}, nil
}

// AppendPrepared buffers a push returned by Prepare under the tenant-local
// sequence seq, with walBytes counting the WAL payload. The whole request
// was validated in Prepare, so an error leaves the memtable untouched.
// Sequences must be contiguous per tenant: the first may be any value
// (replay resumes above 1), each next must equal previous+1.
func (m *Memtable) AppendPrepared(ctx context.Context, id tenant.ID, seq uint64, walBytes int64, p PreparedPush) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.m != m || p.id != id {
		return ErrPreparedMismatch
	}
	if walBytes < 0 {
		return fmt.Errorf("ingest: negative walBytes %d", walBytes)
	}
	if seq == 0 {
		return errors.New("ingest: sequence must start at 1")
	}
	if seq == math.MaxUint64 {
		return errors.New("ingest: sequence would wrap")
	}
	t, err := m.tenant(id)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	if t.hasNext && seq != t.next {
		return fmt.Errorf("%w: %s got %d, want %d", ErrSequence, id, seq, t.next)
	}
	// Keep this critical section short: only builder appends, sequence
	// bookkeeping and a threshold check. Phase 3 benchmarks this path
	// against the 31 MB/s ingest target.
	for i := range p.streams {
		ps := &p.streams[i]
		buf, ok := t.streams[ps.name]
		if !ok {
			buf = &streamBuf{
				schema:   ps.schema,
				promoted: ps.promoted,
				rb:       array.NewRecordBuilder(m.alloc, ps.schema),
			}
			t.streams[ps.name] = buf
		}
		for j := range ps.rows {
			buf.append(&ps.rows[j])
		}
	}
	t.hasNext, t.next = true, seq+1
	if !t.genStarted {
		t.genStarted, t.first = true, seq
	}
	t.last = seq
	t.wal += walBytes
	for i := range p.streams {
		t.mem += p.streams[i].bytes
	}
	if (m.memMax > 0 && t.mem >= m.memMax) || (m.walMax > 0 && t.wal >= m.walMax) {
		t.sealLocked(id, m.alloc)
	}
	return nil
}

// Append is Prepare plus AppendPrepared, for callers that do not keep an
// outer WAL-admission critical section.
func (m *Memtable) Append(ctx context.Context, id tenant.ID, seq uint64, walBytes int64, req *logs.PushRequest) error {
	p, err := m.Prepare(ctx, id, req)
	if err != nil {
		return err
	}
	return m.AppendPrepared(ctx, id, seq, walBytes, p)
}

// prepare groups a request by stream and resolves every new stream's schema
// and the promoted/rest label split, so Prepare mutates nothing before the
// whole request validates.
func (m *Memtable) prepare(ctx context.Context, req *logs.PushRequest) ([]preparedStream, error) {
	var order []string
	byName := make(map[string]*preparedStream)
	for si := range req.Streams {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s := &req.Streams[si]
		if err := s.Labels.Validate(); err != nil {
			return nil, fmt.Errorf("ingest: stream labels: %w", err)
		}
		name := s.Labels.StreamName(m.streamLabels)
		ps, ok := byName[name]
		if !ok {
			promoted := slices.Clone(m.promotedFor(name))
			sc, err := schema.ForStream(promoted)
			if err != nil {
				return nil, fmt.Errorf("ingest: stream %s: %w", name, err)
			}
			ps = &preparedStream{name: name, schema: sc, promoted: promoted}
			byName[name] = ps
			order = append(order, name)
		}
		pro, rest := s.Labels.Split(ps.promoted)
		vals := make([]promotedVal, len(ps.promoted))
		for i, col := range ps.promoted {
			v, ok := pro.Get(col)
			vals[i] = promotedVal{v: v, ok: ok}
		}
		for ei := range s.Entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			e := &s.Entries[ei]
			if err := e.Metadata.Validate(); err != nil {
				return nil, fmt.Errorf("ingest: entry metadata: %w", err)
			}
			ps.rows = append(ps.rows, preparedRow{
				us:       schema.TS(e.Timestamp),
				line:     e.Line,
				promoted: vals,
				rest:     rest,
				// Clone request-owned slices so later caller mutation cannot
				// corrupt prepared data.
				meta: slices.Clone(e.Metadata),
			})
			ps.bytes += int64(len(e.Line)) + admissionOverhead
			for _, l := range s.Labels {
				ps.bytes += int64(len(l.Name) + len(l.Value))
			}
			for _, l := range e.Metadata {
				ps.bytes += int64(len(l.Name) + len(l.Value))
			}
		}
	}
	out := make([]preparedStream, 0, len(order))
	for _, name := range order {
		out = append(out, *byName[name])
	}
	return out, nil
}

// append writes one prepared row to the stream's builders.
func (b *streamBuf) append(r *preparedRow) {
	fields := b.rb.Fields()
	fields[0].(*array.TimestampBuilder).Append(arrow.Timestamp(r.us))
	fields[1].(*array.StringBuilder).Append(r.line)
	appendMap(fields[2].(*array.MapBuilder), r.rest)
	appendMap(fields[3].(*array.MapBuilder), r.meta)
	for i, pv := range r.promoted {
		sb := fields[4+i].(*array.StringBuilder)
		if pv.ok {
			sb.Append(pv.v)
		} else {
			sb.AppendNull()
		}
	}
}

func appendMap(b *array.MapBuilder, ls logs.Labels) {
	b.Append(true)
	kb := b.KeyBuilder().(*array.StringBuilder)
	ib := b.ItemBuilder().(*array.StringBuilder)
	for _, l := range ls {
		kb.Append(l.Name)
		ib.Append(l.Value)
	}
}

func (m *Memtable) tenant(id tenant.ID) (*tenantBuf, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	t := m.tenants[id]
	if t == nil {
		t = &tenantBuf{streams: make(map[string]*streamBuf)}
		m.tenants[id] = t
	}
	return t, nil
}

// sealLocked detaches the active generation's builders and queues it as a
// sealed Generation. Only cheap state moves happen under the tenant lock;
// sorting and hour partitioning are deferred to Generation.Parts.
func (t *tenantBuf) sealLocked(id tenant.ID, alloc memory.Allocator) {
	if !t.genStarted {
		return
	}
	bases := make(map[string]arrow.RecordBatch, len(t.streams))
	for name, buf := range t.streams {
		// Finalize the builder into an immutable record so that a failed or
		// canceled Parts can retry; sorting stays out of this critical
		// section.
		bases[name] = buf.rb.NewRecordBatch()
		buf.rb.Release()
	}
	t.sealed = append(t.sealed, &Generation{
		tenant: id, first: t.first, last: t.last,
		alloc: alloc, bases: bases,
	})
	t.streams = make(map[string]*streamBuf)
	t.genStarted = false
	t.wal, t.mem = 0, 0
}

// Seal closes the tenant's active generation if it consumed any sequences —
// including empty pushes — and queues it for flush. Periodic flush and
// shutdown call this explicitly.
func (m *Memtable) Seal(ctx context.Context, id tenant.ID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := id.Validate(); err != nil {
		return err
	}
	t, err := m.tenant(id)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	t.sealLocked(id, m.alloc)
	return nil
}

// SealAll seals every tenant's active generation, for shutdown.
func (m *Memtable) SealAll(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	for id, t := range m.tenants {
		t.mu.Lock()
		t.sealLocked(id, m.alloc)
		t.mu.Unlock()
	}
	return nil
}

// TakeSealed removes and returns the tenant's queued sealed generations,
// oldest first. Ownership transfers to the caller, which must Release each
// after durable local publication.
func (m *Memtable) TakeSealed(ctx context.Context, id tenant.ID) ([]*Generation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := id.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	t := m.tenants[id]
	m.mu.Unlock()
	if t == nil {
		return nil, nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	gens := t.sealed
	t.sealed = nil
	return gens, nil
}

// Close frees every tenant's active builders and untaken sealed generations,
// for failed startup or shutdown. The memtable cannot be used afterwards.
func (m *Memtable) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	for _, t := range m.tenants {
		t.mu.Lock()
		t.closed = true
		for _, buf := range t.streams {
			buf.rb.Release()
		}
		t.streams = nil
		for _, g := range t.sealed {
			g.Release()
		}
		t.sealed = nil
		t.mu.Unlock()
	}
}

// Generation is a sealed, immutable tenant sequence range. Its Arrow data is
// materialized lazily by Parts: batches sorted by _ts and split at UTC hour
// boundaries.
type Generation struct {
	tenant      tenant.ID
	first, last uint64
	alloc       memory.Allocator

	mu       sync.Mutex
	bases    map[string]arrow.RecordBatch // unsorted records until Parts runs
	parts    []Part                       // materialized result, cached
	released bool
}

// Tenant returns the generation's tenant.
func (g *Generation) Tenant() tenant.ID { return g.tenant }

// Frontier returns the inclusive sequence range this generation covers.
func (g *Generation) Frontier() (first, last uint64) { return g.first, g.last }

// Part is one stream+UTC-hour partition's batch, sorted by _ts.
type Part struct {
	Stream string
	Hour   time.Time // UTC hour start the rows fall in
	// Record is borrowed from the Generation: valid until Release, and must
	// not be released by the caller.
	Record arrow.RecordBatch
}

// Parts materializes the generation's batches sorted by _ts and partitioned
// per stream and UTC hour. It runs entirely outside the tenant lock, so
// appends keep progressing while it sorts. The result is cached; records are
// borrowed until Release. On error or cancellation the detached builders are
// retained, so the caller may retry or Release; no partial result is kept.
func (g *Generation) Parts(ctx context.Context) ([]Part, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.released {
		return nil, ErrClosed
	}
	if g.parts != nil {
		// Clone the descriptor slice so callers cannot mutate release
		// bookkeeping; the records stay borrowed.
		return slices.Clone(g.parts), nil
	}
	ctx = exec.WithAllocator(ctx, g.alloc)
	var parts []Part
	for _, name := range slices.Sorted(maps.Keys(g.bases)) {
		sorted, err := sortedBatch(ctx, g.bases[name])
		if err != nil {
			releaseParts(parts)
			return nil, fmt.Errorf("ingest: generation %d-%d stream %s: %w", g.first, g.last, name, err)
		}
		parts, err = splitHours(ctx, sorted, name, parts)
		if sorted != nil {
			sorted.Release()
		}
		if err != nil {
			releaseParts(parts)
			return nil, fmt.Errorf("ingest: generation %d-%d stream %s: %w", g.first, g.last, name, err)
		}
	}
	g.parts = parts
	// The materialized parts own the data now; free the unsorted records.
	for _, rec := range g.bases {
		rec.Release()
	}
	g.bases = nil
	return slices.Clone(g.parts), nil
}

// sortedBatch returns a new record batch sorted by _ts, stable for equal
// timestamps, or nil for an empty record.
func sortedBatch(ctx context.Context, rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	if rec.NumRows() == 0 {
		return nil, nil
	}
	idx, err := compute.SortIndicesRecordBatch(ctx, rec,
		[]compute.SortKey{{ColumnIndex: 0, Order: compute.SortOrderAscending}})
	if err != nil {
		return nil, err
	}
	defer idx.Release()
	cols := make([]arrow.Array, rec.NumCols())
	for i := range cols {
		c, err := compute.TakeArray(ctx, rec.Column(i), idx)
		if err != nil {
			for _, c := range cols[:i] {
				c.Release()
			}
			return nil, err
		}
		cols[i] = c
	}
	sorted := array.NewRecordBatch(rec.Schema(), cols, rec.NumRows())
	for _, c := range cols {
		c.Release()
	}
	return sorted, nil
}

const hourUs = int64(time.Hour / time.Microsecond)

// splitHours appends one Part per UTC hour boundary of the sorted batch.
// The slices borrow the batch's buffers; the caller releases sorted itself.
func splitHours(ctx context.Context, sorted arrow.RecordBatch, stream string, parts []Part) ([]Part, error) {
	if sorted == nil || sorted.NumRows() == 0 {
		return parts, nil
	}
	ts := sorted.Column(0).(*array.Timestamp)
	n := sorted.NumRows()
	start := int64(0)
	cur := hourOf(ts.Value(0))
	for i := int64(1); i < n; i++ {
		if i%4096 == 0 {
			if err := ctx.Err(); err != nil {
				// Return the accumulated parts so the caller can release the
				// slices already made.
				return parts, err
			}
		}
		if h := hourOf(ts.Value(int(i))); h != cur {
			parts = append(parts, Part{Stream: stream, Hour: hourTime(cur), Record: sorted.NewSlice(start, i)})
			start, cur = i, h
		}
	}
	return append(parts, Part{Stream: stream, Hour: hourTime(cur), Record: sorted.NewSlice(start, n)}), nil
}

// hourOf floors a microsecond timestamp to its UTC hour, correctly for
// pre-epoch values.
func hourOf(us arrow.Timestamp) int64 {
	u := int64(us)
	return u - floorMod(u, hourUs)
}

func floorMod(v, m int64) int64 {
	r := v % m
	if r != 0 && (r < 0) != (m < 0) {
		r += m
	}
	return r
}

func hourTime(h int64) time.Time { return time.UnixMicro(h).UTC() }

func releaseParts(parts []Part) {
	for _, p := range parts {
		if p.Record != nil {
			p.Record.Release()
		}
	}
}

// Release frees the generation's Arrow memory: materialized parts if Parts
// ran, otherwise the detached builders. Idempotent; Parts returns ErrClosed
// afterwards.
func (g *Generation) Release() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.released {
		return
	}
	g.released = true
	releaseParts(g.parts)
	g.parts = nil
	for _, rec := range g.bases {
		rec.Release()
	}
	g.bases = nil
}
