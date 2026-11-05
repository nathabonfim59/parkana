// Package main implements parkbench, the reproducible ingest and query
// benchmark tool for Parkana and a real Loki instance.
//
// parkbench has two jobs. The ingest subcommand generates a deterministic
// corpus with fixed marker literals, pushes it to one Loki-compatible push
// endpoint and writes a JSON manifest describing exactly what was generated
// and acknowledged. The query subcommand (added in B2b) consumes that manifest
// to issue identical queries against both backends; corpus.go and ingest.go
// therefore expose the manifest types and the generation entry points as
// stable, reusable API.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
)

// Corpus layout constants. They are part of the corpus identity: changing any
// of them changes the generated records and the manifest hash.
const (
	// corpusVersion is the manifest/corpus layout version. Version 2 rotates
	// records through the configured streams within shortEvery-record cohorts,
	// so markers no longer correlate with one stream. Version 3 adds a
	// deterministic per-service instance label: every (stream, instance) pair
	// is one full label set and one sequential sender. Rendered line bytes,
	// timestamps and per-service marker placement are unchanged from version 2.
	corpusVersion = 3

	// Marker literals. The frequent marker marks a predictable share of all
	// records; the rare marker is confined to a few contiguous record blocks;
	// the absent marker is never generated; the short needle is exactly four
	// bytes, so it is eligible for index gram lengths 3 and 4 and not 5.
	frequentMarker = "PKFREQ-2v"
	rareMarker     = "PKRARE-9xqz"
	absentMarker   = "PKABSENT-7mvn"
	shortNeedle    = "zq9x"

	// Marker clustering block indexes, in record blocks of markerBlockSize.
	rareBlockA  int64 = 2
	rareBlockB  int64 = 9
	rareBlockC  int64 = 25
	shortBlockA int64 = 4
	shortBlockB int64 = 20

	// frequentEvery marks every frequentEvery-th record.
	frequentEvery = 8
	// rareEvery marks every rareEvery-th record inside a rare block.
	rareEvery = 16
	// shortEvery marks every shortEvery-th record inside a short block.
	shortEvery = 32
	// markerBlockSize is the number of consecutive records in one marker
	// clustering block. A block lands in one or two time partitions, so rare
	// hits cluster in a few files instead of appearing in every batch.
	markerBlockSize = 65536
	// fixedLineBytes is the exact rendered length (before an optional marker
	// suffix) of the fixed-length template.
	fixedLineBytes = 256
	// tailMinBytes is the minimum size of the deterministic tail line that
	// makes the achieved raw-byte total exactly equal to the target.
	tailMinBytes = 16
	// minLineBytes is a conservative lower bound on a rendered line. It is
	// only used to prove that the full-size corpus cannot overflow the int64
	// timestamp arithmetic; every template is much longer than this.
	minLineBytes = tailMinBytes

	defaultStreamLabel = "service_name"
	// instanceLabel carries the deterministic instance index of every generated
	// label set. It is fixed (not configurable) so the corpus identity can rely
	// on it and a manifest needs only the instance count.
	instanceLabel = "instance"
	// defaultInstancesPerService is the number of full sender label sets per
	// stream when the configuration does not set one.
	defaultInstancesPerService = 4
	// maxLabelSets bounds the generated full label sets (streams times
	// instances). It bounds the sender goroutines, manifest size and the
	// corpus hash identity; it is checked before label-set allocation.
	maxLabelSets = 4096
)

// Marker kinds, used as indexes into Corpus.markers.
const (
	markerFrequent = iota
	markerRare
	markerAbsent
	markerShort
	markerKinds
)

// Template indexes referenced by the generator and its tests.
const (
	templateAccessJSON = iota
	templateAppLogfmt
	templatePlain
	templateNginx
	templateFixed
	templateAuditJSON
	templateStack
	templateHeartbeat
	templateTail
	templateCount
)

// rareBlockSet returns the record blocks carrying the rare marker. They are
// absolute record indexes, so the same blocks exist in the small (2 GB) and
// full (25 GB) corpora.
func rareBlockSet() []int64 { return []int64{rareBlockA, rareBlockB, rareBlockC} }

// shortBlockSet returns the record blocks carrying the four-byte short needle.
func shortBlockSet() []int64 { return []int64{shortBlockA, shortBlockB} }

// defaultStreamNames returns the default streams. Four low-cardinality streams
// keep files large while still exercising more than one label set.
func defaultStreamNames() []string { return []string{"api", "checkout", "search", "worker"} }

// isRareBlock reports whether record block b carries the rare marker.
func isRareBlock(b int64) bool {
	switch b {
	case rareBlockA, rareBlockB, rareBlockC:
		return true
	default:
		return false
	}
}

// isShortBlock reports whether record block b carries the short needle.
func isShortBlock(b int64) bool {
	switch b {
	case shortBlockA, shortBlockB:
		return true
	default:
		return false
	}
}

// markerName returns the manifest name of a marker kind.
func markerName(kind int) string {
	switch kind {
	case markerFrequent:
		return "frequent"
	case markerRare:
		return "rare"
	case markerAbsent:
		return "absent"
	case markerShort:
		return "short_4byte"
	default:
		return ""
	}
}

// markerLiteral returns the literal of a marker kind.
func markerLiteral(kind int) string {
	switch kind {
	case markerFrequent:
		return frequentMarker
	case markerRare:
		return rareMarker
	case markerAbsent:
		return absentMarker
	case markerShort:
		return shortNeedle
	default:
		return ""
	}
}

// templateName returns the manifest name of a template index.
func templateName(tpl int) string {
	switch tpl {
	case templateAccessJSON:
		return "access_json"
	case templateAppLogfmt:
		return "app_logfmt"
	case templatePlain:
		return "plain"
	case templateNginx:
		return "nginx"
	case templateFixed:
		return "fixed_256"
	case templateAuditJSON:
		return "audit_json"
	case templateStack:
		return "stack"
	case templateHeartbeat:
		return "heartbeat"
	case templateTail:
		return "tail"
	default:
		return ""
	}
}

// timeLayout is the line-embedded timestamp: microsecond precision, UTC,
// matching the storage resolution so the rendered text does not imply
// precision the store discards.
const timeLayout = "2006-01-02T15:04:05.000000Z"

// CorpusConfig configures deterministic corpus generation. The zero value is
// invalid; NewCorpus validates and normalizes it.
type CorpusConfig struct {
	// Seed fixes the per-record pseudo-random stream.
	Seed uint64
	// Start is the timestamp of record 0. It must be microsecond-aligned and
	// representable as int64 Unix nanoseconds; NewCorpus rejects it otherwise.
	Start time.Time
	// Step is the timestamp distance between consecutive records. It must be
	// a positive multiple of a microsecond.
	Step time.Duration
	// TargetBytes is the exact number of raw line bytes to generate.
	TargetBytes int64
	// Streams are the stream (dataset) names. Records rotate through them
	// within shortEvery-record cohorts, so every stream receives records and
	// markers in every normal file-sized time slice. Empty uses
	// defaultStreamNames().
	Streams []string
	// InstancesPerService is the number of deterministic instance label values
	// generated for every stream. Each (stream, instance) pair is one full
	// label set and one sequential sender; line bytes, timestamps and
	// per-service marker placement do not depend on it. Zero uses
	// defaultInstancesPerService; negative values and a product above
	// maxLabelSets are rejected.
	InstancesPerService int
	// StreamLabel carries the stream name; empty uses service_name. It must
	// not be instanceLabel: that name is reserved for the generated instance.
	StreamLabel string
	// Labels are extra labels on every stream. A duplicate name in
	// StreamLabel is replaced by the stream name. The reserved instanceLabel
	// must not be set: the generator owns it.
	Labels logs.Labels
}

// CorpusInfo is the reproducible identity and achieved totals of one corpus.
// Hash fingerprints the generation settings and ContentHash fingerprints the
// emitted line bytes, so a mismatch between two runs is detectable without
// keeping the corpus; a match is strong evidence, not a cryptographic proof.
type CorpusInfo struct {
	Version int `json:"version"`
	// Hash is the SHA-256 of the generation parameters (seed, start, step,
	// target bytes, streams and labels). It identifies how to regenerate the
	// corpus; the same Hash with a different ContentHash means a generator
	// bug or a changed template, not different user settings.
	Hash string `json:"hash"`
	// ContentHash is the FNV-1a 64-bit fingerprint of every emitted line byte,
	// newline separated. It detects a corpus that differs after generation or
	// transfer; it is not a security hash and cannot be recomputed from the
	// parameters alone.
	ContentHash        string    `json:"content_hash"`
	Seed               uint64    `json:"seed"`
	Start              time.Time `json:"start"`
	StepMicros         int64     `json:"step_micros"`
	TargetRawLineBytes int64     `json:"target_raw_line_bytes"`
	RawLineBytes       int64     `json:"raw_line_bytes"`
	Lines              int64     `json:"lines"`
	Through            time.Time `json:"through"`
	Tenant             string    `json:"tenant,omitempty"`
	StreamLabel        string    `json:"stream_label"`
	// InstancesPerService is the number of full sender label sets per stream.
	// It is corpus identity: changing it changes Hash but not ContentHash or
	// the per-service totals, because instances only relabel records.
	InstancesPerService int            `json:"instances_per_service"`
	Streams             []StreamInfo   `json:"streams"`
	Markers             []MarkerInfo   `json:"markers"`
	Templates           []TemplateStat `json:"templates"`
}

// StreamInfo describes one service and its aggregate share of the corpus. It is
// not a sender label set: each service has InstancesPerService full label sets
// (see Corpus.StreamLabels), so concatenating Streams does not enumerate
// senders and the aggregate labels never carry the instance label.
type StreamInfo struct {
	Name         string            `json:"name"`
	Labels       map[string]string `json:"labels"`
	Lines        int64             `json:"lines"`
	RawLineBytes int64             `json:"raw_line_bytes"`
}

// MarkerInfo describes one marker class: its exact occurrence count, the
// half-open time ranges that contain all occurrences, and the index gram
// lengths for which the literal is long enough to be considered at all.
type MarkerInfo struct {
	Name    string `json:"name"`
	Literal string `json:"literal"`
	Bytes   int    `json:"bytes"`
	Count   int64  `json:"count"`
	// IndexEligibleGrams lists the gram lengths (3, 4, 5) that are not longer
	// than the literal. A needle shorter than the gram length cannot prune;
	// eligibility does not prove pruning, which the query run must establish
	// from the sidecar/pruning metrics.
	IndexEligibleGrams []int         `json:"index_eligible_grams"`
	Ranges             []MarkerRange `json:"ranges"`
	// Schedule lets a query runner compute the expected count for any
	// half-open time range from the manifest alone, without regenerating
	// corpus bytes. See CorpusInfo.ExpectedMarkerCount.
	Schedule MarkerSchedule `json:"schedule"`
}

// MarkerSchedule describes marker occurrences by record index: record i
// carries the marker exactly when i is a multiple of EveryRecords and lies in
// one of Blocks. Blocks are half-open record-index intervals.
type MarkerSchedule struct {
	EveryRecords int64         `json:"every_records"`
	Blocks       []RecordRange `json:"blocks"`
}

// RecordRange is a half-open interval of record indexes [From, Through).
type RecordRange struct {
	From    int64 `json:"from"`
	Through int64 `json:"through"`
}

// MarkerRange is a half-open timestamp interval [From, Through) containing
// Count marker occurrences. Query this interval as-is to reproduce the count.
// A range ends at the last hit plus one step, so a corpus that stops inside a
// clustering block still yields an exact count for the partial block.
type MarkerRange struct {
	From    time.Time `json:"from"`
	Through time.Time `json:"through"`
	Count   int64     `json:"count"`
}

// TemplateStat records how much of the corpus one line template produced.
type TemplateStat struct {
	Name         string `json:"name"`
	Count        int64  `json:"count"`
	RawLineBytes int64  `json:"raw_line_bytes"`
}

// Batch is one bounded slice of generated records, grouped by full label set
// and ordered by timestamp within each label set.
type Batch struct {
	// Streams holds one entry slice per sender label set, in service-major
	// order: slot stream*InstancesPerService+instance. Some slots may be empty.
	// Each slot is one (stream, instance) pair, not one stream.
	Streams [][]logs.Entry
	// Lines and RawBytes are the batch totals (the sum of len(entry.Line)).
	Lines    int64
	RawBytes int64
	// Done reports that this batch contains the final corpus record.
	Done bool
}

// Corpus generates the deterministic benchmark corpus. It is not safe for
// concurrent use: one goroutine must call NextBatch sequentially, which is
// what keeps the byte target and marker counts independent of batching and
// push concurrency.
type Corpus struct {
	cfg          CorpusConfig
	streamLabels []logs.Labels
	streamStats  []StreamInfo
	templates    []TemplateStat
	markers      []MarkerInfo
	markerGroup  [markerKinds][]int64
	hash         string
	hasher       hash.Hash64
	buf          []byte
	index        int64
	total        int64
	lines        int64
	done         bool
	start        time.Time
	step         time.Duration
	target       int64
}

// NewCorpus validates cfg and returns a corpus positioned at record 0.
func NewCorpus(cfg CorpusConfig) (*Corpus, error) {
	if cfg.TargetBytes < tailMinBytes {
		return nil, fmt.Errorf("corpus: -bytes must be at least %d, got %d", tailMinBytes, cfg.TargetBytes)
	}
	if cfg.TargetBytes > 1<<40 {
		return nil, fmt.Errorf("corpus: -bytes must not exceed 1 TiB, got %d", cfg.TargetBytes)
	}
	if cfg.Start.IsZero() {
		return nil, fmt.Errorf("corpus: -start must be set")
	}
	cfg.Start = cfg.Start.UTC()
	if cfg.Start.Nanosecond()%1000 != 0 {
		return nil, fmt.Errorf("corpus: -start must be microsecond-aligned, got %v", cfg.Start)
	}
	if cfg.Start.Before(minLokiTime()) || cfg.Start.After(maxLokiTime()) {
		return nil, fmt.Errorf("corpus: -start %v is outside the int64 nanosecond range [%v, %v]",
			cfg.Start, minLokiTime(), maxLokiTime())
	}
	if cfg.Step <= 0 {
		return nil, fmt.Errorf("corpus: -step must be positive, got %v", cfg.Step)
	}
	if cfg.Step%time.Microsecond != 0 {
		return nil, fmt.Errorf("corpus: -step must be a multiple of %v, got %v", time.Microsecond, cfg.Step)
	}
	// Every line is at least minLineBytes long, so target bytes bound the
	// record count. Reject a step whose duration, or whose absolute end
	// timestamp, would overflow int64 nanoseconds at that bound (relevant for
	// the 25 GB full run and for starts near year 2262).
	maxRecords := cfg.TargetBytes/minLineBytes + 2
	if maxRecords > math.MaxInt64/int64(cfg.Step) {
		return nil, fmt.Errorf("corpus: -step %v overflows the timestamp range for %d bytes", cfg.Step, cfg.TargetBytes)
	}
	span := time.Duration(maxRecords) * cfg.Step
	if cfg.Start.Add(span).After(maxLokiTime()) {
		return nil, fmt.Errorf("corpus: -start %v plus maximum corpus duration %v exceeds the int64 nanosecond range",
			cfg.Start, span)
	}
	if len(cfg.Streams) == 0 {
		cfg.Streams = defaultStreamNames()
	}
	cfg.Streams = dedupeStrings(cfg.Streams)
	for _, name := range cfg.Streams {
		if err := validateStreamName(name); err != nil {
			return nil, err
		}
	}
	if cfg.StreamLabel == "" {
		cfg.StreamLabel = defaultStreamLabel
	}
	if err := (logs.Labels{{Name: cfg.StreamLabel}}).Validate(); err != nil {
		return nil, fmt.Errorf("corpus: -stream-label: %w", err)
	}
	if err := cfg.Labels.Validate(); err != nil {
		return nil, fmt.Errorf("corpus: -label: %w", err)
	}
	cfg.Labels = logs.New(slices.Clone(cfg.Labels)...)
	if cfg.InstancesPerService < 0 {
		return nil, fmt.Errorf("corpus: -instances-per-service must not be negative, got %d", cfg.InstancesPerService)
	}
	if cfg.InstancesPerService == 0 {
		cfg.InstancesPerService = defaultInstancesPerService
	}
	if cfg.InstancesPerService > maxLabelSets {
		return nil, fmt.Errorf("corpus: -instances-per-service %d exceeds the %d label set bound", cfg.InstancesPerService, maxLabelSets)
	}
	if cfg.StreamLabel == instanceLabel {
		return nil, fmt.Errorf("corpus: -stream-label must not be the reserved instance label %q", instanceLabel)
	}
	if _, ok := cfg.Labels.Get(instanceLabel); ok {
		return nil, fmt.Errorf("corpus: -label must not set the reserved instance label %q", instanceLabel)
	}
	// Bound the product before allocating one label set per (stream, instance).
	// len(cfg.Streams) is positive and cfg.InstancesPerService is bounded, so
	// this division cannot overflow or divide by zero.
	if len(cfg.Streams) > maxLabelSets/cfg.InstancesPerService {
		return nil, fmt.Errorf("corpus: %d streams times %d instances exceeds the %d label set bound",
			len(cfg.Streams), cfg.InstancesPerService, maxLabelSets)
	}

	c := &Corpus{
		cfg:         cfg,
		templates:   make([]TemplateStat, templateCount),
		markers:     make([]MarkerInfo, markerKinds),
		markerGroup: [markerKinds][]int64{},
		hasher:      fnv.New64a(),
		start:       cfg.Start,
		step:        cfg.Step,
		target:      cfg.TargetBytes,
	}
	for i := range templateCount {
		c.templates[i].Name = templateName(i)
	}
	for kind := range markerKinds {
		literal := markerLiteral(kind)
		c.markers[kind] = MarkerInfo{
			Name:               markerName(kind),
			Literal:            literal,
			Bytes:              len(literal),
			IndexEligibleGrams: eligibleGrams(len(literal)),
			Ranges:             []MarkerRange{},
			Schedule:           MarkerSchedule{Blocks: []RecordRange{}},
		}
	}
	c.streamStats = make([]StreamInfo, len(cfg.Streams))
	c.streamLabels = make([]logs.Labels, 0, len(cfg.Streams)*cfg.InstancesPerService)
	for i, name := range cfg.Streams {
		pairs := append(slices.Clone(cfg.Labels), logs.Label{Name: cfg.StreamLabel, Value: name})
		ls := logs.New(pairs...)
		if err := ls.Validate(); err != nil {
			return nil, fmt.Errorf("corpus: stream %q: %w", name, err)
		}
		c.streamStats[i] = StreamInfo{Name: name, Labels: ls.Map()}
		for instance := range cfg.InstancesPerService {
			full := append(slices.Clone(ls), logs.Label{Name: instanceLabel, Value: strconv.Itoa(instance)})
			fullLabels := logs.New(full...)
			if err := fullLabels.Validate(); err != nil {
				return nil, fmt.Errorf("corpus: stream %q instance %d: %w", name, instance, err)
			}
			c.streamLabels = append(c.streamLabels, fullLabels)
		}
	}
	hash, err := corpusHash(cfg)
	if err != nil {
		return nil, err
	}
	c.hash = hash
	return c, nil
}

// Config returns the normalized configuration.
func (c *Corpus) Config() CorpusConfig { return c.cfg }

// StreamLabels returns the sorted label set of every sender: one per
// (stream, instance) pair, in service-major order (slot
// stream*InstancesPerService+instance). The manifest Streams stay per-service
// aggregates; this is the sender view.
func (c *Corpus) StreamLabels() []logs.Labels { return c.streamLabels }

// NextBatch renders the next batch of at most maxLines records. It returns a
// batch with Done set when it emitted the final tail record; later calls return
// empty done batches. Generation is sequential: the stopping rule depends only
// on the cumulative raw line bytes, so batch size changes the request grouping
// but never the corpus identity.
func (c *Corpus) NextBatch(maxLines int) (*Batch, error) {
	if maxLines <= 0 {
		return nil, fmt.Errorf("corpus: batch size must be positive, got %d", maxLines)
	}
	b := &Batch{Streams: make([][]logs.Entry, len(c.streamLabels))}
	if c.done {
		b.Done = true
		return b, nil
	}
	for count := 0; count < maxLines; {
		remaining := c.target - c.total
		if remaining <= tailMinBytes {
			c.emitTail(b)
			break
		}
		line, stream, ts, tpl, hits := c.record(c.index)
		if int64(len(line)) > remaining-tailMinBytes {
			c.emitTail(b)
			break
		}
		slot := c.labelSetSlot(c.index, stream)
		c.accept(line, stream, ts, tpl, hits)
		b.Streams[slot] = append(b.Streams[slot], logs.Entry{Timestamp: ts, Line: line})
		b.Lines++
		b.RawBytes += int64(len(line))
		count++
	}
	return b, nil
}

// Manifest returns the corpus identity and the exact achieved totals and
// marker counts. Call it after generation for a complete manifest; before
// generation the totals describe an empty corpus.
func (c *Corpus) Manifest() CorpusInfo {
	m := CorpusInfo{
		Version:             corpusVersion,
		Hash:                c.hash,
		ContentHash:         fmt.Sprintf("%016x", c.hasher.Sum64()),
		Seed:                c.cfg.Seed,
		Start:               c.start,
		StepMicros:          c.step.Microseconds(),
		TargetRawLineBytes:  c.target,
		RawLineBytes:        c.total,
		Lines:               c.lines,
		Through:             c.timestamp(c.lines),
		StreamLabel:         c.cfg.StreamLabel,
		InstancesPerService: c.cfg.InstancesPerService,
		Streams:             slices.Clone(c.streamStats),
		Markers:             slices.Clone(c.markers),
		Templates:           slices.Clone(c.templates),
	}
	for kind := range m.Markers {
		m.Markers[kind].Ranges = slices.Clone(m.Markers[kind].Ranges)
		m.Markers[kind].Schedule = markerSchedule(kind, m.Lines)
	}
	return m
}

// markerSchedule returns the record-index schedule for a marker kind. Normal
// records are [0, lines-1): the final record is the unmarked tail.
func markerSchedule(kind int, lines int64) MarkerSchedule {
	limit := lines - 1
	switch kind {
	case markerFrequent:
		if limit <= 0 {
			return MarkerSchedule{Blocks: []RecordRange{}}
		}
		return MarkerSchedule{
			EveryRecords: frequentEvery,
			Blocks:       []RecordRange{{From: 0, Through: limit}},
		}
	case markerRare:
		return MarkerSchedule{EveryRecords: rareEvery, Blocks: clipBlocks(rareBlockSet(), limit)}
	case markerShort:
		return MarkerSchedule{EveryRecords: shortEvery, Blocks: clipBlocks(shortBlockSet(), limit)}
	default:
		return MarkerSchedule{Blocks: []RecordRange{}}
	}
}

// clipBlocks turns configured block indexes into record ranges clipped to the
// corpus's normal-record limit.
func clipBlocks(blocks []int64, limit int64) []RecordRange {
	out := []RecordRange{}
	for _, block := range blocks {
		from := block * markerBlockSize
		through := min(from+markerBlockSize, limit)
		if from >= through {
			continue
		}
		out = append(out, RecordRange{From: from, Through: through})
	}
	return out
}

// ExpectedMarkerCount returns the exact number of marker occurrences in the
// half-open time range [from, through) using only the manifest. It lets a query
// runner choose a small common interval for both backends without regenerating
// gigabytes of corpus. A range outside the corpus is clamped to it.
func (m CorpusInfo) ExpectedMarkerCount(name string, from, through time.Time) (int64, error) {
	if m.StepMicros <= 0 || m.StepMicros > math.MaxInt64/int64(time.Microsecond) {
		return 0, fmt.Errorf("corpus: manifest step %d microseconds is out of the int64 nanosecond range", m.StepMicros)
	}
	kind := slices.IndexFunc(m.Markers, func(marker MarkerInfo) bool { return marker.Name == name })
	if kind < 0 {
		return 0, fmt.Errorf("corpus: unknown marker %q", name)
	}
	marker := m.Markers[kind]
	if marker.Schedule.EveryRecords <= 0 || len(marker.Schedule.Blocks) == 0 || m.Lines <= 0 {
		return 0, nil
	}
	if !through.After(m.Start) {
		return 0, nil
	}
	if from.Before(m.Start) {
		from = m.Start
	}
	if through.After(m.Through) {
		through = m.Through
	}
	if !from.Before(through) {
		return 0, nil
	}
	step := m.StepMicros * int64(time.Microsecond)
	lo := ceilDiv(from.UnixNano()-m.Start.UnixNano(), step)
	hi := min(ceilDiv(through.UnixNano()-m.Start.UnixNano(), step), m.Lines)
	if lo >= hi {
		return 0, nil
	}
	var count int64
	for _, block := range marker.Schedule.Blocks {
		count += multiplesIn(max(lo, block.From), min(hi, block.Through), marker.Schedule.EveryRecords)
	}
	return count, nil
}

// ceilDiv returns ceil(a/b) for a >= 0 and b > 0 without overflowing a+b.
func ceilDiv(a, b int64) int64 {
	if a <= 0 {
		return 0
	}
	q := a / b
	if a%b != 0 {
		q++
	}
	return q
}

// multiplesIn returns the count of multiples of every in [lo, hi).
func multiplesIn(lo, hi, every int64) int64 {
	if lo >= hi || every <= 0 {
		return 0
	}
	first := ((lo + every - 1) / every) * every
	if first >= hi {
		return 0
	}
	return (hi-1-first)/every + 1
}

// record renders record i, its stream index, timestamp, template index and the
// markers it carries. It is a pure function of the corpus seed and i.
func (c *Corpus) record(i int64) (line string, stream int, ts time.Time, tpl int, hits markerHits) {
	// Choice tables live here, not at package level, so nothing can mutate
	// the fixture between calls.
	levels := [...]string{"info", "warn", "error", "debug"}
	msgs := [...]string{
		"request completed",
		"connection timed out",
		"cache miss",
		"user login succeeded",
		"disk quota exceeded",
		"retrying upstream",
		"invalid token",
		"slow query",
	}
	methods := [...]string{"GET", "POST", "PUT", "DELETE"}
	words := [...]string{"items", "users", "orders", "sessions", "health", "metrics", "cart", "profile"}
	statuses := [...]uint64{200, 201, 204, 301, 400, 401, 403, 404, 500, 502, 503}
	r := corpusRNG(c.cfg.Seed ^ (uint64(i)+1)*0x9e3779b97f4a7c15)
	tpl = int(r.next() % uint64(templateCount-1)) // the tail template is not selected
	stream = streamFor(i, len(c.streamStats))
	ts = c.timestamp(i)
	level := levels[r.next()%uint64(len(levels))]
	msg := msgs[r.next()%uint64(len(msgs))]
	method := methods[r.next()%uint64(len(methods))]
	word := words[r.next()%uint64(len(words))]
	status := statuses[r.next()%uint64(len(statuses))]
	duration := 1 + r.next()%5000
	size := 1 + r.next()%65536

	hits = c.hits(i)
	var markers []byte
	if hits.frequent {
		markers = append(markers, frequentMarker...)
	}
	if hits.rare {
		if len(markers) > 0 {
			markers = append(markers, ' ')
		}
		markers = append(markers, rareMarker...)
	}
	if hits.short {
		if len(markers) > 0 {
			markers = append(markers, ' ')
		}
		markers = append(markers, shortNeedle...)
	}

	b := c.buf[:0]
	switch tpl {
	case templateAccessJSON:
		b = append(b, `{"time":"`...)
		b = ts.AppendFormat(b, timeLayout)
		b = append(b, `","level":"`...)
		b = append(b, level...)
		b = append(b, `","service":"`...)
		b = append(b, c.streamStats[stream].Name...)
		b = append(b, `","msg":"`...)
		b = append(b, msg...)
		b = append(b, `","method":"`...)
		b = append(b, method...)
		b = append(b, `","path":"/api/v1/`...)
		b = append(b, word...)
		b = append(b, '/')
		b = strconv.AppendInt(b, i, 10)
		b = append(b, `","status":`...)
		b = strconv.AppendUint(b, status, 10)
		b = append(b, `,"duration_ms":`...)
		b = strconv.AppendUint(b, duration, 10)
		if len(markers) > 0 {
			b = append(b, `,"marker":"`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
		b = append(b, '}')
	case templateAppLogfmt:
		b = append(b, "ts="...)
		b = ts.AppendFormat(b, timeLayout)
		b = append(b, " level="...)
		b = append(b, level...)
		b = append(b, " service="...)
		b = append(b, c.streamStats[stream].Name...)
		b = append(b, ` msg="`...)
		b = append(b, msg...)
		b = append(b, `" id=`...)
		b = strconv.AppendInt(b, i, 10)
		b = append(b, " latency_ms="...)
		b = strconv.AppendUint(b, duration, 10)
		b = append(b, " trace_id="...)
		b = appendHex(b, &r, 16)
		b = append(b, " span_id="...)
		b = appendHex(b, &r, 8)
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	case templatePlain:
		b = ts.AppendFormat(b, timeLayout)
		b = append(b, ' ')
		b = append(b, strings.ToUpper(level)...)
		b = append(b, ' ')
		b = append(b, c.streamStats[stream].Name...)
		b = append(b, ' ')
		b = append(b, msg...)
		b = append(b, " id="...)
		b = strconv.AppendInt(b, i, 10)
		b = append(b, " status="...)
		b = strconv.AppendUint(b, status, 10)
		b = append(b, " duration="...)
		b = strconv.AppendUint(b, duration, 10)
		b = append(b, "ms"...)
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	case templateNginx:
		b = append(b, "10.0."...)
		b = strconv.AppendUint(b, r.next()%256, 10)
		b = append(b, '.')
		b = strconv.AppendUint(b, r.next()%256, 10)
		b = append(b, ` - - [`...)
		b = ts.AppendFormat(b, timeLayout)
		b = append(b, `] "GET /`...)
		b = append(b, word...)
		b = append(b, '/')
		b = strconv.AppendInt(b, i, 10)
		b = append(b, ` HTTP/1.1" `...)
		b = strconv.AppendUint(b, status, 10)
		b = append(b, ' ')
		b = strconv.AppendUint(b, size, 10)
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	case templateFixed:
		b = append(b, "level="...)
		b = append(b, level...)
		b = append(b, " service="...)
		b = append(b, c.streamStats[stream].Name...)
		b = append(b, " id="...)
		b = strconv.AppendInt(b, i, 10)
		b = append(b, ` msg="`...)
		b = append(b, msg...)
		b = append(b, `" payload=`...)
		for len(b) < fixedLineBytes {
			b = append(b, 'x')
		}
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	case templateAuditJSON:
		b = append(b, `{"time":"`...)
		b = ts.AppendFormat(b, timeLayout)
		b = append(b, `","level":"`...)
		b = append(b, level...)
		b = append(b, `","service":"`...)
		b = append(b, c.streamStats[stream].Name...)
		b = append(b, `","event":"audit","trace":"`...)
		b = appendHex(b, &r, 32)
		b = append(b, `","token":"`...)
		b = appendHex(b, &r, 64)
		b = append(b, `","id":`...)
		b = strconv.AppendInt(b, i, 10)
		if len(markers) > 0 {
			b = append(b, `,"marker":"`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
		b = append(b, '}')
	case templateStack:
		b = ts.AppendFormat(b, timeLayout)
		b = append(b, " ERROR "...)
		b = append(b, c.streamStats[stream].Name...)
		b = append(b, " goroutine 42: "...)
		b = append(b, msg...)
		b = append(b, " id="...)
		b = strconv.AppendInt(b, i, 10)
		b = append(b, ` err="upstream returned status `...)
		b = strconv.AppendUint(b, status, 10)
		b = append(b, `" stack="main.handler->serverHandler->goexit"`...)
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	case templateHeartbeat:
		b = append(b, "tick="...)
		b = strconv.AppendInt(b, i, 10)
		b = append(b, " service="...)
		b = append(b, c.streamStats[stream].Name...)
		b = append(b, " heartbeat ok sed="...)
		b = appendHex(b, &r, 12)
		if len(markers) > 0 {
			b = append(b, ` marker="`...)
			b = append(b, markers...)
			b = append(b, '"')
		}
	}
	c.buf = b
	return string(b), stream, ts, tpl, hits
}

// hits reports the marker classes record i carries.
func (c *Corpus) hits(i int64) markerHits {
	block := i / markerBlockSize
	return markerHits{
		frequent: i%frequentEvery == 0,
		rare:     isRareBlock(block) && i%rareEvery == 0,
		short:    isShortBlock(block) && i%shortEvery == 0,
	}
}

// accept accounts for one emitted record.
func (c *Corpus) accept(line string, stream int, ts time.Time, tpl int, hits markerHits) {
	index := c.index
	c.index++
	c.total += int64(len(line))
	c.lines++
	_, _ = c.hasher.Write([]byte(line))
	_, _ = c.hasher.Write([]byte{'\n'})
	c.streamStats[stream].Lines++
	c.streamStats[stream].RawLineBytes += int64(len(line))
	c.templates[tpl].Count++
	c.templates[tpl].RawLineBytes += int64(len(line))
	c.track(index, hits, ts)
}

// emitTail writes the final filler record that makes the raw byte total exactly
// equal the target. It carries no marker, so marker counts are unaffected.
func (c *Corpus) emitTail(b *Batch) {
	remaining := c.target - c.total
	stream := streamFor(c.index, len(c.streamStats))
	line := make([]byte, 0, remaining)
	line = append(line, "tail="...)
	for int64(len(line)) < remaining {
		line = append(line, 'x')
	}
	ts := c.timestamp(c.index)
	slot := c.labelSetSlot(c.index, stream)
	c.index++
	c.total += int64(len(line))
	c.lines++
	_, _ = c.hasher.Write(line)
	_, _ = c.hasher.Write([]byte{'\n'})
	c.streamStats[stream].Lines++
	c.streamStats[stream].RawLineBytes += int64(len(line))
	c.templates[templateTail].Count++
	c.templates[templateTail].RawLineBytes += int64(len(line))
	b.Streams[slot] = append(b.Streams[slot], logs.Entry{Timestamp: ts, Line: string(line)})
	b.Lines++
	b.RawBytes += int64(len(line))
	b.Done = true
	c.done = true
}

// track records the markers of accepted record i. Hits in the same clustering
// group extend the last half-open range; a new group starts a new range, so
// each rare or short block is independently queryable, including a block that
// the corpus target truncated.
func (c *Corpus) track(i int64, hits markerHits, ts time.Time) {
	block := i / markerBlockSize
	c.hit(markerFrequent, 0, hits.frequent, ts)
	c.hit(markerRare, block, hits.rare, ts)
	c.hit(markerShort, block, hits.short, ts)
}

// hit updates one marker class with one occurrence.
func (c *Corpus) hit(kind int, group int64, present bool, ts time.Time) {
	if !present {
		return
	}
	m := &c.markers[kind]
	m.Count++
	groups := c.markerGroup[kind]
	if n := len(groups); n > 0 && groups[n-1] == group {
		r := &m.Ranges[n-1]
		r.Through = ts.Add(c.step)
		r.Count++
		return
	}
	c.markerGroup[kind] = append(groups, group)
	m.Ranges = append(m.Ranges, MarkerRange{From: ts, Through: ts.Add(c.step), Count: 1})
}

// streamFor returns the stream index of global record i. Records rotate
// through the configured streams within each shortEvery-record cohort: position
// p of cohort k goes to stream (p+k) % numStreams. Every fixed within-cohort
// position therefore visits every stream within numStreams cohorts, so marker
// presence cannot correlate with one stream even when a marker period is a
// multiple of the stream count (the old i%numStreams did). numStreams must be
// positive.
func streamFor(i int64, numStreams int) int {
	return int((i%shortEvery + i/shortEvery) % int64(numStreams))
}

// instanceFor returns the instance index of global record i. It divides the
// shortEvery-record cohort index by the service count and only then takes the
// remainder, so no product can overflow and marker strides cannot alias: a
// marker at i=shortEvery*k lands on service k%numStreams and instance
// floor(k/numStreams)%instances, and k covers every service-instance pair over
// shortEvery*numStreams*instances records. numStreams and instances must be
// positive.
func instanceFor(i int64, numStreams, instances int) int {
	if numStreams <= 0 || instances <= 0 {
		return 0
	}
	return int((i / shortEvery / int64(numStreams)) % int64(instances))
}

// labelSetSlot returns the sender slot of record i for the already computed
// service index. Slots are service-major: service*instances+instance.
func (c *Corpus) labelSetSlot(i int64, service int) int {
	return service*c.cfg.InstancesPerService + instanceFor(i, len(c.streamStats), c.cfg.InstancesPerService)
}

// timestamp returns the timestamp of record i.
func (c *Corpus) timestamp(i int64) time.Time {
	return c.start.Add(time.Duration(i) * c.step)
}

// minLokiTime and maxLokiTime bound the timestamps representable as int64 Unix
// nanoseconds, which is how Loki pushes and the push codec encode them.
func minLokiTime() time.Time { return time.Unix(0, math.MinInt64).UTC() }

func maxLokiTime() time.Time { return time.Unix(0, math.MaxInt64).UTC() }

// markerHits says which marker classes one record carries.
type markerHits struct {
	frequent bool
	rare     bool
	short    bool
}

// corpusRNG is a splitmix64 stream.
type corpusRNG uint64

func (r *corpusRNG) next() uint64 {
	*r += 0x9e3779b97f4a7c15
	z := uint64(*r)
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// appendHex appends n lowercase hex digits from the rng stream.
func appendHex(b []byte, r *corpusRNG, n int) []byte {
	const digits = "0123456789abcdef"
	for range n {
		b = append(b, digits[r.next()&0xf])
	}
	return b
}

// eligibleGrams returns the index gram lengths that are not longer than a
// literal of n bytes: a needle shorter than the gram length cannot prune and
// must scan. Eligibility alone does not prove a query pruned files; the query
// run measures that.
func eligibleGrams(n int) []int {
	var out []int
	for _, g := range [...]int{3, 4, 5} {
		if g <= n {
			out = append(out, g)
		}
	}
	return out
}

// corpusHash hashes the corpus identity: every setting that changes generated
// records, and nothing that only changes how they are pushed.
func corpusHash(cfg CorpusConfig) (string, error) {
	type label struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	type identity struct {
		Version             int      `json:"version"`
		Seed                uint64   `json:"seed"`
		StartMicros         int64    `json:"start_micros"`
		StepMicros          int64    `json:"step_micros"`
		TargetBytes         int64    `json:"target_bytes"`
		StreamLabel         string   `json:"stream_label"`
		InstancesPerService int      `json:"instances_per_service"`
		Streams             []string `json:"streams"`
		Labels              []label  `json:"labels"`
	}
	in := identity{
		Version:             corpusVersion,
		Seed:                cfg.Seed,
		StartMicros:         cfg.Start.UnixMicro(),
		StepMicros:          cfg.Step.Microseconds(),
		TargetBytes:         cfg.TargetBytes,
		StreamLabel:         cfg.StreamLabel,
		InstancesPerService: cfg.InstancesPerService,
		Streams:             cfg.Streams,
	}
	for _, l := range cfg.Labels {
		in.Labels = append(in.Labels, label{Name: l.Name, Value: l.Value})
	}
	b, err := json.Marshal(in)
	if err != nil {
		return "", fmt.Errorf("corpus: hash identity: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// validateStreamName keeps stream names safe for the line templates: they are
// embedded verbatim in JSON, logfmt and plain lines. Rejecting marker literals
// also keeps marker counts provable from the generator's predicates alone.
func validateStreamName(name string) error {
	if name == "" {
		return fmt.Errorf("corpus: stream names must not be empty")
	}
	for _, c := range []byte(name) {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
		default:
			return fmt.Errorf("corpus: stream name %q contains %q; use [A-Za-z0-9_-]", name, c)
		}
	}
	for kind := range markerKinds {
		if literal := markerLiteral(kind); strings.Contains(name, literal) {
			return fmt.Errorf("corpus: stream name %q contains marker %q", name, literal)
		}
	}
	return nil
}

// dedupeStrings removes duplicate strings, keeping the first occurrence.
func dedupeStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}
