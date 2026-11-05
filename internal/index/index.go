// Package index builds and reads search sidecars: small per-file filters the
// querier checks before it adds a Parquet file to a scan (PLAN.md §4 layer 3,
// decision 12).
//
// A sidecar (objstore.SidecarKey of the data key) is a bloom filter over the
// byte n-grams of every line in the file. A line that contains a needle
// contains every n-gram of the needle, so if any n-gram of the needle is
// missing from the filter no line in the file can contain it, and the file is
// skipped. The rules every user must keep:
//
//   - No false negatives, ever: MayContain returns true whenever some line
//     of the file contains the needle as a byte substring (plan.LineContains
//     semantics). Correctness of query results depends on it; the
//     indextest-style property tests in this package pin it.
//   - Only case-sensitive byte-substring filters (plan.LineContains) can
//     prune. `!=`, regexps and case-insensitive matching fall back to a scan
//     unless a caller derives a required literal and proves the derivation.
//   - A needle shorter than the gram length cannot prune (MayContain is true).
//   - A sidecar that is missing, unreadable, corrupt or of an unknown version
//     means "cannot prune", never "skip the file": Decode errors wrap
//     ErrFormat so readers can tell them apart from I/O errors.
//   - Sidecars describe one immutable data object. Writers put the sidecar
//     before cataloging the file with HasSidecar (or before
//     catalog.SetSidecar).
package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"iter"
	"math/bits"

	"github.com/bits-and-blooms/bloom/v3"
)

// Version is the sidecar format version Encode writes and Decode reads.
const Version = 1

// magic starts every sidecar.
var magic = [4]byte{'P', 'K', 'I', 'X'}

// headerLen is magic, version, gram length and two reserved bytes.
const headerLen = 8

var (
	// ErrFormat wraps every Decode error caused by the sidecar's bytes
	// (bad magic, unknown version, truncation, checksum mismatch). The
	// reader treats such a file as having no sidecar.
	ErrFormat = errors.New("index: bad sidecar")
	// ErrTooManyGrams is returned by Builder.Encode when the file has more
	// distinct grams than Params.MaxGrams: the filter would be too large to
	// be worth reading, so the writer stores no sidecar.
	ErrTooManyGrams = errors.New("index: too many distinct grams")
)

// Params tune a sidecar. They are recorded in the sidecar (the gram length
// directly, the false-positive rate through the bloom size), so changing them
// never invalidates existing sidecars.
type Params struct {
	// N is the gram length in bytes, 1..8.
	N int
	// FPRate is the target bloom false-positive rate per gram lookup, in (0, 1).
	FPRate float64
	// MaxGrams caps the distinct grams per file; above it Encode fails with
	// ErrTooManyGrams. It also bounds the builder's memory.
	MaxGrams int
}

// DefaultParams are the phase 3 starting point; benchmarks tune them.
var DefaultParams = Params{N: 4, FPRate: 0.01, MaxGrams: 2 << 20}

// Validate checks p.
func (p Params) Validate() error {
	if p.N < 1 || p.N > 8 || !(p.FPRate > 0 && p.FPRate < 1) || p.MaxGrams < 1 {
		return fmt.Errorf("index: invalid params %+v", p)
	}
	return nil
}

// Builder collects the grams of a file's lines. It is not safe for
// concurrent use.
type Builder struct {
	p       Params
	set     gramSet
	tooMany bool
}

// NewBuilder returns an empty Builder.
func NewBuilder(p Params) (*Builder, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return &Builder{p: p}, nil
}

// Add records one line. Once the gram cap is exceeded Add stops collecting.
//
// It keeps the window packed in a rolling uint64 (the same big-endian packing
// as grams) so each byte costs one shift instead of repacking N bytes, and
// inserts into gramSet instead of a Go map: on real objects the map insert
// was over half of a sidecar build's CPU.
func (b *Builder) Add(line string) {
	n := b.p.N
	if b.tooMany || len(line) < n {
		return
	}
	// For n == 8 the shift is 64 and Go defines it as 0, so mask is all ones.
	mask := uint64(1)<<(8*uint(n)) - 1
	var g uint64
	for i := range n - 1 {
		g = g<<8 | uint64(line[i])
	}
	for i := n - 1; i < len(line); i++ {
		g = (g<<8 | uint64(line[i])) & mask
		if b.set.add(g) && b.set.len() > b.p.MaxGrams {
			b.tooMany = true
			b.set = gramSet{}
			return
		}
	}
}

// Grams returns the number of distinct grams collected so far.
func (b *Builder) Grams() int { return b.set.len() }

// Encode returns the sidecar bytes, or ErrTooManyGrams.
func (b *Builder) Encode() ([]byte, error) {
	if b.tooMany {
		return nil, fmt.Errorf("%w: more than %d", ErrTooManyGrams, b.p.MaxGrams)
	}
	bf := bloom.NewWithEstimates(uint(max(b.set.len(), 1)), b.p.FPRate)
	var key [8]byte
	for g := range b.set.all() {
		binary.BigEndian.PutUint64(key[:], g)
		bf.Add(key[:])
	}
	var buf bytes.Buffer
	buf.Write(magic[:])
	buf.Write([]byte{Version, byte(b.p.N), 0, 0})
	if _, err := bf.WriteTo(&buf); err != nil {
		return nil, fmt.Errorf("index: encode: %w", err)
	}
	return binary.BigEndian.AppendUint32(buf.Bytes(), crc32.ChecksumIEEE(buf.Bytes())), nil
}

// Filter is a decoded sidecar. It is safe for concurrent use.
type Filter struct {
	n  int
	bf *bloom.BloomFilter
}

// Decode parses sidecar bytes. Errors wrap ErrFormat.
func Decode(data []byte) (*Filter, error) {
	if len(data) < headerLen+4 {
		return nil, fmt.Errorf("%w: %d bytes", ErrFormat, len(data))
	}
	body, sum := data[:len(data)-4], binary.BigEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(body) != sum {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrFormat)
	}
	if !bytes.Equal(body[:4], magic[:]) {
		return nil, fmt.Errorf("%w: bad magic", ErrFormat)
	}
	if v := body[4]; v != Version {
		return nil, fmt.Errorf("%w: unknown version %d", ErrFormat, v)
	}
	n := int(body[5])
	if n < 1 || n > 8 {
		return nil, fmt.Errorf("%w: gram length %d", ErrFormat, n)
	}
	if err := checkBloom(body[headerLen:]); err != nil {
		return nil, err
	}
	var bf bloom.BloomFilter
	r := bytes.NewReader(body[headerLen:])
	if _, err := bf.ReadFrom(r); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrFormat, err)
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrFormat, r.Len())
	}
	return &Filter{n: n, bf: &bf}, nil
}

// maxHashes bounds the bloom hash count Decode accepts. Encode's estimate is
// about -log2(FPRate), so 64 admits every FPRate down to 1e-19 while keeping a
// hostile sidecar from making each lookup arbitrarily slow.
const maxHashes = 64

// bloomHeaderLen is the bloom serialization's fixed part (bloom v3 WriteTo):
// m and k as big-endian uint64, then the bitset's length in bits, followed by
// ceil(length/64) big-endian uint64 words.
const bloomHeaderLen = 24

// checkBloom validates a serialized bloom filter's dimensions before it is
// decoded, so a checksum-correct but inconsistent sidecar is ErrFormat
// instead of a panic or a false negative at lookup time: m == 0 divides by
// zero in BloomFilter.Test, a bitset shorter than m reads unset (absent) bits
// for out-of-range locations, and an oversized length would make the bitset
// allocate more than the sidecar holds. Encode always writes m >= 1,
// 1 <= k, and a bitset of exactly m bits.
func checkBloom(b []byte) error {
	if len(b) < bloomHeaderLen {
		return fmt.Errorf("%w: bloom header truncated", ErrFormat)
	}
	m := binary.BigEndian.Uint64(b[0:8])
	k := binary.BigEndian.Uint64(b[8:16])
	length := binary.BigEndian.Uint64(b[16:24])
	payload := uint64(len(b) - bloomHeaderLen)
	switch {
	case m == 0 || m > payload*8:
		return fmt.Errorf("%w: bloom size %d bits for %d payload bytes", ErrFormat, m, payload)
	case k == 0 || k > maxHashes:
		return fmt.Errorf("%w: bloom hash count %d", ErrFormat, k)
	case length != m:
		return fmt.Errorf("%w: bitset length %d, bloom size %d", ErrFormat, length, m)
	case payload != (m+63)/64*8:
		return fmt.Errorf("%w: bitset payload %d bytes for %d bits", ErrFormat, payload, m)
	}
	return nil
}

// MayContain reports whether some line of the file may contain needle as a
// byte substring. False is a proof that none does.
func (f *Filter) MayContain(needle string) bool {
	if len(needle) < f.n {
		return true
	}
	var key [8]byte
	for g := range grams(needle, f.n) {
		binary.BigEndian.PutUint64(key[:], g)
		if !f.bf.Test(key[:]) {
			return false
		}
	}
	return true
}

// GramLen returns the gram length the sidecar was built with.
func (f *Filter) GramLen() int { return f.n }

// grams yields each length-n byte window of s packed big-endian into a
// uint64 (n <= 8, so packing is injective for a fixed n).
func grams(s string, n int) iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		for i := 0; i+n <= len(s); i++ {
			var g uint64
			for j := range n {
				g = g<<8 | uint64(s[i+j])
			}
			if !yield(g) {
				return
			}
		}
	}
}

// gramSet is a set of packed grams: open addressing with linear probing over
// a power-of-two table, kept at most half full. Zero marks an empty slot, so
// the gram 0 (N NUL bytes) is tracked by hasZero instead. The zero value is
// an empty set.
type gramSet struct {
	slots   []uint64
	n       int // non-zero grams stored in slots
	hasZero bool
	shift   uint // 64 - log2(len(slots))
}

// gramSetMinSlots is the first table size; it doubles as the set grows.
const gramSetMinSlots = 1 << 12

// len returns the number of distinct grams in s.
func (s *gramSet) len() int {
	if s.hasZero {
		return s.n + 1
	}
	return s.n
}

// add inserts g and reports whether it was new.
func (s *gramSet) add(g uint64) bool {
	if g == 0 {
		if s.hasZero {
			return false
		}
		s.hasZero = true
		return true
	}
	if 2*(s.n+1) > len(s.slots) {
		s.grow()
	}
	mask := uint64(len(s.slots) - 1)
	for i := s.slot(g); ; i = (i + 1) & mask {
		switch s.slots[i] {
		case g:
			return false
		case 0:
			s.slots[i] = g
			s.n++
			return true
		}
	}
}

// slot is g's home slot: Fibonacci hashing keeps the top bits, which mix
// every input byte, so packed grams that differ only in low bytes spread out.
func (s *gramSet) slot(g uint64) uint64 {
	return (g * 0x9E3779B97F4A7C15) >> s.shift
}

// grow doubles the table (or allocates the first one) and reinserts.
func (s *gramSet) grow() {
	size := max(2*len(s.slots), gramSetMinSlots)
	old := s.slots
	s.slots = make([]uint64, size)
	s.shift = uint(64 - bits.TrailingZeros(uint(size)))
	mask := uint64(size - 1)
	for _, g := range old {
		if g == 0 {
			continue
		}
		i := s.slot(g)
		for s.slots[i] != 0 {
			i = (i + 1) & mask
		}
		s.slots[i] = g
	}
}

// all yields every gram in s, in no particular order.
func (s *gramSet) all() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		if s.hasZero && !yield(0) {
			return
		}
		for _, g := range s.slots {
			if g != 0 && !yield(g) {
				return
			}
		}
	}
}
