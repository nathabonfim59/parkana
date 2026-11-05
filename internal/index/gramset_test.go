package index

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math/rand/v2"
	"testing"

	"github.com/bits-and-blooms/bloom/v3"
)

// gramSetLines returns deterministic lines that exercise the set: NUL runs
// (the zero gram), a small alphabet (many repeats), full-range bytes (many
// distinct grams) and lines shorter than every N.
func gramSetLines(seed uint64) []string {
	r := rand.New(rand.NewPCG(seed, 0x5eed))
	lines := []string{"", "a", "\x00\x00\x00\x00\x00\x00\x00\x00\x00", "abcabcabcabc"}
	for range 300 {
		n := r.IntN(200)
		b := make([]byte, n)
		for i := range b {
			switch r.IntN(4) {
			case 0:
				b[i] = 0
			case 1:
				b[i] = byte('a' + r.IntN(3))
			default:
				b[i] = byte(r.IntN(256))
			}
		}
		lines = append(lines, string(b))
	}
	return lines
}

// mapGrams is the reference the old Builder used: every gram in a Go map.
func mapGrams(lines []string, n int) map[uint64]struct{} {
	m := make(map[uint64]struct{})
	for _, l := range lines {
		for g := range grams(l, n) {
			m[g] = struct{}{}
		}
	}
	return m
}

func TestBuilderGramsMatchMapReference(t *testing.T) {
	lines := gramSetLines(1)
	for n := 1; n <= 8; n++ {
		b, err := NewBuilder(Params{N: n, FPRate: 0.01, MaxGrams: 1 << 30})
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range lines {
			b.Add(l)
		}
		want := mapGrams(lines, n)
		if b.Grams() != len(want) {
			t.Fatalf("N=%d: Grams = %d, want %d", n, b.Grams(), len(want))
		}
		got := make(map[uint64]struct{}, len(want))
		for g := range b.set.all() {
			if _, dup := got[g]; dup {
				t.Fatalf("N=%d: gram %#x yielded twice", n, g)
			}
			got[g] = struct{}{}
			if _, ok := want[g]; !ok {
				t.Fatalf("N=%d: gram %#x not in the reference", n, g)
			}
		}
		if len(got) != len(want) {
			t.Fatalf("N=%d: all yielded %d grams, want %d", n, len(got), len(want))
		}
	}
}

// TestBuilderEncodeMatchesMapReference proves the sidecar bytes are exactly
// what the map-based Builder produced: bloom bits do not depend on insertion
// order, so the same gram set must give byte-identical output.
func TestBuilderEncodeMatchesMapReference(t *testing.T) {
	lines := gramSetLines(2)
	for _, n := range []int{3, 4, 5, 8} {
		p := Params{N: n, FPRate: 0.01, MaxGrams: 1 << 30}
		b, err := NewBuilder(p)
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range lines {
			b.Add(l)
		}
		got, err := b.Encode()
		if err != nil {
			t.Fatal(err)
		}
		ref := mapGrams(lines, n)
		bf := bloom.NewWithEstimates(uint(max(len(ref), 1)), p.FPRate)
		var key [8]byte
		for g := range ref {
			binary.BigEndian.PutUint64(key[:], g)
			bf.Add(key[:])
		}
		var buf bytes.Buffer
		buf.Write(magic[:])
		buf.Write([]byte{Version, byte(n), 0, 0})
		if _, err := bf.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}
		want := binary.BigEndian.AppendUint32(buf.Bytes(), crc32.ChecksumIEEE(buf.Bytes()))
		if !bytes.Equal(got, want) {
			t.Fatalf("N=%d: encoded sidecar differs from the map-based reference", n)
		}
	}
}

func TestBuilderMaxGramsWithSet(t *testing.T) {
	lines := gramSetLines(3)
	distinct := len(mapGrams(lines, 4))
	for _, tc := range []struct {
		max     int
		tooMany bool
	}{
		{distinct, false},
		{distinct - 1, true},
		{1, true},
	} {
		b, err := NewBuilder(Params{N: 4, FPRate: 0.01, MaxGrams: tc.max})
		if err != nil {
			t.Fatal(err)
		}
		for _, l := range lines {
			b.Add(l)
		}
		_, err = b.Encode()
		if tc.tooMany != (err != nil) {
			t.Fatalf("MaxGrams=%d (distinct %d): Encode err = %v, want tooMany=%v", tc.max, distinct, err, tc.tooMany)
		}
		if tc.tooMany && b.Grams() != 0 {
			t.Fatalf("MaxGrams=%d: Grams = %d after overflow, want 0 (memory released)", tc.max, b.Grams())
		}
	}
}

func TestGramSetZeroAndGrowth(t *testing.T) {
	var s gramSet
	if !s.add(0) || s.add(0) {
		t.Fatal("zero gram: first add must be new, second must not")
	}
	const count = 3 * gramSetMinSlots // forces several grows
	for i := uint64(1); i <= count; i++ {
		if !s.add(i << 8) {
			t.Fatalf("gram %d reported as present", i)
		}
	}
	for i := uint64(1); i <= count; i++ {
		if s.add(i << 8) {
			t.Fatalf("gram %d lost after growth", i)
		}
	}
	if s.len() != count+1 {
		t.Fatalf("len = %d, want %d", s.len(), count+1)
	}
	if 2*s.n > len(s.slots) {
		t.Fatalf("load %d/%d above one half", s.n, len(s.slots))
	}
}
