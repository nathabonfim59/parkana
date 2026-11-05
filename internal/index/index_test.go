package index_test

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"testing"

	"github.com/nathabonfim59/parkana/internal/index"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
)

func build(t *testing.T, p index.Params, lines []string) *index.Filter {
	t.Helper()
	b, err := index.NewBuilder(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range lines {
		b.Add(l)
	}
	data, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	f, err := index.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// TestNoFalseNegatives is the contract: every substring of every indexed
// line may be contained, for every gram length.
func TestNoFalseNegatives(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	lines := append([]string{}, plantest.Lines...)
	for range 200 {
		b := make([]byte, rng.IntN(80))
		for i := range b {
			b[i] = byte(rng.IntN(256))
		}
		lines = append(lines, string(b))
	}
	for n := 1; n <= 8; n++ {
		p := index.DefaultParams
		p.N = n
		f := build(t, p, lines)
		if f.GramLen() != n {
			t.Fatalf("GramLen = %d, want %d", f.GramLen(), n)
		}
		for _, l := range lines {
			for i := range len(l) {
				for j := i + 1; j <= len(l); j++ {
					if !f.MayContain(l[i:j]) {
						t.Fatalf("n=%d: false negative for %q in %q", n, l[i:j], l)
					}
				}
			}
		}
	}
}

// TestPrunes checks that absent needles are rejected at a rate close to the
// configured false-positive rate.
func TestPrunes(t *testing.T) {
	var lines []string
	for i := range 2000 {
		lines = append(lines, fmt.Sprintf("GET /api/users/%d 200 %dms trace=%08x", i, i%97, i*7919))
	}
	f := build(t, index.DefaultParams, lines)
	fp := 0
	const probes = 2000
	for i := range probes {
		if f.MayContain(fmt.Sprintf("needle-%d-zq", i)) {
			fp++
		}
	}
	if fp > probes/20 {
		t.Fatalf("%d/%d absent needles not pruned", fp, probes)
	}
	if !f.MayContain("ab") { // shorter than N
		t.Fatal("short needle must not prune")
	}
}

func TestEmptyAndShortLines(t *testing.T) {
	f := build(t, index.DefaultParams, []string{"", "abc"})
	if f.MayContain("abcd") {
		t.Fatal("no line has 4 bytes; abcd must be pruned")
	}
	if !f.MayContain("") || !f.MayContain("abc") {
		t.Fatal("short needles must not prune")
	}
}

func TestTooManyGrams(t *testing.T) {
	p := index.DefaultParams
	p.MaxGrams = 10
	b, err := index.NewBuilder(p)
	if err != nil {
		t.Fatal(err)
	}
	b.Add("0123456789abcdefghij")
	if _, err := b.Encode(); !errors.Is(err, index.ErrTooManyGrams) {
		t.Fatalf("Encode = %v, want ErrTooManyGrams", err)
	}
}

func TestDecodeRejects(t *testing.T) {
	b, err := index.NewBuilder(index.DefaultParams)
	if err != nil {
		t.Fatal(err)
	}
	b.Add("hello world")
	good, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(f func([]byte) []byte) []byte { return f(append([]byte{}, good...)) }
	for name, data := range map[string][]byte{
		"empty":     nil,
		"truncated": good[:len(good)-5],
		"bit flip":  mutate(func(d []byte) []byte { d[len(d)/2] ^= 1; return d }),
		"trailing":  append(append([]byte{}, good...), 0),
	} {
		if _, err := index.Decode(data); !errors.Is(err, index.ErrFormat) {
			t.Errorf("%s: Decode = %v, want ErrFormat", name, err)
		}
	}
	if _, err := index.NewBuilder(index.Params{N: 9, FPRate: 0.01, MaxGrams: 1}); err == nil {
		t.Error("N=9 accepted")
	}
}

// TestDecodeRejectsBadDimensions covers sidecars whose checksum is correct but
// whose bloom dimensions are inconsistent: each must be ErrFormat ("scan the
// file"), never a filter that panics in MayContain (m == 0 divides by zero)
// or reports absent grams from bits past a short bitset.
func TestDecodeRejectsBadDimensions(t *testing.T) {
	b, err := index.NewBuilder(index.DefaultParams)
	if err != nil {
		t.Fatal(err)
	}
	b.Add("hello world")
	good, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	const bloomAt = 8 // index header length
	// resign rewrites one uint64 of the bloom header (0 m, 1 k, 2 bitset
	// length), optionally resizes the payload, and fixes the checksum.
	resign := func(field int, v uint64, payload int) []byte {
		d := append([]byte{}, good[:len(good)-4]...)
		binary.BigEndian.PutUint64(d[bloomAt+8*field:], v)
		if payload >= 0 {
			d = append(d[:bloomAt+24], make([]byte, payload)...)
		}
		return binary.BigEndian.AppendUint32(d, crc32.ChecksumIEEE(d))
	}
	m := binary.BigEndian.Uint64(good[bloomAt:])
	for name, data := range map[string][]byte{
		"zero m":           resign(0, 0, -1),
		"m beyond bits":    resign(0, m+64, -1),
		"payload mismatch": resign(2, m, 8),
		"zero k":           resign(1, 0, -1),
		"huge k":           resign(1, 1<<40, -1),
		"short bitset":     resign(2, m-64, -1),
		"huge bitset":      resign(2, 1<<62, -1),
		"no bloom header":  append(append([]byte{}, good[:bloomAt]...), crc(good[:bloomAt])...),
	} {
		f, err := index.Decode(data)
		if !errors.Is(err, index.ErrFormat) {
			t.Errorf("%s: Decode = %v, want ErrFormat", name, err)
			continue
		}
		if f != nil {
			t.Errorf("%s: Decode returned a filter with an error", name)
		}
	}
	if _, err := index.Decode(good); err != nil {
		t.Fatalf("good sidecar: %v", err)
	}
}

func crc(b []byte) []byte { return binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(b)) }
