package index_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/nathabonfim59/parkana/internal/index"
)

// TestBenchFixtureDeterministic pins the benchmark fixture identity: the same
// version and seed must regenerate identical lines and the documented raw-byte
// total. It also logs the counts each benchmark run prints.
func TestBenchFixtureDeterministic(t *testing.T) {
	first, err := benchCorpora()
	if err != nil {
		t.Fatal(err)
	}
	second, err := benchCorpora()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != len(second) {
		t.Fatalf("corpus count changed: %d then %d", len(first), len(second))
	}
	for i := range first {
		a, b := first[i], second[i]
		if a.name != b.name {
			t.Errorf("corpus %d name = %q, want %q", i, b.name, a.name)
		}
		if a.rawBytes != b.rawBytes {
			t.Errorf("%s raw bytes = %d, want %d", a.name, b.rawBytes, a.rawBytes)
		}
		if !slices.Equal(a.lines, b.lines) {
			t.Errorf("%s lines are not deterministic for fixture version %d seed 0x%x",
				a.name, benchFixtureVersion, benchFixtureSeed)
		}
		if a.rawBytes < benchCorpusRawBytes {
			t.Errorf("%s raw bytes = %d, want at least %d", a.name, a.rawBytes, benchCorpusRawBytes)
		}
		t.Logf("fixture version=%d seed=0x%x corpus=%s lines=%d raw_bytes=%d",
			benchFixtureVersion, benchFixtureSeed, a.name, len(a.lines), a.rawBytes)
	}
}

// TestBenchNeedleProbesAbsent verifies the false-positive methodology on its
// own: every fixed probe has its declared byte length, is unique within its
// length class, and appears in no corpus line by exact substring search.
func TestBenchNeedleProbesAbsent(t *testing.T) {
	const perLen = 64
	corpora, err := benchCorpora()
	if err != nil {
		t.Fatal(err)
	}
	for _, corpus := range corpora {
		probes, err := benchAbsentProbes(corpus, perLen)
		if err != nil {
			t.Fatal(err)
		}
		observed := make(map[byte]struct{}, 256)
		for _, b := range corpusAlphabet(corpus) {
			observed[b] = struct{}{}
		}
		for length := benchProbeMinLen; length <= benchProbeMaxLen; length++ {
			set := probes[length]
			if got := len(set.all); got != perLen {
				t.Errorf("%s: %d-byte probes = %d, want %d", corpus.name, length, got, perLen)
			}
			if got := len(set.inAlphabet); got != perLen/2 {
				t.Errorf("%s: %d-byte in-alphabet probes = %d, want %d", corpus.name, length, got, perLen/2)
			}
			if got := len(set.anyByte); got != perLen-perLen/2 {
				t.Errorf("%s: %d-byte any-byte probes = %d, want %d", corpus.name, length, got, perLen-perLen/2)
			}
			seen := make(map[string]struct{}, perLen)
			for _, needle := range set.all {
				if len(needle) != length {
					t.Errorf("%s: probe %q has %d bytes, want %d", corpus.name, needle, len(needle), length)
				}
				if _, ok := seen[needle]; ok {
					t.Errorf("%s: duplicate %d-byte probe %q", corpus.name, length, needle)
				}
				seen[needle] = struct{}{}
				if lineContains(corpus.lines, needle) {
					t.Errorf("%s: probe %q is present in the corpus", corpus.name, needle)
				}
			}
			for _, needle := range set.inAlphabet {
				for _, b := range []byte(needle) {
					if _, ok := observed[b]; !ok {
						t.Errorf("%s: in-alphabet %d-byte probe %q uses unobserved byte %#x",
							corpus.name, length, needle, b)
					}
				}
			}
		}
	}
}

// TestBenchCorpusNoFalseNegatives checks the contract against the benchmark
// corpora themselves, independent of any statistical expectation: every line,
// and every sampled byte substring of it, must never be pruned.
func TestBenchCorpusNoFalseNegatives(t *testing.T) {
	corpora, err := benchCorpora()
	if err != nil {
		t.Fatal(err)
	}
	rng := benchRNG(benchFixtureSeed ^ 0x6e66_6e) // "nfn"
	for _, corpus := range corpora {
		for _, n := range [...]int{3, 4, 5} {
			p := index.DefaultParams
			p.N = n
			filter := build(t, p, corpus.lines)
			for _, line := range corpus.lines {
				if len(line) < n {
					t.Fatalf("%s: line shorter than N: %q", corpus.name, line)
				}
				if !filter.MayContain(line) {
					t.Fatalf("%s N=%d: whole line pruned: %q", corpus.name, n, line)
				}
			}
			const samples = 2000
			for range samples {
				line := corpus.lines[rng.next()%uint64(len(corpus.lines))]
				length := 1 + int(rng.next()%uint64(min(len(line), 16)))
				pos := int(rng.next() % uint64(len(line)-length+1))
				needle := line[pos : pos+length]
				if !filter.MayContain(needle) {
					t.Fatalf("%s N=%d: false negative for %q in %q", corpus.name, n, needle, line)
				}
			}
		}
	}
}

// TestNeedlesShorterThanGram checks that a needle shorter than the gram length
// never prunes, including multibyte UTF-8 whose byte length differs from its
// rune count, and the empty needle.
func TestNeedlesShorterThanGram(t *testing.T) {
	// A bounded slice is enough: this test is about the length rule, not
	// corpus coverage.
	lines := make([]string, 64)
	for i := range lines {
		lines[i] = benchTemplateLine(i)
	}
	unicodeNeedles := []string{"é", "日", "😀"}
	for n := 1; n <= 8; n++ {
		p := index.DefaultParams
		p.N = n
		filter := build(t, p, lines)
		for length := 0; length < n; length++ {
			needle := strings.Repeat("z", length)
			if !filter.MayContain(needle) {
				t.Errorf("N=%d: %d-byte needle %q pruned, want may-contain", n, length, needle)
			}
		}
		for _, needle := range unicodeNeedles {
			if len(needle) >= n {
				continue
			}
			if !filter.MayContain(needle) {
				t.Errorf("N=%d: %d-byte UTF-8 needle %q pruned, want may-contain", n, len(needle), needle)
			}
		}
	}
}

// TestUnicodeNoFalseNegatives exercises valid multibyte UTF-8 lines, including
// byte substrings that split a rune: the filter is indexed by bytes, so those
// substrings must still be reported as possibly contained.
func TestUnicodeNoFalseNegatives(t *testing.T) {
	lines := []string{
		"héllo wörld, ça va",
		"日本語のログ行です",
		"emoji 😀 and 🚀 in a line",
		"mixed ünïcode 漢字 with ascii",
		"köln café naïve résumé",
		"привет мир логов",
	}
	for n := 1; n <= 8; n++ {
		p := index.DefaultParams
		p.N = n
		filter := build(t, p, lines)
		for _, line := range lines {
			for i := range len(line) {
				for j := i + 1; j <= len(line); j++ {
					if !filter.MayContain(line[i:j]) {
						t.Fatalf("N=%d: false negative for byte substring %q of %q", n, line[i:j], line)
					}
				}
			}
		}
	}
}

// TestCapNearAndOverRealisticData uses a bounded slice of the template corpus
// and a deliberately small MaxGrams (the exact distinct-gram count of that
// slice, about a few thousand, rather than the 2<<20 production default) so
// the near-cap success and one-over-cap failure are exercised on realistic
// lines without a large memory run.
func TestCapNearAndOverRealisticData(t *testing.T) {
	corpus, err := generateBenchCorpus("cap-subset", benchTemplateLine)
	if err != nil {
		t.Fatal(err)
	}
	const subsetBytes = 64 << 10
	var lines []string
	var raw int64
	for _, line := range corpus.lines {
		lines = append(lines, line)
		raw += int64(len(line))
		if raw >= subsetBytes {
			break
		}
	}

	p := index.DefaultParams
	uncapped, err := index.NewBuilder(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
		uncapped.Add(line)
	}
	distinct := uncapped.Grams()
	if distinct < 2 {
		t.Fatalf("subset has %d distinct grams, need at least 2", distinct)
	}
	t.Logf("bounded cap fixture: lines=%d raw_bytes=%d distinct_grams=%d (MaxGrams under test, not a tuned default)",
		len(lines), raw, distinct)

	// Near cap: exactly the distinct-gram count succeeds and keeps the
	// no-false-negative contract on every indexed line.
	atCap := p
	atCap.MaxGrams = distinct
	builder, err := index.NewBuilder(atCap)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
		builder.Add(line)
	}
	if got := builder.Grams(); got != distinct {
		t.Fatalf("at cap: Grams() = %d, want %d", got, distinct)
	}
	data, err := builder.Encode()
	if err != nil {
		t.Fatalf("at cap: Encode() = %v, want success", err)
	}
	filter, err := index.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
		if !filter.MayContain(line) {
			t.Fatalf("at cap: false negative for indexed line %q", line)
		}
	}

	// One under: the first gram that would exceed the cap fails the encode.
	overCap := p
	overCap.MaxGrams = distinct - 1
	builder, err = index.NewBuilder(overCap)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
		builder.Add(line)
	}
	if _, err := builder.Encode(); !errors.Is(err, index.ErrTooManyGrams) {
		t.Fatalf("one under cap: Encode() = %v, want ErrTooManyGrams", err)
	}
	if got := builder.Grams(); got != 0 {
		t.Errorf("one under cap: Grams() = %d, want 0 after the cap clears the set", got)
	}
}

// TestBenchmarkDiagnostics is a guard for the helpers the benchmark prints:
// the memory measurement must succeed and report a positive gram count,
// cumulative allocation and settled retained heap. It deliberately asserts no
// ordering between the two: HeapAlloc is process-global and approximate.
func TestBenchmarkDiagnostics(t *testing.T) {
	corpus, err := generateBenchCorpus("diagnostics", benchHighCardLine)
	if err != nil {
		t.Fatal(err)
	}
	corpus.lines = corpus.lines[:min(len(corpus.lines), 256)]
	mem, err := measureBuild(index.DefaultParams, corpus.lines)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("grams=%d totalAlloc=%d retained=%d", mem.grams, mem.totalAlloc, mem.retained)
	if mem.grams == 0 {
		t.Error("grams = 0")
	}
	if mem.retained == 0 {
		t.Error("retained = 0")
	}
	if mem.totalAlloc == 0 {
		t.Error("totalAlloc = 0")
	}
}

// TestBenchmarkProbeCorpusGuard keeps benchAbsentProbes honest about its
// inputs: an empty corpus cannot yield absent probes and must fail instead of
// looping or returning an empty set.
func TestBenchmarkProbeCorpusGuard(t *testing.T) {
	if _, err := benchAbsentProbes(benchCorpus{name: "empty"}, 1); err == nil {
		t.Fatal("benchAbsentProbes on an empty corpus returned no error")
	}
}
