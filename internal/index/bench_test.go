package index_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/nathabonfim59/parkana/internal/index"
)

// BenchmarkMatrix measures sidecar build cost and pruning quality for every
// gram length N in {3, 4, 5} and per-gram false-positive target in
// {0.01, 0.001}, over the two deterministic corpora from bench_fixture_test.go.
//
// Reported per sub-benchmark:
//
//   - ns/op and MB/s: build (NewBuilder + Add every line + Encode) per
//     iteration. MB/s uses raw line bytes: the sum of len(line) without
//     newline separators, the same definition as parkbench RawLineBytes.
//   - B/op and allocs/op: standard testing allocation counters for that same
//     build+encode path.
//   - build_alloc_MiB and build_retained_MiB: one separate measurement pass
//     per configuration, in a builder that is not the timed one. alloc is the
//     TotalAlloc delta (cumulative allocation, includes garbage); retained is
//     settled live HeapAlloc after a full GC and sweep with the builder alive.
//     HeapAlloc is process-global, so retained is an approximate measurement
//     and no strict ordering between the two is assumed. These are per build,
//     not per b.N; corpus generation is not part of any timed section.
//   - grams: distinct grams in the file.
//   - sidecar_KiB and sidecar_pct_raw: encoded sidecar size and its
//     percentage overhead against the raw line bytes.
//   - fp_L4_pct .. fp_L8_pct: measured whole-needle false-positive rate for
//     the fixed absent probes of each byte length, over both probe classes.
//     fp_L4_alpha_pct .. fp_L8_alpha_pct restrict that to the in-alphabet
//     half, so structural/common-gram false positives are visible. Probes were
//     verified absent by an exact substring search before the run (see
//     benchAbsentProbes). Lengths shorter than N cannot prune, so a 100% rate
//     there is expected and is the explicit N=4 versus N=5 short-needle
//     comparison.
//
// The matrix is bounded: it uses about 1 MiB per corpus and runs a fixed
// number of configurations. Suggested Boat invocation (do not run locally):
//
//	tm-boat test <worktree> -- go test ./internal/index -run '^$' \
//	    -bench '^BenchmarkMatrix$' -benchtime=3x -count=1 -v -timeout=10m
func BenchmarkMatrix(b *testing.B) {
	corpora, err := benchCorpora()
	if err != nil {
		b.Fatal(err)
	}
	for _, corpus := range corpora {
		probes, err := benchAbsentProbes(corpus, benchProbesPerLength)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(corpus.name, func(b *testing.B) {
			b.Logf("fixture_version=%d fixture_seed=0x%x lines=%d raw_bytes=%d (sum of len(line), newlines excluded)",
				benchFixtureVersion, benchFixtureSeed, len(corpus.lines), corpus.rawBytes)
			for _, n := range [...]int{3, 4, 5} {
				for _, fp := range [...]float64{0.01, 0.001} {
					b.Run(fmt.Sprintf("N%d/FP%g", n, fp), func(b *testing.B) {
						benchMatrixConfig(b, corpus, probes, n, fp)
					})
				}
			}
		})
	}
}

// benchMatrixConfig measures one (corpus, N, FPRate) cell.
func benchMatrixConfig(b *testing.B, corpus benchCorpus, probes needleSets, n int, fp float64) {
	b.Helper()
	p := index.DefaultParams
	p.N = n
	p.FPRate = fp

	mem, err := measureBuild(p, corpus.lines)
	if err != nil {
		b.Fatalf("corpus %s: %v", corpus.name, err)
	}

	var sidecar []byte
	b.ReportAllocs()
	b.SetBytes(corpus.rawBytes)
	b.ResetTimer()
	for b.Loop() {
		builder, err := index.NewBuilder(p)
		if err != nil {
			b.Fatal(err)
		}
		for _, line := range corpus.lines {
			builder.Add(line)
		}
		sidecar, err = builder.Encode()
		if err != nil {
			b.Fatalf("corpus %s exceeds MaxGrams=%d: %v", corpus.name, p.MaxGrams, err)
		}
	}
	b.StopTimer()
	runtime.KeepAlive(sidecar)

	filter, err := index.Decode(sidecar)
	if err != nil {
		b.Fatalf("decode own sidecar: %v", err)
	}
	b.ReportMetric(float64(len(sidecar))/1024, "sidecar_KiB")
	b.ReportMetric(float64(len(sidecar))*100/float64(corpus.rawBytes), "sidecar_pct_raw")
	b.ReportMetric(float64(mem.grams), "grams")
	b.ReportMetric(float64(mem.totalAlloc)/(1<<20), "build_alloc_MiB")
	b.ReportMetric(float64(mem.retained)/(1<<20), "build_retained_MiB")

	b.Logf("corpus=%s lines=%d raw_bytes=%d N=%d FPRate=%g grams=%d sidecar_bytes=%d",
		corpus.name, len(corpus.lines), corpus.rawBytes, n, fp, mem.grams, len(sidecar))
	for length := benchProbeMinLen; length <= benchProbeMaxLen; length++ {
		set := probes[length]
		positives := 0
		for _, needle := range set.all {
			if filter.MayContain(needle) {
				positives++
			}
		}
		alphaPositives := 0
		for _, needle := range set.inAlphabet {
			if filter.MayContain(needle) {
				alphaPositives++
			}
		}
		rate := float64(positives) * 100 / float64(len(set.all))
		alphaRate := float64(alphaPositives) * 100 / float64(len(set.inAlphabet))
		b.ReportMetric(rate, fmt.Sprintf("fp_L%d_pct", length))
		b.ReportMetric(alphaRate, fmt.Sprintf("fp_L%d_alpha_pct", length))
		b.Logf("length=%d absent_probes=%d in_alphabet_probes=%d false_positives=%d in_alphabet_false_positives=%d rate_pct=%.3f in_alphabet_rate_pct=%.3f",
			length, len(set.all), len(set.inAlphabet), positives, alphaPositives, rate, alphaRate)
	}
}
