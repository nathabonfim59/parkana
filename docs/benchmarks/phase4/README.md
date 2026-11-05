# Phase 4 benchmark evidence

Curated raw artifacts behind the phase-4 ingest-runtime measurements in
[docs/benchmarks.md](../../benchmarks.md). Every number in the report's
phase-4 tables is read from a file here or derived from two of those files by
the definitions in the main report. Table values are rounded for display; the
files keep full precision and are byte-identical copies of the run artifacts.
Do not edit, pretty-print or recompute them in place.

No credentials, secret environment files, parkbench binaries, full run
transcripts or periodic metrics bulk are published. In addition to the run
artifacts, the final-4 GB directories retain bounded derived evidence
(`verification.json`, `upload-evidence.json`, `final-result.json`,
`identity.json`, `outcome.json`, `count-proof.log`, `argv.json`);
`upload-evidence.json` keeps per-series maxima from bounded HTTP samples plus
the admission receipts, not a metrics time series; sample counts differ per
arm. The scratch paths recorded inside raw metadata (`/tmp/...`) are provenance
notes from the machine that produced the run, not portable evidence.

## Arms and provenance

| directory | preset | source | ingest runtime | Boat run |
|---|---|---|---|---|
| [`final-4gb-acceptance/`](final-4gb-acceptance/) | custom 4 GB (`--bytes 4000000000`) | `46593319414965de857407d0db6f0e79e4fd55dd`, clean | always, interval 1s, flush 4, upload 4, auto 4 GiB budget | bx_6g83s4p7, exit 0 (command 210 s, total 224 s) |
| [`final-4gb-hot/`](final-4gb-hot/) | custom 4 GB (`--bytes 4000000000`), `--streams api --instances-per-service 16` | `46593319414965de857407d0db6f0e79e4fd55dd`, clean | always, interval 1s, flush 4, upload 4, auto 4 GiB budget | bx_xasjdrqq, exit 0 (command 218 s, total 228 s) |
| [`small-foundation/`](small-foundation/) | small 2 GB | `0b3cc17bb2a2cfad1b37cab47cd3a22947d40c34`, clean | legacy source contract, known always/F1/U1; no phase-4 runtime flags | bx_hvm7kf8x, exit 0 (command 546 s, total 556 s) |
| [`small-u1/`](small-u1/) | small 2 GB | `8a523666850e4a7ec41285e7caec14a22952e7f9`, clean | always, interval 1s, flush 4, upload 1 | bx_hvm7kf8x, exit 0 |
| [`small-u2/`](small-u2/) | small 2 GB | `8a523666850e4a7ec41285e7caec14a22952e7f9`, clean | always, interval 1s, flush 4, upload 2 | bx_hvm7kf8x, exit 0 |
| [`small-u4/`](small-u4/) | small 2 GB | `8a523666850e4a7ec41285e7caec14a22952e7f9`, clean | always, interval 1s, flush 4, upload 4 | bx_hvm7kf8x, exit 0 |
| [`hot-u2/`](hot-u2/) | small 2 GB, `--streams api --instances-per-service 16` | `c8c67bd3736d6c82dfe374fe50a3ccbf3d3a737f`, clean | always, interval 1s, flush 4, upload 2 | bx_vzqvm25f, exit 0 (command 198 s, total 205 s) |
| [`full-u2/`](full-u2/) | full 25 GB | `c8c67bd3736d6c82dfe374fe50a3ccbf3d3a737f`, clean | always, flush 4, upload 2 | bx_u36k9ns5, exit 1 (command 2083 s, total 2092 s); original visibility failed |
| [`full-u4/`](full-u4/) | full 25 GB | `c8c67bd3736d6c82dfe374fe50a3ccbf3d3a737f`, clean | always, flush 4, upload 4 | bx_rvk56yz7, exit 1 (command 901 s, total 916 s); container OOMKilled after acknowledgment |

The four small arms ran sequentially on one Boat VM (`bx_hvm7kf8x`: AMD EPYC
4585PX, 8 vCPU / 16 GB, Go 1.27.3), one sample per arm, in the order u1,
foundation, u2, u4; each backend container was limited to 4 CPUs and 8 GiB.
They share one Parkana-only, corpus-v3 replay: explicit start
`2026-11-05T00:17:36.151838Z`, seed `8098146604754141185`, step `100us`,
1,250 entries per request per label set, concurrency 16, four streams
(api, checkout, search, worker) × four instances = 16 label sets and 16
active senders. All four manifests record identical corpus hash
`41adfc61c898c43b18b5bd6d5bc304a7f6200c2a66dc4d8c9e6726db890f3b0c`, content
hash `bc0830c44f62beea`, 13,129,764 acknowledged lines and 2,000,000,000 raw
line bytes; every arm produced 8 Parquet objects and 8 sidecar objects,
passed the restart/catalog count check and returned `valid=true` query
reports across 8 series. The driver was `scripts/bench-phase3.sh` at SHA-256
`9c9f68b371feb5eda0dd3be7a9d1d433946a4cc95b6cca2d1853566868d51d03`, and the
candidate arms ran the same Parkana image and parkbench client.

The final 4 GB arms ran source `4659331` with `--bytes 4000000000`
(`preset=custom`; there is no 4 GB preset and medium stays 10 GB),
`wal.sync=always`, flush 4, upload 4 and an auto-resolved 4 GiB budget on Boat
`bx_6g83s4p7` (acceptance) and `bx_xasjdrqq` (hot stress, `--streams api
--instances-per-service 16`). Both exited 0; acceptance passed all
timing/operational checks, while hot's 28.441 MB/s through missed 31 but
`operational_pass` stayed true. Restart counts were 26,196,634 and 26,554,577,
with 20 + 20 and 3 + 3 data+sidecar objects. Acceptance cgroup `memory.peak`
was 8,589,934,592 bytes, exactly the 8 GiB limit (page cache included, no
headroom claim, `oom_kill` 0); hot was 8,466,714,624 bytes with `oom_kill` 0.
Acceptance receipt source bytes were 1,771,116 to 66,958,175 B across 20
receipts, hot 123,101,930 to 540,318,525 B across 3, so no receipt approached the
2 GiB merge cap; at-cap admission is covered only by unit/profile tests. These
are the current evidence, but neither arm changes a default.

The hot arm is a different workload and a different binary: source `c8c67bd`
on Boat `bx_vzqvm25f` (AMD EPYC 9455, 8 vCPU / 16 GB), one `api` stream with
16 instances (still 16 label sets and 16 senders), 13,309,066 lines,
2 Parquet objects and 2 sidecars, restart count verified, query `valid`.
It is retained for the single-stream observation and is not a controlled arm
of the sweep.

The full-preset arms ran the same corpus-v3 replay as the phase-3 full run
(start `2026-11-03T00:27:57.30252Z`, same seed and step, four streams × four
instances, identical corpus hash
`eb29949b7aa004a2f7e9c499b5818a37e5a2e88a8a340438b1875a6ab5e5d32e`) from clean
`c8c67bd` source on separate Boat large VMs, and neither completed its
original measurement contract. The u2 arm (bx_u36k9ns5, AMD EPYC 4585PX)
acknowledged all rows, then its sampled all-upload-rows counter first read
complete 443.722 s after acknowledgment and repeated exact-count attempts
timed out at the 120 s query budget, so visibility, the query workload and
the restart check never completed. Its directory also retains the authorized
diagnostics recorded after the failure (`diagnostic-count.json` and its
response, `diagnostic-preflight.json`, `diagnostic-resources-pre/post.json`,
`publication-query-observation.json`, `catalog-files.json`): the diagnostic
count returned the exact 163,054,456 over HTTP 200 in 165.521 s outside the
original budget and is not the original visibility timing, and the cgroup
high-water (8,589,938,688 bytes, `oom_kill` 0; the pre- and post-diagnostic
reads are identical) covers the failed run plus prior count attempts, not
pure ingest. `tail-analysis.json` holds the retained stage evidence: the
sidecar build wall sum grew 204.546 → 456.447 s (+251.901 s over 14 builds)
between the ack-adjacent and all-complete samples while process CPU rose
955.410 s in the same window; the sums overlap across jobs, and merge time,
per-object Put time, concurrent job counts and per-job source bytes were not
recorded, so no per-stage split is estimated. The u4 arm (bx_rvk56yz7, AMD
Ryzen 9 9950X) acknowledged all rows, then the container was OOMKilled
(exit 137) during post-acknowledgment upload work; its `memory.peak` is
recorded as unavailable, not as the configured limit.

Each completed arm holds `ingest-parkana.json`, `query-parkana.json`,
`summary.md`, `timings.tsv`, `storage-parkana.txt`, `runtime-parkana.txt`,
`index-build-parkana.txt`, `run-metadata.txt` and the non-secret Compose
snapshots under `config/` (`bench.yaml`, `ingest-runtime.yaml`,
`loki-bench-config.yaml`; credentials are interpolated at run time and never
recorded). Failed arms keep only what they produced: `full-u4/` holds
`ingest-parkana.json`, `run-metadata.txt`, `runtime-parkana.txt`,
`failure-state.json` and `config/`; `full-u2/` holds the same ingest/runtime
set plus `full-run-outcome.json`, `publication-query-observation.json`,
`tail-analysis.json`, `catalog-files.json` and the bounded diagnostic files
(`diagnostic-count.json`, `diagnostic-count-response.json`,
`diagnostic-preflight.json`, `diagnostic-resources-pre.json`,
`diagnostic-resources-post.json`). Periodic metrics bulk, run transcripts and
tail-job observations are not retained.

## Comparison boundaries

- The four small arms are one ordered sweep on one machine: single
  observations with no confidence interval, small enough that scheduler and
  fsync noise dominate. They do not select a default.
- The hot arm differs in source, workload and VM; do not compare its rates
  with the four-arm sweep as if it were the same experiment.
- Loki numbers in the phase-3 sections are a **historical** baseline from a
  different run, machine state and Loki co-located methodology. The phase-4
  arms are Parkana-only; no Loki backend ran alongside them, and no phase-4
  row should be read as a fresh side-by-side Loki comparison.
- Full 25 GB rows come only from the retained evidence here; fields the
  failed arms never produced stay marked unavailable. Do not extrapolate
  them from the 2 GB observations.
- Both full arms failed their original visibility contract in different
  ways: the u4 OOM kill and the u2 publication overrun plus 120 s count
  timeouts are single observations under one 4 CPU / 8 GiB container each,
  on different VMs. The full-u2 diagnostic count (165.521 s) ran outside the
  original budget and is not an original visibility or query timing. Both
  25 GB rows stay FAILED historical results.
- The final 4 GB rates are for 4,000,000,000 raw line bytes, not 25 GB and not
  10 GB, so the historical 25 GB baseline is a size caveat, not a 1:1 row.
- Neither the 25 GB failures nor the 4 GB final arms select a default; the
  product upload-concurrency default stays 4 and `wal.sync` stays `always`.
