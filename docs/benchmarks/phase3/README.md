# Phase 3 benchmark evidence and method

This directory holds the raw evidence behind [docs/benchmarks.md](../../benchmarks.md)
and the detailed method, configuration deltas and interpretation that the main
report only summarizes. Every published number is traceable to a raw file here
or to a simple ratio of two values in those files; the main report stays short
and does not repeat this page's tables.

Raw files are byte-identical copies of the run artifacts. Do not edit them,
pretty-print them or recompute their values in place. Derived tables in this
README or in the main report are always labelled as derived, and their source
files are linked. [`manifest.json`](manifest.json) records the relative path,
byte count and SHA-256 of every raw evidence file. Authored pages (`README.md`)
and the manifest are not listed in the manifest itself.

No credentials, secret environment files, parkbench binaries or full run
transcripts are published. The scratch paths recorded inside raw metadata
(`/tmp/...`) are provenance notes from the machine that produced the run; they
are not part of the portable evidence.

## Runs and provenance

| experiment | folder | what it measures |
|---|---|---|
| historical small preset | [`small-2gb-v1-cacheoff/`](small-2gb-v1-cacheoff/) | 2,000,000,000-byte dual-backend ingest, query and storage on corpus v1 with the footer cache off |
| full preset | `full-25gb-v3-cacheon/` | 25,000,000,000-byte dual-backend ingest, query and storage on corpus v3 with the footer cache on |
| search-index build matrix | [`index-matrix/`](index-matrix/) | sidecar build cost, retained memory, sidecar size and whole-needle false positives for gram lengths 3, 4 and 5 |
| footer cache experiment | [`footer-cache/`](footer-cache/) | Parquet footer metadata cache on and off in the DuckDB query path |
| sender parallelism experiment | [`sender-parallelism-v3/`](sender-parallelism-v3/) | matched four-sender and sixteen-sender Parkana-only ingest on corpus v3 |

### Machine and metadata provenance

Machine labels are taken from the strongest available record, not from a
default guessed at run time.

| run | provider record | measured record | label used |
|---|---|---|---|
| historical small | [`boat-shape.json`](small-2gb-v1-cacheoff/boat-shape.json) | [`run-metadata.txt`](small-2gb-v1-cacheoff/run-metadata.txt) | Boat **large**, 8 vCPU / 16 GB, AMD Ryzen 9 9950X, 8 logical CPUs, 16,768,798,720 bytes RAM |
| full | [`full-boat-shape.json`](full-25gb-v3-cacheon/full-boat-shape.json), [`full-runtime-identity.json`](full-25gb-v3-cacheon/full-runtime-identity.json) | [`run-metadata.txt`](full-25gb-v3-cacheon/run-metadata.txt) | Boat **explicit large**, 8 vCPU / 16 GB, AMD EPYC 4585PX, 16,768,802,816 bytes RAM, Go 1.27.3 |
| search-index matrix | not separately captured | microbenchmark output and run header | snapshot-derived Boat VM, AMD Ryzen 9 9950X, 8 logical CPUs, 16,375,780 KiB `MemTotal`, Go 1.27.3; the provider type was not recorded separately |
| footer cache | not separately captured | summary's environment block | AMD Ryzen 7 9700X, 8 cores, `GOMAXPROCS=8`, Go 1.27.3; RAM not queried on the benchmark VM |
| sender parallelism | not separately captured | [`v3-4/run-metadata.txt`](sender-parallelism-v3/v3-4/run-metadata.txt), [`v3-16/run-metadata.txt`](sender-parallelism-v3/v3-16/run-metadata.txt) | Boat large, AMD Ryzen 7 9700X, 8 vCPU / 16 GB, Go 1.27.3 |

The historical small run's wrapper inherited the base snapshot shape. An
earlier note called the request unspecified, but the provider API record
`boat info bx_tcuxmj9w --json` identifies the actual VM as type `large`,
8 vCPU, 16 GB. `boat-shape.json` preserves that record (id, type, vCPU, memory,
creation time); it contains no creator, network or login fields. All measured
CPU, memory and performance values are unchanged by this correction. The
search-index matrix has no equivalent provider record, so it is described as
snapshot-derived with its measured CPU and memory rather than asserted to be a
default type.

The ambient Go version in `run-metadata.txt` is the launcher shell's toolchain.
The effective compiler is recorded separately: the actual parkbench executable
for the historical small run reported `go1.27.3` through `go version -m`
([`parkbench-buildinfo.json`](small-2gb-v1-cacheoff/parkbench-buildinfo.json)),
and every image pin also builds with Go 1.27.3.

## Definitions

**Raw line bytes.** `sum(len(line))` over emitted log lines: no labels,
timestamps, structured metadata, JSON or protobuf framing, and no compression.
It matches `parkbench ingest`'s `ack_raw_line_bytes` and `plan.Bucket.Bytes`.
Preset sizes are decimal bytes, not GiB.

**Acknowledgment rate.** `raw_line_bytes / elapsed_seconds` from the
`parkbench ingest` report, whose interval runs from generation and first setup
through the last HTTP 204. `ack_wall_s` is the script-observed wall time for
the same process. It is not a durable-commit time for every backend; see
[Durability](#durability) below.

**Through-visibility rate.** `raw_line_bytes / (ack_report_s +
queryable_delay_s)`. The queryable delay runs from the clean ingest exit until
an exact `count_over_time` equals `ack_lines`. It includes the backend's own
flush/commit/upload/merge path, so it is not an isolated upload measurement.

**Query latency.** Each class and window is one series. The query tool performs
two warmups and five timed samples per series and reports nearest-rank p50 and
p99; with five samples p99 is the maximum. A p99 here is not a tail
distribution and has no confidence interval. The eight series are four needle
classes over two windows, for 80 timed responses across both backends.

**Candidate bytes versus logical bytes.** The Parkana query tool reports
candidate compressed catalog file bytes (`parkana_query_bytes_scanned_total`):
catalog Parquet bytes submitted to a scan attempt, including retries. Cache or
object-fetch bytes are separate and are reported only where the query report
measures them. Loki reports `stats.summary.totalBytesProcessed`, a logical byte
count that includes structured metadata. None of these three is physical disk
or network I/O, and none is comparable to another by definition.

**Storage bytes.** Three definitions are never interchanged:

1. Data object bytes: Parkana's live Parquet objects, `sum(bytes)` over
   non-deleted catalog files; for Loki, the exact Garage bucket object bytes.
2. Sidecar object bytes: exact Parkana Garage bucket bytes minus the catalog's
   exact Parquet bytes. `parkana_index_bytes_total` is a separate cumulative
   encoded-sidecar-byte counter from builds, never resident storage. Two ratios
   are recorded: sidecar/compressed Parquet and sidecar/raw line bytes.
3. Local workdir bytes: WAL, object cache and SQLite catalog for Parkana; WAL,
   local index, index cache and compactor state for Loki. Local components are
   excluded from object totals. Loki's `/loki` volume is read through a
   one-shot tool image because the pinned Loki image is distroless. A missing
   measurement is a failure, not a zero.

**Sidecar build duration.** The histogram
`parkana_index_build_duration_seconds{source="ingest"}` is reported as sum,
count, mean and cumulative buckets. Its timer starts immediately around
`storage.BuildSidecar`, after reader creation. That function rereads and
decodes the finalized Parquet file and then constructs and encodes the bloom,
so the duration is the whole read/build operation, not pure gram hashing.
Builds are asynchronous and their observation interval extends past
acknowledgments. Any ratio against acknowledgment, or against acknowledgment
plus visibility, is a normalized cumulative wall-duration ratio. It is not a
CPU share and not a causal estimate of ingest overhead.

## Durability

Parkana completes WAL receipt waiters only after `syncFile` on the tenant's WAL
group, and HTTP admission waits for that receipt before returning 204
(`internal/wal/group.go`, `internal/ingest/service.go`). This covers file
synchronization and the directory-entry durability contract.

Inference from pinned source: Loki v3.7.9 `Push` calls its WAL wrapper
`Log` before returning, and that wrapper calls Prometheus `wlog.Log`. The
Prometheus dependency pinned in Loki's `go.mod` is
`v0.312.1-0.20260612131846-2ad3a8717015`. Its normal `Log` path writes pages
through `segment.Write` without a per-push `Sync`; segment rollover schedules a
sync of the previous segment and `Close` synchronizes the current one. An
acknowledgment after WAL logging therefore does not establish the same
machine-crash guarantee as the Parkana 204. This is an inference from the
pinned call paths, not a claim that Loki never fsyncs or lacks process-crash
replay. The benchmark stack uses the default-enabled Loki WAL and
`replication_factor: 1`.

Primary sources:

- [Loki `Push`](https://github.com/grafana/loki/blob/v3.7.9/pkg/ingester/instance.go#L231-L279)
- [Loki WAL wrapper](https://github.com/grafana/loki/blob/v3.7.9/pkg/ingester/wal.go#L118-L151)
- [Pinned Prometheus version in Loki `go.mod`](https://github.com/grafana/loki/blob/v3.7.9/go.mod#L87)
- [Prometheus page write](https://github.com/prometheus/prometheus/blob/2ad3a8717015/tsdb/wlog/wlog.go#L583-L610)
- [Prometheus `Log`](https://github.com/prometheus/prometheus/blob/2ad3a8717015/tsdb/wlog/wlog.go#L657-L779)
- [Prometheus rollover sync](https://github.com/prometheus/prometheus/blob/2ad3a8717015/tsdb/wlog/wlog.go#L530-L565)

The durability difference matters when comparing acknowledgment rates: the two
backends' measured 204 rates represent different crash guarantees.

## Historical small preset

Folder: [`small-2gb-v1-cacheoff/`](small-2gb-v1-cacheoff/). Source
`f24d4dfdde090da8072eda8cf33a8836315212cd`, clean (`source_dirty=0`). The
source predates both the footer metadata cache and corpus v3. It used corpus
v1: every present marker class landed in the `api` stream, because records map
to streams by index modulo 4 and the marker strides are 8, 16 and 32. Both
backends acknowledged exactly 2,000,000,000 raw line bytes and 13,129,797
lines. Their manifests match on corpus hash, content hash, start, raw line
bytes, lines, acknowledged lines and acknowledged raw line bytes; delivery is
clean on both. Both query reports are `valid=true`, all 80 timed responses
returned the exact expected counts, and there were no failures or timeouts.

The actual Boat machine is type `large`, 8 vCPU / 16 GB
([`boat-shape.json`](small-2gb-v1-cacheoff/boat-shape.json)). Per-backend
containers were limited to 4 CPUs and 8 GiB. Docker was 29.7.0 and Compose
5.3.1. The Parkana image was built from the repository Dockerfile with
`golang:1.27.3-bookworm` and `debian:bookworm-slim`. Exact hashes and the
compiler split are in
[`run-metadata.txt`](small-2gb-v1-cacheoff/run-metadata.txt),
[`image-identities.jsonl`](small-2gb-v1-cacheoff/image-identities.jsonl) and
[`parkbench-buildinfo.json`](small-2gb-v1-cacheoff/parkbench-buildinfo.json).

### Ingest

[`timings.tsv`](small-2gb-v1-cacheoff/timings.tsv),
[`ingest-parkana.json`](small-2gb-v1-cacheoff/ingest-parkana.json) and
[`ingest-loki.json`](small-2gb-v1-cacheoff/ingest-loki.json):

| backend | ack report s | ack wall s | ack MB/s | fraction of 31 MB/s | additional visibility s | raw MB/s through visibility |
|---|---:|---:|---:|---:|---:|---:|
| Parkana | 91.814634001 | 91.825 | 21.783 | 70.27% | 111.766 | 9.824 |
| Loki | 61.775898547 | 61.790 | 32.375 | 104.44% | 12.399 | 26.963 |

| backend | acknowledged lines/s | generation s | generation MB/s | generation % of acknowledgment |
|---|---:|---:|---:|---:|
| Parkana | 143003.31 | 15.679587756 | 127.554 | 17.077% |
| Loki | 212539.15 | 18.139701934 | 110.255 | 29.364% |

Generation runs inside `ack_report_s` while the backend consumes the stream; it
is not an isolated generator rate. No no-backend generation baseline was
measured, so the generation share is not a causal bottleneck estimate. Loki's
explicit flush barrier took 7.464 s and is included in its visibility interval.
The Parkana barrier includes upload, sidecar build and catalog/query work.

### Query

The short window is 19.658 s and the long window is 1,312.980 s
(21m52.980s). The cache state was warmed by correctness barriers and query
warmups; every timed Parkana sample fetched zero object bytes and zero sidecar
bytes and had zero downloads. The footer metadata cache was off.

| class | window | exact count | Parkana p50/p99 ms | Loki p50/p99 ms | Parkana scanned/index-pruned/label-pruned | Parkana candidate compressed bytes | Loki logical bytes processed |
|---|---|---:|---:|---:|---:|---:|---:|
| frequent | short | 24573 | 37.07 / 43.67 | 86.48 / 89.87 | 1 / 3 / 0 | 15846781 | 36973403 |
| frequent | long | 1641225 | 719.11 / 912.20 | 2142.51 / 2924.86 | 4 / 12 / 0 | 103035492 | 2393893910 |
| rare | short | 4096 | 13.42 / 15.50 | 85.80 / 90.65 | 1 / 3 / 0 | 15846781 | 37173472 |
| rare | long | 12288 | 34.49 / 36.02 | 2013.91 / 2192.61 | 1 / 15 / 0 | 15846781 | 2393893910 |
| absent | short | 0 | 0.71 / 0.96 | 79.17 / 80.64 | 0 / 4 / 0 | 0 | 37173472 |
| absent | long | 0 | 0.66 / 0.73 | 1932.51 / 2114.60 | 0 / 16 / 0 | 0 | 2393893910 |
| short_4byte | short | 2048 | 14.75 / 16.86 | 71.43 / 79.58 | 1 / 3 / 0 | 15846781 | 37173472 |
| short_4byte | long | 4096 | 32.09 / 33.46 | 1984.94 / 2105.14 | 1 / 15 / 0 | 15846781 | 2393893910 |

Raw reports: [`query-parkana.json`](small-2gb-v1-cacheoff/query-parkana.json)
and [`query-loki.json`](small-2gb-v1-cacheoff/query-loki.json). Every observed
count equals its expected count. The candidate-byte and Loki logical-byte
columns are deliberately separate units.

### Storage

[`storage-parkana.txt`](small-2gb-v1-cacheoff/storage-parkana.txt),
[`storage-loki.txt`](small-2gb-v1-cacheoff/storage-loki.txt) and
[`storage-loki-post-query.txt`](small-2gb-v1-cacheoff/storage-loki-post-query.txt):

| item | bytes / objects | definition |
|---|---:|---|
| Parkana compressed Parquet | 402562973 / 16 files | live catalog Parquet objects |
| Parkana sidecars | 1783024 / 16 objects | exact Garage bytes minus catalog Parquet |
| Parkana sidecar / compressed Parquet | 0.442918032603% | sidecar overhead on compressed data |
| Parkana sidecar / raw line bytes | 0.0891512% | sidecar overhead on raw lines |
| Parkana total object storage | 404345997 | Parquet plus sidecars |
| Parkana compressed Parquet / raw | 20.12814865% | data compression ratio |
| Parkana local workdir | 496030071 | WAL, cache and SQLite catalog |
| Loki object store before query | 443772152 / 347 objects | Garage `GetBucketInfo` |
| Loki object store after query | 443778689 / 348 objects | delta +6537 bytes |
| Loki object store / raw | 22.1886076% | before query |
| Loki local workdir before query | 2098508509 | WAL, index, index cache, compactor |
| Loki local WAL before / after | 2098423844 / 2098434048 | local retained state |
| Loki local index before / after | 55993 / 57282 | shipping state unknown |

The sidecar write-path counter also reports 1,783,024 bytes and matches stored
sidecar objects. Loki's post-query snapshot grew by one object and 6,537
bytes. The local index counts are the sum of active and retained index bytes
and do not establish whether those bytes shipped; no index upload barrier is
claimed, so the pre-query object total may omit TSDB index still local.

### Sidecar builds

The ingest source reported 16 observations, one per Parquet file, in
[`index-build-parkana.txt`](small-2gb-v1-cacheoff/index-build-parkana.txt):

| cumulative upper bound, s | cumulative count |
|---:|---:|
| 0.005 | 0 |
| 0.02 | 0 |
| 0.08 | 3 |
| 0.32 | 4 |
| 1.28 | 7 |
| 5.12 | 12 |
| 20.48 | 16 |
| 81.92 | 16 |
| +Inf | 16 |

Sum 47.478094748 s, count 16, mean 2.967381 s. Sum divided by acknowledgment
elapsed is 51.7108%; sum divided by acknowledgment plus visibility is
23.3215%. These are normalized cumulative wall-duration ratios, not CPU shares
and not causal overhead estimates.

### Limits of the historical small run

* Marker placement correlates every present class with `api`. Frequent-marker
  pruning of other streams is a property of corpus v1 and does not establish
  that frequent markers prune across all streams. The historical client has
  four label sets, so configured concurrency 16 permits at most four active
  pushes; its 5,000-line batch was a global generator chunk split across sets,
  while corpus v3 sends 1,250 entries per label set.
* Parkana acknowledged 21.783 MB/s, below the 31 MB/s target, and exact
  visibility was another 111.766 s later; through-visibility was 9.824 MB/s.
* Loki's `totalBytesProcessed` is a logical count including metadata, not
  compressed storage and not physical I/O.
* The Loki index shipping barrier was not established.
* The run used footer-cache-off source `f24d4df`; the production cache-on change
  landed later and is not measured here.
* One run, one machine, one deterministic corpus, sequential backends: no
  run-to-run variance or confidence interval. Query p99 is the maximum of five
  samples, the host page cache was not controlled, and the first observation is
  reported separately rather than claimed cold.
* The launcher Go version and the compiler version differ; both are recorded.

## Full preset

Folder: [`full-25gb-v3-cacheon/`](full-25gb-v3-cacheon/). Source
`fe7a39f2a65784e57acba0eb5c6177b33e2d9ec4`, clean (`source_dirty=0`). Target
25,000,000,000 raw line bytes, corpus v3 with sixteen default senders, 1,250
target entries per request per label set, footer cache on and the final write
path. The provider record identifies the actual VM as type `large`, 8 vCPU,
16 GB ([`full-boat-shape.json`](full-25gb-v3-cacheon/full-boat-shape.json));
[`run-metadata.txt`](full-25gb-v3-cacheon/run-metadata.txt) records AMD EPYC
4585PX, 8 logical CPUs, 16,768,802,816 bytes RAM, Go 1.27.3, Docker 29.7.0 and
Compose 5.3.1. Each backend container was limited to 4 CPUs and 8 GiB. The run
was attempted once; there was no rerun and the VM no longer exists. The raw
artifacts are immutable copies.

[`full-build-identities.json`](full-25gb-v3-cacheon/full-build-identities.json)
records the effective parkbench binary (`go1.27.3`, SHA-256
`d7363cb4979efbf9c89c2be6955a4b9d67e80ca350ca684817f36fc6f4f7136b`) and the
image identities. [`full-runtime-identity.json`](full-25gb-v3-cacheon/full-runtime-identity.json)
records the start time, CPU model and flags, `/proc/meminfo`, Go version and
disk capacity captured on the VM.

Both manifests are fully equal on corpus hash
`eb29949b7aa004a2f7e9c499b5818a37e5a2e88a8a340438b1875a6ab5e5d32e`,
content hash `45bea2818eaf36a5`, 25,000,000,000 raw line bytes and
163,054,456 lines. Delivery is clean on both backends: 130,448 successful
requests, zero failed, zero retried, zero ambiguous. The measured `batching`
blocks are identical: `entries_per_request` 1250, `push_requests` 130448,
`attempted_entries` 163054456, `min_entries_per_request` 880,
`max_entries_per_request` 1250, `mean_entries_per_request` 1249.9575003066357.
The measured `parallelism` blocks are identical: `label_sets` 16,
`active_senders` 16, `concurrency_limit` 16, `peak_in_flight_pushes` 16.

### Ingest

[`timings.tsv`](full-25gb-v3-cacheon/timings.tsv),
[`ingest-parkana.json`](full-25gb-v3-cacheon/ingest-parkana.json) and
[`ingest-loki.json`](full-25gb-v3-cacheon/ingest-loki.json):

| backend | ack report s | ack wall s | ack MB/s | fraction of 31 MB/s | additional visibility s | raw MB/s through visibility |
|---|---:|---:|---:|---:|---:|---:|
| Parkana | 579.362547572 | 579.384 | 43.151 | 139.20% | 1349.027 | 12.964 |
| Loki | 277.275735789 | 277.303 | 90.163 | 290.85% | 52.274 | 75.861 |

| backend | acknowledged lines/s | generation s | generation MB/s | generation % of acknowledgment |
|---|---:|---:|---:|---:|
| Parkana | 281437.69 | 257.666638691 | 97.025 | 44.474% |
| Loki | 588058.87 | 196.913126381 | 126.960 | 71.017% |

Generation runs inside `ack_report_s` while the backend consumes the stream.
It is not an isolated generator rate, and no no-backend generation baseline was
measured at this size. Loki's explicit flush barrier took 25.039 s and is
included in its visibility interval. Parkana's visibility interval includes
upload, sidecar build and the catalog/query barrier.

The 1,349.027 s Parkana tail is the dominant end-to-end limit: acknowledgment
is 43.151 MB/s, but only 12.964 MB/s is queryable through visibility. The tail
is the serial flush/merge/upload pipeline plus sidecar builds, not an admission
failure. Loki's tail is 52.274 s. The two backends' acknowledgment rates also
carry the durability difference described above.

### Query

The short window is 19.658 s and the long window is 16,305.446 s. All 80 timed
responses across both backends returned the exact expected counts. Every timed
Parkana sample fetched zero cache bytes, zero object bytes and zero downloads.

| class | window | exact count | Parkana p50/p99 ms | Loki p50/p99 ms | Parkana scanned/index/label-pruned | Parkana candidate compressed bytes | Loki logical bytes processed |
|---|---|---:|---:|---:|---:|---:|---:|
| frequent | short | 24573 | 32.58 / 33.67 | 78.22 / 79.86 | 4 / 0 / 0 | 151709258 | 39293690 |
| frequent | long | 20381807 | 15213.38 / 18644.97 | 22450.48 / 26033.99 | 32 / 0 / 0 | 5126663281 | 29957128721 |
| rare | short | 4096 | 23.65 / 25.29 | 83.51 / 95.64 | 4 / 0 / 0 | 151709258 | 40210544 |
| rare | long | 12288 | 166.33 / 193.38 | 20205.45 / 21043.68 | 4 / 28 / 0 | 151709258 | 29994532106 |
| absent | short | 0 | 0.39 / 0.41 | 91.29 / 97.43 | 0 / 4 / 0 | 0 | 40210544 |
| absent | long | 0 | 0.50 / 0.68 | 17063.69 / 17601.79 | 0 / 32 / 0 | 0 | 29994532106 |
| short_4byte | short | 2048 | 22.00 / 38.81 | 75.81 / 79.76 | 4 / 0 / 0 | 151709258 | 40210544 |
| short_4byte | long | 4096 | 152.34 / 158.13 | 16568.61 / 16952.37 | 4 / 28 / 0 | 151709258 | 29994532106 |

Raw reports: [`query-parkana.json`](full-25gb-v3-cacheon/query-parkana.json)
and [`query-loki.json`](full-25gb-v3-cacheon/query-loki.json).

Frequent-long is the only series that scans all 32 files and index-prunes none.
That is the corrected corpus v3 marker behavior: frequent markers occur in every
stream and every file-sized slice, so the bloom cannot exclude a file for that
needle. Rare and short 4-byte long windows scan 4 files and index-prune 28;
absent prunes all 32 with zero scanned files; label pruning is zero in every
series. Parkana candidate bytes are catalog compressed bytes submitted to a scan
attempt; Loki logical bytes include metadata. Neither is physical I/O, and they
are not comparable units.

### Storage

[`storage-parkana.txt`](full-25gb-v3-cacheon/storage-parkana.txt),
[`storage-loki.txt`](full-25gb-v3-cacheon/storage-loki.txt) and
[`storage-loki-post-query.txt`](full-25gb-v3-cacheon/storage-loki-post-query.txt):

| item | bytes / objects | definition |
|---|---:|---|
| Parkana compressed Parquet | 5126663281 / 32 files | live catalog Parquet objects |
| Parkana sidecars | 3676128 / 32 objects | exact Garage bytes minus catalog Parquet |
| Parkana sidecar / compressed Parquet | 0.0717060552% | sidecar overhead on compressed data |
| Parkana sidecar / raw line bytes | 0.014704512% | sidecar overhead on raw lines |
| Parkana total object storage | 5130339409 | Parquet plus sidecars |
| Parkana compressed Parquet / raw | 20.506653124% | data compression ratio |
| Parkana local workdir | 5248792356 | WAL, cache and SQLite catalog |
| Loki object store before query | 5754005924 / 4369 objects | Garage `GetBucketInfo` |
| Loki object store after query | 5754067094 / 4370 objects | delta +61170 bytes |
| Loki object store / raw | 23.016023696% | before query |
| Loki local workdir before query | 20387291 | WAL 20094976, index 263643, cache 12288, compactor 4096 |
| Loki local workdir after query | 907857 | WAL 700416, index 107974, cache 83083, compactor 4096 |

Before the query phase Loki's local index bytes were 263,643 and after it
107,974. These are all local active and retained index bytes; shipping state
remains unknown and no index upload barrier is claimed.

### Sidecar builds

The ingest source reported 32 observations, one per Parquet file, in
[`index-build-parkana.txt`](full-25gb-v3-cacheon/index-build-parkana.txt):

| cumulative upper bound, s | cumulative count |
|---:|---:|
| 0.005 | 0 |
| 0.02 | 0 |
| 0.08 | 0 |
| 0.32 | 0 |
| 1.28 | 0 |
| 5.12 | 6 |
| 20.48 | 27 |
| 81.92 | 32 |
| +Inf | 32 |

Sum 400.952794173 s, count 32, mean 12.529775 s. Sum divided by acknowledgment
elapsed is 69.2058532%; sum divided by acknowledgment plus visibility is
20.7921058%. These are normalized asynchronous wall-duration ratios, not CPU
shares and not causal estimates of ingest overhead.

### Limits of the full run

* The full and historical small rows differ in size, corpus, footer state,
  source and CPU hardware. Nothing here supports a causal before/after
  comparison between them.
* Parkana's 1,349.027 s visibility tail is the dominant end-to-end limit and is
  not fixed or optimized by this run.
* Frequent-long query latency is a corpus property: with frequent markers in
  every file, the index cannot prune and the query scans all 32 files.
* Loki's logical processed bytes include metadata and are not physical I/O.
* The Loki index shipping barrier was not established.
* One run, one machine, one deterministic corpus, sequential backends. There is
  no rerun, variance estimate or confidence interval; p99 is the maximum of
  five samples.
* The run's CPU and disk state were captured in the runtime identity, but no
  per-phase CPU/IO sample series is published for this run.

## Search-index build matrix

Folder: [`index-matrix/`](index-matrix/). Parsed metrics:
[`index-matrix.json`](index-matrix/index-matrix.json). The matrix used a
separate snapshot-derived Boat VM: AMD Ryzen 9 9950X, 8 logical CPUs,
`MemTotal` 16,375,780 KiB, Go 1.27.3 linux/amd64. Fixture version 1 (distinct
from the sidecar format version), seed `0x7061726b616e6102`. The template
corpus has 7,728 lines and 1,048,595 raw bytes; the high-cardinality corpus has
6,291 lines and 1,048,627 raw bytes. Three build iterations per cell. These are
microbenchmarks, not end-to-end ingest throughput.

| corpus | N | gram FP target | build ms | MB/s | B/op | allocs/op | retained MiB | distinct grams | sidecar KiB | raw overhead % | needle FP 4/5/6/7/8 bytes % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| template | 3 | 0.01 | 6.46 | 162.27 | 160890 | 41 | 0.070 | 2589 | 3.07 | 0.299 | 1.56/1.37/2.34/3.12/2.34 |
| template | 3 | 0.001 | 7.33 | 143.12 | 162554 | 41 | 0.070 | 2589 | 4.58 | 0.448 | 1.17/1.37/2.15/2.73/1.76 |
| template | 4 | 0.01 | 9.15 | 114.57 | 1287866 | 159 | 0.564 | 19576 | 22.94 | 2.240 | 0.195/1.17/2.54/2.34/1.17 |
| template | 4 | 0.001 | 8.86 | 118.32 | 1304250 | 159 | 0.564 | 19576 | 34.39 | 3.359 | 0.195/0.391/1.56/1.76/1.17 |
| template | 5 | 0.01 | 12.93 | 81.12 | 3488618 | 386 | 1.551 | 56931 | 66.65 | 6.509 | 100/0.586/1.37/1.37/1.56 |
| template | 5 | 0.001 | 13.68 | 76.68 | 3857514 | 405 | 1.568 | 56931 | 99.96 | 9.761 | 100/0/0.391/0.586/0.391 |
| highcard | 3 | 0.01 | 8.61 | 121.83 | 323386 | 59 | 0.141 | 6322 | 7.43 | 0.726 | 4.88/9.96/10.6/10.9/9.57 |
| highcard | 3 | 0.001 | 7.67 | 136.72 | 347962 | 60 | 0.141 | 6322 | 11.14 | 1.088 | 4.3/9.38/9.38/10.2/9.18 |
| highcard | 4 | 0.01 | 16.49 | 63.61 | 5162298 | 547 | 2.255 | 84697 | 99.14 | 9.681 | 0.586/7.81/7.81/7.81/7.42 |
| highcard | 4 | 0.001 | 16.98 | 61.76 | 5211450 | 547 | 2.255 | 84697 | 148.70 | 14.520 | 0/6.64/6.84/7.03/7.03 |
| highcard | 5 | 0.01 | 35.64 | 29.42 | 20595008 | 2087 | 9.020 | 301549 | 352.90 | 34.460 | 100/1.17/4.49/3.12/3.32 |
| highcard | 5 | 0.001 | 36.84 | 28.46 | 20775274 | 2087 | 9.020 | 301549 | 529.30 | 51.680 | 100/0.195/3.32/1.56/1.76 |

Each needle rate uses 512 exactly absent probes, half mutations restricted to
the observed corpus byte alphabet. Rates include structural false positives
when every gram exists without the whole needle. The JSON also retains rates
for 256 in-alphabet probes. Zero observed positives do not prove zero
population false positives; 512 samples give 0.195 percentage-point resolution.
Four-byte probes cannot prune with N=5, hence the 100% entries.

Build includes `NewBuilder`, `Add` for every line and `Encode`. Generation,
exact-absence checks and memory diagnostics are outside the timed build.
`B/op` covers the complete timed build. Retained memory is an approximate
process-global `HeapAlloc` delta after a full GC and sweep with the corpus and
builder alive; `TotalAlloc` is recorded separately and excludes `Encode` in that
diagnostic pass.

Measured decision: retain N=4, FP target 0.01 and `MaxGrams=2<<20`. N=5 raises
high-cardinality retained memory 2.255 → 9.020 MiB and sidecar overhead
9.681% → 34.460% at the 1% target while losing four-byte pruning. A 0.1% gram
target increases sidecar bytes by about 50% for a limited reduction in
structural whole-needle positives. N=3 saves memory and storage but increases
high-cardinality whole-needle positives. The bounded fixture supplies no
evidence requiring a production implementation change.

Exact command:

```sh
go test ./internal/index -run '^$' -bench '^BenchmarkMatrix$' \
  -benchtime=3x -count=1 -v -timeout=10m
```

The matrix is a build microbenchmark and must not be compared with preset
ingest throughput.

## Parquet footer cache experiment

Folder: [`footer-cache/`](footer-cache/), summary
[`footer-cache-summary.json`](footer-cache/footer-cache-summary.json). This is
a separate experiment on AMD Ryzen 7 9700X, 8 cores, `GOMAXPROCS=8`, Go
1.27.3. The immediately preceding full-check VM reported 8 CPUs and
16,768,798,720 physical bytes; benchmark-VM RAM was not separately queried and
is not asserted equal.

The reviewed final commit is `b86646b2751226705d122a83a43ac48e25b093cd`. The
tested source was `def5fc569333f19486a15af273d016f6aca44a03` plus three query
files identified by SHA-256 in the summary. The summary's `raw_log` and
`full_check_log` point to local scratch transcripts; the published summary and
its embedded samples are the evidence.

```sh
go test ./internal/query/ \
  -run 'TestParquetFooterCacheProfile|TestProfileQueryCleanupAfterCancellation|TestProfilingMetricSelection' \
  -bench '^BenchmarkParquetFooterCache$' -benchtime=2s -v -count=3
```

| shape | off median ms (range) | on median ms (range) | on change vs off | ranges overlap |
|---|---:|---:|---:|:---:|
| repeated-few | 1.233 (1.215-1.253) | 1.053 (0.973-1.066) | -14.6% | no |
| repeated-many | 4.134 (4.006-5.053) | 4.176 (3.999-4.224) | +1.0% | yes |
| repeated-schemagroups | 6.457 (5.539-6.623) | 5.378 (4.753-5.754) | -16.7% | yes |
| single-cycling-pool | 0.624 (0.620-0.631) | 0.592 (0.585-0.598) | -5.1% | no |

Each range contains three observations, not a confidence interval. The arms ran
in fixed on-then-off order with no randomization. Overlapping ranges are
inconclusive; they are not evidence of equivalence or absence of regression.
This experiment does not establish a general speedup, and its samples must not
be pooled with preset runs.

Profiles from the same command:

* Footer bytes fall from 22,226 to 0 in every repetition.
* The compiled `Select` reads 6,409 bytes cache-on versus 28,635 bytes
  cache-off, with 2 scans and 256 rows in each state. These are DuckDB local
  `total_bytes_read` values, not parkbench candidate bytes, object-download
  bytes or Loki logical bytes.
* Production `Select` query wall median is 3.872 ms (3.807-5.376) cache-on and
  5.748 ms (4.927-10.933) cache-off, with overlapping ranges. Binding medians
  are 2.137 ms cache-on and 3.046 ms cache-off; these metrics need not sum to
  query wall time.

The benchmarks use real `plantest.Encode` Parquet fixtures generated before the
timed loop, with 64 rows per file, 8 or 128 files, and one or four
promoted-label groups. The cycling shape reads one file at a time from 256
paths; it warms after the first cycle and is not an isolated first-read test.
Each scan's time window precedes all records, which isolates footer and binding
work. The host page cache was uncontrolled, external file caching was true, and
`cache_local_files` was false. Cache-on and cache-off were set and checked on
dedicated connections. Allocation counts bound allocations, not Go CPU
overhead. Query SQL and compiler logic were unchanged; the production
connection initializer did change. The profiles expose `DISTINCT`, label
reconstruction and `ORDER BY`, and no measured safe SQL alternative was
established.

## Sender parallelism experiment

Folder: [`sender-parallelism-v3/`](sender-parallelism-v3/). This is a separate
Parkana-only diagnostic, not a dual-backend preset result and not a production
write-path change.

### Design

Two arms on the same Boat large VM (`bx_5r4q5pb7`, AMD Ryzen 7 9700X, 8 vCPU /
16 GB, backend container 4 CPUs / 8 GiB, Go 1.27.3). Source
`fe7a39f2a65784e57acba0eb5c6177b33e2d9ec4` plus two reviewed benchmark files;
`source_dirty=1` records only those files, which do not change the production
server. The server image `sha256:afcfd0bd3a26d098ba47f0399f9a55f465a7e620d90f837e86311d0320f338a9`
is unchanged from `33f7b17`. The v3 client binary SHA-256 is
`d7363cb4979efbf9c89c2be6955a4b9d67e80ca350ca684817f36fc6f4f7136b`. The raw
[`identity.json`](sender-parallelism-v3/identity.json) records the VM type,
server image, source delta and binary hash.

Common flags: `--preset small --parkana-only --skip-queries --cpus 4
--memory 8g --seed 8098146604754141185 --start
2026-11-04T13:23:15.266317Z --skip-build --parkbench <v3 binary>`, with explicit
source identity and fresh per-arm projects and output directories. The
four-sender arm adds `--instances-per-service 1`; the sixteen-sender arm leaves
it unset so the tool default of four applies. Neither overrides batch size,
concurrency or index parameters. Both arms used 1,250 entries per request per
label set and concurrency limit 16.

Both arms acknowledged exactly 2,000,000,000 raw line bytes and 13,129,764
lines, with zero failed, retried or ambiguous requests and zero WAL errors.
Catalog count, query count and post-restart count all matched. Corpus v3
content hash `2ec777b1919f7b0c`, per-service totals, templates, marker totals
and ranges, and timestamps are identical. Corpus identity hashes differ because
the instance count is part of identity. The `instance` label is present in
both arms: one value per service in the control, four in the default arm.

### Results

| actual peak senders | ack MB/s | ack seconds | additional exact visibility seconds | raw MB/s through visibility |
|---:|---:|---:|---:|---:|
| 4 | 5.307477 | 376.826852 | 23.535 | 4.995481 |
| 16 | 24.045978 | 83.173994 | 169.337 | 7.920447 |

The improvement is 4.53x for acknowledgment and 1.59x through exact
visibility. The sixteen-sender arm still reaches only 77.6% of the 31 MB/s
acknowledgment target, and its 169.337-second visibility tail remains
unresolved. Raw timings:
[`v3-4/timings.tsv`](sender-parallelism-v3/v3-4/timings.tsv),
[`v3-16/timings.tsv`](sender-parallelism-v3/v3-16/timings.tsv). Raw manifests:
[`v3-4/ingest-parkana.json`](sender-parallelism-v3/v3-4/ingest-parkana.json),
[`v3-16/ingest-parkana.json`](sender-parallelism-v3/v3-16/ingest-parkana.json).

Request evidence:

| request evidence | 4 senders | 16 senders |
|---|---:|---:|
| instances per service | 1, explicit | 4, tool default |
| configured/active senders | 4/4 | 16/16 |
| concurrency limit | 16 | 16 |
| measured peak pushes | 4 | 16 |
| entries per full-label-set request target | 1250 | 1250 |
| successful requests | 10504 | 10512 |
| mean entries per request | 1249.9775 | 1249.0263 |
| min/max entries per request | 1191/1250 | 608/1250 |

Actual peak is measured until response drain/close, not inferred from the
configured concurrency limit. The small request-count and mean difference comes
from final remainders per label set.

WAL acknowledgment-barrier counters, read from the `parkana_ingest_wal_*`
series in [`v3-4/ack metrics`](sender-parallelism-v3/v3-4/ack-metrics.prom)
and [`v3-16/ack metrics`](sender-parallelism-v3/v3-16/ack-metrics.prom):

| acknowledgment-barrier counters | 4 senders | 16 senders |
|---|---:|---:|
| recorded fsync count | 5299 | 1366 |
| fsync duration sum, seconds | 371.354362 | 79.809837 |
| mean recorded fsync, ms | 70.0801 | 58.4259 |
| completed pushes per recorded fsync | 1.9823 | 7.6955 |

The counters support a WAL-fsync bottleneck and substantially better
group-commit batching with more active senders. Recorded syncs include segment
operations, so pushes/sync approximates group size. Mean fsync latency also
differs; this one ordered pair cannot assign every part of the improvement to
sender count. Treat these as relative results, not expected absolute
performance on another machine or disk.

Stored output and builds:

| stored output and builds | 4 senders | 16 senders |
|---|---:|---:|
| Parquet files / sidecar objects | 116/116 | 12/12 |
| Parquet bytes | 413227991 | 412828828 |
| sidecar bytes | 13093640 | 1379384 |
| ingest build duration count | 116 | 12 |
| ingest build duration sum, seconds | 22.735997 | 24.152590 |

Raw storage: [`v3-4/storage-parkana.txt`](sender-parallelism-v3/v3-4/storage-parkana.txt),
[`v3-16/storage-parkana.txt`](sender-parallelism-v3/v3-16/storage-parkana.txt).
Higher acknowledgment throughput changes asynchronous file formation and
leaves a larger upload backlog at the last acknowledgment. Cumulative index
durations are not CPU shares and cannot explain the full visibility tail.
Exact visibility was checked after acknowledgments and again after restart;
repeated query benchmarks and Loki were skipped.

### Interpretation

The comparison controls the sender count between the arms while holding corpus
content, request size, resources and source constant. It is one ordered pair,
not a randomized matrix: realized fsync latency and file formation differed
between the arms, so the pair cannot attribute every part of the difference to
sender count, cannot be pooled with the preset runs, and does not predict
another machine's absolute rate. The larger active-sender count improves WAL
group commit, but the resulting visibility tail shows that the file formation
and upload pipeline, not only admission, becomes the limiting stage at high
acknowledgment throughput.

## Method details for the preset runs

### Stack and isolation

`deploy/compose/bench.yaml` defines exactly:

* `garage`: the pinned Garage 2.5.0 image, built through the unchanged phase 2
  `garage.Dockerfile` and mounted with the unchanged phase 2 `garage.toml`;
* `garage-init-parkana` / `garage-init-loki`: two one-shot runs of the unchanged
  phase 2 `garage-init.sh`, each creating one fresh bucket and one fresh key on
  the same Garage instance;
* `parkana`: built from the unchanged repository `Dockerfile`;
* `loki`: pinned Loki 3.7.9 with the benchmark-only
  `deploy/compose/loki/bench-config.yaml`;
* `bench-tools`: a one-shot `tools`-profile sqlite container built from the
  unchanged `Dockerfile` `pushgen` target, used only to read the Parkana catalog
  volume (root inside the container so SQLite can open live WAL/shm,
  `sqlite3 -readonly`, no network);
* `bench-tools-loki`: the same tool image over the read-only Loki volume, used
  to measure WAL, index and cache bytes because the pinned Loki image has no
  shell or `du`.

No Alloy, Grafana, push generators or phase 2 profiles start. Grafana and
Alloy are not on the measured ingest/query path.

Each run uses a fresh Compose project name and output directory, and refuses to
run if either exists. A host lock directory prevents a second invocation from
adopting or tearing down the first caller's resources. Cleanup is
`docker compose -p <fresh-project> down -v --remove-orphans`; only the trap of
the invocation that created the project may run it. `--keep` leaves the stack
and its 0600 secrets file for debugging.

Each backend gets its own random Garage bucket and key on the same Garage
instance. Only the script-generated 0600 env file contains them; shell
variables with the same names are removed before every Compose call, and values
are never printed or copied into artifacts. The Loki config receives them
through `-config.expand-env=true`.

Both backend containers use the same `deploy.resources.limits` (default
`cpus=4`, `memory=8g`; overridable) and the same Garage and host-port settings.
Only one backend runs at a time:

1. Garage and both one-shot initializers start.
2. Parkana ingests, reaches exact visibility, is measured and stopped.
3. Loki ingests, is explicitly flushed, reaches exact visibility, is measured
   and stopped.
4. Manifests are compared.
5. Parkana restarts for its query phase, is verified, queried and stopped.
6. Loki restarts for its query phase, is verified, queried and stopped.

Parkana runs with `-roles=ingester,querier` (no compactor): its compactor runs a
cycle at process startup, which would change file counts during a query phase.
After restart only the exact count barrier is re-checked before the query tool
starts, and the runner waits for `parkana_ingest_pending_generations == 0` so no
post-restart upload is in flight. Loki keeps `target=all`, but its compactor
interval is 24h and retention is disabled. The runner waits for
`loki_ingester_flush_queue_length == 0` and captures compactor metrics before
and after the query phase so a non-idle compactor is visible.

### Exact visibility and timing barriers

The script never assumes that a successful push or HTTP 200 means the data is
queryable.

**Parkana (catalog plus query count).** `bench-tools` reads the SQLite catalog
read-only and waits until `sum(rows)` over non-deleted files equals the
manifest's `ack_lines` exactly. More is a failure, not a tolerance. It then
issues the same instant `sum(count_over_time(...))` query the query stage uses,
over `[corpus.start, corpus.through)` with a safe 1-second pad, and waits for
the count to equal `ack_lines` exactly.

**Loki (explicit flush plus query count).** The runner `POST`s `/flush` and
requires HTTP 204. Because the pinned handler only schedules flushes, it then
polls `/metrics` until `loki_ingester_flush_queue_length` is zero and
`loki_ingester_chunks_flushed_total` is stable for a sample, and finally
requires the exact `count_over_time` count. `chunks_flushed_total` is a
CounterVec, so the pre-flush labeled series may legitimately be absent on a
fresh process; the baseline is treated as unavailable and the POST still
happens. Post-flush metric absence, including transport errors, is retried with
the gauge treated as unknown, never as zero. A permanently absent post-flush
counter or queue gauge fails at timeout with diagnostics.

Four timings stay separate in `timings.tsv` and the summary:

| column | definition |
|---|---|
| `ack_report_s` | `parkbench ingest` report `elapsed_seconds` (generation and first setup through last 204) |
| `ack_wall_s` | script-observed wall time from starting `parkbench ingest` to its clean exit |
| `flush_s` | Loki only: `POST /flush` until queue drained and flushed counter stable |
| `queryable_delay_s` | from the clean ingest exit to the first exact count equal to `ack_lines` (polling adds up to 2 s) |

The post-restart count check is deliberately not part of the queryable delay;
it is a data-integrity check for duplicate WAL replay. Timeouts fail the run
with the last observed catalog, count and metric values and a redacted
`diagnostics.log`.

### Query workload

The script calls the settled `parkbench query` contract unchanged:

```sh
parkbench query \
  --backend parkana|loki \
  --url http://127.0.0.1:<parkana|loki-port> \
  --manifest <ingest-<backend>.json> \
  [--metrics-url http://127.0.0.1:<parkana-port>/metrics] \
  [--tenant ID] \
  --repetitions 5 --warmups 2 --timeout 120s \
  --classes frequent,rare,absent,short_4byte \
  --windows short,long \
  --output query-<backend>.json \
  --markdown query-<backend>.md
```

The tool validates the manifest and expected counts before timing, uses the same
queries and ranges for both backends, reports nearest-rank p50/p99, reports the
first observation separately (never claimed cold), and marks missing scan or
byte stats as unavailable instead of zero. Parkana metrics are scraped before
and after each run. Loki's `stats.summary.totalBytesProcessed` is read from its
responses. Queries run against an otherwise idle stack: the other backend is
stopped, Parkana's compactor role is absent, and Loki's compactor is idle.

`--skip-queries` runs the stack, ingest and storage stages only. It is useful as
an ingest and storage smoke independent of query measurement; it is not a
substitute for the published numbers.

### Corpus identity and markers

`cmd/parkbench ingest --output` writes the manifest that the query stage
consumes. The runner:

1. derives or accepts the corpus `start` for the first backend;
2. reads `corpus.start` from that first manifest and passes it explicitly as
   `--start` to the second backend;
3. requires the manifests' `corpus.hash`, `corpus.content_hash`,
   `corpus.start`, `raw_line_bytes`, `lines`, `ack_lines` and
   `ack_raw_line_bytes` to be identical before any query is timed;
4. rejects a manifest whose `delivery.clean` is not true.

Corpus v3 (used by the full preset and the sender-parallelism experiment)
rotates stream assignment by 32-record cohorts, so frequent markers span every
stream and file-sized slice, and rare and short markers span all four default
streams within their blocks. The historical small run used corpus v1, with the
`api`-only correlation described in its limits. Marker literals and schedules
for the measured corpus are in the ingest manifests.

### Configuration deltas from phase 2

Phase 2 files (`deploy/compose/compose.yaml`, `garage.toml`,
`garage.Dockerfile`, `garage-init.sh`, `loki/config.yaml`) and all image pins
are untouched. The benchmark stack adds benchmark-only files and changes only
these values.

Compose and backend resources:

| item | phase 2 | benchmark stack | reason |
|---|---|---|---|
| services | Garage, Parkana, Loki, Alloy, Grafana, push generators | Garage, Parkana, Loki, two one-shot Garage inits, one-shot sqlite tool, one-shot Loki-volume tool | Alloy and Grafana are outside the measured ingest/query path |
| project | `parkana-phase1` | generated `parkana-bench-<time>-<pid>` | fresh, never reused |
| backend limits | none | `cpus=4`, `memory=8g` each (overridable) | identical resources for both sequential backends |
| Garage buckets | one `parkana` bucket, fixed dev key | two random buckets and keys per run | no shared data or credentials between backends |
| Parkana image | `parkana:phase1` | `parkana-bench:phase3` | distinct local tag; same Dockerfile and base pins |
| Garage image | `parkana-garage:v2.5.0` | `parkana-bench-garage:v2.5.0` | distinct local tag; same pin |
| sqlite tool | pushgen image | `parkana-bench-tools:phase3` (same `pushgen` target) | read the catalog volume one-shot |

Parkana configuration:

| key | phase 2 / default | benchmark | reason |
|---|---|---|---|
| `PARKANA_ROLES` | `all` | `ingester,querier` | exclude the startup compactor cycle so query phases see stable files |
| `PARKANA_INGEST_FLUSH_INTERVAL` | `2s` in phase 2 | `1s` | shorter local flush so visibility delay is measured, not a long timer |
| `PARKANA_INGEST_UPLOAD_INTERVAL` | `2s` in phase 2 | `1s` | shorter upload polling |
| `PARKANA_INGEST_UPLOAD_MIN` | `1MiB` in phase 2 | `128MiB` | match the ingester's default object batching instead of the smoke-test size |
| `PARKANA_INGEST_UPLOAD_MAX_AGE` | `30s` in phase 2 | `10s` | upload-eligibility limit, not an end-to-end visibility bound; the full run's measured delay was 1,349 s |
| `PARKANA_INGEST_MEMTABLE_MAX` | engine default `256MiB` | `512MiB` | fewer seals at maximum ingest rate |
| `PARKANA_INGEST_WAL_MAX` | engine default `128MiB` | `256MiB` | fewer key rotations at maximum ingest rate |
| `PARKANA_INGEST_UPLOAD_MERGE_MAX` | engine default `2GiB` | `2GiB` | explicit value recorded; no semantic change |
| `PARKANA_QUERIER_TIMEOUT` | engine default `2m` | `10m` | the full preset count and scan must not time out |
| `PARKANA_INDEX_NGRAM` | default 4 | 4 (explicit) | pin the gram length for reproducible build metrics; the sidecar format version is separate |
| `PARKANA_INDEX_FP_RATE` | default 0.01 | 0.01 (explicit) | pin the bloom false-positive target |
| `PARKANA_INDEX_MAX_GRAMS` | default 2,097,152 | 2,097,152 (explicit) | pin the per-file sidecar cap so skips are explainable |
| sidecar options | defaults | defaults | format and `DefaultParams` are not benchmark overrides |

Loki configuration; everything not listed is copied from the phase 2 config:
`schema: v13`, TSDB index prefix `index_`, 24h period,
`allow_structured_metadata: true`, `volume_enabled: true`,
`discover_log_levels: false`, `discover_service_name: []`, the in-memory ring
and the same server ports.

| key | phase 2 | benchmark | reason |
|---|---|---|---|
| `common.storage` | `filesystem` chunks/rules dirs | absent; S3 below | Loki stores the same data on Garage as Parkana |
| `schema_config.configs[].object_store` | `filesystem` | `s3` | same bucket-backed schema version, different object store |
| `storage_config.tsdb_shipper` | not set | `/loki/index`, `/loki/index_cache` | writable staging for the S3 TSDB shipper |
| `storage_config.aws` | absent | per-run Garage bucket/key/endpoint, path-style, insecure | benchmark-only S3 backend on the same Garage |
| `limits_config.ingestion_rate_mb` | default 4 | 4096 | do not measure Loki's default global throttle |
| `limits_config.ingestion_burst_size_mb` | default 6 | 4096 | allow generator global bursts |
| `limits_config.per_stream_rate_limit` | default 3 MB/s | 64 MB/s | the historical four-label-set client would cap below 31 MB/s; the full sixteen-label-set client would not, but both are raised for headroom |
| `limits_config.per_stream_rate_limit_burst` | default 15 MB | 64 MB | allow each stream's push bursts |
| `compactor.compaction_interval` | default 10m | 24h | no background compaction during a query phase |
| `compactor.retention_enabled` | default false | false (explicit) | no retention work; recorded for clarity |

Bucket and key values arrive only through `-config.expand-env=true` and the
per-run 0600 env file.

### Verified pinned contracts

These are source-verified against the pinned releases, not assumed from mocks.

* **Garage v2.5.0 `GetBucketInfo`**:
  [src/api/admin/api.rs](https://github.com/deuxfleurs-org/garage/blob/v2.5.0/src/api/admin/api.rs#L924-L948)
  defines `GetBucketInfoResponse` with `objects: i64` and `bytes: i64`. The
  pinned CLI prints the unwrapped response JSON
  ([src/garage/cli/remote/mod.rs](https://github.com/deuxfleurs-org/garage/blob/v2.5.0/src/garage/cli/remote/mod.rs#L158-L159)).
  The runner parses exactly those integer fields
  (`garage json-api GetBucketInfo '{"search":"<bucket>"}'`).
* **Loki v3.7.9 `/flush`**:
  [pkg/loki/modules.go](https://github.com/grafana/loki/blob/v3.7.9/pkg/loki/modules.go#L779-L780)
  registers `GET` and `POST /flush`;
  [pkg/ingester/flush.go](https://github.com/grafana/loki/blob/v3.7.9/pkg/ingester/flush.go#L142-L144)
  `FlushHandler` calls `sweepUsers` and writes 204. The handler only schedules
  flushes, so the runner requires `loki_ingester_flush_queue_length == 0`, an
  advancing then stable `loki_ingester_chunks_flushed_total`, and no increase in
  `loki_ingester_chunks_flush_failures_total`
  ([pkg/ingester/metrics.go](https://github.com/grafana/loki/blob/v3.7.9/pkg/ingester/metrics.go#L248-L324)).
* **Loki ingest limits**:
  [pkg/validation/limits.go](https://github.com/grafana/loki/blob/v3.7.9/pkg/validation/limits.go#L54-L56)
  defaults are 4 MB/s global and 6 MB burst, plus 3 MB/s per stream and 15 MB
  burst. The historical four-label-set client at the per-stream default would
  cap aggregate ingest below the 31 MB/s target; the full client's sixteen label
  sets would not, but both limit pairs are raised for headroom. The benchmark
  config records the exact values in `run-metadata.txt` and the delta table.
* **Loki S3 config**:
  [docs/sources/configure/examples/configuration-examples.md](https://github.com/grafana/loki/blob/v3.7.9/docs/sources/configure/examples/configuration-examples.md)
  shows the expanded `storage_config.aws` fields (`bucketnames`, `endpoint`,
  `region`, `access_key_id`, `secret_access_key`, `insecure`, `s3forcepathstyle`)
  and `tsdb_shipper`.
* **Docker Compose project listing**:
  [docker compose ls](https://docs.docker.com/reference/cli/docker/compose/ls/)
  supports table output and `-q`/`--quiet` for names. `--format` is not assumed.
* **Phase 2 parity**: schema `v13`/`from: 2020-10-24`, TSDB `index_`/24h,
  `allow_structured_metadata`, `volume_enabled`, and the disabled discovery
  settings are asserted by an offline regression against
  `deploy/compose/loki/bench-config.yaml`.

The offline `--self-test` regressions use those response shapes and metric
families; they are not a substitute for a real stack smoke.

## Reproduction

Offline checks touch no Docker, Go or network services:

```sh
bash -n scripts/bench-phase3.sh
bash scripts/bench-phase3.sh --self-test
bash scripts/bench-phase3.sh --dry-run --preset small
```

Historical small run (exact source `f24d4df`). Check out a clean tree at
`f24d4dfdde090da8072eda8cf33a8836315212cd` first. `--source-commit` and
`--source-dirty` only label metadata; they do not select or check out code.

```sh
bash scripts/bench-phase3.sh --preset small \
  --source-commit f24d4dfdde090da8072eda8cf33a8836315212cd --source-dirty 0 \
  --seed 8098146604754141185 \
  --start 2026-11-04T13:23:15.266317Z \
  --step 100us --batch-lines 5000 --concurrency 16 \
  --ready-timeout 180 --visibility-timeout 1200 --flush-timeout 300 \
  --output-dir /tmp/parkana-bench/small-2gb-v1-cacheoff
```

This uses the recorded seed, start, step, batch size, concurrency and original
timeout budgets. The seed and start come from the accepted manifests;
`run-metadata.txt` records the original start as tool-derived. A fresh project,
output directory and hardware mean timings cannot be reproduced exactly. Corpus
identity can be reproduced: with the same seed, start, step, batch size and
concurrency, manifests should match on `corpus.hash`, `corpus.content_hash`,
raw line bytes, lines and acknowledged lines.

The recorded historical `--start` may eventually fall outside Loki's
timestamp-age admission window. Do not change Loki config to bypass the age
check. If that happens, run with a fresh start; the corpus hash will differ and
the run must be labeled separately, never presented as an identical historical
replay.

#### Full 25 GB v3 cache-on run (exact source `fe7a39f`)

Check out a clean tree at `fe7a39f2a65784e57acba0eb5c6177b33e2d9ec4` first.
`--source-commit` and `--source-dirty` only label metadata; they do not select
or check out code. The recorded corpus values are seed
`8098146604754141185`, start `2026-11-03T00:27:57.30252Z` and step `100us`. Use
the script defaults of 1,250 entries per request per label set and concurrency
16, and do not pass `--instances-per-service`, so the tool default of four
instances per service applies. Use an explicit large VM, not the inherited
snapshot shape:

```sh
tm-boat test /path/to/worktree --type large -- \
  bash scripts/bench-phase3.sh --preset full --allow-full \
    --source-commit fe7a39f2a65784e57acba0eb5c6177b33e2d9ec4 --source-dirty 0 \
    --seed 8098146604754141185 \
    --start 2026-11-03T00:27:57.30252Z \
    --step 100us \
    --output-dir /tmp/parkana-bench/full
```

The recorded start may fall outside Loki's timestamp-age admission window, as
warned for the historical small run. Use a fresh start and label the run
separately rather than changing Loki config. A fresh project, output directory
and hardware mean timings cannot be reproduced exactly even when corpus identity
matches.

Generic command for a new reviewed source:

```sh
tm-boat test /path/to/worktree -- \
  bash scripts/bench-phase3.sh --preset small \
    --source-commit <reviewed-sha> --source-dirty 0 \
    --output-dir /tmp/parkana-bench/small
```

A failed run keeps its artifacts and writes `diagnostics.log`; it never deletes
a project it did not create.

## Artifact inventory

| file | content |
|---|---|
| `run-metadata.txt` | preset and corpus settings, resources, ports, source commit/dirty, kernel, CPU model/count, RAM, Go/Docker/Compose versions, image pins, script and config hashes, effective batching/parallelism |
| `config/bench.yaml`, `config/loki-bench-config.yaml` | exact non-secret benchmark configuration used by the run |
| `timings.tsv` | acknowledgment, queryable and flush separation |
| `ingest-parkana.json`, `ingest-loki.json` | complete `parkbench ingest` manifests with clean delivery, batching and parallelism blocks |
| `storage-parkana.txt` | data bytes, Parquet file count, exact sidecar objects and bytes from the Garage bucket minus catalog data, cumulative encoded sidecar-byte counter, ratios, build histogram sum/count/mean, local workdir |
| `index-build-parkana.txt` | `parkana_index_build_duration_seconds{source="ingest"}` buckets, sum and count |
| `storage-loki.txt` | exact object-store bytes and objects, WAL/index/index-cache/compactor local bytes, explicit index-shipping-state-unknown snapshot, ratio |
| `storage-loki-post-query.txt` | post-query object-store snapshot, delta, remaining local index/WAL/cache bytes |
| `query-parkana.json/.md`, `query-loki.json/.md` | query reports including p50/p99 and scan/byte deltas |
| `loki-compactor-before-query.txt`, `loki-compactor-after-query.txt` | Loki compactor metric snapshot before and after its query phase |
| `run.log` | full transcript (not published; only a redacted `run-tail.txt` is exported, also not published here) |
| `diagnostics.log` | failure only: Compose state and bounded redacted logs |

Artifacts survive VM teardown because the runner emits a bounded
gzip+base64 tar of the small evidence files to stdout between
`=== BENCH ARTIFACT ARCHIVE BEGIN` and `=== BENCH ARTIFACT ARCHIVE END ===`.
The archive is curated: `secrets.env`, the parkbench binary and the full
`run.log` are excluded; a redacted `run-tail.txt` is included instead. The soft
size bound is 8 MiB (query JSON is dropped first if needed).

A Boat snapshot may not contain `.git`. Pass the reviewed `--source-commit`
(and `--source-dirty 0|1`) or set `PARKANA_BENCH_SOURCE_COMMIT` /
`PARKANA_BENCH_SOURCE_DIRTY`. Without either, metadata records
`source_commit=unavailable` and `source_dirty=unknown`, never a fabricated clean
tree.

Secrets are never in these artifacts; `secrets.env` is 0600 and removed on
cleanup unless `--keep`.

## Evidence maintenance

1. Copy new raw artifacts without editing their bytes.
2. Regenerate [`manifest.json`](manifest.json) over every raw file.
3. Scan all published files for credentials and private keys.
4. Verify every relative link in `docs/benchmarks.md` and this page resolves.
5. Recompute each quoted number from its raw file; do not copy from prose.
