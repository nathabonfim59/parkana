# Parkana phase 3 benchmarks

Parkana is measured against Loki on the same machine, with both backends
storing on the same Garage instance. The preset runs ingest a deterministic
corpus at `small` 2,000,000,000 and `full` 25,000,000,000 raw line bytes and
query it through the Loki-compatible API.

## Results at a glance

| run | backend | ack MB/s | % of 31 MB/s | visibility s | through MB/s | query p50 ms (8 series) | stored object bytes |
|---|---|---:|---:|---:|---:|---:|---:|
| [full 25 GB, corpus v3, cache on](benchmarks/phase3/full-25gb-v3-cacheon/) | Parkana | 43.151 | 139.20% | 1349.027 | 12.964 | 0.39–15213.38 | 5130339409 |
| [full 25 GB, corpus v3, cache on](benchmarks/phase3/full-25gb-v3-cacheon/) | Loki | 90.163 | 290.85% | 52.274 | 75.861 | 75.81–22450.48 | 5754005924 |
| [small 2 GB, corpus v1, cache off](benchmarks/phase3/small-2gb-v1-cacheoff/) | Parkana | 21.783 | 70.27% | 111.766 | 9.824 | 0.66–719.11 | 404345997 |
| [small 2 GB, corpus v1, cache off](benchmarks/phase3/small-2gb-v1-cacheoff/) | Loki | 32.375 | 104.44% | 12.399 | 26.963 | 71.43–2142.51 | 443772152 |

The full Parkana run acknowledges at 43.151 MB/s but needs another 1,349.027 s
before every line is exactly queryable, so through-visibility is 12.964 MB/s;
Loki's tail is 52.274 s and 75.861 MB/s. The presets differ in size, corpus,
footer state and CPU hardware, so their rows are not a controlled comparison.

Acknowledgment durability differs: Parkana returns 204 after the tenant WAL
group has synced, while Loki v3.7.9 acknowledges after its WAL wrapper calls
Prometheus `wlog.Log`, whose normal path writes pages without a per-push
`Sync` (rollover and `Close` synchronize). This is an inference from the pinned
call paths, not a claim that Loki never fsyncs or lacks process-crash replay.
The rates above therefore compare different machine-crash guarantees; detail
and [primary sources](benchmarks/phase3/README.md#durability) are in the
evidence README.

Every published number is read from a raw artifact in
[`docs/benchmarks/phase3/`](benchmarks/phase3/). That directory also holds the
full method, configuration deltas, barrier definitions and evidence
interpretation in its [README](benchmarks/phase3/README.md). Raw files are
byte-identical copies; [`manifest.json`](benchmarks/phase3/manifest.json)
records their sizes and SHA-256 values. Do not pool results across runs: they
use different source states, corpora and, for the microbenchmarks, different
hardware.

Separate experiments, each on its own source and hardware:

| experiment | headline result | evidence |
|---|---|---|
| [sender parallelism, corpus v3, 2 GB](benchmarks/phase3/sender-parallelism-v3/) | 5.307477 → 24.045978 MB/s from 4 → 16 active senders; visibility 23.535 → 169.337 s | [four-sender manifest](benchmarks/phase3/sender-parallelism-v3/v3-4/ingest-parkana.json), [sixteen-sender manifest](benchmarks/phase3/sender-parallelism-v3/v3-16/ingest-parkana.json) |
| [search-index build matrix](benchmarks/phase3/index-matrix/) | N=4 is the retained setting: 63.61–114.57 MB/s build, 0.564–2.255 MiB retained in the measured cells | [index-matrix.json](benchmarks/phase3/index-matrix/index-matrix.json) |
| [footer metadata cache](benchmarks/phase3/footer-cache/) | 1.053 vs 1.233 ms on repeated-few; no general speedup shown | [footer-cache-summary.json](benchmarks/phase3/footer-cache/footer-cache-summary.json) |

## Definitions

**Raw line bytes.** `sum(len(line))` over emitted lines with no labels,
timestamps, metadata, framing or compression. Preset sizes are decimal bytes.

**Acknowledgment rate.** `raw_line_bytes / elapsed_seconds` from the ingest
report, from generation through the last HTTP 204. It is not a uniform
durable-commit time for both backends.

**Through-visibility rate.** `raw_line_bytes / (ack_report_s +
queryable_delay_s)`; the delay ends at the first exact `count_over_time` equal
to `ack_lines`, so it is not an isolated upload time.

**Query latency.** Two warmups and five timed samples per series, nearest-rank
p50/p99 (p99 is the maximum sample); eight series, 80 timed responses across
both backends.

**Candidate, cache and logical bytes.** Parkana reports catalog compressed
candidate bytes submitted to a scan attempt (including retries), cache or
object-fetch bytes where measured, and scan/prune counts. Loki reports
`stats.summary.totalBytesProcessed`, a logical count including metadata. None
is physical I/O and they are not interchangeable.

**Storage and builds.** Data bytes are live catalog Parquet (Parkana) or exact
Garage bytes (Loki); sidecar bytes are Garage minus catalog Parquet, and the
write-path counter is cumulative encoded bytes, not resident storage. Local
workdir bytes are separate. `parkana_index_build_duration_seconds` times the
whole asynchronous `BuildSidecar` read/decode/build/encode, so any ratio
against ingest time is a normalized wall-duration ratio, not CPU or causal
overhead.

## Historical small preset: 2 GB, corpus v1, cache off

Source `f24d4dfdde090da8072eda8cf33a8836315212cd`, clean. This is the
historical accepted small run, not a current-source result. It used corpus v1,
where every present marker class landed in the `api` stream, and the footer
cache was off. The provider record identifies the actual Boat VM as type
`large`, 8 vCPU / 16 GB
([`boat-shape.json`](benchmarks/phase3/small-2gb-v1-cacheoff/boat-shape.json));
[`run-metadata.txt`](benchmarks/phase3/small-2gb-v1-cacheoff/run-metadata.txt)
records the AMD Ryzen 9 9950X CPU, 8 logical CPUs and 16,768,798,720 bytes RAM.
Each backend container was limited to 4 CPUs and 8 GiB.

Both backends acknowledged exactly 2,000,000,000 raw line bytes and 13,129,797
lines. Their manifests match on corpus hash, content hash, start, raw line
bytes, lines, acknowledged lines and acknowledged raw line bytes, and both
deliveries are clean. Both query reports are `valid=true`, all 80 timed
responses returned the exact expected counts, and there were no failures or
timeouts.

Evidence: [run metadata](benchmarks/phase3/small-2gb-v1-cacheoff/run-metadata.txt),
[timings](benchmarks/phase3/small-2gb-v1-cacheoff/timings.tsv), [Parkana ingest](benchmarks/phase3/small-2gb-v1-cacheoff/ingest-parkana.json),
[Loki ingest](benchmarks/phase3/small-2gb-v1-cacheoff/ingest-loki.json), [Parkana query](benchmarks/phase3/small-2gb-v1-cacheoff/query-parkana.json),
[Loki query](benchmarks/phase3/small-2gb-v1-cacheoff/query-loki.json), [Parkana storage](benchmarks/phase3/small-2gb-v1-cacheoff/storage-parkana.txt),
[Loki storage](benchmarks/phase3/small-2gb-v1-cacheoff/storage-loki.txt), [Loki post-query](benchmarks/phase3/small-2gb-v1-cacheoff/storage-loki-post-query.txt),
[sidecar builds](benchmarks/phase3/small-2gb-v1-cacheoff/index-build-parkana.txt).

### Ingest

| backend | ack report s | ack MB/s | fraction of 31 MB/s | additional visibility s | raw MB/s through visibility |
|---|---:|---:|---:|---:|---:|
| Parkana | 91.814634001 | 21.783 | 70.27% | 111.766 | 9.824 |
| Loki | 61.775898547 | 32.375 | 104.44% | 12.399 | 26.963 |

Acknowledgment durability differs between these rows: Parkana's 204 follows a
tenant WAL group fsync, while Loki's follows `wlog.Log` without a per-push sync.
See [durability detail](benchmarks/phase3/README.md#durability).

The ingest reports also separate generation from acknowledgment; generation is
not an isolated bottleneck estimate because it runs while the backend consumes
the stream:

| backend | acknowledged lines/s | generation s | generation MB/s | generation % of acknowledgment |
|---|---:|---:|---:|---:|
| Parkana | 143003.31 | 15.679587756 | 127.554 | 17.077% |
| Loki | 212539.15 | 18.139701934 | 110.255 | 29.364% |

Loki's explicit flush barrier took 7.464 s and is included in its visibility
interval. Parkana's barrier includes upload, sidecar build and catalog/query
work; the 111.766 s delay is not an isolated upload measurement.

### Query

The short window is 19.658 s and the long window is 1,312.980 s. The cache was
warmed by correctness barriers and query warmups; every timed Parkana sample
fetched zero object bytes and zero sidecar bytes and had zero downloads.

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

Every observed count equals its expected count. The Parkana candidate and Loki
logical byte columns are shown side by side but are not comparable units.

### Storage

| item | value | definition |
|---|---:|---|
| Parkana compressed Parquet | 402562973 / 16 files | live catalog objects |
| Parkana sidecars | 1783024 / 16 objects | Garage bytes minus catalog Parquet |
| Parkana sidecar / compressed Parquet | 0.442918032603% | sidecar overhead on compressed data |
| Parkana sidecar / raw line bytes | 0.0891512% | sidecar overhead on raw lines |
| Parkana object storage / raw | 20.21729985% | Parquet plus sidecars |
| Loki object store before query | 443772152 / 347 objects | Garage `GetBucketInfo` |
| Loki object store / raw | 22.1886076% | before query |
| Loki local workdir before query | 2098508509 | WAL, index, index cache, compactor |

Loki's post-query object store grew by one object and 6,537 bytes. Local index
bytes were 55,993 before and 57,282 after; they do not establish whether those
bytes shipped, and no index upload barrier is claimed.

The ingest source reported 16 sidecar builds: sum 47.478094748 s, count 16,
mean 2.967381 s, all 16 at or below the 20.48 s histogram bound. Sum divided by
acknowledgment elapsed is 51.7108%; divided by acknowledgment plus visibility,
23.3215%. These are normalized wall-duration ratios, not CPU shares.

### Limits

* Every present marker class correlates with the `api` stream in corpus v1;
  frequent pruning of other streams is a corpus property, not general behavior.
  Its four label sets allow at most four active pushes despite concurrency 16;
  its 5,000-line batch was a global chunk split across sets, while corpus v3
  sends 1,250 entries per label set.
* Parkana acknowledged 21.783 MB/s and needed 111.766 s more for exact
  visibility; 9.824 MB/s is the honest end-to-end rate.
* Loki's logical byte count is not compressed storage or physical I/O.
* The Loki index shipping barrier was not established.
* The footer-cache-off source predates the production cache and does not
  measure it.
* One run, one machine, one corpus, sequential backends: no variance or
  confidence interval; p99 is the maximum of five samples.
* Launcher Go 1.23.4 differs from the parkbench compiler Go 1.27.3.

## Full preset: 25 GB, corpus v3, cache on

Source `fe7a39f2a65784e57acba0eb5c6177b33e2d9ec4`, clean. The actual Boat VM
was type `large`: AMD EPYC 4585PX, 8 vCPU, 16,768,802,816 bytes RAM, Go
1.27.3, with 4 CPUs and 8 GiB per backend container
([`full-boat-shape.json`](benchmarks/phase3/full-25gb-v3-cacheon/full-boat-shape.json),
[`run-metadata.txt`](benchmarks/phase3/full-25gb-v3-cacheon/run-metadata.txt)).
The run used corpus v3, 16 label sets and senders, 1,250 target entries per
request per label set, the footer cache on and the final write path; it ran
once with no rerun.

| backend | ack report s | ack MB/s | fraction of 31 MB/s | additional visibility s | raw MB/s through visibility |
|---|---:|---:|---:|---:|---:|
| Parkana | 579.362547572 | 43.151 | 139.20% | 1349.027 | 12.964 |
| Loki | 277.275735789 | 90.163 | 290.85% | 52.274 | 75.861 |

Both manifests are fully equal on corpus hash
`eb29949b7aa004a2f7e9c499b5818a37e5a2e88a8a340438b1875a6ab5e5d32e`,
content hash `45bea2818eaf36a5`, 25,000,000,000 raw line bytes and
163,054,456 lines. Delivery is clean with 130,448 successful requests and zero
failed, retried or ambiguous requests. Measured batching is identical on both
backends: 1,250 target, minimum 880, maximum 1,250, mean 1,249.957500 entries
per request. Measured parallelism is 16 label sets, 16 active senders,
concurrency limit 16 and peak 16 in-flight pushes.

Parkana's 1,349.027 s visibility tail dominates its end-to-end result: the
43.151 MB/s acknowledgment falls to 12.964 MB/s through visibility. The tail
is the serial flush, merge and upload pipeline plus sidecar builds, not an
admission failure. Loki's explicit flush took 25.039 s and is included in its
52.274 s visibility interval.

### Query

The short window is 19.658 s and the long window is 16,305.446 s. All 80 timed
responses across both backends returned the exact expected counts. Every timed
Parkana sample fetched zero cache and object bytes and had zero downloads.

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

Frequent-long is the only series that scans all 32 files and index-prunes none:
corpus v3 places frequent markers in every stream and file, so the bloom cannot
exclude a file. Rare and short 4-byte long windows scan 4 files and index-prune
28; absent prunes all 32 with zero scanned files; label pruning is zero
everywhere. Parkana candidate bytes and Loki logical bytes are separate units;
neither is physical I/O.

### Storage

| item | value | definition |
|---|---:|---|
| Parkana compressed Parquet | 5126663281 / 32 files | live catalog objects |
| Parkana sidecars | 3676128 / 32 objects | Garage bytes minus catalog Parquet |
| Parkana sidecar / compressed Parquet | 0.0717060552% | sidecar overhead on compressed data |
| Parkana sidecar / raw line bytes | 0.014704512% | sidecar overhead on raw lines |
| Parkana object storage / raw | 20.521357636% | Parquet plus sidecars |
| Loki object store before query | 5754005924 / 4369 objects (23.016023696% of raw) | Garage `GetBucketInfo` |
| Loki local workdir before query | 20387291 | WAL, index, index cache, compactor |

Loki's post-query object store grew by one object and 61,170 bytes. Its local
index bytes were 263,643 before and 107,974 after
([post-query storage](benchmarks/phase3/full-25gb-v3-cacheon/storage-loki-post-query.txt));
shipping state remains unknown, and no index upload barrier is claimed.

The 32 sidecar builds sum 400.952794173 s (mean 12.529775 s). Cumulative
bucket counts are 6 at 5.12 s, 27 at 20.48 s and 32 at 81.92 s and +Inf. Sum
divided by acknowledgment elapsed is 69.2058532%; divided by acknowledgment
plus visibility, 20.7921058%. These are normalized asynchronous wall-duration
ratios, not CPU shares or causal overhead.

Evidence: [timings](benchmarks/phase3/full-25gb-v3-cacheon/timings.tsv),
[Parkana ingest](benchmarks/phase3/full-25gb-v3-cacheon/ingest-parkana.json), [Loki ingest](benchmarks/phase3/full-25gb-v3-cacheon/ingest-loki.json),
[Parkana query](benchmarks/phase3/full-25gb-v3-cacheon/query-parkana.json), [Loki query](benchmarks/phase3/full-25gb-v3-cacheon/query-loki.json),
[Parkana storage](benchmarks/phase3/full-25gb-v3-cacheon/storage-parkana.txt), [Loki storage](benchmarks/phase3/full-25gb-v3-cacheon/storage-loki.txt),
[sidecar builds](benchmarks/phase3/full-25gb-v3-cacheon/index-build-parkana.txt), [identities](benchmarks/phase3/full-25gb-v3-cacheon/full-build-identities.json).

## Sender parallelism experiment: 2 GB, corpus v3

A matched Parkana-only pair on one Boat `large` VM (AMD Ryzen 7 9700X, 8 vCPU /
16 GB, 4 CPUs / 8 GiB per backend container), source
`fe7a39f2a65784e57acba0eb5c6177b33e2d9ec4` plus two reviewed benchmark files;
the production server source is unchanged. Both arms used corpus v3, 1,250
entries per request per label set, concurrency limit 16, and the same seed,
start and step. The four-sender arm set `--instances-per-service 1`; the
sixteen-sender arm used the default four, giving 16 label sets and 16 active
senders.

| actual peak senders | ack MB/s | ack seconds | additional exact visibility s | raw MB/s through visibility |
|---:|---:|---:|---:|---:|
| 4 | 5.307477 | 376.826852 | 23.535 | 4.995481 |
| 16 | 24.045978 | 83.173994 | 169.337 | 7.920447 |

The improvement is 4.53x for acknowledgment and 1.59x through exact
visibility. Sixteen senders still reach only 77.6% of the 31 MB/s target, and
the 169.337 s visibility tail remains unresolved. Both arms acknowledged
exactly 2,000,000,000 bytes and 13,129,764 lines with zero failed, retried or
ambiguous requests and zero WAL errors; catalog, query and post-restart counts
matched. Corpus v3 content hash and per-service totals are identical; identity
hashes differ because the instance count is part of identity.

Measured WAL counters support a durability-latency bottleneck and better group
commit with more active senders:

| acknowledgment barrier | 4 senders | 16 senders |
|---|---:|---:|
| recorded fsyncs | 5299 | 1366 |
| fsync duration sum, s | 371.354362 | 79.809837 |
| mean recorded fsync, ms | 70.0801 | 58.4259 |
| completed pushes per fsync | 1.9823 | 7.6955 |

This is one ordered pair, not a randomized matrix: realized fsync latency and
file formation differ between the arms, so the pair cannot attribute the whole
difference to sender count. Treat the rates as relative evidence, not expected
absolute performance. The higher acknowledgment rate also changes asynchronous
file formation: the four-sender arm produced 116 Parquet files and 13,093,640
sidecar bytes; the sixteen-sender arm produced 12 files and 1,379,384 sidecar
bytes, and left a larger upload backlog. This pair is not pooled with the
presets.

Evidence: [four-sender manifest](benchmarks/phase3/sender-parallelism-v3/v3-4/ingest-parkana.json),
[sixteen-sender manifest](benchmarks/phase3/sender-parallelism-v3/v3-16/ingest-parkana.json),
[four-sender timings](benchmarks/phase3/sender-parallelism-v3/v3-4/timings.tsv),
[sixteen-sender timings](benchmarks/phase3/sender-parallelism-v3/v3-16/timings.tsv),
[storage](benchmarks/phase3/sender-parallelism-v3/v3-16/storage-parkana.txt).
Full request and WAL details: [evidence README](benchmarks/phase3/README.md#sender-parallelism-experiment).

## Search-index build matrix

Bounded microbenchmark on a separate snapshot-derived Boat VM: AMD Ryzen 9
9950X, 8 logical CPUs, 16,375,780 KiB `MemTotal`, Go 1.27.3. Fixture version 1,
seed `0x7061726b616e6102`; the template corpus has 7,728 lines and 1,048,595
raw bytes, the high-cardinality corpus 6,291 lines and 1,048,627 raw bytes.
Three build iterations per cell. This is not end-to-end ingest throughput.

| corpus | N | gram FP target | build ms | MB/s | B/op | retained MiB | sidecar KiB | raw overhead % | needle FP 4/5/6/7/8 bytes % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| template | 3 | 0.01 | 6.46 | 162.27 | 160890 | 0.070 | 3.07 | 0.299 | 1.56/1.37/2.34/3.12/2.34 |
| template | 3 | 0.001 | 7.33 | 143.12 | 162554 | 0.070 | 4.58 | 0.448 | 1.17/1.37/2.15/2.73/1.76 |
| template | 4 | 0.01 | 9.15 | 114.57 | 1287866 | 0.564 | 22.94 | 2.240 | 0.195/1.17/2.54/2.34/1.17 |
| template | 4 | 0.001 | 8.86 | 118.32 | 1304250 | 0.564 | 34.39 | 3.359 | 0.195/0.391/1.56/1.76/1.17 |
| template | 5 | 0.01 | 12.93 | 81.12 | 3488618 | 1.551 | 66.65 | 6.509 | 100/0.586/1.37/1.37/1.56 |
| template | 5 | 0.001 | 13.68 | 76.68 | 3857514 | 1.568 | 99.96 | 9.761 | 100/0/0.391/0.586/0.391 |
| highcard | 3 | 0.01 | 8.61 | 121.83 | 323386 | 0.141 | 7.43 | 0.726 | 4.88/9.96/10.6/10.9/9.57 |
| highcard | 3 | 0.001 | 7.67 | 136.72 | 347962 | 0.141 | 11.14 | 1.088 | 4.3/9.38/9.38/10.2/9.18 |
| highcard | 4 | 0.01 | 16.49 | 63.61 | 5162298 | 2.255 | 99.14 | 9.681 | 0.586/7.81/7.81/7.81/7.42 |
| highcard | 4 | 0.001 | 16.98 | 61.76 | 5211450 | 2.255 | 148.70 | 14.520 | 0/6.64/6.84/7.03/7.03 |
| highcard | 5 | 0.01 | 35.64 | 29.42 | 20595008 | 9.020 | 352.90 | 34.460 | 100/1.17/4.49/3.12/3.32 |
| highcard | 5 | 0.001 | 36.84 | 28.46 | 20775274 | 9.020 | 529.30 | 51.680 | 100/0.195/3.32/1.56/1.76 |

Needle rates use 512 exactly absent probes, half with mutations restricted to
the corpus alphabet, and include structural false positives when all grams
exist without the whole needle. Zero observed positives do not prove zero
population false positives; 512 samples give 0.195 percentage-point resolution.
Four-byte probes cannot prune with N=5, hence the 100% entries. Build includes
`NewBuilder`, `Add` for every line and `Encode`; generation, absence checks and
memory diagnostics are outside the timed build.

Measured decision: retain N=4, FP target 0.01 and `MaxGrams=2<<20`. N=5 raises
high-cardinality retained memory 2.255 → 9.020 MiB and sidecar overhead
9.681% → 34.460% at the 1% target while losing four-byte pruning. A 0.1% gram
target costs about 50% more sidecar bytes for a limited reduction in structural
positives. N=3 saves memory and storage but increases high-cardinality
whole-needle positives. No production change is justified by this fixture.

Evidence: [index-matrix.json](benchmarks/phase3/index-matrix/index-matrix.json).
Full method and exact command:
[evidence README](benchmarks/phase3/README.md#search-index-build-matrix).

## Parquet footer cache experiment

Separate experiment on AMD Ryzen 7 9700X, 8 cores, `GOMAXPROCS=8`, Go 1.27.3;
the benchmark VM's RAM was not queried. Each range contains three observations,
not a confidence interval; the arms ran on-then-off with no randomization.

| shape | off median ms (range) | on median ms (range) | on change | ranges overlap |
|---|---:|---:|---:|:---:|
| repeated-few | 1.233 (1.215-1.253) | 1.053 (0.973-1.066) | -14.6% | no |
| repeated-many | 4.134 (4.006-5.053) | 4.176 (3.999-4.224) | +1.0% | yes |
| repeated-schemagroups | 6.457 (5.539-6.623) | 5.378 (4.753-5.754) | -16.7% | yes |
| single-cycling-pool | 0.624 (0.620-0.631) | 0.592 (0.585-0.598) | -5.1% | no |

Overlapping ranges are inconclusive; they do not establish equivalence. This
experiment does not show a general speedup and must not be pooled with the
preset runs. Enabling the cache reduced read footer bytes from 22,226 to 0 and
lowered DuckDB local `total_bytes_read` in the compiled `Select` from 28,635 to
6,409 bytes. Those local reads are not parkbench candidate bytes, object
download bytes or Loki logical bytes.

Evidence: [footer-cache-summary.json](benchmarks/phase3/footer-cache/footer-cache-summary.json); full method is in the [evidence README](benchmarks/phase3/README.md#parquet-footer-cache-experiment).

## Reproducing

Offline checks touch no Docker, Go or network services:

```sh
bash -n scripts/bench-phase3.sh
bash scripts/bench-phase3.sh --self-test
bash scripts/bench-phase3.sh --dry-run --preset small
```

The historical small run needs a clean checkout of
`f24d4dfdde090da8072eda8cf33a8836315212cd`; the source flags label metadata
only and do not check out code:

```sh
bash scripts/bench-phase3.sh --preset small \
  --source-commit f24d4dfdde090da8072eda8cf33a8836315212cd --source-dirty 0 \
  --seed 8098146604754141185 \
  --start 2026-11-04T13:23:15.266317Z \
  --step 100us --batch-lines 5000 --concurrency 16 \
  --ready-timeout 180 --visibility-timeout 1200 --flush-timeout 300 \
  --output-dir /tmp/parkana-bench/small-2gb-v1-cacheoff
```

With the same seed, start, step, batch size and concurrency, corpus identity
should reproduce even though timings cannot: a fresh project, output directory
and hardware change the measurements. Compare only after checking
`corpus.hash`, `corpus.content_hash`, raw line bytes, lines and acknowledged
lines. If the recorded historical start falls outside Loki's timestamp-age
admission window, use a fresh start and label that run separately rather than
changing Loki config.

The full preset requires an explicit `--allow-full` or
`PARKANA_BENCH_ALLOW_FULL=1`:

```sh
tm-boat test /path/to/worktree --type large -- \
  bash scripts/bench-phase3.sh --preset full --allow-full \
    --source-commit fe7a39f2a65784e57acba0eb5c6177b33e2d9ec4 --source-dirty 0 \
    --output-dir /tmp/parkana-bench/full
```

The exact clean-source checkout, seed, start, step and defaults are in the
[full reproduction steps](benchmarks/phase3/README.md#full-25-gb-v3-cache-on-run-exact-source-fe7a39f).

## Known limits

* The historical small run uses corpus v1 with one-stream marker correlation
  and the footer cache off; the full run differs in size, corpus, footer state
  and CPU hardware, so the two are not a controlled before/after comparison.
* The sender-parallelism pair is a separate single-host ordered pair on a
  slow-fsync machine; it is not pooled with either preset.
* The Loki index shipping barrier remains unknown, and the Parkana query scan
  and prune metric names belong to the query implementation.
* Every result is one run or one ordered pair with no confidence interval.

## Next steps

* Parallelize the upload pipeline. The sender-parallelism pair shows that
  admission can improve substantially while the serial flush/merge/upload tail
  grows, so the visibility lag is a known pipeline limit rather than an
  acknowledgment-rate artifact.
* Add an opt-in WAL sync interval mode. The current default remains
  always-sync per acknowledgment; any relaxed mode is a separate, explicit
  durability trade-off and is not measured here.

## Phase 4 ingest runtime

Phase 4 measures the ingest runtime controls documented in this section. The
final 4 GB results come first, then the historical 25 GB failures, the small
sweep and a separate hot-stream observation, followed by the tooling
reference. Raw evidence lives in
[`docs/benchmarks/phase4/`](benchmarks/phase4/); every table value is read from
those artifacts and rounded for display. Definitions match the phase-3 ones
above, and none of these results changes a phase-3 number.

### Final 4 GB acceptance and hot-stream stress

Two Parkana-only arms ran the accepted final size of 4,000,000,000 decimal raw
line bytes, selected with `--bytes 4000000000` (the script records
`preset=custom`; there is no 4 GB preset, and medium still means 10 GB) from
clean source `46593319414965de857407d0db6f0e79e4fd55dd`, with
`wal.sync=always`, flush 4, upload 4 and the memory budget auto-resolved to
4 GiB from the container cgroup. Rates are for this actual size and are not a
1:1 comparison with the historical phase-3 25 GB baseline row.

| arm | workload | VM / Boat exit | ack s | ack MB/s | delay s | through MB/s | ACK-window CPU s | memory.peak |
|---|---|---|---:|---:|---:|---:|---:|---:|
| [acceptance](benchmarks/phase4/final-4gb-acceptance/) | 4 streams × 4 instances, 100 µs | bx_6g83s4p7, `tm-boat: exit 0 (command 210s, total 224s)` | 58.333 | 68.572 | 35.743 | 42.519 | 202.280 | 8.000 GiB (at limit) |
| [hot stress](benchmarks/phase4/final-4gb-hot/) | one `api` stream × 16, 50 µs | bx_xasjdrqq, `tm-boat: exit 0 (command 218s, total 228s)` | 30.940 | 129.285 | 109.702 | 28.441 | 101.227 | 7.885 GiB |

Acceptance passed every timing target (ACK ≥ 38.8, delay ≤ 120 s, through ≥
31 MB/s) and the operational checks; restart counts were 26,196,634
(acceptance) and 26,554,577 (hot), and object counts were 20 data + 20 sidecar
versus 3 + 3. Its cgroup `memory.peak` was 8,589,934,592 bytes, exactly the
8 GiB container limit: an at-limit high-water mark including page cache, with
`oom_kill=0` and no headroom claim. The hot row is a separate stress
observation, not acceptance and never pooled with the four-stream arm: ACK and
delay passed, but through was 28.441 MB/s, below 31, while `operational_pass`
stayed true with `memory.peak` 8,466,714,624 bytes and `oom_kill=0`. Peak
aggregate in-flight estimate (not one receipt) was 1,986,393,600 B (acceptance)
and 463,623,680 B (hot); individual receipt source bytes were 1,771,116 to
66,958,175 B across 20 receipts and 123,101,930 to 540,318,525 B across 3
receipts, so no receipt approached the 2 GiB merge cap. At-cap admission is
covered only by unit/profile tests, not by these runs. Neither arm changes a
product default: upload concurrency stays 4 and `wal.sync` stays `always`.

### Full preset: 25 GB, corpus v3 (historical failures)

Two Parkana-only arms replayed the exact phase-3 full corpus, start
`2026-11-03T00:27:57.30252Z`, seed `8098146604754141185`, step `100us`, four
streams × four instances = 16 label sets and 16 senders, from clean
`c8c67bd3736d6c82dfe374fe50a3ccbf3d3a737f` source on separate Boat large VMs.
`wal.sync` was `always` and flush workers 4 in both. Both runs stopped before
completing their original measurement contract, so most columns are marked
unavailable rather than estimated.

| arm | upload workers | ack s | ack MB/s | queryable delay s | through MB/s | ACK-window CPU s | memory.peak GiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| [full-u2](benchmarks/phase4/full-u2/) | 2 | 481.245 | 51.949 | failed | unavailable | 1599.569 | 8.000 (see scope note) |
| [full-u4](benchmarks/phase4/full-u4/) | 4 | 524.173 | 47.694 | unavailable | unavailable | 1882.385 | unavailable |

Both arms acknowledged all 163,054,456 lines and 25,000,000,000 raw line
bytes of the identical phase-3 corpus (same corpus hash
`eb29949b7aa004a2f7e9c499b5818a37e5a2e88a8a340438b1875a6ab5e5d32e`), and
neither met the original publication/visibility target under a 4 CPU / 8 GiB
backend container.

The u2 arm (AMD EPYC 4585PX) acknowledged in 481.245 s. Its 5 s-sampled
all-upload-rows counter first read complete 443.722 s after acknowledgment:
that is a completed-pass observation, not per-job merge durations, and it
already exceeds the target, so the original run failed. Repeated exact-count
attempts hit the 120 s query budget with no answer, so queryable delay, the
through-visibility rate, the query workload and the restart check are
unavailable. Two later bounded reads are recorded as diagnostics, not as
original results: a separate authorized count returned the exact
163,054,456 over HTTP 200 in 165.521 s outside the original budget, and the
pre-diagnostic cgroup `memory.peak` read was 8,589,938,688 bytes (8.000 GiB,
`oom_kill` 0; the post-diagnostic read was identical) with scope "the
original failed run plus prior count attempts", not pure ingest.
`EXPLAIN ANALYZE` was unavailable (no supported HTTP API endpoint; the VM
had no DuckDB CLI and no Python `duckdb` module), so the diagnostic count
time cannot be decomposed; `catalog-files.json` shows all 32 objects
published with sidecars.

Retained stage evidence for the u2 tail is narrow (`tail-analysis.json`):
the sidecar build wall sum grew from 204.546 s over 18 builds near
acknowledgment to 456.447 s over 32 builds when uploads read complete, a
251.901 s delta across 14 builds, while process CPU rose 955.410 s over the
same sampled window. Those wall sums overlap across jobs and cannot be
subtracted from process CPU or elapsed; exact merge time, per-object Put
time, concurrent job counts (upper bound 2) and per-job source bytes were
not recorded, so no per-stage split of the 443.722 s tail is estimated.

The u4 arm (AMD Ryzen 9 9950X) acknowledged in 524.173 s, then the container
was OOMKilled (exit 137) during the post-acknowledgment upload/merge phase.
Queryable delay, the through-visibility rate, the query report and the
restart check are unavailable, and `memory.peak` could not be read after the
kill: `failure-state.json` records it as `null`, never as the configured
8 GiB limit.

Both 25 GB rows remain FAILED historical results; the later 4 GB final arms
above are the current evidence and do not promote them. Neither full arm
selects a default: the small sweep's ordering did not carry to 25 GB, the
memory-admission work merged after these failures is present in the 4 GB
source and measured above, and the product upload-concurrency default stays
4. The only Loki reference is the historical phase-3 full baseline above, a
different run rather than a fresh co-located comparison; the phase-3 raw run
log is not retained in tracked evidence, so the historical 1,349.027 s
Parkana tail cannot be decomposed into publication versus count time either.
No 2 GB number on this page extrapolates to 25 GB, and the 25 GB baseline size
is not directly comparable to the 4 GB final rows.

### Controlled small sweep: 2 GB, corpus v3, wal.sync=always

One Boat VM (AMD EPYC 4585PX, 8 vCPU / 16 GB, Go 1.27.3; backend container
limited to 4 CPUs / 8 GiB) ran four sequential Parkana-only arms, in the
order u1, foundation, u2, u4, over one pinned corpus-v3 replay: explicit
start `2026-11-05T00:17:36.151838Z`, seed
`8098146604754141185`, step `100us`, four streams × four instances = 16 label
sets and 16 senders, 1,250 entries per request per label set. All four
manifests record identical corpus hash, content hash, 13,129,764 acknowledged
lines and 2,000,000,000 raw line bytes; every arm produced 8 Parquet objects
and 8 sidecar objects, passed the restart/catalog count check and returned a
`valid` query report across 8 series. The foundation arm ran source
`0b3cc17bb2a2cfad1b37cab47cd3a22947d40c34` under its legacy always/F1/U1
contract (no phase-4 flags); the other three arms ran the same Parkana image
and parkbench client built from `8a523666850e4a7ec41285e7caec14a22952e7f9`.
`wal.sync` was `always` in every arm.

| arm | flush workers | upload workers | ack s | ack MB/s | queryable delay s | through MB/s | CPU delta s | memory.peak GiB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| [foundation (always/F1/U1)](benchmarks/phase4/small-foundation/) | 1 | 1 | 18.813 | 106.311 | 76.744 | 20.930 | 55.450 | 4.336 |
| [u1](benchmarks/phase4/small-u1/) | 4 | 1 | 29.698 | 67.345 | 77.306 | 18.691 | 76.863 | 4.315 |
| [u2](benchmarks/phase4/small-u2/) | 4 | 2 | 17.383 | 115.052 | 43.736 | 32.723 | 53.917 | 4.355 |
| [u4](benchmarks/phase4/small-u4/) | 4 | 4 | 24.353 | 82.125 | 23.521 | 41.776 | 82.121 | 4.343 |

Ack MB/s is raw line bytes over the acknowledgment report interval; delay is
seconds until the first exact `count_over_time` equals `ack_lines`; through is
raw bytes over their sum; CPU is the container cgroup-v2 `usage_usec` delta
across the ingest command expressed in seconds; memory is the observed cgroup
`memory.peak` high-water mark, not the configured 8 GiB limit. In this sample
u2 posted the highest acknowledgment rate and u4 the shortest visibility
delay; these are single observations, one run per arm on one machine with no
confidence interval, and at 2 GB fsync and scheduler noise can dominate the
spread. They do not select a default: the product upload-concurrency default
stays 4 unless a full-preset decision changes it.

### Single hot stream arm (separate workload)

One observation of a single hot stream: `--streams api
--instances-per-service 16` gives one stream, 16 label sets and 16 senders.
This arm ran newer source `c8c67bd3736d6c82dfe374fe50a3ccbf3d3a737f` on a
different Boat VM (AMD EPYC 9455) than the sweep, and its workload differs:
13,309,066 lines, 2 Parquet objects and 2 sidecars, restart count verified,
query `valid`. With one stream/hour partition under the merge cap it need not
expose enough independent upload receipts to saturate two upload workers.

| arm | wal.sync | flush workers | upload workers | ack s | ack MB/s | queryable delay s | through MB/s | CPU delta s | memory.peak GiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| [hot-u2 (c8c67bd, api×16)](benchmarks/phase4/hot-u2/) | always | 4 | 2 | 25.243 | 79.229 | 75.998 | 19.755 | 63.407 | 4.228 |

Different source, workload and VM: do not compare this row with the sweep
above as if it were another arm of the same experiment, and do not pool it
with the phase-3 presets.

### Runtime controls and flags

`scripts/bench-phase3.sh` exposes the runtime controls needed to separate
acknowledgment from exact visibility:

| script option | Parkana setting | default | valid values |
|---|---|---|---|
| `--wal-sync always\|interval` | `-wal.sync` | `always` | `always` or `interval` |
| `--wal-sync-interval D` | `-wal.sync-interval` | `1s` | positive bounded duration: ns/us/µs/ms/s/m/h components, ≤9 fractional digits, whole nanoseconds, no overflow |
| `--flush-concurrency N` | `-ingest.flush-concurrency` | `4` | `1..64` |
| `--upload-concurrency N` | `-ingest.upload-concurrency` | `4` | `1..64` |
| `--streams LIST` | `-streams` | omitted: parkbench's four corpus-v3 streams | comma-separated `[A-Za-z0-9_-]` names; no marker literals, empty entries or duplicates; streams × instances ≤ 4096 |
| `--reuse-manifest PATH` | `-start` from `corpus.start` only | none | existing ingest manifest; conflicts with `--start`; seed/step/bytes stay current flags |
| `--legacy-runtime` | no phase-4 runtime flags | off | foundation-style binary; known always/F1/U1 only; explicit interval/F>1/U>1 rejected |

Phase 4 defaults flush=4/upload=4; phase 3 was measured with the serial
flush=1/upload=1 reference, so pass `--flush-concurrency 1
--upload-concurrency 1` to reproduce that reference. `always` remains the
default WAL durability mode, so the acknowledgment guarantee is unchanged
unless `interval` is explicitly selected. Under `interval`, live tail
publishes an entry once its WAL write and memtable admission complete, so a
tail viewer can see lines that a machine crash before the next completed
fsync loses; `always` keeps acknowledgment fsync-durable. Upload concurrency
bounds independent upload receipts in flight (each receipt is one merge,
sidecar build and object Put), not individual Puts or file writers. For
modern runs the script writes the four values only to its 0600 per-run env
file and removes the four `PARKANA_BENCH_*` interpolation names from the
environment before invoking Compose, so a caller export cannot override a
run. `deploy/compose/bench.yaml` remains the frozen phase 3 base:
the new `deploy/compose/ingest-runtime.yaml` overlay is passed second with
`-f bench.yaml -f ingest-runtime.yaml` for modern runs and only attaches those
four runtime environment keys to the existing Parkana service;
`--legacy-runtime` omits the overlay entirely. It adds no service, port,
volume, merge cap or search-index cap. `upload-merge-max` stays 2 GiB and
`index-max-grams` stays 2,097,152 unless the existing explicit
`--index-max-grams` control selects another value.

Requested values and their source (flag or default) are recorded in
`run-metadata.txt` and `summary.md`. For modern runs, after Parkana becomes
ready the script reads the running container's actual environment and resource
limits with `docker inspect`; those effective values are recorded in
`runtime-parkana.txt`, appended to `run-metadata.txt`, and included in
`summary.md`. A missing or mismatched modern runtime value fails the run
instead of recording requested settings as if they were effective. The legacy
path records the known source contract instead and is described below.

The exact base and overlay files are copied to
`config/bench.yaml` and `config/ingest-runtime.yaml`, and their SHA-256 values
are recorded in `run-metadata.txt`.

### Single hot stream variant

Omitting `--streams` keeps the four-stream corpus-v3 default byte-for-byte: the
script does not pass `-streams`, so parkbench's own default applies and the
25 GB identity (4 streams × 4 instances = 16 label sets and senders) is
unchanged. For the hot-stream arm pass the stream list, instance count and
concurrency explicitly:

```sh
bash scripts/bench-phase3.sh --preset small \
  --streams api --instances-per-service 16 --concurrency 16 ...
```

That arm has one stream but still 16 full label sets and 16 active senders,
because each `(stream, instance)` pair is one sequential sender.
`run-metadata.txt` records the requested values (`streams_requested`,
`streams_source`) separately from the actual manifest
(`ingest_<backend>_corpus_streams`, `ingest_<backend>_corpus_stream_count`,
`ingest_<backend>_corpus_instances_per_service`, measured
`parallelism.label_sets`/`active_senders`), and `storage-*.txt` records the
actual Parquet/object counts. Query, exact-count, restart and manifest
comparison checks read the manifest and do not assume four streams. Do not read
a one-stream arm as scheduler speedup: one stream may still produce multiple
Parquet objects and receipts across generations and cap chunks; report the
actual stream, label-set and object counts with the result.

### Start pin, reuse and replay

Hour partitions move when the corpus start moves. Pass `--start` explicitly to
pin every sweep arm, or pass `--reuse-manifest PATH` to read ONLY `corpus.start`
from a prior ingest manifest; seed, step, bytes, streams and runtime flags stay
the current flags and are never silently reused. `--reuse-manifest` conflicts
with `--start`, and a missing, unreadable, start-less, non-RFC3339 or invalid
timestamp is rejected before Docker starts. After each ingest the script records
the actual manifest interval (`actual_start`, `actual_through`, plus
per-backend copies), the start source (`explicit`, `reused-manifest` or
`derived-by-tool`), the reused manifest path/SHA-256 and a self-contained
`replay_command`; the same values are printed in the run log and `summary.md`.
The replay pins the actual manifest streams and instance count (so a future
tool-default change cannot move the corpus), includes the selected
`--wal-sync-interval` for interval runs, and ends with
`--output-dir /tmp/parkana-bench-replay-UNIQUE`; replace `UNIQUE` with a fresh
name before running.
`ingest_<backend>_*` also records hash, content hash, seed, step, target/raw
bytes, lines and acknowledged totals, so a reused start or a changed default is
visible without trusting the requested flags.

### Container CPU and memory peak

Before parkbench ingest starts and immediately after it returns (the
acknowledgment endpoint is captured before this read; the CPU read is a bounded
diagnostic after ack), the script reads `/sys/fs/cgroup/cpu.stat` once from the
running backend container with a bounded `compose exec`. It records
`ingest_<backend>_cpu_before_usage_usec`, `_after_`, `_delta_`, both wall
snapshot boundaries and `_wall_s`. This is container-level cgroup-v2 CPU, not
component attribution, and a missing or unreadable counter is recorded as
`unavailable`, never as zero; a backwards counter fails the run. Before the
backend stops, one bounded read of `/sys/fs/cgroup/memory.peak` records the
ingest-stage high-water mark (`parkana_ingest_memory_peak_bytes`) next to, but
distinct from, the configured limit (`parkana_effective_memory_bytes`). There
is no background sampler and no phase reset; an unreadable or malformed value
is recorded as unavailable with a reason. Both captures run for modern and
legacy arms.

### Legacy foundation arm

`--legacy-runtime` selects the foundation0b3cc17-style source contract: the
binary has no phase-4 runtime flags, so the ingest-runtime overlay is not
attached, runtime environment validation is skipped, and metadata records
`parkana_runtime_mode=legacy-foundation`,
`parkana_runtime_source=legacy-source-contract` and
`parkana_runtime_support=absent`. Omitted runtime flags mean the known
always/F1/U1 baseline (`parkana_legacy_known_*`), never a container-effective
claim; explicit `--wal-sync interval`, `--wal-sync-interval`, F>1 or U>1 is
rejected. Before the CPU window the script runs one bounded `parkana -h` inside
the container and records only a support boolean, source and SHA-256 of the
help text; the raw help is never printed or archived because env-backed
defaults may appear. Each of the three runtime flag names is counted: legacy
requires zero, modern requires all three, and a partial or mixed help fails with
a fixed sanitized message. App source identity (`app_source_commit`,
`app_source_dirty`, `source_commit`) and the driver hash
(`driver_script_sha256`, `script_sha256`) are recorded separately.

The measurement contract is unchanged: acknowledgment ends at the last HTTP
204, exact visibility is the first `count_over_time` equal to `ack_lines`, and
the restart/catalog check and query workload still run. Corpus v3 identity,
the 16-sender evidence and every phase 3 barrier stay in place; the runtime,
start/replay, stream, CPU, memory and probe rows are additive. The existing
`--wal-sync`, `--flush-concurrency` and `--upload-concurrency` flags still
support the same-small F4/always U1/U2/U4 sweep without changing corpus bytes,
merge caps or index caps. Source identity stays explicit for Boat archives
without `.git`: pass `--source-commit` and `--source-dirty 0|1|unknown`, and the
metadata records them as explicit instead of guessing.

Dry runs print the same requested settings without touching Docker, Go or the
network:

```sh
bash -n scripts/bench-phase3.sh
bash scripts/bench-phase3.sh --self-test
bash scripts/bench-phase3.sh --dry-run --preset small \
  --wal-sync interval --wal-sync-interval 1s \
  --flush-concurrency 4 --upload-concurrency 4 \
  --source-commit <reviewed-rev> --source-dirty 0
```

A real small run uses a fresh output directory and the phase 3 reproduction
inputs above, with the selected runtime settings passed explicitly:

```sh
tm-boat test /path/to/worktree -- \
  bash scripts/bench-phase3.sh --preset small \
    --wal-sync interval --wal-sync-interval 1s \
    --flush-concurrency 4 --upload-concurrency 4 \
    --cpus 4 --memory 8g \
    --source-commit <reviewed-rev> --source-dirty 0 \
    --output-dir /tmp/parkana-bench/p4-small-interval
```

Record the selected sync mode and both worker counts with the result; do not
pool `always` and `interval` rows or claim the phase 4 target from
microbenchmarks. The 25 GB preset still requires explicit authorization and
`--allow-full`.

For a multi-arm sweep, run the first arm with an explicit `--start` (or let the
tool derive it) and pass each later arm
`--reuse-manifest <first-output>/ingest-parkana.json`; the script prints the
actual interval and a replay command, and never reuses seed, step or bytes
silently. The foundation arm adds `--legacy-runtime` and omits the modern
runtime flags; its evidence says `legacy-source-contract`, not
container-effective, and the bounded help probe rejects a modern binary.

### Medium and custom Parkana runs

`--preset medium` is 10,000,000,000 decimal raw line bytes. `--bytes N`
auto-identifies small/medium/full and labels other values `preset=custom`;
5,000,000,000-byte runs use `--bytes`, not a new preset. The accepted final
size is `--bytes 4000000000` (4,000,000,000 decimal raw line bytes,
`preset=custom`), not the 10 GB medium preset, and it fits the 900 s
whole-run cap. Targets: delay <= 120 s, through
>= 31 MB/s, ACK >= 38.8 MB/s, OOM 0, `memory.peak` recorded. ACK 38.8 is
the 25 GB phase-3 ACK less 10%; do not compare different sizes without a
size caveat.

The four-stream shape is corpus v3, 4 streams x 4 instances = 16 senders,
100 us step, pinned start/seed, Parkana-only, 4 CPUs / 8 GiB,
`wal.sync=always`. The separate hot arm uses `--streams api
--instances-per-service 16 --concurrency 16`, an exact UTC-hour start and
about 50 us step so it fits one hour/one stream-hour partition; keep other
settings, including `upload-max-age`, identical. Its delay/through are hot,
not acceptance, and never pooled with the four-stream arm.

`--skip-queries` skips the query series and, in the current script, the
sidecar>0 gate; it is not evidence that required sidecars exist. Assert
sidecars independently or use a minimal `--classes` set. Existing
exact-count and restart barriers still run.

The script also records `oom_kill` from cgroup-v2 `memory.events` beside
`memory.peak`. Offline checks:

```sh
bash -n scripts/bench-phase3.sh
bash scripts/bench-phase3.sh --self-test
bash scripts/bench-phase3.sh --dry-run --preset medium --parkana-only
bash scripts/bench-phase3.sh --dry-run --bytes 5000000000 --parkana-only
```
