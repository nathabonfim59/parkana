# Parkana phase 3 benchmark run

- project: `sol-a-p4-u4`
- preset: `small` (2000000000 decimal raw line bytes)
- output: `/tmp/sol-a-p4-sweep/u4`
- commit: `8a523666850e4a7ec41285e7caec14a22952e7f9` (dirty files: 0)
- app source: commit `8a523666850e4a7ec41285e7caec14a22952e7f9` (dirty files: 0); driver: `scripts/bench-phase3.sh` sha256 `9c9f68b371feb5eda0dd3be7a9d1d433946a4cc95b6cca2d1853566868d51d03`
- actual corpus interval: start=2026-11-05T00:17:36.151838Z through=2026-11-05T00:39:29.128238Z (start source=explicit)
- replay: `bash scripts/bench-phase3.sh --start 2026-11-05T00:17:36.151838Z --seed 8098146604754141185 --step 100us --bytes 2000000000 --streams api,checkout,search,worker --instances-per-service 4 --batch-lines 1250 --concurrency 16 --wal-sync always --flush-concurrency 4 --upload-concurrency 4 --output-dir /tmp/parkana-bench-replay-UNIQUE` (replace UNIQUE with a fresh output name before running)
- resources: 4 CPU / 8g memory per backend container
- batch: 1250 entries per push request per label set (concurrency 16)
- instances per service: requested 4 (source=explicit)
- streams: requested tool-default (source=tool-default); actual names/count are recorded per backend as ingest_*_corpus_streams
- index max grams: 2097152 (default)
- ingest runtime requested: wal_sync=always (explicit) interval=1s (default) flush_workers=4 (explicit) upload_workers=4 (explicit)
- ingest runtime effective (container inspect): wal_sync=always interval=1s flush_workers=4 upload_workers=4 cpus=4.000 memory_bytes=8589934592
- runtime flag probe: support=present source=bounded-parkana-help sha256=bcf79dcde9d86315c771c70ecb411b450fa1e16ccce309fb4f223bac719437df
- ingest memory peak (cgroup-v2 high-water through ingest, read before stop; not the limit): 4662972416 bytes (source=cgroup-v2-memory.peak)
- backends: parkana only (`--parkana-only`; Loki explicitly skipped)
- query stage: run (B2b contract)

## Ingest

| backend | ack report s | ack wall s | flush s | queryable delay s |
|---|---:|---:|---:|---:|
| parkana | 24.353261236 | 24.360 | — | 23.521 |

Loki was skipped entirely (`--parkana-only`); no Loki timing row is reported.

### Container CPU (cgroup-v2 usage_usec, ingest command only)

| backend | before | after | delta | wall s | source |
|---|---:|---:|---:|---:|---|
| parkana | 70215 | 82191630 | 82121415 | 24.425 | cgroup-v2-cpu.stat |

Container CPU is the cgroup-v2 usage_usec delta across the parkbench ingest command, not component attribution. Unavailable counters are never reported as zero.

## Storage

```
parkana: backend=parkana
parkana: raw_line_bytes=2000000000
parkana: data_object_bytes=412735127
parkana: compressed_parquet_bytes=412735127
parkana: parquet_files=8
parkana: sidecar_objects=8
parkana: sidecar_object_bytes=920744
parkana: sidecar_object_bytes_source=garage_bucket_total_minus_catalog_data
parkana: sidecar_build_counter_bytes=920744
parkana: sidecar_build_counter_source=parkana_index_bytes_total (cumulative builds, not stored objects)
parkana: index_build_duration_seconds_count_ingest=8.000000000
parkana: index_build_duration_seconds_sum_ingest=24.329732073
parkana: index_build_duration_seconds_mean_ingest=3.041217
parkana: index_build_duration_buckets_file=index-build-parkana.txt
parkana: local_workdir_bytes_including_wal_cache_catalog=506361611
parkana: data_bytes_to_raw_line_bytes=0.206368
parkana: sidecar_bytes_to_data_bytes=0.002231
parkana: sidecar_bytes_to_compressed_parquet_bytes=0.002231
parkana: sidecar_bytes_to_raw_line_bytes=0.000460
parkana: sidecar_build_counter_matches_stored=true
```

## Artifacts

- `run-metadata.txt` hardware, versions, commit, config and image pins
- `timings.tsv` acknowledgment/queryable/flush separation
- `ingest-parkana.json` parkbench manifest (`ingest-loki.json` absent: --parkana-only)
- `storage-parkana.txt` data and sidecar bytes
- `skipped.txt` explicit list of skipped Loki stages
- `index-build-parkana.txt` sidecar build histogram buckets for source=ingest
- `runtime-parkana.txt` measured container runtime settings (always/interval, workers, resources)
- `config/bench.yaml`, `config/ingest-runtime.yaml`, `config/loki-bench-config.yaml` exact non-secret config
- `query-parkana.json/.md` query results (`query-loki.*` absent: --parkana-only)
- `run.log` full output; `diagnostics.log` only after a failure

Byte definitions and every configuration delta are in docs/benchmarks.md.
Secrets are never written to these artifacts; `secrets.env` is 0600 and removed on cleanup unless --keep.
