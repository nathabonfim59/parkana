# Parkana phase 3 benchmark run

- project: `p4-final-hot`
- preset: `custom` (4000000000 decimal raw line bytes)
- output: `/home/user/.cache/parkana-p4/sol-a-p4/final-hot/run`
- commit: `46593319414965de857407d0db6f0e79e4fd55dd` (dirty files: 0)
- app source: commit `46593319414965de857407d0db6f0e79e4fd55dd` (dirty files: 0); driver: `scripts/bench-phase3.sh` sha256 `456d0a0e4374b1eb726a6a802b2ee7fdb802d4b15331ce4898707b2938124514`
- actual corpus interval: start=2026-11-03T00:00:00Z through=2026-11-03T00:22:07.72885Z (start source=explicit)
- replay: `bash scripts/bench-phase3.sh --start 2026-11-03T00:00:00Z --seed 8098146604754141185 --step 50us --bytes 4000000000 --streams api --instances-per-service 16 --batch-lines 1250 --concurrency 16 --wal-sync always --flush-concurrency 4 --upload-concurrency 4 --output-dir /tmp/parkana-bench-replay-UNIQUE` (replace UNIQUE with a fresh output name before running)
- resources: 4 CPU / 8g memory per backend container
- batch: 1250 entries per push request per label set (concurrency 16)
- instances per service: requested 16 (source=explicit)
- streams: requested api (source=explicit); actual names/count are recorded per backend as ingest_*_corpus_streams
- index max grams: 2097152 (default)
- ingest runtime requested: wal_sync=always (explicit) interval=1s (default) flush_workers=4 (default) upload_workers=4 (default)
- ingest runtime effective (container inspect): wal_sync=always interval=1s flush_workers=4 upload_workers=4 cpus=4.000 memory_bytes=8589934592
- runtime flag probe: support=present source=bounded-parkana-help sha256=549a9bc6685094c73a7342707c521a998054289c14d7001258e49a37e8816f26
- ingest memory peak (cgroup-v2 high-water through ingest, read before stop; not the limit): 8466714624 bytes (source=cgroup-v2-memory.peak)
- ingest oom_kill (cgroup-v2 memory.events, read before stop): 0 (source=cgroup-v2-memory.events)
- backends: parkana only (`--parkana-only`; Loki explicitly skipped)
- query stage: skipped (`--skip-queries`)

## Ingest

| backend | ack report s | ack wall s | flush s | queryable delay s |
|---|---:|---:|---:|---:|
| parkana | 30.939500666 | 30.946 | — | 109.702 |

Loki was skipped entirely (`--parkana-only`); no Loki timing row is reported.

### Container CPU (cgroup-v2 usage_usec, ingest command only)

| backend | before | after | delta | wall s | source |
|---|---:|---:|---:|---:|---|
| parkana | 65163 | 101292485 | 101227322 | 31.001 | cgroup-v2-cpu.stat |

Container CPU is the cgroup-v2 usage_usec delta across the parkbench ingest command, not component attribution. Unavailable counters are never reported as zero.

## Storage

```
parkana: backend=parkana
parkana: raw_line_bytes=4000000000
parkana: data_object_bytes=800568602
parkana: compressed_parquet_bytes=800568602
parkana: parquet_files=3
parkana: sidecar_objects=3
parkana: sidecar_object_bytes=344660
parkana: sidecar_object_bytes_source=garage_bucket_total_minus_catalog_data
parkana: sidecar_build_counter_bytes=344660
parkana: sidecar_build_counter_source=parkana_index_bytes_total (cumulative builds, not stored objects)
parkana: index_build_duration_seconds_count_ingest=3.000000000
parkana: index_build_duration_seconds_sum_ingest=17.569836266
parkana: index_build_duration_seconds_mean_ingest=5.856612
parkana: index_build_duration_buckets_file=index-build-parkana.txt
parkana: local_workdir_bytes_including_wal_cache_catalog=873243762
parkana: data_bytes_to_raw_line_bytes=0.200142
parkana: sidecar_bytes_to_data_bytes=0.000431
parkana: sidecar_bytes_to_compressed_parquet_bytes=0.000431
parkana: sidecar_bytes_to_raw_line_bytes=0.000086
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
- `run.log` full output; `diagnostics.log` only after a failure

Byte definitions and every configuration delta are in docs/benchmarks.md.
Secrets are never written to these artifacts; `secrets.env` is 0600 and removed on cleanup unless --keep.
