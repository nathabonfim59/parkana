# Parkana phase 3 benchmark run

- project: `sol-a-p4-foundation`
- preset: `small` (2000000000 decimal raw line bytes)
- output: `/tmp/sol-a-p4-sweep/foundation`
- commit: `0b3cc17bb2a2cfad1b37cab47cd3a22947d40c34` (dirty files: 0)
- app source: commit `0b3cc17bb2a2cfad1b37cab47cd3a22947d40c34` (dirty files: 0); driver: `scripts/bench-phase3.sh` sha256 `9c9f68b371feb5eda0dd3be7a9d1d433946a4cc95b6cca2d1853566868d51d03`
- actual corpus interval: start=2026-11-05T00:17:36.151838Z through=2026-11-05T00:39:29.128238Z (start source=explicit)
- replay: `bash scripts/bench-phase3.sh --start 2026-11-05T00:17:36.151838Z --seed 8098146604754141185 --step 100us --bytes 2000000000 --streams api,checkout,search,worker --instances-per-service 4 --batch-lines 1250 --concurrency 16 --legacy-runtime --output-dir /tmp/parkana-bench-replay-UNIQUE` (replace UNIQUE with a fresh output name before running)
- resources: 4 CPU / 8g memory per backend container
- batch: 1250 entries per push request per label set (concurrency 16)
- instances per service: requested 4 (source=explicit)
- streams: requested tool-default (source=tool-default); actual names/count are recorded per backend as ingest_*_corpus_streams
- index max grams: 2097152 (default)
- ingest runtime: legacy source contract, known always/F1/U1; no phase-4 runtime flags (not container-effective)
- runtime flag probe: support=absent source=bounded-parkana-help sha256=8b1e549e53bc6ca1b8c6960a9c20722bfcf030d1e8163fc68e3d8855e82286e8
- ingest memory peak (cgroup-v2 high-water through ingest, read before stop; not the limit): 4655681536 bytes (source=cgroup-v2-memory.peak)
- backends: parkana only (`--parkana-only`; Loki explicitly skipped)
- query stage: run (B2b contract)

## Ingest

| backend | ack report s | ack wall s | flush s | queryable delay s |
|---|---:|---:|---:|---:|
| parkana | 18.812757328 | 18.819 | — | 76.744 |

Loki was skipped entirely (`--parkana-only`); no Loki timing row is reported.

### Container CPU (cgroup-v2 usage_usec, ingest command only)

| backend | before | after | delta | wall s | source |
|---|---:|---:|---:|---:|---|
| parkana | 70756 | 55521199 | 55450443 | 18.880 | cgroup-v2-cpu.stat |

Container CPU is the cgroup-v2 usage_usec delta across the parkbench ingest command, not component attribution. Unavailable counters are never reported as zero.

## Storage

```
parkana: backend=parkana
parkana: raw_line_bytes=2000000000
parkana: data_object_bytes=412770285
parkana: compressed_parquet_bytes=412770285
parkana: parquet_files=8
parkana: sidecar_objects=8
parkana: sidecar_object_bytes=922904
parkana: sidecar_object_bytes_source=garage_bucket_total_minus_catalog_data
parkana: sidecar_build_counter_bytes=922904
parkana: sidecar_build_counter_source=parkana_index_bytes_total (cumulative builds, not stored objects)
parkana: index_build_duration_seconds_count_ingest=8.000000000
parkana: index_build_duration_seconds_sum_ingest=22.107193183
parkana: index_build_duration_seconds_mean_ingest=2.763399
parkana: index_build_duration_buckets_file=index-build-parkana.txt
parkana: local_workdir_bytes_including_wal_cache_catalog=506398518
parkana: data_bytes_to_raw_line_bytes=0.206385
parkana: sidecar_bytes_to_data_bytes=0.002236
parkana: sidecar_bytes_to_compressed_parquet_bytes=0.002236
parkana: sidecar_bytes_to_raw_line_bytes=0.000461
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
