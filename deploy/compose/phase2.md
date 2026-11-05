# Phase 2 development stack

`deploy/compose/compose.yaml` extends the phase 1 Garage + Parkana stack with
Loki, Alloy, Grafana and three finite push generators. The phase 2 services are
behind the `phase2` compose profile, so a plain `up` still starts exactly
`garage`, `garage-init` and `parkana` and the phase 1 scripts keep working.

```sh
# phase 1 (unchanged)
docker compose up --build

# phase 2
docker compose --profile phase2 up --build
```

Everything is development-only: Garage uses fixed throwaway credentials and
Grafana enables anonymous admin for the local network. Never deploy this file
as-is.

## Services and loopback ports

| service | image | host port (default) | purpose |
|---|---|---|---|
| `garage` | `dxflrs/garage:v2.5.0` | `PARKANA_S3_PORT` 3900 | S3-compatible object store |
| `garage-init` | same helper image | — | one-shot layout/key/bucket init |
| `parkana` | built `parkana:phase1` | `PARKANA_HTTP_PORT` 3100 | Parkana (all roles) |
| `loki` | `grafana/loki:3.7.9` | `LOKI_HTTP_PORT` 3101 | baseline, filesystem + TSDB |
| `alloy` | `grafana/alloy:v1.21.3` | `ALLOY_HTTP_PORT` 12345 | Loki push receiver, two-endpoint fan-out |
| `grafana` | `grafana/grafana:13.3.0` | `GRAFANA_HTTP_PORT` 3000 | Explore UI, provisioned datasources |
| `pushgen-base` | built `parkana-pushgen:phase2` | — | 20000-line api/web corpus |
| `pushgen-job` | built `parkana-pushgen:phase2` | — | 1000-line job-only fallback corpus |
| `pushgen-corpus` | built `parkana-pushgen:phase2` | — | 1200-line deterministic comparison corpus |

All published ports bind to `127.0.0.1`. Every port has an environment
override, so a second stack can run beside the first.

## Pinned images

The tag documents the release; the digest makes the build reproducible. All
digests are manifest-list digests, verified on Docker Hub on 2026-11-04.

| image | release | digest |
|---|---|---|
| `golang:1.27.3-bookworm` | Go 1.27.3 | `sha256:d9b6a1beb9474adc2a1ce842573639b44b2a0dd49b6669f51d3821baf9ec37c9` |
| `debian:bookworm-slim` | current bookworm-slim snapshot | `sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3` |
| `dxflrs/garage:v2.5.0` | Garage 2.5.0 | `sha256:677e5b758e4425a325b811c425c026bf35a4bf42f7ddbaa773de3f4fd779a5a5` |
| `grafana/loki:3.7.9` | [Loki v3.7.9](https://github.com/grafana/loki/releases/tag/v3.7.9) | `sha256:0db216b630ae1f348a1584c67f3cead8b4c1fc839169a39501db664744158e28` |
| `grafana/alloy:v1.21.3` | [Alloy v1.21.3](https://github.com/grafana/alloy/releases/tag/v1.21.3) | `sha256:8ab871206bd2d8f89277f53abffee5f452a293980788d52f85fa183a87a505a4` |
| `grafana/grafana:13.3.0` | [Grafana v13.3.0](https://github.com/grafana/grafana/releases/tag/v13.3.0) | `sha256:488984b716c0862639f91a698adbe2862677a5a07bad42da5f03f3030883b205` |

The `*_IMAGE` variables allow another verified tag/digest, but the default is
always exact. `parkana:phase1` and `parkana-pushgen:phase2` are built locally;
their base images are pinned above.

### Grafana Loki datasource plugin

Grafana 13.3.0 does not bundle the Loki datasource. Its default startup path
preinstalls the unversioned `loki` plugin, which would download whatever the
catalog serves that day. This stack pins the plugin synchronously instead:

```text
GF_PLUGINS_PREINSTALL_SYNC=loki@13.2.5@https://grafana.com/api/plugins/loki/versions/13.2.5/download
GF_PLUGINS_PREINSTALL_AUTO_UPDATE=false
GF_PLUGINS_CHECK_FOR_PLUGIN_UPDATES=false
```

The first start needs outbound HTTPS to `grafana.com` (and its download host).
The plugin is cached in the `grafana-data` volume. `validate-phase2.sh` asserts
the running version through `/api/plugins/loki/settings` and prints it with
the Grafana and Loki versions; it does not trust the environment alone.

## Corpus

Three generators use the `cmd/pushgen` binary. B3a changes no Go code; the
deterministic `-profile=corpus` fixture and the microsecond timestamp
alignment come from part A commit `880eb87`. All profiles align the synthetic
start to whole microseconds, so the same corpus has identical timestamps in
Loki and Parkana.

```sh
# 20000 lines: service_name=api and service_name=web, 10000 each
pushgen -url=http://alloy:3100/loki/api/v1/push -format=json \
  -stream=api -stream=web \
  -label=job=frontend -label=env=dev -label=level=info \
  -total=20000 -rate=1000 -batch=100

# 1000 lines: job=legacy, no service_name (exercises the stream fallback)
pushgen -url=http://alloy:3100/loki/api/v1/push -format=json \
  -stream=legacy -stream-label=job \
  -label=env=dev -total=1000 -rate=1000 -batch=100

# 1200 lines: deterministic comparison corpus (part A profile)
pushgen -url=http://alloy:3100/loki/api/v1/push -profile=corpus -format=json \
  -stream=corpus -label=job=corpus -label=env=dev \
  -total=1200 -rate=1000 -batch=100
```

The corpus fixture breakdown for one 1200-line run: JSON 423, logfmt 384,
plain 393; level info 448, warn 413, error 339; pods pod-a and pod-b; every
line carries `trace_id` and `request_id` structured metadata. The profile adds
its own `format`, `level` and `pod` stream labels per record.

Expected total: **22200 lines** by default. The validation derives it as
base + job + corpus; setting `PARKANA_GENERATOR_EXPECTED_TOTAL` is optional and
must match that sum (a conflict is rejected). Compose exposes the counts as
variables:

| variable | default |
|---|---|
| `PARKANA_GENERATOR_BASE_TOTAL` | 20000 |
| `PARKANA_GENERATOR_JOB_TOTAL` | 1000 |
| `PARKANA_GENERATOR_CORPUS_TOTAL` | 1200 (part A corpus profile) |
| `PARKANA_GENERATOR_EXPECTED_TOTAL` | unset: derived as base + job + corpus; when set it must equal that sum |
| `PARKANA_GENERATOR_RATE` | 1000 lines/s |
| `PARKANA_GENERATOR_BATCH` | 100 lines/request |

The corpus service is a third one-shot generator; its invocation is fixed in
`compose.yaml` and only the totals/rate/batch are variables.

## Data path and parity

Alloy's `loki.source.api` receives pushes on port 3100 and `loki.write` fans
the **same entry** out to `http://loki:3100/loki/api/v1/push` and
`http://parkana:3100/loki/api/v1/push`. `use_incoming_timestamp = true`
preserves pushgen's microsecond-aligned, millisecond-spaced timestamps; Alloy
does not add labels (it only drops names starting with `__`). Each endpoint
retries transient failures without changing the entry, so late delivery stays
identical.

Loki's config allows structured metadata but disables its own ingest-time
discovery (`discover_log_levels: false`, `discover_service_name: []`) because
Parkana does not detect levels or invent a service name yet. Without that,
Loki would add `detected_level` and, on the job-only stream, a structured
`service_name` that Parkana cannot produce. Enable discovery again when
Parkana grows the same behaviour.

Parkana runs with `PARKANA_INGEST_UPLOAD_MAX_AGE=30s`: queries see data only
after the ingester has uploaded and cataloged it.

Alloy's `loki.write` sends protobuf with `Content-Encoding: snappy`.
`validate-phase2.sh --smoke` requires Parkana's ingest to accept it: the run
fails unless the physical catalog reaches at least the expected row count,
Parkana's unique `index/stats` entries equal it, and Alloy logs contain no
Parkana ingest rejection. Physical excess is reported as `retry duplicates
stored: N` rather than failing.

The validation keeps the compactor idle (`PARKANA_COMPACTOR_INTERVAL=1h`,
`PARKANA_COMPACTOR_TARGET_SIZE=1B`) while it counts rows, because compaction's
old+new overlap would temporarily double the catalog sum and be
indistinguishable from retry duplicates.

## Grafana logs volume

Explore's histogram is a supplementary **range** query to
`/loki/api/v1/query_range`, not `/loki/api/v1/index/volume_range`. In the
Loki datasource plugin (`grafana/grafana-loki-datasource`, tag v13.2.5,
commit `e1c13be93b47207460dd12d2a61b6a56849d0612`, `src/datasource.ts`
`getSupplementaryQuery`) it builds:

```text
refId:                  logVolume_<original refId>
queryType:              Range
supportingQueryType:    LogsVolume        (wire value "logsVolume")
expr:                   sum by (level, detected_level) (count_over_time(<original log query> | drop __error__[$__auto]))
```

`$__auto` is a literal Grafana macro in the expression; the datasource
interpolates it to the request step before sending. The `| drop __error__`
stage is appended when the original query has no error stage. Parkana's
`query_range` therefore needs `sum by`, `count_over_time`, the step the macro
becomes, and `drop`. `/index/volume` and `/index/volume_range` remain used by
Grafana's Log Volume Explorer (and the planned `lokicompare` volume cases), not
by the Explore histogram.

This is source-verified. The live request must still be captured from the
running Grafana 13.3.0 + pinned plugin (see below); until then the UI claim is
provisional.

## Validation

```sh
# local syntax/selftest (no Docker or network)
sh -n deploy/compose/validate-phase2.sh deploy/compose/pushgen-wait.sh
sh deploy/compose/validate-phase2.sh --selftest
sh deploy/compose/pushgen-wait.sh --selftest

# Boat VM: infrastructure, Loki, Alloy, Grafana provisioning, generators,
# Loki count, physical catalog and unique Parkana index/stats entries
tm-boat test <worktree> -- sh deploy/compose/validate-phase2.sh --smoke

# Boat VM: smoke + restart persistence
tm-boat test <worktree> -- sh deploy/compose/validate-phase2.sh --full
```

`--smoke` verifies Garage, Parkana, Loki, Alloy, Grafana, the pinned plugin, all
three generators, the provisioned datasources, the Loki corpus count, the
physical Parkana catalog sum (read-only `sqlite3` query from the pushgen image
against the `parkana-data` volume; at least the expected rows, with excess
reported as retry duplicates stored) and Parkana's unique `index/stats` entries
over the full generated range. `--full` repeats the Loki-count, catalog and
unique-entry checks after a restart.

Grafana's first start can spend several minutes in SQLite migrations even
though the container is healthy. Readiness, version and datasource waits use
`PARKANA_VALIDATE_GRAFANA_READY_TIMEOUT` seconds (default 600, canonical
positive integer at most 86400), measured as real elapsed time; the same budget
is reused after restart. A shorter budget can be supplied deliberately, but a
cold start that needs more than 600s fails rather than being skipped.

`--selftest` exercises the pure checks and mocked failure paths: canonical
positive totals and conflicts, wrong plugin versions, a Loki config with
discovery enabled, non-zero generator exit, refusal of an existing compose
project, real subprocess cleanup ownership (an existing project must never be
brought down; a fresh project whose `up` fails must have only its own project
removed by the trap), physical catalog excess (passes and reports the exact
count), unique entries below or above the total (fail), wrong Loki counts
(fail) and missing, negative, text, ambiguous, suffixed, spaced, leading-zero,
nested, extra-key or trailing-junk `entries` bodies (fail). The mock also
asserts the full start/end argv and that the unique range starts before
`up`/generators and is reused after restart. `pushgen-wait.sh --selftest`
separately checks the wall-clock readiness deadline, canonical positive
timeout parsing and the failure message.

The script uses a generated compose project and loopback ports 13100/13900/
13101/13245/13000 by default, refuses an existing project or published port,
and removes only its own project. Use `--keep` to inspect a failure.

To capture the live logs-volume request, enable Grafana request logging (for
example `GF_LOG_FILTERS=datasource.loki:debug`), open Explore on the Loki
datasource with `{job="frontend"}`, and look for a request to
`/loki/api/v1/query_range` whose query contains `count_over_time`, `sum by`
and `supportingQueryType=logsVolume`. Record the exact query string and step
in the milestone report. The same check against Parkana is part of milestone 4
(browser evidence).
