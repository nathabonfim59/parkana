# Phase 1 end-to-end check

`scripts/e2e-phase1.sh` is the phase 1 black-box acceptance check: Garage +
Parkana, Loki push JSON/protobuf, Parquet upload, compaction and SIGKILL/WAL
recovery. It starts its own throwaway compose project, reads the Parquet
objects back with DuckDB straight from Garage, and tears the project down.

Phase 2 (Loki parity and the `lokicompare` corpus) has a separate wrapper,
`scripts/e2e-phase2.sh`; see [Phase 2 end-to-end
comparison](#phase-2-end-to-end-comparison). It delegates all stack ownership
to `deploy/compose/validate-phase2.sh` and does not start a second stack.

It is **written but not yet run**. The full e2e remains unrun until the
orchestrator has integrated A + B into an approved branch and the lead approves
the run. The script refuses to touch an existing compose project, so it is safe
to run once approved.

## Prerequisites

- Docker with the Compose v2 plugin, reachable by the current user.
- Go matching `go.mod` (the script builds `./cmd/pushgen`).
- `curl`, GNU coreutils (`timeout`, `date -d @epoch`, `date +%N`).
- DuckDB: the host CLI if present with a resolvable HOME
  (`PARKANA_E2E_DUCKDB_MODE=native` forces it), otherwise the pinned official
  image `duckdb/duckdb:1.5.6`
  (`PARKANA_E2E_DUCKDB_MODE=container` forces it). The image tag was checked on
  Docker Hub; override it with `PARKANA_E2E_DUCKDB_IMAGE` only for another
  verified tag. The image is distroless: no shell, no `Entrypoint`, and
  `CMD ["/duckdb"]`. A plain `docker run IMAGE --version` would therefore try
  to exec `--version` and fail with exit 127, so the script passes
  `--entrypoint /duckdb` on every container invocation (version probe,
  extension setup and queries); only substitute an image with the same
  `duckdb/duckdb` layout. The script installs the `httpfs` extension once into a
  temporary explicit `extension_directory`, so the Docker host needs network
  access to `extensions.duckdb.org` on the first run. The pinned image ships no
  HOME; because DuckDB 1.5 still resolves one before `INSTALL` even when
  `extension_directory` is set, container runs get a private ephemeral tmpfs
  HOME for that requirement only. Native runs keep the user's real HOME. HOME
  is never used as the extension cache.
- The integrated A+B branch, including `deploy/compose/` from B5a.

The e2e never installs Docker or changes system packages.

## Usage

```sh
# offline logic checks; no Docker, DuckDB or services
scripts/e2e-phase1.sh --self-test

# print the resolved plan; no Docker needed
scripts/e2e-phase1.sh --dry-run

# full run (only after the integration is approved)
scripts/e2e-phase1.sh

# debug a failed run: keep the stack and its data volume
scripts/e2e-phase1.sh --keep
```

Useful options/env:

| option | env | default |
|---|---|---|
| `--project NAME` | `PARKANA_E2E_PROJECT` | generated `parkana-e2e-<utc>-<pid>` |
| `--parkana-port` | `PARKANA_HTTP_PORT` | 3100 (busy default auto-picks) |
| `--garage-port` | `PARKANA_S3_PORT` | 3900 (busy default auto-picks) |
| `--total N` | `PARKANA_E2E_TOTAL` | 10000 initial rows |
| `--extra N` | `PARKANA_E2E_EXTRA` | 2000 pre-SIGKILL rows, max 50000 |
| `--bursts N` | `PARKANA_E2E_BURSTS` | 3 (minimum 3) |
| `--rate N` | `PARKANA_E2E_RATE` | 5000 lines/s |
| `--batch N` | `PARKANA_E2E_BATCH` | 100 lines/push |
| `--keep` | — | clean up on exit |

Timeouts are bounded and overridable through `PARKANA_E2E_*_TIMEOUT` variables;
the defaults are generous enough for a first cgo/DuckDB image build. An explicit
port (flag or env) that is already in use is an error; only the defaults are
auto-picked, from 20000-29999.

## Compose contract

The script drives `deploy/compose/compose.yaml` with
`docker compose -p <project> --project-directory deploy/compose -f ...`. It does
not edit the file. The stack must interpolate these variables (defaults keep the
normal development behaviour):

| variable | e2e phase values |
|---|---|
| `PARKANA_HTTP_PORT` | published on `127.0.0.1`; e2e chosen |
| `PARKANA_S3_PORT` | published on `127.0.0.1`; e2e chosen |
| `PARKANA_INGEST_FLUSH_INTERVAL` | 1s / 1h |
| `PARKANA_INGEST_UPLOAD_INTERVAL` | 1s / 1h |
| `PARKANA_INGEST_UPLOAD_MIN` | 1KiB / 1GiB |
| `PARKANA_INGEST_UPLOAD_MAX_AGE` | 1s / 1h |
| `PARKANA_COMPACTOR_INTERVAL` | 1h (idle) / 1s (compact) |
| `PARKANA_COMPACTOR_TARGET_SIZE` | 1B (idle) / 1GiB (merge all) |
| `PARKANA_COMPACTOR_DELETE_DELAY` | 2s |

The preflight fails if a variable is missing from the compose file, and after
each recreation the script inspects the running Parkana container and fails if
a phase value did not reach it. Names that must not change: services `garage`,
`garage-init`, `parkana`; bucket `parkana`; region `garage`; S3 inside the
network `garage:3900`; Parkana inside the network `:3100`. The dev Garage key
defaults to the documented `PARKANA_DEV_GARAGE_KEY_ID` /
`PARKANA_DEV_GARAGE_KEY_SECRET` values; the script never prints either value.

## What it checks

1. **Load.** Builds `pushgen`, starts the stack with the compactor idle
   (interval 1h, target 1B so even a racing startup cycle skips every file),
   pushes `--total` rows in at least three bursts alternating block-snappy
   protobuf and JSON. It waits after each burst until DuckDB sees exactly the
   cumulative row count and a new object, so the uploader cannot merge the
   bursts into one object. Before every burst after the first it also waits
   past the previous observed max timestamp, because pushgen's synthetic range
   can lead the wall clock; this makes the distinct `(_ts, line)` count
   deterministic rather than dependent on burst spacing.
2. **Compaction.** Recreates only Parkana with a 1s compactor, 1GiB target and
   2s delete delay. It waits (bounded, retrying through the old+new
   double-count window) for exactly `--total` distinct `(_ts, line)` rows, the
   same min/max timestamps, fewer objects, and two stable polls. The base
   columns and `_ts` type are rechecked.
3. **SIGKILL recovery.** Recreates Parkana with 1h flush/upload/min-age
   thresholds and an idle compactor (1h, target 1B), pushes `--extra` rows in
   one JSON burst (both wire formats were covered in phase 1) and requires
   every push to be acknowledged with 204 while the container is still alive.
   It proves the rows were not uploaded, disables Docker's restart policy on
   that one throwaway container (`docker update --restart=no`, since compose sets
   `restart: unless-stopped`), `SIGKILL`s only Parkana (exit 137), restarts it
   on the same named volume with short thresholds, and waits for
   exactly `--total + --extra` distinct rows. The earlier min timestamp must be
   unchanged, the max must grow into the extra-row range, and the schema/base
   checks must still pass.

`--extra` is capped at 50000 rows (~6 MiB) so the long-interval phase cannot be
size-sealed by the default 256 MiB memtable or 128 MiB WAL; it must be the
explicit flush/upload tick that is missing. The script waits until the wall
clock has passed the initial synthetic max before the extra push, so the two
data sets cannot share timestamps.

DuckDB reads the objects with
`read_parquet('s3://parkana/default/logs/checkout/*/*/*/*/*.parquet', filename=true)`,
`TimeZone=UTC`, `s3_url_style=path`, `s3_region=garage`, TLS off, and the dev
key. From the host it uses `127.0.0.1:<published S3 port>`; from the pinned
container it joins the compose network and uses `garage:3900`. Container
invocations always pass `--entrypoint /duckdb`, because the pinned image has no
`Entrypoint` and its `CMD` is the DuckDB binary; the version probe keeps its
stderr in the bounded, redacted diagnostics and prints it on failure instead of
reporting a bare "does not run". Credentials are
fed on stdin, never as command-line arguments, and all error output is
redacted. Every invocation runs with `.bail on` and an explicit
`extension_directory`, so a failed `LOAD` or `SET` cannot be followed by a
`SELECT` that makes setup look successful. The cache lives on the host for
native runs and on the mounted `/duckdb-ext` for container runs; the tmpfs
HOME is not the cache.

## Cleanup and safety

- A generated project name is used by default. An existing project (containers,
  labels, the `_default` network, the three declared volumes) is refused.
- On success and failure the script runs `docker compose ... down --volumes
  --remove-orphans` for its own project only. It never runs `prune`, never
  removes images and never touches another project's volumes. A cleanup failure
  makes the script exit 1 when the checks themselves passed; an earlier check
  failure keeps its own exit status.
- `--keep` leaves containers, network and volumes in place and prints the exact
  teardown command. The temporary directory (pushgen binary, DuckDB extension
  cache, logs) is always removed.
- Images built from `Dockerfile`/`garage.Dockerfile` remain after a run; remove
  them manually if disk usage matters. Parallel e2e runs in different worktrees
  also share the compose image tags (`parkana:phase1`,
  `parkana-garage:v2.5.0`), so run them serially unless the compose contract
  gains image-name variables.
- Failures print a bounded, redacted diagnostic: `compose ps`, one `/ready`
  probe, and the last 80 log lines of Parkana, Garage and garage-init.

## Offline checks

`--self-test` exercises the parts that do not need a service: SQL literal
escaping, credential redaction, exact burst splitting, the extra-row cap,
numeric/port validation (rejecting expressions such as `1+1` before Bash
arithmetic), the DuckDB READY marker and stats parser ignoring warning noise,
the container argv (exactly one `--entrypoint /duckdb`, the image immediately
followed by DuckDB's own flags, and no appended binary name), the exact-count
wait (retry then success and timeout), the compaction wait
(rejecting double counts, non-reduced object counts and changed min/max, then
requiring two stable polls), timestamp bounds, the base-column/UTC-microsecond
schema checks, compose project-name validation, cleanup only for a project the
script created (passing only `down --volumes --remove-orphans`), and the exit
status when cleanup fails after otherwise passing checks.

`--dry-run` prints the resolved plan, ports, phase values, DuckDB mode and the
missing-file/compose-contract warnings without starting anything. Both modes are
safe on a machine without Docker or DuckDB.

## Troubleshooting

| symptom | cause |
|---|---|
| `deploy/compose/compose.yaml must interpolate: ...` | B5a has not adopted the nine-variable contract above (or names differ) |
| `parkana env PARKANA_... is ..., want ...` | the compose file does not pass that variable to the container |
| `port ... is already in use` | another dev stack owns an explicitly chosen port; pick another or unset the env override |
| `DuckDB httpfs setup failed` | no network to `extensions.duckdb.org`; preinstall/cache the extension or force the pinned image on a networked host |
| `DuckDB image ... does not run /duckdb --version` | the tag is stale or is not a `duckdb/duckdb` image (that distroless image has no `Entrypoint`, so the binary must be named explicitly); the bounded stderr printed above names the cause |
| `timed out waiting for ...` | profusion of diagnostics follows; inspect the last DuckDB error in the output, or rerun with `--keep` |
| `garage-init failed` | inspect `docker compose -p <project> ... logs garage-init`; the project is kept only with `--keep` |
| `garage did not become healthy` / `garage-init` never runs | compose must let the one-shot init job start before Garage's healthcheck turns green; `service_healthy` on a fresh volume deadlocks, so use `service_started` for `garage-init` |

# Phase 2 end-to-end comparison

`scripts/e2e-phase2.sh` is the reproducible phase 2 wrapper. It is deliberately
thin: `deploy/compose/validate-phase2.sh` remains the **only** owner of the
compose project, readiness, the three finite generators, the Loki count, the
physical Parkana catalog sum, Parkana's unique `index/stats` entries, restart
persistence and cleanup. A real `--full` run needs
Docker with Compose v2, Go matching `go.mod` (the validator builds
`cmd/lokicompare` once before stack startup), plus the validator's curl and
coreutils prerequisites. The wrapper supplies a fresh isolated invocation and,
for `--full`, the comparison corpus (the checked-in Grafana corpus unless
overridden):

```sh
# offline checks; no Docker, Go, network or services
sh -n deploy/compose/validate-phase2.sh
bash -n scripts/e2e-phase2.sh
sh deploy/compose/validate-phase2.sh --selftest
bash scripts/e2e-phase2.sh --self-test

# print the resolved project, ports, corpus and validator argv; touches nothing
bash scripts/e2e-phase2.sh --full --dry-run

# stack/readiness validator run (no comparison)
bash scripts/e2e-phase2.sh --smoke

# full phase 2 acceptance with the repository default corpus
bash scripts/e2e-phase2.sh --full

# full phase 2 acceptance with an explicit corpus override
bash scripts/e2e-phase2.sh --full --compare-corpus /path/to/corpus.json

# keep the validator's stack for debugging; it still owns teardown
bash scripts/e2e-phase2.sh --full --keep

# Boat VM (the worktree is copied, uncommitted diff included)
tm-boat test <worktree> -- bash scripts/e2e-phase2.sh --full
```

The wrapper's offline and mock paths are exercised; a real full-stack
comparison remains a separate, lead-approved acceptance run.

## Corpus default and overrides

`--full` uses `cmd/lokicompare/testdata/grafana-corpus.json` (the checked-in
17-request Grafana corpus) when no corpus was supplied. The default is resolved
from the repository root, not the caller's directory, and must be a readable
file; a checkout without it fails before any container starts.

Precedence is `--compare-corpus` / `--corpus`, then
`PARKANA_E2E_COMPARE_CORPUS`, then the repository default. An explicit path
(flag or environment) is still resolved from the caller's directory before the
validator changes into `deploy/compose`, and it must be a readable file.

An explicitly empty `--compare-corpus ""` is a usage error rather than a silent
fallback to the default. An empty `PARKANA_E2E_COMPARE_CORPUS` is treated like
an unset variable, so the default still applies. `--smoke` never reads a corpus
and rejects corpus/tenant/timeout options.

## What the wrapper supplies

| item | behaviour |
|---|---|
| compose project | generated `parkana-e2e-p2-<utc>-<pid>`; `--project` / `PARKANA_E2E_PROJECT` |
| host ports | Loopback defaults 13100/13900/13101/13245/13000; a busy default is auto-replaced with a free 20000-29999 port; an explicit busy port is an error |
| Garage credentials | fresh random `GK`+24 hex key id and 64 hex secret per run, exported as `PARKANA_DEV_GARAGE_KEY_*`; values are never printed |
| corpus | `--compare-corpus PATH` / `PARKANA_E2E_COMPARE_CORPUS`; default `cmd/lokicompare/testdata/grafana-corpus.json` for `--full` |
| tenant | optional `--tenant ID` / `PARKANA_E2E_TENANT`, passed as `-tenant` |
| request timeout | optional `--timeout DURATION` / `PARKANA_E2E_COMPARE_TIMEOUT`, any positive Go duration such as `30s`, `0.5s`, `+1s` or `0h1m`, passed as `-timeout`; the CLI remains authoritative for zero/negative semantics |
| cleanup | none of its own: the validator's existing trap downs only the project it created |

The wrapper calls the validator exactly once as an argv array (no `eval`, no
arbitrary command hook) and exits with the validator's status unchanged.

## Validator comparison hook

`validate-phase2.sh` has one narrow hook, used only with `--full`. A direct
validator invocation never guesses a corpus: comparison stays opt-in behind the
explicit flag. The wrapper supplies the repository default from the section
above:

```sh
validate-phase2.sh --full --compare-corpus PATH \
  [--compare-tenant ID] [--compare-timeout DURATION]
```

The validator:

1. rejects `--compare-corpus` with `--smoke`/`--selftest`, an empty or missing
   corpus, or tenant/timeout without a corpus, before creating a project;
   `--full` without `--compare-corpus` runs every stack check and skips the
   comparison;
2. builds `./cmd/lokicompare` exactly once into its temporary directory before
   any compose build/up, so a build failure cannot leave a half-started stack;
3. captures the generated-data `START` in Unix nanoseconds immediately before
   the phase 2 `up -d` (the generators start with the stack), whether or not a
   comparison is enabled;
4. runs its existing readiness, version, config, datasource, generator,
   Loki-count, physical catalog-sum (excess is reported as `retry duplicates
   stored: N`), unique Parkana `index/stats` entries and restart checks;
5. captures `RANGE_END` after the initial Loki and catalog barriers, checks
   unique `index/stats` entries over `START..RANGE_END` before and after
   restart, then captures the comparator `END` after every upload/count/restart
   barrier and runs exactly one frozen
   `<tmp>/lokicompare -left <Loki> -right <Parkana> -corpus PATH \
   -start START -end END [-tenant ID] [-timeout DURATION]`;
6. exits with the binary's status: `0` equal, `1` semantic difference, `2`
   invocation/request/response failure. Any other status is reported as a
   comparison failure (`2`). On every path the existing cleanup trap runs and
   removes the temporary binary.

The shared comparison range includes all three generated datasets (base, job
and corpus) and every corpus request uses the same `START`/`END`. `START`
precedes all generation. Two ends are deliberate: `RANGE_END` closes the unique
`index/stats` range once the initial Loki (wall-clock) and catalog barriers
have passed, and that `START..RANGE_END` pair is reused unchanged after
restart; the comparator `END` is captured later, after the restart persistence
checks, so the comparison covers the whole run. The current generated dataset
is 22200 lines (20000 + 1000 + 1200), but the comparison and readiness checks
do not hard-code that total.

`--keep` still belongs to the validator. The wrapper never runs
`docker compose down`, so a comparator failure cannot leak or double-delete a
project. Failure diagnostics are only read after this run has created its stack:
a refused project or a pre-start comparator build failure never reads another
project's compose logs. A failed cleanup after otherwise passing checks makes
the validator exit `1`; an earlier check or comparison status is preserved.

## Exit status

| status | meaning |
|---|---|
| 0 | validator passed; with `--full`, every corpus entry compared equal |
| 1 | a validator check failed, `lokicompare` found a semantic difference, or cleanup failed after otherwise passing checks |
| 2 | invalid invocation, or a `lokicompare` transport/HTTP/JSON failure; also the wrapper's own usage errors |
| other | propagated unchanged from the validator (the wrapper does not swallow it) |

## Self-tests

`validate-phase2.sh --selftest` now includes real subprocess mock runs with a
fake Docker, curl, Go and (for timeout paths) date:

- generator failure (`pushgen-base` exits 7), partial upload (catalog never
  reaches the total), readiness timeout, unhealthy Parkana datasource;
- physical catalog excess (`expected + 100`) passes and prints
  `retry duplicates stored: 100` once per readiness pass; unique Parkana
  `index/stats` entries below or above the expected total fail, as do wrong
  Loki counts and missing, negative, text, ambiguous, suffixed, spaced,
  leading-zero, nested, extra-key or trailing-junk `entries` fields; the
  selector and full start/end argv are asserted, START precedes up/generators,
  and the same range is reused after restart;
- the comparator is built before compose startup; a build failure never reaches
  compose build/up and never reads compose logs;
- comparison stays opt-in: `--full` without `--compare-corpus` completes all
  stack checks, never invokes Go, and never builds or runs the comparator;
- the compiled comparator exits 0/1/2, including argc and every argument
  boundary (the corpus path contains spaces), one shared increasing range, and
  ordering after restart persistence;
- comparator failures trigger the actual owned cleanup trap and down only that
  project; a cleanup failure after success becomes exit 1 while an earlier
  comparator exit 2 is preserved;
- a refused existing project is never brought down, never reads compose logs
  and never reaches the comparator; relative entrypoint and repo-root-relative
  corpus resolution still clean their own project;
- partial-upload diagnostics redact the Garage secret.

`scripts/e2e-phase2.sh --self-test` mocks the validator as a subprocess and
checks the repository default corpus, flag-over-environment-over-default
precedence, an empty environment variable behaving as unset, an explicitly
empty `--compare-corpus ""` being a usage error, caller-relative explicit
corpus resolution, Go-duration acceptance (including `0.5s`, `.5s`, `+1s`,
`0h1m`), numeric/project validation, all-five-port uniqueness in dry-run,
busy-port auto-pick (and explicit-port refusal), random credential shape/export,
validator argc and each argv boundary with a corpus path containing spaces,
0/1/2/other exit propagation, secret-free dry run, no second cleanup owner, and
relative entrypoint resolution. `--dry-run` works without Docker or Go and
prints which corpus source was chosen.

## Troubleshooting

| symptom | cause |
|---|---|
| `--compare-corpus needs a non-empty path` | the flag was passed with an empty value; omit it to use the default (an empty environment variable is treated as unset) |
| `default compare corpus is missing or unreadable` | the checkout lacks `cmd/lokicompare/testdata/grafana-corpus.json`; restore it or pass an explicit corpus |
| `compare corpus is not a readable file` | explicit override path typo, or the corpus has not been produced; the wrapper fails before starting the stack |
| `go is required for --compare-corpus` | the validator builds `cmd/lokicompare` once before stack startup; install Go matching `go.mod` on the Docker host/VM |
| `host port ... is already in use` | an explicit port was given and is busy; omit the override to auto-pick |
| `lokicompare: semantic difference(s)` | exit 1 from the CLI; inspect the per-entry `DIFF` lines in the validator output |
| `lokicompare: invalid invocation or request/response failure` | exit 2 from the CLI; inspect the `FAIL` line and whether Parkana/Loki were healthy |
| `keeping project ...` with `--keep` | expected; use the printed validator teardown command, never a second wrapper cleanup |
