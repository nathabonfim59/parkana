#!/usr/bin/env bash
#
# scripts/bench-phase3.sh - isolated Parkana vs Loki phase 3 benchmark runner.
#
# One invocation owns one fresh Compose project containing only Garage, Parkana
# and Loki (plus one-shot Garage init jobs and a one-shot sqlite tool).  It
# ingests the same deterministic parkbench corpus into each backend
# sequentially, waits for exact acknowledged visibility, records storage and
# timing, then (once cmd/parkbench query is merged) runs the identical query
# corpus against each backend.  Backends never run together: Parkana is
# stopped before Loki starts and each is restarted on the same machine only
# for its query phase.
#
# Safety:
#   * refuses to run when the Compose project name or the output directory
#     already exists (containers, volumes, networks or a bare directory),
#   * refuses to reuse images/volumes from a previous project,
#   * generates fresh Garage buckets and credentials per backend for this run,
#     writes them only to a 0600 env file and never prints their values,
#   * removes only the project it just created, and only on that run's trap.
#
# No real service is touched by --dry-run or --self-test.  --self-test mocks
# Docker, HTTP and parkbench to exercise argument, readiness, metric-absence,
# command-construction, secret and exit-propagation paths offline.
#
# Usage: scripts/bench-phase3.sh [options]
#   --preset small|medium|full   corpus size (default small: 2,000,000,000 raw
#                                line bytes; medium: 10,000,000,000; full:
#                                25,000,000,000)
#   --bytes N                    exact raw line bytes (conflicts with a preset;
#                                preset values auto-identify; others report
#                                preset=custom)
#   --allow-full                 required with --preset full
#   --output-dir DIR             fresh artifact directory (default:
#                                $PARKANA_BENCH_OUTPUT_ROOT/<project>)
#   --project NAME               fresh Compose project (default: generated)
#   --parkbench PATH             prebuilt parkbench binary (default: build it)
#   --source-commit REV          reviewed source commit (default: git HEAD or
#                                PARKANA_BENCH_SOURCE_COMMIT; else unavailable)
#   --source-dirty 0|1|unknown   reviewed dirty state (default: git status or
#                                PARKANA_BENCH_SOURCE_DIRTY; else unknown)
#   --parkana-port PORT          Parkana HTTP host port (default 13100)
#   --garage-port PORT           Garage S3 host port (default 13900)
#   --loki-port PORT             Loki HTTP host port (default 13101)
#   --cpus N                     CPU limit per backend container (default 4)
#   --memory SIZE                memory limit per backend container (default 8g)
#   --seed N                     parkbench corpus seed (default: tool default)
#   --start TIME                 corpus record-0 timestamp (default: derived;
#                                the second backend reuses the first manifest)
#   --step DURATION              timestamp step (default 100us)
#   --batch-lines N              max entries per push request, per label set
#                                (default 1250)
#   --instances-per-service N    deterministic instance label values per
#                                stream (omitted: parkbench tool default)
#   --streams LIST               comma-separated stream names; omitted keeps
#                                the four-stream corpus-v3 tool default. A
#                                single hot stream with 16 senders:
#                                --streams api --instances-per-service 16
#   --reuse-manifest PATH        reuse ONLY corpus.start from a prior ingest
#                                manifest; seed, step and bytes stay the current
#                                flags. Conflicts with --start.
#   --legacy-runtime             foundation0b3cc17-style binary with no phase-4
#                                runtime flags; known always/F1/U1 baseline,
#                                never claimed as container-effective
#   --concurrency N              max in-flight pushes (default 16)
#   --repetitions N              query repetitions (default 5)
#   --warmups N                  query warmups (default 2)
#   --query-timeout DURATION     per-query timeout (default 120s)
#   --classes LIST               query classes (default frequent,rare,absent,short_4byte)
#   --windows LIST               query windows (default short,long)
#   --tenant ID                  X-Scope-OrgID for ingest and query
#   --ready-timeout SECONDS      HTTP readiness budget (default 300)
#   --visibility-timeout SECONDS exact-count budget (default 7200)
#   --flush-timeout SECONDS      Loki flush budget (default 600)
#   --parkana-only               benchmark Parkana only; skip every Loki
#                                service, init, ingest, flush, visibility,
#                                storage and query path (default: both)
#   --index-max-grams N          sidecar MaxGrams cap (default 2097152;
#                                positive decimal int64)
#   --wal-sync always|interval   WAL durability mode (default always; interval
#                                acknowledges after the WAL write, before the
#                                periodic --wal-sync-interval fsync, so live
#                                tail can show lines a crash before the next
#                                fsync loses)
#   --wal-sync-interval DURATION interval-mode fsync period (default 1s; one or
#                                more ns/us/µs/ms/s/m/h components, ≤9 fractional
#                                digits, whole nanoseconds, no overflow)
#   --flush-concurrency N        local Parquet part writers (default 4; 1..64)
#   --upload-concurrency N       independent upload receipts in flight; each
#                                is one merge, sidecar build and object Put
#                                (default 4; 1..64)
#   --skip-build                 do not build images or parkbench
#   --skip-queries               stack/ingest/storage smoke; no query stage
#   --keep                       keep this run's stack/secrets for debugging
#   --dry-run                    print the resolved plan, touch no Docker/Go
#   --self-test                  offline mock regression tests, touch nothing
#   -h, --help                   show this help
#
set -euo pipefail

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
readonly SCRIPT_PATH="$SCRIPT_DIR/$(basename -- "${BASH_SOURCE[0]}")"
readonly COMPOSE_FILE="$REPO_ROOT/deploy/compose/bench.yaml"
readonly RUNTIME_OVERLAY="$REPO_ROOT/deploy/compose/ingest-runtime.yaml"
readonly LOKI_CONFIG="$REPO_ROOT/deploy/compose/loki/bench-config.yaml"
readonly DEFAULT_OUTPUT_ROOT="${PARKANA_BENCH_OUTPUT_ROOT:-${TMPDIR:-/tmp}/parkana-bench}"
readonly SMALL_BYTES=2000000000
readonly MEDIUM_BYTES=10000000000
readonly FULL_BYTES=25000000000
readonly DEFAULT_INDEX_MAX_GRAMS=2097152
readonly DEFAULT_WAL_SYNC="always"
readonly DEFAULT_WAL_SYNC_INTERVAL="1s"
readonly DEFAULT_FLUSH_CONCURRENCY=4
readonly DEFAULT_UPLOAD_CONCURRENCY=4
# Mirrors the ingester's own upper bound for both worker counts.
readonly MAX_RUNTIME_CONCURRENCY=64
# corpus v3 runs one sequential sender per label set, so this is the maximum
# entries in one push request for each label set, not for a whole stream.
readonly DEFAULT_BATCH_LINES=1250
# parkbench bounds generated full label sets at 4096; its own
# -instances-per-service validation uses the same per-field bound.
readonly MAX_INSTANCES_PER_SERVICE=4096
# parkbench bounds generated full label sets (streams times instances) at the
# same 4096 value.
readonly MAX_LABEL_SETS=4096
# One bounded read of the container's cgroup-v2 high-water mark; no sampler.
readonly MEMORY_PEAK_TIMEOUT=10
readonly MAX_INT64=9223372036854775807
readonly MAX_PROJECT_LEN=63
readonly TOOLS_IMAGE="parkana-bench-tools:phase3"
readonly GARAGE_IMAGE_PIN="docker.io/dxflrs/garage:v2.5.0@sha256:677e5b758e4425a325b811c425c026bf35a4bf42f7ddbaa773de3f4fd779a5a5"
readonly LOKI_IMAGE_PIN="grafana/loki:3.7.9@sha256:0db216b630ae1f348a1584c67f3cead8b4c1fc839169a39501db664744158e28"

# Command overrides exist for the offline self-tests only.  A real run always
# uses docker/parkbench from PATH or the explicit --parkbench path.
DOCKER="${DOCKER:-docker}"

# Compose reads its variable file for interpolation, but shell variables win
# over --env-file.  compose() removes these names from the environment so a
# stale export can never make two runs share buckets, credentials or ports.
readonly COMPOSE_ENV_VARS=(
  PARKANA_BENCH_CPUS
  PARKANA_BENCH_MEMORY
  PARKANA_BENCH_PARKANA_PORT
  PARKANA_BENCH_GARAGE_PORT
  PARKANA_BENCH_LOKI_PORT
  PARKANA_BENCH_PARKANA_BUCKET
  PARKANA_BENCH_PARKANA_ACCESS_KEY
  PARKANA_BENCH_PARKANA_SECRET_KEY
  PARKANA_BENCH_LOKI_BUCKET
  PARKANA_BENCH_LOKI_ACCESS_KEY
  PARKANA_BENCH_LOKI_SECRET_KEY
  PARKANA_BENCH_GARAGE_KEY_ID
  PARKANA_BENCH_GARAGE_KEY_SECRET
  LOKI_BENCH_GARAGE_KEY_ID
  LOKI_BENCH_GARAGE_KEY_SECRET
  PARKANA_BENCH_INDEX_MAX_GRAMS
  PARKANA_BENCH_WAL_SYNC
  PARKANA_BENCH_WAL_SYNC_INTERVAL
  PARKANA_BENCH_FLUSH_CONCURRENCY
  PARKANA_BENCH_UPLOAD_CONCURRENCY
)

log() { printf '[bench-p3] %s\n' "$*"; }
warn() { printf '[bench-p3] WARN: %s\n' "$*" >&2; }
fail() { printf '[bench-p3] ERROR: %s\n' "$*" >&2; exit 1; }
usage_fail() { printf '[bench-p3] ERROR: %s\n' "$*" >&2; exit 2; }

usage() {
  sed -n '/^# Usage:/,/^#   -h, --help/p' "$0" | sed 's/^# \{0,1\}//'
}

init_defaults() {
  PRESET=small
  PRESET_EXPLICIT=0
  TARGET_BYTES=$SMALL_BYTES
  BYTES_EXPLICIT=0
  ALLOW_FULL="${PARKANA_BENCH_ALLOW_FULL:-0}"

  OUTPUT_DIR=""
  OUTPUT_DIR_EXPLICIT=0
  PROJECT=""
  PROJECT_EXPLICIT=0
  KEEP=0
  DRY_RUN=0
  SELFTEST=0
  SKIP_BUILD=0
  SKIP_QUERIES=0
  PARKANA_ONLY=0
  INDEX_MAX_GRAMS=$DEFAULT_INDEX_MAX_GRAMS
  INDEX_MAX_GRAMS_EXPLICIT=0
  WAL_SYNC=$DEFAULT_WAL_SYNC
  WAL_SYNC_EXPLICIT=0
  WAL_SYNC_INTERVAL=$DEFAULT_WAL_SYNC_INTERVAL
  WAL_SYNC_INTERVAL_EXPLICIT=0
  FLUSH_CONCURRENCY=$DEFAULT_FLUSH_CONCURRENCY
  FLUSH_CONCURRENCY_EXPLICIT=0
  UPLOAD_CONCURRENCY=$DEFAULT_UPLOAD_CONCURRENCY
  UPLOAD_CONCURRENCY_EXPLICIT=0
  PARKBENCH=""

  PARKANA_PORT="${PARKANA_BENCH_PARKANA_PORT:-13100}"
  GARAGE_PORT="${PARKANA_BENCH_GARAGE_PORT:-13900}"
  LOKI_PORT="${PARKANA_BENCH_LOKI_PORT:-13101}"
  PARKANA_PORT_EXPLICIT=0
  GARAGE_PORT_EXPLICIT=0
  LOKI_PORT_EXPLICIT=0
  CPUS="${PARKANA_BENCH_CPUS:-4}"
  MEMORY="${PARKANA_BENCH_MEMORY:-8g}"

  SEED=""
  START=""
  STEP="100us"
  BATCH_LINES=$DEFAULT_BATCH_LINES
  CONCURRENCY=16
  INSTANCES_PER_SERVICE=""
  INSTANCES_PER_SERVICE_EXPLICIT=0
  STREAMS=""
  STREAMS_EXPLICIT=0
  REUSE_MANIFEST=""
  REUSE_MANIFEST_EXPLICIT=0
  LEGACY_RUNTIME=0
  RUNTIME_MODE=modern
  RUNTIME_SOURCE=""
  RUNTIME_PROBE_SUPPORT=""
  RUNTIME_PROBE_SHA256=""
  RUNTIME_PROBE_SOURCE=""
  LEGACY_KNOWN_WAL_SYNC=""
  LEGACY_KNOWN_FLUSH_CONCURRENCY=""
  LEGACY_KNOWN_UPLOAD_CONCURRENCY=""
  ACTUAL_START=""
  ACTUAL_THROUGH=""
  REPLAY_COMMAND=""
  INGEST_RETRIES=3
  INGEST_ATTEMPT_TIMEOUT="60s"

  REPETITIONS=5
  WARMUPS=2
  QUERY_TIMEOUT="120s"
  CLASSES="frequent,rare,absent,short_4byte"
  WINDOWS="short,long"
  TENANT=""

  READY_TIMEOUT=300
  VISIBILITY_TIMEOUT=7200
  FLUSH_TIMEOUT=600
  INIT_TIMEOUT=180
  STOP_TIMEOUT=60

  SOURCE_COMMIT="${PARKANA_BENCH_SOURCE_COMMIT:-}"
  SOURCE_DIRTY="${PARKANA_BENCH_SOURCE_DIRTY:-}"
  LOCK_ROOT="${PARKANA_BENCH_LOCK_ROOT:-${TMPDIR:-/tmp}/parkana-bench-locks}"
  LOCK_DIR=""
  LOCK_OWNED=0
  RUN_LOG=""
  SECRETS_FILE=""
  STACK_OWNED=0
  RUN_SUCCEEDED=0
  TEST_NO_SLEEP=0

  INGEST_START_AT=""
  ACK_AT=""
  ACK_REPORT_SECONDS=""
  ACK_WALL_SECONDS=""
  QUERYABLE_AT=""
  QUERYABLE_DELAY=""
  LOKI_FLUSH_SECONDS=""

  PARKANA_ACK_AT=""
  PARKANA_ACK_REPORT_SECONDS=""
  PARKANA_ACK_WALL_SECONDS=""
  PARKANA_QUERYABLE_AT=""
  PARKANA_QUERYABLE_DELAY=""
  LOKI_ACK_AT=""
  LOKI_ACK_REPORT_SECONDS=""
  LOKI_ACK_WALL_SECONDS=""
  LOKI_QUERYABLE_AT=""
  LOKI_QUERYABLE_DELAY=""

  PARKANA_URL="http://127.0.0.1:$PARKANA_PORT"
  LOKI_URL="http://127.0.0.1:$LOKI_PORT"

  EFFECTIVE_WAL_SYNC=""
  EFFECTIVE_WAL_SYNC_INTERVAL=""
  EFFECTIVE_FLUSH_CONCURRENCY=""
  EFFECTIVE_UPLOAD_CONCURRENCY=""
  EFFECTIVE_NANO_CPUS=""
  EFFECTIVE_CPU_QUOTA=""
  EFFECTIVE_CPU_PERIOD=""
  EFFECTIVE_CPUS=""
  EFFECTIVE_MEMORY_BYTES=""
  EFFECTIVE_IMAGE=""
  EFFECTIVE_INGEST_MEMORY_PEAK_BYTES=""
  EFFECTIVE_INGEST_MEMORY_PEAK_SOURCE=""
  EFFECTIVE_INGEST_MEMORY_PEAK_REASON=""
  EFFECTIVE_INGEST_OOM_KILL=""
  EFFECTIVE_INGEST_OOM_KILL_SOURCE=""
  EFFECTIVE_INGEST_OOM_KILL_REASON=""
}

parse_args() {
  while (($# > 0)); do
    case "$1" in
      --preset)
        (($# >= 2)) || usage_fail "--preset needs small or full"
        PRESET=$2
        PRESET_EXPLICIT=1
        shift 2
        ;;
      --bytes)
        (($# >= 2)) || usage_fail "--bytes needs a count"
        TARGET_BYTES=$2
        BYTES_EXPLICIT=1
        shift 2
        ;;
      --allow-full) ALLOW_FULL=1; shift ;;
      --output-dir)
        (($# >= 2)) || usage_fail "--output-dir needs a path"
        OUTPUT_DIR=$2
        OUTPUT_DIR_EXPLICIT=1
        shift 2
        ;;
      --project)
        (($# >= 2)) || usage_fail "--project needs a value"
        PROJECT=$2
        PROJECT_EXPLICIT=1
        shift 2
        ;;
      --parkbench)
        (($# >= 2)) || usage_fail "--parkbench needs a path"
        PARKBENCH=$2
        shift 2
        ;;
      --source-commit)
        (($# >= 2)) || usage_fail "--source-commit needs a revision"
        SOURCE_COMMIT=$2
        shift 2
        ;;
      --source-dirty)
        (($# >= 2)) || usage_fail "--source-dirty needs 0, 1 or unknown"
        SOURCE_DIRTY=$2
        shift 2
        ;;
      --parkana-port)
        (($# >= 2)) || usage_fail "--parkana-port needs a value"
        PARKANA_PORT=$2
        PARKANA_PORT_EXPLICIT=1
        shift 2
        ;;
      --garage-port)
        (($# >= 2)) || usage_fail "--garage-port needs a value"
        GARAGE_PORT=$2
        GARAGE_PORT_EXPLICIT=1
        shift 2
        ;;
      --loki-port)
        (($# >= 2)) || usage_fail "--loki-port needs a value"
        LOKI_PORT=$2
        LOKI_PORT_EXPLICIT=1
        shift 2
        ;;
      --cpus)
        (($# >= 2)) || usage_fail "--cpus needs a value"
        CPUS=$2
        shift 2
        ;;
      --memory)
        (($# >= 2)) || usage_fail "--memory needs a size"
        MEMORY=$2
        shift 2
        ;;
      --seed)
        (($# >= 2)) || usage_fail "--seed needs a value"
        SEED=$2
        shift 2
        ;;
      --start)
        (($# >= 2)) || usage_fail "--start needs a timestamp"
        START=$2
        shift 2
        ;;
      --step)
        (($# >= 2)) || usage_fail "--step needs a duration"
        STEP=$2
        shift 2
        ;;
      --batch-lines)
        (($# >= 2)) || usage_fail "--batch-lines needs a value"
        BATCH_LINES=$2
        shift 2
        ;;
      --instances-per-service)
        (($# >= 2)) || usage_fail "--instances-per-service needs a value"
        INSTANCES_PER_SERVICE=$2
        INSTANCES_PER_SERVICE_EXPLICIT=1
        shift 2
        ;;
      --streams)
        (($# >= 2)) || usage_fail "--streams needs a comma-separated list"
        STREAMS=$2
        STREAMS_EXPLICIT=1
        shift 2
        ;;
      --reuse-manifest)
        (($# >= 2)) || usage_fail "--reuse-manifest needs a manifest path"
        REUSE_MANIFEST=$2
        REUSE_MANIFEST_EXPLICIT=1
        shift 2
        ;;
      --legacy-runtime) LEGACY_RUNTIME=1; shift ;;
      --concurrency)
        (($# >= 2)) || usage_fail "--concurrency needs a value"
        CONCURRENCY=$2
        shift 2
        ;;
      --repetitions)
        (($# >= 2)) || usage_fail "--repetitions needs a value"
        REPETITIONS=$2
        shift 2
        ;;
      --warmups)
        (($# >= 2)) || usage_fail "--warmups needs a value"
        WARMUPS=$2
        shift 2
        ;;
      --query-timeout)
        (($# >= 2)) || usage_fail "--query-timeout needs a duration"
        QUERY_TIMEOUT=$2
        shift 2
        ;;
      --classes)
        (($# >= 2)) || usage_fail "--classes needs a list"
        CLASSES=$2
        shift 2
        ;;
      --windows)
        (($# >= 2)) || usage_fail "--windows needs a list"
        WINDOWS=$2
        shift 2
        ;;
      --tenant)
        (($# >= 2)) || usage_fail "--tenant needs a value"
        TENANT=$2
        shift 2
        ;;
      --ready-timeout)
        (($# >= 2)) || usage_fail "--ready-timeout needs seconds"
        READY_TIMEOUT=$2
        shift 2
        ;;
      --visibility-timeout)
        (($# >= 2)) || usage_fail "--visibility-timeout needs seconds"
        VISIBILITY_TIMEOUT=$2
        shift 2
        ;;
      --flush-timeout)
        (($# >= 2)) || usage_fail "--flush-timeout needs seconds"
        FLUSH_TIMEOUT=$2
        shift 2
        ;;
      --parkana-only) PARKANA_ONLY=1; shift ;;
      --index-max-grams)
        (($# >= 2)) || usage_fail "--index-max-grams needs a value"
        INDEX_MAX_GRAMS=$2
        INDEX_MAX_GRAMS_EXPLICIT=1
        shift 2
        ;;
      --wal-sync)
        (($# >= 2)) || usage_fail "--wal-sync needs always or interval"
        WAL_SYNC=$2
        WAL_SYNC_EXPLICIT=1
        shift 2
        ;;
      --wal-sync-interval)
        (($# >= 2)) || usage_fail "--wal-sync-interval needs a duration"
        WAL_SYNC_INTERVAL=$2
        WAL_SYNC_INTERVAL_EXPLICIT=1
        shift 2
        ;;
      --flush-concurrency)
        (($# >= 2)) || usage_fail "--flush-concurrency needs a worker count"
        FLUSH_CONCURRENCY=$2
        FLUSH_CONCURRENCY_EXPLICIT=1
        shift 2
        ;;
      --upload-concurrency)
        (($# >= 2)) || usage_fail "--upload-concurrency needs a worker count"
        UPLOAD_CONCURRENCY=$2
        UPLOAD_CONCURRENCY_EXPLICIT=1
        shift 2
        ;;
      --skip-build) SKIP_BUILD=1; shift ;;
      --skip-queries) SKIP_QUERIES=1; shift ;;
      --keep) KEEP=1; shift ;;
      --dry-run) DRY_RUN=1; shift ;;
      --self-test) SELFTEST=1; shift ;;
      -h | --help) usage; exit 0 ;;
      *) usage_fail "unknown argument '$1' (see --help)" ;;
    esac
  done
}

valid_port() { [[ $1 =~ ^[1-9][0-9]{0,4}$ ]] && (($1 >= 1 && $1 <= 65535)); }
valid_project_name() { [[ $1 =~ ^[a-z0-9][a-z0-9_-]*$ ]] && ((${#1} <= MAX_PROJECT_LEN)); }
valid_tenant_id() { [[ $1 =~ ^[A-Za-z0-9_-]{1,64}$ ]]; }
valid_positive_int() { [[ $1 =~ ^[1-9][0-9]*$ ]]; }
# Stream names mirror cmd/parkbench's corpus validation: [A-Za-z0-9_-] and no
# marker literal (a name containing one would break marker-count proofs).
valid_stream_name() {
  [[ $1 =~ ^[A-Za-z0-9_-]+$ ]] || return 1
  case "$1" in
    *PKFREQ-2v* | *PKRARE-9xqz* | *PKABSENT-7mvn* | *zq9x*) return 1 ;;
  esac
}
valid_nonnegative_int() { [[ $1 == 0 || $1 =~ ^[1-9][0-9]*$ ]]; }
# Positive decimal Go int64 without leading zeros.  Equal-length decimal
# strings compare correctly with the shell's lexicographic [[ < ]], so the
# bound check never overflows shell arithmetic.
valid_positive_int64() {
  valid_positive_int "$1" || return 1
  ((${#1} < ${#MAX_INT64})) && return 0
  ((${#1} > ${#MAX_INT64})) && return 1
  [[ $1 == "$MAX_INT64" || $1 < $MAX_INT64 ]]
}
# strip_leading_zeros removes decimal leading zeros without changing "0".
strip_leading_zeros() {
  local s=$1
  while [[ $s == 0* && ${#s} -gt 1 ]]; do s=${s#0}; done
  printf '%s' "$s"
}

# decimal_le compares two nonnegative decimal strings without converting them
# to a fixed-width integer first.
decimal_le() { # a b
  local a b
  a=$(strip_leading_zeros "$1")
  b=$(strip_leading_zeros "$2")
  ((${#a} < ${#b})) && return 0
  ((${#a} > ${#b})) && return 1
  [[ $a == "$b" || $a < $b ]]
}

# valid_positive_duration accepts a bounded duration syntax that can be
# validated offline: one or more integer/decimal components with up to nine
# fractional digits and units ns, us, µs, ms, s, m or h. Every component's
# fractional part must resolve to whole nanoseconds, and the total must be
# positive and fit time.Duration's int64-nanosecond range. The script rejects
# overflow and fractional nanoseconds (such as 0.1ns) before a container
# starts. Examples: 1s, 250ms, 1h30m, 0.5us.
valid_positive_duration() {
  local rest=$1 total=0 intpart fracpart unit unit_ns frac_ns
  local scale div int_ns frac_add limit
  [[ -n $rest ]] || return 1
  while [[ -n $rest ]]; do
    if [[ $rest =~ ^([0-9]+)(\.([0-9]{1,9}))?(ns|us|µs|ms|s|m|h) ]]; then
      intpart=${BASH_REMATCH[1]}
      fracpart=${BASH_REMATCH[3]}
      unit=${BASH_REMATCH[4]}
      rest=${rest:${#BASH_REMATCH[0]}}
      case "$unit" in
        ns) unit_ns=1 ;;
        us | µs) unit_ns=1000 ;;
        ms) unit_ns=1000000 ;;
        s) unit_ns=1000000000 ;;
        m) unit_ns=60000000000 ;;
        h) unit_ns=3600000000000 ;;
        *) return 1 ;;
      esac
      intpart=$(strip_leading_zeros "$intpart")
      limit=$((MAX_INT64 / unit_ns))
      decimal_le "$intpart" "$limit" || return 1
      int_ns=$((10#$intpart * unit_ns))
      frac_add=0
      if [[ -n $fracpart ]]; then
        while ((${#fracpart} < 9)); do fracpart=${fracpart}0; done
        frac_ns=$((10#$fracpart))
        if ((unit_ns >= 1000000000)); then
          scale=$((unit_ns / 1000000000))
          frac_add=$((frac_ns * scale))
        else
          div=$((1000000000 / unit_ns))
          ((frac_ns % div == 0)) || return 1
          frac_add=$((frac_ns / div))
        fi
      fi
      ((int_ns > MAX_INT64 - total)) && return 1
      total=$((total + int_ns))
      ((frac_add > MAX_INT64 - total)) && return 1
      total=$((total + frac_add))
    else
      return 1
    fi
  done
  ((total > 0))
}
# Worker counts share the ingester's 1..64 bound. Length-first avoids overflow
# from a hostile decimal string before the numeric comparison.
valid_worker_count() {
  valid_positive_int "$1" || return 1
  ((${#1} > 3)) && return 1
  ((10#$1 <= MAX_RUNTIME_CONCURRENCY))
}

resolve_target() {
  PRESET=$(printf '%s' "$PRESET" | tr '[:upper:]' '[:lower:]')
  case "$PRESET" in
    small) local preset_bytes=$SMALL_BYTES ;;
    medium) local preset_bytes=$MEDIUM_BYTES ;;
    full) local preset_bytes=$FULL_BYTES ;;
    *) usage_fail "--preset must be small, medium or full, got '$PRESET'" ;;
  esac
  if ((BYTES_EXPLICIT)); then
    valid_positive_int "$TARGET_BYTES" ||
      usage_fail "--bytes must be a positive decimal byte count, got '$TARGET_BYTES'"
    if ((PRESET_EXPLICIT)) && [[ $TARGET_BYTES != "$preset_bytes" ]]; then
      usage_fail "--bytes $TARGET_BYTES conflicts with --preset $PRESET ($preset_bytes)"
    fi
    if ((PRESET_EXPLICIT == 0)); then
      case "$TARGET_BYTES" in
        "$SMALL_BYTES") PRESET=small ;;
        "$MEDIUM_BYTES") PRESET=medium ;;
        "$FULL_BYTES") PRESET=full ;;
        *) PRESET=custom ;;
      esac
    fi
  else
    TARGET_BYTES=$preset_bytes
  fi
  # The full-size gate is unchanged; medium and custom bytes need no --allow-full.
  if [[ $PRESET == full ]] && ((ALLOW_FULL == 0)); then
    usage_fail "the full 25,000,000,000-byte preset requires --allow-full (or PARKANA_BENCH_ALLOW_FULL=1)"
  fi
}

validate_args() {
  resolve_target

  if [[ -n $REUSE_MANIFEST ]]; then
    [[ -z $START ]] ||
      usage_fail "--reuse-manifest conflicts with --start; pass exactly one"
    [[ -f $REUSE_MANIFEST ]] ||
      usage_fail "--reuse-manifest file not found: $REUSE_MANIFEST"
    local reused_start
    reused_start=$(manifest_string "$REUSE_MANIFEST" start)
    [[ -n $reused_start ]] ||
      usage_fail "--reuse-manifest has no corpus.start: $REUSE_MANIFEST"
    [[ $reused_start =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?(Z|[+-][0-9]{2}:[0-9]{2})$ ]] ||
      usage_fail "--reuse-manifest corpus.start is not RFC3339: $reused_start"
    date -u -d "$reused_start" +%s >/dev/null 2>&1 ||
      usage_fail "--reuse-manifest corpus.start is not a valid timestamp: $reused_start"
    START=$reused_start
  fi
  if ((LEGACY_RUNTIME)); then
    if ((WAL_SYNC_EXPLICIT)) && [[ $WAL_SYNC != always ]]; then
      usage_fail "--legacy-runtime supports only the known always WAL mode, got --wal-sync $WAL_SYNC"
    fi
    if ((WAL_SYNC_INTERVAL_EXPLICIT)); then
      usage_fail "--legacy-runtime has no --wal-sync-interval support"
    fi
    if ((FLUSH_CONCURRENCY_EXPLICIT)) && ((FLUSH_CONCURRENCY != 1)); then
      usage_fail "--legacy-runtime has no phase-4 flush workers; known F1, got --flush-concurrency $FLUSH_CONCURRENCY"
    fi
    if ((UPLOAD_CONCURRENCY_EXPLICIT)) && ((UPLOAD_CONCURRENCY != 1)); then
      usage_fail "--legacy-runtime has no phase-4 upload workers; known U1, got --upload-concurrency $UPLOAD_CONCURRENCY"
    fi
    # Omitted flags default to the legacy source contract, never to a modern
    # runtime flag the old binary does not understand.
    if ((WAL_SYNC_EXPLICIT == 0)); then WAL_SYNC=always; fi
    if ((WAL_SYNC_INTERVAL_EXPLICIT == 0)); then WAL_SYNC_INTERVAL=1s; fi
    if ((FLUSH_CONCURRENCY_EXPLICIT == 0)); then FLUSH_CONCURRENCY=1; fi
    if ((UPLOAD_CONCURRENCY_EXPLICIT == 0)); then UPLOAD_CONCURRENCY=1; fi
    RUNTIME_MODE=legacy-foundation
    LEGACY_KNOWN_WAL_SYNC=always
    LEGACY_KNOWN_FLUSH_CONCURRENCY=1
    LEGACY_KNOWN_UPLOAD_CONCURRENCY=1
  else
    RUNTIME_MODE=modern
  fi

  valid_positive_int "$REPETITIONS" || usage_fail "--repetitions must be positive"
  valid_nonnegative_int "$WARMUPS" || usage_fail "--warmups must be zero or positive"
  valid_positive_int "$BATCH_LINES" || usage_fail "--batch-lines must be positive"
  if ((INSTANCES_PER_SERVICE_EXPLICIT)); then
    valid_positive_int "$INSTANCES_PER_SERVICE" ||
      usage_fail "--instances-per-service must be a positive decimal integer, got '$INSTANCES_PER_SERVICE'"
    ((${#INSTANCES_PER_SERVICE} < 5)) ||
      usage_fail "--instances-per-service must be between 1 and $MAX_INSTANCES_PER_SERVICE"
    ((INSTANCES_PER_SERVICE <= MAX_INSTANCES_PER_SERVICE)) ||
      usage_fail "--instances-per-service must be between 1 and $MAX_INSTANCES_PER_SERVICE"
  fi
  if ((STREAMS_EXPLICIT)); then
    local saved_ifs=$IFS
    local -a raw_streams=()
    local -a stream_names=()
    local stream_name existing joined
    IFS=,
    # `read` drops a trailing empty field, so reject the delimiters explicitly.
    [[ $STREAMS != ,* && $STREAMS != *, && $STREAMS != *,,* ]] ||
      usage_fail "--streams must not contain empty names"
    read -ra raw_streams <<<"$STREAMS" || true
    ((${#raw_streams[@]} > 0)) ||
      usage_fail "--streams must name at least one stream"
    for stream_name in "${raw_streams[@]}"; do
      stream_name=${stream_name#"${stream_name%%[![:space:]]*}"}
      stream_name=${stream_name%"${stream_name##*[![:space:]]}"}
      [[ -n $stream_name ]] ||
        usage_fail "--streams must not contain empty names"
      valid_stream_name "$stream_name" ||
        usage_fail "invalid stream name '$stream_name' (want [A-Za-z0-9_-] without a marker literal)"
      for existing in "${stream_names[@]}"; do
        [[ $existing != "$stream_name" ]] ||
          usage_fail "--streams contains duplicate stream '$stream_name'"
      done
      stream_names+=("$stream_name")
    done
    IFS=$saved_ifs
    local effective_instances=4
    if ((INSTANCES_PER_SERVICE_EXPLICIT)); then
      effective_instances=$INSTANCES_PER_SERVICE
    fi
    ((${#stream_names[@]} <= MAX_LABEL_SETS / effective_instances)) ||
      usage_fail "${#stream_names[@]} streams times $effective_instances instances exceeds the $MAX_LABEL_SETS label-set bound"
    printf -v joined '%s,' "${stream_names[@]}"
    STREAMS=${joined%,}
  fi
  valid_positive_int "$CONCURRENCY" || usage_fail "--concurrency must be positive"
  valid_positive_int "$READY_TIMEOUT" || usage_fail "--ready-timeout must be positive"
  valid_positive_int "$VISIBILITY_TIMEOUT" || usage_fail "--visibility-timeout must be positive"
  valid_positive_int "$FLUSH_TIMEOUT" || usage_fail "--flush-timeout must be positive"
  valid_positive_int64 "$INDEX_MAX_GRAMS" ||
    usage_fail "--index-max-grams must be a positive decimal int64 (1..$MAX_INT64), got '$INDEX_MAX_GRAMS'"
  case "$WAL_SYNC" in
    always | interval) ;;
    *) usage_fail "--wal-sync must be always or interval, got '$WAL_SYNC'" ;;
  esac
  valid_positive_duration "$WAL_SYNC_INTERVAL" ||
    usage_fail "--wal-sync-interval must be a positive whole-nanosecond duration such as 1s or 250ms (≤9 fractional digits, no overflow), got '$WAL_SYNC_INTERVAL'"
  valid_worker_count "$FLUSH_CONCURRENCY" ||
    usage_fail "--flush-concurrency must be an integer between 1 and $MAX_RUNTIME_CONCURRENCY, got '$FLUSH_CONCURRENCY'"
  valid_worker_count "$UPLOAD_CONCURRENCY" ||
    usage_fail "--upload-concurrency must be an integer between 1 and $MAX_RUNTIME_CONCURRENCY, got '$UPLOAD_CONCURRENCY'"

  [[ -f $COMPOSE_FILE ]] || usage_fail "missing benchmark Compose base: $COMPOSE_FILE"
  [[ -f $RUNTIME_OVERLAY ]] || usage_fail "missing phase 4 ingest runtime overlay: $RUNTIME_OVERLAY"

  valid_port "$PARKANA_PORT" || usage_fail "Parkana port must be an integer in 1..65535"
  valid_port "$GARAGE_PORT" || usage_fail "Garage port must be an integer in 1..65535"
  # Compose interpolates every service, so the unused Loki port mapping must
  # still be format-valid in parkana-only mode.  Only its availability probe
  # and uniqueness check are skipped.
  valid_port "$LOKI_PORT" || usage_fail "Loki port must be an integer in 1..65535"
  local -a port_list=("$PARKANA_PORT" "$GARAGE_PORT")
  ((PARKANA_ONLY)) || port_list+=("$LOKI_PORT")
  local seen=() p
  for p in "${port_list[@]}"; do
    if [[ " ${seen[*]:-} " == *" $p "* ]]; then
      usage_fail "host port $p is used twice; the selected backends must differ"
    fi
    seen+=("$p")
  done

  [[ $CPUS =~ ^(0|[1-9][0-9]*)([.][0-9]+)?$|^[.][0-9]+$ ]] ||
    usage_fail "--cpus must be a positive decimal number without leading zeros"
  CPUS=$(awk -v c="$CPUS" 'BEGIN { printf "%.6g", c }')
  awk -v c="$CPUS" 'BEGIN { exit !(c > 0) }' || usage_fail "--cpus must be greater than zero"
  [[ $MEMORY =~ ^([0-9]+)([kKmMgG])?[bB]?$ ]] ||
    usage_fail "--memory must look like 8g or 8192m"
  local mem_raw=${BASH_REMATCH[1]} mem_unit
  mem_unit=${MEMORY:${#mem_raw}}
  local mem_num=$((10#$mem_raw))
  ((mem_num > 0)) || usage_fail "--memory must be greater than zero"
  MEMORY="$mem_num$mem_unit"

  if [[ -n $TENANT ]] && ! valid_tenant_id "$TENANT"; then
    usage_fail "--tenant must be 1-64 characters from [A-Za-z0-9_-]"
  fi
  case "$SOURCE_DIRTY" in
    "" | 0 | 1 | unknown) ;;
    *) usage_fail "--source-dirty must be 0, 1 or unknown" ;;
  esac

  [[ -n $OUTPUT_DIR ]] || OUTPUT_DIR="$DEFAULT_OUTPUT_ROOT"
  if [[ -z $PROJECT ]]; then
    PROJECT="parkana-bench-$(date -u +%y%m%d%H%M%S)-$$"
    PROJECT_EXPLICIT=0
  fi
  valid_project_name "$PROJECT" ||
    usage_fail "invalid compose project name '$PROJECT' (want [a-z0-9][a-z0-9_-]{0,62})"
  if [[ -z $OUTPUT_DIR || $OUTPUT_DIR == "/" ]]; then
    usage_fail "--output-dir must not be empty or /"
  fi
  if [[ $OUTPUT_DIR != /* ]]; then
    OUTPUT_DIR="$PWD/$OUTPUT_DIR"
  fi
  if ((OUTPUT_DIR_EXPLICIT == 0)); then
    OUTPUT_DIR="$OUTPUT_DIR/$PROJECT"
  fi
}

# --- offline building blocks ------------------------------------------------

random_hex() { # bytes -> 2*bytes lowercase hex
  od -An -N "$1" -tx1 /dev/urandom | tr -d ' \n'
}

generate_credentials() {
  # Separate bucket and key for each backend, both on the same Garage node.
  PARKANA_BENCH_PARKANA_BUCKET="parkana-p3-$(random_hex 5)"
  PARKANA_BENCH_LOKI_BUCKET="loki-p3-$(random_hex 5)"
  PARKANA_BENCH_PARKANA_ACCESS_KEY="GK$(random_hex 12)"
  PARKANA_BENCH_PARKANA_SECRET_KEY="$(random_hex 32)"
  PARKANA_BENCH_LOKI_ACCESS_KEY="GK$(random_hex 12)"
  PARKANA_BENCH_LOKI_SECRET_KEY="$(random_hex 32)"
  # garage-init.sh imports one key per invocation; keep its names explicit.
  PARKANA_BENCH_GARAGE_KEY_ID="$PARKANA_BENCH_PARKANA_ACCESS_KEY"
  PARKANA_BENCH_GARAGE_KEY_SECRET="$PARKANA_BENCH_PARKANA_SECRET_KEY"
  LOKI_BENCH_GARAGE_KEY_ID="$PARKANA_BENCH_LOKI_ACCESS_KEY"
  LOKI_BENCH_GARAGE_KEY_SECRET="$PARKANA_BENCH_LOKI_SECRET_KEY"
}

write_env_file() {
  umask 077
  {
    printf 'PARKANA_BENCH_CPUS=%s\n' "$CPUS"
    printf 'PARKANA_BENCH_MEMORY=%s\n' "$MEMORY"
    printf 'PARKANA_BENCH_PARKANA_PORT=%s\n' "$PARKANA_PORT"
    printf 'PARKANA_BENCH_GARAGE_PORT=%s\n' "$GARAGE_PORT"
    printf 'PARKANA_BENCH_LOKI_PORT=%s\n' "$LOKI_PORT"
    printf 'PARKANA_BENCH_PARKANA_BUCKET=%s\n' "$PARKANA_BENCH_PARKANA_BUCKET"
    printf 'PARKANA_BENCH_PARKANA_ACCESS_KEY=%s\n' "$PARKANA_BENCH_PARKANA_ACCESS_KEY"
    printf 'PARKANA_BENCH_PARKANA_SECRET_KEY=%s\n' "$PARKANA_BENCH_PARKANA_SECRET_KEY"
    printf 'PARKANA_BENCH_LOKI_BUCKET=%s\n' "$PARKANA_BENCH_LOKI_BUCKET"
    printf 'PARKANA_BENCH_LOKI_ACCESS_KEY=%s\n' "$PARKANA_BENCH_LOKI_ACCESS_KEY"
    printf 'PARKANA_BENCH_LOKI_SECRET_KEY=%s\n' "$PARKANA_BENCH_LOKI_SECRET_KEY"
    printf 'PARKANA_BENCH_INDEX_MAX_GRAMS=%s\n' "$INDEX_MAX_GRAMS"
    if ((LEGACY_RUNTIME == 0)); then
      printf 'PARKANA_BENCH_WAL_SYNC=%s\n' "$WAL_SYNC"
      printf 'PARKANA_BENCH_WAL_SYNC_INTERVAL=%s\n' "$WAL_SYNC_INTERVAL"
      printf 'PARKANA_BENCH_FLUSH_CONCURRENCY=%s\n' "$FLUSH_CONCURRENCY"
      printf 'PARKANA_BENCH_UPLOAD_CONCURRENCY=%s\n' "$UPLOAD_CONCURRENCY"
    fi
    printf 'PARKANA_BENCH_GARAGE_KEY_ID=%s\n' "$PARKANA_BENCH_GARAGE_KEY_ID"
    printf 'PARKANA_BENCH_GARAGE_KEY_SECRET=%s\n' "$PARKANA_BENCH_GARAGE_KEY_SECRET"
    printf 'LOKI_BENCH_GARAGE_KEY_ID=%s\n' "$LOKI_BENCH_GARAGE_KEY_ID"
    printf 'LOKI_BENCH_GARAGE_KEY_SECRET=%s\n' "$LOKI_BENCH_GARAGE_KEY_SECRET"
  } >"$SECRETS_FILE"
  chmod 0600 "$SECRETS_FILE"
}

# compose_argv stores the owned Compose command (with caller-exported
# interpolation names removed) in the named array. The bounded cgroup reads use
# it directly so they can wrap the whole command in timeout without changing
# the argv the compose wrapper would build.
compose_argv() { # array-name
  local -n ref=$1
  local v
  ref=(env)
  for v in "${COMPOSE_ENV_VARS[@]}"; do ref+=(-u "$v"); done
  ref+=("$DOCKER" compose --project-name "$PROJECT" --env-file "$SECRETS_FILE" -f "$COMPOSE_FILE")
  # The legacy foundation binary has no phase-4 runtime flags, so the overlay
  # must not be attached and its environment keys must not be implied as used.
  if ((LEGACY_RUNTIME == 0)); then
    ref+=(-f "$RUNTIME_OVERLAY")
  fi
}

compose() {
  local -a cmd
  compose_argv cmd
  "${cmd[@]}" "$@"
}

port_free() {
  local port=$1 out
  if command -v ss >/dev/null 2>&1; then
    if out=$(ss -H -ltn "sport = :$port" 2>/dev/null); then
      [[ -z $out ]]
    else
      ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null
    fi
  else
    ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null
  fi
}

check_ports() {
  local item
  local -a items=("Parkana:$PARKANA_PORT" "Garage:$GARAGE_PORT")
  ((PARKANA_ONLY)) || items+=("Loki:$LOKI_PORT")
  for item in "${items[@]}"; do
    port_free "${item#*:}" ||
      fail "${item%%:*} port ${item#*:} is already in use; pass the matching --*-port"
  done
}

epoch_now() { date -u +%s.%N; }
elapsed_seconds() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.3f", b - a }'; }
artifact() { printf '%s/%s' "$OUTPUT_DIR" "$1"; }

check_output_dir_absent() {
  if [[ -e $OUTPUT_DIR || -L $OUTPUT_DIR ]]; then
    fail "output directory already exists (refusing to reuse it): $OUTPUT_DIR"
  fi
}

create_output_dir() {
  local parent
  parent=$(dirname -- "$OUTPUT_DIR")
  mkdir -p -- "$parent"
  # Deliberately not `mkdir -p`: a directory created between the check and here
  # must fail this run instead of being reused.
  mkdir -- "$OUTPUT_DIR" || fail "could not create fresh output directory: $OUTPUT_DIR"
  chmod 0700 "$OUTPUT_DIR"
  RUN_LOG="$OUTPUT_DIR/run.log"
  SECRETS_FILE="$OUTPUT_DIR/secrets.env"
}

# --- Docker/Compose lifecycle ----------------------------------------------

# An atomic host-side lock serialises invocations that name the same Compose
# project.  mkdir is the atomic primitive: a second caller fails closed instead
# of adopting or tearing down the first caller's resources.  The lock contains
# only this run's PID/start info; cleanup removes only a lock this run owns.
acquire_project_lock() {
  LOCK_DIR="$LOCK_ROOT/$PROJECT.lock"
  mkdir -p -- "$LOCK_ROOT" || fail "cannot create lock root: $LOCK_ROOT"
  chmod 0700 "$LOCK_ROOT" 2>/dev/null || true
  if ! mkdir -- "$LOCK_DIR" 2>/dev/null; then
    local owner=""
    [[ -f $LOCK_DIR/owner ]] && owner=$(cat "$LOCK_DIR/owner" 2>/dev/null || true)
    fail "lock already held for compose project '$PROJECT' ($LOCK_DIR${owner:+; $owner}); refusing to run concurrently"
  fi
  LOCK_OWNED=1
  {
    printf 'pid=%s\n' "$$"
    printf 'project=%s\n' "$PROJECT"
    printf 'started=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  } >"$LOCK_DIR/owner"
}

release_project_lock() {
  ((LOCK_OWNED)) || return 0
  [[ -n $LOCK_DIR && -d $LOCK_DIR ]] && rm -rf -- "$LOCK_DIR"
  LOCK_OWNED=0
}

ensure_project_fresh() {
  local out=""
  if ! out="$("$DOCKER" ps -aq --filter "label=com.docker.compose.project=$PROJECT" 2>&1)"; then
    fail "cannot list containers for compose project '$PROJECT': $out"
  fi
  [[ -z $out ]] || fail "compose project '$PROJECT' already has containers; refusing to reuse it"
  if ! out="$("$DOCKER" volume ls -q --filter "label=com.docker.compose.project=$PROJECT" 2>&1)"; then
    fail "cannot list volumes for compose project '$PROJECT': $out"
  fi
  [[ -z $out ]] || fail "compose project '$PROJECT' already has volumes; refusing to reuse it"
  if ! out="$("$DOCKER" network ls -q --filter "label=com.docker.compose.project=$PROJECT" 2>&1)"; then
    fail "cannot list networks for compose project '$PROJECT': $out"
  fi
  [[ -z $out ]] || fail "compose project '$PROJECT' already has networks; refusing to reuse it"
  if ! out="$("$DOCKER" compose ls -a --quiet 2>&1)"; then
    fail "cannot list compose projects: $out"
  fi
  if grep -Fxq -- "$PROJECT" <<<"$out"; then
    fail "compose project '$PROJECT' is already known to Compose; refusing to reuse it"
  fi
}

preflight() {
  "$DOCKER" info >/dev/null 2>&1 || fail "Docker is not reachable"
  ensure_project_fresh
  check_ports
  check_output_dir_absent
}

build_images() {
  ((SKIP_BUILD)) && { log "build skipped (--skip-build)"; return 0; }
  log "building Garage and Parkana images (phase-2 Dockerfile, no changes)"
  compose build garage parkana >>"$RUN_LOG" 2>&1 ||
    fail "compose build garage parkana failed; see $RUN_LOG"
  log "building the benchmark sqlite tool image"
  compose --profile tools build bench-tools >>"$RUN_LOG" 2>&1 ||
    fail "compose build bench-tools failed; see $RUN_LOG"
}

build_parkbench() {
  if [[ -n $PARKBENCH ]]; then
    [[ -x $PARKBENCH ]] || fail "--parkbench is not executable: $PARKBENCH"
    return 0
  fi
  if ((SKIP_BUILD)); then
    PARKBENCH=$(command -v parkbench || true)
    [[ -n $PARKBENCH ]] ||
      fail "--skip-build needs --parkbench PATH or a parkbench binary on PATH"
    return 0
  fi
  mkdir -p "$OUTPUT_DIR/bin"
  log "building cmd/parkbench"
  if ! go -C "$REPO_ROOT" build -trimpath -o "$OUTPUT_DIR/bin/parkbench" ./cmd/parkbench >>"$RUN_LOG" 2>&1; then
    fail "go build ./cmd/parkbench failed; see $RUN_LOG"
  fi
  PARKBENCH="$OUTPUT_DIR/bin/parkbench"
}

check_parkbench_query() {
  "$PARKBENCH" help >/dev/null 2>&1 || fail "parkbench help failed: $PARKBENCH"
  if ((SKIP_QUERIES == 0)) && ! "$PARKBENCH" help 2>&1 | grep -Eq '^[[:space:]]+query([[:space:]]|$)'; then
    fail "parkbench has no query subcommand (B2b not merged); use --skip-queries for a stack/ingest smoke"
  fi
}

wait_one_shot() { # service timeout
  local svc=$1
  local timeout id="" state="" deadline
  timeout=${2:-$INIT_TIMEOUT}
  deadline=$((SECONDS + timeout))
  id=$(compose ps -a -q "$svc" 2>/dev/null | tail -n 1 || true)
  [[ -n $id ]] || fail "compose did not create the one-shot service $svc"
  while :; do
    state=$("$DOCKER" inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$id" 2>/dev/null || true)
    case "$state" in
      "exited 0") return 0 ;;
      "exited "*)
        compose logs --no-color --tail=80 "$svc" >&2 || true
        fail "one-shot service $svc failed ($state)"
        ;;
    esac
    if ((SECONDS >= deadline)); then
      compose logs --no-color --tail=80 "$svc" >&2 || true
      fail "one-shot service $svc did not finish within ${timeout}s (state: ${state:-unknown})"
    fi
    nap 1
  done
}

nap() { ((TEST_NO_SLEEP)) && return 0; sleep "$1"; }

http_status() {
  curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "$1" 2>/dev/null || printf '000'
}

http_get() {
  curl -fsS --max-time 30 "$1" 2>/dev/null
}

http_post() {
  curl -sS -o /dev/null -w '%{http_code}' -X POST --max-time 30 "$1" 2>/dev/null || printf '000'
}

http_query() { # base query time
  local base=$1 query=$2 timestamp=$3
  local -a args=(--get "$base/loki/api/v1/query"
    --data-urlencode "query=$query"
    --data-urlencode "time=$timestamp")
  [[ -z $TENANT ]] || args+=(--header "X-Scope-OrgID: $TENANT")
  curl -fsS --max-time 120 "${args[@]}" 2>/dev/null
}

start_garage_stack() {
  local -a up=(garage garage-init-parkana)
  ((PARKANA_ONLY)) || up+=(garage-init-loki)
  log "starting Garage and the one-shot bucket/key initializers"
  compose up -d "${up[@]}" >>"$RUN_LOG" 2>&1 ||
    fail "compose up garage/garage-init failed; see $RUN_LOG"
  wait_one_shot garage-init-parkana
  ((PARKANA_ONLY)) || wait_one_shot garage-init-loki
}

wait_http_ready() { # label url timeout
  local label=$1 url=$2 timeout=${3:-$READY_TIMEOUT}
  local status="" deadline=$((SECONDS + timeout))
  while :; do
    status=$(http_status "$url")
    if [[ $status == 200 ]]; then
      log "$label ready"
      return 0
    fi
    if ((SECONDS >= deadline)); then
      fail "$label did not become ready within ${timeout}s (last HTTP status: $status; $url)"
    fi
    nap 1
  done
}

start_backend() { # parkana|loki
  local backend=$1 service
  case "$backend" in
    parkana) service=parkana ;;
    loki) service=loki ;;
    *) fail "unknown backend '$backend'" ;;
  esac
  log "starting $backend"
  compose up -d "$service" >>"$RUN_LOG" 2>&1 ||
    fail "compose up $service failed; see $RUN_LOG"
  if [[ $backend == parkana ]]; then
    wait_http_ready Parkana "$PARKANA_URL/ready"
  else
    wait_http_ready Loki "$LOKI_URL/ready"
  fi
}

# env_value extracts one KEY=value line from Docker's Config.Env output. It
# prints an empty string for a missing key so the caller can fail closed with a
# specific message.
env_value() { # env-text key
  printf '%s\n' "$1" | awk -v key="$2" 'index($0, key "=") == 1 {
    print substr($0, length(key) + 2)
    exit
  }'
}

# capture_parkana_runtime reads the settings Docker actually applied to the
# running Parkana container. Requested flags are not evidence: an ambient
# variable, a stale overlay or a dropped merge could change what the process
# sees. The four ingest-runtime variables must be present and equal the owned
# values; otherwise the run fails instead of recording a misleading report.
capture_parkana_runtime() {
  local id env_text resources
  local eff_wal eff_interval eff_flush eff_upload
  local nano_cpus cpu_quota cpu_period memory image
  id=$(compose ps -a -q parkana 2>/dev/null | tail -n 1 || true)
  [[ -n $id ]] || fail "cannot capture Parkana runtime: compose reported no parkana container"
  if ((LEGACY_RUNTIME)); then
    # The foundation binary has no phase-4 runtime flags. Its known source
    # contract is always/F1/U1, but that is not container-effective evidence and
    # merely-present env vars must never be presented as support.
    RUNTIME_SOURCE=legacy-source-contract
    eff_wal=unavailable
    eff_interval=unavailable
    eff_flush=unavailable
    eff_upload=unavailable
  else
    if ! env_text=$("$DOCKER" inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$id" 2>&1); then
      fail "cannot inspect Parkana container environment: $env_text"
    fi
    eff_wal=$(env_value "$env_text" PARKANA_WAL_SYNC)
    eff_interval=$(env_value "$env_text" PARKANA_WAL_SYNC_INTERVAL)
    eff_flush=$(env_value "$env_text" PARKANA_INGEST_FLUSH_CONCURRENCY)
    eff_upload=$(env_value "$env_text" PARKANA_INGEST_UPLOAD_CONCURRENCY)
    [[ -n $eff_wal && -n $eff_interval && -n $eff_flush && -n $eff_upload ]] ||
      fail "Parkana container is missing one of PARKANA_WAL_SYNC, PARKANA_WAL_SYNC_INTERVAL, PARKANA_INGEST_FLUSH_CONCURRENCY or PARKANA_INGEST_UPLOAD_CONCURRENCY"
    [[ $eff_wal == "$WAL_SYNC" ]] ||
      fail "Parkana container PARKANA_WAL_SYNC=$eff_wal but this run requested $WAL_SYNC"
    [[ $eff_interval == "$WAL_SYNC_INTERVAL" ]] ||
      fail "Parkana container PARKANA_WAL_SYNC_INTERVAL=$eff_interval but this run requested $WAL_SYNC_INTERVAL"
    [[ $eff_flush == "$FLUSH_CONCURRENCY" ]] ||
      fail "Parkana container PARKANA_INGEST_FLUSH_CONCURRENCY=$eff_flush but this run requested $FLUSH_CONCURRENCY"
    [[ $eff_upload == "$UPLOAD_CONCURRENCY" ]] ||
      fail "Parkana container PARKANA_INGEST_UPLOAD_CONCURRENCY=$eff_upload but this run requested $UPLOAD_CONCURRENCY"
    RUNTIME_SOURCE=container-inspect
  fi

  if ! resources=$("$DOCKER" inspect --format '{{.HostConfig.NanoCpus}} {{.HostConfig.CpuQuota}} {{.HostConfig.CpuPeriod}} {{.HostConfig.Memory}} {{.Image}}' "$id" 2>&1); then
    fail "cannot inspect Parkana container resources: $resources"
  fi
  read -r nano_cpus cpu_quota cpu_period memory image <<<"$resources" || true
  EFFECTIVE_WAL_SYNC=$eff_wal
  EFFECTIVE_WAL_SYNC_INTERVAL=$eff_interval
  EFFECTIVE_FLUSH_CONCURRENCY=$eff_flush
  EFFECTIVE_UPLOAD_CONCURRENCY=$eff_upload
  EFFECTIVE_NANO_CPUS=${nano_cpus:-0}
  EFFECTIVE_CPU_QUOTA=${cpu_quota:-0}
  EFFECTIVE_CPU_PERIOD=${cpu_period:-0}
  EFFECTIVE_MEMORY_BYTES=${memory:-0}
  EFFECTIVE_IMAGE=${image:-unavailable}
  if [[ $EFFECTIVE_NANO_CPUS =~ ^[0-9]+$ ]] && ((EFFECTIVE_NANO_CPUS > 0)); then
    EFFECTIVE_CPUS=$(awk -v n="$EFFECTIVE_NANO_CPUS" 'BEGIN { printf "%.3f", n / 1000000000 }')
  elif [[ $EFFECTIVE_CPU_QUOTA =~ ^[0-9]+$ && $EFFECTIVE_CPU_PERIOD =~ ^[0-9]+$ ]] &&
    ((EFFECTIVE_CPU_QUOTA > 0 && EFFECTIVE_CPU_PERIOD > 0)); then
    EFFECTIVE_CPUS=$(awk -v q="$EFFECTIVE_CPU_QUOTA" -v p="$EFFECTIVE_CPU_PERIOD" 'BEGIN { printf "%.3f", q / p }')
  else
    EFFECTIVE_CPUS=unavailable
  fi
  {
    printf 'source=%s\n' "$RUNTIME_SOURCE"
    printf 'runtime_mode=%s\n' "$RUNTIME_MODE"
    if ((LEGACY_RUNTIME)); then
      printf 'runtime_support=absent\n'
      printf 'known_wal_sync=%s\n' "$LEGACY_KNOWN_WAL_SYNC"
      printf 'known_flush_concurrency=%s\n' "$LEGACY_KNOWN_FLUSH_CONCURRENCY"
      printf 'known_upload_concurrency=%s\n' "$LEGACY_KNOWN_UPLOAD_CONCURRENCY"
    else
      printf 'runtime_support=present\n'
      printf 'requested_wal_sync=%s\n' "$WAL_SYNC"
      printf 'requested_wal_sync_interval=%s\n' "$WAL_SYNC_INTERVAL"
      printf 'requested_flush_concurrency=%s\n' "$FLUSH_CONCURRENCY"
      printf 'requested_upload_concurrency=%s\n' "$UPLOAD_CONCURRENCY"
    fi
    printf 'container_id=%s\n' "$id"
    printf 'effective_wal_sync=%s\n' "$EFFECTIVE_WAL_SYNC"
    printf 'effective_wal_sync_interval=%s\n' "$EFFECTIVE_WAL_SYNC_INTERVAL"
    printf 'effective_flush_concurrency=%s\n' "$EFFECTIVE_FLUSH_CONCURRENCY"
    printf 'effective_upload_concurrency=%s\n' "$EFFECTIVE_UPLOAD_CONCURRENCY"
    printf 'effective_nano_cpus=%s\n' "$EFFECTIVE_NANO_CPUS"
    printf 'effective_cpu_quota=%s\n' "$EFFECTIVE_CPU_QUOTA"
    printf 'effective_cpu_period=%s\n' "$EFFECTIVE_CPU_PERIOD"
    printf 'effective_cpus=%s\n' "$EFFECTIVE_CPUS"
    printf 'effective_memory_bytes=%s\n' "$EFFECTIVE_MEMORY_BYTES"
    printf 'effective_image=%s\n' "$EFFECTIVE_IMAGE"
  } >"$OUTPUT_DIR/runtime-parkana.txt"
}

# probe_parkana_runtime_flags verifies the binary's own help advertises the
# phase-4 runtime flags. The raw help is never printed, logged or archived: it
# can contain env-backed defaults. Only a support boolean, source and hash are
# recorded. A mismatch fails with a fixed sanitized message.
probe_parkana_runtime_flags() {
  local help_text support sha found=0
  local file="$OUTPUT_DIR/run-metadata.txt"
  if ! help_text=$(run_backend_bounded parkana /usr/local/bin/parkana -h 2>&1); then
    fail "runtime probe unavailable: bounded parkana -h failed; refusing to assume runtime support"
  fi
  [[ -n $help_text ]] ||
    fail "runtime probe unavailable: bounded parkana -h returned no output; refusing to assume runtime support"
  sha=$(printf '%s' "$help_text" | sha256sum | awk '{print $1}')
  if grep -Fq -- '-wal.sync' <<<"$help_text"; then found=$((found + 1)); fi
  if grep -Fq -- '-ingest.flush-concurrency' <<<"$help_text"; then found=$((found + 1)); fi
  if grep -Fq -- '-ingest.upload-concurrency' <<<"$help_text"; then found=$((found + 1)); fi
  if ((LEGACY_RUNTIME)); then
    # Legacy safety: none of the three runtime flags may be present. A partial
    # or mixed help is as unsafe as a modern binary because it would leave the
    # omitted overlay running an unknown subset of modern defaults.
    ((found == 0)) ||
      fail "legacy runtime probe mismatch: binary advertises phase-4 runtime flags; refusing to claim the legacy source contract"
    support=absent
  else
    ((found == 3)) ||
      fail "modern runtime probe mismatch: binary does not advertise all phase-4 runtime flags; refusing to claim modern runtime support"
    support=present
  fi
  RUNTIME_PROBE_SUPPORT=$support
  RUNTIME_PROBE_SHA256=$sha
  RUNTIME_PROBE_SOURCE=bounded-parkana-help
  {
    printf 'runtime_probe_source=%s\n' "$RUNTIME_PROBE_SOURCE"
    printf 'runtime_flag_support=%s\n' "$RUNTIME_PROBE_SUPPORT"
    printf 'runtime_probe_sha256=%s\n' "$RUNTIME_PROBE_SHA256"
  } >>"$OUTPUT_DIR/runtime-parkana.txt"
  [[ -f $file ]] || {
    warn "no run metadata at $file; Parkana runtime probe not recorded"
    return 0
  }
  {
    printf 'parkana_runtime_probe_source=%s\n' "$RUNTIME_PROBE_SOURCE"
    printf 'parkana_runtime_flag_support=%s\n' "$RUNTIME_PROBE_SUPPORT"
    printf 'parkana_runtime_probe_sha256=%s\n' "$RUNTIME_PROBE_SHA256"
  } >>"$file"
}

# append_runtime_evidence adds the measured container settings to the same
# run-metadata.txt that already holds hardware, source and corpus evidence.
append_runtime_evidence() {
  local file="$OUTPUT_DIR/run-metadata.txt"
  [[ -f $file ]] || {
    warn "no run metadata at $file; effective Parkana runtime not recorded"
    return 0
  }
  {
    printf 'parkana_runtime_mode=%s\n' "$RUNTIME_MODE"
    printf 'parkana_effective_source=%s\n' "${RUNTIME_SOURCE:-unavailable}"
    if ((LEGACY_RUNTIME)); then
      printf 'parkana_runtime_support=absent\n'
      printf 'parkana_legacy_known_wal_sync=%s\n' "$LEGACY_KNOWN_WAL_SYNC"
      printf 'parkana_legacy_known_flush_concurrency=%s\n' "$LEGACY_KNOWN_FLUSH_CONCURRENCY"
      printf 'parkana_legacy_known_upload_concurrency=%s\n' "$LEGACY_KNOWN_UPLOAD_CONCURRENCY"
    else
      printf 'parkana_runtime_support=present\n'
    fi
    printf 'parkana_effective_wal_sync=%s\n' "${EFFECTIVE_WAL_SYNC:-unavailable}"
    printf 'parkana_effective_wal_sync_interval=%s\n' "${EFFECTIVE_WAL_SYNC_INTERVAL:-unavailable}"
    printf 'parkana_effective_flush_concurrency=%s\n' "${EFFECTIVE_FLUSH_CONCURRENCY:-unavailable}"
    printf 'parkana_effective_upload_concurrency=%s\n' "${EFFECTIVE_UPLOAD_CONCURRENCY:-unavailable}"
    printf 'parkana_effective_nano_cpus=%s\n' "${EFFECTIVE_NANO_CPUS:-unavailable}"
    printf 'parkana_effective_cpu_quota=%s\n' "${EFFECTIVE_CPU_QUOTA:-unavailable}"
    printf 'parkana_effective_cpu_period=%s\n' "${EFFECTIVE_CPU_PERIOD:-unavailable}"
    printf 'parkana_effective_cpus=%s\n' "${EFFECTIVE_CPUS:-unavailable}"
    printf 'parkana_effective_memory_bytes=%s\n' "${EFFECTIVE_MEMORY_BYTES:-unavailable}"
    printf 'parkana_effective_image=%s\n' "${EFFECTIVE_IMAGE:-unavailable}"
    printf 'parkana_runtime_report=runtime-parkana.txt\n'
  } >>"$file"
}

# cpu_usage_usec extracts the container's cgroup-v2 cumulative CPU time. The
# value is a counter, so a delta is meaningful only when both snapshots are
# readable and nondecreasing.
cpu_usage_usec() { # cpu.stat text
  awk '$1 == "usage_usec" && $2 ~ /^[0-9]+$/ { print $2; exit }' <<<"$1"
}

cpu_field_var() { printf 'CPU_%s_%s' "${1^^}" "$2"; }
set_cpu_field() { # backend field value
  local var
  var=$(cpu_field_var "$1" "$2")
  printf -v "$var" '%s' "$3"
}
get_cpu_field() { # backend field
  local var
  var=$(cpu_field_var "$1" "$2")
  printf '%s' "${!var:-}"
}

# read_backend_cgroup_file reads one small cgroup file from the running backend
# container. It is a single bounded exec, not a sampler: a missing file, an
# unavailable cgroup or a timeout returns empty so the caller records
# unavailable rather than inventing a zero. Unit tests call ingest helpers
# without a stack, so an unset project is also unavailable.
# run_backend_bounded runs one command in a backend container with the same
# timeout bound as the cgroup reads. Its output is for internal verification and
# hashing; callers must not print raw help because env-backed defaults can leak.
run_backend_bounded() { # backend command...
  local backend=$1
  shift
  local -a cmd
  compose_argv cmd
  if command -v timeout >/dev/null 2>&1; then
    timeout "$MEMORY_PEAK_TIMEOUT" "${cmd[@]}" exec -T "$backend" "$@"
  else
    "${cmd[@]}" exec -T "$backend" "$@"
  fi
}

read_backend_cgroup_file() { # backend path
  local backend=$1 path=$2
  [[ -n ${PROJECT:-} && -n ${SECRETS_FILE:-} ]] || return 0
  run_backend_bounded "$backend" cat "$path" 2>/dev/null || true
}

# capture_cpu_snapshot_before runs immediately before parkbench ingest starts.
capture_cpu_snapshot_before() { # backend
  local backend=$1 wall raw usec
  wall=$(epoch_now)
  raw=$(read_backend_cgroup_file "$backend" /sys/fs/cgroup/cpu.stat)
  usec=$(cpu_usage_usec "$raw")
  set_cpu_field "$backend" BEFORE_WALL_S "$wall"
  set_cpu_field "$backend" BEFORE_USEC "${usec:-unavailable}"
  if [[ -n $usec ]]; then
    set_cpu_field "$backend" SOURCE cgroup-v2-cpu.stat
    set_cpu_field "$backend" REASON ""
  else
    set_cpu_field "$backend" SOURCE unavailable
    set_cpu_field "$backend" REASON "unavailable: /sys/fs/cgroup/cpu.stat usage_usec not readable"
  fi
}

# capture_cpu_snapshot_after runs immediately after parkbench ingest returns,
# before the report is parsed. The delta is container-level CPU, not component
# attribution, and a backwards counter fails the run instead of publishing a
# negative or zero delta.
capture_cpu_snapshot_after() { # backend
  local backend=$1 wall raw usec before after delta before_wall
  wall=$(epoch_now)
  raw=$(read_backend_cgroup_file "$backend" /sys/fs/cgroup/cpu.stat)
  usec=$(cpu_usage_usec "$raw")
  before=$(get_cpu_field "$backend" BEFORE_USEC)
  before_wall=$(get_cpu_field "$backend" BEFORE_WALL_S)
  set_cpu_field "$backend" AFTER_WALL_S "$wall"
  set_cpu_field "$backend" AFTER_USEC "${usec:-unavailable}"
  if [[ -n $before_wall ]]; then
    set_cpu_field "$backend" WALL_S "$(elapsed_seconds "$before_wall" "$wall")"
  else
    set_cpu_field "$backend" WALL_S unavailable
  fi
  if [[ $before =~ ^[0-9]+$ && $usec =~ ^[0-9]+$ ]]; then
    if ((usec < before)); then
      fail "$backend cgroup cpu.stat usage_usec went backwards ($before -> $usec); refusing a non-monotonic CPU delta"
    fi
    delta=$((usec - before))
    set_cpu_field "$backend" DELTA_USEC "$delta"
    set_cpu_field "$backend" SOURCE cgroup-v2-cpu.stat
    set_cpu_field "$backend" REASON ""
  else
    set_cpu_field "$backend" DELTA_USEC unavailable
    set_cpu_field "$backend" SOURCE unavailable
    set_cpu_field "$backend" REASON "unavailable: missing or unreadable cpu.stat usage_usec snapshot"
  fi
  append_cpu_evidence "$backend"
}

# append_cpu_evidence publishes the before/after/delta and both wall snapshot
# boundaries. Unavailable fields stay explicit; they are never written as zero.
append_cpu_evidence() { # backend
  local backend=$1 file="$OUTPUT_DIR/run-metadata.txt"
  local reason
  [[ -f $file ]] || {
    warn "no run metadata at $file; $backend CPU snapshot not recorded"
    return 0
  }
  reason=$(get_cpu_field "$backend" REASON)
  {
    printf 'ingest_%s_cpu_before_usage_usec=%s\n' "$backend" "$(get_cpu_field "$backend" BEFORE_USEC)"
    printf 'ingest_%s_cpu_after_usage_usec=%s\n' "$backend" "$(get_cpu_field "$backend" AFTER_USEC)"
    printf 'ingest_%s_cpu_delta_usage_usec=%s\n' "$backend" "$(get_cpu_field "$backend" DELTA_USEC)"
    printf 'ingest_%s_cpu_before_wall_s=%s\n' "$backend" "$(get_cpu_field "$backend" BEFORE_WALL_S)"
    printf 'ingest_%s_cpu_after_wall_s=%s\n' "$backend" "$(get_cpu_field "$backend" AFTER_WALL_S)"
    printf 'ingest_%s_cpu_wall_s=%s\n' "$backend" "$(get_cpu_field "$backend" WALL_S)"
    printf 'ingest_%s_cpu_source=%s\n' "$backend" "$(get_cpu_field "$backend" SOURCE)"
    printf 'ingest_%s_cpu_scope=container cgroup-v2 usage_usec delta across the ingest command; not component attribution\n' "$backend"
    [[ -z $reason ]] || printf 'ingest_%s_cpu_reason=%s\n' "$backend" "$reason"
  } >>"$file"
  if [[ $backend == parkana ]]; then
    {
      printf 'ingest_cpu_before_usage_usec=%s\n' "$(get_cpu_field "$backend" BEFORE_USEC)"
      printf 'ingest_cpu_after_usage_usec=%s\n' "$(get_cpu_field "$backend" AFTER_USEC)"
      printf 'ingest_cpu_delta_usage_usec=%s\n' "$(get_cpu_field "$backend" DELTA_USEC)"
      printf 'ingest_cpu_before_wall_s=%s\n' "$(get_cpu_field "$backend" BEFORE_WALL_S)"
      printf 'ingest_cpu_after_wall_s=%s\n' "$(get_cpu_field "$backend" AFTER_WALL_S)"
      printf 'ingest_cpu_wall_s=%s\n' "$(get_cpu_field "$backend" WALL_S)"
      printf 'ingest_cpu_source=%s\n' "$(get_cpu_field "$backend" SOURCE)"
      [[ -z $reason ]] || printf 'ingest_cpu_reason=%s\n' "$reason"
    } >>"$OUTPUT_DIR/runtime-parkana.txt"
  fi
}

# capture_parkana_memory_peak reads the container cgroup-v2 high-water mark once,
# after ingest/visibility and before the container stops. It is the peak since
# container start through this phase, not an instantaneous sample and not the
# configured memory limit. An unreadable value is recorded as unavailable.
capture_parkana_memory_peak() {
  local raw peak="" source=unavailable reason=""
  raw=$(read_backend_cgroup_file parkana /sys/fs/cgroup/memory.peak)
  raw=$(printf '%s' "$raw" | tr -d ' \r\n')
  if [[ $raw =~ ^[1-9][0-9]*$ ]]; then
    peak=$raw
    source=cgroup-v2-memory.peak
  elif [[ -z $raw ]]; then
    reason="unavailable: /sys/fs/cgroup/memory.peak not readable"
  else
    reason="unavailable: invalid memory.peak value '$raw'"
  fi
  EFFECTIVE_INGEST_MEMORY_PEAK_BYTES=${peak:-unavailable}
  EFFECTIVE_INGEST_MEMORY_PEAK_SOURCE=$source
  EFFECTIVE_INGEST_MEMORY_PEAK_REASON=$reason
  {
    printf 'ingest_memory_peak_bytes=%s\n' "$EFFECTIVE_INGEST_MEMORY_PEAK_BYTES"
    printf 'ingest_memory_peak_source=%s\n' "$source"
    printf 'ingest_memory_peak_scope=container cgroup-v2 high-water since start through ingest, read before stop; not the memory limit\n'
    [[ -z $reason ]] || printf 'ingest_memory_peak_reason=%s\n' "$reason"
  } >>"$OUTPUT_DIR/runtime-parkana.txt"
  append_memory_peak_evidence
}

# append_memory_peak_evidence keeps the peak next to, but distinct from, the
# configured limit already recorded as parkana_effective_memory_bytes.
append_memory_peak_evidence() {
  local file="$OUTPUT_DIR/run-metadata.txt"
  [[ -f $file ]] || {
    warn "no run metadata at $file; Parkana memory peak not recorded"
    return 0
  }
  {
    printf 'parkana_ingest_memory_peak_bytes=%s\n' "${EFFECTIVE_INGEST_MEMORY_PEAK_BYTES:-unavailable}"
    printf 'parkana_ingest_memory_peak_source=%s\n' "${EFFECTIVE_INGEST_MEMORY_PEAK_SOURCE:-unavailable}"
    printf 'parkana_ingest_memory_peak_scope=container cgroup-v2 high-water since start through ingest, read before stop; not the memory limit\n'
    [[ -z ${EFFECTIVE_INGEST_MEMORY_PEAK_REASON:-} ]] ||
      printf 'parkana_ingest_memory_peak_reason=%s\n' "$EFFECTIVE_INGEST_MEMORY_PEAK_REASON"
  } >>"$file"
}

# capture_parkana_memory_events reads the cgroup-v2 oom_kill counter once,
# beside the high-water mark and before the container stops. Unreadable or
# malformed input is unavailable, never zero.
capture_parkana_memory_events() {
  local raw oom="" source=unavailable reason=""
  raw=$(read_backend_cgroup_file parkana /sys/fs/cgroup/memory.events)
  if [[ -z $raw ]]; then
    reason="unavailable: /sys/fs/cgroup/memory.events not readable"
  else
    oom=$(awk '$1 == "oom_kill" && $2 ~ /^[0-9]+$/ { print $2; exit }' <<<"$raw")
    if [[ -n $oom ]]; then
      source=cgroup-v2-memory.events
    else
      reason="unavailable: memory.events has no oom_kill counter"
    fi
  fi
  EFFECTIVE_INGEST_OOM_KILL=${oom:-unavailable}
  EFFECTIVE_INGEST_OOM_KILL_SOURCE=$source
  EFFECTIVE_INGEST_OOM_KILL_REASON=$reason
  {
    printf 'ingest_oom_kill=%s\n' "$EFFECTIVE_INGEST_OOM_KILL"
    printf 'ingest_oom_events_source=%s\n' "$source"
    printf 'ingest_oom_events_scope=container cgroup-v2 memory.events oom_kill counter since container start, read before stop\n'
    [[ -z $reason ]] || printf 'ingest_oom_events_reason=%s\n' "$reason"
  } >>"$OUTPUT_DIR/runtime-parkana.txt"
  local file="$OUTPUT_DIR/run-metadata.txt"
  [[ -f $file ]] || { warn "no run metadata at $file; Parkana OOM counter not recorded"; return 0; }
  {
    printf 'parkana_ingest_oom_kill=%s\n' "$EFFECTIVE_INGEST_OOM_KILL"
    printf 'parkana_ingest_oom_kill_source=%s\n' "$source"
    printf 'parkana_ingest_oom_kill_scope=container cgroup-v2 memory.events oom_kill counter since container start, read before stop\n'
    [[ -z $reason ]] || printf 'parkana_ingest_oom_kill_reason=%s\n' "$reason"
  } >>"$file"
}

stop_backend() { # parkana|loki
  local backend=$1 service
  case "$backend" in
    parkana) service=parkana ;;
    loki) service=loki ;;
    *) fail "unknown backend '$backend'" ;;
  esac
  log "stopping $backend"
  if ! compose stop --timeout 30 "$service" >>"$RUN_LOG" 2>&1; then
    fail "compose stop $service failed; refusing to continue with a possibly running backend"
  fi
  local deadline=$((SECONDS + STOP_TIMEOUT)) id state
  id=$(compose ps -a -q "$service" 2>/dev/null | tail -n 1 || true)
  [[ -n $id ]] ||
    fail "could not inspect $service after stop; cannot prove the backend exited"
  while :; do
    state=$("$DOCKER" inspect -f '{{.State.Status}}' "$id" 2>/dev/null || true)
    case "$state" in
      exited | stopped) return 0 ;;
      dead)
        fail "$service entered state 'dead' after stop; cannot prove a clean exit"
        ;;
    esac
    if ((SECONDS >= deadline)); then
      fail "$service did not exit within ${STOP_TIMEOUT}s after stop (state: ${state:-unavailable})"
    fi
    nap 1
  done
}

teardown_stack() {
  ((STACK_OWNED)) || return 0
  log "removing this run's Compose project (project=$PROJECT)"
  compose down -v --remove-orphans --timeout 30 >>"$RUN_LOG" 2>&1 ||
    warn "compose down failed for project $PROJECT; inspect it manually"
}

collect_diagnostics() {
  local file="$OUTPUT_DIR/diagnostics.log"
  {
    printf '=== compose ps ===\n'
    compose ps -a 2>&1 || true
    printf '\n=== compose images ===\n'
    compose images 2>&1 || true
    local -a svcs=(garage garage-init-parkana)
    ((PARKANA_ONLY)) || svcs+=(garage-init-loki)
    svcs+=(parkana)
    ((PARKANA_ONLY)) || svcs+=(loki)
    local svc
    for svc in "${svcs[@]}"; do
      printf '\n=== logs %s ===\n' "$svc"
      compose logs --no-color --tail=200 "$svc" 2>&1 || true
    done
  } >"$file" 2>&1 || true
  redact_file "$file"
  warn "diagnostics written to $file"
}

on_exit() {
  local status=$1
  trap - EXIT INT TERM
  if ((status != 0)) && [[ -n ${OUTPUT_DIR:-} && -d ${OUTPUT_DIR:-} ]]; then
    collect_diagnostics || true
  fi
  if [[ -n ${OUTPUT_DIR:-} && -d ${OUTPUT_DIR:-} ]]; then
    emit_archive >&3 || warn "artifact archive export failed"
  fi
  if ((STACK_OWNED)); then
    if ((KEEP)); then
      log "keeping stack project '$PROJECT' and $SECRETS_FILE (--keep)"
    else
      teardown_stack || true
    fi
  fi
  release_project_lock || true
  if ((KEEP == 0)) && [[ -n $SECRETS_FILE && -f $SECRETS_FILE ]]; then
    rm -f -- "$SECRETS_FILE"
  fi
  # A failure before the first Compose command owns no stack and no artifacts;
  # remove only the empty directory this run just created.
  if ((status != 0 && STACK_OWNED == 0)) && [[ -n $OUTPUT_DIR && -d $OUTPUT_DIR ]]; then
    rmdir -- "$OUTPUT_DIR" 2>/dev/null || true
  fi
  exit "$status"
}

# --- redaction --------------------------------------------------------------

redact_file() {
  local file=$1 v
  [[ -f $file ]] || return 0
  for v in "${PARKANA_BENCH_PARKANA_SECRET_KEY:-}" "${PARKANA_BENCH_LOKI_SECRET_KEY:-}" \
    "${PARKANA_BENCH_GARAGE_KEY_SECRET:-}" "${LOKI_BENCH_GARAGE_KEY_SECRET:-}" \
    "${PARKANA_BENCH_PARKANA_ACCESS_KEY:-}" "${PARKANA_BENCH_LOKI_ACCESS_KEY:-}" \
    "${PARKANA_BENCH_GARAGE_KEY_ID:-}" "${LOKI_BENCH_GARAGE_KEY_ID:-}"; do
    [[ -n $v ]] || continue
    sed -i "s/$v/<redacted>/g" "$file"
  done
}

# --- parkbench commands -----------------------------------------------------

manifest_string() { # file field
  sed -n "s/^[[:space:]]*\"$2\"[[:space:]]*:[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p" "$1" | head -n 1
}

manifest_uint() { # file field
  sed -n "s/^[[:space:]]*\"$2\"[[:space:]]*:[[:space:]]*\\([0-9][0-9]*\\).*/\\1/p" "$1" | head -n 1
}

manifest_number() { # file field (float or int)
  sed -n "s/^[[:space:]]*\"$2\"[[:space:]]*:[[:space:]]*\\([-0-9.eE+][-0-9.eE+]*\\).*/\\1/p" "$1" | head -n 1
}

manifest_clean() { grep -Eq '"clean"[[:space:]]*:[[:space:]]*true' "$1"; }

# manifest_stream_list prints the actual corpus stream names as a comma list.
# It reads only the root-level corpus.streams array and stops at its closing
# bracket, so marker/template "name" fields can never leak into the evidence.
manifest_stream_list() { # file
  awk '
    /^[ \t]*"streams"[ \t]*:[ \t]*\[/ { in_streams = 1; next }
    in_streams {
      if (match($0, /^[ \t]*\][ \t]*,?[ \t]*$/)) exit
      if (match($0, /^[ \t]*\{?[ \t]*"name"[ \t]*:[ \t]*"/)) {
        line = $0
        sub(/^[ \t]*\{?[ \t]*"name"[ \t]*:[ \t]*"/, "", line)
        sub(/".*/, "", line)
        printf "%s%s", sep, line
        sep = ","
      }
    }
  ' "$1"
}

# Parkbench corpus-v3 ingest report batching schema, fixed by parkbench's
# Report.BatchingStats JSON tags.  Only the exact root-level block and these six
# fields are recorded: push_requests and attempted_entries count HTTP attempts
# that entered client.Do including retries, so mean_entries_per_request is real
# request size and the attempt denominator survives in metadata.
BATCHING_SECTION=batching
BATCHING_FIELDS=(
  entries_per_request
  push_requests
  attempted_entries
  min_entries_per_request
  max_entries_per_request
  mean_entries_per_request
)
# The achieved label-set count comes from corpus identity; sender concurrency
# comes from the measured parallelism block, never from the --concurrency flag.
CORPUS_SECTION=corpus
CORPUS_FIELDS=(instances_per_service)
PARALLELISM_SECTION=parallelism
PARALLELISM_FIELDS=(
  label_sets
  active_senders
  concurrency_limit
  peak_in_flight_pushes
)

# Read one numeric field from an exact root-level object block of a parkbench
# report.  The block is matched by name at the document root only, so an
# identically named key or numeric field inside another object (for example
# delivery.batches or delivery.batching.push_requests) can never be published
# as batching evidence.  Nested objects and arrays inside the block are skipped
# by brace depth.  Print the value only when the whole token is a JSON number;
# missing, null, quote-wrapped and malformed/partial values produce no output so
# the caller can record them as explicitly unavailable.
manifest_object_field() { # file section field
  awk -v section="$2" -v field="$3" '
    function trim(s) {
      sub(/^[ \t]+/, "", s)
      sub(/[ \t]+$/, "", s)
      return s
    }
    {
      line = $0
      if (line ~ /^[ \t]*$/) next
      if (in_section) {
        # Track braces so nested arrays/objects cannot end the block early and
        # only direct children are considered for field extraction.
        if (match(line, /^[ \t]*"[^"]+"[ \t]*:[ \t]*\{[ \t]*$/) ||
            match(line, /^[ \t]*\{[ \t]*,?[ \t]*$/)) {
          depth++
          next
        }
        if (match(line, /^[ \t]*\}[ \t]*,?[ \t]*$/)) {
          if (depth == section_depth) exit
          depth--
          next
        }
        if (depth != section_depth) next
        if (!match(line, /^[ \t]*"[^"]+"[ \t]*:[ \t]*/)) next
        key = line
        sub(/^[ \t]*"/, "", key)
        sub(/".*/, "", key)
        if (key != field) next
        value = line
        sub(/^[ \t]*"[^"]+"[ \t]*:[ \t]*/, "", value)
        value = trim(value)
        sub(/,$/, "", value)
        value = trim(value)
        if (value ~ /^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$/) print value
        exit
      }
      # Only a root-level named object can open the requested block.  The root
      # brace itself is depth 1, so root-level keys sit at depth 1; track bare
      # array-element objects too so nesting cannot fake the block.
      if (match(line, /^[ \t]*"[^"]+"[ \t]*:[ \t]*\{[ \t]*$/)) {
        key = line
        sub(/^[ \t]*"/, "", key)
        sub(/".*/, "", key)
        if (depth == 1 && key == section) {
          in_section = 1
          depth++
          section_depth = depth
        } else {
          depth++
        }
        next
      }
      if (match(line, /^[ \t]*\{[ \t]*,?[ \t]*$/)) {
        depth++
        next
      }
      if (match(line, /^[ \t]*\}[ \t]*,?[ \t]*$/)) {
        if (depth > 0) depth--
        next
      }
    }
  ' "$1"
}

run_ingest_backend() { # backend url output start
  local backend=$1 url=$2 out=$3 start=$4
  local -a args=(-url "$url" -bytes "$TARGET_BYTES" -step "$STEP"
    -batch-lines "$BATCH_LINES" -concurrency "$CONCURRENCY"
    -timeout "$INGEST_ATTEMPT_TIMEOUT" -retries "$INGEST_RETRIES" -output "$out")
  # Omitted means "let the tool choose"; never pass a hardcoded 4 here.
  [[ -z $INSTANCES_PER_SERVICE ]] || args+=(-instances-per-service "$INSTANCES_PER_SERVICE")
  # Omitted streams are omitted too, preserving parkbench's four-stream default.
  [[ $STREAMS_EXPLICIT == 0 ]] || args+=(-streams "$STREAMS")
  [[ -n $SEED ]] || true
  [[ -z $SEED ]] || args+=(-seed "$SEED")
  [[ -z $start ]] || args+=(-start "$start")
  [[ -z $TENANT ]] || args+=(-tenant "$TENANT")

  log "ingesting $TARGET_BYTES raw line bytes into $backend"
  capture_cpu_snapshot_before "$backend"
  INGEST_START_AT=$(epoch_now)
  if ! "$PARKBENCH" ingest "${args[@]}" 2>&1 | tee -a "$RUN_LOG"; then
    fail "parkbench ingest ($backend) failed; see $RUN_LOG"
  fi
  # The acknowledgment endpoint is the ingest command's return. The CPU read is
  # a bounded diagnostic after the ack and must never move this wall endpoint.
  ACK_AT=$(epoch_now)
  capture_cpu_snapshot_after "$backend"
  [[ -f $out ]] || fail "parkbench ingest ($backend) did not write $out"
  manifest_clean "$out" || fail "parkbench ingest ($backend) reported a dirty/incomplete delivery"
  ACK_REPORT_SECONDS=$(manifest_number "$out" elapsed_seconds)
  ACK_WALL_SECONDS=$(elapsed_seconds "$INGEST_START_AT" "$ACK_AT")
  [[ -n $ACK_REPORT_SECONDS ]] || fail "parkbench ingest ($backend) report has no elapsed_seconds"
  local lines bytes
  lines=$(manifest_uint "$out" ack_lines)
  bytes=$(manifest_uint "$out" ack_raw_line_bytes)
  [[ -n $lines && -n $bytes ]] || fail "parkbench ingest ($backend) report has no acknowledged totals"
  record_ingest_evidence "$backend" "$out"
  log "$backend acknowledged $lines lines / $bytes raw bytes in ${ACK_REPORT_SECONDS}s (wall ${ACK_WALL_SECONDS}s)"
}

compare_manifests() {
  local a=$1 b=$2
  local fields=(hash content_hash start through step_micros target_raw_line_bytes raw_line_bytes lines ack_lines ack_raw_line_bytes)
  local f av bv
  for f in "${fields[@]}"; do
    av=$(manifest_string "$a" "$f")
    [[ -n $av ]] || av=$(manifest_uint "$a" "$f")
    bv=$(manifest_string "$b" "$f")
    [[ -n $bv ]] || bv=$(manifest_uint "$b" "$f")
    [[ -n $av && -n $bv ]] || fail "manifest field '$f' missing (parkana='$av', loki='$bv')"
    [[ $av == "$bv" ]] || fail "corpus mismatch: $f parkana='$av' loki='$bv'"
  done
  log "corpus identity verified: hash, content_hash, start, bytes, lines and acknowledgments match"
}

run_query_backend() { # backend base manifest
  local backend=$1 base=$2 manifest=$3
  if ((SKIP_QUERIES)); then
    log "query stage skipped for $backend (--skip-queries)"
    printf 'status=skipped\nreason=--skip-queries\n' >"$OUTPUT_DIR/query-$backend.txt"
    return 0
  fi
  local -a args=(query --backend "$backend" --url "$base" --manifest "$manifest"
    --repetitions "$REPETITIONS" --warmups "$WARMUPS" --timeout "$QUERY_TIMEOUT"
    --classes "$CLASSES" --windows "$WINDOWS"
    --output "$OUTPUT_DIR/query-$backend.json"
    --markdown "$OUTPUT_DIR/query-$backend.md")
  [[ $backend == parkana ]] && args+=(--metrics-url "$base/metrics")
  [[ -z $TENANT ]] || args+=(--tenant "$TENANT")
  log "running query workload against $backend"
  if ! "$PARKBENCH" "${args[@]}" 2>&1 | tee -a "$RUN_LOG"; then
    fail "parkbench query ($backend) failed; query-$backend.json may hold a valid=false report"
  fi
}

# --- visibility and storage -------------------------------------------------

range_ms_from() { # from through
  local from_epoch through_epoch
  from_epoch=$(date -u -d "$1" +%s.%N) || return 1
  through_epoch=$(date -u -d "$2" +%s.%N) || return 1
  awk -v a="$from_epoch" -v b="$through_epoch" 'BEGIN {
    if (b < a) exit 1
    printf "%.0f", (b - a) * 1000 + 1000
  }'
}

count_from_json() {
  [[ -n ${1:-} ]] || return 1
  if grep -Eq '"result"[[:space:]]*:[[:space:]]*\[\]' <<<"$1"; then
    printf '0\n'
    return 0
  fi
  local values
  values=$(grep -o '"value":\[[^]]*\]' <<<"$1" |
    sed -n 's/.*,[[:space:]]*"\([0-9.eE+-]*\)"\]$/\1/p') || return 1
  [[ -n $values ]] || return 1
  awk 'BEGIN { sum = 0; n = 0 }
    { sum += $1; n++ }
    END { if (n == 0) exit 1; printf "%.0f\n", sum }' <<<"$values"
}

exact_count() { # base from through
  local base=$1 from=$2 through=$3 range_ms body
  range_ms=$(range_ms_from "$from" "$through") ||
    fail "cannot compute the corpus range for the visibility count"
  body=$(http_query "$base" "sum(count_over_time({service_name=~\".+\"} [${range_ms}ms]))" "$through") || return 1
  count_from_json "$body"
}

catalog_snapshot() {
  compose --profile tools run --rm --no-deps -T --entrypoint sqlite3 bench-tools \
    -readonly -batch -noheader -separator '|' /data/catalog.db \
    'SELECT COALESCE(SUM(rows),0), COALESCE(SUM(bytes),0), COUNT(*), COALESCE(SUM(CASE WHEN has_sidecar THEN 1 ELSE 0 END),0) FROM files WHERE deleted_at IS NULL;' \
    2>>"$RUN_LOG"
}

catalog_field() { # snapshot index
  local snap=$1 idx=$2
  local IFS='|'
  local -a fields
  # shellcheck disable=SC2206 # deliberate IFS split of a fixed '|' row
  fields=($snap)
  printf '%s' "${fields[$((idx - 1))]:-}"
}

wait_parkana_visible() { # want
  local want=$1 deadline=$((SECONDS + VISIBILITY_TIMEOUT))
  local snap rows got
  while :; do
    snap=$(catalog_snapshot 2>/dev/null || true)
    rows=$(catalog_field "$snap" 1)
    case "$rows" in
      "" | *[!0-9]*) rows="" ;;
    esac
    if [[ -n $rows ]]; then
      if ((rows > want)); then
        fail "Parkana catalog has $rows rows, more than the $want acknowledged (duplicate/unclean run)"
      fi
      if ((rows == want)); then
        got=$(exact_count "$PARKANA_URL" \
          "$(manifest_string "$OUTPUT_DIR/ingest-parkana.json" start)" \
          "$(manifest_string "$OUTPUT_DIR/ingest-parkana.json" through)" 2>/dev/null || true)
        if [[ -n $got ]]; then
          if ((got > want)); then
            fail "Parkana query count is $got, more than the $want acknowledged"
          fi
          if ((got == want)); then
            QUERYABLE_AT=$(epoch_now)
            log "Parkana exact visibility: catalog=$rows, query count=$got"
            return 0
          fi
        fi
      fi
    fi
    if ((SECONDS >= deadline)); then
      fail "Parkana did not expose exactly $want acknowledged rows within ${VISIBILITY_TIMEOUT}s (last catalog='${rows:-unavailable}', last count='${got:-unavailable}')"
    fi
    nap 2
  done
}

metric_sum() { # prometheus-text metric-name
  awk -v name="$2" '
    $0 !~ /^#/ {
      if (index($0, name) == 1) {
        rest = substr($0, length(name) + 1, 1)
        if (rest != "{" && rest != " " && rest != "\t") next
        block = $0
        sub(/^[^{ \t]+(\{[^}]*\})?[ \t]+/, "", block)
        if (block ~ /^[-+]?[0-9.eE+]+$/) { sum += block; found = 1 }
      }
    }
    END { if (found) printf "%.0f\n", sum }' <<<"$1"
}

metric_labeled_sum() { # prometheus-text exact-family-name label-substring
  awk -v name="$2" -v label="$3" '
    $0 !~ /^#/ && index($0, name) == 1 {
      rest = substr($0, length(name) + 1, 1)
      if (rest != "{" && rest != " " && rest != "\t") next
      if (index($0, label) == 0) next
      v = $NF
      if (v ~ /^[-+]?[0-9.eE+]+$/) { sum += v; found = 1 }
    }
    END { if (found) printf "%.9f\n", sum }' <<<"$1"
}

loki_flush_wait() {
  # Pinned Loki v3.7.9: pkg/loki/modules.go registers GET/POST /flush;
  # pkg/ingester/flush.go FlushHandler calls sweepUsers then writes 204, so
  # 204 alone only means "scheduled". The metric names below are registered in
  # pkg/ingester/metrics.go (ingester_flush_queue_length,
  # ingester_chunks_flushed_total, ingester_chunks_flush_failures_total).
  #
  # chunks_flushed_total is a CounterVec and a fresh process may not expose a
  # labeled series until its first successful flush. Treat the pre-flush
  # value as an optional baseline, POST anyway, and require an actual
  # post-flush value (and movement over the baseline when one existed).
  local status before_metrics before_flushed before_failures before_present=0
  local t0 metrics queue flushed failures advanced prev="" stable=0 deadline
  before_metrics=$(http_get "$LOKI_URL/metrics" || true)
  before_flushed=$(metric_sum "$before_metrics" loki_ingester_chunks_flushed_total)
  before_failures=$(metric_sum "$before_metrics" loki_ingester_chunks_flush_failures_total)
  [[ -n $before_flushed ]] && before_present=1
  [[ -n $before_failures ]] || before_failures=0
  status=$(http_post "$LOKI_URL/flush")
  if [[ $status != 204 ]]; then
    fail "Loki /flush returned HTTP $status (the flush barrier cannot be established)"
  fi
  t0=$(epoch_now)
  deadline=$((SECONDS + FLUSH_TIMEOUT))
  while :; do
    metrics=$(http_get "$LOKI_URL/metrics" || true)
    queue=$(metric_sum "$metrics" loki_ingester_flush_queue_length)
    flushed=$(metric_sum "$metrics" loki_ingester_chunks_flushed_total)
    failures=$(metric_sum "$metrics" loki_ingester_chunks_flush_failures_total)
    [[ -n $failures ]] || failures=0
    ((failures > before_failures)) &&
      fail "Loki flush failures increased from $before_failures to $failures; the barrier is invalid"
    # A 204 only schedules flushes (pkg/ingester/flush.go FlushHandler), so a
    # drained queue is not enough: require at least one chunk to have actually
    # been flushed relative to the optional pre-flush baseline, and the counter
    # to stop moving. A fresh CounterVec may expose no labeled series for the
    # first post-flush scrapes while the first flush is in flight, and a
    # transport error yields no samples; neither is zero and both are retried
    # until the deadline.
    if [[ -n $queue && -n $flushed ]]; then
      advanced=0
      if ((before_present)); then
        ((flushed > before_flushed)) && advanced=1
      else
        ((flushed > 0)) && advanced=1
      fi
      if [[ $queue == 0 ]] && ((advanced)); then
        if [[ -n $prev && $flushed == "$prev" ]]; then
          stable=$((stable + 1))
        else
          stable=0
        fi
        ((stable >= 1)) && break
      else
        stable=0
      fi
      prev=$flushed
    else
      # Never turn a missing gauge/counter into zero.
      stable=0
    fi
    if ((SECONDS >= deadline)); then
      fail "Loki post-flush metrics unavailable within ${FLUSH_TIMEOUT}s (queue=${queue:-unavailable}, flushed=${flushed:-unavailable}, pre_flush=${before_flushed:-unavailable})"
    fi
    nap 1
  done
  LOKI_FLUSH_SECONDS=$(elapsed_seconds "$t0" "$(epoch_now)")
  log "Loki flush verified after ${LOKI_FLUSH_SECONDS}s (queue drained, flushed counter advanced and stable)"
}

wait_loki_visible() { # want
  local want=$1 deadline=$((SECONDS + VISIBILITY_TIMEOUT)) got
  while :; do
    got=$(exact_count "$LOKI_URL" \
      "$(manifest_string "$OUTPUT_DIR/ingest-loki.json" start)" \
      "$(manifest_string "$OUTPUT_DIR/ingest-loki.json" through)" 2>/dev/null || true)
    if [[ -n $got ]]; then
      if ((got > want)); then
        fail "Loki query count is $got, more than the $want acknowledged"
      fi
      if ((got == want)); then
        QUERYABLE_AT=$(epoch_now)
        log "Loki exact visibility: query count=$got after flush"
        return 0
      fi
    fi
    if ((SECONDS >= deadline)); then
      fail "Loki did not expose exactly $want acknowledged rows within ${VISIBILITY_TIMEOUT}s (last count='${got:-unavailable}')"
    fi
    nap 2
  done
}

verify_count_after_restart() { # backend base manifest
  local backend=$1 base=$2 manifest=$3 want got deadline
  want=$(manifest_uint "$manifest" ack_lines)
  [[ -n $want ]] || fail "manifest $manifest has no ack_lines"
  log "verifying $backend data after restart"
  deadline=$((SECONDS + VISIBILITY_TIMEOUT))
  while :; do
    got=$(exact_count "$base" \
      "$(manifest_string "$manifest" start)" \
      "$(manifest_string "$manifest" through)" 2>/dev/null || true)
    if [[ -n $got ]]; then
      ((got > want)) && fail "$backend query count after restart is $got, more than $want acknowledged (duplicate replay)"
      ((got == want)) && { log "$backend post-restart count verified: $got"; return 0; }
    fi
    ((SECONDS >= deadline)) &&
      fail "$backend did not return $want rows after restart within ${VISIBILITY_TIMEOUT}s (last count='${got:-unavailable}')"
    nap 2
  done
}

wait_metric_zero() { # label url metric
  local label=$1 url=$2 metric=$3 deadline=$((SECONDS + VISIBILITY_TIMEOUT)) metrics value
  while :; do
    metrics=$(http_get "$url" || true)
    value=$(metric_sum "$metrics" "$metric")
    [[ -n $value ]] || fail "$label metric $metric is missing; cannot prove background work is idle"
    ((${value%.*} == 0)) && { log "$label idle ($metric=0) before query phase"; return 0; }
    ((SECONDS >= deadline)) && fail "$label did not become idle within ${VISIBILITY_TIMEOUT}s ($metric=$value)"
    nap 2
  done
}

write_ratio() { # numerator denominator label
  awk -v a="$1" -v b="$2" -v label="$3" 'BEGIN {
    if (b > 0) printf "%s=%.6f\n", label, a / b
    else printf "%s=null\n", label
  }'
}

measure_storage_parkana() {
  local snap data_bytes files sidecars metrics build_counter_bytes local_bytes raw
  local info bucket_bytes bucket_objects expected_objects sidecar_bytes sidecar_objects
  local build_count build_sum build_mean
  snap=$(catalog_snapshot) || fail "cannot read the Parkana catalog for storage measurement"
  data_bytes=$(catalog_field "$snap" 2)
  files=$(catalog_field "$snap" 3)
  sidecars=$(catalog_field "$snap" 4)
  raw=$(manifest_uint "$OUTPUT_DIR/ingest-parkana.json" raw_line_bytes)
  [[ -n $data_bytes && -n $files && -n $sidecars ]] ||
    fail "Parkana catalog snapshot is missing bytes/files/sidecar counts"
  info=$(garage_bucket_info "$PARKANA_BENCH_PARKANA_BUCKET") ||
    fail "cannot read Parkana's Garage bucket info for exact sidecar bytes"
  bucket_bytes=$(garage_json_uint "$info" bytes)
  bucket_objects=$(garage_json_uint "$info" objects)
  [[ -n $bucket_bytes && -n $bucket_objects ]] ||
    fail "Garage GetBucketInfo has no exact bytes/objects fields for the Parkana bucket"
  ((bucket_bytes >= data_bytes)) ||
    fail "Parkana bucket has $bucket_bytes bytes but the catalog claims $data_bytes data bytes"
  ((bucket_objects >= files)) ||
    fail "Parkana bucket has $bucket_objects objects but the catalog claims $files Parquet files"
  sidecar_bytes=$((bucket_bytes - data_bytes))
  sidecar_objects=$((bucket_objects - files))
  expected_objects=$((files + sidecars))
  ((sidecar_objects == sidecars && bucket_objects == expected_objects)) ||
    fail "Parkana bucket object count $bucket_objects != Parquet files $files + catalog sidecar flags $sidecars (orphan or missing object)"
  metrics=$(http_get "$PARKANA_URL/metrics" || true)
  # This counter measures sidecar bytes *built*, not bytes resident in the
  # bucket. Keep it only as a labelled cross-check; exact storage comes from
  # the Garage bucket total minus the catalog's exact Parquet bytes.
  build_counter_bytes=$(metric_sum "$metrics" parkana_index_bytes_total)
  build_count=$(metric_labeled_sum "$metrics" parkana_index_build_duration_seconds_count 'source="ingest"')
  build_sum=$(metric_labeled_sum "$metrics" parkana_index_build_duration_seconds_sum 'source="ingest"')
  build_mean=""
  if [[ -n $build_sum && -n $build_count ]] && awk -v c="$build_count" 'BEGIN { exit !(c > 0) }'; then
    build_mean=$(awk -v s="$build_sum" -v c="$build_count" 'BEGIN { printf "%.6f", s / c }')
  fi
  printf '%s\n' "$metrics" | grep '^parkana_index_build_duration_seconds' |
    grep 'source="ingest"' >"$OUTPUT_DIR/index-build-parkana.txt" ||
    printf 'no parkana_index_build_duration_seconds{source="ingest"} series\n' >"$OUTPUT_DIR/index-build-parkana.txt"
  local_bytes=""
  local out
  out=$(compose exec -T parkana du -sb /data 2>/dev/null || true)
  [[ $out =~ ^([0-9]+) ]] && local_bytes=${BASH_REMATCH[1]}

  # A published run (query phase enabled) must carry real sidecar evidence.
  # The stack/ingest smoke (--skip-queries) may run before part A is relayed.
  if ((SKIP_QUERIES == 0)); then
    ((sidecar_objects > 0)) ||
      fail "no sidecar objects in the Parkana bucket; part A sidecar writes are not present in this build"
    [[ -n $build_counter_bytes && -n $build_count && -n $build_sum ]] ||
      fail "parkana_index_build_*{source=\"ingest\"} metrics are missing; cannot publish sidecar evidence"
  fi
  {
    printf 'backend=parkana\n'
    printf 'raw_line_bytes=%s\n' "${raw:-null}"
    printf 'data_object_bytes=%s\n' "$data_bytes"
    printf 'compressed_parquet_bytes=%s\n' "$data_bytes"
    printf 'parquet_files=%s\n' "$files"
    printf 'sidecar_objects=%s\n' "$sidecar_objects"
    printf 'sidecar_object_bytes=%s\n' "$sidecar_bytes"
    printf 'sidecar_object_bytes_source=garage_bucket_total_minus_catalog_data\n'
    printf 'sidecar_build_counter_bytes=%s\n' "${build_counter_bytes:-null}"
    printf 'sidecar_build_counter_source=parkana_index_bytes_total (cumulative builds, not stored objects)\n'
    printf 'index_build_duration_seconds_count_ingest=%s\n' "${build_count:-null}"
    printf 'index_build_duration_seconds_sum_ingest=%s\n' "${build_sum:-null}"
    printf 'index_build_duration_seconds_mean_ingest=%s\n' "${build_mean:-null}"
    printf 'index_build_duration_buckets_file=index-build-parkana.txt\n'
    printf 'local_workdir_bytes_including_wal_cache_catalog=%s\n' "${local_bytes:-null}"
    write_ratio "$data_bytes" "${raw:-0}" data_bytes_to_raw_line_bytes
    write_ratio "$sidecar_bytes" "$data_bytes" sidecar_bytes_to_data_bytes
    write_ratio "$sidecar_bytes" "$data_bytes" sidecar_bytes_to_compressed_parquet_bytes
    write_ratio "$sidecar_bytes" "${raw:-0}" sidecar_bytes_to_raw_line_bytes
    if [[ -n $build_counter_bytes ]] && ((build_counter_bytes != sidecar_bytes)); then
      printf 'sidecar_build_counter_matches_stored=false\n'
    elif [[ -n $build_counter_bytes ]]; then
      printf 'sidecar_build_counter_matches_stored=true\n'
    else
      printf 'sidecar_build_counter_matches_stored=null\n'
    fi
  } >"$OUTPUT_DIR/storage-parkana.txt"
  log "Parkana storage: data=${data_bytes}B, sidecars=${sidecar_bytes}B over $sidecar_objects sidecar object(s)"
}

garage_bucket_info() { # bucket
  # Pinned Garage v2.5.0: src/api/admin/api.rs GetBucketInfoResponse has
  # `objects: i64` and `bytes: i64`; src/garage/cli/remote/mod.rs cmd_json_api
  # prints the unwrapped response. No invented field names here.
  compose exec -T garage /usr/local/bin/garage -c /etc/garage.toml \
    json-api GetBucketInfo "{\"search\":\"$1\"}"
}

garage_json_uint() { # json field
  printf '%s\n' "$1" |
    sed -n "s/.*\"$2\"[[:space:]]*:[[:space:]]*\\([0-9][0-9]*\\).*/\\1/p" |
    head -n 1
}

loki_local_snapshot() {
  local out bytes path
  LOKI_LOCAL_TOTAL=""
  LOKI_LOCAL_WAL=""
  LOKI_LOCAL_INDEX=""
  LOKI_LOCAL_INDEX_CACHE=""
  LOKI_LOCAL_COMPACTOR=""
  # The pinned Loki image is distroless (no shell/du). Use the existing
  # one-shot pushgen tool image against the read-only named volume. Never
  # report "unavailable" as if it were a measured local size. The only tab
  # split here is du's own <bytes><TAB><path> pair; callers get keyed globals so
  # a missing directory cannot shift later values into the wrong field.
  out=$(compose --profile tools run --rm --no-deps -T --entrypoint /bin/sh bench-tools-loki -c \
    'du -sb /loki 2>/dev/null; for d in /loki/wal /loki/index /loki/index_cache /loki/compactor; do du -sb "$d" 2>/dev/null; done' \
    2>>"$RUN_LOG") || true
  while IFS=$'\t' read -r bytes path; do
    [[ -n ${bytes:-} && -n ${path:-} ]] || continue
    case "$path" in
      /loki) LOKI_LOCAL_TOTAL=$bytes ;;
      /loki/wal) LOKI_LOCAL_WAL=$bytes ;;
      /loki/index) LOKI_LOCAL_INDEX=$bytes ;;
      /loki/index_cache) LOKI_LOCAL_INDEX_CACHE=$bytes ;;
      /loki/compactor) LOKI_LOCAL_COMPACTOR=$bytes ;;
    esac
  done <<<"$out"
  [[ -n $LOKI_LOCAL_TOTAL ]]
}

measure_storage_loki() {
  local info bytes objects raw
  # Garage v2.5.0 GetBucketInfoResponse: `objects` and `bytes` are i64 fields
  # (src/api/admin/api.rs in the pinned release). `garage json-api` prints the
  # unwrapped response JSON, so these are exact integer bucket totals.
  info=$(garage_bucket_info "$PARKANA_BENCH_LOKI_BUCKET") ||
    fail "cannot read Loki's Garage bucket info for storage measurement"
  bytes=$(garage_json_uint "$info" bytes)
  objects=$(garage_json_uint "$info" objects)
  raw=$(manifest_uint "$OUTPUT_DIR/ingest-loki.json" raw_line_bytes)
  [[ -n $bytes ]] || fail "Garage bucket info has no exact 'bytes' field for the Loki bucket"
  [[ -n $objects ]] || fail "Garage bucket info has no exact 'objects' field for the Loki bucket"
  loki_local_snapshot ||
    fail "Loki local workdir could not be measured via the one-shot tool volume"
  {
    printf 'backend=loki\n'
    printf 'snapshot_phase=post_flush_pre_query\n'
    printf 'raw_line_bytes=%s\n' "${raw:-null}"
    printf 'object_store_bytes=%s\n' "$bytes"
    printf 'objects=%s\n' "$objects"
    printf 'object_store_bytes_source=garage_getbucketinfo_bytes\n'
    printf 'local_workdir_total_bytes=%s\n' "$LOKI_LOCAL_TOTAL"
    printf 'local_wal_bytes=%s\n' "${LOKI_LOCAL_WAL:-null}"
    printf 'local_index_bytes_shipping_state_unknown=%s\n' "${LOKI_LOCAL_INDEX:-null}"
    printf 'local_index_cache_bytes=%s\n' "${LOKI_LOCAL_INDEX_CACHE:-null}"
    printf 'local_compactor_bytes=%s\n' "${LOKI_LOCAL_COMPACTOR:-null}"
    printf 'local_workdir_source=one-shot tool image over read-only loki volume\n'
    printf 'index_ship_barrier=not_established; du measures all local active/retained index bytes and does not prove shipped state; a post-query object-store snapshot follows\n'
    write_ratio "$bytes" "${raw:-0}" object_store_bytes_to_raw_line_bytes
  } >"$OUTPUT_DIR/storage-loki.txt"
  log "Loki storage: object store=${bytes}B over $objects object(s), local index=${LOKI_LOCAL_INDEX:-unknown}B (shipping state unknown)"
}

measure_storage_loki_post_query() {
  local info bytes objects pre_bytes delta
  info=$(garage_bucket_info "$PARKANA_BENCH_LOKI_BUCKET") ||
    fail "cannot read Loki's post-query Garage bucket info"
  bytes=$(garage_json_uint "$info" bytes)
  objects=$(garage_json_uint "$info" objects)
  [[ -n $bytes ]] || fail "post-query Garage bucket info has no exact 'bytes' field"
  pre_bytes=$(awk -F= '$1=="object_store_bytes"{print $2}' "$OUTPUT_DIR/storage-loki.txt" 2>/dev/null | head -n 1)
  delta="null"
  [[ -n $pre_bytes ]] && delta=$((bytes - pre_bytes))
  loki_local_snapshot ||
    fail "Loki post-query local workdir could not be measured via the one-shot tool volume"
  {
    printf 'backend=loki\n'
    printf 'snapshot_phase=post_query\n'
    printf 'object_store_bytes=%s\n' "$bytes"
    printf 'objects=%s\n' "$objects"
    printf 'object_store_bytes_delta_from_pre_query=%s\n' "$delta"
    printf 'local_workdir_total_bytes=%s\n' "$LOKI_LOCAL_TOTAL"
    printf 'local_wal_bytes=%s\n' "${LOKI_LOCAL_WAL:-null}"
    printf 'local_index_bytes_remaining=%s\n' "${LOKI_LOCAL_INDEX:-null}"
    printf 'local_index_cache_bytes=%s\n' "${LOKI_LOCAL_INDEX_CACHE:-null}"
    printf 'local_compactor_bytes=%s\n' "${LOKI_LOCAL_COMPACTOR:-null}"
    printf 'local_workdir_source=one-shot tool image over read-only loki volume\n'
  } >"$OUTPUT_DIR/storage-loki-post-query.txt"
  log "Loki post-query storage: object store=${bytes}B (+${delta}/-), local index=${LOKI_LOCAL_INDEX:-unknown}B"
}

capture_loki_compactor() { # output-file
  local metrics
  metrics=$(http_get "$LOKI_URL/metrics" || true)
  if [[ -z $metrics ]]; then
    printf 'unavailable\n' >"$1"
    return 0
  fi
  printf '%s\n' "$metrics" | grep '^loki_compactor_' >"$1" || printf 'no loki_compactor_* series\n' >"$1"
}

# Boat VMs are destroyed after the run, so the console transcript is the only
# durable copy.  Emit a bounded, gzip+base64 tar of the small evidence files
# between explicit delimiters.  The archive is curated: secrets.env, the
# parkbench binary and the full run.log are never included.  A redacted tail of
# run.log is included instead.
emit_archive() {
  local max=${PARKANA_BENCH_ARCHIVE_MAX_BYTES:-8388608}
  local tmp size f
  local -a core=(
    run-metadata.txt timings.tsv summary.md
    storage-parkana.txt storage-loki.txt
    ingest-parkana.json ingest-loki.json skipped.txt run-tail.txt
    index-build-parkana.txt runtime-parkana.txt
    config/bench.yaml config/ingest-runtime.yaml config/loki-bench-config.yaml
  )
  local -a extras=(
    query-parkana.md query-loki.md
    query-parkana.json query-loki.json
    query-parkana.txt query-loki.txt
    storage-loki-post-query.txt
    loki-compactor-before-query.txt loki-compactor-after-query.txt
    diagnostics.log
  )
  tmp=$(mktemp -d) || { warn "cannot create archive temp dir"; return 1; }
  if [[ -n $RUN_LOG && -f $RUN_LOG ]]; then
    tail -n 400 "$RUN_LOG" >"$OUTPUT_DIR/run-tail.txt" 2>/dev/null || true
    redact_file "$OUTPUT_DIR/run-tail.txt"
  fi

  local -a files=()
  for f in "${core[@]}" "${extras[@]}"; do
    [[ -f $OUTPUT_DIR/$f ]] && files+=("$f")
  done
  if ((${#files[@]} == 0)); then
    warn "no artifact files to export"
    rm -rf "$tmp"
    return 1
  fi

  tar -czf "$tmp/archive.tgz" -C "$OUTPUT_DIR" "${files[@]}" 2>/dev/null ||
    { warn "cannot tar artifacts"; rm -rf "$tmp"; return 1; }
  size=$(wc -c <"$tmp/archive.tgz")
  if ((size > max)); then
    warn "artifact archive is $size bytes; retrying without query JSON"
    files=()
    for f in "${core[@]}"; do
      [[ -f $OUTPUT_DIR/$f ]] && files+=("$f")
    done
    tar -czf "$tmp/archive.tgz" -C "$OUTPUT_DIR" "${files[@]}" 2>/dev/null || true
    size=$(wc -c <"$tmp/archive.tgz")
  fi
  if ((size > max)); then
    warn "artifact archive still $size bytes; exporting only the summary and metadata"
    files=()
    for f in run-metadata.txt timings.tsv summary.md storage-parkana.txt storage-loki.txt; do
      [[ -f $OUTPUT_DIR/$f ]] && files+=("$f")
    done
    tar -czf "$tmp/archive.tgz" -C "$OUTPUT_DIR" "${files[@]}" 2>/dev/null || true
    size=$(wc -c <"$tmp/archive.tgz")
  fi
  printf 'archive_files=%s archive_bytes=%s soft_limit=%s\n' "${#files[@]}" "$size" "$max"
  printf '=== BENCH ARTIFACT ARCHIVE BEGIN (gzip tar, base64, excludes secrets.env) ===\n'
  base64 "$tmp/archive.tgz"
  printf '=== BENCH ARTIFACT ARCHIVE END ===\n'
  rm -rf "$tmp"
}

# --- metadata and summary ---------------------------------------------------

cpu_model() {
  awk -F: '/model name/ { sub(/^[ \t]+/, "", $2); print $2; exit }' /proc/cpuinfo 2>/dev/null || printf 'unavailable'
}

# A Boat test archive may not contain .git.  The lead can pass the reviewed
# commit/dirty state explicitly; otherwise use git when available, and record
# `unavailable`/`unknown` rather than inventing a clean tree.
source_commit_value() {
  if [[ -n $SOURCE_COMMIT ]]; then
    printf '%s' "$SOURCE_COMMIT"
    return 0
  fi
  local rev
  if rev=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null) && [[ -n $rev ]]; then
    printf '%s' "$rev"
  else
    printf 'unavailable'
  fi
}

source_dirty_value() {
  if [[ -n $SOURCE_DIRTY ]]; then
    printf '%s' "$SOURCE_DIRTY"
    return 0
  fi
  local status
  if status=$(git -C "$REPO_ROOT" status --porcelain 2>/dev/null); then
    printf '%s' "$(printf '%s\n' "$status" | grep -c . || true)"
  else
    printf 'unknown'
  fi
}

instances_requested_value() {
  if [[ -n $INSTANCES_PER_SERVICE ]]; then
    printf '%s' "$INSTANCES_PER_SERVICE"
  else
    printf 'tool-default'
  fi
}

streams_requested_value() {
  if ((STREAMS_EXPLICIT)); then
    printf '%s' "$STREAMS"
  else
    printf 'tool-default'
  fi
}

# start_source_value distinguishes an explicit pin, a reused prior manifest and
# the tool's derived default so a silently moving hour partition is visible.
start_source_value() {
  if [[ -n ${REUSE_MANIFEST:-} ]]; then
    printf 'reused-manifest'
  elif [[ -n ${START:-} ]]; then
    printf 'explicit'
  else
    printf 'derived-by-tool'
  fi
}

reuse_manifest_sha256() {
  if [[ -n ${REUSE_MANIFEST:-} ]]; then
    sha256sum "$REUSE_MANIFEST" | awk '{print $1}'
  else
    printf 'not-used'
  fi
}

runtime_flag_source() { # explicit-flag
  if ((${LEGACY_RUNTIME:-0})); then
    printf 'legacy-known'
  elif (($1)); then
    printf 'explicit'
  else
    printf 'default'
  fi
}

write_metadata() {
  local file="$OUTPUT_DIR/run-metadata.txt"
  mkdir -p "$OUTPUT_DIR/config"
  cp -- "$COMPOSE_FILE" "$OUTPUT_DIR/config/bench.yaml" 2>/dev/null || warn "cannot copy benchmark Compose config"
  cp -- "$RUNTIME_OVERLAY" "$OUTPUT_DIR/config/ingest-runtime.yaml" 2>/dev/null || warn "cannot copy ingest runtime overlay"
  cp -- "$LOKI_CONFIG" "$OUTPUT_DIR/config/loki-bench-config.yaml" 2>/dev/null || warn "cannot copy benchmark Loki config"
  {
    printf 'tool=scripts/bench-phase3.sh\n'
    printf 'project=%s\n' "$PROJECT"
    printf 'preset=%s\n' "$PRESET"
    printf 'target_raw_line_bytes=%s\n' "$TARGET_BYTES"
    printf 'seed=%s\n' "${SEED:-tool-default}"
    printf 'requested_start=%s\n' "${START:-derived-by-tool}"
    printf 'start_source=%s\n' "$(start_source_value)"
    printf 'reuse_manifest=%s\n' "${REUSE_MANIFEST:-none}"
    printf 'reuse_manifest_sha256=%s\n' "$(reuse_manifest_sha256)"
    printf 'step=%s\n' "$STEP"
    printf 'batch_lines=%s\n' "$BATCH_LINES"
    printf 'batch_lines_unit=entries per push request\n'
    printf 'batch_lines_scope=per label set\n'
    printf 'concurrency=%s\n' "$CONCURRENCY"
    printf 'instances_per_service_requested=%s\n' "$(instances_requested_value)"
    printf 'instances_per_service_source=%s\n' "$([[ $INSTANCES_PER_SERVICE_EXPLICIT == 1 ]] && printf explicit || printf tool-default)"
    printf 'streams_requested=%s\n' "$(streams_requested_value)"
    printf 'streams_source=%s\n' "$([[ $STREAMS_EXPLICIT == 1 ]] && printf explicit || printf tool-default)"
    printf 'resources_cpus_per_backend=%s\n' "$CPUS"
    printf 'resources_memory_per_backend=%s\n' "$MEMORY"
    printf 'parkana_only=%s\n' "$PARKANA_ONLY"
    printf 'parkana_url=%s\n' "$PARKANA_URL"
    printf 'garage_s3=127.0.0.1:%s\n' "$GARAGE_PORT"
    printf 'parkana_roles=ingester,querier (no compactor during measurement)\n'
    if ((PARKANA_ONLY)); then
      printf 'loki_backend=skipped (--parkana-only)\n'
      printf 'competing_load=none by design: Garage plus Parkana; no Loki backend starts\n'
    else
      printf 'loki_url=%s\n' "$LOKI_URL"
      printf 'competing_load=none by design: Garage plus exactly one backend at a time; other backend stopped\n'
    fi
    printf 'index_ngram=4\n'
    printf 'index_fp_rate=0.01\n'
    printf 'index_max_grams=%s\n' "$INDEX_MAX_GRAMS"
    printf 'index_max_grams_source=%s\n' "$([[ $INDEX_MAX_GRAMS_EXPLICIT == 1 ]] && printf explicit || printf default)"
    printf 'parkana_runtime_mode=%s\n' "$RUNTIME_MODE"
    printf 'parkana_runtime_support=%s\n' "$([[ $RUNTIME_MODE == legacy-foundation ]] && printf absent || printf present)"
    printf 'parkana_runtime_source=%s\n' "$([[ $RUNTIME_MODE == legacy-foundation ]] && printf legacy-source-contract || printf container-inspect)"
    printf 'parkana_runtime_wal_sync=%s\n' "$WAL_SYNC"
    printf 'parkana_runtime_wal_sync_source=%s\n' "$(runtime_flag_source "$WAL_SYNC_EXPLICIT")"
    printf 'parkana_runtime_wal_sync_interval=%s\n' "$WAL_SYNC_INTERVAL"
    printf 'parkana_runtime_wal_sync_interval_source=%s\n' "$(runtime_flag_source "$WAL_SYNC_INTERVAL_EXPLICIT")"
    printf 'parkana_runtime_flush_concurrency=%s\n' "$FLUSH_CONCURRENCY"
    printf 'parkana_runtime_flush_concurrency_source=%s\n' "$(runtime_flag_source "$FLUSH_CONCURRENCY_EXPLICIT")"
    printf 'parkana_runtime_upload_concurrency=%s\n' "$UPLOAD_CONCURRENCY"
    printf 'parkana_runtime_upload_concurrency_source=%s\n' "$(runtime_flag_source "$UPLOAD_CONCURRENCY_EXPLICIT")"
    if ((LEGACY_RUNTIME)); then
      printf 'parkana_legacy_known_wal_sync=%s\n' "$LEGACY_KNOWN_WAL_SYNC"
      printf 'parkana_legacy_known_flush_concurrency=%s\n' "$LEGACY_KNOWN_FLUSH_CONCURRENCY"
      printf 'parkana_legacy_known_upload_concurrency=%s\n' "$LEGACY_KNOWN_UPLOAD_CONCURRENCY"
    fi
    ((PARKANA_ONLY)) || printf 'loki_compactor_interval=24h retention=false\n'
    printf 'compose_file=%s\n' "$COMPOSE_FILE"
    printf 'runtime_overlay=%s\n' "$RUNTIME_OVERLAY"
    printf 'runtime_overlay_attached=%s\n' "$([[ $RUNTIME_MODE == legacy-foundation ]] && printf 0 || printf 1)"
    printf 'loki_config=%s\n' "$LOKI_CONFIG"
    printf 'compose_sha256=%s\n' "$(sha256sum "$COMPOSE_FILE" | awk '{print $1}')"
    printf 'runtime_overlay_sha256=%s\n' "$(sha256sum "$RUNTIME_OVERLAY" | awk '{print $1}')"
    printf 'loki_config_sha256=%s\n' "$(sha256sum "$LOKI_CONFIG" | awk '{print $1}')"
    printf 'script_sha256=%s\n' "$(sha256sum "$SCRIPT_PATH" | awk '{print $1}')"
    printf 'driver_script=%s\n' "scripts/bench-phase3.sh"
    printf 'driver_script_sha256=%s\n' "$(sha256sum "$SCRIPT_PATH" | awk '{print $1}')"
    printf 'app_source_commit=%s\n' "$(source_commit_value)"
    printf 'app_source_dirty=%s\n' "$(source_dirty_value)"
    printf 'parkbench_path=%s\n' "$PARKBENCH"
    printf 'parkbench_sha256=%s\n' "$(sha256sum "$PARKBENCH" 2>/dev/null | awk '{print $1}' || printf unavailable)"
    printf 'source_commit=%s\n' "$(source_commit_value)"
    printf 'source_commit_source=%s\n' "$([[ -n $SOURCE_COMMIT ]] && printf explicit || printf git-or-unavailable)"
    printf 'source_dirty=%s\n' "$(source_dirty_value)"
    printf 'source_dirty_source=%s\n' "$([[ -n $SOURCE_DIRTY ]] && printf explicit || printf git-or-unknown)"
    printf 'kernel=%s\n' "$(uname -a 2>/dev/null || printf unavailable)"
    printf 'cpu_model=%s\n' "$(cpu_model)"
    printf 'cpu_count=%s\n' "$(nproc 2>/dev/null || printf unavailable)"
    printf 'memory_bytes=%s\n' "$(awk '/MemTotal/ { printf "%.0f", $2 * 1024; exit }' /proc/meminfo 2>/dev/null || printf unavailable)"
    printf 'go_version=%s\n' "$(go version 2>/dev/null || printf unavailable)"
    printf 'docker_version=%s\n' "$("$DOCKER" version --format '{{.Server.Version}}' 2>/dev/null || printf unavailable)"
    printf 'compose_version=%s\n' "$("$DOCKER" compose version --short 2>/dev/null || printf unavailable)"
    printf 'garage_image=%s\n' "$GARAGE_IMAGE_PIN"
    printf 'loki_image=%s\n' "$LOKI_IMAGE_PIN"
    printf 'parkana_image=parkana-bench:phase3 (repo Dockerfile, golang:1.27.3-bookworm and debian:bookworm-slim pins)\n'
    printf 'parkana_s3_bucket=%s\n' "${PARKANA_BENCH_PARKANA_BUCKET:-unavailable}"
    if ((PARKANA_ONLY)); then
      printf 'loki_s3_bucket=skipped (--parkana-only)\n'
    else
      printf 'loki_s3_bucket=%s\n' "${PARKANA_BENCH_LOKI_BUCKET:-unavailable}"
    fi
    printf 'credentials=values generated per run and never recorded\n'
  } >"$file"
}

# Append one measured report block to run-metadata.txt.  Each exact field is
# either its measured JSON number (including a real zero) or explicitly
# unavailable, so a partially populated report never looks complete.
append_report_block() { # backend manifest section label fields...
  local backend=$1 manifest=$2 section=$3 label=$4
  shift 4
  local field value complete=1 file="$OUTPUT_DIR/run-metadata.txt"
  for field in "$@"; do
    value=$(manifest_object_field "$manifest" "$section" "$field") || value=""
    if [[ -n $value ]]; then
      printf 'ingest_%s_%s_%s=%s\n' "$backend" "$label" "$field" "$value" >>"$file"
    else
      printf 'ingest_%s_%s_%s=unavailable\n' "$backend" "$label" "$field" >>"$file"
      complete=0
    fi
  done
  if ((complete)); then
    printf 'ingest_%s_%s=complete\n' "$backend" "$label" >>"$file"
  else
    printf 'ingest_%s_%s=incomplete\n' "$backend" "$label" >>"$file"
  fi
}

# Append the measured batching, corpus-identity and parallelism evidence from
# one successful ingest report.  write_metadata runs before ingestion, so this
# must append and never truncate: every static hardware/config/identity line
# stays intact.  Only the backend that actually ran is recorded, and achieved
# concurrency is read from the report instead of derived from configuration.
# replay_command renders a self-contained identity replay: the actual start,
# seed, step, bytes and sender shape of this run. It never claims to reproduce
# timings or hardware.
replay_command() { # manifest
  local manifest=$1 start seed step streams instances
  start=$(manifest_string "$manifest" start)
  seed=$(manifest_uint "$manifest" seed)
  step=$(manifest_uint "$manifest" step_micros)
  printf 'bash scripts/bench-phase3.sh --start %s --seed %s --step %sus --bytes %s' \
    "${start:-<start>}" "${seed:-<seed>}" "${step:-<step>}" "$TARGET_BYTES"
  # Use the actual manifest streams/instances when present so a future change to
  # the tool defaults cannot silently move the replay corpus.
  streams=$(manifest_stream_list "$manifest")
  if [[ -z $streams && ${STREAMS_EXPLICIT:-0} -eq 1 ]]; then
    streams=$STREAMS
  fi
  [[ -z $streams ]] || printf ' --streams %s' "$streams"
  instances=$(manifest_uint "$manifest" instances_per_service)
  if [[ -z $instances && ${INSTANCES_PER_SERVICE_EXPLICIT:-0} -eq 1 ]]; then
    instances=$INSTANCES_PER_SERVICE
  fi
  [[ -z $instances ]] || printf ' --instances-per-service %s' "$instances"
  printf ' --batch-lines %s --concurrency %s' "$BATCH_LINES" "$CONCURRENCY"
  if ((${LEGACY_RUNTIME:-0})); then
    printf ' --legacy-runtime'
  else
    printf ' --wal-sync %s' "$WAL_SYNC"
    if [[ $WAL_SYNC == interval ]]; then
      printf ' --wal-sync-interval %s' "$WAL_SYNC_INTERVAL"
    fi
    printf ' --flush-concurrency %s --upload-concurrency %s' \
      "$FLUSH_CONCURRENCY" "$UPLOAD_CONCURRENCY"
  fi
  printf ' --output-dir /tmp/parkana-bench-replay-UNIQUE'
}

# record_manifest_identity publishes every actual corpus identity field from the
# report, so a reused start or changed default is visible without trusting the
# requested flags.
record_manifest_identity() { # backend manifest file
  local backend=$1 manifest=$2 file=$3 field value
  for field in hash content_hash start through; do
    value=$(manifest_string "$manifest" "$field")
    printf 'ingest_%s_%s=%s\n' "$backend" "$field" "${value:-unavailable}" >>"$file"
  done
  for field in seed step_micros target_raw_line_bytes raw_line_bytes lines ack_lines ack_raw_line_bytes; do
    value=$(manifest_uint "$manifest" "$field")
    printf 'ingest_%s_%s=%s\n' "$backend" "$field" "${value:-unavailable}" >>"$file"
  done
}

# record_actual_interval makes the actual hour partition and replay command
# prominent in both metadata and the run log. It is written once from the first
# manifest; per-backend copies are recorded for every backend.
record_actual_interval() { # backend manifest
  local backend=$1 manifest=$2 file="$OUTPUT_DIR/run-metadata.txt"
  local start through
  start=$(manifest_string "$manifest" start)
  through=$(manifest_string "$manifest" through)
  [[ -n $start && -n $through ]] || {
    warn "$backend manifest has no corpus start/through; actual interval not recorded"
    return 0
  }
  ACTUAL_START=$start
  ACTUAL_THROUGH=$through
  REPLAY_COMMAND=$(replay_command "$manifest")
  {
    printf 'ingest_%s_actual_start=%s\n' "$backend" "$start"
    printf 'ingest_%s_actual_through=%s\n' "$backend" "$through"
  } >>"$file"
  if ! grep -q '^actual_start=' "$file"; then
    {
      printf 'actual_start=%s\n' "$start"
      printf 'actual_through=%s\n' "$through"
      printf 'start_source=%s\n' "$(start_source_value)"
      printf 'reuse_manifest=%s\n' "${REUSE_MANIFEST:-none}"
      printf 'reuse_manifest_sha256=%s\n' "$(reuse_manifest_sha256)"
      printf 'replay_command=%s\n' "$REPLAY_COMMAND"
      printf 'replay_note=replace UNIQUE with a fresh output name before running\n'
    } >>"$file"
    log "corpus actual start=$start through=$through (start_source=$(start_source_value))"
    log "replay: $REPLAY_COMMAND"
    log "replay note: replace UNIQUE with a fresh output name before running"
  fi
}

record_ingest_evidence() { # backend manifest
  local backend=$1 manifest=$2 file="$OUTPUT_DIR/run-metadata.txt"
  [[ -f $manifest ]] || {
    warn "no $backend ingest report at $manifest; measured evidence not recorded"
    return 0
  }
  [[ -f $file ]] || {
    warn "no run metadata at $file; $backend measured evidence not recorded"
    return 0
  }
  append_report_block "$backend" "$manifest" "$BATCHING_SECTION" batching "${BATCHING_FIELDS[@]}"
  append_report_block "$backend" "$manifest" "$CORPUS_SECTION" corpus "${CORPUS_FIELDS[@]}"
  # Actual streams come from the manifest, never from the requested --streams
  # value, so a one-stream request cannot masquerade as a four-stream result.
  local streams stream_count
  streams=$(manifest_stream_list "$manifest")
  if [[ -n $streams ]]; then
    stream_count=$(awk -F, '{ print NF }' <<<"$streams")
    printf 'ingest_%s_corpus_streams=%s\n' "$backend" "$streams" >>"$file"
    printf 'ingest_%s_corpus_stream_count=%s\n' "$backend" "$stream_count" >>"$file"
  else
    printf 'ingest_%s_corpus_streams=unavailable\n' "$backend" >>"$file"
    printf 'ingest_%s_corpus_stream_count=unavailable\n' "$backend" >>"$file"
  fi
  record_manifest_identity "$backend" "$manifest" "$file"
  record_actual_interval "$backend" "$manifest"
  append_report_block "$backend" "$manifest" "$PARALLELISM_SECTION" parallelism "${PARALLELISM_FIELDS[@]}"
}

write_timing() {
  {
    printf 'acknowledgment is parkbench ingest report elapsed_seconds; wall is first setup to last 204.\n'
    printf 'queryable is the first exact count_over_time == ack_lines; polling adds up to 2s.\n'
    printf 'flush is POST /flush to a drained queue plus an advancing, stable chunks_flushed_total with no flush failures.\n'
    if ((LEGACY_RUNTIME)); then
      printf '# parkana_runtime legacy source contract known wal_sync=%s interval=not-supported flush_workers=%s upload_workers=%s (no runtime flags)\n' \
        "$LEGACY_KNOWN_WAL_SYNC" "$LEGACY_KNOWN_FLUSH_CONCURRENCY" "$LEGACY_KNOWN_UPLOAD_CONCURRENCY"
    else
      printf '# parkana_runtime requested wal_sync=%s interval=%s flush_workers=%s upload_workers=%s\n' \
        "$WAL_SYNC" "$WAL_SYNC_INTERVAL" "$FLUSH_CONCURRENCY" "$UPLOAD_CONCURRENCY"
      printf '# parkana_runtime effective wal_sync=%s interval=%s flush_workers=%s upload_workers=%s\n' \
        "${EFFECTIVE_WAL_SYNC:-unavailable}" "${EFFECTIVE_WAL_SYNC_INTERVAL:-unavailable}" \
        "${EFFECTIVE_FLUSH_CONCURRENCY:-unavailable}" "${EFFECTIVE_UPLOAD_CONCURRENCY:-unavailable}"
    fi
    printf 'backend\tack_report_s\tack_wall_s\tflush_s\tqueryable_delay_s\n'
    printf 'parkana\t%s\t%s\tnull\t%s\n' "${PARKANA_ACK_REPORT_SECONDS:-null}" "${PARKANA_ACK_WALL_SECONDS:-null}" "${PARKANA_QUERYABLE_DELAY:-null}"
    if ((PARKANA_ONLY)); then
      # A skipped backend is named explicitly, never emitted as a successful
      # row of zero/null measurements.
      printf '# loki skipped: --parkana-only (no Loki backend ran; no measurement is reported)\n'
    else
      printf 'loki\t%s\t%s\t%s\t%s\n' "${LOKI_ACK_REPORT_SECONDS:-null}" "${LOKI_ACK_WALL_SECONDS:-null}" "${LOKI_FLUSH_SECONDS:-null}" "${LOKI_QUERYABLE_DELAY:-null}"
    fi
  } >"$OUTPUT_DIR/timings.tsv"
}

write_skipped_report() {
  ((PARKANA_ONLY)) || return 0
  {
    printf 'loki_backend=skipped\n'
    printf 'reason=--parkana-only\n'
    printf 'loki_garage_init=skipped\n'
    printf 'loki_ingest=skipped\n'
    printf 'loki_flush_barrier=skipped\n'
    printf 'loki_visibility=skipped\n'
    printf 'loki_storage_pre_query=skipped\n'
    printf 'loki_storage_post_query=skipped\n'
    printf 'loki_query=skipped\n'
    printf 'loki_compactor_metrics=skipped\n'
    printf 'manifest_comparison=skipped (only one backend ran)\n'
  } >"$OUTPUT_DIR/skipped.txt"
}

write_summary() {
  local file="$OUTPUT_DIR/summary.md"
  {
    printf '# Parkana phase 3 benchmark run\n\n'
    printf -- '- project: `%s`\n' "$PROJECT"
    printf -- '- preset: `%s` (%s decimal raw line bytes)\n' "$PRESET" "$TARGET_BYTES"
    printf -- '- output: `%s`\n' "$OUTPUT_DIR"
    printf -- '- commit: `%s` (dirty files: %s)\n' "$(source_commit_value)" "$(source_dirty_value)"
    printf -- '- app source: commit `%s` (dirty files: %s); driver: `scripts/bench-phase3.sh` sha256 `%s`\n' \
      "$(source_commit_value)" "$(source_dirty_value)" "$(sha256sum "$SCRIPT_PATH" | awk '{print $1}')"
    printf -- '- actual corpus interval: start=%s through=%s (start source=%s)\n' \
      "${ACTUAL_START:-unavailable}" "${ACTUAL_THROUGH:-unavailable}" "$(start_source_value)"
    printf -- '- replay: `%s` (replace UNIQUE with a fresh output name before running)\n' "${REPLAY_COMMAND:-unavailable}"
    printf -- '- resources: %s CPU / %s memory per backend container\n' "$CPUS" "$MEMORY"
    printf -- '- batch: %s entries per push request per label set (concurrency %s)\n' "$BATCH_LINES" "$CONCURRENCY"
    printf -- '- instances per service: requested %s (source=%s)\n' "$(instances_requested_value)" "$([[ $INSTANCES_PER_SERVICE_EXPLICIT == 1 ]] && printf explicit || printf tool-default)"
    printf -- '- streams: requested %s (source=%s); actual names/count are recorded per backend as ingest_*_corpus_streams\n' "$(streams_requested_value)" "$([[ $STREAMS_EXPLICIT == 1 ]] && printf explicit || printf tool-default)"
    printf -- '- index max grams: %s (%s)\n' "$INDEX_MAX_GRAMS" "$([[ $INDEX_MAX_GRAMS_EXPLICIT == 1 ]] && printf explicit || printf default)"
    if ((LEGACY_RUNTIME)); then
      printf -- '- ingest runtime: legacy source contract, known always/F1/U1; no phase-4 runtime flags (not container-effective)\n'
    else
      printf -- '- ingest runtime requested: wal_sync=%s (%s) interval=%s (%s) flush_workers=%s (%s) upload_workers=%s (%s)\n' \
        "$WAL_SYNC" "$(runtime_flag_source "$WAL_SYNC_EXPLICIT")" \
        "$WAL_SYNC_INTERVAL" "$(runtime_flag_source "$WAL_SYNC_INTERVAL_EXPLICIT")" \
        "$FLUSH_CONCURRENCY" "$(runtime_flag_source "$FLUSH_CONCURRENCY_EXPLICIT")" \
        "$UPLOAD_CONCURRENCY" "$(runtime_flag_source "$UPLOAD_CONCURRENCY_EXPLICIT")"
      printf -- '- ingest runtime effective (container inspect): wal_sync=%s interval=%s flush_workers=%s upload_workers=%s cpus=%s memory_bytes=%s\n' \
        "${EFFECTIVE_WAL_SYNC:-unavailable}" "${EFFECTIVE_WAL_SYNC_INTERVAL:-unavailable}" \
        "${EFFECTIVE_FLUSH_CONCURRENCY:-unavailable}" "${EFFECTIVE_UPLOAD_CONCURRENCY:-unavailable}" \
        "${EFFECTIVE_CPUS:-unavailable}" "${EFFECTIVE_MEMORY_BYTES:-unavailable}"
    fi
    printf -- '- runtime flag probe: support=%s source=%s sha256=%s\n' \
      "${RUNTIME_PROBE_SUPPORT:-unavailable}" "${RUNTIME_PROBE_SOURCE:-unavailable}" "${RUNTIME_PROBE_SHA256:-unavailable}"
    printf -- '- ingest memory peak (cgroup-v2 high-water through ingest, read before stop; not the limit): %s bytes (source=%s)\n' \
      "${EFFECTIVE_INGEST_MEMORY_PEAK_BYTES:-unavailable}" "${EFFECTIVE_INGEST_MEMORY_PEAK_SOURCE:-unavailable}"
    [[ -z ${EFFECTIVE_INGEST_MEMORY_PEAK_REASON:-} ]] ||
      printf -- '- ingest memory peak note: %s\n' "$EFFECTIVE_INGEST_MEMORY_PEAK_REASON"
    printf -- '- ingest oom_kill (cgroup-v2 memory.events, read before stop): %s (source=%s)\n' \
      "${EFFECTIVE_INGEST_OOM_KILL:-unavailable}" "${EFFECTIVE_INGEST_OOM_KILL_SOURCE:-unavailable}"
    [[ -z ${EFFECTIVE_INGEST_OOM_KILL_REASON:-} ]] ||
      printf -- '- ingest oom_kill note: %s\n' "$EFFECTIVE_INGEST_OOM_KILL_REASON"
    if ((PARKANA_ONLY)); then
      printf -- '- backends: parkana only (`--parkana-only`; Loki explicitly skipped)\n'
    else
      printf -- '- backends: parkana then loki (sequential, isolated)\n'
    fi
    printf -- '- query stage: '
    if ((SKIP_QUERIES)); then printf 'skipped (`--skip-queries`)\n'; else printf 'run (B2b contract)\n'; fi
    printf '\n## Ingest\n\n'
    printf '| backend | ack report s | ack wall s | flush s | queryable delay s |\n'
    printf '|---|---:|---:|---:|---:|\n'
    printf '| parkana | %s | %s | — | %s |\n' "${PARKANA_ACK_REPORT_SECONDS:-null}" "${PARKANA_ACK_WALL_SECONDS:-null}" "${PARKANA_QUERYABLE_DELAY:-null}"
    if ((PARKANA_ONLY)); then
      printf '\nLoki was skipped entirely (`--parkana-only`); no Loki timing row is reported.\n'
    else
      printf '| loki | %s | %s | %s | %s |\n' "${LOKI_ACK_REPORT_SECONDS:-null}" "${LOKI_ACK_WALL_SECONDS:-null}" "${LOKI_FLUSH_SECONDS:-null}" "${LOKI_QUERYABLE_DELAY:-null}"
    fi
    printf '\n### Container CPU (cgroup-v2 usage_usec, ingest command only)\n\n'
    printf '| backend | before | after | delta | wall s | source |\n'
    printf '|---|---:|---:|---:|---:|---|\n'
    printf '| parkana | %s | %s | %s | %s | %s |\n' \
      "$(get_cpu_field parkana BEFORE_USEC)" "$(get_cpu_field parkana AFTER_USEC)" \
      "$(get_cpu_field parkana DELTA_USEC)" "$(get_cpu_field parkana WALL_S)" \
      "$(get_cpu_field parkana SOURCE)"
    if ((PARKANA_ONLY == 0)); then
      printf '| loki | %s | %s | %s | %s | %s |\n' \
        "$(get_cpu_field loki BEFORE_USEC)" "$(get_cpu_field loki AFTER_USEC)" \
        "$(get_cpu_field loki DELTA_USEC)" "$(get_cpu_field loki WALL_S)" \
        "$(get_cpu_field loki SOURCE)"
    fi
    printf '\nContainer CPU is the cgroup-v2 usage_usec delta across the parkbench ingest command, not component attribution. Unavailable counters are never reported as zero.\n'
    printf '\n## Storage\n\n'
    if [[ -f $OUTPUT_DIR/storage-parkana.txt ]]; then
      printf '```\n'; sed 's/^/parkana: /' "$OUTPUT_DIR/storage-parkana.txt"; printf '```\n\n'
    fi
    if [[ -f $OUTPUT_DIR/storage-loki.txt ]]; then
      printf '```\n'; sed 's/^/loki: /' "$OUTPUT_DIR/storage-loki.txt"; printf '```\n\n'
    fi
    printf '## Artifacts\n\n'
    printf -- '- `run-metadata.txt` hardware, versions, commit, config and image pins\n'
    printf -- '- `timings.tsv` acknowledgment/queryable/flush separation\n'
    if ((PARKANA_ONLY)); then
      printf -- '- `ingest-parkana.json` parkbench manifest (`ingest-loki.json` absent: --parkana-only)\n'
      printf -- '- `storage-parkana.txt` data and sidecar bytes\n'
      printf -- '- `skipped.txt` explicit list of skipped Loki stages\n'
    else
      printf -- '- `ingest-parkana.json`, `ingest-loki.json` parkbench manifests\n'
      printf -- '- `storage-parkana.txt`, `storage-loki.txt` data and sidecar bytes\n'
      printf -- '- `storage-loki-post-query.txt` post-query object-store and remaining local index bytes\n'
    fi
    printf -- '- `index-build-parkana.txt` sidecar build histogram buckets for source=ingest\n'
    printf -- '- `runtime-parkana.txt` measured container runtime settings (always/interval, workers, resources)\n'
    printf -- '- `config/bench.yaml`, `config/ingest-runtime.yaml`, `config/loki-bench-config.yaml` exact non-secret config\n'
    if ((SKIP_QUERIES == 0)); then
      if ((PARKANA_ONLY)); then
        printf -- '- `query-parkana.json/.md` query results (`query-loki.*` absent: --parkana-only)\n'
      else
        printf -- '- `query-parkana.json/.md`, `query-loki.json/.md` query results\n'
        printf -- '- `loki-compactor-before-query.txt`, `loki-compactor-after-query.txt` idle-compactor evidence\n'
      fi
    fi
    printf -- '- `run.log` full output; `diagnostics.log` only after a failure\n'
    printf '\nByte definitions and every configuration delta are in docs/benchmarks.md.\n'
    printf 'Secrets are never written to these artifacts; `secrets.env` is 0600 and removed on cleanup unless --keep.\n'
  } >"$file"
  printf '\n'
  cat "$file"
  printf '\n'
  log "artifacts: $OUTPUT_DIR"
}

# --- main flow --------------------------------------------------------------

run_benchmark() {
  local manifest_parkana manifest_loki start
  manifest_parkana="$OUTPUT_DIR/ingest-parkana.json"
  manifest_loki="$OUTPUT_DIR/ingest-loki.json"

  write_metadata
  start_garage_stack

  log "=== Parkana ingest phase ==="
  start_backend parkana
  capture_parkana_runtime
  probe_parkana_runtime_flags
  append_runtime_evidence
  log "Parkana effective runtime: wal_sync=$EFFECTIVE_WAL_SYNC interval=$EFFECTIVE_WAL_SYNC_INTERVAL flush_workers=$EFFECTIVE_FLUSH_CONCURRENCY upload_workers=$EFFECTIVE_UPLOAD_CONCURRENCY cpus=$EFFECTIVE_CPUS memory_bytes=$EFFECTIVE_MEMORY_BYTES"
  run_ingest_backend parkana "$PARKANA_URL/loki/api/v1/push" "$manifest_parkana" "$START"
  PARKANA_ACK_AT=$ACK_AT
  PARKANA_ACK_REPORT_SECONDS=$ACK_REPORT_SECONDS
  PARKANA_ACK_WALL_SECONDS=$ACK_WALL_SECONDS
  wait_parkana_visible "$(manifest_uint "$manifest_parkana" ack_lines)"
  PARKANA_QUERYABLE_AT=$QUERYABLE_AT
  PARKANA_QUERYABLE_DELAY=$(elapsed_seconds "$PARKANA_ACK_AT" "$PARKANA_QUERYABLE_AT")
  measure_storage_parkana
  # Read the cgroup high-water mark once, after ingest/visibility/storage and
  # before the container stops; this is the ingest-stage peak, not the limit.
  capture_parkana_memory_peak
  capture_parkana_memory_events
  stop_backend parkana

  if ((PARKANA_ONLY)); then
    write_skipped_report
    write_timing
    log "=== Parkana query phase ==="
    start_backend parkana
    verify_count_after_restart parkana "$PARKANA_URL" "$manifest_parkana"
    wait_metric_zero Parkana "$PARKANA_URL/metrics" parkana_ingest_pending_generations
    run_query_backend parkana "$PARKANA_URL" "$manifest_parkana"
    stop_backend parkana
    write_summary
    RUN_SUCCEEDED=1
    return 0
  fi

  start=$(manifest_string "$manifest_parkana" start)
  [[ -n $start ]] || fail "the first ingest manifest has no corpus.start"
  log "=== Loki ingest phase (reusing start=$start) ==="
  start_backend loki
  run_ingest_backend loki "$LOKI_URL/loki/api/v1/push" "$manifest_loki" "$start"
  LOKI_ACK_AT=$ACK_AT
  LOKI_ACK_REPORT_SECONDS=$ACK_REPORT_SECONDS
  LOKI_ACK_WALL_SECONDS=$ACK_WALL_SECONDS
  loki_flush_wait
  wait_loki_visible "$(manifest_uint "$manifest_loki" ack_lines)"
  LOKI_QUERYABLE_AT=$QUERYABLE_AT
  LOKI_QUERYABLE_DELAY=$(elapsed_seconds "$LOKI_ACK_AT" "$LOKI_QUERYABLE_AT")
  measure_storage_loki
  stop_backend loki

  compare_manifests "$manifest_parkana" "$manifest_loki"
  write_timing

  log "=== Parkana query phase ==="
  start_backend parkana
  verify_count_after_restart parkana "$PARKANA_URL" "$manifest_parkana"
  wait_metric_zero Parkana "$PARKANA_URL/metrics" parkana_ingest_pending_generations
  run_query_backend parkana "$PARKANA_URL" "$manifest_parkana"
  stop_backend parkana

  log "=== Loki query phase ==="
  start_backend loki
  verify_count_after_restart loki "$LOKI_URL" "$manifest_loki"
  wait_metric_zero Loki "$LOKI_URL/metrics" loki_ingester_flush_queue_length
  capture_loki_compactor "$OUTPUT_DIR/loki-compactor-before-query.txt"
  run_query_backend loki "$LOKI_URL" "$manifest_loki"
  capture_loki_compactor "$OUTPUT_DIR/loki-compactor-after-query.txt"
  measure_storage_loki_post_query
  stop_backend loki

  write_summary
  RUN_SUCCEEDED=1
}

dry_run() {
  local project_note="(fresh check deferred: dry run does not contact Docker)"
  local index_source=default
  ((PROJECT_EXPLICIT)) && project_note="(explicit; fresh check deferred: dry run does not contact Docker)"
  ((INDEX_MAX_GRAMS_EXPLICIT)) && index_source=explicit
  printf '[bench-p3] dry run: no Docker, Go, credentials or services are touched\n'
  printf '  repo root       : %s\n' "$REPO_ROOT"
  printf '  compose file    : %s\n' "$COMPOSE_FILE"
  printf '  loki config     : %s\n' "$LOKI_CONFIG"
  printf '  project         : %s %s\n' "$PROJECT" "$project_note"
  printf '  preset          : %s (%s decimal raw line bytes)\n' "$PRESET" "$TARGET_BYTES"
  if ((PARKANA_ONLY)); then
    printf '  backends        : parkana only (Loki explicitly skipped)\n'
    printf '  host ports      : parkana=%s garage=%s loki=skipped (--parkana-only)\n' "$PARKANA_PORT" "$GARAGE_PORT"
  else
    printf '  backends        : parkana then loki (sequential, isolated)\n'
    printf '  host ports      : parkana=%s garage=%s loki=%s\n' "$PARKANA_PORT" "$GARAGE_PORT" "$LOKI_PORT"
  fi
  printf '  resources       : cpus=%s memory=%s per backend\n' "$CPUS" "$MEMORY"
  printf '  index           : ngram=4 fp=0.01 max_grams=%s (%s)\n' "$INDEX_MAX_GRAMS" "$index_source"
  printf '  ingest runtime  : wal_sync=%s (%s) interval=%s (%s) flush_workers=%s (%s) upload_workers=%s (%s)\n' \
    "$WAL_SYNC" "$(runtime_flag_source "$WAL_SYNC_EXPLICIT")" \
    "$WAL_SYNC_INTERVAL" "$(runtime_flag_source "$WAL_SYNC_INTERVAL_EXPLICIT")" \
    "$FLUSH_CONCURRENCY" "$(runtime_flag_source "$FLUSH_CONCURRENCY_EXPLICIT")" \
    "$UPLOAD_CONCURRENCY" "$(runtime_flag_source "$UPLOAD_CONCURRENCY_EXPLICIT")"
  printf '  runtime mode    : %s (support=%s, source=%s)\n' "$RUNTIME_MODE" \
    "$([[ $RUNTIME_MODE == legacy-foundation ]] && printf absent || printf present)" \
    "$([[ $RUNTIME_MODE == legacy-foundation ]] && printf legacy-source-contract || printf container-inspect)"
  if ((LEGACY_RUNTIME)); then
    printf '  runtime overlay : not attached (--legacy-runtime)\n'
  else
    printf '  runtime overlay : %s\n' "$RUNTIME_OVERLAY"
  fi
  printf '  output dir      : %s (must not exist)\n' "$OUTPUT_DIR"
  printf '  source          : commit=%s dirty=%s (explicit flags/env or git)\n' "$(source_commit_value)" "$(source_dirty_value)"
  printf '  corpus          : seed=%s start=%s step=%s batch=%s entries/request/label set concurrency=%s\n' \
    "${SEED:-tool-default}" "${START:-derived}" "$STEP" "$BATCH_LINES" "$CONCURRENCY"
  printf '  start source    : %s%s\n' "$(start_source_value)" \
    "$([[ -n $REUSE_MANIFEST ]] && printf ' (reuse only corpus.start from %s)' "$REUSE_MANIFEST")"
  printf '  instances       : requested=%s (source=%s)\n' "$(instances_requested_value)" \
    "$([[ $INSTANCES_PER_SERVICE_EXPLICIT == 1 ]] && printf explicit || printf tool-default)"
  printf '  streams         : requested=%s (source=%s)\n' "$(streams_requested_value)" \
    "$([[ $STREAMS_EXPLICIT == 1 ]] && printf explicit || printf tool-default)"
  printf '  query           : classes=%s windows=%s reps=%s warmups=%s timeout=%s\n' \
    "$CLASSES" "$WINDOWS" "$REPETITIONS" "$WARMUPS" "$QUERY_TIMEOUT"
  printf '  Garage creds    : generated per run (values never printed)\n'
  printf '  image pins      : Garage v2.5.0, Loki 3.7.9, repo Dockerfile base pins (unchanged)\n'
  if ((PARKANA_ONLY)); then
    printf '  ingest commands : parkbench ingest -url %s/loki/api/v1/push -bytes %s ... (Loki skipped)\n' "$PARKANA_URL" "$TARGET_BYTES"
    printf '  query commands  : parkbench query --backend parkana --url %s --manifest <manifest> ... (Loki skipped)\n' "$PARKANA_URL"
  else
    printf '  ingest commands : parkbench ingest -url <backend>/loki/api/v1/push -bytes %s ...\n' "$TARGET_BYTES"
    printf '  query commands  : parkbench query --backend <parkana|loki> --url <base> --manifest <manifest> ...\n'
  fi
  printf '  cleanup         : only this run'"'"'s fresh project; --keep leaves it for inspection\n'
  printf '[bench-p3] dry run: no changes made\n'
}

require_cmds() {
  local cmd
  for cmd in curl date dirname basename od tr sed awk grep sha256sum git cp tar gzip base64 mktemp; do
    command -v "$cmd" >/dev/null 2>&1 || usage_fail "missing required command: $cmd"
  done
  command -v "$DOCKER" >/dev/null 2>&1 || usage_fail "docker is not available: $DOCKER"
  if ((SKIP_BUILD == 0)) || [[ -z $PARKBENCH ]]; then
    command -v go >/dev/null 2>&1 || usage_fail "go is required to build parkbench (or pass --parkbench)"
  fi
}

# --- offline self-tests -----------------------------------------------------

self_case() {
  local name=$1
  shift
  if (set +e; "$@"); then
    printf 'ok   %s\n' "$name"
  else
    printf 'FAIL %s\n' "$name" >&2
    return 1
  fi
}

test_defaults_and_presets() (
  local usage_text
  init_defaults
  [[ $PRESET == small && $TARGET_BYTES == 2000000000 ]] || return 1
  [[ $PARKANA_ONLY == 0 && $INDEX_MAX_GRAMS == 2097152 ]] || return 1
  [[ $BATCH_LINES == 1250 ]] || return 1
  [[ $SMALL_BYTES == 2000000000 && $MEDIUM_BYTES == 10000000000 &&
    $FULL_BYTES == 25000000000 ]] || return 1
  usage_text=$(usage)
  [[ $usage_text == *"per label set"* ]] || return 1
  [[ $usage_text == *"(default 1250)"* ]] || return 1
  [[ $usage_text == *"--instances-per-service N"* ]] || return 1
  [[ $usage_text == *"--streams LIST"* ]] || return 1
  [[ $usage_text == *"--reuse-manifest PATH"* ]] || return 1
  [[ $usage_text == *"--legacy-runtime"* ]] || return 1
  [[ $usage_text == *"parkbench tool default"* ]] || return 1
  [[ $usage_text == *"small|medium|full"* && $usage_text == *"10,000,000,000"* ]] || return 1
  (init_defaults; parse_args --batch-lines 77; validate_args; [[ $BATCH_LINES == 77 ]]) || return 1
  (init_defaults; parse_args --preset full --allow-full; validate_args) || return 1
  (init_defaults; parse_args --preset full --allow-full; validate_args; [[ $TARGET_BYTES == 25000000000 ]]) || return 1
  (init_defaults; parse_args --bytes 25000000000 --allow-full; validate_args; [[ $PRESET == full && $TARGET_BYTES == 25000000000 ]]) || return 1
  (init_defaults; parse_args --preset small; validate_args; [[ $TARGET_BYTES == 2000000000 ]]) || return 1
  (init_defaults; parse_args --preset medium; validate_args;
    [[ $PRESET == medium && $TARGET_BYTES == 10000000000 ]]) || return 1
  (init_defaults; parse_args --bytes 10000000000; validate_args;
    [[ $PRESET == medium && $TARGET_BYTES == 10000000000 ]]) || return 1
  (init_defaults; parse_args --bytes 5000000000; validate_args;
    [[ $PRESET == custom && $TARGET_BYTES == 5000000000 ]]) || return 1
)

test_preset_validation() (
  (init_defaults; parse_args --preset medium; validate_args) || return 1
  (init_defaults; parse_args --bytes 5000000000; validate_args) || return 1
  (init_defaults; parse_args --preset full; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --preset small --bytes 25000000000; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --preset medium --bytes 2000000000; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --preset custom; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --bytes -1; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --bytes abc; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --bytes 0; validate_args) >/dev/null 2>&1 && return 1
  return 0
)

test_argument_validation() (
  (init_defaults; parse_args --nope) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --parkana-port 0; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --loki-port 13100; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --project "Bad Name"; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --repetitions 0; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --batch-lines 0; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --batch-lines -1; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --batch-lines 08; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --batch-lines abc; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --cpus abc; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --cpus 0; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --cpus 0.0; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --memory 0g; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --memory 0; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --cpus 08; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --repetitions 08; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --memory 08g; validate_args; [[ $MEMORY == 8g ]]) || return 1
  (init_defaults; parse_args --memory 0008m; validate_args; [[ $MEMORY == 8m ]]) || return 1
  (init_defaults; parse_args --tenant "bad tenant"; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --preset small; validate_args) || return 1
  return 0
)

test_instances_per_service_validation() (
  (init_defaults; validate_args; [[ -z $INSTANCES_PER_SERVICE && $INSTANCES_PER_SERVICE_EXPLICIT == 0 ]]) || return 1
  (init_defaults; parse_args --instances-per-service 1; validate_args; [[ $INSTANCES_PER_SERVICE == 1 && $INSTANCES_PER_SERVICE_EXPLICIT == 1 ]]) || return 1
  (init_defaults; parse_args --instances-per-service 4; validate_args; [[ $INSTANCES_PER_SERVICE == 4 ]]) || return 1
  (init_defaults; parse_args --instances-per-service 4096; validate_args; [[ $INSTANCES_PER_SERVICE == 4096 ]]) || return 1
  local bad
  for bad in 0 -1 08 abc 1e3 1.2 4097 9999 10000 9223372036854775807 999999999999999999999 ""; do
    if (init_defaults; parse_args --instances-per-service "$bad"; validate_args) >/dev/null 2>&1; then
      printf 'accepted invalid --instances-per-service %q\n' "$bad" >&2
      return 1
    fi
  done
  (init_defaults; parse_args --instances-per-service) >/dev/null 2>&1 && return 1
  return 0
)

test_streams_flag_validation() (
  (init_defaults; validate_args; [[ -z $STREAMS && $STREAMS_EXPLICIT == 0 ]]) || return 1
  (init_defaults; parse_args --streams api; validate_args;
    [[ $STREAMS == api && $STREAMS_EXPLICIT == 1 ]]) || return 1
  (init_defaults; parse_args --streams api,checkout,search,worker; validate_args;
    [[ $STREAMS == api,checkout,search,worker ]]) || return 1
  # Surrounding whitespace is normalized before the list is recorded or passed.
  (init_defaults; parse_args --streams " api , checkout "; validate_args;
    [[ $STREAMS == api,checkout ]]) || return 1
  # One stream times the tool's 4096-instance bound stays inside the label-set
  # bound; two streams at that bound does not.
  (init_defaults; parse_args --streams api --instances-per-service 4096; validate_args) || return 1
  if (init_defaults; parse_args --streams api,checkout --instances-per-service 4096; validate_args) >/dev/null 2>&1; then
    printf 'accepted 2 streams x 4096 instances beyond the label-set bound\n' >&2
    return 1
  fi
  local bad
  for bad in "" "," "api," ",api" "api,,checkout" "bad name" "api/1" "api;rm" \
    "PKFREQ-2v" "PKRARE-9xqz" "PKABSENT-7mvn" "zq9x" "api,api"; do
    if (init_defaults; parse_args --streams "$bad"; validate_args) >/dev/null 2>&1; then
      printf 'accepted invalid --streams %q\n' "$bad" >&2
      return 1
    fi
  done
  (init_defaults; parse_args --streams) >/dev/null 2>&1 && return 1
  return 0
)

test_reuse_manifest_validation() (
  local work manifest
  work=$(mktemp -d) || return 1
  manifest="$work/manifest.json"
  cat >"$manifest" <<'JSON'
{
  "corpus": {
    "start": "2026-01-01T00:00:00.000000Z",
    "through": "2026-01-01T00:00:10.000000Z"
  }
}
JSON
  (init_defaults; parse_args --reuse-manifest "$manifest"; validate_args;
    [[ $START == 2026-01-01T00:00:00.000000Z && $SEED == "" && $STEP == 100us &&
      $TARGET_BYTES == 2000000000 && $(start_source_value) == reused-manifest ]]) ||
    { rm -rf "$work"; return 1; }
  # --start and --reuse-manifest cannot silently disagree.
  (init_defaults; parse_args --reuse-manifest "$manifest" --start 2026-01-01T00:00:00Z; validate_args) >/dev/null 2>&1 &&
    { rm -rf "$work"; return 1; }
  (init_defaults; parse_args --reuse-manifest "$work/missing.json"; validate_args) >/dev/null 2>&1 &&
    { rm -rf "$work"; return 1; }
  printf '{\n  "corpus": {\n    "through": "2026-01-01T00:00:00Z"\n  }\n}\n' >"$manifest"
  (init_defaults; parse_args --reuse-manifest "$manifest"; validate_args) >/dev/null 2>&1 &&
    { rm -rf "$work"; return 1; }
  local bad
  for bad in "not-a-time" "tomorrow" "2026-13-99T00:00:00Z"; do
    printf '{\n  "corpus": {\n    "start": "%s"\n  }\n}\n' "$bad" >"$manifest"
    (init_defaults; parse_args --reuse-manifest "$manifest"; validate_args) >/dev/null 2>&1 &&
      { rm -rf "$work"; return 1; }
  done
  (init_defaults; parse_args --reuse-manifest) >/dev/null 2>&1 && { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_legacy_runtime_validation() (
  (init_defaults; parse_args --legacy-runtime; validate_args;
    [[ $RUNTIME_MODE == legacy-foundation && $WAL_SYNC == always &&
      $FLUSH_CONCURRENCY == 1 && $UPLOAD_CONCURRENCY == 1 &&
      $LEGACY_KNOWN_WAL_SYNC == always &&
      $(runtime_flag_source "$WAL_SYNC_EXPLICIT") == legacy-known ]]) || return 1
  # Explicit known-baseline values are allowed; anything else is rejected.
  (init_defaults; parse_args --legacy-runtime --wal-sync always \
    --flush-concurrency 1 --upload-concurrency 1; validate_args) || return 1
  local bad
  for bad in "--wal-sync interval" "--wal-sync-interval 1s" \
    "--flush-concurrency 2" "--upload-concurrency 2"; do
    if (init_defaults; parse_args --legacy-runtime $bad; validate_args) >/dev/null 2>&1; then
      printf 'accepted legacy runtime conflict %q\n' "$bad" >&2
      return 1
    fi
  done
  # The modern default path is unchanged.
  (init_defaults; validate_args; [[ $RUNTIME_MODE == modern && $WAL_SYNC == always &&
    $WAL_SYNC_INTERVAL == 1s && $FLUSH_CONCURRENCY == 4 && $UPLOAD_CONCURRENCY == 4 ]]) || return 1
  return 0
)

test_output_dir_collision() {
  local work out status
  work=$(mktemp -d) || return 1
  mkdir -p "$work/existing"
  set +e
  out=$(cd "$REPO_ROOT" && bash "$SCRIPT_PATH" --dry-run --output-dir "$work/existing" 2>&1)
  status=$?
  set -e
  rm -rf "$work"
  [[ $status != 0 ]] || return 1
  [[ $out == *"output directory already exists"* ]] || return 1
}

test_output_dir_must_be_fresh() (
  OUTPUT_DIR=$(mktemp -d) || return 1
  if (check_output_dir_absent) >/dev/null 2>&1; then
    rm -rf "$OUTPUT_DIR"
    return 1
  fi
  rm -rf "$OUTPUT_DIR"
)

test_project_collision_and_freshness() (
  local work mock
  work=$(mktemp -d) || return 1
  mock="$work/docker"
  cat >"$mock" <<'MOCK'
#!/bin/sh
case "$1 $2" in
  "ps -aq") printf 'container-id\n' ;;
  "volume ls") printf '' ;;
  "network ls") printf '' ;;
  "compose ls") printf '' ;;
  *) exit 0 ;;
esac
MOCK
  chmod 0755 "$mock"
  DOCKER=$mock
  PROJECT=existing-project
  if (ensure_project_fresh) >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi

  cat >"$mock" <<'MOCK'
#!/bin/sh
case "$1 $2" in
  "ps -aq") printf '' ;;
  "volume ls") printf '' ;;
  "network ls") printf '' ;;
  "compose ls") printf '' ;;
  *) exit 0 ;;
esac
MOCK
  chmod 0755 "$mock"
  DOCKER=$mock
  PROJECT=fresh-project
  ensure_project_fresh || { rm -rf "$work"; return 1; }

  # A leftover volume alone (no containers) must also block reuse.
  cat >"$mock" <<'MOCK'
#!/bin/sh
case "$1 $2" in
  "ps -aq") printf '' ;;
  "volume ls") printf 'volume-id\n' ;;
  "network ls") printf '' ;;
  "compose ls") printf '' ;;
  *) exit 0 ;;
esac
MOCK
  chmod 0755 "$mock"
  DOCKER=$mock
  if (ensure_project_fresh) >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi

  # Compose metadata (or a network) alone must also block reuse.
  cat >"$mock" <<'MOCK'
#!/bin/sh
case "$1 $2" in
  "ps -aq") printf '' ;;
  "volume ls") printf '' ;;
  "network ls") printf '' ;;
  "compose ls") printf 'fresh-project\n' ;;
  *) exit 0 ;;
esac
MOCK
  chmod 0755 "$mock"
  DOCKER=$mock
  if (ensure_project_fresh) >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi

  # Docker inspection errors must fail closed, never look like "fresh".
  cat >"$mock" <<'MOCK'
#!/bin/sh
case "$1 $2" in
  "ps -aq") printf 'cannot inspect daemon\n'; exit 1 ;;
  *) exit 0 ;;
esac
MOCK
  chmod 0755 "$mock"
  DOCKER=$mock
  if (ensure_project_fresh) >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
)

test_image_pins() (
  grep -Fq "$GARAGE_IMAGE_PIN" "$COMPOSE_FILE" || return 1
  grep -Fq "$LOKI_IMAGE_PIN" "$COMPOSE_FILE" || return 1
  ! grep -Fq '${GARAGE_IMAGE' "$COMPOSE_FILE" || return 1
  ! grep -Fq '${LOKI_IMAGE' "$COMPOSE_FILE" || return 1
)

test_compose_ls_argv() (
  local work mock log
  work=$(mktemp -d) || return 1
  mock="$work/docker"
  log="$work/argv"
  cat >"$mock" <<'MOCK'
#!/bin/sh
printf '%s\n' "$*" >>"${MOCK_ARGV_LOG:?}"
exit 0
MOCK
  chmod 0755 "$mock"
  export MOCK_ARGV_LOG=$log
  DOCKER=$mock
  PROJECT=fresh-project
  ensure_project_fresh || { rm -rf "$work"; return 1; }
  grep -Fq 'compose ls -a --quiet' "$log" || { rm -rf "$work"; return 1; }
  ! grep -Fq -- '--format' "$log" || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_wait_one_shot() (
  local work mock log
  work=$(mktemp -d) || return 1
  mock="$work/docker"
  log="$work/argv"
  cat >"$mock" <<'MOCK'
#!/bin/sh
printf '%s\n' "$*" >>"${MOCK_ARGV_LOG:?}"
if [ "$1" = "inspect" ]; then
  printf '%s\n' "${MOCK_STATE:-exited 0}"
  exit 0
fi
case "$*" in
  *" ps -a -q "*) printf 'container-id\n'; exit 0 ;;
esac
exit 0
MOCK
  chmod 0755 "$mock"
  export MOCK_ARGV_LOG=$log
  DOCKER=$mock
  PROJECT=selftest-project
  SECRETS_FILE="$work/secrets.env"
  RUN_LOG="$work/run.log"
  TEST_NO_SLEEP=1
  INIT_TIMEOUT=180

  # No second argument: must not evaluate deadline before timeout is set
  # under set -u.
  MOCK_STATE="exited 0"
  export MOCK_STATE
  unset timeout deadline 2>/dev/null || true
  (wait_one_shot garage-init-parkana) >/dev/null 2>&1 ||
    { rm -rf "$work"; return 1; }

  # Exited nonzero must fail.
  MOCK_STATE="exited 1"
  export MOCK_STATE
  if (wait_one_shot garage-init-parkana) >/dev/null 2>&1; then rm -rf "$work"; return 1; fi

  # Never exiting must hit the deadline and fail.
  MOCK_STATE="running 0"
  export MOCK_STATE
  if (wait_one_shot garage-init-parkana 0) >/dev/null 2>&1; then rm -rf "$work"; return 1; fi
  rm -rf "$work"
)

test_project_lock() (
  local work
  work=$(mktemp -d) || return 1
  LOCK_ROOT="$work/locks"
  PROJECT=lock-project
  LOCK_OWNED=0
  LOCK_DIR=""
  acquire_project_lock || { rm -rf "$work"; return 1; }
  [[ -f $LOCK_DIR/owner && -d $LOCK_DIR ]] || { rm -rf "$work"; return 1; }
  if (acquire_project_lock) >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  release_project_lock
  [[ ! -e $LOCK_DIR ]] || { rm -rf "$work"; return 1; }
  # Release is idempotent and never touches a lock this run does not own.
  release_project_lock
  rm -rf "$work"
)

test_credentials_are_separate_and_silent() (
  local first_parkana first_loki
  generate_credentials || return 1
  [[ $PARKANA_BENCH_PARKANA_BUCKET =~ ^parkana-p3-[0-9a-f]{10}$ ]] || return 1
  [[ $PARKANA_BENCH_LOKI_BUCKET =~ ^loki-p3-[0-9a-f]{10}$ ]] || return 1
  [[ $PARKANA_BENCH_PARKANA_ACCESS_KEY =~ ^GK[0-9a-f]{24}$ ]] || return 1
  [[ $PARKANA_BENCH_LOKI_ACCESS_KEY =~ ^GK[0-9a-f]{24}$ ]] || return 1
  [[ $PARKANA_BENCH_PARKANA_SECRET_KEY =~ ^[0-9a-f]{64}$ ]] || return 1
  [[ $PARKANA_BENCH_LOKI_SECRET_KEY =~ ^[0-9a-f]{64}$ ]] || return 1
  [[ $PARKANA_BENCH_PARKANA_BUCKET != "$PARKANA_BENCH_LOKI_BUCKET" ]] || return 1
  [[ $PARKANA_BENCH_PARKANA_ACCESS_KEY != "$PARKANA_BENCH_LOKI_ACCESS_KEY" ]] || return 1
  first_parkana=$PARKANA_BENCH_PARKANA_SECRET_KEY
  first_loki=$PARKANA_BENCH_LOKI_SECRET_KEY
  generate_credentials
  [[ $PARKANA_BENCH_PARKANA_SECRET_KEY != "$first_parkana" ]] || return 1
  [[ $PARKANA_BENCH_LOKI_SECRET_KEY != "$first_loki" ]] || return 1
)

test_env_file_mode_and_contents() (
  local work
  work=$(mktemp -d) || return 1
  SECRETS_FILE="$work/secrets.env"
  generate_credentials
  write_env_file || { rm -rf "$work"; return 1; }
  [[ $(stat -c '%a' "$SECRETS_FILE") == 600 ]] || { rm -rf "$work"; return 1; }
  grep -Fq "PARKANA_BENCH_PARKANA_BUCKET=$PARKANA_BENCH_PARKANA_BUCKET" "$SECRETS_FILE" ||
    { rm -rf "$work"; return 1; }
  [[ $(grep -c '^PARKANA_BENCH_PARKANA_SECRET_KEY=' "$SECRETS_FILE") == 1 ]] ||
    { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_dry_run_never_generates_secrets() (
  local work out
  work=$(mktemp -d) || return 1
  out=$(cd "$REPO_ROOT" && bash "$SCRIPT_PATH" --dry-run --output-dir "$work/out" 2>&1) ||
    { rm -rf "$work"; return 1; }
  [[ $out != *GK[0-9a-f]* ]] || { rm -rf "$work"; return 1; }
  [[ $out != *PARKANA_BENCH_PARKANA_SECRET_KEY=* ]] || { rm -rf "$work"; return 1; }
  [[ $out != *PARKANA_BENCH_LOKI_SECRET_KEY=* ]] || { rm -rf "$work"; return 1; }
  [[ ! -e $work/out ]] || { rm -rf "$work"; return 1; }
  [[ $out == *"values never printed"* ]] || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_redaction() (
  PARKANA_BENCH_PARKANA_SECRET_KEY=deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef
  PARKANA_BENCH_LOKI_SECRET_KEY=cafebabecafebabecafebabecafebabecafebabecafebabecafebabecafebabe
  local work
  work=$(mktemp -d) || return 1
  printf 'a=%s b=%s\n' "$PARKANA_BENCH_PARKANA_SECRET_KEY" "$PARKANA_BENCH_LOKI_SECRET_KEY" >"$work/log"
  redact_file "$work/log"
  ! grep -Fq "$PARKANA_BENCH_PARKANA_SECRET_KEY" "$work/log" || { rm -rf "$work"; return 1; }
  ! grep -Fq "$PARKANA_BENCH_LOKI_SECRET_KEY" "$work/log" || { rm -rf "$work"; return 1; }
  grep -Fq '<redacted>' "$work/log" || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_manifest_matching() (
  local work a b
  work=$(mktemp -d) || return 1
  a="$work/a.json"
  b="$work/b.json"
  cat >"$a" <<'JSON'
{
  "corpus": {
    "hash": "h",
    "content_hash": "c",
    "start": "2026-01-01T00:00:00Z",
    "through": "2026-01-01T00:00:10Z",
    "step_micros": 100,
    "target_raw_line_bytes": 100,
    "raw_line_bytes": 100,
    "lines": 10
  },
  "ack_lines": 10,
  "ack_raw_line_bytes": 100,
  "delivery": {
    "clean": true
  }
}
JSON
  cp "$a" "$b"
  compare_manifests "$a" "$b" >/dev/null || { rm -rf "$work"; return 1; }
  sed -i 's/"content_hash": "c"/"content_hash": "d"/' "$b"
  if (compare_manifests "$a" "$b") >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
)

test_catalog_and_count_parsing() (
  local snap
  snap='10|1234|3|2'
  [[ $(catalog_field "$snap" 1) == 10 && $(catalog_field "$snap" 2) == 1234 ]] || return 1
  [[ $(catalog_field "$snap" 3) == 3 && $(catalog_field "$snap" 4) == 2 ]] || return 1
  [[ $(count_from_json '{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"42"]}]}}') == 42 ]] || return 1
  [[ $(count_from_json '{"status":"success","data":{"resultType":"vector","result":[]}}') == 0 ]] || return 1
  if (count_from_json '{"status":"success","data":{}}') >/dev/null 2>&1; then return 1; fi
  if (count_from_json '') >/dev/null 2>&1; then return 1; fi
)

test_range_ms() (
  local got
  got=$(range_ms_from 2026-01-01T00:00:00.000000Z 2026-01-01T00:00:01.000000Z) || return 1
  [[ $got == 2000 ]] || return 1
  got=$(range_ms_from 2026-01-01T00:00:01Z 2026-01-01T00:00:00Z) && return 1
  return 0
)

test_readiness_failure_and_success() (
  READY_TIMEOUT=0
  TEST_NO_SLEEP=1
  http_status() { printf '503\n'; }
  if (wait_http_ready Parkana http://x/ready) >/dev/null 2>&1; then return 1; fi
  http_status() { printf '200\n'; }
  (wait_http_ready Parkana http://x/ready) >/dev/null 2>&1 || return 1
)

test_metric_absence() (
  local work state
  work=$(mktemp -d) || return 1
  state="$work/calls"
  LOKI_URL=http://loki
  # Success cases use the real default-sized budget: even with a no-sleep test
  # harness, SECONDS can advance across awk/cat subprocesses, so a 1s budget
  # would be flaky. Expected-timeout cases pass FLUSH_TIMEOUT=0 explicitly.
  FLUSH_TIMEOUT=60
  TEST_NO_SLEEP=1
  http_post() { printf '204\n'; }

  # Permanently absent post-flush counter: fail at the deadline with
  # diagnostics (FLUSH_TIMEOUT=0 makes the first iteration the deadline in the
  # no-sleep test harness).
  http_get() { printf 'loki_ingester_flush_queue_length 0\nloki_ingester_memory_chunks 1\n'; }
  if (FLUSH_TIMEOUT=0 loki_flush_wait) >/dev/null 2>&1; then rm -rf "$work"; return 1; fi

  # Normal fresh-process race: no pre-flush labeled series, then two post-flush
  # polls where the counter is still absent while the flush queue is non-empty,
  # then the first successful flush exposes the CounterVec at 1. Valid.
  printf '0\n' >"$state"
  http_get() {
    local n
    n=$(cat "$state")
    n=$((n + 1))
    printf '%s\n' "$n" >"$state"
    case "$n" in
      1) printf 'loki_ingester_flush_queue_length 3\n' ;;
      2) printf 'loki_ingester_flush_queue_length 3\n' ;;
      3) printf 'loki_ingester_flush_queue_length 2\n' ;;
      4) printf 'loki_ingester_flush_queue_length 0\nloki_ingester_chunks_flushed_total{reason="forced"} 1\n' ;;
      *) printf 'loki_ingester_flush_queue_length 0\nloki_ingester_chunks_flushed_total{reason="forced"} 1\n' ;;
    esac
  }
  (loki_flush_wait) >/dev/null 2>&1 || { rm -rf "$work"; return 1; }

  # Transport errors after POST also yield absent samples; retry until a real
  # success instead of treating the gap as zero.
  printf '0\n' >"$state"
  http_get() {
    local n
    n=$(cat "$state")
    n=$((n + 1))
    printf '%s\n' "$n" >"$state"
    case "$n" in
      1) printf 'loki_ingester_flush_queue_length 0\n' ;;
      2 | 3) printf '' ;;
      4) printf 'loki_ingester_flush_queue_length 0\nloki_ingester_chunks_flushed_total{reason="forced"} 1\n' ;;
      *) printf 'loki_ingester_flush_queue_length 0\nloki_ingester_chunks_flushed_total{reason="forced"} 1\n' ;;
    esac
  }
  (loki_flush_wait) >/dev/null 2>&1 || { rm -rf "$work"; return 1; }

  # Pre-flush absent and still absent after flush: fail at timeout.
  http_get() { printf 'loki_ingester_flush_queue_length 0\n'; }
  if (FLUSH_TIMEOUT=0 loki_flush_wait) >/dev/null 2>&1; then rm -rf "$work"; return 1; fi

  # Existing baseline 7 -> 8 with a drained queue: valid.
  printf '0\n' >"$state"
  http_get() {
    local n
    n=$(cat "$state")
    n=$((n + 1))
    printf '%s\n' "$n" >"$state"
    if ((n == 1)); then
      printf 'loki_ingester_flush_queue_length 0\nloki_ingester_chunks_flushed_total{reason="forced"} 7\n'
    else
      printf 'loki_ingester_flush_queue_length 0\nloki_ingester_chunks_flushed_total{reason="forced"} 8\n'
    fi
  }
  (loki_flush_wait) >/dev/null 2>&1 || { rm -rf "$work"; return 1; }

  # Baseline 8 -> 8: a drained queue without movement must not be accepted.
  printf '0\n' >"$state"
  http_get() {
    local n
    n=$(cat "$state")
    n=$((n + 1))
    printf '%s\n' "$n" >"$state"
    printf 'loki_ingester_flush_queue_length 0\nloki_ingester_chunks_flushed_total{reason="forced"} 8\nloki_ingester_chunks_flush_failures_total 0\n'
  }
  if (FLUSH_TIMEOUT=0 loki_flush_wait) >/dev/null 2>&1; then rm -rf "$work"; return 1; fi
  rm -rf "$work"
)

test_flush_http_failure() (
  LOKI_URL=http://loki
  FLUSH_TIMEOUT=1
  TEST_NO_SLEEP=1
  http_post() { printf '500\n'; }
  if (loki_flush_wait) >/dev/null 2>&1; then return 1; fi
)

test_exit_propagation_ingest() (
  local work mock
  work=$(mktemp -d) || return 1
  mock="$work/parkbench"
  cat >"$mock" <<'MOCK'
#!/bin/sh
echo 'mock ingest failure'
exit 7
MOCK
  chmod 0755 "$mock"
  PARKBENCH=$mock
  RUN_LOG="$work/log"
  TARGET_BYTES=16
  OUTPUT_DIR=$work
  if (run_ingest_backend parkana http://x "$work/out.json" "") >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
)

test_ingest_command_construction() (
  local work mock log out start
  work=$(mktemp -d) || return 1
  mock="$work/parkbench"
  log="$work/argv"
  out="$work/manifest.json"
  start=2026-01-01T00:00:00.000000Z
  cat >"$mock" <<'MOCK'
#!/bin/sh
{
  i=0
  for a in "$@"; do printf 'ARGV %s %s\n' "$i" "$a"; i=$((i + 1)); done
} >>"${MOCK_PARKBENCH_LOG:?}"
out=""
prev=""
for a in "$@"; do
  if [ "$prev" = "-output" ]; then out=$a; fi
  prev=$a
done
cat >"$out" <<'JSON'
{
  "corpus": {
    "hash": "h",
    "content_hash": "c",
    "start": "2026-01-01T00:00:00Z",
    "through": "2026-01-01T00:00:10Z",
    "step_micros": 100,
    "target_raw_line_bytes": 2000000000,
    "raw_line_bytes": 2000000000,
    "lines": 10,
    "instances_per_service": 4
  },
  "ack_lines": 10,
  "ack_raw_line_bytes": 2000000000,
  "elapsed_seconds": 1.25,
  "delivery": {
    "clean": true,
    "batches": 999,
    "batching": {
      "push_requests": 777
    }
  },
  "batching": {
    "entries_per_request": 1250,
    "push_requests": 8,
    "attempted_entries": 9990,
    "min_entries_per_request": 1240,
    "max_entries_per_request": 1250,
    "mean_entries_per_request": 1248.75
  },
  "parallelism": {
    "label_sets": 16,
    "active_senders": 15,
    "concurrency_limit": 16,
    "peak_in_flight_pushes": 12
  }
}
JSON
echo 'mock ingest ok'
exit 0
MOCK
  chmod 0755 "$mock"
  export MOCK_PARKBENCH_LOG=$log
  PARKBENCH=$mock
  RUN_LOG="$work/run.log"
  OUTPUT_DIR=$work
  printf 'tool=scripts/bench-phase3.sh\n' >"$work/run-metadata.txt"
  TARGET_BYTES=2000000000
  STEP=100us
  BATCH_LINES=1250
  CONCURRENCY=16
  INSTANCES_PER_SERVICE=""
  INSTANCES_PER_SERVICE_EXPLICIT=0
  STREAMS=""
  STREAMS_EXPLICIT=0
  INGEST_ATTEMPT_TIMEOUT=60s
  INGEST_RETRIES=3
  SEED=""
  TENANT=tenant-a
  run_ingest_backend parkana http://parkana/loki/api/v1/push "$out" "$start" >/dev/null ||
    { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 0 ingest' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 1 -url' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 2 http://parkana/loki/api/v1/push' "$log" || { rm -rf "$work"; return 1; }
  grep -Fq -- '-bytes' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 4 2000000000' "$log" || { rm -rf "$work"; return 1; }
  grep -Fq -- '-start' "$log" || { rm -rf "$work"; return 1; }
  grep -Fq -- "$start" "$log" || { rm -rf "$work"; return 1; }
  grep -Fq -- '-tenant' "$log" || { rm -rf "$work"; return 1; }
  grep -Fq -- 'tenant-a' "$log" || { rm -rf "$work"; return 1; }
  grep -Fq -- '-output' "$log" || { rm -rf "$work"; return 1; }
  grep -Fq -- "$out" "$log" || { rm -rf "$work"; return 1; }
  # corpus v3: one sequential sender per label set, so the configured value is
  # the per-label-set request ceiling and the default is 1250.
  [[ $(argv_value "$log" -batch-lines) == 1250 ]] || { rm -rf "$work"; return 1; }
  # Omitted means the flag is absent, so the tool's own default applies; never
  # pass a hardcoded 4 as if the caller had requested it.
  [[ -z $(argv_value "$log" -instances-per-service) ]] || { rm -rf "$work"; return 1; }
  : >"$log"
  BATCH_LINES=77
  run_ingest_backend parkana http://parkana/loki/api/v1/push "$work/out77.json" "$start" >/dev/null ||
    { rm -rf "$work"; return 1; }
  [[ $(argv_value "$log" -batch-lines) == 77 ]] || { rm -rf "$work"; return 1; }
  [[ -z $(argv_value "$log" -instances-per-service) ]] || { rm -rf "$work"; return 1; }
  : >"$log"
  INSTANCES_PER_SERVICE=1
  run_ingest_backend parkana http://parkana/loki/api/v1/push "$work/out1.json" "$start" >/dev/null ||
    { rm -rf "$work"; return 1; }
  [[ $(argv_value "$log" -instances-per-service) == 1 ]] || { rm -rf "$work"; return 1; }
  : >"$log"
  INSTANCES_PER_SERVICE=4
  run_ingest_backend parkana http://parkana/loki/api/v1/push "$work/out4.json" "$start" >/dev/null ||
    { rm -rf "$work"; return 1; }
  [[ $(argv_value "$log" -instances-per-service) == 4 ]] || { rm -rf "$work"; return 1; }
  # Omitted streams stay omitted, so parkbench keeps its own four-stream default.
  : >"$log"
  INSTANCES_PER_SERVICE=""
  STREAMS=""
  STREAMS_EXPLICIT=0
  run_ingest_backend parkana http://parkana/loki/api/v1/push "$work/out-default-streams.json" "$start" >/dev/null ||
    { rm -rf "$work"; return 1; }
  [[ -z $(argv_value "$log" -streams) ]] || { rm -rf "$work"; return 1; }
  # An explicit list is passed verbatim; the single-hot-stream arm also passes
  # its explicit instance count so one stream still has 16 label sets/senders.
  : >"$log"
  STREAMS=api
  STREAMS_EXPLICIT=1
  INSTANCES_PER_SERVICE=16
  run_ingest_backend parkana http://parkana/loki/api/v1/push "$work/out-one-stream.json" "$start" >/dev/null ||
    { rm -rf "$work"; return 1; }
  [[ $(argv_value "$log" -streams) == api ]] || { rm -rf "$work"; return 1; }
  [[ $(argv_value "$log" -instances-per-service) == 16 ]] || { rm -rf "$work"; return 1; }
  : >"$log"
  STREAMS=api,checkout
  run_ingest_backend parkana http://parkana/loki/api/v1/push "$work/out-two-streams.json" "$start" >/dev/null ||
    { rm -rf "$work"; return 1; }
  [[ $(argv_value "$log" -streams) == api,checkout ]] || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_ack_before_cpu_snapshot() (
  local work mock out state
  work=$(mktemp -d) || return 1
  mock="$work/parkbench"
  out="$work/manifest.json"
  state="$work/clock"
  printf '0\n' >"$state"
  cat >"$mock" <<'MOCK'
#!/bin/sh
out="" prev=""
for a in "$@"; do
  [ "$prev" = "-output" ] && out=$a
  prev=$a
done
cat >"$out" <<'JSON'
{
  "corpus": {
    "hash": "h",
    "content_hash": "c",
    "start": "2026-01-01T00:00:00Z",
    "through": "2026-01-01T00:00:10Z",
    "step_micros": 100,
    "target_raw_line_bytes": 2000000000,
    "raw_line_bytes": 1000,
    "lines": 10,
    "instances_per_service": 4
  },
  "ack_lines": 10,
  "ack_raw_line_bytes": 1000,
  "elapsed_seconds": 1.0,
  "delivery": { "clean": true },
  "batching": {},
  "parallelism": {}
}
JSON
exit 0
MOCK
  chmod 0755 "$mock"
  PARKBENCH=$mock
  RUN_LOG="$work/run.log"
  OUTPUT_DIR=$work
  printf 'tool=test\n' >"$work/run-metadata.txt"
  TARGET_BYTES=2000000000
  STEP=100us
  BATCH_LINES=1250
  CONCURRENCY=16
  INSTANCES_PER_SERVICE=""
  INSTANCES_PER_SERVICE_EXPLICIT=0
  STREAMS=""
  STREAMS_EXPLICIT=0
  REUSE_MANIFEST=""
  LEGACY_RUNTIME=0
  INGEST_ATTEMPT_TIMEOUT=60s
  INGEST_RETRIES=3
  SEED=""
  TENANT=""
  prepare_mock_docker "$work/docker" || { rm -rf "$work"; return 1; }
  DOCKER="$work/docker"
  PROJECT=selftest-project
  SECRETS_FILE="$work/secrets.env"
  : >"$SECRETS_FILE"
  MOCK_CPU_STATE="$work/cpu-state"
  printf '0\n' >"$MOCK_CPU_STATE"
  export MOCK_DOCKER_LOG="$work/docker.argv" MOCK_CPU_STATE
  # The fake clock advances 7s during the post-ack CPU read. ACK_AT must keep
  # the ingest-return value (1002), not the later snapshot wall (1009).
  epoch_now() {
    local n
    n=$(cat "$state")
    n=$((n + 1))
    printf '%s\n' "$n" >"$state"
    case "$n" in
      1) printf '1000.000000000' ;;
      2) printf '1001.000000000' ;;
      3) printf '1002.000000000' ;;
      *) printf '1009.000000000' ;;
    esac
  }
  run_ingest_backend parkana http://x "$out" "" >/dev/null || { rm -rf "$work"; return 1; }
  [[ $ACK_AT == 1002.000000000 && $ACK_WALL_SECONDS == 1.000 ]] ||
    { rm -rf "$work"; return 1; }
  [[ $(get_cpu_field parkana AFTER_WALL_S) == 1009.000000000 ]] ||
    { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_batching_metadata_append() (
  local work f
  work=$(mktemp -d) || return 1
  OUTPUT_DIR=$work
  printf 'tool=scripts/bench-phase3.sh\nbatch_lines=1250\nbatch_lines_unit=entries per push request\n' >"$work/static"
  cp "$work/static" "$work/run-metadata.txt"
  f="$work/report.json"
  cat >"$f" <<'JSON'
{
  "ack_lines": 10,
  "delivery": {
    "clean": true,
    "batches": 999,
    "requests": 9,
    "entries_per_request": 7,
    "batching": {
      "push_requests": 777
    }
  },
  "corpus": {
    "streams": [
      {
        "name": "api",
        "lines": 5
      }
    ],
    "instances_per_service": 4
  },
  "batching": {
    "entries_per_request": 1250,
    "push_requests": 8,
    "attempted_entries": 9990,
    "min_entries_per_request": 1240,
    "max_entries_per_request": 1250,
    "mean_entries_per_request": 1248.75
  },
  "parallelism": {
    "label_sets": 16,
    "active_senders": 15,
    "concurrency_limit": 16,
    "peak_in_flight_pushes": 12
  }
}
JSON
  record_ingest_evidence parkana "$f" || { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_batching_entries_per_request=1250' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_batching_push_requests=8' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_batching_attempted_entries=9990' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_batching_min_entries_per_request=1240' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_batching_max_entries_per_request=1250' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_batching_mean_entries_per_request=1248.75' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_batching=complete' ||
    { rm -rf "$work"; return 1; }
  # Nested streams inside corpus must not end the block before the actual
  # instances_per_service field, and measured parallelism is report data.
  require_grep "$work/run-metadata.txt" 'ingest_parkana_corpus_instances_per_service=4' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_corpus=complete' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_parallelism_label_sets=16' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_parallelism_active_senders=15' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_parallelism_concurrency_limit=16' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_parallelism_peak_in_flight_pushes=12' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_parallelism=complete' ||
    { rm -rf "$work"; return 1; }
  # Static hardware/config/identity lines survive verbatim.
  head -n 3 "$work/run-metadata.txt" | diff -u "$work/static" - >/dev/null ||
    { rm -rf "$work"; return 1; }
  # Only the backend that ran is named; never a fabricated Loki row.
  require_no_grep "$work/run-metadata.txt" 'ingest_loki_' || { rm -rf "$work"; return 1; }
  # Non-batching numeric report fields are not mixed into the batching block;
  # actual corpus identity (including ack_lines) is recorded separately.
  require_grep "$work/run-metadata.txt" 'ingest_parkana_ack_lines=10' ||
    { rm -rf "$work"; return 1; }
  require_no_grep "$work/run-metadata.txt" 'batches' || { rm -rf "$work"; return 1; }
  require_no_regex "$work/run-metadata.txt" 'ingest_parkana_batching_.*=(999|777|7)$' ||
    { rm -rf "$work"; return 1; }

  # Missing, null and malformed/partial fields stay explicitly unavailable.
  cat >"$f" <<'JSON'
{
  "delivery": {
    "entries_per_request": 7,
    "batches": 999,
    "batching": {
      "push_requests": 777
    }
  },
  "batching": {
    "entries_per_request": null,
    "push_requests": 12abc,
    "min_entries_per_request": 1.2.3,
    "max_entries_per_request": "1250",
    "mean_entries_per_request": 0x10
  }
}
JSON
  record_ingest_evidence loki "$f" || { rm -rf "$work"; return 1; }
  for v in entries_per_request push_requests attempted_entries \
    min_entries_per_request max_entries_per_request mean_entries_per_request; do
    require_grep "$work/run-metadata.txt" "ingest_loki_batching_$v=unavailable" ||
      { rm -rf "$work"; return 1; }
  done
  require_grep "$work/run-metadata.txt" 'ingest_loki_batching=incomplete' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_loki_corpus_instances_per_service=unavailable' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_loki_corpus=incomplete' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_loki_parallelism_label_sets=unavailable' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_loki_parallelism_active_senders=unavailable' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_loki_parallelism=incomplete' ||
    { rm -rf "$work"; return 1; }
  require_no_regex "$work/run-metadata.txt" 'ingest_loki_batching_.*=(999|777|7)$' ||
    { rm -rf "$work"; return 1; }

  # A real zero in the report is preserved as measured, not replaced.
  cat >"$f" <<'JSON'
{
  "batching": {
    "entries_per_request": 0,
    "push_requests": 0,
    "attempted_entries": 0,
    "min_entries_per_request": 0,
    "max_entries_per_request": 0,
    "mean_entries_per_request": 0
  }
}
JSON
  record_ingest_evidence parkana "$f" || { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_batching_entries_per_request=0' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_batching_push_requests=0' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_batching_attempted_entries=0' ||
    { rm -rf "$work"; return 1; }

  # Scientific notation is a complete JSON number and is preserved verbatim.
  cat >"$f" <<'JSON'
{
  "batching": {
    "entries_per_request": 1250,
    "push_requests": 8,
    "attempted_entries": 9990,
    "min_entries_per_request": 1240,
    "max_entries_per_request": 1250,
    "mean_entries_per_request": 1.24875e3
  }
}
JSON
  record_ingest_evidence parkana "$f" || { rm -rf "$work"; return 1; }
  require_grep "$work/run-metadata.txt" 'ingest_parkana_batching_mean_entries_per_request=1.24875e3' ||
    { rm -rf "$work"; return 1; }

  rm -rf "$work"
)

test_exit_propagation_query() (
  local work mock
  work=$(mktemp -d) || return 1
  mock="$work/parkbench"
  cat >"$mock" <<'MOCK'
#!/bin/sh
echo 'mock query failure'
exit 1
MOCK
  chmod 0755 "$mock"
  PARKBENCH=$mock
  RUN_LOG="$work/log"
  OUTPUT_DIR=$work
  SKIP_QUERIES=0
  if (run_query_backend parkana http://x "$work/m.json") >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
)

test_command_construction() (
  local work mock log
  work=$(mktemp -d) || return 1
  mock="$work/docker"
  log="$work/argv"
  cat >"$mock" <<'MOCK'
#!/bin/sh
{
  i=0
  for a in "$@"; do printf 'ARGV %s %s\n' "$i" "$a"; i=$((i + 1)); done
} >>"${MOCK_ARGV_LOG:?}"
exit 0
MOCK
  chmod 0755 "$mock"
  DOCKER=$mock
  PROJECT=selftest-project
  SECRETS_FILE="$work/secrets.env"
  printf 'PARKANA_BENCH_LOKI_SECRET_KEY=secret-value\n' >"$SECRETS_FILE"
  export MOCK_ARGV_LOG=$log
  compose up -d garage parkana || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 0 compose' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 1 --project-name' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 2 selftest-project' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 3 --env-file' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq "ARGV 4 $SECRETS_FILE" "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 5 -f' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq "ARGV 6 $COMPOSE_FILE" "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 7 -f' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq "ARGV 8 $RUNTIME_OVERLAY" "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 9 up' "$log" || { rm -rf "$work"; return 1; }
  grep -Fq 'secret-value' "$log" && { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_stop_proof() (
  local work mock log
  work=$(mktemp -d) || return 1
  mock="$work/docker"
  log="$work/argv"
  cat >"$mock" <<'MOCK'
#!/bin/sh
if [ "$1" = "inspect" ]; then printf '%s\n' "${MOCK_STATE:-exited}"; exit 0; fi
printf '%s\n' "$*" >>"${MOCK_ARGV_LOG:?}"
case "$*" in
  *" stop "*)
    if [ "${MOCK_NO_ID:-0}" = 1 ]; then exit "${MOCK_STOP_EXIT:-0}"; fi
    exit "${MOCK_STOP_EXIT:-0}"
    ;;
  *" ps -a -q "*)
    if [ "${MOCK_NO_ID:-0}" = 1 ]; then exit 0; fi
    printf 'container-id\n'
    exit 0
    ;;
esac
exit 0
MOCK
  chmod 0755 "$mock"
  DOCKER=$mock
  PROJECT=selftest-project
  SECRETS_FILE="$work/secrets.env"
  RUN_LOG="$work/run.log"
  export MOCK_ARGV_LOG=$log
  TEST_NO_SLEEP=1
  STOP_TIMEOUT=0

  MOCK_STOP_EXIT=0 MOCK_STATE=exited
  export MOCK_STOP_EXIT MOCK_STATE MOCK_NO_ID
  (stop_backend parkana) >/dev/null 2>&1 || { rm -rf "$work"; return 1; }

  MOCK_STOP_EXIT=1 MOCK_STATE=exited
  export MOCK_STOP_EXIT MOCK_STATE
  if (stop_backend parkana) >/dev/null 2>&1; then rm -rf "$work"; return 1; fi

  MOCK_STOP_EXIT=0 MOCK_STATE=running
  export MOCK_STOP_EXIT MOCK_STATE
  if (stop_backend parkana) >/dev/null 2>&1; then rm -rf "$work"; return 1; fi

  MOCK_STOP_EXIT=0 MOCK_STATE=exited MOCK_NO_ID=1
  export MOCK_STOP_EXIT MOCK_STATE MOCK_NO_ID
  if (stop_backend parkana) >/dev/null 2>&1; then rm -rf "$work"; return 1; fi
  rm -rf "$work"
)

test_cleanup_ownership() (  local work mock log
  work=$(mktemp -d) || return 1
  mock="$work/docker"
  log="$work/argv"
  cat >"$mock" <<'MOCK'
#!/bin/sh
printf '%s\n' "$*" >>"${MOCK_ARGV_LOG:?}"
exit 0
MOCK
  chmod 0755 "$mock"
  DOCKER=$mock
  PROJECT=selftest-project
  SECRETS_FILE="$work/secrets.env"
  RUN_LOG="$work/run.log"
  export MOCK_ARGV_LOG=$log
  STACK_OWNED=0
  teardown_stack || { rm -rf "$work"; return 1; }
  [[ ! -s $log ]] || { rm -rf "$work"; return 1; }
  STACK_OWNED=1
  teardown_stack || { rm -rf "$work"; return 1; }
  grep -Fq 'down -v --remove-orphans' "$log" || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_source_identity() (
  SOURCE_COMMIT=abc123
  SOURCE_DIRTY=1
  [[ $(source_commit_value) == abc123 ]] || return 1
  [[ $(source_dirty_value) == 1 ]] || return 1
  SOURCE_COMMIT=""
  SOURCE_DIRTY=""
  git() { return 128; }
  [[ $(source_commit_value) == unavailable ]] || return 1
  [[ $(source_dirty_value) == unknown ]] || return 1
  # A clean git tree reports zero only when git actually answered.
  git() { return 0; }
  [[ $(source_dirty_value) == 0 ]] || return 1
)

test_storage_parsing() (
  local info
  info='{"id":"abc","bytes":12345,"objects":17,"unfinished_uploads":0,"quotas":{"max_size":null,"max_objects":null}}'
  [[ $(garage_json_uint "$info" bytes) == 12345 ]] || return 1
  [[ $(garage_json_uint "$info" objects) == 17 ]] || return 1
  [[ -z $(garage_json_uint "$info" missing_field) ]] || return 1
  [[ -z $(garage_json_uint '{"not_bytes":1}' bytes) ]] || return 1
  [[ -z $(metric_sum 'other_metric 5' parkana_index_bytes_total) ]] || return 1
  [[ $(metric_sum 'parkana_index_bytes_total{source="ingest"} 10
parkana_index_bytes_total{source="backfill"} 5' parkana_index_bytes_total) == 15 ]] || return 1
  # Exact family matching: near-names must not be summed.
  [[ -z $(metric_sum 'parkana_index_bytes_total_created 10' parkana_index_bytes_total) ]] || return 1
  [[ -z $(metric_sum 'parkana_index_bytes_total_extra 10' parkana_index_bytes_total) ]] || return 1
  [[ $(metric_sum 'parkana_index_bytes_total_created 10
parkana_index_bytes_total{source="ingest"} 3' parkana_index_bytes_total) == 3 ]] || return 1
)

test_metric_idle() (
  TEST_NO_SLEEP=1
  VISIBILITY_TIMEOUT=1
  http_get() { printf 'parkana_ingest_pending_generations 0\n'; }
  (wait_metric_zero Parkana http://parkana/metrics parkana_ingest_pending_generations) >/dev/null 2>&1 || return 1
  http_get() { printf 'something_else 1\n'; }
  if (wait_metric_zero Parkana http://parkana/metrics parkana_ingest_pending_generations) >/dev/null 2>&1; then return 1; fi
  http_get() { printf 'parkana_ingest_pending_generations 3\n'; }
  if (VISIBILITY_TIMEOUT=0 wait_metric_zero Parkana http://parkana/metrics parkana_ingest_pending_generations) >/dev/null 2>&1; then return 1; fi
)

test_loki_local_parsing() (
  RUN_LOG=$(mktemp) || return 1
  compose() {
    printf '100\t/loki\n50\t/loki/wal\n30\t/loki/index\n10\t/loki/index_cache\n5\t/loki/compactor\n'
  }
  loki_local_snapshot || { rm -f "$RUN_LOG"; return 1; }
  [[ $LOKI_LOCAL_TOTAL == 100 && $LOKI_LOCAL_WAL == 50 && $LOKI_LOCAL_INDEX == 30 ]] ||
    { rm -f "$RUN_LOG"; return 1; }
  [[ $LOKI_LOCAL_INDEX_CACHE == 10 && $LOKI_LOCAL_COMPACTOR == 5 ]] ||
    { rm -f "$RUN_LOG"; return 1; }

  # A missing middle directory must leave that key empty, never shift the
  # later index_cache/compactor values into the wrong fields.
  compose() {
    printf '100\t/loki\n50\t/loki/wal\n10\t/loki/index_cache\n5\t/loki/compactor\n'
  }
  loki_local_snapshot || { rm -f "$RUN_LOG"; return 1; }
  [[ $LOKI_LOCAL_TOTAL == 100 && $LOKI_LOCAL_WAL == 50 && -z $LOKI_LOCAL_INDEX ]] ||
    { rm -f "$RUN_LOG"; return 1; }
  [[ $LOKI_LOCAL_INDEX_CACHE == 10 && $LOKI_LOCAL_COMPACTOR == 5 ]] ||
    { rm -f "$RUN_LOG"; return 1; }
  rm -f "$RUN_LOG"
)

test_artifact_archive() (
  local work out list
  work=$(mktemp -d) || return 1
  OUTPUT_DIR=$work
  RUN_LOG="$work/run.log"
  printf 'secret-like content\n' >"$work/secrets.env"
  printf 'summary\n' >"$work/summary.md"
  printf 'metadata\n' >"$work/run-metadata.txt"
  printf 'timings\n' >"$work/timings.tsv"
  printf 'storage\n' >"$work/storage-parkana.txt"
  printf '{"corpus":{}}\n' >"$work/ingest-parkana.json"
  printf 'log line\n' >"$RUN_LOG"
  out="$work/archive.txt"
  emit_archive >"$out" || { rm -rf "$work"; return 1; }
  grep -Fq '=== BENCH ARTIFACT ARCHIVE BEGIN' "$out" || { rm -rf "$work"; return 1; }
  grep -Fq '=== BENCH ARTIFACT ARCHIVE END ===' "$out" || { rm -rf "$work"; return 1; }
  list="$work/list"
  awk '/=== BENCH ARTIFACT ARCHIVE BEGIN/{f=1; next} /=== BENCH ARTIFACT ARCHIVE END ===/{f=0} f' "$out" |
    base64 -d | tar -tzf - >"$list" || { rm -rf "$work"; return 1; }
  grep -Fxq 'summary.md' "$list" || { rm -rf "$work"; return 1; }
  ! grep -Fxq 'secrets.env' "$list" || { rm -rf "$work"; return 1; }
  ! grep -Fxq 'run.log' "$list" || { rm -rf "$work"; return 1; }
  grep -Fxq 'run-tail.txt' "$list" || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_loki_config_parity() (
  local cfg="$LOKI_CONFIG"
  [[ -f $cfg ]] || return 1
  grep -Fq 'schema: v13' "$cfg" || return 1
  grep -Fq 'from: 2020-10-24' "$cfg" || return 1
  grep -Fq 'object_store: s3' "$cfg" || return 1
  ! grep -Fq 'object_store: filesystem' "$cfg" || return 1
  grep -Fq 'allow_structured_metadata: true' "$cfg" || return 1
  grep -Fq 'volume_enabled: true' "$cfg" || return 1
  grep -Fq 'discover_log_levels: false' "$cfg" || return 1
  grep -Fq 'discover_service_name: []' "$cfg" || return 1
  grep -Fq 'bucketnames: ${LOKI_BENCH_S3_BUCKET}' "$cfg" || return 1
  grep -Fq 's3forcepathstyle: true' "$cfg" || return 1
  grep -Fq 'insecure: true' "$cfg" || return 1
  grep -Fq 'compaction_interval: 24h' "$cfg" || return 1
  ! grep -Eq '^[[:space:]]*ingestion_rate_mb:[[:space:]]*4[[:space:]]*$' "$cfg" || return 1
  grep -Fq 'per_stream_rate_limit: 64MB' "$cfg" || return 1
  grep -Fq 'per_stream_rate_limit_burst: 64MB' "$cfg" || return 1
)

test_no_phase2_edits() {
  local status
  status=$(git -C "$REPO_ROOT" status --porcelain -- deploy/compose/compose.yaml deploy/compose/loki/config.yaml deploy/compose/garage.toml deploy/compose/garage.Dockerfile deploy/compose/garage-init.sh 2>/dev/null || true)
  [[ -z $status ]]
}

require_file() { [[ -f $1 ]] || { printf 'missing file: %s\n' "$1" >&2; return 1; }; }
require_absent() { [[ ! -e $1 ]] || { printf 'unexpected file: %s\n' "$1" >&2; return 1; }; }
require_grep() { grep -Fq -- "$2" "$1" || { printf 'missing %s in %s\n' "$2" "$1" >&2; return 1; }; }
argv_value() { # argv-log flag
  awk -v flag="$2" '$1 == "ARGV" { if (prev == flag) { print $3; exit } prev = $3 }' "$1"
}
require_no_grep() { ! grep -Fq -- "$2" "$1" || { printf 'unexpected %s in %s\n' "$2" "$1" >&2; return 1; }; }
require_regex() { grep -Eq -- "$2" "$1" || { printf 'missing pattern %s in %s\n' "$2" "$1" >&2; return 1; }; }
require_no_regex() { ! grep -Eq -- "$2" "$1" || { printf 'unexpected pattern %s in %s\n' "$2" "$1" >&2; return 1; }; }

prepare_mock_docker() { # path
  cat >"$1" <<'MOCK'
#!/bin/sh
printf '%s\n' "$*" >>"${MOCK_DOCKER_LOG:?}"
case "$*" in
  *"Config.Env"*)
    printf 'PARKANA_WAL_SYNC=%s\n' "${MOCK_EFFECTIVE_WAL_SYNC:-always}"
    printf 'PARKANA_WAL_SYNC_INTERVAL=%s\n' "${MOCK_EFFECTIVE_WAL_SYNC_INTERVAL:-1s}"
    printf 'PARKANA_INGEST_FLUSH_CONCURRENCY=%s\n' "${MOCK_EFFECTIVE_FLUSH_CONCURRENCY:-4}"
    printf 'PARKANA_INGEST_UPLOAD_CONCURRENCY=%s\n' "${MOCK_EFFECTIVE_UPLOAD_CONCURRENCY:-4}"
    printf 'UNRELATED_KEY=not-a-runtime-setting\n'
    ;;
  *"HostConfig.NanoCpus"*)
    printf '%s %s %s %s %s\n' \
      "${MOCK_EFFECTIVE_NANO_CPUS:-4000000000}" \
      "${MOCK_EFFECTIVE_CPU_QUOTA:-0}" \
      "${MOCK_EFFECTIVE_CPU_PERIOD:-0}" \
      "${MOCK_EFFECTIVE_MEMORY_BYTES:-8589934592}" \
      "${MOCK_EFFECTIVE_IMAGE:-sha256:mock-parkana}"
    ;;
  *"exec -T parkana /usr/local/bin/parkana -h"*)
    case "${MOCK_LEGACY_BINARY:-0}" in
      1) printf 'Usage of parkana:\n  -roles string\n' ;;
      partial) printf 'Usage of parkana:\n  -roles string\n  -wal.sync string\n' ;;
      *)
        printf 'Usage of parkana:\n  -wal.sync string\n  -wal.sync-interval duration\n  -ingest.flush-concurrency int\n  -ingest.upload-concurrency int\n'
        ;;
    esac
    ;;
  *"exec -T parkana cat /sys/fs/cgroup/memory.peak"*)
    printf '%s\n' "${MOCK_MEMORY_PEAK_BYTES-3221225472}"
    ;;
  *"exec -T parkana cat /sys/fs/cgroup/memory.events"*)
    if [ "${MOCK_OOM_UNAVAILABLE:-0}" = 1 ]; then exit 0; fi
    if [ "${MOCK_OOM_MALFORMED:-0}" = 1 ]; then printf 'not-a-counter\n'; exit 0; fi
    printf 'low 0\noom 0\noom_kill %s\noom_group_kill 0\n' "${MOCK_OOM_KILL-0}"
    ;;
  *"cat /sys/fs/cgroup/cpu.stat"*)
    if [ "${MOCK_CPU_UNAVAILABLE:-0}" = 1 ]; then exit 0; fi
    n=0
    if [ -n "${MOCK_CPU_STATE:-}" ] && [ -f "$MOCK_CPU_STATE" ]; then n=$(cat "$MOCK_CPU_STATE"); fi
    n=$((n + 1))
    if [ -n "${MOCK_CPU_STATE:-}" ]; then printf '%s\n' "$n" >"$MOCK_CPU_STATE"; fi
    printf 'usage_usec %s\nuser_usec 1\nsystem_usec 1\n' "$((n * 1000000))"
    ;;
  *"State.ExitCode"*) printf 'exited 0\n' ;;
  *"inspect"*) printf 'exited\n' ;;
  *"ps -a -q"*) printf 'container-id\n' ;;
  *"du -sb /data"*) printf '4096\t/data\n' ;;
  *"version --format"*) printf 'mock-docker\n' ;;
  *"compose version --short"*) printf 'mock-compose\n' ;;
esac
exit 0
MOCK
  chmod 0755 "$1"
}

prepare_mock_parkbench() { # path
  cat >"$1" <<'MOCK'
#!/bin/sh
printf '%s\n' "$*" >>"${MOCK_PARKBENCH_LOG:?}"
case "$1" in
  help)
    printf 'parkbench\n  ingest ...\n  query ...\n'
    exit 0
    ;;
  ingest)
    out="" prev=""
    for a in "$@"; do
      [ "$prev" = "-output" ] && out=$a
      prev=$a
    done
    streams_json=""
    old_ifs=$IFS
    IFS=,
    for stream in ${MOCK_REPORT_STREAMS:-api,checkout,search,worker}; do
      [ -n "$streams_json" ] && streams_json="$streams_json,
"
      streams_json="$streams_json      {
        \"name\": \"$stream\",
        \"labels\": {},
        \"lines\": 5,
        \"raw_line_bytes\": 5
      }"
    done
    IFS=$old_ifs
    cat >"$out" <<JSON
{
  "corpus": {
    "hash": "h",
    "content_hash": "c",
    "start": "2026-01-01T00:00:00Z",
    "through": "2026-01-01T00:00:10Z",
    "step_micros": 100,
    "target_raw_line_bytes": 2000000000,
    "raw_line_bytes": 1000,
    "lines": 10,
    "instances_per_service": ${MOCK_REPORT_INSTANCES:-4},
    "streams": [
${streams_json}
    ]
  },
  "ack_lines": 10,
  "ack_raw_line_bytes": 1000,
  "elapsed_seconds": 1.5,
  "delivery": {
    "clean": true,
    "batches": 999,
    "batching": {
      "push_requests": 777
    }
  },
  "batching": {
    "entries_per_request": 1250,
    "push_requests": 8,
    "attempted_entries": 9990,
    "min_entries_per_request": 1240,
    "max_entries_per_request": 1250,
    "mean_entries_per_request": 1248.75
  },
  "parallelism": {
    "label_sets": ${MOCK_REPORT_LABEL_SETS:-16},
    "active_senders": ${MOCK_REPORT_ACTIVE_SENDERS:-15},
    "concurrency_limit": 16,
    "peak_in_flight_pushes": 12
  }
}
JSON
    exit 0
    ;;
  query)
    out="" md="" prev=""
    for a in "$@"; do
      case "$prev" in
        --output) out=$a ;;
        --markdown) md=$a ;;
      esac
      prev=$a
    done
    printf '{"valid":true}\n' >"$out"
    printf '| query | observed |\n' >"$md"
    exit 0
    ;;
esac
exit 2
MOCK
  chmod 0755 "$1"
}

# Runs the real run_benchmark orchestration against offline Docker, HTTP and
# parkbench doubles.  Every Docker/parkbench call and every log event is
# recorded so tests can prove which backends actually ran.
run_mock_benchmark() { # mode(parkana-only|dual) cap work
  local mode=$1 cap=$2 work=$3
  local -a args=(--output-dir "$work/out" --index-max-grams "$cap")
  local snapshot=${MOCK_CATALOG_SNAPSHOT:-'10|1000|1|1'}
  local garage_json=${MOCK_GARAGE_INFO:-'{"bytes":1500,"objects":2}'}
  init_defaults
  [[ $mode == parkana-only ]] && args+=(--parkana-only)
  [[ -n ${MOCK_INSTANCES_PER_SERVICE:-} ]] && args+=(--instances-per-service "$MOCK_INSTANCES_PER_SERVICE")
  [[ -n ${MOCK_STREAMS:-} ]] && args+=(--streams "$MOCK_STREAMS")
  [[ -n ${MOCK_REUSE_MANIFEST:-} ]] && args+=(--reuse-manifest "$MOCK_REUSE_MANIFEST")
  [[ ${MOCK_LEGACY:-0} == 1 ]] && args+=(--legacy-runtime)
  [[ -n ${MOCK_WAL_SYNC:-} ]] && args+=(--wal-sync "$MOCK_WAL_SYNC")
  [[ -n ${MOCK_WAL_SYNC_INTERVAL:-} ]] && args+=(--wal-sync-interval "$MOCK_WAL_SYNC_INTERVAL")
  [[ -n ${MOCK_FLUSH_CONCURRENCY:-} ]] && args+=(--flush-concurrency "$MOCK_FLUSH_CONCURRENCY")
  [[ -n ${MOCK_UPLOAD_CONCURRENCY:-} ]] && args+=(--upload-concurrency "$MOCK_UPLOAD_CONCURRENCY")
  [[ ${MOCK_SKIP_QUERIES:-0} == 1 ]] && args+=(--skip-queries)
  parse_args "${args[@]}"
  validate_args
  OUTPUT_DIR="$work/out"
  mkdir -p "$OUTPUT_DIR" || return 1
  RUN_LOG="$OUTPUT_DIR/run.log"
  SECRETS_FILE="$OUTPUT_DIR/secrets.env"
  STACK_OWNED=1
  TEST_NO_SLEEP=1
  INIT_TIMEOUT=60
  STOP_TIMEOUT=60
  READY_TIMEOUT=60
  VISIBILITY_TIMEOUT=60
  FLUSH_TIMEOUT=60
  generate_credentials
  write_env_file
  MOCK_DOCKER_LOG="$work/docker.argv"
  MOCK_PARKBENCH_LOG="$work/parkbench.argv"
  MOCK_EVENT_LOG="$work/events"
  MOCK_LOKI_SCRAPE="$work/loki-scrapes"
  MOCK_CPU_STATE="$work/cpu-state"
  printf '0\n' >"$MOCK_CPU_STATE"
  # The Docker double reports the settings the real container should have
  # received. A test can preseed a MOCK_EFFECTIVE_* value to simulate a
  # mismatch without changing the requested flags.
  export MOCK_EFFECTIVE_WAL_SYNC="${MOCK_EFFECTIVE_WAL_SYNC:-$WAL_SYNC}"
  export MOCK_EFFECTIVE_WAL_SYNC_INTERVAL="${MOCK_EFFECTIVE_WAL_SYNC_INTERVAL:-$WAL_SYNC_INTERVAL}"
  export MOCK_EFFECTIVE_FLUSH_CONCURRENCY="${MOCK_EFFECTIVE_FLUSH_CONCURRENCY:-$FLUSH_CONCURRENCY}"
  export MOCK_EFFECTIVE_UPLOAD_CONCURRENCY="${MOCK_EFFECTIVE_UPLOAD_CONCURRENCY:-$UPLOAD_CONCURRENCY}"
  # The report double's actual corpus values stay independent of the requested
  # flags so tests can prove requested and actual are recorded separately.
  export MOCK_REPORT_STREAMS="${MOCK_REPORT_STREAMS:-api,checkout,search,worker}"
  export MOCK_REPORT_INSTANCES="${MOCK_REPORT_INSTANCES:-4}"
  export MOCK_REPORT_LABEL_SETS="${MOCK_REPORT_LABEL_SETS:-16}"
  export MOCK_REPORT_ACTIVE_SENDERS="${MOCK_REPORT_ACTIVE_SENDERS:-15}"
  # Legacy-mode flows need the double to answer parkana -h without runtime
  # flags; a test can force the modern help text to prove rejection.
  export MOCK_LEGACY_BINARY="${MOCK_LEGACY_BINARY:-${MOCK_LEGACY:-0}}"
  export MOCK_DOCKER_LOG MOCK_PARKBENCH_LOG MOCK_EVENT_LOG MOCK_LOKI_SCRAPE MOCK_CPU_STATE
  : >"$MOCK_DOCKER_LOG"
  : >"$MOCK_PARKBENCH_LOG"
  : >"$MOCK_EVENT_LOG"
  printf '0\n' >"$MOCK_LOKI_SCRAPE"
  DOCKER="$work/docker"
  PARKBENCH="$work/parkbench"
  prepare_mock_docker "$DOCKER" || return 1
  prepare_mock_parkbench "$PARKBENCH" || return 1

  http_status() { printf '200\n'; }
  http_post() { printf '204\n'; }
  http_query() {
    printf '{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000000,"10"]}]}}'
  }
  catalog_snapshot() { printf '%s\n' "$snapshot"; }
  garage_bucket_info() { printf '%s\n' "$garage_json"; }
  loki_local_snapshot() {
    LOKI_LOCAL_TOTAL=100
    LOKI_LOCAL_WAL=50
    LOKI_LOCAL_INDEX=30
    LOKI_LOCAL_INDEX_CACHE=10
    LOKI_LOCAL_COMPACTOR=5
    return 0
  }
  http_get() {
    local n
    case "$1" in
      "$LOKI_URL/metrics")
        n=$(cat "$MOCK_LOKI_SCRAPE")
        n=$((n + 1))
        printf '%s\n' "$n" >"$MOCK_LOKI_SCRAPE"
        if ((n == 1)); then
          printf 'loki_ingester_flush_queue_length 2\nloki_ingester_chunks_flushed_total{reason="forced"} 1\nloki_ingester_flush_failures_total 0\n'
        else
          printf 'loki_ingester_flush_queue_length 0\nloki_ingester_chunks_flushed_total{reason="forced"} 2\nloki_ingester_flush_failures_total 0\n'
        fi
        ;;
      "$PARKANA_URL/metrics")
        printf 'parkana_index_bytes_total{source="ingest"} 500\n'
        printf 'parkana_index_build_duration_seconds_count{source="ingest"} 1\n'
        printf 'parkana_index_build_duration_seconds_sum{source="ingest"} 0.5\n'
        printf 'parkana_ingest_pending_generations 0\n'
        ;;
      *) return 1 ;;
    esac
  }
  log() { printf '[bench-p3] %s\n' "$*" >>"$MOCK_EVENT_LOG"; printf '[bench-p3] %s\n' "$*"; }
  run_benchmark
}

test_a5_flag_and_cap_validation() (
  (init_defaults; validate_args; [[ $PARKANA_ONLY == 0 && $INDEX_MAX_GRAMS == 2097152 ]]) || return 1
  (init_defaults; parse_args --parkana-only; validate_args; [[ $PARKANA_ONLY == 1 ]]) || return 1
  (init_defaults; parse_args --index-max-grams 4; validate_args; [[ $INDEX_MAX_GRAMS == 4 ]]) || return 1
  (init_defaults; parse_args --index-max-grams 1; validate_args; [[ $INDEX_MAX_GRAMS == 1 ]]) || return 1
  (init_defaults; parse_args --index-max-grams 9223372036854775807; validate_args; [[ $INDEX_MAX_GRAMS == 9223372036854775807 ]]) || return 1
  local bad
  for bad in 0 -1 08 abc "" 1e3 9223372036854775808 18446744073709551616; do
    if (init_defaults; parse_args --index-max-grams "$bad"; validate_args) >/dev/null 2>&1; then
      printf 'accepted invalid --index-max-grams %q\n' "$bad" >&2
      return 1
    fi
  done
  (init_defaults; parse_args --index-max-grams) >/dev/null 2>&1 && return 1
  return 0
)

test_a5_env_and_compose_propagation() (
  local work mock log found=0 v
  work=$(mktemp -d) || return 1
  SECRETS_FILE="$work/secrets.env"
  INDEX_MAX_GRAMS=65536
  CPUS=4
  MEMORY=8g
  PARKANA_PORT=13100
  GARAGE_PORT=13900
  LOKI_PORT=13101
  generate_credentials
  write_env_file || { rm -rf "$work"; return 1; }
  [[ $(stat -c '%a' "$SECRETS_FILE") == 600 ]] || { rm -rf "$work"; return 1; }
  require_grep "$SECRETS_FILE" 'PARKANA_BENCH_INDEX_MAX_GRAMS=65536' || { rm -rf "$work"; return 1; }
  require_grep "$COMPOSE_FILE" '${PARKANA_BENCH_INDEX_MAX_GRAMS:-2097152}' || { rm -rf "$work"; return 1; }
  for v in "${COMPOSE_ENV_VARS[@]}"; do
    [[ $v == PARKANA_BENCH_INDEX_MAX_GRAMS ]] && found=1
  done
  ((found)) || { rm -rf "$work"; return 1; }

  # A caller-exported value must never override the owned env file.
  mock="$work/docker"
  log="$work/argv"
  cat >"$mock" <<'MOCK'
#!/bin/sh
printf 'CAP=%s\n' "${PARKANA_BENCH_INDEX_MAX_GRAMS-unset}" >>"${MOCK_ARGV_LOG:?}"
printf '%s\n' "$*" >>"${MOCK_ARGV_LOG:?}"
exit 0
MOCK
  chmod 0755 "$mock"
  export MOCK_ARGV_LOG=$log
  export PARKANA_BENCH_INDEX_MAX_GRAMS=999999
  DOCKER=$mock
  PROJECT=selftest-project
  if ! compose up -d parkana; then
    unset PARKANA_BENCH_INDEX_MAX_GRAMS
    rm -rf "$work"
    return 1
  fi
  unset PARKANA_BENCH_INDEX_MAX_GRAMS
  require_grep "$log" 'CAP=unset' || { rm -rf "$work"; return 1; }
  require_grep "$log" "--env-file $SECRETS_FILE" || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_a5_dry_run_reports_controls() (
  local work po dual
  work=$(mktemp -d) || return 1
  po=$(cd "$REPO_ROOT" && bash "$SCRIPT_PATH" --dry-run --parkana-only --index-max-grams 65536 --instances-per-service 1 --output-dir "$work/po" 2>&1) ||
    { rm -rf "$work"; return 1; }
  [[ $po == *"backends        : parkana only"* ]] || { rm -rf "$work"; return 1; }
  [[ $po == *"max_grams=65536"* ]] || { rm -rf "$work"; return 1; }
  [[ $po == *"batch=1250 entries/request/label set"* ]] || { rm -rf "$work"; return 1; }
  [[ $po == *"requested=1 (source=explicit)"* ]] || { rm -rf "$work"; return 1; }
  [[ $po == *"loki=skipped"* ]] || { rm -rf "$work"; return 1; }
  [[ $po != *"GK"* ]] || { rm -rf "$work"; return 1; }
  [[ $po != *"PARKANA_BENCH_PARKANA_SECRET_KEY="* ]] || { rm -rf "$work"; return 1; }
  [[ ! -e $work/po ]] || { rm -rf "$work"; return 1; }
  dual=$(cd "$REPO_ROOT" && bash "$SCRIPT_PATH" --dry-run --output-dir "$work/dual" 2>&1) ||
    { rm -rf "$work"; return 1; }
  [[ $dual == *"backends        : parkana then loki"* ]] || { rm -rf "$work"; return 1; }
  [[ $dual == *"max_grams=2097152"* ]] || { rm -rf "$work"; return 1; }
  [[ $dual == *"batch=1250 entries/request/label set"* ]] || { rm -rf "$work"; return 1; }
  [[ $dual == *"requested=tool-default (source=tool-default)"* ]] || { rm -rf "$work"; return 1; }
  [[ $dual == *"parkana=13100 garage=13900 loki=13101"* ]] || { rm -rf "$work"; return 1; }
  [[ ! -e $work/dual ]] || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_a5_parkana_only_ports_and_init() (
  local work mock log
  work=$(mktemp -d) || return 1
  # The unused Loki port may collide (it is never probed or published) but it
  # must still be format-valid because Compose interpolates every service.
  (init_defaults; parse_args --parkana-only --loki-port 13100; validate_args; [[ $PARKANA_ONLY == 1 ]]) || return 1
  (init_defaults; parse_args --parkana-only --loki-port abc; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; parse_args --parkana-only --loki-port 0; validate_args) >/dev/null 2>&1 && return 1
  log="$work/probes"
  : >"$log"
  init_defaults
  PARKANA_ONLY=1
  PARKANA_PORT=13100
  GARAGE_PORT=13900
  LOKI_PORT=13101
  port_free() { printf '%s\n' "$1" >>"$log"; return 0; }
  check_ports || { rm -rf "$work"; return 1; }
  [[ $(wc -l <"$log") == 2 ]] || { rm -rf "$work"; return 1; }
  require_grep "$log" '13100' || { rm -rf "$work"; return 1; }
  require_grep "$log" '13900' || { rm -rf "$work"; return 1; }
  require_no_grep "$log" '13101' || { rm -rf "$work"; return 1; }
  : >"$log"
  PARKANA_ONLY=0
  check_ports || { rm -rf "$work"; return 1; }
  require_grep "$log" '13101' || { rm -rf "$work"; return 1; }

  mock="$work/docker"
  log="$work/argv"
  cat >"$mock" <<'MOCK'
#!/bin/sh
printf '%s\n' "$*" >>"${MOCK_ARGV_LOG:?}"
if [ "$1" = "inspect" ]; then printf 'exited 0\n'; exit 0; fi
case "$*" in
  *" ps -a -q "*) printf 'container-id\n'; exit 0 ;;
esac
exit 0
MOCK
  chmod 0755 "$mock"
  export MOCK_ARGV_LOG=$log
  : >"$log"
  DOCKER=$mock
  PROJECT=selftest-project
  SECRETS_FILE="$work/secrets.env"
  RUN_LOG="$work/run.log"
  TEST_NO_SLEEP=1
  INIT_TIMEOUT=60
  PARKANA_ONLY=1
  start_garage_stack || { rm -rf "$work"; return 1; }
  require_grep "$log" 'up -d garage garage-init-parkana' || { rm -rf "$work"; return 1; }
  require_no_grep "$log" 'garage-init-loki' || { rm -rf "$work"; return 1; }
  : >"$log"
  PARKANA_ONLY=0
  start_garage_stack || { rm -rf "$work"; return 1; }
  require_grep "$log" 'garage-init-loki' || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_a5_sidecar_publication_not_relaxed() (
  local work
  work=$(mktemp -d) || return 1
  # A cap-1 experiment can legitimately produce no sidecars; --skip-queries is
  # the existing escape hatch for that smoke and must still work.
  if ! (MOCK_SKIP_QUERIES=1 MOCK_CATALOG_SNAPSHOT='10|1000|1|0' \
    MOCK_GARAGE_INFO='{"bytes":1000,"objects":1}' \
    run_mock_benchmark parkana-only 1 "$work/smoke") >/dev/null 2>&1; then
    printf 'skip-queries no-sidecar smoke failed\n' >&2
    rm -rf "$work"
    return 1
  fi
  require_grep "$work/smoke/out/storage-parkana.txt" 'sidecar_objects=0' || { rm -rf "$work"; return 1; }
  # The same no-sidecar result with the query/publication phase enabled must
  # still fail closed: --index-max-grams does not weaken the check.
  if (MOCK_CATALOG_SNAPSHOT='10|1000|1|0' MOCK_GARAGE_INFO='{"bytes":1000,"objects":1}' \
    run_mock_benchmark parkana-only 1 "$work/publish") >/dev/null 2>&1; then
    printf 'publication check was relaxed for a no-sidecar catalog\n' >&2
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
)

test_a5_mainflow_parkana_only() (
  local work f
  work=$(mktemp -d) || return 1
  if ! (run_mock_benchmark parkana-only 777 "$work") >/dev/null 2>&1; then
    printf 'mock parkana-only benchmark flow failed\n' >&2
    rm -rf "$work"
    return 1
  fi
  for f in run-metadata.txt timings.tsv summary.md storage-parkana.txt \
    index-build-parkana.txt ingest-parkana.json query-parkana.json query-parkana.md skipped.txt; do
    require_file "$work/out/$f" || { rm -rf "$work"; return 1; }
  done
  for f in ingest-loki.json storage-loki.txt storage-loki-post-query.txt \
    query-loki.json query-loki.md query-loki.txt \
    loki-compactor-before-query.txt loki-compactor-after-query.txt; do
    require_absent "$work/out/$f" || { rm -rf "$work"; return 1; }
  done
  require_grep "$work/out/run-metadata.txt" 'parkana_only=1' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'index_max_grams=777' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'batch_lines=1250' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'batch_lines_unit=entries per push request' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'batch_lines_scope=per label set' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'instances_per_service_requested=tool-default' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'instances_per_service_source=tool-default' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'streams_requested=tool-default' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'streams_source=tool-default' ||
    { rm -rf "$work"; return 1; }
  require_no_grep "$work/parkbench.argv" '-streams' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_corpus_streams=api,checkout,search,worker' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_corpus_stream_count=4' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_corpus_instances_per_service=4' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_parallelism_label_sets=16' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_parallelism_active_senders=15' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_parallelism_concurrency_limit=16' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_parallelism_peak_in_flight_pushes=12' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_parallelism=complete' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_cpu_delta_usage_usec=1000000' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_cpu_source=cgroup-v2-cpu.stat' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'actual_start=2026-01-01T00:00:00Z' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'start_source=derived-by-tool' ||
    { rm -rf "$work"; return 1; }
  require_regex "$work/out/run-metadata.txt" '^driver_script_sha256=[0-9a-f]{64}$' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'app_source_commit=' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_runtime_flag_support=present' ||
    { rm -rf "$work"; return 1; }
  require_regex "$work/out/run-metadata.txt" '^parkana_runtime_probe_sha256=[0-9a-f]{64}$' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'runtime flag probe: support=present' ||
    { rm -rf "$work"; return 1; }
  require_no_grep "$work/out/runtime-parkana.txt" 'Usage of parkana' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/runtime-parkana.txt" 'ingest_memory_peak_bytes=3221225472' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_ingest_memory_peak_bytes=3221225472' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_effective_memory_bytes=8589934592' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'ingest memory peak' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" '| parkana | 1000000 | 2000000 | 1000000 |' ||
    { rm -rf "$work"; return 1; }
  require_regex "$work/out/run-metadata.txt" '^ingest_parkana_batching_.*entries_per_request=1250$' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_batching_attempted_entries=9990' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_batching=complete' ||
    { rm -rf "$work"; return 1; }
  require_no_regex "$work/out/run-metadata.txt" '^ingest_.*batches' || { rm -rf "$work"; return 1; }
  require_no_regex "$work/out/run-metadata.txt" '^ingest_.*=(999|777)$' || { rm -rf "$work"; return 1; }
  require_no_regex "$work/out/run-metadata.txt" '^ingest_loki_' || { rm -rf "$work"; return 1; }
  require_no_grep "$work/out/run-metadata.txt" 'loki_url=' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/skipped.txt" 'loki_backend=skipped' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/timings.tsv" '# loki skipped: --parkana-only' || { rm -rf "$work"; return 1; }
  require_no_regex "$work/out/timings.tsv" '^loki[[:space:]]' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'Loki explicitly skipped' || { rm -rf "$work"; return 1; }
  require_no_regex "$work/out/summary.md" '^\| loki \|' || { rm -rf "$work"; return 1; }
  require_grep "$work/docker.argv" 'garage-init-parkana' || { rm -rf "$work"; return 1; }
  require_no_grep "$work/docker.argv" 'garage-init-loki' || { rm -rf "$work"; return 1; }
  require_no_grep "$work/docker.argv" 'up -d loki' || { rm -rf "$work"; return 1; }
  require_no_grep "$work/docker.argv" 'stop --timeout 30 loki' || { rm -rf "$work"; return 1; }
  require_no_grep "$work/docker.argv" 'bench-tools-loki' || { rm -rf "$work"; return 1; }
  # Parkana pushes to its Loki-compatible /loki/api/v1/push path; the selected
  # backend and Loki endpoint must be absent, not that protocol pathname.
  require_no_grep "$work/parkbench.argv" '--backend loki' || { rm -rf "$work"; return 1; }
  require_no_grep "$work/parkbench.argv" 'ingest-loki.json' || { rm -rf "$work"; return 1; }
  require_no_grep "$work/parkbench.argv" 'http://127.0.0.1:13101' || { rm -rf "$work"; return 1; }
  require_no_regex "$work/events" 'loki' || { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'Parkana exact visibility' || { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'ingesting 2000000000 raw line bytes into parkana' || { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'verifying parkana data after restart' || { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'parkana post-restart count verified' || { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'running query workload against parkana' || { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'Parkana storage:' || { rm -rf "$work"; return 1; }
  require_no_grep "$work/events" 'corpus identity verified' || { rm -rf "$work"; return 1; }
  OUTPUT_DIR="$work/out"
  RUN_LOG="$work/out/run.log"
  emit_archive >"$work/archive.txt" || { rm -rf "$work"; return 1; }
  awk '/=== BENCH ARTIFACT ARCHIVE BEGIN/{f=1; next} /=== BENCH ARTIFACT ARCHIVE END ===/{f=0} f' "$work/archive.txt" |
    base64 -d | tar -tzf - >"$work/archive-list" || { rm -rf "$work"; return 1; }
  require_grep "$work/archive-list" 'skipped.txt' || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_a5_mainflow_default_dual() (
  local work f
  work=$(mktemp -d) || return 1
  if ! (run_mock_benchmark dual 2097152 "$work") >/dev/null 2>&1; then
    printf 'mock default benchmark flow failed\n' >&2
    rm -rf "$work"
    return 1
  fi
  for f in ingest-parkana.json ingest-loki.json storage-parkana.txt storage-loki.txt \
    storage-loki-post-query.txt query-parkana.json query-loki.json query-parkana.md query-loki.md \
    loki-compactor-before-query.txt loki-compactor-after-query.txt \
    run-metadata.txt timings.tsv summary.md; do
    require_file "$work/out/$f" || { rm -rf "$work"; return 1; }
  done
  require_absent "$work/out/skipped.txt" || { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_only=0' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'index_max_grams=2097152' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'batch_lines=1250' || { rm -rf "$work"; return 1; }
  require_regex "$work/out/run-metadata.txt" '^ingest_parkana_batching_.*entries_per_request=1250$' ||
    { rm -rf "$work"; return 1; }
  require_regex "$work/out/run-metadata.txt" '^ingest_loki_batching_.*entries_per_request=1250$' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_batching_attempted_entries=9990' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_loki_batching_attempted_entries=9990' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_batching=complete' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_loki_batching=complete' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'instances_per_service_requested=tool-default' ||
    { rm -rf "$work"; return 1; }
  for backend in parkana loki; do
    require_grep "$work/out/run-metadata.txt" "ingest_${backend}_corpus_instances_per_service=4" ||
      { rm -rf "$work"; return 1; }
    require_grep "$work/out/run-metadata.txt" "ingest_${backend}_parallelism_label_sets=16" ||
      { rm -rf "$work"; return 1; }
    require_grep "$work/out/run-metadata.txt" "ingest_${backend}_parallelism_active_senders=15" ||
      { rm -rf "$work"; return 1; }
    require_grep "$work/out/run-metadata.txt" "ingest_${backend}_parallelism_peak_in_flight_pushes=12" ||
      { rm -rf "$work"; return 1; }
    require_grep "$work/out/run-metadata.txt" "ingest_${backend}_parallelism=complete" ||
      { rm -rf "$work"; return 1; }
  done
  require_no_regex "$work/out/run-metadata.txt" '^ingest_.*batches' || { rm -rf "$work"; return 1; }
  require_no_regex "$work/out/run-metadata.txt" '^ingest_.*=(999|777)$' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'loki_url=http://127.0.0.1:13101' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_loki_cpu_delta_usage_usec=1000000' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" '| loki | 3000000 | 4000000 | 1000000 |' ||
    { rm -rf "$work"; return 1; }
  require_regex "$work/out/timings.tsv" '^loki[[:space:]]' || { rm -rf "$work"; return 1; }
  require_grep "$work/docker.argv" 'garage-init-loki' || { rm -rf "$work"; return 1; }
  require_grep "$work/docker.argv" 'up -d loki' || { rm -rf "$work"; return 1; }
  require_grep "$work/docker.argv" 'stop --timeout 30 loki' || { rm -rf "$work"; return 1; }
  require_grep "$work/parkbench.argv" '--backend loki' || { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'corpus identity verified' || { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'Loki exact visibility' || { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'running query workload against loki' || { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'Parkana exact visibility' || { rm -rf "$work"; return 1; }
  OUTPUT_DIR="$work/out"
  RUN_LOG="$work/out/run.log"
  emit_archive >"$work/archive.txt" || { rm -rf "$work"; return 1; }
  awk '/=== BENCH ARTIFACT ARCHIVE BEGIN/{f=1; next} /=== BENCH ARTIFACT ARCHIVE END ===/{f=0} f' "$work/archive.txt" |
    base64 -d | tar -tzf - >"$work/archive-list" || { rm -rf "$work"; return 1; }
  require_no_grep "$work/archive-list" 'skipped.txt' || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_instances_explicit_metadata() (
  local work
  work=$(mktemp -d) || return 1
  if ! (MOCK_INSTANCES_PER_SERVICE=2 run_mock_benchmark parkana-only 2097152 "$work") >/dev/null 2>&1; then
    printf 'mock parkana-only flow with explicit instances failed\n' >&2
    rm -rf "$work"
    return 1
  fi
  require_grep "$work/out/run-metadata.txt" 'instances_per_service_requested=2' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'instances_per_service_source=explicit' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/parkbench.argv" '-instances-per-service 2' || { rm -rf "$work"; return 1; }
  # The report's actual corpus identity wins over the requested flag.
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_corpus_instances_per_service=4' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'requested 2 (source=explicit)' ||
    { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_streams_explicit_metadata() (
  local work
  work=$(mktemp -d) || return 1
  if ! (MOCK_STREAMS=api MOCK_INSTANCES_PER_SERVICE=16 \
    MOCK_REPORT_STREAMS=api MOCK_REPORT_INSTANCES=16 \
    MOCK_REPORT_LABEL_SETS=16 MOCK_REPORT_ACTIVE_SENDERS=16 \
    run_mock_benchmark parkana-only 2097152 "$work") >/dev/null 2>&1; then
    printf 'mock single-stream benchmark flow failed\n' >&2
    rm -rf "$work"
    return 1
  fi
  # Requested values are explicit and distinct from the measured report.
  require_grep "$work/out/run-metadata.txt" 'streams_requested=api' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'streams_source=explicit' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/parkbench.argv" '-streams api' || { rm -rf "$work"; return 1; }
  require_grep "$work/parkbench.argv" '-instances-per-service 16' || { rm -rf "$work"; return 1; }
  require_grep "$work/parkbench.argv" '-concurrency 16' || { rm -rf "$work"; return 1; }
  # Actual corpus evidence: one stream still carries 16 label sets/senders.
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_corpus_streams=api' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_corpus_stream_count=1' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_corpus_instances_per_service=16' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_parallelism_label_sets=16' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_parallelism_active_senders=16' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'streams: requested api (source=explicit)' ||
    { rm -rf "$work"; return 1; }
  # Count, restart and query checks derive from the manifest, not --streams.
  require_grep "$work/events" 'Parkana exact visibility' || { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'parkana post-restart count verified' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/query-parkana.json" '"valid":true' || { rm -rf "$work"; return 1; }
  require_regex "$work/parkbench.argv" '^query --backend parkana .*--manifest' ||
    { rm -rf "$work"; return 1; }
  require_no_regex "$work/parkbench.argv" '^query .*--streams' || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_streams_requested_vs_actual() (
  local work
  work=$(mktemp -d) || return 1
  # The request names one stream but the double's report still describes the
  # four-stream corpus: metadata must publish both, never the request as actual.
  if ! (MOCK_STREAMS=api run_mock_benchmark parkana-only 2097152 "$work") >/dev/null 2>&1; then
    printf 'mock requested-vs-actual stream flow failed\n' >&2
    rm -rf "$work"
    return 1
  fi
  require_grep "$work/out/run-metadata.txt" 'streams_requested=api' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_corpus_streams=api,checkout,search,worker' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_corpus_stream_count=4' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/parkbench.argv" '-streams api' || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_reuse_manifest_flow() (
  local work manifest
  work=$(mktemp -d) || return 1
  manifest="$work/reuse.json"
  cat >"$manifest" <<'JSON'
{
  "corpus": {
    "start": "2025-12-31T23:00:00.000000Z",
    "through": "2025-12-31T23:59:59.000000Z"
  }
}
JSON
  if ! (MOCK_REUSE_MANIFEST="$manifest" MOCK_WAL_SYNC=interval \
    MOCK_WAL_SYNC_INTERVAL=250ms run_mock_benchmark parkana-only 2097152 "$work") >/dev/null 2>&1; then
    printf 'mock reuse-manifest flow failed\n' >&2
    rm -rf "$work"
    return 1
  fi
  require_grep "$work/out/run-metadata.txt" 'start_source=reused-manifest' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" "reuse_manifest=$manifest" ||
    { rm -rf "$work"; return 1; }
  require_regex "$work/out/run-metadata.txt" '^reuse_manifest_sha256=[0-9a-f]{64}$' ||
    { rm -rf "$work"; return 1; }
  # Actual manifest interval is prominent and distinct from the reused start.
  require_grep "$work/out/run-metadata.txt" 'actual_start=2026-01-01T00:00:00Z' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'actual_through=2026-01-01T00:00:10Z' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_actual_start=2026-01-01T00:00:00Z' ||
    { rm -rf "$work"; return 1; }
  require_regex "$work/out/run-metadata.txt" '^replay_command=bash scripts/bench-phase3.sh --start 2026-01-01T00:00:00Z' ||
    { rm -rf "$work"; return 1; }
  # The replay pins the actual manifest streams/instances, includes the selected
  # interval, and uses a runnable output placeholder, never a redirection token.
  require_grep "$work/out/run-metadata.txt" '--streams api,checkout,search,worker' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" '--instances-per-service 4' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" '--wal-sync interval --wal-sync-interval 250ms' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" '--output-dir /tmp/parkana-bench-replay-UNIQUE' ||
    { rm -rf "$work"; return 1; }
  require_no_grep "$work/out/run-metadata.txt" '<fresh-output-dir>' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'replay_note=replace UNIQUE with a fresh output name before running' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/parkbench.argv" '-start 2025-12-31T23:00:00.000000Z' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'corpus actual start=2026-01-01T00:00:00Z' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'replay: bash scripts/bench-phase3.sh' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'actual corpus interval' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'replay:' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'replace UNIQUE with a fresh output name' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'replay note: replace UNIQUE with a fresh output name' ||
    { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_legacy_runtime_flow() (
  local work
  work=$(mktemp -d) || return 1
  if ! (MOCK_LEGACY=1 run_mock_benchmark parkana-only 2097152 "$work") >/dev/null 2>&1; then
    printf 'mock legacy-runtime flow failed\n' >&2
    rm -rf "$work"
    return 1
  fi
  require_grep "$work/out/run-metadata.txt" 'parkana_runtime_mode=legacy-foundation' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_runtime_support=absent' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_runtime_source=legacy-source-contract' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_legacy_known_wal_sync=always' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_legacy_known_flush_concurrency=1' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_legacy_known_upload_concurrency=1' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_runtime_wal_sync_source=legacy-known' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_effective_wal_sync=unavailable' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_runtime_flag_support=absent' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_runtime_probe_source=bounded-parkana-help' ||
    { rm -rf "$work"; return 1; }
  require_regex "$work/out/run-metadata.txt" '^parkana_runtime_probe_sha256=[0-9a-f]{64}$' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'runtime_overlay_attached=0' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/runtime-parkana.txt" 'source=legacy-source-contract' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/runtime-parkana.txt" 'runtime_support=absent' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/runtime-parkana.txt" 'known_wal_sync=always' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/runtime-parkana.txt" 'runtime_flag_support=absent' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'legacy source contract' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'runtime flag probe: support=absent' ||
    { rm -rf "$work"; return 1; }
  require_no_grep "$work/docker.argv" 'ingest-runtime.yaml' || { rm -rf "$work"; return 1; }
  # CPU and memory capture still run for the legacy arm.
  require_grep "$work/out/run-metadata.txt" 'ingest_parkana_cpu_delta_usage_usec=1000000' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/run-metadata.txt" 'parkana_ingest_memory_peak_bytes=3221225472' ||
    { rm -rf "$work"; return 1; }
  # Raw help is never printed or archived.
  for f in run-metadata.txt runtime-parkana.txt summary.md run.log; do
    require_no_grep "$work/out/$f" 'Usage of parkana' || { rm -rf "$work"; return 1; }
  done
  rm -rf "$work"
)

test_legacy_runtime_probe_rejects_modern() (
  local work out scenario legacy binary
  for scenario in "1 0" "1 partial" "0 partial"; do
    read -r legacy binary <<<"$scenario"
    work=$(mktemp -d) || return 1
    if out=$(MOCK_LEGACY="$legacy" MOCK_LEGACY_BINARY="$binary" \
      run_mock_benchmark parkana-only 2097152 "$work" 2>&1); then
      printf 'runtime probe accepted mismatch scenario %q\n' "$scenario" >&2
      rm -rf "$work"
      return 1
    fi
    [[ $out == *"runtime probe mismatch"* ]] || { rm -rf "$work"; return 1; }
    [[ $out != *"Usage of parkana"* ]] || { rm -rf "$work"; return 1; }
    rm -rf "$work"
  done
)

test_a5_mainflow_parkana_path_unchanged() (
  local work filter a b
  work=$(mktemp -d) || return 1
  if ! (run_mock_benchmark parkana-only 2097152 "$work/po") >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  if ! (run_mock_benchmark dual 2097152 "$work/dual") >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  filter='^\[bench-p3\] (starting parkana|stopping parkana|ingesting [0-9]+ raw line bytes into parkana|Parkana |verifying parkana|running query workload against parkana)'
  a="$work/a"
  b="$work/b"
  grep -E "$filter" "$work/po/events" >"$a" || true
  grep -E "$filter" "$work/dual/events" >"$b" || true
  [[ -s $a ]] || { rm -rf "$work"; return 1; }
  diff -u "$a" "$b" || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_a6_runtime_flag_validation() (
  (init_defaults; validate_args; [[ $WAL_SYNC == always && $WAL_SYNC_INTERVAL == 1s &&
    $FLUSH_CONCURRENCY == 4 && $UPLOAD_CONCURRENCY == 4 ]]) || return 1
  (init_defaults; parse_args --wal-sync interval; validate_args; [[ $WAL_SYNC == interval ]]) || return 1
  (init_defaults; parse_args --wal-sync always; validate_args; [[ $WAL_SYNC == always ]]) || return 1
  (init_defaults; parse_args --wal-sync-interval 250ms; validate_args; [[ $WAL_SYNC_INTERVAL == 250ms ]]) || return 1
  (init_defaults; parse_args --wal-sync-interval 1h30m; validate_args; [[ $WAL_SYNC_INTERVAL == 1h30m ]]) || return 1
  for v in 0.5us 1.5us 0.0000001s 9223372036854775807ns; do
    (init_defaults; parse_args --wal-sync-interval "$v"; validate_args;
      [[ $WAL_SYNC_INTERVAL == "$v" ]]) || return 1
  done
  for n in 1 4 64; do
    (init_defaults; parse_args --flush-concurrency "$n" --upload-concurrency "$n"; validate_args;
      [[ $FLUSH_CONCURRENCY == "$n" && $UPLOAD_CONCURRENCY == "$n" ]]) || return 1
  done
  local bad flag
  for bad in sometimes Always ALWAYS ""; do
    (init_defaults; parse_args --wal-sync "$bad"; validate_args) >/dev/null 2>&1 && return 1
  done
  for bad in 0 0s 00s 0.000s -1s 1 1e3s 1.5 1S "1s " "" \
    0.1ns 1.5ns 0.0005us 0.0000000001s 999999999999h 9223372036854775808ns; do
    (init_defaults; parse_args --wal-sync-interval "$bad"; validate_args) >/dev/null 2>&1 && return 1
  done
  for flag in --flush-concurrency --upload-concurrency; do
    for bad in 0 -1 65 1000 abc 1.5 08 ""; do
      (init_defaults; parse_args "$flag" "$bad"; validate_args) >/dev/null 2>&1 && return 1
    done
  done
  for flag in --wal-sync --wal-sync-interval --flush-concurrency --upload-concurrency; do
    (init_defaults; parse_args "$flag") >/dev/null 2>&1 && return 1
  done
  return 0
)

test_a6_env_and_compose_propagation() (
  local work mock log v found
  work=$(mktemp -d) || return 1
  SECRETS_FILE="$work/secrets.env"
  WAL_SYNC=interval
  WAL_SYNC_INTERVAL=250ms
  FLUSH_CONCURRENCY=2
  UPLOAD_CONCURRENCY=8
  CPUS=4
  MEMORY=8g
  PARKANA_PORT=13100
  GARAGE_PORT=13900
  LOKI_PORT=13101
  generate_credentials
  write_env_file || { rm -rf "$work"; return 1; }
  [[ $(stat -c '%a' "$SECRETS_FILE") == 600 ]] || { rm -rf "$work"; return 1; }
  for v in \
    'PARKANA_BENCH_WAL_SYNC=interval' \
    'PARKANA_BENCH_WAL_SYNC_INTERVAL=250ms' \
    'PARKANA_BENCH_FLUSH_CONCURRENCY=2' \
    'PARKANA_BENCH_UPLOAD_CONCURRENCY=8'; do
    require_grep "$SECRETS_FILE" "$v" || { rm -rf "$work"; return 1; }
  done
  for v in PARKANA_BENCH_WAL_SYNC PARKANA_BENCH_WAL_SYNC_INTERVAL \
    PARKANA_BENCH_FLUSH_CONCURRENCY PARKANA_BENCH_UPLOAD_CONCURRENCY; do
    found=0
    for w in "${COMPOSE_ENV_VARS[@]}"; do [[ $w == "$v" ]] && found=1; done
    ((found)) || { rm -rf "$work"; return 1; }
  done
  require_grep "$RUNTIME_OVERLAY" '${PARKANA_BENCH_WAL_SYNC:-always}' || { rm -rf "$work"; return 1; }
  require_grep "$RUNTIME_OVERLAY" '${PARKANA_BENCH_WAL_SYNC_INTERVAL:-1s}' || { rm -rf "$work"; return 1; }
  require_grep "$RUNTIME_OVERLAY" '${PARKANA_BENCH_FLUSH_CONCURRENCY:-4}' || { rm -rf "$work"; return 1; }
  require_grep "$RUNTIME_OVERLAY" '${PARKANA_BENCH_UPLOAD_CONCURRENCY:-4}' || { rm -rf "$work"; return 1; }

  # A caller export must never beat the owned env file.
  mock="$work/docker"
  log="$work/argv"
  cat >"$mock" <<'MOCK'
#!/bin/sh
printf 'WAL=%s\n' "${PARKANA_BENCH_WAL_SYNC-unset}" >>"${MOCK_ARGV_LOG:?}"
printf 'INTERVAL=%s\n' "${PARKANA_BENCH_WAL_SYNC_INTERVAL-unset}" >>"${MOCK_ARGV_LOG:?}"
printf 'FLUSH=%s\n' "${PARKANA_BENCH_FLUSH_CONCURRENCY-unset}" >>"${MOCK_ARGV_LOG:?}"
printf 'UPLOAD=%s\n' "${PARKANA_BENCH_UPLOAD_CONCURRENCY-unset}" >>"${MOCK_ARGV_LOG:?}"
printf '%s\n' "$*" >>"${MOCK_ARGV_LOG:?}"
exit 0
MOCK
  chmod 0755 "$mock"
  export MOCK_ARGV_LOG=$log
  export PARKANA_BENCH_WAL_SYNC=interval
  export PARKANA_BENCH_WAL_SYNC_INTERVAL=9s
  export PARKANA_BENCH_FLUSH_CONCURRENCY=32
  export PARKANA_BENCH_UPLOAD_CONCURRENCY=32
  DOCKER=$mock
  PROJECT=selftest-project
  if ! compose up -d parkana; then
    unset PARKANA_BENCH_WAL_SYNC PARKANA_BENCH_WAL_SYNC_INTERVAL \
      PARKANA_BENCH_FLUSH_CONCURRENCY PARKANA_BENCH_UPLOAD_CONCURRENCY
    rm -rf "$work"
    return 1
  fi
  unset PARKANA_BENCH_WAL_SYNC PARKANA_BENCH_WAL_SYNC_INTERVAL \
    PARKANA_BENCH_FLUSH_CONCURRENCY PARKANA_BENCH_UPLOAD_CONCURRENCY
  for v in 'WAL=unset' 'INTERVAL=unset' 'FLUSH=unset' 'UPLOAD=unset'; do
    require_grep "$log" "$v" || { rm -rf "$work"; return 1; }
  done
  require_grep "$log" "-f $COMPOSE_FILE -f $RUNTIME_OVERLAY" || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_a6_effective_capture() (
  local work
  work=$(mktemp -d) || return 1
  mkdir -p "$work/out" || { rm -rf "$work"; return 1; }
  OUTPUT_DIR="$work/out"
  WAL_SYNC=interval
  WAL_SYNC_INTERVAL=250ms
  FLUSH_CONCURRENCY=2
  UPLOAD_CONCURRENCY=8
  DOCKER="$work/docker"
  PROJECT=selftest-project
  prepare_mock_docker "$DOCKER" || { rm -rf "$work"; return 1; }
  export MOCK_DOCKER_LOG="$work/docker.argv"
  export MOCK_EFFECTIVE_WAL_SYNC=interval
  export MOCK_EFFECTIVE_WAL_SYNC_INTERVAL=250ms
  export MOCK_EFFECTIVE_FLUSH_CONCURRENCY=2
  export MOCK_EFFECTIVE_UPLOAD_CONCURRENCY=8
  export MOCK_EFFECTIVE_NANO_CPUS=2000000000
  export MOCK_EFFECTIVE_MEMORY_BYTES=4294967296
  export MOCK_EFFECTIVE_IMAGE=sha256:mock-parkana
  capture_parkana_runtime || { rm -rf "$work"; return 1; }
  [[ $EFFECTIVE_WAL_SYNC == interval && $EFFECTIVE_WAL_SYNC_INTERVAL == 250ms &&
    $EFFECTIVE_FLUSH_CONCURRENCY == 2 && $EFFECTIVE_UPLOAD_CONCURRENCY == 8 ]] ||
    { rm -rf "$work"; return 1; }
  [[ $EFFECTIVE_NANO_CPUS == 2000000000 && $EFFECTIVE_CPUS == 2.000 &&
    $EFFECTIVE_MEMORY_BYTES == 4294967296 &&
    $EFFECTIVE_IMAGE == sha256:mock-parkana ]] || { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/runtime-parkana.txt" 'effective_wal_sync=interval' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/runtime-parkana.txt" 'source=container-inspect' ||
    { rm -rf "$work"; return 1; }
  printf 'tool=test\n' >"$OUTPUT_DIR/run-metadata.txt"
  append_runtime_evidence || { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'tool=test' || { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'parkana_effective_flush_concurrency=2' ||
    { rm -rf "$work"; return 1; }
  # A container that reports a different value must fail, not be recorded.
  export MOCK_EFFECTIVE_WAL_SYNC=always
  if (capture_parkana_runtime) >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  export MOCK_EFFECTIVE_WAL_SYNC=
  if (capture_parkana_runtime) >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  unset MOCK_EFFECTIVE_WAL_SYNC MOCK_EFFECTIVE_WAL_SYNC_INTERVAL \
    MOCK_EFFECTIVE_FLUSH_CONCURRENCY MOCK_EFFECTIVE_UPLOAD_CONCURRENCY \
    MOCK_EFFECTIVE_NANO_CPUS MOCK_EFFECTIVE_MEMORY_BYTES MOCK_EFFECTIVE_IMAGE
  rm -rf "$work"
)

test_cgroup_snapshot_capture() (
  local work
  work=$(mktemp -d) || return 1
  mkdir -p "$work/out" || { rm -rf "$work"; return 1; }
  OUTPUT_DIR="$work/out"
  DOCKER="$work/docker"
  PROJECT=selftest-project
  SECRETS_FILE="$work/secrets.env"
  : >"$SECRETS_FILE"
  printf 'tool=test\n' >"$OUTPUT_DIR/run-metadata.txt"
  MOCK_CPU_STATE="$work/cpu-state"
  printf '0\n' >"$MOCK_CPU_STATE"
  prepare_mock_docker "$DOCKER" || { rm -rf "$work"; return 1; }
  export MOCK_DOCKER_LOG="$work/docker.argv" MOCK_CPU_STATE

  # Two bounded cpu.stat reads produce a monotonic container-level delta.
  capture_cpu_snapshot_before parkana || { rm -rf "$work"; return 1; }
  capture_cpu_snapshot_after parkana || { rm -rf "$work"; return 1; }
  [[ $(get_cpu_field parkana BEFORE_USEC) == 1000000 &&
    $(get_cpu_field parkana AFTER_USEC) == 2000000 &&
    $(get_cpu_field parkana DELTA_USEC) == 1000000 &&
    $(get_cpu_field parkana SOURCE) == cgroup-v2-cpu.stat ]] ||
    { rm -rf "$work"; return 1; }
  [[ $(get_cpu_field parkana BEFORE_WALL_S) =~ ^[0-9]+\.[0-9]+$ &&
    $(get_cpu_field parkana AFTER_WALL_S) =~ ^[0-9]+\.[0-9]+$ &&
    $(get_cpu_field parkana WALL_S) =~ ^[0-9]+\.[0-9]+$ ]] ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/runtime-parkana.txt" 'ingest_cpu_before_usage_usec=1000000' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/runtime-parkana.txt" 'ingest_cpu_after_usage_usec=2000000' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/runtime-parkana.txt" 'ingest_cpu_delta_usage_usec=1000000' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'ingest_parkana_cpu_before_usage_usec=1000000' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'ingest_parkana_cpu_after_usage_usec=2000000' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'ingest_parkana_cpu_delta_usage_usec=1000000' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'ingest_parkana_cpu_source=cgroup-v2-cpu.stat' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'ingest_parkana_cpu_scope=container cgroup-v2 usage_usec delta across the ingest command; not component attribution' ||
    { rm -rf "$work"; return 1; }
  require_regex "$OUTPUT_DIR/run-metadata.txt" '^ingest_parkana_cpu_(before|after)_wall_s=[0-9]+\.[0-9]+$' ||
    { rm -rf "$work"; return 1; }

  # memory.peak is a high-water value distinct from the configured limit.
  capture_parkana_memory_peak || { rm -rf "$work"; return 1; }
  [[ $EFFECTIVE_INGEST_MEMORY_PEAK_BYTES == 3221225472 &&
    $EFFECTIVE_INGEST_MEMORY_PEAK_SOURCE == cgroup-v2-memory.peak ]] ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/runtime-parkana.txt" 'ingest_memory_peak_bytes=3221225472' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/runtime-parkana.txt" 'ingest_memory_peak_source=cgroup-v2-memory.peak' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/runtime-parkana.txt" 'ingest_memory_peak_scope=container cgroup-v2 high-water since start through ingest, read before stop; not the memory limit' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'parkana_ingest_memory_peak_bytes=3221225472' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'parkana_ingest_memory_peak_source=cgroup-v2-memory.peak' ||
    { rm -rf "$work"; return 1; }

  capture_parkana_memory_events || { rm -rf "$work"; return 1; }
  [[ $EFFECTIVE_INGEST_OOM_KILL == 0 &&
    $EFFECTIVE_INGEST_OOM_KILL_SOURCE == cgroup-v2-memory.events ]] ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/runtime-parkana.txt" 'ingest_oom_kill=0' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'parkana_ingest_oom_kill=0' ||
    { rm -rf "$work"; return 1; }
  export MOCK_OOM_KILL=3
  capture_parkana_memory_events || { rm -rf "$work"; return 1; }
  [[ $EFFECTIVE_INGEST_OOM_KILL == 3 ]] || { rm -rf "$work"; return 1; }
  export MOCK_OOM_UNAVAILABLE=1
  capture_parkana_memory_events || { rm -rf "$work"; return 1; }
  [[ $EFFECTIVE_INGEST_OOM_KILL == unavailable ]] ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'parkana_ingest_oom_kill_reason=unavailable: /sys/fs/cgroup/memory.events not readable' ||
    { rm -rf "$work"; return 1; }
  unset MOCK_OOM_UNAVAILABLE
  export MOCK_OOM_MALFORMED=1
  capture_parkana_memory_events || { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'parkana_ingest_oom_kill_reason=unavailable: memory.events has no oom_kill counter' ||
    { rm -rf "$work"; return 1; }
  unset MOCK_OOM_MALFORMED MOCK_OOM_KILL

  # An unreadable cpu.stat is explicitly unavailable, never zero.
  export MOCK_CPU_UNAVAILABLE=1
  capture_cpu_snapshot_before parkana || { rm -rf "$work"; return 1; }
  capture_cpu_snapshot_after parkana || { rm -rf "$work"; return 1; }
  [[ $(get_cpu_field parkana BEFORE_USEC) == unavailable &&
    $(get_cpu_field parkana AFTER_USEC) == unavailable &&
    $(get_cpu_field parkana DELTA_USEC) == unavailable &&
    $(get_cpu_field parkana SOURCE) == unavailable ]] ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'ingest_parkana_cpu_before_usage_usec=unavailable' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'ingest_parkana_cpu_after_usage_usec=unavailable' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'ingest_parkana_cpu_delta_usage_usec=unavailable' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'ingest_parkana_cpu_reason=unavailable: missing or unreadable cpu.stat usage_usec snapshot' ||
    { rm -rf "$work"; return 1; }
  require_no_regex "$OUTPUT_DIR/run-metadata.txt" '^ingest_parkana_cpu_(before|after|delta)_usage_usec=0$' ||
    { rm -rf "$work"; return 1; }
  unset MOCK_CPU_UNAVAILABLE

  # A backwards counter must fail the run, not publish a zero/negative delta.
  printf '1\n' >"$MOCK_CPU_STATE"
  capture_cpu_snapshot_before parkana || { rm -rf "$work"; return 1; }
  printf '0\n' >"$MOCK_CPU_STATE"
  if (capture_cpu_snapshot_after parkana) >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi

  # Missing and malformed memory.peak values stay unavailable with a reason.
  export MOCK_MEMORY_PEAK_BYTES=
  capture_parkana_memory_peak || { rm -rf "$work"; return 1; }
  [[ $EFFECTIVE_INGEST_MEMORY_PEAK_BYTES == unavailable &&
    $EFFECTIVE_INGEST_MEMORY_PEAK_SOURCE == unavailable ]] ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'parkana_ingest_memory_peak_bytes=unavailable' ||
    { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" 'parkana_ingest_memory_peak_reason=unavailable: /sys/fs/cgroup/memory.peak not readable' ||
    { rm -rf "$work"; return 1; }
  export MOCK_MEMORY_PEAK_BYTES=12abc
  capture_parkana_memory_peak || { rm -rf "$work"; return 1; }
  require_grep "$OUTPUT_DIR/run-metadata.txt" "parkana_ingest_memory_peak_reason=unavailable: invalid memory.peak value '12abc'" ||
    { rm -rf "$work"; return 1; }
  require_no_regex "$OUTPUT_DIR/run-metadata.txt" '^parkana_ingest_memory_peak_bytes=0$' ||
    { rm -rf "$work"; return 1; }
  unset MOCK_MEMORY_PEAK_BYTES
  rm -rf "$work"
)

test_a6_dry_run_runtime() (
  local work out defaults
  work=$(mktemp -d) || return 1
  # Ambient PARKANA_BENCH_* values are not inputs; a flag still wins.
  out=$(PARKANA_BENCH_WAL_SYNC=interval PARKANA_BENCH_WAL_SYNC_INTERVAL=9s \
    PARKANA_BENCH_FLUSH_CONCURRENCY=32 PARKANA_BENCH_UPLOAD_CONCURRENCY=32 \
    bash "$SCRIPT_PATH" --dry-run --wal-sync always --wal-sync-interval 250ms \
      --flush-concurrency 2 --upload-concurrency 8 --output-dir "$work/out" 2>&1) ||
    { rm -rf "$work"; return 1; }
  [[ $out == *"ingest runtime  : wal_sync=always (explicit)"* ]] || { rm -rf "$work"; return 1; }
  [[ $out == *"interval=250ms (explicit)"* ]] || { rm -rf "$work"; return 1; }
  [[ $out == *"flush_workers=2 (explicit)"* ]] || { rm -rf "$work"; return 1; }
  [[ $out == *"upload_workers=8 (explicit)"* ]] || { rm -rf "$work"; return 1; }
  [[ $out == *"runtime overlay : $RUNTIME_OVERLAY"* ]] || { rm -rf "$work"; return 1; }
  [[ ! -e $work/out ]] || { rm -rf "$work"; return 1; }
  # With no flags, the same ambient exports still leave the phase 4 defaults.
  defaults=$(PARKANA_BENCH_WAL_SYNC=interval PARKANA_BENCH_WAL_SYNC_INTERVAL=9s \
    PARKANA_BENCH_FLUSH_CONCURRENCY=32 PARKANA_BENCH_UPLOAD_CONCURRENCY=32 \
    bash "$SCRIPT_PATH" --dry-run --output-dir "$work/defaults" 2>&1) ||
    { rm -rf "$work"; return 1; }
  [[ $defaults == *"wal_sync=always (default)"* ]] || { rm -rf "$work"; return 1; }
  [[ $defaults == *"interval=1s (default)"* ]] || { rm -rf "$work"; return 1; }
  [[ $defaults == *"flush_workers=4 (default)"* ]] || { rm -rf "$work"; return 1; }
  [[ $defaults == *"upload_workers=4 (default)"* ]] || { rm -rf "$work"; return 1; }
  [[ ! -e $work/defaults ]] || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_a6_runtime_evidence() (
  local work f
  work=$(mktemp -d) || return 1
  if ! (PARKANA_BENCH_SOURCE_COMMIT=abc123 PARKANA_BENCH_SOURCE_DIRTY=0 \
    MOCK_WAL_SYNC=interval MOCK_WAL_SYNC_INTERVAL=250ms \
    MOCK_FLUSH_CONCURRENCY=2 MOCK_UPLOAD_CONCURRENCY=8 \
    run_mock_benchmark parkana-only 2097152 "$work") >/dev/null 2>&1; then
    printf 'mock runtime evidence flow failed\n' >&2
    rm -rf "$work"
    return 1
  fi
  for f in run-metadata.txt summary.md runtime-parkana.txt \
    config/ingest-runtime.yaml secrets.env; do
    require_file "$work/out/$f" || { rm -rf "$work"; return 1; }
  done
  for pair in \
    'parkana_runtime_wal_sync=interval' \
    'parkana_runtime_wal_sync_source=explicit' \
    'parkana_runtime_wal_sync_interval=250ms' \
    'parkana_runtime_wal_sync_interval_source=explicit' \
    'parkana_runtime_flush_concurrency=2' \
    'parkana_runtime_flush_concurrency_source=explicit' \
    'parkana_runtime_upload_concurrency=8' \
    'parkana_runtime_upload_concurrency_source=explicit' \
    'parkana_effective_wal_sync=interval' \
    'parkana_effective_wal_sync_interval=250ms' \
    'parkana_effective_flush_concurrency=2' \
    'parkana_effective_upload_concurrency=8' \
    'parkana_effective_nano_cpus=4000000000' \
    'parkana_effective_cpus=4.000' \
    'parkana_effective_memory_bytes=8589934592' \
    'parkana_effective_source=container-inspect' \
    'source_commit=abc123' \
    'source_commit_source=explicit' \
    'source_dirty=0' \
    'source_dirty_source=explicit'; do
    require_grep "$work/out/run-metadata.txt" "$pair" || { rm -rf "$work"; return 1; }
  done
  require_regex "$work/out/run-metadata.txt" '^compose_sha256=[0-9a-f]{64}$' ||
    { rm -rf "$work"; return 1; }
  require_regex "$work/out/run-metadata.txt" '^runtime_overlay_sha256=[0-9a-f]{64}$' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/runtime-parkana.txt" 'effective_wal_sync=interval' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/timings.tsv" '# parkana_runtime effective wal_sync=interval' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'wal_sync=interval' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'flush_workers=2' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'upload_workers=8' || { rm -rf "$work"; return 1; }
  require_grep "$work/out/summary.md" 'effective (container inspect)' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/out/secrets.env" 'PARKANA_BENCH_WAL_SYNC=interval' ||
    { rm -rf "$work"; return 1; }
  # Phase 3 identity/barrier evidence is additive, never replaced.
  for pair in \
    'batch_lines=1250' \
    'ingest_parkana_corpus_instances_per_service=4' \
    'ingest_parkana_parallelism_label_sets=16' \
    'ingest_parkana_parallelism_active_senders=15' \
    'ingest_parkana_batching=complete'; do
    require_grep "$work/out/run-metadata.txt" "$pair" || { rm -rf "$work"; return 1; }
  done
  require_grep "$work/events" 'Parkana exact visibility' || { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'verifying parkana data after restart' ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/events" 'running query workload against parkana' ||
    { rm -rf "$work"; return 1; }

  OUTPUT_DIR="$work/out"
  RUN_LOG="$work/out/run.log"
  emit_archive >"$work/archive.txt" || { rm -rf "$work"; return 1; }
  awk '/=== BENCH ARTIFACT ARCHIVE BEGIN/{f=1; next} /=== BENCH ARTIFACT ARCHIVE END ===/{f=0} f' \
    "$work/archive.txt" | base64 -d | tar -tzf - >"$work/archive-list" ||
    { rm -rf "$work"; return 1; }
  require_grep "$work/archive-list" 'runtime-parkana.txt' || { rm -rf "$work"; return 1; }
  require_grep "$work/archive-list" 'config/ingest-runtime.yaml' ||
    { rm -rf "$work"; return 1; }

  # The same flow with no runtime flags keeps the phase 4 defaults.
  if ! (run_mock_benchmark parkana-only 2097152 "$work/defaults") >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  for pair in \
    'parkana_runtime_wal_sync=always' \
    'parkana_runtime_wal_sync_source=default' \
    'parkana_runtime_wal_sync_interval=1s' \
    'parkana_runtime_flush_concurrency=4' \
    'parkana_runtime_upload_concurrency=4' \
    'parkana_effective_wal_sync=always' \
    'parkana_effective_flush_concurrency=4' \
    'parkana_effective_upload_concurrency=4'; do
    require_grep "$work/defaults/out/run-metadata.txt" "$pair" || { rm -rf "$work"; return 1; }
  done
  rm -rf "$work"
)

test_a6_base_compose_frozen() (
  local status
  status=$(git -C "$REPO_ROOT" status --porcelain -- deploy/compose/bench.yaml 2>/dev/null || true)
  [[ -z $status ]] || return 1
  require_file "$RUNTIME_OVERLAY" || return 1
  require_regex "$RUNTIME_OVERLAY" '^services:' || return 1
  require_regex "$RUNTIME_OVERLAY" '^  parkana:' || return 1
  local key
  for key in PARKANA_WAL_SYNC PARKANA_WAL_SYNC_INTERVAL \
    PARKANA_INGEST_FLUSH_CONCURRENCY PARKANA_INGEST_UPLOAD_CONCURRENCY; do
    require_grep "$RUNTIME_OVERLAY" "$key:" || return 1
  done
  for key in PARKANA_INGEST_UPLOAD_MERGE_MAX PARKANA_INGEST_UPLOAD_MIN \
    PARKANA_INGEST_MEMTABLE_MAX PARKANA_INDEX_MAX_GRAMS; do
    require_no_grep "$RUNTIME_OVERLAY" "$key" || return 1
  done
  require_no_grep "$COMPOSE_FILE" 'PARKANA_WAL_SYNC' || return 1
  require_no_grep "$COMPOSE_FILE" 'PARKANA_INGEST_FLUSH_CONCURRENCY' || return 1
  require_no_grep "$COMPOSE_FILE" 'PARKANA_INGEST_UPLOAD_CONCURRENCY' || return 1
  require_grep "$COMPOSE_FILE" 'PARKANA_INGEST_UPLOAD_MERGE_MAX: 2GiB' || return 1
  require_grep "$COMPOSE_FILE" 'PARKANA_BENCH_INDEX_MAX_GRAMS:-2097152' || return 1
)

self_test() {
  local failed=0
  printf '[bench-p3] self-test: offline arguments, collisions, readiness, metrics, cleanup and exit paths\n'
  self_case "presets resolve decimal 2,000,000,000 / 10,000,000,000 / 25,000,000,000 bytes" test_defaults_and_presets || failed=1
  self_case "preset/bytes conflicts, full gate, custom bytes and malformed values are handled" test_preset_validation || failed=1
  self_case "ports, project names and numeric arguments are validated" test_argument_validation || failed=1
  self_case "--instances-per-service is omitted by default and accepts 1..4096 only" test_instances_per_service_validation || failed=1
  self_case "--streams validates names, duplicates, marker literals and the label-set bound" test_streams_flag_validation || failed=1
  self_case "--reuse-manifest reuses only corpus.start and rejects conflicts/missing/invalid starts" test_reuse_manifest_validation || failed=1
  self_case "legacy runtime accepts only known always/F1/U1 and rejects modern flags" test_legacy_runtime_validation || failed=1
  self_case "an existing output directory is refused" test_output_dir_collision || failed=1
  self_case "output directories must be freshly created" test_output_dir_must_be_fresh || failed=1
  self_case "an existing compose project is refused; a fresh one passes" test_project_collision_and_freshness || failed=1
  self_case "compose project listing uses the supported -a --quiet argv" test_compose_ls_argv || failed=1
  self_case "benchmark image pins are literal and match metadata constants" test_image_pins || failed=1
  self_case "wait_one_shot handles an unset timeout, exit 0, failure and timeout" test_wait_one_shot || failed=1
  self_case "an atomic host lock blocks concurrent same-project adoption" test_project_lock || failed=1
  self_case "per-backend buckets and credentials are random and distinct" test_credentials_are_separate_and_silent || failed=1
  self_case "the secrets env file is 0600 and holds one value per key" test_env_file_mode_and_contents || failed=1
  self_case "dry run generates no credentials and creates nothing" test_dry_run_never_generates_secrets || failed=1
  self_case "diagnostics redact every generated secret" test_redaction || failed=1
  self_case "manifest hashes/start/acknowledgments must match exactly" test_manifest_matching || failed=1
  self_case "catalog row parsing and count-query JSON edge cases" test_catalog_and_count_parsing || failed=1
  self_case "visibility range arithmetic is half-open safe" test_range_ms || failed=1
  self_case "readiness failure and success paths" test_readiness_failure_and_success || failed=1
  self_case "pre-flush CounterVec absence is tolerated; post-flush absence/no-movement fail" test_metric_absence || failed=1
  self_case "Loki /flush HTTP failure propagates" test_flush_http_failure || failed=1
  self_case "parkbench ingest failures propagate (exit 7)" test_exit_propagation_ingest || failed=1
  self_case "parkbench ingest argv carries exact bytes, start, tenant and output" test_ingest_command_construction || failed=1
  self_case "ACK_AT is the ingest-return time, before the bounded CPU-after read" test_ack_before_cpu_snapshot || failed=1
  self_case "batching evidence appends per-backend report values without losing metadata" test_batching_metadata_append || failed=1
  self_case "parkbench query failures propagate (exit 1)" test_exit_propagation_query || failed=1
  self_case "Compose argv is exact and contains no secret values" test_command_construction || failed=1
  self_case "stop must prove the backend exited; failures and ambiguity fail" test_stop_proof || failed=1
  self_case "cleanup only touches a stack this run owns" test_cleanup_ownership || failed=1
  self_case "Garage bucket and sidecar metric parsing" test_storage_parsing || failed=1
  self_case "background-work idle metric must be present and zero" test_metric_idle || failed=1
  self_case "Loki local WAL/index/cache bytes parse from the tool volume" test_loki_local_parsing || failed=1
  self_case "source commit/dirty overrides and git-less archives stay honest" test_source_identity || failed=1
  self_case "artifact archive is bounded, delimited and excludes secrets" test_artifact_archive || failed=1
  self_case "benchmark Loki config preserves phase-2 schema/discovery/parser settings" test_loki_config_parity || failed=1
  self_case "phase-2 compose/image/config files are untouched" test_no_phase2_edits || failed=1
  self_case "--parkana-only and --index-max-grams accept only bounded positive int64" test_a5_flag_and_cap_validation || failed=1
  self_case "index cap reaches the owned env file and Compose, not caller env" test_a5_env_and_compose_propagation || failed=1
  self_case "dry run reports parkana-only and the actual index cap without resources" test_a5_dry_run_reports_controls || failed=1
  self_case "parkana-only does not probe the Loki port or start garage-init-loki" test_a5_parkana_only_ports_and_init || failed=1
  self_case "sidecar publication stays required unless --skip-queries is used" test_a5_sidecar_publication_not_relaxed || failed=1
  self_case "parkana-only main flow keeps every Parkana check and calls no Loki path" test_a5_mainflow_parkana_only || failed=1
  self_case "default main flow still runs both backends and reports Loki" test_a5_mainflow_default_dual || failed=1
  self_case "the Parkana phase is identical with and without --parkana-only" test_a5_mainflow_parkana_path_unchanged || failed=1
  self_case "explicit --instances-per-service is passed and requested metadata stays distinct from actual" test_instances_explicit_metadata || failed=1
  self_case "one explicit hot stream keeps 16 label sets/senders and actual stream evidence" test_streams_explicit_metadata || failed=1
  self_case "requested streams never overwrite the actual manifest streams" test_streams_requested_vs_actual || failed=1
  self_case "reuse-manifest records the actual interval and a replay command" test_reuse_manifest_flow || failed=1
  self_case "legacy runtime records the source contract, probes flags and keeps CPU/memory evidence" test_legacy_runtime_flow || failed=1
  self_case "legacy runtime rejects modern and partial help with a fixed sanitized probe error" test_legacy_runtime_probe_rejects_modern || failed=1
  self_case "runtime flags enforce always/interval, bounded whole-ns durations and 1..64 workers" test_a6_runtime_flag_validation || failed=1
  self_case "runtime settings reach the owned env file and Compose; caller exports do not" test_a6_env_and_compose_propagation || failed=1
  self_case "effective workers/resources come from the container, and mismatches fail closed" test_a6_effective_capture || failed=1
  self_case "cgroup cpu.stat before/after/delta and memory.peak are honest and monotonic" test_cgroup_snapshot_capture || failed=1
  self_case "dry run reports explicit runtime flags and ignores ambient exports" test_a6_dry_run_runtime || failed=1
  self_case "runtime evidence is additive, source-aware and archived without changing phase 3 identity" test_a6_runtime_evidence || failed=1
  self_case "bench.yaml stays frozen and the overlay changes no merge or gram cap" test_a6_base_compose_frozen || failed=1
  if ((failed)); then
    printf '[bench-p3] self-test: FAILED\n' >&2
    return 1
  fi
  printf '[bench-p3] self-test: all checks passed\n'
  return 0
}

# --- entrypoint -------------------------------------------------------------

main() {
  init_defaults
  parse_args "$@"
  if ((SELFTEST)); then
    self_test
    return
  fi
  validate_args
  if ((DRY_RUN)); then
    check_output_dir_absent
    dry_run
    return
  fi
  require_cmds
  acquire_project_lock
  # Until the artifact directory exists, exiting only has to release the lock.
  trap 'release_project_lock' EXIT
  trap 'release_project_lock; exit 130' INT
  trap 'release_project_lock; exit 143' TERM
  preflight
  create_output_dir
  # Everything below writes to the artifact directory; keep stdout visible and
  # the full transcript in run.log.
  exec 3>&1
  exec > >(tee -a "$RUN_LOG") 2>&1
  trap - EXIT INT TERM
  trap 'on_exit $?' EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  # Re-check after creating our artifact directory: the host lock stops
  # another bench-phase3 invocation, and this second check catches manual
  # Docker use between the two inspections.
  ensure_project_fresh

  generate_credentials
  write_env_file
  STACK_OWNED=1
  log "project=$PROJECT preset=$PRESET bytes=$TARGET_BYTES output=$OUTPUT_DIR"
  log "start source=$(start_source_value) requested_start=${START:-derived-by-tool}"
  if ((LEGACY_RUNTIME)); then
    log "ingest runtime: legacy source contract, known always/F1/U1; no phase-4 runtime flags"
  else
    log "ingest runtime requested: wal_sync=$WAL_SYNC interval=$WAL_SYNC_INTERVAL flush_workers=$FLUSH_CONCURRENCY upload_workers=$UPLOAD_CONCURRENCY"
  fi
  log "resources requested: cpus=$CPUS memory=$MEMORY per backend container"
  log "Garage buckets and credentials generated for this run (values hidden)"
  build_images
  build_parkbench
  check_parkbench_query
  run_benchmark
  ((RUN_SUCCEEDED)) || fail "benchmark did not complete"
}

main "$@"
