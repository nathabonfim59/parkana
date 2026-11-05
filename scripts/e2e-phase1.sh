#!/usr/bin/env bash
#
# scripts/e2e-phase1.sh - end-to-end check of Parkana's phase 1 stack.
#
# What it proves (against Garage, not a mock):
#   1. pushgen can push deterministic data as JSON and as block-snappy
#      protobuf, in several bounded bursts, and every acknowledged row lands
#      in object storage exactly once.
#   2. The compactor replaces the small uploaded objects with fewer objects
#      without losing, duplicating or retyping a row (same min/max, exact row
#      count, same base schema).
#   3. A SIGKILL of the Parkana container after acknowledged pushes loses
#      nothing: a restart from the same data volume recovers all rows from the
#      WAL, uploads them, and keeps the schema and the earlier timestamp range.
#
# The check is deliberately black-box: it reads Parquet objects with DuckDB
# straight from the S3 endpoint, exactly like a Grafana/duckdb reader would,
# and never inspects Parkana's internals. Each phase is a separate Parkana
# container recreation on the same named volume, so the WAL/local state must
# survive a real process death.
#
# Prerequisites: a Docker host with the compose v2 plugin, Go matching go.mod
# (to build cmd/pushgen), curl, GNU coreutils (timeout/date -d @epoch), and
# either the DuckDB CLI or network access to pull the pinned duckdb/duckdb
# image. See deploy/compose/e2e.md for the full runbook.
#
# Scope: phase 1 only (Garage + Parkana). Grafana/Loki are not involved.
#
# This script never edits deploy/compose/*; it supplies the stack's documented
# environment contract and fails loudly if the compose file does not honor it.
# It refuses to reuse a compose project with existing resources, removes only
# the containers/network/volumes of the project it created, and leaves built
# images alone.
#
# Exit status: 0 all phases passed; 1 any prerequisite, push, wait, check or
# cleanup step failed. Diagnostics are bounded and redacted.
#
# Usage: scripts/e2e-phase1.sh [options]
#   --self-test          run the offline logic checks and exit (no Docker)
#   --dry-run            print the resolved plan and exit (no Docker needed)
#   --project NAME       compose project name (default: generated, unique)
#   --parkana-port PORT  host port for Parkana HTTP (default 3100/PARKANA_HTTP_PORT)
#   --garage-port PORT   host port for Garage S3 (default 3900/PARKANA_S3_PORT)
#   --total N            rows for the initial load (default 10000)
#   --extra N            rows pushed before SIGKILL (default 2000, max 50000)
#   --bursts N           initial bursts, at least 3 (default 3)
#   --rate N             pushgen lines/second (default 5000)
#   --batch N            pushgen lines per request (default 100)
#   --keep               leave the compose stack running on completion/failure
#   -h, --help           show this help
#
set -euo pipefail

readonly SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
readonly COMPOSE_DIR="$REPO_ROOT/deploy/compose"
readonly COMPOSE_FILE="$COMPOSE_DIR/compose.yaml"

# The stack contract implemented by deploy/compose/compose.yaml (B5a) and
# consumed here (B5b). Do not rename one side without the other.
readonly S3_BUCKET=parkana
readonly S3_REGION=garage
readonly TENANT=default
readonly STREAM=checkout
readonly S3_SERVICE_ENDPOINT=garage:3900 # inside the compose network
readonly PARKANA_SERVICE_PORT=3100
readonly GARAGE_SERVICE_PORT=3900
# DuckDB's isolated extension cache: a host temp dir, mounted at this fixed
# container path when DuckDB runs in the pinned image. HOME is not the cache.
# The pinned image ships no HOME at all, and DuckDB 1.5 still resolves a home
# directory before INSTALL even when extension_directory is set, so container
# invocations get a private ephemeral tmpfs home for that runtime requirement
# only; native runs keep the user's real HOME and only set extension_directory.
readonly DUCKDB_EXT_CONTAINER=/duckdb-ext
readonly DUCKDB_HOME_CONTAINER=/duckdb-home
# The pinned official image is distroless: no shell, no Entrypoint, and
# CMD ["/duckdb"]. A bare `docker run IMAGE --version` therefore replaces the
# command with "--version" and fails with exit 127, so every container
# invocation names the binary itself through --entrypoint. This is stable
# across tags of duckdb/duckdb and does not depend on the image's PATH.
readonly DUCKDB_CONTAINER_BIN=/duckdb

# pushgen timestamps: entry i of a run is started_at + i milliseconds, even
# though the run itself is paced by -rate. A run of N lines can therefore carry
# timestamps up to N ms ahead of the wall clock when rate > 1000. The initial
# bursts wait past the previous observed max and the crash phase waits past
# MAX0, so synthetic ranges never overlap; bounds check
# max <= measured_end + largest_burst_ms + SLACK_US.
readonly SLACK_US=1000000 # 1s of clock/sample slack

# Extra rows stay far below the default memtable (256MiB) and WAL (128MiB)
# size seals, so the long-interval crash phase can only be sealed by the
# explicit long flush tick and not by size. ~130 bytes/row at the cap is ~6MiB.
readonly MAX_EXTRA=50000

log() { printf '[e2e] %s\n' "$*"; }
warn() { printf '[e2e] WARN: %s\n' "$*" >&2; }
fail() {
  printf '[e2e] ERROR: %s\n' "$*" >&2
  # Trigger the EXIT trap (diagnostics + scoped cleanup) with status 1.
  exit 1
}

usage() {
  sed -n '/^# Usage:/,/^#   -h, --help/p' "$0" | sed 's/^# \{0,1\}//'
}

init_defaults() {
  PROJECT=${PARKANA_E2E_PROJECT:-}
  PROJECT_EXPLICIT=0
  if [[ -n $PROJECT ]]; then
    PROJECT_EXPLICIT=1
  fi

  TOTAL=${PARKANA_E2E_TOTAL:-10000}
  EXTRA=${PARKANA_E2E_EXTRA:-2000}
  BURSTS=${PARKANA_E2E_BURSTS:-3}
  RATE=${PARKANA_E2E_RATE:-5000}
  BATCH=${PARKANA_E2E_BATCH:-100}

  HTTP_PORT=${PARKANA_HTTP_PORT:-3100}
  S3_PORT=${PARKANA_S3_PORT:-3900}
  HTTP_PORT_EXPLICIT=0
  S3_PORT_EXPLICIT=0
  if [[ -n ${PARKANA_HTTP_PORT:-} ]]; then
    HTTP_PORT_EXPLICIT=1
  fi
  if [[ -n ${PARKANA_S3_PORT:-} ]]; then
    S3_PORT_EXPLICIT=1
  fi

  DUCKDB_IMAGE=${PARKANA_E2E_DUCKDB_IMAGE:-duckdb/duckdb:1.5.6}
  DUCKDB_MODE_PREF=${PARKANA_E2E_DUCKDB_MODE:-auto}

  # Development-only fixed Garage credentials. The compose file defaults to the
  # same values; the S3 endpoint needs them too. They are never printed.
  ACCESS_KEY=${PARKANA_DEV_GARAGE_KEY_ID:-GK5c7e9a3b1d8f2e6c4a0b9d8f}
  SECRET_KEY=${PARKANA_DEV_GARAGE_KEY_SECRET:-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef}
  export PARKANA_DEV_GARAGE_KEY_ID=$ACCESS_KEY
  export PARKANA_DEV_GARAGE_KEY_SECRET=$SECRET_KEY

  WAIT_TIMEOUT=${PARKANA_E2E_WAIT_TIMEOUT:-180}
  POLL_INTERVAL=${PARKANA_E2E_POLL_INTERVAL:-2}
  READY_TIMEOUT=${PARKANA_E2E_READY_TIMEOUT:-180}
  BUILD_TIMEOUT=${PARKANA_E2E_BUILD_TIMEOUT:-1800}
  UP_TIMEOUT=${PARKANA_E2E_UP_TIMEOUT:-300}
  STOP_TIMEOUT=${PARKANA_E2E_STOP_TIMEOUT:-90}
  DOWN_TIMEOUT=${PARKANA_E2E_DOWN_TIMEOUT:-180}
  PUSH_TIMEOUT=${PARKANA_E2E_PUSH_TIMEOUT:-300}
  DUCKDB_TIMEOUT=${PARKANA_E2E_DUCKDB_TIMEOUT:-120}
  DUCKDB_INSTALL_TIMEOUT=${PARKANA_E2E_DUCKDB_INSTALL_TIMEOUT:-300}
  PULL_TIMEOUT=${PARKANA_E2E_PULL_TIMEOUT:-600}

  KEEP=0
  DRY_RUN=0
  SELFTEST=0
  STACK_CREATED=0
  WORK=""
  COMPOSE=()
  PHASE_ENV=()
  NETWORK=""
  DUCKDB_MODE=""
  DUCKDB_EXT_HOST=""
  DUCK_ENDPOINT=""
  LAST_DUCK_ERR=""
  T0_US=0
  T1_US=0
  T2_US=0
  T3_US=0
  N0=0
  OBJ0=0
  MIN0=0
  MAX0=0
}

parse_args() {
  while (($# > 0)); do
    case "$1" in
      --self-test) SELFTEST=1; shift ;;
      --dry-run) DRY_RUN=1; shift ;;
      --keep) KEEP=1; shift ;;
      --project)
        (($# >= 2)) || fail "--project needs a value"
        PROJECT=$2
        PROJECT_EXPLICIT=1
        shift 2
        ;;
      --parkana-port)
        (($# >= 2)) || fail "--parkana-port needs a value"
        HTTP_PORT=$2
        HTTP_PORT_EXPLICIT=1
        shift 2
        ;;
      --garage-port)
        (($# >= 2)) || fail "--garage-port needs a value"
        S3_PORT=$2
        S3_PORT_EXPLICIT=1
        shift 2
        ;;
      --total)
        (($# >= 2)) || fail "--total needs a value"
        TOTAL=$2
        shift 2
        ;;
      --extra)
        (($# >= 2)) || fail "--extra needs a value"
        EXTRA=$2
        shift 2
        ;;
      --bursts)
        (($# >= 2)) || fail "--bursts needs a value"
        BURSTS=$2
        shift 2
        ;;
      --rate)
        (($# >= 2)) || fail "--rate needs a value"
        RATE=$2
        shift 2
        ;;
      --batch)
        (($# >= 2)) || fail "--batch needs a value"
        BATCH=$2
        shift 2
        ;;
      -h | --help) usage; exit 0 ;;
      *) fail "unknown argument '$1' (see --help)" ;;
    esac
  done
}

# Numeric inputs are checked as strings before any Bash arithmetic: an
# unchecked value such as "1+1" would otherwise be evaluated as an expression.
is_uint() { [[ $1 =~ ^[1-9][0-9]{0,8}$ ]]; }
is_rate() { [[ $1 =~ ^(0|[1-9][0-9]{0,8})([.][0-9]+)?$ ]]; }
valid_port() { [[ $1 =~ ^[1-9][0-9]{0,4}$ ]] && (($1 >= 1 && $1 <= 65535)); }
valid_project_name() { [[ $1 =~ ^[a-z0-9][a-z0-9_-]*$ ]]; }
extra_limit_ok() { [[ $1 =~ ^[1-9][0-9]{0,4}$ ]] && (($1 >= 1 && $1 <= MAX_EXTRA)); }

validate_args() {
  is_uint "$TOTAL" || fail "--total must be a positive integer"
  is_uint "$EXTRA" || fail "--extra must be a positive integer"
  is_uint "$BURSTS" || fail "--bursts must be a positive integer"
  is_rate "$RATE" || fail "--rate must be a non-negative number"
  is_uint "$BATCH" || fail "--batch must be a positive integer"
  ((${#PROJECT} <= 63)) || fail "--project must be at most 63 characters"
  (($(printf '%s' "$TOTAL") >= 1)) || fail "--total must be at least 1"
  (($(printf '%s' "$EXTRA") >= 1)) || fail "--extra must be at least 1"
  extra_limit_ok "$EXTRA" ||
    fail "--extra must be at most $MAX_EXTRA so it cannot size-seal the memtable/WAL in the long-interval phase"
  (($(printf '%s' "$BURSTS") >= 3)) || fail "--bursts must be at least 3"
  (($(printf '%s' "$TOTAL") >= $(printf '%s' "$BURSTS"))) || fail "--total must be at least --bursts"
  (($(printf '%s' "$BATCH") >= 1)) || fail "--batch must be at least 1"
  valid_port "$HTTP_PORT" || fail "--parkana-port must be an integer in 1..65535"
  valid_port "$S3_PORT" || fail "--garage-port must be an integer in 1..65535"
  [[ $HTTP_PORT != "$S3_PORT" ]] || fail "--parkana-port and --garage-port must differ"
  case $DUCKDB_MODE_PREF in
    auto | native | container) ;;
    *) fail "PARKANA_E2E_DUCKDB_MODE must be auto, native or container, got '$DUCKDB_MODE_PREF'" ;;
  esac

  # These are used in arithmetic deadlines/sleeps, so they get the same
  # expression-before-arithmetic treatment as the ports.
  local timeout_var
  for timeout_var in WAIT_TIMEOUT POLL_INTERVAL READY_TIMEOUT BUILD_TIMEOUT UP_TIMEOUT \
    STOP_TIMEOUT DOWN_TIMEOUT PUSH_TIMEOUT DUCKDB_TIMEOUT DUCKDB_INSTALL_TIMEOUT PULL_TIMEOUT; do
    is_uint "${!timeout_var}" || fail "PARKANA_E2E_${timeout_var} must be a positive integer (seconds)"
  done

  if [[ -n $PROJECT ]]; then
    valid_project_name "$PROJECT" || fail "invalid compose project name '$PROJECT' (want [a-z0-9][a-z0-9_-]*)"
  else
    PROJECT="parkana-e2e-$(date -u +%y%m%d%H%M%S)-$$"
    valid_project_name "$PROJECT" || fail "generated project name '$PROJECT' is invalid"
  fi
}

# --- small helpers ---------------------------------------------------------

run_bounded() {
  local timeout_secs=$1
  shift
  timeout "$timeout_secs" "$@"
}

now_us() {
  local ns
  ns=$(date -u +%s%N)
  printf '%s' $((ns / 1000))
}

fmt_us() {
  local us=$1
  if ((${us:-0} < 0)); then
    printf '%s' "$us"
    return
  fi
  printf '%s.%06dZ' "$(date -u -d "@$((us / 1000000))" +%Y-%m-%dT%H:%M:%S)" "$((us % 1000000))"
}

sql_lit() {
  local s=$1
  s=${s//\'/\'\'}
  printf "'%s'" "$s"
}

redact() {
  local line
  while IFS= read -r line || [[ -n $line ]]; do
    if [[ -n ${ACCESS_KEY:-} ]]; then
      line=${line//"$ACCESS_KEY"/<redacted>}
    fi
    if [[ -n ${SECRET_KEY:-} ]]; then
      line=${line//"$SECRET_KEY"/<redacted>}
    fi
    printf '%s\n' "$line"
  done
}

print_file_tail() {
  local file=$1 lines=${2:-30}
  if [[ -f $file ]]; then
    { tail -n "$lines" -- "$file" 2>/dev/null || true; } | redact >&2
  fi
}

burst_size() { # total bursts index -> exact per-burst line count
  local total=$1 bursts=$2 index=$3
  printf '%s' $((total / bursts + (index < total % bursts ? 1 : 0)))
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

choose_port() { # preferred explicit label envname -> port on stdout
  local preferred=$1 explicit=$2 label=$3 envname=$4 candidate i
  if port_free "$preferred"; then
    printf '%s' "$preferred"
    return 0
  fi
  if ((explicit)); then
    printf '[e2e] ERROR: %s port %s is already in use; pass the matching flag or set %s\n' \
      "$label" "$preferred" "$envname" >&2
    return 1
  fi
  for ((i = 0; i < 50; i++)); do
    candidate=$((20000 + RANDOM % 10000))
    if [[ $candidate == "${HTTP_PORT:-}" || $candidate == "${S3_PORT:-}" ]]; then
      continue
    fi
    if port_free "$candidate"; then
      printf '[e2e] %s port %s is busy; using %s\n' "$label" "$preferred" "$candidate" >&2
      printf '%s' "$candidate"
      return 0
    fi
  done
  printf '[e2e] ERROR: could not find a free %s port; set %s explicitly\n' "$label" "$envname" >&2
  return 1
}

set_phase_env() { # flush upload upload-min upload-max-age compactor-interval target-size delete-delay
  PHASE_ENV=(
    "PARKANA_INGEST_FLUSH_INTERVAL=$1"
    "PARKANA_INGEST_UPLOAD_INTERVAL=$2"
    "PARKANA_INGEST_UPLOAD_MIN=$3"
    "PARKANA_INGEST_UPLOAD_MAX_AGE=$4"
    "PARKANA_COMPACTOR_INTERVAL=$5"
    "PARKANA_COMPACTOR_TARGET_SIZE=$6"
    "PARKANA_COMPACTOR_DELETE_DELAY=$7"
  )
}

compose_do() {
  local timeout_secs=$1
  shift
  run_bounded "$timeout_secs" env "${PHASE_ENV[@]}" "${COMPOSE[@]}" "$@"
}

# --- self tests (no Docker, DuckDB or services) ----------------------------

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

test_sql_lit() {
  [[ $(sql_lit "abc") == "'abc'" ]] || return 1
  [[ $(sql_lit "O'Brien") == "'O''Brien'" ]] || return 1
  [[ $(sql_lit "") == "''" ]] || return 1
}

test_redact() (
  ACCESS_KEY="KEY_do_not_print"
  SECRET_KEY="SECRET_do_not_print"
  local out
  out=$(printf 'key=%s value=%s done\n' "$ACCESS_KEY" "$SECRET_KEY" | redact)
  [[ $out == *"<redacted>"* ]] || return 1
  [[ $out != *"$ACCESS_KEY"* && $out != *"$SECRET_KEY"* ]] || return 1
  ACCESS_KEY=""
  SECRET_KEY=""
  out=$(printf 'plain line\n' | redact)
  [[ $out == "plain line" ]] || return 1
)

test_burst_split() {
  local total=10000 bursts=3 i t sum=0
  for ((i = 0; i < bursts; i++)); do
    t=$(burst_size "$total" "$bursts" "$i")
    ((${t:-0} > 0)) || return 1
    sum=$((sum + t))
  done
  ((${sum:-0} == total)) || return 1
  [[ $(burst_size 5 3 0) == 2 && $(burst_size 5 3 1) == 2 && $(burst_size 5 3 2) == 1 ]] || return 1
}

test_extra_limit() {
  extra_limit_ok 1 && extra_limit_ok "$MAX_EXTRA" && ! extra_limit_ok $((MAX_EXTRA + 1))
}

test_duckdb_ready() {
  duckdb_ready $'installing...\nREADY' || return 1
  duckdb_ready $'READY\r' || return 1
  ! duckdb_ready "installing..." || return 1
}

test_duckdb_container_argv() (
  DUCKDB_MODE=container
  DUCKDB_IMAGE=duckdb/duckdb:1.5.6
  DUCKDB_EXT_HOST=/tmp/parkana-e2e-selftest-ext
  DUCKDB_INSTALL_TIMEOUT=5
  DUCKDB_TIMEOUT=5
  NETWORK=parkana-e2e-selftest-net
  # Capture the docker argv instead of running Docker.
  run_bounded() {
    printf '%s\n' "$*"
  }
  local setup_args query_args entry
  setup_args=$(duckdb_setup_exec </dev/null) || return 1
  query_args=$(duckdb_query_exec </dev/null) || return 1

  # The pinned image has no Entrypoint and CMD ["/duckdb"]; a later argument
  # (e.g. --version) would replace the binary and fail with exit 127. Every
  # invocation must name the binary explicitly, exactly once, and the image must
  # stay the last token before DuckDB's own flags.
  entry="--entrypoint $DUCKDB_CONTAINER_BIN"
  [[ $(grep -oF -- "$entry" <<<"$setup_args" | wc -l) -eq 1 ]] || return 1
  [[ $(grep -oF -- "$entry" <<<"$query_args" | wc -l) -eq 1 ]] || return 1
  [[ $(grep -oF -- "$DUCKDB_IMAGE" <<<"$setup_args" | wc -l) -eq 1 ]] || return 1
  [[ $(grep -oF -- "$DUCKDB_IMAGE" <<<"$query_args" | wc -l) -eq 1 ]] || return 1
  [[ $setup_args == *"$DUCKDB_IMAGE -init /dev/null -noheader -list" ]] || return 1
  [[ $query_args == *"$DUCKDB_IMAGE -init /dev/null -noheader -list" ]] || return 1

  # Isolation invariants: private stdin, caller uid/gid, ephemeral tmpfs HOME,
  # explicit extension cache, and the compose network for queries only.
  [[ $setup_args == *"-i --entrypoint"* ]] || return 1
  [[ $setup_args == *"--user $(id -u):$(id -g)"* ]] || return 1
  [[ $setup_args == *"--tmpfs /duckdb-home:rw,mode=1777"* ]] || return 1
  [[ $setup_args == *"-e HOME=/duckdb-home"* ]] || return 1
  [[ $setup_args == *"$DUCKDB_EXT_HOST:/duckdb-ext"* ]] || return 1
  [[ $setup_args != *"--network"* ]] || return 1
  [[ $query_args == *"--network $NETWORK"* ]] || return 1
)

test_stats_parser() (
  parse_stats_output $'Warning: legacy setting\n12|3|1000|2000|12|0' || return 1
  [[ ${STAT_N:-} == 12 && ${STAT_OBJECTS:-} == 3 && ${STAT_MIN_US:-} == 1000 ]] || return 1
  [[ ${STAT_MAX_US:-} == 2000 && ${STAT_DISTINCT:-} == 12 && ${STAT_BAD_BASE:-} == 0 ]] || return 1
  ! parse_stats_output "Warning only" || return 1
)

test_port_validation() (
  valid_port 3100 || return 1
  valid_port 65535 || return 1
  ! valid_port 0 || return 1
  ! valid_port 65536 || return 1
  ! valid_port 01 || return 1
  ! valid_port abc || return 1
  ! valid_port '1+1' || return 1
  ! valid_port 123456 || return 1
  ! valid_port "" || return 1

  # The old arithmetic-only path accepted "1+1" as port 2. validate_args must
  # reject expression-looking values before they reach compose or a URL.
  HTTP_PORT='1+1'
  HTTP_PORT_EXPLICIT=1
  if (validate_args) >/dev/null 2>&1; then
    return 1
  fi
  HTTP_PORT=3100
  S3_PORT=abc
  S3_PORT_EXPLICIT=1
  if (validate_args) >/dev/null 2>&1; then
    return 1
  fi
  S3_PORT=3900
  WAIT_TIMEOUT='1+1'
  if (validate_args) >/dev/null 2>&1; then
    return 1
  fi
  WAIT_TIMEOUT=180
  return 0
)

test_wait_rows_exact() (
  WAIT_TIMEOUT=3
  POLL_INTERVAL=0.02
  local dir n=0
  dir=$(mktemp -d) || return 1
  read_stats() {
    n=$((n + 1))
    printf 'call\n' >>"$dir/calls"
    if ((n < 3)); then
      STAT_N=5; STAT_OBJECTS=1; STAT_MIN_US=1; STAT_MAX_US=2; STAT_DISTINCT=5; STAT_BAD_BASE=0
    else
      STAT_N=10; STAT_OBJECTS=2; STAT_MIN_US=1; STAT_MAX_US=2; STAT_DISTINCT=10; STAT_BAD_BASE=0
    fi
    return 0
  }
  wait_rows 10 2 "self-test exact count" || { rm -rf "$dir"; return 1; }
  ((${n:-0} >= 3)) || { rm -rf "$dir"; return 1; }
  rm -rf "$dir"
)

test_wait_rows_timeout() (
  WAIT_TIMEOUT=1
  POLL_INTERVAL=0.02
  local err
  err=$(mktemp) || return 1
  read_stats() {
    STAT_N=5; STAT_OBJECTS=1; STAT_MIN_US=1; STAT_MAX_US=2; STAT_DISTINCT=5; STAT_BAD_BASE=0
    return 0
  }
  if (wait_rows 10 0 "self-test timeout") 2>"$err"; then
    rm -f "$err"
    return 1
  fi
  grep -q "timed out" "$err" || { cat "$err" >&2; rm -f "$err"; return 1; }
  rm -f "$err"
)

test_wait_compacted() (
  WAIT_TIMEOUT=3
  POLL_INTERVAL=0.02
  local n=0
  read_stats() {
    n=$((n + 1))
    case $n in
      1) STAT_N=20; STAT_OBJECTS=4; STAT_MIN_US=100; STAT_MAX_US=200; STAT_DISTINCT=20; STAT_BAD_BASE=0 ;;
      2) STAT_N=10; STAT_OBJECTS=3; STAT_MIN_US=100; STAT_MAX_US=200; STAT_DISTINCT=10; STAT_BAD_BASE=0 ;;
      3) STAT_N=10; STAT_OBJECTS=2; STAT_MIN_US=100; STAT_MAX_US=201; STAT_DISTINCT=10; STAT_BAD_BASE=0 ;;
      4 | 5) STAT_N=10; STAT_OBJECTS=2; STAT_MIN_US=100; STAT_MAX_US=200; STAT_DISTINCT=10; STAT_BAD_BASE=0 ;;
      *) STAT_N=10; STAT_OBJECTS=2; STAT_MIN_US=100; STAT_MAX_US=200; STAT_DISTINCT=10; STAT_BAD_BASE=0 ;;
    esac
    return 0
  }
  wait_compacted 10 3 100 200 || return 1
  ((${n:-0} >= 5))
)

test_bounds() (
  check_initial_bounds 1000000 4000000 900000 2100000 1500 || return 1
  if (check_initial_bounds 1000000 5000000 900000 2100000 1500) >/dev/null 2>&1; then
    return 1
  fi
  if (check_initial_bounds 4000000 1000000 900000 2100000 1500) >/dev/null 2>&1; then
    return 1
  fi
)

test_schema() (
  local good bad
  WORK=$(mktemp -d) || return 1
  good=$'_ts|TIMESTAMP WITH TIME ZONE|YES|PRI||\nline|VARCHAR|YES|||\nlabels|MAP(VARCHAR, VARCHAR)|YES|||\nmeta|MAP(VARCHAR, VARCHAR)|YES|||\nlevel|VARCHAR|YES|||'
  bad=$'_ts|TIMESTAMP_NS|YES|PRI||\nline|VARCHAR|YES|||\nlabels|MAP(VARCHAR, VARCHAR)|YES|||\nmeta|MAP(VARCHAR, VARCHAR)|YES|||'
  duckdb_sql() { printf '%s\n' "$good"; }
  validate_schema || { rm -rf "$WORK"; return 1; }
  duckdb_sql() { printf '%s\n' "$bad"; }
  if (validate_schema) >/dev/null 2>&1; then
    rm -rf "$WORK"
    return 1
  fi
  rm -rf "$WORK"
)

test_project_name() {
  valid_project_name "parkana-e2e-1234" || return 1
  valid_project_name "a" || return 1
  ! valid_project_name "Bad Name" || return 1
  ! valid_project_name "-leading" || return 1
}

test_cleanup_scope() (
  local dir
  dir=$(mktemp -d) || return 1
  teardown() {
    printf 'teardown\n' >>"$dir/calls"
    return 0
  }
  STACK_CREATED=0
  KEEP=0
  cleanup_stack
  [[ ! -s $dir/calls ]] || { rm -rf "$dir"; return 1; }
  STACK_CREATED=1
  KEEP=1
  cleanup_stack
  [[ ! -s $dir/calls ]] || { rm -rf "$dir"; return 1; }
  STACK_CREATED=1
  KEEP=0
  cleanup_stack
  [[ $(wc -l <"$dir/calls") -eq 1 ]] || { rm -rf "$dir"; return 1; }

  # A failed teardown must propagate instead of being swallowed.
  teardown() {
    printf 'teardown\n' >>"$dir/calls"
    return 1
  }
  STACK_CREATED=1
  KEEP=0
  if cleanup_stack; then
    rm -rf "$dir"
    return 1
  fi
  [[ $(wc -l <"$dir/calls") -eq 2 ]] || { rm -rf "$dir"; return 1; }
  rm -rf "$dir"
)

test_finish_status() {
  [[ $(finish_status 0 0) == 0 ]] || return 1
  [[ $(finish_status 0 1) == 1 ]] || return 1
  [[ $(finish_status 1 0) == 1 ]] || return 1
  [[ $(finish_status 130 1) == 130 ]] || return 1
  [[ $(finish_status 143 1) == 143 ]] || return 1
}

test_teardown_args() (
  local dir
  dir=$(mktemp -d) || return 1
  STACK_CREATED=1
  PROJECT=test-project
  COMPOSE=(docker compose -p test-project -f compose.yaml)
  PHASE_ENV=()
  DOWN_TIMEOUT=1
  run_bounded() {
    printf '%s\n' "$*" >>"$dir/args"
    return 0
  }
  log() { :; }
  teardown || { rm -rf "$dir"; return 1; }
  local args
  args=$(cat "$dir/args" 2>/dev/null || true)
  [[ $args == *"test-project"* ]] || { rm -rf "$dir"; return 1; }
  [[ $args == *"down --volumes --remove-orphans"* ]] || { rm -rf "$dir"; return 1; }
  [[ $args != *"--rmi"* && $args != *"prune"* && $args != *"system"* ]] || { rm -rf "$dir"; return 1; }
  rm -rf "$dir"
)

self_test() {
  local failed=0
  printf '[e2e] self-test: offline logic checks\n'
  self_case "SQL literal escaping" test_sql_lit || failed=1
  self_case "credential redaction" test_redact || failed=1
  self_case "exact burst split" test_burst_split || failed=1
  self_case "extra-row memtable bound" test_extra_limit || failed=1
  self_case "DuckDB READY marker ignores setup noise" test_duckdb_ready || failed=1
  self_case "DuckDB container argv names the entrypoint, no appended binary" test_duckdb_container_argv || failed=1
  self_case "stats parser ignores warning noise" test_stats_parser || failed=1
  self_case "numeric args reject expressions/out-of-range" test_port_validation || failed=1
  self_case "exact-count wait retries then passes" test_wait_rows_exact || failed=1
  self_case "exact-count wait times out" test_wait_rows_timeout || failed=1
  self_case "compaction wait rejects double count/bad files/minmax" test_wait_compacted || failed=1
  self_case "timestamp bounds" test_bounds || failed=1
  self_case "schema base columns and microsecond timestamp" test_schema || failed=1
  self_case "compose project names" test_project_name || failed=1
  self_case "cleanup only for a created project" test_cleanup_scope || failed=1
  self_case "cleanup failure propagates / original failure wins" test_finish_status || failed=1
  self_case "teardown scope and args" test_teardown_args || failed=1
  if ((failed)); then
    printf '[e2e] self-test: FAILED\n' >&2
    return 1
  fi
  printf '[e2e] self-test: all checks passed\n'
  return 0
}

# --- plan / prerequisites --------------------------------------------------

dry_run() {
  printf '[e2e] dry run: no Docker, DuckDB or services are touched\n'
  printf '  repo root       : %s\n' "$REPO_ROOT"
  printf '  compose file    : %s\n' "$COMPOSE_FILE"
  printf '  project         : %s%s\n' "$PROJECT" "$([[ $PROJECT_EXPLICIT == 1 ]] && printf ' (explicit)' || printf ' (generated)')"
  if ((HTTP_PORT_EXPLICIT || S3_PORT_EXPLICIT)); then
    printf '  host ports      : parkana=%s garage=%s (explicit)\n' "$HTTP_PORT" "$S3_PORT"
  else
    printf '  host ports      : parkana=%s garage=%s (probed at run time; busy preferred ports auto-pick)\n' "$HTTP_PORT" "$S3_PORT"
  fi
  printf '  initial load    : %s rows in %s bursts, rate=%s/s, batch=%s, formats=protobuf,json,protobuf...\n' \
    "$TOTAL" "$BURSTS" "$RATE" "$BATCH"
  printf '  crash phase     : %s extra rows (max %s), SIGKILL of parkana only\n' "$EXTRA" "$MAX_EXTRA"
  local duckdb_plan
  case $DUCKDB_MODE_PREF in
    native) duckdb_plan="host CLI (forced)" ;;
    container) duckdb_plan="container image $DUCKDB_IMAGE (forced)" ;;
    *)
      if command -v duckdb >/dev/null 2>&1 && [[ -n ${HOME:-} && -d ${HOME:-} ]]; then
        duckdb_plan="host CLI"
      else
        duckdb_plan="container image $DUCKDB_IMAGE (pinned fallback)"
      fi
      ;;
  esac
  printf '  duckdb          : %s\n' "$duckdb_plan"
  printf '  credentials     : PARKANA_DEV_GARAGE_KEY_ID/SECRET (values hidden)\n'
  printf '  phases:\n'
  printf '    1 load       flush=1s upload=1s upload-min=1KiB max-age=1s compactor=1h target=1B delete=2s\n'
  printf '    2 compact    flush=1s upload=1s upload-min=1KiB max-age=1s compactor=1s target=1GiB delete=2s\n'
  printf '    3a long      flush=1h upload=1h upload-min=1GiB max-age=1h compactor=1h target=1B delete=2s\n'
  printf '    3b recover   flush=1s upload=1s upload-min=1KiB max-age=1s compactor=1h target=1B delete=2s\n'
  printf '  cleanup         : compose project %s only; images remain; --keep leaves the stack\n' "$PROJECT"

  local required=(
    "$COMPOSE_FILE"
    "$COMPOSE_DIR/garage.toml"
    "$COMPOSE_DIR/garage-init.sh"
    "$COMPOSE_DIR/garage.Dockerfile"
    "$REPO_ROOT/Dockerfile"
    "$REPO_ROOT/cmd/pushgen/main.go"
  )
  local f missing_files=()
  for f in "${required[@]}"; do
    [[ -e $f ]] || missing_files+=("$f")
  done
  if ((${#missing_files[@]} > 0)); then
    printf '  WARN missing files (expected before integration): %s\n' "${missing_files[*]}"
  fi

  if [[ -f $COMPOSE_FILE ]]; then
    local vars=("${COMPOSE_CONTRACT[@]}") missing_vars=() v
    for v in "${vars[@]}"; do
      compose_uses_var "$v" || missing_vars+=("$v")
    done
    if ((${#missing_vars[@]} > 0)); then
      printf '  WARN compose file does not interpolate: %s\n' "${missing_vars[*]}"
    else
      printf '  compose contract: all %s variables interpolated\n' "${#vars[@]}"
    fi
  fi
  printf '[e2e] dry run: no changes made\n'
}

# Variables the compose file must interpolate (deploy owner contract).
COMPOSE_CONTRACT=(
  PARKANA_HTTP_PORT
  PARKANA_S3_PORT
  PARKANA_INGEST_FLUSH_INTERVAL
  PARKANA_INGEST_UPLOAD_INTERVAL
  PARKANA_INGEST_UPLOAD_MIN
  PARKANA_INGEST_UPLOAD_MAX_AGE
  PARKANA_COMPACTOR_INTERVAL
  PARKANA_COMPACTOR_TARGET_SIZE
  PARKANA_COMPACTOR_DELETE_DELAY
)

compose_uses_var() {
  grep -Fq "\${$1" "$COMPOSE_FILE" 2>/dev/null || grep -Fq "\$$1" "$COMPOSE_FILE" 2>/dev/null
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

preflight() {
  local cmd
  # ss is optional (port_free falls back to /dev/tcp); the rest are required.
  for cmd in docker go curl timeout date grep sed tail head cat sleep env mktemp mkdir chmod rm id; do
    require_cmd "$cmd"
  done
  [[ $(date -u +%s%N) =~ ^[0-9]+$ ]] || fail "date does not support +%N (GNU coreutils required)"
  [[ $(date -u -d @0 +%s 2>/dev/null) == 0 ]] || fail "date does not support -d @epoch (GNU coreutils required)"

  local f
  for f in \
    "$COMPOSE_FILE" \
    "$COMPOSE_DIR/garage.toml" \
    "$COMPOSE_DIR/garage-init.sh" \
    "$COMPOSE_DIR/garage.Dockerfile" \
    "$REPO_ROOT/Dockerfile" \
    "$REPO_ROOT/cmd/pushgen/main.go"; do
    [[ -f $f ]] || fail "missing required file: $f (is this an integrated A+B branch?)"
  done

  local v missing=()
  for v in "${COMPOSE_CONTRACT[@]}"; do
    compose_uses_var "$v" || missing+=("$v")
  done
  ((${#missing[@]} == 0)) ||
    fail "deploy/compose/compose.yaml must interpolate: ${missing[*]} (phase 1 compose contract)"

  run_bounded 30 docker compose version >/dev/null 2>&1 || fail "docker compose v2 plugin is required"
  run_bounded 30 docker info >/dev/null 2>&1 || fail "docker daemon is not reachable"
}

check_project_free() {
  local project=$1 out
  if ! out=$(run_bounded 30 docker ps -aq --filter "label=com.docker.compose.project=$project"); then
    fail "docker ps failed while checking project '$project'"
  fi
  [[ -z $out ]] || fail "compose project '$project' already has containers; refusing to reuse it"

  if ! out=$(run_bounded 30 docker volume ls -q --filter "label=com.docker.compose.project=$project"); then
    fail "docker volume ls failed while checking project '$project'"
  fi
  [[ -z $out ]] || fail "compose project '$project' already has volumes; refusing to reuse it"

  if ! out=$(run_bounded 30 docker network ls -q --filter "label=com.docker.compose.project=$project"); then
    fail "docker network ls failed while checking project '$project'"
  fi
  [[ -z $out ]] || fail "compose project '$project' already has networks; refusing to reuse it"

  # Also guard the exact volume names the compose file declares, even if a
  # stale volume lost its project label (that could hold user data).
  local vol
  for vol in parkana-data garage-meta garage-data; do
    if run_bounded 10 docker volume inspect "${project}_${vol}" >/dev/null 2>&1; then
      fail "volume '${project}_${vol}' already exists; refusing to reuse project '$project'"
    fi
  done
}

resolve_ports() {
  local http s3
  http=$(choose_port "$HTTP_PORT" "$HTTP_PORT_EXPLICIT" "Parkana HTTP" PARKANA_HTTP_PORT) || exit 1
  HTTP_PORT=$http
  s3=$(choose_port "$S3_PORT" "$S3_PORT_EXPLICIT" "Garage S3" PARKANA_S3_PORT) || exit 1
  [[ $http != "$s3" ]] || fail "resolved Parkana and Garage ports are both $http"
  S3_PORT=$s3
  export PARKANA_HTTP_PORT=$HTTP_PORT
  export PARKANA_S3_PORT=$S3_PORT
  log "host ports: parkana=$HTTP_PORT garage=$S3_PORT"
}

build_pushgen() {
  log "building cmd/pushgen"
  if ! (cd -- "$REPO_ROOT" && timeout "$BUILD_TIMEOUT" go build -o "$WORK/pushgen" ./cmd/pushgen) \
    >"$WORK/go-build.log" 2>&1; then
    print_file_tail "$WORK/go-build.log" 40
    fail "go build ./cmd/pushgen failed"
  fi
  [[ -x $WORK/pushgen ]] || fail "go build did not produce $WORK/pushgen"
}

build_images() {
  log "building compose images (the first build can take a while)"
  if ! compose_do "$BUILD_TIMEOUT" build >"$WORK/compose-build.log" 2>&1; then
    print_file_tail "$WORK/compose-build.log" 60
    fail "docker compose build failed"
  fi
}

# --- DuckDB ----------------------------------------------------------------

duckdb_ext_path() {
  if [[ $DUCKDB_MODE == native ]]; then
    printf '%s' "$DUCKDB_EXT_HOST"
  else
    printf '%s' "$DUCKDB_EXT_CONTAINER"
  fi
}

duckdb_ready() { # output -> true when the READY marker is present
  local out=${1//$'\r'/}
  printf '%s\n' "$out" | grep -qx 'READY'
}

duckdb_setup_exec() {
  if [[ $DUCKDB_MODE == native ]]; then
    (
      exec timeout "$DUCKDB_INSTALL_TIMEOUT" duckdb -init /dev/null -noheader -list
    )
  else
    run_bounded "$DUCKDB_INSTALL_TIMEOUT" docker run --rm -i \
      --entrypoint "$DUCKDB_CONTAINER_BIN" \
      --user "$(id -u):$(id -g)" \
      --tmpfs "$DUCKDB_HOME_CONTAINER:rw,mode=1777" \
      -e HOME="$DUCKDB_HOME_CONTAINER" \
      -v "$DUCKDB_EXT_HOST:$DUCKDB_EXT_CONTAINER" \
      "$DUCKDB_IMAGE" -init /dev/null -noheader -list
  fi
}

duckdb_query_exec() {
  if [[ $DUCKDB_MODE == native ]]; then
    (
      exec timeout "$DUCKDB_TIMEOUT" duckdb -init /dev/null -noheader -list
    )
  else
    [[ -n $NETWORK ]] || fail "internal: DuckDB container has no compose network"
    run_bounded "$DUCKDB_TIMEOUT" docker run --rm -i \
      --network "$NETWORK" \
      --entrypoint "$DUCKDB_CONTAINER_BIN" \
      --user "$(id -u):$(id -g)" \
      --tmpfs "$DUCKDB_HOME_CONTAINER:rw,mode=1777" \
      -e HOME="$DUCKDB_HOME_CONTAINER" \
      -v "$DUCKDB_EXT_HOST:$DUCKDB_EXT_CONTAINER" \
      "$DUCKDB_IMAGE" -init /dev/null -noheader -list
  fi
}

duckdb_init() {
  local ver
  case $DUCKDB_MODE_PREF in
    auto)
      if command -v duckdb >/dev/null 2>&1 && [[ -n ${HOME:-} && -d ${HOME:-} ]]; then
        DUCKDB_MODE=native
      else
        DUCKDB_MODE=container
      fi
      ;;
    native)
      require_cmd duckdb
      DUCKDB_MODE=native
      ;;
    container) DUCKDB_MODE=container ;;
  esac

  if [[ $DUCKDB_MODE == native ]]; then
    # DuckDB resolves a home directory before INSTALL even with an explicit
    # extension_directory; native runs use the caller's real HOME unchanged.
    if [[ -z ${HOME:-} || ! -d ${HOME:-} ]]; then
      fail "native DuckDB needs a resolvable HOME; set HOME or use PARKANA_E2E_DUCKDB_MODE=container"
    fi
    if ! ver=$(run_bounded 15 duckdb --version 2>"$WORK/duckdb-version.err"); then
      print_file_tail "$WORK/duckdb-version.err" 20
      fail "duckdb CLI is present but not runnable"
    fi
    log "duckdb: host CLI (${ver%%$'\n'*})"
  else
    require_cmd docker
    if ! run_bounded 30 docker image inspect "$DUCKDB_IMAGE" >/dev/null 2>&1; then
      log "pulling pinned DuckDB image $DUCKDB_IMAGE"
      if ! run_bounded "$PULL_TIMEOUT" docker pull "$DUCKDB_IMAGE" >"$WORK/duckdb-pull.log" 2>&1; then
        print_file_tail "$WORK/duckdb-pull.log" 20
        fail "cannot pull DuckDB image $DUCKDB_IMAGE (set PARKANA_E2E_DUCKDB_IMAGE to a verified tag)"
      fi
    fi
    # The image has no Entrypoint (see DUCKDB_CONTAINER_BIN). Keep its stderr in
    # a bounded log and show it on failure instead of discarding the reason.
    if ! ver=$(run_bounded 60 docker run --rm --entrypoint "$DUCKDB_CONTAINER_BIN" \
      "$DUCKDB_IMAGE" --version 2>"$WORK/duckdb-version.err"); then
      print_file_tail "$WORK/duckdb-version.err" 20
      fail "DuckDB image $DUCKDB_IMAGE does not run $DUCKDB_CONTAINER_BIN --version (set PARKANA_E2E_DUCKDB_IMAGE to a verified duckdb/duckdb tag)"
    fi
    log "duckdb: container image $DUCKDB_IMAGE (${ver%%$'\n'*})"
  fi

  DUCKDB_EXT_HOST="$WORK/duckdb-ext"
  mkdir -p "$DUCKDB_EXT_HOST"
  chmod 700 "$DUCKDB_EXT_HOST"

  local err="$WORK/duckdb-setup.err" out ext
  ext=$(sql_lit "$(duckdb_ext_path)")
  if out=$(printf '.bail on\nSET extension_directory=%s;\nLOAD httpfs;\nSELECT %s;\n' "$ext" "'READY'" |
    duckdb_setup_exec 2>"$err") && duckdb_ready "$out"; then
    return 0
  fi
  log "installing DuckDB httpfs extension (network required once)"
  if ! out=$(printf '.bail on\nSET extension_directory=%s;\nINSTALL httpfs;\nLOAD httpfs;\nSELECT %s;\n' "$ext" "'READY'" |
    duckdb_setup_exec 2>"$err") ||
    ! duckdb_ready "$out"; then
    print_file_tail "$err" 10
    fail "DuckDB httpfs setup failed"
  fi
}

discover_network() {
  [[ $DUCKDB_MODE == container ]] || return 0
  if [[ -n $NETWORK ]]; then
    return 0
  fi
  NETWORK=$(run_bounded 30 docker network ls -q \
    --filter "label=com.docker.compose.project=$PROJECT" \
    --filter "label=com.docker.compose.network=default" | head -n1) || true
  if [[ -z $NETWORK ]] && run_bounded 10 docker network inspect "${PROJECT}_default" >/dev/null 2>&1; then
    NETWORK="${PROJECT}_default"
  fi
  [[ -n $NETWORK ]] || fail "cannot find the compose network for project '$PROJECT'"
}

duckdb_sql() {
  local body=$1 prelude ext
  ext=$(sql_lit "$(duckdb_ext_path)")
  prelude=".bail on
SET extension_directory=$ext;
LOAD httpfs;
SET TimeZone='UTC';
SET s3_endpoint=$(sql_lit "$DUCK_ENDPOINT");
SET s3_region=$(sql_lit "$S3_REGION");
SET s3_url_style='path';
SET s3_use_ssl=false;
SET s3_access_key_id=$(sql_lit "$ACCESS_KEY");
SET s3_secret_access_key=$(sql_lit "$SECRET_KEY");"
  printf '%s\n%s\n' "$prelude" "$body" | duckdb_query_exec
}

s3_glob() {
  printf 's3://%s/%s/logs/%s/*/*/*/*/*.parquet' "$S3_BUCKET" "$TENANT" "$STREAM"
}

stats_sql() {
  local glob
  glob=$(sql_lit "$(s3_glob)")
  cat <<SQL
WITH r AS (
  SELECT _ts, line, labels, meta, filename
  FROM read_parquet($glob, filename=true)
)
SELECT (SELECT count(*) FROM r),
       (SELECT count(DISTINCT filename) FROM r),
       (SELECT min(epoch_us(_ts)) FROM r),
       (SELECT max(epoch_us(_ts)) FROM r),
       (SELECT count(*) FROM (SELECT DISTINCT _ts, line FROM r)),
       (SELECT count(*) FROM r WHERE _ts IS NULL OR line IS NULL OR labels IS NULL OR meta IS NULL);
SQL
}

parse_stats_output() { # output -> STAT_*; false when no six-integer row
  local out=$1 line="" candidate
  while IFS= read -r candidate; do
    if [[ $candidate =~ ^[0-9]+\|[0-9]+\|[0-9]+\|[0-9]+\|[0-9]+\|[0-9]+$ ]]; then
      line=$candidate
      break
    fi
  done <<<"$out"
  [[ -n $line ]] || return 1
  IFS='|' read -r STAT_N STAT_OBJECTS STAT_MIN_US STAT_MAX_US STAT_DISTINCT STAT_BAD_BASE <<<"$line"
  return 0
}

read_stats() {
  local out
  LAST_DUCK_ERR=""
  if ! out=$(duckdb_sql "$(stats_sql)" 2>"$WORK/duck.err"); then
    LAST_DUCK_ERR=$(redact <"$WORK/duck.err" | tail -n 3)
    return 1
  fi
  parse_stats_output "$out" || {
    LAST_DUCK_ERR="unexpected DuckDB stats output: $out"
    return 1
  }
  return 0
}

validate_schema() {
  local out line name type rest
  if ! out=$(duckdb_sql "DESCRIBE SELECT * FROM read_parquet($(sql_lit "$(s3_glob)"));" 2>"$WORK/schema.err"); then
    LAST_DUCK_ERR=$(redact <"$WORK/schema.err" | tail -n 3)
    fail "schema query failed: $LAST_DUCK_ERR"
  fi
  local -a names=() types=()
  while IFS='|' read -r name type rest; do
    [[ $name =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
    names+=("$name")
    types+=("$type")
  done <<<"$out"

  ((${#names[@]} >= 4)) || fail "schema has ${#names[@]} columns; want the 4 base columns"
  [[ ${names[0]} == _ts && ${names[1]} == line && ${names[2]} == labels && ${names[3]} == meta ]] ||
    fail "base columns are '${names[*]:0:4}'; want _ts,line,labels,meta"
  [[ ${types[0]} == "TIMESTAMP WITH TIME ZONE" ]] ||
    fail "_ts type is '${types[0]}'; want TIMESTAMP WITH TIME ZONE (UTC microseconds)"
  [[ ${types[1]} == VARCHAR ]] || fail "line type is '${types[1]}'; want VARCHAR"
  [[ ${types[2]} == "MAP(VARCHAR, VARCHAR)" && ${types[3]} == "MAP(VARCHAR, VARCHAR)" ]] ||
    fail "labels/meta types are '${types[2]}'/'${types[3]}'; want MAP(VARCHAR, VARCHAR)"

  local i
  for ((i = 4; i < ${#names[@]}; i++)); do
    [[ ${names[i]} != _* ]] || fail "unexpected reserved column '${names[i]}'"
    [[ ${types[i]} == VARCHAR ]] || fail "promoted column '${names[i]}' type is '${types[i]}'; want VARCHAR"
  done
}

# --- waits -----------------------------------------------------------------

wait_rows() { # want [min_objects] [label]
  local want=$1 min_objects=${2:-0} label=${3:-rows}
  local deadline=$((SECONDS + WAIT_TIMEOUT))
  while ((SECONDS < deadline)); do
    if read_stats; then
      if ((STAT_N == want && STAT_DISTINCT == want && STAT_BAD_BASE == 0 && STAT_OBJECTS >= min_objects)); then
        return 0
      fi
    fi
    sleep "$POLL_INTERVAL"
  done
  if read_stats; then
    fail "timed out waiting for $label: want n=$want distinct=$want min_objects=$min_objects, got n=$STAT_N objects=$STAT_OBJECTS distinct=$STAT_DISTINCT bad_base=$STAT_BAD_BASE (last DuckDB error: ${LAST_DUCK_ERR:-none})"
  fi
  fail "timed out waiting for $label: want n=$want (last DuckDB error: ${LAST_DUCK_ERR:-none})"
}

wait_compacted() { # want_n prev_objects want_min want_max
  local want=$1 prev_objects=$2 want_min=$3 want_max=$4
  local deadline=$((SECONDS + WAIT_TIMEOUT)) previous=""
  while ((SECONDS < deadline)); do
    if read_stats &&
      ((STAT_N == want && STAT_DISTINCT == want && STAT_BAD_BASE == 0 &&
        STAT_OBJECTS < prev_objects && STAT_MIN_US == want_min && STAT_MAX_US == want_max)); then
      local signature="$STAT_N:$STAT_OBJECTS:$STAT_MIN_US:$STAT_MAX_US"
      if [[ $signature == "$previous" ]]; then
        return 0
      fi
      previous=$signature
    else
      previous=""
    fi
    sleep "$POLL_INTERVAL"
  done
  if read_stats; then
    fail "timed out waiting for compaction: want n=$want objects<$prev_objects min=$want_min max=$want_max, got n=$STAT_N objects=$STAT_OBJECTS min=$STAT_MIN_US max=$STAT_MAX_US distinct=$STAT_DISTINCT bad_base=$STAT_BAD_BASE (last DuckDB error: ${LAST_DUCK_ERR:-none})"
  fi
  fail "timed out waiting for compaction (last DuckDB error: ${LAST_DUCK_ERR:-none})"
}

wait_past() { # timestamp_us; wait until the wall clock is past it (bounded)
  local target=$1 now wait_s deadline
  now=$(now_us)
  wait_s=$(((target - now) / 1000000 + 10))
  if ((wait_s < 10)); then
    wait_s=10
  fi
  deadline=$((SECONDS + wait_s))
  while :; do
    now=$(now_us)
    if ((now > target + SLACK_US)); then
      return 0
    fi
    if ((SECONDS >= deadline)); then
      fail "wall clock did not pass the recorded max timestamp within ${wait_s}s"
    fi
    sleep 0.2
  done
}

check_initial_bounds() { # min max t0 t1 largest_burst
  local min=$1 max=$2 t0=$3 t1=$4 largest=$5
  ((min <= max)) || fail "min timestamp $min is after max timestamp $max"
  ((min >= t0 - SLACK_US)) || fail "min timestamp $min is before the first push started ($t0)"
  ((max <= t1 + largest * 1000 + SLACK_US)) ||
    fail "max timestamp $max is after the last push plus its synthetic span ($t1 + ${largest}ms)"
}

# --- compose lifecycle -----------------------------------------------------

wait_garage() {
  local deadline=$((SECONDS + READY_TIMEOUT)) id health
  while ((SECONDS < deadline)); do
    id=$(compose_do 30 ps -q garage 2>/dev/null || true)
    if [[ -n $id ]]; then
      health=$(run_bounded 10 docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$id" 2>/dev/null || true)
      if [[ $health == healthy ]]; then
        return 0
      fi
      if [[ $health == exited || $health == dead ]]; then
        fail "garage container is '$health' before becoming healthy"
      fi
    fi
    sleep 2
  done
  fail "garage did not become healthy within ${READY_TIMEOUT}s"
}

wait_garage_init() {
  local deadline=$((SECONDS + READY_TIMEOUT)) id state
  while ((SECONDS < deadline)); do
    id=$(compose_do 30 ps -a -q garage-init 2>/dev/null || true)
    if [[ -n $id ]]; then
      state=$(run_bounded 10 docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$id" 2>/dev/null || true)
      case $state in
        "exited 0") return 0 ;;
        "exited "*) fail "garage-init failed ($state)" ;;
      esac
    fi
    sleep 2
  done
  fail "garage-init did not exit 0 within ${READY_TIMEOUT}s"
}

wait_ready() {
  local deadline=$((SECONDS + READY_TIMEOUT)) id state
  while ((SECONDS < deadline)); do
    if curl -fsS --max-time 5 "http://127.0.0.1:$HTTP_PORT/ready" >/dev/null 2>&1; then
      return 0
    fi
    id=$(compose_do 20 ps -q parkana 2>/dev/null || true)
    if [[ -n $id ]]; then
      state=$(run_bounded 10 docker inspect -f '{{.State.Status}}' "$id" 2>/dev/null || true)
      if [[ $state == exited || $state == dead ]]; then
        fail "parkana container is '$state' before becoming ready"
      fi
    fi
    sleep 1
  done
  fail "parkana /ready did not return 200 within ${READY_TIMEOUT}s"
}

wait_killed() {
  local id=$1 deadline=$((SECONDS + 30)) state
  while ((SECONDS < deadline)); do
    state=$(run_bounded 10 docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$id" 2>/dev/null || true)
    case $state in
      "exited 137" | "dead 137") return 0 ;;
      "exited "*) fail "parkana exited with '$state' after SIGKILL; want exit code 137" ;;
    esac
    sleep 0.2
  done
  fail "parkana did not report a SIGKILL exit (137) within 30s (last state: ${state:-unknown})"
}

assert_published_port() {
  local service=$1 container_port=$2 want=$3 out got
  out=$(compose_do 30 port "$service" "$container_port" 2>/dev/null) || fail "cannot read published port for $service:$container_port"
  out=${out%%$'\n'*}
  got=${out##*:}
  [[ $got == "$want" ]] ||
    fail "compose published $service:$container_port as '$out', want host port $want (port contract not honored)"
}

assert_parkana_env() {
  local key=$1 want=$2 id got
  id=$(compose_do 30 ps -q parkana 2>/dev/null) || fail "cannot find the parkana container"
  [[ -n $id ]] || fail "cannot find the parkana container"
  got=$(run_bounded 30 docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$id" |
    sed -n "s/^${key}=//p" | head -n1)
  [[ $got == "$want" ]] ||
    fail "parkana env $key is '${got:-<unset>}', want '$want' (compose does not interpolate $key; deploy contract missing)"
}

up_stack() {
  if ! compose_do "$UP_TIMEOUT" up -d >"$WORK/up.log" 2>&1; then
    print_file_tail "$WORK/up.log" 40
    fail "docker compose up failed"
  fi
}

recreate_parkana() {
  local logfile=$1
  if ! compose_do "$UP_TIMEOUT" up -d --no-deps --force-recreate parkana >"$logfile" 2>&1; then
    print_file_tail "$logfile" 40
    fail "recreating the parkana container failed"
  fi
  wait_ready
}

push_burst() { # format total
  local format=$1 total=$2 logfile="$WORK/pushgen-$1-$2.log"
  if ! run_bounded "$PUSH_TIMEOUT" "$WORK/pushgen" \
    -url="http://127.0.0.1:$HTTP_PORT/loki/api/v1/push" \
    -format="$format" \
    -stream="$STREAM" \
    -total="$total" \
    -rate="$RATE" \
    -batch="$BATCH" >"$logfile" 2>&1; then
    printf '[e2e] pushgen %s burst (%s rows) failed; last output:\n' "$format" "$total" >&2
    print_file_tail "$logfile" 20
    return 1
  fi
  return 0
}

# --- phases ----------------------------------------------------------------

phase_initial() {
  log "phase 1: ${TOTAL} rows in ${BURSTS} alternating-format bursts (compactor idle)"
  # A 1-byte target makes even a racing startup compactor cycle skip every
  # real Parquet file, so nothing can be merged before the deterministic
  # compaction phase.
  set_phase_env 1s 1s 1KiB 1s 1h 1B 2s

  STACK_CREATED=1
  up_stack
  wait_garage
  wait_garage_init
  wait_ready
  assert_published_port garage "$GARAGE_SERVICE_PORT" "$S3_PORT"
  assert_published_port parkana "$PARKANA_SERVICE_PORT" "$HTTP_PORT"
  assert_parkana_env PARKANA_INGEST_FLUSH_INTERVAL 1s
  assert_parkana_env PARKANA_INGEST_UPLOAD_INTERVAL 1s
  assert_parkana_env PARKANA_INGEST_UPLOAD_MIN 1KiB
  assert_parkana_env PARKANA_INGEST_UPLOAD_MAX_AGE 1s
  assert_parkana_env PARKANA_COMPACTOR_INTERVAL 1h
  assert_parkana_env PARKANA_COMPACTOR_TARGET_SIZE 1B
  assert_parkana_env PARKANA_COMPACTOR_DELETE_DELAY 2s

  if [[ $DUCKDB_MODE == native ]]; then
    DUCK_ENDPOINT="127.0.0.1:$S3_PORT"
  else
    discover_network
    DUCK_ENDPOINT=$S3_SERVICE_ENDPOINT
  fi
  log "duckdb endpoint: $DUCK_ENDPOINT (bucket $S3_BUCKET, region $S3_REGION, path style, no TLS)"

  local cumulative=0 previous_objects=0 previous_max=0 i total format
  for ((i = 0; i < BURSTS; i++)); do
    total=$(burst_size "$TOTAL" "$BURSTS" "$i")
    if ((i % 2 == 0)); then format=protobuf; else format=json; fi
    if ((i == 0)); then
      T0_US=$(now_us)
    else
      # pushgen stamps a run from its own start clock and can put its synthetic
      # range ahead of the wall clock. Waiting past the previous observed max
      # guarantees that no two initial bursts share a timestamp, so the
      # distinct (_ts, line) count is deterministic instead of luck.
      log "waiting past the previous synthetic max before burst $((i + 1))"
      wait_past "$previous_max"
    fi
    push_burst "$format" "$total" || fail "pushgen burst $((i + 1)) failed"
    T1_US=$(now_us)
    cumulative=$((cumulative + total))
    log "burst $((i + 1))/$BURSTS: $format $total rows acknowledged (204); waiting for upload"
    wait_rows "$cumulative" "$((previous_objects + 1))" "burst $((i + 1)) upload"
    read_stats || fail "cannot read stats after burst $((i + 1)): $LAST_DUCK_ERR"
    ((STAT_OBJECTS > previous_objects)) ||
      fail "burst $((i + 1)) did not create a new object (objects=$STAT_OBJECTS)"
    previous_objects=$STAT_OBJECTS
    previous_max=$STAT_MAX_US
  done

  read_stats || fail "cannot read initial stats: $LAST_DUCK_ERR"
  N0=$STAT_N
  OBJ0=$STAT_OBJECTS
  MIN0=$STAT_MIN_US
  MAX0=$STAT_MAX_US
  ((N0 == TOTAL)) || fail "initial load has $N0 rows, want $TOTAL"
  validate_schema
  check_initial_bounds "$MIN0" "$MAX0" "$T0_US" "$T1_US" "$(((TOTAL + BURSTS - 1) / BURSTS))"
  log "phase 1 ok: $N0 rows in $OBJ0 objects, min $(fmt_us "$MIN0"), max $(fmt_us "$MAX0")"
}

phase_compaction() {
  log "phase 2: compaction (expect fewer objects, same $N0 rows and identical min/max)"
  set_phase_env 1s 1s 1KiB 1s 1s 1GiB 2s

  if ! compose_do "$STOP_TIMEOUT" stop --timeout 30 parkana >"$WORK/stop.log" 2>&1; then
    print_file_tail "$WORK/stop.log" 20
    fail "stopping parkana before compaction failed"
  fi
  recreate_parkana "$WORK/up-compact.log"
  assert_parkana_env PARKANA_COMPACTOR_INTERVAL 1s
  assert_parkana_env PARKANA_COMPACTOR_TARGET_SIZE 1GiB
  assert_parkana_env PARKANA_COMPACTOR_DELETE_DELAY 2s

  log "waiting up to ${WAIT_TIMEOUT}s for stable exact count and obsolete-object deletion"
  wait_compacted "$N0" "$OBJ0" "$MIN0" "$MAX0"
  read_stats || fail "cannot read compacted stats: $LAST_DUCK_ERR"
  ((STAT_OBJECTS < OBJ0)) || fail "compaction did not reduce the object count ($STAT_OBJECTS >= $OBJ0)"
  validate_schema
  log "phase 2 ok: $STAT_N rows in $STAT_OBJECTS objects (was $OBJ0), min/max unchanged"
}

phase_crash() {
  log "phase 3: SIGKILL recovery (long flush/upload intervals, then restart)"
  # Keep the compactor idle here too: this phase tests WAL recovery, and a
  # startup merge would create a transient old+new double-count window exactly
  # when the pre-kill "not uploaded yet" check runs.
  set_phase_env 1h 1h 1GiB 1h 1h 1B 2s

  recreate_parkana "$WORK/up-long.log"
  assert_parkana_env PARKANA_INGEST_FLUSH_INTERVAL 1h
  assert_parkana_env PARKANA_INGEST_UPLOAD_INTERVAL 1h
  assert_parkana_env PARKANA_INGEST_UPLOAD_MIN 1GiB
  assert_parkana_env PARKANA_INGEST_UPLOAD_MAX_AGE 1h
  assert_parkana_env PARKANA_COMPACTOR_INTERVAL 1h
  assert_parkana_env PARKANA_COMPACTOR_TARGET_SIZE 1B

  # The compactor is idle (1h, 1B), so this is a plain baseline wait: the
  # already-compacted data must still be exactly N0 rows before the extra load.
  wait_rows "$N0" 1 "long-interval baseline"

  # pushgen's synthetic timestamps can lead the wall clock; wait until they are
  # fully in the past so the extra rows can never reuse an existing timestamp.
  wait_past "$MAX0"

  # The compose file uses restart: unless-stopped. Disable it for this one
  # throwaway container so SIGKILL is terminal and the WAL recovery is
  # observable instead of Docker silently restarting the old process.
  local id
  id=$(compose_do 30 ps -q parkana 2>/dev/null || true)
  [[ -n $id ]] || fail "cannot find the parkana container before the crash load"
  if ! run_bounded 30 docker update --restart=no "$id" >/dev/null 2>&1; then
    fail "cannot disable parkana's restart policy before SIGKILL"
  fi

  T2_US=$(now_us)

  # One JSON burst is enough here: both wire formats were already exercised by
  # the initial load, and a single run cannot overlap its own timestamp range.
  push_burst json "$EXTRA" || fail "extra pushgen burst failed"
  T3_US=$(now_us)
  log "$EXTRA extra rows acknowledged with 204; checking they are not uploaded yet"

  # The container must still be the same live process that acknowledged them.
  local running
  running=$(run_bounded 10 docker inspect -f '{{.State.Running}}' "$id" 2>/dev/null || true)
  [[ $running == true ]] || fail "parkana is not running after acknowledging the extra rows"

  sleep 5
  read_stats || fail "cannot read stats during the long-interval phase: $LAST_DUCK_ERR"
  ((STAT_N == N0)) ||
    fail "extra rows were uploaded during the 1h-interval phase ($STAT_N rows, want $N0); long-interval contract not honored"

  if ! compose_do 30 kill --signal SIGKILL parkana >"$WORK/kill.log" 2>&1; then
    print_file_tail "$WORK/kill.log" 20
    fail "SIGKILL of the parkana container failed"
  fi
  wait_killed "$id"
  log "parkana killed with SIGKILL (exit 137); restarting the same volume with short intervals"

  # Keep the compactor idle during WAL recovery as well: this phase proves the
  # replayed rows are uploaded exactly once, not that a merge transitions
  # cleanly. Both objects coexist and the count is N0 + EXTRA.
  set_phase_env 1s 1s 1KiB 1s 1h 1B 2s
  recreate_parkana "$WORK/up-recover.log"
  assert_parkana_env PARKANA_INGEST_FLUSH_INTERVAL 1s
  assert_parkana_env PARKANA_INGEST_UPLOAD_INTERVAL 1s
  assert_parkana_env PARKANA_INGEST_UPLOAD_MIN 1KiB
  assert_parkana_env PARKANA_INGEST_UPLOAD_MAX_AGE 1s
  assert_parkana_env PARKANA_COMPACTOR_INTERVAL 1h
  assert_parkana_env PARKANA_COMPACTOR_TARGET_SIZE 1B

  local want=$((N0 + EXTRA))
  log "waiting up to ${WAIT_TIMEOUT}s for WAL replay + upload of $want rows"
  wait_rows "$want" 1 "SIGKILL recovery"
  read_stats || fail "cannot read recovered stats: $LAST_DUCK_ERR"
  ((STAT_N == want && STAT_DISTINCT == want)) || fail "recovered stats are n=$STAT_N distinct=$STAT_DISTINCT, want $want"
  ((STAT_MIN_US == MIN0)) || fail "recovered min timestamp is $STAT_MIN_US, want unchanged $MIN0"
  ((STAT_MAX_US > MAX0)) || fail "recovered max timestamp $STAT_MAX_US did not grow past $MAX0"
  ((STAT_MAX_US >= T2_US - SLACK_US)) || fail "recovered max timestamp $STAT_MAX_US predates the extra push ($T2_US)"
  ((STAT_MAX_US <= T3_US + EXTRA * 1000 + SLACK_US)) ||
    fail "recovered max timestamp $STAT_MAX_US is after the extra push plus its synthetic span"
  validate_schema
  log "phase 3 ok: $STAT_N rows in $STAT_OBJECTS objects, min $(fmt_us "$STAT_MIN_US"), max $(fmt_us "$STAT_MAX_US")"
}

summary() {
  printf '\n[e2e] PASS: phase 1 e2e checks passed\n'
  printf '  initial rows/objects : %s / %s\n' "$N0" "$OBJ0"
  printf '  recovered rows       : %s (extra %s)\n' "$((N0 + EXTRA))" "$EXTRA"
  printf '  min timestamp        : %s\n' "$(fmt_us "$MIN0")"
  printf '  project              : %s\n' "$PROJECT"
  printf '  cleanup              : project containers/network/volumes only; built images remain\n'
}

# --- diagnostics / cleanup -------------------------------------------------

diagnose() {
  [[ -n ${WORK:-} && -d ${WORK:-} ]] || return 0
  if ((STACK_CREATED == 0)); then
    printf '[e2e] no compose stack was created; nothing to inspect\n' >&2
    return 0
  fi
  {
    printf '[e2e] diagnostics for project %s (bounded, redacted)\n' "$PROJECT"
    printf '  --- docker compose ps\n'
    compose_do 30 ps -a 2>&1 || true
    printf '  --- curl /ready\n'
    curl -fsS --max-time 3 "http://127.0.0.1:$HTTP_PORT/ready" 2>&1 || true
    printf '\n  --- logs (tail 80)\n'
    compose_do 30 logs --no-color --tail=80 parkana garage garage-init 2>&1 || true
  } | redact | tail -n 250 >&2
}

teardown() {
  ((STACK_CREATED == 1)) || return 0
  if ((KEEP == 1)); then
    return 0
  fi
  log "removing compose project $PROJECT (containers, network, volumes) and leaving images"
  if ! compose_do "$DOWN_TIMEOUT" down --volumes --remove-orphans; then
    warn "docker compose down failed; the project may need manual cleanup"
    return 1
  fi
  return 0
}

cleanup_stack() {
  if ((STACK_CREATED == 0)); then
    return 0
  fi
  if ((KEEP == 1)); then
    return 0
  fi
  teardown
}

# finish_status keeps the original failure when the run failed, but reports a
# cleanup failure when the checks themselves passed (exit status 1).
finish_status() { # main_status cleanup_status
  if (($1 != 0)); then
    printf '%s' "$1"
  else
    printf '%s' "$2"
  fi
}

keep_instructions() {
  ((STACK_CREATED == 1)) || return 0
  printf '[e2e] keeping compose project %s running\n' "$PROJECT"
  printf '  Parkana: http://127.0.0.1:%s  Garage S3: 127.0.0.1:%s\n' "$HTTP_PORT" "$S3_PORT"
  printf '  teardown: docker compose -p %s --project-directory %s -f %s down --volumes --remove-orphans\n' \
    "$PROJECT" "$COMPOSE_DIR" "$COMPOSE_FILE"
}

on_exit() {
  local status=$? cleanup_status=0
  trap - EXIT INT TERM
  if ((status != 0)); then
    diagnose || true
  fi
  if ((KEEP == 1)); then
    keep_instructions || true
  else
    cleanup_stack || cleanup_status=$?
  fi
  if [[ -n ${WORK:-} && -d ${WORK:-} ]]; then
    rm -rf -- "$WORK" || true
  fi
  status=$(finish_status "$status" "$cleanup_status")
  exit "$status"
}

# --- main ------------------------------------------------------------------

run_flow() {
  preflight

  COMPOSE=(docker compose -p "$PROJECT" --project-directory "$COMPOSE_DIR" -f "$COMPOSE_FILE")
  set_phase_env 1s 1s 1KiB 1s 1h 1GiB 2s

  # Temporary state (pushgen binary, DuckDB extension cache, logs) lives here
  # and is always removed, including in --keep mode.
  WORK=$(mktemp -d "${TMPDIR:-/tmp}/parkana-e2e.XXXXXX") || fail "cannot create a temporary directory"
  chmod 700 "$WORK"
  trap on_exit EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM

  # Fail before creating anything if the project would be reused.
  check_project_free "$PROJECT"
  resolve_ports
  build_pushgen
  build_images
  duckdb_init

  phase_initial
  phase_compaction
  phase_crash
  summary
}

main() {
  init_defaults
  parse_args "$@"
  validate_args
  if ((SELFTEST == 1)); then
    self_test
    exit $?
  fi
  if ((DRY_RUN == 1)); then
    dry_run
    exit 0
  fi
  run_flow
}

main "$@"
