#!/usr/bin/env bash
#
# scripts/e2e-phase2.sh - reproducible phase 2 end-to-end wrapper.
#
# The validator (deploy/compose/validate-phase2.sh) owns the single compose
# startup, readiness, generator, exact-count, restart and cleanup flow. This
# wrapper only supplies a fresh, isolated invocation:
#
#   * a unique compose project and unused loopback ports,
#   * random throwaway Garage credentials for this run,
#   * the checked-in lokicompare corpus by default for --full, with an
#     explicit --compare-corpus/PARKANA_E2E_COMPARE_CORPUS override, optional
#     tenant and per-request timeout,
#   * one validator call, with its 0/1/2 exit status propagated unchanged.
#
# It never starts a second stack and never runs `docker compose down`: the
# validator's existing trap is the only cleanup owner, including when the
# comparison fails.
#
# The default corpus is cmd/lokicompare/testdata/grafana-corpus.json, resolved
# from this repository (not the caller's directory). An explicit flag or
# PARKANA_E2E_COMPARE_CORPUS wins over it; explicit relative paths stay
# caller-relative. Use --smoke for the stack/readiness check, which never reads
# a corpus; all readiness assertions stay in the validator.
#
# No Go, Docker or Boat VM is needed for --self-test or --dry-run. Docker and
# Go are needed by the validator when a real run reaches the comparison.
#
# Usage: scripts/e2e-phase2.sh [options]
#   --full                      full phase 2 acceptance (default)
#   --smoke                     stack/readiness validator run (no comparison)
#   --compare-corpus PATH       lokicompare corpus (default:
#                               cmd/lokicompare/testdata/grafana-corpus.json)
#   --tenant ID                 X-Scope-OrgID for lokicompare
#   --timeout DURATION          lokicompare per-request timeout (Go duration,
#                               e.g. 30s, 0.5s, +1s, 0h1m)
#   --project NAME              compose project (default: generated)
#   --parkana-port PORT         Parkana HTTP host port (default 13100)
#   --garage-port PORT          Garage S3 host port (default 13900)
#   --loki-port PORT            Loki HTTP host port (default 13101)
#   --alloy-port PORT           Alloy HTTP host port (default 13245)
#   --grafana-port PORT         Grafana HTTP host port (default 13000)
#   --keep                      leave the validator's stack for debugging
#   --dry-run                   print the resolved plan, touch nothing
#   --self-test                 offline mock tests, touch nothing
#   -h, --help                  show this help
#
set -euo pipefail

readonly SCRIPT_PATH="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/$(basename -- "${BASH_SOURCE[0]}")"
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
readonly COMPOSE_DIR="$REPO_ROOT/deploy/compose"
readonly DEFAULT_VALIDATOR="$COMPOSE_DIR/validate-phase2.sh"
readonly DEFAULT_CORPUS="$REPO_ROOT/cmd/lokicompare/testdata/grafana-corpus.json"
readonly MAX_PROJECT_LEN=63

# The only validator used by a real run. Self-tests replace this variable with
# a mock executable; there is no environment/CLI hook to execute a different
# validator from production.
VALIDATOR="$DEFAULT_VALIDATOR"

log() { printf '[e2e-p2] %s\n' "$*"; }
warn() { printf '[e2e-p2] WARN: %s\n' "$*" >&2; }
usage_fail() {
  printf '[e2e-p2] ERROR: %s\n' "$*" >&2
  exit 2
}

usage() {
  sed -n '/^# Usage:/,/^#   -h, --help/p' "$0" | sed 's/^# \{0,1\}//'
}

init_defaults() {
  MODE=full
  PROJECT="${PARKANA_E2E_PROJECT:-}"
  PROJECT_EXPLICIT=0
  [[ -n $PROJECT ]] && PROJECT_EXPLICIT=1

  CORPUS="${PARKANA_E2E_COMPARE_CORPUS:-}"
  CORPUS_SOURCE=""
  [[ -n $CORPUS ]] && CORPUS_SOURCE=env
  TENANT="${PARKANA_E2E_TENANT:-}"
  COMPARE_TIMEOUT="${PARKANA_E2E_COMPARE_TIMEOUT:-}"

  HTTP_PORT="${PARKANA_HTTP_PORT:-13100}"
  S3_PORT="${PARKANA_S3_PORT:-13900}"
  LOKI_PORT="${LOKI_HTTP_PORT:-13101}"
  ALLOY_PORT="${ALLOY_HTTP_PORT:-13245}"
  GRAFANA_PORT="${GRAFANA_HTTP_PORT:-13000}"
  HTTP_EXPLICIT=0
  S3_EXPLICIT=0
  LOKI_EXPLICIT=0
  ALLOY_EXPLICIT=0
  GRAFANA_EXPLICIT=0
  [[ -n ${PARKANA_HTTP_PORT:-} ]] && HTTP_EXPLICIT=1
  [[ -n ${PARKANA_S3_PORT:-} ]] && S3_EXPLICIT=1
  [[ -n ${LOKI_HTTP_PORT:-} ]] && LOKI_EXPLICIT=1
  [[ -n ${ALLOY_HTTP_PORT:-} ]] && ALLOY_EXPLICIT=1
  [[ -n ${GRAFANA_HTTP_PORT:-} ]] && GRAFANA_EXPLICIT=1

  KEEP=0
  DRY_RUN=0
  SELFTEST=0
  GARAGE_KEY_ID=""
  GARAGE_KEY_SECRET=""
  VALIDATOR_ARGS=()
}

parse_args() {
  while (($# > 0)); do
    case "$1" in
      --full) MODE=full; shift ;;
      --smoke) MODE=smoke; shift ;;
      --compare-corpus | --corpus)
        (($# >= 2)) || usage_fail "$1 needs a path"
        CORPUS=$2
        CORPUS_SOURCE=flag
        shift 2
        ;;
      --tenant)
        (($# >= 2)) || usage_fail "--tenant needs a value"
        TENANT=$2
        shift 2
        ;;
      --timeout | --compare-timeout)
        (($# >= 2)) || usage_fail "$1 needs a duration"
        COMPARE_TIMEOUT=$2
        shift 2
        ;;
      --project)
        (($# >= 2)) || usage_fail "--project needs a value"
        PROJECT=$2
        PROJECT_EXPLICIT=1
        shift 2
        ;;
      --parkana-port)
        (($# >= 2)) || usage_fail "--parkana-port needs a value"
        HTTP_PORT=$2
        HTTP_EXPLICIT=1
        shift 2
        ;;
      --garage-port)
        (($# >= 2)) || usage_fail "--garage-port needs a value"
        S3_PORT=$2
        S3_EXPLICIT=1
        shift 2
        ;;
      --loki-port)
        (($# >= 2)) || usage_fail "--loki-port needs a value"
        LOKI_PORT=$2
        LOKI_EXPLICIT=1
        shift 2
        ;;
      --alloy-port)
        (($# >= 2)) || usage_fail "--alloy-port needs a value"
        ALLOY_PORT=$2
        ALLOY_EXPLICIT=1
        shift 2
        ;;
      --grafana-port)
        (($# >= 2)) || usage_fail "--grafana-port needs a value"
        GRAFANA_PORT=$2
        GRAFANA_EXPLICIT=1
        shift 2
        ;;
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
valid_duration() {
  # Accept Go's positive duration syntax; lokicompare (via time.ParseDuration)
  # remains authoritative for zero/negative values and unit combinations.
  [[ $1 == 0 ]] ||
    [[ $1 =~ ^[+]?(([0-9]+([.][0-9]*)?)|([.][0-9]+))(ns|us|µs|μs|ms|s|m|h)((([0-9]+([.][0-9]*)?)|([.][0-9]+))(ns|us|µs|μs|ms|s|m|h))*$ ]]
}

canonical_corpus() {
  local dir base resolved
  dir=$(dirname -- "$CORPUS")
  base=$(basename -- "$CORPUS")
  resolved=$(cd -- "$dir" 2>/dev/null && pwd) ||
    usage_fail "compare corpus directory does not exist: $CORPUS"
  CORPUS="$resolved/$base"
}

# apply_corpus_default fills in the checked-in corpus for a full run only when
# neither the flag nor the environment supplied one. Smoke never reads a corpus.
apply_corpus_default() {
  if [[ $MODE == full && -z $CORPUS ]]; then
    CORPUS=$DEFAULT_CORPUS
    CORPUS_SOURCE=default
  fi
}

validate_args() {
  if [[ $CORPUS_SOURCE == flag && -z $CORPUS ]]; then
    usage_fail "--compare-corpus needs a non-empty path"
  fi
  apply_corpus_default
  [[ $MODE == full || $MODE == smoke ]] || usage_fail "mode must be full or smoke"
  valid_port "$HTTP_PORT" || usage_fail "Parkana port must be an integer in 1..65535"
  valid_port "$S3_PORT" || usage_fail "Garage port must be an integer in 1..65535"
  valid_port "$LOKI_PORT" || usage_fail "Loki port must be an integer in 1..65535"
  valid_port "$ALLOY_PORT" || usage_fail "Alloy port must be an integer in 1..65535"
  valid_port "$GRAFANA_PORT" || usage_fail "Grafana port must be an integer in 1..65535"
  local seen=() p
  for p in "$HTTP_PORT" "$S3_PORT" "$LOKI_PORT" "$ALLOY_PORT" "$GRAFANA_PORT"; do
    if [[ " ${seen[*]:-} " == *" $p "* ]]; then
      usage_fail "host port $p is used twice; all five host ports must differ"
    fi
    seen+=("$p")
  done

  if [[ -n $TENANT$COMPARE_TIMEOUT && -z $CORPUS ]]; then
    usage_fail "--tenant/--timeout require --compare-corpus"
  fi
  if [[ -n $TENANT ]] && ! valid_tenant_id "$TENANT"; then
    usage_fail "--tenant must be 1-64 characters from [A-Za-z0-9_-]"
  fi
  if [[ -n $COMPARE_TIMEOUT ]] && ! valid_duration "$COMPARE_TIMEOUT"; then
    usage_fail "--timeout must be a positive Go duration such as 30s or 1m"
  fi

  if [[ $MODE == full ]]; then
    [[ -n $CORPUS ]] || usage_fail "internal error: full mode has no corpus"
    if [[ $CORPUS_SOURCE == default ]]; then
      [[ -f $CORPUS && -r $CORPUS ]] ||
        usage_fail "default compare corpus is missing or unreadable: $CORPUS (pass --compare-corpus PATH)"
    else
      [[ -f $CORPUS && -r $CORPUS ]] || usage_fail "compare corpus is not a readable file: $CORPUS"
    fi
    canonical_corpus
  else
    [[ -z $CORPUS ]] || usage_fail "--compare-corpus is only valid with --full"
  fi

  if [[ -z $PROJECT ]]; then
    PROJECT="parkana-e2e-p2-$(date -u +%y%m%d%H%M%S)-$$"
  fi
  valid_project_name "$PROJECT" ||
    usage_fail "invalid compose project name '$PROJECT' (want [a-z0-9][a-z0-9_-]{0,62})"
}

# --- credentials and ports -------------------------------------------------

random_hex() { # bytes -> 2*bytes lowercase hex, no newline
  od -An -N "$1" -tx1 /dev/urandom | tr -d ' \n'
}

generate_credentials() {
  GARAGE_KEY_ID="GK$(random_hex 12)"
  GARAGE_KEY_SECRET="$(random_hex 32)"
  export PARKANA_DEV_GARAGE_KEY_ID=$GARAGE_KEY_ID
  export PARKANA_DEV_GARAGE_KEY_SECRET=$GARAGE_KEY_SECRET
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

port_used_by_this_run() {
  local port=$1
  [[ $port == "$HTTP_PORT" || $port == "$S3_PORT" || $port == "$LOKI_PORT" ||
    $port == "$ALLOY_PORT" || $port == "$GRAFANA_PORT" ]]
}

choose_port() { # preferred explicit label envname
  local preferred=$1 explicit=$2 label=$3 envname=$4 candidate i
  if port_free "$preferred"; then
    printf '%s' "$preferred"
    return 0
  fi
  if ((explicit)); then
    usage_fail "$label port $preferred is already in use; pass the matching flag or set $envname"
  fi
  for ((i = 0; i < 100; i++)); do
    candidate=$((20000 + RANDOM % 10000))
    port_used_by_this_run "$candidate" && continue
    if port_free "$candidate"; then
      printf '[e2e-p2] %s port %s is busy; using %s\n' "$label" "$preferred" "$candidate" >&2
      printf '%s' "$candidate"
      return 0
    fi
  done
  usage_fail "could not find a free $label port; set $envname or pass the matching flag"
}

resolve_ports() {
  local http s3 loki alloy grafana
  http=$(choose_port "$HTTP_PORT" "$HTTP_EXPLICIT" "Parkana HTTP" PARKANA_HTTP_PORT) || exit 2
  HTTP_PORT=$http
  s3=$(choose_port "$S3_PORT" "$S3_EXPLICIT" "Garage S3" PARKANA_S3_PORT) || exit 2
  S3_PORT=$s3
  loki=$(choose_port "$LOKI_PORT" "$LOKI_EXPLICIT" "Loki" LOKI_HTTP_PORT) || exit 2
  LOKI_PORT=$loki
  alloy=$(choose_port "$ALLOY_PORT" "$ALLOY_EXPLICIT" "Alloy" ALLOY_HTTP_PORT) || exit 2
  ALLOY_PORT=$alloy
  grafana=$(choose_port "$GRAFANA_PORT" "$GRAFANA_EXPLICIT" "Grafana" GRAFANA_HTTP_PORT) || exit 2
  GRAFANA_PORT=$grafana

  local seen=() p
  for p in "$HTTP_PORT" "$S3_PORT" "$LOKI_PORT" "$ALLOY_PORT" "$GRAFANA_PORT"; do
    [[ " ${seen[*]:-} " != *" $p "* ]] || usage_fail "resolved host port $p is used twice"
    seen+=("$p")
  done
}

export_ports() {
  export PARKANA_HTTP_PORT=$HTTP_PORT
  export PARKANA_S3_PORT=$S3_PORT
  export LOKI_HTTP_PORT=$LOKI_PORT
  export ALLOY_HTTP_PORT=$ALLOY_PORT
  export GRAFANA_HTTP_PORT=$GRAFANA_PORT
}

# --- validator invocation --------------------------------------------------

build_validator_args() {
  VALIDATOR_ARGS=("$VALIDATOR" "--$MODE")
  VALIDATOR_ARGS+=(--project "$PROJECT")
  ((KEEP == 0)) || VALIDATOR_ARGS+=(--keep)
  if [[ $MODE == full ]]; then
    VALIDATOR_ARGS+=(--compare-corpus "$CORPUS")
    [[ -z $TENANT ]] || VALIDATOR_ARGS+=(--compare-tenant "$TENANT")
    [[ -z $COMPARE_TIMEOUT ]] || VALIDATOR_ARGS+=(--compare-timeout "$COMPARE_TIMEOUT")
  fi
}

run_validator() {
  local status=0
  "${VALIDATOR_ARGS[@]}" || status=$?
  return "$status"
}

require_cmds() {
  local cmd
  for cmd in date dirname basename od tr; do
    command -v "$cmd" >/dev/null 2>&1 || usage_fail "missing required command: $cmd"
  done
  [[ -x $VALIDATOR ]] || usage_fail "validator is not executable: $VALIDATOR"
}

# --- plan / dry run --------------------------------------------------------

dry_run() {
  printf '[e2e-p2] dry run: no Docker, Go or services are touched\n'
  printf '  repo root       : %s\n' "$REPO_ROOT"
  printf '  validator       : %s\n' "$VALIDATOR"
  printf '  mode            : %s\n' "$MODE"
  printf '  project         : %s%s\n' "$PROJECT" "$([[ $PROJECT_EXPLICIT == 1 ]] && printf ' (explicit)' || printf ' (generated)')"
  printf '  host ports      : parkana=%s garage=%s loki=%s alloy=%s grafana=%s\n' \
    "$HTTP_PORT" "$S3_PORT" "$LOKI_PORT" "$ALLOY_PORT" "$GRAFANA_PORT"
  if [[ $MODE == full ]]; then
    printf '  corpus          : %s (%s)\n' "$CORPUS" "$CORPUS_SOURCE"
    printf '  tenant          : %s\n' "${TENANT:-<not set: lokicompare sends no X-Scope-OrgID>}"
    printf '  compare timeout : %s\n' "${COMPARE_TIMEOUT:-<lokicompare default>}"
  else
    printf '  corpus          : <not used in smoke mode>\n'
  fi
  printf '  Garage creds    : generated per run (values hidden)\n'
  printf '  cleanup owner   : validator (its existing compose trap); this wrapper never downs a stack\n'
  printf '  validator argv  :'
  build_validator_args
  printf ' %q' "${VALIDATOR_ARGS[@]}"
  printf '\n'
  printf '[e2e-p2] dry run: no changes made\n'
}

# --- self tests (no Docker, Go or services) --------------------------------

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

test_help_and_defaults() (
  init_defaults
  [[ $MODE == full && $HTTP_PORT == 13100 && $S3_PORT == 13900 ]] || return 1
  [[ $LOKI_PORT == 13101 && $ALLOY_PORT == 13245 && $GRAFANA_PORT == 13000 ]] || return 1
  [[ -z $CORPUS && -z $TENANT && -z $COMPARE_TIMEOUT ]] || return 1
)

test_corpus_default_and_precedence() (
  local work got
  work=$(mktemp -d) || return 1
  printf '[]\n' >"$work/corpus.json"
  printf '[]\n' >"$work/env.json"
  printf '[]\n' >"$work/flag.json"

  init_defaults
  PROJECT=selftest-project
  MODE=full
  CORPUS=""
  CORPUS_SOURCE=""
  validate_args || { rm -rf "$work"; return 1; }
  [[ $CORPUS == "$DEFAULT_CORPUS" && $CORPUS_SOURCE == default ]] ||
    { rm -rf "$work"; return 1; }
  [[ $DEFAULT_CORPUS == "$REPO_ROOT/cmd/lokicompare/testdata/grafana-corpus.json" ]] ||
    { rm -rf "$work"; return 1; }
  [[ -f $DEFAULT_CORPUS && -r $DEFAULT_CORPUS ]] || { rm -rf "$work"; return 1; }

  # The environment override replaces the default.
  PARKANA_E2E_COMPARE_CORPUS="$work/env.json" init_defaults
  PROJECT=selftest-project
  MODE=full
  [[ $CORPUS == "$work/env.json" && $CORPUS_SOURCE == env ]] ||
    { rm -rf "$work"; return 1; }
  validate_args || { rm -rf "$work"; return 1; }
  [[ $CORPUS == "$work/env.json" ]] || { rm -rf "$work"; return 1; }

  # An explicit flag beats the environment.
  PARKANA_E2E_COMPARE_CORPUS="$work/env.json" init_defaults
  PROJECT=selftest-project
  MODE=full
  parse_args --compare-corpus "$work/flag.json"
  [[ $CORPUS == "$work/flag.json" && $CORPUS_SOURCE == flag ]] ||
    { rm -rf "$work"; return 1; }
  validate_args || { rm -rf "$work"; return 1; }

  # An explicit relative path still resolves from the caller's directory.
  got=$(cd "$work" || exit 1
    init_defaults
    PROJECT=selftest-project
    MODE=full
    CORPUS=corpus.json
    CORPUS_SOURCE=flag
    validate_args || exit 1
    printf '%s' "$CORPUS")
  [[ $got == "$work/corpus.json" ]] || { rm -rf "$work"; return 1; }

  # An explicit missing path is still an error, never the default.
  init_defaults
  PROJECT=selftest-project
  MODE=full
  CORPUS="$work/missing.json"
  CORPUS_SOURCE=flag
  if (validate_args) >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi

  # An explicitly empty flag is a usage error, never the default.
  init_defaults
  PROJECT=selftest-project
  MODE=full
  parse_args --compare-corpus ""
  [[ -z $CORPUS && $CORPUS_SOURCE == flag ]] || { rm -rf "$work"; return 1; }
  if (validate_args) >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  MODE=smoke
  if (validate_args) >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi

  # An empty environment variable behaves like an unset one.
  PARKANA_E2E_COMPARE_CORPUS="" init_defaults
  PROJECT=selftest-project
  MODE=full
  [[ -z $CORPUS && -z $CORPUS_SOURCE ]] || { rm -rf "$work"; return 1; }
  validate_args || { rm -rf "$work"; return 1; }
  [[ $CORPUS == "$DEFAULT_CORPUS" && $CORPUS_SOURCE == default ]] ||
    { rm -rf "$work"; return 1; }

  # Smoke never receives the default and still rejects a corpus.
  init_defaults
  PROJECT=selftest-project
  MODE=smoke
  CORPUS=""
  CORPUS_SOURCE=""
  validate_args || { rm -rf "$work"; return 1; }
  [[ -z $CORPUS && -z $CORPUS_SOURCE ]] || { rm -rf "$work"; return 1; }
  CORPUS="$work/corpus.json"
  CORPUS_SOURCE=flag
  if (validate_args) >/dev/null 2>&1; then
    rm -rf "$work"
    return 1
  fi
  rm -rf "$work"
)

test_tenant_timeout_validate() (
  local work
  work=$(mktemp -d) || return 1
  printf '[]\n' >"$work/corpus.json"
  init_defaults
  MODE=full
  PROJECT=selftest-project
  CORPUS="$work/corpus.json"

  TENANT="tenant-a"
  (validate_args) || { rm -rf "$work"; return 1; }
  TENANT="bad tenant"
  if (validate_args) >/dev/null 2>&1; then rm -rf "$work"; return 1; fi
  TENANT=""
  local d
  for d in 30s 1m30s 0.5s .5s +1s 0h1m 0; do
    COMPARE_TIMEOUT=$d
    (validate_args) || { rm -rf "$work"; return 1; }
  done
  for d in 1+1 abc -1s 1.5; do
    COMPARE_TIMEOUT=$d
    if (validate_args) >/dev/null 2>&1; then rm -rf "$work"; return 1; fi
  done
  COMPARE_TIMEOUT=""
  TENANT="tenant-a"
  CORPUS=""
  MODE=smoke
  if (validate_args) >/dev/null 2>&1; then rm -rf "$work"; return 1; fi
  rm -rf "$work"
)

test_numeric_and_project_validation() (
  init_defaults
  MODE=smoke
  PROJECT=selftest-project
  HTTP_PORT="1+1"
  if (validate_args) >/dev/null 2>&1; then return 1; fi
  HTTP_PORT=13100
  S3_PORT=65536
  if (validate_args) >/dev/null 2>&1; then return 1; fi
  S3_PORT=13900
  PROJECT="Bad Name"
  if (validate_args) >/dev/null 2>&1; then return 1; fi
  PROJECT=ok-project
  (validate_args) || return 1

  GRAFANA_PORT=13100
  if (validate_args) >/dev/null 2>&1; then return 1; fi
  GRAFANA_PORT=13000
  LOKI_PORT=13900
  if (validate_args) >/dev/null 2>&1; then return 1; fi
  LOKI_PORT=13101
  S3_PORT=13100
  if (validate_args) >/dev/null 2>&1; then return 1; fi
  S3_PORT=13900
  (validate_args) || return 1
)

test_dry_run_rejects_duplicate_ports() {
  local out status
  set +e
  out=$(cd "$REPO_ROOT" && bash scripts/e2e-phase2.sh --smoke --dry-run --loki-port 13100 2>&1)
  status=$?
  set -e
  [[ $status == 2 ]] || return 1
  [[ $out == *"host port 13100 is used twice"* ]] || return 1
}

test_port_auto_pick() (
  init_defaults
  HTTP_PORT=23100
  S3_PORT=23900
  LOKI_PORT=23101
  ALLOY_PORT=23245
  GRAFANA_PORT=23000
  HTTP_EXPLICIT=0
  S3_EXPLICIT=0
  LOKI_EXPLICIT=0
  ALLOY_EXPLICIT=0
  GRAFANA_EXPLICIT=0
  # Preferred ports are busy, every candidate is free.
  port_free() {
    case "$1" in
      23100 | 23900 | 23101 | 23245 | 23000) return 1 ;;
      *) return 0 ;;
    esac
  }
  resolve_ports
  [[ $HTTP_PORT != 23100 && $S3_PORT != 23900 && $LOKI_PORT != 23101 ]] || return 1
  [[ $ALLOY_PORT != 23245 && $GRAFANA_PORT != 23000 ]] || return 1
  [[ $HTTP_PORT != "$S3_PORT" && $HTTP_PORT != "$LOKI_PORT" && $HTTP_PORT != "$ALLOY_PORT" ]] || return 1
  [[ $LOKI_PORT != "$ALLOY_PORT" && $ALLOY_PORT != "$GRAFANA_PORT" ]] || return 1

  # An explicit busy port is an error, never a silent move.
  HTTP_PORT=23100
  HTTP_EXPLICIT=1
  if (resolve_ports) >/dev/null 2>&1; then return 1; fi
)

test_random_credentials() (
  generate_credentials
  [[ $GARAGE_KEY_ID =~ ^GK[0-9a-f]{24}$ ]] || return 1
  [[ $GARAGE_KEY_SECRET =~ ^[0-9a-f]{64}$ ]] || return 1
  local first_id=$GARAGE_KEY_ID first_secret=$GARAGE_KEY_SECRET
  generate_credentials
  [[ $GARAGE_KEY_ID != "$first_id" && $GARAGE_KEY_SECRET != "$first_secret" ]] || return 1
  [[ ${PARKANA_DEV_GARAGE_KEY_ID:-} == "$GARAGE_KEY_ID" ]] || return 1
  [[ ${PARKANA_DEV_GARAGE_KEY_SECRET:-} == "$GARAGE_KEY_SECRET" ]] || return 1
)

test_validator_argv_and_exit() (
  local work fake log corpus
  work=$(mktemp -d) || return 1
  fake="$work/validator"
  log="$work/argv"
  cat >"$fake" <<'MOCK'
#!/bin/sh
{
    printf 'ARGC %s\n' "$#"
    i=0
    for arg in "$@"; do
        printf 'ARGV %s %s\n' "$i" "$arg"
        i=$((i + 1))
    done
    printf 'ports=%s,%s,%s,%s,%s\n' "$PARKANA_HTTP_PORT" "$PARKANA_S3_PORT" "$LOKI_HTTP_PORT" "$ALLOY_HTTP_PORT" "$GRAFANA_HTTP_PORT"
} >>"${MOCK_VALIDATOR_LOG:?}"
exit "${MOCK_VALIDATOR_EXIT:-0}"
MOCK
  chmod 0755 "$fake"
  export MOCK_VALIDATOR_LOG=$log

  init_defaults
  MODE=full
  PROJECT=selftest-project
  corpus="$work/corpus with spaces.json"
  printf '[]\n' >"$corpus"
  CORPUS=$corpus
  TENANT=tenant-a
  COMPARE_TIMEOUT=30s
  KEEP=1
  VALIDATOR=$fake
  build_validator_args
  export_ports

  MOCK_VALIDATOR_EXIT=0
  run_validator || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGC 10' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 0 --full' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 1 --project' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 2 selftest-project' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 3 --keep' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 4 --compare-corpus' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq "ARGV 5 $corpus" "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 6 --compare-tenant' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 7 tenant-a' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 8 --compare-timeout' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 9 30s' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq -- 'ports=13100,13900,13101,13245,13000' "$log" || { rm -rf "$work"; return 1; }

  local want_status got
  for want_status in 0 1 2 124; do
    MOCK_VALIDATOR_EXIT=$want_status
    export MOCK_VALIDATOR_EXIT
    got=0
    run_validator || got=$?
    [[ $got == "$want_status" ]] || { rm -rf "$work"; return 1; }
  done

  # Smoke mode must not forward compare flags.
  : >"$log"
  MOCK_VALIDATOR_EXIT=0
  export MOCK_VALIDATOR_EXIT
  MODE=smoke
  CORPUS=""
  TENANT=""
  COMPARE_TIMEOUT=""
  KEEP=0
  build_validator_args
  run_validator || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGC 3' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 0 --smoke' "$log" || { rm -rf "$work"; return 1; }
  grep -Fxq 'ARGV 2 selftest-project' "$log" || { rm -rf "$work"; return 1; }
  ! grep -Fq -- '--compare-corpus' "$log" || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_dry_run_hides_secret() (
  local work out
  work=$(mktemp -d) || return 1
  printf '[]\n' >"$work/corpus.json"
  init_defaults
  MODE=full
  PROJECT=selftest-project
  CORPUS="$work/corpus.json"
  generate_credentials
  out=$(dry_run) || { rm -rf "$work"; return 1; }
  [[ $out != *"$GARAGE_KEY_SECRET"* && $out != *"$GARAGE_KEY_ID"* ]] || { rm -rf "$work"; return 1; }
  [[ $out == *"values hidden"* && $out == *"validator"* ]] || { rm -rf "$work"; return 1; }
  rm -rf "$work"
)

test_dry_run_default_corpus() {
  local out work
  # Root-relative invocation still resolves the repository default.
  out=$(cd "$REPO_ROOT" && bash scripts/e2e-phase2.sh --full --dry-run) || return 1
  [[ $out == *"corpus          : $DEFAULT_CORPUS (default)"* ]] || return 1

  # The default is repository-relative, not caller-relative: run the absolute
  # wrapper path from an unrelated directory and expect the same absolute path.
  work=$(mktemp -d) || return 1
  out=$(cd "$work" && bash "$SCRIPT_PATH" --full --dry-run) ||
    { rm -rf "$work"; return 1; }
  [[ $out == *"repo root       : $REPO_ROOT"* ]] ||
    { rm -rf "$work"; return 1; }
  [[ $out == *"corpus          : $DEFAULT_CORPUS (default)"* ]] ||
    { rm -rf "$work"; return 1; }
  rm -rf "$work"
}

test_dry_run_env_and_flag_precedence() {
  local work out env_corpus flag_corpus
  work=$(mktemp -d) || return 1
  env_corpus="$work/env corpus.json"
  flag_corpus="$work/flag corpus.json"
  printf '[]\n' >"$env_corpus"
  printf '[]\n' >"$flag_corpus"

  # The environment override must survive into the script, not just init_defaults.
  out=$(PARKANA_E2E_COMPARE_CORPUS="$env_corpus" \
    bash "$SCRIPT_PATH" --full --dry-run) || { rm -rf "$work"; return 1; }
  [[ $out == *"corpus          : $env_corpus (env)"* ]] ||
    { rm -rf "$work"; return 1; }

  # An explicit flag must beat that environment for the whole invocation.
  out=$(PARKANA_E2E_COMPARE_CORPUS="$env_corpus" \
    bash "$SCRIPT_PATH" --full --dry-run --compare-corpus "$flag_corpus") ||
    { rm -rf "$work"; return 1; }
  [[ $out == *"corpus          : $flag_corpus (flag)"* ]] ||
    { rm -rf "$work"; return 1; }
  [[ $out != *"$env_corpus"* ]] || { rm -rf "$work"; return 1; }
  rm -rf "$work"
}

test_no_second_cleanup_owner() {
  ! grep -Eq '(^|[[:space:]])docker[[:space:]]+compose([[:space:]].*)?[[:space:]]down([[:space:]]|$)' "$SCRIPT_PATH"
}

test_relative_entrypoint() {
  local out
  out=$(cd "$REPO_ROOT" && bash scripts/e2e-phase2.sh --smoke --dry-run) || return 1
  [[ $out == *"validator       : $DEFAULT_VALIDATOR"* ]] || return 1
  [[ $out == *"no changes made"* ]] || return 1
}

self_test() {
  local failed=0
  printf '[e2e-p2] self-test: offline logic and mock subprocesses\n'
  self_case "defaults and help surface" test_help_and_defaults || failed=1
  self_case "full defaults to the repo corpus; flag/env precedence preserved" test_corpus_default_and_precedence || failed=1
  self_case "tenant/timeout validation accepts Go positive durations" test_tenant_timeout_validate || failed=1
  self_case "ports and project names reject expressions/duplicates" test_numeric_and_project_validation || failed=1
  self_case "dry run rejects duplicate ports before any mode" test_dry_run_rejects_duplicate_ports || failed=1
  self_case "busy defaults auto-pick distinct ports; explicit busy fails" test_port_auto_pick || failed=1
  self_case "fresh Garage credentials are random and exported" test_random_credentials || failed=1
  self_case "validator argv and 0/1/2 exit propagation (subprocess)" test_validator_argv_and_exit || failed=1
  self_case "dry run never prints Garage credentials" test_dry_run_hides_secret || failed=1
  self_case "dry run shows the repository default corpus" test_dry_run_default_corpus || failed=1
  self_case "subprocess env override and flag precedence" test_dry_run_env_and_flag_precedence || failed=1
  self_case "wrapper has no second cleanup owner" test_no_second_cleanup_owner || failed=1
  self_case "relative entrypoint resolves the validator" test_relative_entrypoint || failed=1
  if ((failed)); then
    printf '[e2e-p2] self-test: FAILED\n' >&2
    return 1
  fi
  printf '[e2e-p2] self-test: all checks passed\n'
  return 0
}

# --- main ------------------------------------------------------------------

main() {
  init_defaults
  parse_args "$@"
  if ((SELFTEST)); then
    self_test
    return
  fi
  if ((DRY_RUN)); then
    # Validate without touching Docker/Go so a typo fails before a stack.
    validate_args
    dry_run
    return
  fi
  require_cmds
  validate_args
  generate_credentials
  resolve_ports
  export_ports
  build_validator_args

  log "mode=$MODE project=$PROJECT"
  log "ports: parkana=$HTTP_PORT garage=$S3_PORT loki=$LOKI_PORT alloy=$ALLOY_PORT grafana=$GRAFANA_PORT"
  if [[ $MODE == full ]]; then
    log "corpus=$CORPUS ($CORPUS_SOURCE)"
  fi
  log "Garage credentials: generated for this run (values hidden)"

  local status=0
  run_validator || status=$?
  case "$status" in
    0) log "PASS ($MODE): validator 0" ;;
    1) warn "FAIL ($MODE): validator/comparison reported a semantic difference or check failure (exit 1)" ;;
    2) warn "FAIL ($MODE): validator/comparator invocation or request/response failure (exit 2)" ;;
    *) warn "FAIL ($MODE): validator exited $status" ;;
  esac
  exit "$status"
}

main "$@"
