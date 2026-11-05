#!/usr/bin/env bash
#
# scripts/e2e-phase4-otlp.sh - native OTLP end-to-end comparison wrapper.
#
# Starts the native OTLP comparison stack from the base Compose file plus the
# OTLP overlay (deploy/compose/otlp.compose.yaml), waits with bounded deadlines,
# sends the deterministic otlpcompare fixture through three isolated transports,
# and requires otlpcompare to report EQUAL for both stores:
#
#   1. direct-protobuf       protobuf, no gzip: Loki /otlp/v1/logs and
#                            Parkana /v1/logs, sent to each store directly.
#   2. direct-json-gzip      JSON + gzip: the same two native endpoints.
#   3. alloy-tee-mixed-gzip  mixed protobuf/JSON + gzip: one send to Alloy's
#                            native otelcol.receiver.otlp, which tees the same
#                            OTLP to both stores without loki.write conversion.
#
# Every case uses its own service.namespace/run-id, so cases cannot collide in
# either store. Each send writes a manifest; compare reads it and asserts exact
# per-case entry counts on BOTH backends. --full then restarts every writer
# (Parkana, Loki and Alloy) and reruns every compare against the same manifests,
# proving exact counts survive restart.
#
# Docker, Go and the network are needed for a real run. The script creates one
# fresh Compose project, six loopback ports, one fresh output directory and a
# per-project lock; it refuses an existing or locked project or busy explicit
# port, fails closed when a project listing errors, and removes only its own
# Compose project and lock. Manifests, reports and output are preserved on
# success and failure. Diagnostics are sanitized. No shell command prints a
# credential.
#
# Usage: scripts/e2e-phase4-otlp.sh [options]
#   --full                     stack + transports + restart recheck (default)
#   --smoke                    stack, readiness and effective settings only
#   --tenant ID                X-Scope-OrgID (only "default" is supported)
#   --timeout DURATION         otlpcompare per-request timeout (default 30s)
#   --wait-timeout DURATION    compare visibility wait (default 60s)
#   --poll-interval DURATION   compare poll interval (default 1s)
#   --ready-timeout SECONDS    bounded stack/receiver readiness (default 180)
#   --retries N                send attempts per fixture case (default 5)
#   --project NAME             compose project (default: generated)
#   --out DIR                  fresh output directory (default: mktemp -d)
#   --parkana-port PORT        Parkana HTTP host port (default 14100)
#   --garage-port PORT         Garage S3 host port (default 14900)
#   --loki-port PORT           Loki HTTP host port (default 14101)
#   --alloy-port PORT          Alloy UI host port (default 14245)
#   --alloy-otlp-http-port P   Alloy OTLP/HTTP host port (default 14318)
#   --alloy-otlp-grpc-port P   Alloy OTLP/gRPC host port (default 14317)
#   --keep                     leave the compose project for debugging
#                              (manifests/reports/output are always kept)
#   --dry-run                  print the resolved plan; touch nothing
#   --self-test                offline mock/red-case tests; no Docker or Go
#   -h, --help                 show this help
#
# Environment overrides: PARKANA_OTLP_E2E_PROJECT, PARKANA_OTLP_E2E_OUT,
# PARKANA_OTLP_E2E_READY_TIMEOUT, PARKANA_OTLP_E2E_COMPOSE_TIMEOUT,
# PARKANA_OTLP_E2E_LOCK_ROOT and the same host-port variables as the base
# stack. Parkana ingest/WAL settings are documented in docs/otlp.md.
#
set -euo pipefail

readonly SCRIPT_PATH="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/$(basename -- "${BASH_SOURCE[0]}")"
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
readonly COMPOSE_DIR="$REPO_ROOT/deploy/compose"
readonly BASE_COMPOSE="$COMPOSE_DIR/compose.yaml"
readonly OVERLAY_COMPOSE="$COMPOSE_DIR/otlp.compose.yaml"
readonly ALLOY_CONFIG="$COMPOSE_DIR/otlp.alloy"
readonly LOKI_CONFIG="$COMPOSE_DIR/otlp.loki.yaml"
readonly MAX_PROJECT_LEN=63
readonly MAX_READY_TIMEOUT=86400

readonly ALLOY_PIN='grafana/alloy:v1.21.3@sha256:8ab871206bd2d8f89277f53abffee5f452a293980788d52f85fa183a87a505a4'
readonly LOKI_PIN='grafana/loki:3.7.9@sha256:0db216b630ae1f348a1584c67f3cead8b4c1fc839169a39501db664744158e28'
readonly LOKI_EMPTY_PUSH_MARKER='error at least one valid stream is required for ingestion'

DOCKER="${DOCKER:-docker}"
GO="${GO:-go}"

log() { printf '[e2e-p4-otlp] %s\n' "$*"; }
warn() { printf '[e2e-p4-otlp] WARN: %s\n' "$*" >&2; }
say() { printf '\n=== %s ===\n' "$*"; }
usage_fail() {
  printf '[e2e-p4-otlp] ERROR: %s\n' "$*" >&2
  exit 2
}
fail() {
  printf '[e2e-p4-otlp] FAIL: %s\n' "$*" >&2
  exit 1
}

usage() {
  sed -n '/^# Usage:/,/^#   -h, --help/p' "$0" | sed 's/^# \{0,1\}//'
}

init_defaults() {
  MODE=full
  PROJECT="${PARKANA_OTLP_E2E_PROJECT:-}"
  PROJECT_EXPLICIT=0
  [[ -n $PROJECT ]] && PROJECT_EXPLICIT=1

  OUT_DIR="${PARKANA_OTLP_E2E_OUT:-}"
  OUT_EXPLICIT=0
  [[ -n $OUT_DIR ]] && OUT_EXPLICIT=1

  TENANT=default
  TIMEOUT=30s
  WAIT_TIMEOUT=60s
  POLL_INTERVAL=1s
  SEND_READY_TIMEOUT=30s
  READY_TIMEOUT="${PARKANA_OTLP_E2E_READY_TIMEOUT:-180}"
  COMPOSE_TIMEOUT="${PARKANA_OTLP_E2E_COMPOSE_TIMEOUT:-900}"
  RETRIES=5
  LOCK_ROOT="${PARKANA_OTLP_E2E_LOCK_ROOT:-${TMPDIR:-/tmp}}"
  LOCK_DIR=""
  LOCK_OWNED=0

  PARKANA_PORT="${PARKANA_HTTP_PORT:-14100}"
  S3_PORT="${PARKANA_S3_PORT:-14900}"
  LOKI_PORT="${LOKI_HTTP_PORT:-14101}"
  ALLOY_PORT="${ALLOY_HTTP_PORT:-14245}"
  ALLOY_OTLP_HTTP_PORT="${ALLOY_OTLP_HTTP_PORT:-14318}"
  ALLOY_OTLP_GRPC_PORT="${ALLOY_OTLP_GRPC_PORT:-14317}"
  PARKANA_EXPLICIT=0
  S3_EXPLICIT=0
  LOKI_EXPLICIT=0
  ALLOY_EXPLICIT=0
  ALLOY_OTLP_HTTP_EXPLICIT=0
  ALLOY_OTLP_GRPC_EXPLICIT=0
  [[ -n ${PARKANA_HTTP_PORT:-} ]] && PARKANA_EXPLICIT=1
  [[ -n ${PARKANA_S3_PORT:-} ]] && S3_EXPLICIT=1
  [[ -n ${LOKI_HTTP_PORT:-} ]] && LOKI_EXPLICIT=1
  [[ -n ${ALLOY_HTTP_PORT:-} ]] && ALLOY_EXPLICIT=1
  [[ -n ${ALLOY_OTLP_HTTP_PORT:-} ]] && ALLOY_OTLP_HTTP_EXPLICIT=1
  [[ -n ${ALLOY_OTLP_GRPC_PORT:-} ]] && ALLOY_OTLP_GRPC_EXPLICIT=1

  FLUSH_INTERVAL="${PARKANA_INGEST_FLUSH_INTERVAL:-1s}"
  UPLOAD_INTERVAL="${PARKANA_INGEST_UPLOAD_INTERVAL:-1s}"
  UPLOAD_MIN="${PARKANA_INGEST_UPLOAD_MIN:-1B}"
  UPLOAD_MAX_AGE="${PARKANA_INGEST_UPLOAD_MAX_AGE:-2s}"
  ROLES="${PARKANA_ROLES:-ingester,querier}"
  FLUSH_CONCURRENCY="${PARKANA_INGEST_FLUSH_CONCURRENCY:-2}"
  UPLOAD_CONCURRENCY="${PARKANA_INGEST_UPLOAD_CONCURRENCY:-2}"
  WAL_SYNC="${PARKANA_WAL_SYNC:-always}"
  WAL_SYNC_INTERVAL="${PARKANA_WAL_SYNC_INTERVAL:-1s}"

  KEEP=0
  DRY_RUN=0
  SELFTEST=0
  GARAGE_KEY_ID=""
  GARAGE_KEY_SECRET=""
  COMPARE_BIN=""
  created=0
  CASE_NAMES=()
  CASE_MANIFESTS=()
}

parse_args() {
  while (($# > 0)); do
    case "$1" in
      --full) MODE=full; shift ;;
      --smoke) MODE=smoke; shift ;;
      --tenant)
        (($# >= 2)) || usage_fail "--tenant needs a value"
        TENANT=$2
        shift 2
        ;;
      --timeout)
        (($# >= 2)) || usage_fail "--timeout needs a duration"
        TIMEOUT=$2
        shift 2
        ;;
      --wait-timeout)
        (($# >= 2)) || usage_fail "--wait-timeout needs a duration"
        WAIT_TIMEOUT=$2
        shift 2
        ;;
      --poll-interval)
        (($# >= 2)) || usage_fail "--poll-interval needs a duration"
        POLL_INTERVAL=$2
        shift 2
        ;;
      --ready-timeout)
        (($# >= 2)) || usage_fail "--ready-timeout needs seconds"
        READY_TIMEOUT=$2
        shift 2
        ;;
      --retries)
        (($# >= 2)) || usage_fail "--retries needs a value"
        RETRIES=$2
        shift 2
        ;;
      --project)
        (($# >= 2)) || usage_fail "--project needs a value"
        PROJECT=$2
        PROJECT_EXPLICIT=1
        shift 2
        ;;
      --out)
        (($# >= 2)) || usage_fail "--out needs a path"
        OUT_DIR=$2
        OUT_EXPLICIT=1
        shift 2
        ;;
      --parkana-port)
        (($# >= 2)) || usage_fail "--parkana-port needs a value"
        PARKANA_PORT=$2
        PARKANA_EXPLICIT=1
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
      --alloy-otlp-http-port)
        (($# >= 2)) || usage_fail "--alloy-otlp-http-port needs a value"
        ALLOY_OTLP_HTTP_PORT=$2
        ALLOY_OTLP_HTTP_EXPLICIT=1
        shift 2
        ;;
      --alloy-otlp-grpc-port)
        (($# >= 2)) || usage_fail "--alloy-otlp-grpc-port needs a value"
        ALLOY_OTLP_GRPC_PORT=$2
        ALLOY_OTLP_GRPC_EXPLICIT=1
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
valid_duration() {
  [[ $1 == 0 ]] ||
    [[ $1 =~ ^[+]?(([0-9]+([.][0-9]*)?)|([.][0-9]+))(ns|us|µs|μs|ms|s|m|h)((([0-9]+([.][0-9]*)?)|([.][0-9]+))(ns|us|µs|μs|ms|s|m|h))*$ ]]
}
positive_duration() {
  valid_duration "$1" || return 1
  local num=${1#+}
  num=${num%%[a-zµμ]*}
  [[ $num =~ [1-9] ]]
}
valid_ready_timeout() {
  [[ $1 =~ ^[1-9][0-9]{0,4}$ ]] && (($1 <= MAX_READY_TIMEOUT))
}
valid_retries() { [[ $1 =~ ^[1-9][0-9]{0,2}$ ]] && (($1 >= 1 && $1 <= 100)); }
valid_tenant() { [[ $1 =~ ^[A-Za-z0-9_-]{1,64}$ ]]; }

validate_args() {
  [[ $MODE == full || $MODE == smoke ]] || usage_fail "mode must be full or smoke"
  valid_tenant "$TENANT" || usage_fail "--tenant must be 1-64 characters from [A-Za-z0-9_-]"
  [[ $TENANT == default ]] ||
    usage_fail "--tenant=$TENANT is unsupported: Parkana multi-tenancy is not implemented; use the default tenant"

  positive_duration "$TIMEOUT" || usage_fail "--timeout must be a positive Go duration such as 30s"
  positive_duration "$WAIT_TIMEOUT" || usage_fail "--wait-timeout must be a positive Go duration such as 60s"
  positive_duration "$POLL_INTERVAL" || usage_fail "--poll-interval must be a positive Go duration such as 1s"
  valid_ready_timeout "$READY_TIMEOUT" ||
    usage_fail "--ready-timeout must be a positive integer number of seconds at most $MAX_READY_TIMEOUT"
  valid_ready_timeout "$COMPOSE_TIMEOUT" ||
    usage_fail "PARKANA_OTLP_E2E_COMPOSE_TIMEOUT must be a positive integer number of seconds at most $MAX_READY_TIMEOUT"
  valid_retries "$RETRIES" || usage_fail "--retries must be an integer in 1..100"

  for spec in \
    "Parkana:$PARKANA_PORT" "Garage:$S3_PORT" "Loki:$LOKI_PORT" \
    "Alloy UI:$ALLOY_PORT" "Alloy OTLP/HTTP:$ALLOY_OTLP_HTTP_PORT" "Alloy OTLP/gRPC:$ALLOY_OTLP_GRPC_PORT"; do
    label=${spec%%:*}
    port=${spec#*:}
    valid_port "$port" || usage_fail "$label port must be an integer in 1..65535"
  done
  local seen=() p
  for p in "$PARKANA_PORT" "$S3_PORT" "$LOKI_PORT" "$ALLOY_PORT" "$ALLOY_OTLP_HTTP_PORT" "$ALLOY_OTLP_GRPC_PORT"; do
    [[ " ${seen[*]:-} " != *" $p "* ]] || usage_fail "host port $p is used twice; all six host ports must differ"
    seen+=("$p")
  done

  positive_duration "$FLUSH_INTERVAL" || usage_fail "PARKANA_INGEST_FLUSH_INTERVAL must be positive"
  positive_duration "$UPLOAD_INTERVAL" || usage_fail "PARKANA_INGEST_UPLOAD_INTERVAL must be positive"
  positive_duration "$UPLOAD_MAX_AGE" || usage_fail "PARKANA_INGEST_UPLOAD_MAX_AGE must be positive"
  [[ $ROLES == ingester,querier ]] ||
    usage_fail "PARKANA_ROLES must be ingester,querier for the OTLP comparison"
  [[ $WAL_SYNC == always || $WAL_SYNC == interval ]] ||
    usage_fail "PARKANA_WAL_SYNC must be always or interval"
  positive_duration "$WAL_SYNC_INTERVAL" || usage_fail "PARKANA_WAL_SYNC_INTERVAL must be positive"
  [[ $UPLOAD_MIN =~ ^[1-9][0-9]*[A-Za-z]*$ ]] || usage_fail "PARKANA_INGEST_UPLOAD_MIN must be a positive size"
  if ! [[ $FLUSH_CONCURRENCY =~ ^[1-9][0-9]{0,2}$ ]] || (($FLUSH_CONCURRENCY > 64)); then
    usage_fail "PARKANA_INGEST_FLUSH_CONCURRENCY must be an integer in 1..64"
  fi
  if ! [[ $UPLOAD_CONCURRENCY =~ ^[1-9][0-9]{0,2}$ ]] || (($UPLOAD_CONCURRENCY > 64)); then
    usage_fail "PARKANA_INGEST_UPLOAD_CONCURRENCY must be an integer in 1..64"
  fi

  if [[ -n $PROJECT ]]; then
    valid_project_name "$PROJECT" ||
      usage_fail "invalid compose project name '$PROJECT' (want [a-z0-9][a-z0-9_-]{0,62})"
  fi
  if [[ $OUT_EXPLICIT == 1 ]]; then
    [[ -n $OUT_DIR ]] || usage_fail "--out needs a non-empty path"
    [[ ! -e $OUT_DIR ]] || usage_fail "--out must name a path that does not exist (a fresh output directory is required): $OUT_DIR"
  fi
}

# --- configuration checks --------------------------------------------------

# strip_hash_comments removes YAML full-line and trailing # comments. The
# checked files contain no '#' in values.
strip_hash_comments() { sed -e 's/[[:space:]]*#.*$//' "$1"; }

# strip_slash_comments removes full-line // comments from Alloy source.
strip_slash_comments() { grep -v '^[[:space:]]*//' "$1" || true; }

check_alloy_config_file() {
  cfg=$1
  code=$(strip_slash_comments "$cfg")
  grep -q 'otelcol.receiver.otlp' <<<"$code" ||
    { echo "Alloy config has no otelcol.receiver.otlp" >&2; return 1; }
  grep -q 'otelcol.exporter.otlphttp "loki"' <<<"$code" ||
    { echo "Alloy config has no Loki otlphttp exporter" >&2; return 1; }
  grep -q 'otelcol.exporter.otlphttp "parkana"' <<<"$code" ||
    { echo "Alloy config has no Parkana otlphttp exporter" >&2; return 1; }
  grep -Eq 'logs_endpoint[[:space:]]*=[[:space:]]*"http://loki:3100/otlp/v1/logs"' <<<"$code" ||
    { echo "Loki exporter logs_endpoint is not the native /otlp/v1/logs path" >&2; return 1; }
  grep -Eq 'logs_endpoint[[:space:]]*=[[:space:]]*"http://parkana:3100/v1/logs"' <<<"$code" ||
    { echo "Parkana exporter logs_endpoint is not the native /v1/logs path" >&2; return 1; }
  grep -q 'otelcol.exporter.otlphttp.loki.input' <<<"$code" ||
    { echo "receiver does not fan out to the Loki exporter" >&2; return 1; }
  grep -q 'otelcol.exporter.otlphttp.parkana.input' <<<"$code" ||
    { echo "receiver does not fan out to the Parkana exporter" >&2; return 1; }
  if grep -q 'loki.write' <<<"$code"; then
    echo "Alloy config converts through loki.write; native OTLP tee is required" >&2
    return 1
  fi
  return 0
}

check_loki_config_file() {
  cfg=$1
  code=$(strip_hash_comments "$cfg")
  grep -q '^auth_enabled: false' <<<"$code" ||
    { echo "Loki config must keep auth disabled for the dev stack" >&2; return 1; }
  grep -q '^[[:space:]]*allow_structured_metadata: true' <<<"$code" ||
    { echo "Loki config must allow structured metadata" >&2; return 1; }
  grep -q '^[[:space:]]*discover_log_levels: false' <<<"$code" ||
    { echo "Loki config must disable log-level discovery" >&2; return 1; }
  if grep -q 'discover_service_name' <<<"$code"; then
    echo "Loki config must leave service-name discovery at the upstream default" >&2
    return 1
  fi
  if grep -q 'otlp_config' <<<"$code"; then
    echo "Loki config must leave distributor.otlp_config at the upstream defaults" >&2
    return 1
  fi
  return 0
}

# loki_effective_disables_service_discovery matches the phase 2 helper: the
# effective /config response may render an empty list inline or on the next
# line. It reads a file, never a pipeline fed by a large config string, so
# grep's early exit cannot turn into a SIGPIPE under pipefail.
loki_effective_disables_service_discovery() {
  cfg=$1
  grep -Eq 'discover_service_name:[[:space:]]*\[\]' "$cfg" && return 0
  line=$(awk '/discover_service_name:/{getline; print; exit}' "$cfg")
  grep -Eq '^[[:space:]]*\[\]' <<<"$line"
}

check_loki_effective() {
  cfg=$1
  grep -q 'discover_log_levels: false' "$cfg" ||
    { echo "effective Loki config enables log-level discovery" >&2; return 1; }
  grep -q 'allow_structured_metadata: true' "$cfg" ||
    { echo "effective Loki config does not allow structured metadata" >&2; return 1; }
  if loki_effective_disables_service_discovery "$cfg"; then
    echo "effective Loki config disables service-name discovery; defaults are required" >&2
    return 1
  fi
  return 0
}

check_static_files() {
  [[ -r $BASE_COMPOSE ]] || fail "missing base compose file: $BASE_COMPOSE"
  [[ -r $OVERLAY_COMPOSE ]] || fail "missing OTLP overlay: $OVERLAY_COMPOSE"
  [[ -r $ALLOY_CONFIG ]] || fail "missing Alloy OTLP config: $ALLOY_CONFIG"
  [[ -r $LOKI_CONFIG ]] || fail "missing Loki OTLP config: $LOKI_CONFIG"
  grep -Fq "$ALLOY_PIN" "$BASE_COMPOSE" ||
    fail "base compose does not pin the phase 2 Alloy image ($ALLOY_PIN)"
  grep -Fq "$LOKI_PIN" "$BASE_COMPOSE" ||
    fail "base compose does not pin Loki 3.7.9 ($LOKI_PIN)"
  if grep -Eq '^[[:space:]]*image:' "$OVERLAY_COMPOSE"; then
    fail "OTLP overlay must not override an image pin"
  fi
  grep -Fq './otlp.alloy:/etc/alloy/otlp.alloy:ro' "$OVERLAY_COMPOSE" ||
    fail "OTLP overlay does not mount otlp.alloy"
  grep -Fq './otlp.loki.yaml:/etc/loki/otlp-config.yaml:ro' "$OVERLAY_COMPOSE" ||
    fail "OTLP overlay does not mount otlp.loki.yaml"
  grep -Fq 'ALLOY_OTLP_HTTP_PORT' "$OVERLAY_COMPOSE" ||
    fail "OTLP overlay does not publish Alloy OTLP/HTTP"
  grep -Fq 'PARKANA_ROLES: ingester,querier' "$OVERLAY_COMPOSE" ||
    fail "OTLP overlay does not select the ingester/querier roles"
  if grep -q 'PARKANA_COMPACTOR_' "$OVERLAY_COMPOSE"; then
    fail "OTLP overlay must not tune the compactor; the compactor role is not started"
  fi
  check_alloy_config_file "$ALLOY_CONFIG" || fail "Alloy OTLP config failed static checks"
  check_loki_config_file "$LOKI_CONFIG" || fail "Loki OTLP config failed static checks"
}

# --- credentials, ports and output -----------------------------------------

random_hex() { od -An -N "$1" -tx1 /dev/urandom | tr -d ' \n'; }

generate_credentials() {
  # Explicit credentials are honored so a CI/e2e harness can supply throwaway
  # values (and so the self-test can prove redaction). Otherwise every run
  # generates fresh dev credentials.
  if [[ -n ${PARKANA_DEV_GARAGE_KEY_ID:-} && -n ${PARKANA_DEV_GARAGE_KEY_SECRET:-} ]]; then
    GARAGE_KEY_ID=$PARKANA_DEV_GARAGE_KEY_ID
    GARAGE_KEY_SECRET=$PARKANA_DEV_GARAGE_KEY_SECRET
  else
    GARAGE_KEY_ID="GK$(random_hex 12)"
    GARAGE_KEY_SECRET="$(random_hex 32)"
  fi
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
  [[ $port == "$PARKANA_PORT" || $port == "$S3_PORT" || $port == "$LOKI_PORT" ||
    $port == "$ALLOY_PORT" || $port == "$ALLOY_OTLP_HTTP_PORT" || $port == "$ALLOY_OTLP_GRPC_PORT" ]]
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
    candidate=$((20000 + RANDOM % 20000))
    port_used_by_this_run "$candidate" && continue
    if port_free "$candidate"; then
      printf '[e2e-p4-otlp] %s port %s is busy; using %s\n' "$label" "$preferred" "$candidate" >&2
      printf '%s' "$candidate"
      return 0
    fi
  done
  usage_fail "could not find a free $label port; set $envname or pass the matching flag"
}

resolve_ports() {
  local value
  value=$(choose_port "$PARKANA_PORT" "$PARKANA_EXPLICIT" "Parkana HTTP" PARKANA_HTTP_PORT) || exit 2
  PARKANA_PORT=$value
  value=$(choose_port "$S3_PORT" "$S3_EXPLICIT" "Garage S3" PARKANA_S3_PORT) || exit 2
  S3_PORT=$value
  value=$(choose_port "$LOKI_PORT" "$LOKI_EXPLICIT" "Loki" LOKI_HTTP_PORT) || exit 2
  LOKI_PORT=$value
  value=$(choose_port "$ALLOY_PORT" "$ALLOY_EXPLICIT" "Alloy UI" ALLOY_HTTP_PORT) || exit 2
  ALLOY_PORT=$value
  value=$(choose_port "$ALLOY_OTLP_HTTP_PORT" "$ALLOY_OTLP_HTTP_EXPLICIT" "Alloy OTLP/HTTP" ALLOY_OTLP_HTTP_PORT) || exit 2
  ALLOY_OTLP_HTTP_PORT=$value
  value=$(choose_port "$ALLOY_OTLP_GRPC_PORT" "$ALLOY_OTLP_GRPC_EXPLICIT" "Alloy OTLP/gRPC" ALLOY_OTLP_GRPC_PORT) || exit 2
  ALLOY_OTLP_GRPC_PORT=$value

  local seen=() p
  for p in "$PARKANA_PORT" "$S3_PORT" "$LOKI_PORT" "$ALLOY_PORT" "$ALLOY_OTLP_HTTP_PORT" "$ALLOY_OTLP_GRPC_PORT"; do
    [[ " ${seen[*]:-} " != *" $p "* ]] || usage_fail "resolved host port $p is used twice"
    seen+=("$p")
  done
}

export_settings() {
  export PARKANA_HTTP_PORT=$PARKANA_PORT
  export PARKANA_S3_PORT=$S3_PORT
  export LOKI_HTTP_PORT=$LOKI_PORT
  export ALLOY_HTTP_PORT=$ALLOY_PORT
  export ALLOY_OTLP_HTTP_PORT ALLOY_OTLP_GRPC_PORT
  export PARKANA_INGEST_FLUSH_INTERVAL=$FLUSH_INTERVAL
  export PARKANA_INGEST_UPLOAD_INTERVAL=$UPLOAD_INTERVAL
  export PARKANA_INGEST_UPLOAD_MIN=$UPLOAD_MIN
  export PARKANA_INGEST_UPLOAD_MAX_AGE=$UPLOAD_MAX_AGE
  export PARKANA_ROLES=$ROLES
  export PARKANA_INGEST_FLUSH_CONCURRENCY=$FLUSH_CONCURRENCY
  export PARKANA_INGEST_UPLOAD_CONCURRENCY=$UPLOAD_CONCURRENCY
  export PARKANA_WAL_SYNC=$WAL_SYNC
  export PARKANA_WAL_SYNC_INTERVAL=$WAL_SYNC_INTERVAL
}

set_urls() {
  PARKANA_URL="http://127.0.0.1:${PARKANA_PORT}"
  LOKI_URL="http://127.0.0.1:${LOKI_PORT}"
  ALLOY_URL="http://127.0.0.1:${ALLOY_PORT}"
  ALLOY_OTLP_URL="http://127.0.0.1:${ALLOY_OTLP_HTTP_PORT}"
  LOKI_OTLP_URL="$LOKI_URL/otlp/v1/logs"
  PARKANA_OTLP_URL="$PARKANA_URL/v1/logs"
  ALLOY_OTLP_LOGS_URL="$ALLOY_OTLP_URL/v1/logs"
}

create_out_dir() {
  if [[ -z $OUT_DIR ]]; then
    OUT_DIR=$(mktemp -d "${TMPDIR:-/tmp}/parkana-otlp-e2e.XXXXXX")
  else
    mkdir -p "$OUT_DIR"
  fi
  chmod 700 "$OUT_DIR"
  mkdir -p "$OUT_DIR/bin"
}

# --- compose ---------------------------------------------------------------

compose_quick() {
  timeout 30 "$DOCKER" compose -p "$PROJECT" --project-directory "$COMPOSE_DIR" \
    -f "$BASE_COMPOSE" -f "$OVERLAY_COMPOSE" --profile phase2 "$@"
}

compose_long() {
  timeout "$COMPOSE_TIMEOUT" "$DOCKER" compose -p "$PROJECT" --project-directory "$COMPOSE_DIR" \
    -f "$BASE_COMPOSE" -f "$OVERLAY_COMPOSE" --profile phase2 "$@"
}

sanitize() { grep -Fv -- "$PARKANA_DEV_GARAGE_KEY_SECRET" || true; }

# project_exists returns 0 when any container, volume or network belongs to the
# project, 1 when all three listings are empty, and 2 when a listing itself
# fails. A failed listing is never treated as "no project": starting over an
# unreadable daemon state could collide with or tear down someone else's run.
project_exists() {
  local containers volumes networks
  containers=$(compose_quick ps -a -q) || return 2
  volumes=$(timeout 20 "$DOCKER" volume ls -q --filter "label=com.docker.compose.project=$PROJECT") || return 2
  networks=$(timeout 20 "$DOCKER" network ls -q --filter "label=com.docker.compose.project=$PROJECT") || return 2
  [[ -n $containers$volumes$networks ]] && return 0
  return 1
}

ensure_project_available() {
  local rc=0
  project_exists || rc=$?
  case "$rc" in
    0) fail "compose project '$PROJECT' already exists" ;;
    1) return 0 ;;
    *) fail "could not list compose project '$PROJECT' (containers, volumes or networks); refusing to start" ;;
  esac
}

# check_published_ports also fails closed: an unreadable container list must
# not be mistaken for "all ports are free".
check_published_ports() {
  local ports port
  ports=$(timeout 20 "$DOCKER" ps --format '{{.Ports}}') ||
    fail "could not list published container ports; refusing to start"
  for port in "$PARKANA_PORT" "$S3_PORT" "$LOKI_PORT" "$ALLOY_PORT" "$ALLOY_OTLP_HTTP_PORT" "$ALLOY_OTLP_GRPC_PORT"; do
    if grep -q ":$port->" <<<"$ports"; then
      fail "host port $port is already published"
    fi
  done
}

# The lock is created before the project listing and removed in cleanup. It
# makes two invocations for the same project fail before either can inspect or
# tear down the other's containers. Only the owner removes it.
acquire_project_lock() {
  mkdir -p "$LOCK_ROOT" || fail "could not create lock root: $LOCK_ROOT"
  LOCK_DIR="$LOCK_ROOT/parkana-otlp-e2e-lock-$PROJECT"
  if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    fail "compose project '$PROJECT' is locked by another invocation: $LOCK_DIR"
  fi
  LOCK_OWNED=1
  printf '%s\n' "$$" >"$LOCK_DIR/pid" 2>/dev/null || true
}

release_project_lock() {
  if [[ $LOCK_OWNED == 1 && -n $LOCK_DIR ]]; then
    rm -rf "$LOCK_DIR" 2>/dev/null || true
    LOCK_OWNED=0
  fi
}

diagnostics() {
  compose_quick ps 2>&1 | sanitize >&2 || true
  for svc in garage garage-init parkana loki alloy; do
    echo "--- $svc (last 40 lines) ---" >&2
    compose_quick logs --no-color --tail=40 "$svc" 2>&1 | sanitize >&2 || true
  done
}

cleanup_project() {
  if [[ $created == 1 && $KEEP == 0 ]]; then
    if ! compose_long down --volumes --remove-orphans >/dev/null 2>&1; then
      echo "cleanup failed: docker compose down did not remove project '$PROJECT'" >&2
      return 1
    fi
  fi
  return 0
}

cleanup() {
  status=$?
  cleanup_status=0
  trap - EXIT INT TERM
  if [[ $status -ne 0 && $created == 1 ]]; then
    diagnostics || true
  fi
  if [[ $created == 1 && $KEEP == 1 ]]; then
    echo "keeping project '$PROJECT'; teardown: $DOCKER compose -p $PROJECT --project-directory $COMPOSE_DIR -f $BASE_COMPOSE -f $OVERLAY_COMPOSE --profile phase2 down --volumes --remove-orphans" >&2
  fi
  cleanup_project || cleanup_status=$?
  release_project_lock
  # Evidence (manifests and reports) is always kept. --keep controls only the
  # Compose project, never the output directory.
  if [[ -n $OUT_DIR ]]; then
    printf '[e2e-p4-otlp] output preserved: %s\n' "$OUT_DIR"
  fi
  if [[ $status -ne 0 ]]; then
    exit "$status"
  fi
  exit "$cleanup_status"
}

# --- readiness -------------------------------------------------------------

now() { date +%s; }

wait_http() { # url name timeout
  url=$1 name=$2 timeout=${3:-180}
  start=$(now)
  deadline=$((start + timeout))
  while :; do
    if curl -fsS --connect-timeout 2 --max-time 5 "$url" >/dev/null 2>&1; then
      return 0
    fi
    t=$(now)
    if [[ $t -ge $deadline ]]; then
      echo "$name not ready after $((t - start))s: $url" >&2
      return 1
    fi
    sleep 1
  done
}

wait_version() { # url want timeout label
  url=$1 want=$2 timeout=${3:-180} label=${4:-$url}
  start=$(now)
  deadline=$((start + timeout))
  while :; do
    body=$(curl -fsS --connect-timeout 2 --max-time 10 "$url" 2>/dev/null || true)
    got=$(sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' <<<"$body" | sed -n '1p')
    if [[ $got == "$want" ]]; then
      echo "$label version: $got"
      return 0
    fi
    t=$(now)
    if [[ $t -ge $deadline ]]; then
      echo "$label version is '$got', want $want after $((t - start))s" >&2
      return 1
    fi
    sleep 1
  done
}

wait_initialized() { # service want timeout
  svc=$1 want=$2 timeout=${3:-300}
  start=$(now)
  deadline=$((start + timeout))
  while :; do
    id=$(compose_quick ps -a -q "$svc" 2>/dev/null || true)
    if [[ -n $id ]]; then
      state=$(timeout 10 "$DOCKER" inspect -f '{{.State.Status}}' "$id" 2>/dev/null || true)
      if [[ $state == exited ]]; then
        code=$(timeout 10 "$DOCKER" inspect -f '{{.State.ExitCode}}' "$id" 2>/dev/null || true)
        if [[ $code == "$want" ]]; then
          return 0
        fi
        echo "$svc exited $code, want $want" >&2
        return 1
      fi
    fi
    t=$(now)
    if [[ $t -ge $deadline ]]; then
      echo "$svc did not exit within $((t - start))s" >&2
      return 1
    fi
    sleep 1
  done
}

# probe_otlp posts an empty OTLP protobuf export, the same readiness payload
# otlpcompare uses. A 2xx is a healthy receiver. Loki additionally answers an
# empty push with 422 (MissingStreams) after the OTLP route and decoder have
# accepted the request; allow_empty_rejection accepts that one status so the
# probe still proves the native route exists. Any other status, including 404
# from a missing route, fails.
probe_otlp() {
  url=$1
  allow_empty_rejection=${2:-0}
  local body
  body=$(mktemp "${TMPDIR:-/tmp}/parkana-otlp-probe.XXXXXX") || return 1
  code=$(curl -sS --connect-timeout 2 --max-time 10 -o "$body" -w '%{http_code}' \
    -X POST -H 'Content-Type: application/x-protobuf' --data-binary '' "$url" 2>/dev/null || true)
  OTLP_PROBE_CODE=$code
  case "$code" in
    2??)
      rm -f "$body"
      return 0
      ;;
    422)
      [[ $allow_empty_rejection == 1 ]] || {
        rm -f "$body"
        return 1
      }
      # The body is the OTLP/gRPC Status protobuf written by Loki's
      # OTLPError (pkg/loghttp/push/push.go:571-601). Its message field is
      # validation.MissingStreamsErrorMsg (pkg/validation/validate.go:15),
      # including the leading "error " prefix. Requiring the full marker
      # rejects an unrelated 422 and proves the OTLP route decoded the empty
      # export before rejecting the push. The body stays in a tempfile: a Bash
      # command substitution would drop its NUL bytes.
      if grep -aqF -- "$LOKI_EMPTY_PUSH_MARKER" "$body"; then
        rm -f "$body"
        return 0
      fi
      rm -f "$body"
      return 1
      ;;
    *)
      rm -f "$body"
      return 1
      ;;
  esac
}

wait_otlp_endpoint() { # url name timeout [allow-empty-rejection]
  url=$1 name=$2 timeout=${3:-180} allow=${4:-0}
  start=$(now)
  deadline=$((start + timeout))
  while :; do
    if probe_otlp "$url" "$allow"; then
      return 0
    fi
    t=$(now)
    if [[ $t -ge $deadline ]]; then
      echo "$name OTLP probe is '${OTLP_PROBE_CODE:-<no response>}', want 2xx (or Loki's empty-push 422) after $((t - start))s: $url" >&2
      return 1
    fi
    sleep 1
  done
}

check_parkana_env() {
  local env_parkana want
  env_parkana=$(compose_quick exec -T parkana sh -c 'env' 2>/dev/null || true)
  for want in \
    "PARKANA_ROLES=$ROLES" \
    "PARKANA_INGEST_FLUSH_INTERVAL=$FLUSH_INTERVAL" \
    "PARKANA_INGEST_UPLOAD_INTERVAL=$UPLOAD_INTERVAL" \
    "PARKANA_INGEST_UPLOAD_MIN=$UPLOAD_MIN" \
    "PARKANA_INGEST_UPLOAD_MAX_AGE=$UPLOAD_MAX_AGE" \
    "PARKANA_INGEST_FLUSH_CONCURRENCY=$FLUSH_CONCURRENCY" \
    "PARKANA_INGEST_UPLOAD_CONCURRENCY=$UPLOAD_CONCURRENCY" \
    "PARKANA_WAL_SYNC=$WAL_SYNC" \
    "PARKANA_WAL_SYNC_INTERVAL=$WAL_SYNC_INTERVAL"; do
    grep -qx -- "$want" <<<"$env_parkana" ||
      fail "effective Parkana setting missing: $want"
  done
  echo "Parkana roles=$ROLES flush=$FLUSH_INTERVAL upload=$UPLOAD_INTERVAL upload-min=$UPLOAD_MIN max-age=$UPLOAD_MAX_AGE workers=$FLUSH_CONCURRENCY/$UPLOAD_CONCURRENCY WAL=$WAL_SYNC/$WAL_SYNC_INTERVAL: OK"
}

check_effective_loki() {
  local loki_config_file
  loki_config_file="$OUT_DIR/loki-effective-config.yaml"
  curl -fsS --connect-timeout 2 --max-time 10 -o "$loki_config_file" "$LOKI_URL/config" ||
    fail "Loki /config is not available"
  check_loki_effective "$loki_config_file" ||
    fail "effective Loki config is not the intended OTLP comparison config (raw config: $loki_config_file)"
  grep -E '^[[:space:]]*(allow_structured_metadata|discover_log_levels|discover_service_name):' "$loki_config_file" |
    sed 's/^[[:space:]]*/  /' || true
  echo "Loki log-level discovery off, service-name discovery default, structured metadata on: OK"
  echo "raw effective Loki config: $loki_config_file"
}

# --- comparator ------------------------------------------------------------

build_comparator() {
  command -v -- "$GO" >/dev/null 2>&1 || fail "go is required for --full (otlpcompare is built from source)"
  COMPARE_BIN="$OUT_DIR/bin/otlpcompare"
  if ! "$GO" -C "$REPO_ROOT" build -o "$COMPARE_BIN" ./cmd/otlpcompare; then
    fail "go build ./cmd/otlpcompare failed; the OTLP comparator is required and no stack was started"
  fi
  [[ -x $COMPARE_BIN ]] || fail "go build did not produce $COMPARE_BIN"
  log "comparator: $COMPARE_BIN"
}

anchor_now() {
  date -u +%Y-%m-%dT%H:%M:%S.%6NZ
}

send_case() { # target manifest namespace runid anchor codec gzip
  local target=$1 manifest=$2 ns=$3 rid=$4 anchor=$5 codec=$6 gzip=$7
  local args=(
    "$COMPARE_BIN" send
    -target "$target"
    -manifest "$manifest"
    -namespace "$ns"
    -run-id "$rid"
    -anchor "$anchor"
    -codec "$codec"
    -tenant "$TENANT"
    -timeout "$TIMEOUT"
    -retries "$RETRIES"
    -ready-timeout "$SEND_READY_TIMEOUT"
  )
  ((gzip == 1)) && args+=(-gzip)
  "${args[@]}"
}

compare_case() { # manifest report
  local manifest=$1 report=$2
  "$COMPARE_BIN" compare \
    -loki "$LOKI_URL" \
    -parkana "$PARKANA_URL" \
    -manifest "$manifest" \
    -tenant "$TENANT" \
    -timeout "$TIMEOUT" \
    -wait-timeout "$WAIT_TIMEOUT" \
    -poll-interval "$POLL_INTERVAL" \
    -ready-timeout "$SEND_READY_TIMEOUT" \
    -format text \
    -report "$report"
}

compare_or_fail() { # name manifest report
  local name=$1 manifest=$2 report=$3 status=0
  compare_case "$manifest" "$report" || status=$?
  case "$status" in
    0)
      [[ -s $report ]] || fail "case $name: comparator reported EQUAL but wrote no report: $report"
      log "case $name: EQUAL (report $report)"
      ;;
    1)
      fail "case $name: semantic difference between Loki and Parkana (report $report)"
      ;;
    2)
      echo "[e2e-p4-otlp] FAIL: case $name: comparator invocation or request/data failure (exit 2)" >&2
      exit 2
      ;;
    *)
      echo "[e2e-p4-otlp] FAIL: case $name: comparator exited $status" >&2
      exit "$status"
      ;;
  esac
  return 0
}

send_or_fail() { # label target manifest namespace runid anchor codec gzip
  local label=$1
  shift
  local status=0
  send_case "$@" || status=$?
  if [[ $status -ne 0 ]]; then
    echo "[e2e-p4-otlp] FAIL: $label: send exited $status (invocation or request/data failure)" >&2
    exit 2
  fi
}

run_direct_case() { # name codec gzip
  local name=$1 codec=$2 gzip=$3
  local ns="$PROJECT-$name" rid="$PROJECT-$name" anchor
  anchor=$(anchor_now)
  [[ $anchor =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]{15}Z$ ]] || fail "could not build an RFC3339 anchor: $anchor"
  local man_loki="$OUT_DIR/manifest-$name-loki.json"
  local man_parkana="$OUT_DIR/manifest-$name-parkana.json"

  say "case $name (direct, codec=$codec gzip=$gzip)"
  send_or_fail "$name/loki" "$LOKI_OTLP_URL" "$man_loki" "$ns" "$rid" "$anchor" "$codec" "$gzip"
  send_or_fail "$name/parkana" "$PARKANA_OTLP_URL" "$man_parkana" "$ns" "$rid" "$anchor" "$codec" "$gzip"
  compare_or_fail "$name" "$man_loki" "$OUT_DIR/report-$name.json"

  CASE_NAMES+=("$name")
  CASE_MANIFESTS+=("$man_loki")
}

run_alloy_case() { # name codec gzip
  local name=$1 codec=$2 gzip=$3
  local ns="$PROJECT-$name" rid="$PROJECT-$name" anchor
  anchor=$(anchor_now)
  [[ $anchor =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]{15}Z$ ]] || fail "could not build an RFC3339 anchor: $anchor"
  local manifest="$OUT_DIR/manifest-$name-alloy.json"

  say "case $name (Alloy tee, codec=$codec gzip=$gzip)"
  send_or_fail "$name/alloy" "$ALLOY_OTLP_LOGS_URL" "$manifest" "$ns" "$rid" "$anchor" "$codec" "$gzip"
  compare_or_fail "$name" "$manifest" "$OUT_DIR/report-$name.json"

  CASE_NAMES+=("$name")
  CASE_MANIFESTS+=("$manifest")
}

run_cases() {
  say "transport cases"
  run_direct_case direct-protobuf protobuf 0
  run_direct_case direct-json-gzip json 1
  run_alloy_case alloy-tee-mixed-gzip mixed 1
}

restart_write_paths() {
  say "restart writers and recompare"
  compose_long restart parkana loki alloy ||
    fail "writer restart failed"
  wait_http "$PARKANA_URL/ready" "Parkana /ready after restart" "$READY_TIMEOUT" ||
    fail "Parkana not ready after restart"
  wait_http "$LOKI_URL/ready" "Loki /ready after restart" "$READY_TIMEOUT" ||
    fail "Loki not ready after restart"
  wait_http "$ALLOY_URL/-/ready" "Alloy /-/ready after restart" "$READY_TIMEOUT" ||
    fail "Alloy not ready after restart"
  echo "Parkana, Loki and Alloy restarted and ready"
}

recompare_after_restart() {
  local i name manifest
  for i in "${!CASE_NAMES[@]}"; do
    name=${CASE_NAMES[$i]}
    manifest=${CASE_MANIFESTS[$i]}
    compare_or_fail "$name/after-restart" "$manifest" "$OUT_DIR/report-$name-after-restart.json"
  done
}

# --- plan / dry run --------------------------------------------------------

dry_run() {
  printf '[e2e-p4-otlp] dry run: no Docker, Go or services are touched\n'
  printf '  repo root       : %s\n' "$REPO_ROOT"
  printf '  compose         : %s + %s (profile phase2)\n' "$BASE_COMPOSE" "$OVERLAY_COMPOSE"
  printf '  mode            : %s\n' "$MODE"
  printf '  project         : %s%s\n' "$PROJECT" "$([[ $PROJECT_EXPLICIT == 1 ]] && printf ' (explicit)' || printf ' (generated)')"
  printf '  output          : %s%s (kept on success and failure)\n' "${OUT_DIR:-<generated mktemp directory>}" "$([[ $OUT_EXPLICIT == 1 ]] && printf ' (explicit)' || printf ' (generated)')"
  printf '  host ports      : parkana=%s garage=%s loki=%s alloy=%s alloy-otlp-http=%s alloy-otlp-grpc=%s\n' \
    "$PARKANA_PORT" "$S3_PORT" "$LOKI_PORT" "$ALLOY_PORT" "$ALLOY_OTLP_HTTP_PORT" "$ALLOY_OTLP_GRPC_PORT"
  printf '  otlp targets    : loki=%s parkana=%s alloy=%s\n' \
    "http://127.0.0.1:$LOKI_PORT/otlp/v1/logs" \
    "http://127.0.0.1:$PARKANA_PORT/v1/logs" \
    "http://127.0.0.1:$ALLOY_OTLP_HTTP_PORT/v1/logs"
  printf '  tenant          : %s\n' "$TENANT"
  printf '  timeouts        : request=%s wait=%s poll=%s ready=%ss compose=%ss send-ready=%s retries=%s\n' \
    "$TIMEOUT" "$WAIT_TIMEOUT" "$POLL_INTERVAL" "$READY_TIMEOUT" "$COMPOSE_TIMEOUT" "$SEND_READY_TIMEOUT" "$RETRIES"
  printf '  parkana         : roles=%s flush=%s upload=%s upload-min=%s max-age=%s workers=%s/%s WAL=%s/%s\n' \
    "$ROLES" "$FLUSH_INTERVAL" "$UPLOAD_INTERVAL" "$UPLOAD_MIN" "$UPLOAD_MAX_AGE" "$FLUSH_CONCURRENCY" "$UPLOAD_CONCURRENCY" "$WAL_SYNC" "$WAL_SYNC_INTERVAL"
  if [[ $MODE == full ]]; then
    printf '  cases           : direct-protobuf, direct-json-gzip, alloy-tee-mixed-gzip\n'
    printf '  restart         : parkana, loki, alloy; every case recompared\n'
  else
    printf '  cases           : not run in smoke mode\n'
  fi
  printf '  Garage creds    : generated per run (values hidden)\n'
  printf '  lock            : %s/parkana-otlp-e2e-lock-%s\n' "$LOCK_ROOT" "$PROJECT"
  printf '  cleanup scope   : this invocation only (its own project and lock)\n'
  printf '[e2e-p4-otlp] dry run: no changes made\n'
}

# --- self tests ------------------------------------------------------------

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

test_defaults() (
  init_defaults
  [[ $MODE == full && $PARKANA_PORT == 14100 && $S3_PORT == 14900 ]] || return 1
  [[ $LOKI_PORT == 14101 && $ALLOY_PORT == 14245 ]] || return 1
  [[ $ALLOY_OTLP_HTTP_PORT == 14318 && $ALLOY_OTLP_GRPC_PORT == 14317 ]] || return 1
  [[ $TENANT == default && $TIMEOUT == 30s && $WAIT_TIMEOUT == 60s ]] || return 1
  [[ $POLL_INTERVAL == 1s && $READY_TIMEOUT == 180 && $RETRIES == 5 ]] || return 1
  [[ $COMPOSE_TIMEOUT == 900 && $ROLES == ingester,querier ]] || return 1
  [[ $FLUSH_INTERVAL == 1s && $UPLOAD_MIN == 1B && $UPLOAD_MAX_AGE == 2s ]] || return 1
  [[ $FLUSH_CONCURRENCY == 2 && $UPLOAD_CONCURRENCY == 2 ]] || return 1
  [[ $WAL_SYNC == always ]]
)

test_validation_rejects() {
  local bad
  for bad in "--tenant acme" "--timeout 0s" "--wait-timeout -1s" "--poll-interval 0" \
    "--ready-timeout 0" "--ready-timeout 99999" "--retries 0" "--retries 101" \
    "--project Bad_Name" "--parkana-port 70000" "--alloy-otlp-http-port 1+1"; do
    # shellcheck disable=SC2086
    if (init_defaults; parse_args $bad; validate_args) >/dev/null 2>&1; then
      echo "invalid arguments accepted: $bad" >&2
      return 1
    fi
  done
  if (init_defaults; OUT_EXPLICIT=1; OUT_DIR=/; validate_args) >/dev/null 2>&1; then
    echo "an existing output directory was accepted" >&2
    return 1
  fi
  return 0
}

test_duplicate_ports_rejected() {
  (init_defaults; ALLOY_OTLP_HTTP_PORT=14100; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; WAL_SYNC=sometimes; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; UPLOAD_MIN=0; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; ROLES=all; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; FLUSH_CONCURRENCY=65; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; UPLOAD_CONCURRENCY=0; validate_args) >/dev/null 2>&1 && return 1
  (init_defaults; FLUSH_CONCURRENCY=64; validate_args) >/dev/null 2>&1 || return 1
  return 0
}

test_config_checks() {
  local dir=$1
  check_alloy_config_file "$ALLOY_CONFIG" || return 1
  check_loki_config_file "$LOKI_CONFIG" || return 1

  printf 'otelcol.receiver.otlp "x" { output { logs = [some.other.input] } }\n' >"$dir/alloy-bad.alloy"
  if check_alloy_config_file "$dir/alloy-bad.alloy" >/dev/null 2>&1; then return 1; fi
  printf 'otelcol.receiver.otlp "x" { output { logs = [] } }\nloki.write "x" {}\n' >"$dir/alloy-write.alloy"
  if check_alloy_config_file "$dir/alloy-write.alloy" >/dev/null 2>&1; then return 1; fi

  cp "$LOKI_CONFIG" "$dir/loki-bad.yaml"
  printf '\nlimits_config:\n  discover_service_name: []\n' >>"$dir/loki-bad.yaml"
  if check_loki_config_file "$dir/loki-bad.yaml" >/dev/null 2>&1; then return 1; fi
  cp "$LOKI_CONFIG" "$dir/loki-otlp.yaml"
  printf '\ndistributor:\n  otlp_config:\n    resource_attributes:\n      ignore_defaults: true\n' >>"$dir/loki-otlp.yaml"
  if check_loki_config_file "$dir/loki-otlp.yaml" >/dev/null 2>&1; then return 1; fi

  cat >"$dir/loki-effective-good.yaml" <<'YAML'
limits_config:
  allow_structured_metadata: true
  discover_log_levels: false
  discover_service_name:
  - service
  - job
YAML
  check_loki_effective "$dir/loki-effective-good.yaml" || return 1

  cat >"$dir/loki-effective-empty-discovery.yaml" <<'YAML'
limits_config:
  allow_structured_metadata: true
  discover_log_levels: false
  discover_service_name: []
YAML
  if check_loki_effective "$dir/loki-effective-empty-discovery.yaml" >/dev/null 2>&1; then
    return 1
  fi

  cat >"$dir/loki-effective-levels.yaml" <<'YAML'
limits_config:
  allow_structured_metadata: true
  discover_log_levels: true
  discover_service_name:
  - service
YAML
  if check_loki_effective "$dir/loki-effective-levels.yaml" >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

# test_loki_effective_large_fixture guards the pipefail/SIGPIPE regression: a
# grep -q fed by a large config string can exit before printf finishes, making
# an otherwise valid config look invalid. The checker reads a file/here-string
# instead, so a real-shaped multi-kilobyte config must pass.
test_loki_effective_large_fixture() {
  local dir=$1 file="$dir/loki-effective-large.yaml" bad="$dir/loki-effective-large-bad.yaml"
  {
    printf 'limits_config:\n'
    printf '  allow_structured_metadata: true\n'
    printf '  discover_log_levels: false\n'
    printf '  discover_service_name:\n'
    printf '  - service\n'
    printf '  - job\n'
    for i in $(seq 1 5000); do
      printf '  # padding line %05d: realistic effective config content\n' "$i"
    done
    printf 'server:\n  log_level: info\n'
  } >"$file"
  [[ $(wc -c <"$file") -ge 100000 ]] ||
    { echo "selftest: large Loki config fixture is under 100KB" >&2; return 1; }
  check_loki_effective "$file" || return 1

  sed 's/discover_log_levels: false/discover_log_levels: true/' "$file" >"$bad"
  if check_loki_effective "$bad" >/dev/null 2>&1; then
    echo "selftest: large config with log-level discovery enabled passed" >&2
    return 1
  fi
  sed '/discover_log_levels:/d' "$file" >"$bad"
  if check_loki_effective "$bad" >/dev/null 2>&1; then
    echo "selftest: large config missing log-level setting passed" >&2
    return 1
  fi
  sed 's/^  discover_service_name:/  discover_service_name: []\n  # removed list:/' "$file" >"$bad"
  if check_loki_effective "$bad" >/dev/null 2>&1; then
    echo "selftest: large config with empty service-name discovery passed" >&2
    return 1
  fi
  return 0
}

# write_mocks creates a Docker/curl/go/otlpcompare mock set in $1 and prints
# the template path through the global MOCK_CLI_TEMPLATE.
test_otlp_probe_statuses() {
  local dir=$1
  curl() {
    out=""
    prev=""
    for a in "$@"; do
      if [ "$prev" = "-o" ]; then out=$a; fi
      prev=$a
    done
    if [ -n "$out" ]; then
      if [ -n "${MOCK_BODY_FILE:-}" ]; then
        cat "$MOCK_BODY_FILE" >"$out"
      else
        printf '%s' "${MOCK_BODY:-}" >"$out"
      fi
    fi
    printf '%s' "${MOCK_STATUS:-204}"
  }
  MOCK_STATUS=204
  probe_otlp http://mock/logs 0 || return 1
  MOCK_STATUS=422
  MOCK_BODY=$LOKI_EMPTY_PUSH_MARKER
  probe_otlp http://mock/logs 1 || return 1
  if probe_otlp http://mock/logs 0; then return 1; fi

  # A binary OTLP/gRPC Status body contains NUL bytes; the tempfile path must
  # still find the exact marker.
  printf 'prefix\000error at least one valid stream is required for ingestion\000suffix' >"$dir/status-422.bin"
  MOCK_BODY_FILE="$dir/status-422.bin"
  probe_otlp http://mock/logs 1 || return 1
  unset MOCK_BODY_FILE

  MOCK_BODY='error something else'
  if probe_otlp http://mock/logs 1; then return 1; fi
  MOCK_BODY=''
  MOCK_STATUS=404
  if probe_otlp http://mock/logs 1; then return 1; fi
  MOCK_STATUS=000
  if probe_otlp http://mock/logs 1; then return 1; fi
  return 0
}

write_mocks() {
  local dir=$1
  mkdir -p "$dir/bin" "$dir/home"
  MOCK_CLI_TEMPLATE="$dir/cli-template"

  cat >"$dir/bin/docker" <<'MOCK'
#!/bin/sh
log=${MOCK_DOCKER_LOG:?MOCK_DOCKER_LOG not set}
printf 'DOCKER %s\n' "$*" >>"$log"
case "$1" in
  compose)
    shift
    if [ "${MOCK_VALIDATE_F_PATHS:-0}" = 1 ]; then
      prev=""
      for arg in "$@"; do
        if [ "$prev" = "-f" ] && [ ! -f "$arg" ]; then
          echo "mock: compose -f path does not exist: $arg" >&2
          exit 2
        fi
        prev=$arg
      done
      printf 'F_PATHS_OK\n' >>"${MOCK_DOCKER_LOG:?MOCK_DOCKER_LOG not set}"
    fi
    case " $* " in
      *" config --services "*)
        printf 'garage\ngarage-init\nparkana\nloki\nalloy\ngrafana\n'
        exit 0
        ;;
      *" config "*)
        [ "${MOCK_CONFIG_FAIL:-0}" = 1 ] && exit 1
        exit 0
        ;;
      *" build "*)
        [ "${MOCK_BUILD_FAIL:-0}" = 1 ] && exit 1
        exit 0
        ;;
      *" up -d "*)
        if [ "${MOCK_UP_FAIL:-0}" = 1 ]; then
          echo "mock: up failure" >&2
          exit 1
        fi
        printf 'UP\n' >>"${MOCK_EVENT_LOG:-/dev/null}"
        exit 0
        ;;
      *" ps -a -q "*)
        case "${MOCK_LIST_FAIL:-0}" in 1|project) exit 1 ;; esac
        last=""
        for arg in "$@"; do last=$arg; done
        if [ "$last" = "-q" ]; then
          [ "${MOCK_PROJECT_EXISTS:-0}" = 1 ] && echo mock-container
        else
          echo "id-$last"
        fi
        exit 0
        ;;
      *" ps "*)
        exit 0
        ;;
      *" exec -T parkana "*)
        printf '%s\n' "${MOCK_PARKANA_ENV:-}"
        exit 0
        ;;
      *" logs "*)
        [ -n "${MOCK_LOGS:-}" ] && printf '%s\n' "$MOCK_LOGS"
        exit 0
        ;;
      *" restart "*)
        printf 'RESTART\n' >>"${MOCK_EVENT_LOG:-/dev/null}"
        exit "${MOCK_RESTART_EXIT:-0}"
        ;;
      *" down "*)
        printf 'DOWN %s\n' "$*" >>"${MOCK_EVENT_LOG:-/dev/null}"
        exit "${MOCK_DOWN_EXIT:-0}"
        ;;
    esac
    exit 0
    ;;
  inspect)
    case "$3" in
      *ExitCode*) echo "${MOCK_INIT_EXIT:-0}" ;;
      *Status*) echo "${MOCK_INIT_STATE:-exited}" ;;
      *) echo "" ;;
    esac
    exit 0
    ;;
  ps)
    case "${MOCK_LIST_FAIL:-0}" in 1|ports) exit 1 ;; esac
    exit 0
    ;;
  volume)
    case "${MOCK_LIST_FAIL:-0}" in 1|volume) exit 1 ;; esac
    exit 0
    ;;
  network)
    case "${MOCK_LIST_FAIL:-0}" in 1|network) exit 1 ;; esac
    exit 0
    ;;
esac
exit 0
MOCK

  cat >"$dir/bin/curl" <<'MOCK'
#!/bin/sh
url=""
out=""
prev=""
for a in "$@"; do
  case "$a" in
    http*) url=$a ;;
  esac
  if [ "$prev" = "-o" ]; then out=$a; fi
  prev=$a
done
[ "${MOCK_CURL_FAIL:-0}" = 1 ] && exit 1
case "$url" in
  */ready) exit 0 ;;
  */loki/api/v1/status/buildinfo) printf '{"version":"3.7.9"}'; exit 0 ;;
  */config)
    body='limits_config:
  allow_structured_metadata: true
  discover_log_levels: false
  discover_service_name:
  - service
'
    if [ -n "$out" ]; then printf '%s' "$body" >"$out"; else printf '%s' "$body"; fi
    exit 0
    ;;
  */v1/logs|*/otlp/v1/logs)
    case " $* " in
      *" -w "*)
        if [ -n "$out" ]; then printf '%s' "${MOCK_OTLP_BODY:-}" >"$out"; fi
        printf '%s' "${MOCK_OTLP_STATUS:-204}"
        ;;
    esac
    exit 0
    ;;
esac
exit 0
MOCK

  cat >"$dir/bin/go" <<'MOCK'
#!/bin/sh
printf 'GO %s\n' "$*" >>"${MOCK_GO_LOG:?MOCK_GO_LOG not set}"
[ "${MOCK_GO_FAIL:-0}" = 1 ] && exit 1
out=""
prev=""
for arg in "$@"; do
  if [ "$prev" = "-o" ]; then out=$arg; fi
  prev=$arg
done
[ -n "$out" ] || exit 2
cp "${MOCK_CLI_TEMPLATE:?MOCK_CLI_TEMPLATE not set}" "$out" || exit 2
chmod 0755 "$out"
exit 0
MOCK

  cat >"$dir/bin/timeout" <<'MOCK'
#!/bin/sh
dur=$1
shift
printf 'TIMEOUT %s %s\n' "$dur" "$*" >>"${MOCK_TIMEOUT_LOG:?MOCK_TIMEOUT_LOG not set}"
exec "$@"
MOCK

  cat >"$MOCK_CLI_TEMPLATE" <<'MOCK'
#!/bin/sh
sub=$1
shift
printf 'CALL %s\n' "$sub" >>"${MOCK_CLI_LOG:?MOCK_CLI_LOG not set}"
printf 'CALL %s\n' "$sub" >>"${MOCK_EVENT_LOG:-/dev/null}"
prev=""
for arg in "$@"; do
  printf 'ARG %s\n' "$arg" >>"$MOCK_CLI_LOG"
  if [ "$prev" = "-manifest" ] || [ "$prev" = "-report" ]; then
    printf '{}\n' >"$arg"
  fi
  prev=$arg
done
case "$sub" in
  send) exit "${MOCK_SEND_EXIT:-0}" ;;
  compare)
    if [ "${MOCK_COMPARE_FAIL_AFTER_RESTART:-0}" = 1 ] &&
      grep -q '^RESTART$' "${MOCK_EVENT_LOG:-/dev/null}"; then
      exit 1
    fi
    exit "${MOCK_COMPARE_EXIT:-0}"
    ;;
esac
exit 0
MOCK

  chmod 0755 "$dir/bin/docker" "$dir/bin/curl" "$dir/bin/go" "$dir/bin/timeout" "$MOCK_CLI_TEMPLATE"
}

# run_mocked runs the wrapper as a fresh subprocess with the mock tools.
run_mocked() { # workdir label expected-status args...
  local dir=$1 label=$2 want=$3
  shift 3
  : >"$dir/docker.log"
  : >"$dir/go.log"
  : >"$dir/cli.log"
  : >"$dir/events.log"
  : >"$dir/timeout.log"
  local args=("$@")
  if [[ " ${args[*]} " != *" --out "* ]]; then
    LAST_OUT="$dir/out-$RANDOM-$RANDOM"
    args+=(--out "$LAST_OUT")
  fi
  local envs=(
    "PATH=$dir/bin:$PATH"
    "DOCKER=$dir/bin/docker"
    "GO=$dir/bin/go"
    "MOCK_DOCKER_LOG=$dir/docker.log"
    "MOCK_GO_LOG=$dir/go.log"
    "MOCK_CLI_LOG=$dir/cli.log"
    "MOCK_EVENT_LOG=$dir/events.log"
    "MOCK_TIMEOUT_LOG=$dir/timeout.log"
    "PARKANA_OTLP_E2E_LOCK_ROOT=$dir/locks"
    "MOCK_CLI_TEMPLATE=$dir/cli-template"
    "MOCK_VALIDATE_F_PATHS=${MOCK_VALIDATE_F_PATHS:-0}"
    "MOCK_PARKANA_ENV=PARKANA_ROLES=ingester,querier
PARKANA_INGEST_FLUSH_INTERVAL=1s
PARKANA_INGEST_UPLOAD_INTERVAL=1s
PARKANA_INGEST_UPLOAD_MIN=1B
PARKANA_INGEST_UPLOAD_MAX_AGE=2s
PARKANA_INGEST_FLUSH_CONCURRENCY=2
PARKANA_INGEST_UPLOAD_CONCURRENCY=2
PARKANA_WAL_SYNC=always
PARKANA_WAL_SYNC_INTERVAL=1s"
  )
  local got=0
  set +e
  if [[ -n ${MOCK_CWD:-} ]]; then
    ( cd "$MOCK_CWD" && env "${envs[@]}" bash "$SCRIPT_PATH" "${args[@]}" ) >"$dir/out.log" 2>&1
  else
    env "${envs[@]}" bash "$SCRIPT_PATH" "${args[@]}" >"$dir/out.log" 2>&1
  fi
  got=$?
  set -e
  if [[ $got != "$want" ]]; then
    echo "selftest: $label exited $got, want $want" >&2
    cat "$dir/out.log" >&2
    return 1
  fi
  return 0
}

common_mock_args() { # workdir
  printf '%s\n' \
    --project mock-project \
    --parkana-port 25100 \
    --garage-port 25900 \
    --loki-port 25101 \
    --alloy-port 25245 \
    --alloy-otlp-http-port 25318 \
    --alloy-otlp-grpc-port 25317
}

test_full_success() {
  local dir=$1
  run_mocked "$dir" "full success" 0 --full $(common_mock_args "$dir") || return 1
  local sends compares
  sends=$(grep -c '^CALL send$' "$dir/cli.log" || true)
  compares=$(grep -c '^CALL compare$' "$dir/cli.log" || true)
  [[ $sends == 5 && $compares == 6 ]] || {
    echo "selftest: expected 5 sends and 6 compares, got $sends/$compares" >&2
    cat "$dir/cli.log" >&2
    return 1
  }
  grep -q '^RESTART$' "$dir/events.log" || return 1
  grep -q "down .*--volumes --remove-orphans" "$dir/events.log" || return 1
  [[ -d $LAST_OUT ]] || { echo "selftest: output/evidence was not preserved" >&2; return 1; }
  [[ $(find "$LAST_OUT" -maxdepth 1 -name 'manifest-*.json' | wc -l) -eq 5 ]] || return 1
  [[ $(find "$LAST_OUT" -maxdepth 1 -name 'report-*.json' | wc -l) -eq 6 ]] || return 1
  [[ ! -e $dir/locks/parkana-otlp-e2e-lock-mock-project ]] || return 1

  # Cold build/start use the long Compose timeout; probes keep the short one.
  grep -Eq '^TIMEOUT 900 .* build ' "$dir/timeout.log" || return 1
  grep -Eq '^TIMEOUT 900 .* up -d ' "$dir/timeout.log" || return 1
  grep -Eq '^TIMEOUT 30 .* config' "$dir/timeout.log" || return 1

  # Restart must happen after the three pre-restart compares and before the
  # three post-restart compares.
  awk '/^CALL compare$/ { n++ } /^RESTART$/ { r=n } END { exit !(r==3 && n==6) }' \
    "$dir/events.log" || {
    echo "selftest: restart did not sit between the two compare groups" >&2
    cat "$dir/events.log" >&2
    return 1
  }

  # Every direct case sends to both native paths with one shared anchor; the
  # tee case sends once to Alloy. Namespaces are isolated per case.
  awk '
    /^CALL / {
      if (call != "") print call, target, ns, rid, anchor
      call = $2; target = ""; ns = ""; rid = ""; anchor = ""
    }
    /^ARG -target$/ { getline; target = $0; sub(/^ARG /, "", target) }
    /^ARG -namespace$/ { getline; ns = $0; sub(/^ARG /, "", ns) }
    /^ARG -run-id$/ { getline; rid = $0; sub(/^ARG /, "", rid) }
    /^ARG -anchor$/ { getline; anchor = $0; sub(/^ARG /, "", anchor) }
    END { if (call != "") print call, target, ns, rid, anchor }
  ' "$dir/cli.log" | grep '^send ' >"$dir/sends.txt"
  [[ $(wc -l <"$dir/sends.txt") -eq 5 ]] || return 1
  [[ $(awk '{print $3}' "$dir/sends.txt" | sort -u | wc -l) -eq 3 ]] || return 1
  [[ $(grep -c 'http://127.0.0.1:25101/otlp/v1/logs' "$dir/sends.txt") -eq 2 ]] || return 1
  [[ $(grep -c 'http://127.0.0.1:25100/v1/logs' "$dir/sends.txt") -eq 2 ]] || return 1
  [[ $(grep -c 'http://127.0.0.1:25318/v1/logs' "$dir/sends.txt") -eq 1 ]] || return 1

  # Each case has one namespace/run-id pair; a direct case's two sends share
  # the same anchor. Three distinct anchors mean the script captured one per
  # case instead of per request.
  [[ $(awk '{print $3}' "$dir/sends.txt" | sort -u | wc -l) -eq 3 ]] || return 1
  [[ $(awk '{print $4}' "$dir/sends.txt" | sort -u | wc -l) -eq 3 ]] || return 1
  [[ $(awk '{print $3, $5}' "$dir/sends.txt" | sort -u | wc -l) -eq 3 ]] || return 1
  [[ $(awk '{print $5}' "$dir/sends.txt" | sort -u | wc -l) -eq 3 ]] || return 1

  # gzip is set on exactly the two gzip cases (3 requests).
  [[ $(grep -c '^ARG -gzip$' "$dir/cli.log" || true) -eq 3 ]] || return 1

  # The comparator was built exactly once before compose startup.
  [[ $(grep -c '^GO ' "$dir/go.log" || true) -eq 1 ]] || return 1
  grep -q ' build ' "$dir/go.log" || return 1
  return 0
}

test_compose_paths_from_unrelated_cwd() {
  local dir=$1 cwd
  cwd=$(mktemp -d "$dir/cwd.XXXXXX")
  MOCK_CWD="$cwd" MOCK_VALIDATE_F_PATHS=1 run_mocked "$dir" "unrelated cwd" 0 \
    --smoke $(common_mock_args "$dir") || return 1
  unset MOCK_CWD MOCK_VALIDATE_F_PATHS

  # Every compose invocation must carry the absolute checked-in paths. A
  # relative -f path makes the mock fail before the script can reach readiness
  # because the file does not exist in the unrelated caller directory.
  grep -Fq -- "-f $BASE_COMPOSE -f $OVERLAY_COMPOSE" "$dir/docker.log" || return 1
  grep -Fq -- "--project-directory $COMPOSE_DIR" "$dir/docker.log" || return 1
  grep -q '^F_PATHS_OK$' "$dir/docker.log" ||
    { echo "selftest: the -f path validator did not run" >&2; return 1; }
  if grep -Eq -- '(^| )-f (compose|otlp\.compose)\.yaml( |$)' "$dir/docker.log"; then
    echo "selftest: a relative compose -f path survived" >&2
    cat "$dir/docker.log" >&2
    return 1
  fi
  [[ -f $BASE_COMPOSE && -f $OVERLAY_COMPOSE ]] || return 1
  [[ -d $LAST_OUT ]] || return 1
  return 0
}

test_compare_exit_propagation() {
  local dir=$1 want
  for want in 1 2; do
    MOCK_COMPARE_EXIT=$want run_mocked "$dir" "compare exit $want" "$want" \
      --full $(common_mock_args "$dir") || return 1
    grep -q "down .*--volumes --remove-orphans" "$dir/events.log" || return 1
    [[ -d $LAST_OUT ]] || { echo "selftest: failure removed output evidence" >&2; return 1; }
    [[ ! -e $dir/locks/parkana-otlp-e2e-lock-mock-project ]] || return 1
  done
  return 0
}

test_send_failure_propagation() {
  local dir=$1
  MOCK_SEND_EXIT=2 run_mocked "$dir" "send failure" 2 --full $(common_mock_args "$dir") || return 1
  ! grep -q '^CALL compare$' "$dir/cli.log" || return 1
  grep -q "down .*--volumes --remove-orphans" "$dir/events.log" || return 1
  [[ -d $LAST_OUT ]] || return 1
  return 0
}

test_restart_failure_propagation() {
  local dir=$1
  MOCK_COMPARE_FAIL_AFTER_RESTART=1 run_mocked "$dir" "post-restart difference" 1 \
    --full $(common_mock_args "$dir") || return 1
  grep -q '^RESTART$' "$dir/events.log" || return 1
  grep -q "down .*--volumes --remove-orphans" "$dir/events.log" || return 1
  [[ -d $LAST_OUT ]] || return 1
  return 0
}

test_up_failure_cleanup() {
  local dir=$1
  MOCK_UP_FAIL=1 MOCK_LOGS="mock garage-init logs" run_mocked "$dir" "up failure" 1 \
    --smoke $(common_mock_args "$dir") || return 1
  grep -q "down .*--volumes --remove-orphans" "$dir/events.log" || return 1
  grep -q 'mock garage-init logs' "$dir/out.log" || return 1
  [[ -d $LAST_OUT ]] || return 1
  [[ ! -e $dir/locks/parkana-otlp-e2e-lock-mock-project ]] || return 1
  return 0
}

test_build_failure() {
  local dir=$1
  MOCK_GO_FAIL=1 run_mocked "$dir" "build failure" 1 --full $(common_mock_args "$dir") || return 1
  grep -q 'go build ./cmd/otlpcompare failed' "$dir/out.log" || return 1
  if grep -Eq ' build | up -d ' "$dir/docker.log"; then
    echo "selftest: build failure reached compose" >&2
    return 1
  fi
  [[ -d $LAST_OUT ]] || return 1
  [[ ! -e $dir/locks/parkana-otlp-e2e-lock-mock-project ]] || return 1
  return 0
}

test_existing_project_refused() {
  local dir=$1
  MOCK_PROJECT_EXISTS=1 run_mocked "$dir" "existing project" 1 --smoke $(common_mock_args "$dir") || return 1
  if grep -q ' down ' "$dir/docker.log"; then
    echo "selftest: pre-existing project was brought down" >&2
    return 1
  fi
  if grep -q ' logs ' "$dir/docker.log"; then
    echo "selftest: pre-existing project logs were read" >&2
    return 1
  fi
  [[ -d $LAST_OUT ]] || return 1
  [[ ! -e $dir/locks/parkana-otlp-e2e-lock-mock-project ]] || return 1
  return 0
}

test_project_lock() {
  local dir=$1 lock="$1/locks/parkana-otlp-e2e-lock-mock-project"
  mkdir -p "$lock"
  run_mocked "$dir" "held project lock" 1 --smoke $(common_mock_args "$dir") || return 1
  grep -q 'locked by another invocation' "$dir/out.log" || return 1
  ! grep -Eq ' up -d | down | logs ' "$dir/docker.log" || return 1
  [[ -d $LAST_OUT ]] || return 1
  [[ -d $lock ]] || return 1
  rm -rf "$lock"
  # Green: once the owner releases the lock, another invocation proceeds and
  # removes its own lock.
  run_mocked "$dir" "lock released" 0 --smoke $(common_mock_args "$dir") || return 1
  [[ ! -e $lock ]] || return 1
  return 0
}

test_list_failure() {
  local dir=$1 mode
  for mode in project volume network ports; do
    MOCK_LIST_FAIL=$mode run_mocked "$dir" "listing failure $mode" 1 --smoke \
      $(common_mock_args "$dir") || return 1
    grep -q 'could not list' "$dir/out.log" || return 1
    ! grep -Eq ' up -d | down | logs ' "$dir/docker.log" || return 1
    [[ -d $LAST_OUT ]] || return 1
    [[ ! -e $dir/locks/parkana-otlp-e2e-lock-mock-project ]] || return 1
  done
  return 0
}

test_readiness_timeout() {
  local dir=$1
  MOCK_CURL_FAIL=1 run_mocked "$dir" "readiness timeout" 1 --smoke \
    --ready-timeout 1 $(common_mock_args "$dir") || return 1
  grep -q 'not ready' "$dir/out.log" || return 1
  grep -q "down .*--volumes --remove-orphans" "$dir/events.log" || return 1
  [[ -d $LAST_OUT ]] || return 1
  return 0
}

test_otlp_route_readiness() {
  local dir=$1
  # Process readiness succeeds but the native OTLP route answers 503. The
  # script must fail on the route probe, not wait forever or send fixtures.
  MOCK_OTLP_STATUS=503 run_mocked "$dir" "OTLP route not ready" 1 --smoke \
    --ready-timeout 1 $(common_mock_args "$dir") || return 1
  grep -q 'OTLP probe' "$dir/out.log" || return 1
  ! grep -q '^CALL ' "$dir/cli.log" || return 1
  grep -q "down .*--volumes --remove-orphans" "$dir/events.log" || return 1
  [[ -d $LAST_OUT ]] || return 1
  return 0
}

test_cleanup_failure() {
  local dir=$1
  MOCK_DOWN_EXIT=1 run_mocked "$dir" "cleanup failure" 1 --smoke $(common_mock_args "$dir") || return 1
  grep -q 'cleanup failed' "$dir/out.log" || return 1
  [[ -d $LAST_OUT ]] || return 1
  [[ ! -e $dir/locks/parkana-otlp-e2e-lock-mock-project ]] || return 1
  return 0
}

test_init_failure() {
  local dir=$1
  MOCK_INIT_EXIT=7 run_mocked "$dir" "garage-init failure" 1 --smoke $(common_mock_args "$dir") || return 1
  grep -q 'garage-init exited 7' "$dir/out.log" || return 1
  grep -q "down .*--volumes --remove-orphans" "$dir/events.log" || return 1
  [[ -d $LAST_OUT ]] || return 1
  return 0
}

test_keep_compose_only() {
  local dir=$1 out=$1/run-keep
  run_mocked "$dir" "keep" 0 --smoke --keep \
    --project mock-project --out "$out" \
    --parkana-port 25100 --garage-port 25900 --loki-port 25101 \
    --alloy-port 25245 --alloy-otlp-http-port 25318 --alloy-otlp-grpc-port 25317 || return 1
  ! grep -q '^DOWN ' "$dir/events.log" || return 1
  [[ -d $out ]] || return 1
  [[ ! -e $dir/locks/parkana-otlp-e2e-lock-mock-project ]] || return 1
  return 0
}

test_secret_redaction() {
  local dir=$1 secret='PARKANA_SELFTEST_SECRET_0123456789abcdef'
  PARKANA_DEV_GARAGE_KEY_ID='GKSELFTESTKEY000000000000' \
    PARKANA_DEV_GARAGE_KEY_SECRET="$secret" \
    MOCK_UP_FAIL=1 MOCK_LOGS="leak $secret" \
    run_mocked "$dir" "secret redaction" 1 --smoke $(common_mock_args "$dir") || return 1
  if grep -Fq -- "$secret" "$dir/out.log"; then
    echo "selftest: diagnostics leaked the Garage secret" >&2
    return 1
  fi
  return 0
}

self_test() {
  local failed=0 dir
  printf '[e2e-p4-otlp] self-test: offline logic and mocked subprocesses\n'
  dir=$(mktemp -d "${TMPDIR:-/tmp}/parkana-otlp-selftest.XXXXXX")
  write_mocks "$dir"

  self_case "defaults and port set" test_defaults || failed=1
  self_case "argument validation rejects unsupported/bad values" test_validation_rejects || failed=1
  self_case "duplicate ports, ready bound and WAL mode rejected" test_duplicate_ports_rejected || failed=1
  self_case "static Alloy/Loki config checks (good and red cases)" test_config_checks "$dir" || failed=1
  self_case "large effective Loki config passes under pipefail" test_loki_effective_large_fixture "$dir" || failed=1
  self_case "OTLP probe: 2xx, exact Loki 422 body, wrong 422/404 rejected" test_otlp_probe_statuses "$dir" || failed=1
  self_case "full success: 5 sends, 6 compares, restart, evidence kept" test_full_success "$dir" || failed=1
  self_case "absolute compose -f paths work from an unrelated cwd" test_compose_paths_from_unrelated_cwd "$dir" || failed=1
  self_case "compare exit 1/2 propagation keeps evidence and cleans" test_compare_exit_propagation "$dir" || failed=1
  self_case "send failure propagates 2 before compare" test_send_failure_propagation "$dir" || failed=1
  self_case "post-restart difference propagates 1" test_restart_failure_propagation "$dir" || failed=1
  self_case "up failure is diagnosed and cleaned" test_up_failure_cleanup "$dir" || failed=1
  self_case "build failure happens before compose" test_build_failure "$dir" || failed=1
  self_case "existing project is refused untouched" test_existing_project_refused "$dir" || failed=1
  self_case "held project lock blocks a second invocation" test_project_lock "$dir" || failed=1
  self_case "container/volume/network listing failure fails closed" test_list_failure "$dir" || failed=1
  self_case "bounded readiness timeout cleans up" test_readiness_timeout "$dir" || failed=1
  self_case "native OTLP route probe fails before send" test_otlp_route_readiness "$dir" || failed=1
  self_case "cleanup failure is reported" test_cleanup_failure "$dir" || failed=1
  self_case "garage-init non-zero exit propagates" test_init_failure "$dir" || failed=1
  self_case "--keep retains only the compose project" test_keep_compose_only "$dir" || failed=1
  self_case "diagnostics never print the Garage secret" test_secret_redaction "$dir" || failed=1

  if ((failed)); then
    printf '[e2e-p4-otlp] self-test: FAILED (workdir %s)\n' "$dir" >&2
    return 1
  fi
  rm -rf "$dir"
  printf '[e2e-p4-otlp] self-test: all checks passed\n'
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
  validate_args
  set_urls
  if ((DRY_RUN)); then
    dry_run
    return
  fi

  local cmd
  for cmd in "$DOCKER" curl timeout date grep awk sed od tr mktemp rm mkdir chmod; do
    command -v -- "$cmd" >/dev/null 2>&1 || usage_fail "missing required command: $cmd"
  done
  [[ $MODE == smoke ]] || command -v -- "$GO" >/dev/null 2>&1 || usage_fail "missing required command: $GO"

  if [[ -z $PROJECT ]]; then
    PROJECT="parkana-e2e-p4-otlp-$(date -u +%y%m%d%H%M%S)-$$"
  fi
  create_out_dir
  trap cleanup EXIT INT TERM
  acquire_project_lock

  generate_credentials
  resolve_ports
  export_settings

  say "static checks"
  compose_quick config >/dev/null || fail "compose config failed"
  check_static_files
  echo "base pin: $ALLOY_PIN"
  echo "overlay: $OVERLAY_COMPOSE (no image override)"
  echo "Alloy: native OTLP receiver -> Loki /otlp/v1/logs + Parkana /v1/logs (no loki.write)"
  echo "Loki: default service-name discovery, log-level discovery off, otlp_config defaults"

  if [[ $MODE == full ]]; then
    build_comparator
  fi

  ensure_project_available
  check_published_ports

  log "mode=$MODE project=$PROJECT"
  log "output=$OUT_DIR"
  log "ports: parkana=$PARKANA_PORT garage=$S3_PORT loki=$LOKI_PORT alloy=$ALLOY_PORT alloy-otlp-http=$ALLOY_OTLP_HTTP_PORT alloy-otlp-grpc=$ALLOY_OTLP_GRPC_PORT"
  log "Garage credentials: generated for this run (values hidden)"

  say "build and start"
  created=1
  compose_long build garage parkana || fail "compose build failed"
  compose_long up -d garage garage-init parkana loki alloy || fail "compose up failed"
  wait_initialized garage-init 0 300 || fail "garage-init did not exit 0"

  say "readiness"
  wait_http "$PARKANA_URL/ready" "Parkana /ready" "$READY_TIMEOUT" || fail "Parkana not ready"
  wait_http "$LOKI_URL/ready" "Loki /ready" "$READY_TIMEOUT" || fail "Loki not ready"
  wait_http "$ALLOY_URL/-/ready" "Alloy /-/ready" "$READY_TIMEOUT" || fail "Alloy not ready"
  wait_version "$LOKI_URL/loki/api/v1/status/buildinfo" 3.7.9 "$READY_TIMEOUT" "Loki" ||
    fail "Loki version mismatch"
  wait_otlp_endpoint "$ALLOY_OTLP_LOGS_URL" "Alloy" "$READY_TIMEOUT" || fail "Alloy OTLP/HTTP receiver not ready"
  wait_otlp_endpoint "$LOKI_OTLP_URL" "Loki" "$READY_TIMEOUT" 1 || fail "Loki OTLP/HTTP endpoint not ready"
  wait_otlp_endpoint "$PARKANA_OTLP_URL" "Parkana" "$READY_TIMEOUT" || fail "Parkana OTLP/HTTP endpoint not ready"
  echo "Parkana /ready, Loki 3.7.9 /ready, Alloy /-/ready and all three OTLP endpoints: OK"

  say "effective settings"
  check_parkana_env
  check_effective_loki
  if [[ $WAL_SYNC == interval ]]; then
    warn "WAL sync is interval: acks precede fsync; receipts remain durable"
  fi

  if [[ $MODE == smoke ]]; then
    say "PASS (smoke)"
    echo "smoke covers stack, native OTLP routes and effective settings; --full adds transports, compares and restart"
    return
  fi

  run_cases
  restart_write_paths
  recompare_after_restart

  say "PASS (full)"
  echo "all transport cases EQUAL before and after restart; reports in $OUT_DIR"
}

main "$@"
