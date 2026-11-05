#!/usr/bin/env bash
#
# scripts/e2e-phase4-sql.sh - end-to-end check for the phase 4 SQL datasource.
#
# One isolated compose project (base phase 2 stack + deploy/compose/sql.yaml),
# one deterministic three-line fixture pushed straight into Parkana, and
# assertions through Grafana's /api/ds/query:
#
#   * the plugin is built from grafana/parkana-datasource (frontend in the
#     pinned Node container with typecheck/tests, backend via the plugin
#     Makefile) and loaded unsigned;
#   * the provisioned datasource uid=parkana-sql answers CheckHealth, so the
#     plugin process talks to POST /api/v1/sql;
#   * a table query returns the exact fixture values;
#   * a time_series query buckets by the $__interval macro and aggregates with
#     sum(), returning the exact epoch-millisecond timestamps and numeric points;
#   * $__timeFrom/$__timeTo expand and still select all three rows;
#   * a filesystem escape (read_csv_auto('/etc/passwd')) is rejected through the
#     plugin and leaks no file content;
#   * the default tenant header is accepted by Parkana and another tenant ID is
#     rejected while multi-tenancy is disabled (no unsupported flag is enabled).
#
# The script owns the only cleanup: it starts and removes its own compose
# project, volumes and network, and never touches another project. Build and
# Docker work belongs on a Boat VM:
#
#   tm-boat test <worktree> -- scripts/e2e-phase4-sql.sh
#
# --self-test runs shell-only checks plus the embedded JSON assertion helper
# (when the host Go is at least the pinned go.mod version; no Docker);
# --dry-run prints the resolved plan without touching Docker.
#
# Usage: scripts/e2e-phase4-sql.sh [options]
#   --project NAME        compose project (default: generated)
#   --parkana-port PORT   Parkana HTTP host port (default: random unused)
#   --garage-port PORT    Garage S3 host port (default: random unused)
#   --loki-port PORT      Loki HTTP host port (default: random unused)
#   --grafana-port PORT   Grafana HTTP host port (default: random unused)
#   --timeout SECONDS     Grafana first-start/readiness budget (default 600)
#   --keep                leave the stack running for debugging
#   --dry-run             print the resolved plan, touch nothing
#   --self-test           offline checks, touch nothing (no Docker)
#   -h, --help            show this help
#
set -euo pipefail

readonly SCRIPT_PATH="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/$(basename -- "${BASH_SOURCE[0]}")"
readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
readonly COMPOSE_DIR="$REPO_ROOT/deploy/compose"
readonly BASE_COMPOSE="$COMPOSE_DIR/compose.yaml"
readonly SQL_COMPOSE="$COMPOSE_DIR/sql.yaml"
readonly PROVISIONING="$COMPOSE_DIR/grafana-sql/provisioning/datasources/sql.yaml"
readonly PLUGIN_DIR="$REPO_ROOT/grafana/parkana-datasource"
readonly PLUGIN_DIST="$PLUGIN_DIR/dist"
readonly PLUGIN_ID=parkana-sql-datasource
readonly PLUGIN_BINARY=gpx_parkana_sql_datasource_linux_amd64
readonly DATASOURCE_UID=parkana-sql
readonly MAX_PROJECT_LEN=63
readonly MAX_READY_TIMEOUT=86400
# Same pinned builder image as the repository Dockerfile; only used when the
# host has no Go toolchain.
readonly GO_IMAGE='golang:1.27.3-bookworm@sha256:d9b6a1beb9474adc2a1ce842573639b44b2a0dd49b6669f51d3821baf9ec37c9'

DOCKER="${DOCKER:-docker}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT INT TERM

# Small helpers -----------------------------------------------------------------

log() { printf '[e2e-p4-sql] %s\n' "$*"; }
warn() { printf '[e2e-p4-sql] WARN: %s\n' "$*" >&2; }
say() { printf '\n=== %s ===\n' "$*"; }
now() { date +%s; }
fail() {
  printf '[e2e-p4-sql] FAIL: %s\n' "$*" >&2
  exit 1
}
usage_fail() {
  printf '[e2e-p4-sql] ERROR: %s\n' "$*" >&2
  exit 2
}
usage() {
  sed -n '/^# Usage:/,/^#   -h, --help/p' "$0" | sed 's/^# \{0,1\}//'
}

init_defaults() {
  PROJECT="${PARKANA_E2E_SQL_PROJECT:-}"
  PROJECT_EXPLICIT=0
  [[ -n $PROJECT ]] && PROJECT_EXPLICIT=1

  HTTP_PORT=""
  S3_PORT=""
  LOKI_PORT=""
  GRAFANA_PORT=""
  HTTP_EXPLICIT=0
  S3_EXPLICIT=0
  LOKI_EXPLICIT=0
  GRAFANA_EXPLICIT=0
  if [[ -n ${PARKANA_HTTP_PORT:-} ]]; then HTTP_PORT=$PARKANA_HTTP_PORT HTTP_EXPLICIT=1; fi
  if [[ -n ${PARKANA_S3_PORT:-} ]]; then S3_PORT=$PARKANA_S3_PORT S3_EXPLICIT=1; fi
  if [[ -n ${LOKI_HTTP_PORT:-} ]]; then LOKI_PORT=$LOKI_HTTP_PORT LOKI_EXPLICIT=1; fi
  if [[ -n ${GRAFANA_HTTP_PORT:-} ]]; then GRAFANA_PORT=$GRAFANA_HTTP_PORT GRAFANA_EXPLICIT=1; fi

  READY_TIMEOUT="${PARKANA_E2E_SQL_TIMEOUT:-600}"
  KEEP=0
  DRY_RUN=0
  SELFTEST=0
  GARAGE_KEY_ID=""
  GARAGE_KEY_SECRET=""
  CREATED=0
  FROM_MS=""
  TO_MS=""
  T0_NS=""
}

parse_args() {
  while (($# > 0)); do
    case "$1" in
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
      --grafana-port)
        (($# >= 2)) || usage_fail "--grafana-port needs a value"
        GRAFANA_PORT=$2
        GRAFANA_EXPLICIT=1
        shift 2
        ;;
      --timeout)
        (($# >= 2)) || usage_fail "--timeout needs a value"
        READY_TIMEOUT=$2
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
valid_timeout() { [[ $1 =~ ^[1-9][0-9]{0,4}$ ]] && (($1 >= 1 && $1 <= MAX_READY_TIMEOUT)); }

validate_args() {
  [[ -n $READY_TIMEOUT ]] || usage_fail "--timeout must not be empty"
  valid_timeout "$READY_TIMEOUT" ||
    usage_fail "--timeout must be a positive integer <= $MAX_READY_TIMEOUT seconds, got '$READY_TIMEOUT'"

  local port name value explicit
  for name in HTTP S3 LOKI GRAFANA; do
    case "$name" in
      HTTP) port=$HTTP_PORT; explicit=$HTTP_EXPLICIT ;;
      S3) port=$S3_PORT; explicit=$S3_EXPLICIT ;;
      LOKI) port=$LOKI_PORT; explicit=$LOKI_EXPLICIT ;;
      GRAFANA) port=$GRAFANA_PORT; explicit=$GRAFANA_EXPLICIT ;;
    esac
    if [[ -n $port ]]; then
      valid_port "$port" || usage_fail "$name host port must be an integer in 1..65535, got '$port'"
      ((explicit)) || usage_fail "internal error: $name port is set but not explicit"
    fi
  done

  if [[ -z $PROJECT ]]; then
    PROJECT="parkana-e2e-p4sql-$(date -u +%y%m%d%H%M%S)-$$"
  fi
  valid_project_name "$PROJECT" ||
    usage_fail "invalid compose project name '$PROJECT' (want [a-z0-9][a-z0-9_-]{0,62})"
}

validate_resolved_ports() {
  local seen=() p
  for p in "$HTTP_PORT" "$S3_PORT" "$LOKI_PORT" "$GRAFANA_PORT"; do
    valid_port "$p" || usage_fail "resolved host port '$p' is invalid"
    if [[ " ${seen[*]:-} " == *" $p "* ]]; then
      usage_fail "host port $p is used twice; all four host ports must differ"
    fi
    seen+=("$p")
  done
}

# Credentials and ports ---------------------------------------------------------

random_hex() { od -An -N "$1" -tx1 /dev/urandom | tr -d ' \n'; }

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
  [[ -n $HTTP_PORT && $port == "$HTTP_PORT" ]] ||
    [[ -n $S3_PORT && $port == "$S3_PORT" ]] ||
    [[ -n $LOKI_PORT && $port == "$LOKI_PORT" ]] ||
    [[ -n $GRAFANA_PORT && $port == "$GRAFANA_PORT" ]]
}

choose_random_port() { # label
  local label=$1 candidate i
  for ((i = 0; i < 200; i++)); do
    candidate=$((20000 + RANDOM % 20000))
    port_used_by_this_run "$candidate" && continue
    if port_free "$candidate"; then
      printf '%s' "$candidate"
      return 0
    fi
  done
  fail "could not find a free $label port for this run"
}

resolve_one_port() { # preferred explicit label envname
  local preferred=$1 explicit=$2 label=$3 envname=$4
  if ((explicit)); then
    if ! port_free "$preferred"; then
      fail "$label port $preferred is already in use; pass another value or set $envname"
    fi
    printf '%s' "$preferred"
    return 0
  fi
  choose_random_port "$label"
}

resolve_ports() {
  HTTP_PORT=$(resolve_one_port "$HTTP_PORT" "$HTTP_EXPLICIT" "Parkana HTTP" PARKANA_HTTP_PORT) || exit 2
  S3_PORT=$(resolve_one_port "$S3_PORT" "$S3_EXPLICIT" "Garage S3" PARKANA_S3_PORT) || exit 2
  LOKI_PORT=$(resolve_one_port "$LOKI_PORT" "$LOKI_EXPLICIT" "Loki" LOKI_HTTP_PORT) || exit 2
  GRAFANA_PORT=$(resolve_one_port "$GRAFANA_PORT" "$GRAFANA_EXPLICIT" "Grafana" GRAFANA_HTTP_PORT) || exit 2
  validate_resolved_ports
}

export_ports() {
  export PARKANA_HTTP_PORT=$HTTP_PORT
  export PARKANA_S3_PORT=$S3_PORT
  export LOKI_HTTP_PORT=$LOKI_PORT
  export GRAFANA_HTTP_PORT=$GRAFANA_PORT
}

# The base compose file already wires these PARKANA_* names; shorten only the
# flush/upload window so the tiny fixture becomes SQL-visible quickly and keep
# the compactor out of the assertion window.
export_ingest_defaults() {
  export PARKANA_INGEST_FLUSH_INTERVAL="${PARKANA_INGEST_FLUSH_INTERVAL:-1s}"
  export PARKANA_INGEST_UPLOAD_INTERVAL="${PARKANA_INGEST_UPLOAD_INTERVAL:-1s}"
  export PARKANA_INGEST_UPLOAD_MAX_AGE="${PARKANA_INGEST_UPLOAD_MAX_AGE:-2s}"
  export PARKANA_COMPACTOR_INTERVAL="${PARKANA_COMPACTOR_INTERVAL:-1h}"
}

# Compose / stack ---------------------------------------------------------------

compose() {
  "$DOCKER" compose -p "$PROJECT" -f "$BASE_COMPOSE" -f "$SQL_COMPOSE" --profile phase2 "$@"
}

project_exists() {
  if [[ -n $("$DOCKER" compose -p "$PROJECT" -f "$BASE_COMPOSE" -f "$SQL_COMPOSE" --profile phase2 ps -a -q 2>/dev/null || true) ]]; then
    return 0
  fi
  if [[ -n $("$DOCKER" volume ls -q --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null || true) ]]; then
    return 0
  fi
  if [[ -n $("$DOCKER" network ls -q --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null || true) ]]; then
    return 0
  fi
  return 1
}

ensure_project_available() {
  ! project_exists
}

port_published() {
  "$DOCKER" ps --format '{{.Ports}}' 2>/dev/null | grep -q ":$1->"
}

sanitize() {
  local patterns=()
  [[ -n ${GARAGE_KEY_SECRET:-} ]] && patterns+=(-e "$GARAGE_KEY_SECRET")
  [[ -n ${GARAGE_KEY_ID:-} ]] && patterns+=(-e "$GARAGE_KEY_ID")
  if ((${#patterns[@]} > 0)); then
    grep -Fv "${patterns[@]}" || true
  else
    cat
  fi
}

diagnostics() {
  warn "diagnostics follow (bounded tails, credentials filtered)"
  compose ps 2>&1 | sanitize >&2 || true
  local svc
  for svc in garage garage-init parkana loki grafana; do
    printf -- '--- %s (last 40 lines) ---\n' "$svc" >&2
    compose logs --no-color --tail=40 "$svc" 2>&1 | sanitize >&2 || true
  done
}

cleanup_project() {
  if ((CREATED == 1)) && ((KEEP == 0)); then
    if ! compose down --volumes --remove-orphans >/dev/null 2>&1; then
      warn "cleanup failed: docker compose down did not remove project '$PROJECT'"
      return 1
    fi
  fi
  return 0
}

cleanup() {
  local status=$? cleanup_status=0
  trap - EXIT INT TERM
  if ((status != 0)) && ((CREATED == 1)); then
    diagnostics || true
  fi
  if ((CREATED == 1)) && ((KEEP == 1)); then
    warn "keeping project '$PROJECT' on Grafana host port ${GRAFANA_PORT:-?} (${GRAFANA_URL:-}); teardown: $DOCKER compose -p $PROJECT -f $BASE_COMPOSE -f $SQL_COMPOSE --profile phase2 down --volumes --remove-orphans"
  fi
  cleanup_project || cleanup_status=$?
  rm -rf "$TMP"
  if ((status != 0)); then
    exit "$status"
  fi
  exit "$cleanup_status"
}

wait_http() { # url name timeout
  local url=$1 name=$2 timeout=${3:-180} start deadline t
  start=$(now)
  deadline=$((start + timeout))
  while :; do
    if curl -fsS --connect-timeout 2 --max-time 5 "$url" >/dev/null 2>&1; then
      return 0
    fi
    t=$(now)
    if ((t >= deadline)); then
      printf '[e2e-p4-sql] %s not ready after %ss: %s\n' "$name" "$((t - start))" "$url" >&2
      return 1
    fi
    sleep 1
  done
}

wait_service_exit() { # service want timeout
  local svc=$1 want=$2 timeout=${3:-300} start deadline t id state code
  start=$(now)
  deadline=$((start + timeout))
  while :; do
    id=$(compose ps -a -q "$svc" 2>/dev/null || true)
    if [[ -n $id ]]; then
      state=$("$DOCKER" inspect -f '{{.State.Status}}' "$id" 2>/dev/null || true)
      if [[ $state == exited ]]; then
        code=$("$DOCKER" inspect -f '{{.State.ExitCode}}' "$id" 2>/dev/null || true)
        if [[ $code == "$want" ]]; then
          return 0
        fi
        printf '[e2e-p4-sql] %s exited %s, want %s\n' "$svc" "$code" "$want" >&2
        return 1
      fi
    fi
    t=$(now)
    if ((t >= deadline)); then
      printf '[e2e-p4-sql] %s did not exit within %ss\n' "$svc" "$((t - start))" >&2
      return 1
    fi
    sleep 1
  done
}

wait_datasource() { # uid timeout
  wait_http "$GRAFANA_URL/api/datasources/uid/$1" "Grafana datasource $1" "${2:-180}"
}

wait_health() { # uid timeout
  local uid=$1 timeout=${2:-180} start deadline t body
  start=$(now)
  deadline=$((start + timeout))
  body=""
  while :; do
    body=$(curl -fsS --connect-timeout 2 --max-time 10 \
      "$GRAFANA_URL/api/datasources/uid/$uid/health" 2>/dev/null || true)
    if printf '%s' "$body" | grep -Eq '"status"[[:space:]]*:[[:space:]]*"OK"'; then
      return 0
    fi
    t=$(now)
    if ((t >= deadline)); then
      printf '[e2e-p4-sql] %s health not OK after %ss: %s\n' \
        "$uid" "$((t - start))" "$(printf '%s' "$body" | head -c 300 | sanitize)" >&2
      return 1
    fi
    sleep 2
  done
}

plugin_version() {
  sed -n 's/^[[:space:]]*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
    "$PLUGIN_DIR/package.json" | head -n 1
}

# Plugin build ------------------------------------------------------------------

# The helper is built in a temporary directory with no go.mod, so a bare host
# `go` does not honor the version pinned by the repository: a too-old toolchain
# misses std packages such as encoding/json/v2. Use the host only when it is at
# least the pinned root go.mod version; otherwise fall back to the pinned Go
# container, the same image the Dockerfile builds with.
pinned_go_version() {
  sed -n 's/^go \([0-9][0-9.]*\)$/\1/p' "$REPO_ROOT/go.mod" | head -n 1
}

go_probe_version() { # go-binary [workdir]
  local go_bin=$1 dir=${2:-.} out
  [[ -n $go_bin ]] || return 1
  # Probe in the directory the helper is actually built in. A bare `go version`
  # from the repo root honors GOTOOLCHAIN=auto and reports the pinned toolchain
  # from go.mod, while the helper's temporary dir has no go.mod and falls back
  # to the base host Go.
  out=$(cd "$dir" && "$go_bin" version 2>/dev/null) || return 1
  out=${out#go version }
  out=${out%% *}
  out=${out#go}
  out=${out%%-*}
  out=${out%%+*}
  [[ $out =~ ^[0-9]+(\.[0-9]+)+$ ]] || return 1
  printf '%s' "$out"
}

version_ge() { # have want
  local IFS=. have=$1 want=$2 i
  local -a have_parts want_parts
  read -r -a have_parts <<<"$have"
  read -r -a want_parts <<<"$want"
  for ((i = 0; i < ${#want_parts[@]}; i++)); do
    ((10#${have_parts[i]:-0} > 10#${want_parts[i]:-0})) && return 0
    ((10#${have_parts[i]:-0} < 10#${want_parts[i]:-0})) && return 1
  done
  return 0
}

helper_toolchain() { # go-binary [probe-dir]; prints host|container
  local go_bin=$1 probe_dir=${2:-.} pinned version
  pinned=$(pinned_go_version) || return 1
  [[ -n $pinned ]] || return 1
  if [[ -n $go_bin ]] && version=$(go_probe_version "$go_bin" "$probe_dir"); then
    if version_ge "$version" "$pinned"; then
      printf 'host'
      return 0
    fi
  fi
  printf 'container'
}

# True when the offline self-test can build and run the helper itself: a host
# Go at least as new as the pinned root version. An older host uses the pinned
# container on a real run, which the offline self-test must not pull.
helper_build_available() {
  local go_bin pinned version
  command -v go >/dev/null 2>&1 || return 1
  go_bin=$(command -v go)
  pinned=$(pinned_go_version) || return 1
  [[ -n $pinned ]] || return 1
  version=$(go_probe_version "$go_bin" "$TMP") || return 1
  version_ge "$version" "$pinned"
}

build_checker() {
  write_checker
  local go_bin="" pinned toolchain
  if command -v go >/dev/null 2>&1; then
    go_bin=$(command -v go)
  fi
  pinned=$(pinned_go_version) || fail "could not read the pinned Go version from $REPO_ROOT/go.mod"
  [[ -n $pinned ]] || fail "could not read the pinned Go version from $REPO_ROOT/go.mod"
  toolchain=$(helper_toolchain "$go_bin" "$TMP") ||
    fail "could not choose a Go toolchain for the offline JSON assertion helper"
  if [[ $toolchain == host ]]; then
    log "building the offline JSON assertion helper with host Go $(go_probe_version "$go_bin" "$TMP") (pinned $pinned)"
    (cd "$TMP" && go build -o "$TMP/sqlcheck" sqlcheck.go) ||
      fail "could not build the offline JSON assertion helper"
  else
    log "host Go does not satisfy the pinned $pinned; building the offline JSON assertion helper in $GO_IMAGE"
    "$DOCKER" run --rm --user "$(id -u):$(id -g)" --env HOME=/tmp \
      --volume "$TMP:/src" --workdir /src "$GO_IMAGE" \
      sh -c 'GOCACHE=/tmp/gocache go build -o /src/sqlcheck /src/sqlcheck.go' ||
      fail "could not build the offline JSON assertion helper"
  fi
}

checker() { "$TMP/sqlcheck" "$@"; }

build_plugin() {
  [[ -f $PLUGIN_DIR/src/plugin.json ]] ||
    fail "plugin source not found at $PLUGIN_DIR (this worktree does not include grafana/parkana-datasource)"
  [[ -x $PLUGIN_DIR/scripts/build-frontend.sh ]] ||
    fail "plugin frontend build script is missing or not executable"
  [[ -f $PLUGIN_DIR/Makefile ]] || fail "plugin Makefile is missing"

  log "building plugin backend (Makefile)"
  if command -v go >/dev/null 2>&1; then
    make -C "$PLUGIN_DIR" backend ||
      fail "plugin backend build failed"
  else
    log "host go not found; building the plugin backend in $GO_IMAGE"
    "$DOCKER" run --rm --user "$(id -u):$(id -g)" --env HOME=/tmp \
      --volume "$REPO_ROOT:/work" --workdir /work/grafana/parkana-datasource \
      "$GO_IMAGE" make backend ||
      fail "plugin backend build failed"
  fi

  log "building plugin frontend (pinned Node container; typecheck and unit tests)"
  "$PLUGIN_DIR/scripts/build-frontend.sh" --check ||
    fail "plugin frontend build failed"

  local binary=$PLUGIN_DIST/$PLUGIN_BINARY
  [[ -f $PLUGIN_DIST/plugin.json ]] || fail "frontend build did not produce dist/plugin.json"
  [[ -f $PLUGIN_DIST/module.js ]] || fail "frontend build did not produce dist/module.js"
  [[ -x $binary ]] || fail "backend build did not produce an executable $binary"
  grep -Eq '"id"[[:space:]]*:[[:space:]]*"parkana-sql-datasource"' "$PLUGIN_DIST/plugin.json" ||
    fail "dist/plugin.json does not declare $PLUGIN_ID"
  grep -Eq '"executable"[[:space:]]*:[[:space:]]*"gpx_parkana_sql_datasource"' "$PLUGIN_DIST/plugin.json" ||
    fail "dist/plugin.json does not name gpx_parkana_sql_datasource"
  log "plugin dist ready: $PLUGIN_DIST ($PLUGIN_BINARY)"
}

# Embedded JSON helper ----------------------------------------------------------
#
# The assertion helper is built once from this heredoc. It stays out of the
# repository module on purpose: the e2e script is the only consumer, and every
# assertion is a typed JSON check instead of brittle sed/grep on Grafana's
# response encoding. Modes: request, sqlrequest, table, count, error,
# datasource, plugin, sqlcount.
write_checker() {
  cat >"$TMP/sqlcheck.go" <<'GO'
package main

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"math/big"
	"os"
	"strconv"
	"strings"
)

type frameField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type frame struct {
	Schema struct {
		Meta struct {
			Type string `json:"type"`
		} `json:"meta"`
		Fields []frameField `json:"fields"`
	} `json:"schema"`
	Data struct {
		Values [][]jsontext.Value `json:"values"`
	} `json:"data"`
}

type queryResult struct {
	Status int     `json:"status"`
	Error  string  `json:"error"`
	Frames []frame `json:"frames"`
}

type dsResponse struct {
	Results map[string]queryResult `json:"results"`
	Message string                 `json:"message"`
}

type endpointColumn struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	DatabaseType string `json:"databaseType"`
}

type endpointResponse struct {
	Columns   []endpointColumn   `json:"columns"`
	Rows      [][]jsontext.Value `json:"rows"`
	Truncated bool               `json:"truncated"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "request":
		err = cmdRequest(os.Args[2:])
	case "sqlrequest":
		err = cmdSQLRequest(os.Args[2:])
	case "table":
		err = cmdTable(os.Args[2:])
	case "count":
		err = cmdCount(os.Args[2:])
	case "error":
		err = cmdError(os.Args[2:])
	case "datasource":
		err = cmdDatasource(os.Args[2:])
	case "plugin":
		err = cmdPlugin(os.Args[2:])
	case "sqlcount":
		err = cmdSQLCount(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  sqlcheck request <table|time_series> <from-ms> <to-ms>   (SQL on stdin)
  sqlcheck sqlrequest <from-rfc3339> <to-rfc3339>          (SQL on stdin)
  sqlcheck table <meta-type> <name,type,cell...>...        (Grafana response on stdin)
  sqlcheck count <want>                                    (Grafana response on stdin)
  sqlcheck error <expected-status> <required> <forbidden>    (Grafana response on stdin)
  sqlcheck datasource <uid> <type> <tenant>                (datasource JSON on stdin)
  sqlcheck plugin <id> <version>                           (plugin settings JSON on stdin)
  sqlcheck sqlcount <want>                                 (/api/v1/sql response on stdin)
cells: s:text, n:number, b:true|false, z:null`)
	os.Exit(2)
}

func readStdin() ([]byte, error) {
	return io.ReadAll(os.Stdin)
}

func writeJSON(v any) error {
	return json.MarshalWrite(os.Stdout, v)
}

func cmdRequest(args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("request wants <format> <from-ms> <to-ms>")
	}
	sql, err := readStdin()
	if err != nil {
		return err
	}
	body := map[string]any{
		"queries": []any{map[string]any{
			"refId":         "A",
			"datasource":    map[string]any{"type": "parkana-sql-datasource", "uid": "parkana-sql"},
			"rawSQL":        string(sql),
			"format":        args[0],
			"intervalMs":    1000,
			"maxDataPoints": 1000,
		}},
		"from": args[1],
		"to":   args[2],
	}
	return writeJSON(body)
}

func cmdSQLRequest(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("sqlrequest wants <from-rfc3339> <to-rfc3339>")
	}
	sql, err := readStdin()
	if err != nil {
		return err
	}
	return writeJSON(map[string]any{
		"query": string(sql),
		"from":  args[0],
		"to":    args[1],
	})
}

func readDS() (dsResponse, error) {
	var resp dsResponse
	if err := json.UnmarshalRead(os.Stdin, &resp); err != nil {
		return resp, fmt.Errorf("decode Grafana response: %w", err)
	}
	return resp, nil
}

func resultA(resp dsResponse) (queryResult, error) {
	result, ok := resp.Results["A"]
	if !ok {
		if resp.Message != "" {
			return queryResult{}, fmt.Errorf("Grafana returned no result A: %s", resp.Message)
		}
		return queryResult{}, fmt.Errorf("Grafana response has no result A")
	}
	if result.Error != "" {
		return result, fmt.Errorf("query error: %s", result.Error)
	}
	return result, nil
}

func cmdTable(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("table wants <meta-type> <field-spec>...")
	}
	resp, err := readDS()
	if err != nil {
		return err
	}
	result, err := resultA(resp)
	if err != nil {
		return err
	}
	if len(result.Frames) != 1 {
		return fmt.Errorf("expected exactly 1 frame, got %d", len(result.Frames))
	}
	f := result.Frames[0]
	if got := f.Schema.Meta.Type; got != args[0] {
		return fmt.Errorf("frame meta type %q, want %q", got, args[0])
	}
	specs := args[1:]
	if len(f.Schema.Fields) != len(specs) {
		return fmt.Errorf("frame has %d fields, want %d", len(f.Schema.Fields), len(specs))
	}
	if len(f.Data.Values) != len(f.Schema.Fields) {
		return fmt.Errorf("frame has %d value columns for %d fields", len(f.Data.Values), len(f.Schema.Fields))
	}
	for i, spec := range specs {
		parts := strings.Split(spec, ",")
		if len(parts) < 2 {
			return fmt.Errorf("bad field spec %q", spec)
		}
		name, typ := parts[0], parts[1]
		if got := f.Schema.Fields[i].Name; got != name {
			return fmt.Errorf("field %d name %q, want %q", i, got, name)
		}
		if got := f.Schema.Fields[i].Type; got != typ {
			return fmt.Errorf("field %q type %q, want %q", name, got, typ)
		}
		wants := parts[2:]
		cells := f.Data.Values[i]
		if len(cells) != len(wants) {
			return fmt.Errorf("field %q has %d values, want %d", name, len(cells), len(wants))
		}
		for j, want := range wants {
			if !cellMatches(cells[j], want) {
				return fmt.Errorf("field %q row %d value %s, want %s", name, j, string(cells[j]), want)
			}
		}
	}
	return nil
}

func cmdCount(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("count wants <want>")
	}
	resp, err := readDS()
	if err != nil {
		return err
	}
	result, err := resultA(resp)
	if err != nil {
		return err
	}
	if len(result.Frames) != 1 || len(result.Frames[0].Schema.Fields) != 1 {
		return fmt.Errorf("count expected one frame with one column")
	}
	values := result.Frames[0].Data.Values
	if len(values) != 1 || len(values[0]) != 1 {
		return fmt.Errorf("count expected exactly one value")
	}
	if !cellMatches(values[0][0], "n:"+args[0]) {
		return fmt.Errorf("count value %s, want %s", string(values[0][0]), args[0])
	}
	return nil
}

func cmdError(args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("error wants <expected-status> <required-substring> <forbidden-substring>")
	}
	expectedStatus, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("error status %q is not an integer", args[0])
	}
	resp, err := readDS()
	if err != nil {
		return err
	}
	result, ok := resp.Results["A"]
	if !ok {
		if resp.Message != "" {
			return fmt.Errorf("Grafana returned no result A: %s", resp.Message)
		}
		return fmt.Errorf("Grafana response has no result A")
	}
	if result.Status != expectedStatus {
		return fmt.Errorf("result A status %d, want %d", result.Status, expectedStatus)
	}
	if result.Error == "" {
		return fmt.Errorf("result A has no error text")
	}
	if len(result.Frames) > 0 {
		return fmt.Errorf("error result still returned %d frames", len(result.Frames))
	}
	if args[1] != "" && !strings.Contains(result.Error, args[1]) {
		return fmt.Errorf("error text %q does not contain the required SQL/policy marker %q", result.Error, args[1])
	}
	if args[2] != "" && strings.Contains(result.Error, args[2]) {
		return fmt.Errorf("error text leaked forbidden content %q", args[2])
	}
	message := result.Error
	if len(message) > 300 {
		message = message[:300] + "..."
	}
	message = strings.ReplaceAll(message, "\n", " ")
	fmt.Println(message)
	return nil
}

func cmdDatasource(args []string) error {
	if len(args) != 3 {
		return fmt.Errorf("datasource wants <uid> <type> <tenant>")
	}
	var ds struct {
		UID      string `json:"uid"`
		Type     string `json:"type"`
		JSONData struct {
			Tenant string `json:"tenant"`
		} `json:"jsonData"`
	}
	if err := json.UnmarshalRead(os.Stdin, &ds); err != nil {
		return fmt.Errorf("decode datasource: %w", err)
	}
	if ds.UID != args[0] {
		return fmt.Errorf("datasource uid %q, want %q", ds.UID, args[0])
	}
	if ds.Type != args[1] {
		return fmt.Errorf("datasource type %q, want %q", ds.Type, args[1])
	}
	if ds.JSONData.Tenant != args[2] {
		return fmt.Errorf("datasource tenant %q, want %q", ds.JSONData.Tenant, args[2])
	}
	return nil
}

func cmdPlugin(args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("plugin wants <id> <version>")
	}
	var plugin struct {
		ID   string `json:"id"`
		Info struct {
			Version string `json:"version"`
		} `json:"info"`
	}
	if err := json.UnmarshalRead(os.Stdin, &plugin); err != nil {
		return fmt.Errorf("decode plugin settings: %w", err)
	}
	if plugin.ID != args[0] {
		return fmt.Errorf("plugin id %q, want %q", plugin.ID, args[0])
	}
	if plugin.Info.Version != args[1] {
		return fmt.Errorf("plugin version %q, want %q (frontend build may not have replaced %%VERSION%%)", plugin.Info.Version, args[1])
	}
	return nil
}

func cmdSQLCount(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("sqlcount wants <want>")
	}
	var resp endpointResponse
	if err := json.UnmarshalRead(os.Stdin, &resp); err != nil {
		return fmt.Errorf("decode SQL response: %w", err)
	}
	if len(resp.Columns) != 1 || len(resp.Rows) != 1 || len(resp.Rows[0]) != 1 {
		return fmt.Errorf("SQL response shape is not one row with one column")
	}
	if !cellMatches(resp.Rows[0][0], "n:"+args[0]) {
		return fmt.Errorf("SQL value %s, want %s", string(resp.Rows[0][0]), args[0])
	}
	return nil
}

func cellMatches(raw jsontext.Value, want string) bool {
	if len(want) < 2 || want[1] != ':' {
		return false
	}
	value := want[2:]
	switch want[0] {
	case 's':
		if raw.Kind() != jsontext.KindString {
			return false
		}
		var s string
		if json.Unmarshal(raw, &s) != nil {
			return false
		}
		return s == value
	case 'n':
		return raw.Kind() == jsontext.KindNumber && numberEqual(string(raw), value)
	case 'b':
		return (raw.Kind() == jsontext.KindTrue && value == "true") ||
			(raw.Kind() == jsontext.KindFalse && value == "false")
	case 'z':
		return raw.Kind() == jsontext.KindNull
	default:
		return false
	}
}

func numberEqual(a, b string) bool {
	ra, ok := new(big.Rat).SetString(a)
	if !ok {
		return false
	}
	rb, ok := new(big.Rat).SetString(b)
	if !ok {
		return false
	}
	return ra.Cmp(rb) == 0
}
GO
}

# Fixture and Grafana queries ---------------------------------------------------

write_fixture_sql() {
  cat >"$TMP/q-count.sql" <<'SQL'
SELECT count(*) AS n
FROM logs
WHERE _ts >= $__timeFrom
  AND _ts < $__timeTo
  AND labels['job'] = 'sql-e2e'
SQL

  cat >"$TMP/q-table.sql" <<'SQL'
SELECT coalesce(level, labels['level']) AS level,
       line,
       CAST(labels['code'] AS BIGINT) AS code
FROM logs
WHERE labels['job'] = 'sql-e2e'
ORDER BY _ts
SQL

  cat >"$TMP/q-series.sql" <<'SQL'
SELECT time_bucket($__interval, _ts) AS time,
       sum(CAST(labels['code'] AS DOUBLE)) AS value
FROM logs
WHERE labels['job'] = 'sql-e2e'
GROUP BY time
ORDER BY time
SQL

  cat >"$TMP/q-escape.sql" <<'SQL'
SELECT * FROM read_csv_auto('/etc/passwd')
SQL

  cat >"$TMP/q-one.sql" <<'SQL'
SELECT 1 AS one
SQL
}

fixture_payload() { # t0-ns
  local t0=$1
  printf '{"streams":[{"stream":{"job":"sql-e2e","level":"info","code":"200","service_name":"sql-e2e"},"values":[["%s","alpha"],["%s","gamma"]]},{"stream":{"job":"sql-e2e","level":"warn","code":"500","service_name":"sql-e2e"},"values":[["%s","beta"]]}]}' \
    "$t0" "$((t0 + 2000000000))" "$((t0 + 1000000000))"
}

push_fixture() {
  local t0=$1 body status
  body=$(fixture_payload "$t0")
  status=$(curl -sS --connect-timeout 2 --max-time 30 \
    -o "$TMP/push-response" -w '%{http_code}' \
    -H 'Content-Type: application/json' -H 'X-Scope-OrgID: default' \
    --data-binary "$body" "$PARKANA_URL/loki/api/v1/push") ||
    fail "fixture push to Parkana failed"
  if [[ $status != 204 ]]; then
    fail "fixture push returned HTTP $status: $(head -c 300 "$TMP/push-response" 2>/dev/null | sanitize)"
  fi
  log "fixture pushed: 3 lines at $t0, +1s, +2s"
}

ds_request() { # sql-file format out
  local sql_file=$1 format=$2 out=$3
  checker request "$format" "$FROM_MS" "$TO_MS" <"$sql_file" >"$TMP/ds-body.json" || return 1
  DS_HTTP_STATUS=$(curl -sS --connect-timeout 2 --max-time 60 \
    -H 'Content-Type: application/json' -H 'Accept: application/json' \
    -o "$out" -w '%{http_code}' \
    --data-binary @"$TMP/ds-body.json" "$GRAFANA_URL/api/ds/query") || return 1
  [[ $DS_HTTP_STATUS == 200 ]]
}

query_ds() { # sql-file format out label
  if ! ds_request "$1" "$2" "$3"; then
    fail "$4: Grafana /api/ds/query failed (HTTP ${DS_HTTP_STATUS:-none}): $(head -c 400 "$3" 2>/dev/null | sanitize)"
  fi
}

wait_visible() { # want timeout
  local want=$1 timeout=${2:-180} start deadline t
  start=$(now)
  deadline=$((start + timeout))
  while :; do
    if ds_request "$TMP/q-count.sql" table "$TMP/resp-count.json" 2>/dev/null &&
      checker count "$want" <"$TMP/resp-count.json" 2>/dev/null; then
      log "SQL visibility: $want fixture rows visible through Grafana"
      return 0
    fi
    t=$(now)
    if ((t >= deadline)); then
      warn "last SQL visibility response: $(head -c 300 "$TMP/resp-count.json" 2>/dev/null | sanitize || true)"
      fail "SQL visibility did not reach $want rows within $((t - start))s"
    fi
    sleep 2
  done
}

# Assertions --------------------------------------------------------------------

assert_datasource() {
  curl -fsS --connect-timeout 2 --max-time 10 \
    "$GRAFANA_URL/api/datasources/uid/$DATASOURCE_UID" -o "$TMP/resp-datasource.json" ||
    fail "could not read provisioned datasource $DATASOURCE_UID"
  checker datasource "$DATASOURCE_UID" "$PLUGIN_ID" default <"$TMP/resp-datasource.json" ||
    fail "provisioned datasource does not match uid/type/default tenant"
  log "provisioned datasource: uid=$DATASOURCE_UID type=$PLUGIN_ID tenant=default"
}

assert_plugin() {
  local version=$1
  [[ -n $version ]] || fail "could not read the plugin version from package.json"
  curl -fsS --connect-timeout 2 --max-time 10 \
    "$GRAFANA_URL/api/plugins/$PLUGIN_ID/settings" -o "$TMP/resp-plugin.json" ||
    fail "Grafana did not load the $PLUGIN_ID plugin"
  checker plugin "$PLUGIN_ID" "$version" <"$TMP/resp-plugin.json" ||
    fail "Grafana plugin metadata does not match the built plugin version $version"
  log "plugin loaded unsigned: $PLUGIN_ID version=$version"
}

assert_macros() {
  query_ds "$TMP/q-count.sql" table "$TMP/resp-count.json" "macro count"
  checker count 3 <"$TMP/resp-count.json" ||
    fail "\$__timeFrom/\$__timeTo count query did not return 3"
  log "macros: \$__timeFrom/\$__timeTo selected exactly 3 rows"
}

assert_table() {
  query_ds "$TMP/q-table.sql" table "$TMP/resp-table.json" "table query"
  checker table 'table' \
    'level,string,s:info,s:warn,s:info' \
    'line,string,s:alpha,s:beta,s:gamma' \
    'code,number,n:200,n:500,n:200' <"$TMP/resp-table.json" ||
    fail "table frame did not match the deterministic fixture"
  log "table frame: exact level/line/code values"
}

assert_time_series() {
  query_ds "$TMP/q-series.sql" time_series "$TMP/resp-series.json" "time series query"
  checker table 'timeseries-wide' \
    "time,time,n:$((T0_MS)),n:$((T0_MS + 1000)),n:$((T0_MS + 2000))" \
    'value,number,n:200,n:500,n:200' <"$TMP/resp-series.json" ||
    fail "time_series frame did not match the deterministic fixture timestamps/values"
  log "time_series frame: \$__interval bucket + sum(code) exact (epoch ms $T0_MS,$((T0_MS + 1000)),$((T0_MS + 2000)))"
}

check_escape_response() { # http-status response-file
  local status=$1 file=$2
  case "$status" in
    200 | 400) ;;
    *) return 1 ;;
  esac
  checker error 400 'SQL endpoint returned 400' 'root:' <"$file" >/dev/null
}

assert_escape_rejected() {
  local message
  # Grafana may return 400 when every per-query DataResponse fails, so accept
  # either transport status; the checker still requires result A status 400.
  ds_request "$TMP/q-escape.sql" table "$TMP/resp-escape.json" || true
  message=$(check_escape_response "${DS_HTTP_STATUS:-none}" "$TMP/resp-escape.json" 2>/dev/null &&
    checker error 400 'SQL endpoint returned 400' 'root:' <"$TMP/resp-escape.json") ||
    fail "filesystem escape was not a rejected SQL/policy error (HTTP ${DS_HTTP_STATUS:-none}): $(head -c 300 "$TMP/resp-escape.json" 2>/dev/null | sanitize)"
  log "rejected SQL escape: $message"
}

assert_tenant_boundary() {
  local status
  checker sqlrequest "$FROM_RFC" "$TO_RFC" <"$TMP/q-one.sql" >"$TMP/sql-one-body.json" ||
    fail "could not build the direct SQL request"

  status=$(curl -sS --connect-timeout 2 --max-time 30 \
    -o "$TMP/sql-default.json" -w '%{http_code}' \
    -H 'Content-Type: application/json' -H 'X-Scope-OrgID: default' \
    --data-binary @"$TMP/sql-one-body.json" "$PARKANA_URL/api/v1/sql") ||
    fail "direct SQL request failed"
  if [[ $status != 200 ]]; then
    fail "default-tenant SQL request returned HTTP $status: $(head -c 300 "$TMP/sql-default.json" | sanitize)"
  fi
  checker sqlcount 1 <"$TMP/sql-default.json" ||
    fail "default-tenant SELECT 1 did not return exactly 1"

  status=$(curl -sS --connect-timeout 2 --max-time 30 \
    -o "$TMP/sql-other.json" -w '%{http_code}' \
    -H 'Content-Type: application/json' -H 'X-Scope-OrgID: other-tenant' \
    --data-binary @"$TMP/sql-one-body.json" "$PARKANA_URL/api/v1/sql") ||
    fail "non-default tenant request failed unexpectedly"
  if [[ $status != 401 ]]; then
    fail "single-tenant Parkana accepted X-Scope-OrgID other-tenant (HTTP $status)"
  fi
  log "tenant boundary: default accepted, other-tenant rejected (401)"
}

# Plan / dry run ----------------------------------------------------------------

dry_run() {
  resolve_ports
  printf '[e2e-p4-sql] dry run: no Docker, build or service is touched\n'
  printf '  repo root        : %s\n' "$REPO_ROOT"
  printf '  base compose     : %s\n' "$BASE_COMPOSE"
  printf '  overlay          : %s\n' "$SQL_COMPOSE"
  printf '  provisioning     : %s\n' "$PROVISIONING"
  printf '  plugin dir       : %s\n' "$PLUGIN_DIR"
  printf '  plugin dist      : %s\n' "$PLUGIN_DIST"
  printf '  project          : %s%s\n' "$PROJECT" "$([[ $PROJECT_EXPLICIT == 1 ]] && printf ' (explicit)' || printf ' (generated)')"
  printf '  host ports       : parkana=%s garage=%s loki=%s grafana=%s (resolved now; a real run resolves again)\n' \
    "$HTTP_PORT" "$S3_PORT" "$LOKI_PORT" "$GRAFANA_PORT"
  printf '  Grafana budget   : %ss\n' "$READY_TIMEOUT"
  printf '  ingest window    : flush/upload %s/%s, upload-max-age %s\n' \
    "${PARKANA_INGEST_FLUSH_INTERVAL:-1s}" "${PARKANA_INGEST_UPLOAD_INTERVAL:-1s}" "${PARKANA_INGEST_UPLOAD_MAX_AGE:-2s}"
  printf '  plugin build     : make -C %s backend; %s/scripts/build-frontend.sh --check\n' "$PLUGIN_DIR" "$PLUGIN_DIR"
  printf '  stack services   : garage, garage-init, parkana, grafana (loki and its dependencies included)\n'
  printf '  compose argv     :'
  printf ' %q' "$DOCKER" compose -p "$PROJECT" -f "$BASE_COMPOSE" -f "$SQL_COMPOSE" --profile phase2 up -d garage garage-init parkana grafana
  printf '\n'
  printf '  Garage creds     : generated per run (values hidden)\n'
  printf '  fixture          : 3 rows (alpha/info/200, beta/warn/500, gamma/info/200)\n'
  printf '  cleanup owner    : this script only, guarded by created/--keep\n'
  printf '[e2e-p4-sql] dry run: no changes made\n'
}

# Self tests --------------------------------------------------------------------

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
  unset PARKANA_E2E_SQL_PROJECT PARKANA_E2E_SQL_TIMEOUT \
    PARKANA_HTTP_PORT PARKANA_S3_PORT LOKI_HTTP_PORT GRAFANA_HTTP_PORT
  init_defaults
  [[ $KEEP == 0 && $DRY_RUN == 0 && $SELFTEST == 0 ]] || return 1
  [[ $READY_TIMEOUT == 600 ]] || return 1
  [[ -z $PROJECT && -z $HTTP_PORT && -z $S3_PORT ]] || return 1
)

test_validation() (
  init_defaults
  PROJECT=selftest-project
  HTTP_PORT=23100
  HTTP_EXPLICIT=1
  S3_PORT=23900
  S3_EXPLICIT=1
  LOKI_PORT=23101
  LOKI_EXPLICIT=1
  GRAFANA_PORT=23000
  GRAFANA_EXPLICIT=1
  validate_args || return 1

  PROJECT="Bad Name"
  if (validate_args) >/dev/null 2>&1; then return 1; fi
  PROJECT=selftest-project

  HTTP_PORT=1+1
  if (validate_args) >/dev/null 2>&1; then return 1; fi
  HTTP_PORT=65536
  if (validate_args) >/dev/null 2>&1; then return 1; fi
  HTTP_PORT=23100

  S3_PORT=23100
  if (validate_resolved_ports) >/dev/null 2>&1; then return 1; fi
  S3_PORT=23900
  validate_resolved_ports || return 1

  READY_TIMEOUT=0
  if (validate_args) >/dev/null 2>&1; then return 1; fi
  READY_TIMEOUT=86401
  if (validate_args) >/dev/null 2>&1; then return 1; fi
  READY_TIMEOUT=600
  validate_args || return 1

  if (parse_args --bogus) >/dev/null 2>&1; then return 1; fi
)

test_port_auto_pick() (
  init_defaults
  HTTP_PORT=23100
  HTTP_EXPLICIT=0
  S3_PORT=23900
  S3_EXPLICIT=0
  LOKI_PORT=23101
  LOKI_EXPLICIT=0
  GRAFANA_PORT=23000
  GRAFANA_EXPLICIT=0
  # Preferred values are busy, every random candidate is free.
  port_free() {
    case "$1" in
      23100 | 23900 | 23101 | 23000) return 1 ;;
      *) return 0 ;;
    esac
  }
  resolve_ports || return 1
  [[ $HTTP_PORT != 23100 && $S3_PORT != 23900 && $LOKI_PORT != 23101 && $GRAFANA_PORT != 23000 ]] || return 1
  [[ $HTTP_PORT != "$S3_PORT" && $HTTP_PORT != "$LOKI_PORT" && $HTTP_PORT != "$GRAFANA_PORT" ]] || return 1
  [[ $S3_PORT != "$LOKI_PORT" && $S3_PORT != "$GRAFANA_PORT" && $LOKI_PORT != "$GRAFANA_PORT" ]] || return 1
  for p in "$HTTP_PORT" "$S3_PORT" "$LOKI_PORT" "$GRAFANA_PORT"; do
    valid_port "$p" || return 1
  done

  # An explicit busy port is an error, never a silent move.
  HTTP_PORT=23100
  HTTP_EXPLICIT=1
  if (port_free() { return 1; }; resolve_ports) >/dev/null 2>&1; then return 1; fi
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

test_fixture_payload() (
  local body
  body=$(fixture_payload 1700000000000000000)
  [[ $body == *'"alpha"'* && $body == *'"beta"'* && $body == *'"gamma"'* ]] || return 1
  [[ $body == *'"1700000000000000000"'* && $body == *'"1700000001000000000"'* && $body == *'"1700000002000000000"'* ]] || return 1
  [[ $body == *'"job":"sql-e2e"'* && $body == *'"code":"200"'* && $body == *'"code":"500"'* ]] || return 1
)

test_sql_heredocs_keep_macros() (
  write_fixture_sql
  grep -Fq '$__timeFrom' "$TMP/q-count.sql" || return 1
  grep -Fq '$__timeTo' "$TMP/q-count.sql" || return 1
  grep -Fq "labels['job']" "$TMP/q-table.sql" || return 1
  grep -Fq "read_csv_auto('/etc/passwd')" "$TMP/q-escape.sql" || return 1
  # The series fixture must exercise the interval macro and aggregation end
  # to end: a literal $__interval in the heredoc, a time alias and GROUP BY.
  grep -Fq '$__interval' "$TMP/q-series.sql" || return 1
  grep -Fq 'time_bucket($__interval, _ts) AS time' "$TMP/q-series.sql" || return 1
  grep -Fq "sum(CAST(labels['code'] AS DOUBLE)) AS value" "$TMP/q-series.sql" || return 1
  grep -Fq 'GROUP BY time' "$TMP/q-series.sql" || return 1
  grep -Fq 'ORDER BY time' "$TMP/q-series.sql" || return 1
)

test_overlay_provisioning() (
  # The base compose mounts /etc/grafana/provisioning read-only, so the SQL
  # overlay cannot nest a file mount there. It redirects Grafana to a merged
  # directory instead; the base datasource copy must stay byte-identical so
  # the loki/parkana assertions still hold.
  local base_loki="$COMPOSE_DIR/grafana/provisioning/datasources/loki.yaml"
  local merged_loki="$COMPOSE_DIR/grafana-sql/provisioning/datasources/loki.yaml"
  cmp -s "$base_loki" "$merged_loki" || return 1

  grep -Fq 'GF_PATHS_PROVISIONING: /etc/grafana/provisioning-sql' "$SQL_COMPOSE" || return 1
  grep -Fq './grafana-sql/provisioning:/etc/grafana/provisioning-sql:ro' "$SQL_COMPOSE" || return 1

  # Never nest a file mount inside the base's read-only provisioning mount,
  # and do not redefine the base directory mount in this overlay.
  if grep -Fq '/etc/grafana/provisioning/datasources' "$SQL_COMPOSE"; then return 1; fi
  if grep -Fq './grafana/provisioning:' "$SQL_COMPOSE"; then return 1; fi

  # The merged tree still carries the SQL datasource.
  grep -Fq 'uid: parkana-sql' "$COMPOSE_DIR/grafana-sql/provisioning/datasources/sql.yaml" || return 1
)

test_cleanup_ownership() (
  # cleanup_project is the only `compose down --volumes --remove-orphans`
  # invocation; the printed teardown hint has no output redirection.
  [[ $(grep -cE '^[[:space:]]*(if ! )?compose down --volumes' "$SCRIPT_PATH") == 1 ]] || return 1

  # A project this run did not create is never brought down.
  (
    CREATED=0
    KEEP=0
    TMP=$(mktemp -d)
    compose() { echo CALLED; return 1; }
    cleanup
  ) >"$TMP/out" 2>"$TMP/err" || true
  ! grep -q CALLED "$TMP/out" || return 1

  # --keep prints the teardown command and never calls down.
  (
    CREATED=1
    KEEP=1
    TMP=$(mktemp -d)
    compose() { echo CALLED; return 1; }
    cleanup
  ) >"$TMP/out" 2>"$TMP/err" || true
  ! grep -q CALLED "$TMP/out" || return 1
  grep -q 'keeping project' "$TMP/err" || return 1

  # A stack this run created is removed exactly once.
  local log
  log=$(mktemp)
  (
    CREATED=1
    KEEP=0
    TMP=$(mktemp -d)
    compose() {
      printf '%s\n' "$*" >>"$log"
    }
    cleanup
  ) >/dev/null 2>&1 || true
  grep -q 'down --volumes --remove-orphans' "$log" || return 1
  rm -f "$log"
)

test_checker() (
  build_checker || return 1
  local table_good table_bad count_good error_good error_leak error_health ds_good plugin_good

  table_good='{"results":{"A":{"status":200,"frames":[{"schema":{"meta":{"type":"table"},"fields":[{"name":"level","type":"string"},{"name":"code","type":"number"}]},"data":{"values":[["info","warn"],[200,500]]}}]}}}'
  table_bad='{"results":{"A":{"status":200,"frames":[{"schema":{"meta":{"type":"table"},"fields":[{"name":"level","type":"string"},{"name":"code","type":"number"}]},"data":{"values":[["info","warn"],[200,501]]}}]}}}'
  count_good='{"results":{"A":{"status":200,"frames":[{"schema":{"meta":{"type":"table"},"fields":[{"name":"n","type":"number"}]},"data":{"values":[[3]]}}]}}}'
  error_good='{"results":{"A":{"status":400,"error":"plugin: SQL endpoint returned 400 Bad Request: sql: statement rejected by policy","frames":[]}}}'
  error_leak='{"results":{"A":{"status":400,"error":"plugin: SQL endpoint returned 400 Bad Request: root:x:0:0","frames":[]}}}'
  error_health='{"results":{"A":{"status":400,"error":"plugin: health check failed","frames":[]}}}'
  ds_good='{"uid":"parkana-sql","type":"parkana-sql-datasource","jsonData":{"tenant":"default"}}'
  plugin_good='{"id":"parkana-sql-datasource","info":{"version":"1.0.0"}}'

  printf '%s' "$table_good" | checker table 'table' 'level,string,s:info,s:warn' 'code,number,n:200,n:500' || return 1
  if printf '%s' "$table_bad" | checker table 'table' 'level,string,s:info,s:warn' 'code,number,n:200,n:500' >/dev/null 2>&1; then return 1; fi
  printf '%s' "$count_good" | checker count 3 || return 1
  if printf '%s' "$count_good" | checker count 4 >/dev/null 2>&1; then return 1; fi
  printf '%s' "$error_good" | checker error 400 'SQL endpoint returned 400' 'root:' >/dev/null || return 1
  if printf '%s' "$error_good" | checker error 500 'SQL endpoint returned 400' 'root:' >/dev/null 2>&1; then return 1; fi
  if printf '%s' "$error_good" | checker error 400 'SQL endpoint returned 400' 'rejected' >/dev/null 2>&1; then return 1; fi
  if printf '%s' "$error_leak" | checker error 400 'SQL endpoint returned 400' 'root:' >/dev/null 2>&1; then return 1; fi
  if printf '%s' "$error_health" | checker error 400 'SQL endpoint returned 400' 'root:' >/dev/null 2>&1; then return 1; fi
  printf '%s' "$ds_good" | checker datasource parkana-sql parkana-sql-datasource default || return 1
  if printf '%s' "$ds_good" | checker datasource parkana-sql parkana-sql-datasource other >/dev/null 2>&1; then return 1; fi
  printf '%s' "$plugin_good" | checker plugin parkana-sql-datasource 1.0.0 || return 1
  if printf '%s' "$plugin_good" | checker plugin parkana-sql-datasource 9.9.9 >/dev/null 2>&1; then return 1; fi

  printf 'SELECT 1' | checker request table 1 2 >"$TMP/request.json" || return 1
  grep -Fq '"rawSQL":"SELECT 1"' "$TMP/request.json" || return 1
  grep -Fq '"format":"table"' "$TMP/request.json" || return 1
  printf 'SELECT 1' | checker sqlrequest 2026-01-01T00:00:00Z 2026-01-01T00:01:00Z >"$TMP/sqlrequest.json" || return 1
  grep -Fq '"query":"SELECT 1"' "$TMP/sqlrequest.json" || return 1
)

test_escape_classification() (
  build_checker || return 1
  local good wrong health leak status
  local good_file="$TMP/escape-good.json" wrong_file="$TMP/escape-wrong.json"
  local health_file="$TMP/escape-health.json" leak_file="$TMP/escape-leak.json"
  good='{"results":{"A":{"status":400,"error":"plugin: SQL endpoint returned 400 Bad Request: sql: statement rejected by policy","frames":[]}}}'
  wrong='{"results":{"A":{"status":500,"error":"plugin: SQL endpoint returned 400 Bad Request: sql: statement rejected by policy","frames":[]}}}'
  health='{"results":{"A":{"status":400,"error":"plugin: health check failed","frames":[]}}}'
  leak='{"results":{"A":{"status":400,"error":"plugin: SQL endpoint returned 400 Bad Request: root:x:0:0","frames":[]}}}'
  printf '%s' "$good" >"$good_file" || return 1
  printf '%s' "$wrong" >"$wrong_file" || return 1
  printf '%s' "$health" >"$health_file" || return 1
  printf '%s' "$leak" >"$leak_file" || return 1

  # A per-query failure may arrive as HTTP 200 or HTTP 400; both are accepted
  # only when result A carries status 400 and a SQL/policy error.
  for status in 200 400; do
    (
      ds_request() {
        DS_HTTP_STATUS=$status
        cp "$good_file" "$3"
        [[ $status == 200 ]]
      }
      assert_escape_rejected
    ) >/dev/null 2>&1 || return 1
  done

  # Transport 5xx/404 and wrong error classes must never count as a rejection.
  for status in 500 404; do
    if (
      ds_request() {
        DS_HTTP_STATUS=$status
        cp "$good_file" "$3"
        [[ $status == 200 ]]
      }
      assert_escape_rejected
    ) >/dev/null 2>&1; then
      return 1
    fi
  done
  if (
    ds_request() { DS_HTTP_STATUS=200; cp "$wrong_file" "$3"; return 0; }
    assert_escape_rejected
  ) >/dev/null 2>&1; then return 1; fi
  if (
    ds_request() { DS_HTTP_STATUS=400; cp "$health_file" "$3"; return 1; }
    assert_escape_rejected
  ) >/dev/null 2>&1; then return 1; fi
  if (
    ds_request() { DS_HTTP_STATUS=200; cp "$leak_file" "$3"; return 0; }
    assert_escape_rejected
  ) >/dev/null 2>&1; then return 1; fi
)

test_helper_toolchain() (
  # Regression: the helper's temporary dir has no go.mod, so the old build
  # used whatever host `go` happened to be on PATH. A host Go older than the
  # pinned root version must fall back to the pinned Go container instead of
  # building with a toolchain that lacks encoding/json/v2.
  local bin="$TMP/toolchain-bin" log="$TMP/toolchain.log" pinned expected
  mkdir -p "$bin"
  cat >"$bin/go" <<'SH'
#!/usr/bin/env bash
case "$1" in
  version)
    if [[ -n ${FAKE_GO_VERSION:-} ]]; then
      printf 'go version %s linux/amd64\n' "$FAKE_GO_VERSION"
    elif [[ -f go.mod ]] && grep -Eq "^go ${PINNED_GO_VERSION:-}" go.mod; then
      printf 'go version go%s linux/amd64\n' "$PINNED_GO_VERSION"
    else
      printf 'go version go%s linux/amd64\n' "${FAKE_OLD_GO_VERSION:-1.25.0}"
    fi
    ;;
  build) printf 'host-build\n' >>"${TC_LOG:?}" ;;
  *) exit 0 ;;
esac
SH
  cat >"$bin/docker" <<'SH'
#!/usr/bin/env bash
printf 'container-run: %s\n' "$*" >>"${TC_LOG:?}"
SH
  chmod +x "$bin/go" "$bin/docker"
  export TC_LOG="$log"

  pinned=$(pinned_go_version) || return 1
  expected=$(grep -E '^go [0-9]' "$REPO_ROOT/go.mod" | head -n 1 | cut -d' ' -f2)
  [[ -n $pinned && $pinned == "$expected" ]] || return 1

  assert_choice() { # name expect host|container
    local name=$1 expect=$2
    if [[ $expect == host ]]; then
      grep -Fq host-build "$log" || {
        printf 'FAIL toolchain %s: host build not used\n' "$name" >&2
        return 1
      }
      if grep -Fq container-run "$log"; then
        printf 'FAIL toolchain %s: pinned container used unexpectedly\n' "$name" >&2
        return 1
      fi
    else
      grep -Fq container-run "$log" || {
        printf 'FAIL toolchain %s: pinned container not used\n' "$name" >&2
        return 1
      }
      if grep -Fq host-build "$log"; then
        printf 'FAIL toolchain %s: incompatible host build used\n' "$name" >&2
        return 1
      fi
      grep -Fq 'golang:1.27.3-bookworm' "$log" || {
        printf 'FAIL toolchain %s: fallback is not the pinned Go image\n' "$name" >&2
        return 1
      }
    fi
  }

  toolchain_case() { # fake-go-version expect host|container
    local fake=$1 expect=$2
    : >"$log"
    (
      cd "$TMP" || exit 1
      PATH="$bin:$PATH"
      DOCKER="$bin/docker"
      export FAKE_GO_VERSION=$fake
      build_checker
    ) >/dev/null 2>&1 || {
      printf 'FAIL toolchain %s: build_checker failed\n' "$fake" >&2
      return 1
    }
    assert_choice "$fake" "$expect"
  }

  # Emulate GOTOOLCHAIN=auto from the repo root: a bare probe there reports the
  # pinned go.mod toolchain even when the base host Go is older, while the
  # helper's temporary dir reports the old host Go. build_checker must decide
  # from the temporary-dir context it actually builds in.
  cwd_case() { # workdir name
    local workdir=$1 name=$2
    : >"$log"
    (
      cd "$workdir" || exit 1
      PATH="$bin:$PATH"
      DOCKER="$bin/docker"
      unset FAKE_GO_VERSION
      export PINNED_GO_VERSION="$pinned" FAKE_OLD_GO_VERSION=1.25.0
      build_checker
    ) >/dev/null 2>&1 || {
      printf 'FAIL toolchain %s: build_checker failed\n' "$name" >&2
      return 1
    }
    assert_choice "$name" container
  }

  local probe
  probe=$(cd "$REPO_ROOT" && PATH="$bin:$PATH" PINNED_GO_VERSION="$pinned" FAKE_OLD_GO_VERSION=1.25.0 go_probe_version "$bin/go") || return 1
  [[ $probe == "$pinned" ]] || return 1
  probe=$(cd "$TMP" && PATH="$bin:$PATH" PINNED_GO_VERSION="$pinned" FAKE_OLD_GO_VERSION=1.25.0 go_probe_version "$bin/go") || return 1
  [[ $probe == 1.25.0 ]] || return 1

  toolchain_case go1.25.0 container || return 1
  toolchain_case "$pinned" host || return 1
  toolchain_case go1.28.1 host || return 1
  toolchain_case bogus container || return 1
  cwd_case "$REPO_ROOT" "cwd-sensitive probe uses the helper dir" || return 1
)

test_main_generates_credentials() (
  # Exercise the real main flow up to the compose start with every effectful
  # dependency mocked out, so a missing generate_credentials call in main is
  # caught without Docker or a stack.
  require_cmds() { :; }
  resolve_ports() {
    HTTP_PORT=25000
    S3_PORT=25001
    LOKI_PORT=25002
    GRAFANA_PORT=25003
  }
  ensure_project_available() { return 0; }
  port_published() { return 1; }
  write_fixture_sql() { :; }
  build_checker() { :; }
  build_plugin() { :; }
  compose() { return 0; }
  wait_service_exit() { return 0; }
  wait_http() { return 0; }
  wait_datasource() { return 0; }
  wait_health() { return 0; }
  assert_datasource() { :; }
  assert_plugin() { :; }
  plugin_version() { printf '1.0.0'; }
  push_fixture() { :; }
  wait_visible() { :; }
  assert_macros() { :; }
  assert_table() { :; }
  assert_time_series() { :; }
  assert_escape_rejected() { :; }
  assert_tenant_boundary() { :; }
  cleanup() { :; }

  main --project selftest-main --timeout 1 >/dev/null 2>&1 || return 1
  [[ $GARAGE_KEY_ID =~ ^GK[0-9a-f]{24}$ ]] || return 1
  [[ $GARAGE_KEY_SECRET =~ ^[0-9a-f]{64}$ ]] || return 1
  [[ $GARAGE_KEY_ID != GK5c7e9a3b1d8f2e6c4a0b9d8f ]] || return 1
  [[ $GARAGE_KEY_SECRET != 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef ]] || return 1
  [[ ${PARKANA_DEV_GARAGE_KEY_ID:-} == "$GARAGE_KEY_ID" ]] || return 1
  [[ ${PARKANA_DEV_GARAGE_KEY_SECRET:-} == "$GARAGE_KEY_SECRET" ]] || return 1
)

self_test() {
  local failed=0
  printf '[e2e-p4-sql] self-test: offline logic and embedded JSON helper\n'
  self_case "defaults and ports are unset until resolved" test_defaults || failed=1
  self_case "argument validation rejects bad ports/projects/timeouts" test_validation || failed=1
  self_case "random ports avoid the explicit/busy set; explicit busy fails" test_port_auto_pick || failed=1
  self_case "fresh Garage credentials are random and exported" test_random_credentials || failed=1
  self_case "fixture payload keeps deterministic timestamps and values" test_fixture_payload || failed=1
  self_case "SQL heredocs keep literal macro and quote tokens" test_sql_heredocs_keep_macros || failed=1
  self_case "SQL overlay redirects Grafana provisioning without a nested read-only mount" test_overlay_provisioning || failed=1
  self_case "cleanup is guarded to this run's own project" test_cleanup_ownership || failed=1
  self_case "helper toolchain probes the TMP build dir and honors the pinned go.mod" test_helper_toolchain || failed=1
  if helper_build_available; then
    self_case "JSON assertion helper accepts the expected frames and rejects drift" test_checker || failed=1
    self_case "escape accepts only 200/400 SQL-policy 400 errors" test_escape_classification || failed=1
  else
    printf 'skip JSON assertion helper execution (host Go is missing or older than the pinned go.mod version; real runs use the pinned Go container)\n'
  fi
  self_case "real main path generates and exports Garage credentials" test_main_generates_credentials || failed=1
  if ((failed)); then
    printf '[e2e-p4-sql] self-test: FAILED\n' >&2
    return 1
  fi
  printf '[e2e-p4-sql] self-test: all checks passed\n'
  return 0
}

# Main --------------------------------------------------------------------------

require_cmds() {
  local cmd
  for cmd in "$DOCKER" curl od tr date mktemp sed grep head; do
    command -v "$cmd" >/dev/null 2>&1 || usage_fail "missing required command: $cmd"
  done
  if command -v go >/dev/null 2>&1; then
    command -v make >/dev/null 2>&1 || usage_fail "missing required command: make (needed for the plugin backend)"
  fi
}

main() {
  init_defaults
  parse_args "$@"
  if ((SELFTEST)); then
    self_test
    return
  fi
  if ((DRY_RUN)); then
    validate_args
    dry_run
    return
  fi

  require_cmds
  validate_args
  generate_credentials
  resolve_ports
  export_ports
  export_ingest_defaults
  ensure_project_available || fail "compose project '$PROJECT' already exists"
  local port
  for port in "$HTTP_PORT" "$S3_PORT" "$LOKI_PORT" "$GRAFANA_PORT"; do
    if port_published "$port"; then
      fail "host port $port is already published by another container"
    fi
  done

  PARKANA_URL="http://127.0.0.1:$HTTP_PORT"
  GRAFANA_URL="http://127.0.0.1:$GRAFANA_PORT"

  say "plan"
  log "project=$PROJECT"
  log "ports: parkana=$HTTP_PORT garage=$S3_PORT loki=$LOKI_PORT grafana=$GRAFANA_PORT"
  log "flush/upload: ${PARKANA_INGEST_FLUSH_INTERVAL}/${PARKANA_INGEST_UPLOAD_INTERVAL}, upload-max-age ${PARKANA_INGEST_UPLOAD_MAX_AGE}"
  log "plugin: $PLUGIN_DIR"
  log "Garage credentials: generated for this run (values hidden)"

  write_fixture_sql
  build_checker
  build_plugin

  say "static compose check"
  compose config >/dev/null || fail "compose config is invalid for the SQL overlay"
  log "base + overlay compose config: OK"

  say "stack start"
  CREATED=1
  trap cleanup EXIT INT TERM
  compose build garage parkana || {
    CREATED=0
    fail "image build failed"
  }
  compose up -d garage garage-init parkana grafana || fail "docker compose up failed"
  wait_service_exit garage-init 0 300 || fail "garage-init did not exit 0"

  say "readiness"
  wait_http "$PARKANA_URL/ready" "Parkana /ready" 180 || fail "Parkana not ready"
  wait_http "$GRAFANA_URL/api/health" "Grafana /api/health" "$READY_TIMEOUT" || fail "Grafana not ready"
  wait_datasource "$DATASOURCE_UID" "$READY_TIMEOUT" || fail "SQL datasource was not provisioned"
  wait_health "$DATASOURCE_UID" "$READY_TIMEOUT" || fail "SQL datasource health check failed"
  # The overlay adds one provisioning file under the base read-only directory
  # mount. Both base datasources must remain provisioned: this also catches a
  # nested single-file mount that accidentally hides the base directory.
  wait_datasource loki "$READY_TIMEOUT" || fail "base Loki datasource provisioning was lost under the SQL overlay"
  wait_datasource parkana "$READY_TIMEOUT" || fail "base Parkana datasource provisioning was lost under the SQL overlay"
  log "Parkana ready, Grafana ready, datasource $DATASOURCE_UID healthy"
  log "base provisioning preserved: loki + parkana datasources present"
  assert_datasource
  assert_plugin "$(plugin_version)"

  say "fixture push"
  T0_NS=$(( $(date +%s) * 1000000000 ))
  T0_MS=$((T0_NS / 1000000))
  FROM_MS=$((T0_MS - 60000))
  TO_MS=$((T0_MS + 60000))
  FROM_RFC=$(date -u -d "@$((T0_NS / 1000000000 - 60))" +%Y-%m-%dT%H:%M:%SZ)
  TO_RFC=$(date -u -d "@$((T0_NS / 1000000000 + 60))" +%Y-%m-%dT%H:%M:%SZ)
  push_fixture "$T0_NS"

  say "SQL visibility"
  wait_visible 3 180

  say "Grafana SQL assertions"
  assert_macros
  assert_table
  assert_time_series
  assert_escape_rejected

  say "tenant boundary"
  assert_tenant_boundary

  say "PASS (phase 4 SQL e2e)"
  log "project=$PROJECT; Grafana $GRAFANA_URL; datasource uid=$DATASOURCE_UID"
  if ((KEEP == 1)); then
    log "stack kept: project=$PROJECT grafana-host-port=$GRAFANA_PORT ($GRAFANA_URL) parkana-host-port=$HTTP_PORT datasource-uid=$DATASOURCE_UID"
  fi
}

main "$@"
