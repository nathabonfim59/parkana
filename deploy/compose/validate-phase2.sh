#!/bin/sh
# Phase 2 stack validation for Parkana's compose file.
#
# This is the B3a acceptance check: build the Parkana/pushgen images, start the
# `phase2` profile (Garage, Garage init, Parkana, Loki, Alloy, Grafana and the
# three finite generators), assert every readiness endpoint and effective
# setting, then require the generated corpus to be visible in Loki, in
# Parkana's physical catalog and through Parkana's unique index/stats read API.
# `--full` repeats those checks across a restart.
#
# Docker work belongs on a Boat VM:
#
#   tm-boat test <worktree> -- sh deploy/compose/validate-phase2.sh --smoke
#   tm-boat test <worktree> -- sh deploy/compose/validate-phase2.sh --full
#
# `--smoke` checks infrastructure, Loki, Alloy, Grafana datasource
# provisioning, all three generators, the Loki count, the physical Parkana
# catalog sum (at least the expected rows; excess is reported as retry
# duplicates stored) and Parkana's unique index/stats entries over the full
# generated range. Parkana's datasource health is reported as pending in smoke.
# `--full` repeats the catalog/unique/Loki readiness after a restart and is the
# real acceptance.
#
# `--selftest` runs the pure shell checks and mocked failure paths with no
# Docker, network or services.
#
# Expected total: base + job + corpus. The three line-count variables default
# to 20000, 1000 and 1200 (currently 22200). An explicit
# PARKANA_GENERATOR_EXPECTED_TOTAL must match that sum; a conflict is an error.
#
# Grafana's first start can spend minutes in SQLite migrations. The Grafana
# readiness/version/datasource budget is PARKANA_VALIDATE_GRAFANA_READY_TIMEOUT
# seconds (default 600, canonical positive integer <= 86400), measured as real
# elapsed time and reused after restart.
#
# The script uses its own compose project and loopback ports, refuses to touch
# an existing project, and deletes only what it created. The pinned Grafana
# image does not bundle the Loki datasource; GF_PLUGINS_PREINSTALL_SYNC
# downloads loki@13.2.5 from grafana.com on first start, so the Docker host
# needs outbound HTTPS to grafana.com. That download is cached in the
# grafana-data volume and verified by version, not assumed.
#
# Alloy sends protobuf pushes with `Content-Encoding: snappy` (its loki.write
# wire format). Parkana's ingest accepts that encoding; the smoke run fails if
# Alloy logs a Parkana ingest rejection, the catalog row count is short, or
# Parkana's unique index/stats entries differ from the generated total.
#
# `--full --compare-corpus PATH` builds `./cmd/lokicompare` once into this
# script's temporary directory *before* any compose build/up, then runs the
# frozen binary after the exact counts and restart check while this script
# still owns the compose project. Comparison stays opt-in: PATH is required
# with the flag and has no default, so a direct validator run never guesses a
# corpus. scripts/e2e-phase2.sh supplies the checked-in
# cmd/lokicompare/testdata/grafana-corpus.json when its own default applies.
# The comparison gets one stable nanosecond range: START is captured just
# before the phase 2 stack is started (the generators start with `up -d`) and
# END after every upload/count barrier has passed. The binary's 0/1/2 exit
# status is propagated unchanged through the existing cleanup trap, which also
# removes the temporary binary.
set -eu

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
CALLER_CWD=$(pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
cd "$SCRIPT_DIR"
SCRIPT_PATH="$SCRIPT_DIR/$(basename "$0")"

MODE=smoke
KEEP=0
SELFTEST=0
PROJECT="${PARKANA_VALIDATE_P2_PROJECT:-}"
DOCKER="${DOCKER:-docker}"
TMP=$(mktemp -d)
COMPARE_CORPUS=""
COMPARE_TENANT=""
COMPARE_TIMEOUT=""
COMPARE_START_NS=""
COMPARE_END_NS=""
COMPARE_BIN=""
RANGE_START_NS=""
RANGE_END_NS=""

usage() {
    cat <<'EOF'
usage: validate-phase2.sh [--smoke|--full|--selftest] [--project NAME] [--keep]
                          [--compare-corpus PATH [--compare-tenant ID]
                           [--compare-timeout DURATION]]

  --smoke     stack readiness: infra, Loki, generators, physical catalog and
              unique Parkana index/stats entries over the generated range
              (default; datasource health is reported as pending)
  --full      smoke + restart persistence
  --selftest  pure shell and mocked failure checks; no Docker or network
  --project   compose project name (default: generated)
  --keep      leave containers/volumes running for debugging
  --compare-corpus PATH
              build ./cmd/lokicompare once before stack startup, then run it
              against Loki and Parkana after the --full counts/restart; PATH
              is required, there is no default. scripts/e2e-phase2.sh supplies
              cmd/lokicompare/testdata/grafana-corpus.json by default.
  --compare-tenant ID
              X-Scope-OrgID passed to lokicompare (requires --compare-corpus)
  --compare-timeout DURATION
              per-request lokicompare timeout, e.g. 30s (requires
              --compare-corpus; the CLI default is used when omitted)
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --smoke) MODE=smoke ;;
        --full) MODE=full ;;
        --selftest) SELFTEST=1 ;;
        --project) shift; PROJECT="${1:?--project needs a name}" ;;
        --keep) KEEP=1 ;;
        --compare-corpus) shift; COMPARE_CORPUS="${1:?--compare-corpus needs a path}" ;;
        --compare-tenant) shift; COMPARE_TENANT="${1:?--compare-tenant needs a value}" ;;
        --compare-timeout) shift; COMPARE_TIMEOUT="${1:?--compare-timeout needs a value}" ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

say() { printf '\n=== %s ===\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
now() { date +%s; }

MAX_GENERATOR_TOTAL=100000000
MAX_READY_TIMEOUT=86400

is_positive_uint() { # canonical positive decimal: first digit 1-9, rest digits
    case "$1" in
        ""|0*|*[!0-9]*) return 1 ;;
    esac
    [ "${#1}" -le 9 ] || return 1
    [ "$1" -le "$MAX_GENERATOR_TOTAL" ]
}

is_ready_timeout() { # canonical positive decimal timeout in seconds
    case "$1" in
        ""|0*|*[!0-9]*) return 1 ;;
    esac
    [ "${#1}" -le 5 ] || return 1
    [ "$1" -le "$MAX_READY_TIMEOUT" ]
}

json_version() {
    sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1
}

plugin_version_from_json() {
    printf '%s' "$1" |
        sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([0-9][0-9.]*\)".*/\1/p' |
        head -n 1
}

check_plugin_version() { # json want
    [ "$(plugin_version_from_json "$1")" = "$2" ]
}

loki_config_disables_service_discovery() { # config
    printf '%s\n' "$1" | grep -Eq 'discover_service_name:[[:space:]]*\[\]' && return 0
    line=$(printf '%s\n' "$1" | awk '/discover_service_name:/{getline; print; exit}')
    printf '%s\n' "$line" | grep -Eq '^[[:space:]]*\[\]'
}

check_loki_config() { # config
    cfg=$1
    printf '%s\n' "$cfg" | grep -q 'discover_log_levels: false' || return 1
    printf '%s\n' "$cfg" | grep -q 'allow_structured_metadata: true' || return 1
    loki_config_disables_service_discovery "$cfg" || return 1
    return 0
}

resolve_expected() {
    base=${PARKANA_GENERATOR_BASE_TOTAL:-20000}
    job=${PARKANA_GENERATOR_JOB_TOTAL:-1000}
    corpus=${PARKANA_GENERATOR_CORPUS_TOTAL:-1200}
    for v in "$base" "$job" "$corpus"; do
        is_positive_uint "$v" || {
            echo "generator line counts must be positive integers <= ${MAX_GENERATOR_TOTAL}, got '$v'" >&2
            return 1
        }
    done
    sum=$((base + job + corpus))
    [ "$sum" -le "$MAX_GENERATOR_TOTAL" ] || {
        echo "base+job+corpus=$sum exceeds ${MAX_GENERATOR_TOTAL}" >&2
        return 1
    }
    if [ -n "${PARKANA_GENERATOR_EXPECTED_TOTAL:-}" ]; then
        is_positive_uint "$PARKANA_GENERATOR_EXPECTED_TOTAL" || {
            echo "PARKANA_GENERATOR_EXPECTED_TOTAL must be a positive integer <= ${MAX_GENERATOR_TOTAL}, got '$PARKANA_GENERATOR_EXPECTED_TOTAL'" >&2
            return 1
        }
        [ "$PARKANA_GENERATOR_EXPECTED_TOTAL" -eq "$sum" ] || {
            echo "PARKANA_GENERATOR_EXPECTED_TOTAL=$PARKANA_GENERATOR_EXPECTED_TOTAL conflicts with base+job+corpus=$sum" >&2
            return 1
        }
    fi
    printf '%s\n' "$sum"
}

validate_compare_args() {
    if [ -z "$COMPARE_CORPUS" ]; then
        if [ -n "$COMPARE_TENANT$COMPARE_TIMEOUT" ]; then
            echo "--compare-tenant/--compare-timeout require --compare-corpus" >&2
            return 1
        fi
        return 0
    fi
    if [ "$MODE" != full ]; then
        echo "--compare-corpus is only valid with --full" >&2
        return 1
    fi
    case "$COMPARE_CORPUS" in
        /*) ;;
        *) COMPARE_CORPUS="$CALLER_CWD/$COMPARE_CORPUS" ;;
    esac
    _compare_dir=$(dirname -- "$COMPARE_CORPUS")
    _compare_base=$(basename -- "$COMPARE_CORPUS")
    _compare_resolved=$(cd -- "$_compare_dir" 2>/dev/null && pwd) || {
        echo "compare corpus directory does not exist: $COMPARE_CORPUS" >&2
        return 1
    }
    COMPARE_CORPUS="$_compare_resolved/$_compare_base"
    if [ ! -f "$COMPARE_CORPUS" ] || [ ! -r "$COMPARE_CORPUS" ]; then
        echo "compare corpus is not a readable file: $COMPARE_CORPUS" >&2
        return 1
    fi
    return 0
}

build_corpus_compare() {
    [ -n "$COMPARE_CORPUS" ] || return 0
    command -v go >/dev/null 2>&1 ||
        fail "go is required for --compare-corpus (lokicompare is built from source)"
    COMPARE_BIN="$TMP/lokicompare"
    if ! go -C "$REPO_ROOT" build -o "$COMPARE_BIN" ./cmd/lokicompare; then
        fail "go build ./cmd/lokicompare failed"
    fi
    [ -x "$COMPARE_BIN" ] || fail "go build did not produce $COMPARE_BIN"
    echo "lokicompare binary: $COMPARE_BIN"
}

run_corpus_compare() {
    set -- "$COMPARE_BIN" \
        -left "$LOKI_URL" \
        -right "$PARKANA_URL" \
        -corpus "$COMPARE_CORPUS" \
        -start "$COMPARE_START_NS" \
        -end "$COMPARE_END_NS"
    if [ -n "$COMPARE_TENANT" ]; then
        set -- "$@" -tenant "$COMPARE_TENANT"
    fi
    if [ -n "$COMPARE_TIMEOUT" ]; then
        set -- "$@" -timeout "$COMPARE_TIMEOUT"
    fi
    "$@"
}

compose() { timeout 30 "$DOCKER" compose -p "$PROJECT" --project-directory . -f compose.yaml --profile phase2 "$@"; }
compose_phase1() { timeout 30 "$DOCKER" compose -p "$PROJECT" --project-directory . -f compose.yaml "$@"; }

sanitize() { grep -Fv -- "$PARKANA_DEV_GARAGE_KEY_SECRET" || true; }

project_exists() {
    [ -n "$(compose_phase1 ps -a -q 2>/dev/null || true)" ] && return 0
    [ -n "$(timeout 20 "$DOCKER" volume ls -q --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null || true)" ] && return 0
    [ -n "$(timeout 20 "$DOCKER" network ls -q --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null || true)" ] && return 0
    return 1
}

ensure_project_available() {
    if project_exists; then
        echo "compose project '$PROJECT' already exists" >&2
        return 1
    fi
    return 0
}

port_published() { timeout 20 "$DOCKER" ps --format '{{.Ports}}' 2>/dev/null | grep -q ":$1->"; }

diagnostics() {
    compose ps 2>&1 | sanitize >&2 || true
    for svc in parkana loki alloy grafana pushgen-base pushgen-job pushgen-corpus garage-init; do
        echo "--- $svc (last 40 lines) ---" >&2
        compose logs --no-color --tail=40 "$svc" 2>&1 | sanitize >&2 || true
    done
}

cleanup_project() {
    if [ "$created" = 1 ] && [ "$KEEP" = 0 ]; then
        if ! compose down --volumes --remove-orphans >/dev/null 2>&1; then
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
    if [ "$status" -ne 0 ] && [ "$created" = 1 ]; then
        diagnostics || true
    fi
    if [ "$created" = 1 ] && [ "$KEEP" = 1 ]; then
        echo "keeping project '$PROJECT'; teardown: $DOCKER compose -p $PROJECT --project-directory deploy/compose -f compose.yaml --profile phase2 down --volumes --remove-orphans" >&2
    fi
    cleanup_project || cleanup_status=$?
    rm -rf "$TMP"
    if [ "$status" -ne 0 ]; then
        exit "$status"
    fi
    exit "$cleanup_status"
}

wait_http() { # url name timeout
    url=$1 name=$2 timeout=${3:-180}
    start=$(now)
    deadline=$((start + timeout))
    while :; do
        if curl -fsS --connect-timeout 2 --max-time 5 "$url" >/dev/null 2>&1; then
            return 0
        fi
        t=$(now)
        if [ "$t" -ge "$deadline" ]; then
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
        got=$(printf '%s' "$body" | json_version)
        if [ "$got" = "$want" ]; then
            echo "$label version: $got"
            return 0
        fi
        t=$(now)
        if [ "$t" -ge "$deadline" ]; then
            echo "$label version is '$got', want $want after $((t - start))s" >&2
            return 1
        fi
        sleep 1
    done
}

wait_service_exit() { # service want timeout
    svc=$1 want=$2 timeout=${3:-300}
    start=$(now)
    deadline=$((start + timeout))
    while :; do
        id=$(compose ps -a -q "$svc" 2>/dev/null || true)
        if [ -n "$id" ]; then
            state=$(timeout 10 "$DOCKER" inspect -f '{{.State.Status}}' "$id")
            if [ "$state" = exited ]; then
                code=$(timeout 10 "$DOCKER" inspect -f '{{.State.ExitCode}}' "$id")
                if [ "$code" = "$want" ]; then
                    return 0
                fi
                echo "$svc exited $code, want $want" >&2
                return 1
            fi
        fi
        t=$(now)
        if [ "$t" -ge "$deadline" ]; then
            echo "$svc did not exit within $((t - start))s" >&2
            return 1
        fi
        sleep 1
    done
}

json_count() { # stdin JSON vector -> first numeric value
    sed -n 's/.*"value":\[[^,]*,"\([0-9][0-9]*\)"\].*/\1/p' | head -n 1
}

# json_uint_field extracts one field from the known flat index/stats object:
# exactly streams, chunks, bytes and entries, each a canonical non-negative
# decimal (0 or no leading zero). It accepts JSON whitespace only between
# tokens, rejects nested values, extra or duplicate keys, junk after the final
# brace and whitespace inside a number, and returns empty rather than a
# default. It is deliberately not a general JSON parser.
json_uint_field() { # stdin flat index/stats JSON -> decimal field value
    field=$1
    awk -v want="$field" '
        { body = body " " $0 }
        END {
            sub(/^[[:space:]]+/, "", body)
            sub(/[[:space:]]+$/, "", body)
            if (substr(body, 1, 1) != "{" || substr(body, length(body), 1) != "}") exit 1
            pos = 2
            end = length(body)
            pairs = 0
            found = ""
            while (pos < end) {
                rest = substr(body, pos)
                if (!match(rest, /^[[:space:]]*"[A-Za-z_][A-Za-z0-9_]*"[[:space:]]*:[[:space:]]*[0-9]+[[:space:]]*[,}]/)) exit 1
                token = substr(rest, RSTART, RLENGTH)
                key = token
                sub(/^[[:space:]]*"/, "", key)
                sub(/".*$/, "", key)
                value = token
                sub(/^[^:]*:[[:space:]]*/, "", value)
                sub(/[[:space:]]*[,}][[:space:]]*$/, "", value)
                if (value !~ /^(0|[1-9][0-9]*)$/) exit 1
                if (key != "streams" && key != "chunks" && key != "bytes" && key != "entries") exit 1
                if (seen[key]++) exit 1
                if (key == want) found = value
                pairs++
                pos += RLENGTH
            }
            if (pos != end + 1) exit 1
            if (pairs != 4) exit 1
            if (!("streams" in seen) || !("chunks" in seen) || !("bytes" in seen) || !("entries" in seen)) exit 1
            if (found == "") exit 1
            print found
        }
    '
}

loki_count() {
    curl -fsS --connect-timeout 2 --max-time 5 -G "$LOKI_URL/loki/api/v1/query" \
        --data-urlencode 'query=sum(count_over_time({job=~".+"}[1h]))' \
        --data-urlencode "time=$(date +%s%N)" | json_count
}

wait_count() { # backend want timeout
    backend=$1 want=$2 timeout=${3:-180}
    start=$(now)
    deadline=$((start + timeout))
    while :; do
        case "$backend" in
            loki) got=$(loki_count 2>/dev/null || true) ;;
            *) echo "unknown backend $backend" >&2; return 1 ;;
        esac
        if [ "$got" = "$want" ]; then
            echo "$backend count: $got"
            return 0
        fi
        t=$(now)
        if [ "$t" -ge "$deadline" ]; then
            echo "$backend count is '$got', want $want after $((t - start))s" >&2
            return 1
        fi
        sleep 2
    done
}

# parkana_stats_entries asks Parkana's index/stats for the unique row count over
# the captured full generated range. The selector covers base, job and corpus.
parkana_stats_entries() { # start_ns end_ns
    curl -fsS --connect-timeout 2 --max-time 10 -G "$PARKANA_URL/loki/api/v1/index/stats" \
        --data-urlencode 'query={job=~".+"}' \
        --data-urlencode "start=$1" \
        --data-urlencode "end=$2"
}

wait_stats() { # want start_ns end_ns timeout
    want=$1 start_ns=$2 end_ns=$3 timeout=${4:-240}
    t0=$(now)
    deadline=$((t0 + timeout))
    while :; do
        body=$(parkana_stats_entries "$start_ns" "$end_ns" 2>/dev/null || true)
        got=$(printf '%s' "$body" | json_uint_field entries || true)
        if [ -n "$got" ] && [ "$got" = "$want" ]; then
            echo "Parkana index/stats entries: $got (unique rows, full range)"
            return 0
        fi
        t=$(now)
        if [ "$t" -ge "$deadline" ]; then
            if [ -z "$got" ]; then
                echo "Parkana index/stats response has no valid entries, want $want after $((t - t0))s: ${body:-<empty>}" >&2
            else
                echo "Parkana index/stats entries is '$got', want $want after $((t - t0))s" >&2
            fi
            return 1
        fi
        sleep 2
    done
}

wait_datasource() { # uid timeout
    uid=$1 timeout=${2:-180}
    wait_http "$GRAFANA_URL/api/datasources/uid/$uid" "Grafana datasource $uid" "$timeout"
}

parkana_volume() {
    vol=$(timeout 20 "$DOCKER" volume ls -q \
        --filter "label=com.docker.compose.project=$PROJECT" \
        --filter "label=com.docker.compose.volume=parkana-data" 2>/dev/null | head -n 1)
    [ -n "$vol" ] || vol="${PROJECT}_parkana-data"
    printf '%s\n' "$vol"
}

parkana_catalog_rows() {
    vol=$(parkana_volume)
    timeout 30 "$DOCKER" run --rm --user 0:0 \
        -v "$vol:/data" \
        --entrypoint sqlite3 parkana-pushgen:phase2 \
        -readonly -batch -noheader /data/catalog.db \
        'SELECT COALESCE(SUM(rows), 0) FROM files WHERE deleted_at IS NULL;' 2>/dev/null |
        tail -n 1
}

wait_catalog() { # want timeout; physical rows may exceed want via retry copies
    want=$1 timeout=${2:-240}
    t0=$(now)
    deadline=$((t0 + timeout))
    while :; do
        got=$(parkana_catalog_rows 2>/dev/null || true)
        case "$got" in
            ""|*[!0-9]*) ;;
            *)
                if [ "$got" -ge "$want" ]; then
                    excess=$((got - want))
                    echo "Parkana catalog rows: $got (expected $want)"
                    echo "retry duplicates stored: $excess"
                    return 0
                fi
                ;;
        esac
        t=$(now)
        if [ "$t" -ge "$deadline" ]; then
            echo "Parkana catalog rows are '$got', want at least $want after $((t - t0))s" >&2
            return 1
        fi
        sleep 2
    done
}

alloy_parkana_rejected() {
    compose logs --no-color alloy 2>/dev/null |
        grep -Eq 'parkana:3100.*status=400|unsupported content encoding "snappy"'
}

datasource_health() { # uid -> OK or error body
    curl -fsS --connect-timeout 2 --max-time 10 "$GRAFANA_URL/api/datasources/uid/$1/health" 2>/dev/null || true
}

selftest() {
    echo "selftest: expected-total derivation"
    for t in 1 22200 100000000; do
        is_positive_uint "$t" || { echo "selftest: valid total '$t' rejected" >&2; exit 1; }
    done
    for t in 0 00 00000 00008 01 abc -1 100000001 1234567890; do
        if is_positive_uint "$t"; then
            echo "selftest: invalid total '$t' accepted" >&2
            exit 1
        fi
    done
    for t in 1 600 86400; do
        is_ready_timeout "$t" || { echo "selftest: valid ready timeout '$t' rejected" >&2; exit 1; }
    done
    for t in 0 00 00000 00008 01 abc -1 86401 123456; do
        if is_ready_timeout "$t"; then
            echo "selftest: invalid ready timeout '$t' accepted" >&2
            exit 1
        fi
    done
    got=$(PARKANA_GENERATOR_BASE_TOTAL=5 PARKANA_GENERATOR_JOB_TOTAL=6 PARKANA_GENERATOR_CORPUS_TOTAL=7 resolve_expected)
    [ "$got" = 18 ] || { echo "selftest: expected total got '$got', want 18" >&2; exit 1; }
    got=$(PARKANA_GENERATOR_BASE_TOTAL=5 PARKANA_GENERATOR_JOB_TOTAL=6 PARKANA_GENERATOR_CORPUS_TOTAL=7 PARKANA_GENERATOR_EXPECTED_TOTAL=18 resolve_expected)
    [ "$got" = 18 ] || { echo "selftest: explicit matching total rejected" >&2; exit 1; }
    if (PARKANA_GENERATOR_BASE_TOTAL=5 PARKANA_GENERATOR_JOB_TOTAL=6 PARKANA_GENERATOR_CORPUS_TOTAL=7 PARKANA_GENERATOR_EXPECTED_TOTAL=19 resolve_expected >/dev/null 2>&1); then
        echo "selftest: conflicting explicit total was accepted" >&2
        exit 1
    fi
    if (PARKANA_GENERATOR_BASE_TOTAL=5x resolve_expected >/dev/null 2>&1); then
        echo "selftest: non-integer total was accepted" >&2
        exit 1
    fi
    if (PARKANA_GENERATOR_BASE_TOTAL=0 PARKANA_GENERATOR_JOB_TOTAL=1 PARKANA_GENERATOR_CORPUS_TOTAL=1 resolve_expected >/dev/null 2>&1); then
        echo "selftest: zero base total was accepted" >&2
        exit 1
    fi
    if (PARKANA_GENERATOR_BASE_TOTAL=1234567890 resolve_expected >/dev/null 2>&1); then
        echo "selftest: oversized total was accepted" >&2
        exit 1
    fi
    if (PARKANA_GENERATOR_BASE_TOTAL=100000000 PARKANA_GENERATOR_JOB_TOTAL=1 PARKANA_GENERATOR_CORPUS_TOTAL=1 resolve_expected >/dev/null 2>&1); then
        echo "selftest: overflowing sum was accepted" >&2
        exit 1
    fi

    echo "selftest: version parsing"
    check_plugin_version '{"info":{"version":"13.2.5"}}' 13.2.5 ||
        { echo "selftest: correct plugin version rejected" >&2; exit 1; }
    if (check_plugin_version '{"info":{"version":"13.2.4"}}' 13.2.5); then
        echo "selftest: wrong plugin version accepted" >&2
        exit 1
    fi

    echo "selftest: index/stats entries parser"
    got=$(printf '%s' '{"streams":2,"chunks":1,"bytes":10,"entries":7}' | json_uint_field entries)
    [ "$got" = 7 ] || { echo "selftest: valid entries count '$got' rejected" >&2; exit 1; }
    got=$(printf '%s' '{ "entries" : 7 , "bytes" : 10 , "streams" : 2 , "chunks" : 1 }' | json_uint_field entries)
    [ "$got" = 7 ] || { echo "selftest: reordered/spaced entries count '$got' rejected" >&2; exit 1; }
    got=$(printf '%s\n' '{"streams":2,' '"chunks":1,' '"bytes":10,' '"entries":7}' | json_uint_field entries)
    [ "$got" = 7 ] || { echo "selftest: multiline entries count '$got' rejected" >&2; exit 1; }
    got=$(printf '%s' '{"streams":0,"chunks":0,"bytes":0,"entries":0}' | json_uint_field entries)
    [ "$got" = 0 ] || { echo "selftest: zero entries count '$got' rejected" >&2; exit 1; }
    for bad in \
        '{"streams":2,"chunks":1,"bytes":10}' \
        '{"streams":2,"chunks":1,"bytes":10,"entries":-1}' \
        '{"streams":2,"chunks":1,"bytes":10,"entries":"7"}' \
        '{"streams":2,"chunks":1,"bytes":10,"entries":7x}' \
        '{"streams":2,"chunks":1,"bytes":10,"entries":7,"entries":7}' \
        '{"streams":2,"chunks":1,"bytes":10,"entries":22 200}' \
        '{"streams":2,"chunks":1,"bytes":10,"entries":022200}' \
        '{"streams":2,"chunks":1,"bytes":10,"entries":{"n":22200}}' \
        '{"streams":2,"chunks":1,"bytes":10,"entries":22200,"extra":1}' \
        '{"streams":2,"chunks":1,"bytes":10,"entries":22200,}' \
        '{"streams":2,"chunks":1,"bytes":10,"entries":22200}{"x":1}' \
        '{}' \
        'not-json'; do
        got=$(printf '%s' "$bad" | json_uint_field entries || true)
        if [ -n "$got" ]; then
            echo "selftest: invalid entries response accepted ($bad -> $got)" >&2
            exit 1
        fi
    done

    echo "selftest: bounded Grafana cold-start readiness"
    if (
        MOCK_READY_AFTER=4
        MOCK_CURL_COUNT="$TMP/curl-count-180"
        MOCK_TIME_FILE="$TMP/time-180"
        : >"$MOCK_CURL_COUNT"
        echo 0 >"$MOCK_TIME_FILE"
        now() {
            n=$(cat "$MOCK_TIME_FILE")
            n=$((n + 60))
            printf '%s\n' "$n" >"$MOCK_TIME_FILE"
            echo "$n"
        }
        sleep() { :; }
        curl() {
            n=$(cat "$MOCK_CURL_COUNT" 2>/dev/null || echo 0)
            n=$((n + 1))
            printf '%s\n' "$n" >"$MOCK_CURL_COUNT"
            [ "$n" -ge "$MOCK_READY_AFTER" ]
        }
        wait_http "http://mock/health" "mock" 180
    ); then
        echo "selftest: mock readiness accepted under the 180s budget" >&2
        exit 1
    fi
    (
        MOCK_READY_AFTER=4
        MOCK_CURL_COUNT="$TMP/curl-count-600"
        MOCK_TIME_FILE="$TMP/time-600"
        : >"$MOCK_CURL_COUNT"
        echo 0 >"$MOCK_TIME_FILE"
        now() {
            n=$(cat "$MOCK_TIME_FILE")
            n=$((n + 60))
            printf '%s\n' "$n" >"$MOCK_TIME_FILE"
            echo "$n"
        }
        sleep() { :; }
        curl() {
            n=$(cat "$MOCK_CURL_COUNT" 2>/dev/null || echo 0)
            n=$((n + 1))
            printf '%s\n' "$n" >"$MOCK_CURL_COUNT"
            [ "$n" -ge "$MOCK_READY_AFTER" ]
        }
        wait_http "http://mock/health" "mock" 600
    ) || {
        echo "selftest: mock readiness after >180s rejected under the 600s budget" >&2
        exit 1
    }

    echo "selftest: Loki effective-config checks"
    good_cfg='limits_config:
  allow_structured_metadata: true
  discover_log_levels: false
  discover_service_name: []'
    check_loki_config "$good_cfg" || { echo "selftest: good Loki config rejected" >&2; exit 1; }
    bad_cfg=$(printf '%s\n' "$good_cfg" | sed 's/discover_service_name: \[\]/discover_service_name: [job]/')
    if (check_loki_config "$bad_cfg"); then
        echo "selftest: config with service discovery enabled accepted" >&2
        exit 1
    fi

    echo "selftest: generator exit propagation"
    cat >"$TMP/docker" <<'MOCK'
#!/bin/sh
case "$1" in
    compose)
        printf '%s\n' "$*" >>"${MOCK_LOG:?MOCK_LOG not set}"
        ;;
    inspect)
        case "$3" in
            *ExitCode*) echo "${MOCK_EXIT:-0}" ;;
            *Status*) echo "${MOCK_STATE:-exited}" ;;
        esac
        ;;
esac
exit 0
MOCK
    chmod 0755 "$TMP/docker"
    DOCKER="$TMP/docker"
    compose() { echo mock-container; }
    export MOCK_STATE MOCK_EXIT MOCK_PROJECT_EXISTS
    MOCK_STATE=exited
    MOCK_EXIT=7
    if (wait_service_exit pushgen-base 0 5); then
        echo "selftest: non-zero generator exit accepted" >&2
        exit 1
    fi
    MOCK_EXIT=0
    wait_service_exit pushgen-base 0 5 || { echo "selftest: zero generator exit rejected" >&2; exit 1; }

    echo "selftest: existing-project refusal"
    compose_phase1() { [ "${MOCK_PROJECT_EXISTS:-0}" = 1 ] && echo mock-container; return 0; }
    MOCK_PROJECT_EXISTS=1
    if (ensure_project_available); then
        echo "selftest: existing project accepted" >&2
        exit 1
    fi
    MOCK_PROJECT_EXISTS=0
    ensure_project_available || { echo "selftest: fresh project rejected" >&2; exit 1; }

    echo "selftest: cleanup ownership (subprocess)"
    script=$SCRIPT_PATH
    cat >"$TMP/docker-sub" <<'MOCK'
#!/bin/sh
log="${MOCK_LOG:?MOCK_LOG not set}"
if [ "$1" = compose ]; then
    shift
    case " $* " in
        *" config --services "*)
            printf 'garage\ngarage-init\nparkana\n'
            exit 0
            ;;
        *" config "*)
            exit 0
            ;;
        *" ps -a -q "*)
            [ "${MOCK_PROJECT_EXISTS:-0}" = 1 ] && echo mock-container
            exit 0
            ;;
        *" ps "*)
            exit 0
            ;;
        *" build "*)
            exit 0
            ;;
        *" up -d "*)
            echo "mock: up failure" >&2
            exit 1
            ;;
        *" down "*)
            printf '%s\n' "$*" >>"$log"
            exit 0
            ;;
        *" logs "*)
            exit 0
            ;;
    esac
    exit 0
fi
case "$1" in
    ps|volume|network) exit 0 ;;
    inspect) echo exited ;;
esac
exit 0
MOCK
    chmod 0755 "$TMP/docker-sub"
    MOCK_LOG="$TMP/sub-down.log"
    MOCK_OUT="$TMP/sub-out.log"
    export MOCK_LOG MOCK_OUT
    export DOCKER="$TMP/docker-sub"
    export PARKANA_HTTP_PORT=23100 PARKANA_S3_PORT=23900
    export LOKI_HTTP_PORT=23101 ALLOY_HTTP_PORT=23245 GRAFANA_HTTP_PORT=23000

    : >"$MOCK_LOG"
    MOCK_PROJECT_EXISTS=1
    export MOCK_PROJECT_EXISTS
    if sh "$script" --project sub-existing --smoke >"$MOCK_OUT" 2>&1; then
        echo "selftest: existing project subprocess succeeded" >&2
        exit 1
    fi
    if [ -s "$MOCK_LOG" ]; then
        echo "selftest: preexisting project was brought down by subprocess: $(cat "$MOCK_LOG")" >&2
        exit 1
    fi

    echo "selftest: relative entrypoint"
    repo_root=$(cd ../.. && pwd)
    : >"$MOCK_LOG"
    MOCK_PROJECT_EXISTS=0
    if (cd "$repo_root" && sh deploy/compose/validate-phase2.sh --project rel-fresh --smoke >"$MOCK_OUT" 2>&1); then
        echo "selftest: relative-entrypoint fresh project subprocess succeeded" >&2
        exit 1
    fi
    grep -q -- "-p rel-fresh .*--profile phase2 down --volumes --remove-orphans" "$MOCK_LOG" ||
        { echo "selftest: relative entrypoint did not reach cleanup: $(cat "$MOCK_LOG")" >&2; exit 1; }

    : >"$MOCK_LOG"
    MOCK_PROJECT_EXISTS=0
    export MOCK_PROJECT_EXISTS
    if sh "$script" --project sub-own --smoke >"$MOCK_OUT" 2>&1; then
        echo "selftest: fresh project subprocess did not propagate up failure" >&2
        exit 1
    fi
    grep -q -- "-p sub-own .*--profile phase2 down --volumes --remove-orphans" "$MOCK_LOG" ||
        { echo "selftest: fresh project trap did not bring down its own project: $(cat "$MOCK_LOG")" >&2; exit 1; }
    if grep -q -- "-p sub-existing" "$MOCK_LOG"; then
        echo "selftest: fresh project trap touched another project" >&2
        exit 1
    fi

    echo "selftest: compare option validation"
    COMPARE_CORPUS=""
    COMPARE_TENANT=""
    COMPARE_TIMEOUT=""
    MODE=full
    validate_compare_args || { echo "selftest: no-corpus compare options rejected" >&2; exit 1; }
    MODE=smoke
    printf '[]\n' >"$TMP/selftest-corpus.json"
    if (COMPARE_CORPUS="$TMP/selftest-corpus.json"; validate_compare_args); then
        echo "selftest: --compare-corpus accepted with --smoke" >&2
        exit 1
    fi
    MODE=full
    if (COMPARE_TENANT=tenant-a; validate_compare_args); then
        echo "selftest: --compare-tenant without a corpus accepted" >&2
        exit 1
    fi
    if (COMPARE_CORPUS="$TMP/missing-corpus.json"; validate_compare_args); then
        echo "selftest: missing compare corpus accepted" >&2
        exit 1
    fi
    COMPARE_CORPUS="$TMP/selftest-corpus.json"
    validate_compare_args || { echo "selftest: readable compare corpus rejected" >&2; exit 1; }

    echo "selftest: empty --compare-corpus rejected by the parser (subprocess)"
    set +e
    DOCKER=false sh "$SCRIPT_PATH" --project empty-corpus --full \
        --compare-corpus "" >"$TMP/empty-corpus.out" 2>&1
    empty_status=$?
    set -e
    [ "$empty_status" -ne 0 ] ||
        { echo "selftest: explicit empty --compare-corpus accepted" >&2; exit 1; }
    grep -q -- '--compare-corpus needs a path' "$TMP/empty-corpus.out" ||
        { echo "selftest: empty --compare-corpus did not name the flag: $(cat "$TMP/empty-corpus.out")" >&2; exit 1; }

    echo "selftest: full subprocess paths (mock Docker/curl/go)"
    mock="$TMP/mock-full"
    mkdir -p "$mock/bin"
    cat >"$mock/bin/docker" <<'MOCK'
#!/bin/sh
log=${MOCK_DOCKER_LOG:?MOCK_DOCKER_LOG not set}
printf '%s\n' "$*" >>"$log"
if [ "$1" = compose ]; then
    shift
    last=""
    for a in "$@"; do last=$a; done
    events=${MOCK_EVENT_LOG:-/dev/null}
    case " $* " in
        *" up -d "*) printf 'UP\n' >>"$events" ;;
    esac
    case " $* " in
        *" config --services "*) printf 'garage\ngarage-init\nparkana\n'; exit 0 ;;
        *" config "*) exit 0 ;;
        *" ps -a -q "*)
            case "$last" in
                pushgen-*) printf 'GENERATOR %s\n' "$last" >>"$events" ;;
            esac
            if [ "$last" = "-q" ]; then
                [ "${MOCK_PROJECT_EXISTS:-0}" = 1 ] && echo mock-container
            else
                printf 'id-%s\n' "$last"
            fi
            exit 0
            ;;
        *" ps "*) exit 0 ;;
        *" build "*) exit 0 ;;
        *" up -d "*) exit 0 ;;
        *" restart "*) exit 0 ;;
        *" down "*)
            if [ "${MOCK_DOWN_EXIT:-0}" != 0 ]; then
                echo "mock: down failure" >&2
                exit "${MOCK_DOWN_EXIT}"
            fi
            exit 0
            ;;
        *" exec -T parkana "*) printf 'PARKANA_INGEST_UPLOAD_MAX_AGE=30s\n'; exit 0 ;;
        *" exec -T grafana "*)
            printf 'GF_AUTH_ANONYMOUS_ENABLED=true\nGF_PLUGINS_PREINSTALL_SYNC=loki@13.2.5@https://grafana.com/x\n'
            exit 0
            ;;
        *" logs "*)
            if [ -n "${MOCK_SECRET_LEAK:-}" ]; then
                echo "compose log $MOCK_SECRET_LEAK" >&2
            fi
            exit 0
            ;;
    esac
    exit 0
fi
case "$1" in
    inspect)
        case "$3" in
            *ExitCode*)
                case "$4" in
                    *pushgen-base*) echo "${MOCK_PUSHGEN_BASE_EXIT:-0}" ;;
                    *pushgen-job*) echo "${MOCK_PUSHGEN_JOB_EXIT:-0}" ;;
                    *pushgen-corpus*) echo "${MOCK_PUSHGEN_CORPUS_EXIT:-0}" ;;
                    *) echo 0 ;;
                esac
                ;;
            *Status*) echo "${MOCK_STATE:-exited}" ;;
        esac
        exit 0
        ;;
    ps|volume|network) exit 0 ;;
    run)
        case " $* " in
            *" sqlite3 "*) echo "${MOCK_CATALOG_ROWS:-22200}" ;;
        esac
        exit 0
        ;;
esac
exit 0
MOCK
    cat >"$mock/bin/curl" <<'MOCK'
#!/bin/sh
url=""
query=""
start=""
end=""
time=""
for a in "$@"; do
    case "$a" in
        http*) url=$a ;;
        query=*) query=${a#query=} ;;
        start=*) start=${a#start=} ;;
        end=*) end=${a#end=} ;;
        time=*) time=${a#time=} ;;
    esac
done
log=${MOCK_CURL_LOG:-/dev/null}
[ "${MOCK_CURL_FAIL:-0}" = 1 ] && exit 1
case "$url" in
    */ready) exit 0 ;;
    */loki/api/v1/status/buildinfo) printf '{"version":"3.7.9"}'; exit 0 ;;
    */api/health) printf '{"version":"13.3.0"}'; exit 0 ;;
    */api/plugins/loki/settings) printf '{"version":"13.2.5"}'; exit 0 ;;
    */api/datasources/uid/parkana/health)
        if [ -n "${MOCK_PARKANA_HEALTH:-}" ]; then
            printf '%s' "$MOCK_PARKANA_HEALTH"
        else
            printf '{"status":"OK"}'
        fi
        exit 0
        ;;
    */api/datasources/uid/*/health) printf '{"status":"OK"}'; exit 0 ;;
    */api/datasources/uid/*) exit 0 ;;
    */config) printf 'limits_config:\n  allow_structured_metadata: true\n  discover_log_levels: false\n  discover_service_name: []\n'; exit 0 ;;
    */loki/api/v1/index/stats)
        printf 'STATS url=%s query=%s start=%s end=%s\n' "$url" "$query" "$start" "$end" >>"$log"
        # Only answer a genuine full-range request: a wrong selector, a
        # missing/malformed bound, or a non-increasing range must not pass.
        if [ "$query" != '{job=~".+"}' ]; then
            printf '{"streams":1,"chunks":1,"bytes":1}'
            exit 0
        fi
        case "$start" in ""|*[!0-9]*) printf '{"streams":1,"chunks":1,"bytes":1}'; exit 0 ;; esac
        case "$end" in ""|*[!0-9]*) printf '{"streams":1,"chunks":1,"bytes":1}'; exit 0 ;; esac
        [ "$start" -lt "$end" ] || { printf '{"streams":1,"chunks":1,"bytes":1}'; exit 0; }
        case "${MOCK_STATS_INVALID:-}" in
            missing) printf '{"streams":1,"chunks":1,"bytes":1}'; exit 0 ;;
            text) printf '{"streams":1,"chunks":1,"bytes":1,"entries":"many"}'; exit 0 ;;
            negative) printf '{"streams":1,"chunks":1,"bytes":1,"entries":-1}'; exit 0 ;;
            ambiguous) printf '{"streams":1,"chunks":1,"bytes":1,"entries":22200,"entries":22200}'; exit 0 ;;
            suffix) printf '{"streams":1,"chunks":1,"bytes":1,"entries":22200x}'; exit 0 ;;
            space) printf '{"streams":1,"chunks":1,"bytes":1,"entries":22 200}'; exit 0 ;;
            leading) printf '{"streams":1,"chunks":1,"bytes":1,"entries":022200}'; exit 0 ;;
            nested) printf '{"streams":1,"chunks":1,"bytes":1,"entries":{"n":22200}}'; exit 0 ;;
            extra) printf '{"streams":1,"chunks":1,"bytes":1,"entries":22200,"extra":1}'; exit 0 ;;
            trailing) printf '{"streams":1,"chunks":1,"bytes":1,"entries":22200,}'; exit 0 ;;
            empty) printf '{}'; exit 0 ;;
            junk) printf 'not-json'; exit 0 ;;
        esac
        printf '{"streams":1,"chunks":1,"bytes":1,"entries":%s}' "${MOCK_STATS_ENTRIES:-22200}"
        exit 0
        ;;
    */loki/api/v1/query)
        printf 'QUERY url=%s time=%s\n' "$url" "$time" >>"$log"
        printf '{"data":{"resultType":"vector","result":[{"metric":{},"value":[0,"%s"]}]}}' "${MOCK_TOTAL:-22200}"
        exit 0
        ;;
esac
exit 0
MOCK
    cli_template="$mock/cli-template"
    cat >"$cli_template" <<'MOCK'
#!/bin/sh
printf 'CLI\n' >>"${MOCK_GO_LOG:?MOCK_GO_LOG not set}"
printf 'CLI\n' >>"${MOCK_DOCKER_LOG:?MOCK_DOCKER_LOG not set}"
printf 'ARGC %s\n' "$#" >>"${MOCK_GO_LOG:?MOCK_GO_LOG not set}"
i=0
for arg in "$@"; do
    printf 'ARGV %s %s\n' "$i" "$arg" >>"${MOCK_GO_LOG:?MOCK_GO_LOG not set}"
    i=$((i + 1))
done
exit "${MOCK_CLI_EXIT:-0}"
MOCK
    chmod 0755 "$cli_template"
    cat >"$mock/bin/go" <<'MOCK'
#!/bin/sh
if [ "${MOCK_BUILD_EXIT:-0}" != 0 ]; then
    exit "${MOCK_BUILD_EXIT}"
fi
out=""
prev=""
for arg in "$@"; do
    if [ "$prev" = "-o" ]; then out=$arg; fi
    prev=$arg
done
case " $* " in
    *" build "*" ./cmd/lokicompare "*)
        [ -n "$out" ] || exit 2
        cp "${MOCK_CLI_TEMPLATE:?MOCK_CLI_TEMPLATE not set}" "$out" || exit 2
        chmod 0755 "$out" || exit 2
        printf 'GOBUILD %s\n' "$out" >>"${MOCK_GO_LOG:?MOCK_GO_LOG not set}"
        exit 0
        ;;
esac
echo "unexpected go invocation: $*" >&2
exit 2
MOCK
    chmod 0755 "$mock/bin/docker" "$mock/bin/curl" "$mock/bin/go"

    time_bin="$mock/time-bin"
    mkdir -p "$time_bin"
    cat >"$time_bin/date" <<'MOCK'
#!/bin/sh
case "$1" in
    +%s)
        n=$(cat "${MOCK_DATE_FILE:?MOCK_DATE_FILE not set}" 2>/dev/null || echo 0)
        n=$((n + 1000))
        printf '%s\n' "$n" >"$MOCK_DATE_FILE"
        printf 'DATE_S %s\n' "$n" >>"${MOCK_EVENT_LOG:-/dev/null}"
        echo "$n"
        ;;
    +%s%N)
        n=$(cat "${MOCK_DATE_FILE:?MOCK_DATE_FILE not set}" 2>/dev/null || echo 0)
        n=$((n + 1000))
        printf '%s\n' "$n" >"$MOCK_DATE_FILE"
        printf 'DATE_NS %s\n' "${n}000000000" >>"${MOCK_EVENT_LOG:-/dev/null}"
        echo "${n}000000000"
        ;;
    *) exec /bin/date "$@" ;;
esac
MOCK
    chmod 0755 "$time_bin/date"

    export DOCKER="$mock/bin/docker"
    export MOCK_DOCKER_LOG="$TMP/mock-docker.log"
    export MOCK_GO_LOG="$TMP/mock-go.log"
    export MOCK_CURL_LOG="$TMP/mock-curl.log"
    export MOCK_EVENT_LOG="$TMP/mock-events.log"
    export MOCK_PROJECT_EXISTS=0
    export MOCK_CLI_EXIT=0
    export MOCK_BUILD_EXIT=0
    export MOCK_CLI_TEMPLATE="$cli_template"
    export MOCK_TOTAL=22200
    export MOCK_STATS_ENTRIES=22200
    export MOCK_DATE_FILE="$TMP/mock-date"
    export PARKANA_HTTP_PORT=23100 PARKANA_S3_PORT=23900
    export LOKI_HTTP_PORT=23101 ALLOY_HTTP_PORT=23245 GRAFANA_HTTP_PORT=23000
    export PARKANA_GENERATOR_BASE_TOTAL=20000
    export PARKANA_GENERATOR_JOB_TOTAL=1000
    export PARKANA_GENERATOR_CORPUS_TOTAL=1200
    unset PARKANA_GENERATOR_EXPECTED_TOTAL MOCK_CURL_FAIL MOCK_PARKANA_HEALTH \
        MOCK_CATALOG_ROWS MOCK_PUSHGEN_BASE_EXIT MOCK_SECRET_LEAK MOCK_DOWN_EXIT \
        MOCK_STATS_INVALID
    selftest_secret="PARKANA_SELFTEST_SECRET_0123456789abcdef"
    export PARKANA_DEV_GARAGE_KEY_SECRET="$selftest_secret"
    corpus_dir="$TMP/selftest corpus dir"
    mkdir -p "$corpus_dir"
    corpus="$corpus_dir/a corpus.json"
    printf '[]\n' >"$corpus"
    PATH="$mock/bin:$PATH"
    export PATH

    run_mock_validator() { # label want args...
        run_label=$1
        run_want=$2
        shift 2
        : >"$MOCK_DOCKER_LOG"
        : >"$MOCK_GO_LOG"
        : >"$MOCK_CURL_LOG"
        : >"$MOCK_EVENT_LOG"
        set +e
        sh "$SCRIPT_PATH" "$@" >"$TMP/mock.out" 2>&1
        run_got=$?
        set -e
        if [ "$run_got" != "$run_want" ]; then
            echo "selftest: $run_label exited $run_got, want $run_want" >&2
            cat "$TMP/mock.out" >&2
            exit 1
        fi
    }

    grep_down() { # project
        grep -q -- "-p $1 .*--profile phase2 down --volumes --remove-orphans" "$MOCK_DOCKER_LOG"
    }
    grep_no_foreign_down() {
        ! grep -q -- "--rmi\|prune\| system " "$MOCK_DOCKER_LOG"
    }

    echo "selftest: comparison stays opt-in (subprocess)"
    MOCK_CLI_EXIT=0
    export MOCK_CLI_EXIT
    run_mock_validator "full without compare" 0 --project optin-full --full
    grep_down optin-full ||
        { echo "selftest: full without compare did not clean its project" >&2; exit 1; }
    if grep -q . "$MOCK_GO_LOG"; then
        echo "selftest: full without --compare-corpus invoked go: $(cat "$MOCK_GO_LOG")" >&2
        exit 1
    fi
    if grep -q 'corpus comparison' "$TMP/mock.out"; then
        echo "selftest: full without --compare-corpus ran a comparison" >&2
        exit 1
    fi

    echo "selftest: compiled comparator equal, argv boundaries and shared range (subprocess)"
    MOCK_CLI_EXIT=0
    export MOCK_CLI_EXIT
    export PATH="$time_bin:$PATH"
    : >"$MOCK_DATE_FILE"
    run_mock_validator "comparator equal" 0 --project cmp-zero --full --compare-corpus "$corpus" \
        --compare-tenant tenant-a --compare-timeout 30s
    grep_down cmp-zero || { echo "selftest: equal compare did not clean its project" >&2; exit 1; }
    grep_no_foreign_down || { echo "selftest: equal compare used a foreign cleanup command" >&2; exit 1; }
    [ "$(grep -c '^GOBUILD ' "$MOCK_GO_LOG" || true)" -eq 1 ] ||
        { echo "selftest: lokicompare was not built exactly once" >&2; exit 1; }
    [ "$(grep -c '^CLI$' "$MOCK_GO_LOG" || true)" -eq 1 ] ||
        { echo "selftest: compiled comparator ran more than once" >&2; exit 1; }
    grep -Fxq 'ARGC 14' "$MOCK_GO_LOG" ||
        { echo "selftest: comparator argc is wrong (argv flattened?)" >&2; cat "$MOCK_GO_LOG" >&2; exit 1; }
    grep -Fxq 'ARGV 0 -left' "$MOCK_GO_LOG" || { echo "selftest: comparator -left boundary wrong" >&2; exit 1; }
    grep -Fxq 'ARGV 1 http://127.0.0.1:23101' "$MOCK_GO_LOG" ||
        { echo "selftest: comparator left URL boundary wrong" >&2; exit 1; }
    grep -Fxq 'ARGV 2 -right' "$MOCK_GO_LOG" || { echo "selftest: comparator -right boundary wrong" >&2; exit 1; }
    grep -Fxq 'ARGV 3 http://127.0.0.1:23100' "$MOCK_GO_LOG" ||
        { echo "selftest: comparator right URL boundary wrong" >&2; exit 1; }
    grep -Fxq 'ARGV 4 -corpus' "$MOCK_GO_LOG" || { echo "selftest: comparator -corpus boundary wrong" >&2; exit 1; }
    grep -Fxq "ARGV 5 $corpus" "$MOCK_GO_LOG" ||
        { echo "selftest: comparator corpus with spaces was split: $(cat "$MOCK_GO_LOG")" >&2; exit 1; }
    grep -Fxq 'ARGV 6 -start' "$MOCK_GO_LOG" || { echo "selftest: comparator -start boundary wrong" >&2; exit 1; }
    grep -Fxq 'ARGV 8 -end' "$MOCK_GO_LOG" || { echo "selftest: comparator -end boundary wrong" >&2; exit 1; }
    grep -Fxq 'ARGV 10 -tenant' "$MOCK_GO_LOG" || { echo "selftest: comparator -tenant boundary wrong" >&2; exit 1; }
    grep -Fxq 'ARGV 11 tenant-a' "$MOCK_GO_LOG" || { echo "selftest: comparator tenant boundary wrong" >&2; exit 1; }
    grep -Fxq 'ARGV 12 -timeout' "$MOCK_GO_LOG" || { echo "selftest: comparator -timeout boundary wrong" >&2; exit 1; }
    grep -Fxq 'ARGV 13 30s' "$MOCK_GO_LOG" || { echo "selftest: comparator timeout boundary wrong" >&2; exit 1; }
    start_ns=$(sed -n 's/^ARGV 7 //p' "$MOCK_GO_LOG")
    end_ns=$(sed -n 's/^ARGV 9 //p' "$MOCK_GO_LOG")
    [ -n "$start_ns" ] && [ -n "$end_ns" ] && [ "$end_ns" -gt "$start_ns" ] ||
        { echo "selftest: compare range missing/not increasing: $start_ns..$end_ns" >&2; exit 1; }
    cmp_date_line=$(grep -n '^DATE_NS ' "$MOCK_EVENT_LOG" | head -n 1 | cut -d: -f1)
    cmp_up_line=$(grep -n '^UP$' "$MOCK_EVENT_LOG" | head -n 1 | cut -d: -f1)
    cmp_gen_line=$(grep -n '^GENERATOR ' "$MOCK_EVENT_LOG" | head -n 1 | cut -d: -f1)
    cmp_first_date=$(sed -n 's/^DATE_NS //p' "$MOCK_EVENT_LOG" | head -n 1)
    [ -n "$cmp_date_line" ] && [ -n "$cmp_up_line" ] && [ -n "$cmp_gen_line" ] ||
        { echo "selftest: comparator range/up/generator events missing" >&2; cat "$MOCK_EVENT_LOG" >&2; exit 1; }
    [ "$cmp_date_line" -lt "$cmp_up_line" ] && [ "$cmp_date_line" -lt "$cmp_gen_line" ] ||
        { echo "selftest: comparator START was not captured before up/generators" >&2; exit 1; }
    [ "$start_ns" = "$cmp_first_date" ] ||
        { echo "selftest: comparator start $start_ns is not the pre-up range start $cmp_first_date" >&2; exit 1; }
    awk '/restart / { restart = NR } /^CLI$/ { cli = NR } END { exit !(restart && cli && restart < cli) }' \
        "$MOCK_DOCKER_LOG" ||
        { echo "selftest: compare did not run after restart persistence" >&2; exit 1; }

    for want in 1 2; do
        MOCK_CLI_EXIT=$want
        export MOCK_CLI_EXIT
        run_mock_validator "compiled comparator exit $want" "$want" \
            --project "cmp-$want" --full --compare-corpus "$corpus"
        grep_down "cmp-$want" ||
            { echo "selftest: comparator exit $want did not trigger the owned cleanup" >&2; exit 1; }
        if [ "$want" = 1 ]; then
            grep -q 'semantic difference' "$TMP/mock.out" ||
                { echo "selftest: comparator exit 1 not reported as a difference" >&2; exit 1; }
        else
            grep -q 'request/response failure' "$TMP/mock.out" ||
                { echo "selftest: comparator exit 2 not reported as a failure" >&2; exit 1; }
        fi
    done
    MOCK_CLI_EXIT=0
    export MOCK_CLI_EXIT
    export PATH="${PATH#"$time_bin:"}"

    echo "selftest: comparator build failure before compose startup (subprocess)"
    MOCK_BUILD_EXIT=1
    export MOCK_BUILD_EXIT
    run_mock_validator "build failure" 1 --project build-fail --full --compare-corpus "$corpus"
    grep -q 'go build ./cmd/lokicompare failed' "$TMP/mock.out" ||
        { echo "selftest: build failure was not reported" >&2; exit 1; }
    if grep -Eq -- ' build | up -d | down ' "$MOCK_DOCKER_LOG"; then
        echo "selftest: build failure reached compose startup" >&2
        exit 1
    fi
    if grep -q -- ' logs ' "$MOCK_DOCKER_LOG"; then
        echo "selftest: pre-start build failure read foreign compose logs" >&2
        exit 1
    fi
    MOCK_BUILD_EXIT=0
    export MOCK_BUILD_EXIT

    echo "selftest: cleanup failure after comparator success becomes 1 (subprocess)"
    MOCK_DOWN_EXIT=1
    export MOCK_DOWN_EXIT
    run_mock_validator "comparator 0 + down failure" 1 \
        --project cmp-downfail0 --full --compare-corpus "$corpus"
    grep -q 'cleanup failed' "$TMP/mock.out" ||
        { echo "selftest: cleanup failure after success was not reported" >&2; exit 1; }
    grep_down cmp-downfail0 ||
        { echo "selftest: cleanup failure test did not attempt the owned down" >&2; exit 1; }

    echo "selftest: earlier comparator failure survives cleanup failure (subprocess)"
    MOCK_CLI_EXIT=2
    export MOCK_CLI_EXIT
    run_mock_validator "comparator 2 + down failure" 2 \
        --project cmp-downfail2 --full --compare-corpus "$corpus"
    grep -q 'request/response failure' "$TMP/mock.out" ||
        { echo "selftest: comparator failure was lost when cleanup also failed" >&2; exit 1; }
    grep -q 'cleanup failed' "$TMP/mock.out" ||
        { echo "selftest: cleanup failure after comparator failure was not reported" >&2; exit 1; }
    MOCK_DOWN_EXIT=0
    MOCK_CLI_EXIT=0
    export MOCK_DOWN_EXIT MOCK_CLI_EXIT

    echo "selftest: relative corpus resolves from caller cwd (subprocess)"
    : >"$MOCK_DOCKER_LOG"
    : >"$MOCK_GO_LOG"
    set +e
    (cd "$repo_root" && sh deploy/compose/validate-phase2.sh --project rel-corpus --full \
        --compare-corpus deploy/compose/e2e.md) >"$TMP/mock.out" 2>&1
    rel_corpus_status=$?
    set -e
    [ "$rel_corpus_status" = 0 ] ||
        { echo "selftest: relative corpus run exited $rel_corpus_status" >&2; cat "$TMP/mock.out" >&2; exit 1; }
    grep_down rel-corpus ||
        { echo "selftest: relative corpus run did not clean its project" >&2; exit 1; }
    grep -Fxq "ARGV 5 $repo_root/deploy/compose/e2e.md" "$MOCK_GO_LOG" ||
        { echo "selftest: relative corpus did not resolve from the caller cwd: $(cat "$MOCK_GO_LOG")" >&2; exit 1; }

    echo "selftest: generator failure (subprocess)"
    MOCK_PUSHGEN_BASE_EXIT=7
    export MOCK_PUSHGEN_BASE_EXIT
    run_mock_validator "generator failure" 1 \
        --project gen-fail --full --compare-corpus "$corpus"
    grep -q 'pushgen-base exited 7' "$TMP/mock.out" ||
        { echo "selftest: generator exit was not reported" >&2; exit 1; }
    grep_down gen-fail ||
        { echo "selftest: generator failure did not trigger the owned cleanup" >&2; exit 1; }
    if grep -q '^CLI$' "$MOCK_GO_LOG"; then
        echo "selftest: compare ran after a generator failure" >&2
        exit 1
    fi
    unset MOCK_PUSHGEN_BASE_EXIT

    echo "selftest: unhealthy datasource (subprocess)"
    MOCK_PARKANA_HEALTH='{"status":"ERROR"}'
    export MOCK_PARKANA_HEALTH
    run_mock_validator "unhealthy datasource" 1 \
        --project bad-ds --full --compare-corpus "$corpus"
    grep -q 'Parkana datasource health' "$TMP/mock.out" ||
        { echo "selftest: unhealthy datasource was not reported" >&2; exit 1; }
    grep_down bad-ds ||
        { echo "selftest: unhealthy datasource did not trigger the owned cleanup" >&2; exit 1; }
    unset MOCK_PARKANA_HEALTH

    echo "selftest: readiness timeout (subprocess)"
    export MOCK_CURL_FAIL=1
    export PATH="$time_bin:$PATH"
    : >"$MOCK_DATE_FILE"
    run_mock_validator "readiness timeout" 1 --project timeout --smoke
    grep -q 'not ready' "$TMP/mock.out" ||
        { echo "selftest: readiness timeout was not reported" >&2; exit 1; }
    grep_down timeout ||
        { echo "selftest: readiness timeout did not trigger the owned cleanup" >&2; exit 1; }
    unset MOCK_CURL_FAIL
    export PATH="${PATH#"$time_bin:"}"

    echo "selftest: partial upload (subprocess, diagnostics redacted)"
    MOCK_CATALOG_ROWS=1
    MOCK_SECRET_LEAK="$selftest_secret"
    export MOCK_CATALOG_ROWS MOCK_SECRET_LEAK
    export PATH="$time_bin:$PATH"
    : >"$MOCK_DATE_FILE"
    run_mock_validator "partial upload" 1 \
        --project partial --full --compare-corpus "$corpus"
    grep -q 'Parkana catalog rows are' "$TMP/mock.out" ||
        { echo "selftest: partial upload was not reported" >&2; exit 1; }
    if grep -Fq -- "$selftest_secret" "$TMP/mock.out"; then
        echo "selftest: diagnostics leaked the Garage secret" >&2
        exit 1
    fi
    grep_down partial ||
        { echo "selftest: partial upload did not trigger the owned cleanup" >&2; exit 1; }
    unset MOCK_CATALOG_ROWS MOCK_SECRET_LEAK
    export PATH="${PATH#"$time_bin:"}"

    echo "selftest: retry duplicates stored and unique stats (subprocess)"
    export PATH="$time_bin:$PATH"
    : >"$MOCK_DATE_FILE"
    MOCK_CATALOG_ROWS=22300
    MOCK_STATS_ENTRIES=22200
    MOCK_TOTAL=22200
    export MOCK_CATALOG_ROWS MOCK_STATS_ENTRIES MOCK_TOTAL
    run_mock_validator "duplicate rows accepted" 0 --project dup-ok --full
    grep_down dup-ok ||
        { echo "selftest: duplicate-row run did not clean its project" >&2; exit 1; }
    [ "$(grep -c 'retry duplicates stored: 100' "$TMP/mock.out" || true)" -eq 2 ] ||
        { echo "selftest: excess was not reported once per readiness pass" >&2; cat "$TMP/mock.out" >&2; exit 1; }
    grep -q 'Parkana index/stats entries: 22200' "$TMP/mock.out" ||
        { echo "selftest: unique stats count was not reported" >&2; cat "$TMP/mock.out" >&2; exit 1; }
    [ "$(grep -c '^STATS ' "$MOCK_CURL_LOG" || true)" -eq 2 ] ||
        { echo "selftest: full mode did not query unique stats before and after restart" >&2; cat "$MOCK_CURL_LOG" >&2; exit 1; }
    stats_first=$(grep '^STATS ' "$MOCK_CURL_LOG" | head -n 1)
    stats_last=$(grep '^STATS ' "$MOCK_CURL_LOG" | tail -n 1)
    [ "$stats_first" = "$stats_last" ] ||
        { echo "selftest: restart did not reuse the generated range: $stats_first vs $stats_last" >&2; exit 1; }
    stats_query=$(printf '%s\n' "$stats_first" | sed -n 's/^STATS url=[^ ]* query=\([^ ]*\) start=.*/\1/p')
    stats_start=$(printf '%s\n' "$stats_first" | sed -n 's/^STATS .* start=\([0-9][0-9]*\) end=.*/\1/p')
    stats_end=$(printf '%s\n' "$stats_first" | sed -n 's/^STATS .* end=\([0-9][0-9]*\)$/\1/p')
    case "$stats_first" in
        "STATS url=http://127.0.0.1:23100/loki/api/v1/index/stats "*) ;;
        *) echo "selftest: unique stats did not use Parkana index/stats: $stats_first" >&2; exit 1 ;;
    esac
    [ "$stats_query" = '{job=~".+"}' ] ||
        { echo "selftest: unique stats selector is '$stats_query'" >&2; exit 1; }
    [ -n "$stats_start" ] && [ -n "$stats_end" ] && [ "$stats_start" -lt "$stats_end" ] ||
        { echo "selftest: unique stats range is missing or not increasing: $stats_start..$stats_end" >&2; exit 1; }
    loki_time=$(grep '^QUERY ' "$MOCK_CURL_LOG" | head -n 1 | sed -n 's/.* time=\([0-9][0-9]*\)$/\1/p')
    [ -n "$loki_time" ] && [ "$stats_start" -le "$loki_time" ] && [ "$stats_end" -ge "$loki_time" ] ||
        { echo "selftest: unique stats range does not span the count barrier: $stats_start..$stats_end vs $loki_time" >&2; exit 1; }
    event_date_line=$(grep -n '^DATE_NS ' "$MOCK_EVENT_LOG" | head -n 1 | cut -d: -f1)
    event_up_line=$(grep -n '^UP$' "$MOCK_EVENT_LOG" | head -n 1 | cut -d: -f1)
    event_gen_line=$(grep -n '^GENERATOR ' "$MOCK_EVENT_LOG" | head -n 1 | cut -d: -f1)
    [ -n "$event_date_line" ] && [ -n "$event_up_line" ] && [ -n "$event_gen_line" ] ||
        { echo "selftest: range/up/generator events missing" >&2; cat "$MOCK_EVENT_LOG" >&2; exit 1; }
    [ "$event_date_line" -lt "$event_up_line" ] && [ "$event_date_line" -lt "$event_gen_line" ] ||
        { echo "selftest: generated range START was not captured before up/generators" >&2; cat "$MOCK_EVENT_LOG" >&2; exit 1; }
    first_date_ns=$(sed -n 's/^DATE_NS //p' "$MOCK_EVENT_LOG" | head -n 1)
    [ "$stats_start" = "$first_date_ns" ] ||
        { echo "selftest: unique stats start $stats_start is not the pre-up range start $first_date_ns" >&2; exit 1; }

    echo "selftest: unique stats below/above expected fails (subprocess)"
    MOCK_CATALOG_ROWS=22300
    MOCK_TOTAL=22200
    export MOCK_CATALOG_ROWS MOCK_TOTAL
    for n in 22199 22201; do
        MOCK_STATS_ENTRIES=$n
        export MOCK_STATS_ENTRIES
        : >"$MOCK_DATE_FILE"
        run_mock_validator "unique stats $n" 1 --project "stats-$n" --full
        grep -q "Parkana index/stats entries is '$n', want 22200" "$TMP/mock.out" ||
            { echo "selftest: unique stats $n was not reported" >&2; cat "$TMP/mock.out" >&2; exit 1; }
        grep_down "stats-$n" ||
            { echo "selftest: unique stats $n did not trigger the owned cleanup" >&2; exit 1; }
    done

    echo "selftest: invalid or ambiguous stats entries never pass (subprocess)"
    for mode in missing text negative ambiguous suffix space leading nested extra trailing empty junk; do
        MOCK_STATS_INVALID=$mode
        export MOCK_STATS_INVALID
        : >"$MOCK_DATE_FILE"
        run_mock_validator "stats invalid $mode" 1 --project "stats-bad-$mode" --full
        grep -q 'Parkana index/stats response has no valid entries' "$TMP/mock.out" ||
            { echo "selftest: invalid stats '$mode' was not reported" >&2; cat "$TMP/mock.out" >&2; exit 1; }
        grep_down "stats-bad-$mode" ||
            { echo "selftest: invalid stats '$mode' did not trigger the owned cleanup" >&2; exit 1; }
    done
    unset MOCK_STATS_INVALID

    echo "selftest: wrong Loki count fails the same readiness path (subprocess)"
    MOCK_TOTAL=22199
    export MOCK_TOTAL
    : >"$MOCK_DATE_FILE"
    run_mock_validator "loki count wrong" 1 --project loki-wrong --full
    grep -q "loki count is '22199', want 22200" "$TMP/mock.out" ||
        { echo "selftest: wrong Loki count was not reported" >&2; cat "$TMP/mock.out" >&2; exit 1; }
    if grep -q '^STATS ' "$MOCK_CURL_LOG"; then
        echo "selftest: unique stats ran after the Loki count barrier failed" >&2
        exit 1
    fi
    grep_down loki-wrong ||
        { echo "selftest: wrong Loki count did not trigger the owned cleanup" >&2; exit 1; }

    echo "selftest: smoke also requires unique stats (subprocess)"
    MOCK_STATS_INVALID=missing
    MOCK_STATS_ENTRIES=22200
    MOCK_TOTAL=22200
    MOCK_CATALOG_ROWS=22200
    export MOCK_STATS_INVALID MOCK_STATS_ENTRIES MOCK_TOTAL MOCK_CATALOG_ROWS
    : >"$MOCK_DATE_FILE"
    run_mock_validator "smoke stats missing" 1 --project smoke-stats --smoke
    grep -q 'Parkana index/stats response has no valid entries' "$TMP/mock.out" ||
        { echo "selftest: smoke did not require valid unique stats" >&2; cat "$TMP/mock.out" >&2; exit 1; }
    grep_down smoke-stats ||
        { echo "selftest: smoke stats failure did not trigger the owned cleanup" >&2; exit 1; }
    unset MOCK_STATS_INVALID MOCK_CATALOG_ROWS
    MOCK_STATS_ENTRIES=22200
    MOCK_TOTAL=22200
    export MOCK_STATS_ENTRIES MOCK_TOTAL
    export PATH="${PATH#"$time_bin:"}"

    echo "selftest: refused existing project with compare (subprocess)"
    export MOCK_PROJECT_EXISTS=1
    run_mock_validator "existing project" 1 \
        --project existing-cmp --full --compare-corpus "$corpus"
    if grep -q -- ' down ' "$MOCK_DOCKER_LOG"; then
        echo "selftest: refused existing project was brought down" >&2
        exit 1
    fi
    if grep -q -- ' logs ' "$MOCK_DOCKER_LOG"; then
        echo "selftest: refused existing project read foreign compose logs" >&2
        exit 1
    fi
    if grep -q '^CLI$' "$MOCK_GO_LOG"; then
        echo "selftest: compare ran for a refused existing project" >&2
        exit 1
    fi
    export MOCK_PROJECT_EXISTS=0

    echo "selftest: relative entrypoint with compare (subprocess)"
    export PATH="$mock/bin:$PATH"
    set +e
    (cd "$repo_root" && sh deploy/compose/validate-phase2.sh --project rel-cmp --full \
        --compare-corpus "$corpus") >"$TMP/mock.out" 2>&1
    rel_status=$?
    set -e
    [ "$rel_status" = 0 ] ||
        { echo "selftest: relative entrypoint compare exited $rel_status" >&2; cat "$TMP/mock.out" >&2; exit 1; }
    grep_down rel-cmp ||
        { echo "selftest: relative entrypoint compare did not clean its project" >&2; exit 1; }
    grep -q '^CLI$' "$MOCK_GO_LOG" ||
        { echo "selftest: relative entrypoint never ran the comparator" >&2; exit 1; }

    rm -rf "$TMP"
    echo "selftest: PASS"
}

if [ "$SELFTEST" = 1 ]; then
    if [ -n "$COMPARE_CORPUS$COMPARE_TENANT$COMPARE_TIMEOUT" ]; then
        echo "--compare-corpus/--compare-tenant/--compare-timeout are not valid with --selftest" >&2
        exit 2
    fi
    selftest
    exit 0
fi

case "$PROJECT" in
    "") PROJECT="parkana-p2-$(date +%s)-$$" ;;
    *[!a-z0-9_-]*|"")
        echo "invalid compose project name '$PROJECT' (want [a-z0-9][a-z0-9_-]*)" >&2
        exit 2
        ;;
esac
case "$PROJECT" in
    [a-z0-9]*) ;;
    *) echo "invalid compose project name '$PROJECT'" >&2; exit 2 ;;
esac

if ! validate_compare_args; then
    exit 2
fi

export PARKANA_HTTP_PORT="${PARKANA_HTTP_PORT:-13100}"
export PARKANA_S3_PORT="${PARKANA_S3_PORT:-13900}"
export LOKI_HTTP_PORT="${LOKI_HTTP_PORT:-13101}"
export ALLOY_HTTP_PORT="${ALLOY_HTTP_PORT:-13245}"
export GRAFANA_HTTP_PORT="${GRAFANA_HTTP_PORT:-13000}"
export PARKANA_DEV_GARAGE_KEY_ID="${PARKANA_DEV_GARAGE_KEY_ID:-GK5c7e9a3b1d8f2e6c4a0b9d8f}"
export PARKANA_DEV_GARAGE_KEY_SECRET="${PARKANA_DEV_GARAGE_KEY_SECRET:-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef}"
# Keep the compactor out of the exact-count window: its old+new overlap would
# make the catalog row sum temporarily exceed the generated corpus and be
# indistinguishable from retry duplicates.
export PARKANA_COMPACTOR_INTERVAL="${PARKANA_COMPACTOR_INTERVAL:-1h}"
export PARKANA_COMPACTOR_TARGET_SIZE="${PARKANA_COMPACTOR_TARGET_SIZE:-1B}"

if ! EXPECTED=$(resolve_expected); then
    fail "generator totals are inconsistent"
fi
GRAFANA_READY_TIMEOUT="${PARKANA_VALIDATE_GRAFANA_READY_TIMEOUT:-600}"
is_ready_timeout "$GRAFANA_READY_TIMEOUT" ||
    fail "PARKANA_VALIDATE_GRAFANA_READY_TIMEOUT must be a positive integer <= ${MAX_READY_TIMEOUT}, got '$GRAFANA_READY_TIMEOUT'"

PARKANA_URL="http://127.0.0.1:${PARKANA_HTTP_PORT}"
LOKI_URL="http://127.0.0.1:${LOKI_HTTP_PORT}"
ALLOY_URL="http://127.0.0.1:${ALLOY_HTTP_PORT}"
GRAFANA_URL="http://127.0.0.1:${GRAFANA_HTTP_PORT}"
created=0

trap cleanup EXIT INT TERM

# Build the frozen comparison binary before any compose build/up. A build
# failure therefore cannot leave a half-started stack behind. The binary lives
# in TMP and is removed by the existing cleanup trap.
build_corpus_compare

say "static checks"
compose_phase1 config >/dev/null
compose config >/dev/null
phase1_services=$(compose_phase1 config --services | sort | tr '\n' ' ')
[ "$phase1_services" = "garage garage-init parkana " ] ||
    fail "default compose services changed: '$phase1_services' (want garage garage-init parkana)"
for pin in \
    'golang:1.27.3-bookworm@sha256:d9b6a1beb9474adc2a1ce842573639b44b2a0dd49b6669f51d3821baf9ec37c9' \
    'debian:bookworm-slim@sha256:99c0b25c46d0214d5a2091092e7908907dcaecd6a962cde0ac700acee222b6f3' \
    'grafana/loki:3.7.9@sha256:0db216b630ae1f348a1584c67f3cead8b4c1fc839169a39501db664744158e28' \
    'grafana/alloy:v1.21.3@sha256:8ab871206bd2d8f89277f53abffee5f452a293980788d52f85fa183a87a505a4' \
    'grafana/grafana:13.3.0@sha256:488984b716c0862639f91a698adbe2862677a5a07bad42da5f03f3030883b205' \
    'dxflrs/garage:v2.5.0@sha256:677e5b758e4425a325b811c425c026bf35a4bf42f7ddbaa773de3f4fd779a5a5'; do
    grep -Fq "$pin" ../../Dockerfile compose.yaml garage.Dockerfile 2>/dev/null ||
        fail "missing pinned image: $pin"
done
echo "phase 1 default services: $phase1_services"
echo "image pins: OK"
echo "generator expected total: $EXPECTED (base+job+corpus)"
if [ -n "$COMPARE_CORPUS" ]; then
    echo "corpus comparison: $COMPARE_CORPUS (frozen lokicompare binary)"
fi

ensure_project_available || fail "refusing existing compose project '$PROJECT'"
for port in "$PARKANA_HTTP_PORT" "$PARKANA_S3_PORT" "$LOKI_HTTP_PORT" "$ALLOY_HTTP_PORT" "$GRAFANA_HTTP_PORT"; do
    if port_published "$port"; then
        fail "host port $port is already published"
    fi
done

say "build and start"
created=1
"$DOCKER" compose -p "$PROJECT" --project-directory . -f compose.yaml --profile phase2 build
# The finite generators start with the stack, so the generated-data range
# begins before `up -d` whether or not a corpus comparison is enabled. The
# same START anchors the optional comparator. END is captured only after the
# wall-clock Loki barrier and the catalog barrier below, so it includes all
# generated timestamps even when pushgen's synthetic range leads the wall
# clock.
RANGE_START_NS=$(date +%s%N)
if [ -n "$COMPARE_CORPUS" ]; then
    COMPARE_START_NS=$RANGE_START_NS
fi
"$DOCKER" compose -p "$PROJECT" --project-directory . -f compose.yaml --profile phase2 up -d
wait_service_exit garage-init 0 300 || fail "garage-init did not exit 0"

say "readiness"
wait_http "$PARKANA_URL/ready" "Parkana /ready" || fail "Parkana not ready"
wait_http "$LOKI_URL/ready" "Loki /ready" || fail "Loki not ready"
wait_http "$ALLOY_URL/-/ready" "Alloy /-/ready" || fail "Alloy not ready"
wait_http "$GRAFANA_URL/api/health" "Grafana /api/health" "$GRAFANA_READY_TIMEOUT" || fail "Grafana not ready"
echo "Parkana /ready, Loki /ready, Alloy /-/ready, Grafana /api/health: OK"

say "versions"
wait_version "$LOKI_URL/loki/api/v1/status/buildinfo" 3.7.9 180 "Loki" || fail "Loki version mismatch"
wait_version "$GRAFANA_URL/api/health" 13.3.0 "$GRAFANA_READY_TIMEOUT" "Grafana" || fail "Grafana version mismatch"
wait_version "$GRAFANA_URL/api/plugins/loki/settings" 13.2.5 "$GRAFANA_READY_TIMEOUT" "Loki datasource plugin" ||
    fail "Loki datasource plugin version mismatch (want 13.2.5)"
echo "versions: Loki 3.7.9, Grafana 13.3.0, Loki datasource plugin 13.2.5"

say "effective settings"
env_parkana=$(compose exec -T parkana sh -c 'env' 2>/dev/null || true)
printf '%s\n' "$env_parkana" | grep -qx 'PARKANA_INGEST_UPLOAD_MAX_AGE=30s' ||
    fail "Parkana upload-max-age is not 30s"
env_grafana=$(compose exec -T grafana sh -c 'env' 2>/dev/null || true)
printf '%s\n' "$env_grafana" | grep -qx 'GF_AUTH_ANONYMOUS_ENABLED=true' ||
    fail "Grafana anonymous auth is not enabled"
printf '%s\n' "$env_grafana" | grep -q '^GF_PLUGINS_PREINSTALL_SYNC=loki@13.2.5@' ||
    fail "Grafana plugin pin is not in the container environment"
wait_http "$LOKI_URL/config" "Loki /config" 60 || fail "Loki /config not available"
loki_config=$(curl -fsS --connect-timeout 2 --max-time 10 "$LOKI_URL/config")
check_loki_config "$loki_config" || fail "effective Loki config is not parity-safe (discover_log_levels false, discover_service_name disabled, structured metadata allowed)"
printf '%s\n' "$loki_config" |
    grep -E '^[[:space:]]*(allow_structured_metadata|discover_log_levels|discover_service_name):' |
    sed 's/^[[:space:]]*/  /'
echo "Parkana upload-max-age=30s, Grafana anonymous admin + pinned plugin, Loki log-level and service-name discovery off + structured metadata on: OK"

say "datasources"
wait_datasource loki "$GRAFANA_READY_TIMEOUT" || fail "Grafana datasource loki not provisioned"
wait_datasource parkana "$GRAFANA_READY_TIMEOUT" || fail "Grafana datasource parkana not provisioned"
loki_health=$(datasource_health loki)
printf '%s' "$loki_health" | grep -q '"status":"OK"' || fail "Loki datasource health: $loki_health"
echo "Grafana datasource Loki: OK"
parkana_health=$(datasource_health parkana)
if [ "$MODE" = full ]; then
    printf '%s' "$parkana_health" | grep -q '"status":"OK"' || fail "Parkana datasource health: $parkana_health"
    echo "Grafana datasource Parkana: OK"
else
    if printf '%s' "$parkana_health" | grep -q '"status":"OK"'; then
        echo "Grafana datasource Parkana: OK"
    else
        echo "Grafana datasource Parkana: PENDING (smoke mode; read API may still be a stub)"
    fi
fi

generator_summary() {
    for svc in pushgen-base pushgen-job pushgen-corpus; do
        line=$(compose logs --no-color --tail=1 "$svc" 2>/dev/null | tail -n 1)
        echo "  $svc: $line"
    done
}

say "generators"
wait_service_exit pushgen-base 0 300 || fail "pushgen-base failed"
wait_service_exit pushgen-job 0 300 || fail "pushgen-job failed"
wait_service_exit pushgen-corpus 0 300 || fail "pushgen-corpus failed"
generator_summary
echo "pushgen-base, pushgen-job and pushgen-corpus exited 0 (expected total $EXPECTED lines)"

say "Loki corpus"
wait_count loki "$EXPECTED" 240 || fail "Loki count did not reach $EXPECTED"

say "Parkana ingest and catalog"
wait_catalog "$EXPECTED" 240 || fail "Parkana catalog did not reach $EXPECTED rows"
if alloy_parkana_rejected; then
    fail "Alloy reported a Parkana ingest rejection (status 400 or unsupported content encoding)"
fi
echo "Alloy to Parkana rejections: none"

# The Loki count polls at wall-clock now, so reaching EXPECTED means no
# generated timestamp is still in the future. Capturing END here (and reusing
# it after restart) keeps one full-range unique check for both modes.
RANGE_END_NS=$(date +%s%N)
[ -n "$RANGE_START_NS" ] || fail "generated-data range start was not captured"
[ "$RANGE_END_NS" -gt "$RANGE_START_NS" ] ||
    fail "generated-data range is empty ($RANGE_START_NS..$RANGE_END_NS)"

say "Parkana unique stats"
wait_stats "$EXPECTED" "$RANGE_START_NS" "$RANGE_END_NS" 240 ||
    fail "Parkana index/stats entries did not reach $EXPECTED unique rows"

if [ "$MODE" = full ]; then
    say "restart persistence"
    timeout 120 "$DOCKER" compose -p "$PROJECT" --project-directory . -f compose.yaml --profile phase2 restart loki parkana grafana
    wait_http "$PARKANA_URL/ready" "Parkana /ready after restart" || fail "Parkana not ready after restart"
    wait_http "$LOKI_URL/ready" "Loki /ready after restart" || fail "Loki not ready after restart"
    wait_http "$GRAFANA_URL/api/health" "Grafana /api/health after restart" "$GRAFANA_READY_TIMEOUT" || fail "Grafana not ready after restart"
    wait_count loki "$EXPECTED" 240 || fail "Loki count changed after restart"
    wait_catalog "$EXPECTED" 240 || fail "Parkana catalog changed after restart"
    wait_stats "$EXPECTED" "$RANGE_START_NS" "$RANGE_END_NS" 240 ||
        fail "Parkana index/stats entries changed after restart"
    echo "counts survived restart: Loki=$EXPECTED Parkana unique=$EXPECTED"
fi

if [ -n "$COMPARE_CORPUS" ]; then
    COMPARE_END_NS=$(date +%s%N)
    [ -n "$COMPARE_START_NS" ] || fail "corpus comparison start was not captured"
    [ "$COMPARE_END_NS" -gt "$COMPARE_START_NS" ] ||
        fail "corpus comparison range is empty ($COMPARE_START_NS..$COMPARE_END_NS)"
    say "corpus comparison"
    echo "lokicompare: left=$LOKI_URL right=$PARKANA_URL corpus=$COMPARE_CORPUS"
    echo "range: $COMPARE_START_NS..$COMPARE_END_NS (one shared range for every entry)"
    compare_status=0
    run_corpus_compare || compare_status=$?
    case "$compare_status" in
        0) echo "lokicompare: every corpus entry compared equal" ;;
        1)
            echo "lokicompare: semantic difference(s) between Loki and Parkana" >&2
            exit 1
            ;;
        2)
            echo "lokicompare: invalid invocation or request/response failure" >&2
            exit 2
            ;;
        *)
            echo "lokicompare: unexpected exit status $compare_status" >&2
            exit 2
            ;;
    esac
fi

say "PASS ($MODE)"
if [ "$MODE" = smoke ]; then
    echo "smoke requires the physical catalog and Parkana's unique index/stats entries; --full adds restart persistence."
fi
