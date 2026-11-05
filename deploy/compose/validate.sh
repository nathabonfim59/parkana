#!/bin/sh
# Local validation for the phase 1 compose stack.
#
# Runs on a machine with Docker (not the developer laptop by default): builds
# both images, boots Garage + garage-init + Parkana, checks readiness and a
# real S3 PUT/GET with the development key, reruns garage-init for
# idempotence, and finally simulates an interrupted `layout assign` to prove
# the bootstrap applies a staged layout correctly.
#
#   sh deploy/compose/validate.sh
#   sh deploy/compose/validate.sh --selftest
#
# It runs in its own compose project (unique unless PARKANA_VALIDATE_PROJECT
# is set), never touches an existing `parkana-phase1` stack, refuses to start
# if its project name or host ports are already in use, and deletes only the
# resources it created. It uses PARKANA_HTTP_PORT=13100 / PARKANA_S3_PORT=13900
# by default so it never collides with a normal `docker compose up` on
# 3100/3900.
set -eu

cd "$(dirname "$0")"

export PARKANA_HTTP_PORT="${PARKANA_HTTP_PORT:-13100}"
export PARKANA_S3_PORT="${PARKANA_S3_PORT:-13900}"
export PARKANA_DEV_GARAGE_KEY_ID="${PARKANA_DEV_GARAGE_KEY_ID:-GK5c7e9a3b1d8f2e6c4a0b9d8f}"
export PARKANA_DEV_GARAGE_KEY_SECRET="${PARKANA_DEV_GARAGE_KEY_SECRET:-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef}"

DOCKER="${DOCKER:-docker}"
PROJECT="${PARKANA_VALIDATE_PROJECT:-parkana-validate-$(date +%s)-$$}"
S3="http://127.0.0.1:${PARKANA_S3_PORT}/parkana"
TMP=$(mktemp -d)
created=0

# Everything is scoped by -p so the developer's parkana-phase1 stack is never
# touched. Config interpolation also prints the resolved name; verify it
# before creating anything.
compose() { "$DOCKER" compose -p "$PROJECT" "$@"; }

say() { printf '\n=== %s ===\n' "$*"; }

# Drop any line containing the S3 secret before diagnostics reach the
# terminal or a log: the upstream CLI may print an imported key.
sanitize() {
    grep -Fv -- "$PARKANA_DEV_GARAGE_KEY_SECRET" || true
}

project_exists() {
    [ -n "$(compose ps -a -q 2>/dev/null || true)" ] && return 0
    [ -n "$("$DOCKER" volume ls -q --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null || true)" ] && return 0
    [ -n "$("$DOCKER" network ls -q --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null || true)" ] && return 0
    return 1
}

port_published() {
    "$DOCKER" ps --format '{{.Ports}}' 2>/dev/null | grep -q ":$1->"
}

cleanup() {
    status=$?
    trap - EXIT INT TERM
    if [ "$created" = 1 ]; then
        compose down -v --remove-orphans >/dev/null 2>&1 || true
    fi
    rm -rf "$TMP"
    exit "$status"
}
trap cleanup EXIT INT TERM

wait_ready() {
    url=$1
    i=0
    until curl -fsS --connect-timeout 2 --max-time 5 "$url" >/dev/null 2>&1; do
        i=$((i + 1))
        [ "$i" -ge 120 ] && { echo "not ready after 120s: $url" >&2; return 1; }
        sleep 1
    done
}

wait_init() {
    want=$1
    i=0
    while :; do
        id=$(compose ps -a -q garage-init)
        if [ -n "$id" ]; then
            state=$("$DOCKER" inspect -f '{{.State.Status}}' "$id")
            if [ "$state" = exited ]; then
                code=$("$DOCKER" inspect -f '{{.State.ExitCode}}' "$id")
                if [ "$code" = "$want" ]; then
                    return 0
                fi
                compose logs garage-init 2>&1 | sanitize >&2 || true
                echo "garage-init exited $code, want $want" >&2
                return 1
            fi
        fi
        i=$((i + 1))
        [ "$i" -ge 300 ] && {
            compose logs garage-init 2>&1 | sanitize >&2 || true
            echo "garage-init did not exit" >&2
            return 1
        }
        sleep 1
    done
}

# Exercises the two safety behaviors without Docker: an existing project is
# refused, and a non-zero one-shot exit is propagated rather than masked by a
# pipeline.
selftest() {
    mkdir -p "$TMP/bin"
    cat >"$TMP/bin/docker" <<'MOCK'
#!/bin/sh
if [ "$1" = compose ]; then
    shift 3
    case "$1" in
        ps) [ "${MOCK_PS:-0}" = 1 ] && echo mock-container-id ;;
        logs) echo "mock garage-init logs" ;;
    esac
    exit 0
fi
if [ "$1" = inspect ]; then
    case "$3" in
        *ExitCode*) echo "${MOCK_CODE:-0}" ;;
        *Status*) echo "${MOCK_STATE:-exited}" ;;
    esac
    exit 0
fi
exit 0
MOCK
    chmod 0755 "$TMP/bin/docker"
    DOCKER="$TMP/bin/docker"
    PROJECT=selftest
    export MOCK_PS=1 MOCK_STATE=exited MOCK_CODE=0

    project_exists || { echo "selftest: existing project not detected" >&2; exit 1; }

    MOCK_CODE=7
    if wait_init 0; then
        echo "selftest: non-zero garage-init exit was not propagated" >&2
        exit 1
    fi

    MOCK_CODE=0
    wait_init 0 || { echo "selftest: successful garage-init rejected" >&2; exit 1; }
    echo "selftest: PASS"
}

if [ "${1:-}" = "--selftest" ]; then
    selftest
    exit 0
fi

say "compose project"
resolved=$(compose config 2>/dev/null | sed -n '1s/^name: //p')
[ "$resolved" = "$PROJECT" ] || {
    echo "refusing: compose resolved project '$resolved', want '$PROJECT'" >&2
    exit 1
}
echo "$PROJECT"

if project_exists; then
    echo "refusing: compose project '$PROJECT' already exists" >&2
    exit 1
fi
if port_published "$PARKANA_HTTP_PORT" || port_published "$PARKANA_S3_PORT"; then
    echo "refusing: host port $PARKANA_HTTP_PORT or $PARKANA_S3_PORT is already published" >&2
    exit 1
fi

say "compose config"
compose config >/dev/null

say "build and start"
created=1
compose up -d --build
wait_init 0
wait_ready "http://127.0.0.1:${PARKANA_HTTP_PORT}/ready"

say "host port bindings"
compose port garage 3900
compose port parkana 3100
compose port garage 3900 | grep -q "^127.0.0.1:${PARKANA_S3_PORT}$"
compose port parkana 3100 | grep -q "^127.0.0.1:${PARKANA_HTTP_PORT}$"

say "parkana endpoints"
curl -fsS --connect-timeout 2 --max-time 5 "http://127.0.0.1:${PARKANA_HTTP_PORT}/ready"
echo
curl -fsS --connect-timeout 2 --max-time 5 "http://127.0.0.1:${PARKANA_HTTP_PORT}/loki/api/v1/status/buildinfo"
echo

say "phase env effective inside parkana"
compose exec -T parkana sh -c 'env' | grep -E '^PARKANA_(INGEST|COMPACTOR)_' | sort

say "non-root images"
"$DOCKER" image inspect -f 'parkana user={{.Config.User}}' parkana:phase1
"$DOCKER" image inspect -f 'garage user={{.Config.User}}' parkana-garage:v2.5.0

say "garage key and bucket"
compose exec -T garage /usr/local/bin/garage -c /etc/garage.toml key info "$PARKANA_DEV_GARAGE_KEY_ID" >/dev/null
compose exec -T garage /usr/local/bin/garage -c /etc/garage.toml bucket info parkana |
    grep -Eq "^RWO[[:space:]]+$PARKANA_DEV_GARAGE_KEY_ID"

say "real S3 PUT/GET with the dev key"
printf 'parkana-garage-validation\n' >"$TMP/payload"
curl -fsS --connect-timeout 2 --max-time 10 --aws-sigv4 "aws:amz:garage:s3" \
    --user "$PARKANA_DEV_GARAGE_KEY_ID:$PARKANA_DEV_GARAGE_KEY_SECRET" \
    --upload-file "$TMP/payload" "$S3/validate/object.txt"
curl -fsS --connect-timeout 2 --max-time 10 --aws-sigv4 "aws:amz:garage:s3" \
    --user "$PARKANA_DEV_GARAGE_KEY_ID:$PARKANA_DEV_GARAGE_KEY_SECRET" \
    -o "$TMP/got" "$S3/validate/object.txt"
cmp "$TMP/payload" "$TMP/got"

say "phase env override substitution"
export PARKANA_INGEST_FLUSH_INTERVAL=3s
export PARKANA_COMPACTOR_INTERVAL=11s
compose up -d parkana
wait_ready "http://127.0.0.1:${PARKANA_HTTP_PORT}/ready"
compose exec -T parkana sh -c 'env' | grep -Eq '^PARKANA_INGEST_FLUSH_INTERVAL=3s$'
compose exec -T parkana sh -c 'env' | grep -Eq '^PARKANA_COMPACTOR_INTERVAL=11s$'

say "garage-init idempotence"
if ! compose run --rm --no-deps garage-init >"$TMP/init-rerun.log" 2>&1; then
    sanitize <"$TMP/init-rerun.log" >&2
    echo "garage-init rerun failed" >&2
    exit 1
fi
sanitize <"$TMP/init-rerun.log"
grep -q "already has an active layout role" "$TMP/init-rerun.log"
grep -q "key $PARKANA_DEV_GARAGE_KEY_ID already exists" "$TMP/init-rerun.log"
grep -q "bucket 'parkana' already exists" "$TMP/init-rerun.log"
[ "$(compose exec -T garage /usr/local/bin/garage -c /etc/garage.toml key list | grep -c "$PARKANA_DEV_GARAGE_KEY_ID")" = 1 ]

say "interrupted staged-layout recovery"
compose down -v --remove-orphans >/dev/null
created=0
created=1
compose up -d garage
i=0
until compose exec -T garage /usr/local/bin/garage -c /etc/garage.toml status >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -ge 60 ] && { echo "garage CLI not reachable" >&2; exit 1; }
    sleep 1
done
NODE_ID=$(compose exec -T garage /usr/local/bin/garage -c /etc/garage.toml status |
    awk '/HEALTHY NODES/{h=1;next} h && NF>=3 && $1!="ID" {print $1; exit}')
[ -n "$NODE_ID" ]
compose exec -T garage /usr/local/bin/garage -c /etc/garage.toml layout assign -z garage -c 1GB "$NODE_ID"
compose exec -T garage /usr/local/bin/garage -c /etc/garage.toml layout show | grep -q "STAGED ROLE CHANGES"
compose up -d garage-init
wait_init 0
compose exec -T garage /usr/local/bin/garage -c /etc/garage.toml layout show |
    awk -v id="$NODE_ID" '
        /^==== CURRENT CLUSTER LAYOUT/ {cur=1; next}
        /^Current cluster layout version:/ {cur=0}
        cur && $1 == id {found=1}
        END {exit !found}'
compose exec -T garage /usr/local/bin/garage -c /etc/garage.toml bucket info parkana |
    grep -Eq "^RWO[[:space:]]+$PARKANA_DEV_GARAGE_KEY_ID"

say "PASS"
