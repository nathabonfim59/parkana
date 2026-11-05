#!/bin/sh
# Wait for Alloy's loki.source.api readiness endpoint, then run pushgen.
#
# The compose stack starts the generator after Alloy's container has started,
# but `service_started` does not mean the receive server is listening yet.
# pushgen itself fails on the first request instead of retrying, so one early
# request would exit the one-shot container and lose the finite corpus. This
# wrapper polls the receive API (loki.source.api serves /ready on the same
# port as /loki/api/v1/push) before starting the single pushgen run.
#
# PUSHGEN_READY_URL            readiness URL (default http://alloy:3100/ready)
# PUSHGEN_READY_TIMEOUT_SECONDS positive integer deadline in seconds
#                              (default 120, maximum 86400)
#
# The deadline is wall-clock time, not an attempt count: one curl can take up
# to two seconds and each failed attempt sleeps one second before the next.

set -eu

is_positive_uint() { # canonical positive decimal: first digit 1-9, rest digits
    case "$1" in
        ""|0*|*[!0-9]*) return 1 ;;
    esac
    [ "${#1}" -le 5 ] || return 1
    [ "$1" -le 86400 ]
}

wait_for_ready() {
    ready="${PUSHGEN_READY_URL:-http://alloy:3100/ready}"
    timeout="${PUSHGEN_READY_TIMEOUT_SECONDS:-120}"

    if ! is_positive_uint "$timeout"; then
        echo "pushgen-wait: PUSHGEN_READY_TIMEOUT_SECONDS must be a positive integer <= 86400, got '$timeout'" >&2
        return 2
    fi

    start=$(date +%s)
    deadline=$((start + timeout))
    while :; do
        if curl --fail --silent --show-error --connect-timeout 1 --max-time 2 "$ready" >/dev/null 2>&1; then
            return 0
        fi
        now=$(date +%s)
        if [ "$now" -ge "$deadline" ]; then
            echo "pushgen-wait: $ready not ready after $((now - start))s (deadline ${timeout}s)" >&2
            return 1
        fi
        sleep 1
    done
}

selftest() {
    tmp=$(mktemp -d)
    trap 'rm -rf "$tmp"' EXIT
    mkdir -p "$tmp/bin"
    cat >"$tmp/bin/curl" <<'MOCK'
#!/bin/sh
exit 7
MOCK
    chmod 0755 "$tmp/bin/curl"
    old_path=$PATH
    PATH="$tmp/bin:$PATH"
    export PATH
    export PUSHGEN_READY_URL="http://example.invalid/ready"
    export PUSHGEN_READY_TIMEOUT_SECONDS=1

    start=$(date +%s)
    if wait_for_ready 2>/dev/null; then
        echo "selftest: failing readiness was accepted" >&2
        exit 1
    fi
    elapsed=$(($(date +%s) - start))
    if [ "$elapsed" -lt 1 ] || [ "$elapsed" -gt 5 ]; then
        echo "selftest: failure deadline used $elapsed s, want 1..5" >&2
        exit 1
    fi

    for t in 1 120 86400; do
        is_positive_uint "$t" || { echo "selftest: valid timeout '$t' rejected" >&2; exit 1; }
    done
    for t in 0 00 00000 00008 01 abc -1 86401 123456; do
        if is_positive_uint "$t"; then
            echo "selftest: invalid timeout '$t' accepted" >&2
            exit 1
        fi
    done

    PATH=$old_path
    export PATH
    echo "selftest: PASS"
}

if [ "${1:-}" = "--selftest" ]; then
    selftest
    exit 0
fi

wait_for_ready
exec /usr/local/bin/pushgen "$@"
