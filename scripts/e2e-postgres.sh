#!/usr/bin/env bash
#
# scripts/e2e-postgres.sh - end-to-end smoke test of the Postgres catalog.
#
# What it proves, against a real Postgres server and the real binary:
#   1. An all-in-one Parkana (-roles=all) starts with -catalog.driver=postgres,
#      the DSN given only through PARKANA_CATALOG_DSN, and becomes ready.
#   2. Lines pushed through POST /loki/api/v1/push become visible through
#      /loki/api/v1/query_range once uploaded and cataloged, exactly once,
#      and a label matcher selects exactly its line.
#   3. After SIGTERM (clean exit 0) a restart on the same data dir and schema
#      returns the same lines.
#   4. A second, querier-only process with its own data dir, the same catalog
#      schema and the same fs object directory returns the same lines while
#      the first one runs: two processes share one catalog.
#   5. The files table of the test schema holds rows (checked with psql).
#
# It runs everything on the local host: no Docker. The catalog lives in its
# own schema, parkana_e2e_<random hex>, created with psql and dropped on exit
# together with the temp dir and both processes.
#
# Prerequisites: bash, psql, curl, GNU coreutils (timeout, od), make and the
# Go toolchain of go.mod with cgo (to build bin/parkana), and a reachable
# Postgres database the given role may create schemas in.
#
# Environment:
#   PARKANA_TEST_POSTGRES_DSN  required: base DSN in URL form
#                              (postgres://user:pass@host:5432/db?sslmode=...)
#                              without a search_path. Never printed.
#   PARKANA_E2E_BIN            optional: a prebuilt parkana binary; default is
#                              to run `make build` and use bin/parkana.
#   PARKANA_E2E_TIMEOUT        optional: seconds for each wait (default 60).
#
# Usage: scripts/e2e-postgres.sh
#
# Exit status: 0 every step passed; 1 a prerequisite or step failed (the
# message names the step; process logs are shown with the DSN masked).
#
set -euo pipefail

readonly SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
readonly WAIT_SECONDS=${PARKANA_E2E_TIMEOUT:-60}
readonly SERVICE=e2e-pg

BASE_DSN=${PARKANA_TEST_POSTGRES_DSN:-}
# Keep the base DSN out of every child's environment: only
# PARKANA_CATALOG_DSN carries a DSN, and only to parkana.
unset PARKANA_TEST_POSTGRES_DSN

STEP=prerequisites
WORK=
SCHEMA=
SCHEMA_CREATED=0
CATALOG_DSN=
PG_HOST= PG_PORT= PG_USER= PG_PASS= PG_DB= PG_SSLMODE=
ALL_PID= QUERIER_PID=
ALL_LOG= QUERIER_LOG=

# mask replaces the DSNs and the password in its argument. The password is
# masked in its DSN forms (":<pw>@", "password=<pw>"), and bare only when it
# is long enough (8+ bytes) not to hit ordinary words such as "parkana".
mask() {
	local s=$1
	[[ -n $CATALOG_DSN ]] && s=${s//"$CATALOG_DSN"/<dsn>}
	[[ -n $BASE_DSN ]] && s=${s//"$BASE_DSN"/<dsn>}
	if [[ -n $PG_PASS ]]; then
		s=${s//":$PG_PASS@"/:<password>@}
		s=${s//"password=$PG_PASS"/password=<password>}
		((${#PG_PASS} >= 8)) && s=${s//"$PG_PASS"/<password>}
	fi
	printf '%s\n' "$s"
}

log() { mask "e2e-postgres: [$STEP] $*" >&2; }

# show_logs prints the tail of each process log, masked.
show_logs() {
	local f
	for f in "$ALL_LOG" "$QUERIER_LOG"; do
		[[ -n $f && -s $f ]] || continue
		echo "----- $(basename -- "$f") (last 40 lines) -----" >&2
		mask "$(tail -n 40 -- "$f")" >&2
	done
}

fail() {
	mask "e2e-postgres: FAIL [$STEP]: $*" >&2
	show_logs
	exit 1
}

# stop_pid sends SIGTERM to pid and waits up to 30s, then SIGKILLs it.
stop_pid() {
	local pid=$1 i
	[[ -n $pid ]] || return 0
	kill -TERM "$pid" 2>/dev/null || return 0
	for ((i = 0; i < 150; i++)); do
		kill -0 "$pid" 2>/dev/null || break
		sleep 0.2
	done
	kill -KILL "$pid" 2>/dev/null || true
	wait "$pid" 2>/dev/null || true
}

cleanup() {
	local rc=$?
	trap - EXIT
	stop_pid "$QUERIER_PID"
	stop_pid "$ALL_PID"
	if ((SCHEMA_CREATED)); then
		if ! run_psql -c "DROP SCHEMA $SCHEMA CASCADE" >/dev/null; then
			echo "e2e-postgres: cleanup: could not drop schema $SCHEMA" >&2
			rc=1
		fi
	fi
	[[ -n $WORK ]] && rm -rf -- "$WORK"
	if ((rc == 0)); then
		echo "e2e-postgres: PASS"
	fi
	exit "$rc"
}

# urldecode decodes %XX escapes.
urldecode() { printf '%b' "${1//%/\\x}"; }

# parse_dsn splits a URL-form DSN into the PG_* variables psql gets through
# its environment, so the password never reaches a command line.
parse_dsn() {
	local dsn=$1 re query kv k v
	re='^postgres(ql)?://(([^:@/]*)(:([^@/]*))?@)?([^/?]*)(/([^?]*))?(\?(.*))?$'
	[[ $dsn =~ $re ]] || fail "PARKANA_TEST_POSTGRES_DSN is not a postgres:// URL"
	PG_USER=$(urldecode "${BASH_REMATCH[3]}")
	PG_PASS=$(urldecode "${BASH_REMATCH[5]}")
	local hostport=${BASH_REMATCH[6]}
	PG_DB=$(urldecode "${BASH_REMATCH[8]}")
	query=${BASH_REMATCH[10]}
	if [[ $hostport =~ ^(.*):([0-9]+)$ ]]; then
		PG_HOST=${BASH_REMATCH[1]}
		PG_PORT=${BASH_REMATCH[2]}
	else
		PG_HOST=$hostport
	fi
	PG_HOST=${PG_HOST#[}
	PG_HOST=${PG_HOST%]}
	PG_HOST=$(urldecode "$PG_HOST")
	if [[ -n $query ]]; then
		local -a pairs
		IFS='&' read -r -a pairs <<<"$query"
		for kv in "${pairs[@]}"; do
			k=${kv%%=*}
			v=$(urldecode "${kv#*=}")
			case $k in
			host) PG_HOST=$v ;;
			port) PG_PORT=$v ;;
			user) PG_USER=$v ;;
			password) PG_PASS=$v ;;
			dbname) PG_DB=$v ;;
			sslmode) PG_SSLMODE=$v ;;
			search_path) fail "PARKANA_TEST_POSTGRES_DSN must not set search_path: the script picks the schema" ;;
			*) log "psql ignores DSN parameter '$k'" ;;
			esac
		done
	fi
}

# run_psql runs psql on the base database, configured only through its
# environment, bounded by a timeout.
run_psql() {
	(
		[[ -n $PG_HOST ]] && export PGHOST=$PG_HOST
		[[ -n $PG_PORT ]] && export PGPORT=$PG_PORT
		[[ -n $PG_USER ]] && export PGUSER=$PG_USER
		[[ -n $PG_PASS ]] && export PGPASSWORD=$PG_PASS
		[[ -n $PG_DB ]] && export PGDATABASE=$PG_DB
		[[ -n $PG_SSLMODE ]] && export PGSSLMODE=$PG_SSLMODE
		# Keep NOTICE/DETAIL chatter (DROP SCHEMA ... CASCADE) off stderr.
		export PGOPTIONS='-c client_min_messages=warning'
		exec timeout 30 psql -X -q -At -v ON_ERROR_STOP=1 "$@"
	)
}

# start_parkana LOGFILE ARGS... starts the binary in the background with the
# catalog DSN in its environment only and sets STARTED_PID.
start_parkana() {
	local logfile=$1
	shift
	(
		export PARKANA_CATALOG_DSN=$CATALOG_DSN
		exec "$BIN" -http.addr=127.0.0.1:0 -catalog.driver=postgres "$@"
	) >"$logfile" 2>&1 &
	STARTED_PID=$!
}

# wait_ready LOGFILE PID prints the process's base URL once it logged its
# listen address and /ready answers 200.
wait_ready() {
	local logfile=$1 pid=$2 deadline addr=
	deadline=$((SECONDS + WAIT_SECONDS))
	while ((SECONDS < deadline)); do
		kill -0 "$pid" 2>/dev/null || fail "process exited during startup"
		if [[ -z $addr ]]; then
			addr=$(grep -o 'msg="http server listening" addr=[^ ]*' -- "$logfile" | head -n 1 | sed 's/.*addr=//') || true
		fi
		if [[ -n $addr ]] && curl -fsS -o /dev/null --max-time 5 "http://$addr/ready" 2>/dev/null; then
			printf 'http://%s\n' "$addr"
			return 0
		fi
		sleep 0.5
	done
	fail "not ready within ${WAIT_SECONDS}s"
}

# query_lines URL LOGQL prints the sorted fixture lines a query_range returns.
query_lines() {
	local url=$1 q=$2 body
	body=$(curl -fsS --max-time 10 -G "$url/loki/api/v1/query_range" \
		--data-urlencode "query=$q" \
		--data-urlencode "start=$Q_START" \
		--data-urlencode "end=$Q_END" \
		--data-urlencode "limit=100" \
		--data-urlencode "direction=forward") || return 1
	grep -o "e2epg-$RUN-[0-9]*" <<<"$body" | sort | tr '\n' ' ' || true
}

# expect_lines URL LOGQL WANT polls until the query returns exactly WANT (a
# space-terminated sorted list) or the wait expires.
expect_lines() {
	local url=$1 q=$2 want=$3 got= deadline
	deadline=$((SECONDS + WAIT_SECONDS))
	while ((SECONDS < deadline)); do
		got=$(query_lines "$url" "$q") || got="<query failed>"
		[[ $got == "$want" ]] && return 0
		sleep 0.5
	done
	fail "query $q: got [${got}], want [${want}]"
}

# expect_queries URL checks the full stream and one label matcher.
expect_queries() {
	local url=$1
	expect_lines "$url" "{service_name=\"$SERVICE\"}" "$WANT_ALL"
	expect_lines "$url" "{service_name=\"$SERVICE\", color=\"blue\"}" "e2epg-$RUN-2 "
}

# sigterm_and_wait sends SIGTERM to pid and requires exit status 0.
sigterm_and_wait() {
	local pid=$1 deadline rc=0
	kill -TERM "$pid" || fail "SIGTERM: process already gone"
	deadline=$((SECONDS + WAIT_SECONDS))
	while kill -0 "$pid" 2>/dev/null; do
		((SECONDS < deadline)) || fail "process did not exit within ${WAIT_SECONDS}s of SIGTERM"
		sleep 0.2
	done
	wait "$pid" || rc=$?
	((rc == 0)) || fail "process exited with status $rc after SIGTERM, want 0"
}

trap cleanup EXIT

for tool in psql curl timeout od make; do
	command -v "$tool" >/dev/null || fail "$tool not found"
done
[[ -n $BASE_DSN ]] || fail "PARKANA_TEST_POSTGRES_DSN is not set"
[[ $BASE_DSN == postgres://* || $BASE_DSN == postgresql://* ]] ||
	fail "PARKANA_TEST_POSTGRES_DSN must be in URL form (postgres://...)"
parse_dsn "$BASE_DSN"

RUN=$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')
SCHEMA=parkana_e2e_$RUN
if [[ $BASE_DSN == *\?* ]]; then
	CATALOG_DSN="$BASE_DSN&search_path=$SCHEMA"
else
	CATALOG_DSN="$BASE_DSN?search_path=$SCHEMA"
fi
WORK=$(mktemp -d "${TMPDIR:-/tmp}/parkana-e2e-pg.XXXXXX")
OBJ_DIR=$WORK/objects
ALL_DATA=$WORK/all
QUERIER_DATA=$WORK/querier
mkdir -p -- "$OBJ_DIR" "$ALL_DATA" "$QUERIER_DATA"

STEP=build
if [[ -n ${PARKANA_E2E_BIN:-} ]]; then
	BIN=$PARKANA_E2E_BIN
else
	log "make build"
	timeout 900 make -C "$REPO_ROOT" build >"$WORK/build.log" 2>&1 || {
		tail -n 40 -- "$WORK/build.log" >&2
		fail "make build failed"
	}
	BIN=$REPO_ROOT/bin/parkana
fi
[[ -x $BIN ]] || fail "binary $BIN is not executable"

STEP=schema
run_psql -c "CREATE SCHEMA $SCHEMA" >/dev/null || fail "psql could not create schema $SCHEMA"
SCHEMA_CREATED=1
log "schema $SCHEMA"

# Short flush/upload pacing: a line is uploaded and cataloged within about a
# second instead of waiting for -ingest.upload-min (128MiB) or
# -ingest.upload-max-age (10m), see docs/configuration.md.
ALL_ARGS=(
	-roles=all
	-data.dir="$ALL_DATA"
	-storage.backend=fs
	-storage.fs.dir="$OBJ_DIR"
	-ingest.flush-interval=100ms
	-ingest.upload-interval=100ms
	-ingest.upload-max-age=1ms
	-ingest.upload-min=1B
)

STEP=start
ALL_LOG=$WORK/all-1.log
start_parkana "$ALL_LOG" "${ALL_ARGS[@]}"
ALL_PID=$STARTED_PID
ALL_URL=$(wait_ready "$ALL_LOG" "$ALL_PID")
log "all-in-one ready at $ALL_URL"

STEP=push
NOW_NS=$(($(date +%s) * 1000000000))
TS=$((NOW_NS - 60 * 1000000000))
Q_START=$((NOW_NS - 600 * 1000000000))
Q_END=$((NOW_NS + 600 * 1000000000))
WANT_ALL="e2epg-$RUN-1 e2epg-$RUN-2 e2epg-$RUN-3 "
PUSH_BODY=$(
	cat <<EOF
{"streams":[
 {"stream":{"service_name":"$SERVICE","color":"red"},"values":[["$TS","e2epg-$RUN-1"]]},
 {"stream":{"service_name":"$SERVICE","color":"blue"},"values":[["$((TS + 1000000000))","e2epg-$RUN-2"]]},
 {"stream":{"service_name":"$SERVICE","color":"green"},"values":[["$((TS + 2000000000))","e2epg-$RUN-3"]]}
]}
EOF
)
code=$(curl -sS --max-time 10 -o "$WORK/push.out" -w '%{http_code}' -X POST \
	-H 'Content-Type: application/json' --data-binary "$PUSH_BODY" \
	"$ALL_URL/loki/api/v1/push") || fail "push request failed"
[[ $code == 204 ]] || fail "push returned HTTP $code: $(head -c 500 -- "$WORK/push.out")"

STEP=query
expect_queries "$ALL_URL"
log "3 lines visible"

STEP=restart
sigterm_and_wait "$ALL_PID"
ALL_PID=
ALL_LOG=$WORK/all-2.log
start_parkana "$ALL_LOG" "${ALL_ARGS[@]}"
ALL_PID=$STARTED_PID
ALL_URL=$(wait_ready "$ALL_LOG" "$ALL_PID")
expect_queries "$ALL_URL"
log "same lines after SIGTERM and restart"

STEP=second-querier
QUERIER_LOG=$WORK/querier.log
start_parkana "$QUERIER_LOG" \
	-roles=querier \
	-data.dir="$QUERIER_DATA" \
	-storage.backend=fs \
	-storage.fs.dir="$OBJ_DIR"
QUERIER_PID=$STARTED_PID
QUERIER_URL=$(wait_ready "$QUERIER_LOG" "$QUERIER_PID")
expect_queries "$QUERIER_URL"
log "querier-only process at $QUERIER_URL returns the same lines"

STEP=catalog-rows
rows=$(run_psql -c "SELECT count(*) FROM $SCHEMA.files") || fail "psql could not count files"
[[ $rows =~ ^[0-9]+$ ]] && ((rows > 0)) || fail "files table in $SCHEMA has '$rows' rows, want > 0"
log "files table holds $rows row(s)"

STEP=shutdown
sigterm_and_wait "$QUERIER_PID"
QUERIER_PID=
sigterm_and_wait "$ALL_PID"
ALL_PID=
