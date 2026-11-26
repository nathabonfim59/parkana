#!/usr/bin/env bash
# parkana-query.sh: run one bounded LogQL query_range against Parkana.
#
# It refuses queries that read many files for nothing: windows longer than a
# maximum, windows in the future, and LogQL without a |= needle of at least
# 4 bytes (the search-sidecar gram length). Credentials, if any, come from a
# curl config file, never from the command line.
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: parkana-query.sh --start TIME --end TIME [options] 'LOGQL'

Runs GET /loki/api/v1/query_range and prints the JSON response.

TIME is RFC 3339 (2026-11-24T10:00:00Z), Unix seconds, or "now". Without
GNU date (BSD, macOS) an RFC 3339 TIME must be UTC and end in Z.

Options:
  --start TIME        window start (required)
  --end TIME          window end, exclusive (required)
  --limit N           maximum entries (default 100)
  --direction DIR     backward (default, newest first) or forward
  --step DURATION     step for metric queries (passed through as given)
  --max-window DUR    longest window allowed (default $PARKANA_MAX_WINDOW or 6h);
                      DUR is a number with s, m, h or d
  --lock FILE         hold an flock on FILE so only one query runs at a time
  -h, --help          show this help

Environment (read by this script only, not by the Parkana server):
  PARKANA_URL          base URL (default http://localhost:3100)
  PARKANA_CURL_CONFIG  curl config file with credentials (passed with -K)
  PARKANA_MAX_WINDOW   default for --max-window

Exit status: 0 on HTTP 2xx, 1 on an HTTP or network error, 2 when the query
is refused or the arguments are wrong.
EOF
}

die() {
	printf 'parkana-query: %s\n' "$1" >&2
	exit 2
}

# parse_time TIME prints TIME as Unix seconds with GNU date (-d), or, where
# date has no -d (BSD and macOS), with BSD date -j for UTC RFC 3339 times
# ending in Z, fractional seconds dropped. It fails when neither parses.
parse_time() {
	local t=$1
	date -d "$t" +%s 2>/dev/null && return 0
	if [[ $t =~ ^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(\.[0-9]+)?Z$ ]]; then
		date -j -u -f '%Y-%m-%dT%H:%M:%SZ' "${BASH_REMATCH[1]}Z" +%s 2>/dev/null && return 0
	fi
	return 1
}

# to_epoch TIME prints TIME as Unix seconds.
to_epoch() {
	case $1 in
	now) date +%s ;;
	*[!0-9]*) parse_time "$1" || die "cannot parse time '$1' (without GNU date, use UTC RFC 3339 with Z: 2026-11-24T10:00:00Z)" ;;
	'') die "empty time" ;;
	*) printf '%s\n' "$1" ;;
	esac
}

# to_seconds DUR prints a duration such as 90s, 30m, 6h or 2d in seconds.
to_seconds() {
	local n=${1%[smhd]} unit=${1##*[0-9]}
	case $n in '' | *[!0-9]*) die "bad duration '$1' (use a number with s, m, h or d)" ;; esac
	case $unit in
	s | '') printf '%s\n' "$n" ;;
	m) printf '%s\n' $((n * 60)) ;;
	h) printf '%s\n' $((n * 3600)) ;;
	d) printf '%s\n' $((n * 86400)) ;;
	*) die "bad duration '$1'" ;;
	esac
}

# longest_needle QUERY prints the byte length of the longest |= "..." or
# |= `...` needle in QUERY, or 0 when there is none.
longest_needle() {
	local best=0 lit body found
	# The pattern is a grep regexp, not shell: single quotes are intended.
	# shellcheck disable=SC2016
	found=$(printf '%s\n' "$1" | grep -oE '\|=[[:space:]]*("([^"\\]|\\.)*"|`[^`]*`)' || true)
	while IFS= read -r lit; do
		[[ -n $lit ]] || continue
		body=${lit#*[\"\`]}
		body=${body%[\"\`]}
		body=$(printf '%b' "$body")
		local n
		n=$(printf '%s' "$body" | LC_ALL=C wc -c)
		n=${n//[!0-9]/}
		((n > best)) && best=$n
	done <<<"$found"
	printf '%s\n' "$best"
}

start='' end='' limit=100 direction=backward step='' lock=''
max_window=${PARKANA_MAX_WINDOW:-6h}
query=''
while (($#)); do
	case $1 in
	--start | --end | --limit | --direction | --step | --max-window | --lock)
		(($# >= 2)) || die "$1 needs a value"
		case $1 in
		--start) start=$2 ;;
		--end) end=$2 ;;
		--limit) limit=$2 ;;
		--direction) direction=$2 ;;
		--step) step=$2 ;;
		--max-window) max_window=$2 ;;
		--lock) lock=$2 ;;
		esac
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	--)
		(($# == 2)) || die "-- must be followed by exactly one query"
		query=$2
		shift 2
		;;
	-*) die "unknown option $1 (see --help)" ;;
	*)
		[[ -z $query ]] || die "one query only"
		query=$1
		shift
		;;
	esac
done

[[ -n $query ]] || die "no LogQL query given (see --help)"
[[ -n $start && -n $end ]] || die "--start and --end are required"
case $limit in '' | *[!0-9]* | 0) die "--limit must be a positive integer" ;; esac
case $direction in backward | forward) ;; *) die "--direction must be backward or forward" ;; esac

s=$(to_epoch "$start")
e=$(to_epoch "$end")
now=$(date +%s)
max=$(to_seconds "$max_window")
((s < e)) || die "start must be before end"
((s <= now)) || die "the window starts in the future"
((e <= now + 60)) || die "the window ends in the future; use --end now"
((e - s <= max)) || die "window of $((e - s))s is longer than --max-window $max_window; search it in steps"

needle=$(longest_needle "$query")
((needle >= 4)) || die 'the query has no |= needle of 4 bytes or more, so no file can be skipped; add |= "literal" (see the skill'"'"'s What prunes table)'

url=${PARKANA_URL:-http://localhost:3100}
args=(-sS -G --fail-with-body "$url/loki/api/v1/query_range"
	--data-urlencode "query=$query"
	--data-urlencode "start=$s" --data-urlencode "end=$e"
	--data-urlencode "limit=$limit" --data-urlencode "direction=$direction")
[[ -z $step ]] || args+=(--data-urlencode "step=$step")
if [[ -n ${PARKANA_CURL_CONFIG:-} ]]; then
	[[ -r $PARKANA_CURL_CONFIG ]] || die "cannot read PARKANA_CURL_CONFIG"
	args=(-K "$PARKANA_CURL_CONFIG" "${args[@]}")
fi

run() {
	curl "${args[@]}" || exit 1
	echo
}
if [[ -n $lock ]]; then
	command -v flock >/dev/null || die "--lock needs flock(1)"
	exec 9>"$lock"
	flock -w 300 9 || die "could not take the lock on $lock"
fi
run
