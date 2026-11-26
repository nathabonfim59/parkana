#!/usr/bin/env bash
# redaction-scan.sh: find secrets and personal data in Parkana log samples and
# build ingest redaction pipelines for them, without printing a raw value.
#
# Every pattern runs inside Parkana (POST /api/v1/pipelines/dry-run), so the
# matching is exactly what an ingest pipeline will do. Nothing is written to
# Parkana: no push, no pipeline create, update or delete. Credentials, if any,
# come from a curl config file, never from the command line.
set -euo pipefail

usage() {
	cat <<'EOF'
Usage: redaction-scan.sh COMMAND [options]

Commands:
  sample   --selector LOGQL --start TIME --end TIME --out FILE [--limit N] [--needle TEXT]
           Reads the newest entries of one bounded window with query_range
           and saves them as dry-run entries (one JSON object per line, mode
           0600). Prints only how many entries it saved.
  import   --stream NAME --in TEXTFILE --out FILE
           Turns a local file with one log line per line into the same
           entry format, labelled service_name=NAME.
  scan     --in FILE [--examples N]
           Reports, per catalog pattern, how many lines and metadata values
           it matches, which field names look sensitive, and up to N
           examples (default 5) masked by the catalog and those fields.
  propose  --in FILE --name NAME --selector DATASET [--patterns ID,...] [--fields NAME,...]
           Prints an ingest pipeline definition with redact steps for what
           scan found (or for the given patterns and fields). Add parse or
           promote steps after these steps, never before.
  check    --def FILE --in FILE [--examples N]
           Dry-runs a definition over the samples and scans the result.
           Exit status 3 when anything sensitive is left.

TIME is RFC 3339 (2026-11-24T10:00:00Z), Unix seconds, or "now". Without
GNU date (BSD, macOS) an RFC 3339 TIME must be UTC and end in Z.

Options:
  --max-window DUR    longest sample window (default $PARKANA_MAX_WINDOW or 1h);
                      DUR is a number with s, m, h or d
  -h, --help          show this help

Environment (read by this script only, not by the Parkana server):
  PARKANA_URL          base URL (default http://localhost:3100)
  PARKANA_CURL_CONFIG  curl config file with credentials (passed with -K)
  PARKANA_MAX_WINDOW   default for --max-window
  PARKANA_CATALOG      pattern catalog (default ../patterns.json next to this script)

Exit status: 0 on success, 1 on an HTTP or network error, 2 when the
arguments are wrong or a request is refused, 3 when check finds residue.
EOF
}

die() {
	printf 'redaction-scan: %s\n' "$1" >&2
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

here=$(cd "$(dirname "$0")" && pwd)
catalog=${PARKANA_CATALOG:-$here/../patterns.json}
url=${PARKANA_URL:-http://localhost:3100}
chunk=200

command -v jq >/dev/null || die "jq is required"
command -v curl >/dev/null || die "curl is required"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
umask 077

curl_base() {
	local args=(-sS --fail-with-body)
	if [[ -n ${PARKANA_CURL_CONFIG:-} ]]; then
		[[ -r $PARKANA_CURL_CONFIG ]] || die "cannot read PARKANA_CURL_CONFIG"
		args=(-K "$PARKANA_CURL_CONFIG" "${args[@]}")
	fi
	printf '%s\0' "${args[@]}"
}
mapfile -d '' CURL < <(curl_base)

# dryrun DEF ENTRIES prints the dry-run output entries of the definition in
# file DEF over the entries in file ENTRIES (JSON lines), one per line, sent
# in chunks of $chunk. The dropped count of all chunks goes to $work/dropped.
dryrun() {
	local def=$1 entries=$2 part body out dropped=0
	rm -f "$work"/chunk.*
	split -l "$chunk" -a 5 "$entries" "$work/chunk."
	for part in "$work"/chunk.*; do
		[[ -e $part ]] || continue
		body=$work/body.json out=$work/out.json
		jq -s --slurpfile def "$def" '{pipeline: $def[0], entries: .}' "$part" >"$body"
		if ! curl "${CURL[@]}" -X POST "$url/api/v1/pipelines/dry-run" \
			-H 'Content-Type: application/json' --data-binary "@$body" -o "$out"; then
			# The error text is Parkana's (validation or limits), never an entry.
			printf '\nredaction-scan: dry-run failed\n' >&2
			exit 1
		fi
		jq -c '.entries[]' "$out"
		dropped=$((dropped + $(jq '.dropped' "$out")))
	done
	printf '%s\n' "$dropped" >"$work/dropped"
}

# def_of STEPS_JSON prints a scan definition (on_error keep, so outputs map
# one to one onto inputs) with the given steps.
def_of() {
	jq -n --argjson steps "$1" '{name: "redaction-scan", stage: "ingest", selector: "*", steps: $steps}'
}

# field_steps NAMES_JSON prints the redact steps that mask the values of the
# given normalized field names in logfmt/key=value and JSON form; _ in a
# name matches _, . or - in the line.
field_steps() {
	jq -c --argjson names "$1" --arg oe "$2" -n '
		[$names[] | (gsub("_"; "[._-]")) as $n |
			{type: "redact", pattern: ("(?i)\\b(" + $n + "=)(?:\"(?:[^\"\\\\]|\\\\.)*\"|[^\\s&\"]+)"), replace: "${1}***", on_error: $oe},
			{type: "redact", pattern: ("(?i)(\"" + $n + "\"\\s*:\\s*)\"(?:[^\"\\\\]|\\\\.)*\""), replace: "${1}\"***\"", on_error: $oe}]'
}

# catalog_steps ON_ERROR [IDS_JSON] prints the catalog's redact steps in
# catalog order: every non-variant pattern, or the listed ids.
catalog_steps() {
	jq -c --arg oe "$1" --argjson ids "${2:-null}" '
		[.patterns[] | select(if $ids == null then .variant_of == null else (.id as $i | $ids | index($i)) end)
		 | {type: "redact", pattern, replace, on_error: $oe}]' "$catalog"
}

# detect ENTRIES writes $work/detect.json: per pattern the lines and
# metadata values it changes, and the field names that look sensitive with
# how many of their values the catalog leaves unmasked.
detect() {
	local entries=$1 id steps
	jq -c '{line, labels, meta: {}}' "$entries" >"$work/lines.jsonl"
	# Metadata values and stream labels are scanned as "name=value" lines;
	# labels are reported as label:<name>.
	jq -c '. as $e | ((.meta // {}) | to_entries[]), ((.labels // {}) | to_entries[] | .key |= "label:" + .)
		| {line: (.key + "=" + .value), labels: $e.labels, meta: {}}' "$entries" >"$work/metalines.jsonl"
	jq -r '((.meta // {}) | to_entries[]), ((.labels // {}) | to_entries[] | .key |= "label:" + .) | .key' "$entries" >"$work/metakeys.txt"
	printf '{}' >"$work/counts.json"
	for id in $(jq -r '.patterns[] | select(.variant_of == null) | .id' "$catalog"); do
		steps=$(catalog_steps keep "[\"$id\"]")
		def_of "$steps" >"$work/def.json"
		dryrun "$work/def.json" "$work/lines.jsonl" >"$work/l.out"
		dryrun "$work/def.json" "$work/metalines.jsonl" >"$work/m.out"
		jq -n --arg id "$id" --slurpfile c "$work/counts.json" \
			--slurpfile li "$work/lines.jsonl" --slurpfile lo "$work/l.out" \
			--slurpfile mi "$work/metalines.jsonl" --slurpfile mo "$work/m.out" \
			--rawfile mk "$work/metakeys.txt" '
			($mk | split("\n") | map(select(. != ""))) as $keys |
			$c[0] + {($id): {
				lines: ([range($li | length)] | map(select($li[.].line != $lo[.].line)) | length),
				meta: ([range($mi | length)] | map(select($mi[.].line != $mo[.].line) | $keys[.])
					| group_by(.) | map({(.[0]): length}) | add // {})}}' >"$work/counts.next"
		mv "$work/counts.next" "$work/counts.json"
	done
	# Field names: parse each line as JSON and as logfmt after the catalog
	# masked it, plus the metadata names the entries already carry.
	steps=$(catalog_steps keep)
	def_of "$(jq -c '. + [{type: "parse_json"}]' <<<"$steps")" >"$work/def.json"
	dryrun "$work/def.json" "$work/lines.jsonl" >"$work/pj.out"
	def_of "$(jq -c '. + [{type: "parse_logfmt"}]' <<<"$steps")" >"$work/def.json"
	dryrun "$work/def.json" "$work/lines.jsonl" >"$work/pl.out"
	def_of "$steps" >"$work/def.json"
	dryrun "$work/def.json" "$work/metalines.jsonl" >"$work/m.out"
	jq -n --slurpfile cat "$catalog" --slurpfile c "$work/counts.json" \
		--slurpfile pj "$work/pj.out" --slurpfile pl "$work/pl.out" --slurpfile mo "$work/m.out" \
		--rawfile mk "$work/metakeys.txt" '
		$cat[0].field_names.pattern as $re |
		def masked: test("^(\\*\\*\\*|\"\\*\\*\\*\"|<[a-z0-9-]+>)$") or startswith("***") or endswith("***");
		def parsed($o): [$o[] | .meta | select(.pipeline_error == null) | to_entries[]];
		($mk | split("\n") | map(select(. != ""))) as $keys |
		([parsed($pj), parsed($pl)] | add) as $fromline |
		([range($mo | length)] | map({key: $keys[.], value: ($mo[.].line | sub("^[^=]*="; "")), meta: true})) as $frommeta |
		{patterns: $c[0],
		 fields: ([$fromline[] | . + {meta: false}] + $frommeta
			| map(select(.key | sub("^label:"; "") | test($re)))
			| group_by(.key)
			| map({(.[0].key): {
				seen: length,
				unmasked: (map(select(.value | masked | not)) | length),
				metadata: (any(.[]; .meta)),
				min_len: (map(.value | length) | min),
				max_len: (map(.value | length) | max)}})
			| add // {})}' >"$work/detect.json"
}

# display ENTRIES N [placeholders] prints up to N lines masked by the
# catalog, the flagged fields and a mask for long token-like strings, cut to
# 200 characters. It shows only lines the display masks changed or, with
# "placeholders", lines that already hold a placeholder (the output of a
# redaction pipeline), so a line nothing was found in is never printed.
display() {
	local entries=$1 n=$2 mode=${3:-} fields steps
	fields=$(jq -c '.fields | keys' "$work/detect.json")
	steps=$(jq -c -n --argjson a "$(catalog_steps keep)" --argjson b "$(field_steps "$fields" keep)" \
		'$a + $b + [{type: "redact", pattern: "[A-Za-z0-9+/_=.-]{24,}", replace: "<long-string>", on_error: "keep"}]')
	def_of "$steps" >"$work/def.json"
	jq -c '{line, labels, meta: {}}' "$entries" >"$work/lines.jsonl"
	dryrun "$work/def.json" "$work/lines.jsonl" >"$work/d.out"
	jq -n -r --argjson n "$n" --arg mode "$mode" --slurpfile i "$work/lines.jsonl" --slurpfile o "$work/d.out" '
		[range($i | length) | select($i[.].line != $o[.].line
			or ($mode == "placeholders" and ($i[.].line | test("\\*\\*\\*|<[a-z0-9-]+>"))))
		 | $o[.].line][:$n][]
		| if length > 200 then .[:200] + "..." else . end | "  " + .'
}

report() {
	jq -r '
		"pattern                 lines  metadata",
		(.patterns | to_entries[] | select(.value.lines > 0 or (.value.meta | length) > 0)
		 | "\(.key | . + " " * (22 - length)) \(.value.lines | tostring | . + " " * (6 - length)) \(.value.meta | to_entries | map("\(.key):\(.value)") | join(",") | if . == "" then "-" else . end)"),
		"",
		"field (by name)         seen   unmasked  metadata  value length",
		(.fields | to_entries[]
		 | "\(.key | . + " " * (22 - length)) \(.value.seen | tostring | . + " " * (6 - length)) \(.value.unmasked | tostring | . + " " * (9 - length)) \(.value.metadata | tostring | . + " " * (9 - length)) \(.value.min_len)-\(.value.max_len)")
	' "$work/detect.json"
}

check_in() {
	[[ -n $1 ]] || die "--in is required"
	[[ -r $1 ]] || die "cannot read $1"
	jq -e -s 'all(.[]; type == "object" and (.line | type) == "string")' "$1" >/dev/null ||
		die "$1 is not one JSON entry per line (use sample or import)"
}

cmd=${1:-}
[[ -n $cmd ]] || {
	usage >&2
	exit 2
}
shift
selector='' start='' end='' out='' in='' limit=200 needle='' stream='' examples=5
name='' def='' patterns='' fields=''
max_window=${PARKANA_MAX_WINDOW:-1h}
while (($#)); do
	case $1 in
	-h | --help)
		usage
		exit 0
		;;
	--selector | --start | --end | --out | --in | --limit | --needle | --stream | --examples | --name | --def | --patterns | --fields | --max-window)
		(($# >= 2)) || die "$1 needs a value"
		case $1 in
		--selector) selector=$2 ;;
		--start) start=$2 ;;
		--end) end=$2 ;;
		--out) out=$2 ;;
		--in) in=$2 ;;
		--limit) limit=$2 ;;
		--needle) needle=$2 ;;
		--stream) stream=$2 ;;
		--examples) examples=$2 ;;
		--name) name=$2 ;;
		--def) def=$2 ;;
		--patterns) patterns=$2 ;;
		--fields) fields=$2 ;;
		--max-window) max_window=$2 ;;
		esac
		shift 2
		;;
	*) die "unknown argument $1 (see --help)" ;;
	esac
done
case $examples in '' | *[!0-9]*) die "--examples must be a number" ;; esac
[[ -r $catalog ]] || die "cannot read the pattern catalog $catalog"

case $cmd in
-h | --help)
	usage
	;;
sample)
	[[ -n $selector && -n $start && -n $end && -n $out ]] || die "sample needs --selector, --start, --end and --out"
	case $limit in '' | *[!0-9]* | 0) die "--limit must be a positive integer" ;; esac
	((limit <= 1000)) || die "--limit is at most 1000 (the dry-run maximum)"
	[[ ! -e $out ]] || die "$out exists; samples are never overwritten"
	s=$(to_epoch "$start")
	e=$(to_epoch "$end")
	now=$(date +%s)
	max=$(to_seconds "$max_window")
	((s < e)) || die "start must be before end"
	((s <= now)) || die "the window starts in the future"
	((e <= now + 60)) || die "the window ends in the future; use --end now"
	((e - s <= max)) || die "window of $((e - s))s is longer than --max-window $max_window; sample a shorter window"
	query=$selector
	[[ -z $needle ]] || query="$selector |= $(jq -n --arg n "$needle" '$n')"
	curl "${CURL[@]}" -G "$url/loki/api/v1/query_range" \
		-H 'X-Loki-Response-Encoding-Flags: categorize-labels' \
		--data-urlencode "query=$query" --data-urlencode "start=$s" --data-urlencode "end=$e" \
		--data-urlencode "limit=$limit" --data-urlencode direction=backward \
		-o "$work/q.json" || {
		printf '\nredaction-scan: query failed\n' >&2
		exit 1
	}
	jq -e '.data.resultType == "streams"' "$work/q.json" >/dev/null || die "the selector must be a log query, not a metric query"
	jq -c '.data.result[] | .stream as $l | .values[] | {line: .[1], labels: $l, meta: ((.[2] // {}).structuredMetadata // {})}' "$work/q.json" >"$out"
	printf 'saved %s entries to %s\n' "$(wc -l <"$out" | tr -d ' ')" "$out"
	;;
import)
	[[ -n $stream && -n $in && -n $out ]] || die "import needs --stream, --in and --out"
	[[ -r $in ]] || die "cannot read $in"
	[[ ! -e $out ]] || die "$out exists; samples are never overwritten"
	jq -R -c --arg s "$stream" 'select(. != "") | {line: ., labels: {service_name: $s}, meta: {}}' "$in" >"$out"
	n=$(wc -l <"$out" | tr -d ' ')
	((n <= 1000)) || printf 'redaction-scan: %s has %s entries; scan works in chunks, but keep samples small\n' "$out" "$n" >&2
	printf 'saved %s entries to %s\n' "$n" "$out"
	;;
scan)
	check_in "$in"
	printf 'samples: %s entries, %s metadata values\n\n' "$(wc -l <"$in" | tr -d ' ')" "$(jq -s 'map(.meta // {} | length) | add // 0' "$in")"
	detect "$in"
	report
	printf '\nexamples (masked by the catalog, the fields above and long strings):\n'
	display "$in" "$examples"
	;;
propose)
	check_in "$in"
	[[ -n $name && -n $selector ]] || die "propose needs --name and --selector"
	detect "$in"
	if [[ -n $patterns ]]; then
		ids=$(jq -c -n --arg p "$patterns" '$p | split(",")')
	else
		ids=$(jq -c '[.patterns | to_entries[] | select(.value.lines > 0) | .key]' "$work/detect.json")
	fi
	if [[ -n $fields ]]; then
		names=$(jq -c -n --arg f "$fields" '$f | split(",")')
	else
		names=$(jq -c '[.fields | to_entries[] | select((.value.metadata | not) and .value.unmasked > 0) | .key]' "$work/detect.json")
	fi
	# Metadata the entries already carry: one step per field and pattern
	# found in it, and a whole-value mask for sensitive field names.
	meta_steps=$(jq -c --slurpfile cat "$catalog" '
		[(.patterns | to_entries[] | .key as $id | .value.meta | keys[] | select(startswith("label:") | not) | {id: $id, field: .})] as $hits |
		[$cat[0].patterns[] | . as $p | $hits[] | select(.id == $p.id)
		 | {type: "redact", pattern: $p.pattern, replace: $p.replace, target: .field, on_error: "drop"}]
		+ [.fields | to_entries[] | select(.value.metadata and .value.unmasked > 0 and (.key | startswith("label:") | not)) | {type: "redact", pattern: "(?s).+", replace: "***", target: .key, on_error: "drop"}]' "$work/detect.json")
	labels=$(jq -r '[(.patterns[] | .meta | keys[]), (.fields | to_entries[] | select(.value.unmasked > 0) | .key)] | map(select(startswith("label:"))) | unique | join(", ")' "$work/detect.json")
	[[ -z $labels ]] || printf 'redaction-scan: labels a redact step cannot change: %s (drop them at the shipper, or overwrite them with a promote step)\n' "$labels" >&2
	jq -n --arg name "$name" --arg sel "$selector" \
		--argjson a "$(catalog_steps drop "$ids")" --argjson b "$(field_steps "$names" drop)" --argjson c "$meta_steps" \
		'{name: $name, stage: "ingest", selector: $sel, steps: ($a + $b + $c)}' >"$work/proposal.json"
	n=$(jq '.steps | length' "$work/proposal.json")
	((n > 0)) || die "nothing to redact in the lines or metadata of these samples"
	((n <= 32)) || die "$n steps is more than the 32 a pipeline allows; pass fewer --patterns or --fields and put the rest in a second pipeline"
	cat "$work/proposal.json"
	;;
check)
	check_in "$in"
	[[ -n $def && -r $def ]] || die "check needs a readable --def"
	jq -e '.stage == "ingest"' "$def" >/dev/null || die "the definition is not an ingest pipeline"
	dryrun "$def" "$in" >"$work/after.jsonl"
	dropped=$(cat "$work/dropped")
	errors=$(jq -s 'map(select(.meta.pipeline_error != null)) | length' "$work/after.jsonl")
	printf 'entries: %s in, %s out, %s dropped, %s with pipeline_error\n\n' \
		"$(wc -l <"$in" | tr -d ' ')" "$(wc -l <"$work/after.jsonl" | tr -d ' ')" "$dropped" "$errors"
	printf 'left after the pipeline:\n'
	detect "$work/after.jsonl"
	report
	residue=$(jq '([.patterns[] | .lines + (.meta | add // 0)] | add) + ([.fields[] | .unmasked] | add // 0)' "$work/detect.json")
	printf '\nexamples after the pipeline, still masked for display:\n'
	display "$work/after.jsonl" "$examples" placeholders
	if ((residue > 0)); then
		printf '\nRESIDUE: %s values the catalog or the field names still find\n' "$residue"
		exit 3
	fi
	printf '\nno residue found\n'
	;;
*) die "unknown command $cmd (see --help)" ;;
esac
