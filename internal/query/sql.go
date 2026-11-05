// SQL compilation for the querier's plan.Source: a Selection over the
// listed object groups becomes one fully parameterized DuckDB statement.
// The SQL text contains only Parkana's own keywords, constant column names
// and identifiers taken from the trusted catalog (promoted label names);
// every client-controlled value is a bound parameter.
package query

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/schema"
)

// selectQuery is one compiled statement: parameterized SQL plus the arguments
// for its placeholders, in order.
type selectQuery struct {
	sql  string
	args []any
}

// compiler assembles a selectQuery, keeping placeholders and arguments in
// lockstep.
type compiler struct {
	b    strings.Builder
	args []any
}

func (c *compiler) printf(format string, args ...any) {
	fmt.Fprintf(&c.b, format, args...)
}

// bind appends a ? placeholder bound to v.
func (c *compiler) bind(v any) {
	c.printf("?")
	c.args = append(c.args, v)
}

// quoteIdent quotes name as one DuckDB identifier, doubling any embedded
// double quotes. It is the only place an identifier is spliced into SQL, and
// it is called only with Parkana's constant column names and promoted label
// names taken from the catalog — never with raw client strings, which reach
// SQL as bound parameters or not at all.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// placeholders returns "?, ?, ..." with n placeholders.
func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// fileGroup is one group of listed objects sharing one promoted-label set:
// one physical schema shape (the same column names), so one read_parquet
// over the group's paths binds without DuckDB's case-insensitive identifier
// folding and one projection normalizes every row of the group. Grouping
// keeps planning linear in distinct schemas rather than file count.
type fileGroup struct {
	// promoted holds the group's promoted label names, sorted, exact case:
	// the keys of catalog File.LabelValues. The schema contract guarantees
	// they neither collide with each other nor fold onto base column names,
	// so each quoted reference is unambiguous inside the group.
	promoted []string
	// paths are the group's local object paths, in listing order.
	paths []string
}

// selectSQL compiles a validated sel over groups into the Select statement:
//
//	SELECT "_ts", "line", "labels_json", "meta_json" FROM (
//	  SELECT DISTINCT "_ts", "line", labels_json, meta_json
//	  FROM ( <normalized group subqueries, UNION ALL'd> )
//	) ORDER BY "_ts" ASC|DESC LIMIT ?
//
// The DISTINCT over the complete stored row identity (canonical metadata
// included) runs before the blocking ORDER BY and LIMIT, so at-least-once
// collector retries never consume a limit slot. The single ORDER BY over the
// union keeps the blocking sort (and with it Select's retry boundary: every
// input file is read before the first row). Filenames, matcher names and
// values, regexp patterns, line filter values, label names in the canonical
// map, both timestamp bounds and the limit are bound parameters.
func selectSQL(sel plan.Selection, groups []fileGroup) selectQuery {
	c := compiler{}
	ts, line := quoteIdent(schema.ColTS), quoteIdent(schema.ColLine)
	c.printf("SELECT %s, %s, labels_json, meta_json FROM (", ts, line)
	distinctNormRelation(&c, sel, groups)
	c.printf(")")
	c.printf(" ORDER BY %s", ts)
	if sel.Direction == plan.Backward {
		c.printf(" DESC")
	} else {
		c.printf(" ASC")
	}
	if sel.Limit > 0 {
		c.printf(" LIMIT ")
		c.bind(sel.Limit)
	}
	return selectQuery{sql: c.b.String(), args: c.args}
}

// normRelation compiles sel's normalized relation into c: one subquery per
// group, all UNION ALL'd by position, each projecting the group's rows as
//
//	"_ts", "line", labels_json, meta_json
//
// where labels_json is the row's COMPLETE label set as a canonically
// key-sorted, case-sensitive map JSON, built inside the group: the labels
// map entries plus the group's promoted columns (a NULL promoted column
// contributes nothing, a present one wins over a same-named map entry).
// meta_json is the stored metadata map in its physical order (no
// canonicalization), which the raw-order proof in the aggregate metadata
// tests reads. Equal label sets therefore produce equal labels_json values
// regardless of which labels a file promoted, so B1c's Aggregate can GROUP
// BY labels_json and B2c's metadata can SELECT DISTINCT it over this same
// relation. The selection's matchers, line filters and time range are
// pushed down into every group.
func normRelation(c *compiler, sel plan.Selection, groups []fileGroup) {
	relation(c, sel, groups, false)
}

// relation compiles the normalized relation into c. When canonicalMeta is
// true it projects each group's metadata already canonicalized (see
// canonicalMetaExpr), which is exactly the row-identity form; when false it
// keeps the raw stored order for normRelation's raw-order callers.
func relation(c *compiler, sel plan.Selection, groups []fileGroup, canonicalMeta bool) {
	for gi, g := range groups {
		if gi > 0 {
			c.printf(" UNION ALL ")
		}
		c.groupSubquery(sel, g, canonicalMeta)
	}
}

// canonicalMetaExpr canonicalizes one row's structured metadata directly from
// the stored MAP column: NULL and empty maps coalesce to the empty map, the
// entries are sorted by key (and then value) and the rebuilt map is serialized
// once. Equal metadata therefore yields one string whatever the source map
// order, while a present key with an empty value keeps its entry and stays
// distinct from an absent key. A NULL map value stays a JSON null, distinct
// from an empty string. The expression references only the trusted meta
// column and binds nothing.
func canonicalMetaExpr(metaCol string) string {
	return "CAST(to_json(map_from_entries(list_sort(map_entries(coalesce(" + metaCol + ", map([], [])))))) AS VARCHAR)"
}

// distinctNormRelation compiles sel's normalized relation into c wrapped in a
// DISTINCT over the complete stored-row identity:
//
//	SELECT DISTINCT "_ts", "line", labels_json, meta_json
//	FROM ( <normalized group subqueries, UNION ALL'd> )
//
// Two rows are one stored row exactly when their TS, complete canonical label
// set, canonical metadata and line are equal, so retries delivered at least
// once collapse regardless of which file promoted which label or in which
// order the Parquet map stored the metadata. Each group computes the
// canonical metadata from its stored MAP and serializes it once
// (canonicalMetaExpr), so no JSON is parsed back into a map. The projection
// keeps metadata in the identity even when Aggregate without IncludeMeta does
// not return or group by it; Select returns it, and both methods deduplicate
// on it. Deduplicating by labels and line alone would erase rows that differ
// only in metadata. DISTINCT is blocking: it consumes every selected row
// before a caller can order, limit or group, which is the required placement.
func distinctNormRelation(c *compiler, sel plan.Selection, groups []fileGroup) {
	ts, line := quoteIdent(schema.ColTS), quoteIdent(schema.ColLine)
	c.printf("SELECT DISTINCT %s, %s, labels_json, meta_json FROM (", ts, line)
	relation(c, sel, groups, true)
	c.printf(")")
}

// groupSubquery compiles one group's normalized subquery. canonicalMeta
// selects the row-identity metadata projection (canonicalMetaExpr) instead of
// the raw physical-order JSON normRelation callers read.
func (c *compiler) groupSubquery(sel plan.Selection, g fileGroup, canonicalMeta bool) {
	ts, line := quoteIdent(schema.ColTS), quoteIdent(schema.ColLine)
	labelsCol, metaCol := quoteIdent(schema.ColLabels), quoteIdent(schema.ColMeta)

	c.printf("SELECT %s, %s, ", ts, line)
	c.canonicalLabelsExpr(labelsCol, g.promoted)
	c.printf(" AS labels_json, ")
	if canonicalMeta {
		c.printf("%s", canonicalMetaExpr(metaCol))
	} else {
		c.printf("CAST(to_json(%s) AS VARCHAR)", metaCol)
	}
	c.printf(" AS meta_json")

	c.printf(" FROM read_parquet([%s], union_by_name = true)", placeholders(len(g.paths)))
	for _, p := range g.paths {
		c.args = append(c.args, p)
	}

	// WHERE TRUE keeps the predicate list valid when a caller passes no
	// matchers and no line filters (the private metadata reader); every
	// predicate then prefixes AND.
	c.printf(" WHERE TRUE")
	for _, m := range sel.Matchers {
		c.printf(" AND ")
		c.matcherPred(m, g)
	}
	for _, f := range sel.LineFilters {
		c.printf(" AND ")
		c.lineFilterPred(f)
	}
	// Stored _ts values are whole microseconds; bounds are rounded up to the
	// µs grid so nanosecond boundaries never truncate onto the wrong side.
	c.printf(" AND %s >= ", ts)
	c.bind(time.UnixMicro(ceilMicro(sel.From)).UTC())
	c.printf(" AND %s < ", ts)
	c.bind(time.UnixMicro(ceilMicro(sel.Through)).UTC())
}

// canonicalLabelsExpr appends the complete, canonical label-map expression:
// the labels map's entries (minus any promoted names) concatenated with the
// group's promoted columns as struct entries, sorted by key and rebuilt as
// a map. Each promoted entry's value is the column coalesced with the
// labels-map entry of the same name: a present column wins, a NULL column
// falls back to the map value, and only when both are absent is the label
// omitted (the entry drops out as NULL). Key names are bound parameters;
// the value side references only trusted catalog identifiers. With no
// promoted labels the labels map is only re-sorted. (Within one file the
// ingester never stores a promoted label in the map, so the overlap rules
// only matter for hand-written files.)
func (c *compiler) canonicalLabelsExpr(labelsCol string, promoted []string) {
	c.printf("CAST(to_json(coalesce(map_from_entries(")
	if len(promoted) == 0 {
		c.printf("list_sort(map_entries(%s))", labelsCol)
		c.printf("), map([], []))) AS VARCHAR)")
		return
	}
	c.printf("list_sort(list_concat(")
	c.printf("list_filter(map_entries(%s), lambda e: NOT list_contains(", labelsCol)
	c.bind(promoted)
	c.printf(", e.key)), ")
	c.printf("list_filter([")
	for i, name := range promoted {
		if i > 0 {
			c.printf(", ")
		}
		c.printf("{'key': ")
		c.bind(name)
		c.printf(", 'value': coalesce(%s, ", quoteIdent(name))
		c.mapExpr(name)
		c.printf(")}")
	}
	c.printf("], lambda e: e.value IS NOT NULL)")
	c.printf("))), map([], []))) AS VARCHAR)")
}

// matcherPred appends m's predicate over the effective value of its label:
// the group's promoted column when the name matches one exactly (labels are
// case-sensitive; a NULL column falls back to the labels map), else the
// labels map entry; a missing label reads as an empty string, preserving the
// missing-label semantics of the reference Source. The map key is a bound
// parameter; = and <> compare bound values and =~ / !~ use
// regexp_full_match, which anchors the RE2 pattern the way plan.Matcher does.
//
// DuckDB refuses to bind a VARCHAR that is not valid UTF-8, while plan
// matchers may carry arbitrary bytes in names and in = / != values (Go strings
// compare as bytes; NewMatcher does not require UTF-8). An invalid-UTF8 value compares
// the effective label's bytes instead: lower(hex(<label expr>)) against
// hex.EncodeToString of the matcher value, emitted as bound lowercase ASCII.
// The result is exactly Go's byte equality, and the empty-string encoding keeps
// missing == "" intact. Regexp values cannot reach this path: Go's regexp only
// compiles valid UTF-8 patterns.
func (c *compiler) matcherPred(m plan.Matcher, g fileGroup) {
	switch m.Type {
	case plan.MatchEqual, plan.MatchNotEqual:
		op := " = "
		if m.Type == plan.MatchNotEqual {
			op = " <> "
		}
		if utf8.ValidString(m.Value) {
			c.labelExpr(m.Name, g)
			c.printf("%s", op)
			c.bind(m.Value)
			return
		}
		c.printf("lower(hex(")
		c.labelExpr(m.Name, g)
		c.printf("))%s", op)
		c.bind(hex.EncodeToString([]byte(m.Value)))
	case plan.MatchRegexp, plan.MatchNotRegexp:
		if m.Type == plan.MatchNotRegexp {
			c.printf("NOT ")
		}
		c.printf("regexp_full_match(")
		c.labelExpr(m.Name, g)
		c.printf(", ")
		c.bind(m.Value)
		c.printf(")")
	}
}

// lineFilterPred appends f's predicate over the line column: contains is
// DuckDB's byte substring test (no LIKE wildcards to escape), and line
// regexps run unanchored through regexp_matches, matching plan's |~. A
// contains needle that is not valid UTF-8 (a lone partial-rune byte, which
// DuckDB cannot bind as a VARCHAR) falls back to a byte-aligned hex regexp
// over lower(hex(line)) (lowercase, like Go's hex.EncodeToString):
// ^(?:..)* consumes whole bytes (two hex digits each) before the hex-encoded
// needle, so the match is exactly strings.Contains on raw bytes and never
// lands on an odd nibble. Regexp patterns cannot reach the fallback: Go's
// regexp only compiles valid UTF-8.
func (c *compiler) lineFilterPred(f plan.LineFilter) {
	switch f.Type {
	case plan.LineContains, plan.LineNotContains:
		if f.Type == plan.LineNotContains {
			c.printf("NOT ")
		}
		if utf8.ValidString(f.Value) {
			c.printf("contains(%s, ", quoteIdent(schema.ColLine))
			c.bind(f.Value)
			c.printf(")")
			return
		}
		c.printf("regexp_matches(lower(hex(%s)), ", quoteIdent(schema.ColLine))
		c.bind("^(?:..)*" + hex.EncodeToString([]byte(f.Value)))
		c.printf(")")
	case plan.LineRegexp, plan.LineNotRegexp:
		if f.Type == plan.LineNotRegexp {
			c.printf("NOT ")
		}
		c.printf("regexp_matches(%s, ", quoteIdent(schema.ColLine))
		c.bind(f.Value)
		c.printf(")")
	}
}

// labelExpr appends the effective value of label name: the group's promoted
// column when it promotes exactly that name (falling back to the labels map
// when the column is NULL), else the labels map entry; NULL (absent from
// both) reads as an empty string. The map key is a bound parameter, except
// for an invalid-UTF8 name, which DuckDB cannot bind: that lookup compares
// hex-encoded map keys instead (see mapHexExpr). Promoted names come from the
// Parquet schema, so they are valid UTF-8; an invalid-UTF8 name can never
// match one.
func (c *compiler) labelExpr(name string, g fileGroup) {
	if !utf8.ValidString(name) {
		c.printf("coalesce(")
		c.mapHexExpr(name)
		c.printf(", '')")
		return
	}
	if slices.Contains(g.promoted, name) {
		c.printf("coalesce(coalesce(%s, ", quoteIdent(name))
		c.mapExpr(name)
		c.printf("), '')")
		return
	}
	c.printf("coalesce(")
	c.mapExpr(name)
	c.printf(", '')")
}

// mapExpr appends the labels map entry for a valid-UTF8 name.
func (c *compiler) mapExpr(name string) {
	c.printf("%s[", quoteIdent(schema.ColLabels))
	c.bind(name)
	c.printf("]")
}

// mapHexExpr appends the labels map value for a name DuckDB cannot bind as a
// VARCHAR. It finds the key whose bytes match lower(hex(key)) == the bound
// lowercase hex of name in map-key order and extracts the value at that
// position; an absent key yields NULL, which callers coalesce to the empty
// string. Map keys are case-sensitive and unique, so this is exactly the
// labels[name] lookup semantics without feeding invalid bytes to DuckDB.
func (c *compiler) mapHexExpr(name string) {
	labelsCol := quoteIdent(schema.ColLabels)
	c.printf("list_extract(map_values(%s), list_position(list_transform(map_keys(%s), lambda k: lower(hex(k))), ", labelsCol, labelsCol)
	c.bind(hex.EncodeToString([]byte(name)))
	c.printf("))")
}

// ceilMicro rounds t up to a whole microsecond on the Unix µs grid. Stored
// _ts values are whole microseconds, so comparing them against
// ceilµs(From) / ceilµs(Through) is exactly plan's From <= ts < Through for
// nanosecond bounds: a sub-µs From still excludes the µs below it and a
// sub-µs Through still includes it. Pre-epoch times work because
// time.Time's nanosecond part is always non-negative: UnixMicro floors, and
// one bump on a sub-µs remainder is the exact ceiling.
func ceilMicro(t time.Time) int64 {
	us := t.UnixMicro()
	if t.Nanosecond()%1000 != 0 {
		us++
	}
	return us
}
