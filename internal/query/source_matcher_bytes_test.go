package query

import (
	"cmp"
	"encoding/hex"
	"fmt"
	"iter"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// matcherBytesFiles lays out one stream over both physical shapes the SQL
// compiler handles: one file promotes service_name and level, the other keeps
// both in the labels map. Stored data is valid UTF-8 throughout, including
// empty, control-byte and non-ASCII label values, so every invalid byte in
// these tests comes from the matcher, never from storage.
func matcherBytesFiles() []plantest.File {
	row := func(off time.Duration, ls map[string]string) plan.Row {
		return plan.Row{TS: plantest.T0.Add(off), Line: fmt.Sprintf("line %d", off/time.Second), Labels: logs.FromMap(ls)}
	}
	return []plantest.File{
		{
			Tenant: tenant.Default, Stream: "api", Promoted: []string{"service_name", "level"},
			Rows: []plan.Row{
				row(0, map[string]string{"service_name": "api", "level": "info", "env": "prod"}),
				row(time.Second, map[string]string{"service_name": "api", "env": "dev", "région": "münchen"}),
				row(2*time.Second, map[string]string{"service_name": "api", "level": "", "env": "pröd"}),
			},
		},
		{
			Tenant: tenant.Default, Stream: "api",
			Rows: []plan.Row{
				row(0, map[string]string{"service_name": "api", "level": "warn", "env": "pr\x13od"}),
				row(time.Second, map[string]string{"service_name": "api", "env": "prod"}),
			},
		},
	}
}

// matcherBytesCase is one selector evaluated against the same fixture by both
// the public Select and Aggregate regressions. want is the MemSource row
// count, kept explicit so an oracle that silently matched nothing cannot make
// the test vacuous.
type matcherBytesCase struct {
	name    string
	matcher plan.Matcher
	want    int
}

func matcherBytesCases() []matcherBytesCase {
	eq := func(name, value string) plan.Matcher { return plan.MustMatcher(plan.MatchEqual, name, value) }
	ne := func(name, value string) plan.Matcher { return plan.MustMatcher(plan.MatchNotEqual, name, value) }
	re := func(name, value string) plan.Matcher { return plan.MustMatcher(plan.MatchRegexp, name, value) }
	nre := func(name, value string) plan.Matcher { return plan.MustMatcher(plan.MatchNotRegexp, name, value) }
	const invalidName = "\xff"
	return []matcherBytesCase{
		// Existing semantics on valid stored values: Unicode, control bytes
		// and the empty/missing equivalence stay on the ordinary fast path.
		{name: "valid-unicode-value", matcher: eq("env", "pröd"), want: 1},
		{name: "valid-unicode-value-not", matcher: ne("env", "pröd"), want: 4},
		{name: "valid-control-byte-value", matcher: eq("env", "pr\x13od"), want: 1},
		{name: "valid-control-byte-value-not", matcher: ne("env", "pr\x13od"), want: 4},
		{name: "valid-unicode-name", matcher: eq("région", "münchen"), want: 1},
		{name: "valid-unicode-name-not", matcher: ne("région", "münchen"), want: 4},
		{name: "valid-name-is-case-sensitive", matcher: eq("LEVEL", "info"), want: 0},
		{name: "valid-name-is-case-sensitive-not", matcher: ne("LEVEL", "info"), want: 5},
		{name: "missing-equals-empty", matcher: eq("level", ""), want: 3},
		{name: "missing-not-equal-empty", matcher: ne("level", ""), want: 2},

		// Invalid-UTF8 values: equality and inequality over the label's raw
		// bytes. level is a promoted column in one file and a map entry in the
		// other; env is a map entry in both.
		{name: "invalid-value-promoted-equal", matcher: eq("level", "\xff"), want: 0},
		{name: "invalid-value-promoted-not-equal", matcher: ne("level", "\xff"), want: 5},
		{name: "invalid-value-map-equal", matcher: eq("env", "\xff"), want: 0},
		{name: "invalid-value-map-not-equal", matcher: ne("env", "\xff"), want: 5},
		{name: "invalid-value-empty-prefix", matcher: eq("env", "\x00\xff"), want: 0},

		// Invalid-UTF8 names never equal a valid stored key, so they read the
		// missing-label empty string for every matcher type. The equality and
		// inequality pair against "" is the direct missing-label check.
		{name: "invalid-name-equals-empty", matcher: eq(invalidName, ""), want: 5},
		{name: "invalid-name-not-equal-empty", matcher: ne(invalidName, ""), want: 0},
		{name: "invalid-name-equals-value", matcher: eq(invalidName, "prod"), want: 0},
		{name: "invalid-name-not-equal-value", matcher: ne(invalidName, "prod"), want: 5},
		{name: "invalid-name-regexp-match-empty", matcher: re(invalidName, ".*"), want: 5},
		{name: "invalid-name-regexp-not-match-empty", matcher: nre(invalidName, ".*"), want: 0},
		{name: "invalid-name-regexp-value", matcher: re(invalidName, "prod"), want: 0},
		{name: "invalid-name-regexp-not-value", matcher: nre(invalidName, "prod"), want: 5},

		// Both sides invalid exercise the hex value comparison and the hex
		// map-key lookup in one statement.
		{name: "invalid-name-and-value-equal", matcher: eq(invalidName, "\xfe"), want: 0},
		{name: "invalid-name-and-value-not-equal", matcher: ne(invalidName, "\xfe"), want: 5},
	}
}

// TestSelectInvalidMatcherBytes is the public Select regression for matchers
// whose name or value is not valid UTF-8. NewMatcher and Selection.Validate
// accept these bytes, and plan.MemSource compares them as raw Go strings;
// DuckDB cannot bind an invalid-UTF8 VARCHAR, so the compiler compares ASCII
// hex instead. Every case is checked against the MemSource oracle.
func TestSelectInvalidMatcherBytes(t *testing.T) {
	files := matcherBytesFiles()
	src, ref := caseTestEnv(t, files...)
	svc := plan.MustMatcher(plan.MatchEqual, "service_name", "api")
	from, through := plantest.T0.Add(-time.Hour), plantest.T0.Add(time.Hour)

	for _, tc := range matcherBytesCases() {
		t.Run(tc.name, func(t *testing.T) {
			sel := plan.Selection{
				Tenant: tenant.Default, Matchers: []plan.Matcher{svc, tc.matcher},
				From: from, Through: through, Direction: plan.Forward,
			}
			got, err := collectRows(ref.Select(t.Context(), sel))
			if err != nil {
				t.Fatalf("reference Select: %v", err)
			}
			if len(got) != tc.want {
				t.Fatalf("fixture/oracle mismatch: MemSource returned %d rows, want %d", len(got), tc.want)
			}
			assertRowsAgainstRef(t, src, ref, sel)
		})
	}
}

// TestAggregateInvalidMatcherBytes is the public Aggregate regression for the
// same invalid-UTF8 matcher bytes: Aggregate runs the identical pushed-down
// predicates over the normalized relation, so its buckets must equal the
// MemSource oracle's.
func TestAggregateInvalidMatcherBytes(t *testing.T) {
	files := matcherBytesFiles()
	src, ref := caseTestEnv(t, files...)
	svc := plan.MustMatcher(plan.MatchEqual, "service_name", "api")
	from, through := plantest.T0.Add(-time.Hour), plantest.T0.Add(time.Hour)
	agg := plan.BucketAgg{Width: time.Hour, Origin: plantest.T0}

	for _, tc := range matcherBytesCases() {
		t.Run(tc.name, func(t *testing.T) {
			sel := plan.Selection{
				Tenant: tenant.Default, Matchers: []plan.Matcher{svc, tc.matcher},
				From: from, Through: through, Direction: plan.Forward,
			}
			assertMatcherBytesBuckets(t, src, ref, sel, agg)
		})
	}
}

// TestSelectSQLInvalidMatcherBytes checks the compiled statement for invalid
// matcher bytes: neither the raw name nor the raw value may be spliced into
// SQL, both are bound as valid-UTF8 lowercase hex, the invalid-name lookup is
// the map-key hex join, and placeholders stay in lockstep with arguments.
func TestSelectSQLInvalidMatcherBytes(t *testing.T) {
	name := "\xff"
	value := "a\xfe\x80b"
	sel := plan.Selection{
		Tenant: tenant.Default,
		Matchers: []plan.Matcher{
			plan.MustMatcher(plan.MatchEqual, "service_name", "api"),
			plan.MustMatcher(plan.MatchEqual, name, value),
			plan.MustMatcher(plan.MatchRegexp, name, ".*"),
		},
		From: plantest.T0, Through: plantest.T0.Add(time.Hour),
	}
	q := selectSQL(sel, []fileGroup{{promoted: []string{"service_name"}, paths: []string{"/tmp/x.parquet"}}})

	for _, raw := range []string{name, value} {
		if strings.Contains(q.sql, raw) {
			t.Fatalf("raw matcher bytes %q spliced into SQL: %s", raw, q.sql)
		}
		if slices.Contains(q.args, any(raw)) {
			t.Fatalf("raw matcher bytes %q bound as a parameter: %v", raw, q.args)
		}
	}
	nameHex, valueHex := hex.EncodeToString([]byte(name)), hex.EncodeToString([]byte(value))
	if !slices.Contains(q.args, any(nameHex)) {
		t.Errorf("invalid matcher name not bound as hex %q: %v", nameHex, q.args)
	}
	if !slices.Contains(q.args, any(valueHex)) {
		t.Errorf("invalid matcher value not bound as hex %q: %v", valueHex, q.args)
	}
	for _, arg := range q.args {
		if s, ok := arg.(string); ok && !utf8.ValidString(s) {
			t.Errorf("bound string argument is not valid UTF-8: %q", s)
		}
	}
	if n, args := strings.Count(q.sql, "?"), len(q.args); n != args {
		t.Errorf("%d placeholders for %d arguments: %s", n, args, q.sql)
	}
	for _, shape := range []string{
		"lower(hex(",
		"list_extract(map_values(\"labels\")",
		"list_position(list_transform(map_keys(\"labels\"), lambda k: lower(hex(k)))",
	} {
		if !strings.Contains(q.sql, shape) {
			t.Errorf("compiled SQL missing %q: %s", shape, q.sql)
		}
	}
}

// assertMatcherBytesBuckets compares Aggregate against the MemSource reference
// exactly, bucket for bucket, in the same order the conformance suite uses.
func assertMatcherBytesBuckets(t *testing.T, src, ref plan.Source, sel plan.Selection, agg plan.BucketAgg) {
	t.Helper()
	got, err := collectMatcherBytesBuckets(src.Aggregate(t.Context(), sel, agg))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	want, err := collectMatcherBytesBuckets(ref.Aggregate(t.Context(), sel, agg))
	if err != nil {
		t.Fatalf("reference Aggregate: %v", err)
	}
	key := func(b plan.Bucket) string {
		// Compare the start exactly: seconds plus the full nanosecond
		// remainder, not a truncated microsecond count.
		return fmt.Sprintf("%d.%09d|%s|%d|%d", b.Start.Unix(), b.Start.Nanosecond(), b.Labels, b.Count, b.Bytes)
	}
	order := func(a, b plan.Bucket) int {
		return cmp.Or(a.Start.Compare(b.Start), cmp.Compare(a.Labels.String(), b.Labels.String()))
	}
	slices.SortFunc(got, order)
	slices.SortFunc(want, order)
	if len(got) != len(want) {
		t.Fatalf("got %d buckets, want %d\n got: %v\n want: %v", len(got), len(want), got, want)
	}
	for i := range got {
		if got[i].Start.Location() != time.UTC {
			t.Fatalf("bucket start %s is not UTC", got[i].Start)
		}
		if !got[i].Start.Equal(want[i].Start) {
			t.Fatalf("bucket %d start = %s, want %s", i, got[i].Start, want[i].Start)
		}
		if key(got[i]) != key(want[i]) {
			t.Fatalf("bucket %d differs:\n got  %s\n want %s", i, key(got[i]), key(want[i]))
		}
	}
}

func collectMatcherBytesBuckets(seq iter.Seq2[plan.Bucket, error]) ([]plan.Bucket, error) {
	var out []plan.Bucket
	for b, err := range seq {
		if err != nil {
			return out, err
		}
		out = append(out, b)
	}
	return out, nil
}
