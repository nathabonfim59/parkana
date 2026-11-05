package query

// Executor tests of the SQL API: materialization semantics (time bounds,
// tenants, promotions, empty windows), the typed result contract, limits and
// truncation, failure classes and lease cleanup. Fixtures load the way the
// ingester stores them, through the same helpers the source tests use.

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"iter"
	"maps"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
	"uuid"
)

// testSQLOptions are small limits that keep every test fast.
func testSQLOptions() sqlOptions {
	return sqlOptions{MemoryLimitBytes: 1 << 30, Threads: 1, MaxRows: 100, DefaultLimit: 50}
}

// openSQLExecEnv loads files the way the ingester stores them and returns an
// executor over the same fixture resolver the LogQL source tests use, with
// the recording catalog in between so listing filters are assertable.
func openSQLExecEnv(t *testing.T, files []plantest.File, opts sqlOptions) (*sqlExec, *sourceEnv) {
	t.Helper()
	env := openSourceEnv(t, files, defaultStreamLabels)
	e, err := newSQLExec(opts, env.cat, fixtureResolver{env.dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return e, env
}

// sqlQuery runs one executor request over env's whole span.
func sqlQuery(t *testing.T, e *sqlExec, q string) sqlResult {
	t.Helper()
	res, err := e.Query(t.Context(), sqlRequest{
		Tenant: tenant.Default, Query: q,
		From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
	})
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return res
}

// leaseCountingResolver wraps a resolver and counts leases handed out versus
// released, proving every failure path gives its leases back.
type leaseCountingResolver struct {
	inner fileResolver
	mu    sync.Mutex
	held  int
	total int
}

func (r *leaseCountingResolver) Acquire(ctx context.Context, t tenant.ID, key string) (string, func(), error) {
	r.mu.Lock()
	r.total++
	r.mu.Unlock()
	p, release, err := r.inner.Acquire(ctx, t, key)
	if err != nil {
		return "", nil, err
	}
	r.mu.Lock()
	r.held++
	r.mu.Unlock()
	return p, sync.OnceFunc(func() {
		release()
		r.mu.Lock()
		r.held--
		r.mu.Unlock()
	}), nil
}

func (r *leaseCountingResolver) stats() (held, total int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.held, r.total
}

// listCountingCatalog counts ListFiles calls, delegating to the wrapped
// recording catalog, so a test can prove rejected requests never list.
type listCountingCatalog struct {
	catalog.Catalog
	mu    sync.Mutex
	lists int
}

func (c *listCountingCatalog) ListFiles(ctx context.Context, f catalog.Filter) iter.Seq2[catalog.File, error] {
	c.mu.Lock()
	c.lists++
	c.mu.Unlock()
	return c.Catalog.ListFiles(ctx, f)
}

func (c *listCountingCatalog) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lists
}

// TestSQLExecHalfOpenBounds proves the materialization window is exactly
// [From, To) on the µs grid, including nanosecond bounds that round up on
// both sides: a file spans both boundaries and only in-range rows appear.
func TestSQLExecHalfOpenBounds(t *testing.T) {
	rw := func(ts time.Time, line string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "api", "env": "prod"})}
	}
	from := plantest.T0
	to := plantest.T0.Add(60 * time.Second)
	rows := []plan.Row{
		rw(from.Add(-time.Microsecond), "before from"),
		rw(from.Add(-time.Nanosecond), "nano before from"),
		rw(from, "exactly from"),                         // included
		rw(from.Add(500*time.Nanosecond), "nano inside"), // µs-grid row at from+0? no: stored µs
		rw(from.Add(time.Second), "inside"),
		rw(to.Add(-time.Microsecond), "last inside"),
		rw(to, "exactly to"), // excluded
		rw(to.Add(time.Nanosecond), "nano after to"),
		rw(to.Add(time.Millisecond), "after to"),
	}
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Promoted: []string{"env"}, Rows: rows}
	e, _ := openSQLExecEnv(t, []plantest.File{f}, testSQLOptions())

	// Fixture rows live on the µs grid: from.Add(500ns) stores at from's µs,
	// and to.Add(1ns) stores at to's µs, so nanosecond bounds round up on
	// both sides exactly as ceilMicro prescribes.
	for _, tc := range []struct {
		name  string
		from  time.Time
		to    time.Time
		lines []string
	}{
		{"exact-micro-bounds", from, to,
			[]string{"exactly from", "nano inside", "inside", "last inside"}},
		{"nano-from-between", from.Add(500 * time.Nanosecond), to.Add(500 * time.Nanosecond),
			[]string{"inside", "last inside", "exactly to", "nano after to"}},
		{"nano-around-first-boundary", from.Add(-time.Nanosecond), from.Add(1500 * time.Nanosecond),
			[]string{"exactly from", "nano inside"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := e.Query(t.Context(), sqlRequest{
				Tenant: tenant.Default, Query: "SELECT line FROM logs ORDER BY _ts",
				From: tc.from, To: tc.to,
			})
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			got := make([]string, len(res.Rows))
			for i, row := range res.Rows {
				got[i] = row[0].(string)
			}
			if strings.Join(got, "|") != strings.Join(tc.lines, "|") {
				t.Fatalf("lines = %v, want %v", got, tc.lines)
			}
		})
	}
}

// TestSQLExecEmptyWindows proves two empty shapes: a range with no files at
// all still exposes the base-schema logs relation, and a listed zero-row
// Parquet file contributes its schema with no rows.
func TestSQLExecEmptyWindows(t *testing.T) {
	rw := func(ts time.Time, line string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "api"})}
	}
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{rw(plantest.T0, "one")}}
	e, env := openSQLExecEnv(t, []plantest.File{f}, testSQLOptions())

	t.Run("no-files-in-range", func(t *testing.T) {
		res, err := e.Query(t.Context(), sqlRequest{
			Tenant: tenant.Default, Query: "SELECT count(*) AS n FROM logs",
			From: plantest.T0.Add(100 * time.Hour), To: plantest.T0.Add(101 * time.Hour),
		})
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if string(res.Rows[0][0].(jsontext.Value)) != "0" {
			t.Fatalf("count = %v", res.Rows[0][0])
		}
		// The relation exists with the base columns.
		cols, err := e.Query(t.Context(), sqlRequest{
			Tenant: tenant.Default, Query: "SELECT * FROM logs",
			From: plantest.T0.Add(100 * time.Hour), To: plantest.T0.Add(101 * time.Hour),
		})
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, c := range cols.Columns {
			names = append(names, c.Name)
		}
		if strings.Join(names, ",") != "_ts,line,labels,meta" {
			t.Fatalf("empty-window columns = %v", names)
		}
	})

	t.Run("zero-row-file", func(t *testing.T) {
		// A valid Parquet file with zero rows, listed with a window the way a
		// writer would record it.
		data, st, err := plantest.Encode(plantest.File{Tenant: tenant.Default, Stream: "api", Promoted: []string{"env"}})
		if err != nil {
			t.Fatal(err)
		}
		key := objstore.DataKey(tenant.Default, "api", plantest.T0.Add(10*time.Minute), uuid.NewV7())
		if err := envStore(t, env).Put(t.Context(), key, bytes.NewReader(data), int64(len(data))); err != nil {
			t.Fatal(err)
		}
		if err := env.cat.AddFiles(t.Context(), catalog.File{
			Tenant: tenant.Default, Stream: "api", Key: key,
			MinTS: plantest.T0, MaxTS: plantest.T0.Add(time.Hour), Rows: st.Rows, Bytes: int64(len(data)),
			LabelValues: st.LabelValues, LabelsComplete: st.LabelsComplete,
		}); err != nil {
			t.Fatal(err)
		}
		res := sqlQuery(t, e, "SELECT count(*) AS n FROM logs")
		if string(res.Rows[0][0].(jsontext.Value)) != "1" {
			t.Fatalf("count = %v, want 1 (the one real row)", res.Rows[0][0])
		}
		cols := sqlQuery(t, e, "SELECT * FROM logs LIMIT 1")
		if len(cols.Columns) != 5 || cols.Columns[4].Name != "env" {
			t.Fatalf("columns = %+v, want the promoted env column present", cols.Columns)
		}
	})
}

// TestSQLExecEmptyRowsJSON pins the frozen empty-result shape: rows encodes as
// an empty array (not null) and truncated is present and false.
func TestSQLExecEmptyRowsJSON(t *testing.T) {
	e, _ := openSQLExecEnv(t, nil, testSQLOptions())
	res := sqlQuery(t, e, "SELECT line FROM logs WHERE line = 'no match'")
	if res.Rows == nil {
		t.Fatal("Rows is nil, want an empty slice")
	}
	enc, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"rows":[]`, `"truncated":false`} {
		if !strings.Contains(string(enc), want) {
			t.Errorf("encoded empty result missing %s: %s", want, enc)
		}
	}
}

// envStore returns env's store handle for extra fixtures, closed with the
// test; openSourceEnv builds the directory from an objstore.FS root, and the
// extra handle owns its own os.Root.
func envStore(t *testing.T, env *sourceEnv) objstore.Store {
	t.Helper()
	store, err := objstore.NewFS(env.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// TestSQLExecTenantIsolation proves a planted same-stream same-time file of
// another tenant never appears, in either direction, and that concurrent
// requests for different tenants cannot observe each other's rows.
func TestSQLExecTenantIsolation(t *testing.T) {
	rw := func(ts time.Time, line string, ls map[string]string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	base := plantest.T0
	var mine, theirs []plan.Row
	for i := range 40 {
		mine = append(mine, rw(base.Add(time.Duration(i)*time.Second), fmt.Sprintf("default-tenant-sentinel %d", i),
			map[string]string{"service_name": "api", "env": "prod"}))
		theirs = append(theirs, rw(base.Add(time.Duration(i)*time.Second), fmt.Sprintf("other-tenant-sentinel %d", i),
			map[string]string{"service_name": "api", "env": "prod"}))
	}
	files := []plantest.File{
		{Tenant: tenant.Default, Stream: "api", Promoted: []string{"env"}, Rows: mine},
		{Tenant: plantest.OtherTenant, Stream: "api", Promoted: []string{"env"}, Rows: theirs},
	}
	e, _ := openSQLExecEnv(t, files, testSQLOptions())

	res := sqlQuery(t, e, "SELECT line FROM logs")
	if len(res.Rows) != 40 {
		t.Fatalf("got %d rows, want 40", len(res.Rows))
	}
	for _, row := range res.Rows {
		if !strings.HasPrefix(row[0].(string), "default-tenant-sentinel") {
			t.Fatalf("leaked row %q", row[0])
		}
	}
	other, err := e.Query(t.Context(), sqlRequest{
		Tenant: plantest.OtherTenant, Query: "SELECT line FROM logs",
		From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(other.Rows) != 40 {
		t.Fatalf("other tenant got %d rows, want 40", len(other.Rows))
	}
	for _, row := range other.Rows {
		if !strings.HasPrefix(row[0].(string), "other-tenant-sentinel") {
			t.Fatalf("leaked row %q", row[0])
		}
	}

	// Concurrent mixed-tenant requests: 8 workers per tenant, several rounds.
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			tID := tenant.Default
			prefix := "default-tenant-sentinel"
			if w%2 == 1 {
				tID = plantest.OtherTenant
				prefix = "other-tenant-sentinel"
			}
			for range 3 {
				res, err := e.Query(t.Context(), sqlRequest{
					Tenant: tID, Query: "SELECT line FROM logs ORDER BY _ts",
					From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
				})
				if err != nil {
					t.Errorf("worker %d: %v", w, err)
					return
				}
				if len(res.Rows) != 40 {
					t.Errorf("worker %d: %d rows", w, len(res.Rows))
					return
				}
				for _, row := range res.Rows {
					if !strings.HasPrefix(row[0].(string), prefix) {
						t.Errorf("worker %d: leaked row %q", w, row[0])
						return
					}
				}
			}
		})
	}
	wg.Wait()
}

// TestSQLExecOutsideRangeSentinel proves rows outside the requested window
// never appear through any query: the materialized relation is the only thing
// user SQL can reach, and it holds only in-range rows.
func TestSQLExecOutsideRangeSentinel(t *testing.T) {
	rw := func(ts time.Time, line string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "api"})}
	}
	rows := []plan.Row{
		rw(plantest.T0.Add(-time.Second), "BEFORE-WINDOW-SENTINEL"),
		rw(plantest.T0, "IN-RANGE-KEEP"),
		rw(plantest.T0.Add(time.Second), "AFTER-WINDOW-SENTINEL"),
	}
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: rows}
	e, _ := openSQLExecEnv(t, []plantest.File{f}, testSQLOptions())
	// The window holds only the middle row; a sentinel sits on each side.
	res, err := e.Query(t.Context(), sqlRequest{
		Tenant: tenant.Default, Query: "SELECT line FROM logs UNION ALL SELECT line FROM logs",
		From: plantest.T0.Add(-time.Microsecond), To: plantest.T0.Add(500 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range res.Rows {
		if strings.Contains(row[0].(string), "SENTINEL") {
			t.Fatalf("out-of-range row leaked: %q", row[0])
		}
	}
	if len(res.Rows) != 2 { // the one in-range row, twice
		t.Fatalf("got %d rows, want 2", len(res.Rows))
	}
}

// TestSQLExecPromotedAndMapLabels proves a label promoted in one file and
// map-backed in another is reachable both ways: the union table keeps the
// promoted column (NULL for the map-backed rows) and the labels map (holding
// the label for the map-backed rows).
func TestSQLExecPromotedAndMapLabels(t *testing.T) {
	rw := func(ts time.Time, line string, level string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "api", "level": level})}
	}
	promotedFile := plantest.File{Tenant: tenant.Default, Stream: "api", Promoted: []string{"level"}, Rows: []plan.Row{
		rw(plantest.T0, "column level", "error"),
	}}
	mapFile := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{
		rw(plantest.T0.Add(time.Second), "map level", "warn"),
	}}
	e, _ := openSQLExecEnv(t, []plantest.File{promotedFile, mapFile}, testSQLOptions())

	res := sqlQuery(t, e, `SELECT line, level, labels['level'] AS from_map FROM logs ORDER BY _ts`)
	if len(res.Rows) != 2 {
		t.Fatalf("got %d rows", len(res.Rows))
	}
	colRow, mapRow := res.Rows[0], res.Rows[1]
	if colRow[1] != "error" {
		t.Errorf("promoted row column level = %#v, want error", colRow[1])
	}
	if mapRow[1] != nil {
		t.Errorf("map row column level = %#v, want NULL", mapRow[1])
	}
	if mapRow[2] != "warn" {
		t.Errorf("map row labels['level'] = %#v, want warn", mapRow[2])
	}
	if colRow[2] != nil {
		t.Errorf("promoted row labels['level'] = %#v, want NULL (not in the map)", colRow[2])
	}
}

// TestSQLExecNoDedup pins the stored-data semantics: retry-delivered
// duplicate rows are returned as stored, not collapsed the way LogQL dedups.
func TestSQLExecNoDedup(t *testing.T) {
	row := plan.Row{
		TS: plantest.T0, Line: "duplicate me",
		Labels: logs.FromMap(map[string]string{"service_name": "api"}),
	}
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{row, row}}
	e, _ := openSQLExecEnv(t, []plantest.File{f}, testSQLOptions())
	res := sqlQuery(t, e, "SELECT count(*) AS n FROM logs")
	if string(res.Rows[0][0].(jsontext.Value)) != "2" {
		t.Fatalf("count = %v, want 2 (stored rows, no dedup)", res.Rows[0][0])
	}
}

// TestSQLExecRelationShape pins the logs relation's column shape and kinds:
// _ts is a time column, line a string, labels and meta json maps, promoted
// columns strings.
func TestSQLExecRelationShape(t *testing.T) {
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Promoted: []string{"level"}, Rows: []plan.Row{{
		TS: plantest.T0, Line: "l",
		Labels: logs.FromMap(map[string]string{"service_name": "api", "level": "info"}),
		Meta:   logs.FromMap(map[string]string{"trace_id": "t1"}),
	}}}
	e, _ := openSQLExecEnv(t, []plantest.File{f}, testSQLOptions())
	res := sqlQuery(t, e, "SELECT * FROM logs")
	want := []sqlColumn{
		{Name: "_ts", Type: "time", DatabaseType: "TIMESTAMPTZ"},
		{Name: "line", Type: "string", DatabaseType: "VARCHAR"},
		{Name: "labels", Type: "json", DatabaseType: "MAP(VARCHAR, VARCHAR)"},
		{Name: "meta", Type: "json", DatabaseType: "MAP(VARCHAR, VARCHAR)"},
		{Name: "level", Type: "string", DatabaseType: "VARCHAR"},
	}
	if len(res.Columns) != len(want) {
		t.Fatalf("columns = %+v", res.Columns)
	}
	for i, w := range want {
		if res.Columns[i] != w {
			t.Errorf("column %d = %+v, want %+v", i, res.Columns[i], w)
		}
	}
	row := res.Rows[0]
	if ts, ok := row[0].(time.Time); !ok || ts != plantest.T0.UTC() {
		t.Errorf("_ts = %#v, want %s UTC", row[0], plantest.T0.UTC())
	}
	if row[1] != "l" {
		t.Errorf("line = %#v", row[1])
	}
	labels, ok := row[2].(map[string]any)
	if !ok || labels["service_name"] != "api" || labels["level"] != nil {
		t.Errorf("labels = %#v", row[2])
	}
	meta, ok := row[3].(map[string]any)
	if !ok || meta["trace_id"] != "t1" {
		t.Errorf("meta = %#v", row[3])
	}
	if row[4] != "info" {
		t.Errorf("level = %#v, want info", row[4])
	}
}

// TestSQLExecValues pins the typed value contract on literals: exact numbers
// (DECIMAL, HUGEINT, int64 extremes), shortest floats, null, booleans,
// strings, UTC timestamps, clock times, lists, structs, JSON columns, blobs
// and intervals — and that encoding the row with encoding/json/v2 reproduces
// the exact number tokens.
func TestSQLExecValues(t *testing.T) {
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{{
		TS: plantest.T0, Line: "l", Labels: logs.FromMap(map[string]string{"service_name": "api"}),
	}}}
	e, _ := openSQLExecEnv(t, []plantest.File{f}, testSQLOptions())
	q := `SELECT
		NULL AS n,
		TRUE AS b,
		'txt' AS s,
		42 AS i,
		9223372036854775807 AS max_big,
		-9223372036854775808 AS min_big,
		170141183460469231731687303715884105727::HUGEINT AS huge,
		0.1 + 0.2 AS dec_sum,
		0.1::DOUBLE + 0.2::DOUBLE AS dbl_sum,
		3.0 AS dec_whole,
		[1, 2, 3] AS l,
		{'a': 1, 'b': 'x'} AS st,
		to_json({'k': 'v'}) AS js,
		'abc'::BLOB AS blob,
		'550e8400-e29b-41d4-a716-446655440000'::UUID AS uid,
		TIMESTAMP '2026-11-01 10:00:00.123456' AS naive_ts,
		TIMESTAMPTZ '2026-11-01 10:00:00.123456+02' AS tz_ts,
		TIME '10:11:12.5' AS tm,
		INTERVAL '1 day 2:03:04' AS iv
	FROM logs`
	res := sqlQuery(t, e, q)
	if len(res.Rows) != 1 {
		t.Fatalf("rows: %d", len(res.Rows))
	}
	row := res.Rows[0]
	byName := map[string]any{}
	for i, c := range res.Columns {
		byName[c.Name] = row[i]
	}
	if v := byName["n"]; v != nil {
		t.Errorf("n = %#v, want nil", v)
	}
	if v := byName["b"]; v != true {
		t.Errorf("b = %#v", v)
	}
	if v := byName["s"]; v != "txt" {
		t.Errorf("s = %#v", v)
	}
	num := func(name, want string) {
		t.Helper()
		v, ok := byName[name].(jsontext.Value)
		if !ok || string(v) != want {
			t.Errorf("%s = %#v, want exact number %s", name, byName[name], want)
		}
	}
	num("i", "42")
	num("max_big", "9223372036854775807")
	num("min_big", "-9223372036854775808")
	num("huge", "170141183460469231731687303715884105727")
	num("dec_sum", "0.3")
	num("dbl_sum", "0.30000000000000004")
	num("dec_whole", "3") // the driver's decimal text drops trailing-zero padding
	if l, ok := byName["l"].([]any); !ok || len(l) != 3 || string(l[0].(jsontext.Value)) != "1" {
		t.Errorf("l = %#v", byName["l"])
	}
	st, ok := byName["st"].(map[string]any)
	if !ok || string(st["a"].(jsontext.Value)) != "1" || st["b"] != "x" {
		t.Errorf("st = %#v", byName["st"])
	}
	js, ok := byName["js"].(map[string]any)
	if !ok || js["k"] != "v" {
		t.Errorf("js = %#v", byName["js"])
	}
	if v := byName["blob"]; v != "YWJj" { // base64("abc")
		t.Errorf("blob = %#v, want YWJj", v)
	}
	if v := byName["uid"]; v != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("uid = %#v, want canonical UUID text", v)
	}
	if ts, ok := byName["naive_ts"].(time.Time); !ok || ts.Format(time.RFC3339Nano) != "2026-11-01T10:00:00.123456Z" {
		t.Errorf("naive_ts = %#v", byName["naive_ts"])
	}
	if ts, ok := byName["tz_ts"].(time.Time); !ok || ts.Format(time.RFC3339Nano) != "2026-11-01T08:00:00.123456Z" {
		t.Errorf("tz_ts = %#v, want UTC-shifted", byName["tz_ts"])
	}
	if v := byName["tm"]; v != "10:11:12.5" {
		t.Errorf("tm = %#v, want 10:11:12.5", v)
	}
	iv, ok := byName["iv"].(map[string]any)
	if !ok || string(iv["days"].(jsontext.Value)) != "1" || string(iv["micros"].(jsontext.Value)) != "7384000000" {
		t.Errorf("iv = %#v", byName["iv"])
	}

	// Column kinds for the literal types.
	kind := map[string]string{}
	for _, c := range res.Columns {
		kind[c.Name] = c.Type
	}
	for name, want := range map[string]string{
		"n": "string", "b": "boolean", "s": "string", "i": "number",
		"max_big": "number", "huge": "number", "dec_sum": "number", "dbl_sum": "number",
		"l": "json", "st": "json", "js": "json", "blob": "string", "uid": "string",
		"naive_ts": "time", "tz_ts": "time", "tm": "string", "iv": "json",
	} {
		if kind[name] != want {
			t.Errorf("column %q kind = %q, want %q", name, kind[name], want)
		}
	}
	if k := kind["n"]; k != "string" {
		t.Logf("NULL column kind: %q (databaseType %q)", k, kind["n"])
	}

	// The M2 encoder reproduces exact number tokens with encoding/json/v2.
	enc, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`170141183460469231731687303715884105727`,
		`,0.3,`,
		`,0.30000000000000004,`,
	} {
		if !strings.Contains(string(enc), want) {
			t.Errorf("encoded row missing %s:\n%s", want, enc)
		}
	}
}

// TestSQLExecBroadScalarContract covers the remaining driver scan types and
// keeps the exact-number contract through composite recursion: every integer
// family, FLOAT, DATE, TIMETZ, non-string map keys, nested lists and nested
// structs.
func TestSQLExecBroadScalarContract(t *testing.T) {
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{{
		TS: plantest.T0, Line: "l", Labels: logs.FromMap(map[string]string{"service_name": "api"}),
	}}}
	e, _ := openSQLExecEnv(t, []plantest.File{f}, testSQLOptions())
	res := sqlQuery(t, e, `SELECT
		1::TINYINT AS i8, 1::SMALLINT AS i16, 1::INTEGER AS i32, 1::BIGINT AS i64,
		1::UTINYINT AS u8, 1::USMALLINT AS u16, 1::UINTEGER AS u32, 1::UBIGINT AS u64,
		1::UHUGEINT AS uhuge,
		1.5::FLOAT AS f32,
		DATE '2026-01-02' AS d, TIME WITH TIME ZONE '10:00:00+02' AS tz,
		MAP {1: 'a'} AS imap,
		MAP {true: 't'} AS bmap,
		MAP {2::UBIGINT: 'u'} AS ukey,
		[[1, 2], [3]] AS nested,
		{'a': {'b': 1}} AS snest`)
	byName := map[string]any{}
	for i, c := range res.Columns {
		byName[c.Name] = res.Rows[0][i]
	}
	for _, name := range []string{"i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "uhuge"} {
		if v, ok := byName[name].(jsontext.Value); !ok || string(v) != "1" {
			t.Errorf("%s = %#v, want exact number 1", name, byName[name])
		}
	}
	if v, ok := byName["f32"].(jsontext.Value); !ok || string(v) != "1.5" {
		t.Errorf("f32 = %#v, want exact number 1.5", byName["f32"])
	}
	if ts, ok := byName["d"].(time.Time); !ok || ts.Format(time.RFC3339Nano) != "2026-01-02T00:00:00Z" {
		t.Errorf("d = %#v, want the UTC date", byName["d"])
	}
	if v := byName["tz"]; v != "10:00:00+02:00" {
		t.Errorf("tz = %#v, want the clock with offset", v)
	}
	if m, ok := byName["imap"].(map[string]any); !ok || m["1"] != "a" {
		t.Errorf("imap = %#v, want map key \"1\"", byName["imap"])
	}
	if m, ok := byName["bmap"].(map[string]any); !ok || m["true"] != "t" {
		t.Errorf("bmap = %#v, want map key \"true\"", byName["bmap"])
	}
	if m, ok := byName["ukey"].(map[string]any); !ok || m["2"] != "u" {
		t.Errorf("ukey = %#v, want map key \"2\"", byName["ukey"])
	}
	nested, ok := byName["nested"].([]any)
	if !ok || len(nested) != 2 {
		t.Fatalf("nested = %#v", byName["nested"])
	}
	inner, ok := nested[0].([]any)
	if !ok || len(inner) != 2 || string(inner[0].(jsontext.Value)) != "1" || string(inner[1].(jsontext.Value)) != "2" {
		t.Errorf("nested[0] = %#v", nested[0])
	}
	snest, ok := byName["snest"].(map[string]any)
	if !ok {
		t.Fatalf("snest = %#v", byName["snest"])
	}
	innerStruct, ok := snest["a"].(map[string]any)
	if !ok || string(innerStruct["b"].(jsontext.Value)) != "1" {
		t.Errorf("snest = %#v", snest)
	}
}

// TestSQLExecNonFiniteRejected proves non-finite floats fail the request with
// the client SQL error: JSON cannot carry them.
func TestSQLExecNonFiniteRejected(t *testing.T) {
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{{
		TS: plantest.T0, Line: "l", Labels: logs.FromMap(map[string]string{"service_name": "api"}),
	}}}
	e, _ := openSQLExecEnv(t, []plantest.File{f}, testSQLOptions())
	for _, q := range []string{
		"SELECT 'NaN'::DOUBLE AS v FROM logs",
		"SELECT 'Infinity'::DOUBLE AS v FROM logs",
		"SELECT 1.0::DOUBLE / 0 AS v FROM logs",
	} {
		_, err := e.Query(t.Context(), sqlRequest{
			Tenant: tenant.Default, Query: q,
			From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
		})
		if !errors.Is(err, errSQLRejected) {
			t.Errorf("query %q error = %v, want errSQLRejected", q, err)
		}
	}
}

// TestSQLExecTemporalOutOfJSONRangeRejected proves temporal values the frozen
// response cannot encode fail as client SQL errors before any encoder runs.
// DuckDB infinities scan as far-out years (DATE year 5881580, TIMESTAMPTZ
// +-294247/-290308) and finite dates can exceed 9999; RFC 3339 carries only
// four-digit years. The guard also reaches temporal cells nested in lists and
// structs, while NULL, year 9999, UTC-relative and pre-epoch values stay
// supported.
func TestSQLExecTemporalOutOfJSONRangeRejected(t *testing.T) {
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{{
		TS: plantest.T0, Line: "l", Labels: logs.FromMap(map[string]string{"service_name": "api"}),
	}}}
	e, _ := openSQLExecEnv(t, []plantest.File{f}, testSQLOptions())
	req := sqlRequest{
		Tenant: tenant.Default,
		From:   plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
	}
	for name, q := range map[string]string{
		"date infinity":                 "SELECT DATE 'infinity' AS v FROM logs",
		"timestamptz infinity":          "SELECT TIMESTAMPTZ 'infinity' AS v FROM logs",
		"timestamptz negative infinity": "SELECT TIMESTAMPTZ '-infinity' AS v FROM logs",
		"timestamp infinity":            "SELECT TIMESTAMP 'infinity' AS v FROM logs",
		"date year 10000":               "SELECT DATE '10000-01-01' AS v FROM logs",
		"date list infinity":            "SELECT [DATE 'infinity'] AS v FROM logs",
		"timestamptz struct -infinity":  `SELECT {'a': TIMESTAMPTZ '-infinity'} AS v FROM logs`,
		"timestamptz map infinity":      "SELECT MAP {'k': TIMESTAMPTZ 'infinity'} AS v FROM logs",
		"date list finite out of range": "SELECT [DATE '10000-01-01'] AS v FROM logs",
	} {
		r := req
		r.Query = q
		if _, err := e.Query(t.Context(), r); !errors.Is(err, errSQLRejected) {
			t.Errorf("%s: error = %v, want errSQLRejected", name, err)
		}
	}

	res := sqlQuery(t, e, `SELECT
		DATE '9999-12-31' AS last_day,
		TIMESTAMP '1969-12-31 23:59:59.999999' AS pre_epoch,
		TIMESTAMPTZ '2026-11-01 10:00:00.123456+02' AS tz,
		NULL::DATE AS n
	FROM logs`)
	if len(res.Rows) != 1 {
		t.Fatalf("control rows = %d, want 1", len(res.Rows))
	}
	byName := map[string]any{}
	for i, c := range res.Columns {
		byName[c.Name] = res.Rows[0][i]
	}
	for name, want := range map[string]string{
		"last_day":  "9999-12-31T00:00:00Z",
		"pre_epoch": "1969-12-31T23:59:59.999999Z",
		"tz":        "2026-11-01T08:00:00.123456Z",
	} {
		tm, ok := byName[name].(time.Time)
		if !ok || tm.Format(time.RFC3339Nano) != want {
			t.Errorf("%s = %#v, want %s", name, byName[name], want)
		}
	}
	if v := byName["n"]; v != nil {
		t.Errorf("n = %#v, want nil", v)
	}
}

// TestSQLExecTruncationAndLimits proves the row-cap contract: limit 0 uses
// the default, results over the limit are truncated with truncated=true, and
// negative or over-max limits are rejected.
func TestSQLExecTruncationAndLimits(t *testing.T) {
	var rows []plan.Row
	for i := range 25 {
		rows = append(rows, plan.Row{
			TS: plantest.T0.Add(time.Duration(i) * time.Second), Line: fmt.Sprintf("row %d", i),
			Labels: logs.FromMap(map[string]string{"service_name": "api"}),
		})
	}
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: rows}
	opts := testSQLOptions() // MaxRows 100, DefaultLimit 50
	e, _ := openSQLExecEnv(t, []plantest.File{f}, opts)
	req := sqlRequest{
		Tenant: tenant.Default, Query: "SELECT line FROM logs ORDER BY _ts",
		From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
	}

	res := sqlQuery(t, e, "SELECT line FROM logs ORDER BY _ts")
	if len(res.Rows) != 25 || res.Truncated {
		t.Fatalf("default fetch: %d rows, truncated=%v", len(res.Rows), res.Truncated)
	}

	for _, tc := range []struct {
		limit     int
		wantRows  int
		truncated bool
	}{
		{limit: 10, wantRows: 10, truncated: true},
		{limit: 25, wantRows: 25, truncated: false},
		{limit: 26, wantRows: 25, truncated: false},
	} {
		r := req
		r.Limit = tc.limit
		res, err := e.Query(t.Context(), r)
		if err != nil {
			t.Fatalf("limit %d: %v", tc.limit, err)
		}
		if len(res.Rows) != tc.wantRows || res.Truncated != tc.truncated {
			t.Errorf("limit %d: %d rows, truncated=%v, want %d/%v",
				tc.limit, len(res.Rows), res.Truncated, tc.wantRows, tc.truncated)
		}
	}

	// An explicit user LIMIT smaller than the request limit is not truncation.
	r := req
	r.Limit = 100
	res, err := e.Query(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 25 || res.Truncated {
		t.Fatalf("small table under max: %d rows truncated=%v", len(res.Rows), res.Truncated)
	}

	for _, bad := range []int{-1, 101, 1000} {
		r := req
		r.Limit = bad
		if _, err := e.Query(t.Context(), r); !errors.Is(err, errSQLLimits) {
			t.Errorf("limit %d error = %v, want errSQLLimits", bad, err)
		}
	}
}

// TestSQLExecUserLimitWithTruncation proves truncation composes with a user
// LIMIT: only rows beyond the request limit count as truncation, and the
// user's own LIMIT is honored inside DuckDB.
func TestSQLExecUserLimitWithTruncation(t *testing.T) {
	var rows []plan.Row
	for i := range 30 {
		rows = append(rows, plan.Row{
			TS: plantest.T0.Add(time.Duration(i) * time.Second), Line: fmt.Sprintf("row %d", i),
			Labels: logs.FromMap(map[string]string{"service_name": "api"}),
		})
	}
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: rows}
	e, _ := openSQLExecEnv(t, []plantest.File{f}, testSQLOptions())
	res, err := e.Query(t.Context(), sqlRequest{
		Tenant: tenant.Default, Query: "SELECT line FROM logs LIMIT 5",
		From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour), Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The user LIMIT caps the query at 5 rows; the request limit of 10 never
	// sees an 11th row, so nothing is truncated.
	if len(res.Rows) != 5 || res.Truncated {
		t.Fatalf("got %d rows, truncated=%v, want 5/false", len(res.Rows), res.Truncated)
	}
	res, err = e.Query(t.Context(), sqlRequest{
		Tenant: tenant.Default, Query: "SELECT line FROM logs",
		From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour), Limit: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 5 || !res.Truncated {
		t.Fatalf("got %d rows, truncated=%v, want 5/true", len(res.Rows), res.Truncated)
	}
}

// bigRows builds n rows of incompressible hex lines so a small memory budget
// cannot hold the materialized table.
func bigRows(n, size int) []plan.Row {
	rng := rand.New(rand.NewPCG(7, 11))
	hexDigits := "0123456789abcdef"
	rows := make([]plan.Row, n)
	for i := range n {
		b := make([]byte, size)
		for j := range b {
			b[j] = hexDigits[rng.IntN(16)]
		}
		rows[i] = plan.Row{
			TS: plantest.T0.Add(time.Duration(i) * time.Microsecond), Line: string(b),
			Labels: logs.FromMap(map[string]string{"service_name": "api", "batch": "big"}),
		}
	}
	return rows
}

// TestSQLExecMemoryLimit proves materialization over the budget fails with
// the typed limit error (spill is disabled, so the budget is the only way
// out) and that a narrower range succeeds afterwards.
func TestSQLExecMemoryLimit(t *testing.T) {
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: bigRows(40000, 600)}
	opts := sqlOptions{MemoryLimitBytes: 16 << 20, Threads: 1, MaxRows: 100, DefaultLimit: 50}
	e, _ := openSQLExecEnv(t, []plantest.File{f}, opts)
	_, err := e.Query(t.Context(), sqlRequest{
		Tenant: tenant.Default, Query: "SELECT count(*) AS n FROM logs",
		From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
	})
	if !errors.Is(err, errSQLMemory) {
		t.Fatalf("over-budget error = %v, want errSQLMemory", err)
	}
	// A narrow window fits the budget and works: rows live at T0+iµs, so
	// [T0, T0+1000µs) holds i = 0..999.
	res, err := e.Query(t.Context(), sqlRequest{
		Tenant: tenant.Default, Query: "SELECT count(*) AS n FROM logs",
		From: plantest.T0, To: plantest.T0.Add(1000 * time.Microsecond),
	})
	if err != nil {
		t.Fatalf("narrow window: %v", err)
	}
	if n := string(res.Rows[0][0].(jsontext.Value)); n != "1000" {
		t.Fatalf("narrow count = %s, want 1000", n)
	}
}

// TestSQLExecDeadlineDuringLoading proves a deadline that fires while
// materialization is still loading aborts the request with its context error.
func TestSQLExecDeadlineDuringLoading(t *testing.T) {
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: bigRows(40000, 600)}
	e, _ := openSQLExecEnv(t, []plantest.File{f}, testSQLOptions())
	// Warm the catalog/cache path with a narrow, fast request so the short
	// deadline below is spent in the real materialization CTAS rather than in
	// first-use listing, leasing or sandbox setup.
	if _, err := e.Query(t.Context(), sqlRequest{
		Tenant: tenant.Default, Query: "SELECT count(*) AS n FROM logs",
		From: plantest.T0, To: plantest.T0.Add(1000 * time.Microsecond),
	}); err != nil {
		t.Fatalf("warm query: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := e.Query(ctx, sqlRequest{
		Tenant: tenant.Default, Query: "SELECT count(*) AS n FROM logs",
		From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
	})
	if err == nil {
		t.Fatal("big materialization completed inside 20ms")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
}

// TestSQLExecRejectedSQLClasses proves user statements that pass shape
// validation still fail inside the sandbox with the client SQL error, and
// that parameters are rejected at execution.
func TestSQLExecRejectedSQLClasses(t *testing.T) {
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{{
		TS: plantest.T0, Line: "l", Labels: logs.FromMap(map[string]string{"service_name": "api"}),
	}}}
	e, _ := openSQLExecEnv(t, []plantest.File{f}, testSQLOptions())
	for _, q := range []string{
		"SELECT * FROM read_parquet('/etc/passwd')",
		"SELECT * FROM read_csv('whatever.csv')",
		"SELECT * FROM glob('*')",
		"SELECT * FROM nosuchtable",
		"SELECT nosuchcolumn FROM logs",
		"SELECT $1",
		"SELECT ?",
		"ATTACH ':memory:' AS m",
		"FROM logs",
		"VALUES (1)",
	} {
		_, err := e.Query(t.Context(), sqlRequest{
			Tenant: tenant.Default, Query: q,
			From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
		})
		if err == nil {
			t.Errorf("query %q succeeded inside the sandbox", q)
			continue
		}
		if !errors.Is(err, errSQLRejected) && !errors.Is(err, errSQLStatement) {
			t.Errorf("query %q error = %v, want errSQLRejected or errSQLStatement", q, err)
		}
	}
}

// TestSQLExecCleanupAfterFailures proves every failure path releases all
// object leases and leaves the executor usable: corrupt materialization,
// validation rejection, execution failure and deadline aborts.
func TestSQLExecCleanupAfterFailures(t *testing.T) {
	rw := func(ts time.Time, line string) plan.Row {
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(map[string]string{"service_name": "api"})}
	}
	good := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{rw(plantest.T0, "good")}}
	env := openSourceEnv(t, []plantest.File{good}, defaultStreamLabels)

	// A corrupt object, cataloged like a real file so materialization fails.
	corruptKey := objstore.DataKey(tenant.Default, "api", plantest.T0.Add(time.Minute), uuid.NewV7())
	if err := envStore(t, env).Put(t.Context(), corruptKey, strings.NewReader("not parquet at all"), 18); err != nil {
		t.Fatal(err)
	}
	if err := env.cat.AddFiles(t.Context(), catalog.File{
		Tenant: tenant.Default, Stream: "api", Key: corruptKey,
		MinTS: plantest.T0.Add(10 * time.Minute), MaxTS: plantest.T0.Add(11 * time.Minute),
		Rows: 1, Bytes: 18,
	}); err != nil {
		t.Fatal(err)
	}

	cr := &leaseCountingResolver{inner: fixtureResolver{env.dir}}
	opts := testSQLOptions()
	e, err := newSQLExec(opts, env.cat, cr, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The wide window hits the corrupt file; the narrow one holds only the
	// good row, so later phases exercise validation, execution and cleanup
	// past materialization.
	wide := sqlRequest{
		Tenant: tenant.Default,
		From:   plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
		Query: "SELECT count(*) AS n FROM logs",
	}
	narrow := sqlRequest{
		Tenant: tenant.Default,
		From:   plantest.T0.Add(-time.Microsecond), To: plantest.T0.Add(500 * time.Millisecond),
		Query: "SELECT count(*) AS n FROM logs",
	}
	check := func(stage string) {
		t.Helper()
		if held, total := cr.stats(); held != 0 {
			t.Fatalf("%s: %d leases still held (of %d acquired)", stage, held, total)
		}
	}

	if _, err := e.Query(t.Context(), wide); err == nil {
		t.Fatal("corrupt materialization succeeded")
	}
	check("corrupt materialization")

	r := narrow
	r.Query = "CREATE TABLE x(i INT)"
	if _, err := e.Query(t.Context(), r); !errors.Is(err, errSQLStatement) {
		t.Fatalf("policy rejection = %v", err)
	}
	check("policy rejection")

	r.Query = "SELECT nosuchcolumn FROM logs"
	if _, err := e.Query(t.Context(), r); !errors.Is(err, errSQLRejected) {
		t.Fatalf("execution rejection = %v", err)
	}
	check("execution rejection")

	ctx, cancel := context.WithTimeout(t.Context(), time.Nanosecond)
	defer cancel()
	r.Query = "SELECT count(*) FROM logs"
	if _, err := e.Query(ctx, r); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pre-expired context = %v", err)
	}
	check("expired context")

	// The executor still answers correctly after all failures.
	res, err := e.Query(t.Context(), narrow)
	if err != nil {
		t.Fatalf("recovery query: %v", err)
	}
	if string(res.Rows[0][0].(jsontext.Value)) != "1" {
		t.Fatalf("recovery count = %v", res.Rows[0][0])
	}
	check("recovery")
}

// TestSQLExecRequestValidation pins the executor's own request checks:
// tenant, bounds, empty SQL.
func TestSQLExecRequestValidation(t *testing.T) {
	e, _ := openSQLExecEnv(t, nil, testSQLOptions())
	base := sqlRequest{
		Tenant: tenant.Default, Query: "SELECT 1",
		From: plantest.T0, To: plantest.T0.Add(time.Hour),
	}
	for name, mutate := range map[string]func(*sqlRequest){
		"bad tenant":    func(r *sqlRequest) { r.Tenant = tenant.ID("in valid") },
		"reversed":      func(r *sqlRequest) { r.From, r.To = r.To, r.From },
		"equal bounds":  func(r *sqlRequest) { r.To = r.From },
		"zero from":     func(r *sqlRequest) { r.From = time.Time{} },
		"empty query":   func(r *sqlRequest) { r.Query = "  " },
		"comment query": func(r *sqlRequest) { r.Query = "-- nothing" },
	} {
		r := base
		mutate(&r)
		_, err := e.Query(t.Context(), r)
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestSQLExecPreflightBeforeResources proves cap- and depth-rejected payloads
// fail before any resource is touched. The catalog is populated and the test
// first runs a control request that demonstrably lists and leases, so the
// zero ListFiles/Acquire assertions after each rejection cannot pass on an
// empty fixture by accident.
func TestSQLExecPreflightBeforeResources(t *testing.T) {
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{{
		TS: plantest.T0, Line: "control",
		Labels: logs.FromMap(map[string]string{"service_name": "api"}),
	}}}
	env := openSourceEnv(t, []plantest.File{f}, defaultStreamLabels)
	cc := &listCountingCatalog{Catalog: env.cat}
	cr := &leaseCountingResolver{inner: fixtureResolver{env.dir}}
	e, err := newSQLExec(testSQLOptions(), cc, cr, nil)
	if err != nil {
		t.Fatal(err)
	}
	req := sqlRequest{
		Tenant: tenant.Default,
		From:   plantest.T0, To: plantest.T0.Add(time.Hour),
	}

	// Control: a valid request lists the one populated file and leases it.
	control := req
	control.Query = "SELECT count(*) AS n FROM logs"
	res, err := e.Query(t.Context(), control)
	if err != nil {
		t.Fatalf("control query: %v", err)
	}
	if got := string(res.Rows[0][0].(jsontext.Value)); got != "1" {
		t.Fatalf("control count = %s, want 1", got)
	}
	if got := cc.count(); got != 1 {
		t.Fatalf("control ListFiles calls = %d, want 1", got)
	}
	if held, total := cr.stats(); held != 0 || total != 1 {
		t.Fatalf("control leases: held %d, total %d; want 0/1", held, total)
	}

	for name, q := range map[string]string{
		"over cap":           "SELECT '" + strings.Repeat("a", (64<<10)+8) + "'",
		"over depth":         "SELECT " + strings.Repeat("(", 1500) + "1" + strings.Repeat(")", 1500),
		"deep case":          "SELECT " + strings.Repeat("CASE WHEN TRUE THEN ", 1500) + "1" + strings.Repeat(" ELSE 1 END", 1500),
		"empty text":         "   ",
		"bad tenant":         "SELECT 1",
		"bad bounds":         "SELECT 1",
		"bad limit":          "SELECT 1",
		"recursive explicit": "WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t",
		"recursive nested":   "SELECT * FROM (WITH RECURSIVE t AS (SELECT 1) SELECT * FROM t) s",
		"recursive comments": "WITH /* c */ RECURSIVE t AS (SELECT 1) SELECT * FROM t",
		"recursive logs":     "WITH RECURSIVE logs AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM logs WHERE n<3) SELECT * FROM logs",
		"recursive nbsp":     "WITH\u00a0RECURSIVE logs AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM logs WHERE n<3) SELECT * FROM logs",
		"recursive vt":       "WITH\vRECURSIVE logs AS (SELECT 1 AS n UNION ALL SELECT n+1 FROM logs WHERE n<3) SELECT * FROM logs",
	} {
		r := req
		r.Query = q
		switch name {
		case "bad tenant":
			r.Tenant = tenant.ID("in valid")
		case "bad bounds":
			r.From, r.To = r.To, r.From
		case "bad limit":
			r.Limit = -5
		}
		listsBefore := cc.count()
		heldBefore, totalBefore := cr.stats()
		if _, err := e.Query(t.Context(), r); err == nil {
			t.Errorf("%s: accepted", name)
		} else if strings.HasPrefix(name, "recursive") && !errors.Is(err, errSQLRecursive) {
			t.Errorf("%s: error = %v, want errSQLRecursive", name, err)
		}
		held, total := cr.stats()
		if got := cc.count(); got != listsBefore {
			t.Errorf("%s: ListFiles calls = %d, want unchanged %d", name, got, listsBefore)
		}
		if held != heldBefore || total != totalBefore {
			t.Errorf("%s: resolver leases held/total = %d/%d, want unchanged %d/%d",
				name, held, total, heldBefore, totalBefore)
		}
	}
}

// TestSQLExecRejectsIntrospection runs the settings/introspection escapes
// end to end through the executor, against materialized data: every one must
// fail without leaking rows.
func TestSQLExecRejectsIntrospection(t *testing.T) {
	f := plantest.File{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{{
		TS: plantest.T0, Line: "l", Labels: logs.FromMap(map[string]string{"service_name": "api"}),
	}}}
	e, _ := openSQLExecEnv(t, []plantest.File{f}, testSQLOptions())
	for _, q := range []string{
		"SELECT current_setting('memory_limit') AS v",
		"SELECT * FROM duckdb_settings() WHERE name = 'home_directory'",
		"SELECT * FROM pg_settings",
		"SELECT * FROM information_schema.tables",
		"SELECT * FROM pragma_table_info('logs')",
		"SELECT * FROM query('SELECT line FROM logs')",
		"SELECT * FROM query_table('logs')",
		"SELECT * FROM json_execute_serialized_sql('{}')",
		"SELECT getenv('HOME') AS home",
		"SELECT getvariable('HOME') AS home",
		"WITH c AS (SELECT * FROM duckdb_settings()) SELECT * FROM c",
		"SELECT * FROM logs l, LATERAL (SELECT * FROM sqlite_master) s",
		"SELECT * FROM main.logs",
	} {
		res, err := e.Query(t.Context(), sqlRequest{
			Tenant: tenant.Default, Query: q,
			From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
		})
		if err == nil {
			t.Errorf("query %q returned %d rows", q, len(res.Rows))
			continue
		}
		if !errors.Is(err, errSQLStatement) && !errors.Is(err, errSQLRejected) && !errors.Is(err, errSQLText) {
			t.Errorf("query %q error = %v, want a client-class rejection", q, err)
		}
	}
}

// TestSQLExecOptionsValidation pins the executor's option checks and the
// approved defaults. Each invalid branch is asserted through o.validate
// directly — so every branch is reached with its own message — and again
// through the constructor with valid dependencies, so a regression in
// ordering (checks shadowed by the dependency checks) still fails here.
func TestSQLExecOptionsValidation(t *testing.T) {
	for name, tc := range map[string]struct {
		opts sqlOptions
		want string
	}{
		"zero memory":     {sqlOptions{MemoryLimitBytes: 0, Threads: 1, MaxRows: 10, DefaultLimit: 5}, "memory limit must be positive"},
		"negative memory": {sqlOptions{MemoryLimitBytes: -1, Threads: 1, MaxRows: 10, DefaultLimit: 5}, "memory limit must be positive"},
		"zero threads":    {sqlOptions{MemoryLimitBytes: 1, Threads: 0, MaxRows: 10, DefaultLimit: 5}, "threads must be at least 1"},
		"zero max rows":   {sqlOptions{MemoryLimitBytes: 1, Threads: 1, MaxRows: 0, DefaultLimit: 5}, "max rows must be at least 1"},
		"default too big": {sqlOptions{MemoryLimitBytes: 1, Threads: 1, MaxRows: 10, DefaultLimit: 11}, "default limit 11 out of [1, 10]"},
		"default zero":    {sqlOptions{MemoryLimitBytes: 1, Threads: 1, MaxRows: 10, DefaultLimit: 0}, "default limit 0 out of [1, 10]"},
	} {
		if err := tc.opts.validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: validate = %v, want message containing %q", name, err, tc.want)
		}
	}
	// The constructor reaches the option check with valid dependencies.
	env := openSourceEnv(t, nil, defaultStreamLabels)
	for name, o := range map[string]sqlOptions{
		"zero memory":    {MemoryLimitBytes: 0, Threads: 1, MaxRows: 10, DefaultLimit: 5},
		"zero threads":   {MemoryLimitBytes: 1, Threads: 0, MaxRows: 10, DefaultLimit: 5},
		"zero max rows":  {MemoryLimitBytes: 1, Threads: 1, MaxRows: 0, DefaultLimit: 5},
		"default limits": {MemoryLimitBytes: 1, Threads: 1, MaxRows: 10, DefaultLimit: 11},
	} {
		if _, err := newSQLExec(o, env.cat, fixtureResolver{env.dir}, nil); err == nil || !strings.Contains(err.Error(), "query: sql:") {
			t.Errorf("%s: constructor = %v, want the option error", name, err)
		}
	}
	if _, err := newSQLExec(testSQLOptions(), env.cat, fixtureResolver{env.dir}, nil); err != nil {
		t.Fatalf("valid options with valid dependencies: %v", err)
	}
	d := defaultSQLOptions()
	if d.MemoryLimitBytes != 1<<30 || d.Threads != 1 || d.MaxRows != 10000 || d.DefaultLimit != 1000 {
		t.Fatalf("defaults = %+v", d)
	}
	if err := d.validate(); err != nil {
		t.Fatalf("defaults invalid: %v", err)
	}
}

// TestSQLExecCatalogFilter pins the listing contract: tenant and time only,
// never a stream constraint, never label pruning — SQL sees every stream the
// tenant stored in the window.
func TestSQLExecCatalogFilter(t *testing.T) {
	files := []plantest.File{
		{Tenant: tenant.Default, Stream: "api", Rows: []plan.Row{{TS: plantest.T0, Line: "api", Labels: logs.FromMap(map[string]string{"service_name": "api"})}}},
		{Tenant: tenant.Default, Stream: "web", Rows: []plan.Row{{TS: plantest.T0, Line: "web", Labels: logs.FromMap(map[string]string{"service_name": "web"})}}},
	}
	env := openSourceEnv(t, files, defaultStreamLabels)
	e, err := newSQLExec(testSQLOptions(), env.cat, fixtureResolver{env.dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Query(t.Context(), sqlRequest{
		Tenant: tenant.Default, Query: "SELECT line FROM logs ORDER BY line",
		From: plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 2 || res.Rows[0][0] != "api" || res.Rows[1][0] != "web" {
		t.Fatalf("rows = %+v, want api and web", res.Rows)
	}
	f := env.cat.lastFilter()
	if f.Tenant != tenant.Default || f.Stream != "" || f.Match != nil {
		t.Fatalf("filter = %+v, want tenant-only", f)
	}
	if !f.From.Equal(plantest.T0.Add(-time.Hour)) || !f.Through.Equal(plantest.T0.Add(3*time.Hour)) {
		t.Fatalf("filter bounds = %s..%s", f.From, f.Through)
	}
}

// TestSQLExecConcurrentAcrossSchemas proves concurrent requests over files
// with different promoted-column sets materialize independently and correctly.
func TestSQLExecConcurrentAcrossSchemas(t *testing.T) {
	rw := func(line string, ts time.Time, promoted map[string]string, extra map[string]string) plan.Row {
		ls := map[string]string{"service_name": "api"}
		maps.Copy(ls, promoted)
		maps.Copy(ls, extra)
		return plan.Row{TS: ts, Line: line, Labels: logs.FromMap(ls)}
	}
	f1 := plantest.File{Tenant: tenant.Default, Stream: "api", Promoted: []string{"level"}, Rows: []plan.Row{
		rw("col level", plantest.T0, map[string]string{"level": "info"}, nil),
	}}
	f2 := plantest.File{Tenant: tenant.Default, Stream: "api", Promoted: []string{"pod"}, Rows: []plan.Row{
		rw("col pod", plantest.T0.Add(time.Second), nil, map[string]string{"pod": "p1"}),
	}}
	e, _ := openSQLExecEnv(t, []plantest.File{f1, f2}, testSQLOptions())

	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for range 5 {
				res, err := e.Query(t.Context(), sqlRequest{
					Tenant: tenant.Default,
					Query:  "SELECT coalesce(level, 'NULL') AS lvl, coalesce(pod, 'NULL') AS pod FROM logs ORDER BY line",
					From:   plantest.T0.Add(-time.Hour), To: plantest.T0.Add(3 * time.Hour),
				})
				if err != nil {
					t.Errorf("worker %d: %v", w, err)
					return
				}
				if len(res.Rows) != 2 {
					t.Errorf("worker %d: %d rows", w, len(res.Rows))
					return
				}
				if res.Rows[0][0] != "info" || res.Rows[0][1] != "NULL" || res.Rows[1][0] != "NULL" || res.Rows[1][1] != "p1" {
					t.Errorf("worker %d: rows = %+v", w, res.Rows)
					return
				}
			}
		})
	}
	wg.Wait()
}
