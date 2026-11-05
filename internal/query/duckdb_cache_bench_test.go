// Footer-cache benchmarks and the DuckDB profiling harness for B3b
// (PLAN.md phase 3, part B). Everything here is test-only support: no
// production SQL, source or service code is involved.
//
// Methodology:
//
//   - Fixtures are real Parquet files written by plantest.Encode (the storage
//     stack), never synthetic byte blobs. Generation happens before b.Loop,
//     so it is never measured; memory is bounded by construction (bounded row
//     counts, bounded file counts).
//   - Fixture mtimes are normalized into the past once at setup. That is
//     deterministic setup only, not a requirement for local footer reuse:
//     local footer-cache entries validate by version-tag equality
//     (device, inode, size, mtime seconds); the ten-second mtime rule belongs
//     to the tag-less remote branch. No test or benchmark sleeps.
//   - The external-file byte cache is held explicitly at its enabled default
//     and asserted per connection, with cache_local_files=false asserted too,
//     so local bytes are never served by the byte cache and results isolate
//     footer metadata. The footer cache itself is toggled per dedicated
//     connection (SET parquet_metadata_cache=false|true) and the effective
//     value asserted, so one arm can never silently measure the other.
//   - Every measured multi-file query opens all fixture files but reads
//     almost no data (a timestamp window bound as parameters that lies
//     provably before the data range), so per-file cost is dominated by
//     footer work.
//   - No cold host-page-cache is claimed: repeated reads of the same local
//     files stay in the OS page cache by design; the comparison is footer
//     re-parse (cache off) versus in-memory footer reuse (cache on).
//
// Boat commands (lead/sol-c run; runner never starts Boat):
//
//	go test ./internal/query/ -run 'TestOpenDBConnectionSettings|TestOpenDBConnectionInitAfterCancel|TestParquetMetadataCacheLifetime|TestParquetFooterCacheProfile' -v -count=1
//	go test ./internal/query/ -run '^$' -bench 'BenchmarkParquetFooterCache' -benchtime=2s -count=1
//
// Expected output schema: one benchmark line per arm, named
// BenchmarkParquetFooterCache/<shape>/cache=<on|off>-<N>, reporting ns/op,
// B/op and allocs/op; the profile test logs, per pass, a line of the form
// "profile <pass>: cpu_time=<s> query_total_time=<s> optimizer_total_time=<s>
// binding_time=<s> binder_total_time=<s> create_plan=<s> scans=<n>
// scan_rows=<n> scan_timing=<s>(sum) bytes_read=<n>" plus the flattened
// operator tree. query_total_time comes from the unique metric group carrying
// sql (the query's wall time); optimizer_total_time and binder_total_time are
// those groups' own totals, labeled so no first-match total_time can be
// misread as query time. scans/scan_rows/scan_timing summarize every
// TABLE_SCAN node (one per schema group), not just the first. Metric names
// are lowercase (cpu_time, total_time, binding_time, create_plan,
// rows_scanned, total_bytes_read). The passes are footer-cold, footer-warm
// (windowed, footer isolation), overlapping-scan, production-select-cache-on
// and production-select-cache-off (the Source's own compiled Select:
// per-group reads, promoted merge, row-identity DISTINCT, blocking ORDER BY;
// one pass per footer-cache state on the same verified fixtures) and
// synthetic-distinct-labels (labels-map JSON only; explicitly NOT Source
// shape). This file is self-contained against the baseline query package: it
// must compile and run when copied alone onto a tree whose duckdb.go/
// duckdb_test.go are unmodified, because every cache experiment sets its
// state per dedicated connection.
package query

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"uuid"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/plan/plantest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// benchFileRows builds the deterministic rows of bench fixture file i:
// rowsPer rows, one per microsecond starting at plantest.T0 plus i hours,
// with labels the promoted group sets consume and distinct lines per file.
func benchFileRows(i, rowsPer int) []plan.Row {
	base := plantest.T0.Add(time.Duration(i) * time.Hour)
	envs := []string{"prod", "dev"}
	levels := []string{"info", "warn", "error"}
	rows := make([]plan.Row, rowsPer)
	for j := range rowsPer {
		m := map[string]string{"service_name": "bench", "env": envs[(i+j)%2], "pod": fmt.Sprintf("p%d", i%4)}
		if i%3 == 0 {
			m["level"] = levels[j%3] // promoted as a column in group 0/2 files
		}
		rows[j] = plan.Row{TS: base.Add(time.Duration(j) * time.Microsecond),
			Line: fmt.Sprintf("bench file %d row %d error=%v", i, j, j%7 == 0), Labels: logs.FromMap(m)}
	}
	return rows
}

// benchPromoted returns the promoted-label set of schema group g (g in
// [0, groups); every group gets a distinct set so grouping is real).
func benchPromoted(g int) []string {
	switch g % 4 {
	case 0:
		return []string{"level"}
	case 1:
		return nil
	case 2:
		return []string{"level", "pod"}
	default:
		return []string{"env"}
	}
}

// benchFile is one generated fixture on disk.
type benchFile struct {
	path     string
	promoted []string
}

// writeBenchFixtures writes files fixtures under dir: files files split
// evenly over groups schema groups, rowsPer rows each, every mtime normalized
// once into the past (deterministic setup; see the file doc comment).
func writeBenchFixtures(tb testing.TB, dir string, files, groups, rowsPer int) []benchFile {
	tb.Helper()
	out := make([]benchFile, 0, files)
	perGroup := files / max(groups, 1)
	past := time.Now().Add(-time.Minute)
	for i := range files {
		g := min(i/perGroup, groups-1)
		promoted := benchPromoted(g)
		data, _, err := plantest.Encode(plantest.File{
			Tenant: tenant.Default, Stream: "bench", Promoted: promoted,
			Rows: benchFileRows(i, rowsPer),
		})
		if err != nil {
			tb.Fatalf("encode fixture %d: %v", i, err)
		}
		p := filepath.Join(dir, fmt.Sprintf("bench-%04d.parquet", i))
		if err := os.WriteFile(p, data, 0o600); err != nil {
			tb.Fatalf("write fixture %d: %v", i, err)
		}
		if err := os.Chtimes(p, past, past); err != nil {
			tb.Fatalf("normalize fixture mtime %d: %v", i, err)
		}
		out = append(out, benchFile{path: p, promoted: promoted})
	}
	return out
}

// openBenchDB opens the production OpenDB shape for benchmarks.
func openBenchDB(b *testing.B) *sql.DB {
	b.Helper()
	db, err := OpenDB(b.Context())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

// footerCacheConn returns one dedicated connection with the footer cache
// explicitly enabled or disabled and the byte-cache isolation asserted:
// enable_external_file_cache stays at its enabled default while
// cache_local_files stays false, so local bytes can never come from the byte
// cache and any cache effect measured here is footer metadata alone.
func footerCacheConn(tb testing.TB, db *sql.DB, footerCache bool) *sql.Conn {
	tb.Helper()
	conn, err := db.Conn(tb.Context())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { conn.Close() })
	want := "false"
	if footerCache {
		want = "true"
	}
	if _, err := conn.ExecContext(tb.Context(), "SET parquet_metadata_cache="+want); err != nil {
		tb.Fatal(err)
	}
	// Pin the byte-cache isolation state explicitly, not just by default.
	if _, err := conn.ExecContext(tb.Context(), "SET enable_external_file_cache=true"); err != nil {
		tb.Fatal(err)
	}
	var got, ext, localFiles string
	if err := conn.QueryRowContext(tb.Context(),
		"SELECT current_setting('parquet_metadata_cache'), current_setting('enable_external_file_cache'), current_setting('cache_local_files')").
		Scan(&got, &ext, &localFiles); err != nil {
		tb.Fatal(err)
	}
	if got != want {
		tb.Fatalf("parquet_metadata_cache = %q, want %q", got, want)
	}
	if ext != "true" || localFiles != "false" {
		tb.Fatalf("byte-cache state = (%q, %q), want (true, false) so local bytes cannot be served from it", ext, localFiles)
	}
	return conn
}

// benchPlaceholders returns "?, ?, ..." with n placeholders.
func benchPlaceholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// benchWindow returns the measured scan's timestamp bounds as parameters: a
// window provably before every fixture row, so each file is opened and its
// footer consulted but no row survives.
func benchWindow() (time.Time, time.Time) {
	return plantest.T0.Add(-time.Microsecond).UTC(), plantest.T0.UTC()
}

// benchScanQuery builds the measured scan over paths: one read_parquet per
// identical promoted set (the Source's per-schema-group shape, built inline
// so production SQL stays untouched), UNION ALL'd under one outer aggregate,
// plus the pruning window. Arguments follow placeholder order exactly.
func benchScanQuery(paths []benchFile) (string, []any) {
	groups := make([][]string, 0, 4)
	index := make(map[string]int)
	args := make([]any, 0, len(paths)+2)
	for _, f := range paths {
		key := strings.Join(f.promoted, "\x00")
		gi, ok := index[key]
		if !ok {
			gi = len(groups)
			index[key] = gi
			groups = append(groups, nil)
		}
		groups[gi] = append(groups[gi], f.path)
	}
	var union strings.Builder
	for gi := range groups {
		if gi > 0 {
			union.WriteString("\nUNION ALL\n")
		}
		union.WriteString("SELECT \"_ts\", \"line\" FROM read_parquet([" + benchPlaceholders(len(groups[gi])) + "], union_by_name = true)")
		for _, p := range groups[gi] {
			args = append(args, p)
		}
	}
	from, through := benchWindow()
	args = append(args, from, through)
	// The outer aggregate is already blocking: it consumes every input before
	// its single result row, mirroring the Source's attempt boundary without
	// borrowing production SQL.
	q := "SELECT count(*), min(\"_ts\") FROM (\n" + union.String() +
		"\n) WHERE \"_ts\" >= ? AND \"_ts\" < ?"
	return q, args
}

// runBenchScan executes the scan once on conn, failing the benchmark on error.
func runBenchScan(tb testing.TB, conn *sql.Conn, q string, args ...any) {
	tb.Helper()
	var n int
	var min sql.NullTime
	if err := conn.QueryRowContext(tb.Context(), q, args...).Scan(&n, &min); err != nil {
		tb.Fatalf("scan: %v", err)
	}
}

// BenchmarkParquetFooterCache measures identical immutable local files under
// the footer cache on and off: repeated reads of a few and of many files, a
// many-file multi-schema-group scan, and single-file reads cycling over a
// pool larger than one pass (the first pass over each path is a cold footer
// read; later passes repeat, so the cache=on arm converges to warm and the
// cache=off arm stays cold — never claimed as a host-page-cache cold run).
func BenchmarkParquetFooterCache(b *testing.B) {
	const rowsPer = 64
	shape := func(name string, files, groups int, cycling bool) {
		b.Run(name, func(b *testing.B) {
			dir := b.TempDir()
			fixtures := writeBenchFixtures(b, dir, files, groups, rowsPer)
			q, args := benchScanQuery(fixtures)
			db := openBenchDB(b)
			for _, arm := range []struct {
				name string
				on   bool
			}{{"cache=on", true}, {"cache=off", false}} {
				b.Run(arm.name, func(b *testing.B) {
					// Allocation counts are part of the reported output.
					b.ReportAllocs()
					conn := footerCacheConn(b, db, arm.on)
					if cycling {
						// One file per iteration, advancing cursor: pool paths
						// repeat only after a full pass.
						i := 0
						for b.Loop() {
							f := fixtures[i%len(fixtures)]
							i++
							runBenchScan(b, conn, "SELECT count(*), min(\"_ts\") FROM read_parquet(?) WHERE \"_ts\" >= ? AND \"_ts\" < ?", any(f.path), args[len(args)-2], args[len(args)-1])
						}
						return
					}
					for b.Loop() {
						runBenchScan(b, conn, q, args...)
					}
				})
			}
		})
	}
	shape("repeated-few", 8, 1, false)
	shape("repeated-many", 128, 1, false)
	shape("repeated-schemagroups", 128, 4, false)
	shape("single-cycling-pool", 256, 1, true)
}

// profileCleanupTimeout bounds the context profiling is disabled on after a
// profileQuery, independently of the request context.
const profileCleanupTimeout = 5 * time.Second

// profileQuery runs query on the dedicated connection conn with profiling
// enabled and returns the collected tree. Profiling is connection-local in
// DuckDB, so the same conn executes the SET, the query and the fetch; rows
// are drained and closed on every path before the info is read, and profiling
// is disabled again afterwards — on a detached, bounded context, so a canceled
// or expired request context can never leave profiling enabled on a
// connection callers reuse. Cleanup errors are preserved next to the failure
// that triggered them.
func profileQuery(ctx context.Context, conn *sql.Conn, query string, args ...any) (duckdb.ProfilingInfo, error) {
	return profileQueryAfterEnable(ctx, conn, nil, query, args...)
}

// profileQueryAfterEnable is profileQuery with a test-only seam: afterEnable,
// when non-nil, runs on the same connection after profiling is definitely
// enabled and before the query starts. Tests use it as a barrier to arrange
// request-context state exactly at that point, with no timing assumptions.
func profileQueryAfterEnable(ctx context.Context, conn *sql.Conn, afterEnable func(), query string, args ...any) (duckdb.ProfilingInfo, error) {
	if _, err := conn.ExecContext(ctx, "SET enable_profiling='no_output'"); err != nil {
		return duckdb.ProfilingInfo{}, fmt.Errorf("enable profiling: %w", err)
	}
	if afterEnable != nil {
		afterEnable()
	}
	disable := func() error {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), profileCleanupTimeout)
		defer cancel()
		_, err := conn.ExecContext(cctx, "PRAGMA disable_profiling")
		return err
	}
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return duckdb.ProfilingInfo{}, errors.Join(fmt.Errorf("profiled query: %w", err), disable())
	}
	defer rows.Close() // idempotent; covers the error paths below
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		return duckdb.ProfilingInfo{}, errors.Join(fmt.Errorf("profiled rows: %w", err), disable())
	}
	if err := rows.Close(); err != nil {
		return duckdb.ProfilingInfo{}, errors.Join(fmt.Errorf("close profiled rows: %w", err), disable())
	}
	info, err := duckdb.GetProfilingInfo(conn)
	return info, errors.Join(err, disable())
}

// TestProfileQueryCleanupAfterCancellation is the regression for the
// profileQuery cleanup contract: a query that fails under a canceled request
// context must still leave profiling disabled on the connection, because
// callers reuse dedicated connections. The after-enable seam cancels the
// context exactly after profiling is definitely enabled and before the query
// runs — no timing assumption about query duration or machine speed — so the
// failure path and its cleanup both run while the request context is dead.
// The original context.Canceled must survive into the returned error, and a
// cleanup that rode the dead request context would leave
// enable_profiling='no_output' behind on the connection.
func TestProfileQueryCleanupAfterCancellation(t *testing.T) {
	db := openTestDB(t)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	enabled := make(chan struct{})
	type result struct {
		info duckdb.ProfilingInfo
		err  error
	}
	done := make(chan result, 1)
	go func() {
		info, err := profileQueryAfterEnable(ctx, conn, func() {
			close(enabled)
			<-ctx.Done() // hold the seam until the test cancels
		}, "SELECT count(*) FROM range(10)")
		done <- result{info, err}
	}()
	// Wait for the seam, the helper's early return (for instance when the
	// SET itself fails and profiling was never enabled), or the test's own
	// deadline — never block on a barrier that may never fire.
	var res result
	select {
	case <-enabled:
		cancel() // profiling is on; cancel before the query can start
		res = <-done
	case r := <-done:
		t.Fatalf("profileQuery never reached the profiling seam: %v", r.err)
	case <-t.Context().Done():
		t.Fatal("test context ended before the profiling seam was reached")
	}
	if res.err == nil {
		t.Fatal("query after cancellation unexpectedly succeeded; the regression did not exercise the failure path")
	}
	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("profileQuery error = %v, want the original context.Canceled preserved", res.err)
	}

	var profiling sql.NullString
	if err := conn.QueryRowContext(t.Context(), "SELECT current_setting('enable_profiling')").Scan(&profiling); err != nil {
		t.Fatalf("read enable_profiling after failed profileQuery: %v", err)
	}
	if profiling.Valid && profiling.String == "no_output" {
		t.Fatal("profiling still enabled after a failed profileQuery; cleanup must run on an independent context")
	}

	// The connection is reusable and the helper works end to end afterwards.
	info, err := profileQuery(t.Context(), conn, "SELECT 42")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := profilingMetric(info, "cpu_time"); !ok {
		t.Error("post-cleanup profiled query carries no cpu_time metric")
	}
}

// TestProfilingMetricSelection is the regression for the profiling selectors:
// DuckDB puts a total_time on the optimizer, planner, binder and query metric
// groups alike, and the optimizer group sorts before the query group, so a
// first-match total_time reports optimizer time as query wall time. The
// selectors must take query time from the unique group carrying sql, label
// the other groups' totals separately, and summarize every TABLE_SCAN node —
// one per schema group — instead of the first. The synthetic tree pins the
// conflicting values deterministically; the actual-tree half proves the
// selection has work to do on a real DuckDB plan and picks the right node.
func TestProfilingMetricSelection(t *testing.T) {
	synthetic := duckdb.ProfilingInfo{
		Children: []duckdb.ProfilingInfo{
			{Metrics: map[string]string{ // optimizer group, depth-first first
				"join_order":      "9.9e-05",
				"total_time":      "0.300000",
				"filter_pushdown": "1e-05",
			}},
			{Metrics: map[string]string{ // query group
				"sql":                "SELECT 42",
				"cpu_time":           "0.000012",
				"total_time":         "0.001200",
				"total_rows_scanned": "0",
			}},
			{Metrics: map[string]string{ // binder group
				"binding_time": "0.500000",
				"total_time":   "0.400000",
			}},
			{Metrics: map[string]string{ // planner group
				"create_plan": "0.000010",
				"total_time":  "0.020000",
			}},
			{Children: []duckdb.ProfilingInfo{ // two scans in separate subtrees
				{Metrics: map[string]string{"type": "TABLE_SCAN", "rows_scanned": "128", "timing": "0.001"}},
				{Metrics: map[string]string{"type": "PROJECTION"}},
			}},
			{Children: []duckdb.ProfilingInfo{
				{Metrics: map[string]string{"type": "TABLE_SCAN", "rows_scanned": "128", "timing": "0.002"}},
			}},
		},
	}

	q, ok := queryMetrics(synthetic)
	if !ok {
		t.Fatal("queryMetrics found no query group in the synthetic tree")
	}
	if q.Metrics["total_time"] != "0.001200" || q.Metrics["sql"] != "SELECT 42" {
		t.Fatalf("queryMetrics picked %v, want the sql-carrying group with total_time 0.001200", q.Metrics)
	}
	if v, ok := groupMetric(synthetic, "join_order", "total_time"); !ok || v != "0.300000" {
		t.Errorf("optimizer total_time = %q (ok=%v), want 0.300000", v, ok)
	}
	if v, ok := groupMetric(synthetic, "binding_time", "total_time"); !ok || v != "0.400000" {
		t.Errorf("binder total_time = %q (ok=%v), want 0.400000", v, ok)
	}
	if v, ok := groupMetric(synthetic, "create_plan", "total_time"); !ok || v != "0.020000" {
		t.Errorf("planner total_time = %q (ok=%v), want 0.020000", v, ok)
	}
	scans := summarizeScans(synthetic)
	if scans.count != 2 || scans.rowsScanned != 256 || !scans.timingParsed || scans.timingSum != 0.003 {
		t.Errorf("scan summary = {count:%d rows:%d timing:%v parsed:%v}, want {2, 256, 0.003, true}", scans.count, scans.rowsScanned, scans.timingSum, scans.timingParsed)
	}

	// Actual tree: the selection must be non-vacuous (several total_time
	// groups exist) and pick the node that carries sql.
	db := openTestDB(t)
	conn := footerCacheConn(t, db, true)
	info, err := profileQuery(t.Context(), conn, "SELECT 42")
	if err != nil {
		t.Fatal(err)
	}
	var totalTimes int
	var walk func(duckdb.ProfilingInfo)
	walk = func(n duckdb.ProfilingInfo) {
		if _, ok := n.Metrics["total_time"]; ok {
			totalTimes++
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(info)
	if totalTimes < 2 {
		t.Fatalf("actual profiling tree carries %d total_time values, want >= 2 so the selector choice is meaningful", totalTimes)
	}
	rq, ok := queryMetrics(info)
	if !ok {
		t.Fatal("queryMetrics found no query group in the actual tree")
	}
	if _, ok := rq.Metrics["sql"]; !ok {
		t.Error("queryMetrics picked a node without sql on the actual tree")
	}
	want := rq.Metrics["total_time"]
	if first, ok := profilingMetric(info, "total_time"); ok && first == want && totalTimes > 1 {
		t.Logf("note: first-match total_time equals the query group's on this plan (%s); the synthetic half still pins the ordering hazard", want)
	}
}

// profilingFind returns the first node (depth-first) whose operator type
// equals typ, and false when no node carries that type.
func profilingFind(n duckdb.ProfilingInfo, typ string) (duckdb.ProfilingInfo, bool) {
	if n.Metrics["type"] == typ {
		return n, true
	}
	for _, c := range n.Children {
		if found, ok := profilingFind(c, typ); ok {
			return found, true
		}
	}
	return duckdb.ProfilingInfo{}, false
}

// profilingMetric returns the first value of metric on any node, depth-first.
// It is only for metric names that exist on exactly one node kind (cpu_time's
// query group, binding_time's binder group, create_plan's planner group,
// total_bytes_read's IO group). total_time exists on the query, optimizer,
// planner and binder groups alike, so a first-match total_time is NOT query
// wall time; use queryMetrics for that.
func profilingMetric(n duckdb.ProfilingInfo, metric string) (string, bool) {
	if v, ok := n.Metrics[metric]; ok {
		return v, true
	}
	for _, c := range n.Children {
		if v, ok := profilingMetric(c, metric); ok {
			return v, true
		}
	}
	return "", false
}

// queryMetrics returns the query-level metric group: the unique node carrying
// the sql metric, which is also the only group with cpu_time and the query's
// real total wall time. Its total_time is the number to report as query time;
// the optimizer, planner and binder groups each carry their own total_time
// that a first-match lookup would silently confuse with it.
func queryMetrics(n duckdb.ProfilingInfo) (duckdb.ProfilingInfo, bool) {
	if _, ok := n.Metrics["sql"]; ok {
		return n, true
	}
	for _, c := range n.Children {
		if found, ok := queryMetrics(c); ok {
			return found, true
		}
	}
	return duckdb.ProfilingInfo{}, false
}

// groupMetric returns metric from the first node carrying marker, labeled by
// that marker: "join_order" identifies the optimizer group, "create_plan" the
// planner group, "binding_time" the binder group.
func groupMetric(n duckdb.ProfilingInfo, marker, metric string) (string, bool) {
	if _, ok := n.Metrics[marker]; ok {
		v, ok := n.Metrics[metric]
		return v, ok
	}
	for _, c := range n.Children {
		if v, ok := groupMetric(c, marker, metric); ok {
			return v, true
		}
	}
	return "", false
}

// profilingScan summarizes every TABLE_SCAN node, not just the first: a
// multi-schema-group query has one scan operator per group, and each counts
// only its own files' rows.
type profilingScan struct {
	count        int
	rowsScanned  int64
	timingSum    float64
	timingParsed bool
}

// summarizeScans walks the whole tree and sums rows and timing over all
// TABLE_SCAN nodes.
func summarizeScans(n duckdb.ProfilingInfo) profilingScan {
	var s profilingScan
	if n.Metrics["type"] == "TABLE_SCAN" {
		s.count = 1
		if v, err := strconv.ParseInt(cmp.Or(n.Metrics["rows_scanned"], "0"), 10, 64); err == nil {
			s.rowsScanned = v
		}
		if v, err := strconv.ParseFloat(cmp.Or(n.Metrics["timing"], ""), 64); err == nil {
			s.timingSum = v
			s.timingParsed = true
		}
	}
	for _, c := range n.Children {
		child := summarizeScans(c)
		s.count += child.count
		s.rowsScanned += child.rowsScanned
		s.timingSum += child.timingSum
		s.timingParsed = s.timingParsed || child.timingParsed
	}
	return s
}

// profilingLog prints the flattened operator tree with a stable prefix and
// sorted metric keys, so Boat -v output can be compared across runs.
func profilingLog(t *testing.T, n duckdb.ProfilingInfo, prefix string) {
	t.Helper()
	if len(n.Metrics) > 0 {
		parts := make([]string, 0, len(n.Metrics))
		for _, k := range slices.Sorted(maps.Keys(n.Metrics)) {
			parts = append(parts, k+"="+n.Metrics[k])
		}
		t.Logf("%s%s", prefix, strings.Join(parts, " "))
	}
	for _, c := range n.Children {
		profilingLog(t, c, prefix+"  ")
	}
}

// logProfilePass extracts and logs the numbers the B3b report tracks. Query
// wall time comes from the query group (the unique node carrying sql), and the
// optimizer, planner and binder totals are labeled as their own numbers —
// DuckDB puts a total_time on each of those groups, so any first-match lookup
// would misreport one of them as query time. Scans are summarized over ALL
// TABLE_SCAN nodes: a multi-group query has one scan operator per schema
// group. Operator costs beyond the scans (DISTINCT, label-JSON
// reconstruction) are logged as the raw tree because metric coverage varies
// by plan shape.
func logProfilePass(t *testing.T, pass string, info duckdb.ProfilingInfo) {
	t.Helper()
	get := func(name string) string {
		v, ok := profilingMetric(info, name)
		if !ok {
			return "unavailable"
		}
		return v
	}
	group := func(marker, metric string) string {
		v, ok := groupMetric(info, marker, metric)
		if !ok {
			return "unavailable"
		}
		return v
	}
	queryTotal := "unavailable"
	if q, ok := queryMetrics(info); ok {
		if v, ok := q.Metrics["total_time"]; ok {
			queryTotal = v
		}
	}
	scans := summarizeScans(info)
	scanTiming := "unavailable"
	if scans.timingParsed {
		scanTiming = fmt.Sprintf("%.6f(sum)", scans.timingSum)
	}
	t.Logf("profile %s: cpu_time=%s query_total_time=%s optimizer_total_time=%s binding_time=%s binder_total_time=%s create_plan=%s scans=%d scan_rows=%d scan_timing=%s bytes_read=%s",
		pass, get("cpu_time"), queryTotal, group("join_order", "total_time"),
		group("binding_time", "binding_time"), group("binding_time", "total_time"),
		group("create_plan", "create_plan"), scans.count, scans.rowsScanned, scanTiming, get("total_bytes_read"))
	profilingLog(t, info, "  "+pass+" | ")
}

// profilingBytes returns the query-level total_bytes_read metric as a number,
// with ok false when the metric is absent or unparsable.
func profilingBytes(info duckdb.ProfilingInfo) (float64, bool) {
	v, ok := profilingMetric(info, "total_bytes_read")
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// TestParquetFooterCacheProfile exercises the profiling harness end to end on
// dedicated connections with the footer cache explicitly on (footerCacheConn),
// so the experiment holds even while OpenDB leaves the setting at DuckDB's
// default, and pins the before/after footer-cache evidence: the same
// multi-group windowed scan is profiled twice — the first pass reads every
// footer from disk, the second reuses the cached footers — then a
// row-returning scan proves TABLE_SCAN metrics, the Source's own compiled
// Select is drained, verified and profiled once per cache state (on and off),
// and a synthetic DISTINCT-plus-label-JSON query logs its operator costs, all
// without touching production SQL. Asserted deterministically: the first pass
// reads a positive number of file bytes (the footers), the second reads none,
// and cpu_time, TABLE_SCAN with scanned rows, and the verified production
// select all exist. Timings are logged, never compared, because wall-clock
// comparisons belong to the Boat benchmark round.
func TestParquetFooterCacheProfile(t *testing.T) {
	db := openTestDB(t)
	dir := t.TempDir()
	fixtures := writeBenchFixtures(t, dir, 8, 2, 32)
	q, args := benchScanQuery(fixtures)
	conn := footerCacheConn(t, db, true)

	// The windowed scan's plan prunes to EMPTY_RESULT from footer stats, so
	// its file bytes are footer bytes: positive on the first pass, zero once
	// the footer cache serves. That difference is the whole B3b measurement.
	first, err := profileQuery(t.Context(), conn, q, args...)
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, ok := profilingBytes(first)
	if !ok || firstBytes <= 0 {
		t.Fatalf("first pass read %v footer bytes (ok=%v), want > 0: the footers must come from disk", firstBytes, ok)
	}
	second, err := profileQuery(t.Context(), conn, q, args...)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, ok := profilingBytes(second)
	if !ok || secondBytes != 0 {
		t.Fatalf("second pass read %v file bytes (ok=%v), want 0: cached footers must be reused", secondBytes, ok)
	}
	for _, pass := range []struct {
		name string
		info duckdb.ProfilingInfo
	}{{"footer-cold", first}, {"footer-warm", second}} {
		if _, ok := profilingMetric(pass.info, "cpu_time"); !ok {
			t.Errorf("%s: profiling tree carries no cpu_time metric", pass.name)
		}
		if _, ok := profilingMetric(pass.info, "binding_time"); !ok {
			t.Errorf("%s: profiling tree carries no binding_time metric", pass.name)
		}
		logProfilePass(t, pass.name, pass.info)
	}

	// A window that actually overlaps the data keeps the scans in the plan,
	// so TABLE_SCAN operator metrics exist for the Boat report.
	scanQ := q
	scanArgs := append(slices.Clone(args[:len(args)-2]), plantest.T0.UTC(), plantest.T0.Add(time.Hour*300).UTC())
	scanInfo, err := profileQuery(t.Context(), conn, scanQ, scanArgs...)
	if err != nil {
		t.Fatal(err)
	}
	scan, ok := profilingFind(scanInfo, "TABLE_SCAN")
	if !ok {
		t.Fatal("overlapping profiling tree has no TABLE_SCAN operator")
	}
	if v, err := strconv.ParseFloat(cmp.Or(scan.Metrics["rows_scanned"], "0"), 64); err != nil || v <= 0 {
		t.Errorf("TABLE_SCAN rows_scanned = %q, want a positive count", scan.Metrics["rows_scanned"])
	}
	logProfilePass(t, "overlapping-scan", scanInfo)

	// Production-shape pass: compile the real Select through the Source's own
	// compiler (selectSQL + groupFiles, used read-only; sql.go and source.go
	// are untouched) over the same fixtures with an overlapping selection, so
	// the logged plan carries the actual per-group reads, promoted-column
	// merge into canonical labels_json, the row-identity DISTINCT and the
	// blocking ORDER BY. The rows are drained and verified against the
	// fixture expectation first: every row carries service_name=bench, all
	// timestamps fall inside the window, and no fixture row duplicates
	// another, so the exact expected count is files x rowsPer.
	sel := plan.Selection{
		Tenant:    tenant.Default,
		From:      plantest.T0,
		Through:   plantest.T0.Add(300 * time.Hour),
		Matchers:  []plan.Matcher{plan.MustMatcher(plan.MatchEqual, "service_name", "bench")},
		Direction: plan.Forward, // oldest first, so the first row is fixture 0 row 0
	}
	if err := sel.Validate(); err != nil {
		t.Fatal(err)
	}
	catFiles := make([]catalog.File, len(fixtures))
	paths := make([]string, len(fixtures))
	for i, f := range fixtures {
		lv := make(map[string][]string, len(f.promoted))
		for _, p := range f.promoted {
			lv[p] = []string{"x"}
		}
		// Real object keys follow the store's contract even as test dummies:
		// DataKey mints {tenant}/logs/{stream}/.../{uuidv7}.parquet at the
		// fixture's own hour, exactly as the ingester would.
		catFiles[i] = catalog.File{Tenant: tenant.Default, Stream: "bench",
			Key:         objstore.DataKey(tenant.Default, "bench", benchFileRows(i, 1)[0].TS, uuid.NewV7()),
			LabelValues: lv}
		paths[i] = f.path
	}
	prodQ := selectSQL(sel, groupFiles(catFiles, paths))

	type prodRow struct {
		ts         time.Time
		line       string
		labelsJSON sql.NullString
		metaJSON   sql.NullString
	}
	rows, err := conn.QueryContext(t.Context(), prodQ.sql, prodQ.args...)
	if err != nil {
		t.Fatalf("production select: %v", err)
	}
	got := 0
	var firstProd prodRow
	for rows.Next() {
		var r prodRow
		if err := rows.Scan(&r.ts, &r.line, &r.labelsJSON, &r.metaJSON); err != nil {
			t.Fatalf("production row %d: %v", got, err)
		}
		if got == 0 {
			firstProd = r
		}
		got++
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatal(err)
	}
	wantRows := len(fixtures) * 32
	if got != wantRows {
		t.Fatalf("production select returned %d rows, want %d (files x rowsPer)", got, wantRows)
	}
	var firstLabels map[string]string
	if err := json.Unmarshal([]byte(firstProd.labelsJSON.String), &firstLabels); err != nil {
		t.Fatalf("first row labels_json %q: %v", firstProd.labelsJSON.String, err)
	}
	if firstLabels["service_name"] != "bench" {
		t.Errorf("first row labels = %v, want the matcher's service_name=bench reconstructed", firstLabels)
	}
	// Fixture 0 is group 0, which promotes "level" as a column; the canonical
	// merge must fold it back into labels_json next to the map entries.
	if firstLabels["level"] != "info" {
		t.Errorf("first row labels = %v, want the promoted level=info merged back in", firstLabels)
	}
	if firstProd.line != "bench file 0 row 0 error=true" {
		t.Errorf("first row line = %q, want the oldest fixture row (ordered select)", firstProd.line)
	}
	// Canonical token order, asserted against the fixture generator's own row:
	// labels_json must equal the key-sorted JSON of benchFileRows(0,1)[0]'s
	// complete label map. encoding/json/v2 traverses maps non-deterministically
	// by default, so the expected bytes are produced with the Deterministic
	// option (sorted keys), the same canonical form the SQL builds with
	// list_sort and must reproduce token for token.
	wantJSON, err := json.Marshal(benchFileRows(0, 1)[0].Labels.Map(), json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	if firstProd.labelsJSON.String != string(wantJSON) {
		t.Errorf("first row labels_json = %s, want the canonical fixture JSON %s", firstProd.labelsJSON.String, wantJSON)
	}

	prodInfo, err := profileQuery(t.Context(), conn, prodQ.sql, prodQ.args...)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := profilingFind(prodInfo, "TABLE_SCAN"); !ok {
		t.Error("production profiling tree has no TABLE_SCAN operator")
	}
	if _, ok := profilingMetric(prodInfo, "cpu_time"); !ok {
		t.Error("production profiling tree carries no cpu_time metric")
	}
	logProfilePass(t, "production-select-cache-on", prodInfo)

	// The same verified production select with the footer cache OFF, on its
	// own dedicated connection: footers are re-read from disk on every bind.
	// Presence metrics only — no timing or byte-count assertions here; the
	// Boat report compares the two labeled passes.
	connOff := footerCacheConn(t, db, false)
	prodOffInfo, err := profileQuery(t.Context(), connOff, prodQ.sql, prodQ.args...)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := profilingFind(prodOffInfo, "TABLE_SCAN"); !ok {
		t.Error("production cache-off profiling tree has no TABLE_SCAN operator")
	}
	if _, ok := profilingMetric(prodOffInfo, "cpu_time"); !ok {
		t.Error("production cache-off profiling tree carries no cpu_time metric")
	}
	logProfilePass(t, "production-select-cache-off", prodOffInfo)

	// Synthetic DISTINCT + label-JSON pass. This is NOT the Source's shape —
	// it reconstructs only the stored labels map (no promoted-column merge,
	// no canonical metadata, no row-identity DISTINCT, no ORDER BY), so its
	// numbers cannot substantiate production DISTINCT/label-reconstruction
	// costs; the production-select pass above carries those. It is kept only
	// as a minimal isolation probe of the labels-map JSON operator alone. The
	// query runs once unprofiled so the counted label sets are verified, then
	// once profiled.
	var union strings.Builder
	byGroup := make(map[string][]string)
	var order []string
	for _, f := range fixtures {
		key := strings.Join(f.promoted, "\x00")
		if _, ok := byGroup[key]; !ok {
			order = append(order, key)
		}
		byGroup[key] = append(byGroup[key], f.path)
	}
	dargs := make([]any, 0, len(fixtures))
	for _, key := range order {
		ps := byGroup[key]
		if union.Len() > 0 {
			union.WriteString("\nUNION ALL\n")
		}
		union.WriteString("SELECT CAST(to_json(coalesce(map_from_entries(list_sort(map_entries(\"labels\"))), map([], []))) AS VARCHAR) AS labels_json\nFROM read_parquet([" + benchPlaceholders(len(ps)) + "], union_by_name = true)")
		for _, p := range ps {
			dargs = append(dargs, p)
		}
	}
	distinct := "SELECT count(DISTINCT labels_json) FROM (\n" + union.String() + "\n)"
	var distinctLabels int
	if err := conn.QueryRowContext(t.Context(), distinct, dargs...).Scan(&distinctLabels); err != nil {
		t.Fatal(err)
	}
	if distinctLabels == 0 {
		t.Fatal("synthetic distinct query counted zero distinct label sets; the fixtures are wrong")
	}
	distinctInfo, err := profileQuery(t.Context(), conn, distinct, dargs...)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := profilingFind(distinctInfo, "TABLE_SCAN"); !ok {
		t.Error("synthetic distinct profiling tree has no TABLE_SCAN operator")
	}
	logProfilePass(t, "synthetic-distinct-labels", distinctInfo)
}
