// The querier's plan.Source: files listed by the catalog, resolved to local
// paths, read as Parquet through DuckDB with the SQL compiled in sql.go.
package query

import (
	"context"
	"database/sql"
	"encoding/json/v2"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"maps"
	"slices"
	"strings"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/plan"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// fileResolver acquires a local readable path for one stored object, holding
// it usable until released. The querier's object cache implements it; tests
// supply fixture resolvers over plain directories.
type fileResolver interface {
	// Acquire returns the local path of object key for tenant t, keeping
	// the path usable until the returned release function is called, once,
	// by the caller that got it. A lease only protects against the cache's
	// own eviction: the FS compactor may still delete an object after its
	// delay, so readers must tolerate disappearance and retry. A missing
	// object returns an error wrapping fs.ErrNotExist wherever the backend
	// can tell that from other failures.
	Acquire(ctx context.Context, t tenant.ID, key string) (path string, release func(), err error)
}

// source reads rows out of the Parquet objects the catalog lists, through
// DuckDB. It owns no resources of its own: the role that built it owns the
// *sql.DB, the catalog, the resolver and, when pruning is enabled, the
// sidecar cache.
type source struct {
	db           *sql.DB
	cat          catalog.Catalog
	resolve      fileResolver
	streamLabels []string

	// sidecars decodes search sidecars for index pruning; nil disables it
	// and every listed file stays a scan candidate.
	sidecars *sidecarCache
	// metrics counts scanned and pruned files; nil disables counting while
	// pruning itself keeps working.
	metrics *scanMetrics
	// log receives sidecar-failure debug logs; never nil.
	log *slog.Logger
}

// sourceOptions carries a source's optional collaborators. Zero values keep
// newSource's behavior — no sidecar cache, no scan counting; label-equality
// pruning through catalog.Filter.Matches is always active either way.
type sourceOptions struct {
	// sidecars is the decoded search-sidecar cache index pruning consults;
	// nil disables index pruning.
	sidecars *sidecarCache
	// metrics receives the scanned/pruned file and byte counters; nil
	// disables counting.
	metrics *scanMetrics
	// logger receives sidecar failure debug logs; nil discards.
	logger *slog.Logger
}

// newSource returns a Source reading cat's live files through db, resolving
// object keys with resolve. streamLabels is the configured stream-label
// priority (config.Config.StreamLabels, e.g. ["service_name", "job"]); it is
// used only to prune the catalog listing, exactly as
// logs.Labels.StreamName picks the dataset at ingest. Label-equality pruning
// through catalog.Filter.Matches is always active; use newSourceWithOptions
// to also enable search-sidecar index pruning and scan metrics.
func newSource(db *sql.DB, cat catalog.Catalog, resolve fileResolver, streamLabels []string) *source {
	return newSourceWithOptions(db, cat, resolve, streamLabels, sourceOptions{})
}

// newSourceWithOptions is newSource with the pruning and metrics collaborators
// of opts wired in.
func newSourceWithOptions(db *sql.DB, cat catalog.Catalog, resolve fileResolver, streamLabels []string, opts sourceOptions) *source {
	return &source{
		db:           db,
		cat:          cat,
		resolve:      resolve,
		streamLabels: slices.Clone(streamLabels),
		sidecars:     opts.sidecars,
		metrics:      opts.metrics,
		log:          cmpLogger(opts.logger),
	}
}

// Select implements plan.Source.
//
// Attempt boundary for retries: an attempt is the catalog listing, the label
// and index pruning of it, then acquiring every remaining lease, then
// compiling and running the statement, then priming its first row — all
// before the first row is yielded. Every compiled query wraps its groups'
// UNION ALL in one ORDER BY, which DuckDB runs as a blocking sort over the
// union, so by the first fetched row every input file has been opened and
// read. openRows retries that whole attempt once, after a fresh listing and
// fresh pruning, when a file disappears between listing and the first row
// (with the object key where known); nothing has been yielded yet, so a retry
// yields no duplicates. After the first row the sort has consumed all inputs,
// so later file deletion cannot corrupt what remains of the iteration and a
// failure there is reported, never retried.
func (s *source) Select(ctx context.Context, sel plan.Selection) iter.Seq2[plan.Row, error] {
	return func(yield func(plan.Row, error) bool) {
		if err := sel.Validate(); err != nil {
			yield(plan.Row{}, err)
			return
		}
		if err := ctx.Err(); err != nil {
			yield(plan.Row{}, fmt.Errorf("query: select: %w", err))
			return
		}
		rows, err := s.openRows(ctx, sel, func(groups []fileGroup) selectQuery {
			return selectSQL(sel, groups)
		})
		if err != nil {
			yield(plan.Row{}, err)
			return
		}
		if rows == nil {
			return
		}
		defer rows.Close()
		for rows.Next() {
			r, err := scanRow(rows)
			if err != nil {
				yield(plan.Row{}, errors.Join(err, rows.Close()))
				return
			}
			if !yield(r, nil) {
				return
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			yield(plan.Row{}, fmt.Errorf("query: select: %w", err))
		}
	}
}

// groupFiles groups listed catalog files (and their acquired paths, in the
// same order) by promoted-label set: files with identical physical schema
// shape share one read_parquet and one normalization subquery. Groups keep
// first-seen (listing) order, so compiled SQL is deterministic. A file's
// promoted names are the exact-case keys of its LabelValues; the schema
// contract guarantees the set has no internal case collisions.
func groupFiles(files []catalog.File, paths []string) []fileGroup {
	var groups []fileGroup
	byKey := make(map[string]int)
	for i, f := range files {
		promoted := slices.Sorted(maps.Keys(f.LabelValues))
		key := strings.Join(promoted, "\x00")
		gi, ok := byKey[key]
		if !ok {
			gi = len(groups)
			byKey[key] = gi
			groups = append(groups, fileGroup{promoted: promoted})
		}
		groups[gi].paths = append(groups[gi].paths, paths[i])
	}
	return groups
}

// listObjects returns the catalog files sel can read. The catalog is listed
// exactly once, by tenant, time overlap and the derived stream; label
// equality pruning then runs here through catalog.Filter.Matches itself —
// never reimplemented — over a filter whose Match map holds every MatchEqual
// matcher of sel, empty values included. Running it in the querier instead of
// pushing Match into the listing keeps one catalog pass and lets the
// pruned_labels counter record exactly the files the method rejects: since
// the listing already enforced tenant, stream, time and liveness, every
// rejection is a complete-summary label rejection. Matches keeps files whose
// summary is incomplete (LabelsComplete false, the pre-phase-3 rows) or has
// no entry for a constrained label (the label is not promoted there, so its
// values live only in the labels map), and per-group SQL predicates re-check
// every matcher exactly, so pruning can never change results.
func (s *source) listObjects(ctx context.Context, sel plan.Selection) ([]catalog.File, error) {
	f := catalog.Filter{Tenant: sel.Tenant, From: sel.From, Through: sel.Through}
	if stream, ok := deriveStream(s.streamLabels, sel.Matchers); ok {
		f.Stream = stream
	}
	prune := f
	prune.Match = matchEqualities(sel.Matchers)
	var files []catalog.File
	for file, err := range s.cat.ListFiles(ctx, f) {
		if err != nil {
			return nil, fmt.Errorf("query: list files: %w", err)
		}
		if !prune.Matches(file) {
			s.countFiles("pruned_labels", 1)
			continue
		}
		files = append(files, file)
	}
	return files, nil
}

// matchEqualities returns the label equalities of ms as a catalog Match map,
// empty values included: an = matcher forces the label's effective value,
// and complete summaries record "" for rows without the label, so even
// {foo=""} constraints prune safely. Later equalities on the same name win;
// SQL re-checks every matcher exactly, so a contradictory pair never loses
// rows. The result is nil when ms has no equality matcher.
func matchEqualities(ms []plan.Matcher) map[string]string {
	var m map[string]string
	for _, matcher := range ms {
		if matcher.Type == plan.MatchEqual {
			if m == nil {
				m = make(map[string]string, len(ms))
			}
			m[matcher.Name] = matcher.Value
		}
	}
	return m
}

// containsNeedles returns the needles of sel's |= line filters: the only line
// filters a search sidecar can answer. Every other operator (line !=, |~, !~)
// stays a scan; empty and too-short needles are passed through because
// index.Filter.MayContain answers them conservatively (true) by itself.
func containsNeedles(sel plan.Selection) []string {
	var needles []string
	for _, f := range sel.LineFilters {
		if f.Type == plan.LineContains {
			needles = append(needles, f.Value)
		}
	}
	return needles
}

// pruneIndex drops files whose decoded search sidecar proves that no line of
// the file can contain one of the |= needles: with ANDed filters, any definite
// absence drops the file. Only files the catalog flags HasSidecar are checked;
// every other file, every fetch or decode failure (retained, debug-logged,
// and counted by the sidecar cache's own metrics), and every needle the
// filter cannot answer stays a scan candidate — index pruning never
// introduces a false negative. ctx is checked before each candidate so a
// canceled request stops pruning immediately and the caller's own context
// fails the attempt at acquisition or in SQL instead of walking a long
// candidate list.
func (s *source) pruneIndex(ctx context.Context, sel plan.Selection, files []catalog.File) []catalog.File {
	needles := containsNeedles(sel)
	if s.sidecars == nil || len(needles) == 0 || len(files) == 0 {
		return files
	}
	kept := files[:0]
	for i, f := range files {
		if ctx.Err() != nil {
			// Cancellation is not a pruning decision: keep every remaining
			// file and let the attempt fail fast downstream.
			kept = append(kept, files[i:]...)
			return kept
		}
		if f.HasSidecar && !s.mayContainAll(ctx, sel.Tenant, f.Key, needles) {
			s.countFiles("pruned_index", 1)
			continue
		}
		kept = append(kept, f)
	}
	return kept
}

// mayContainAll reports whether the sidecar of tenant t's data object key
// leaves any possibility that some line contains every needle. A sidecar that
// cannot be fetched or decoded fails open: the file stays scannable.
func (s *source) mayContainAll(ctx context.Context, t tenant.ID, key string, needles []string) bool {
	filter, err := s.sidecars.Get(ctx, t, key)
	if err != nil {
		s.log.Debug("query: sidecar unavailable, keeping file scannable", "key", key, "err", err)
		return true
	}
	for _, needle := range needles {
		if !filter.MayContain(needle) {
			return false
		}
	}
	return true
}

// countScan records one attempt's submission to DuckDB: every candidate file
// counts as scanned, with its catalog Bytes, the moment the statement is about
// to run — after acquisitions succeeded — so failed SQL and disappearance
// retries count while a failed acquisition (nothing submitted) does not.
func (s *source) countScan(files []catalog.File) {
	if s.metrics == nil {
		return
	}
	var bytes int64
	for _, f := range files {
		bytes += f.Bytes
	}
	s.metrics.files.WithLabelValues("scanned").Add(float64(len(files)))
	s.metrics.bytes.Add(float64(bytes))
}

// countFiles records n files under result when metrics are wired.
func (s *source) countFiles(result string, n int) {
	if s.metrics == nil || n == 0 {
		return
	}
	s.metrics.files.WithLabelValues(result).Add(float64(n))
}

// deriveStream returns the dataset (stream) the matchers force, following
// the configured stream-label priority exactly as logs.Labels.StreamName
// does at ingest. ok is false when the selector leaves the stream
// undetermined: a priority label without an equality matcher may still be
// present with any value, naming another dataset — a job equality alone
// proves nothing while service_name has priority. An equality to "" forces
// the label absent or empty, so StreamName falls through to the next key;
// when every key is forced empty the dataset is the default stream. The
// stream names a dataset and is never read back as a label.
func deriveStream(streamLabels []string, ms []plan.Matcher) (stream string, ok bool) {
	for _, key := range streamLabels {
		v, forced := equalValue(ms, key)
		if !forced {
			return "", false
		}
		if v != "" {
			return logs.SanitizeStream(v), true
		}
	}
	return logs.DefaultStream, true
}

// equalValue returns the value the equality matchers on name force, and
// whether one exists at all.
func equalValue(ms []plan.Matcher, name string) (string, bool) {
	for _, m := range ms {
		if m.Name == name && m.Type == plan.MatchEqual {
			return m.Value, true
		}
	}
	return "", false
}

// acquire leases every listed object for t and returns the local paths plus
// a release for all of them. A failed acquisition releases what it already
// holds and wraps the failing object's key, so a disappeared file is
// identifiable in the error before any row is yielded.
func (s *source) acquire(ctx context.Context, t tenant.ID, files []catalog.File) ([]string, func(), error) {
	paths := make([]string, 0, len(files))
	var releases []func()
	release := func() {
		for _, r := range releases {
			r()
		}
	}
	for _, f := range files {
		p, rel, err := s.resolve.Acquire(ctx, t, f.Key)
		if err != nil {
			release()
			return nil, func() {}, fmt.Errorf("query: acquire %s: %w", f.Key, err)
		}
		paths = append(paths, p)
		releases = append(releases, rel)
	}
	return paths, release, nil
}

// decodeMap parses one canonical label or meta map JSON. NULL or an empty
// string decodes to a nil map: no labels.
func decodeMap(ns sql.NullString) (map[string]string, error) {
	if !ns.Valid || ns.String == "" {
		return nil, nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(ns.String), &m); err != nil {
		return nil, err
	}
	return m, nil
}
