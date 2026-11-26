package catalog_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/catalog/catalogtest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// The tests in this file run two *catalog.Postgres on one schema, as two
// processes would, and need PARKANA_TEST_POSTGRES_DSN. Each is bounded by
// testTimeout so a deadlock or a lost wakeup fails instead of hanging.

// testTimeout bounds every Postgres concurrency and cancellation test.
const testTimeout = time.Minute

// twoInstances opens two catalogs on one fresh schema and a plain admin
// pool on it.
func twoInstances(t *testing.T) (a, b *catalog.Postgres, admin *sql.DB) {
	t.Helper()
	dsn := catalogtest.PostgresDSN(t)
	return openPostgres(t, dsn), openPostgres(t, dsn), pgAdmin(t, dsn)
}

// boundedContext returns t's context limited to testTimeout.
func boundedContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}

// race runs every fn at once (after a common start barrier) and returns
// their errors in order.
func race(fns ...func() error) []error {
	start := make(chan struct{})
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	for i, fn := range fns {
		wg.Go(func() {
			<-start
			errs[i] = fn()
		})
	}
	close(start)
	wg.Wait()
	return errs
}

// TestPostgresAddFilesLockOrder proves AddFiles inserts a batch in key
// order whatever order the caller gives: with key "m" held uncommitted by
// another transaction, AddFiles(z, m, a) has inserted "a" but not yet "z"
// when it blocks. Concurrent inserters of overlapping batches therefore
// wait on each other in one order instead of deadlocking. When the holder
// commits "m", the batch fails with ErrExists and leaves nothing.
func TestPostgresAddFilesLockOrder(t *testing.T) {
	c, _, admin := twoInstances(t)
	ctx := boundedContext(t)

	h := newHolder(t, ctx, admin)
	h.insertUncommitted(t, ctx, "m")
	done, _ := startBlocked(t, ctx, admin, h, func(ctx context.Context) error {
		return c.AddFiles(ctx, testFile("z", "api", 0, time.Minute), testFile("m", "api", 0, time.Minute), testFile("a", "api", 0, time.Minute))
	})
	if err := probeInsert(t, ctx, admin, "a"); sqlState(err) != pgLockNotAvailable {
		t.Fatalf("probe insert a = %v, want lock_not_available: AddFiles must insert a before it waits for m", err)
	}
	if err := probeInsert(t, ctx, admin, "z"); err != nil {
		t.Fatalf("probe insert z = %v, want nil: AddFiles must not insert z before m (key order)", err)
	}
	if err := h.tx.Commit(); err != nil {
		t.Fatalf("holder commit: %v", err)
	}
	if err := <-done; !errors.Is(err, catalog.ErrExists) {
		t.Fatalf("AddFiles after holder committed m = %v, want ErrExists", err)
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}); !slices.Equal(got, []string{"m"}) {
		t.Fatalf("files = %v, want only the holder's [m]", got)
	}
}

// TestPostgresReplaceFilesLockOrder proves ReplaceFiles locks its remove
// rows in key order, not in the order the rows are stored: the rows are
// written z, m, a (so a sequential or bitmap scan meets z first; index scans
// are disabled for the catalog's session so the plan cannot hide a missing
// ORDER BY), "m" is locked by another transaction, and when ReplaceFiles
// blocks it holds "a" but not "z". When the holder lets go the replace
// completes.
func TestPostgresReplaceFilesLockOrder(t *testing.T) {
	dsn := catalogtest.PostgresDSN(t)
	c := openPostgres(t, withParam(t, dsn, "enable_indexscan", "off"))
	admin := pgAdmin(t, dsn)
	ctx := boundedContext(t)
	for _, key := range []string{"z", "m", "a"} {
		if err := c.AddFiles(ctx, testFile(key, "api", 0, time.Minute)); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}

	h := newHolder(t, ctx, admin)
	h.lockFile(t, ctx, "m")
	done, _ := startBlocked(t, ctx, admin, h, func(ctx context.Context) error {
		return c.ReplaceFiles(ctx, tenant.Default, []string{"z", "m", "a"}, []catalog.File{testFile("merged", "api", 0, time.Minute)})
	})
	if err := probeLock(t, ctx, admin, "a"); sqlState(err) != pgLockNotAvailable {
		t.Fatalf("probe lock a = %v, want lock_not_available: ReplaceFiles must lock a before it waits for m", err)
	}
	if err := probeLock(t, ctx, admin, "z"); err != nil {
		t.Fatalf("probe lock z = %v, want nil: ReplaceFiles must not lock z before m (key order)", err)
	}
	if err := h.tx.Rollback(); err != nil {
		t.Fatalf("holder rollback: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("ReplaceFiles after holder released m: %v", err)
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"merged"}) {
		t.Fatalf("live files = %v, want [merged]", got)
	}
}

// TestPostgresConcurrentOverlappingAddFiles races AddFiles batches that
// share keys with their neighbours, from both instances and in crossed key
// orders, over many rounds: every batch is added whole or fails whole with
// ErrExists (never another error, such as a deadlock), and exactly the
// winners' keys are listed.
func TestPostgresConcurrentOverlappingAddFiles(t *testing.T) {
	a, b, _ := twoInstances(t)
	ctx := boundedContext(t)
	const (
		rounds  = 25
		writers = 6
	)
	for r := range rounds {
		stream := fmt.Sprintf("r%02d", r)
		shared := func(i int) string { return fmt.Sprintf("%s-s%d", stream, i%writers) }
		batches := make([][]catalog.File, writers)
		fns := make([]func() error, writers)
		for i := range writers {
			keys := []string{fmt.Sprintf("%s-u%d", stream, i), shared(i), shared(i + 1)}
			if i%2 == 1 {
				slices.Reverse(keys)
			}
			for _, k := range keys {
				batches[i] = append(batches[i], testFile(k, stream, 0, time.Minute))
			}
			c := []*catalog.Postgres{a, b}[i%2]
			fns[i] = func() error { return c.AddFiles(ctx, batches[i]...) }
		}
		var want []string
		for i, err := range race(fns...) {
			switch {
			case err == nil:
				for _, f := range batches[i] {
					want = append(want, f.Key)
				}
			case !errors.Is(err, catalog.ErrExists):
				t.Fatalf("round %d writer %d: %v, want nil or ErrExists", r, i, err)
			}
		}
		if len(want) == 0 {
			t.Fatalf("round %d: no batch won", r)
		}
		slices.Sort(want)
		for _, c := range []*catalog.Postgres{a, b} {
			got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default, Stream: stream})
			slices.Sort(got)
			if !slices.Equal(got, want) {
				t.Fatalf("round %d: listed %v, want exactly the winners' keys %v", r, got, want)
			}
		}
	}
}

// TestPostgresConcurrentReplaceFiles races ReplaceFiles calls from both
// instances that remove the same keys, given in the same or in crossed
// order, each adding its own file, over many rounds: exactly one wins, every
// loser gets ErrNotFound (never a deadlock or another error) and changed
// nothing, so only the winner's file is added.
func TestPostgresConcurrentReplaceFiles(t *testing.T) {
	for _, crossed := range []bool{false, true} {
		t.Run(fmt.Sprintf("crossed=%v", crossed), func(t *testing.T) {
			a, b, _ := twoInstances(t)
			ctx := boundedContext(t)
			const (
				rounds     = 50
				contenders = 4
			)
			for r := range rounds {
				stream := fmt.Sprintf("r%02d", r)
				k1, k2 := stream+"-k1", stream+"-k2"
				if err := a.AddFiles(ctx, testFile(k1, stream, 0, time.Minute), testFile(k2, stream, time.Minute, 2*time.Minute)); err != nil {
					t.Fatalf("round %d seed: %v", r, err)
				}
				fns := make([]func() error, contenders)
				for i := range contenders {
					remove := []string{k1, k2}
					if crossed && i%2 == 1 {
						remove = []string{k2, k1}
					}
					add := testFile(fmt.Sprintf("%s-add%d", stream, i), stream, 0, 2*time.Minute)
					c := []*catalog.Postgres{a, b}[i%2]
					fns[i] = func() error { return c.ReplaceFiles(ctx, tenant.Default, remove, []catalog.File{add}) }
				}
				winner := -1
				for i, err := range race(fns...) {
					switch {
					case err == nil:
						if winner >= 0 {
							t.Fatalf("round %d: contenders %d and %d both replaced %s, %s", r, winner, i, k1, k2)
						}
						winner = i
					case !errors.Is(err, catalog.ErrNotFound):
						t.Fatalf("round %d contender %d: %v, want nil or ErrNotFound", r, i, err)
					}
				}
				if winner < 0 {
					t.Fatalf("round %d: no contender won", r)
				}
				want := []string{fmt.Sprintf("%s-add%d", stream, winner)}
				if got := listKeys(t, b, catalog.Filter{Tenant: tenant.Default, Stream: stream}); !slices.Equal(got, want) {
					t.Fatalf("round %d: live %v, want only the winner's %v", r, got, want)
				}
				all := listKeys(t, a, catalog.Filter{Tenant: tenant.Default, Stream: stream, IncludeDeleted: true})
				slices.Sort(all)
				if wantAll := []string{want[0], k1, k2}; !slices.Equal(all, slices.Sorted(slices.Values(wantAll))) {
					t.Fatalf("round %d: all files %v, want %v (no loser file)", r, all, wantAll)
				}
			}
		})
	}
}

// TestPostgresConcurrentPipelineCreate races creates of one pipeline name
// from goroutines spread over both instances, each with its own spec, over
// several rounds: exactly one wins per round, the rest get
// ErrVersionConflict, the stored pipeline is the winner's at version 1, and
// the change token counts one write per round.
func TestPostgresConcurrentPipelineCreate(t *testing.T) {
	a, b, _ := twoInstances(t)
	ctx := boundedContext(t)
	const (
		rounds  = 10
		writers = 16
	)
	for r := range rounds {
		name := fmt.Sprintf("race%02d", r)
		fns := make([]func() error, writers)
		for i := range writers {
			c := []*catalog.Postgres{a, b}[i%2]
			def := pipelineDef(tenant.Default, name, catalog.PipelineStageIngest, "api", fmt.Sprintf(`{"writer":%d}`, i))
			fns[i] = func() error { _, err := c.PutPipeline(ctx, def, 0); return err }
		}
		winner := -1
		for i, err := range race(fns...) {
			switch {
			case err == nil:
				if winner >= 0 {
					t.Fatalf("round %d: writers %d and %d both created %s", r, winner, i, name)
				}
				winner = i
			case !errors.Is(err, catalog.ErrVersionConflict):
				t.Fatalf("round %d writer %d: %v, want nil or ErrVersionConflict", r, i, err)
			}
		}
		if winner < 0 {
			t.Fatalf("round %d: no writer won", r)
		}
		got, err := b.GetPipeline(ctx, tenant.Default, name)
		if err != nil || got.Version != 1 || string(got.Spec) != fmt.Sprintf(`{"writer":%d}`, winner) {
			t.Fatalf("round %d: stored %+v, %v, want writer %d's spec at version 1", r, got, err, winner)
		}
		if token := pipelineToken(t, a, tenant.Default); token != int64(r+1) {
			t.Fatalf("round %d: token = %d, want %d", r, token, r+1)
		}
	}
}

// TestPostgresPipelineTokenAcrossInstances has writers on both instances
// create, update and delete pipelines of their own names concurrently: every
// write succeeds, the change token each writer reads after its write
// strictly increases, and the final token equals the number of writes, so
// no two writes, even from different processes, got the same token.
func TestPostgresPipelineTokenAcrossInstances(t *testing.T) {
	a, b, _ := twoInstances(t)
	ctx := boundedContext(t)
	const (
		writers = 6
		updates = 8
	)
	// Per writer: one create, the updates, one delete.
	const writesPerWriter = updates + 2
	fns := make([]func() error, writers)
	for w := range writers {
		c := []*catalog.Postgres{a, b}[w%2]
		name := fmt.Sprintf("w%d", w)
		fns[w] = func() error {
			last := int64(-1)
			observe := func() error {
				token, err := c.PipelineChangeToken(ctx, tenant.Default)
				if err != nil {
					return err
				}
				if token <= last {
					return fmt.Errorf("writer %s: token %d after write, not above the %d seen before", name, token, last)
				}
				last = token
				return nil
			}
			version := int64(0)
			for i := range updates + 1 {
				def := pipelineDef(tenant.Default, name, catalog.PipelineStageIngest, "api", fmt.Sprintf(`{"i":%d}`, i))
				got, err := c.PutPipeline(ctx, def, version)
				if err != nil {
					return fmt.Errorf("writer %s put %d: %w", name, i, err)
				}
				version = got.Version
				if err := observe(); err != nil {
					return err
				}
			}
			if _, err := c.DeletePipeline(ctx, tenant.Default, name, version); err != nil {
				return fmt.Errorf("writer %s delete: %w", name, err)
			}
			return observe()
		}
	}
	for _, err := range race(fns...) {
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []*catalog.Postgres{a, b} {
		if token := pipelineToken(t, c, tenant.Default); token != writers*writesPerWriter {
			t.Fatalf("final token = %d, want %d (one per successful write)", token, writers*writesPerWriter)
		}
	}
	if got := pipelineNames(t, b, tenant.Default); len(got) != 0 {
		t.Fatalf("live pipelines = %v, want none", got)
	}
}

// TestPostgresReaderSeesOtherInstance checks every write committed on one
// instance is visible to the next read on the other, with no cache or
// snapshot in between.
func TestPostgresReaderSeesOtherInstance(t *testing.T) {
	a, b, _ := twoInstances(t)
	ctx := boundedContext(t)

	if err := a.AddFiles(ctx, testFile("x", "api", 0, time.Minute)); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	if got := listKeys(t, b, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"x"}) {
		t.Fatalf("b sees %v after a's AddFiles, want [x]", got)
	}
	if got, err := b.Streams(ctx, tenant.Default); err != nil || !slices.Equal(got, []string{"api"}) {
		t.Fatalf("b Streams = %v, %v, want [api]", got, err)
	}
	if err := a.SetSidecar(ctx, tenant.Default, "x"); err != nil {
		t.Fatalf("SetSidecar: %v", err)
	}
	if got := listFiles(t, b, catalog.Filter{Tenant: tenant.Default}); len(got) != 1 || !got[0].HasSidecar {
		t.Fatalf("b sees %+v after a's SetSidecar, want x with a sidecar", got)
	}
	if err := a.ReplaceFiles(ctx, tenant.Default, []string{"x"}, []catalog.File{testFile("y", "web", 0, time.Minute)}); err != nil {
		t.Fatalf("ReplaceFiles: %v", err)
	}
	if got := listKeys(t, b, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"y"}) {
		t.Fatalf("b sees %v after a's ReplaceFiles, want [y]", got)
	}
	if got, err := b.Streams(ctx, tenant.Default); err != nil || !slices.Equal(got, []string{"web"}) {
		t.Fatalf("b Streams after replace = %v, %v, want [web]", got, err)
	}
	if purged, err := a.PurgeDeleted(ctx, tenant.Default, time.Now().Add(time.Hour)); err != nil || !slices.Equal(purged, []string{"x"}) {
		t.Fatalf("PurgeDeleted = %v, %v, want [x]", purged, err)
	}
	if got := listKeys(t, b, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}); !slices.Equal(got, []string{"y"}) {
		t.Fatalf("b sees %v after a's purge, want [y]", got)
	}

	if _, err := a.PutPipeline(ctx, pipelineDef(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"v":1}`), 0); err != nil {
		t.Fatalf("PutPipeline: %v", err)
	}
	if got, err := b.GetPipeline(ctx, tenant.Default, "p"); err != nil || got.Version != 1 {
		t.Fatalf("b GetPipeline after a's create = %+v, %v", got, err)
	}
	if got := pipelineNames(t, b, tenant.Default); !slices.Equal(got, []string{"p"}) {
		t.Fatalf("b ListPipelines = %v, want [p]", got)
	}
	if got := pipelineToken(t, b, tenant.Default); got != 1 {
		t.Fatalf("b token = %d, want 1", got)
	}
	if _, err := a.DeletePipeline(ctx, tenant.Default, "p", 1); err != nil {
		t.Fatalf("DeletePipeline: %v", err)
	}
	if _, err := b.GetPipeline(ctx, tenant.Default, "p"); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("b GetPipeline after a's delete = %v, want ErrNotFound", err)
	}
	if got := pipelineToken(t, b, tenant.Default); got != 2 {
		t.Fatalf("b token after delete = %d, want 2", got)
	}
}
