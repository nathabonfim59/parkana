package catalog_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// The tests in this file need PARKANA_TEST_POSTGRES_DSN.

// postgresPoolSize is catalog.Postgres's connection pool bound; the
// early-break tests loop past it to prove iterators give connections back.
const postgresPoolSize = 16

// TestPostgresCanceledWriteRollsBack parks each write method behind a
// transaction that holds the row it needs, cancels its context while it
// waits, and checks the call returns context.Canceled, left nothing behind
// (also the parts it had already written) once the holder lets go, and the
// catalog works afterwards. It mirrors TestSQLiteCanceledWriteRollsBack;
// no timer decides when the cancel happens.
func TestPostgresCanceledWriteRollsBack(t *testing.T) {
	for _, tc := range []struct {
		name string
		// seed prepares the catalog; hold takes the row lock the call waits for.
		seed  func(t *testing.T, ctx context.Context, c *catalog.Postgres)
		hold  func(t *testing.T, ctx context.Context, h *holder)
		call  func(ctx context.Context, c *catalog.Postgres) error
		check func(t *testing.T, ctx context.Context, c *catalog.Postgres)
	}{
		{
			name: "AddFiles",
			seed: func(*testing.T, context.Context, *catalog.Postgres) {},
			// "a" is inserted before the call waits for "m".
			hold: func(t *testing.T, ctx context.Context, h *holder) { h.insertUncommitted(t, ctx, "m") },
			call: func(ctx context.Context, c *catalog.Postgres) error {
				return c.AddFiles(ctx, testFile("a", "api", 0, time.Minute), testFile("m", "api", 0, time.Minute), testFile("z", "api", 0, time.Minute))
			},
			check: func(t *testing.T, ctx context.Context, c *catalog.Postgres) {
				if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}); len(got) != 0 {
					t.Fatalf("files after canceled AddFiles = %v, want none", got)
				}
				if err := c.AddFiles(ctx, testFile("a", "api", 0, time.Minute), testFile("m", "api", 0, time.Minute), testFile("z", "api", 0, time.Minute)); err != nil {
					t.Fatalf("AddFiles after canceled AddFiles: %v", err)
				}
			},
		},
		{
			name: "ReplaceFiles",
			seed: func(t *testing.T, ctx context.Context, c *catalog.Postgres) {
				if err := c.AddFiles(ctx, testFile("old", "api", 0, time.Minute)); err != nil {
					t.Fatalf("seed: %v", err)
				}
			},
			// "old" is locked and marked deleted before the call waits to
			// insert "new".
			hold: func(t *testing.T, ctx context.Context, h *holder) { h.insertUncommitted(t, ctx, "new") },
			call: func(ctx context.Context, c *catalog.Postgres) error {
				return c.ReplaceFiles(ctx, tenant.Default, []string{"old"}, []catalog.File{testFile("new", "api", 0, time.Minute)})
			},
			check: func(t *testing.T, ctx context.Context, c *catalog.Postgres) {
				if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}); !slices.Equal(got, []string{"old"}) {
					t.Fatalf("files after canceled ReplaceFiles = %v, want [old]", got)
				}
				if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"old"}) {
					t.Fatalf("live files after canceled ReplaceFiles = %v, want [old] still live", got)
				}
				if err := c.ReplaceFiles(ctx, tenant.Default, []string{"old"}, []catalog.File{testFile("new", "api", 0, time.Minute)}); err != nil {
					t.Fatalf("ReplaceFiles after canceled ReplaceFiles: %v", err)
				}
			},
		},
		{
			name: "SetSidecar",
			seed: func(t *testing.T, ctx context.Context, c *catalog.Postgres) {
				if err := c.AddFiles(ctx, testFile("f", "api", 0, time.Minute)); err != nil {
					t.Fatalf("seed: %v", err)
				}
			},
			hold: func(t *testing.T, ctx context.Context, h *holder) { h.lockFile(t, ctx, "f") },
			call: func(ctx context.Context, c *catalog.Postgres) error { return c.SetSidecar(ctx, tenant.Default, "f") },
			check: func(t *testing.T, ctx context.Context, c *catalog.Postgres) {
				if got := listFiles(t, c, catalog.Filter{Tenant: tenant.Default}); len(got) != 1 || got[0].HasSidecar {
					t.Fatalf("files after canceled SetSidecar = %+v, want f without a sidecar", got)
				}
				if err := c.SetSidecar(ctx, tenant.Default, "f"); err != nil {
					t.Fatalf("SetSidecar after canceled SetSidecar: %v", err)
				}
			},
		},
		{
			name: "PurgeDeleted",
			seed: func(t *testing.T, ctx context.Context, c *catalog.Postgres) {
				gone := testFile("gone", "api", 0, time.Minute)
				gone.DeletedAt = testStart
				if err := c.AddFiles(ctx, gone); err != nil {
					t.Fatalf("seed: %v", err)
				}
			},
			hold: func(t *testing.T, ctx context.Context, h *holder) { h.lockFile(t, ctx, "gone") },
			call: func(ctx context.Context, c *catalog.Postgres) error {
				_, err := c.PurgeDeleted(ctx, tenant.Default, testStart.Add(time.Hour))
				return err
			},
			check: func(t *testing.T, ctx context.Context, c *catalog.Postgres) {
				if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}); !slices.Equal(got, []string{"gone"}) {
					t.Fatalf("files after canceled PurgeDeleted = %v, want [gone]", got)
				}
				if purged, err := c.PurgeDeleted(ctx, tenant.Default, testStart.Add(time.Hour)); err != nil || !slices.Equal(purged, []string{"gone"}) {
					t.Fatalf("PurgeDeleted after canceled PurgeDeleted = %v, %v, want [gone]", purged, err)
				}
			},
		},
		{
			name: "PutPipeline",
			seed: seedPipeline,
			hold: lockPipelineRow,
			call: func(ctx context.Context, c *catalog.Postgres) error {
				_, err := c.PutPipeline(ctx, pipelineDef(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"v":2}`), 1)
				return err
			},
			check: func(t *testing.T, ctx context.Context, c *catalog.Postgres) {
				checkPipelineUnchanged(t, ctx, c)
				if got, err := c.PutPipeline(ctx, pipelineDef(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"v":2}`), 1); err != nil || got.Version != 2 {
					t.Fatalf("PutPipeline after canceled PutPipeline = %+v, %v", got, err)
				}
				if token := pipelineToken(t, c, tenant.Default); token != 2 {
					t.Fatalf("token after recovery = %d, want 2", token)
				}
			},
		},
		{
			name: "DeletePipeline",
			seed: seedPipeline,
			hold: lockPipelineRow,
			call: func(ctx context.Context, c *catalog.Postgres) error {
				_, err := c.DeletePipeline(ctx, tenant.Default, "p", 1)
				return err
			},
			check: func(t *testing.T, ctx context.Context, c *catalog.Postgres) {
				checkPipelineUnchanged(t, ctx, c)
				if _, err := c.DeletePipeline(ctx, tenant.Default, "p", 1); err != nil {
					t.Fatalf("DeletePipeline after canceled DeletePipeline: %v", err)
				}
				if token := pipelineToken(t, c, tenant.Default); token != 2 {
					t.Fatalf("token after recovery = %d, want 2", token)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, admin := twoInstances(t)
			ctx := boundedContext(t)
			tc.seed(t, ctx, c)
			h := newHolder(t, ctx, admin)
			tc.hold(t, ctx, h)
			done, cancel := startBlocked(t, ctx, admin, h, func(ctx context.Context) error { return tc.call(ctx, c) })
			cancel()
			checkServerCanceled(t, tc.name, <-done)
			// While the holder still holds, nothing may be left waiting
			// behind it: the server must have canceled the statement.
			h.waitNoWaiters(t, ctx, admin)
			if err := h.tx.Rollback(); err != nil {
				t.Fatalf("holder rollback: %v", err)
			}
			tc.check(t, ctx, c)
			exerciseCatalog(t, ctx, c, "after")
		})
	}
}

// pgQueryCanceled is SQLSTATE query_canceled.
const pgQueryCanceled = "57014"

// checkServerCanceled fails t unless err, from a call canceled while its
// statement waited on the server, matches context.Canceled and carries the
// server's query_canceled error: the statement was canceled on the server,
// not just abandoned by the client.
func checkServerCanceled(t *testing.T, what string, err error) {
	t.Helper()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled %s = %v, want context.Canceled", what, err)
	}
	if code := sqlState(err); code != pgQueryCanceled {
		t.Fatalf("canceled %s = %v (SQLSTATE %q), want the server's query_canceled %s", what, err, code, pgQueryCanceled)
	}
}

// TestPostgresCanceledReadIsCanceled parks each read method behind a
// transaction holding an exclusive lock on its table, cancels its context
// while it waits, and checks the call returns context.Canceled with the
// server's query_canceled error, leaves no statement waiting on the server,
// and works afterwards on the same pool.
func TestPostgresCanceledReadIsCanceled(t *testing.T) {
	for _, tc := range []struct {
		name, table string
		call        func(ctx context.Context, c *catalog.Postgres) error
	}{
		{"ListFiles", "files", func(ctx context.Context, c *catalog.Postgres) error {
			for _, err := range c.ListFiles(ctx, catalog.Filter{Tenant: tenant.Default}) {
				if err != nil {
					return err
				}
			}
			return nil
		}},
		{"Streams", "files", func(ctx context.Context, c *catalog.Postgres) error {
			_, err := c.Streams(ctx, tenant.Default)
			return err
		}},
		{"GetPipeline", "pipelines", func(ctx context.Context, c *catalog.Postgres) error {
			_, err := c.GetPipeline(ctx, tenant.Default, "p")
			return err
		}},
		{"ListPipelines", "pipelines", func(ctx context.Context, c *catalog.Postgres) error {
			_, err := c.ListPipelines(ctx, tenant.Default)
			return err
		}},
		{"PipelineChangeToken", "pipelines", func(ctx context.Context, c *catalog.Postgres) error {
			_, err := c.PipelineChangeToken(ctx, tenant.Default)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, admin := twoInstances(t)
			ctx := boundedContext(t)
			if err := c.AddFiles(ctx, testFile("a", "api", 0, time.Minute)); err != nil {
				t.Fatalf("seed: %v", err)
			}
			seedPipeline(t, ctx, c)
			h := newHolder(t, ctx, admin)
			h.exec(t, ctx, `LOCK TABLE `+tc.table+` IN ACCESS EXCLUSIVE MODE`)
			done, cancel := startBlocked(t, ctx, admin, h, func(ctx context.Context) error { return tc.call(ctx, c) })
			cancel()
			checkServerCanceled(t, tc.name, <-done)
			h.waitNoWaiters(t, ctx, admin)
			if err := h.tx.Rollback(); err != nil {
				t.Fatalf("holder rollback: %v", err)
			}
			if err := tc.call(ctx, c); err != nil {
				t.Fatalf("%s after the canceled call: %v", tc.name, err)
			}
			exerciseCatalog(t, ctx, c, "after")
		})
	}
}

// seedPipeline creates pipeline p at version 1 (token 1).
func seedPipeline(t *testing.T, ctx context.Context, c *catalog.Postgres) {
	t.Helper()
	if _, err := c.PutPipeline(ctx, pipelineDef(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"v":1}`), 0); err != nil {
		t.Fatalf("seed pipeline: %v", err)
	}
}

// lockPipelineRow locks pipeline p's row in the holder's transaction.
func lockPipelineRow(t *testing.T, ctx context.Context, h *holder) {
	t.Helper()
	h.exec(t, ctx, `SELECT 1 FROM pipelines WHERE "tenant" = $1 AND "name" = 'p' FOR UPDATE`, string(tenant.Default))
}

// checkPipelineUnchanged fails t unless p is still the seeded version 1
// with token 1.
func checkPipelineUnchanged(t *testing.T, ctx context.Context, c *catalog.Postgres) {
	t.Helper()
	got, err := c.GetPipeline(ctx, tenant.Default, "p")
	if err != nil || got.Version != 1 || string(got.Spec) != `{"v":1}` {
		t.Fatalf("pipeline after canceled write = %+v, %v, want version 1 unchanged", got, err)
	}
	if token := pipelineToken(t, c, tenant.Default); token != 1 {
		t.Fatalf("token after canceled write = %d, want 1", token)
	}
}

// TestPostgresPreCanceledContext checks every method returns
// context.Canceled for a context canceled before the call, and writes
// nothing.
func TestPostgresPreCanceledContext(t *testing.T) {
	c, _, _ := twoInstances(t)
	ctx := boundedContext(t)
	if err := c.AddFiles(ctx, testFile("a", "api", 0, time.Minute)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	seedPipeline(t, ctx, c)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	def := pipelineDef(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{"v":2}`)
	for name, call := range map[string]func() error{
		"AddFiles":     func() error { return c.AddFiles(canceled, testFile("b", "api", 0, time.Minute)) },
		"ReplaceFiles": func() error { return c.ReplaceFiles(canceled, tenant.Default, []string{"a"}, nil) },
		"SetSidecar":   func() error { return c.SetSidecar(canceled, tenant.Default, "a") },
		"Streams":      func() error { _, err := c.Streams(canceled, tenant.Default); return err },
		"PurgeDeleted": func() error { _, err := c.PurgeDeleted(canceled, tenant.Default, time.Now()); return err },
		"ListFiles": func() error {
			var last error
			for _, err := range c.ListFiles(canceled, catalog.Filter{Tenant: tenant.Default}) {
				last = err
			}
			return last
		},
		"GetPipeline":         func() error { _, err := c.GetPipeline(canceled, tenant.Default, "p"); return err },
		"ListPipelines":       func() error { _, err := c.ListPipelines(canceled, tenant.Default); return err },
		"PipelineChangeToken": func() error { _, err := c.PipelineChangeToken(canceled, tenant.Default); return err },
		"PutPipeline":         func() error { _, err := c.PutPipeline(canceled, def, 1); return err },
		"DeletePipeline":      func() error { _, err := c.DeletePipeline(canceled, tenant.Default, "p", 1); return err },
	} {
		if err := call(); !errors.Is(err, context.Canceled) {
			t.Errorf("%s with a canceled context = %v, want context.Canceled", name, err)
		}
	}
	if got := listFiles(t, c, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}); len(got) != 1 || got[0].Key != "a" || got[0].Deleted() || got[0].HasSidecar {
		t.Fatalf("files after canceled calls = %+v, want a unchanged", got)
	}
	checkPipelineUnchanged(t, ctx, c)
}

// TestPostgresCancelMidListFiles cancels a ListFiles after its first row,
// more times than the pool has connections: each iteration stops with
// context.Canceled after exactly one file, and the catalog works afterwards,
// so canceled iterators give their connections back.
func TestPostgresCancelMidListFiles(t *testing.T) {
	c, _, _ := twoInstances(t)
	ctx := boundedContext(t)
	files := make([]catalog.File, 500)
	for i := range files {
		files[i] = testFile(fmt.Sprintf("f%03d", i), "api", time.Duration(i)*time.Millisecond, time.Duration(i+1)*time.Millisecond)
	}
	if err := c.AddFiles(ctx, files...); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for i := range 2 * postgresPoolSize {
		listCtx, cancel := context.WithCancel(ctx)
		yielded := 0
		var listErr error
		for _, err := range c.ListFiles(listCtx, catalog.Filter{Tenant: tenant.Default}) {
			if err != nil {
				listErr = err
				break
			}
			yielded++
			if yielded == 1 {
				cancel()
			}
		}
		cancel()
		if !errors.Is(listErr, context.Canceled) {
			t.Fatalf("iteration %d: error = %v, want context.Canceled", i, listErr)
		}
		if yielded != 1 {
			t.Fatalf("iteration %d: yielded %d files, want 1", i, yielded)
		}
	}
	exerciseCatalog(t, ctx, c, "after")
}

// TestPostgresEarlyBreakReleasesConnections breaks out of ListFiles after
// one file more times than the pool has connections, then (within the
// test's timeout, so a leaked connection fails instead of hanging) calls
// every method; inside one open iteration the other methods, including a
// nested ListFiles and a write, must not deadlock either. It mirrors
// TestSQLiteEarlyBreakReentrant.
func TestPostgresEarlyBreakReleasesConnections(t *testing.T) {
	c, _, _ := twoInstances(t)
	ctx := boundedContext(t)
	if err := c.AddFiles(ctx,
		testFile("a", "api", 0, time.Minute),
		testFile("b", "api", time.Minute, 2*time.Minute),
		testFile("c", "api", 2*time.Minute, 3*time.Minute),
	); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for i := range 3 * postgresPoolSize {
		found := 0
		for _, err := range c.ListFiles(ctx, catalog.Filter{Tenant: tenant.Default}) {
			if err != nil {
				t.Fatalf("iteration %d: ListFiles: %v", i, err)
			}
			found++
			break
		}
		if found != 1 {
			t.Fatalf("iteration %d: yielded %d files before break, want 1", i, found)
		}
	}

	for f, err := range c.ListFiles(ctx, catalog.Filter{Tenant: tenant.Default}) {
		if err != nil {
			t.Fatalf("ListFiles: %v", err)
		}
		if _, err := c.Streams(ctx, tenant.Default); err != nil {
			t.Fatalf("reentrant Streams: %v", err)
		}
		if err := c.AddFiles(ctx, testFile("during", f.Stream, 3*time.Minute, 4*time.Minute)); err != nil {
			t.Fatalf("reentrant AddFiles: %v", err)
		}
		nested := 0
		for range c.ListFiles(ctx, catalog.Filter{Tenant: tenant.Default}) {
			nested++
		}
		if nested != 4 {
			t.Fatalf("nested ListFiles yielded %d, want 4", nested)
		}
		break
	}
	if got := listKeys(t, c, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"a", "b", "c", "during"}) {
		t.Fatalf("after early breaks = %v", got)
	}
	exerciseCatalog(t, ctx, c, "after")
}
