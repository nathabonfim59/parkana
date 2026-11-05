package catalog

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// TestSQLiteCanceledWriteRollsBack holds an immediate write transaction on one
// handle, forces a deadline on a second handle's update, then checks the
// update left no row or token change and that the second handle can write
// afterwards.
func TestSQLiteCanceledWriteRollsBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "catalog.db")

	open := func() *SQLite {
		c, err := NewSQLite(path)
		if err != nil {
			t.Fatalf("NewSQLite: %v", err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	holder := open()
	writer := open()

	def := func(spec string) PipelineDefinition {
		return PipelineDefinition{
			Tenant: tenant.Default, Name: "p", Stage: PipelineStageIngest,
			Selector: "api", Spec: []byte(spec),
		}
	}
	if _, err := writer.PutPipeline(ctx, def(`{"v":1}`), 0); err != nil {
		t.Fatalf("seed: %v", err)
	}

	conn, err := holder.write.Conn(ctx)
	if err != nil {
		t.Fatalf("holder connection: %v", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("holder begin immediate: %v", err)
	}
	released := false
	defer func() {
		if !released {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`)
		}
	}()

	writeCtx, writeCancel := context.WithTimeout(ctx, 150*time.Millisecond)
	_, err = writer.PutPipeline(writeCtx, def(`{"v":2}`), 1)
	writeCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("contended update = %v, want context.DeadlineExceeded", err)
	}

	if _, err := conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`); err != nil {
		t.Fatalf("holder rollback: %v", err)
	}
	released = true

	got, err := writer.GetPipeline(ctx, tenant.Default, "p")
	if err != nil {
		t.Fatalf("GetPipeline after canceled write: %v", err)
	}
	if got.Version != 1 || string(got.Spec) != `{"v":1}` {
		t.Fatalf("canceled write mutated the row: %+v", got)
	}
	if token, err := writer.PipelineChangeToken(ctx, tenant.Default); err != nil || token != 1 {
		t.Fatalf("token after canceled write = %d, %v, want 1, nil", token, err)
	}

	updated, err := writer.PutPipeline(ctx, def(`{"v":2}`), 1)
	if err != nil {
		t.Fatalf("write after canceled write: %v", err)
	}
	if updated.Version != 2 {
		t.Fatalf("version after recovery = %d, want 2", updated.Version)
	}
	if token, err := writer.PipelineChangeToken(ctx, tenant.Default); err != nil || token != 2 {
		t.Fatalf("token after recovery = %d, %v, want 2, nil", token, err)
	}
}
