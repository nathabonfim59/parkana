package catalog

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// GetPipeline implements Catalog.
func (s *SQLite) GetPipeline(ctx context.Context, t tenant.ID, name string) (PipelineDefinition, error) {
	return getPipeline(ctx, s.read, t, name, sqliteDialect)
}

// ListPipelines implements Catalog.
func (s *SQLite) ListPipelines(ctx context.Context, t tenant.ID) ([]PipelineDefinition, error) {
	return listPipelines(ctx, s.read, t, sqliteDialect)
}

// PutPipeline implements Catalog.
//
// It runs in an immediate write transaction: the write lock is taken before
// the reads, so a writer on a separately opened handle waits (busy_timeout)
// and then sees the previous writer's committed row instead of failing a
// deferred read-to-write upgrade with SQLITE_BUSY_SNAPSHOT.
func (s *SQLite) PutPipeline(ctx context.Context, def PipelineDefinition, expectedVersion int64) (PipelineDefinition, error) {
	if err := ctx.Err(); err != nil {
		return PipelineDefinition{}, err
	}
	if err := def.validate(); err != nil {
		return PipelineDefinition{}, err
	}
	var out PipelineDefinition
	err := s.withWriteTx(ctx, func(conn *sql.Conn) error {
		existing, found, maxToken, err := readPipelineState(ctx, conn, def.Tenant, def.Name, sqliteDialect)
		if err != nil {
			return err
		}
		var token int64
		out, token, err = pipelinePut(def, expectedVersion, existing, found, maxToken, s.now())
		if err != nil {
			return err
		}

		if _, err := conn.ExecContext(ctx,
			`INSERT INTO pipelines (`+pipelineColumns+`, "change_token")
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
			ON CONFLICT("tenant", "name") DO UPDATE SET
				"stage" = excluded."stage",
				"selector" = excluded."selector",
				"spec" = excluded."spec",
				"version" = excluded."version",
				"change_token" = excluded."change_token",
				"created_at" = excluded."created_at",
				"updated_at" = excluded."updated_at",
				"deleted_at" = NULL`,
			string(out.Tenant), out.Name, string(out.Stage), out.Selector, out.Spec,
			out.Version, out.CreatedAt.UnixMicro(), out.UpdatedAt.UnixMicro(), token,
		); err != nil {
			return fmt.Errorf("catalog: sqlite: write pipeline %q: %w", def.Name, err)
		}
		return nil
	})
	if err != nil {
		return PipelineDefinition{}, err
	}
	return out, nil
}

// DeletePipeline implements Catalog.
func (s *SQLite) DeletePipeline(ctx context.Context, t tenant.ID, name string, expectedVersion int64) (PipelineDefinition, error) {
	if err := ctx.Err(); err != nil {
		return PipelineDefinition{}, err
	}
	if err := t.Validate(); err != nil {
		return PipelineDefinition{}, err
	}
	var out PipelineDefinition
	err := s.withWriteTx(ctx, func(conn *sql.Conn) error {
		existing, found, maxToken, err := readPipelineState(ctx, conn, t, name, sqliteDialect)
		if err != nil {
			return err
		}
		tomb, token, err := pipelineDelete(name, expectedVersion, existing, found, maxToken, s.now())
		if err != nil {
			return err
		}

		res, err := conn.ExecContext(ctx,
			`UPDATE pipelines SET "version" = ?, "change_token" = ?, "updated_at" = ?, "deleted_at" = ?
			WHERE "tenant" = ? AND "name" = ? AND "deleted_at" IS NULL`,
			tomb.Version, token, tomb.UpdatedAt.UnixMicro(), tomb.DeletedAt.UnixMicro(), string(t), name)
		if err != nil {
			return fmt.Errorf("catalog: sqlite: delete pipeline %q: %w", name, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("catalog: sqlite: delete pipeline %q: %w", name, err)
		}
		if n != 1 {
			return fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
		}
		out = tomb
		return nil
	})
	if err != nil {
		return PipelineDefinition{}, err
	}
	return out, nil
}

// PipelineChangeToken implements Catalog.
func (s *SQLite) PipelineChangeToken(ctx context.Context, t tenant.ID) (int64, error) {
	return pipelineChangeToken(ctx, s.read, t, sqliteDialect)
}

// withWriteTx runs fn inside an immediate write transaction on the single
// write connection. BEGIN IMMEDIATE acquires SQLite's write lock before any
// read, so independently opened catalogs serialize instead of racing a
// deferred transaction upgrade. Any error rolls the transaction back.
func (s *SQLite) withWriteTx(ctx context.Context, fn func(*sql.Conn) error) error {
	conn, err := s.write.Conn(ctx)
	if err != nil {
		return fmt.Errorf("catalog: sqlite: write connection: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return fmt.Errorf("catalog: sqlite: begin write: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`)
		}
	}()
	if err := fn(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return fmt.Errorf("catalog: sqlite: commit write: %w", err)
	}
	rollback = false
	return nil
}
