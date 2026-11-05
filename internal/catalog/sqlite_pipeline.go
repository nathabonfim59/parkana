package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// pipelineColumns is the pipelines column list in scanPipeline order.
const pipelineColumns = `"tenant", "name", "stage", "selector", "spec", "version", "created_at", "updated_at", "deleted_at"`

// GetPipeline implements Catalog.
func (s *SQLite) GetPipeline(ctx context.Context, t tenant.ID, name string) (PipelineDefinition, error) {
	if err := ctx.Err(); err != nil {
		return PipelineDefinition{}, err
	}
	if err := t.Validate(); err != nil {
		return PipelineDefinition{}, err
	}
	def, err := scanPipeline(s.read.QueryRowContext(ctx,
		`SELECT `+pipelineColumns+` FROM pipelines WHERE "tenant" = ? AND "name" = ? AND "deleted_at" IS NULL`,
		string(t), name))
	if errors.Is(err, sql.ErrNoRows) {
		return PipelineDefinition{}, fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
	}
	if err != nil {
		return PipelineDefinition{}, fmt.Errorf("catalog: sqlite: get pipeline %q: %w", name, err)
	}
	return def, nil
}

// ListPipelines implements Catalog.
func (s *SQLite) ListPipelines(ctx context.Context, t tenant.ID) ([]PipelineDefinition, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := t.Validate(); err != nil {
		return nil, err
	}
	rows, err := s.read.QueryContext(ctx,
		`SELECT `+pipelineColumns+` FROM pipelines WHERE "tenant" = ? AND "deleted_at" IS NULL ORDER BY "name"`,
		string(t))
	if err != nil {
		return nil, fmt.Errorf("catalog: sqlite: list pipelines: %w", err)
	}
	defer rows.Close()
	var out []PipelineDefinition
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		def, err := scanPipeline(rows)
		if err != nil {
			return nil, fmt.Errorf("catalog: sqlite: list pipelines: %w", err)
		}
		out = append(out, def)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: sqlite: list pipelines: %w", err)
	}
	return out, nil
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
		existing, err := scanPipeline(conn.QueryRowContext(ctx,
			`SELECT `+pipelineColumns+` FROM pipelines WHERE "tenant" = ? AND "name" = ?`,
			string(def.Tenant), def.Name))
		exists := err == nil
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("catalog: sqlite: read pipeline %q: %w", def.Name, err)
		}

		var maxToken int64
		if err := conn.QueryRowContext(ctx,
			`SELECT COALESCE(MAX("change_token"), 0) FROM pipelines WHERE "tenant" = ?`,
			string(def.Tenant)).Scan(&maxToken); err != nil {
			return fmt.Errorf("catalog: sqlite: read pipeline token: %w", err)
		}

		now := pipelineMicros(s.now())
		out = PipelineDefinition{
			Tenant:    def.Tenant,
			Name:      def.Name,
			Stage:     def.Stage,
			Selector:  def.Selector,
			Spec:      clonePipelineSpec(def.Spec),
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}
		switch {
		case !exists:
			if expectedVersion != 0 {
				return pipelineNotFound(def.Name)
			}
		case existing.Deleted():
			// Create expects version zero even over a tombstone; the new
			// version continues above it so recreation never resets it. A
			// nonzero expected version names no live row.
			if expectedVersion != 0 {
				return pipelineNotFound(def.Name)
			}
			if existing.Version == maxInt64 {
				return errVersionExhausted
			}
			out.Version = existing.Version + 1
		default:
			if expectedVersion != existing.Version {
				return versionConflict(def.Name, expectedVersion, existing.Version)
			}
			if existing.Version == maxInt64 {
				return errVersionExhausted
			}
			out.Version = existing.Version + 1
			out.CreatedAt = existing.CreatedAt
		}
		if maxToken == maxInt64 {
			return errVersionExhausted
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
			out.Version, out.CreatedAt.UnixMicro(), out.UpdatedAt.UnixMicro(), maxToken+1,
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
		existing, err := scanPipeline(conn.QueryRowContext(ctx,
			`SELECT `+pipelineColumns+` FROM pipelines WHERE "tenant" = ? AND "name" = ?`,
			string(t), name))
		if errors.Is(err, sql.ErrNoRows) || err == nil && existing.Deleted() {
			return fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
		}
		if err != nil {
			return fmt.Errorf("catalog: sqlite: read pipeline %q: %w", name, err)
		}
		if expectedVersion != existing.Version {
			return versionConflict(name, expectedVersion, existing.Version)
		}
		if existing.Version == maxInt64 {
			return errVersionExhausted
		}

		var maxToken int64
		if err := conn.QueryRowContext(ctx,
			`SELECT COALESCE(MAX("change_token"), 0) FROM pipelines WHERE "tenant" = ?`,
			string(t)).Scan(&maxToken); err != nil {
			return fmt.Errorf("catalog: sqlite: read pipeline token: %w", err)
		}
		if maxToken == maxInt64 {
			return errVersionExhausted
		}

		now := pipelineMicros(s.now())
		res, err := conn.ExecContext(ctx,
			`UPDATE pipelines SET "version" = ?, "change_token" = ?, "updated_at" = ?, "deleted_at" = ?
			WHERE "tenant" = ? AND "name" = ? AND "deleted_at" IS NULL`,
			existing.Version+1, maxToken+1, now.UnixMicro(), now.UnixMicro(), string(t), name)
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
		existing.Version++
		existing.UpdatedAt = now
		existing.DeletedAt = now
		out = existing
		return nil
	})
	if err != nil {
		return PipelineDefinition{}, err
	}
	return out, nil
}

// PipelineChangeToken implements Catalog.
func (s *SQLite) PipelineChangeToken(ctx context.Context, t tenant.ID) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := t.Validate(); err != nil {
		return 0, err
	}
	var token int64
	if err := s.read.QueryRowContext(ctx,
		`SELECT COALESCE(MAX("change_token"), 0) FROM pipelines WHERE "tenant" = ?`,
		string(t)).Scan(&token); err != nil {
		return 0, fmt.Errorf("catalog: sqlite: read pipeline token: %w", err)
	}
	return token, nil
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

// scanPipeline decodes one pipelines row.
func scanPipeline(row rowScanner) (PipelineDefinition, error) {
	var (
		def                  PipelineDefinition
		tenantID, stage      string
		version              int64
		createdAt, updatedAt int64
		deletedAt            sql.NullInt64
	)
	if err := row.Scan(&tenantID, &def.Name, &stage, &def.Selector, &def.Spec,
		&version, &createdAt, &updatedAt, &deletedAt); err != nil {
		return PipelineDefinition{}, err
	}
	def.Tenant = tenant.ID(tenantID)
	def.Stage = PipelineStage(stage)
	def.Version = version
	def.CreatedAt = time.UnixMicro(createdAt).UTC()
	def.UpdatedAt = time.UnixMicro(updatedAt).UTC()
	if def.Spec == nil {
		def.Spec = []byte{}
	}
	if deletedAt.Valid {
		def.DeletedAt = time.UnixMicro(deletedAt.Int64).UTC()
	}
	return def, nil
}
