package catalog

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// GetPipeline implements Catalog.
func (p *Postgres) GetPipeline(ctx context.Context, t tenant.ID, name string) (PipelineDefinition, error) {
	def, err := getPipeline(ctx, p.db, t, name, postgresDialect)
	return def, ctxError(ctx, err)
}

// ListPipelines implements Catalog.
func (p *Postgres) ListPipelines(ctx context.Context, t tenant.ID) ([]PipelineDefinition, error) {
	defs, err := listPipelines(ctx, p.db, t, postgresDialect)
	return defs, ctxError(ctx, err)
}

// PipelineChangeToken implements Catalog.
func (p *Postgres) PipelineChangeToken(ctx context.Context, t tenant.ID) (int64, error) {
	token, err := pipelineChangeToken(ctx, p.db, t, postgresDialect)
	return token, ctxError(ctx, err)
}

// PutPipeline implements Catalog.
//
// The transaction first takes the tenant's pipeline advisory lock, so
// writers in every process serialize per tenant: each sees the previous
// writer's committed row and token, concurrent creates have exactly one
// winner, and the change token strictly increases.
func (p *Postgres) PutPipeline(ctx context.Context, def PipelineDefinition, expectedVersion int64) (PipelineDefinition, error) {
	if err := ctx.Err(); err != nil {
		return PipelineDefinition{}, err
	}
	if err := def.validate(); err != nil {
		return PipelineDefinition{}, err
	}
	var out PipelineDefinition
	err := p.inTx(ctx, "put pipeline", func(tx *sql.Tx) error {
		if err := lockPipelines(ctx, tx, def.Tenant); err != nil {
			return err
		}
		existing, found, maxToken, err := readPipelineState(ctx, tx, def.Tenant, def.Name, postgresDialect)
		if err != nil {
			return err
		}
		stored, token, err := pipelinePut(def, expectedVersion, existing, found, maxToken, p.now())
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO pipelines (`+pipelineColumns+`, "change_token")
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, $9)
			ON CONFLICT ("tenant", "name") DO UPDATE SET
				"stage" = excluded."stage",
				"selector" = excluded."selector",
				"spec" = excluded."spec",
				"version" = excluded."version",
				"change_token" = excluded."change_token",
				"created_at" = excluded."created_at",
				"updated_at" = excluded."updated_at",
				"deleted_at" = NULL`,
			string(stored.Tenant), stored.Name, string(stored.Stage), stored.Selector, stored.Spec,
			stored.Version, stored.CreatedAt.UnixMicro(), stored.UpdatedAt.UnixMicro(), token,
		); err != nil {
			return fmt.Errorf("catalog: postgres: write pipeline %q: %w", def.Name, err)
		}
		out = stored
		return nil
	})
	if err != nil {
		return PipelineDefinition{}, err
	}
	return out, nil
}

// DeletePipeline implements Catalog. It serializes with PutPipeline through
// the tenant's pipeline advisory lock.
func (p *Postgres) DeletePipeline(ctx context.Context, t tenant.ID, name string, expectedVersion int64) (PipelineDefinition, error) {
	if err := ctx.Err(); err != nil {
		return PipelineDefinition{}, err
	}
	if err := t.Validate(); err != nil {
		return PipelineDefinition{}, err
	}
	var out PipelineDefinition
	err := p.inTx(ctx, "delete pipeline", func(tx *sql.Tx) error {
		if err := lockPipelines(ctx, tx, t); err != nil {
			return err
		}
		existing, found, maxToken, err := readPipelineState(ctx, tx, t, name, postgresDialect)
		if err != nil {
			return err
		}
		tomb, token, err := pipelineDelete(name, expectedVersion, existing, found, maxToken, p.now())
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE pipelines SET "version" = $1, "change_token" = $2, "updated_at" = $3, "deleted_at" = $4
			WHERE "tenant" = $5 AND "name" = $6 AND "deleted_at" IS NULL`,
			tomb.Version, token, tomb.UpdatedAt.UnixMicro(), tomb.DeletedAt.UnixMicro(), string(t), name)
		if err != nil {
			return fmt.Errorf("catalog: postgres: delete pipeline %q: %w", name, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("catalog: postgres: delete pipeline %q: %w", name, err)
		}
		if n != 1 {
			return pipelineNotFound(name)
		}
		out = tomb
		return nil
	})
	if err != nil {
		return PipelineDefinition{}, err
	}
	return out, nil
}

// lockPipelines takes the tenant's pipeline write lock for the rest of tx.
func lockPipelines(ctx context.Context, tx *sql.Tx, t tenant.ID) error {
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		postgresPipelinesLock+string(t)); err != nil {
		return fmt.Errorf("catalog: postgres: lock pipelines of %q: %w", t, err)
	}
	return nil
}
