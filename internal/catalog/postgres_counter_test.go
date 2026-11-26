package catalog_test

import (
	"database/sql"
	"errors"
	"math"
	"testing"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/catalog/catalogtest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// TestPostgresPipelineCounterExhaustion is TestPipelineCounterExhaustion's
// sqlite case on Postgres: with a pipeline's version or the tenant's change
// token forced to MaxInt64 by SQL, an update or delete that would overflow
// fails with the exhaustion error and changes nothing. Needs
// PARKANA_TEST_POSTGRES_DSN.
func TestPostgresPipelineCounterExhaustion(t *testing.T) {
	c := openPostgres(t, catalogtest.PostgresDSN(t))
	ctx := boundedContext(t)
	const maxInt64 = int64(math.MaxInt64)
	input := pipelineDef(tenant.Default, "p", catalog.PipelineStageIngest, "api", `{}`)
	if _, err := c.PutPipeline(ctx, input, 0); err != nil {
		t.Fatalf("create: %v", err)
	}
	force := func(t *testing.T, version, token int64) {
		t.Helper()
		if _, err := c.DB().ExecContext(ctx,
			`UPDATE pipelines SET "version" = $1, "change_token" = $2 WHERE "tenant" = $3 AND "name" = $4`,
			version, token, string(tenant.Default), "p"); err != nil {
			t.Fatalf("force counters: %v", err)
		}
	}
	record := func(t *testing.T) (int64, int64, bool) {
		t.Helper()
		var version, token int64
		var deleted sql.NullInt64
		if err := c.DB().QueryRowContext(ctx,
			`SELECT "version", "change_token", "deleted_at" FROM pipelines WHERE "tenant" = $1 AND "name" = $2`,
			string(tenant.Default), "p").Scan(&version, &token, &deleted); err != nil {
			t.Fatalf("read counters: %v", err)
		}
		return version, token, deleted.Valid
	}

	for _, tc := range []struct {
		name           string
		version, token int64
		call           func() error
	}{
		{"version overflow update", maxInt64, 1, func() error { _, err := c.PutPipeline(ctx, input, maxInt64); return err }},
		{"token overflow update", 1, maxInt64, func() error { _, err := c.PutPipeline(ctx, input, 1); return err }},
		{"version overflow delete", maxInt64, 1, func() error { _, err := c.DeletePipeline(ctx, tenant.Default, "p", maxInt64); return err }},
		{"token overflow delete", 1, maxInt64, func() error { _, err := c.DeletePipeline(ctx, tenant.Default, "p", 1); return err }},
	} {
		force(t, tc.version, tc.token)
		if err := tc.call(); !errors.Is(err, catalog.ErrVersionExhausted) {
			t.Fatalf("%s = %v, want the exhaustion error", tc.name, err)
		}
		if version, token, deleted := record(t); version != tc.version || token != tc.token || deleted {
			t.Fatalf("after %s: version %d token %d deleted %v, want %d %d false", tc.name, version, token, deleted, tc.version, tc.token)
		}
	}
}
