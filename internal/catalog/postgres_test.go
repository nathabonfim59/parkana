package catalog_test

import (
	"testing"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/catalog/catalogtest"
)

// TestPostgres runs the conformance suite against catalog.Postgres, each
// catalog in a fresh schema. It needs PARKANA_TEST_POSTGRES_DSN (see
// catalogtest.PostgresDSN) and skips without it.
func TestPostgres(t *testing.T) {
	catalogtest.Run(t, func(t *testing.T) catalog.Catalog {
		c, err := catalog.NewPostgres(t.Context(), catalogtest.PostgresDSN(t))
		if err != nil {
			t.Fatalf("NewPostgres: %v", err)
		}
		return c
	})
}
