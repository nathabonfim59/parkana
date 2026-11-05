package catalog_test

import (
	"testing"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/catalog/catalogtest"
)

func TestMemory(t *testing.T) {
	catalogtest.Run(t, func(*testing.T) catalog.Catalog { return catalog.NewMemory() })
}
