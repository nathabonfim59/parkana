package config

import (
	"flag"
	"fmt"

	"github.com/nathabonfim59/parkana/internal/index"
)

// Index holds the search-sidecar build tuning shared by the roles that build
// sidecars (PLAN.md decision 12, internal/index): the ingester's uploads and
// the compactor's merge output and backfill pass. Decoded sidecars carry
// their own parameters, so these settings never affect readers. Defaults
// come from index.DefaultParams, so tuning the index package tunes the flags.
type Index struct {
	N        int     // gram length in bytes, 1..8
	FPRate   float64 // target bloom false-positive rate per gram lookup, in (0, 1)
	MaxGrams int     // distinct-gram cap per file; larger files get no sidecar
}

func (c *Index) register(fs *flag.FlagSet) {
	*c = Index{
		N:        index.DefaultParams.N,
		FPRate:   index.DefaultParams.FPRate,
		MaxGrams: index.DefaultParams.MaxGrams,
	}
	fs.IntVar(&c.N, "index.ngram", c.N, "gram length in bytes for search sidecars (1-8)")
	fs.Float64Var(&c.FPRate, "index.fp-rate", c.FPRate, "target bloom false-positive rate per gram lookup, in (0, 1)")
	fs.IntVar(&c.MaxGrams, "index.max-grams", c.MaxGrams, "distinct-gram cap per file; larger files get no sidecar")
}

func (c Index) validate() []error {
	if err := c.Params().Validate(); err != nil {
		return []error{fmt.Errorf("-index.ngram, -index.fp-rate and -index.max-grams: %w", err)}
	}
	return nil
}

// Params returns the section as index.Params, ready for index.NewBuilder.
func (c Index) Params() index.Params {
	return index.Params{N: c.N, FPRate: c.FPRate, MaxGrams: c.MaxGrams}
}
