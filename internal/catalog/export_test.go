package catalog

import "database/sql"

// This file exports internals to the external tests of package catalog
// (catalog_test), which can use catalogtest while package catalog's own
// tests cannot (catalogtest imports catalog).

// DB returns the catalog's connection pool, for tests that set rows behind
// the catalog's back.
func (p *Postgres) DB() *sql.DB { return p.db }

// ErrVersionExhausted is errVersionExhausted.
var ErrVersionExhausted = errVersionExhausted
