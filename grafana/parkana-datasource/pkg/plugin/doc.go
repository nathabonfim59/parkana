// Package plugin implements the backend of the Parkana Grafana data source.
//
// # Query model
//
// Each query's JSON carries rawSQL (the DuckDB SELECT), format ("table" or
// "time_series") and an optional limit. The backend URL and tenant come only
// from the Grafana data source settings (JSONData url/tenant, with the
// standard URL field as a fallback); no query can redirect the request or
// choose another tenant.
//
// # Time range and macros
//
// The Grafana time range is sent as required RFC3339Nano UTC from/to bounds.
// $__timeFrom, $__timeTo and $__interval are replaced with DuckDB literals
// outside SQL strings, quoted identifiers, dollar-quoted strings and
// comments. The parenthesized form ($__timeFrom()) is accepted as well.
//
// # Response and numeric precision
//
// The /api/v1/sql response columns are typed (time, number, string, boolean,
// json). Cells are decoded as jsontext.Value, so numbers keep their exact
// tokens and never pass through an intermediate float64. Number columns become
// numeric frame fields only when every value is exactly representable for
// Grafana: integers must fit the safe float64 range (2^53-1) and FLOAT/DOUBLE/
// REAL values must be finite. DECIMAL and wider integer values become string
// fields with the original token text. Queries that need approximate chartable
// values should CAST the column AS DOUBLE explicitly.
//
// An omitted format is inferred: a column named time plus at least one numeric
// value column and no other column kinds becomes time_series; everything else
// becomes a table. Explicit table stays a table. A table frame preserves the
// endpoint's column order and duplicate names. time_series requires a column
// named "time" of type time, at least one numeric value column (no precision
// fallback), unique timestamps and returns the time field first with rows
// sorted by time ascending, matching the data plane time-series wide contract.
//
// # Health
//
// CheckHealth sends a bounded SELECT 1 through the same endpoint, so it
// proves connectivity, tenant resolution and the SQL request path.
package plugin
