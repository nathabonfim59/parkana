// Package schema defines the column layout of Parkana's Parquet files.
//
// Every log file has the base columns below, then the stream's promoted label
// columns (low-cardinality labels configured per stream, as nullable strings),
// then any typed columns added by ingest pipelines. Schema evolution is
// additive only: columns are appended, never removed, renamed or retyped.
package schema

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

// Base column names. Names starting with '_' are reserved for Parkana.
const (
	// ColTS is the entry timestamp: Parquet TIMESTAMP(MICROS, UTC), the sort key
	// of every file. DuckDB reads it as TIMESTAMPTZ; queriers run with
	// TimeZone=UTC.
	ColTS = "_ts"
	// ColLine is the raw log line.
	ColLine = "line"
	// ColLabels holds the stream labels that are not promoted to columns.
	ColLabels = "labels"
	// ColMeta holds Loki structured metadata and OTLP attributes.
	ColMeta = "meta"
)

// TSType is the Arrow type of ColTS.
var TSType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}

// LabelMapType is the Arrow type of ColLabels and ColMeta.
var LabelMapType = arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)

// TS converts t to the stored timestamp value (Unix microseconds).
func TS(t time.Time) int64 { return t.UnixMicro() }

// Time converts a stored timestamp value back to a UTC time.
func Time(us int64) time.Time { return time.UnixMicro(us).UTC() }

// IsReserved reports whether name collides with a base column or the reserved
// '_' prefix, so it can't be used for a promoted label or pipeline field.
// Base names compare case-insensitively: DuckDB resolves column names without
// case, so a promoted "LINE" would shadow or be renamed against "line".
func IsReserved(name string) bool {
	return strings.EqualFold(name, ColLine) || strings.EqualFold(name, ColLabels) ||
		strings.EqualFold(name, ColMeta) || len(name) > 0 && name[0] == '_'
}

// Collide reports whether two column names would be the same column to a
// case-insensitive reader such as DuckDB while being different label names.
func Collide(a, b string) bool { return a != b && strings.EqualFold(a, b) }

// Base returns the fields every log file starts with.
func Base() []arrow.Field {
	return []arrow.Field{
		{Name: ColTS, Type: TSType},
		{Name: ColLine, Type: arrow.BinaryTypes.String},
		{Name: ColLabels, Type: LabelMapType},
		{Name: ColMeta, Type: LabelMapType},
	}
}

// ForStream returns the Arrow schema for a stream with the given promoted
// labels, in order after the base columns.
func ForStream(promoted []string) (*arrow.Schema, error) {
	fields := Base()
	for i, name := range promoted {
		if name == "" || IsReserved(name) {
			return nil, fmt.Errorf("schema: promoted label %q is reserved or empty", name)
		}
		if slices.Contains(promoted[:i], name) {
			return nil, fmt.Errorf("schema: promoted label %q listed twice", name)
		}
		if j := slices.IndexFunc(promoted[:i], func(p string) bool { return Collide(p, name) }); j >= 0 {
			return nil, fmt.Errorf("schema: promoted labels %q and %q differ only in case", promoted[j], name)
		}
		fields = append(fields, arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true})
	}
	return arrow.NewSchema(fields, nil), nil
}
