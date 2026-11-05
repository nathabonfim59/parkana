package objstore

import (
	"fmt"
	"strings"
	"time"
	"uuid"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// File extensions for data files and their search sidecars.
const (
	ExtData    = ".parquet"
	ExtSidecar = ".idx"
)

// LogPrefix is the key prefix of a tenant's stream:
// {tenant}/logs/{stream}/
func LogPrefix(t tenant.ID, stream string) string {
	return fmt.Sprintf("%s/logs/%s/", t, stream)
}

// DataKey returns the object key of a new log file:
// {tenant}/logs/{stream}/{yyyy}/{mm}/{dd}/{hh}/{uuidv7}.parquet
// The hour partition comes from minTS (UTC); UUIDv7 keeps keys time-ordered.
// stream must already be sanitized (logs.SanitizeStream).
func DataKey(t tenant.ID, stream string, minTS time.Time, id uuid.UUID) string {
	return LogPrefix(t, stream) + minTS.UTC().Format("2006/01/02/15/") + id.String() + ExtData
}

// NewDataKey is DataKey with a fresh UUIDv7.
func NewDataKey(t tenant.ID, stream string, minTS time.Time) string {
	return DataKey(t, stream, minTS, uuid.NewV7())
}

// SidecarKey returns the search-sidecar key for a data key.
func SidecarKey(dataKey string) string {
	return strings.TrimSuffix(dataKey, ExtData) + ExtSidecar
}
