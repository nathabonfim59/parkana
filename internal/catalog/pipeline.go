package catalog

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/nathabonfim59/parkana/internal/logs"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// ErrVersionConflict is returned when a pipeline write's expected version does
// not match the live version, including a create for a name that is live.
var ErrVersionConflict = errors.New("catalog: pipeline version conflict")

// ErrInvalidPipeline is returned when a pipeline definition fails the
// catalog's structural validation: name, stage or selector.
var ErrInvalidPipeline = errors.New("catalog: invalid pipeline definition")

// errVersionExhausted is returned before a version or change-token increment
// would overflow int64. It is not exported because callers cannot recover.
var errVersionExhausted = errors.New("catalog: pipeline version or change token exhausted")

// maxInt64 is the largest value an int64 version or change token can hold.
const maxInt64 = int64(math.MaxInt64)

// PipelineStage says when a pipeline runs.
type PipelineStage string

const (
	// PipelineStageIngest runs the pipeline before the WAL.
	PipelineStageIngest PipelineStage = "ingest"
	// PipelineStageRead runs the pipeline on query results.
	PipelineStageRead PipelineStage = "read"
)

// Validate reports whether s is a known stage.
func (s PipelineStage) Validate() error {
	switch s {
	case PipelineStageIngest, PipelineStageRead:
		return nil
	default:
		return fmt.Errorf("%w: stage %q must be %q or %q", ErrInvalidPipeline, s, PipelineStageIngest, PipelineStageRead)
	}
}

// PipelineDefinition is one stored pipeline definition.
//
// Spec is opaque to the catalog: callers compile and validate it, the catalog
// stores and returns the bytes verbatim. Version and the timestamps are owned
// by the catalog; callers' values for them are ignored by PutPipeline.
type PipelineDefinition struct {
	Tenant tenant.ID
	// Name identifies the pipeline within its tenant. It is 1-128 bytes from
	// [A-Za-z0-9._-].
	Name  string
	Stage PipelineStage
	// Selector is the exact sanitized dataset name the pipeline applies to,
	// or "*" for every dataset. The catalog stores it verbatim.
	Selector string
	// Spec is the caller-validated JSON pipeline specification, stored and
	// returned byte for byte.
	Spec []byte
	// Version is the optimistic-concurrency version: 1 for a newly created
	// name and one higher after every successful replace or delete. A
	// deleted name keeps its version, and recreation continues from it.
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
	// DeletedAt is set for a tombstone. Tombstones are kept so versions never
	// reset; GetPipeline and ListPipelines omit them.
	DeletedAt time.Time
}

// Deleted reports whether the definition was logically deleted.
func (d PipelineDefinition) Deleted() bool { return !d.DeletedAt.IsZero() }

// Clone returns a deep copy of d.
func (d PipelineDefinition) Clone() PipelineDefinition {
	d.Spec = clonePipelineSpec(d.Spec)
	return d
}

// clonePipelineSpec copies spec and normalises nil to an empty slice, so the
// value stored by either backend is never NULL.
func clonePipelineSpec(spec []byte) []byte {
	out := make([]byte, len(spec))
	copy(out, spec)
	return out
}

// validate checks the parts the catalog owns. Spec validation belongs to the
// pipeline compiler.
func (d PipelineDefinition) validate() error {
	if err := d.Tenant.Validate(); err != nil {
		return err
	}
	if err := validatePipelineName(d.Name); err != nil {
		return err
	}
	if err := d.Stage.Validate(); err != nil {
		return err
	}
	if err := validatePipelineSelector(d.Selector); err != nil {
		return err
	}
	return nil
}

// validatePipelineSelector accepts exactly the "*" wildcard or an already
// sanitized dataset name (logs.SanitizeStream is idempotent on it).
func validatePipelineSelector(selector string) error {
	if selector == "" {
		return fmt.Errorf("%w: selector must not be empty", ErrInvalidPipeline)
	}
	if len(selector) > 128 {
		return fmt.Errorf("%w: selector must be at most 128 bytes", ErrInvalidPipeline)
	}
	if selector != "*" && logs.SanitizeStream(selector) != selector {
		return fmt.Errorf("%w: selector %q must be \"*\" or an already sanitized dataset name", ErrInvalidPipeline, selector)
	}
	return nil
}

const maxPipelineNameLen = 128

func validatePipelineName(name string) error {
	if len(name) == 0 || len(name) > maxPipelineNameLen {
		return fmt.Errorf("%w: name must be 1-%d bytes", ErrInvalidPipeline, maxPipelineNameLen)
	}
	if name == "." || name == ".." {
		return fmt.Errorf("%w: name %q is reserved", ErrInvalidPipeline, name)
	}
	for _, c := range []byte(name) {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			return fmt.Errorf("%w: name %q contains %q", ErrInvalidPipeline, name, c)
		}
	}
	return nil
}

// pipelineMicros normalises a timestamp to the catalog's representation:
// UTC, truncated toward negative infinity to whole microseconds. Memory and
// SQLite store identical values.
func pipelineMicros(t time.Time) time.Time {
	return time.UnixMicro(t.UnixMicro()).UTC()
}
