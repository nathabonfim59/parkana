package catalog

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// pipelineRecord is one in-memory pipeline row and its change token. The
// token is the tenant change token value at the row's last mutation; the
// tenant token is the maximum over its rows.
type pipelineRecord struct {
	def   PipelineDefinition
	token int64
}

// GetPipeline implements Catalog.
func (m *Memory) GetPipeline(ctx context.Context, t tenant.ID, name string) (PipelineDefinition, error) {
	if err := ctx.Err(); err != nil {
		return PipelineDefinition{}, err
	}
	if err := t.Validate(); err != nil {
		return PipelineDefinition{}, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	// The context may have been canceled while the lock was contended.
	if err := ctx.Err(); err != nil {
		return PipelineDefinition{}, err
	}
	rec, ok := m.pipelines[t][name]
	if !ok || rec.def.Deleted() {
		return PipelineDefinition{}, fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
	}
	return rec.def.Clone(), nil
}

// ListPipelines implements Catalog.
func (m *Memory) ListPipelines(ctx context.Context, t tenant.ID) ([]PipelineDefinition, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := t.Validate(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	// The context may have been canceled while the lock was contended, even
	// when the tenant has no pipelines to copy.
	if err := ctx.Err(); err != nil {
		m.mu.RUnlock()
		return nil, err
	}
	out := make([]PipelineDefinition, 0, len(m.pipelines[t]))
	for _, rec := range m.pipelines[t] {
		// Copying can outlive the entry check for a large tenant.
		if err := ctx.Err(); err != nil {
			m.mu.RUnlock()
			return nil, err
		}
		if !rec.def.Deleted() {
			out = append(out, rec.def.Clone())
		}
	}
	m.mu.RUnlock()
	slices.SortFunc(out, func(a, b PipelineDefinition) int { return cmp.Compare(a.Name, b.Name) })
	return out, nil
}

// PutPipeline implements Catalog.
func (m *Memory) PutPipeline(ctx context.Context, def PipelineDefinition, expectedVersion int64) (PipelineDefinition, error) {
	if err := ctx.Err(); err != nil {
		return PipelineDefinition{}, err
	}
	if err := def.validate(); err != nil {
		return PipelineDefinition{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// The context may have been canceled while the lock was contended.
	if err := ctx.Err(); err != nil {
		return PipelineDefinition{}, err
	}

	rows := m.pipelines[def.Tenant]
	rec, ok := rows[def.Name]
	now := pipelineMicros(m.now())
	out := PipelineDefinition{
		Tenant:    def.Tenant,
		Name:      def.Name,
		Stage:     def.Stage,
		Selector:  def.Selector,
		Spec:      clonePipelineSpec(def.Spec),
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	switch {
	case !ok:
		if expectedVersion != 0 {
			return PipelineDefinition{}, pipelineNotFound(def.Name)
		}
	case rec.def.Deleted():
		// Create expects version zero even over a tombstone; the new version
		// continues above it so recreation never resets versioning. A nonzero
		// expected version names no live row.
		if expectedVersion != 0 {
			return PipelineDefinition{}, pipelineNotFound(def.Name)
		}
		if rec.def.Version == maxInt64 {
			return PipelineDefinition{}, errVersionExhausted
		}
		out.Version = rec.def.Version + 1
	default:
		if expectedVersion != rec.def.Version {
			return PipelineDefinition{}, versionConflict(def.Name, expectedVersion, rec.def.Version)
		}
		if rec.def.Version == maxInt64 {
			return PipelineDefinition{}, errVersionExhausted
		}
		out.Version = rec.def.Version + 1
		out.CreatedAt = rec.def.CreatedAt
	}

	token := m.maxPipelineToken(def.Tenant)
	if token == maxInt64 {
		return PipelineDefinition{}, errVersionExhausted
	}
	if rows == nil {
		rows = make(map[string]pipelineRecord)
		m.pipelines[def.Tenant] = rows
	}
	rows[def.Name] = pipelineRecord{def: out, token: token + 1}
	return out.Clone(), nil
}

// DeletePipeline implements Catalog.
func (m *Memory) DeletePipeline(ctx context.Context, t tenant.ID, name string, expectedVersion int64) (PipelineDefinition, error) {
	if err := ctx.Err(); err != nil {
		return PipelineDefinition{}, err
	}
	if err := t.Validate(); err != nil {
		return PipelineDefinition{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// The context may have been canceled while the lock was contended.
	if err := ctx.Err(); err != nil {
		return PipelineDefinition{}, err
	}

	rows := m.pipelines[t]
	rec, ok := rows[name]
	if !ok || rec.def.Deleted() {
		return PipelineDefinition{}, fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
	}
	if expectedVersion != rec.def.Version {
		return PipelineDefinition{}, versionConflict(name, expectedVersion, rec.def.Version)
	}
	if rec.def.Version == maxInt64 {
		return PipelineDefinition{}, errVersionExhausted
	}
	token := m.maxPipelineToken(t)
	if token == maxInt64 {
		return PipelineDefinition{}, errVersionExhausted
	}
	now := pipelineMicros(m.now())
	rec.def.Version++
	rec.def.UpdatedAt = now
	rec.def.DeletedAt = now
	rows[name] = pipelineRecord{def: rec.def, token: token + 1}
	return rec.def.Clone(), nil
}

// PipelineChangeToken implements Catalog.
func (m *Memory) PipelineChangeToken(ctx context.Context, t tenant.ID) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := t.Validate(); err != nil {
		return 0, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	// The context may have been canceled while the lock was contended.
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return m.maxPipelineToken(t), nil
}

// maxPipelineToken returns the tenant's current change token. Tombstones are
// never purged, so the maximum over the tenant's rows is monotonic. The
// caller must hold m.mu (read or write).
func (m *Memory) maxPipelineToken(t tenant.ID) int64 {
	var token int64
	for _, rec := range m.pipelines[t] {
		token = max(token, rec.token)
	}
	return token
}

func versionConflict(name string, expected, actual int64) error {
	return fmt.Errorf("%w: %q expected version %d, live version is %d", ErrVersionConflict, name, expected, actual)
}

func pipelineNotFound(name string) error {
	return fmt.Errorf("%w: pipeline %q", ErrNotFound, name)
}
