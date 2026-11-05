package catalog

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/nathabonfim59/parkana/internal/tenant"
)

// Memory is an in-process Catalog. Its contents are lost on exit.
type Memory struct {
	mu        sync.RWMutex
	files     map[tenant.ID]map[string]File
	pipelines map[tenant.ID]map[string]pipelineRecord
	now       func() time.Time
}

var _ Catalog = (*Memory)(nil)

// NewMemory returns an empty in-memory catalog.
func NewMemory() *Memory {
	return &Memory{
		files:     make(map[tenant.ID]map[string]File),
		pipelines: make(map[tenant.ID]map[string]pipelineRecord),
		now:       time.Now,
	}
}

func (m *Memory) AddFiles(ctx context.Context, files ...File) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.checkNew(files); err != nil {
		return err
	}
	m.insert(files)
	return nil
}

func (m *Memory) ReplaceFiles(ctx context.Context, t tenant.ID, remove []string, add []File) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range remove {
		if f, ok := m.files[t][k]; !ok || f.Deleted() {
			return fmt.Errorf("%w: %s", ErrNotFound, k)
		}
	}
	if err := m.checkNew(add); err != nil {
		return err
	}
	now := m.now()
	for _, k := range remove {
		f := m.files[t][k]
		f.DeletedAt = now
		m.files[t][k] = f
	}
	m.insert(add)
	return nil
}

func (m *Memory) checkNew(files []File) error {
	seen := make(map[string]bool, len(files))
	for _, f := range files {
		if err := f.Tenant.Validate(); err != nil {
			return err
		}
		id := string(f.Tenant) + "\x00" + f.Key
		if _, ok := m.files[f.Tenant][f.Key]; ok || seen[id] {
			return fmt.Errorf("%w: %s", ErrExists, f.Key)
		}
		seen[id] = true
	}
	return nil
}

func (m *Memory) insert(files []File) {
	now := m.now()
	for _, f := range files {
		f = f.Clone()
		if f.CreatedAt.IsZero() {
			f.CreatedAt = now
		}
		if m.files[f.Tenant] == nil {
			m.files[f.Tenant] = make(map[string]File)
		}
		m.files[f.Tenant][f.Key] = f
	}
}

func (m *Memory) ListFiles(ctx context.Context, flt Filter) iter.Seq2[File, error] {
	return func(yield func(File, error) bool) {
		m.mu.RLock()
		var out []File
		for _, f := range m.files[flt.Tenant] {
			if flt.Matches(f) {
				out = append(out, f.Clone())
			}
		}
		m.mu.RUnlock()
		SortFiles(out)
		for _, f := range out {
			if err := ctx.Err(); err != nil {
				yield(File{}, err)
				return
			}
			if !yield(f, nil) {
				return
			}
		}
	}
}

func (m *Memory) Streams(ctx context.Context, t tenant.ID) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	set := make(map[string]struct{})
	for _, f := range m.files[t] {
		if !f.Deleted() {
			set[f.Stream] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(set)), nil
}

func (m *Memory) SetSidecar(ctx context.Context, t tenant.ID, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[t][key]
	if !ok || f.Deleted() {
		return fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	f.HasSidecar = true
	m.files[t][key] = f
	return nil
}

func (m *Memory) PurgeDeleted(ctx context.Context, t tenant.ID, cutoff time.Time) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for k, f := range m.files[t] {
		if f.Deleted() && f.DeletedAt.Before(cutoff) {
			keys = append(keys, k)
			delete(m.files[t], k)
		}
	}
	slices.Sort(keys)
	return keys, nil
}

func (m *Memory) Close() error { return nil }
