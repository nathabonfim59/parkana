package objstore

import (
	"fmt"
	"path/filepath"
)

// LocalPath returns the absolute path of an existing FS object for local
// readers such as DuckDB. It is an optional capability, not part of Store.
// The key and its target are checked under the store's os.Root before a
// path is returned. Callers must tolerate an object disappearing afterward;
// the path does not pin an object against Delete or external filesystem edits.
func (s *FS) LocalPath(key string) (string, error) {
	if err := s.validate(key); err != nil {
		return "", err
	}
	st, err := s.root.Stat(key)
	if err != nil {
		return "", fmt.Errorf("objstore: local path %s: %w", key, err)
	}
	if st.IsDir() {
		return "", fmt.Errorf("objstore: local path %s: %w", key, ErrNotExist)
	}
	p, err := filepath.Abs(filepath.Join(s.root.Name(), filepath.FromSlash(key)))
	if err != nil {
		return "", fmt.Errorf("objstore: local path %s: %w", key, err)
	}
	return p, nil
}
