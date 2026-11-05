package config

import (
	"cmp"
	"flag"
	"fmt"
	"path/filepath"
	"time"
)

// Querier holds the querier's execution settings: the local cache of
// Parquet objects DuckDB reads, the in-memory cache of decoded search
// sidecars, and per-query bounds. Owned by the querier (internal/query);
// LogQL language limits live in LogQL.
type Querier struct {
	CacheDir         string        // local copies of objects; empty: <data.dir>/cache
	CacheSize        ByteSize      // evict least recently used objects above this
	SidecarCacheSize ByteSize      // decoded search sidecars kept in memory (LRU)
	Timeout          time.Duration // per-request deadline
}

func (c *Querier) register(fs *flag.FlagSet) {
	*c = Querier{CacheSize: 10 * GiB, SidecarCacheSize: 64 * MiB, Timeout: 2 * time.Minute}
	fs.StringVar(&c.CacheDir, "querier.cache-dir", "", "directory for cached copies of Parquet objects (empty: <data.dir>/cache)")
	fs.Var(&c.CacheSize, "querier.cache-size", "evict cached objects above this total size")
	fs.Var(&c.SidecarCacheSize, "querier.sidecar-cache-size", "evict cached search sidecars above this decoded size")
	fs.DurationVar(&c.Timeout, "querier.timeout", c.Timeout, "per-request query deadline")
}

// CachePath returns the directory holding cached Parquet objects: CacheDir,
// or <dataDir>/cache when it is empty (see the flag help).
func (c Querier) CachePath(dataDir string) string {
	return cmp.Or(c.CacheDir, filepath.Join(dataDir, "cache"))
}

func (c Querier) validate() []error {
	var errs []error
	if c.CacheSize <= 0 || c.SidecarCacheSize <= 0 || c.Timeout <= 0 {
		errs = append(errs, fmt.Errorf("-querier.cache-size, -querier.sidecar-cache-size and -querier.timeout must be positive"))
	}
	return errs
}
