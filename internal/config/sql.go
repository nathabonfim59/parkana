package config

import (
	"errors"
	"flag"
	"fmt"
	"time"
)

// SQL bounds the querier's SQL endpoint (POST /api/v1/sql): result caps and
// per-request DuckDB resource limits. Owned by internal/query.
type SQL struct {
	MaxRows        int           // most rows one SQL response may return
	DefaultLimit   int           // row limit applied when the request omits limit
	MaxResultBytes ByteSize      // largest encoded SQL response body
	Timeout        time.Duration // per-request SQL deadline
	MemoryLimit    ByteSize      // DuckDB memory budget per SQL request
	Threads        int           // DuckDB worker threads per SQL request
	MaxConcurrent  int           // most SQL requests executing at once
}

func (c *SQL) register(fs *flag.FlagSet) {
	*c = SQL{
		MaxRows:        10000,
		DefaultLimit:   1000,
		MaxResultBytes: 8 * MiB,
		Timeout:        30 * time.Second,
		MemoryLimit:    GiB,
		Threads:        1,
		MaxConcurrent:  2,
	}
	fs.IntVar(&c.MaxRows, "sql.max-rows", c.MaxRows, "most rows one /api/v1/sql response may return")
	fs.IntVar(&c.DefaultLimit, "sql.default-limit", c.DefaultLimit, "row limit applied when an /api/v1/sql request omits limit")
	fs.Var(&c.MaxResultBytes, "sql.max-result-bytes", "largest encoded /api/v1/sql response body")
	fs.DurationVar(&c.Timeout, "sql.timeout", c.Timeout, "per-request /api/v1/sql deadline")
	fs.Var(&c.MemoryLimit, "sql.memory-limit", "DuckDB memory budget per /api/v1/sql request")
	fs.IntVar(&c.Threads, "sql.threads", c.Threads, "DuckDB worker threads per /api/v1/sql request")
	fs.IntVar(&c.MaxConcurrent, "sql.max-concurrent", c.MaxConcurrent, "most /api/v1/sql requests executing at once")
}

func (c SQL) validate() []error {
	var errs []error
	if c.MaxRows <= 0 {
		errs = append(errs, errors.New("-sql.max-rows must be positive"))
	}
	if c.DefaultLimit <= 0 {
		errs = append(errs, errors.New("-sql.default-limit must be positive"))
	}
	if c.MaxResultBytes <= 0 {
		errs = append(errs, errors.New("-sql.max-result-bytes must be positive"))
	}
	if c.Timeout <= 0 {
		errs = append(errs, errors.New("-sql.timeout must be positive"))
	}
	if c.MemoryLimit <= 0 {
		errs = append(errs, errors.New("-sql.memory-limit must be positive"))
	}
	if c.Threads <= 0 {
		errs = append(errs, errors.New("-sql.threads must be positive"))
	}
	if c.MaxConcurrent <= 0 {
		errs = append(errs, errors.New("-sql.max-concurrent must be positive"))
	}
	if c.DefaultLimit > 0 && c.MaxRows > 0 && c.DefaultLimit > c.MaxRows {
		errs = append(errs, fmt.Errorf("-sql.default-limit (%d) must not exceed -sql.max-rows (%d)", c.DefaultLimit, c.MaxRows))
	}
	return errs
}
