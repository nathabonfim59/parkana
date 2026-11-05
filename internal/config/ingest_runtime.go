package config

import (
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// WAL sync modes for IngestRuntime.WALSync.
const (
	// WALSyncModeAlways fsyncs every WAL write before the push is
	// acknowledged.
	WALSyncModeAlways = "always"
	// WALSyncModeInterval acknowledges after the WAL write and memtable
	// admission, and fsyncs dirty data on IngestRuntime.WALSyncInterval.
	WALSyncModeInterval = "interval"
)

// maxRuntimeConcurrency bounds FlushConcurrency and UploadConcurrency so a
// misconfigured worker count cannot exhaust memory or file descriptors.
const maxRuntimeConcurrency = 64

// IngestRuntime tunes the ingester pipeline's runtime behavior: the WAL
// fsync policy and the worker counts for local flush and object upload. It
// is embedded in Ingest. Unlike the rest of Ingest, a zero field is
// meaningful: it selects the flag default, so a programmatically built
// Ingest behaves like the CLI defaults. Explicitly invalid flag or
// environment values are rejected when they are parsed.
type IngestRuntime struct {
	// WALSync selects when WAL writes become durable: WALSyncModeAlways
	// (the default) or WALSyncModeInterval.
	WALSync string
	// WALSyncInterval is how often dirty WAL data is fsynced in
	// WALSyncModeInterval mode. Unused when WALSync is always.
	WALSyncInterval time.Duration
	// FlushConcurrency bounds how many memtable parts write local Parquet
	// in parallel.
	FlushConcurrency int
	// UploadConcurrency bounds how many independent upload receipts
	// upload to the object store in parallel.
	UploadConcurrency int
	// UploadMemory is the upload pipeline's memory budget in bytes. Zero
	// (the default) selects an automatic budget resolved by the ingester
	// from the container and GOMEMLIMIT memory limits; a positive value
	// fixes the budget.
	UploadMemory ByteSize
}

// registerRuntime sets the runtime defaults and registers their flags. It
// runs after Ingest.register initializes the struct, so the defaults survive
// the struct assignment there.
func (c *Ingest) registerRuntime(fs *flag.FlagSet) {
	c.WALSync = WALSyncModeAlways
	c.WALSyncInterval = time.Second
	c.FlushConcurrency = 4
	c.UploadConcurrency = 4
	c.UploadMemory = 0 // automatic budget
	fs.Var(walSyncFlag{&c.WALSync}, "wal.sync",
		"WAL durability mode: 'always' fsyncs every write before ack; 'interval' acks after the write and fsyncs on -wal.sync-interval")
	fs.Var(positiveDurationFlag{&c.WALSyncInterval}, "wal.sync-interval",
		"how often dirty WAL data is fsynced in -wal.sync=interval mode")
	fs.Var(workerCountFlag{&c.FlushConcurrency}, "ingest.flush-concurrency",
		"how many memtable parts may write local Parquet in parallel")
	fs.Var(workerCountFlag{&c.UploadConcurrency}, "ingest.upload-concurrency",
		"how many independent upload receipts may upload in parallel")
	fs.Var(uploadMemoryFlag{&c.UploadMemory}, "ingest.upload-memory",
		"memory budget for the upload pipeline; 0 derives an automatic budget from the container and GOMEMLIMIT limits")
}

// validateRuntime checks fields a hand-built Config could hold. A zero
// field means "use the default" and is valid; the flag values above reject
// explicit nonpositive or out-of-range input at parse time, so only
// programmatic configs can reach validation with one.
func (c Ingest) validateRuntime() []error {
	var errs []error
	switch c.WALSync {
	case "", WALSyncModeAlways, WALSyncModeInterval:
	default:
		errs = append(errs, fmt.Errorf("-wal.sync must be %q or %q, got %q",
			WALSyncModeAlways, WALSyncModeInterval, c.WALSync))
	}
	if c.WALSyncInterval < 0 {
		errs = append(errs, fmt.Errorf("-wal.sync-interval must be positive"))
	}
	for name, n := range map[string]int{
		"ingest.flush-concurrency":  c.FlushConcurrency,
		"ingest.upload-concurrency": c.UploadConcurrency,
	} {
		if n < 0 || n > maxRuntimeConcurrency {
			errs = append(errs, fmt.Errorf("-%s must be between 1 and %d, got %d",
				name, maxRuntimeConcurrency, n))
		}
	}
	if c.UploadMemory < 0 {
		errs = append(errs, fmt.Errorf("-ingest.upload-memory must not be negative"))
	}
	return errs
}

// walSyncFlag backs -wal.sync. Like the other runtime flags it validates in
// Set so an explicit "-wal.sync=" or a typo is rejected on the command line
// and in the environment, while an empty WALSync field in a hand-built
// Config still means WALSyncModeAlways.
type walSyncFlag struct{ s *string }

func (f walSyncFlag) String() string {
	if f.s == nil {
		return ""
	}
	return *f.s
}

func (f walSyncFlag) Set(s string) error {
	switch s {
	case WALSyncModeAlways, WALSyncModeInterval:
		*f.s = s
		return nil
	default:
		return fmt.Errorf("must be %q or %q, got %q", WALSyncModeAlways, WALSyncModeInterval, s)
	}
}

// positiveDurationFlag backs -wal.sync-interval. Rejecting a non-positive
// value in Set is what keeps an explicit "-wal.sync-interval=0" an error
// while a zero field in a hand-built Config still means "use the default".
type positiveDurationFlag struct{ d *time.Duration }

func (f positiveDurationFlag) String() string {
	if f.d == nil {
		return "0s"
	}
	return f.d.String()
}

func (f positiveDurationFlag) Set(s string) error {
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return err
	}
	if d <= 0 {
		return fmt.Errorf("must be positive, got %s", d)
	}
	*f.d = d
	return nil
}

// workerCountFlag backs the concurrency flags. As with
// positiveDurationFlag, Set rejects out-of-range values so an explicit 0
// stays an error even though a zero field means "use the default".
type workerCountFlag struct{ n *int }

func (f workerCountFlag) String() string {
	if f.n == nil {
		return "0"
	}
	return strconv.Itoa(*f.n)
}

func (f workerCountFlag) Set(s string) error {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return fmt.Errorf("invalid worker count %q", s)
	}
	if n <= 0 {
		return fmt.Errorf("worker count must be positive, got %d", n)
	}
	if n > maxRuntimeConcurrency {
		return fmt.Errorf("worker count must be at most %d, got %d", maxRuntimeConcurrency, n)
	}
	*f.n = n
	return nil
}

// uploadMemoryFlag backs -ingest.upload-memory, where 0 means the
// automatic budget. It parses like ByteSize.Set but rejects products
// that would overflow int64; Set would silently wrap, so "16777216TiB"
// would wrap to 0 and read as "automatic".
type uploadMemoryFlag struct{ b *ByteSize }

func (f uploadMemoryFlag) String() string {
	if f.b == nil {
		return "0"
	}
	return f.b.String()
}

func (f uploadMemoryFlag) Set(s string) error {
	n, mult, err := parseSizedNumber(s)
	if err != nil {
		return err
	}
	if n > math.MaxInt64/int64(mult) {
		return fmt.Errorf("size %q overflows", s)
	}
	*f.b = ByteSize(n) * mult
	return nil
}

// parseSizedNumber splits a ByteSize input into its count and unit
// multiplier so Set can bound-check before multiplying.
func parseSizedNumber(s string) (int64, ByteSize, error) {
	t := strings.TrimSpace(s)
	mult := ByteSize(1)
	for _, u := range byteUnits {
		if n, ok := strings.CutSuffix(t, u.suffix); ok {
			t, mult = strings.TrimSpace(n), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil || n < 0 {
		return 0, 0, fmt.Errorf("invalid size %q", s)
	}
	return n, mult, nil
}
