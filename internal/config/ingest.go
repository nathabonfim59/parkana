package config

import (
	"flag"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/schema"
)

// Ingest holds the ingester's push limits, WAL/memtable/flush/upload settings
// (PLAN.md §2) and promoted-label selection.
type Ingest struct {
	MaxLineSize    ByteSize // reject push lines longer than this
	MaxRequestSize ByteSize // reject push requests larger than this

	MemtableMaxBytes ByteSize      // seal the memtable at this size
	WALMaxBytes      ByteSize      // or when the WAL reaches this size
	FlushInterval    time.Duration // immutables → local Parquet
	UploadInterval   time.Duration // local Parquet → object store
	UploadMinBytes   ByteSize      // upload a partition once it reaches this size
	UploadMaxAge     time.Duration // or once its oldest file is this old
	UploadMergeMax   ByteSize      // merge local files up to this size per upload

	// Promotions moves label values out of the labels map into their own
	// nullable string columns, per stream. The stream name is exact, or "*"
	// for every stream without an own assignment. Column order is kept.
	Promotions []StreamPromotion

	// IngestRuntime carries the WAL sync mode and pipeline worker counts
	// (ingest_runtime.go).
	IngestRuntime
}

// StreamPromotion promotes named label columns for one stream (or every
// stream, when Stream is "*").
type StreamPromotion struct {
	Stream  string   // exact stream name, or "*"
	Columns []string // label names to promote, in configured order
}

func (c *Ingest) register(fs *flag.FlagSet) {
	*c = Ingest{
		MaxLineSize: 256 * KiB, MaxRequestSize: 16 * MiB,
		MemtableMaxBytes: 256 * MiB, WALMaxBytes: 128 * MiB,
		FlushInterval: 5 * time.Second, UploadInterval: 10 * time.Second,
		UploadMinBytes: 128 * MiB, UploadMaxAge: 600 * time.Second, UploadMergeMax: 2 * GiB,
		Promotions: []StreamPromotion{{Stream: "*", Columns: []string{"level"}}},
	}
	c.registerRuntime(fs)
	fs.Var(&c.MaxLineSize, "ingest.max-line-size", "reject push lines longer than this")
	fs.Var(&c.MaxRequestSize, "ingest.max-request-size", "reject push requests larger than this")
	fs.Var(&c.MemtableMaxBytes, "ingest.memtable-max", "seal the memtable at this size")
	fs.Var(&c.WALMaxBytes, "ingest.wal-max", "seal when the WAL reaches this size")
	fs.DurationVar(&c.FlushInterval, "ingest.flush-interval", c.FlushInterval, "how often sealed memtables are written to local Parquet")
	fs.DurationVar(&c.UploadInterval, "ingest.upload-interval", c.UploadInterval, "how often local Parquet is considered for upload")
	fs.Var(&c.UploadMinBytes, "ingest.upload-min", "upload a partition once its local files reach this size")
	fs.DurationVar(&c.UploadMaxAge, "ingest.upload-max-age", c.UploadMaxAge, "upload a local file once it is this old")
	fs.Var(&c.UploadMergeMax, "ingest.upload-merge-max", "merge local files up to this size per uploaded object")
	fs.Var(&promotedFlag{promotions: &c.Promotions}, "ingest.promoted-labels",
		"labels to promote to columns per stream, as stream=col1,col2; '*' is every stream without an own assignment; repeat the flag or use ';' to add streams; an empty value resets")
}

func (c Ingest) validate() []error {
	var errs []error
	for name, d := range map[string]time.Duration{
		"ingest.flush-interval": c.FlushInterval, "ingest.upload-interval": c.UploadInterval,
		"ingest.upload-max-age": c.UploadMaxAge,
	} {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("-%s must be positive", name))
		}
	}
	if c.MemtableMaxBytes <= 0 || c.WALMaxBytes <= 0 {
		errs = append(errs, fmt.Errorf("-ingest.memtable-max and -ingest.wal-max must be positive"))
	}
	if c.MaxLineSize <= 0 || c.MaxRequestSize <= 0 {
		errs = append(errs, fmt.Errorf("-ingest.max-line-size and -ingest.max-request-size must be positive"))
	}
	if c.MaxLineSize > c.MaxRequestSize {
		errs = append(errs, fmt.Errorf("-ingest.max-line-size must not exceed -ingest.max-request-size"))
	}
	if c.UploadMinBytes <= 0 || c.UploadMergeMax <= 0 {
		errs = append(errs, fmt.Errorf("-ingest.upload-min and -ingest.upload-merge-max must be positive"))
	}
	if c.UploadMinBytes > c.UploadMergeMax {
		errs = append(errs, fmt.Errorf("-ingest.upload-min must not exceed -ingest.upload-merge-max"))
	}
	errs = append(errs, validatePromotions(c.Promotions)...)
	errs = append(errs, c.validateRuntime()...)
	return errs
}

// PromotedFor returns the promoted label columns for a stream name: the
// stream's own assignment when one exists, otherwise the wildcard ("*")
// assignment, otherwise nil. The result is a copy; callers may keep or modify
// it.
func (c Ingest) PromotedFor(stream string) []string {
	for _, p := range c.Promotions {
		if p.Stream == stream {
			return slices.Clone(p.Columns)
		}
	}
	for _, p := range c.Promotions {
		if p.Stream == "*" {
			return slices.Clone(p.Columns)
		}
	}
	return nil
}

// validatePromotions checks the stream names, column names and duplicates of
// a full promotion list, however it was built.
func validatePromotions(ps []StreamPromotion) []error {
	var errs []error
	wildcard := false
	seen := make(map[string]bool, len(ps))
	for _, p := range ps {
		if p.Stream == "*" {
			if wildcard {
				errs = append(errs, fmt.Errorf("-ingest.promoted-labels: duplicate wildcard assignment"))
			}
			wildcard = true
		} else {
			if !validStreamName(p.Stream) {
				errs = append(errs, fmt.Errorf("-ingest.promoted-labels: invalid stream name %q (want [a-z0-9_-], max 128 bytes)", p.Stream))
			}
			if seen[p.Stream] {
				errs = append(errs, fmt.Errorf("-ingest.promoted-labels: duplicate assignment for stream %q", p.Stream))
			}
			seen[p.Stream] = true
		}
		if len(p.Columns) == 0 {
			errs = append(errs, fmt.Errorf("-ingest.promoted-labels: stream %s has no columns", p.Stream))
			continue
		}
		for i, col := range p.Columns {
			switch {
			case !validColumnName(col):
				errs = append(errs, fmt.Errorf("-ingest.promoted-labels: stream %s: invalid column name %q", p.Stream, col))
			case schema.IsReserved(col):
				errs = append(errs, fmt.Errorf("-ingest.promoted-labels: stream %s: column %q is reserved", p.Stream, col))
			case slices.Contains(p.Columns[:i], col):
				errs = append(errs, fmt.Errorf("-ingest.promoted-labels: stream %s: column %q listed twice", p.Stream, col))
			case slices.ContainsFunc(p.Columns[:i], func(c string) bool { return schema.Collide(c, col) }):
				errs = append(errs, fmt.Errorf("-ingest.promoted-labels: stream %s: column %q differs only in case from another column", p.Stream, col))
			}
		}
	}
	return errs
}

// promotedFlag is the flag.Value behind -ingest.promoted-labels. A non-empty
// value appends assignments; an empty value resets all promotions, including
// the built-in wildcard default. The first explicit wildcard replaces the
// built-in one; further wildcards are rejected, as are repeated streams.
type promotedFlag struct {
	promotions  *[]StreamPromotion
	wildcardSet bool // an explicit wildcard has been given through this flag
}

func (f *promotedFlag) String() string {
	if f.promotions == nil {
		return ""
	}
	var b strings.Builder
	for i, p := range *f.promotions {
		if i > 0 {
			b.WriteByte(';')
		}
		b.WriteString(p.Stream)
		b.WriteByte('=')
		b.WriteString(strings.Join(p.Columns, ","))
	}
	return b.String()
}

func (f *promotedFlag) Set(s string) error {
	if strings.TrimSpace(s) == "" {
		*f.promotions = nil
		return nil
	}
	for part := range strings.SplitSeq(s, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		p, err := parsePromotion(part)
		if err != nil {
			return err
		}
		if err := f.add(p); err != nil {
			return err
		}
	}
	return nil
}

// add merges one parsed assignment into the configured promotions. An exact
// stream may be assigned once; an explicit wildcard replaces the built-in
// wildcard and may itself be given only once.
func (f *promotedFlag) add(p StreamPromotion) error {
	i := slices.IndexFunc(*f.promotions, func(q StreamPromotion) bool { return q.Stream == p.Stream })
	switch {
	case i < 0:
		*f.promotions = append(*f.promotions, p)
	case p.Stream != "*" || f.wildcardSet:
		return fmt.Errorf("-ingest.promoted-labels: duplicate assignment for stream %q", p.Stream)
	default:
		(*f.promotions)[i] = p // replace the built-in wildcard
	}
	if p.Stream == "*" {
		f.wildcardSet = true
	}
	return nil
}

// parsePromotion parses one stream=col1,col2 assignment. It checks the shape
// only; names and columns are validated by Ingest.validate.
func parsePromotion(s string) (StreamPromotion, error) {
	stream, cols, ok := strings.Cut(s, "=")
	if !ok {
		return StreamPromotion{}, fmt.Errorf("-ingest.promoted-labels: %q is not stream=col1,col2", s)
	}
	stream = strings.TrimSpace(stream)
	if stream == "" {
		return StreamPromotion{}, fmt.Errorf("-ingest.promoted-labels: %q has an empty stream name", s)
	}
	var out []string
	for col := range strings.SplitSeq(cols, ",") {
		col = strings.TrimSpace(col)
		if col == "" {
			return StreamPromotion{}, fmt.Errorf("-ingest.promoted-labels: stream %s: empty column in %q", stream, s)
		}
		out = append(out, col)
	}
	return StreamPromotion{Stream: stream, Columns: out}, nil
}

// validStreamName reports whether name can equal a sanitized stream name
// (logs.SanitizeStream output): [a-z0-9_-], 1 to 128 bytes.
func validStreamName(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range []byte(s) {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// validColumnName reports whether name is a legal promoted label name, with
// the same rule as label names: [a-zA-Z_][a-zA-Z0-9_]*.
func validColumnName(s string) bool {
	if s == "" {
		return false
	}
	for i, c := range []byte(s) {
		switch {
		case c == '_', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}
