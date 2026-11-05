// Package config loads Parkana's configuration from command-line flags, with
// an environment fallback: flag -storage.s3.bucket reads PARKANA_STORAGE_S3_BUCKET
// when not given on the command line. Flags win over the environment.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// EnvPrefix prefixes every environment variable.
const EnvPrefix = "PARKANA_"

// Role is a part of Parkana that can run in this process.
type Role string

const (
	RoleIngester  Role = "ingester"
	RoleQuerier   Role = "querier"
	RoleCompactor Role = "compactor"
	RoleScheduler Role = "scheduler"
)

// AllRoles is what -roles=all expands to, in start order.
var AllRoles = []Role{RoleIngester, RoleQuerier, RoleCompactor, RoleScheduler}

// Config is the full configuration. Defaults come from Load.
type Config struct {
	Roles       []Role
	HTTPAddr    string
	LogLevel    slog.Level
	LogFormat   string // "text" or "json"
	MultiTenant bool
	// StreamLabels picks a log's stream (dataset): the first label present.
	StreamLabels []string
	// DataDir holds the WAL and local Parquet files awaiting upload.
	DataDir string
	Storage Storage
	Catalog Catalog
	// Per-role sections live in their own files (ingest.go, compactor.go,
	// querier.go, logql.go) with their own flags, so roles can grow config
	// independently.
	Ingest    Ingest
	Compactor Compactor
	Querier   Querier
	LogQL     LogQL
	Pipeline  Pipeline
	// Index tunes search-sidecar builds; decoded sidecars carry their own.
	Index Index
	// SQL bounds the querier's SQL endpoint (POST /api/v1/sql).
	SQL SQL
}

// Storage selects the object store.
type Storage struct {
	Backend string // "fs" or "s3"
	FSDir   string
	S3      S3
}

// S3 configures an S3-compatible store (AWS, MinIO, Garage, ...).
type S3 struct {
	Endpoint  string
	Bucket    string
	Region    string
	AccessKey string
	SecretKey string
	Insecure  bool // plain HTTP, for local MinIO
	// CreateBucket makes startup create Bucket when it does not exist. The
	// default is false: the bucket must already exist.
	CreateBucket bool
}

// Catalog selects the metadata store.
type Catalog struct {
	Driver string // "memory" or "sqlite" (later "postgres")
	DSN    string
}

// Load parses args (without the program name). getenv is os.Getenv in
// production. The returned error wraps flag.ErrHelp for -h.
func Load(args []string, getenv func(string) string, output io.Writer) (Config, error) {
	var c Config
	fs := flag.NewFlagSet("parkana", flag.ContinueOnError)
	fs.SetOutput(output)

	roles := listFlag{"all"}
	fs.Var(&roles, "roles", "comma-separated roles to run: all, or any of ingester,querier,compactor,scheduler")
	fs.StringVar(&c.HTTPAddr, "http.addr", ":3100", "HTTP listen address (Loki's default port)")
	level := fs.String("log.level", "info", "log level: debug, info, warn, error")
	fs.StringVar(&c.LogFormat, "log.format", "text", "log format: text or json")
	fs.BoolVar(&c.MultiTenant, "multi-tenant", false, "accept tenants other than 'default' in X-Scope-OrgID (not supported yet)")
	streamLabels := listFlag{"service_name", "job"}
	fs.Var(&streamLabels, "stream.labels", "labels that name a log's stream, first present wins")
	fs.StringVar(&c.DataDir, "data.dir", "./data", "directory for the WAL and local Parquet files")

	fs.StringVar(&c.Storage.Backend, "storage.backend", "fs", "object store: fs or s3")
	fs.StringVar(&c.Storage.FSDir, "storage.fs.dir", "", "object store directory for -storage.backend=fs (empty: <data.dir>/objects)")
	fs.StringVar(&c.Storage.S3.Endpoint, "storage.s3.endpoint", "", "S3 endpoint host[:port]")
	fs.StringVar(&c.Storage.S3.Bucket, "storage.s3.bucket", "", "S3 bucket")
	fs.StringVar(&c.Storage.S3.Region, "storage.s3.region", "us-east-1", "S3 region")
	fs.StringVar(&c.Storage.S3.AccessKey, "storage.s3.access-key", "", "S3 access key (prefer the env var)")
	fs.StringVar(&c.Storage.S3.SecretKey, "storage.s3.secret-key", "", "S3 secret key (prefer the env var)")
	fs.BoolVar(&c.Storage.S3.Insecure, "storage.s3.insecure", false, "use plain HTTP (local MinIO)")
	fs.BoolVar(&c.Storage.S3.CreateBucket, "storage.s3.create-bucket", false, "create the S3 bucket at startup when it does not exist")

	fs.StringVar(&c.Catalog.Driver, "catalog.driver", "sqlite", "catalog: memory or sqlite")
	fs.StringVar(&c.Catalog.DSN, "catalog.dsn", "", "catalog data source (sqlite: file path; empty: <data.dir>/catalog.db)")

	c.Ingest.register(fs)
	c.Compactor.register(fs)
	c.Querier.register(fs)
	c.LogQL.register(fs)
	c.Pipeline.register(fs)
	c.Index.register(fs)
	c.SQL.register(fs)

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if fs.NArg() > 0 {
		return Config{}, fmt.Errorf("config: unexpected arguments %q", fs.Args())
	}
	if err := applyEnv(fs, getenv); err != nil {
		return Config{}, err
	}
	// Empty backend paths derive from -data.dir, so a custom data directory
	// keeps the local object store and catalog together unless overridden.
	if c.Catalog.DSN == "" {
		c.Catalog.DSN = filepath.Join(c.DataDir, "catalog.db")
	}
	if c.Storage.FSDir == "" {
		c.Storage.FSDir = filepath.Join(c.DataDir, "objects")
	}

	var err error
	if c.Roles, err = parseRoles(roles); err != nil {
		return Config{}, err
	}
	if err := c.LogLevel.UnmarshalText([]byte(*level)); err != nil {
		return Config{}, fmt.Errorf("config: -log.level: %w", err)
	}
	c.StreamLabels = streamLabels
	return c, c.Validate()
}

// Validate checks values that flags can't type-check.
func (c Config) Validate() error {
	var errs []error
	if c.LogFormat != "text" && c.LogFormat != "json" {
		errs = append(errs, fmt.Errorf("-log.format must be text or json, got %q", c.LogFormat))
	}
	if c.MultiTenant {
		errs = append(errs, errors.New("-multi-tenant is not supported yet"))
	}
	if len(c.StreamLabels) == 0 {
		errs = append(errs, errors.New("-stream.labels must not be empty"))
	}
	switch c.Storage.Backend {
	case "fs":
		if c.Storage.FSDir == "" {
			errs = append(errs, errors.New("-storage.fs.dir is required"))
		}
	case "s3":
		if c.Storage.S3.Endpoint == "" || c.Storage.S3.Bucket == "" {
			errs = append(errs, errors.New("-storage.s3.endpoint and -storage.s3.bucket are required"))
		}
	default:
		errs = append(errs, fmt.Errorf("-storage.backend must be fs or s3, got %q", c.Storage.Backend))
	}
	if !slices.Contains([]string{"memory", "sqlite"}, c.Catalog.Driver) {
		errs = append(errs, fmt.Errorf("-catalog.driver must be memory or sqlite, got %q", c.Catalog.Driver))
	}
	errs = append(errs, c.Ingest.validate()...)
	errs = append(errs, c.Compactor.validate()...)
	errs = append(errs, c.Querier.validate()...)
	errs = append(errs, c.LogQL.validate()...)
	errs = append(errs, c.Pipeline.validate()...)
	errs = append(errs, c.Index.validate()...)
	errs = append(errs, c.SQL.validate()...)
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return nil
}

// Has reports whether role r is enabled.
func (c Config) Has(r Role) bool { return slices.Contains(c.Roles, r) }

// EnvName returns the environment variable read for flag name.
func EnvName(flagName string) string {
	return EnvPrefix + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(flagName))
}

// applyEnv sets every flag not given on the command line from its env var.
func applyEnv(fs *flag.FlagSet, getenv func(string) string) error {
	set := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	var errs []error
	fs.VisitAll(func(f *flag.Flag) {
		if set[f.Name] {
			return
		}
		if v := getenv(EnvName(f.Name)); v != "" {
			if err := fs.Set(f.Name, v); err != nil {
				errs = append(errs, fmt.Errorf("config: %s: %w", EnvName(f.Name), err))
			}
		}
	})
	return errors.Join(errs...)
}

func parseRoles(names []string) ([]Role, error) {
	var roles []Role
	for _, n := range names {
		r := Role(strings.TrimSpace(n))
		switch {
		case r == "all":
			roles = append(roles, AllRoles...)
		case slices.Contains(AllRoles, r):
			roles = append(roles, r)
		default:
			return nil, fmt.Errorf("config: unknown role %q", n)
		}
	}
	// Dedup, keeping AllRoles order.
	var out []Role
	for _, r := range AllRoles {
		if slices.Contains(roles, r) {
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("config: -roles is empty")
	}
	return out, nil
}

// listFlag is a comma-separated string list.
type listFlag []string

func (l *listFlag) String() string { return strings.Join(*l, ",") }

func (l *listFlag) Set(s string) error {
	*l = nil
	for part := range strings.SplitSeq(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			*l = append(*l, part)
		}
	}
	return nil
}

// ByteSize is a size in bytes that parses "512", "64KiB", "256MiB", "2GiB",
// "100MB" (SI) and so on.
type ByteSize int64

const (
	KiB ByteSize = 1 << (10 * (iota + 1))
	MiB
	GiB
	TiB
)

var byteUnits = []struct {
	suffix string
	mult   ByteSize
}{
	{"KiB", KiB}, {"MiB", MiB}, {"GiB", GiB}, {"TiB", TiB},
	{"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12}, {"B", 1},
}

func (b *ByteSize) Set(s string) error {
	s = strings.TrimSpace(s)
	mult := ByteSize(1)
	for _, u := range byteUnits {
		if n, ok := strings.CutSuffix(s, u.suffix); ok {
			s, mult = strings.TrimSpace(n), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return fmt.Errorf("invalid size %q", s)
	}
	*b = ByteSize(n) * mult
	return nil
}

func (b *ByteSize) String() string {
	for _, u := range byteUnits[:4] {
		if v := *b; v >= u.mult && v%u.mult == 0 && (u.mult == TiB || v%(u.mult<<10) != 0) {
			return strconv.FormatInt(int64(v/u.mult), 10) + u.suffix
		}
	}
	return strconv.FormatInt(int64(*b), 10)
}
