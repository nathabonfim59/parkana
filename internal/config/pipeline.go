package config

import (
	"flag"
	"fmt"
	"time"
)

// Pipeline holds the reload timings shared by every role that applies
// pipelines (PLAN.md §6). Each role polls the catalog's per-tenant change
// token on its own; these values only size that loop.
type Pipeline struct {
	// PollInterval is how often a role checks the change token of every
	// tenant it has seen.
	PollInterval time.Duration
	// ReloadTimeout bounds one tenant's poll reload, including its catalog
	// requests. First use of a tenant loads synchronously with the
	// operation's own context instead.
	ReloadTimeout time.Duration
}

// Defaults for the pipeline reload loop. They mirror
// pipeline.DefaultPollInterval and pipeline.DefaultReloadTimeout; config
// cannot import internal/pipeline because pipeline imports ingest, which
// imports config. Tests in cmd/parkana compare the two sets.
const (
	// DefaultPipelinePollInterval is the default -pipeline.poll-interval.
	DefaultPipelinePollInterval = time.Second
	// DefaultPipelineReloadTimeout is the default -pipeline.reload-timeout.
	DefaultPipelineReloadTimeout = 5 * time.Second
)

func (c *Pipeline) register(fs *flag.FlagSet) {
	*c = Pipeline{
		PollInterval:  DefaultPipelinePollInterval,
		ReloadTimeout: DefaultPipelineReloadTimeout,
	}
	fs.DurationVar(&c.PollInterval, "pipeline.poll-interval", c.PollInterval,
		"how often each role checks for changed pipeline definitions")
	fs.DurationVar(&c.ReloadTimeout, "pipeline.reload-timeout", c.ReloadTimeout,
		"upper bound on one tenant's pipeline reload, including catalog requests")
}

func (c Pipeline) validate() []error {
	var errs []error
	if c.PollInterval <= 0 {
		errs = append(errs, fmt.Errorf("-pipeline.poll-interval must be positive, got %s", c.PollInterval))
	}
	if c.ReloadTimeout <= 0 {
		errs = append(errs, fmt.Errorf("-pipeline.reload-timeout must be positive, got %s", c.ReloadTimeout))
	}
	return errs
}
