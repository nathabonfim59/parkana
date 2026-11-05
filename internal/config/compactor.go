package config

import (
	"flag"
	"fmt"
	"time"
)

// Compactor holds the compactor's merge and retention settings.
type Compactor struct {
	Interval    time.Duration // how often to look for work
	TargetSize  ByteSize      // merge small files up to about this size
	Retention   time.Duration // delete data older than this; 0 keeps forever
	DeleteDelay time.Duration // keep replaced files this long for in-flight queries
	// SidecarBackfill builds search sidecars for data files cataloged
	// before sidecars existed.
	SidecarBackfill bool
}

func (c *Compactor) register(fs *flag.FlagSet) {
	*c = Compactor{Interval: time.Minute, TargetSize: 512 * MiB, DeleteDelay: 10 * time.Minute, SidecarBackfill: true}
	fs.DurationVar(&c.Interval, "compactor.interval", c.Interval, "how often the compactor looks for work")
	fs.Var(&c.TargetSize, "compactor.target-size", "merge small files up to about this size")
	fs.DurationVar(&c.Retention, "compactor.retention", c.Retention, "delete data older than this (0 = keep forever)")
	fs.DurationVar(&c.DeleteDelay, "compactor.delete-delay", c.DeleteDelay, "keep replaced files this long before deleting the objects")
	fs.BoolVar(&c.SidecarBackfill, "compactor.sidecar-backfill", c.SidecarBackfill, "build search sidecars for files cataloged without one")
}

func (c Compactor) validate() []error {
	var errs []error
	if c.Interval <= 0 || c.TargetSize <= 0 {
		errs = append(errs, fmt.Errorf("-compactor.interval and -compactor.target-size must be positive"))
	}
	if c.Retention < 0 || c.DeleteDelay < 0 {
		errs = append(errs, fmt.Errorf("-compactor.retention and -compactor.delete-delay must not be negative"))
	}
	return errs
}
