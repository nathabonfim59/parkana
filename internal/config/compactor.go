package config

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

// MinOrphanGrace is the shortest non-zero -compactor.orphan-grace. The
// ingester may publish a Put up to 30 minutes after it completed, so the
// compactor must wait well past that before it calls an unreferenced object
// an orphan.
const MinOrphanGrace = time.Hour

// OrphanMinCandidates is the floor of orphan GC's safety stop: a sweep with
// fewer candidates always proceeds, whatever their share of the listed
// objects. Legitimate orphans (interrupted uploads, upload key rotations,
// failed merges) come a few per crash, so the floor lets a small store shed
// its leftovers without tripping the stop, and it caps what a wrong, empty or
// rolled-back catalog can cost a tiny store at OrphanMinCandidates-1 objects.
const OrphanMinCandidates = 10

// Compactor holds the compactor's merge, retention and orphan GC settings.
type Compactor struct {
	Interval    time.Duration // how often to look for work
	TargetSize  ByteSize      // merge small files up to about this size
	Retention   time.Duration // delete data older than this; 0 keeps forever
	DeleteDelay time.Duration // keep replaced files this long for in-flight queries
	// SidecarBackfill builds search sidecars for data files cataloged
	// before sidecars existed.
	SidecarBackfill bool
	// OrphanGrace is how long an object no catalog row references must be
	// untouched before orphan GC deletes it. 0 turns orphan GC off; any
	// other value is at least MinOrphanGrace.
	OrphanGrace time.Duration
	// OrphanMaxFraction is orphan GC's safety stop: a sweep with at least
	// OrphanMinCandidates candidates that are more than this fraction of the
	// tenant's listed objects deletes nothing. It is in (0, 1]; 1 turns the
	// stop off.
	OrphanMaxFraction float64
}

func (c *Compactor) register(fs *flag.FlagSet) {
	*c = Compactor{
		Interval: time.Minute, TargetSize: 512 * MiB, DeleteDelay: 10 * time.Minute, SidecarBackfill: true,
		OrphanGrace: MinOrphanGrace, OrphanMaxFraction: 0.1,
	}
	fs.DurationVar(&c.Interval, "compactor.interval", c.Interval, "how often the compactor looks for work")
	fs.Var(&c.TargetSize, "compactor.target-size", "merge small files up to about this size")
	fs.DurationVar(&c.Retention, "compactor.retention", c.Retention, "delete data older than this (0 = keep forever)")
	fs.DurationVar(&c.DeleteDelay, "compactor.delete-delay", c.DeleteDelay, "keep replaced files this long before deleting the objects")
	fs.BoolVar(&c.SidecarBackfill, "compactor.sidecar-backfill", c.SidecarBackfill, "build search sidecars for files cataloged without one")
	fs.DurationVar(&c.OrphanGrace, "compactor.orphan-grace", c.OrphanGrace,
		fmt.Sprintf("delete objects no catalog row references once they are this old (0 = never; otherwise at least %s)", shortDuration(MinOrphanGrace)))
	fs.Float64Var(&c.OrphanMaxFraction, "compactor.orphan-max-fraction", c.OrphanMaxFraction,
		fmt.Sprintf("skip an orphan sweep whose %d or more candidates exceed this fraction of the listed objects (0 < f <= 1; 1 = never skip)", OrphanMinCandidates))
}

func (c Compactor) validate() []error {
	var errs []error
	if c.Interval <= 0 || c.TargetSize <= 0 {
		errs = append(errs, fmt.Errorf("-compactor.interval and -compactor.target-size must be positive"))
	}
	if c.Retention < 0 || c.DeleteDelay < 0 {
		errs = append(errs, fmt.Errorf("-compactor.retention and -compactor.delete-delay must not be negative"))
	}
	if err := c.ValidateOrphanGC(); err != nil {
		errs = append(errs, err)
	}
	return errs
}

// ValidateOrphanGC reports an OrphanGrace that is negative or below
// MinOrphanGrace without being 0, and an OrphanMaxFraction outside (0, 1].
func (c Compactor) ValidateOrphanGC() error {
	switch {
	case c.OrphanGrace < 0 || c.OrphanGrace > 0 && c.OrphanGrace < MinOrphanGrace:
		return fmt.Errorf("-compactor.orphan-grace must be 0 (off) or at least %s, got %s: the ingester may publish an upload up to 30m after its Put", shortDuration(MinOrphanGrace), c.OrphanGrace)
	// The negated form also rejects NaN.
	case !(c.OrphanMaxFraction > 0 && c.OrphanMaxFraction <= 1):
		return fmt.Errorf("-compactor.orphan-max-fraction must be greater than 0 and at most 1, got %v", c.OrphanMaxFraction)
	}
	return nil
}

// shortDuration formats d without trailing zero units: 1h rather than 1h0m0s.
func shortDuration(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = strings.TrimSuffix(s, "0s")
	}
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}
