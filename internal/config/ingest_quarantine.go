package config

import (
	"flag"
	"fmt"
	"time"
)

// defaultQuarantineRetention keeps quarantined local files for a week.
const defaultQuarantineRetention = 7 * 24 * time.Hour

// registerQuarantine sets the quarantine retention default and registers its
// flag. It runs after Ingest.register initializes the struct.
func (c *Ingest) registerQuarantine(fs *flag.FlagSet) {
	c.QuarantineRetention = defaultQuarantineRetention
	fs.DurationVar(&c.QuarantineRetention, "ingest.quarantine-retention", c.QuarantineRetention,
		"delete files in the local quarantine directory once they have been there this long; 0 keeps them forever")
}

// validateQuarantine rejects a negative retention; zero means keep forever.
func (c Ingest) validateQuarantine() []error {
	if c.QuarantineRetention < 0 {
		return []error{fmt.Errorf("-ingest.quarantine-retention must not be negative, got %s", c.QuarantineRetention)}
	}
	return nil
}
