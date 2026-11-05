package config

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"time"
)

// Default live-tail settings. The buffer is counted in log entries, not
// batches, so one large push cannot bypass it.
const (
	DefaultTailBufferEntries = 1024
	DefaultTailFlushInterval = 100 * time.Millisecond
)

// LogQL holds the LogQL engine's limits and live tail settings. Owned by
// internal/logql and internal/tail.
type LogQL struct {
	MaxEntries int // largest limit a log query may ask for (Loki: max_entries_limit_per_query)
	MaxSeries  int // most series a metric query may return (Loki: max_query_series)
	Tail       Tail
}

// Tail holds the live-tail transport settings. A querier subscribes to every
// URL in IngesterURLs; an ingester serves the private endpoint those URLs
// name.
type Tail struct {
	// IngesterURLs are comma-separated HTTP(S) base URLs of ingesters. They
	// must not carry credentials or a fragment.
	IngesterURLs []string
	// BufferEntries bounds the entries queued for one slow subscriber; a
	// subscriber that overflows is evicted rather than blocking a push.
	BufferEntries int
	// FlushInterval is how often a querier may coalesce streamed batches.
	FlushInterval time.Duration
}

func (c *LogQL) register(fs *flag.FlagSet) {
	*c = LogQL{
		MaxEntries: 5000,
		MaxSeries:  500,
		Tail: Tail{
			BufferEntries: DefaultTailBufferEntries,
			FlushInterval: DefaultTailFlushInterval,
		},
	}
	fs.IntVar(&c.MaxEntries, "logql.max-entries", c.MaxEntries, "largest limit a log query may ask for")
	fs.IntVar(&c.MaxSeries, "logql.max-series", c.MaxSeries, "most series a metric query may return")
	fs.Var((*listFlag)(&c.Tail.IngesterURLs), "tail.ingester-urls", "comma-separated HTTP(S) base URLs of tail ingesters (empty: derive a loopback URL when this process runs the ingester role)")
	fs.IntVar(&c.Tail.BufferEntries, "tail.buffer-entries", c.Tail.BufferEntries, "most entries buffered for one slow live-tail subscriber before it is evicted")
	fs.DurationVar(&c.Tail.FlushInterval, "tail.flush-interval", c.Tail.FlushInterval, "how often a querier may coalesce live-tail batches")
}

func (c LogQL) validate() []error {
	var errs []error
	if c.MaxEntries <= 0 || c.MaxSeries <= 0 {
		errs = append(errs, fmt.Errorf("-logql.max-entries and -logql.max-series must be positive"))
	}
	if c.Tail.BufferEntries <= 0 {
		errs = append(errs, fmt.Errorf("-tail.buffer-entries must be positive"))
	}
	if c.Tail.FlushInterval <= 0 {
		errs = append(errs, fmt.Errorf("-tail.flush-interval must be positive"))
	}
	for i, raw := range c.Tail.IngesterURLs {
		if err := validateTailURL(raw); err != nil {
			errs = append(errs, fmt.Errorf("-tail.ingester-urls: entry %d: %w", i+1, err))
		}
	}
	return errs
}

// validateTailURL checks one -tail.ingester-urls entry: an absolute HTTP(S)
// base URL with a host, without credentials, a query or a fragment, so it is
// safe to log and to dial. Errors never echo the URL: a rejected entry may
// contain a password or token.
func validateTailURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return errors.New("not a valid URL")
	}
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return errors.New("scheme must be http or https")
	case u.Host == "":
		return errors.New("missing host")
	case u.User != nil:
		return errors.New("must not contain credentials")
	case u.RawQuery != "" || u.ForceQuery:
		return errors.New("must not contain a query string")
	case u.Fragment != "":
		return errors.New("must not contain a fragment")
	}
	return nil
}

// TailIngesterURLs returns the ingester URLs a querier should subscribe to.
// Configured URLs win; otherwise, when this process runs the ingester role, it
// derives one loopback URL from HTTPAddr and maps wildcard listeners to
// 127.0.0.1. A querier-only process without configured URLs gets nil, which
// leaves the public tail endpoint free to answer 503.
func (c Config) TailIngesterURLs() []string {
	if len(c.LogQL.Tail.IngesterURLs) > 0 {
		return slices.Clone(c.LogQL.Tail.IngesterURLs)
	}
	if !c.Has(RoleIngester) {
		return nil
	}
	return []string{loopbackTailURL(c.HTTPAddr)}
}

// loopbackTailURL turns an HTTP listen address into a dialable loopback base
// URL. Wildcard hosts (empty, 0.0.0.0, ::) become 127.0.0.1; a missing port is
// allowed because Config is validated separately.
func loopbackTailURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		host, port = strings.Trim(addr, "[]"), ""
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	}
	return (&url.URL{Scheme: "http", Host: host}).String()
}
