package catalog_test

import (
	"context"
	"errors"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/catalog/catalogtest"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// secretMarker is part of every password the redaction tests use, in every
// whitespace- or punctuation-separated piece of it, so a leak of any piece,
// raw or decoded, shows up as the marker in the error text.
const secretMarker = catalogtest.SecretMarker

// checkRedacted fails t if err is nil, lacks the package prefix, or contains
// the secret marker.
func checkRedacted(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("NewPostgres succeeded, want an error")
	}
	msg := err.Error()
	if strings.Contains(msg, secretMarker) {
		t.Fatalf("error leaks the password: %s", msg)
	}
	if !strings.HasPrefix(msg, "catalog: postgres: ") {
		t.Fatalf("error %q lacks the catalog: postgres: prefix", msg)
	}
}

// TestPostgresDSNRedaction opens DSNs that hold a password and fail, at
// parse time or at connect time (nothing listens on 127.0.0.1:1), and checks
// the error never contains the password, raw or decoded. The cases are
// catalogtest.RedactionCases, shared with the config package's redaction
// test. It needs no database.
func TestPostgresDSNRedaction(t *testing.T) {
	for _, tc := range catalogtest.RedactionCases {
		t.Run(tc.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			c, err := catalog.NewPostgres(ctx, tc.DSN)
			if err == nil {
				c.Close()
			}
			checkRedacted(t, err)
			if secret, ok := tc.Leak(err.Error()); ok {
				t.Fatalf("error leaks %q: %s", secret, err)
			}
		})
	}
}

// TestPostgresEmptyDSN checks the empty DSN is refused before any connect.
func TestPostgresEmptyDSN(t *testing.T) {
	if _, err := catalog.NewPostgres(t.Context(), ""); err == nil {
		t.Fatal("NewPostgres(\"\") succeeded, want an error")
	}
}

// silentServer listens on a loopback port and accepts connections without
// ever answering, so a Postgres client hangs in its startup handshake until
// its context ends. It returns the port.
func silentServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var (
		mu    sync.Mutex
		conns []net.Conn
		wg    sync.WaitGroup
	)
	wg.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	})
	t.Cleanup(func() {
		ln.Close()
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("listener address: %v", err)
	}
	return port
}

// TestPostgresContextErrorsSurvive checks a NewPostgres that hangs in the
// connect handshake ends with the context's error, still matchable with
// errors.Is through the redaction wrapper, and without the password. It needs
// no database.
func TestPostgresContextErrorsSurvive(t *testing.T) {
	port := silentServer(t)
	dsns := map[string]string{
		"keyword": "host=127.0.0.1 port=" + port + " user=u password='S3cr3tA S3cr3tB' dbname=db sslmode=disable connect_timeout=60",
		"url":     "postgres://u:S3cr3t%40S3cr3t@127.0.0.1:" + port + "/db?sslmode=disable&connect_timeout=60",
	}
	for form, dsn := range dsns {
		t.Run(form+"/deadline", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
			defer cancel()
			_, err := catalog.NewPostgres(ctx, dsn)
			checkRedacted(t, err)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error = %v, want errors.Is context.DeadlineExceeded", err)
			}
		})
		t.Run(form+"/canceled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, err := catalog.NewPostgres(ctx, dsn)
			checkRedacted(t, err)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want errors.Is context.Canceled", err)
			}
		})
	}
}

// withPassword returns dsn with its password replaced by password.
func withPassword(t *testing.T, dsn, password string) string {
	t.Helper()
	if !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://") {
		// A later keyword overrides an earlier one.
		return dsn + " password='" + password + "'"
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("DSN in URL form does not parse")
	}
	q := u.Query()
	q.Del("password")
	u.RawQuery = q.Encode()
	u.User = url.UserPassword(u.User.Username(), password)
	return u.String()
}

// TestPostgresWrongPassword connects to the test server with a wrong
// password: the error names the authentication failure (the *pgconn.PgError
// is still reachable) and never contains the password. It needs
// PARKANA_TEST_POSTGRES_DSN and a server that checks passwords for that
// user (not trust authentication).
func TestPostgresWrongPassword(t *testing.T) {
	dsn := withPassword(t, catalogtest.PostgresDSN(t), secretMarker+"Wrong "+secretMarker+"@:%x")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	c, err := catalog.NewPostgres(ctx, dsn)
	if err == nil {
		c.Close()
		t.Skip("the test server accepted a wrong password (trust authentication): nothing to redact")
	}
	checkRedacted(t, err)
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	if !ok {
		t.Fatalf("error %v carries no *pgconn.PgError", err)
	}
	// 28P01 invalid_password, 28000 invalid_authorization_specification.
	if pgErr.Code != "28P01" && pgErr.Code != "28000" {
		t.Fatalf("SQLSTATE = %s (%v), want an authentication failure", pgErr.Code, err)
	}
}

// TestPostgresUniqueViolationIsErrExists checks every path where Postgres
// rejects a key with a unique violation (23505) reports catalog.ErrExists,
// with no raw driver error type in the chain, and changes nothing: a key
// that is live, deleted, added by another instance, or re-added by
// ReplaceFiles. Needs PARKANA_TEST_POSTGRES_DSN.
func TestPostgresUniqueViolationIsErrExists(t *testing.T) {
	dsn := catalogtest.PostgresDSN(t)
	a := openPostgres(t, dsn)
	b := openPostgres(t, dsn)
	ctx := boundedContext(t)

	if err := a.AddFiles(ctx, testFile("live", "api", 0, time.Minute), testFile("old", "api", 0, time.Minute)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := a.ReplaceFiles(ctx, tenant.Default, []string{"old"}, nil); err != nil {
		t.Fatalf("delete old: %v", err)
	}
	checkExists := func(t *testing.T, what string, err error) {
		t.Helper()
		if !errors.Is(err, catalog.ErrExists) {
			t.Fatalf("%s = %v, want ErrExists", what, err)
		}
		if _, ok := errors.AsType[*pgconn.PgError](err); ok {
			t.Fatalf("%s = %v: the raw *pgconn.PgError leaks through", what, err)
		}
	}
	// The existing key sorts last so the batch's other rows are inserted
	// before the violation: the rollback must undo them.
	checkExists(t, "AddFiles live key", a.AddFiles(ctx, testFile("a-new", "api", 0, time.Minute), testFile("live", "api", 0, time.Minute)))
	checkExists(t, "AddFiles deleted key", a.AddFiles(ctx, testFile("a-new", "api", 0, time.Minute), testFile("old", "api", 0, time.Minute)))
	checkExists(t, "AddFiles key from other instance", b.AddFiles(ctx, testFile("a-new", "web", 0, time.Minute), testFile("live", "web", 0, time.Minute)))
	checkExists(t, "ReplaceFiles add live key", b.ReplaceFiles(ctx, tenant.Default, []string{"live"}, []catalog.File{testFile("a-new", "api", 0, time.Minute), testFile("live", "api", 0, time.Minute)}))
	checkExists(t, "ReplaceFiles add deleted key", b.ReplaceFiles(ctx, tenant.Default, []string{"live"}, []catalog.File{testFile("a-new", "api", 0, time.Minute), testFile("old", "api", 0, time.Minute)}))

	if got := listKeys(t, a, catalog.Filter{Tenant: tenant.Default}); !slices.Equal(got, []string{"live"}) {
		t.Fatalf("live files after rejected writes = %v, want [live]", got)
	}
	if got := listKeys(t, b, catalog.Filter{Tenant: tenant.Default, IncludeDeleted: true}); !slices.Equal(got, []string{"live", "old"}) {
		t.Fatalf("all files after rejected writes = %v, want [live old]", got)
	}
	if got, err := b.Streams(ctx, tenant.Default); err != nil || !slices.Equal(got, []string{"api"}) {
		t.Fatalf("Streams after rejected writes = %v, %v, want [api]", got, err)
	}
}
