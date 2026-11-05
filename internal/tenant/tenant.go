// Package tenant carries the tenant id through every code path.
//
// Parkana runs single-tenant for now, but every WAL path, object key, catalog
// row, cache key and query is scoped by a tenant ID so that turning
// multi-tenancy on later is a config switch, not a data migration.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

// ID identifies a tenant. It is safe to use as a path or object-key segment.
type ID string

// Default is the only tenant while multi-tenancy is off.
const Default ID = "default"

// Header is the Loki convention for passing the tenant (org) id.
const Header = "X-Scope-OrgID"

const maxLen = 64

var (
	ErrInvalid      = errors.New("tenant: invalid id")
	ErrNotPermitted = errors.New("tenant: multi-tenancy is disabled; only the default tenant is accepted")
)

// Validate reports whether id is usable as a tenant id: 1-64 characters from
// [A-Za-z0-9_-].
func (id ID) Validate() error {
	if len(id) == 0 || len(id) > maxLen {
		return fmt.Errorf("%w: length must be 1-%d, got %d", ErrInvalid, maxLen, len(id))
	}
	for _, c := range []byte(id) {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
		default:
			return fmt.Errorf("%w: %q contains %q", ErrInvalid, id, c)
		}
	}
	return nil
}

type ctxKey struct{}

// With returns a context carrying id.
func With(ctx context.Context, id ID) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// From returns the tenant stored in ctx, or Default when there is none.
func From(ctx context.Context) ID {
	if id, ok := ctx.Value(ctxKey{}).(ID); ok {
		return id
	}
	return Default
}

// Resolve turns an X-Scope-OrgID header value into a tenant id. When
// multiTenant is false, the header must be empty or equal to Default.
func Resolve(header string, multiTenant bool) (ID, error) {
	if header == "" {
		return Default, nil
	}
	id := ID(header)
	if err := id.Validate(); err != nil {
		return "", err
	}
	if !multiTenant && id != Default {
		return "", ErrNotPermitted
	}
	return id, nil
}

// Middleware resolves the tenant from the request header and stores it in the
// request context. Invalid or disallowed tenants get 401, as in Loki.
func Middleware(multiTenant bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, err := Resolve(r.Header.Get(Header), multiTenant)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(With(r.Context(), id)))
	})
}
