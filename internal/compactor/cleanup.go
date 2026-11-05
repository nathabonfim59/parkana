package compactor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nathabonfim59/parkana/internal/catalog"
	"github.com/nathabonfim59/parkana/internal/objstore"
	"github.com/nathabonfim59/parkana/internal/tenant"
)

// cleanupOrphanGrace is how long an unreferenced object must be untouched
// before an orphan scan may delete it. A file that was just written but whose
// catalog row has not landed yet must survive.
const cleanupOrphanGrace = time.Hour

// cleanupStats reports what one cleanup pass did.
type cleanupStats struct {
	// marked is the number of live catalog rows logically deleted by
	// retention. Their objects stay until a later purge.
	marked int
	// purged is the number of deleted catalog rows dropped by purge. Their
	// data and sidecar objects are deleted in the same pass.
	purged int
	// orphaned is the number of unreferenced objects deleted by the orphan
	// scan.
	orphaned int
}

// cleanup runs one retention, delete-delay purge and orphan-GC pass for id.
//
// Retention marks whole live files whose MaxTS is strictly before
// now-retention; it never deletes objects. Purge drops catalog rows deleted
// before now-deleteDelay and deletes their data and sidecar objects, whether
// or not the row says the sidecar exists. The orphan scan then removes objects
// under the tenant's logs prefix that no catalog row references (deleted rows
// included) and whose object ModTime is strictly older than now-1h.
//
// The three phases are independent: a failure in one is joined with the
// others' errors and does not stop the remaining work. An incomplete catalog
// listing does disable the orphan scan, because deleting objects without the
// full reference set would be unsafe. Cancellation is checked between phases
// and between deletions.
func cleanup(ctx context.Context, store objstore.Store, cat catalog.Catalog, id tenant.ID, now time.Time, retention, deleteDelay time.Duration) (cleanupStats, error) {
	var st cleanupStats
	if err := ctx.Err(); err != nil {
		return st, fmt.Errorf("compactor: cleanup: %w", err)
	}
	if err := id.Validate(); err != nil {
		return st, fmt.Errorf("compactor: cleanup: tenant: %w", err)
	}

	var errs []error
	if retention > 0 {
		marked, err := cleanupRetention(ctx, cat, id, now, retention)
		st.marked = marked
		if err != nil {
			errs = append(errs, fmt.Errorf("compactor: cleanup: retention: %w", err))
		}
	}
	if err := ctx.Err(); err != nil {
		return st, errors.Join(append(errs, fmt.Errorf("compactor: cleanup: %w", err))...)
	}

	purged, err := cleanupPurge(ctx, store, cat, id, now, deleteDelay)
	st.purged = purged
	if err != nil {
		errs = append(errs, fmt.Errorf("compactor: cleanup: purge: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return st, errors.Join(append(errs, fmt.Errorf("compactor: cleanup: %w", err))...)
	}

	orphaned, err := cleanupOrphans(ctx, store, cat, id, now)
	st.orphaned = orphaned
	if err != nil {
		errs = append(errs, fmt.Errorf("compactor: cleanup: orphans: %w", err))
	}
	return st, errors.Join(errs...)
}

// cleanupRetention marks live files whose MaxTS is strictly before
// now-retention. It returns the number of rows marked.
func cleanupRetention(ctx context.Context, cat catalog.Catalog, id tenant.ID, now time.Time, retention time.Duration) (int, error) {
	cutoff := now.Add(-retention)
	var keys []string
	for f, err := range cat.ListFiles(ctx, catalog.Filter{Tenant: id}) {
		if err != nil {
			return 0, fmt.Errorf("list files: %w", err)
		}
		// An iterator may end normally after the context was canceled. Check
		// every yield so a canceled pass never reaches ReplaceFiles.
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		// MaxTS is inclusive, so a file ending exactly at the cutoff may
		// still hold data inside the window.
		if f.MaxTS.Before(cutoff) {
			keys = append(keys, f.Key)
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, nil
	}
	if err := cat.ReplaceFiles(ctx, id, keys, nil); err != nil {
		return 0, fmt.Errorf("mark %d expired files: %w", len(keys), err)
	}
	return len(keys), nil
}

// cleanupPurge drops catalog rows whose delete delay has elapsed and deletes
// their data and sidecar objects. It returns the number of rows purged.
func cleanupPurge(ctx context.Context, store objstore.Store, cat catalog.Catalog, id tenant.ID, now time.Time, deleteDelay time.Duration) (int, error) {
	keys, err := cat.PurgeDeleted(ctx, id, now.Add(-deleteDelay))
	if err != nil {
		return 0, fmt.Errorf("purge catalog: %w", err)
	}
	prefix := cleanupTenantLogsPrefix(id)
	var errs []error
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		// A malformed catalog row must not let purge reach another tenant's
		// objects. Report it, drop nothing, and keep going with valid keys.
		if err := objstore.ValidateKey(key); err != nil {
			errs = append(errs, fmt.Errorf("purge invalid key: %w", err))
			continue
		}
		if !strings.HasPrefix(key, prefix) {
			errs = append(errs, fmt.Errorf("purge key %q: not under tenant %q logs prefix", key, id))
			continue
		}
		// The sidecar is deleted unconditionally: the row is already gone, so
		// leaving an unlisted sidecar behind would be an orphan either way.
		for _, k := range [2]string{key, objstore.SidecarKey(key)} {
			if err := store.Delete(ctx, k); err != nil && !errors.Is(err, objstore.ErrNotExist) {
				errs = append(errs, fmt.Errorf("delete %s: %w", k, err))
			}
		}
	}
	return len(keys), errors.Join(errs...)
}

// cleanupTenantLogsPrefix is the key prefix of everything a tenant stores:
// {tenant}/logs/.
func cleanupTenantLogsPrefix(id tenant.ID) string {
	return string(id) + "/logs/"
}

// cleanupOrphans deletes unreferenced objects under the tenant's logs prefix
// whose object ModTime is strictly older than now-1h. It returns the number of
// objects deleted.
func cleanupOrphans(ctx context.Context, store objstore.Store, cat catalog.Catalog, id tenant.ID, now time.Time) (int, error) {
	prefix := cleanupTenantLogsPrefix(id)

	// Every catalog row protects its data key and sidecar sibling, deleted
	// rows included: they stay listed until purge so in-flight queries can
	// finish, and a deleted row's object must not be reclaimed early.
	protected := make(map[string]struct{})
	for f, err := range cat.ListFiles(ctx, catalog.Filter{Tenant: id, IncludeDeleted: true}) {
		if err != nil {
			return 0, fmt.Errorf("list catalog references: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		protected[f.Key] = struct{}{}
		protected[objstore.SidecarKey(f.Key)] = struct{}{}
	}
	// The iterator can end cleanly after cancellation; never list or delete
	// objects without the full reference set.
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	graceCutoff := now.Add(-cleanupOrphanGrace)
	var (
		orphaned int
		errs     []error
	)
	for info, err := range store.List(ctx, prefix) {
		if err != nil {
			errs = append(errs, fmt.Errorf("list objects: %w", err))
			break
		}
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		// Re-check the prefix so a Store that ignores the List prefix can
		// never make us delete another tenant's object.
		if !strings.HasPrefix(info.Key, prefix) {
			continue
		}
		if _, ok := protected[info.Key]; ok {
			continue
		}
		// An unknown ModTime is not proof of age; keep the object.
		if info.ModTime.IsZero() || !info.ModTime.Before(graceCutoff) {
			continue
		}
		if err := store.Delete(ctx, info.Key); err != nil && !errors.Is(err, objstore.ErrNotExist) {
			errs = append(errs, fmt.Errorf("delete orphan %s: %w", info.Key, err))
			continue
		}
		orphaned++
	}
	return orphaned, errors.Join(errs...)
}
