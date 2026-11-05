// Package query is the querier role: it reads the Parquet files listed in the
// catalog with DuckDB (a plan.Source) and serves Loki's read API on top of the
// LogQL engine (PLAN.md §5, §8).
package query

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
)

// OpenDB opens an in-process DuckDB database for querying Parquet files.
// Every connection runs with TimeZone=UTC (AGENTS.md timestamps contract), so
// _ts comes back as UTC, and with the Parquet footer (metadata) cache enabled.
//
// The per-connection initializer runs on a connection-lifetime context, never
// on ctx: connections are opened lazily, long after the constructor's context
// may have expired, and one expired creation context must not poison every
// future connection. ctx stays in the signature for opening the database
// itself; the driver opens synchronously today, so nothing else captures it.
func OpenDB(ctx context.Context) (*sql.DB, error) {
	conn, err := duckdb.NewConnector("", func(execer driver.ExecerContext) error {
		// The statement list is a local so no package-level state exists.
		for _, stmt := range []string{
			"SET TimeZone='UTC'",
			// Cache parsed Parquet footers in the database's object cache,
			// keyed by file path. Registered by the parquet extension as a
			// BOOLEAN defaulting to false (DuckDB d54cbbe3, the source commit
			// of the libduckdb v0.20000.0 static build pinned by duckdb-go
			// v2.20000.0: extension/parquet/parquet_extension.cpp:1145-1147).
			//
			// The reader consults the cache on every open (extension/parquet/
			// parquet_reader.cpp:1406-1417): a path's cached footer is reused
			// only when ParquetFileMetadataCache::IsValid accepts it
			// (extension/parquet/parquet_file_metadata_cache.cpp:28,90-93),
			// which delegates to ExternalFileCache::IsValid
			// (src/storage/external_file_cache/external_file_cache.cpp:196-227).
			// For local files the cached entry carries a version tag built
			// from device id, inode, file size and mtime truncated to seconds
			// (src/common/local_file_system.cpp:2062-2069, captured through
			// src/storage/external_file_cache/caching_file_system.cpp:34-42),
			// and validation is tag equality (external_file_cache.cpp:201-203)
			// under the default validate_external_file_cache=VALIDATE_ALL
			// (src/common/settings.json:1573-1577). The 10-second mtime rule
			// in that function belongs to the tag-less remote branch only.
			// Any rewrite that changes inode, size or mtime second therefore
			// reloads the footer; a deleted file fails the open outright.
			// Reused footers also skip the on-disk footer read entirely, so
			// benchmark controls toggle this setting per connection.
			//
			// Safety of caching by path rests on two layers, neither of which
			// is the store: FS.Put itself can atomically overwrite any key
			// (internal/objstore/fs.go temp+rename).
			//
			// Defense-in-depth — validation: every cached footer is
			// revalidated on every open against the version tag, and
			// replacements commonly change it (every Parkana write path
			// publishes through temp+rename — objstore FS.Put, the querier's
			// object cache — landing a new inode and fresh mtime), so a
			// replaced file's footer is reloaded and a deleted file fails the
			// open. The tag is not guaranteed to change on every replacement:
			// allocator inode-number reuse, an identical size and a
			// same-second rewrite can leave it equal — including the
			// querier's own redownload of the same immutable key within the
			// same second. That is safe only because of the layer below: an
			// equal tag over equal bytes serves an equal footer.
			//
			// The safety proof — at-most-once keys (line numbers against
			// sol-b at def5fc5, containing the M2.1 fix
			// 5ec9e01f1ac424d03ac3abc1dd75cbc01ddca1f7 "Rotate upload keys
			// before every resumed Put"): the querier opens only
			// catalog-listed keys (source.go listObjects); the catalog row is
			// written only after Put succeeded (putObject upload.go:643-667 —
			// "a catalog row is never written before this returns nil" —
			// called from putBeforeCatalog ahead of publishUpload's AddFiles
			// upload.go:685); and once the catalog knows a key — live OR
			// tombstoned — the receipt completes without ever touching the
			// object again (processUpload upload.go:334-367; catalogRowStatus
			// upload.go:712-735 lists with IncludeDeleted at :721).
			//
			// A key's bytes are written at most once. Fresh reserved work Puts
			// its reservation key once (ReserveUpload mints it from the
			// upload's UUIDv7); every RESUMED merged/stored receipt rotates to
			// a fresh UUIDv7 key before its first Put, unconditionally — a
			// recorded completion proves an earlier Put of the current key
			// succeeded, and an unknown one may hide it, so age no longer
			// matters (putBeforeCatalog upload.go:399-411; RefreshUploadKey
			// local.go:812-840 assigns objstore.NewDataKey at :830; a Put that
			// outlives the 30m grace also rotates mid-loop,
			// upload.go:441-445). The unconditional rotation covers resumed
			// receipts that outlive publication, compaction and catalog purge
			// alike: even when the old row is gone, the old key is never
			// written again. The compactor streams every merge output to a
			// fresh UUIDv7 key (compactor/merge.go:110) and only ever deletes
			// replaced inputs (cleanup.go).
			"SET parquet_metadata_cache=true",
		} {
			if _, err := execer.ExecContext(context.Background(), stmt, nil); err != nil {
				return fmt.Errorf("query: connection init %s: %w", stmt, err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("query: open duckdb: %w", err)
	}
	return sql.OpenDB(conn), nil
}
