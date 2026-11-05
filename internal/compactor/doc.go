// Package compactor merges small Parquet log files into larger ones so
// queries read fewer objects.
//
// The compactor is a role: New builds an app.Service that periodically scans
// the catalog, groups live files by tenant, stream and the UTC hour in their
// object key, and merges each group of small files that fits the configured
// target size. Every merge streams storage.Merge straight into a new object
// and then swaps the catalog atomically with Catalog.ReplaceFiles.
//
// Each cycle also publishes search sidecars (internal/index): a merge builds
// the sidecar of its final output object and puts it before the catalog swap,
// so a row with HasSidecar never points at a missing sidecar. A file whose
// grams exceed the configured cap is published without one. The backfill pass
// (compactor.sidecar-backfill) then visits every other live file listed
// without a sidecar (files written before sidecars existed, over-cap files
// and files whose flag update failed) and builds one from the stored object.
// It runs after cleanup and merges, isolates per-file failures, and remembers
// over-cap keys in a bounded per-service LRU so it does not re-read the same
// over-cap file every cycle; that memory is empty on restart, so the first
// pass after a restart re-attempts them at one full read per file.
//
// Backfill only sets HasSidecar: it never recomputes or upgrades the label
// summary recorded in the catalog. Pre-phase-3 files keep LabelsComplete=false
// until a merge rewrites them, so single-file or over-target old files stay
// outside label pruning even after their sidecar is built.
//
// A merge never deletes its source objects: on failure they and their catalog
// rows stay intact, and a merge that fails after the upload leaves an
// unreferenced output object rather than a live row pointing at missing data.
// Deleting objects is the cleanup pass's job: each cycle also runs retention
// marking, delete-delay purge and orphan GC, which do remove objects.
//
// Orphan GC only reclaims an object once it is older than one hour
// (cleanupOrphanGrace). That grace period is a cross-part contract: an
// uploader must never AddFiles an object whose Put is older than 30 minutes,
// so a successful upload cannot be mistaken for an orphan before its catalog
// row becomes visible.
package compactor
