# Storage

Parkana stores logs as Parquet files in an object store: a local directory or
any store that speaks the S3 API. Each file holds the rows of one stream and
one UTC hour, sorted by time, and has a small search sidecar next to it. The
object store holds only immutable files; which files are live, and what each
one covers, is the [catalog](catalog.md)'s job. This page describes the keys,
the file format, the sidecars and the two backends.

## Object keys

Every data file has the key

```text
{tenant}/logs/{stream}/{yyyy}/{mm}/{dd}/{hh}/{uuidv7}.parquet
```

and its search sidecar sits next to it, with `.idx` instead of `.parquet`:

```text
default/logs/checkout/2026/11/24/01/01a20b32-a04a-7127-bf0e-0619d1e8ec69.parquet
default/logs/checkout/2026/11/24/01/01a20b32-a04a-7127-bf0e-0619d1e8ec69.idx
```

- `{tenant}` is the tenant id; it is `default` while Parkana is single tenant.
- `{stream}` is the stream name chosen by
  [`-stream.labels`](configuration.md#streams) (see
  [stream naming](ingest.md#stream-naming)).
- `{yyyy}/{mm}/{dd}/{hh}` is the UTC hour of the file's oldest row. The
  ingester and the compactor never put rows of two hours in one file.
- `{uuidv7}` is a time-ordered UUID, so keys sort by creation time.

Keys are written once and never overwritten: an upload that has to be
repeated gets a new key (see [ingest](ingest.md#crash-recovery)). That is why
the querier can cache objects by key without ever checking them for changes.

## Parquet files

Every file starts with four base columns, followed by the stream's promoted
label columns:

| column | Parquet / Arrow type | holds |
|---|---|---|
| `_ts` | `TIMESTAMP(MICROS, UTC)` | the entry timestamp, the sort key |
| `line` | string | the log line |
| `labels` | `MAP<string, string>` | stream labels that are not promoted |
| `meta` | `MAP<string, string>` | Loki structured metadata and OTLP attributes |
| promoted labels | nullable string | one column per label named in [`-ingest.promoted-labels`](configuration.md#ingest) for the stream |

Timestamps are stored in microseconds. Loki pushes nanoseconds, so Parkana
truncates them when it builds the memtable rows; queries return the stored
microsecond value as nanoseconds. [LogQL compatibility](query/logql.md#microsecond-storage-and-strict-comparison)
covers what that means for comparisons.

A promoted label is removed from the `labels` map and stored in its own
column; a row without that label has a null there. With the default
`-ingest.promoted-labels=*=level`, every stream gets a `level` column.

The writer uses zstd compression, column statistics and the page index, and
cuts row groups at about 4 MiB of data or 262,144 rows. Rows are written in
`_ts` order, and the file records `_ts` as its sorting column. The Arrow
schema is stored in the file's metadata.

The names `line`, `labels` and `meta`, in any letter case, and every name that
starts with `_` are reserved: they cannot be promoted labels.

## Schema evolution

Schema changes are additive only. Promoted columns are appended; no column is
ever renamed, retyped or dropped.

Changing `-ingest.promoted-labels` affects new files only. Files already
written keep the columns they had, so one stream can have files where a label
is a column and files where it sits in the `labels` map. The querier handles
both: it groups files by their promoted columns and reads a label as the
column value, or the map entry when the column is null, or the empty string
when neither exists. A compactor merge of such files unions their columns and
fills missing ones with nulls; values that were in the map stay in the map, so
the result reads the same.

## Search sidecars

A sidecar is a byte n-gram bloom filter over every line in its data file. When
a LogQL query has a `|=` filter, the querier checks each candidate file's
sidecar and skips files that cannot contain the needle. Sidecars never cause a
false negative: only `|=` needles prune, a needle shorter than the gram length
does not prune, and a missing, unreadable or unknown sidecar means "scan the
file". Regular expressions and `!=` filters always scan.

The format is small and versioned: the magic `PKIX`, a format version, the
gram length, the serialized bloom filter and a CRC32. The gram length and
false-positive rate are stored in each sidecar, so changing the settings only
affects sidecars built later.

| flag | default | effect |
|---|---|---|
| [`-index.ngram`](configuration.md#search-sidecars) | `4` | gram length in bytes (1 to 8); needles shorter than this never prune |
| `-index.fp-rate` | `0.01` | target false-positive rate of one gram lookup |
| `-index.max-grams` | `2097152` | a file with more distinct grams gets no sidecar and is always scanned |

The ingester builds the sidecar of every file it uploads and puts the sidecar
object before it adds the file to the catalog, so a catalog row that says the
sidecar exists never points at a missing one. A compactor merge builds the
sidecar of its output the same way, and the compactor's
[backfill](compactor.md#sidecar-backfill) builds sidecars for files that have
none.

Label pruning does not use the sidecar: it uses the label values recorded in
the catalog (see [catalog](catalog.md#what-the-catalog-stores)).

## Backends

[`-storage.backend`](configuration.md#storage-and-catalog) picks the object
store: `fs` (the default) or `s3`.

### Local directory (fs)

The fs backend keeps objects under
[`-storage.fs.dir`](configuration.md#storage-and-catalog), by default
`<data.dir>/objects`. Every access goes through Go's `os.Root`, so a key can
never reach outside that directory. Each write goes to a temporary file under
`.tmp/`, is fsynced, renamed into place, and then every directory from the
key's directory up to the root is fsynced, so a stored object survives a
machine crash. A delete fsyncs the directory too. Empty directories are left
in place.

With the fs backend the querier reads the files in place; the query cache is
not used (see [query](query/index.md#object-cache)).

### S3-compatible stores

The s3 backend uses the minio-go client. It needs
[`-storage.s3.endpoint`](configuration.md#storage-and-catalog) and
`-storage.s3.bucket`; Parkana refuses to start without them.

| flag | default | effect |
|---|---|---|
| `-storage.s3.endpoint` | none | `host[:port]` of the S3 API, without a scheme |
| `-storage.s3.bucket` | none | the bucket that holds every object |
| `-storage.s3.region` | `us-east-1` | the region used to sign requests and to create the bucket |
| `-storage.s3.access-key`, `-storage.s3.secret-key` | none | static credentials; pass them as `PARKANA_STORAGE_S3_ACCESS_KEY` and `PARKANA_STORAGE_S3_SECRET_KEY` |
| `-storage.s3.insecure` | `false` | plain HTTP instead of HTTPS, with unsigned payloads; for local test stores only |
| `-storage.s3.create-bucket` | `false` | create the bucket at startup when it does not exist |

Only static access keys are supported: Parkana does not read AWS credential
files, instance roles or the `AWS_*` environment variables. Without
`-storage.s3.create-bucket`, startup does not contact the store, so a wrong
endpoint or key shows up on the first upload or query. Requests honor the
`HTTPS_PROXY` and `HTTP_PROXY` environment variables.

Parkana does not choose the bucket addressing style; minio-go does, from the
endpoint. It uses virtual-host style (`bucket.host`) for Amazon S3, Google
Cloud Storage and Aliyun OSS endpoints, and path style (`host/bucket`) for
every other endpoint, so self-hosted stores need no DNS entry per bucket.
Over HTTPS, a bucket name with a dot always uses path style.

Objects of unknown size, such as a compactor merge that is streamed while it
is written, are uploaded in 16 MiB multipart parts.

What has been run against which store:

- Garage is the store of the development stack in `deploy/compose/` and of
  the benchmarks: endpoint `garage:3900`, region `garage`, `insecure`, bucket
  created by the stack.
- The backend's unit tests run against an in-memory S3 fake; an integration
  test runs against any real store named by `PARKANA_TEST_S3_ENDPOINT`.
- For Amazon S3, use the regional endpoint of the bucket's region and set
  `-storage.s3.region` to that region.
- MinIO, Ceph RGW and Cloudflare R2 speak the same API and need only the
  endpoint, region and keys above; they are not part of the project's test
  runs.

`-data.dir` still holds the WAL, local Parquet waiting for upload, the SQLite
catalog and the query cache with the S3 backend, so keep it on a persistent
disk.

## Durability

Parkana does not replicate data itself; it relies on the durability of the
object store and of the disk under `-data.dir`. Everything it reports as
durable is fsynced along with the directory entries that make it reachable:
WAL appends, local Parquet, the ingest manifests and fs backend objects. With
S3, an object counts as stored once the store answers the upload.
