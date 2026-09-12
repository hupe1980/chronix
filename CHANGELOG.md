# Changelog

Notable changes to Chronix. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/). Chronix is pre-1.0:
per [CONTRIBUTING.md](CONTRIBUTING.md), a breaking change bumps the minor
version and a fix bumps the patch — there is no stability promise before 1.0,
and no migration tooling for the on-disk format.

## [Unreleased]

### Added

- **Webhook signing secrets rotate.** `triggers.webhook_signing_secrets` is
  a list, newest first, and every delivery carries one `v1,<sig>` per secret
  in the space-delimited `webhook-signature` header Standard Webhooks defines
  for this. A receiver holding any one of them verifies, so sender and
  receivers can be updated in either order; with a single secret every
  in-flight delivery failed the moment either side changed.
  `CHRONIX_WEBHOOK_SIGNING_SECRET` takes the list comma-separated.
- **Scheduled checkpoints.** `[database.checkpoints]` — an interval, a
  directory and a `keep` count — and the maintenance thread takes them,
  beside the flush, compaction, rollup and retention passes it already runs.
  Off by default. The first runs at startup rather than one interval later,
  pruning happens after a successful run, and an unfinished run is removed on
  sight. A backup was the one maintenance task that still needed a scheduler
  the embedded deployment does not have.
- **Per-column encryption is configurable.** `[database.field_encryption]`
  names a column and an **environment variable** — never a key — so the key
  is not on the disk it protects and a stolen backup stays unreadable.
  AES-256-GCM per block, each bound to its column name and the segment's
  creation timestamp. Compaction re-encrypts rather than decrypting on the
  way through, and a Parquet export, the cold-tier archive and any rollup
  over the measurement are **refused**, naming the column, because each
  would write the plaintext somewhere the segment's protection does not
  reach. Only a field may be encrypted: a tag is part of the series key and
  is written in plaintext in the segment's sidecar, tag index and bloom
  filter, so declaring one is a write error. Rotation is a second key id;
  every declared key must resolve at startup. The format capability existed
  and was reachable from nothing — no configuration turned it on, the read
  paths were not key-aware, and compaction would have silently decrypted.
- **`backup()` is a checkpoint.** It is driven by the catalog rather than by a
  directory walk, takes a segment lease so nothing it is copying can be
  unlinked under it, and **hard-links** segment files when the target shares a
  filesystem — so a checkpoint of a large database is near instant and costs
  no space until those segments are compacted away. `BackupManifest` gains
  `segments`, the number its catalog names.
- **A restore hard-links the backup's segments** and copies only `catalog/`
  and `wal/` — linkable if and only if immutable, so restoring a large backup
  costs a directory entry per segment rather than its bytes.
- **`Chronix::verify_backup()` and `POST /api/v1/admin/backup/verify`** check
  a backup without restoring it — the same verification a restore runs, on
  its own, so the backups you are keeping can be checked before you need them.
- **`chronix_backups_total`, `chronix_backup_failures_total`,
  `chronix_backup_bytes_total` and `chronix_backup_duration_seconds`**, with
  the two counters published at zero so an alert on a nightly checkpoint that
  stopped running can fire from the first scrape. Panels in
  `dashboards/storage.json`.
- **`restore()` verifies before it copies**: every segment the backup's
  catalog names must be present at its recorded size, and the count must match
  the manifest. An incomplete backup is refused rather than restored into a
  database that fails at its first query. `Chronix::open()` asks the same
  question of any data directory.
- Backing up repeatedly into one directory is a rolling checkpoint: files the
  new one does not name are removed, and the previous manifest is deleted
  first, so a re-checkpoint that fails part-way leaves nothing restorable.

- **`time_bucket()` takes an `origin`**, so a bucket boundary need not be
  midnight on the 1st: `time_bucket('1mo', _time, '', '2024-01-15')` is a
  billing month that runs from the 15th, and
  `time_bucket('1d', _time, 'Europe/Berlin', '2024-01-01 06:00:00')` is a
  shift day that starts at six. Only the origin's phase matters. A monthly
  origin after day 28 is refused rather than clamped — that day is missing
  from some months, so it is not a monthly boundary.
- **Arrow Flight `DoPut` can backfill.** Put `{"backfill": true}` in
  `FlightData.app_metadata`; it was the one write surface that could not
  import history. An unrecognised key is refused rather than ignored.
- `TimeBucket::fixed(Duration)`, because a `Duration` *is* a fixed span —
  which is also why it cannot produce a calendar bucket, and says so.
- **Rollups take an `origin` too**, on the HTTP API and on `RollupBuilder`,
  and the rollup listing now reports it — so what the listing returns can be
  typed straight back into a create request, which is what its documentation
  already promised.

### Changed

- **Breaking:** the catalog records a segment's path **relative** to the
  `segments/` directory, as a `SegmentFile`. `SegmentCatalogEntry.path:
  PathBuf` is now `SegmentCatalogEntry.file: SegmentFile`, and
  `SegmentFile::resolve(segments_dir)` is the only way to an openable path.
  A data directory is relocatable as a result — see *Fixed*.
- **Breaking:** `triggers.webhook_signing_secret` is now
  `triggers.webhook_signing_secrets`, a list;
  `PipelineConfig::webhook_signing_secret` is now `webhook_signing_secrets`;
  and `WebhookConfig::signing_secret` is now `signing_secrets`, with
  `WebhookConfig::with_secrets` beside `new`.
- **Breaking:** `CompactionTask` carries `segments_dir` and a relative
  `output`, with `output_path()` resolving the two; `PrunedSegment` loses its
  `path` field, which nothing read.
- **Breaking:** point-in-time recovery is removed —
  `Chronix::restore_pitr`, `POST /api/v1/admin/restore/pitr` and
  `WalWriter::archive_before`. The endpoint did nothing (see *Removed*).
- **Breaking:** `chronix_engine::storage::{LocalFsBackend, EncryptingBackend}`
  are removed. Neither had a caller.
- **Breaking:** `QueryBuilder::downsample()` takes a `TimeBucket` instead of
  a `Duration`, and `QueryPlan::Downsample` carries one. The native plan can
  now express a calendar day or month, and — more to the point — it now means
  the same thing as `time_bucket()` in SQL and as a rollup tier. A `1d` there
  used to be 86 400 seconds of UTC while `1d` in SQL was the zone's day, so
  the two surfaces disagreed on every daylight-saving transition.
- **Breaking:** `TimeBucket` and `BucketWidth` moved from `chronix::timebucket`
  to `chronix_core::timebucket`, and are in `chronix::prelude`. They are part
  of the data model, and every crate that buckets time now shares one
  definition.
- **Breaking:** the native query API's `EXPLAIN` reports a `Downsample` node's
  bucket as its width, timezone and whether it is a calendar bucket, replacing
  `interval_ms` — which had to invent a length for a month.

- **Breaking (gRPC):** `SqlValue` and `FieldValue` gain a `json` variant,
  carrying the JSON encoding of a value with no scalar protobuf field — a
  list, struct, map, interval or binary cell. These previously arrived as the
  literal text `"<unsupported: List(Float64)>"` in the `string` field, which
  no client could tell from a string.

### Fixed

- **A restored backup is the database that was backed up.** The catalog stored
  absolute segment paths, so a restore onto a fresh directory produced an
  **empty** database — `open()`'s orphan sweep compared the restored files
  against paths naming the original directory, matched none, and deleted every
  one — and a restore *beside* the original silently read the original's
  segments until the copy's first compaction unlinked them. The existing
  round-trip test never flushed, so it exercised a backup with no segments in
  it.
- **A backup taken while the database is working is complete.** `backup()`
  walked `wal/`, `segments/` and `catalog/` with `read_dir`: a WAL file
  truncated by the next flush vanished mid-copy and failed the backup with a
  bare `ENOENT`; a catalog snapshot landing mid-copy could pair the old
  snapshot with the log that snapshot had truncated, losing every transition
  between them; and a segment flushed between two directory walks was named by
  the copied catalog and absent from the copy.
- **`Chronix::tag_keys()` and `tag_values()` see unflushed series.** They read
  the inverted tag index, which is built at flush — so a freshly started
  database reported no labels at all, and a tag appearing only in recent data
  was invisible. `chronixd`'s `/labels` was always correct, because it scans;
  the two now agree.
- **`last_value()` finds a pre-1970 point.** It scanned the memtable from `0`
  rather than `i64::MIN` and returned `None` for a series a query returns rows
  for.
- **Every `histogram!` now reaches a Prometheus scrape as a histogram.** The
  exporter was left unconfigured, and `metrics-exporter-prometheus` renders
  histograms as *summaries* unless buckets are set — so no `_bucket` series
  existed, `histogram_quantile()` had nothing to read, and 24 of 45 panel
  targets in the bundled dashboards were permanently empty. A summary's
  quantiles also cannot be aggregated across replicas, and the estimator was
  independently wrong for non-latency distributions
  (`chronix_batch_size{quantile="0.5"}` read `0.9998` for observed values of
  1 and 4320).
- **`dashboards/query-performance.json` queried metrics only a
  `--features cluster` build emits**, so all four panels were empty on every
  standard `chronixd`. Rewritten against the metrics a default build exports:
  query and write latency percentiles, throughput, batch size, SQL plan-cache
  and PromQL scan-cache hit ratios, and segment pruning.
- Write-path counters are published at **zero** from startup, so an
  ingestion-error panel reads `0` rather than "No data" and an alert on it can
  fire from the first scrape.
- A soft delete's grace period no longer depends on the raw wall clock. The
  GC pass compared the deadline against `SystemTime::now()`, so one bad
  reading — a gateway with no battery-backed RTC, an NTP server handing out a
  date in the next century — closed the recovery window instantly and
  hard-deleted the data it existed to protect. It now measures from the same
  reference retention uses: the clock capped by the newest timestamp held.
- `RollupBuilder::bucket()` dropped the bucket's origin. It decomposed the
  `TimeBucket` into width and timezone strings and re-parsed them at
  `build()`, so a tier declared through any protocol handler bucketed from
  midnight on the 1st however the caller had asked.
- Every Arrow type a query can produce now reaches the client as a value.
  Each wire surface carried its own conversion table over a different subset
  of Arrow's types, so an unenumerated type came back as the string
  `"<unsupported: Date32>"` inside a column declared `Date32`, under
  `200 OK` — `SELECT CAST(_time AS DATE) … GROUP BY 1`, an ordinary
  group-by-day, was such a query — or was dropped from the row entirely. One
  encoding now serves every surface, and its match is exhaustive over
  `DataType`, so coverage is a build error rather than a promise. Arrow Flight
  SQL was unaffected throughout: it streams the batch unmodified.
- A decimal column with a **negative scale** (legal in Arrow, produced by a
  cast) reached the client as `null` rather than its digits.
- A SQL mistake now answers `400` with the reason. `SELECT 1/0`,
  `CAST('abc' AS INT)`, `to_timestamp('not-a-date')`,
  `date_trunc('fortnight', …)` and an unknown time zone answered
  `500 … an internal error occurred`, with the message that named the problem
  redacted on the way out. A chronix error raised under a SQL query — a full
  memtable, a query deadline — again keeps its own status instead of being
  flattened to `500`.
- Converting a query result back into points dropped any field column whose
  Arrow type it did not enumerate, `Decimal128` among them. That path serves
  distributed reads **and Raft region snapshots**, so replicating a region
  silently discarded every exact-decimal field it held. It is now one shared
  conversion, and a type it cannot represent fails the snapshot rather than
  disappearing from it.

### Removed

- **Point-in-time recovery.** `Chronix::restore_pitr` and
  `POST /api/v1/admin/restore/pitr` did nothing: the target sequence had to be
  at or after the backup's own, the backup's WAL ends there, so the replay
  window was always empty and every target sequence produced a byte-identical
  database. The other half — `WalWriter::archive_before`, which copies WAL
  files aside before truncation — had no caller, no configuration and no
  documentation, so there was no archive to recover from. It was also the one
  admin endpoint that wrote no audit record.
- **`WalEntry::Delete`.** A delete wrote and fsynced a WAL record carrying its
  resolved tombstones, and nothing could read it: `execute_delete` flushes
  first, so every point a tombstone covers is already in a segment and below
  the WAL floor, which replay never reads. Its stated purpose was
  point-in-time restore. What is left is one `fsync` per delete instead of
  two — the catalog manifest, which was always the durable record. WAL
  discriminant `0x01` is retired and not reused.
- **`EncryptingBackend` and `LocalFsBackend`.** Neither had a caller anywhere,
  and `EncryptingBackend` could only ever have encrypted cold-tier objects,
  because the cold tier is the only implementor of the trait it wraps.
  Chronix does not encrypt its own data directory and no setting made it: the
  security guide's `storage.encryption.enabled` was a key that never parsed,
  and its claims of WAL encryption and an HMAC manifest check were both
  phantom (the manifest is CRC-32C). The documentation now says what is
  actually offered — filesystem or volume encryption for the data directory,
  the bucket's own for the cold tier, and the `.csx` format's per-column
  AES-256-GCM, which fails closed and which no configuration enables.
- `chronixd snapshot` / `chronixd restore` / `chronixd bench` from the docs.
  `chronixd` takes no subcommands.

## [0.4.0]

### Changed

- **Breaking:** webhook delivery now sends a [CloudEvents](https://cloudevents.io)
  1.0 envelope, signed per the [Standard Webhooks](https://www.standardwebhooks.com)
  `v1` scheme (`webhook-id` / `webhook-timestamp` / `webhook-signature`),
  replacing the `X-Chronix-Signature: sha256=<hex>` header. Signing the
  timestamp alongside the body lets a receiver reject a replayed request,
  which the old body-only signature could not express.

### Fixed

- A measurement dropped with `soft_delete_ttl` configured is now actually
  invisible — to SQL, PromQL, the native query API, gRPC and Flight SQL — for
  its whole grace period, and the pending state survives a restart. It was
  previously tracked in memory only and consulted by nothing on the read
  path, so the data stayed fully readable until the background pass
  eventually deleted it.
- HTTP, gRPC and Flight SQL measurement listing, and PromQL discovery, now
  resolve "what measurements exist" through the same accessors the query path
  uses, so a pending-drop measurement can no longer appear in one listing and
  not another.

## [0.3.0] and earlier

Predate this file. See the `v0.1.0` / `v0.2.0` / `v0.3.0` git tags.
