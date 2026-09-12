//! Archiving cold data to object storage.
//!
//! The write half of the cold tier: data older than a threshold is encoded to
//! Parquet, uploaded, verified, and then **dropped from the hot database**.
//! The read half is [`crate::sql::cold_tier::register_cold_tier`], which
//! exposes the archive as its own SQL table — and DuckDB, Polars and Spark
//! read the same objects directly.
//!
//! # What an object contains
//!
//! An object is built from the **read path**
//! ([`execute_iter`](crate::Chronix::execute_iter)), so it holds what a query
//! would return: deduplicated last-write-wins, tombstones applied, in the hot
//! tier's schema. A deleted row is not in the archive; an overwritten point
//! appears once, at its winning value.
//!
//! Deduplication is only meaningful across every segment covering a range, so
//! the unit is a `(measurement, shard)` **group**, archived only when it is
//! complete: no other live segment and no unflushed row overlaps its range,
//! and every rollup fed by the measurement is materialised past it. An
//! incomplete group stays hot and is retried next pass.
//!
//! # Why archiving removes the data
//!
//! A cold object is Parquet, which has nowhere to put a series bloom or a tag
//! index. Serving it through the hot read path would let a query silently
//! change cost class when its range crossed the boundary, so the boundary is
//! explicit: after archiving, the data lives in the archive table and nowhere
//! else. This is retention with a copy kept.
//!
//! ```no_run
//! # use std::time::Duration;
//! # use std::sync::Arc;
//! # async fn example(db: Arc<chronix::Chronix>) -> Result<(), Box<dyn std::error::Error>> {
//! use chronix::cold_archive::ArchiveConfig;
//!
//! let outcome = db
//!     .archive_cold_segments(&ArchiveConfig {
//!         cold_after: Duration::from_secs(30 * 86_400),
//!         remote_url: "s3://bucket/chronix".to_string(),
//!         ..Default::default()
//!     })
//!     .await?;
//! println!("archived {} objects, {} rows", outcome.objects, outcome.rows);
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use chronix_core::{SegmentState, ShardId};
use chronix_engine::index::SegmentCatalogEntry;
use chronix_engine::objstore::{
    ArchiveObject, ObjectStoreBackend, ObjectStoreConfig, ParquetArchiveWriter, TieringConfig,
    TieringEngine,
};
use tracing::{info, warn};

use crate::error::{DbError, Result};
use crate::Chronix;

/// Policy for [`Chronix::archive_cold_segments`].
#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    /// How old the newest point in a group must be before it is archived.
    pub cold_after: Duration,
    /// Object-store URL to archive into (`s3://`, `gs://`, `az://`, `file://`).
    pub remote_url: String,
    /// Maximum archive objects to write in one call.
    ///
    /// Archiving reads, re-encodes and uploads a whole `(measurement, shard)`
    /// group, so an unbounded pass over a large backlog is a long stall on a
    /// small machine. The caller runs the operation again for the next batch.
    pub max_objects_per_run: usize,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            cold_after: Duration::from_secs(30 * 86_400),
            remote_url: String::new(),
            max_objects_per_run: 8,
        }
    }
}

/// What one archival pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveOutcome {
    /// Archive objects uploaded and verified — one per `(measurement, shard)`.
    pub objects: usize,
    /// Source segments dropped from the hot database.
    pub segments: usize,
    /// Rows written to the archive, after deduplication and tombstones.
    pub rows: u64,
    /// Bytes of local segment data dropped.
    pub bytes: u64,
    /// Groups that were eligible but could not be archived, and were left hot.
    ///
    /// Non-zero means the archive is incomplete *and* the data is still hot —
    /// never that data was lost. A failed upload leaves the segments exactly
    /// where they were.
    pub failed: usize,
    /// Whether `max_objects_per_run` stopped this pass with work still
    /// eligible.
    ///
    /// The bound is a rate limit rather than a truncation — the next pass
    /// picks up where this one stopped — but a caller invoking
    /// `archive_cold_segments` directly, rather than through the server's
    /// periodic task, had no way to learn that one call was not the whole
    /// job. `true` means call again.
    pub more_pending: bool,
}

/// One `(measurement, shard)` unit of archiving.
struct Group {
    measurement: String,
    shard_id: ShardId,
    segments: Vec<SegmentCatalogEntry>,
    min_timestamp: i64,
    max_timestamp: i64,
}

impl Chronix {
    /// Archive data older than `config.cold_after` to object storage.
    ///
    /// Groups the catalog into `(measurement, shard)` units, reads each
    /// complete cold group through the read path, writes it as one Parquet
    /// object under `measurement=<m>/shard=<n>/`, verifies the upload, and
    /// only then drops the source segments — in that order, so a crash at any
    /// point leaves either hot data or archived data, never a catalog entry
    /// without a file.
    ///
    /// # Errors
    ///
    /// Returns an error if the object store cannot be reached. Per-group
    /// failures are counted in [`ArchiveOutcome::failed`] rather than aborting
    /// the pass: one unreadable group must not stop the rest.
    pub async fn archive_cold_segments(&self, config: &ArchiveConfig) -> Result<ArchiveOutcome> {
        if config.remote_url.is_empty() {
            return Err(DbError::Internal(
                "archive_cold_segments requires a remote_url".into(),
            ));
        }

        // The archive writes plaintext Parquet into a bucket, so an
        // encrypted column cannot go: the whole point of encrypting it is
        // that the bytes at rest are ciphertext, and a background pass that
        // copies the same values out in the clear — hours later, to a
        // different system — is the opposite. Refused for the database
        // rather than per measurement, because the pass runs unattended and
        // a partial archive is worse than none.
        #[cfg(feature = "field-encryption")]
        if !self.config.field_encryption.is_empty() {
            let declared: Vec<&str> = self
                .config
                .field_encryption
                .columns
                .keys()
                .map(String::as_str)
                .collect();
            self.refuse_encrypted_columns(declared, "the cold archive")?;
        }

        // The wall clock, deliberately uncapped — unlike retention, which
        // measures age from `min(clock, newest timestamp held)` so that one
        // bad reading of the clock cannot empty the database.
        //
        // The rule is: **cap what cannot be undone.** A retention pass
        // deletes; a clock a century ahead therefore destroys everything
        // irreversibly, and the cap is worth what it costs. Archiving
        // *moves* — the rows stay queryable through the archive table — so
        // the worst a wrong clock does here is tier the hot window early,
        // which is slow rather than lost. Capping it would cost more than it
        // buys: a database that stops receiving writes would stop tiering,
        // which is exactly when a gateway wants old data off its SD card.
        let now_ns = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);
        let cutoff =
            now_ns.saturating_sub(i64::try_from(config.cold_after.as_nanos()).unwrap_or(i64::MAX));

        // Leased across encode and upload, because both read these files and
        // the upload can take minutes. Released before `drop_archived`: the
        // pass retires the very segments it leased, and one holding its own
        // lease would defer its own reclamation for ever.
        // See `db::leases`.
        let (groups, read_lease) = self.cold_groups(cutoff, config.max_objects_per_run);
        let more_pending = groups.len() >= config.max_objects_per_run;
        if groups.is_empty() {
            return Ok(ArchiveOutcome::default());
        }

        let remote = ObjectStoreBackend::new(ObjectStoreConfig {
            url: config.remote_url.clone(),
            cache: None,
            multipart_threshold_bytes: 8 * 1024 * 1024,
            max_concurrent_downloads: 4,
            backoff: chronix_engine::objstore::BackoffConfig::default(),
        })
        .await
        .map_err(|e| DbError::Internal(format!("cold archive store: {e}")))?;

        let engine = TieringEngine::new(
            TieringConfig {
                cold_after: config.cold_after,
                remote_url: config.remote_url.clone(),
            },
            remote,
        );

        let mut outcome = ArchiveOutcome::default();
        let mut archived: Vec<Group> = Vec::new();

        for group in groups {
            // Encoding is CPU- and I/O-bound and holds no lock across an
            // await; the upload is network-bound and holds nothing at all.
            let encoded = match self.encode_group(&group) {
                Ok(Some(encoded)) => encoded,
                Ok(None) => {
                    // Every row in the group was deleted. There is nothing to
                    // archive, and the segments are pure tombstone shadow —
                    // dropping them is the whole point of the pass.
                    info!(
                        measurement = %group.measurement,
                        shard = group.shard_id.0,
                        "cold archive: group is empty after tombstones — dropping without an object"
                    );
                    archived.push(group);
                    continue;
                }
                Err(e) => {
                    warn!(
                        measurement = %group.measurement,
                        shard = group.shard_id.0,
                        error = %e,
                        "cold archive: could not encode group, leaving it hot"
                    );
                    outcome.failed += 1;
                    continue;
                }
            };

            let object = match ArchiveObject::new(
                group.measurement.clone(),
                group.shard_id,
                object_name(&group),
            ) {
                Ok(object) => object,
                Err(e) => {
                    warn!(
                        measurement = %group.measurement,
                        error = %e,
                        "cold archive: cannot name an object for this measurement"
                    );
                    outcome.failed += 1;
                    continue;
                }
            };

            match engine.upload_archive(&object, &encoded.bytes).await {
                Ok(key) => {
                    info!(
                        measurement = %group.measurement,
                        shard = group.shard_id.0,
                        object = %key,
                        rows = encoded.rows,
                        "group archived to cold storage"
                    );
                    outcome.rows += encoded.rows;
                }
                Err(e) => {
                    // The segments stay hot and queryable. That is the correct
                    // failure direction for an operation whose next step is a
                    // delete.
                    warn!(
                        measurement = %group.measurement,
                        shard = group.shard_id.0,
                        error = %e,
                        "cold archive: upload failed, leaving the group hot"
                    );
                    outcome.failed += 1;
                    continue;
                }
            }

            outcome.objects += 1;
            archived.push(group);
        }

        drop(read_lease);
        if !archived.is_empty() {
            self.drop_archived(&archived, &mut outcome);
        }

        outcome.more_pending = more_pending;
        metrics::counter!("chronix_cold_archive_objects_total").increment(outcome.objects as u64);
        metrics::counter!("chronix_cold_archive_rows_total").increment(outcome.rows);
        metrics::counter!("chronix_cold_archive_bytes_total").increment(outcome.bytes);
        if outcome.failed > 0 {
            metrics::counter!("chronix_cold_archive_failures_total")
                .increment(outcome.failed as u64);
        }

        Ok(outcome)
    }

    /// Group the catalog into complete, cold `(measurement, shard)` units.
    ///
    /// Holds the catalog read lock for the grouping only, and returns a lease
    /// on every segment it looked at so the encode and upload that follow
    /// cannot have their files unlinked underneath them.
    fn cold_groups(
        &self,
        cutoff: i64,
        max_groups: usize,
    ) -> (Vec<Group>, crate::db::leases::SegmentLease<'_>) {
        let (all, lease) = {
            let catalog = self.catalog.read();
            let all: Vec<SegmentCatalogEntry> = catalog
                .all_segments()
                .into_iter()
                .filter(|e| e.state == SegmentState::Active)
                .cloned()
                .collect();
            let lease = self
                .segment_leases
                .acquire(all.iter().map(|e| e.segment_id));
            (all, lease)
        };

        let mut by_group: BTreeMap<(String, i64), Vec<SegmentCatalogEntry>> = BTreeMap::new();
        for entry in all.iter().cloned() {
            by_group
                .entry((entry.measurement.clone(), entry.shard_id.0))
                .or_default()
                .push(entry);
        }

        let mut groups = Vec::new();
        for ((measurement, shard), segments) in by_group {
            if groups.len() >= max_groups {
                break;
            }
            let Some(min_timestamp) = segments.iter().map(|e| e.min_timestamp).min() else {
                continue;
            };
            let Some(max_timestamp) = segments.iter().map(|e| e.max_timestamp).max() else {
                continue;
            };

            if max_timestamp >= cutoff {
                continue;
            }

            // Completeness. Deduplication is only correct over every segment
            // that covers the range, so a group that shares its range with a
            // segment staying hot cannot be archived: the archive would hold a
            // value the hot tier overrules. Compaction keeps a segment inside
            // one shard, so this normally passes — but "normally" is the shape
            // this tree has been bitten by, so it is checked rather than
            // assumed.
            let overlaps_outsider = all.iter().any(|e| {
                e.measurement == measurement
                    && e.shard_id.0 != shard
                    && e.min_timestamp <= max_timestamp
                    && e.max_timestamp >= min_timestamp
            });
            if overlaps_outsider {
                warn!(
                    measurement = %measurement,
                    shard,
                    "cold archive: a segment outside this shard overlaps its range — keeping it hot"
                );
                metrics::counter!("chronix_cold_archive_groups_incomplete_total").increment(1);
                continue;
            }

            // An unflushed row in range would be archived *and* left hot,
            // which is the duplication this redesign exists to remove.
            if !self
                .shards
                .scan_measurement(&measurement, min_timestamp, max_timestamp)
                .is_empty()
            {
                warn!(
                    measurement = %measurement,
                    shard,
                    "cold archive: unflushed rows overlap this group — keeping it hot"
                );
                metrics::counter!("chronix_cold_archive_groups_incomplete_total").increment(1);
                continue;
            }

            // Archiving removes the data from the hot database, so it is a
            // deletion as far as every rollup fed by that measurement is
            // concerned: a group whose buckets have not been aggregated yet
            // must stay hot until they have. Without this gate a device that
            // was offline long enough for its backlog to be older than
            // `cold_after` had its raw data archived before the tier it was
            // being kept for was ever computed.
            if !self.rollups_materialised_past(&measurement, max_timestamp.saturating_add(1)) {
                warn!(
                    measurement = %measurement,
                    shard,
                    "cold archive: rollups not yet materialised past this group — keeping it hot"
                );
                metrics::counter!("chronix_cold_archive_groups_awaiting_rollup_total").increment(1);
                continue;
            }

            groups.push(Group {
                measurement,
                shard_id: ShardId(shard),
                segments,
                min_timestamp,
                max_timestamp,
            });
        }

        (groups, lease)
    }

    /// Read one group through the read path and encode it as a Parquet object.
    ///
    /// Returns `None` when the group has no surviving rows — every row was
    /// deleted — in which case there is nothing to upload and the segments are
    /// dropped outright.
    fn encode_group(&self, group: &Group) -> Result<Option<Encoded>> {
        let schema = self.archive_schema(&group.measurement)?;

        let plan = self
            .query()
            .measurement(&group.measurement)
            .range(group.min_timestamp, group.max_timestamp)
            .build()?;

        let mut writer = ParquetArchiveWriter::new(schema.clone())
            .map_err(|e| DbError::Internal(format!("cold archive writer: {e}")))?;

        // Tiering to object storage is background work with no caller
        // waiting on it, so `query_timeout` does not apply.
        for batch in self.execute_iter(&plan)?.without_deadline() {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = crate::sql::to_archive_batch(&batch, &schema)
                .map_err(|e| DbError::Internal(format!("cold archive batch: {e}")))?;
            writer
                .write(&batch)
                .map_err(|e| DbError::Internal(format!("cold archive write: {e}")))?;
        }

        let (bytes, rows) = writer
            .finish()
            .map_err(|e| DbError::Internal(format!("cold archive finish: {e}")))?;

        if rows == 0 {
            return Ok(None);
        }
        Ok(Some(Encoded { bytes, rows }))
    }

    /// The Arrow schema an archive object for `measurement` is written with.
    ///
    /// This is the hot SQL schema — `_time` as `Timestamp(Nanosecond)`, then
    /// tags sorted, then fields sorted — **plus** the namespace tag when the
    /// measurement carries one. The hot tier hides that tag because a SQL
    /// session is already scoped to one tenant; an archive is an operator-level
    /// artifact covering every tenant, and dropping the only column that says
    /// whose row this is would be data loss.
    fn archive_schema(&self, measurement: &str) -> Result<SchemaRef> {
        let ms = self.schema(measurement).ok_or_else(|| {
            DbError::Internal(format!(
                "cold archive: no schema for measurement {measurement}"
            ))
        })?;
        Ok(crate::sql::measurement_schema_to_archive_arrow(&ms))
    }

    /// Drop every archived group's segments from the hot database.
    ///
    /// Through `retire_segments`, the one path every pass that removes a
    /// segment takes. Doing it per group inside the upload loop would
    /// interleave lock acquisition with network I/O for no benefit.
    fn drop_archived(&self, archived: &[Group], outcome: &mut ArchiveOutcome) {
        // One retirement per archived group, through the same path retention
        // and compaction take: the rows leave the local database now, the
        // bytes leave once no running scan still holds them.
        for group in archived {
            let retired = self.retire_segments(&group.segments);
            outcome.failed += group.segments.len() - retired.total();
            outcome.segments += retired.total();
            outcome.bytes += retired.bytes_freed;
        }

        // Archiving is a delete from the hot database, so the values derived
        // from what it holds — the cardinality budget, the namespace index,
        // the last-value cache — are repaired here as they are after
        // retention.
        if outcome.segments > 0 {
            self.repair_live_series();
        }
    }
}

/// An encoded archive object, ready to upload.
struct Encoded {
    bytes: Vec<u8>,
    rows: u64,
}

/// Object name for a group: its time range in epoch nanoseconds.
///
/// The range is in the name so the archive is self-describing to somebody
/// listing the bucket, and so two passes over the same shard cannot collide: a
/// group re-formed after a compaction has a different range only if its data
/// changed, and an identical range means an identical object, so re-uploading
/// is idempotent rather than a lost update.
fn object_name(group: &Group) -> String {
    format!(
        "part-{}-{}.parquet",
        group.min_timestamp, group.max_timestamp
    )
}
