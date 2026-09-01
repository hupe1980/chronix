//! Archiving cold segments to object storage.
//!
//! The write half of the cold tier: segments older than a threshold are
//! re-encoded to Parquet, uploaded, verified, and then **dropped from the hot
//! database**. The read half is [`crate::sql::cold_tier::register_cold_tier`],
//! which exposes the archive as its own SQL table — and DuckDB, Polars and
//! Spark read the same objects directly (D6, D32).
//!
//! # Why archiving removes the segment
//!
//! A cold object is Parquet, which has nowhere to put a series bloom or a tag
//! index. Serving it through the hot read path would mean a query whose time
//! range crosses the boundary quietly changes cost class, with nothing in the
//! plan saying so — so the boundary is explicit: after archiving, the segment
//! lives in the archive table and nowhere else. This is retention with a copy
//! kept (D49).
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
//! println!("archived {} segments, {} bytes", outcome.segments, outcome.bytes);
//! # Ok(())
//! # }
//! ```

use std::time::Duration;

use chronix_core::{NamespaceId, SegmentState};
use chronix_engine::objstore::{
    ColdFormat, ObjectStoreBackend, ObjectStoreConfig, TieringCandidate, TieringConfig,
    TieringEngine,
};
use tracing::{info, warn};

use crate::error::{DbError, Result};
use crate::Chronix;

/// Policy for [`Chronix::archive_cold_segments`].
#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    /// How old a segment's newest point must be before it is archived.
    pub cold_after: Duration,
    /// Object-store URL to archive into (`s3://`, `gs://`, `az://`, `file://`).
    pub remote_url: String,
    /// Archive format. Parquet by default — an archive only chronix can read
    /// is a walled garden.
    pub cold_format: ColdFormat,
    /// Maximum segments to archive in one call.
    ///
    /// Archiving reads, re-encodes and uploads whole segments, so an
    /// unbounded pass over a large backlog is a long stall on a small
    /// machine. The caller runs the operation again for the next batch.
    pub max_segments_per_run: usize,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            cold_after: Duration::from_secs(30 * 86_400),
            remote_url: String::new(),
            cold_format: ColdFormat::Parquet,
            max_segments_per_run: 64,
        }
    }
}

/// What one archival pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveOutcome {
    /// Segments uploaded, verified and dropped from the hot database.
    pub segments: usize,
    /// Bytes of local segment data archived.
    pub bytes: u64,
    /// Eligible segments that could not be archived and were left in place.
    ///
    /// Non-zero means the archive is incomplete *and* the data is still hot —
    /// never that data was lost. A failed upload leaves the segment exactly
    /// where it was.
    pub failed: usize,
}

impl Chronix {
    /// Archive segments older than `config.cold_after` to object storage.
    ///
    /// For each eligible segment: upload it, verify the object landed, drop
    /// the catalog entry, then delete the local file — in that order, so a
    /// crash at any point leaves either a hot segment or an archived one,
    /// never a catalog entry without a file.
    ///
    /// # Errors
    ///
    /// Returns an error if the object store cannot be reached. Per-segment
    /// failures are counted in [`ArchiveOutcome::failed`] rather than
    /// aborting the pass: one unreadable segment must not stop the rest.
    pub async fn archive_cold_segments(&self, config: &ArchiveConfig) -> Result<ArchiveOutcome> {
        if config.remote_url.is_empty() {
            return Err(DbError::Internal(
                "archive_cold_segments requires a remote_url".into(),
            ));
        }

        let now_ns = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);
        let cutoff =
            now_ns.saturating_sub(i64::try_from(config.cold_after.as_nanos()).unwrap_or(i64::MAX));

        // Snapshot the candidates under a read lock, then release it: the
        // uploads are network-bound and must not hold the catalog.
        let candidates: Vec<_> = {
            let catalog = self.catalog.read();
            catalog
                .all_segments()
                .into_iter()
                .filter(|e| e.state == SegmentState::Active && e.max_timestamp < cutoff)
                .take(config.max_segments_per_run)
                .cloned()
                .collect()
        };

        if candidates.is_empty() {
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
                cold_format: config.cold_format,
            },
            remote,
        );

        let mut outcome = ArchiveOutcome::default();
        let mut archived = Vec::new();

        for entry in candidates {
            let Some(name) = entry.path.file_name().and_then(|n| n.to_str()) else {
                warn!(path = %entry.path.display(), "cold archive: unnamed segment path");
                outcome.failed += 1;
                continue;
            };

            let candidate = TieringCandidate {
                namespace: NamespaceId::default_namespace(),
                shard_id: entry.shard_id,
                segment_name: name.to_string(),
                local_path: entry.path.clone(),
                max_timestamp: entry.max_timestamp,
                byte_size: entry.byte_size,
            };

            match engine.tier_segment(&candidate).await {
                Ok(object) => {
                    info!(
                        segment_id = entry.segment_id.0,
                        object = %object,
                        "segment archived to cold storage"
                    );
                }
                Err(e) => {
                    // The segment stays hot and queryable. That is the correct
                    // failure direction for an operation whose next step is a
                    // delete.
                    warn!(
                        segment_id = entry.segment_id.0,
                        error = %e,
                        "cold archive: upload failed, leaving segment hot"
                    );
                    outcome.failed += 1;
                    continue;
                }
            }

            archived.push(entry);
        }

        // Drop every archived segment in one pass, taking the catalog, time
        // index and bloom locks in their documented order. Doing this
        // per segment inside the loop would interleave lock acquisition with
        // network I/O for no benefit.
        if !archived.is_empty() {
            let mut catalog = self.catalog.write();
            let mut time_idx = self.time_index.write();
            let mut blooms = self.blooms.write();

            for entry in &archived {
                // Catalog first, then the file — a crash between them loses a
                // segment that is already in the archive, never one that is
                // not.
                if let Err(e) = catalog.remove_segment(entry.segment_id) {
                    warn!(
                        segment_id = entry.segment_id.0,
                        error = %e,
                        "cold archive: failed to remove catalog entry"
                    );
                    outcome.failed += 1;
                    continue;
                }
                blooms.remove(&entry.segment_id.0);
                if let Some(idx) = time_idx.get_mut(&entry.shard_id) {
                    // The segment may predate this index's rebuild; either way
                    // it is gone now.
                    let _ = idx.remove_segment(entry.segment_id);
                }
                self.tag_index.remove_segment(entry.segment_id);
                self.metadata_cache.remove(entry.segment_id);
                self.segment_cache.invalidate_segment(entry.segment_id);

                for path in [entry.path.clone(), entry.path.with_extension("bloom")] {
                    if let Err(e) = std::fs::remove_file(&path) {
                        if e.kind() != std::io::ErrorKind::NotFound {
                            warn!(path = %path.display(), error = %e, "cold archive: failed to remove local file");
                        }
                    }
                }

                outcome.segments += 1;
                outcome.bytes += entry.byte_size;
            }
        }

        metrics::counter!("chronix_cold_archive_segments_total").increment(outcome.segments as u64);
        metrics::counter!("chronix_cold_archive_bytes_total").increment(outcome.bytes);
        if outcome.failed > 0 {
            metrics::counter!("chronix_cold_archive_failures_total")
                .increment(outcome.failed as u64);
        }

        Ok(outcome)
    }
}
