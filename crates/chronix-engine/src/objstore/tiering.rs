//! Cold-storage archival.
//!
//! Uploads a segment to object storage, re-encoding it to Parquet on the way
//! so the archive is readable by DuckDB, Polars and Spark rather than only by
//! chronix.
//!
//! # This is an archive, not a transparent tier
//!
//! A cold object has no series bloom and no skip index — Parquet has nowhere
//! to put them — so it is queried as its own named SQL table via
//! `chronix::sql::register_cold_tier`, not unioned into the hot measurement
//!. Archiving therefore *removes* the segment from the hot database:
//! `Chronix::archive_cold_segments` uploads, verifies, and only then drops the
//! catalog entry and the local file, in that order.
//!
//! This engine owns the upload and the verification. It deliberately does not
//! touch the local file: the catalog entry must go first, or a crash between
//! the two leaves the database pointing at a segment that is no longer there.
//! Only the caller holding the catalog can order those two steps correctly,
//! and this module used to delete the file itself — by default — while
//! nothing anywhere updated the catalog.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use chronix_core::{NamespaceId, ShardId};

use crate::objstore::backend::ObjectStoreBackend;
use crate::objstore::error::{ObjStoreError, Result};
use crate::objstore::parquet_tier::{csx_file_to_parquet, ColdFormat};
use crate::storage::StorageBackend;

/// Configuration for the cold-storage tiering policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringConfig {
    /// How old a segment's max timestamp must be (relative to now) before
    /// it is eligible for archival. Only segments whose `max_timestamp`
    /// is older than `now - cold_after` are candidates.
    pub cold_after: Duration,
    /// URL of the remote object store (e.g. `s3://bucket/prefix`).
    pub remote_url: String,
    /// Format cold objects are written in.
    ///
    /// Defaults to [`ColdFormat::Parquet`], which is the whole point of the
    /// cold tier: an archive only chronix can read is a walled garden, and
    /// Spark, DuckDB and Polars reading it directly is worth more than the
    /// encoding win on data nobody queries hot. Set this to
    /// [`ColdFormat::Csx`] to upload verbatim instead, which is smaller and
    /// keeps the segment's blooms and skip index.
    #[serde(default)]
    pub cold_format: ColdFormat,
}

/// Object name for the Parquet re-encoding of a `.csx` segment.
///
/// The extension is swapped rather than appended so the archive reads as an
/// ordinary Parquet dataset to a tool that globs `*.parquet`.
fn parquet_object_name(segment_name: &str) -> String {
    let stem = segment_name.strip_suffix(".csx").unwrap_or(segment_name);
    format!("{stem}.parquet")
}

/// A segment that is a candidate for tiering.
#[derive(Debug, Clone)]
pub struct TieringCandidate {
    /// Namespace (tenant) the segment belongs to.
    pub namespace: NamespaceId,
    /// Shard ID of the segment.
    pub shard_id: ShardId,
    /// Segment file name.
    pub segment_name: String,
    /// Path to the local `.csx` file, as recorded in the catalog.
    pub local_path: std::path::PathBuf,
    /// Maximum timestamp in the segment.
    pub max_timestamp: i64,
    /// Segment size in bytes.
    pub byte_size: u64,
}

/// Result of archiving a single segment.
#[derive(Debug)]
pub struct TieringResult {
    /// The candidate that was archived.
    pub candidate: TieringCandidate,
    /// Object-store path the archive was written to.
    pub object_path: String,
}

/// Cold-storage archiver.
///
/// Uploads segments to a remote object store, re-encoding to Parquet.
/// Operates on individual segments via [`tier_segment`](Self::tier_segment);
/// candidate selection, catalog removal and local deletion belong to the
/// database, which is the only thing that can order them safely.
///
/// # Architecture
///
/// ```text
/// ┌─────────────┐      ┌──────────────────┐
/// │  Local FS    │─────▶│  TieringEngine   │
/// │  (hot data)  │      │  ┌─────────────┐ │
/// └─────────────┘      │  │  policy cfg  │ │
///                      │  └─────────────┘ │
///                      │        │          │
///                      │        ▼          │
///                      │  ┌─────────────┐ │
///                      │  │   upload     │ │
///                      │  └─────────────┘ │
///                      └────────┬─────────┘
///                               │
///                      ┌────────▼─────────┐
///                      │  Object Store    │
///                      │  (cold data)     │
///                      └──────────────────┘
/// ```
pub struct TieringEngine {
    /// Configuration.
    config: TieringConfig,
    /// Remote object store backend (destination).
    remote: ObjectStoreBackend,
}

impl TieringEngine {
    /// Create a new archiver.
    ///
    /// The source is a filesystem path per call, not a `StorageBackend`: the
    /// backend's layout is `ns_<ns>/shard_<n>/<name>` and the database writes
    /// its segments flat under `segments/`, so routing the read through it
    /// meant the archiver could not open a single real segment. The catalog
    /// entry already carries the exact path; the abstraction only stood
    /// between them.
    pub fn new(config: TieringConfig, remote: ObjectStoreBackend) -> Self {
        Self { config, remote }
    }

    /// Returns the tiering configuration.
    #[must_use]
    pub fn config(&self) -> &TieringConfig {
        &self.config
    }

    /// Check if a segment is eligible for cold tiering.
    ///
    /// A segment is eligible if its `max_timestamp` is older than
    /// `now - cold_after`, where both values are in nanoseconds since
    /// epoch (matching segment header timestamp units).
    #[must_use]
    pub fn is_eligible(&self, max_timestamp_ns: i64, now_ns: i64) -> bool {
        let cold_after_ns = i64::try_from(self.config.cold_after.as_nanos()).unwrap_or(i64::MAX);
        let threshold = now_ns.saturating_sub(cold_after_ns);
        max_timestamp_ns < threshold
    }

    /// Tier a single segment from local to remote storage.
    ///
    /// 1. Reads the segment from local storage.
    /// 2. Re-encodes it to the configured archive format.
    /// 3. Uploads it and verifies the object landed at the expected size.
    ///
    /// Returns the object-store path the archive was written to. The local
    /// file is left alone: only the caller holding the catalog can drop the
    /// entry before deleting the file, which is the only crash-safe order.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment cannot be read or uploaded, or if the
    /// uploaded object cannot be verified — an unverified upload must not be
    /// reported as an archive, because the caller's next step is to delete the
    /// original.
    pub async fn tier_segment(&self, candidate: &TieringCandidate) -> Result<String> {
        // 1. Read the segment from wherever the catalog says it is.
        let data = std::fs::read(&candidate.local_path).map_err(ObjStoreError::Io)?;

        // 2. Re-encode for the archive if the policy asks for it.
        //
        // `SegmentReader` mmaps, so `.csx` bytes that are not already a file
        // would need one; here they are, so the source path is used directly.
        // The alternative — a second, bytes-based reader path — is a duplicate
        // implementation of segment decoding, the bug class this tree has paid
        // for most often.
        let object_name = match self.config.cold_format {
            ColdFormat::Csx => candidate.segment_name.clone(),
            ColdFormat::Parquet => parquet_object_name(&candidate.segment_name),
        };
        let upload_path = crate::storage::SegmentPath::new(
            candidate.namespace.clone(),
            candidate.shard_id,
            object_name,
        )
        .map_err(ObjStoreError::Storage)?;

        let data = match self.config.cold_format {
            ColdFormat::Csx => data,
            ColdFormat::Parquet => csx_file_to_parquet(&candidate.local_path)?,
        };

        info!(
            shard = candidate.shard_id.0,
            segment = %candidate.segment_name,
            size = data.len(),
            format = %self.config.cold_format,
            "archiving segment to object storage"
        );

        // 3. Upload.
        self.remote.put_segment(&upload_path, &data).await?;

        metrics::counter!("chronix_objstore_tiering_segments_total").increment(1);
        metrics::counter!("chronix_objstore_tiering_bytes_total").increment(data.len() as u64);

        // 4. Verify the object landed before telling the caller it is safe to
        //    drop the original. A HEAD is enough: it confirms the object
        //    exists and has the expected size, which is what a truncated or
        //    interrupted upload fails.
        //
        //    This *returns an error* rather than logging and carrying on. The
        //    old form logged a warning, skipped its own local deletion, and
        //    reported success — so a caller that removed the catalog entry on
        //    a successful return would drop data that was never archived.
        match self.remote.segment_size(&upload_path).await? {
            Some(remote_size) if remote_size == data.len() => {}
            Some(remote_size) => {
                return Err(ObjStoreError::IntegrityCheckFailed {
                    object: upload_path.to_string(),
                    expected: data.len(),
                    found: Some(remote_size),
                });
            }
            None => {
                return Err(ObjStoreError::IntegrityCheckFailed {
                    object: upload_path.to_string(),
                    expected: data.len(),
                    found: None,
                });
            }
        }

        let object_path = upload_path.to_string();

        debug!(
            shard = candidate.shard_id.0,
            segment = %candidate.segment_name,
            object = %object_path,
            "segment archived"
        );

        Ok(object_path)
    }

    /// Tier a batch of candidates, returning results for each.
    ///
    /// Errors on individual segments are logged but do not abort the batch.
    /// Re-checks eligibility before each tier operation to
    /// prevent races where a segment was modified between the initial
    /// eligibility scan and the actual upload.
    pub async fn tier_batch(&self, candidates: Vec<TieringCandidate>) -> Vec<TieringResult> {
        let mut results = Vec::with_capacity(candidates.len());

        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;

        for candidate in candidates {
            // Re-validate eligibility immediately before
            // tiering to narrow the race window between the batch scan
            // and the actual upload.
            if !self.is_eligible(candidate.max_timestamp, now_ns) {
                debug!(
                    shard = candidate.shard_id.0,
                    segment = %candidate.segment_name,
                    "segment no longer eligible for tiering, skipping"
                );
                continue;
            }

            match self.tier_segment(&candidate).await {
                Ok(object_path) => {
                    results.push(TieringResult {
                        candidate,
                        object_path,
                    });
                }
                Err(e) => {
                    warn!(
                        shard = candidate.shard_id.0,
                        segment = %candidate.segment_name,
                        error = %e,
                        "failed to tier segment"
                    );
                }
            }
        }

        info!(tiered = results.len(), "batch tiering complete");

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// An archiver over an in-memory object store, plus the directory the
    /// source segments are written to.
    async fn setup() -> (TieringEngine, TempDir) {
        let tmp = TempDir::new().unwrap();
        let remote_store = Arc::new(object_store::memory::InMemory::new());
        let remote = ObjectStoreBackend::from_store(remote_store, None);

        let config = TieringConfig {
            cold_format: ColdFormat::Csx,
            cold_after: Duration::from_secs(3600), // 1 hour
            remote_url: "memory://test".to_string(),
        };

        (TieringEngine::new(config, remote), tmp)
    }

    /// Write a source segment and return the candidate that names it.
    fn local_segment(
        dir: &TempDir,
        shard_id: ShardId,
        name: &str,
        data: &[u8],
    ) -> TieringCandidate {
        let path = dir.path().join(name);
        std::fs::write(&path, data).unwrap();
        TieringCandidate {
            namespace: default_ns(),
            shard_id,
            segment_name: name.to_string(),
            local_path: path,
            max_timestamp: 0,
            byte_size: data.len() as u64,
        }
    }

    fn default_ns() -> NamespaceId {
        NamespaceId::default_namespace()
    }

    #[tokio::test]
    async fn is_eligible_works() {
        let (engine, _tmp) = setup().await;

        // cold_after = 1 hour = 3_600_000_000_000 ns
        // now_ns = 10_000_000_000_000, threshold = 6_400_000_000_000
        let now_ns: i64 = 10_000_000_000_000;

        // Segment with max_ts = 5T ns — older than threshold → eligible
        assert!(engine.is_eligible(5_000_000_000_000, now_ns));

        // Segment with max_ts = 7T ns — newer than threshold → not eligible
        assert!(!engine.is_eligible(7_000_000_000_000, now_ns));

        // Exactly at threshold boundary
        assert!(!engine.is_eligible(6_400_000_000_000, now_ns));
    }

    /// The archive must never remove the original: the catalog entry has to
    /// go first, and only the database can do that. This engine deleting the
    /// file itself — which it did, by default, while nothing updated the
    /// catalog — left the database pointing at a segment that was gone.
    #[tokio::test]
    async fn tier_segment_uploads_and_leaves_the_local_copy_alone() {
        let (engine, tmp) = setup().await;
        let shard = ShardId(1);
        let name = "seg_001.csx";

        let candidate = local_segment(&tmp, shard, name, b"segment-data-123");

        let object = engine.tier_segment(&candidate).await.unwrap();
        assert!(object.ends_with(name), "archived object path: {object}");

        assert!(
            candidate.local_path.exists(),
            "the archiver must not delete the segment the catalog still lists"
        );

        let path = crate::storage::SegmentPath::new(default_ns(), shard, name).unwrap();
        let remote_data = engine.remote.get_segment(&path).await.unwrap();
        assert_eq!(remote_data, b"segment-data-123");
    }

    #[tokio::test]
    async fn tier_batch_returns_results() {
        let (engine, tmp) = setup().await;

        let candidates: Vec<_> = (0..3)
            .map(|i| {
                local_segment(
                    &tmp,
                    ShardId(0),
                    &format!("seg_{i:03}.csx"),
                    format!("data-{i}").as_bytes(),
                )
            })
            .collect();

        let results = engine.tier_batch(candidates).await;
        assert_eq!(results.len(), 3);

        for result in &results {
            assert!(
                result.object_path.ends_with(".csx"),
                "archived object path: {}",
                result.object_path
            );
        }
    }

    #[tokio::test]
    async fn tier_batch_skips_failed_segments() {
        let (engine, tmp) = setup().await;

        let candidates = vec![
            local_segment(&tmp, ShardId(0), "exists.csx", b"data"),
            TieringCandidate {
                namespace: default_ns(),
                shard_id: ShardId(0),
                segment_name: "missing.csx".to_string(),
                local_path: tmp.path().join("missing.csx"), // never written
                max_timestamp: 0,
                byte_size: 0,
            },
        ];

        let results = engine.tier_batch(candidates).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].candidate.segment_name, "exists.csx");
    }

    #[test]
    fn tiering_config_serde_roundtrip() {
        let config = TieringConfig {
            cold_format: ColdFormat::Csx,
            cold_after: Duration::from_secs(7200),
            remote_url: "s3://my-bucket/cold".to_string(),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: TieringConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.cold_after, config.cold_after);
        assert_eq!(deserialized.remote_url, config.remote_url);
    }
}

#[cfg(test)]
mod parquet_cold_tier_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use chronix_core::{FieldValue, NamespaceId, Point, SeriesKey, ShardId};

    use super::*;
    use crate::objstore::backend::ObjectStoreBackend;
    use crate::segment::{SegmentWriter, SegmentWriterConfig};
    use crate::storage::{SegmentPath, StorageBackend};

    fn point(host: &str, ts: i64, value: f64) -> Point {
        let mut tags = BTreeMap::new();
        tags.insert("host".to_string(), host.to_string());
        let key = SeriesKey::new("power", tags).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("watts".to_string(), FieldValue::F64(value));
        Point::new(key, fields, ts).unwrap()
    }

    /// A tiered segment must be an ordinary Parquet file that a reader with no
    /// knowledge of chronix can open — that is the entire argument for the cold
    /// tier not being `.csx`.
    #[tokio::test]
    async fn a_tiered_segment_is_readable_as_plain_parquet() {
        let tmp = tempfile::tempdir().unwrap();
        let local_dir = tmp.path().join("local");
        std::fs::create_dir_all(&local_dir).unwrap();

        // Write a real .csx segment through the normal writer.
        let ns = NamespaceId::default_namespace();
        let shard = ShardId(1);
        let seg_name = "seg_cold_0001.csx";
        let staging = tmp.path().join(seg_name);
        {
            let mut writer = SegmentWriter::new(&staging, SegmentWriterConfig::default()).unwrap();
            let points: Vec<Point> = (0..500)
                .map(|i| {
                    point(
                        if i % 2 == 0 { "a" } else { "b" },
                        1_000 + i64::from(i),
                        f64::from(i) * 0.25,
                    )
                })
                .collect();
            writer.write_rows(&points).unwrap();
            writer.finalize().unwrap();
        }
        let csx_bytes = std::fs::read(&staging).unwrap();

        let remote_store = Arc::new(object_store::memory::InMemory::new());
        let remote = ObjectStoreBackend::from_store(remote_store, None);
        let engine = TieringEngine::new(
            TieringConfig {
                cold_format: ColdFormat::Parquet,
                cold_after: Duration::from_secs(0),
                remote_url: "memory://test".to_string(),
            },
            remote,
        );

        let candidate = TieringCandidate {
            namespace: ns.clone(),
            shard_id: shard,
            segment_name: seg_name.to_string(),
            local_path: staging.clone(),
            max_timestamp: 1_000,
            byte_size: csx_bytes.len() as u64,
        };
        engine.tier_segment(&candidate).await.unwrap();

        // The object must be stored under a .parquet name, so an external tool
        // globbing `*.parquet` finds it.
        let parquet_path = SegmentPath::new(ns, shard, "seg_cold_0001.parquet").unwrap();
        let stored = engine.remote.get_segment(&parquet_path).await.unwrap();
        assert_eq!(
            &stored[..4],
            b"PAR1",
            "a cold object must carry the Parquet magic bytes"
        );

        // And it must decode with a stock Parquet reader, values intact.
        let batches = crate::objstore::parquet_tier::parquet_to_batches(stored).unwrap();
        let total: usize = batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum();
        assert_eq!(total, 500, "every row must survive the re-encode");

        let schema = batches[0].schema();
        assert!(
            schema.field_with_name("host").is_ok(),
            "tag columns must survive: {:?}",
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        );
        assert!(
            schema.field_with_name("watts").is_ok(),
            "fields must survive"
        );
    }

    /// The verbatim path must still work, and must not rename the object.
    #[tokio::test]
    async fn csx_cold_format_uploads_verbatim() {
        let tmp = tempfile::tempdir().unwrap();
        let local_dir = tmp.path().join("local");
        std::fs::create_dir_all(&local_dir).unwrap();

        let ns = NamespaceId::default_namespace();
        let shard = ShardId(2);
        let seg_path = SegmentPath::new(ns.clone(), shard, "raw_0001.csx").unwrap();
        let source = local_dir.join("raw_0001.csx");
        std::fs::write(&source, b"not a real segment").unwrap();

        let remote_store = Arc::new(object_store::memory::InMemory::new());
        let remote = ObjectStoreBackend::from_store(remote_store, None);
        let engine = TieringEngine::new(
            TieringConfig {
                cold_format: ColdFormat::Csx,
                cold_after: Duration::from_secs(0),
                remote_url: "memory://test".to_string(),
            },
            remote,
        );

        engine
            .tier_segment(&TieringCandidate {
                namespace: ns.clone(),
                shard_id: shard,
                segment_name: "raw_0001.csx".to_string(),
                local_path: source,
                max_timestamp: 0,
                byte_size: 18,
            })
            .await
            .unwrap();

        let stored = engine.remote.get_segment(&seg_path).await.unwrap();
        assert_eq!(stored, b"not a real segment");
    }

    #[test]
    fn parquet_object_name_swaps_the_extension() {
        assert_eq!(parquet_object_name("seg_0001.csx"), "seg_0001.parquet");
        assert_eq!(parquet_object_name("no_extension"), "no_extension.parquet");
    }
}
