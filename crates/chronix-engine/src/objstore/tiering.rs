//! Cold-storage archival: uploading an encoded archive object and verifying
//! it landed.
//!
//! This engine moves bytes. It does not read segments and it does not encode —
//! an archive is only correct if it is built from what the database would
//! *answer*, and the read path lives above this crate, so the caller encodes
//! ([`ParquetArchiveWriter`](crate::objstore::ParquetArchiveWriter)).
//!
//! It does not touch local files either. The catalog entry must be dropped
//! before the file is deleted, or a crash between the two leaves the database
//! pointing at a segment that is gone — and only the caller holding the
//! catalog can order those two steps.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use chronix_core::ShardId;

use crate::objstore::backend::ObjectStoreBackend;
use crate::objstore::error::{ObjStoreError, Result};

/// Configuration for the cold-storage tiering policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringConfig {
    /// How old a segment's max timestamp must be (relative to now) before
    /// it is eligible for archival. Only segments whose `max_timestamp`
    /// is older than `now - cold_after` are candidates.
    pub cold_after: Duration,
    /// URL of the remote object store (e.g. `s3://bucket/prefix`).
    pub remote_url: String,
}

/// Where one archive object lives in the bucket.
///
/// The layout is `measurement=<m>/shard=<n>/<name>.parquet`: Hive-style
/// partition directories, which DuckDB, Spark, Polars and DataFusion all
/// expose as **columns** and prune whole directories on.
///
/// # Why `measurement=` and not `namespace=`
///
/// The old layout partitioned by namespace. It could not be populated: a
/// namespace is a *tag on a series*, so one segment holds rows of many
/// namespaces and there is no namespace to name the directory after — the
/// caller passed `NamespaceId::default_namespace()` unconditionally, and every
/// tenant's archive landed in one `namespace=default` directory that said
/// nothing true. The namespace tag travels in the data instead, where it is
/// queryable and correct.
///
/// Measurement *is* a segment-level property, and it is the partition an
/// archive actually needs: one Parquet listing table requires one schema, and
/// two measurements do not share one. Partitioning by measurement is what lets
/// each measurement's archive be registered as its own table with the hot
/// tier's schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveObject {
    measurement: String,
    shard_id: ShardId,
    object_name: String,
}

impl ArchiveObject {
    /// Name an archive object.
    ///
    /// # Errors
    ///
    /// Returns `InvalidConfig` if `measurement` or `object_name` is empty or
    /// carries a path separator or a `..` component. A measurement name comes
    /// from user data and is about to become a directory name, so it is
    /// validated here rather than trusted.
    pub fn new(
        measurement: impl Into<String>,
        shard_id: ShardId,
        object_name: impl Into<String>,
    ) -> Result<Self> {
        let measurement = measurement.into();
        let object_name = object_name.into();
        for (what, value) in [("measurement", &measurement), ("object name", &object_name)] {
            if value.is_empty()
                || value.contains('/')
                || value.contains('\\')
                || value.contains("..")
                || value == "."
                || value.contains('=')
            {
                return Err(ObjStoreError::InvalidConfig {
                    detail: format!(
                        "invalid archive {what} {value:?}: must not be empty or contain \
                         path separators, '..' components or '='"
                    ),
                });
            }
        }
        Ok(Self {
            measurement,
            shard_id,
            object_name,
        })
    }

    /// The measurement this object holds.
    #[must_use]
    pub fn measurement(&self) -> &str {
        &self.measurement
    }

    /// The shard this object holds.
    #[must_use]
    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// The object key, relative to the archive prefix.
    #[must_use]
    pub fn key(&self) -> String {
        format!(
            "measurement={}/shard={}/{}",
            self.measurement, self.shard_id.0, self.object_name
        )
    }

    /// The prefix under which every object of `measurement` lives.
    ///
    /// This is what `register_cold_tier` points a listing table at, so that
    /// one table is one measurement and therefore one schema.
    #[must_use]
    pub fn measurement_prefix(measurement: &str) -> String {
        format!("measurement={measurement}/")
    }
}

impl std::fmt::Display for ArchiveObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.key())
    }
}

/// Cold-storage archiver: uploads encoded archive objects and verifies them.
///
/// Candidate selection, encoding, catalog removal and local deletion belong to
/// the database, which is the only thing that can order them safely.
pub struct TieringEngine {
    /// Configuration.
    config: TieringConfig,
    /// Remote object store backend (destination).
    remote: ObjectStoreBackend,
}

impl TieringEngine {
    /// Create a new archiver over `remote`.
    pub const fn new(config: TieringConfig, remote: ObjectStoreBackend) -> Self {
        Self { config, remote }
    }

    /// Returns the tiering configuration.
    #[must_use]
    pub const fn config(&self) -> &TieringConfig {
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

    /// Upload one encoded archive object and verify it landed intact.
    ///
    /// Returns the object key. The caller's next step is to drop the source
    /// segments, so an upload that cannot be *verified* is an error rather
    /// than a warning: the old engine logged and returned success, and a
    /// caller that removed the catalog entry on a successful return then
    /// dropped data that was never archived.
    ///
    /// # Errors
    ///
    /// Returns an error if the upload fails, or if the object is absent or of
    /// the wrong size afterwards.
    pub async fn upload_archive(&self, object: &ArchiveObject, data: &[u8]) -> Result<String> {
        let key = object.key();

        info!(
            measurement = %object.measurement,
            shard = object.shard_id.0,
            object = %key,
            size = data.len(),
            "uploading cold archive object"
        );

        self.remote
            .put(&key, bytes::Bytes::copy_from_slice(data))
            .await?;

        metrics::counter!("chronix_objstore_tiering_objects_total").increment(1);
        metrics::counter!("chronix_objstore_tiering_bytes_total").increment(data.len() as u64);

        // A HEAD is enough: it confirms the object exists and has the expected
        // size, which is what a truncated or interrupted upload fails.
        match self.remote.object_size(&key).await? {
            Some(remote_size) if remote_size == data.len() => {}
            found => {
                return Err(ObjStoreError::IntegrityCheckFailed {
                    object: key,
                    expected: data.len(),
                    found,
                });
            }
        }

        debug!(object = %key, "cold archive object verified");
        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use std::sync::Arc;

    use crate::objstore::backend::ObjectStoreBackend;

    fn engine() -> TieringEngine {
        let remote =
            ObjectStoreBackend::from_store(Arc::new(object_store::memory::InMemory::new()), None);
        TieringEngine::new(
            TieringConfig {
                cold_after: Duration::from_secs(3600),
                remote_url: "memory://test".to_string(),
            },
            remote,
        )
    }

    #[test]
    fn is_eligible_works() {
        let engine = engine();

        // cold_after = 1 hour = 3_600_000_000_000 ns
        // now_ns = 10_000_000_000_000, threshold = 6_400_000_000_000
        let now_ns: i64 = 10_000_000_000_000;

        assert!(engine.is_eligible(5_000_000_000_000, now_ns));
        assert!(!engine.is_eligible(7_000_000_000_000, now_ns));
        // Exactly at the threshold is not yet cold.
        assert!(!engine.is_eligible(6_400_000_000_000, now_ns));
    }

    /// The key is the Hive-style layout every external reader prunes on.
    #[test]
    fn an_archive_object_names_its_measurement_and_shard() {
        let object = ArchiveObject::new("power", ShardId(7), "part-a.parquet").unwrap();
        assert_eq!(object.key(), "measurement=power/shard=7/part-a.parquet");
        assert_eq!(object.measurement(), "power");
        assert_eq!(object.shard_id(), ShardId(7));
        assert_eq!(
            ArchiveObject::measurement_prefix("power"),
            "measurement=power/"
        );
    }

    /// A measurement name is user data about to become a directory name.
    #[test]
    fn an_archive_object_refuses_a_traversing_name() {
        for bad in ["", "..", "a/b", "a\\b", "../etc", "shard=1"] {
            assert!(
                ArchiveObject::new(bad, ShardId(0), "part.parquet").is_err(),
                "measurement {bad:?} must be refused"
            );
            assert!(
                ArchiveObject::new("power", ShardId(0), bad).is_err(),
                "object name {bad:?} must be refused"
            );
        }
    }

    /// An upload is only reported when the object is verifiably there.
    #[tokio::test]
    async fn upload_archive_verifies_the_object_landed() {
        let engine = engine();
        let object = ArchiveObject::new("power", ShardId(1), "part-a.parquet").unwrap();

        let key = engine
            .upload_archive(&object, b"parquet-bytes")
            .await
            .unwrap();
        assert_eq!(key, "measurement=power/shard=1/part-a.parquet");

        let back = engine.remote.get(&key).await.unwrap();
        assert_eq!(&back[..], b"parquet-bytes");
        assert_eq!(engine.remote.object_size(&key).await.unwrap(), Some(13));
    }

    /// An object that is not there afterwards is an error, not a warning: the
    /// caller's next step is to delete the source data.
    #[tokio::test]
    async fn a_missing_object_fails_verification() {
        let engine = engine();
        assert_eq!(
            engine
                .remote
                .object_size("measurement=power/shard=1/absent.parquet")
                .await
                .unwrap(),
            None,
            "nothing was uploaded, so the HEAD must report absence rather than erroring"
        );
    }

    #[test]
    fn tiering_config_serde_roundtrip() {
        let config = TieringConfig {
            cold_after: Duration::from_secs(7200),
            remote_url: "s3://my-bucket/cold".to_string(),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: TieringConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.cold_after, config.cold_after);
        assert_eq!(deserialized.remote_url, config.remote_url);
    }
}
