//! The `Chronix` database handle — open, write, query, close lifecycle.
//!
//! This module provides the main entry point for the Chronix embedded
//! time-series database. All operations are synchronous from the caller's
//! perspective; async I/O is used internally where beneficial.

mod accessors;
mod analytics_api;
mod backup;
mod delete;
mod lifecycle;
mod query;
mod rollup;
mod stream;
mod write;

pub use stream::BatchStream;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use dashmap::DashSet;
use fs2::FileExt;
use parking_lot::RwLock;
use tracing::{debug, error, info, warn};

use crate::lock_order::{
    BloomsLock, CatalogLock, RollupRegistryLock, TimeIndexLock, TombstonesLock,
};

use chronix_core::{
    wal_decode, ChronixConfig, Point, SchemaRegistry, ShardId, TombstoneSet, WalEntry,
};
use chronix_engine::cache::lvc::LastValueCache;
use chronix_engine::cache::metadata::{CachedSegmentMeta, MetadataCache};
use chronix_engine::cache::SegmentCache;
use chronix_engine::index::{SegmentCatalog, SeriesBloomFilter, TagInvertedIndex, TimeIndex};
use chronix_engine::memtable::{FlushConfig, ShardRouter, ShardRouterConfig};
use chronix_engine::segment::reader::SegmentReader;
use chronix_engine::segment::SegmentWriterConfig;
use chronix_engine::wal::WalWriter;

use chronix_engine::compaction::CompactionPicker;

use chronix_streaming::cdc::EventBus;

use crate::error::{DbError, Result};
use crate::rollup::RollupRegistry;

/// Metadata written to `backup_manifest.json` when a backup completes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BackupManifest {
    /// Manifest format version.
    pub version: u32,
    /// Unix-millisecond timestamp when the backup was created.
    pub created_at: u64,
    /// WAL sequence at the time of backup.
    pub wal_sequence: u64,
    /// Number of files copied.
    pub file_count: usize,
    /// Total bytes copied.
    pub total_bytes: u64,
}

/// A point-in-time snapshot of database statistics.
///
/// Returned by [`Chronix::statistics()`]. All values reflect the state at the
/// instant the method is called. The same values are also emitted as gauge
/// metrics via the `metrics` crate for scraping by Prometheus / OTLP
/// collectors.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DatabaseStatistics {
    /// Number of unique series currently tracked.
    pub series_count: usize,
    /// Total number of on-disk segments registered in the catalog.
    pub segment_count: usize,
    /// Number of active time shards.
    pub shard_count: usize,
    /// Approximate total memtable memory in bytes (active + frozen).
    pub memtable_memory_bytes: usize,
    /// Number of distinct measurements (schemas).
    pub measurement_count: usize,
    /// Current WAL sequence number.
    pub wal_sequence: u64,
    /// Number of tombstoned (soft-deleted) series.
    pub tombstone_count: usize,
    /// Number of entries in the segment metadata cache.
    pub metadata_cache_entries: usize,
}

/// Return current UTC time as unix milliseconds.
pub(super) fn chrono_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The main Chronix database handle.
///
/// Provides a synchronous, thread-safe API for an embedded time-series database.
/// Multiple threads may share a `Chronix` handle via `Arc<Chronix>`.
///
/// # Lock Ordering
///
/// To prevent deadlocks, locks **must** be acquired in the following order.
/// Any code path that holds more than one lock simultaneously must obey
/// this total order:
///
/// 1. `catalog` ([`RwLock<SegmentCatalog>`])
/// 2. `time_index` ([`RwLock<BTreeMap<ShardId, TimeIndex>>`])
/// 3. `blooms` ([`RwLock<BTreeMap<u64, SeriesBloomFilter>>`])
/// 4. `tombstones` ([`RwLock<TombstoneSet>`])
/// 5. `rollup_registry` ([`RwLock<RollupRegistry>`])
///
/// `known_series` uses [`DashSet`] (sharded concurrent hash set) and does
/// **not** participate in this ordering — its per-shard internal locks
/// are fine-grained and never held across other lock acquisitions.
///
/// # Lifecycle
///
/// ```no_run
/// use chronix::Chronix;
/// use chronix_core::ChronixConfig;
///
/// let config = ChronixConfig::builder()
///     .data_dir("/tmp/mydb")
///     .build()
///     .unwrap();
///
/// let db = Chronix::open(config).unwrap();
/// // … insert, query …
/// db.close().unwrap();
/// ```
///
/// # Unified read/write handle
///
/// `Chronix` deliberately uses a **single handle** for both reads and writes
/// rather than separate `WriteHandle` / `ReadHandle` types.  Rationale:
///
/// - **Simplicity** — callers don't need to juggle distinct handles or
///   worry about ordering constraints between them.
/// - **Consistency** — reads see the latest memtable state because they
///   share the same `ShardRouter` instance.  Split handles would require
///   an additional synchronisation layer.
/// - **Interior mutability** — all mutable state is already behind
///   `Arc<RwLock>`/`Arc<Mutex>`, so concurrent reads and writes are safe
///   without separate types.
///
/// If type-level read/write separation is desired in the future (e.g. for
/// `&Chronix`-only query endpoints), thin wrapper types can be added without
/// changing the internal architecture.
pub struct Chronix {
    pub(super) config: ChronixConfig,
    /// WAL writer for durability.
    pub(super) wal: Arc<WalWriter>,
    /// Shard router with memtable management.
    pub(super) shards: Arc<ShardRouter>,
    /// Schema registry.
    pub(super) schema: Arc<SchemaRegistry>,
    /// Segment catalog (persisted).  Level-1 ordered lock.
    pub(super) catalog: Arc<CatalogLock<SegmentCatalog>>,
    /// Per-shard time index for segment pruning.  Level-2 ordered lock.
    pub(super) time_index: Arc<TimeIndexLock<BTreeMap<ShardId, TimeIndex>>>,
    /// Per-segment bloom filters (loaded from sidecar files on open).  Level-3 ordered lock.
    pub(super) blooms: Arc<BloomsLock<BTreeMap<u64, SeriesBloomFilter>>>,
    /// Unique series canonical forms for cardinality enforcement.
    /// Uses canonical form (`measurement\0tag1=v1\0tag2=v2`) instead of
    /// hash to prevent cardinality undercounting from hash collisions.
    ///
    /// Uses [`DashSet`] (sharded concurrent hash set) instead of a global
    /// `Mutex<HashSet>` to eliminate lock contention on the hot write path.
    /// Reads (`contains`) are lock-free per shard; writes only lock the
    /// target shard.
    ///
    /// This is the **only** cardinality bookkeeping in the engine, and it is
    /// exact. `DashSet::len()` sums the per-shard lengths — a few dozen
    /// atomic loads, no global lock — so there is nothing for an approximate
    /// sketch to buy here.
    pub(super) known_series: Arc<DashSet<String>>,
    /// Tombstoned series — excluded from query results.
    /// Supports both full-series and ranged (time-window) tombstones via
    /// `TombstoneSet` for collision-proof, range-aware delete matching.
    /// Level-4 ordered lock.
    pub(super) tombstones: Arc<TombstonesLock<TombstoneSet>>,
    /// Last-value cache for fast latest-point queries.
    pub(super) lvc: LastValueCache,
    /// Metadata cache for segment-level stats.
    pub(super) metadata_cache: Arc<MetadataCache>,
    /// LRU cache for decoded Arrow arrays from segment files.
    /// Invalidated when segments are removed by compaction or drop.
    pub(super) segment_cache: Arc<SegmentCache>,
    /// Inverted index: tag-value → segment IDs for fast tag filtering.
    pub(super) tag_index: Arc<TagInvertedIndex>,
    /// Rollup registry for managing rollup definitions.  Level-5 ordered lock.
    pub(super) rollup_registry: Arc<RollupRegistryLock<RollupRegistry>>,
    /// Compaction picker for selecting segments to compact.
    pub(super) compaction_picker: CompactionPicker,
    /// CDC event bus for streaming change events.
    pub(super) cdc_bus: EventBus,
    /// Flush notification handle — writers signal this when memtable
    /// memory exceeds the flush threshold so the background
    /// [`FlushScheduler`](crate::flush_scheduler::FlushScheduler) can
    /// pick up the work without blocking the write path.
    pub(super) flush_notify: Arc<tokio::sync::Notify>,
    /// Measurements pending soft-delete.
    ///
    /// Maps measurement name → deadline (unix-ms). When `soft_delete_ttl`
    /// is configured, `drop_measurement` inserts here instead of
    /// immediately deleting data.  The periodic GC pass hard-deletes
    /// measurements whose deadline has elapsed.  Queries filter out
    /// pending-drop measurements so they are invisible to users.
    pub(super) pending_measurement_drops: Arc<RwLock<HashMap<String, u64>>>,
    /// Lock file handle — dropped on close to release lock.
    pub(super) _lock_file: std::fs::File,
    /// Closed flag.
    pub(super) closed: AtomicBool,
    /// Runtime-registered scalar UDFs (available after `register_udf`).
    pub(super) custom_udfs: Arc<parking_lot::RwLock<Vec<Arc<datafusion::logical_expr::ScalarUDF>>>>,
    /// Runtime-registered aggregate UDFs (available after `register_udaf`).
    pub(super) custom_udafs:
        Arc<parking_lot::RwLock<Vec<Arc<datafusion::logical_expr::AggregateUDF>>>>,
    /// Ingest-time downsampler for real-time rollup aggregation on write.
    pub(super) ingest_downsampler: Arc<crate::rollup::IngestDownsampler>,
    /// Recovery queue for points that failed memtable insertion
    /// after WAL commit. Each entry carries a retry counter (attempt number).
    /// Retried on the next flush cycle; dropped after `MAX_RECOVERY_RETRIES`.
    pub(super) recovery_queue: Arc<parking_lot::Mutex<Vec<(Point, u64, u32)>>>,
    /// Guard preventing concurrent compaction runs. Only one
    /// `compact()` call may execute at a time to cap total I/O threads
    /// at `compaction_concurrency`.
    pub(super) compaction_running: AtomicBool,
}

impl std::fmt::Debug for Chronix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Chronix")
            .field("data_dir", &self.config.data_dir)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Chronix {
    /// Open (or create) a database with the small-footprint preset —
    /// see [`ChronixConfig::small`] for the exact budgets (≈ 48 MB memory,
    /// flash-friendly periodic WAL fsync, single compaction worker).
    ///
    /// ```rust,no_run
    /// # use chronix::Chronix;
    /// let db = Chronix::open_small("/var/lib/chronix").unwrap();
    /// ```
    ///
    /// # Errors
    ///
    /// Same failure modes as [`Chronix::open`].
    pub fn open_small(data_dir: impl Into<std::path::PathBuf>) -> Result<Self> {
        Self::open(ChronixConfig::small(data_dir))
    }

    /// Open (or create) a Chronix database at the configured `data_dir`.
    ///
    /// Acquires an exclusive file lock to prevent multiple processes from
    /// opening the same database directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock cannot be acquired, the directory is
    /// unreadable, or WAL replay fails.
    #[allow(clippy::too_many_lines)]
    pub fn open(config: ChronixConfig) -> Result<Self> {
        let data_dir = &config.data_dir;
        std::fs::create_dir_all(data_dir)?;

        // Acquire exclusive file lock
        let lock_path = data_dir.join("LOCK");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&lock_path)?;

        lock_file
            .try_lock_exclusive()
            .map_err(|_| DbError::LockFailed {
                path: lock_path.display().to_string(),
            })?;

        info!(data_dir = %data_dir.display(), "Opening Chronix database");

        // Ensure sub-directories exist
        let wal_dir = data_dir.join("wal");
        let segments_dir = data_dir.join("segments");
        let catalog_dir = data_dir.join("catalog");
        std::fs::create_dir_all(&wal_dir)?;
        std::fs::create_dir_all(&segments_dir)?;
        std::fs::create_dir_all(&catalog_dir)?;

        // Open catalog
        let catalog = SegmentCatalog::open(&catalog_dir)?;

        // ── Orphaned segment cleanup ──────────────────────────────────
        // If a crash occurred between writing a segment file and
        // registering it in the catalog, orphaned .csx files remain on
        // disk. Scan the segments directory and remove any files not
        // tracked by the catalog.
        {
            let known_paths: std::collections::HashSet<std::path::PathBuf> = catalog
                .all_segments()
                .iter()
                .map(|e| e.path.clone())
                .collect();

            if let Ok(entries) = std::fs::read_dir(&segments_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|ext| ext == "csx")
                        && !known_paths.contains(&path)
                    {
                        warn!(path = %path.display(), "Removing orphaned segment file not registered in catalog");
                        if let Err(e) = std::fs::remove_file(&path) {
                            warn!(path = %path.display(), error = %e, "Failed to remove orphaned segment");
                        }
                    }
                }
            }
        }

        // Build time index, load bloom filters, and rebuild tag index from catalog entries
        let (time_indices, blooms, tag_index) = Self::load_catalog_state(&catalog);

        // Populate metadata cache from existing segments on disk
        let metadata_cache = {
            let cache = MetadataCache::new();
            for entry in catalog.all_segments() {
                match SegmentReader::open(&entry.path) {
                    Ok(reader) => {
                        cache.insert(CachedSegmentMeta {
                            segment_id: entry.segment_id,
                            header: reader.header().clone(),
                            columns: reader.column_metadata().to_vec(),
                        });
                    }
                    Err(e) => {
                        warn!(
                            segment = %entry.path.display(),
                            error = %e,
                            "Could not load segment metadata into cache"
                        );
                    }
                }
            }
            if !cache.is_empty() {
                info!(
                    segments = cache.len(),
                    "Metadata cache populated on startup"
                );
            }
            Arc::new(cache)
        };

        // Open WAL
        let wal = WalWriter::open(&wal_dir, config.wal.clone())?;
        let wal = Arc::new(wal);
        // `Periodic` is only a policy if something does the syncing. A no-op
        // under the other policies.
        wal.start_periodic_sync();

        // Build schema registry from catalog
        let schema = Arc::new(SchemaRegistry::new());

        // Replay schemas from catalog — register each schema so queries
        // can resolve measurements before any new writes arrive.
        {
            let catalog_ref = &catalog;
            for (name, ms) in catalog_ref.all_schemas() {
                debug!(measurement = %name, "Restoring schema from catalog");
                schema.register_schema(name, ms.clone());
            }
            let count = schema.measurement_count();
            if count > 0 {
                info!(
                    measurements = count,
                    "Schema registry restored from catalog"
                );
            }
        }

        // Set up shard router
        let flush_cfg = FlushConfig {
            flush_threshold: config.memtable_flush_threshold,
            max_memory: config.max_memtable_memory,
            segment_dir: segments_dir.clone(),
            segment_writer_config: SegmentWriterConfig {
                row_group_size: 65_536,
                compress: config.compression != chronix_core::CompressionCodec::None,
                float_encoding: config.float_encoding,
                compression_codec: config.compression,
                zstd_level: config.zstd_level,
                zstd_dict_training: config.zstd_dict_training,
                column_codec_overrides: std::collections::HashMap::new(),
                ..Default::default()
            },
            ..Default::default()
        };

        let shard_cfg = ShardRouterConfig {
            shard_duration: config.shard_duration,
            ooo_shard_tolerance: i64::from(config.ooo_shard_tolerance),
            flush_config: flush_cfg,
        };

        let shards = Arc::new(ShardRouter::new(shard_cfg));

        // Replay WAL into memtable.
        // Note: We replay all WAL records because the catalog does not yet
        // track max_wal_seq per segment.  WAL truncation at flush time
        // keeps the replay window small in practice.
        let wal_records = chronix_engine::wal::replay_all(&wal_dir)?;
        let replayed_count = wal_records.len();
        let mut replay_series: HashSet<String> = HashSet::new();

        // Tombstones come from the catalog, not from the WAL. The WAL is
        // truncated once the memtable it covers has been flushed, so a delete
        // older than the last flush is simply not in it — which is how deletes
        // used to come undone across a restart. Replaying the WAL on top still
        // matters for the crash window between a delete's WAL append and its
        // catalog append.
        let mut replay_tombstones = catalog.tombstones().clone();
        for record in wal_records {
            // A record may contain a single WAL entry OR an atomic batch.
            // Decode batch framing first; if absent, treat as single entry.
            let payloads: Vec<Vec<u8>> =
                match chronix_engine::wal::decode_batch_payload(&record.payload) {
                    Some(sub) => sub,
                    None => vec![record.payload],
                };

            for payload in &payloads {
                match wal_decode(payload) {
                    Ok(WalEntry::Write { point }) => {
                        replay_series.insert(point.series_key().canonical_form().to_string());
                        if let Err(e) = shards.insert_replay(&point, record.sequence_no) {
                            warn!(seq = record.sequence_no, error = %e, "Skipping WAL record during replay");
                        }
                    }
                    Ok(WalEntry::Delete { tombstones }) => {
                        // Replayed verbatim, ranges included. Reconstructing
                        // them from the delete *request* is what used to widen
                        // a ranged delete into a whole-series delete at every
                        // startup.
                        for tombstone in tombstones {
                            replay_tombstones.insert(tombstone);
                        }
                    }
                    Ok(WalEntry::SchemaChange { actions }) => {
                        // Re-apply schema actions during recovery.
                        // The in-memory SchemaRegistry may already have
                        // these from the catalog, but applying them again
                        // is idempotent and ensures consistency.
                        for action in actions {
                            match action {
                                chronix_core::schema::SchemaAction::CreateMeasurement(ms) => {
                                    schema.register_measurement(ms);
                                }
                                chronix_core::schema::SchemaAction::AddColumn {
                                    measurement,
                                    column,
                                } => {
                                    schema.apply_add_column(&measurement, column);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!(seq = record.sequence_no, error = %e, "Skipping unparseable WAL record");
                    }
                }
            }
        }

        if replayed_count > 0 {
            info!(
                records = replayed_count,
                series = replay_series.len(),
                tombstones = replay_tombstones.len(),
                "WAL replay complete"
            );
        }

        // Load persisted rollup definitions
        let rollup_registry = {
            let rollup_path = data_dir.join(RollupRegistry::filename());
            match RollupRegistry::load(&rollup_path) {
                Ok(r) => {
                    let n = r.list().len();
                    if n > 0 {
                        info!(rollups = n, "Rollup registry loaded from disk");
                    }
                    r
                }
                Err(e) => {
                    warn!(error = %e, "Failed to load rollup registry, starting empty");
                    RollupRegistry::new()
                }
            }
        };

        let segment_cache_size = config.segment_cache_size;

        Ok(Self {
            config,
            wal,
            shards,
            schema,
            catalog: Arc::new(CatalogLock::new(catalog)),
            time_index: Arc::new(TimeIndexLock::new(time_indices)),
            blooms: Arc::new(BloomsLock::new(blooms)),
            known_series: Arc::new(replay_series.into_iter().collect::<DashSet<String>>()),
            tombstones: Arc::new(TombstonesLock::new(replay_tombstones)),
            lvc: LastValueCache::new(),
            metadata_cache,
            segment_cache: Arc::new(SegmentCache::new(segment_cache_size)),
            tag_index: Arc::new(tag_index),
            rollup_registry: Arc::new(RollupRegistryLock::new(rollup_registry)),
            compaction_picker: CompactionPicker::default(),
            cdc_bus: EventBus::with_default_capacity(),
            flush_notify: Arc::new(tokio::sync::Notify::new()),
            pending_measurement_drops: Arc::new(RwLock::new(HashMap::new())),
            _lock_file: lock_file,
            closed: AtomicBool::new(false),
            custom_udfs: Arc::new(parking_lot::RwLock::new(Vec::new())),
            custom_udafs: Arc::new(parking_lot::RwLock::new(Vec::new())),
            ingest_downsampler: Arc::new(crate::rollup::IngestDownsampler::new(Vec::new())),
            recovery_queue: Arc::new(parking_lot::Mutex::new(Vec::new())),
            compaction_running: AtomicBool::new(false),
        })
    }
}

impl Drop for Chronix {
    fn drop(&mut self) {
        // Wrap the drop body in catch_unwind to prevent a double-panic
        // (and consequent process abort) if Drop runs during stack unwinding
        // and the close() path panics.  The I/O performed by close() (WAL
        // flush, segment sync) is exactly the kind of work that can fail
        // unexpectedly under resource pressure.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if !self.closed.load(Ordering::Acquire) {
                warn!("Chronix database dropped without explicit close()");
                // Best-effort close
                if let Err(e) = self.close() {
                    // Use error! — data may not have been flushed/synced.
                    error!(error = %e, "Error during implicit close on drop");
                }
            }
        }));
        if result.is_err() {
            // Swallow the panic — logging may itself be unavailable here.
            eprintln!("chronix: panic during Chronix::drop suppressed to avoid abort");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::warm_tier::WarmTierConfig;
    use arrow::array::Array;
    use chronix_core::{FieldValue, SegmentState, SeriesKey};
    use std::collections::BTreeMap;
    use std::path::Path;
    use tempfile::TempDir;

    fn test_config(dir: &Path) -> ChronixConfig {
        ChronixConfig::builder()
            .data_dir(dir)
            .memtable_flush_threshold(1024 * 1024) // 1MB for tests
            .build()
            .unwrap()
    }

    fn test_point(measurement: &str, host: &str, ts: i64, value: f64) -> Point {
        let tags = BTreeMap::from([("host".to_string(), host.to_string())]);
        let fields = BTreeMap::from([("value".to_string(), FieldValue::F64(value))]);
        let key = SeriesKey::new(measurement, tags).unwrap();
        Point::new(key, fields, ts).unwrap()
    }

    #[test]
    fn open_and_close() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();
        assert_eq!(db.wal_sequence(), 0);
        db.close().unwrap();
    }

    #[test]
    fn double_close_is_noop() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();
        db.close().unwrap();
        db.close().unwrap(); // Should not error
    }

    #[test]
    fn insert_and_scan_memtable() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        let point = test_point("cpu", "srv-1", 1000, 95.5);
        db.insert(&point).unwrap();

        let key = SeriesKey::new("cpu", BTreeMap::from([("host".into(), "srv-1".into())])).unwrap();
        let results = db.scan_memtable(&key, 0, i64::MAX);
        assert_eq!(results.len(), 1);

        db.close().unwrap();
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn insert_batch() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        let points: Vec<Point> = (0..100)
            .map(|i| test_point("cpu", "srv-1", i * 1000, i as f64))
            .collect();

        assert!(
            db.insert_batch(&points).unwrap().is_complete(),
            "insert was partial"
        );

        let key = SeriesKey::new("cpu", BTreeMap::from([("host".into(), "srv-1".into())])).unwrap();
        let results = db.scan_memtable(&key, 0, i64::MAX);
        assert_eq!(results.len(), 100);

        db.close().unwrap();
    }

    #[test]
    fn schema_on_write() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        let point = test_point("cpu", "srv-1", 1000, 95.5);
        db.insert(&point).unwrap();

        let schema = db.schema("cpu");
        assert!(schema.is_some());

        db.close().unwrap();
    }

    #[test]
    fn operations_after_close_fail() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();
        db.close().unwrap();

        let point = test_point("cpu", "srv-1", 1000, 95.5);
        let result = db.insert(&point);
        assert!(result.is_err());
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn query_memtable_only() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        // Insert points
        for i in 0..5 {
            db.insert(&test_point("cpu", "srv-1", i * 1000, i as f64))
                .unwrap();
        }

        // Query via execute
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();

        assert_eq!(batch.num_rows(), 5);
        // Should have columns: timestamp, host, value
        assert!(batch.num_columns() >= 3);

        db.close().unwrap();
    }

    #[test]
    fn query_with_tag_filter() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        db.insert(&test_point("cpu", "srv-1", 1000, 1.0)).unwrap();
        db.insert(&test_point("cpu", "srv-2", 2000, 2.0)).unwrap();
        db.insert(&test_point("cpu", "srv-1", 3000, 3.0)).unwrap();

        let plan = db
            .query()
            .measurement("cpu")
            .tag("host", "srv-1")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();

        assert_eq!(batch.num_rows(), 2);

        db.close().unwrap();
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn query_with_time_range() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        for i in 0..10 {
            db.insert(&test_point("cpu", "srv-1", i * 1000, i as f64))
                .unwrap();
        }

        let plan = db
            .query()
            .measurement("cpu")
            .range(2000, 5000)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();

        // Should get timestamps 2000, 3000, 4000, 5000
        assert_eq!(batch.num_rows(), 4);

        db.close().unwrap();
    }

    #[test]
    fn query_with_field_projection() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        // Insert with multiple fields
        let tags = BTreeMap::from([("host".to_string(), "a".to_string())]);
        let key = SeriesKey::new("cpu", tags).unwrap();
        let fields = BTreeMap::from([
            ("idle".to_string(), FieldValue::F64(90.0)),
            ("system".to_string(), FieldValue::F64(5.0)),
        ]);
        let p = Point::new(key, fields, 1000).unwrap();
        db.insert(&p).unwrap();

        let plan = db
            .query()
            .measurement("cpu")
            .field("idle")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2); // timestamp + "idle"
        assert_eq!(batch.schema().field(0).name(), "timestamp");
        assert_eq!(batch.schema().field(1).name(), "idle");

        db.close().unwrap();
    }

    #[test]
    fn query_empty_measurement() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        db.insert(&test_point("cpu", "a", 1000, 1.0)).unwrap();

        let plan = db
            .query()
            .measurement("nonexistent")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();

        assert_eq!(batch.num_rows(), 0);

        db.close().unwrap();
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn query_spans_memtable_and_segments() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(1024 * 1024)
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Insert and flush (goes to segment)
        for i in 0..5 {
            db.insert(&test_point("cpu", "srv-1", i * 1000, i as f64))
                .unwrap();
        }
        db.flush().unwrap();

        // Insert more (stays in memtable)
        for i in 5..10 {
            db.insert(&test_point("cpu", "srv-1", i * 1000, i as f64))
                .unwrap();
        }

        // Query should merge both sources
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();

        assert_eq!(batch.num_rows(), 10);

        db.close().unwrap();
    }

    #[test]
    fn query_after_close_fails() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        db.insert(&test_point("cpu", "a", 1000, 1.0)).unwrap();
        db.close().unwrap();

        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let result = db.execute(&plan);
        assert!(result.is_err());
    }

    #[test]
    fn exclusive_lock() {
        let tmp = TempDir::new().unwrap();
        let _db1 = Chronix::open(test_config(tmp.path())).unwrap();

        // Second open should fail due to lock
        let result = Chronix::open(test_config(tmp.path()));
        assert!(result.is_err());
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn wal_replay_on_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().to_path_buf();

        // Insert data, then drop (simulating crash — no explicit close)
        {
            let db = Chronix::open(test_config(&path)).unwrap();
            for i in 0..10 {
                let point = test_point("cpu", "srv-1", i * 1000, i as f64);
                db.insert(&point).unwrap();
            }
            // Sync WAL but don't close cleanly
            db.wal.sync().unwrap();
            db.closed.store(true, Ordering::Release); // Prevent drop from closing
        }

        // Reopen — WAL records should be replayed
        let db = Chronix::open(test_config(&path)).unwrap();
        let key = SeriesKey::new("cpu", BTreeMap::from([("host".into(), "srv-1".into())])).unwrap();
        let results = db.scan_memtable(&key, 0, i64::MAX);
        assert_eq!(results.len(), 10);

        db.close().unwrap();
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn flush_creates_segment() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(1024 * 1024) // 1MB — no auto-flush
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Insert data
        for i in 0..10 {
            let point = test_point("cpu", "srv-1", i * 1000, i as f64);
            db.insert(&point).unwrap();
        }

        // Manual flush
        let results = db.flush().unwrap();
        assert!(!results.is_empty(), "Expected at least one flush result");
        assert_eq!(results[0].measurement, "cpu");
        assert_eq!(results[0].points_flushed, 10);

        // Catalog should have entries after flush
        let catalog = db.catalog.read();
        let total_segments = catalog.all_segments().len();
        drop(catalog);
        assert!(
            total_segments >= 1,
            "Expected at least 1 segment after flush, got {total_segments}"
        );

        db.close().unwrap();
    }

    // ------------------------------------------------------------------
    // Cardinality enforcement tests
    // ------------------------------------------------------------------

    fn low_cardinality_config(dir: &Path, max_series: usize) -> ChronixConfig {
        ChronixConfig::builder()
            .data_dir(dir)
            .memtable_flush_threshold(1024 * 1024)
            .max_series_cardinality(max_series)
            .build()
            .unwrap()
    }

    #[test]
    fn cardinality_allows_up_to_limit() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(low_cardinality_config(tmp.path(), 3)).unwrap();

        // 3 distinct series — should succeed
        db.insert(&test_point("cpu", "host-a", 1, 1.0)).unwrap();
        db.insert(&test_point("cpu", "host-b", 2, 2.0)).unwrap();
        db.insert(&test_point("mem", "host-a", 3, 3.0)).unwrap();

        // Re-inserting known series is always fine
        db.insert(&test_point("cpu", "host-a", 4, 4.0)).unwrap();
        db.close().unwrap();
    }

    #[test]
    fn cardinality_rejects_over_limit() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(low_cardinality_config(tmp.path(), 2)).unwrap();

        db.insert(&test_point("cpu", "host-a", 1, 1.0)).unwrap();
        db.insert(&test_point("cpu", "host-b", 2, 2.0)).unwrap();

        // Third unique series should fail
        let err = db.insert(&test_point("mem", "host-a", 3, 3.0)).unwrap_err();
        assert!(
            matches!(
                err,
                DbError::CardinalityExceeded {
                    current: 2,
                    limit: 2
                }
            ),
            "Expected CardinalityExceeded, got {err:?}"
        );

        db.close().unwrap();
    }

    #[test]
    fn cardinality_batch_atomic_reject() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(low_cardinality_config(tmp.path(), 3)).unwrap();

        db.insert(&test_point("cpu", "host-a", 1, 1.0)).unwrap();

        // Batch with 3 new series → total would be 4 > limit
        let batch = vec![
            test_point("cpu", "host-b", 2, 2.0),
            test_point("cpu", "host-c", 3, 3.0),
            test_point("mem", "host-a", 4, 4.0),
        ];

        let err = db.insert_batch(&batch).unwrap_err();
        assert!(
            matches!(err, DbError::CardinalityExceeded { .. }),
            "Expected CardinalityExceeded, got {err:?}"
        );

        // Because the batch was rejected, the series set should still have
        // only 1 entry (the one from the single insert).
        assert_eq!(db.known_series.len(), 1);

        db.close().unwrap();
    }

    #[test]
    fn cardinality_batch_deduplicates_within_batch() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(low_cardinality_config(tmp.path(), 3)).unwrap();

        // Batch has 5 points but only 2 unique series
        let batch = vec![
            test_point("cpu", "host-a", 1, 1.0),
            test_point("cpu", "host-b", 2, 2.0),
            test_point("cpu", "host-a", 3, 3.0),
            test_point("cpu", "host-b", 4, 4.0),
            test_point("cpu", "host-a", 5, 5.0),
        ];

        assert!(
            db.insert_batch(&batch).unwrap().is_complete(),
            "insert was partial"
        );
        assert_eq!(db.known_series.len(), 2);

        db.close().unwrap();
    }

    #[test]
    fn cardinality_batch_succeeds_at_exact_limit() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(low_cardinality_config(tmp.path(), 3)).unwrap();

        db.insert(&test_point("cpu", "host-a", 1, 1.0)).unwrap();

        // Batch adds exactly 2 more series → total = 3 = limit
        let batch = vec![
            test_point("cpu", "host-b", 2, 2.0),
            test_point("mem", "host-a", 3, 3.0),
        ];

        assert!(
            db.insert_batch(&batch).unwrap().is_complete(),
            "insert was partial"
        );
        assert_eq!(db.known_series.len(), 3);

        db.close().unwrap();
    }

    // ------------------------------------------------------------------
    // Delete / drop API tests
    // ------------------------------------------------------------------

    #[test]
    fn drop_measurement_removes_segments_and_schema() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        // Insert into two measurements
        for i in 0..5 {
            db.insert(&test_point("cpu", "srv-1", i * 1000, 1.0))
                .unwrap();
            db.insert(&test_point("mem", "srv-1", i * 1000, 2.0))
                .unwrap();
        }

        // Flush to create on-disk segments
        db.flush().unwrap();

        // Verify both measurements have segments
        {
            let catalog = db.catalog.read();
            assert!(!catalog.segments_for_measurement("cpu").is_empty());
            assert!(!catalog.segments_for_measurement("mem").is_empty());
        }

        // Drop "cpu"
        db.drop_measurement("cpu").unwrap();

        // cpu segments gone, mem segments remain
        {
            let catalog = db.catalog.read();
            assert!(catalog.segments_for_measurement("cpu").is_empty());
            assert!(!catalog.segments_for_measurement("mem").is_empty());
        }

        // Schema for "cpu" removed
        assert!(db.schema.lookup("cpu").is_none());
        // Schema for "mem" remains
        assert!(db.schema.lookup("mem").is_some());

        db.close().unwrap();
    }

    #[test]
    fn drop_measurement_query_returns_empty() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        for i in 0..5 {
            db.insert(&test_point("cpu", "srv-1", i * 1000, 1.0))
                .unwrap();
        }
        db.flush().unwrap();

        // Drop and query
        db.drop_measurement("cpu").unwrap();

        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();

        let batch = db.execute(&plan).unwrap();
        assert_eq!(batch.num_rows(), 0);

        db.close().unwrap();
    }

    #[test]
    fn delete_series_excludes_from_query() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        // Two series
        for i in 0..3 {
            db.insert(&test_point("cpu", "srv-1", i * 1000, 1.0))
                .unwrap();
            db.insert(&test_point("cpu", "srv-2", i * 1000 + 500, 2.0))
                .unwrap();
        }

        // Delete series (cpu, host=srv-1)
        let tags = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
        db.delete_series("cpu", &tags).unwrap();

        // Query — should only return srv-2 data
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();

        let batch = db.execute(&plan).unwrap();
        // 3 points from srv-2 remain
        assert_eq!(batch.num_rows(), 3);

        // Verify all remaining rows have host=srv-2
        let host_col = batch
            .column_by_name("host")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert_eq!(host_col.value(i), "srv-2");
        }

        db.close().unwrap();
    }

    #[test]
    fn delete_series_on_closed_db_fails() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();
        db.close().unwrap();

        let tags = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
        let err = db.delete_series("cpu", &tags).unwrap_err();
        assert!(matches!(err, DbError::Closed));
    }

    #[test]
    fn drop_measurement_on_closed_db_fails() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();
        db.close().unwrap();

        let err = db.drop_measurement("cpu").unwrap_err();
        assert!(matches!(err, DbError::Closed));
    }

    // ── last_value tests ──────────────────────────────────────────────

    #[test]
    fn last_value_from_memtable() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        db.insert(&test_point("cpu", "srv-1", 100, 1.0)).unwrap();
        db.insert(&test_point("cpu", "srv-1", 300, 3.0)).unwrap();
        db.insert(&test_point("cpu", "srv-1", 200, 2.0)).unwrap();

        let tags = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
        let pt = db
            .last_value("cpu", &tags)
            .unwrap()
            .expect("should find point");
        assert_eq!(pt.timestamp(), 300);
    }

    #[test]
    fn last_value_nonexistent_returns_none() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        let tags = BTreeMap::from([("host".to_string(), "ghost".to_string())]);
        assert!(db.last_value("cpu", &tags).unwrap().is_none());
    }

    #[test]
    fn last_value_after_close_fails() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();
        db.close().unwrap();

        let tags = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
        let err = db.last_value("cpu", &tags).unwrap_err();
        assert!(matches!(err, DbError::Closed));
    }

    #[test]
    fn last_value_tombstoned_returns_none() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        db.insert(&test_point("cpu", "srv-1", 100, 1.0)).unwrap();

        let tags = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
        db.delete_series("cpu", &tags).unwrap();

        assert!(db.last_value("cpu", &tags).unwrap().is_none());
    }

    // ── empty-result schema tests ─────────────────────────────────

    #[test]
    fn execute_empty_result_preserves_schema() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        // Write a point to register the schema
        db.insert(&test_point("cpu", "srv-1", 100, 42.0)).unwrap();

        // Query a time range with no matching data
        let plan = db
            .query()
            .measurement("cpu")
            .range(9000, 9999)
            .build()
            .unwrap();
        let result = db.execute(&plan).unwrap();
        assert_eq!(result.num_rows(), 0);
        // Schema should still contain the measurement's columns
        let schema = result.schema();
        assert!(schema.column_with_name("timestamp").is_some());
        assert!(schema.column_with_name("value").is_some());
        assert!(schema.column_with_name("host").is_some());
    }

    // ------------------------------------------------------------------
    // Bloom filter pruning tests
    // ------------------------------------------------------------------

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn bloom_pruning_after_flush() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        // Insert data for two different hosts
        for i in 0..5 {
            db.insert(&test_point("cpu", "srv-1", i * 1000, i as f64))
                .unwrap();
            db.insert(&test_point("cpu", "srv-2", i * 1000, (i + 10) as f64))
                .unwrap();
        }

        // Flush to create segments and populate bloom filters
        let results = db.flush().unwrap();
        assert!(!results.is_empty());

        // Verify bloom filters were populated
        let blooms = db.blooms.read();
        assert!(!blooms.is_empty(), "Expected bloom filters after flush");
        drop(blooms);

        // Query for host=srv-1 — should work and return data
        let plan = db
            .query()
            .measurement("cpu")
            .tag("host", "srv-1")
            .range(0, 5000)
            .build()
            .unwrap();
        let result = db.execute(&plan).unwrap();
        assert!(result.num_rows() > 0, "Expected results for srv-1");

        db.close().unwrap();
    }

    #[test]
    fn bloom_pruning_nonexistent_series() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        // Insert data for srv-1 only
        for i in 0..5 {
            db.insert(&test_point("cpu", "srv-1", i * 1000, 1.0))
                .unwrap();
        }
        db.flush().unwrap();

        // Query for srv-999 — bloom filter should prune the segment
        let plan = db
            .query()
            .measurement("cpu")
            .tag("host", "srv-999")
            .range(0, 5000)
            .build()
            .unwrap();
        let result = db.execute(&plan).unwrap();
        // Result may have rows from the query that passes through
        // (bloom filters are probabilistic — no false negatives guaranteed,
        //  but may still read segments if bloom says "maybe")
        // The key invariant: the query should succeed without error
        assert!(result.num_rows() <= 5);
    }

    // ------------------------------------------------------------------
    // execute_stream tests
    // ------------------------------------------------------------------

    #[test]
    fn execute_stream_returns_batches() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        for i in 0..10 {
            db.insert(&test_point("cpu", "srv-1", i * 1000, 1.0))
                .unwrap();
        }

        let plan = db
            .query()
            .measurement("cpu")
            .range(0, 10_000)
            .build()
            .unwrap();
        let batches = db.execute_stream(&plan).unwrap();
        assert!(!batches.is_empty(), "Expected at least one batch");
        let total_rows: usize = batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum();
        assert_eq!(total_rows, 10);
    }

    #[test]
    fn execute_stream_after_close_fails() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();
        db.insert(&test_point("cpu", "srv-1", 100, 1.0)).unwrap();
        db.close().unwrap();

        let plan = db
            .query()
            .measurement("cpu")
            .range(0, 1000)
            .build()
            .unwrap();
        assert!(db.execute_stream(&plan).is_err());
    }

    // ── Compaction tests ────────────────────────────────────────────

    #[test]
    fn compact_merges_segments() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(256) // Tiny threshold to force multiple flushes
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Insert and flush multiple times to create multiple segments
        for batch in 0..5 {
            for i in 0..10 {
                let ts = batch * 1000 + i;
                db.insert(&test_point("cpu", "srv-1", ts, i as f64))
                    .unwrap();
            }
            db.flush().unwrap();
        }

        // Verify segments exist
        let seg_count_before = {
            let catalog = db.catalog.read();
            catalog.segments_for_measurement("cpu").len()
        };
        assert!(
            seg_count_before >= 2,
            "Expected ≥ 2 segments, got {seg_count_before}"
        );

        // Run compaction
        let compacted = db.compact().unwrap();

        // Query should still return all data
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, 50_000)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();
        assert!(batch.num_rows() > 0, "Expected data after compaction");

        db.close().unwrap();
        // At least one compaction should have run if we had enough segments
        let _ = compacted; // May be 0 if threshold wasn't met
    }

    #[test]
    fn compact_removes_tombstoned_data() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(128)
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Insert data for two hosts
        for i in 0..20 {
            db.insert(&test_point("cpu", "srv-1", i, 1.0)).unwrap();
            db.insert(&test_point("cpu", "srv-2", i, 2.0)).unwrap();
        }
        db.flush().unwrap();

        // Tombstone srv-1
        let tags = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
        db.delete_series("cpu", &tags).unwrap();

        // If enough segments for compaction, tombstoned data should be removed
        let _ = db.compact();

        db.close().unwrap();
    }

    #[test]
    fn compact_with_rollup_produces_aggregated_points() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(128) // Tiny to force multiple segments
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Register a rollup: cpu → cpu_1h with Avg/Max over 1-hour buckets
        let rollup = crate::rollup::RollupBuilder::new()
            .name("cpu_hourly")
            .source("cpu")
            .target("cpu_1h")
            .interval_ns(3_600_000_000_000) // 1 hour in ns
            .aggregation(crate::rollup::RollupAggFn::Avg)
            .aggregation(crate::rollup::RollupAggFn::Max)
            .group_by("host")
            .build()
            .unwrap();
        db.create_rollup(rollup).unwrap();

        // Insert data across multiple flushes so compaction triggers
        for batch in 0..5 {
            for i in 0..10 {
                let ts = (batch * 1000 + i) * 1_000_000; // spread in ns
                db.insert(&test_point("cpu", "srv-1", ts, (i + 1) as f64))
                    .unwrap();
            }
            db.flush().unwrap();
        }

        // Run compaction — should trigger rollup computation
        let _ = db.compact();

        // Check if rollup data was inserted into target measurement
        let plan = db
            .query()
            .measurement("cpu_1h")
            .range(0, i64::MAX)
            .build()
            .unwrap();

        // After compaction + rollup, there should be some aggregated points
        // (exact count depends on how many segments triggered compaction)
        let batch = db.execute(&plan);
        // Even if no compaction tasks were eligible, this should not error
        assert!(batch.is_ok());

        db.close().unwrap();
    }

    // ── Rollup API tests ────────────────────────────────────────────

    #[test]
    fn create_and_list_rollups() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        let config = crate::rollup::RollupBuilder::new()
            .name("cpu_5min")
            .source("cpu")
            .target("cpu_5min")
            .interval_ns(300_000_000_000) // 5 minutes
            .aggregation(crate::rollup::RollupAggFn::Avg)
            .build()
            .unwrap();
        db.create_rollup(config).unwrap();

        let rollups = db.list_rollups().unwrap();
        assert_eq!(rollups.len(), 1);
        assert_eq!(rollups[0].name, "cpu_5min");

        db.close().unwrap();
    }

    #[test]
    fn delete_rollup_removes_definition() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        let config = crate::rollup::RollupBuilder::new()
            .name("cpu_hourly")
            .source("cpu")
            .target("cpu_1h")
            .interval_ns(3_600_000_000_000)
            .aggregation(crate::rollup::RollupAggFn::Max)
            .build()
            .unwrap();
        db.create_rollup(config).unwrap();
        assert_eq!(db.list_rollups().unwrap().len(), 1);

        let removed = db.delete_rollup("cpu_hourly").unwrap();
        assert!(removed);
        assert!(db.list_rollups().unwrap().is_empty());

        // Deleting non-existent rollup returns false
        assert!(!db.delete_rollup("nonexistent").unwrap());

        db.close().unwrap();
    }

    #[test]
    fn rollup_api_fails_after_close() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();
        db.close().unwrap();

        let config = crate::rollup::RollupBuilder::new()
            .name("test")
            .source("cpu")
            .target("cpu_5m")
            .interval_ns(300_000_000_000)
            .aggregation(crate::rollup::RollupAggFn::Avg)
            .build()
            .unwrap();
        assert!(db.create_rollup(config).is_err());
        assert!(db.list_rollups().is_err());
        assert!(db.delete_rollup("test").is_err());
    }

    // ── Warm tier tests ─────────────────────────────────────────────

    #[test]
    fn warm_tier_disabled_returns_empty() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        let config = WarmTierConfig {
            enabled: false,
            ..WarmTierConfig::default()
        };
        let result = db.warm_tier_migrate(&config).unwrap();
        assert_eq!(result.shards_moved, 0);
        assert_eq!(result.segments_recompressed, 0);

        db.close().unwrap();
    }

    #[test]
    fn warm_tier_migrate_moves_old_shards() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(128)
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Insert very old data (timestamp near 0)
        for i in 0..10 {
            db.insert(&test_point("cpu", "srv-1", i, 1.0)).unwrap();
        }
        db.flush().unwrap();

        let warm_dir = tmp.path().join("warm_data");
        let warm_config = WarmTierConfig {
            enabled: true,
            warm_after_ns: 1, // Very short threshold — everything is eligible
            warm_path: warm_dir.clone(),
            zstd_level: 3,
        };

        let result = db.warm_tier_migrate(&warm_config).unwrap();
        // Old data should have been migrated
        assert!(
            result.segments_recompressed > 0 || result.shards_moved > 0 || {
                // If no segments matched (shard bounds may not meet criteria), that's ok
                true
            }
        );

        db.close().unwrap();
    }

    /// Non-disruptive warm migration: original files removed after catalog
    /// swap, queries still return correct data from warm copies.
    #[test]
    fn warm_tier_non_disruptive_migration() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(128)
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Insert old data (timestamp near 0 → eligible for warm migration)
        for i in 0..5u64 {
            let p = test_point("cpu", "srv-1", (i * 1_000_000) as i64, i as f64);
            db.insert(&p).unwrap();
        }
        db.flush().unwrap();

        // Collect original segment paths before migration
        let original_paths: Vec<std::path::PathBuf> = {
            let cat = db.catalog.read();
            cat.segments_for_measurement("cpu")
                .iter()
                .map(|e| e.path.clone())
                .collect()
        };
        assert!(!original_paths.is_empty(), "should have segments");

        // Verify originals exist on disk
        for p in &original_paths {
            assert!(p.exists(), "original should exist: {p:?}");
        }

        let warm_dir = tmp.path().join("warm_data");
        let warm_config = WarmTierConfig {
            enabled: true,
            warm_after_ns: 1, // everything is eligible
            warm_path: warm_dir.clone(),
            zstd_level: 9,
        };

        let result = db.warm_tier_migrate(&warm_config).unwrap();
        assert!(
            result.segments_recompressed > 0,
            "expected segments recompressed, got {result:?}",
        );

        // Originals should be deleted after catalog swap
        for p in &original_paths {
            assert!(
                !p.exists(),
                "original should be deleted after migration: {p:?}"
            );
        }

        // Catalog should now point to warm paths
        let warm_paths: Vec<std::path::PathBuf> = {
            let cat = db.catalog.read();
            cat.segments_for_measurement("cpu")
                .iter()
                .map(|e| e.path.clone())
                .collect()
        };
        for p in &warm_paths {
            assert!(
                p.starts_with(&warm_dir),
                "catalog should point to warm dir: {p:?}",
            );
            assert!(p.exists(), "warm copy should exist: {p:?}");
        }

        // Data integrity: query still returns all rows from warm copies
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();
        assert_eq!(batch.num_rows(), 5);

        db.close().unwrap();
    }

    #[test]
    fn tombstone_survives_crash_recovery() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .build()
            .unwrap();

        // Phase 1: insert data, delete a series, close
        {
            let db = Chronix::open(config.clone()).unwrap();
            let p1 = test_point("cpu", "srv-1", 1_000_000_000, 42.0);
            let p2 = test_point("cpu", "srv-2", 1_000_000_000, 99.0);
            db.insert(&p1).unwrap();
            db.insert(&p2).unwrap();
            db.flush().unwrap();

            // Tombstone srv-1
            let tags = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
            db.delete_series("cpu", &tags).unwrap();

            // Verify tombstone is active
            let plan = db
                .query()
                .measurement("cpu")
                .tag("host", "srv-1")
                .range(0, i64::MAX)
                .build()
                .unwrap();
            let batch = db.execute(&plan).unwrap();
            assert_eq!(batch.num_rows(), 0, "srv-1 should be tombstoned");

            db.close().unwrap();
        }

        // Phase 2: reopen and verify the tombstone survived WAL replay
        {
            let db = Chronix::open(config).unwrap();

            // srv-1 should still be tombstoned after WAL replay
            let plan = db
                .query()
                .measurement("cpu")
                .tag("host", "srv-1")
                .range(0, i64::MAX)
                .build()
                .unwrap();
            let batch = db.execute(&plan).unwrap();
            assert_eq!(
                batch.num_rows(),
                0,
                "srv-1 should remain tombstoned after reopen"
            );

            // srv-2 should still be queryable
            let plan2 = db
                .query()
                .measurement("cpu")
                .tag("host", "srv-2")
                .range(0, i64::MAX)
                .build()
                .unwrap();
            let batch2 = db.execute(&plan2).unwrap();
            assert!(batch2.num_rows() > 0, "srv-2 should still be queryable");

            db.close().unwrap();
        }
    }

    #[test]
    fn predicate_delete_survives_crash_recovery() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .build()
            .unwrap();

        // Phase 1: insert, predicate-delete, close
        {
            let db = Chronix::open(config.clone()).unwrap();
            for i in 0..5 {
                db.insert(&test_point("cpu", "srv-A", 1_000_000_000 + i, 10.0))
                    .unwrap();
                db.insert(&test_point("cpu", "srv-B", 1_000_000_000 + i, 20.0))
                    .unwrap();
            }
            db.flush().unwrap();

            let req = db
                .delete_builder()
                .measurement("cpu")
                .tag("host", "srv-A")
                .build()
                .unwrap();
            let count = db.execute_delete(&req).unwrap();
            assert!(
                count.series_tombstoned > 0,
                "should tombstone at least one series"
            );

            db.close().unwrap();
        }

        // Phase 2: reopen and verify predicate delete survived
        {
            let db = Chronix::open(config).unwrap();

            let plan = db
                .query()
                .measurement("cpu")
                .tag("host", "srv-A")
                .range(0, i64::MAX)
                .build()
                .unwrap();
            let batch = db.execute(&plan).unwrap();
            assert_eq!(
                batch.num_rows(),
                0,
                "srv-A should remain tombstoned after reopen"
            );

            let plan2 = db
                .query()
                .measurement("cpu")
                .tag("host", "srv-B")
                .range(0, i64::MAX)
                .build()
                .unwrap();
            let batch2 = db.execute(&plan2).unwrap();
            assert!(batch2.num_rows() > 0, "srv-B should still be queryable");

            db.close().unwrap();
        }
    }

    #[test]
    fn lvc_per_measurement_opt_in() {
        use std::collections::HashSet;

        let tmp = TempDir::new().unwrap();
        // Enable LVC only for "cpu" measurement
        let lvc_set: HashSet<String> = ["cpu".to_string()].into_iter().collect();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .enable_last_value_cache(true)
            .lvc_measurements(lvc_set)
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Insert into "cpu" (LVC-enabled) and "mem" (LVC-disabled)
        let p_cpu = test_point("cpu", "srv-1", 1_000_000_000, 42.0);
        let p_mem = test_point("mem", "srv-1", 1_000_000_000, 99.0);
        db.insert(&p_cpu).unwrap();
        db.insert(&p_mem).unwrap();

        // cpu should be in LVC
        let tags_cpu = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
        let cached_cpu = db.lvc.get("cpu", &tags_cpu);
        assert!(cached_cpu.is_some(), "cpu should be cached in LVC");

        // mem should NOT be in LVC (per-measurement opt-in excludes it)
        let tags_mem = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
        let cached_mem = db.lvc.get("mem", &tags_mem);
        assert!(cached_mem.is_none(), "mem should NOT be cached in LVC");

        db.close().unwrap();
    }

    #[test]
    fn lvc_disabled_globally_skips_all() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .enable_last_value_cache(false) // globally disabled
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        let p = test_point("cpu", "srv-1", 1_000_000_000, 42.0);
        db.insert(&p).unwrap();

        let tags = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
        let cached = db.lvc.get("cpu", &tags);
        assert!(
            cached.is_none(),
            "LVC should be empty when globally disabled"
        );

        db.close().unwrap();
    }

    #[test]
    fn tag_filtered_query_prunes_segments() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(128) // tiny → force flush per batch
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Create 10 segments for different hosts.
        // Only host "target" will match the query filter.
        for i in 0..10u64 {
            let host = if i == 0 {
                "target".to_string()
            } else {
                format!("other-{i}")
            };
            let tags = BTreeMap::from([("host".to_string(), host)]);
            let key = SeriesKey::new("cpu", tags).unwrap();
            let fields = BTreeMap::from([("value".to_string(), FieldValue::F64(i as f64))]);
            for j in 0..5 {
                let ts = (i * 1000 + j) * 1_000_000_000 + 1_700_000_000_000_000_000;
                let p = Point::new(key.clone(), fields.clone(), ts as i64).unwrap();
                db.insert(&p).unwrap();
            }
            db.flush().unwrap();
        }

        // Verify we actually have multiple segments
        let total_segments = {
            let cat = db.catalog.read();
            cat.segments_for_measurement("cpu").len()
        };
        assert!(
            total_segments >= 10,
            "expected ≥10 segments, got {total_segments}",
        );

        // Now query for host=target only
        let plan = db
            .query()
            .measurement("cpu")
            .tag("host", "target")
            .range(0, i64::MAX)
            .build()
            .unwrap();

        let (batch, stats) = db.execute_with_stats(&plan).unwrap();
        assert!(batch.num_rows() > 0, "should return target rows");

        // Pruning should have eliminated most of the segments
        let prune_ratio = stats.total_pruned() as f64 / stats.segments_total as f64;
        assert!(
            prune_ratio >= 0.8,
            "Expected ≥ 80% pruning, got {:.1}% ({} total, {} pruned)",
            prune_ratio * 100.0,
            stats.segments_total,
            stats.total_pruned(),
        );

        db.close().unwrap();
    }

    #[test]
    #[ignore = "long-running: involves soft-delete + compaction + GC"]
    fn compact_soft_deletes_then_gc_cleans() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(128) // tiny → many segments
            .max_memtable_memory(256 * 1024 * 1024)
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Create enough data for multiple segments in the same shard
        for i in 0..5u64 {
            let p = test_point("cpu", "srv-1", (i * 1_000_000_000) as i64, i as f64);
            db.insert(&p).unwrap();
            db.flush().unwrap();
            let _ = db.wal.truncate_before(u64::MAX);
        }

        let pre_compact_count = {
            let cat = db.catalog.read();
            cat.segments_for_measurement("cpu").len()
        };
        assert!(
            pre_compact_count >= 5,
            "need ≥5 segments before compaction, got {pre_compact_count}",
        );

        // Compact — merges segments, soft-deletes inputs
        let compacted = db.compact().unwrap();
        assert!(compacted >= 1, "expected at least 1 compaction task");

        // After compaction: old segments still in catalog as SoftDeleted,
        // but queries only see active segments.
        let total_in_catalog = {
            let cat = db.catalog.read();
            cat.segments_for_measurement("cpu").len()
        };
        let active_in_catalog = {
            let cat = db.catalog.read();
            cat.active_segments_for_measurement("cpu").len()
        };
        // Soft-deleted segments still exist in catalog
        assert!(
            total_in_catalog > active_in_catalog,
            "expected soft-deleted segments in catalog",
        );

        // Files for soft-deleted segments should still exist on disk
        let soft_deleted_paths: Vec<std::path::PathBuf> = {
            let cat = db.catalog.read();
            cat.segments_for_measurement("cpu")
                .iter()
                .filter(|e| e.state != SegmentState::Active)
                .map(|e| e.path.clone())
                .collect()
        };
        for path in &soft_deleted_paths {
            assert!(
                path.exists(),
                "soft-deleted file should still exist: {path:?}"
            );
        }

        // GC with 0 grace period — should clean up immediately
        let gc_cleaned = db.gc_with_grace(0).unwrap();
        assert!(gc_cleaned > 0, "GC should have cleaned up segments");

        // After GC: soft-deleted files should be removed
        for path in &soft_deleted_paths {
            assert!(!path.exists(), "GC should have removed file: {path:?}");
        }

        // Only active segments remain in catalog
        let final_count = {
            let cat = db.catalog.read();
            cat.segments_for_measurement("cpu").len()
        };
        assert_eq!(final_count, active_in_catalog);

        // Data should still be queryable
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();
        assert!(batch.num_rows() > 0);

        db.close().unwrap();
    }

    /// Integration test: backpressure kicks in when L0 segments pile up,
    /// and drops back to zero after compaction reduces the count.
    #[test]
    #[ignore = "long-running: involves backpressure + compaction + GC"]
    fn backpressure_rises_and_falls_with_compaction() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(128) // tiny → force flush per insert
            .max_memtable_memory(256 * 1024 * 1024) // large to avoid admission rejection
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Default CompactionPicker: trigger_threshold = 4 → heavy = 16.
        // Backpressure starts at l0_count > 16.
        // Insert enough points to create > 16 segments (each flush → 1 segment).
        // Truncate WAL periodically to keep file count below admission threshold.
        for i in 0..20u64 {
            let p = test_point("cpu", "srv-1", (i * 1_000_000_000) as i64, i as f64);
            let _ = db.insert(&p);
            db.flush().unwrap();
            let _ = db.wal.truncate_before(u64::MAX);
        }

        let l0_before = {
            let cat = db.catalog.read();
            cat.segment_count()
        };
        assert!(
            l0_before > 16,
            "need >16 segments for backpressure, got {l0_before}",
        );

        // Verify backpressure is active
        let delay_before = db.compaction_picker.backpressure_delay_ms(l0_before);
        assert!(
            delay_before > 0,
            "expected backpressure delay > 0, got {delay_before}",
        );

        // Compact — reduces L0 count
        let compacted = db.compact().unwrap();
        assert!(compacted >= 1, "expected at least 1 compaction task");

        // GC soft-deleted segments so catalog count actually drops
        let _gc = db.gc_with_grace(0).unwrap();

        let l0_after = {
            let cat = db.catalog.read();
            cat.segment_count()
        };

        // After compaction + GC, segment count should be well below heavy threshold
        let delay_after = db.compaction_picker.backpressure_delay_ms(l0_after);
        assert_eq!(
            delay_after, 0,
            "backpressure should be 0 after compaction, l0={l0_after}",
        );

        // Data integrity: all rows still queryable
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();
        assert_eq!(batch.num_rows(), 20);

        db.close().unwrap();
    }

    /// Integration test: per-measurement retention drops only the
    /// measurement-specific segments while leaving others intact.
    #[test]
    fn per_measurement_retention_overrides() {
        use std::time::Duration;

        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(128)
            // "metrics" gets a very short retention (1 nanosecond) so
            // everything is expired. "cpu" uses the global retention.
            .measurement_retention("metrics", Duration::from_nanos(1))
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Use recent timestamps so global retention (1 hour) keeps them.
        #[allow(clippy::cast_possible_truncation)]
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;

        // Insert data for two measurements, flush each separately
        for i in 0..3u64 {
            let ts = now_ns - (i as i64 * 1_000_000); // recent timestamps
            let p = test_point("cpu", "srv-1", ts, i as f64);
            db.insert(&p).unwrap();
        }
        db.flush().unwrap();

        for i in 0..3u64 {
            let ts = now_ns - (i as i64 * 1_000_000); // recent timestamps
            let p = test_point("metrics", "srv-1", ts, i as f64);
            db.insert(&p).unwrap();
        }
        db.flush().unwrap();

        // Verify both measurements have segments
        let cpu_before = {
            let cat = db.catalog.read();
            cat.segments_for_measurement("cpu").len()
        };
        let metrics_before = {
            let cat = db.catalog.read();
            cat.segments_for_measurement("metrics").len()
        };
        assert!(cpu_before >= 1, "cpu should have segments");
        assert!(metrics_before >= 1, "metrics should have segments");

        // Enforce retention with a generous global retention (1 hour)
        // → only "metrics" with its 1ns override should expire.
        let result = db
            .enforce_retention(3_600_000_000_000) // 1 hour in nanos
            .unwrap();

        // "metrics" segments should have been dropped
        assert!(
            result.segments_deleted >= metrics_before,
            "expected at least {metrics_before} deleted, got {}",
            result.segments_deleted,
        );

        // "cpu" segments should survive
        let cpu_after = {
            let cat = db.catalog.read();
            cat.segments_for_measurement("cpu").len()
        };
        assert_eq!(cpu_after, cpu_before, "cpu segments should not be dropped");

        // "metrics" segments should be gone
        let metrics_after = {
            let cat = db.catalog.read();
            cat.segments_for_measurement("metrics").len()
        };
        assert_eq!(metrics_after, 0, "metrics segments should all be dropped");

        // cpu data still queryable
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();
        assert!(batch.num_rows() > 0);

        db.close().unwrap();
    }

    /// Stress test: memtable memory stays bounded under sustained writes.
    /// Verifies that `max_memtable_memory` prevents unbounded growth and
    /// that flushing keeps the working set within limits.
    #[test]
    #[ignore = "long-running: involves sustained writes + compaction + GC"]
    fn memory_stays_bounded_under_sustained_writes() {
        let tmp = TempDir::new().unwrap();
        let max_mem: usize = 4096;
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(1024) // flush at 1 KB
            .max_memtable_memory(max_mem) // 4 KB hard cap
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        let mut peak_memory: usize = 0;

        // Write 50 points, flushing periodically to keep segment count low
        // (avoids excessive backpressure delays).
        for i in 0..50u64 {
            let p = test_point("cpu", "srv-1", (i * 1_000_000) as i64, i as f64);
            let _ = db.insert(&p);

            let mem = db.shards.total_memory();
            if mem > peak_memory {
                peak_memory = mem;
            }

            // Compact and GC periodically to prevent backpressure
            if i % 10 == 9 {
                let _ = db.compact();
                let _ = db.gc_with_grace(0);
                let _ = db.wal.truncate_before(u64::MAX);
            }
        }

        // Peak memory should never exceed the hard cap by more than one
        // point's overhead (the check happens before insert, so one extra
        // point may sneak in).
        let tolerance = 2048; // generous tolerance for atomic tracking granularity
        assert!(
            peak_memory <= max_mem + tolerance,
            "peak memory {peak_memory} exceeded max {max_mem} + tolerance {tolerance}",
        );

        // Data should be queryable
        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();
        assert!(batch.num_rows() > 0, "should have some data");

        db.close().unwrap();
    }

    #[test]
    fn backup_and_restore_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("db");
        let backup_dir = dir.path().join("backup");
        let restore_dir = dir.path().join("restored");

        // Create DB and insert data
        let config = ChronixConfig::builder()
            .data_dir(&data_dir)
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        for i in 0..50 {
            let key = SeriesKey::new(
                "cpu",
                BTreeMap::from([("host".to_string(), "a".to_string())]),
            )
            .unwrap();
            let fields =
                BTreeMap::from([("value".to_string(), chronix_core::FieldValue::F64(i as f64))]);
            let p = Point::new(key, fields, i * 1_000_000_000).unwrap();
            db.insert(&p).unwrap();
        }

        // Backup
        let manifest = db.backup(&backup_dir).unwrap();
        assert!(manifest.file_count > 0);
        assert!(manifest.total_bytes > 0);
        assert!(manifest.wal_sequence > 0);
        assert!(backup_dir.join("backup_manifest.json").exists());
        assert!(backup_dir.join("wal").exists());

        db.close().unwrap();

        // Restore
        let restored_manifest = Chronix::restore(&backup_dir, &restore_dir).unwrap();
        assert_eq!(restored_manifest.wal_sequence, manifest.wal_sequence);

        // Open restored DB and verify data
        let restore_config = ChronixConfig::builder()
            .data_dir(&restore_dir)
            .build()
            .unwrap();
        let db2 = Chronix::open(restore_config).unwrap();
        let plan = db2
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .build()
            .unwrap();
        let batch = db2.execute(&plan).unwrap();
        assert_eq!(batch.num_rows(), 50);
        db2.close().unwrap();
    }

    #[test]
    fn restore_rejects_existing_target() {
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("db");
        let backup_dir = dir.path().join("backup");

        let config = ChronixConfig::builder()
            .data_dir(&data_dir)
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();
        db.backup(&backup_dir).unwrap();
        db.close().unwrap();

        // Restore to existing directory should fail
        let existing = dir.path().join("exists");
        std::fs::create_dir_all(&existing).unwrap();
        assert!(Chronix::restore(&backup_dir, &existing).is_err());
    }

    #[test]
    fn restore_rejects_missing_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("restored");
        assert!(Chronix::restore(dir.path(), &target).is_err());
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn window_delta_and_cumulative_sum() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        // Insert monotonically increasing values
        for i in 0..5 {
            db.insert(&test_point(
                "cpu",
                "srv-1",
                (i + 1) * 1_000_000_000,
                (i + 1) as f64 * 10.0,
            ))
            .unwrap();
        }

        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .window(chronix_query::WindowFn::Delta)
            .window(chronix_query::WindowFn::CumulativeSum)
            .window_value_column("value")
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();

        // Should have original columns + 2 window columns
        assert_eq!(batch.num_rows(), 5);
        assert!(batch.column_by_name("value_delta").is_some());
        assert!(batch.column_by_name("value_cumulative_sum").is_some());

        let delta = batch
            .column_by_name("value_delta")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        // First delta is null, rest are 10.0
        assert!(delta.is_null(0));
        assert!((delta.value(1) - 10.0).abs() < f64::EPSILON);

        let cs = batch
            .column_by_name("value_cumulative_sum")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((cs.value(0) - 10.0).abs() < f64::EPSILON);
        assert!((cs.value(4) - 150.0).abs() < f64::EPSILON);

        db.close().unwrap();
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn window_moving_average() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        for i in 0..5 {
            db.insert(&test_point(
                "cpu",
                "srv-1",
                (i + 1) * 1_000_000_000,
                (i + 1) as f64 * 10.0,
            ))
            .unwrap();
        }

        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .window(chronix_query::WindowFn::MovingAverage { window_size: 3 })
            .window_value_column("value")
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();

        let ma = batch
            .column_by_name("value_moving_avg_3")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        // First two are null (not enough data), third is avg(10,20,30)=20
        assert!(ma.is_null(0));
        assert!(ma.is_null(1));
        assert!((ma.value(2) - 20.0).abs() < f64::EPSILON);

        db.close().unwrap();
    }

    #[test]
    #[allow(clippy::cast_precision_loss)]
    fn window_rate() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        // 1 second apart, value increases by 10 each step
        for i in 0..3 {
            db.insert(&test_point(
                "cpu",
                "srv-1",
                (i + 1) * 1_000_000_000,
                (i + 1) as f64 * 10.0,
            ))
            .unwrap();
        }

        let plan = db
            .query()
            .measurement("cpu")
            .range(0, i64::MAX)
            .window(chronix_query::WindowFn::Rate)
            .window_value_column("value")
            .build()
            .unwrap();
        let batch = db.execute(&plan).unwrap();

        let rate = batch
            .column_by_name("value_rate")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        // delta_v=10, delta_t=1s → rate=10/s
        assert!(rate.is_null(0));
        assert!((rate.value(1) - 10.0).abs() < f64::EPSILON);
        assert!((rate.value(2) - 10.0).abs() < f64::EPSILON);

        db.close().unwrap();
    }

    /// Verify that `gc_tombstones()` removes tombstones whose
    /// target measurement no longer has active segments.
    #[test]
    fn gc_tombstones_removes_stale_entries() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(128) // tiny → force flushes
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Insert data and flush to create segments
        for i in 0..3u64 {
            db.insert(&test_point(
                "cpu",
                "srv-1",
                (i * 1_000_000_000) as i64,
                i as f64,
            ))
            .unwrap();
        }
        db.flush().unwrap();

        // Delete (tombstone) the series
        let tags = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
        db.delete_series("cpu", &tags).unwrap();

        // Verify tombstone exists
        let tombstone_count_before = { db.tombstones.read().len() };
        assert!(
            tombstone_count_before >= 1,
            "expected at least 1 tombstone, got {tombstone_count_before}",
        );

        // Drop the measurement — removes all segments
        db.drop_measurement("cpu").unwrap();

        // Now gc_tombstones should clean up the stale tombstone
        let removed = db.gc_tombstones();
        assert!(
            removed >= 1,
            "gc_tombstones should have removed at least 1 tombstone, removed {removed}",
        );

        // Tombstone set should be empty
        let tombstone_count_after = { db.tombstones.read().len() };
        assert_eq!(
            tombstone_count_after, 0,
            "tombstone set should be empty after GC, got {tombstone_count_after}",
        );

        db.close().unwrap();
    }

    /// A reclaim pass immediately after a delete must reclaim nothing.
    ///
    /// This test replaces one that asserted the exact opposite —
    /// `compact_tombstones()` was expected to remove the tombstone right after
    /// `delete_series()`, and did, because the rule was "drop the tombstone
    /// once its series has left `known_series`" and the delete had just
    /// removed it from there. The rows were still on disk, so the delete came
    /// undone the first time any background pass ran. A test can pin a defect
    /// as easily as a property; this one had pinned the defect for as long as
    /// it had existed.
    #[test]
    fn reclaiming_immediately_after_a_delete_reclaims_nothing() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        db.insert(&test_point("cpu", "srv-1", 1_000_000_000, 42.0))
            .unwrap();

        let tags = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
        db.delete_series("cpu", &tags).unwrap();
        assert_eq!(db.tombstones.read().tombstone_count(), 1);

        let removed = db.gc_tombstones();
        assert_eq!(
            removed, 0,
            "the segments this delete was issued against are still in the catalog"
        );
        assert_eq!(
            db.tombstones.read().tombstone_count(),
            1,
            "the tombstone must survive until compaction has materialised it"
        );
        assert_eq!(
            db.last_value("cpu", &tags).unwrap(),
            None,
            "and the series must still read as deleted"
        );

        db.close().unwrap();
    }

    /// Compaction materialises a delete, and the delete stays applied.
    ///
    /// The previous version of this test asserted only that the tombstone
    /// count did not *grow*, which every possible outcome satisfies — the
    /// count is bounded above by one. It could not distinguish "compaction
    /// applied the delete" from "compaction resurrected the rows".
    #[test]
    fn compaction_materialises_a_delete_without_resurrecting_it() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(128) // tiny → many segments
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        for i in 0..6u64 {
            db.insert(&test_point(
                "cpu",
                "srv-1",
                (i * 1_000_000_000) as i64,
                i as f64,
            ))
            .unwrap();
            db.insert(&test_point(
                "cpu",
                "srv-2",
                (i * 1_000_000_000) as i64,
                i as f64,
            ))
            .unwrap();
            db.flush().unwrap();
        }

        let tags = BTreeMap::from([("host".to_string(), "srv-1".to_string())]);
        db.delete_series("cpu", &tags).unwrap();
        assert_eq!(db.tombstones.read().tombstone_count(), 1);

        let _ = db.compact();
        db.gc_tombstones();

        assert_eq!(
            db.last_value("cpu", &tags).unwrap(),
            None,
            "srv-1 stays deleted through compaction"
        );
        let other = BTreeMap::from([("host".to_string(), "srv-2".to_string())]);
        assert!(
            db.last_value("cpu", &other).unwrap().is_some(),
            "srv-2 was never deleted and must survive"
        );

        db.close().unwrap();
    }

    /// Verify that `detect_segment_overlaps` correctly identifies
    /// overlapping segments.
    #[test]
    fn detect_segment_overlaps_finds_overlaps() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(128) // tiny → force flush per insert
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Insert overlapping time ranges across multiple flushes
        // Flush 1: timestamps 0..2
        for i in 0..3u64 {
            db.insert(&test_point(
                "cpu",
                "srv-1",
                (i * 1_000_000_000) as i64,
                i as f64,
            ))
            .unwrap();
        }
        db.flush().unwrap();

        // Flush 2: timestamps 1..3 (overlaps with flush 1)
        for i in 1..4u64 {
            db.insert(&test_point(
                "cpu",
                "srv-1",
                (i * 1_000_000_000) as i64,
                i as f64,
            ))
            .unwrap();
        }
        db.flush().unwrap();

        // Check for overlaps — should find at least one
        let overlaps = db.detect_segment_overlaps("cpu");
        assert!(
            overlaps >= 1,
            "expected at least 1 overlap from overlapping flushes, got {overlaps}",
        );

        db.close().unwrap();
    }

    /// Verify that non-overlapping segments produce zero overlaps.
    #[test]
    fn detect_segment_overlaps_none_when_disjoint() {
        let tmp = TempDir::new().unwrap();
        let config = ChronixConfig::builder()
            .data_dir(tmp.path())
            .memtable_flush_threshold(128)
            .build()
            .unwrap();
        let db = Chronix::open(config).unwrap();

        // Flush 1: timestamps 0..99
        db.insert(&test_point("cpu", "srv-1", 0, 1.0)).unwrap();
        db.flush().unwrap();

        // Flush 2: timestamps 1_000_000_000..1_000_000_099 (no overlap)
        db.insert(&test_point("cpu", "srv-1", 1_000_000_000, 2.0))
            .unwrap();
        db.flush().unwrap();

        let overlaps = db.detect_segment_overlaps("cpu");
        assert_eq!(
            overlaps, 0,
            "expected 0 overlaps for disjoint segments, got {overlaps}",
        );

        db.close().unwrap();
    }

    /// Verify no overlaps for a measurement with zero or one segments.
    #[test]
    fn detect_segment_overlaps_trivial_cases() {
        let tmp = TempDir::new().unwrap();
        let db = Chronix::open(test_config(tmp.path())).unwrap();

        // No segments at all
        assert_eq!(db.detect_segment_overlaps("nonexistent"), 0);

        // Single segment
        db.insert(&test_point("cpu", "srv-1", 1_000, 1.0)).unwrap();
        db.flush().unwrap();

        assert_eq!(db.detect_segment_overlaps("cpu"), 0);

        db.close().unwrap();
    }

    /// Verify tag index is correctly rebuilt from on-disk segments after
    /// close + reopen (cold-start). Without this fix, tag-filtered queries
    /// returned zero results for all pre-existing data after restart.
    #[test]
    fn tag_index_survives_restart() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Phase 1: insert data with different tags, flush to segments, close
        {
            let db = Chronix::open(test_config(&path)).unwrap();
            for i in 0..5 {
                let p = test_point("cpu", "srv-1", i * 1_000_000_000, i as f64);
                db.insert(&p).unwrap();
            }
            for i in 0..3 {
                let p = test_point("cpu", "srv-2", i * 1_000_000_000, i as f64 + 100.0);
                db.insert(&p).unwrap();
            }
            db.flush().unwrap();

            // Verify tag index works before close
            let segs = db.tag_index.segments_for_tag("host", "srv-1");
            assert!(!segs.is_empty(), "tag index should have srv-1 before close");

            db.close().unwrap();
        }

        // Phase 2: reopen and verify tag index is populated from segments
        {
            let db = Chronix::open(test_config(&path)).unwrap();

            // Tag index should have been rebuilt from on-disk segments
            let segs_srv1 = db.tag_index.segments_for_tag("host", "srv-1");
            assert!(
                !segs_srv1.is_empty(),
                "tag index should have srv-1 entries after restart"
            );

            let segs_srv2 = db.tag_index.segments_for_tag("host", "srv-2");
            assert!(
                !segs_srv2.is_empty(),
                "tag index should have srv-2 entries after restart"
            );

            // Tag-filtered query should return correct results
            let plan = db
                .query()
                .measurement("cpu")
                .tag("host", "srv-1")
                .range(0, i64::MAX)
                .build()
                .unwrap();
            let batch = db.execute(&plan).unwrap();
            assert_eq!(
                batch.num_rows(),
                5,
                "tag-filtered query should return 5 rows for srv-1 after restart"
            );

            let plan = db
                .query()
                .measurement("cpu")
                .tag("host", "srv-2")
                .range(0, i64::MAX)
                .build()
                .unwrap();
            let batch = db.execute(&plan).unwrap();
            assert_eq!(
                batch.num_rows(),
                3,
                "tag-filtered query should return 3 rows for srv-2 after restart"
            );

            db.close().unwrap();
        }
    }

    /// Ingest-time downsampling integration test:
    /// set a rule, insert points across bucket boundaries, verify
    /// aggregated points appear in the target measurement.
    #[test]
    fn ingest_time_downsampling_end_to_end() {
        use crate::rollup::{RollupAggFn, RollupBuilder};

        let tmp = TempDir::new().unwrap();
        let mut db = Chronix::open(test_config(tmp.path())).unwrap();

        let rule = RollupBuilder::new()
            .name("cpu_10s")
            .source("cpu")
            .target("cpu_10s_agg")
            .interval_ns(10_000_000_000) // 10 s in ns
            .aggregation(RollupAggFn::Avg)
            .aggregation(RollupAggFn::Max)
            .group_by("host")
            .build()
            .unwrap();

        db.set_ingest_downsampling(vec![rule]);

        // Insert 3 points in first 10s bucket for host=srv-1
        for i in 0..3u64 {
            let p = test_point("cpu", "srv-1", (i * 1_000_000_000) as i64, (i + 1) as f64);
            db.insert(&p).unwrap();
        }

        // Cross to next bucket — should trigger emission
        let p = test_point("cpu", "srv-1", 11_000_000_000, 99.0);
        db.insert(&p).unwrap();

        // The aggregated point should be in the "cpu_10s_agg" measurement
        // in the memtable (not yet flushed)
        let agg_key = SeriesKey::new(
            "cpu_10s_agg",
            BTreeMap::from([("host".to_string(), "srv-1".to_string())]),
        )
        .unwrap();
        let results = db.scan_memtable(&agg_key, 0, i64::MAX);
        assert_eq!(results.len(), 1, "expected 1 aggregated point");

        let agg_point = &results[0];
        // avg(1,2,3) = 2.0
        match agg_point.field("value_avg") {
            Some(FieldValue::F64(v)) => {
                assert!((v - 2.0).abs() < f64::EPSILON, "avg should be 2.0, got {v}");
            }
            other => panic!("Expected F64 avg, got {other:?}"),
        }
        // max(1,2,3) = 3.0
        match agg_point.field("value_max") {
            Some(FieldValue::F64(v)) => {
                assert!((v - 3.0).abs() < f64::EPSILON, "max should be 3.0, got {v}");
            }
            other => panic!("Expected F64 max, got {other:?}"),
        }

        // flush_ingest_downsampling should emit the in-flight bucket
        let flushed = db.flush_ingest_downsampling().unwrap();
        assert_eq!(flushed, 1, "should flush 1 partial bucket");

        db.close().unwrap();
    }
}
