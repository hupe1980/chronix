//! Segment catalog with manifest persistence.
//!
//! The [`SegmentCatalog`] tracks metadata for all segments in the database,
//! organized by shard. Changes are logged to a manifest WAL for crash-safe
//! persistence, with periodic snapshots to bound recovery time.
//!
//! # Manifest format
//!
//! The manifest WAL uses a **length-prefixed postcard** binary format:
//! each entry is `[u32-le length][postcard payload]`. This provides:
//!
//! - **Fast serialization**: postcard is far faster than JSON and
//!   produces ~3x smaller output.
//! - **Crash-safe framing**: length prefix + CRC32c integrity check
//!   detects truncated or corrupt entries reliably.
//! - **Bounded replay**: binary parsing is O(entries), not O(bytes).
//!
//! Snapshots use the same postcard format (single blob).

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::segment::stats::ColumnStats;
use chronix_core::{
    MeasurementSchema, SegmentFile, SegmentId, SegmentState, ShardId, Timestamp, Tombstone,
    TombstoneSet,
};

use crate::index::error::{IndexError, Result};

/// Per-column statistics stored in the catalog for pruning.
///
/// Contains the column name, data type, and min/max statistics so the
/// pruning pipeline can eliminate segments without opening any file.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogColumnStats {
    /// Column name (e.g. "temperature", "host").
    pub name: String,
    /// Column data type tag (from `crate::segment::metadata::data_types`).
    pub data_type: u8,
    /// Column role tag (0= timestamp, 1= tag, 2= field).
    pub role: u8,
    /// Digits after the decimal point, for a decimal column; `None`
    /// otherwise.
    ///
    /// Here as well as in the segment because the schema repair at open
    /// works from *this* summary and never opens a `.csx` file: without the
    /// scale, a decimal column the manifest had lost could only be restored
    /// as some other type, which is worse than not restoring it.
    #[serde(default)]
    pub decimal_scale: Option<u8>,
    /// Min/max/null statistics.
    pub stats: ColumnStats,
}

/// Metadata for a single segment in the catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentCatalogEntry {
    /// Unique segment identifier.
    pub segment_id: SegmentId,
    /// The shard this segment belongs to.
    pub shard_id: ShardId,
    /// Measurement name this segment contains data for.
    pub measurement: String,
    /// Where the `.csx` file lives, relative to the `segments/` directory.
    ///
    /// Relative, so the catalog describes *a* database rather than the
    /// directory this one happens to sit in — see [`SegmentFile`].
    pub file: SegmentFile,
    /// Minimum timestamp in the segment (inclusive).
    pub min_timestamp: Timestamp,
    /// Maximum timestamp in the segment (inclusive).
    pub max_timestamp: Timestamp,
    /// Total number of rows in the segment.
    pub row_count: u64,
    /// Number of unique series in the segment.
    pub series_count: u32,
    /// File size in bytes.
    pub byte_size: u64,
    /// Number of row groups.
    pub row_group_count: u32,
    /// Number of columns.
    pub column_count: u16,
    /// Per-column statistics for predicate pushdown pruning.
    #[serde(default)]
    pub column_stats: Vec<CatalogColumnStats>,
    /// Lifecycle state of this segment.
    #[serde(default)]
    pub state: SegmentState,
}

/// Manifest entry types for WAL-style persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ManifestEntry {
    /// A segment was added.
    AddSegment(SegmentCatalogEntry),
    /// A segment was removed.
    RemoveSegment(u64),
    /// A schema was registered or updated.
    SetSchema(MeasurementSchema),
    /// A schema was removed.
    RemoveSchema(String),
    /// A tombstone was recorded by a delete.
    AddTombstone(Tombstone),
    /// A tombstone was reclaimed — every segment it was issued against is gone.
    RemoveTombstone(Tombstone),
    /// Every WAL record with a sequence number at or below this is in a
    /// segment. Replay starts after it.
    SetWalFloor(u64),
    /// Compaction merged `inputs` into `output`: every tombstone that named
    /// an input now names the output too.
    Compacted {
        /// The input segment ids.
        inputs: Vec<u64>,
        /// The output segment id.
        output: u64,
    },
    /// A rollup definition was created or replaced. The payload is the
    /// facade's `RollupConfig`, opaque here — the catalog persists it, the
    /// facade interprets it.
    SetRollup {
        /// The rollup's name, its identity.
        name: String,
        /// Postcard-encoded definition.
        definition: Vec<u8>,
    },
    /// A rollup definition was deleted.
    RemoveRollup(String),
    /// A rollup's materialisation state changed: its watermark advanced, or
    /// a write below the watermark invalidated some of its buckets.
    SetRollupState {
        /// The rollup's name.
        name: String,
        /// Postcard-encoded state.
        state: Vec<u8>,
    },
    /// A measurement was soft-deleted: it is pending a hard delete at
    /// `deadline_ms` unless cancelled first.
    ///
    /// Durable for the same reason a tombstone is: an in-memory-only
    /// pending-drop map is undone by every restart, silently un-dropping a
    /// measurement an operator was told was gone and forgetting the deadline
    /// that was supposed to reclaim its disk.
    SetPendingMeasurementDrop {
        /// The measurement pending deletion.
        measurement: String,
        /// Unix-ms deadline after which the next GC pass hard-deletes it.
        deadline_ms: u64,
    },
    /// A pending drop was cancelled — by a restore, or by the hard delete
    /// that finally acted on it.
    CancelPendingMeasurementDrop(String),
}

/// A rollup's persisted definition and state, as opaque payloads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollupRecord {
    /// Postcard-encoded definition, absent while only state is known.
    #[serde(default)]
    pub definition: Vec<u8>,
    /// Postcard-encoded materialisation state.
    #[serde(default)]
    pub state: Vec<u8>,
}

/// Segment metadata catalog with manifest persistence.
///
/// Tracks all segment metadata organized by shard, along with measurement
/// schemas. Changes are persisted to a manifest WAL file, with periodic
/// snapshots to prevent unbounded manifest growth.
///
/// # Persistence
///
/// Changes are appended to `manifest.wal` as length-prefixed, CRC-32C
/// framed postcard entries.
/// Periodic snapshots written to `manifest.snapshot.bin`.
/// On startup: load latest snapshot, replay WAL entries.
///
/// # Concurrency
///
/// The catalog currently uses a single `&mut self` borrow (i.e. external
/// synchronisation via an `RwLock` or `Mutex`) for all shards.  This is
/// acceptable because catalog mutations (segment add/remove, schema
/// updates) are infrequent (flush/compaction cadence) and short-lived.
///
/// If catalog contention ever becomes measurable under very high shard
/// counts, the internal `BTreeMap<ShardId, Vec<…>>` can be replaced with
/// a `DashMap<ShardId, Vec<…>>` to allow per-shard concurrent access
/// without changing the external API.
#[derive(Debug)]
pub struct SegmentCatalog {
    /// Segments organized by shard.
    segments: BTreeMap<ShardId, Vec<SegmentCatalogEntry>>,
    /// Measurement schemas.
    schemas: HashMap<String, MeasurementSchema>,
    /// Tombstones recorded by deletes.
    ///
    /// These live in the catalog rather than in the data WAL, and that is the
    /// fix for a delete that did not survive a restart. The data WAL is
    /// truncated once the memtable it covers has been flushed, so a delete
    /// logged before a flush was discarded by it — the tombstone existed only
    /// in memory from then on, and the deleted rows came back at the next
    /// open. The manifest is only ever rewritten by a snapshot that carries
    /// its full contents forward.
    tombstones: TombstoneSet,
    /// The WAL floor: every data-WAL record with `sequence_no <= wal_floor`
    /// has been flushed into a segment that this catalog registers.
    ///
    /// Recorded here rather than inferred from the WAL files, because the
    /// active WAL file is never deleted by truncation. Without a floor every
    /// restart replayed that file — up to 32 MB of already-flushed points —
    /// into the memtable, and the next flush wrote them to disk a second
    /// time. Dedup hid the duplicates from queries; the flash wear was real.
    wal_floor: u64,
    /// Rollup definitions and state, persisted with everything else that
    /// a restart must not lose. They used to live in a hand-written JSON
    /// file beside the data directory with no fsync and no CRC, and a
    /// failure to parse it was logged and swallowed — leaving a database
    /// that believed it had no rollups, and a retention pass that then
    /// dropped the raw data those rollups existed to preserve.
    rollups: BTreeMap<String, RollupRecord>,
    /// Measurements pending a hard delete, by the unix-ms deadline a GC pass
    /// acts on. See [`ManifestEntry::SetPendingMeasurementDrop`].
    pending_measurement_drops: BTreeMap<String, u64>,
    /// Monotonically increasing catalog version.
    manifest_seq: u64,
    /// Directory for manifest files.
    manifest_dir: PathBuf,
    /// Number of changes since last snapshot.
    changes_since_snapshot: u64,
    /// Next segment ID to assign.
    next_segment_id: u64,
    /// Persistent manifest-log handle (kept open for append).
    ///
    /// `dyn DurableFile` rather than `File` so a test can make the write
    /// fail. A durability path is defined by what it does when the write
    /// fails, and a filesystem will not fail on request.
    manifest_wal_file: Option<Box<dyn crate::durable::DurableFile>>,
    /// Bytes of `manifest.wal` known to hold whole records.
    ///
    /// The rewind point. An append that fails part-way truncates back to it,
    /// so the file never holds a fragment with a valid record after it —
    /// which replay reads as corruption, and used to answer by silently
    /// discarding every transition that followed.
    manifest_good_len: u64,
    /// Number of appends written but not yet `sync_data()`d.
    ///
    /// Normally at most one: every append syncs. It grows only inside
    /// [`in_one_sync`](Self::in_one_sync).
    pending_sync: u64,
    /// Depth of the [`in_one_sync`](Self::in_one_sync) nesting.
    ///
    /// Non-zero means "this transition is not finished; do not fsync yet".
    defer_sync: u32,
    /// How many times the manifest has actually been fsynced.
    ///
    /// Exported as `chronix_catalog_fsync_total`, beside the WAL's own
    /// counter: on flash-backed storage the fsync rate *is* the wear rate,
    /// and the catalog was paying one per row of bookkeeping. It is also what
    /// makes the batching testable — a claim about a number of syscalls that
    /// nothing counts is a claim that rots.
    manifest_syncs: u64,
}

/// The largest a single manifest record may plausibly be.
///
/// A length header that claims more than this is corruption rather than a
/// record: the biggest real entry is a schema or a batch of tombstones, and
/// neither approaches it.
const MAX_MANIFEST_RECORD_BYTES: usize = 64 * 1024 * 1024;

/// What sits at an offset in the manifest log.
enum RecordAt {
    /// A whole record whose CRC checks out.
    Valid {
        /// Byte range of the postcard payload.
        payload: std::ops::Range<usize>,
        /// Offset the next record starts at.
        next: usize,
    },
    /// Not a record, with the reason for the log.
    Bad(String),
}

/// Snapshot interval: take a snapshot every N changes.
const SNAPSHOT_INTERVAL: u64 = 1000;

#[cfg(test)]
thread_local! {
    /// Bytes the manifest log may still accept on this thread before every
    /// write returns `ENOSPC`.
    ///
    /// Thread-local rather than a field, so the budget reaches the file a
    /// `SegmentCatalog` opens *lazily*, without a constructor argument every
    /// caller in the tree would have to thread through for one test.
    /// `i64::MAX` is a disk that never fills, which is what every test that
    /// is not about `ENOSPC` gets.
    static MANIFEST_DISK_BUDGET: std::sync::Arc<std::sync::atomic::AtomicI64> =
        std::sync::Arc::new(std::sync::atomic::AtomicI64::new(i64::MAX));
}

/// Sync on **every** manifest append, so no catalog mutation is lost to a
/// power failure.
///
/// A single mutation — registering a segment, updating a schema — is
/// infrequent enough that its fsync is free. What is not free is a mutation
/// made of *many* appends: a compaction of four segments is six, and a delete
/// producing a hundred tombstones is a hundred. Those are batched by
/// [`SegmentCatalog::in_one_sync`], which is where the count belongs — a
/// global batch size would defer the appends nobody is going to sync.
const SYNC_BATCH_SIZE: u64 = 1;

// Compile-time guarantee: SegmentCatalog is safe to share across threads.
const _: () = {
    const fn _assert_send<T: Send + Sync>() {}
    _assert_send::<SegmentCatalog>();
};

impl SegmentCatalog {
    /// Create a new empty catalog persisted in `manifest_dir`.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest directory cannot be created.
    pub fn new(manifest_dir: impl Into<PathBuf>) -> Result<Self> {
        let manifest_dir = manifest_dir.into();
        std::fs::create_dir_all(&manifest_dir)?;

        Ok(Self {
            segments: BTreeMap::new(),
            schemas: HashMap::new(),
            tombstones: TombstoneSet::new(),
            wal_floor: 0,
            rollups: BTreeMap::new(),
            pending_measurement_drops: BTreeMap::new(),
            manifest_seq: 0,
            manifest_dir,
            changes_since_snapshot: 0,
            next_segment_id: 1,
            manifest_wal_file: None,
            manifest_good_len: 0,
            pending_sync: 0,
            defer_sync: 0,
            manifest_syncs: 0,
        })
    }

    /// Open an existing catalog, loading from snapshot + replaying manifest WAL.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot or manifest cannot be read/parsed.
    pub fn open(manifest_dir: impl Into<PathBuf>) -> Result<Self> {
        let manifest_dir = manifest_dir.into();
        std::fs::create_dir_all(&manifest_dir)?;

        let bin_snapshot_path = manifest_dir.join("manifest.snapshot.bin");
        let mut catalog = if bin_snapshot_path.exists() {
            let data = std::fs::read(&bin_snapshot_path)?;
            let snapshot: CatalogSnapshot = postcard::from_bytes(&data)
                .map_err(|e| IndexError::BinarySerialization(e.to_string()))?;
            // Reject snapshots from future versions we can't parse.
            if snapshot.format_version > CATALOG_FORMAT_VERSION {
                return Err(IndexError::BinarySerialization(format!(
                    "catalog snapshot format version {} is newer than supported version {CATALOG_FORMAT_VERSION}",
                    snapshot.format_version
                )));
            }
            debug!(
                seq = snapshot.manifest_seq,
                segments = snapshot.segments.len(),
                format_version = snapshot.format_version,
                "loaded catalog snapshot (postcard)"
            );
            Self {
                segments: snapshot.segments,
                schemas: snapshot.schemas,
                tombstones: snapshot.tombstones,
                wal_floor: snapshot.wal_floor,
                rollups: snapshot.rollups,
                pending_measurement_drops: snapshot.pending_measurement_drops,
                manifest_seq: snapshot.manifest_seq,
                manifest_dir: manifest_dir.clone(),
                changes_since_snapshot: 0,
                next_segment_id: snapshot.next_segment_id,
                manifest_wal_file: None,
                manifest_good_len: 0,
                pending_sync: 0,
                defer_sync: 0,
                manifest_syncs: 0,
            }
        } else {
            Self {
                segments: BTreeMap::new(),
                schemas: HashMap::new(),
                tombstones: TombstoneSet::new(),
                wal_floor: 0,
                rollups: BTreeMap::new(),
                pending_measurement_drops: BTreeMap::new(),
                manifest_seq: 0,
                manifest_dir: manifest_dir.clone(),
                changes_since_snapshot: 0,
                next_segment_id: 1,
                manifest_wal_file: None,
                manifest_good_len: 0,
                pending_sync: 0,
                defer_sync: 0,
                manifest_syncs: 0,
            }
        };

        // Replay manifest WAL
        let wal_path = manifest_dir.join("manifest.wal");
        if wal_path.exists() {
            catalog.replay_manifest(&wal_path)?;
        }

        Ok(catalog)
    }

    /// Add a segment to the catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be written.
    pub fn add_segment(&mut self, entry: SegmentCatalogEntry) -> Result<()> {
        let manifest_entry = ManifestEntry::AddSegment(entry.clone());
        self.append_manifest(&manifest_entry)?;

        self.segments.entry(entry.shard_id).or_default().push(entry);

        self.maybe_snapshot();
        Ok(())
    }

    /// Remove a segment from the catalog by its ID.
    ///
    /// Returns the removed entry if found.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be written.
    pub fn remove_segment(&mut self, segment_id: SegmentId) -> Result<Option<SegmentCatalogEntry>> {
        let manifest_entry = ManifestEntry::RemoveSegment(segment_id.0);
        self.append_manifest(&manifest_entry)?;

        for entries in self.segments.values_mut() {
            if let Some(pos) = entries.iter().position(|e| e.segment_id == segment_id) {
                let removed = entries.remove(pos);
                self.maybe_snapshot();
                return Ok(Some(removed));
            }
        }

        self.maybe_snapshot();
        Ok(None)
    }

    /// Get all segments for a specific shard.
    #[must_use]
    pub fn get_segments_for_shard(&self, shard_id: &ShardId) -> &[SegmentCatalogEntry] {
        self.segments.get(shard_id).map_or(&[], Vec::as_slice)
    }

    /// Mark a segment as soft-deleted.
    ///
    /// The segment remains in the catalog and is still queryable by code
    /// that explicitly opts in to soft-deleted segments — but the default
    /// query path (via `active_segments_for_measurement`) hides it.
    /// A subsequent call to `remove_segment` will hard-delete it.
    pub fn soft_delete_segment(&mut self, segment_id: SegmentId, now_ms: u64) -> Result<bool> {
        // Find which measurement key and entry index contains this segment.
        let location = self.segments.iter().find_map(|(measurement, entries)| {
            entries
                .iter()
                .position(|e| e.segment_id == segment_id)
                .map(|idx| (*measurement, idx))
        });

        let Some((measurement, idx)) = location else {
            return Ok(false);
        };

        // Persist FIRST, then mutate in-memory state.
        // If append_manifest fails, the in-memory state remains
        // unchanged and the segment stays Active.
        let entry = &self.segments[&measurement][idx];
        let mut updated_entry = entry.clone();
        updated_entry.state = SegmentState::SoftDeleted {
            deleted_at_ms: now_ms,
        };
        let manifest_entry = ManifestEntry::AddSegment(updated_entry);
        self.append_manifest(&manifest_entry)?;

        // Now safe to mutate in-memory state.
        if let Some(entries) = self.segments.get_mut(&measurement) {
            entries[idx].state = SegmentState::SoftDeleted {
                deleted_at_ms: now_ms,
            };
        }
        Ok(true)
    }

    /// Get all *active* segments belonging to a specific measurement.
    ///
    /// Excludes segments that are soft-deleted (waiting for GC), and every
    /// segment of a measurement pending a hard delete — a "drop" that leaves
    /// its rows scannable for the whole grace period is not a drop. The
    /// segments themselves are untouched, so a restore before the deadline
    /// costs nothing and loses nothing.
    #[must_use]
    pub fn active_segments_for_measurement(&self, measurement: &str) -> Vec<&SegmentCatalogEntry> {
        if self.pending_measurement_drops.contains_key(measurement) {
            return Vec::new();
        }
        self.segments
            .values()
            .flat_map(|v| v.iter())
            .filter(|e| e.measurement == measurement && e.state == SegmentState::Active)
            .collect()
    }

    /// Segments that have been retired — taken out of every query — but
    /// whose files are still on disk.
    ///
    /// A retirement unlinks the file in the same step that removes the
    /// catalog entry; an entry reaches this state only when a running scan
    /// had already been handed the path, so garbage collection is what
    /// finishes the job once the reader has gone.
    #[must_use]
    pub fn retired_segments(&self) -> Vec<&SegmentCatalogEntry> {
        self.segments
            .values()
            .flat_map(|v| v.iter())
            .filter(|e| matches!(e.state, SegmentState::SoftDeleted { .. }))
            .collect()
    }

    /// Get all segments across all shards.
    #[must_use]
    pub fn all_segments(&self) -> Vec<&SegmentCatalogEntry> {
        self.segments.values().flat_map(|v| v.iter()).collect()
    }

    /// Get all segments belonging to a specific measurement.
    #[must_use]
    pub fn segments_for_measurement(&self, measurement: &str) -> Vec<&SegmentCatalogEntry> {
        self.segments
            .values()
            .flat_map(|v| v.iter())
            .filter(|e| e.measurement == measurement)
            .collect()
    }

    /// Get the total number of segments.
    /// The largest number of active segments any one `(shard, measurement)`
    /// holds — the count a compaction pass can actually reduce, and so the
    /// number write backpressure is keyed on.
    #[must_use]
    pub fn max_segments_per_shard_measurement(&self) -> usize {
        let mut counts: std::collections::HashMap<(i64, &str), usize> =
            std::collections::HashMap::new();
        for e in self.all_segments() {
            if e.state == chronix_core::SegmentState::Active {
                *counts
                    .entry((e.shard_id.0, e.measurement.as_str()))
                    .or_default() += 1;
            }
        }
        counts.into_values().max().unwrap_or(0)
    }

    /// Approximate heap bytes held by the catalog.
    ///
    /// One entry per segment, each carrying a measurement name, a path and its
    /// per-column statistics — so this grows with segment count, not with row
    /// count, and it is the term that a long-running gateway accumulates
    /// between compactions. The schema registry and the tombstone set are
    /// counted with it: all three live for the process's lifetime, which is
    /// what makes them worth a number rather than an estimate.
    #[must_use]
    pub fn memory_bytes(&self) -> usize {
        let entries: usize = self
            .segments
            .values()
            .map(|v| {
                v.capacity() * std::mem::size_of::<SegmentCatalogEntry>()
                    + v.iter()
                        .map(|e| {
                            e.measurement.capacity()
                                + e.file.as_relative().as_os_str().len()
                                + e.column_stats.capacity()
                                    * std::mem::size_of::<CatalogColumnStats>()
                                + e.column_stats
                                    .iter()
                                    .map(|c| c.name.capacity())
                                    .sum::<usize>()
                        })
                        .sum::<usize>()
            })
            .sum();

        let schemas: usize = self
            .schemas
            .iter()
            .map(|(name, ms)| {
                name.capacity()
                    + ms.columns()
                        .iter()
                        .map(|c| c.name.capacity() + std::mem::size_of_val(c))
                        .sum::<usize>()
            })
            .sum();

        entries + schemas + self.tombstones.len() * std::mem::size_of::<u64>() * 4
    }
    /// Shards holding at least one active segment.
    ///
    /// Derived rather than tracked: a `TimeIndex` was maintained beside the
    /// catalog on every flush, compaction and retirement to answer this and
    /// nothing else, and its own count drifted — a shard emptied by
    /// per-measurement retention kept its entry and stayed counted.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.segments
            .values()
            .filter(|entries| entries.iter().any(|e| e.state == SegmentState::Active))
            .count()
    }

    /// Total number of segments the catalog holds, in any state.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.values().map(Vec::len).sum()
    }

    /// Allocate the next segment ID.
    pub fn next_segment_id(&mut self) -> SegmentId {
        let id = SegmentId(self.next_segment_id);
        self.next_segment_id += 1;
        id
    }

    /// Get the current manifest sequence number.
    #[must_use]
    pub fn manifest_seq(&self) -> u64 {
        self.manifest_seq
    }

    /// Every persisted rollup, by name.
    #[must_use]
    pub fn rollups(&self) -> &BTreeMap<String, RollupRecord> {
        &self.rollups
    }

    /// Persist a rollup definition, durably.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be written or synced.
    pub fn set_rollup(&mut self, name: &str, definition: Vec<u8>) -> Result<()> {
        self.append_manifest(&ManifestEntry::SetRollup {
            name: name.to_string(),
            definition: definition.clone(),
        })?;
        self.rollups.entry(name.to_string()).or_default().definition = definition;
        self.sync_manifest()?;
        self.maybe_snapshot();
        Ok(())
    }

    /// Persist a rollup's materialisation state, durably.
    ///
    /// Called after the rollup's points are durable, never before: a
    /// watermark that outlives the rows it claims is a permanent hole in
    /// the tier, because nothing recomputes a bucket the watermark has
    /// already passed.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be written or synced.
    pub fn set_rollup_state(&mut self, name: &str, state: Vec<u8>) -> Result<()> {
        self.append_manifest(&ManifestEntry::SetRollupState {
            name: name.to_string(),
            state: state.clone(),
        })?;
        self.rollups.entry(name.to_string()).or_default().state = state;
        self.sync_manifest()?;
        self.maybe_snapshot();
        Ok(())
    }

    /// Delete a rollup definition and its state.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be written.
    pub fn remove_rollup(&mut self, name: &str) -> Result<bool> {
        if !self.rollups.contains_key(name) {
            return Ok(false);
        }
        self.append_manifest(&ManifestEntry::RemoveRollup(name.to_string()))?;
        self.rollups.remove(name);
        self.sync_manifest()?;
        self.maybe_snapshot();
        Ok(true)
    }

    /// Every measurement pending a hard delete, and the unix-ms deadline
    /// each was given.
    #[must_use]
    pub fn pending_measurement_drops(&self) -> &BTreeMap<String, u64> {
        &self.pending_measurement_drops
    }

    /// Whether `measurement` is pending a hard delete.
    #[must_use]
    pub fn is_measurement_pending_drop(&self, measurement: &str) -> bool {
        self.pending_measurement_drops.contains_key(measurement)
    }

    /// Mark `measurement` pending a hard delete at `deadline_ms`, durably.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be written or synced.
    pub fn set_measurement_pending_drop(
        &mut self,
        measurement: &str,
        deadline_ms: u64,
    ) -> Result<()> {
        self.append_manifest(&ManifestEntry::SetPendingMeasurementDrop {
            measurement: measurement.to_string(),
            deadline_ms,
        })?;
        self.pending_measurement_drops
            .insert(measurement.to_string(), deadline_ms);
        self.sync_manifest()?;
        self.maybe_snapshot();
        Ok(())
    }

    /// Cancel a pending drop \u2014 a restore, or the hard delete that finally
    /// acted on it \u2014 durably.
    ///
    /// Returns whether `measurement` was pending.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be written or synced.
    pub fn cancel_measurement_pending_drop(&mut self, measurement: &str) -> Result<bool> {
        if !self.pending_measurement_drops.contains_key(measurement) {
            return Ok(false);
        }
        self.append_manifest(&ManifestEntry::CancelPendingMeasurementDrop(
            measurement.to_string(),
        ))?;
        self.pending_measurement_drops.remove(measurement);
        self.sync_manifest()?;
        self.maybe_snapshot();
        Ok(true)
    }

    /// The tombstones recorded by deletes.
    #[must_use]
    pub fn tombstones(&self) -> &TombstoneSet {
        &self.tombstones
    }

    /// Record the tombstones produced by a delete, durably.
    ///
    /// The manifest append is fsynced before this returns, so a delete that
    /// has been acknowledged has been persisted — which is the property the
    /// previous design lacked, because it kept tombstones only in memory and
    /// in a data WAL that the next flush was free to truncate.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be written or synced.
    pub fn record_tombstones(&mut self, tombstones: &[Tombstone]) -> Result<()> {
        // One fsync for the delete, not one per tombstone: a delete over a
        // measurement with a hundred series produces a hundred of them.
        self.in_one_sync(|catalog| {
            for tombstone in tombstones {
                catalog.append_manifest(&ManifestEntry::AddTombstone(tombstone.clone()))?;
                catalog.tombstones.insert(tombstone.clone());
            }
            Ok(())
        })?;
        self.maybe_snapshot();
        Ok(())
    }

    /// Register a compaction's output and retire its inputs, as one
    /// durable step.
    ///
    /// Adds `output`, soft-deletes every input, and extends every tombstone
    /// that named an input to the output. That last part is what closes the
    /// window in which a delete issued *while* the merge ran was undone: the
    /// merge applied the tombstones it snapshotted, the late tombstone named
    /// only the inputs, and once GC removed them the tombstone was reclaimed
    /// with its rows alive in the output.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be written.
    pub fn complete_compaction(
        &mut self,
        output: SegmentCatalogEntry,
        inputs: &[SegmentId],
        now_ms: u64,
    ) -> Result<()> {
        let output_id = output.segment_id.0;
        let input_ids: Vec<u64> = inputs.iter().map(|id| id.0).collect();
        // One fsync for the whole transition, not one per input segment.
        self.in_one_sync(|catalog| {
            catalog.add_segment(output)?;
            for input in inputs {
                catalog.soft_delete_segment(*input, now_ms)?;
            }
            catalog.append_manifest(&ManifestEntry::Compacted {
                inputs: input_ids.clone(),
                output: output_id,
            })
        })?;
        self.tombstones
            .extend_to_compaction_output(&input_ids, output_id);
        self.maybe_snapshot();
        Ok(())
    }

    /// Reclaim tombstones whose segments have all left the catalog.
    ///
    /// A segment leaves the catalog only by being rewritten — compaction
    /// applies tombstones as it merges — or by being deleted outright, so once
    /// none of a tombstone's segments remains, no stored row can still match
    /// it. Returns the number reclaimed.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be written.
    pub fn reclaim_tombstones(&mut self) -> Result<usize> {
        let live: std::collections::HashSet<u64> = self
            .segments
            .values()
            .flatten()
            .map(|e| e.segment_id.0)
            .collect();

        // A tombstone with no recorded segments predates nothing we can prove
        // about, so it is kept. Every tombstone this engine writes records the
        // segments it was issued against.
        let removed = self.tombstones.retain_tombstones(|t| {
            t.segments.is_empty() || t.segments.iter().any(|id| live.contains(id))
        });

        for tombstone in &removed {
            self.append_manifest(&ManifestEntry::RemoveTombstone(tombstone.clone()))?;
        }
        if !removed.is_empty() {
            self.sync_manifest()?;
            self.maybe_snapshot();
        }
        Ok(removed.len())
    }

    /// Register or update a measurement schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be written.
    pub fn set_schema(&mut self, schema: MeasurementSchema) -> Result<()> {
        let entry = ManifestEntry::SetSchema(schema.clone());
        self.append_manifest(&entry)?;
        self.schemas
            .insert(schema.measurement().to_string(), schema);
        self.maybe_snapshot();
        Ok(())
    }

    /// Remove a measurement schema.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest cannot be written.
    pub fn remove_schema(&mut self, measurement: &str) -> Result<Option<MeasurementSchema>> {
        let entry = ManifestEntry::RemoveSchema(measurement.to_string());
        self.append_manifest(&entry)?;
        let removed = self.schemas.remove(measurement);
        self.maybe_snapshot();
        Ok(removed)
    }

    /// The WAL floor: the highest data-WAL sequence number known to be
    /// fully represented by the segments this catalog registers.
    ///
    /// `open()` replays only records above it.
    #[must_use]
    pub fn wal_floor(&self) -> u64 {
        self.wal_floor
    }

    /// Raise the WAL floor to `sequence_no`.
    ///
    /// Persisted before it is applied, like every other catalog change. A
    /// floor never moves backwards; a lower value is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest append fails.
    pub fn set_wal_floor(&mut self, sequence_no: u64) -> Result<()> {
        if sequence_no <= self.wal_floor {
            return Ok(());
        }
        self.append_manifest(&ManifestEntry::SetWalFloor(sequence_no))?;
        self.wal_floor = sequence_no;
        self.maybe_snapshot();
        Ok(())
    }

    /// Look up a measurement schema.
    #[must_use]
    pub fn get_schema(&self, measurement: &str) -> Option<&MeasurementSchema> {
        self.schemas.get(measurement)
    }

    /// Returns all schema names.
    #[must_use]
    pub fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    /// Returns an iterator over all `(name, schema)` pairs.
    pub fn all_schemas(&self) -> impl Iterator<Item = (&String, &MeasurementSchema)> {
        self.schemas.iter()
    }

    /// Force a snapshot of the current catalog state.
    ///
    /// # Errors
    ///
    /// Returns an error if the snapshot cannot be written.
    pub fn force_snapshot(&mut self) -> Result<()> {
        self.write_snapshot()?;
        self.changes_since_snapshot = 0;
        Ok(())
    }

    /// Wrap the manifest file, so a test can make the write fail.
    ///
    /// The production build hands the `File` through untouched; the test
    /// build routes it via a budget that reports `ENOSPC`, which is the
    /// failure a flash-backed gateway actually meets. Mirrors the WAL's own
    /// `sink`.
    #[cfg(not(test))]
    fn sink(file: std::fs::File) -> Box<dyn crate::durable::DurableFile> {
        Box::new(file)
    }

    /// See the production `sink`. A budget of `i64::MAX` is a file that never
    /// fills, which is what every test that is not about `ENOSPC` gets.
    #[cfg(test)]
    fn sink(file: std::fs::File) -> Box<dyn crate::durable::DurableFile> {
        let budget = MANIFEST_DISK_BUDGET.with(Clone::clone);
        Box::new(crate::durable::FullDiskFile::new(file, budget))
    }

    /// Append a manifest entry to the log.
    ///
    /// Format: `[u32-le length][postcard payload][u32-le CRC32c]`
    ///
    /// # A failed append leaves nothing behind
    ///
    /// Three `write_all` calls, and a short write inside any of them — an
    /// `ENOSPC` at the boundary — leaves a fragment. That used to stay: the
    /// next append, once a retention pass had freed space, wrote a valid
    /// record *after* it, and replay then read the fragment as the end of the
    /// log and **silently discarded every transition that followed**. The
    /// caller had been told those transitions were durable, and `open()`'s
    /// orphan sweep deletes the segment files the restored catalog no longer
    /// names.
    ///
    /// So a failed append truncates back to the last whole record, the same
    /// way `WalWriter::rewind_to_durable` does — the error the caller sees
    /// and the bytes on the disk say the same thing. The catalog is the
    /// durable record of what may be deleted; it is the last place a write
    /// may half-succeed in silence.
    fn append_manifest(&mut self, entry: &ManifestEntry) -> Result<()> {
        self.manifest_seq += 1;

        // Lazily open the WAL file handle and keep it for future appends.
        if self.manifest_wal_file.is_none() {
            let wal_path = self.manifest_dir.join("manifest.wal");
            let f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)?;
            self.manifest_good_len = f.metadata()?.len();
            self.manifest_wal_file = Some(Self::sink(f));
        }
        let file = self
            .manifest_wal_file
            .as_mut()
            .ok_or_else(|| IndexError::BinarySerialization("WAL file not opened".into()))?;

        let payload = postcard::to_stdvec(entry)
            .map_err(|e| IndexError::BinarySerialization(e.to_string()))?;
        let len = payload.len() as u32;
        let crc = crc32c::crc32c(&payload);

        let record_len = 4 + payload.len() as u64 + 4;
        let written = (|| -> std::io::Result<()> {
            file.write_all(&len.to_le_bytes())?;
            file.write_all(&payload)?;
            file.write_all(&crc.to_le_bytes())?;
            // Always flush to kernel buffers so data is ordered.
            file.flush()
        })();

        if let Err(e) = written {
            // The record is not whole. Truncate back to the last one that
            // was, so the file never holds a fragment with a valid record
            // after it — see this function's own documentation for what that
            // used to cost. `manifest_seq` is put back too: a sequence
            // consumed by a record that does not exist makes the catalog's
            // version disagree with its log.
            self.manifest_seq -= 1;
            self.rewind_manifest();
            return Err(e.into());
        }
        self.manifest_good_len += record_len;

        // Synced here unless a caller has declared that this append is one
        // step of a larger transition (`in_one_sync`), in which case that
        // caller's single `sync_manifest()` covers it. Data reaches the
        // kernel either way.
        self.pending_sync += 1;
        if self.defer_sync == 0 && self.pending_sync >= SYNC_BATCH_SIZE {
            file.sync_data()?;
            self.pending_sync = 0;
            self.manifest_syncs += 1;
            metrics::counter!("chronix_catalog_fsync_total").increment(1);
        }

        self.changes_since_snapshot += 1;
        Ok(())
    }

    /// Discard whatever a failed append left past the last whole record.
    ///
    /// The handle is dropped and reopened rather than reused: a write that
    /// failed leaves the file position where the kernel left it, and an
    /// append-mode handle would then write the *next* record after the
    /// fragment we are about to remove. Reopening also means a rewind that
    /// itself fails leaves no handle behind — the next append reopens, sees
    /// the real length, and tells the truth about where the log ends.
    fn rewind_manifest(&mut self) {
        let good = self.manifest_good_len;
        self.manifest_wal_file = None;
        self.pending_sync = 0;
        let path = self.manifest_dir.join("manifest.wal");
        match std::fs::OpenOptions::new().write(true).open(&path) {
            Ok(f) => {
                if let Err(e) = crate::durable::DurableFile::set_len(&f, good) {
                    warn!(error = %e, offset = good, "manifest rewind failed");
                } else if let Err(e) = crate::durable::DurableFile::sync_all(&f) {
                    warn!(error = %e, "manifest rewind sync failed");
                }
            }
            Err(e) => warn!(error = %e, "manifest rewind could not reopen the log"),
        }
    }

    /// How many times the manifest has been fsynced since this catalog was
    /// opened.
    ///
    /// One per catalog *transition*, not per append: a compaction retiring
    /// four segments is one, and so is a delete however many tombstones it
    /// produces. Also exported as `chronix_catalog_fsync_total`.
    #[must_use]
    pub fn manifest_syncs(&self) -> u64 {
        self.manifest_syncs
    }

    /// Run `body` as one durable manifest transition: the appends it makes are
    /// written but fsynced **once**, when it returns.
    ///
    /// A compaction of four segments is six appends — the output, four
    /// soft-deletes and the `Compacted` record — and a delete is one per
    /// tombstone. On flash-backed storage the fsync rate *is* the wear rate,
    /// which is why the WAL has a periodic policy.
    ///
    /// Everything `body` appended is durable when this returns, on the error
    /// path as much as the success path, so a transition that failed half-way
    /// is as persistent as it would be if each append had synced. A crash
    /// *during* `body` leaves what it always could: the manifest is a replayed
    /// log, so a partial transition means an input segment still live beside
    /// its compaction output, which reads deduplicate and the next compaction
    /// clears.
    ///
    /// **Only for a transition that ends in a sync**, and only where the
    /// caller does nothing irreversible until it returns: a file unlinked
    /// while the appends are still unsynced would survive a crash as a
    /// catalog entry naming a path that is gone.
    ///
    /// # Errors
    ///
    /// Returns `body`'s error, or the error of the fsync that follows it.
    pub fn in_one_sync<T>(&mut self, body: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        self.defer_sync += 1;
        let out = body(self);
        self.defer_sync -= 1;
        // Before `out?`: a half-finished transition must be as durable as it
        // was when each of its appends synced on its own.
        self.sync_manifest()?;
        out
    }

    /// Flush any pending manifest writes to durable storage.
    ///
    /// Call this after a batch of mutations to ensure all appended entries
    /// survive a power failure.  This is a no-op if there are no pending
    /// un-synced writes.
    pub fn sync_manifest(&mut self) -> Result<()> {
        if self.pending_sync > 0 {
            if let Some(ref f) = self.manifest_wal_file {
                f.sync_data()?;
            }
            self.pending_sync = 0;
            self.manifest_syncs += 1;
            metrics::counter!("chronix_catalog_fsync_total").increment(1);
        }
        Ok(())
    }

    /// Replay manifest log entries from a file.
    ///
    /// Reads length-prefixed postcard records with CRC32c integrity.
    ///
    /// # A bad record is the tail, or it is corruption
    ///
    /// The difference decides everything and it was decided by a *byte
    /// count*: "more than eight bytes remain, so this is mid-stream". A
    /// process that dies between two `write_all` calls leaves a fragment at
    /// the end of the log, which is ordinary and must be skipped; a fragment
    /// with **valid records after it** is corruption, and treating it as the
    /// tail silently discards every transition that follows. Those
    /// transitions were acknowledged, and the segment files a restored
    /// catalog no longer names are deleted by `open()`'s orphan sweep — so
    /// the quiet answer ends in deleted data, which is the one outcome a
    /// durable record of *what may be deleted* must not produce.
    ///
    /// So the question is asked directly: **is there a whole, CRC-valid
    /// record later in this file?** If there is, this is corruption and the
    /// catalog refuses to open, naming both offsets. If there is not, it is
    /// the tail, and it is skipped. Refusing is the right failure: pass 53
    /// made `open()` refuse a catalog naming a segment file that is not
    /// there, for the same reason — a catalog that is *partly* true reads as
    /// a catalog.
    fn replay_manifest(&mut self, wal_path: &Path) -> Result<()> {
        let buf = std::fs::read(wal_path)?;

        let mut replayed = 0u64;
        let mut pos = 0usize;

        while pos < buf.len() {
            match Self::decode_record(&buf, pos) {
                RecordAt::Valid { payload, next } => {
                    match postcard::from_bytes::<ManifestEntry>(&buf[payload]) {
                        Ok(entry) => {
                            self.apply_manifest_entry(entry);
                            self.manifest_seq += 1;
                            replayed += 1;
                            pos = next;
                        }
                        Err(e) => {
                            match Self::classify_bad_record(
                                &buf,
                                pos,
                                &format!("entry does not decode: {e}"),
                            ) {
                                Some(err) => return Err(err),
                                None => break,
                            }
                        }
                    }
                }
                RecordAt::Bad(reason) => match Self::classify_bad_record(&buf, pos, &reason) {
                    Some(err) => return Err(err),
                    None => break,
                },
            }
        }

        if replayed > 0 {
            debug!(replayed, "replayed manifest entries");
        }

        Ok(())
    }

    /// Decide whether a bad record at `pos` is the log's tail or corruption.
    ///
    /// `Some` when a valid record follows, so this is corruption and the
    /// catalog must refuse; `None` when nothing does, which is the tail a
    /// crash between two writes leaves and is skipped.
    fn classify_bad_record(buf: &[u8], pos: usize, reason: &str) -> Option<IndexError> {
        if let Some(next) = Self::next_valid_record(buf, pos) {
            return Some(IndexError::Manifest {
                detail: format!(
                    "mid-stream corruption in the manifest at offset {pos} ({reason}); a \
                     valid record follows at offset {next}, so this is not a \
                     truncated tail — replaying past it would silently drop every \
                     transition between them. Restore from a backup, or verify the \
                     volume."
                ),
            });
        }
        warn!(
            offset = pos,
            reason, "manifest replay: incomplete record at the tail, skipping"
        );
        metrics::counter!("chronix_catalog_tail_records_skipped_total").increment(1);
        None
    }

    /// The offset of the next whole, CRC-valid record after `pos`, if any.
    ///
    /// A byte-by-byte scan, which is affordable because it runs once, only on
    /// the corruption path, over a log bounded by the snapshot interval.
    fn next_valid_record(buf: &[u8], pos: usize) -> Option<usize> {
        (pos + 1..buf.len())
            .find(|&at| matches!(Self::decode_record(buf, at), RecordAt::Valid { .. }))
    }

    /// Read one record at `at`.
    fn decode_record(buf: &[u8], at: usize) -> RecordAt {
        let Some(len_bytes) = buf.get(at..at + 4) else {
            return RecordAt::Bad("truncated length header".into());
        };
        let payload_len =
            u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        if payload_len == 0 || payload_len > MAX_MANIFEST_RECORD_BYTES {
            return RecordAt::Bad(format!("implausible record length {payload_len}"));
        }
        let payload_start = at + 4;
        let crc_start = payload_start + payload_len;
        let Some(crc_bytes) = buf.get(crc_start..crc_start + 4) else {
            return RecordAt::Bad("record extends past the end of the log".into());
        };
        let stored = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
        let payload = payload_start..crc_start;
        if crc32c::crc32c(&buf[payload.clone()]) != stored {
            return RecordAt::Bad("CRC mismatch".into());
        }
        RecordAt::Valid {
            payload,
            next: crc_start + 4,
        }
    }

    /// Apply a manifest entry to the in-memory state (no persistence).
    fn apply_manifest_entry(&mut self, entry: ManifestEntry) {
        match entry {
            ManifestEntry::AddSegment(seg) => {
                // Track next_segment_id
                if seg.segment_id.0 >= self.next_segment_id {
                    self.next_segment_id = seg.segment_id.0 + 1;
                }
                // Dedup: remove any existing entry with the same segment_id
                // before pushing. This handles WAL replay of soft_delete_segment
                // which re-adds segments with updated state.
                let shard_entries = self.segments.entry(seg.shard_id).or_default();
                shard_entries.retain(|e| e.segment_id != seg.segment_id);
                shard_entries.push(seg);
            }
            ManifestEntry::RemoveSegment(id) => {
                for entries in self.segments.values_mut() {
                    entries.retain(|e| e.segment_id.0 != id);
                }
            }
            ManifestEntry::SetSchema(schema) => {
                self.schemas
                    .insert(schema.measurement().to_string(), schema);
            }
            ManifestEntry::RemoveSchema(name) => {
                self.schemas.remove(&name);
            }
            ManifestEntry::AddTombstone(t) => {
                self.tombstones.insert(t);
            }
            ManifestEntry::RemoveTombstone(t) => {
                self.tombstones.retain_tombstones(|existing| existing != &t);
            }
            ManifestEntry::SetWalFloor(seq) => {
                self.wal_floor = self.wal_floor.max(seq);
            }
            ManifestEntry::SetRollup { name, definition } => {
                self.rollups.entry(name).or_default().definition = definition;
            }
            ManifestEntry::RemoveRollup(name) => {
                self.rollups.remove(&name);
            }
            ManifestEntry::SetRollupState { name, state } => {
                self.rollups.entry(name).or_default().state = state;
            }
            ManifestEntry::Compacted { inputs, output } => {
                self.tombstones.extend_to_compaction_output(&inputs, output);
            }
            ManifestEntry::SetPendingMeasurementDrop {
                measurement,
                deadline_ms,
            } => {
                self.pending_measurement_drops
                    .insert(measurement, deadline_ms);
            }
            ManifestEntry::CancelPendingMeasurementDrop(measurement) => {
                self.pending_measurement_drops.remove(&measurement);
            }
        }
    }

    /// The whole catalog, encoded — everything a reopen needs.
    fn snapshot_bytes(&self) -> Result<Vec<u8>> {
        let snapshot = CatalogSnapshot {
            format_version: CATALOG_FORMAT_VERSION,
            segments: self.segments.clone(),
            schemas: self.schemas.clone(),
            tombstones: self.tombstones.clone(),
            wal_floor: self.wal_floor,
            rollups: self.rollups.clone(),
            pending_measurement_drops: self.pending_measurement_drops.clone(),
            manifest_seq: self.manifest_seq,
            next_segment_id: self.next_segment_id,
        };
        postcard::to_stdvec(&snapshot).map_err(|e| IndexError::BinarySerialization(e.to_string()))
    }

    /// Write this catalog, as it stands, as the whole catalog of a database
    /// rooted at `manifest_dir` — a directory that is *not* this one.
    ///
    /// This is what a backup captures instead of copying `catalog/` file by
    /// file. Copying was the defect: a snapshot landing mid-copy replaces
    /// `manifest.snapshot.bin` **and truncates `manifest.wal`**, so a reader
    /// walking the directory can pair the old snapshot with the emptied log
    /// and lose every transition since — silently, and reported as a
    /// successful backup. One encode of one consistent in-memory state cannot
    /// be torn, and the empty log beside it says there is nothing to replay.
    ///
    /// The caller must hold the catalog read lock for as long as it needs the
    /// segments this names to still be there.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or written.
    pub fn write_snapshot_to(&self, manifest_dir: &Path) -> Result<()> {
        std::fs::create_dir_all(manifest_dir)?;
        let data = self.snapshot_bytes()?;
        let snapshot_path = manifest_dir.join("manifest.snapshot.bin");
        let tmp_path = manifest_dir.join("manifest.snapshot.bin.tmp");
        let write_tmp = (|| -> std::io::Result<()> {
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(&data)?;
            file.sync_all()
        })();
        if let Err(e) = write_tmp {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e.into());
        }
        if let Err(e) = std::fs::rename(&tmp_path, &snapshot_path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e.into());
        }
        // An empty log, written rather than left absent: a `manifest.wal`
        // carried over from an older backup in the same directory would be
        // replayed on top of this snapshot.
        let wal = std::fs::File::create(manifest_dir.join("manifest.wal"))?;
        wal.sync_all()?;
        let dir = std::fs::File::open(manifest_dir)?;
        dir.sync_all()?;
        Ok(())
    }

    /// Write a full snapshot and truncate the WAL.
    fn write_snapshot(&mut self) -> Result<()> {
        let data = self.snapshot_bytes()?;

        // Atomic snapshot write: write temp → fsync → rename → fsync parent.
        let snapshot_path = self.manifest_dir.join("manifest.snapshot.bin");
        let tmp_path = self.manifest_dir.join("manifest.snapshot.bin.tmp");
        // The temp file is removed on failure. The segment writer and the
        // local storage backend both do this; the catalog did not, so a
        // snapshot interrupted by `ENOSPC` left a partial `.tmp` behind —
        // consuming the space whose absence caused the failure, on the one
        // file the database cannot afford to lose.
        let write_tmp = (|| -> std::io::Result<()> {
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(&data)?;
            file.sync_all()
        })();
        if let Err(e) = write_tmp {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e.into());
        }
        if let Err(e) = std::fs::rename(&tmp_path, &snapshot_path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e.into());
        }

        // Fsync parent directory to make the rename durable.
        // This MUST succeed before truncating the WAL; otherwise a crash
        // could leave us with the old snapshot and an empty WAL → data loss.
        let dir = std::fs::File::open(&self.manifest_dir).map_err(|e| {
            std::io::Error::new(
                e.kind(),
                format!("failed to open manifest dir for fsync: {e}"),
            )
        })?;
        dir.sync_all()?;

        // Truncate WAL and invalidate the persistent file handle so it
        // gets re-opened on the next append.
        let wal_path = self.manifest_dir.join("manifest.wal");
        if wal_path.exists() {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&wal_path)?;
            // The truncation must be durable too. Replay is idempotent for
            // every entry kind, so a resurrected entry is harmless — but an
            // unsynced truncation is exactly the kind of "probably fine"
            // this catalog does not deal in.
            file.sync_all()?;
        }
        self.manifest_wal_file = None;
        // The log is empty, so the rewind point is the start of it. Leaving
        // the old value would make the next failed append truncate *up*,
        // which `set_len` will happily do — extending the file with zeros
        // that replay reads as a zero-length record.
        self.manifest_good_len = 0;

        debug!(seq = self.manifest_seq, "catalog snapshot written");
        Ok(())
    }

    /// Take a snapshot if the change count exceeds the threshold.
    ///
    /// **A failed snapshot is not a failed transition**, and it must not be
    /// reported as one. The manifest append is the commit point; a snapshot
    /// only shortens the log that replay reads. Returning the snapshot's
    /// error from `add_segment` told the flush that registering the segment
    /// had failed — and the flush answers that by **deleting the segment
    /// files it just wrote**, while the catalog entry naming them is already
    /// durable. The next `open()` then refuses the database outright,
    /// because a catalog that names a file which is not there is one pass 53
    /// taught it not to trust.
    ///
    /// So it is logged and retried: `changes_since_snapshot` is left where it
    /// is, so the next mutation attempts it again, and a longer log costs
    /// replay time rather than data.
    fn maybe_snapshot(&mut self) {
        if self.changes_since_snapshot < SNAPSHOT_INTERVAL {
            return;
        }
        match self.write_snapshot() {
            Ok(()) => self.changes_since_snapshot = 0,
            Err(e) => {
                warn!(
                    error = %e,
                    changes = self.changes_since_snapshot,
                    "catalog snapshot failed; the transition is committed and the \
                     log keeps growing until one succeeds"
                );
                metrics::counter!("chronix_catalog_snapshot_failures_total").increment(1);
            }
        }
    }
}

/// Current catalog snapshot format version — 1, the first that ships.
///
/// Unlike the segment header this is compared with `>`: a snapshot from a
/// *newer* Chronix is refused, an older one is read.
const CATALOG_FORMAT_VERSION: u32 = 1;

/// Serializable snapshot of the catalog state.
#[derive(Debug, Serialize, Deserialize)]
struct CatalogSnapshot {
    /// Format version for forward compatibility.
    #[serde(default)]
    format_version: u32,
    segments: BTreeMap<ShardId, Vec<SegmentCatalogEntry>>,
    schemas: HashMap<String, MeasurementSchema>,
    /// Tombstones, as of this snapshot. A snapshot that omitted them would
    /// undo every delete the moment the manifest WAL was folded into it.
    #[serde(default)]
    tombstones: TombstoneSet,
    /// See [`SegmentCatalog::wal_floor`].
    #[serde(default)]
    wal_floor: u64,
    /// Rollup definitions and their materialisation state, by name. Opaque
    /// bytes: the catalog is where they are durable, the facade is where
    /// they mean something.
    #[serde(default)]
    rollups: BTreeMap<String, RollupRecord>,
    /// See [`SegmentCatalog::pending_measurement_drops`].
    #[serde(default)]
    pending_measurement_drops: BTreeMap<String, u64>,
    manifest_seq: u64,
    next_segment_id: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_entry(id: u64, shard: i64, min_ts: i64, max_ts: i64) -> SegmentCatalogEntry {
        SegmentCatalogEntry {
            segment_id: SegmentId(id),
            shard_id: ShardId(shard),
            measurement: "cpu".to_string(),
            file: SegmentFile::new(ShardId(shard), &format!("seg_{id}.csx")).unwrap(),
            min_timestamp: min_ts,
            max_timestamp: max_ts,
            row_count: 1000,
            series_count: 10,
            byte_size: 4096,
            row_group_count: 1,
            column_count: 5,
            column_stats: Vec::new(),
            state: SegmentState::default(),
        }
    }

    /// Set the manifest log's remaining byte budget for this thread.
    fn set_disk_budget(bytes: i64) -> std::sync::Arc<std::sync::atomic::AtomicI64> {
        MANIFEST_DISK_BUDGET.with(|b| {
            b.store(bytes, std::sync::atomic::Ordering::SeqCst);
            std::sync::Arc::clone(b)
        })
    }

    /// **A failed manifest append must lose the transition it failed on, and
    /// nothing else.**
    ///
    /// The shape this reproduces is the one an embedded gateway on flash
    /// actually meets: the disk fills, an append fails part-way, a retention
    /// pass frees space, and the next append succeeds. The fragment the
    /// failure left is then *mid-stream*, and replay used to read it as the
    /// end of the log — returning `Ok` while **silently discarding every
    /// transition after it**. Those transitions had been acknowledged, and
    /// `open()`'s orphan sweep deletes the segment files a restored catalog
    /// no longer names, so the silence ended in deleted data.
    ///
    /// Driven through the real write path rather than by writing a fragment
    /// by hand: the question is what the *writer* leaves behind, and a
    /// hand-made fragment answers a question nobody asked.
    #[test]
    fn a_failed_manifest_append_loses_only_itself() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog");
        let budget = set_disk_budget(i64::MAX);

        {
            let mut cat = SegmentCatalog::new(&path).unwrap();
            cat.add_segment(test_entry(1, 0, 0, 100)).unwrap();

            // Room for a few bytes of the next record and no more: a short
            // write inside `write_all`, which is what a real filesystem does
            // at the boundary.
            budget.store(6, std::sync::atomic::Ordering::SeqCst);
            let refused = cat.add_segment(test_entry(2, 0, 100, 200));
            assert!(refused.is_err(), "a full disk must refuse the append");

            // Retention frees space, and the next transition lands.
            budget.store(i64::MAX, std::sync::atomic::Ordering::SeqCst);
            cat.add_segment(test_entry(3, 0, 200, 300))
                .expect("an append after the disk frees up must succeed");
        }

        set_disk_budget(i64::MAX);
        let cat = SegmentCatalog::open(&path).expect("the catalog must still open");
        let ids: Vec<u64> = cat.all_segments().iter().map(|e| e.segment_id.0).collect();
        assert_eq!(
            ids,
            vec![1, 3],
            "the refused transition is gone and the two acknowledged ones are not"
        );
    }

    /// **A failed snapshot must not be reported as a failed transition.**
    ///
    /// The snapshot only shortens the log replay reads; the append is the
    /// commit point. Returning the snapshot's error from `add_segment` told
    /// the flush that registration had failed — and the flush answers that by
    /// **deleting the segment files it just wrote**, while the catalog entry
    /// naming them is already durable. The next `open()` then refuses the
    /// database outright, because a catalog naming a file that is not there
    /// is one it does not trust.
    ///
    /// The snapshot is made to fail on its own, without touching the log: a
    /// *directory* where it wants to create `manifest.snapshot.bin.tmp`. A
    /// full disk would do it too, and would also stop the append — which is a
    /// different case, and mixing them is how a test ends up proving neither.
    #[test]
    fn a_failed_snapshot_does_not_fail_the_transition() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog");
        set_disk_budget(i64::MAX);

        let mut cat = SegmentCatalog::new(&path).unwrap();
        for i in 0..SNAPSHOT_INTERVAL - 1 {
            cat.add_segment(test_entry(i + 1, 0, 0, 100)).unwrap();
        }

        // The next append takes the change count to the threshold, so the
        // snapshot runs — into this.
        std::fs::create_dir_all(path.join("manifest.snapshot.bin.tmp")).unwrap();

        let last = SNAPSHOT_INTERVAL;
        cat.add_segment(test_entry(last, 0, 100, 200))
            .expect("the append committed; only the snapshot failed");
        assert_eq!(cat.segment_count() as u64, SNAPSHOT_INTERVAL);

        // And it is durable: every transition replays, because the log was
        // never truncated by the snapshot that did not happen.
        drop(cat);
        std::fs::remove_dir_all(path.join("manifest.snapshot.bin.tmp")).unwrap();
        let reopened = SegmentCatalog::open(&path).expect("the catalog must open");
        assert_eq!(
            reopened.segment_count() as u64,
            SNAPSHOT_INTERVAL,
            "every acknowledged transition survives a failed snapshot"
        );
    }

    /// The in-memory catalog and the log agree after a refusal.
    ///
    /// Write-ahead order already gave this — `append_manifest` runs before
    /// the in-memory mutation — so it is pinned rather than fixed: a later
    /// edit that mutates first would make the running process disagree with
    /// its own restart, which is the hardest class of bug to see.
    #[test]
    fn a_refused_append_leaves_the_in_memory_catalog_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("catalog");
        let budget = set_disk_budget(i64::MAX);

        let mut cat = SegmentCatalog::new(&path).unwrap();
        cat.add_segment(test_entry(1, 0, 0, 100)).unwrap();
        let before = cat.manifest_seq();

        budget.store(6, std::sync::atomic::Ordering::SeqCst);
        assert!(cat.add_segment(test_entry(2, 0, 100, 200)).is_err());
        budget.store(i64::MAX, std::sync::atomic::Ordering::SeqCst);

        assert_eq!(
            cat.all_segments().len(),
            1,
            "a refused append must not be visible in memory"
        );
        assert_eq!(
            cat.manifest_seq(),
            before,
            "a sequence consumed by a record that does not exist makes the \
             catalog's version disagree with its log"
        );
    }

    /// A compaction is **one** fsync, whatever it retires.
    ///
    /// It used to be one per append: the output, one per input segment, and
    /// the `Compacted` record — six for a four-segment merge, and compaction
    /// runs every maintenance interval. On the flash the design partner
    /// writes to, the fsync rate is the wear rate.
    #[test]
    fn a_compaction_costs_one_manifest_fsync() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = SegmentCatalog::new(dir.path()).unwrap();

        let inputs: Vec<SegmentId> = (1..=4i64)
            .map(|i| {
                #[allow(clippy::cast_sign_loss)]
                let e = test_entry(i as u64, 0, i * 100, i * 100 + 50);
                let id = e.segment_id;
                catalog.add_segment(e).unwrap();
                id
            })
            .collect();

        let before = catalog.manifest_syncs();
        catalog
            .complete_compaction(test_entry(99, 0, 100, 450), &inputs, 0)
            .unwrap();
        assert_eq!(
            catalog.manifest_syncs() - before,
            1,
            "one transition, one fsync — it was six"
        );

        // …and it is durable: a reopen sees the output live and the inputs
        // soft-deleted, which is the property the per-append fsync bought.
        drop(catalog);
        let reopened = SegmentCatalog::open(dir.path()).unwrap();
        let live: Vec<u64> = reopened
            .active_segments_for_measurement("cpu")
            .iter()
            .map(|e| e.segment_id.0)
            .collect();
        assert_eq!(live, vec![99], "only the compaction output survives");
    }

    /// A delete is one fsync, not one per tombstone.
    #[test]
    fn a_delete_costs_one_manifest_fsync() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = SegmentCatalog::new(dir.path()).unwrap();
        let tombstones: Vec<Tombstone> = (0..25)
            .map(|i| Tombstone::ranged(format!("cpu,host=h{i}"), 0, 1_000))
            .collect();

        let before = catalog.manifest_syncs();
        catalog.record_tombstones(&tombstones).unwrap();
        assert_eq!(
            catalog.manifest_syncs() - before,
            1,
            "25 tombstones, 1 fsync"
        );

        drop(catalog);
        let reopened = SegmentCatalog::open(dir.path()).unwrap();
        assert_eq!(
            reopened.tombstones().len(),
            25,
            "every tombstone survived the reopen"
        );
    }

    /// A single mutation still syncs on its own — the batching is opt-in, and
    /// the retention, GC and cold-tier loops rely on that.
    #[test]
    fn a_lone_append_still_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = SegmentCatalog::new(dir.path()).unwrap();
        let before = catalog.manifest_syncs();
        catalog.add_segment(test_entry(1, 0, 100, 200)).unwrap();
        assert_eq!(catalog.manifest_syncs() - before, 1);
        catalog.soft_delete_segment(SegmentId(1), 0).unwrap();
        assert_eq!(catalog.manifest_syncs() - before, 2);
    }

    #[test]
    fn add_and_get_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = SegmentCatalog::new(dir.path()).unwrap();

        catalog.add_segment(test_entry(1, 0, 100, 200)).unwrap();
        catalog.add_segment(test_entry(2, 0, 300, 400)).unwrap();
        catalog.add_segment(test_entry(3, 1, 100, 200)).unwrap();

        assert_eq!(catalog.get_segments_for_shard(&ShardId(0)).len(), 2);
        assert_eq!(catalog.get_segments_for_shard(&ShardId(1)).len(), 1);
        assert_eq!(catalog.get_segments_for_shard(&ShardId(99)).len(), 0);
        assert_eq!(catalog.segment_count(), 3);
    }

    #[test]
    fn remove_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = SegmentCatalog::new(dir.path()).unwrap();

        catalog.add_segment(test_entry(1, 0, 100, 200)).unwrap();
        catalog.add_segment(test_entry(2, 0, 300, 400)).unwrap();

        let removed = catalog.remove_segment(SegmentId(1)).unwrap();
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().segment_id, SegmentId(1));
        assert_eq!(catalog.segment_count(), 1);

        // Remove non-existent
        let removed = catalog.remove_segment(SegmentId(99)).unwrap();
        assert!(removed.is_none());
    }

    #[test]
    fn persist_and_reload() {
        let dir = tempfile::tempdir().unwrap();

        // Create catalog and add segments
        {
            let mut catalog = SegmentCatalog::new(dir.path()).unwrap();
            catalog.add_segment(test_entry(1, 0, 100, 200)).unwrap();
            catalog.add_segment(test_entry(2, 0, 300, 400)).unwrap();
            catalog.add_segment(test_entry(3, 1, 500, 600)).unwrap();
        }

        // Reload from manifest
        let catalog = SegmentCatalog::open(dir.path()).unwrap();
        assert_eq!(catalog.segment_count(), 3);
        assert_eq!(catalog.get_segments_for_shard(&ShardId(0)).len(), 2);
        assert_eq!(catalog.get_segments_for_shard(&ShardId(1)).len(), 1);
    }

    #[test]
    fn snapshot_and_reload() {
        let dir = tempfile::tempdir().unwrap();

        // Create, populate, snapshot
        {
            let mut catalog = SegmentCatalog::new(dir.path()).unwrap();
            catalog.add_segment(test_entry(1, 0, 100, 200)).unwrap();
            catalog.add_segment(test_entry(2, 0, 300, 400)).unwrap();
            catalog.force_snapshot().unwrap();
            // Add more after snapshot
            catalog.add_segment(test_entry(3, 1, 500, 600)).unwrap();
        }

        // Reload—should see all 3 from snapshot + WAL
        let catalog = SegmentCatalog::open(dir.path()).unwrap();
        assert_eq!(catalog.segment_count(), 3);
    }

    #[test]
    fn schema_persistence() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut catalog = SegmentCatalog::new(dir.path()).unwrap();
            let schema = MeasurementSchema::new("cpu");
            catalog.set_schema(schema).unwrap();
        }

        let catalog = SegmentCatalog::open(dir.path()).unwrap();
        assert!(catalog.get_schema("cpu").is_some());
        assert_eq!(catalog.get_schema("cpu").unwrap().measurement(), "cpu");
    }

    #[test]
    fn remove_schema() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = SegmentCatalog::new(dir.path()).unwrap();
        catalog.set_schema(MeasurementSchema::new("cpu")).unwrap();
        assert!(catalog.get_schema("cpu").is_some());

        catalog.remove_schema("cpu").unwrap();
        assert!(catalog.get_schema("cpu").is_none());
    }

    #[test]
    fn next_segment_id_monotonic() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = SegmentCatalog::new(dir.path()).unwrap();

        assert_eq!(catalog.next_segment_id(), SegmentId(1));
        assert_eq!(catalog.next_segment_id(), SegmentId(2));
        assert_eq!(catalog.next_segment_id(), SegmentId(3));
    }

    #[test]
    fn next_segment_id_survives_reload() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut catalog = SegmentCatalog::new(dir.path()).unwrap();
            catalog.add_segment(test_entry(5, 0, 100, 200)).unwrap();
            catalog.add_segment(test_entry(10, 0, 300, 400)).unwrap();
        }

        let mut catalog = SegmentCatalog::open(dir.path()).unwrap();
        // next_segment_id should be 11 (max existing + 1)
        let next = catalog.next_segment_id();
        assert!(
            next.0 >= 11,
            "next_segment_id should be >= 11, got {}",
            next.0
        );
    }

    #[test]
    fn add_remove_add_roundtrip() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut catalog = SegmentCatalog::new(dir.path()).unwrap();
            catalog.add_segment(test_entry(1, 0, 100, 200)).unwrap();
            catalog.remove_segment(SegmentId(1)).unwrap();
            catalog.add_segment(test_entry(2, 0, 300, 400)).unwrap();
        }

        let catalog = SegmentCatalog::open(dir.path()).unwrap();
        assert_eq!(catalog.segment_count(), 1);
        assert_eq!(
            catalog.get_segments_for_shard(&ShardId(0))[0].segment_id,
            SegmentId(2)
        );
    }

    #[test]
    fn all_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = SegmentCatalog::new(dir.path()).unwrap();

        catalog.add_segment(test_entry(1, 0, 100, 200)).unwrap();
        catalog.add_segment(test_entry(2, 1, 300, 400)).unwrap();
        catalog.add_segment(test_entry(3, 2, 500, 600)).unwrap();

        let all = catalog.all_segments();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn manifest_replay_tail_truncation_tolerated() {
        // Corrupt entries at the end (tail truncation from a crash) should be
        // skipped with a warning — not an error.
        let dir = tempfile::tempdir().unwrap();

        {
            let mut catalog = SegmentCatalog::new(dir.path()).unwrap();
            catalog.add_segment(test_entry(1, 0, 100, 200)).unwrap();
            catalog.add_segment(test_entry(2, 0, 300, 400)).unwrap();
        }

        // Append a truncated binary frame at the tail to simulate a crash mid-write.
        let wal_path = dir.path().join("manifest.wal");
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();
        // Write a length header but no payload — simulates crash after partial write
        let fake_len: u32 = 128;
        f.write_all(&fake_len.to_le_bytes()).unwrap();
        f.write_all(&[0xDE, 0xAD]).unwrap(); // partial payload
        drop(f);

        // Replay should succeed — tail corruption is tolerated.
        let catalog = SegmentCatalog::open(dir.path()).unwrap();
        assert_eq!(catalog.segment_count(), 2);
    }

    #[test]
    fn manifest_replay_midstream_corruption_errors() {
        // A corrupt entry in the middle (valid entries follow it) is real
        // corruption and must cause an error.
        let dir = tempfile::tempdir().unwrap();

        {
            let mut catalog = SegmentCatalog::new(dir.path()).unwrap();
            catalog.add_segment(test_entry(1, 0, 100, 200)).unwrap();
        }

        // Write a corrupt binary frame (valid length, bad CRC) followed by a valid frame.
        let wal_path = dir.path().join("manifest.wal");
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap();

        // Corrupt frame: valid-looking length, garbage payload, wrong CRC
        let corrupt_payload = b"CORRUPT_DATA_HERE_1234";
        let corrupt_len = corrupt_payload.len() as u32;
        let wrong_crc: u32 = 0xDEADBEEF;
        f.write_all(&corrupt_len.to_le_bytes()).unwrap();
        f.write_all(corrupt_payload).unwrap();
        f.write_all(&wrong_crc.to_le_bytes()).unwrap();

        // Valid frame after the corrupt one
        let valid_entry = ManifestEntry::AddSegment(test_entry(3, 0, 500, 600));
        let valid_payload = postcard::to_stdvec(&valid_entry).unwrap();
        let valid_len = valid_payload.len() as u32;
        let valid_crc = crc32c::crc32c(&valid_payload);
        f.write_all(&valid_len.to_le_bytes()).unwrap();
        f.write_all(&valid_payload).unwrap();
        f.write_all(&valid_crc.to_le_bytes()).unwrap();
        drop(f);

        // Replay must refuse, and say enough to act on: *where* the damage
        // is and *where* the next good record is. A refusal an operator
        // cannot locate is a refusal they can only answer by restoring.
        let result = SegmentCatalog::open(dir.path());
        assert!(result.is_err(), "expected error for mid-stream corruption");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("mid-stream corruption"),
            "error should mention mid-stream corruption, got: {err_msg}"
        );
        assert!(
            err_msg.contains("a valid record follows at offset"),
            "the error must name the offset that proves this is not a tail: {err_msg}"
        );
    }
}
