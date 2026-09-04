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
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::segment::stats::ColumnStats;
use chronix_core::{
    MeasurementSchema, SegmentId, SegmentState, ShardId, Timestamp, Tombstone, TombstoneSet,
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
    /// Filesystem path to the `.csx` file.
    pub path: PathBuf,
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
    /// Monotonically increasing catalog version.
    manifest_seq: u64,
    /// Directory for manifest files.
    manifest_dir: PathBuf,
    /// Number of changes since last snapshot.
    changes_since_snapshot: u64,
    /// Next segment ID to assign.
    next_segment_id: u64,
    /// Persistent WAL file handle (kept open for append).
    manifest_wal_file: Option<std::fs::File>,
    /// Number of appends since last `sync_data()`. When this reaches
    /// [`SYNC_BATCH_SIZE`], an automatic fsync is issued. Callers can
    /// also call [`sync_manifest()`](Self::sync_manifest) explicitly.
    pending_sync: u64,
}

/// Snapshot interval: take a snapshot every N changes.
const SNAPSHOT_INTERVAL: u64 = 1000;

/// Sync on every manifest append to guarantee zero
/// catalog mutations are lost on power failure. Catalog mutations
/// (segment register, soft-delete, schema update) are infrequent
/// enough that the extra fsync cost is negligible.
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
            manifest_seq: 0,
            manifest_dir,
            changes_since_snapshot: 0,
            next_segment_id: 1,
            manifest_wal_file: None,
            pending_sync: 0,
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
                manifest_seq: snapshot.manifest_seq,
                manifest_dir: manifest_dir.clone(),
                changes_since_snapshot: 0,
                next_segment_id: snapshot.next_segment_id,
                manifest_wal_file: None,
                pending_sync: 0,
            }
        } else {
            Self {
                segments: BTreeMap::new(),
                schemas: HashMap::new(),
                tombstones: TombstoneSet::new(),
                wal_floor: 0,
                rollups: BTreeMap::new(),
                manifest_seq: 0,
                manifest_dir: manifest_dir.clone(),
                changes_since_snapshot: 0,
                next_segment_id: 1,
                manifest_wal_file: None,
                pending_sync: 0,
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

        self.maybe_snapshot()?;
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
                self.maybe_snapshot()?;
                return Ok(Some(removed));
            }
        }

        self.maybe_snapshot()?;
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
    /// Excludes segments that are soft-deleted (waiting for GC).
    #[must_use]
    pub fn active_segments_for_measurement(&self, measurement: &str) -> Vec<&SegmentCatalogEntry> {
        self.segments
            .values()
            .flat_map(|v| v.iter())
            .filter(|e| e.measurement == measurement && e.state == SegmentState::Active)
            .collect()
    }

    /// Get segments that are past the soft-delete grace period and can
    /// be hard-deleted.
    #[must_use]
    pub fn expired_soft_deleted(
        &self,
        now_ms: u64,
        grace_period_ms: u64,
    ) -> Vec<&SegmentCatalogEntry> {
        self.segments
            .values()
            .flat_map(|v| v.iter())
            .filter(|e| {
                if let SegmentState::SoftDeleted { deleted_at_ms } = e.state {
                    now_ms.saturating_sub(deleted_at_ms) >= grace_period_ms
                } else {
                    false
                }
            })
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
                                + e.path.as_os_str().len()
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
        self.maybe_snapshot()?;
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
        self.maybe_snapshot()?;
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
        self.maybe_snapshot()?;
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
        for tombstone in tombstones {
            self.append_manifest(&ManifestEntry::AddTombstone(tombstone.clone()))?;
            self.tombstones.insert(tombstone.clone());
        }
        self.sync_manifest()?;
        self.maybe_snapshot()?;
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
        self.add_segment(output)?;
        for input in inputs {
            self.soft_delete_segment(*input, now_ms)?;
        }
        let inputs: Vec<u64> = inputs.iter().map(|id| id.0).collect();
        self.append_manifest(&ManifestEntry::Compacted {
            inputs: inputs.clone(),
            output: output_id,
        })?;
        self.tombstones
            .extend_to_compaction_output(&inputs, output_id);
        self.sync_manifest()?;
        self.maybe_snapshot()?;
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
            self.maybe_snapshot()?;
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
        self.maybe_snapshot()?;
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
        self.maybe_snapshot()?;
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
        self.maybe_snapshot()?;
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

    /// Append a manifest entry to the WAL file.
    ///
    /// Format: `[u32-le length][postcard payload][u32-le CRC32c]`
    fn append_manifest(&mut self, entry: &ManifestEntry) -> Result<()> {
        self.manifest_seq += 1;

        // Lazily open the WAL file handle and keep it for future appends.
        if self.manifest_wal_file.is_none() {
            let wal_path = self.manifest_dir.join("manifest.wal");
            let f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&wal_path)?;
            self.manifest_wal_file = Some(f);
        }
        let file = self
            .manifest_wal_file
            .as_mut()
            .ok_or_else(|| IndexError::BinarySerialization("WAL file not opened".into()))?;

        let payload = postcard::to_stdvec(entry)
            .map_err(|e| IndexError::BinarySerialization(e.to_string()))?;
        let len = payload.len() as u32;
        let crc = crc32c::crc32c(&payload);
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&payload)?;
        file.write_all(&crc.to_le_bytes())?;
        // Always flush to kernel buffers so data is ordered.
        file.flush()?;

        // Batch sync — only issue sync_data() every SYNC_BATCH_SIZE
        // appends to reduce fsync syscall overhead during compaction/flush
        // bursts.  Data is still flushed to kernel buffers on every write.
        self.pending_sync += 1;
        if self.pending_sync >= SYNC_BATCH_SIZE {
            file.sync_data()?;
            self.pending_sync = 0;
        }

        self.changes_since_snapshot += 1;
        Ok(())
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
        }
        Ok(())
    }

    /// Replay manifest WAL entries from a file.
    ///
    /// Reads length-prefixed postcard records with CRC32c integrity.
    /// Truncated tail records (common crash artefact) are skipped
    /// with a warning. Mid-stream corruption returns an error.
    fn replay_manifest(&mut self, wal_path: &Path) -> Result<()> {
        let mut file = std::fs::File::open(wal_path)?;
        let file_len = file.metadata()?.len();

        let mut replayed = 0u64;
        let mut tail_skipped = 0u64;
        let mut pos = 0u64;

        loop {
            if pos >= file_len {
                break;
            }
            // Read length header (4 bytes)
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    tail_skipped += 1;
                    warn!(
                        offset = pos,
                        "manifest replay: truncated length header at tail"
                    );
                    break;
                }
                Err(e) => return Err(e.into()),
            }
            let payload_len = u32::from_le_bytes(len_buf) as usize;
            pos += 4;

            // Sanity check payload length
            if payload_len == 0 || payload_len > 64 * 1024 * 1024 {
                if pos + (payload_len as u64) + 4 > file_len {
                    tail_skipped += 1;
                    warn!(
                        offset = pos - 4,
                        payload_len, "manifest replay: truncated entry at tail"
                    );
                    break;
                }
                return Err(IndexError::Manifest {
                    detail: format!(
                        "invalid manifest entry length {payload_len} at offset {}",
                        pos - 4
                    ),
                });
            }

            // Read payload + CRC
            let total = payload_len + 4; // payload + CRC32c
            if pos + total as u64 > file_len {
                tail_skipped += 1;
                warn!(
                    offset = pos - 4,
                    "manifest replay: truncated payload at tail"
                );
                break;
            }

            let mut buf = vec![0u8; total];
            if let Err(e) = file.read_exact(&mut buf) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    tail_skipped += 1;
                    warn!(offset = pos - 4, "manifest replay: truncated read at tail");
                    break;
                }
                return Err(e.into());
            }

            let payload = &buf[..payload_len];
            let stored_crc = u32::from_le_bytes([
                buf[payload_len],
                buf[payload_len + 1],
                buf[payload_len + 2],
                buf[payload_len + 3],
            ]);
            let computed_crc = crc32c::crc32c(payload);

            if stored_crc != computed_crc {
                // Check if there are more valid records after this
                let remaining = file_len - pos - total as u64;
                if remaining > 8 {
                    return Err(IndexError::Manifest {
                        detail: format!(
                            "CRC mismatch at offset {} (stored={stored_crc:#x}, computed={computed_crc:#x}) — mid-stream corruption",
                            pos - 4,
                        ),
                    });
                }
                tail_skipped += 1;
                warn!(
                    offset = pos - 4,
                    "manifest replay: CRC mismatch at tail, skipping"
                );
                break;
            }

            match postcard::from_bytes::<ManifestEntry>(payload) {
                Ok(entry) => {
                    self.apply_manifest_entry(entry);
                    self.manifest_seq += 1;
                    replayed += 1;
                }
                Err(e) => {
                    // Check if remaining data could contain valid entries
                    let remaining = file_len - pos - total as u64;
                    if remaining > 8 {
                        return Err(IndexError::Manifest {
                            detail: format!("deserialization error at offset {}: {e}", pos - 4,),
                        });
                    }
                    tail_skipped += 1;
                    warn!(offset = pos - 4, error = %e, "manifest replay: skipping corrupt tail entry");
                    break;
                }
            }

            pos += total as u64;
        }

        if tail_skipped > 0 {
            warn!(
                tail_skipped,
                "manifest replay: truncated tail entries skipped"
            );
        }
        if replayed > 0 {
            debug!(replayed, "replayed manifest entries");
        }

        Ok(())
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
        }
    }

    /// Write a full snapshot and truncate the WAL.
    fn write_snapshot(&mut self) -> Result<()> {
        let snapshot = CatalogSnapshot {
            format_version: CATALOG_FORMAT_VERSION,
            segments: self.segments.clone(),
            schemas: self.schemas.clone(),
            tombstones: self.tombstones.clone(),
            wal_floor: self.wal_floor,
            rollups: self.rollups.clone(),
            manifest_seq: self.manifest_seq,
            next_segment_id: self.next_segment_id,
        };

        let data = postcard::to_stdvec(&snapshot)
            .map_err(|e| IndexError::BinarySerialization(e.to_string()))?;

        // Atomic snapshot write: write temp → fsync → rename → fsync parent.
        let snapshot_path = self.manifest_dir.join("manifest.snapshot.bin");
        let tmp_path = self.manifest_dir.join("manifest.snapshot.bin.tmp");
        {
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(&data)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp_path, &snapshot_path)?;

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

        debug!(seq = self.manifest_seq, "catalog snapshot written");
        Ok(())
    }

    /// Take a snapshot if the change count exceeds the threshold.
    fn maybe_snapshot(&mut self) -> Result<()> {
        if self.changes_since_snapshot >= SNAPSHOT_INTERVAL {
            self.write_snapshot()?;
            self.changes_since_snapshot = 0;
        }
        Ok(())
    }
}

/// Current catalog snapshot format version.
const CATALOG_FORMAT_VERSION: u32 = 2;

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
            path: PathBuf::from(format!("shard_{shard}/seg_{id}.csx")),
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

        // Replay should fail with a Manifest error.
        let result = SegmentCatalog::open(dir.path());
        assert!(result.is_err(), "expected error for mid-stream corruption");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("mid-stream corruption"),
            "error should mention mid-stream corruption, got: {err_msg}"
        );
    }
}
