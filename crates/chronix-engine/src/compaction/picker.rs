//! Compaction picker — decides which segments to compact.
//!
//! Implements a **hybrid** compaction strategy combining:
//!
//! - **TWCS** (Time-Window Compaction Strategy) — groups segments by time
//!   shard and triggers compaction when L0 count exceeds a threshold.
//! - **Size-tiered merging** — within each time window, groups segments
//!   by size tier and merges segments in the same tier when their count
//!   reaches a configurable threshold.
//!
//! Both strategies respect shard boundaries (never compact across shards)
//! and support multi-level tiering (L0→L1→L2).
//!
//! A **write-amplification budget** tracks cumulative bytes rewritten per
//! compaction cycle. Low-priority merges are deferred when the budget is
//! exhausted, preventing runaway background I/O.
//!
//! Compaction policies are **configurable per namespace** via
//! [`CompactionPolicy`].

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::index::SegmentCatalogEntry;
use chronix_core::ShardId;

// Re-export `SegmentState` from `chronix_core` for backward compatibility.
pub use chronix_core::SegmentState;

/// Compaction level for segment tiering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CompactionLevel {
    /// Level 0: direct memtable flushes.
    L0 = 0,
    /// Level 1: first compaction pass.
    L1 = 1,
    /// Level 2: full shard optimization.
    L2 = 2,
}

impl CompactionLevel {
    /// Returns the next compaction level.
    #[must_use]
    pub fn next(self) -> Self {
        match self {
            Self::L0 => Self::L1,
            Self::L1 | Self::L2 => Self::L2,
        }
    }
}

impl std::fmt::Display for CompactionLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::L0 => write!(f, "L0"),
            Self::L1 => write!(f, "L1"),
            Self::L2 => write!(f, "L2"),
        }
    }
}

// ---------------------------------------------------------------------------
// Size-tier classification
// ---------------------------------------------------------------------------

/// Size tier for grouping segments by byte size.
///
/// Used by the size-tiered compaction pass to group similarly-sized
/// segments together, preventing merges of very small segments with very
/// large ones (which would waste I/O bandwidth).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SizeTier {
    /// Segments < 1 MB.
    Tiny,
    /// Segments 1–10 MB.
    Small,
    /// Segments 10–100 MB.
    Medium,
    /// Segments ≥ 100 MB.
    Large,
}

impl SizeTier {
    /// Classify a segment by its byte size.
    #[must_use]
    pub fn classify(byte_size: u64) -> Self {
        match byte_size {
            0..1_048_576 => Self::Tiny,              // < 1 MB
            1_048_576..10_485_760 => Self::Small,    // 1–10 MB
            10_485_760..104_857_600 => Self::Medium, // 10–100 MB
            _ => Self::Large,                        // ≥ 100 MB
        }
    }
}

impl std::fmt::Display for SizeTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tiny => write!(f, "Tiny(<1MB)"),
            Self::Small => write!(f, "Small(1-10MB)"),
            Self::Medium => write!(f, "Medium(10-100MB)"),
            Self::Large => write!(f, "Large(≥100MB)"),
        }
    }
}

// ---------------------------------------------------------------------------
// Compaction policy
// ---------------------------------------------------------------------------

/// Compaction strategy selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CompactionStrategy {
    /// Pure TWCS — count-based trigger only (legacy).
    Twcs,
    /// Size-tiered only — groups by segment size.
    SizeTiered,
    /// Hybrid TWCS + size-tiered (recommended default).
    #[default]
    Hybrid,
}

/// Per-namespace compaction policy configuration.
///
/// Controls how the compaction picker selects candidate segments for
/// merging. Every setting has a sensible default; namespaces without
/// an explicit policy inherit the global default.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionPolicy {
    /// Which strategy to use (default: Hybrid).
    pub strategy: CompactionStrategy,
    /// Minimum segments in a count group to trigger TWCS compaction (default: 4).
    pub count_trigger_threshold: usize,
    /// Minimum segments in a size tier to trigger size-tiered merge (default: 4).
    pub size_tier_threshold: usize,
    /// Maximum write amplification ratio per cycle (default: 10.0).
    /// Compaction tasks are deferred when cumulative bytes rewritten
    /// exceed `max_write_amplification * total_input_bytes`.
    pub max_write_amplification: f64,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            strategy: CompactionStrategy::default(),
            count_trigger_threshold: 4,
            size_tier_threshold: 4,
            max_write_amplification: 10.0,
        }
    }
}

impl CompactionPolicy {
    /// Clamp all thresholds to valid ranges.
    ///
    /// - `count_trigger_threshold` and `size_tier_threshold` are clamped to `[2, 1000]`.
    /// - `max_write_amplification` is clamped to `[1.0, 1000.0]`.
    #[must_use]
    pub fn validated(mut self) -> Self {
        self.count_trigger_threshold = self.count_trigger_threshold.clamp(2, 1000);
        self.size_tier_threshold = self.size_tier_threshold.clamp(2, 1000);
        self.max_write_amplification = self.max_write_amplification.clamp(1.0, 1000.0);
        self
    }
}

// ---------------------------------------------------------------------------
// Write-amplification tracker
// ---------------------------------------------------------------------------

/// Tracks cumulative bytes rewritten per compaction cycle for
/// write-amplification budgeting.
///
/// Thread-safe via atomics; designed to be shared across concurrent
/// compaction tasks.
#[derive(Debug)]
pub struct WriteAmpTracker {
    /// Total input bytes across all compaction tasks in this cycle.
    total_input_bytes: AtomicU64,
    /// Total bytes rewritten (output) across all tasks.
    total_output_bytes: AtomicU64,
}

impl Default for WriteAmpTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteAmpTracker {
    /// Create a fresh tracker for a new compaction cycle.
    #[must_use]
    pub fn new() -> Self {
        Self {
            total_input_bytes: AtomicU64::new(0),
            total_output_bytes: AtomicU64::new(0),
        }
    }

    /// Record bytes processed by a compaction task.
    pub fn record(&self, input_bytes: u64, output_bytes: u64) {
        self.total_input_bytes
            .fetch_add(input_bytes, Ordering::Relaxed);
        self.total_output_bytes
            .fetch_add(output_bytes, Ordering::Relaxed);
    }

    /// Current write amplification ratio (output / input).
    ///
    /// Returns 0.0 if no input has been recorded.
    #[must_use]
    pub fn write_amplification(&self) -> f64 {
        let input = self.total_input_bytes.load(Ordering::Relaxed);
        let output = self.total_output_bytes.load(Ordering::Relaxed);
        if input == 0 {
            return 0.0;
        }
        output as f64 / input as f64
    }

    /// Returns `true` if the budget is exhausted.
    #[must_use]
    pub fn budget_exhausted(&self, max_ratio: f64) -> bool {
        self.write_amplification() > max_ratio
    }

    /// Total input bytes recorded.
    #[must_use]
    pub fn input_bytes(&self) -> u64 {
        self.total_input_bytes.load(Ordering::Relaxed)
    }

    /// Total output bytes recorded.
    #[must_use]
    pub fn output_bytes(&self) -> u64 {
        self.total_output_bytes.load(Ordering::Relaxed)
    }

    /// Reset the tracker for a new cycle.
    pub fn reset(&self) {
        self.total_input_bytes.store(0, Ordering::Relaxed);
        self.total_output_bytes.store(0, Ordering::Relaxed);
    }
}

/// A compaction task describing which segments to merge.
#[derive(Debug, Clone)]
pub struct CompactionTask {
    /// The shard being compacted.
    pub shard_id: ShardId,
    /// Input segments to merge.
    pub input_segments: Vec<SegmentCatalogEntry>,
    /// Target compaction level for the output.
    pub target_level: CompactionLevel,
    /// Source compaction level of the inputs.
    pub source_level: CompactionLevel,
    /// Output path for the compacted segment.
    pub output_path: PathBuf,
}

impl CompactionTask {
    /// Total input bytes for this task.
    #[must_use]
    pub fn total_input_bytes(&self) -> u64 {
        self.input_segments.iter().map(|s| s.byte_size).sum()
    }
}

/// Hybrid compaction picker combining TWCS and size-tiered strategies.
///
/// The picker operates in two passes:
///
/// 1. **TWCS pass** — groups segments by `(shard_id, measurement)` and
///    triggers compaction when the count exceeds `count_trigger_threshold`.
///
/// 2. **Size-tiered pass** — within each `(shard_id, measurement)` group,
///    classifies segments by [`SizeTier`] and triggers merges when a
///    tier's count exceeds `size_tier_threshold`.
///
/// The [`CompactionPolicy`] selects which passes run:
/// - `Twcs` → TWCS pass only
/// - `SizeTiered` → size-tiered pass only
/// - `Hybrid` → both passes, TWCS first (deduplicates tasks)
#[derive(Debug, Clone, Default)]
pub struct CompactionPicker {
    /// Active compaction policy.
    policy: CompactionPolicy,
}

impl CompactionPicker {
    /// Create a picker with the given count-based trigger threshold.
    ///
    /// Uses the default policy with `Hybrid` strategy.
    #[must_use]
    pub fn new(trigger_threshold: usize) -> Self {
        Self {
            policy: CompactionPolicy {
                count_trigger_threshold: trigger_threshold,
                size_tier_threshold: trigger_threshold,
                ..CompactionPolicy::default()
            },
        }
    }

    /// Create a picker from an explicit [`CompactionPolicy`].
    #[must_use]
    pub fn with_policy(policy: CompactionPolicy) -> Self {
        Self { policy }
    }

    /// Returns the active compaction policy.
    #[must_use]
    pub fn policy(&self) -> &CompactionPolicy {
        &self.policy
    }

    /// Returns the count-based trigger threshold.
    #[must_use]
    pub fn trigger_threshold(&self) -> usize {
        self.policy.count_trigger_threshold
    }

    /// Scan a set of segments and produce compaction tasks.
    ///
    /// Uses the configured policy to run TWCS, size-tiered, or hybrid
    /// compaction. Groups segments by `(shard_id, measurement)`.
    ///
    /// Segments in `Compacting` or `SoftDeleted` state should be filtered
    /// out before calling this method.
    #[must_use]
    pub fn pick(
        &self,
        segments: &[SegmentCatalogEntry],
        output_dir: &std::path::Path,
    ) -> Vec<CompactionTask> {
        self.pick_level(segments, output_dir, CompactionLevel::L0)
    }

    /// Scan segments at a specific compaction level and produce tasks.
    ///
    /// Enables multi-level compaction (L0→L1, L1→L2).
    #[must_use]
    pub fn pick_level(
        &self,
        segments: &[SegmentCatalogEntry],
        output_dir: &std::path::Path,
        source_level: CompactionLevel,
    ) -> Vec<CompactionTask> {
        let target_level = source_level.next();

        // Group by (shard_id, measurement)
        let mut groups: BTreeMap<(ShardId, &str), Vec<&SegmentCatalogEntry>> = BTreeMap::new();
        for seg in segments {
            groups
                .entry((seg.shard_id, seg.measurement.as_str()))
                .or_default()
                .push(seg);
        }

        let mut tasks = Vec::new();

        match self.policy.strategy {
            CompactionStrategy::Twcs => {
                self.pick_twcs(&groups, output_dir, source_level, target_level, &mut tasks);
            }
            CompactionStrategy::SizeTiered => {
                self.pick_size_tiered(&groups, output_dir, source_level, target_level, &mut tasks);
            }
            CompactionStrategy::Hybrid => {
                // TWCS first (count-based), then size-tiered for remaining groups
                let twcs_shards =
                    self.pick_twcs(&groups, output_dir, source_level, target_level, &mut tasks);
                // Size-tiered pass on groups NOT already selected by TWCS
                let remaining: BTreeMap<(ShardId, &str), Vec<&SegmentCatalogEntry>> = groups
                    .into_iter()
                    .filter(|(key, _)| !twcs_shards.contains(key))
                    .collect();
                self.pick_size_tiered(
                    &remaining,
                    output_dir,
                    source_level,
                    target_level,
                    &mut tasks,
                );
            }
        }

        tasks
    }

    /// TWCS pass: trigger when count ≥ threshold.
    ///
    /// Returns the set of `(shard_id, measurement)` keys that produced tasks.
    fn pick_twcs<'a>(
        &self,
        groups: &BTreeMap<(ShardId, &'a str), Vec<&SegmentCatalogEntry>>,
        output_dir: &std::path::Path,
        source_level: CompactionLevel,
        target_level: CompactionLevel,
        tasks: &mut Vec<CompactionTask>,
    ) -> std::collections::BTreeSet<(ShardId, &'a str)> {
        let mut selected = std::collections::BTreeSet::new();

        for ((shard_id, measurement), group) in groups {
            if group.len() >= self.policy.count_trigger_threshold {
                let output_path = Self::output_path(output_dir, *shard_id, measurement, group);
                tasks.push(CompactionTask {
                    shard_id: *shard_id,
                    input_segments: group.iter().map(|s| (*s).clone()).collect(),
                    source_level,
                    target_level,
                    output_path,
                });
                selected.insert((*shard_id, *measurement));
            }
        }

        selected
    }

    /// Size-tiered pass: classify by [`SizeTier`], trigger when tier count ≥ threshold.
    fn pick_size_tiered(
        &self,
        groups: &BTreeMap<(ShardId, &str), Vec<&SegmentCatalogEntry>>,
        output_dir: &std::path::Path,
        source_level: CompactionLevel,
        target_level: CompactionLevel,
        tasks: &mut Vec<CompactionTask>,
    ) {
        for ((shard_id, measurement), group) in groups {
            // Sub-group by size tier
            let mut tiers: BTreeMap<SizeTier, Vec<&SegmentCatalogEntry>> = BTreeMap::new();
            for seg in group {
                let tier = SizeTier::classify(seg.byte_size);
                tiers.entry(tier).or_default().push(seg);
            }

            for tier_segments in tiers.values() {
                if tier_segments.len() >= self.policy.size_tier_threshold {
                    let output_path =
                        Self::output_path(output_dir, *shard_id, measurement, tier_segments);
                    tasks.push(CompactionTask {
                        shard_id: *shard_id,
                        input_segments: tier_segments.iter().map(|s| (*s).clone()).collect(),
                        source_level,
                        target_level,
                        output_path,
                    });
                }
            }
        }
    }

    /// Build the output path for a compaction task.
    fn output_path(
        output_dir: &std::path::Path,
        shard_id: ShardId,
        measurement: &str,
        segments: &[&SegmentCatalogEntry],
    ) -> PathBuf {
        output_dir
            .join(format!("shard_{}", shard_id.0))
            .join(format!(
                "{measurement}_compacted_{}.csx",
                segments.iter().map(|s| s.segment_id.0).max().unwrap_or(0)
            ))
    }

    /// Check if write backpressure should be applied.
    ///
    /// Returns a throttle factor: 0 = no throttle, >0 = delay in
    /// milliseconds per write. Escalates linearly as L0 count grows
    /// beyond `4 × trigger_threshold`.
    #[must_use]
    pub fn backpressure_delay_ms(&self, l0_count: usize) -> u64 {
        let heavy_threshold = self.policy.count_trigger_threshold * 4;
        if l0_count <= heavy_threshold {
            return 0;
        }
        let excess = l0_count - heavy_threshold;
        // Linear: 10ms per excess segment, max 500ms.
        (excess as u64 * 10).min(500)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronix_core::SegmentId;
    use std::path::PathBuf;

    fn test_entry(id: u64, shard: i64, measurement: &str) -> SegmentCatalogEntry {
        test_entry_sized(id, shard, measurement, 4096)
    }

    fn test_entry_sized(id: u64, shard: i64, measurement: &str, size: u64) -> SegmentCatalogEntry {
        SegmentCatalogEntry {
            segment_id: SegmentId(id),
            shard_id: ShardId(shard),
            measurement: measurement.to_string(),
            path: PathBuf::from(format!("shard_{shard}/seg_{id}.csx")),
            min_timestamp: id as i64 * 1000,
            max_timestamp: id as i64 * 1000 + 999,
            row_count: 1000,
            series_count: 10,
            byte_size: size,
            row_group_count: 1,
            column_count: 5,
            column_stats: Vec::new(),
            state: SegmentState::default(),
        }
    }

    // ── TWCS tests (backward compat) ────────────────────────────────

    #[test]
    fn picker_no_compaction_below_threshold() {
        let picker = CompactionPicker::new(4);
        let segments = vec![
            test_entry(1, 0, "cpu"),
            test_entry(2, 0, "cpu"),
            test_entry(3, 0, "cpu"),
        ];
        let tasks = picker.pick(&segments, std::path::Path::new("/out"));
        assert!(tasks.is_empty());
    }

    #[test]
    fn picker_triggers_at_threshold() {
        let picker = CompactionPicker::new(4);
        let segments = vec![
            test_entry(1, 0, "cpu"),
            test_entry(2, 0, "cpu"),
            test_entry(3, 0, "cpu"),
            test_entry(4, 0, "cpu"),
        ];
        let tasks = picker.pick(&segments, std::path::Path::new("/out"));
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].input_segments.len(), 4);
        assert_eq!(tasks[0].shard_id, ShardId(0));
        assert_eq!(tasks[0].target_level, CompactionLevel::L1);
    }

    #[test]
    fn picker_groups_by_shard_and_measurement() {
        let picker = CompactionPicker::new(2);
        let segments = vec![
            test_entry(1, 0, "cpu"),
            test_entry(2, 0, "cpu"),
            test_entry(3, 0, "mem"),
            test_entry(4, 1, "cpu"),
            test_entry(5, 1, "cpu"),
        ];
        let tasks = picker.pick(&segments, std::path::Path::new("/out"));
        // Two groups above threshold: (shard=0,cpu) and (shard=1,cpu)
        assert_eq!(tasks.len(), 2);
    }

    #[test]
    fn picker_never_crosses_shard_boundary() {
        let picker = CompactionPicker::new(3);
        let segments = vec![
            test_entry(1, 0, "cpu"),
            test_entry(2, 0, "cpu"),
            test_entry(3, 1, "cpu"),
            test_entry(4, 1, "cpu"),
        ];
        // Each shard only has 2 segments — below threshold of 3
        let tasks = picker.pick(&segments, std::path::Path::new("/out"));
        assert!(tasks.is_empty());
    }

    #[test]
    fn backpressure_none_below_heavy_threshold() {
        let picker = CompactionPicker::new(4);
        assert_eq!(picker.backpressure_delay_ms(0), 0);
        assert_eq!(picker.backpressure_delay_ms(15), 0);
        assert_eq!(picker.backpressure_delay_ms(16), 0);
    }

    #[test]
    fn backpressure_escalates_linearly() {
        let picker = CompactionPicker::new(4);
        // heavy_threshold = 16
        assert_eq!(picker.backpressure_delay_ms(17), 10);
        assert_eq!(picker.backpressure_delay_ms(18), 20);
        assert_eq!(picker.backpressure_delay_ms(26), 100);
    }

    #[test]
    fn backpressure_caps_at_500ms() {
        let picker = CompactionPicker::new(4);
        assert_eq!(picker.backpressure_delay_ms(100), 500);
    }

    #[test]
    fn compaction_level_next() {
        assert_eq!(CompactionLevel::L0.next(), CompactionLevel::L1);
        assert_eq!(CompactionLevel::L1.next(), CompactionLevel::L2);
        assert_eq!(CompactionLevel::L2.next(), CompactionLevel::L2);
    }

    #[test]
    fn segment_state_display() {
        assert_eq!(SegmentState::Active.to_string(), "Active");
        assert_eq!(SegmentState::Compacting.to_string(), "Compacting");
        assert_eq!(
            SegmentState::SoftDeleted { deleted_at_ms: 123 }.to_string(),
            "SoftDeleted(at=123)"
        );
    }

    // ── Size-tier tests ─────────────────────────────────────────────

    #[test]
    fn size_tier_classification() {
        assert_eq!(SizeTier::classify(0), SizeTier::Tiny);
        assert_eq!(SizeTier::classify(500_000), SizeTier::Tiny);
        assert_eq!(SizeTier::classify(1_048_575), SizeTier::Tiny);
        assert_eq!(SizeTier::classify(1_048_576), SizeTier::Small);
        assert_eq!(SizeTier::classify(5_000_000), SizeTier::Small);
        assert_eq!(SizeTier::classify(10_485_760), SizeTier::Medium);
        assert_eq!(SizeTier::classify(50_000_000), SizeTier::Medium);
        assert_eq!(SizeTier::classify(104_857_600), SizeTier::Large);
        assert_eq!(SizeTier::classify(1_000_000_000), SizeTier::Large);
    }

    #[test]
    fn size_tiered_picker_groups_by_tier() {
        let policy = CompactionPolicy {
            strategy: CompactionStrategy::SizeTiered,
            size_tier_threshold: 2,
            ..CompactionPolicy::default()
        };
        let picker = CompactionPicker::with_policy(policy);

        // 4 tiny segments + 1 small segment — should merge the 4 tiny ones
        let segments = vec![
            test_entry_sized(1, 0, "cpu", 100),       // Tiny
            test_entry_sized(2, 0, "cpu", 200),       // Tiny
            test_entry_sized(3, 0, "cpu", 5_000_000), // Small
        ];
        let tasks = picker.pick(&segments, std::path::Path::new("/out"));
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].input_segments.len(), 2); // Only tiny segments
    }

    #[test]
    fn size_tiered_never_crosses_shard_boundary() {
        let policy = CompactionPolicy {
            strategy: CompactionStrategy::SizeTiered,
            size_tier_threshold: 2,
            ..CompactionPolicy::default()
        };
        let picker = CompactionPicker::with_policy(policy);

        // 1 tiny in shard 0 + 1 tiny in shard 1 — should NOT merge
        let segments = vec![
            test_entry_sized(1, 0, "cpu", 100),
            test_entry_sized(2, 1, "cpu", 200),
        ];
        let tasks = picker.pick(&segments, std::path::Path::new("/out"));
        assert!(tasks.is_empty());
    }

    #[test]
    fn hybrid_deduplicates_twcs_and_size_tiered() {
        let policy = CompactionPolicy {
            strategy: CompactionStrategy::Hybrid,
            count_trigger_threshold: 3,
            size_tier_threshold: 3,
            ..CompactionPolicy::default()
        };
        let picker = CompactionPicker::with_policy(policy);

        // 3 tiny segments in one shard — TWCS picks them, size-tiered
        // should NOT duplicate the task
        let segments = vec![
            test_entry_sized(1, 0, "cpu", 100),
            test_entry_sized(2, 0, "cpu", 200),
            test_entry_sized(3, 0, "cpu", 300),
        ];
        let tasks = picker.pick(&segments, std::path::Path::new("/out"));
        assert_eq!(tasks.len(), 1); // NOT 2
    }

    #[test]
    fn hybrid_size_tiered_catches_remaining_groups() {
        let policy = CompactionPolicy {
            strategy: CompactionStrategy::Hybrid,
            count_trigger_threshold: 10, // high threshold — TWCS won't trigger
            size_tier_threshold: 3,
            ..CompactionPolicy::default()
        };
        let picker = CompactionPicker::with_policy(policy);

        // 4 tiny segments — below TWCS threshold (10) but above size-tier (3)
        let segments = vec![
            test_entry_sized(1, 0, "cpu", 100),
            test_entry_sized(2, 0, "cpu", 200),
            test_entry_sized(3, 0, "cpu", 300),
            test_entry_sized(4, 0, "cpu", 400),
        ];
        let tasks = picker.pick(&segments, std::path::Path::new("/out"));
        assert_eq!(tasks.len(), 1); // size-tiered catches them
    }

    #[test]
    fn twcs_only_strategy_ignores_size_tiers() {
        let policy = CompactionPolicy {
            strategy: CompactionStrategy::Twcs,
            count_trigger_threshold: 10,
            size_tier_threshold: 2,
            ..CompactionPolicy::default()
        };
        let picker = CompactionPicker::with_policy(policy);

        // 3 tiny segments — below TWCS threshold, above size-tier
        let segments = vec![
            test_entry_sized(1, 0, "cpu", 100),
            test_entry_sized(2, 0, "cpu", 200),
            test_entry_sized(3, 0, "cpu", 300),
        ];
        let tasks = picker.pick(&segments, std::path::Path::new("/out"));
        assert!(tasks.is_empty()); // TWCS-only: size-tier not used
    }

    // ── Write-amplification budget tests ────────────────────────────

    #[test]
    fn write_amp_tracker_initially_zero() {
        let tracker = WriteAmpTracker::new();
        assert_eq!(tracker.write_amplification(), 0.0);
        assert!(!tracker.budget_exhausted(10.0));
    }

    #[test]
    fn write_amp_tracker_records_correctly() {
        let tracker = WriteAmpTracker::new();
        tracker.record(1000, 1000); // 1:1 amp
        assert!((tracker.write_amplification() - 1.0).abs() < f64::EPSILON);

        tracker.record(1000, 2000); // cumulative: 2000 in, 3000 out
        assert!((tracker.write_amplification() - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn write_amp_tracker_budget_exhausted() {
        let tracker = WriteAmpTracker::new();
        tracker.record(1000, 15_000); // 15× amplification
        assert!(tracker.budget_exhausted(10.0));
        assert!(!tracker.budget_exhausted(20.0));
    }

    #[test]
    fn write_amp_tracker_reset() {
        let tracker = WriteAmpTracker::new();
        tracker.record(1000, 5000);
        assert!(tracker.input_bytes() > 0);
        tracker.reset();
        assert_eq!(tracker.input_bytes(), 0);
        assert_eq!(tracker.output_bytes(), 0);
    }

    // ── Policy configuration tests ──────────────────────────────────

    #[test]
    fn default_policy_is_hybrid() {
        let policy = CompactionPolicy::default();
        assert_eq!(policy.strategy, CompactionStrategy::Hybrid);
        assert_eq!(policy.count_trigger_threshold, 4);
        assert_eq!(policy.size_tier_threshold, 4);
        assert!((policy.max_write_amplification - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn with_policy_constructor() {
        let policy = CompactionPolicy {
            strategy: CompactionStrategy::SizeTiered,
            count_trigger_threshold: 8,
            size_tier_threshold: 6,
            max_write_amplification: 5.0,
        };
        let picker = CompactionPicker::with_policy(policy.clone());
        assert_eq!(picker.policy().strategy, CompactionStrategy::SizeTiered);
        assert_eq!(picker.policy().size_tier_threshold, 6);
    }

    #[test]
    fn compaction_task_total_input_bytes() {
        let segments = vec![
            test_entry_sized(1, 0, "cpu", 1000),
            test_entry_sized(2, 0, "cpu", 2000),
            test_entry_sized(3, 0, "cpu", 3000),
        ];
        let task = CompactionTask {
            shard_id: ShardId(0),
            input_segments: segments,
            source_level: CompactionLevel::L0,
            target_level: CompactionLevel::L1,
            output_path: PathBuf::from("/out"),
        };
        assert_eq!(task.total_input_bytes(), 6000);
    }

    #[test]
    fn policy_validated_clamps_thresholds() {
        let policy = CompactionPolicy {
            strategy: CompactionStrategy::Hybrid,
            count_trigger_threshold: 0,
            size_tier_threshold: 100_000,
            max_write_amplification: 0.1,
        }
        .validated();

        assert_eq!(policy.count_trigger_threshold, 2);
        assert_eq!(policy.size_tier_threshold, 1000);
        assert!((policy.max_write_amplification - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn policy_validated_keeps_valid_values() {
        let policy = CompactionPolicy::default().validated();
        assert_eq!(policy.count_trigger_threshold, 4);
        assert_eq!(policy.size_tier_threshold, 4);
        assert!((policy.max_write_amplification - 10.0).abs() < f64::EPSILON);
    }
}
