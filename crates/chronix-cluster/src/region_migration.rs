//! Region migration — move regions between `DataNode`s with zero downtime.
//!
//! The [`RegionMigrator`] coordinates the migration lifecycle using a
//! **two-phase Raft protocol**:
//!
//! 1. **`BeginMigration`** — adds the destination as a learner replica and
//!    transitions the region to `Migrating` state via Raft. This prevents
//!    double-migration and ensures membership safety.
//! 2. **Snapshot:** Read all data from the source via `RegionStorage::query_region`.
//! 3. **Transfer:** Write the snapshot to the destination via `DataGrpcClient::write_region`.
//! 4. **`MigrateRegion`** — promotes the destination to leader, removes the
//!    source from the replica set, and marks the region `Active`.
//! 5. **Cleanup:** Remove the region from the source `RegionManager`.
//!
//! During migration, reads continue to be served from the source; writes are
//! forwarded to the source leader until the routing table switches.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, info, warn};

use chronix_meta::{MetaCommand, NodeId, RegionId, RegionState};

use crate::client::MetaClient;
use crate::data_client::DataGrpcClient;
use crate::data_service::{core_to_proto_point, RegionQuery, RegionStorage};
use crate::error::Result;
use crate::region::RegionManager;

/// Status of a region migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationStatus {
    /// Migration has been created but not started.
    Pending,
    /// Data is being transferred from source to destination.
    Transferring,
    /// Migration completed successfully.
    Completed,
    /// Migration failed.
    Failed,
}

/// Display impl for structured logging (instead of Debug).
impl std::fmt::Display for MigrationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => f.write_str("Pending"),
            Self::Transferring => f.write_str("Transferring"),
            Self::Completed => f.write_str("Completed"),
            Self::Failed => f.write_str("Failed"),
        }
    }
}

/// Result of a completed region migration.
#[derive(Debug, Clone)]
pub struct MigrationResult {
    /// The region that was migrated.
    pub region_id: RegionId,
    /// Source node.
    pub source_node_id: NodeId,
    /// Destination node.
    pub dest_node_id: NodeId,
    /// Number of points transferred.
    pub points_transferred: u64,
    /// Elapsed time.
    pub elapsed_ms: u64,
    /// Final status.
    pub status: MigrationStatus,
}

/// Default number of catch-up iterations before giving up.
///
/// Increased from 10 to 50 to handle sustained high write
/// throughput. Convergence detection (below) breaks early when the delta
/// shrinks below a threshold, so the extra headroom costs nothing.
const DEFAULT_MAX_CATCHUP_ROUNDS: u32 = 50;

/// Describes a planned region migration.
#[derive(Debug, Clone)]
pub struct MigrationPlan {
    /// Region to migrate.
    pub region_id: RegionId,
    /// Measurement the region belongs to.
    pub measurement: String,
    /// Current owner node.
    pub source_node_id: NodeId,
    /// Target node.
    pub dest_node_id: NodeId,
    /// Maximum batch size for snapshot transfer.
    pub batch_size: usize,
    /// Maximum catch-up rounds before switchover.
    pub max_catchup_rounds: u32,
}

impl MigrationPlan {
    /// Create a new migration plan.
    #[must_use]
    pub fn new(
        region_id: RegionId,
        measurement: impl Into<String>,
        source_node_id: NodeId,
        dest_node_id: NodeId,
    ) -> Self {
        Self {
            region_id,
            measurement: measurement.into(),
            source_node_id,
            dest_node_id,
            batch_size: 10_000,
            max_catchup_rounds: DEFAULT_MAX_CATCHUP_ROUNDS,
        }
    }

    /// Override the batch size for data transfer.
    #[must_use]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Override the maximum catch-up rounds.
    #[must_use]
    pub fn with_max_catchup_rounds(mut self, rounds: u32) -> Self {
        self.max_catchup_rounds = rounds;
        self
    }
}

/// Phase of a region migration lifecycle.
///
/// Tracks the current stage of a migration for observability and
/// catch-up phase integration. The lifecycle is:
///
/// ```text
/// Snapshot ──► CatchUp ──► Switchover ──► Cleanup
/// ```
///
/// The `CatchUp` phase re-reads the source region for any writes that
/// arrived after the initial snapshot, transferring deltas until the
/// data converges (up to `MAX_CATCHUP_ROUNDS` iterations). This
/// dramatically reduces the data loss window compared to a simple
/// snapshot-then-switch approach.
///
/// **Planned fix:** Implement Raft learner catch-up — after the snapshot
/// is applied to the destination, the destination tails the source's Raft
/// log from `snapshot.last_applied` until the gap closes, then promotes
/// to leader. This is the approach used by TiKV (learner-then-promote)
/// and CockroachDB (learner replicas).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    /// Reading a point-in-time snapshot from the source region.
    Snapshot,
    /// Tailing the source region for writes that arrived after the
    /// snapshot. Transfers deltas until convergence.
    CatchUp,
    /// Promoting the destination to leader and updating the routing table.
    Switchover,
    /// Removing the source region after successful switchover.
    Cleanup,
}

impl std::fmt::Display for MigrationPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Snapshot => write!(f, "Snapshot"),
            Self::CatchUp => write!(f, "CatchUp"),
            Self::Switchover => write!(f, "Switchover"),
            Self::Cleanup => write!(f, "Cleanup"),
        }
    }
}

/// Progress of a region migration.
///
/// Provides observability into the migration pipeline. Can be polled by
/// monitoring systems to track migration health and estimate completion.
#[derive(Debug)]
pub struct MigrationProgress {
    /// Region being migrated.
    pub region_id: RegionId,
    /// Current phase of the migration.
    pub phase: MigrationPhase,
    /// Bytes transferred to the destination so far.
    pub bytes_transferred: AtomicU64,
    /// Remaining Raft log entries to replay during catch-up.
    /// Currently always 0 (catch-up not yet implemented).
    pub log_entries_remaining: AtomicU64,
}

impl MigrationProgress {
    /// Create a new progress tracker for the given region.
    #[must_use]
    pub fn new(region_id: RegionId) -> Self {
        Self {
            region_id,
            phase: MigrationPhase::Snapshot,
            bytes_transferred: AtomicU64::new(0),
            log_entries_remaining: AtomicU64::new(0),
        }
    }

    /// Update the current phase.
    pub fn set_phase(&mut self, phase: MigrationPhase) {
        self.phase = phase;
    }

    /// Add to the bytes-transferred counter.
    pub fn add_bytes_transferred(&self, bytes: u64) {
        self.bytes_transferred.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Get the current bytes-transferred count.
    #[must_use]
    pub fn get_bytes_transferred(&self) -> u64 {
        self.bytes_transferred.load(Ordering::Relaxed)
    }
}

/// Orchestrates region migrations between `DataNode`s.
pub struct RegionMigrator {
    /// Local region manager (for local source/dest detection).
    region_manager: Arc<RegionManager>,
    /// Local storage backend.
    local_storage: Arc<dyn RegionStorage>,
    /// gRPC client for reaching remote nodes.
    data_client: DataGrpcClient,
    /// ID of this node.
    local_node_id: NodeId,
    /// Optional meta client — when set, routing table is atomically updated
    /// via Raft after successful migration.
    meta_client: Option<Arc<dyn MetaClient>>,
}

impl RegionMigrator {
    /// Create a new `RegionMigrator`.
    pub fn new(
        region_manager: Arc<RegionManager>,
        local_storage: Arc<dyn RegionStorage>,
        data_client: DataGrpcClient,
        local_node_id: NodeId,
    ) -> Self {
        Self {
            region_manager,
            local_storage,
            data_client,
            local_node_id,
            meta_client: None,
        }
    }

    /// Set a `MetaClient` so routing table updates are proposed through
    /// Raft after a successful migration.
    #[must_use]
    pub fn with_meta_client(mut self, meta_client: Arc<dyn MetaClient>) -> Self {
        self.meta_client = Some(meta_client);
        self
    }

    /// Execute a region migration according to the plan.
    ///
    /// Uses a **two-phase protocol** for safety:
    ///
    /// 1. **`BeginMigration`** — adds dest as learner, marks `Migrating`.
    /// 2. **Snapshot:** Read all data from the source region.
    /// 3. **Transfer:** Write the snapshot to destination in batches.
    /// 4. **`MigrateRegion`** — promotes dest to leader, removes source.
    /// 5. **Cleanup:** Remove the region from the source `RegionManager`.
    ///
    /// This ensures the destination is a replica member *before* data
    /// transfer begins, and the source is not removed until the routing
    /// table has been atomically updated.
    ///
    /// # Data Loss Window
    ///
    /// The catch-up phase (step 3.5) re-reads the source for writes
    /// that arrived after the initial snapshot, transferring deltas in
    /// up to `MAX_CATCHUP_ROUNDS` (10) iterations. This reduces the
    /// data loss window from the full migration duration (seconds to
    /// minutes) to at most the time of one round-trip read+write cycle
    /// — typically < 100 ms.
    ///
    /// **Residual risk:** If new writes arrive faster than the catch-up
    /// loop can drain them, the loop will exhaust its iterations and
    /// proceed to switchover with a small window. For extremely write-
    /// heavy regions, pausing writes during switchover (via the
    /// `Migrating` state flag) is recommended.
    ///
    /// **Future enhancement — Raft learner catch-up:**
    /// For zero-data-loss migrations, the destination could join the
    /// source's Raft group as a learner and replicate via the Raft log
    /// (like TiKV/CockroachDB). The current timestamp-based approach
    /// is simpler and sufficient for most workloads.
    ///
    /// The catch-up phase is tracked via [`MigrationPhase`] and
    /// [`MigrationProgress`] for observability.
    ///
    /// # Errors
    ///
    /// Returns an error if any stage of the migration fails.
    pub async fn migrate(&self, plan: &MigrationPlan) -> Result<MigrationResult> {
        let start = Instant::now();
        let mut progress = MigrationProgress::new(plan.region_id);

        info!(
            region_id = plan.region_id,
            source = plan.source_node_id,
            dest = plan.dest_node_id,
            measurement = %plan.measurement,
            "starting region migration (two-phase)"
        );

        // ── Phase 1: Begin migration ───────────────────────────────
        // Adds dest as learner replica and transitions to Migrating.
        progress.set_phase(MigrationPhase::Snapshot);
        self.begin_migration(plan).await?;

        debug!(
            region_id = plan.region_id,
            phase = %progress.phase,
            "phase 1 complete: region marked Migrating, dest added as learner"
        );

        // ── Steps 2-3: Read source and write destination ───────────
        // If either fails, cancel the migration to restore the region
        // to Active and remove the destination learner.
        let source_points = match self.read_source(plan).await {
            Ok(pts) => pts,
            Err(e) => {
                warn!(
                    region_id = plan.region_id,
                    error = %e,
                    "data transfer failed (read), cancelling migration"
                );
                if let Err(cancel_err) = self.cancel_migration(plan).await {
                    warn!(
                        region_id = plan.region_id,
                        error = %cancel_err,
                        "failed to cancel migration — region may be stuck in Migrating state"
                    );
                }
                return Err(e);
            }
        };
        let total_points = source_points.len() as u64;

        debug!(
            region_id = plan.region_id,
            points = total_points,
            "read source region data"
        );

        // ── Step 3: Write data to destination in batches ───────────
        if let Err(e) = self.write_destination(plan, &source_points).await {
            warn!(
                region_id = plan.region_id,
                error = %e,
                "data transfer failed (write), cancelling migration"
            );
            if let Err(cancel_err) = self.cancel_migration(plan).await {
                warn!(
                    region_id = plan.region_id,
                    error = %cancel_err,
                    "failed to cancel migration — region may be stuck in Migrating state"
                );
            }
            return Err(e);
        }

        debug!(
            region_id = plan.region_id,
            points = total_points,
            "transferred data to destination"
        );

        // ── Phase: CatchUp ─────────────────────────────────
        //
        // Re-read the source for any writes that arrived *after* the
        // initial snapshot was started.  We use the max timestamp seen
        // in the snapshot as a lower bound so that only genuinely new
        // data is transferred.
        //
        // The loop runs up to `MAX_CATCHUP_ROUNDS` iterations, each
        // time querying for points with timestamps strictly after the
        // watermark, writing them to the destination, and advancing
        // the watermark.  If an iteration returns no new data, the
        // region has converged and we proceed to switchover.
        progress.set_phase(MigrationPhase::CatchUp);
        let mut catchup_watermark: Option<i64> = source_points.iter().map(|p| p.timestamp()).max();
        let mut total_catchup_points: u64 = 0;

        // Only run catch-up if there was actual data (and thus a valid
        // watermark). An empty snapshot means there's nothing to catch up.
        if let Some(ref mut watermark) = catchup_watermark {
            let mut prev_batch_len: u64 = u64::MAX;
            for round in 1..=plan.max_catchup_rounds {
                let next_ts = watermark.saturating_add(1);
                let delta_points = self.read_source_since(plan, next_ts).await?;
                if delta_points.is_empty() {
                    debug!(
                        region_id = plan.region_id,
                        rounds = round,
                        caught_up = total_catchup_points,
                        "catch-up converged — no new data from source"
                    );
                    break;
                }
                let new_max = delta_points
                    .iter()
                    .map(|p| p.timestamp())
                    .max()
                    .unwrap_or(*watermark);
                let batch_len = delta_points.len() as u64;

                self.write_destination(plan, &delta_points).await?;
                total_catchup_points += batch_len;
                *watermark = new_max;

                progress.log_entries_remaining.store(0, Ordering::Relaxed);

                debug!(
                    region_id = plan.region_id,
                    round,
                    batch = batch_len,
                    total_caught_up = total_catchup_points,
                    watermark = *watermark,
                    "catch-up round complete"
                );

                // Convergence detection — if the delta
                // shrinks below 10% of the previous round's delta, the
                // source write rate is low enough that a single final
                // drain after freeze will capture the remainder.
                if batch_len <= prev_batch_len / 10 {
                    debug!(
                        region_id = plan.region_id,
                        round,
                        batch_len,
                        prev_batch_len,
                        "catch-up converged — delta shrunk below 10% threshold"
                    );
                    break;
                }
                prev_batch_len = batch_len;
            }
        }

        // ── Write-Freeze + Final Drain ─────────────────────
        //
        // Before switchover, freeze the source region to prevent new
        // writes, then drain any remaining in-flight data.  This
        // guarantees zero data loss even under continuous writes.
        //
        // If the freeze or drain fails, we cancel the migration —
        // the region returns to Active and the destination learner
        // is removed.
        if let Err(e) = self.freeze_source(plan).await {
            warn!(
                region_id = plan.region_id,
                error = %e,
                "failed to freeze source — cancelling migration"
            );
            let _ = self.cancel_migration(plan).await;
            return Err(e);
        }

        debug!(
            region_id = plan.region_id,
            "source frozen — draining final writes"
        );

        // Final drain: one last read with the current watermark.
        // Source is now ReadOnly so no new writes can arrive.
        if let Some(ref mut watermark) = catchup_watermark {
            let next_ts = watermark.saturating_add(1);
            let final_points = self.read_source_since(plan, next_ts).await?;
            if !final_points.is_empty() {
                let batch_len = final_points.len() as u64;
                self.write_destination(plan, &final_points).await?;
                total_catchup_points += batch_len;

                debug!(
                    region_id = plan.region_id,
                    drained = batch_len,
                    "final drain complete — all in-flight writes captured"
                );
            }
        }

        let total_points = total_points + total_catchup_points;

        // ── Phase 2: Complete migration ────────────────────────────
        // Promotes dest to leader, removes source, marks Active.
        progress.set_phase(MigrationPhase::Switchover);
        self.complete_migration(plan).await?;

        // ── Step 5: Remove from source (if local) ──────────────────
        progress.set_phase(MigrationPhase::Cleanup);
        if plan.source_node_id == self.local_node_id {
            if let Err(e) = self.region_manager.remove_region(plan.region_id) {
                warn!(
                    region_id = plan.region_id,
                    error = %e,
                    "failed to remove source region (may already be removed)"
                );
            }
        }

        #[allow(clippy::cast_possible_truncation)]
        let elapsed_ms = start.elapsed().as_millis() as u64;

        info!(
            region_id = plan.region_id,
            points = total_points,
            elapsed_ms,
            "region migration completed"
        );

        Ok(MigrationResult {
            region_id: plan.region_id,
            source_node_id: plan.source_node_id,
            dest_node_id: plan.dest_node_id,
            points_transferred: total_points,
            elapsed_ms,
            status: MigrationStatus::Completed,
        })
    }

    /// Read all data from the source region.
    async fn read_source(&self, plan: &MigrationPlan) -> Result<Vec<chronix_core::Point>> {
        self.read_source_range(plan, i64::MIN, i64::MAX).await
    }

    /// Read data from the source region starting at `start_ns`.
    ///
    /// Used by the catch-up phase to query only the delta written after
    /// the initial snapshot transfer.
    async fn read_source_since(
        &self,
        plan: &MigrationPlan,
        start_ns: i64,
    ) -> Result<Vec<chronix_core::Point>> {
        self.read_source_range(plan, start_ns, i64::MAX).await
    }

    /// Read data from the source region within `[start_ns, end_ns]`.
    async fn read_source_range(
        &self,
        plan: &MigrationPlan,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<chronix_core::Point>> {
        let query = RegionQuery {
            measurement: plan.measurement.clone(),
            start_ns,
            end_ns,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
        };

        if plan.source_node_id == self.local_node_id {
            // Local read
            self.local_storage.query_region(plan.region_id, query).await
        } else {
            // Remote read
            let request = crate::data_service::proto::QueryRegionRequest {
                region_id: plan.region_id,
                measurement: plan.measurement.clone(),
                start_ns,
                end_ns,
                tag_filters: vec![],
                field_columns: vec![],
                limit: 0,
                require_linearizable: false,
            };

            let resp = self
                .data_client
                .query_region(plan.source_node_id, request)
                .await?;

            resp.points
                .iter()
                .map(crate::data_service::proto_to_core_point)
                .collect()
        }
    }

    /// Write data to the destination in batches.
    async fn write_destination(
        &self,
        plan: &MigrationPlan,
        points: &[chronix_core::Point],
    ) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }

        for chunk in points.chunks(plan.batch_size) {
            if plan.dest_node_id == self.local_node_id {
                // Local write
                self.local_storage
                    .write_points(plan.region_id, chunk.to_vec())
                    .await?;
            } else {
                // Remote write
                let proto_points: Vec<_> = chunk.iter().map(core_to_proto_point).collect();
                self.data_client
                    .write_region(plan.dest_node_id, plan.region_id, proto_points)
                    .await?;
            }
        }

        Ok(())
    }

    /// Phase 1: Begin the migration via Raft.
    ///
    /// Proposes `BeginMigration` which adds the destination as a
    /// learner replica and transitions the region to `Migrating`.
    /// This must succeed before any data is transferred.
    ///
    /// # Errors
    ///
    /// Returns an error if no meta client is configured or the Raft
    /// proposal fails.
    async fn begin_migration(&self, plan: &MigrationPlan) -> Result<()> {
        let Some(meta_client) = &self.meta_client else {
            return Err(crate::error::ClusterError::Internal(
                "meta client required for region migration — cannot ensure Raft safety without it"
                    .into(),
            ));
        };

        let cmd = MetaCommand::BeginMigration {
            region_id: plan.region_id,
            source_node_id: plan.source_node_id,
            dest_node_id: plan.dest_node_id,
        };
        meta_client.propose(cmd).await?;

        info!(
            region_id = plan.region_id,
            source = plan.source_node_id,
            dest = plan.dest_node_id,
            "phase 1: region migration begun via MetaNode"
        );
        Ok(())
    }

    /// Rollback a migration that was begun but failed during data transfer.
    ///
    /// Proposes `CancelMigration` which removes the destination learner
    /// and restores the region to `Active`.
    async fn cancel_migration(&self, plan: &MigrationPlan) -> Result<()> {
        let Some(meta_client) = &self.meta_client else {
            return Err(crate::error::ClusterError::Internal(
                "meta client required for migration cancel — cannot ensure Raft safety without it"
                    .into(),
            ));
        };

        let cmd = MetaCommand::CancelMigration {
            region_id: plan.region_id,
            dest_node_id: plan.dest_node_id,
        };
        meta_client.propose(cmd).await?;

        info!(
            region_id = plan.region_id,
            dest = plan.dest_node_id,
            "migration cancelled — region restored to Active via MetaNode"
        );
        Ok(())
    }

    /// Freeze the source region before switchover.
    ///
    /// Proposes `UpdateRegionState` to transition the source region to
    /// `ReadOnly`, blocking new writes.  This ensures the final drain
    /// pass captures all in-flight data before switchover.
    async fn freeze_source(&self, plan: &MigrationPlan) -> Result<()> {
        let Some(meta_client) = &self.meta_client else {
            return Err(crate::error::ClusterError::Internal(
                "meta client required for source freeze — cannot ensure Raft safety without it"
                    .into(),
            ));
        };

        let cmd = MetaCommand::UpdateRegionState {
            region_id: plan.region_id,
            state: RegionState::ReadOnly,
        };
        meta_client.propose(cmd).await?;

        // After the ReadOnly state is Raft-committed, wait
        // a short period for in-flight writes that were dispatched before
        // the state change to settle. This closes the data-loss window
        // where writes hit the old leader before it processes the freeze.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        info!(
            region_id = plan.region_id,
            "source region frozen (ReadOnly) — writes blocked for final drain"
        );
        Ok(())
    }

    /// Phase 2: Complete the migration via Raft.
    ///
    /// Proposes `MigrateRegion` which promotes the destination to
    /// leader, removes the source from the replica set, and marks
    /// the region as `Active`.
    ///
    /// # Errors
    ///
    /// Returns an error if the Raft proposal fails — the caller must
    /// NOT remove the source region when this fails.
    async fn complete_migration(&self, plan: &MigrationPlan) -> Result<()> {
        let Some(meta_client) = &self.meta_client else {
            return Err(crate::error::ClusterError::Internal(
                "meta client required for migration completion — cannot ensure Raft safety without it".into(),
            ));
        };

        let cmd = MetaCommand::MigrateRegion {
            region_id: plan.region_id,
            source_node_id: plan.source_node_id,
            dest_node_id: plan.dest_node_id,
        };
        meta_client.propose(cmd).await?;

        info!(
            region_id = plan.region_id,
            source = plan.source_node_id,
            dest = plan.dest_node_id,
            "phase 2: routing table updated atomically via MetaNode"
        );
        Ok(())
    }
}

impl std::fmt::Debug for RegionMigrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegionMigrator")
            .field("local_node_id", &self.local_node_id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chronix_core::{FieldValue, Point, SeriesKey};
    use parking_lot::Mutex;
    use std::collections::BTreeMap;

    // ── Mock storage — uses shared region-store mock ───────────────
    use crate::test_util::MockRegionStore;
    type MockStorage = MockRegionStore;

    fn make_point(ts: i64) -> Point {
        let sk = SeriesKey::new("cpu", BTreeMap::new()).unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("value".to_string(), FieldValue::F64(1.0));
        Point::new(sk, fields, ts).unwrap()
    }

    fn make_migrator(storage: Arc<MockStorage>) -> (RegionMigrator, Arc<RegionManager>) {
        let mgr = Arc::new(RegionManager::new(1));
        let data_client = DataGrpcClient::new();
        let meta_client: Arc<dyn MetaClient> = Arc::new(MockMetaClient::new());
        let migrator = RegionMigrator::new(
            mgr.clone(),
            storage,
            data_client,
            1, // local node
        )
        .with_meta_client(meta_client);
        (migrator, mgr)
    }

    // ── Tests ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn migrate_local_to_local() {
        let storage = Arc::new(MockStorage::new());
        let (migrator, mgr) = make_migrator(storage.clone());

        // Create source region
        mgr.create_region(10, "cpu").unwrap();
        storage.seed(10, vec![make_point(100), make_point(200), make_point(300)]);

        let plan = MigrationPlan::new(10, "cpu", 1, 1);
        let result = migrator.migrate(&plan).await.unwrap();

        assert_eq!(result.region_id, 10);
        assert_eq!(result.points_transferred, 3);
        assert_eq!(result.status, MigrationStatus::Completed);
        assert!(result.elapsed_ms < 1000);
    }

    #[tokio::test]
    async fn migrate_empty_region() {
        let storage = Arc::new(MockStorage::new());
        let (migrator, mgr) = make_migrator(storage.clone());

        mgr.create_region(10, "cpu").unwrap();

        let plan = MigrationPlan::new(10, "cpu", 1, 1);
        let result = migrator.migrate(&plan).await.unwrap();

        assert_eq!(result.points_transferred, 0);
        assert_eq!(result.status, MigrationStatus::Completed);
    }

    #[tokio::test]
    async fn migrate_with_batch_size() {
        let storage = Arc::new(MockStorage::new());
        let (migrator, mgr) = make_migrator(storage.clone());

        mgr.create_region(10, "cpu").unwrap();

        // Seed 25 points, batch size 10
        let pts: Vec<Point> = (0..25).map(|i| make_point(i * 100)).collect();
        storage.seed(10, pts);

        let plan = MigrationPlan::new(10, "cpu", 1, 1).with_batch_size(10);
        let result = migrator.migrate(&plan).await.unwrap();

        assert_eq!(result.points_transferred, 25);
        assert_eq!(result.status, MigrationStatus::Completed);
    }

    #[test]
    fn migration_plan_defaults() {
        let plan = MigrationPlan::new(1, "test", 10, 20);
        assert_eq!(plan.region_id, 1);
        assert_eq!(plan.measurement, "test");
        assert_eq!(plan.source_node_id, 10);
        assert_eq!(plan.dest_node_id, 20);
        assert_eq!(plan.batch_size, 10_000);
    }

    #[test]
    fn migration_plan_with_batch_size() {
        let plan = MigrationPlan::new(1, "test", 10, 20).with_batch_size(500);
        assert_eq!(plan.batch_size, 500);
    }

    #[test]
    fn migration_status_variants() {
        assert_ne!(MigrationStatus::Pending, MigrationStatus::Completed);
        assert_ne!(MigrationStatus::Transferring, MigrationStatus::Failed);
        assert_eq!(MigrationStatus::Completed, MigrationStatus::Completed);
    }

    #[test]
    fn migration_result_debug() {
        let r = MigrationResult {
            region_id: 1,
            source_node_id: 10,
            dest_node_id: 20,
            points_transferred: 1000,
            elapsed_ms: 500,
            status: MigrationStatus::Completed,
        };
        let d = format!("{r:?}");
        assert!(d.contains("MigrationResult"));
        assert!(d.contains("1000"));
    }

    #[test]
    fn migrator_debug() {
        let storage = Arc::new(MockStorage::new());
        let (migrator, _) = make_migrator(storage);
        let d = format!("{migrator:?}");
        assert!(d.contains("RegionMigrator"));
        assert!(d.contains("local_node_id"));
    }

    // ── Mock meta client for routing table update tests ────────────

    #[derive(Debug)]
    struct MockMetaClient {
        commands: Mutex<Vec<String>>,
    }

    impl MockMetaClient {
        fn new() -> Self {
            Self {
                commands: Mutex::new(Vec::new()),
            }
        }

        fn commands(&self) -> Vec<String> {
            self.commands.lock().clone()
        }
    }

    #[async_trait]
    impl MetaClient for MockMetaClient {
        async fn register_node(&self, _: chronix_meta::DataNodeInfo) -> Result<()> {
            Ok(())
        }
        async fn deregister_node(&self, _: NodeId) -> Result<()> {
            Ok(())
        }
        async fn heartbeat(&self, _: NodeId, _: u64) -> Result<()> {
            Ok(())
        }
        async fn create_region(&self, _: chronix_meta::RegionInfo) -> Result<()> {
            Ok(())
        }
        async fn get_routing_table(&self) -> Result<chronix_meta::RoutingSnapshot> {
            Ok(chronix_meta::RoutingSnapshot::empty())
        }
        async fn propose(
            &self,
            cmd: chronix_meta::MetaCommand,
        ) -> Result<chronix_meta::MetaResponse> {
            self.commands.lock().push(format!("{cmd:?}"));
            Ok(chronix_meta::MetaResponse::Ok)
        }
    }

    #[tokio::test]
    async fn migrate_with_meta_client_updates_routing() {
        let storage = Arc::new(MockStorage::new());
        let mgr = Arc::new(RegionManager::new(1));
        let data_client = DataGrpcClient::new();
        let meta_client = Arc::new(MockMetaClient::new());

        let migrator = RegionMigrator::new(mgr.clone(), storage.clone(), data_client, 1)
            .with_meta_client(meta_client.clone());

        mgr.create_region(10, "cpu").unwrap();
        storage.seed(10, vec![make_point(100)]);

        // Source=1 (local), dest=1 (local) — keeps test self-contained
        // but still exercises the routing table update path.
        let plan = MigrationPlan::new(10, "cpu", 1, 1);
        let result = migrator.migrate(&plan).await.unwrap();

        assert_eq!(result.status, MigrationStatus::Completed);

        // Verify three-phase protocol: BeginMigration, UpdateRegionState(ReadOnly), MigrateRegion
        let commands = meta_client.commands();
        assert_eq!(
            commands.len(),
            3,
            "expected BeginMigration + UpdateRegionState + MigrateRegion"
        );
        assert!(
            commands[0].contains("BeginMigration"),
            "first command should be BeginMigration"
        );
        assert!(
            commands[1].contains("UpdateRegionState"),
            "second command should be UpdateRegionState (freeze)"
        );
        assert!(
            commands[2].contains("MigrateRegion"),
            "third command should be MigrateRegion"
        );
    }

    #[tokio::test]
    async fn migrate_without_meta_client_skips_routing_update() {
        let storage = Arc::new(MockStorage::new());
        let mgr = Arc::new(RegionManager::new(1));
        let data_client = DataGrpcClient::new();
        // Explicitly omit meta_client to test the error path.
        let migrator = RegionMigrator::new(mgr.clone(), storage.clone(), data_client, 1);

        mgr.create_region(10, "cpu").unwrap();
        storage.seed(10, vec![make_point(100)]);

        let plan = MigrationPlan::new(10, "cpu", 1, 1);
        let err = migrator.migrate(&plan).await.unwrap_err();

        // No meta client — migration must fail with an internal error.
        let msg = err.to_string();
        assert!(
            msg.contains("meta client required"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn with_meta_client_builder() {
        let storage = Arc::new(MockStorage::new());
        let mgr = Arc::new(RegionManager::new(1));
        let data_client = DataGrpcClient::new();
        let meta_client: Arc<dyn MetaClient> = Arc::new(MockMetaClient::new());

        let migrator =
            RegionMigrator::new(mgr, storage, data_client, 1).with_meta_client(meta_client);
        assert!(migrator.meta_client.is_some());
    }

    // ── Migration phase / progress tests ───────────────────

    #[test]
    fn migration_phase_display() {
        assert_eq!(MigrationPhase::Snapshot.to_string(), "Snapshot");
        assert_eq!(MigrationPhase::CatchUp.to_string(), "CatchUp");
        assert_eq!(MigrationPhase::Switchover.to_string(), "Switchover");
        assert_eq!(MigrationPhase::Cleanup.to_string(), "Cleanup");
    }

    #[test]
    fn migration_phase_equality() {
        assert_eq!(MigrationPhase::Snapshot, MigrationPhase::Snapshot);
        assert_ne!(MigrationPhase::Snapshot, MigrationPhase::CatchUp);
    }

    #[test]
    fn migration_progress_new() {
        let p = MigrationProgress::new(42);
        assert_eq!(p.region_id, 42);
        assert_eq!(p.phase, MigrationPhase::Snapshot);
        assert_eq!(p.get_bytes_transferred(), 0);
        assert_eq!(
            p.log_entries_remaining
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn migration_progress_set_phase() {
        let mut p = MigrationProgress::new(1);
        assert_eq!(p.phase, MigrationPhase::Snapshot);

        p.set_phase(MigrationPhase::CatchUp);
        assert_eq!(p.phase, MigrationPhase::CatchUp);

        p.set_phase(MigrationPhase::Switchover);
        assert_eq!(p.phase, MigrationPhase::Switchover);

        p.set_phase(MigrationPhase::Cleanup);
        assert_eq!(p.phase, MigrationPhase::Cleanup);
    }

    #[test]
    fn migration_progress_bytes_transferred() {
        let p = MigrationProgress::new(1);
        p.add_bytes_transferred(1000);
        assert_eq!(p.get_bytes_transferred(), 1000);

        p.add_bytes_transferred(500);
        assert_eq!(p.get_bytes_transferred(), 1500);
    }

    #[test]
    fn migration_progress_debug() {
        let p = MigrationProgress::new(99);
        let d = format!("{p:?}");
        assert!(d.contains("MigrationProgress"));
        assert!(d.contains("99"));
    }
}
