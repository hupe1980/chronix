//! Rollup and warm-tier migration methods for [`Chronix`].

use std::collections::BTreeMap;

use metrics::{counter, histogram};
use tracing::{error, info, warn};

use chronix_core::{Point, ShardId};
use chronix_engine::index::SegmentCatalogEntry;

use crate::error::{DbError, Result};
use crate::rollup::{RollupConfig, RollupRegistry};
use crate::warm_tier::{self, WarmTierConfig};

impl super::Chronix {
    /// Cascade rollup points through multi-tier rollup chains.
    ///
    /// If the target measurement of a rollup has further rollups defined,
    /// this method converts the inserted points into Arrow batches and
    /// computes the next tier, up to a depth limit of 4 to prevent
    /// infinite loops.
    ///
    /// Returns `false` if **any** tier in the chain failed to materialise.
    /// Callers that are about to delete the source data (retention) must
    /// treat that as a reason to keep it: a 1 s→1 min→15 min chain whose
    /// 15 min tier never materialised has silently lost the long-retention
    /// aggregate the raw data was being traded for.
    pub(super) fn cascade_rollup(
        &self,
        target_measurement: &str,
        points: &[Point],
        depth: usize,
    ) -> bool {
        const MAX_CHAIN_DEPTH: usize = 4;
        if points.is_empty() {
            return true;
        }
        if depth >= MAX_CHAIN_DEPTH {
            warn!(
                target_measurement,
                depth, "Rollup chain hit the depth limit — deeper tiers not materialised"
            );
            counter!("chronix_rollup_chain_truncated_total").increment(1);
            return false;
        }

        let next_rollups: Vec<crate::rollup::RollupConfig> = {
            let reg = self.rollup_registry.read();
            reg.rollups_for_source(target_measurement)
                .into_iter()
                .cloned()
                .collect()
        };
        if next_rollups.is_empty() {
            return true;
        }

        // Convert points to RecordBatch for rollup computation
        let Some(batch) = crate::rollup::points_to_record_batch(points) else {
            warn!(
                target_measurement,
                depth, "Could not build a batch for the next rollup tier"
            );
            return false;
        };
        let batches = [batch];

        let mut all_ok = true;
        for rollup_config in &next_rollups {
            let rollup_points = crate::rollup::compute_rollup_points(&batches, rollup_config);
            if rollup_points.is_empty() {
                continue;
            }
            if let Err(e) = self.insert_batch(&rollup_points) {
                warn!(
                    rollup = %rollup_config.name,
                    error = %e,
                    depth,
                    "Failed to insert cascaded rollup points"
                );
                all_ok = false;
            } else {
                info!(
                    rollup = %rollup_config.name,
                    points = rollup_points.len(),
                    depth,
                    "Cascaded rollup tier computed"
                );
                all_ok &= self.cascade_rollup(
                    &rollup_config.target_measurement,
                    &rollup_points,
                    depth + 1,
                );
            }
        }
        all_ok
    }

    // ── Rollup API ──────────────────────────────────────────────────

    /// Register a rollup configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed or validation fails.
    pub fn create_rollup(&self, config: RollupConfig) -> Result<()> {
        self.check_open()?;
        let mut registry = self.rollup_registry.write();
        registry
            .add(config)
            .map_err(|e| DbError::Internal(format!("rollup registration failed: {e}")))?;
        let path = self.config.data_dir.join(RollupRegistry::filename());
        registry
            .save(&path)
            .map_err(|e| DbError::Internal(format!("rollup persistence failed: {e}")))?;
        Ok(())
    }

    /// List all registered rollup configurations.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed.
    pub fn list_rollups(&self) -> Result<Vec<RollupConfig>> {
        self.check_open()?;
        Ok(self
            .rollup_registry
            .read()
            .list()
            .into_iter()
            .cloned()
            .collect())
    }

    /// Remove a rollup configuration by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed or the rollup
    /// doesn't exist.
    pub fn delete_rollup(&self, name: &str) -> Result<bool> {
        self.check_open()?;
        let mut registry = self.rollup_registry.write();
        let removed = registry.remove(name).is_some();
        if removed {
            let path = self.config.data_dir.join(RollupRegistry::filename());
            registry
                .save(&path)
                .map_err(|e| DbError::Internal(format!("rollup persistence failed: {e}")))?;
        }
        Ok(removed)
    }

    // ── Warm Tier ───────────────────────────────────────────────────

    /// Migrate eligible shards to the warm storage tier.
    ///
    /// Evaluates shard age against the warm tier configuration and
    /// copies qualifying segments to the warm path. After migration,
    /// catalog entries are updated to point to the new location.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed.
    pub fn warm_tier_migrate(&self, config: &WarmTierConfig) -> Result<warm_tier::WarmTierResult> {
        self.check_open()?;

        if !config.enabled {
            return Ok(warm_tier::WarmTierResult {
                shards_moved: 0,
                segments_recompressed: 0,
                bytes_saved: 0,
            });
        }

        let now_ns = i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        )
        .unwrap_or(i64::MAX);

        // Collect shard bounds from time index
        let shard_bounds: BTreeMap<ShardId, (i64, i64)> = {
            let time_idx = self.time_index.read();
            time_idx
                .iter()
                .map(|(&shard_id, idx)| {
                    let all = idx.all_entries();
                    let (min, max) = all.iter().fold((i64::MAX, i64::MIN), |(lo, hi), e| {
                        (lo.min(e.min_ts), hi.max(e.max_ts))
                    });
                    (shard_id, (min, max))
                })
                .collect()
        };

        let eligible = warm_tier::shards_to_warm(&shard_bounds, now_ns, config.warm_after_ns);

        if eligible.is_empty() {
            return Ok(warm_tier::WarmTierResult {
                shards_moved: 0,
                segments_recompressed: 0,
                bytes_saved: 0,
            });
        }

        let mut total_result = warm_tier::WarmTierResult {
            shards_moved: 0,
            segments_recompressed: 0,
            bytes_saved: 0,
        };

        for &shard_id in &eligible {
            let entries: Vec<SegmentCatalogEntry> = {
                let catalog = self.catalog.read();
                catalog
                    .all_segments()
                    .into_iter()
                    .filter(|e| e.shard_id == shard_id)
                    .cloned()
                    .collect()
            };

            let segment_paths: Vec<std::path::PathBuf> =
                entries.iter().map(|e| e.path.clone()).collect();

            let warm_dir = config.warm_path.join(format!("shard_{}", shard_id.0));

            let migration_start = std::time::Instant::now();
            let shard_result =
                warm_tier::migrate_shard_segments(&segment_paths, &warm_dir, config.zstd_level);
            let migration_secs = migration_start.elapsed().as_secs_f64();

            // Non-disruptive swap: catalog still points to original path
            // during re-compression. Only after verifying the warm copy
            // exists do we atomically swap the catalog entry.
            if shard_result.segments_recompressed > 0 {
                let mut catalog = self.catalog.write();
                for entry in &entries {
                    if let Some(file_name) = entry.path.file_name() {
                        let warm_path = warm_dir.join(file_name);
                        if warm_path.exists() {
                            let new_size = std::fs::metadata(&warm_path)
                                .map(|m| m.len())
                                .unwrap_or(entry.byte_size);
                            let mut updated = entry.clone();
                            updated.path = warm_path;
                            updated.byte_size = new_size;
                            // Atomic swap: remove then add under the same lock.
                            // If add fails, re-insert the original to avoid orphaning.
                            if let Err(e) = catalog.remove_segment(entry.segment_id) {
                                warn!(segment_id = ?entry.segment_id, error = %e, "warm_tier: failed to remove old catalog entry");
                                continue;
                            }
                            if let Err(e) = catalog.add_segment(updated) {
                                warn!(segment_id = ?entry.segment_id, error = %e, "warm_tier: failed to add updated catalog entry, rolling back");
                                // Rollback: re-insert the original entry
                                if let Err(e2) = catalog.add_segment(entry.clone()) {
                                    error!(segment_id = ?entry.segment_id, error = %e2, "warm_tier: rollback failed — segment lost from catalog");
                                }
                            }
                        }
                    }
                }
                // Catalog now points to warm copies — safe to remove originals.
                for entry in &entries {
                    if let Some(file_name) = entry.path.file_name() {
                        let warm_path = warm_dir.join(file_name);
                        if warm_path.exists() {
                            if let Err(e) = std::fs::remove_file(&entry.path) {
                                if e.kind() != std::io::ErrorKind::NotFound {
                                    warn!(path = %entry.path.display(), error = %e, "warm_tier: failed to remove original segment");
                                }
                            }
                            if let Err(e) = std::fs::remove_file(entry.path.with_extension("bloom"))
                            {
                                if e.kind() != std::io::ErrorKind::NotFound {
                                    warn!(path = %entry.path.display(), error = %e, "warm_tier: failed to remove original bloom sidecar");
                                }
                            }
                        }
                    }
                }

                histogram!("chronix_warm_tier_recompression_duration_seconds")
                    .record(migration_secs);
            }

            total_result.shards_moved += shard_result.shards_moved;
            total_result.segments_recompressed += shard_result.segments_recompressed;
            total_result.bytes_saved += shard_result.bytes_saved;
        }

        if total_result.shards_moved > 0 {
            counter!("chronix_warm_tier_shards_moved_total")
                .increment(total_result.shards_moved as u64);
            counter!("chronix_warm_tier_bytes_saved_total").increment(total_result.bytes_saved);
            info!(
                shards = total_result.shards_moved,
                segments = total_result.segments_recompressed,
                "Warm tier migration completed"
            );
        }

        Ok(total_result)
    }
}
