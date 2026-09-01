//! Accessor methods (getters / setters) for [`Chronix`].

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use metrics::gauge;

use chronix_core::{ChronixConfig, MeasurementSchema, SchemaRegistry};
use chronix_engine::index::{SegmentCatalog, SeriesBloomFilter, TagInvertedIndex};
use chronix_engine::memtable::ShardRouter;
use chronix_engine::wal::WalWriter;
use chronix_streaming::cdc::{EventBus, FilteredSubscription, SubscriptionFilter};

use crate::error::Result;
use crate::lock_order::{BloomsLock, CatalogLock, RollupRegistryLock};
use crate::rollup::RollupRegistry;

use super::{Chronix, DatabaseStatistics};

impl Chronix {
    /// Register a custom scalar UDF that will be available in SQL queries.
    pub fn register_udf(&self, udf: Arc<datafusion::logical_expr::ScalarUDF>) {
        self.custom_udfs.write().push(udf);
    }

    /// Register a custom aggregate UDF that will be available in SQL queries.
    pub fn register_udaf(&self, udaf: Arc<datafusion::logical_expr::AggregateUDF>) {
        self.custom_udafs.write().push(udaf);
    }

    /// Configure ingest-time downsampling rules.
    ///
    /// Incoming points matching a rule's source measurement will be
    /// accumulated in memory and emitted as aggregated points to the
    /// target measurement when a time-bucket boundary is crossed.
    ///
    /// Rules follow the same [`RollupConfig`](crate::rollup::RollupConfig)
    /// format as compaction-time rollups.
    pub fn set_ingest_downsampling(&mut self, rules: Vec<crate::rollup::RollupConfig>) {
        self.ingest_downsampler = Arc::new(crate::rollup::IngestDownsampler::new(rules));
    }

    /// Flush any partial ingest-time downsample accumulators, emitting
    /// points for incomplete time buckets.
    pub fn flush_ingest_downsampling(&self) -> Result<usize> {
        let points = self.ingest_downsampler.flush_all();
        if points.is_empty() {
            return Ok(0);
        }
        // Report what was actually stored, not what was offered: a batch
        // insert returns `Ok` even when the memtable rejects points (e.g.
        // too far out of order), so returning `points.len()` claimed success
        // for writes that never landed.
        let result = self.insert_batch(&points)?;
        if result.is_partial() {
            tracing::warn!(
                offered = points.len(),
                inserted = result.memtable_inserted,
                rejected = result.errors.len(),
                "ingest downsample flush partially rejected"
            );
        }
        Ok(result.memtable_inserted)
    }

    /// Return a snapshot of all runtime-registered scalar UDFs.
    pub fn custom_udfs(&self) -> Vec<Arc<datafusion::logical_expr::ScalarUDF>> {
        self.custom_udfs.read().clone()
    }

    /// Return a snapshot of all runtime-registered aggregate UDFs.
    pub fn custom_udafs(&self) -> Vec<Arc<datafusion::logical_expr::AggregateUDF>> {
        self.custom_udafs.read().clone()
    }

    /// Return the schema for a measurement, if it exists.
    #[must_use]
    pub fn schema(&self, measurement: &str) -> Option<Arc<MeasurementSchema>> {
        self.schema.lookup(measurement)
    }

    /// Return a snapshot of the schema registry.
    #[must_use]
    pub fn schema_registry(&self) -> &SchemaRegistry {
        &self.schema
    }

    /// Return a reference to the segment catalog.
    pub fn catalog(&self) -> &Arc<CatalogLock<SegmentCatalog>> {
        &self.catalog
    }

    /// Return the shard router.
    #[must_use]
    pub fn shards(&self) -> &Arc<ShardRouter> {
        &self.shards
    }

    /// Return the database configuration.
    #[must_use]
    pub fn config(&self) -> &ChronixConfig {
        &self.config
    }

    /// Return a snapshot of current database statistics.
    ///
    /// Provides on-demand database metrics including series
    /// cardinality, segment counts, memory usage, and WAL position.
    /// Also emits the values as gauge metrics via the `metrics` crate.
    ///
    /// `series_count` is the **exact** number of live series — the same
    /// number the write-path cardinality limit is checked against, so the
    /// gauge and the admission decision cannot disagree.
    #[must_use]
    pub fn statistics(&self) -> DatabaseStatistics {
        let series_count = self.known_series.len();
        let segment_count = {
            let catalog = self.catalog.read();
            catalog.segment_count()
        };
        let shard_count = {
            let ti = self.time_index.read();
            ti.len()
        };
        let memtable_memory_bytes = self.shards.total_memory();
        let measurement_count = self.schema.measurement_count();
        let wal_sequence = self.wal.current_sequence();
        let tombstone_count = {
            let ts = self.tombstones.read();
            ts.len()
        };
        let metadata_cache_entries = self.metadata_cache.len();

        // Emit gauge metrics so Prometheus/OTLP scrapers pick them up.
        gauge!("chronix_series_count").set(series_count as f64);
        gauge!("chronix_segment_count").set(segment_count as f64);
        gauge!("chronix_shard_count").set(shard_count as f64);
        gauge!("chronix_memtable_memory_bytes").set(memtable_memory_bytes as f64);
        gauge!("chronix_measurement_count").set(measurement_count as f64);
        gauge!("chronix_wal_sequence").set(wal_sequence as f64);
        gauge!("chronix_tombstone_count").set(tombstone_count as f64);
        gauge!("chronix_metadata_cache_entries").set(metadata_cache_entries as f64);

        DatabaseStatistics {
            series_count,
            segment_count,
            shard_count,
            memtable_memory_bytes,
            measurement_count,
            wal_sequence,
            tombstone_count,
            metadata_cache_entries,
        }
    }

    /// Return the WAL writer.
    #[must_use]
    pub fn wal(&self) -> &Arc<WalWriter> {
        &self.wal
    }

    /// Return the flush notification handle.
    ///
    /// The [`FlushScheduler`](crate::flush_scheduler::FlushScheduler)
    /// can call `set_flush_notify` to wire
    /// in its own `Notify`, then the write path's `maybe_flush()` will
    /// signal it instead of flushing inline.
    #[must_use]
    pub fn flush_notify(&self) -> &Arc<tokio::sync::Notify> {
        &self.flush_notify
    }

    /// Return the path to the data directory.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }

    /// Return the current WAL sequence number.
    #[must_use]
    pub fn wal_sequence(&self) -> u64 {
        self.wal.current_sequence()
    }

    /// Return a reference to the bloom filter map for query pruning.
    pub fn bloom_filters(&self) -> &Arc<BloomsLock<BTreeMap<u64, SeriesBloomFilter>>> {
        &self.blooms
    }

    /// Return a reference to the tag inverted index.
    ///
    /// Used for enumerating tag keys and values (e.g. PromQL `label_values`).
    pub fn tag_index(&self) -> &Arc<TagInvertedIndex> {
        &self.tag_index
    }

    /// Return all distinct tag key names present in the index.
    ///
    /// This is an O(n) scan over the inverted index keys, but cheap
    /// because the index already partitions entries by `"key=value"`.
    /// Results are sorted alphabetically.
    #[must_use]
    pub fn tag_keys(&self) -> Vec<String> {
        self.tag_index.all_keys()
    }

    /// Return all distinct values for a given tag key.
    ///
    /// This scans the inverted index for entries matching `"key="` prefix
    /// and collects the unique values. Results are sorted alphabetically.
    #[must_use]
    pub fn tag_values(&self, key: &str) -> Vec<String> {
        self.tag_index.values_for_key(key)
    }

    /// Access the underlying CDC event bus.
    ///
    /// Use this to create raw [`chronix_streaming::cdc::Subscription`]s or inspect
    /// bus statistics (published/lagged counts).
    pub fn event_bus(&self) -> &EventBus {
        &self.cdc_bus
    }

    /// Create a filtered CDC subscription.
    ///
    /// Returns a [`FilteredSubscription`] that only delivers events matching
    /// the provided filter criteria (measurement, tags, event type).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use chronix_streaming::cdc::SubscriptionFilter;
    /// use chronix::Chronix;
    /// # let db: Chronix = todo!();
    ///
    /// let filter = SubscriptionFilter::all()
    ///     .measurement("cpu")
    ///     .event_type("point_written");
    /// let sub = db.subscribe(filter);
    /// ```
    pub fn subscribe(&self, filter: SubscriptionFilter) -> FilteredSubscription {
        FilteredSubscription::new(&self.cdc_bus, filter)
    }

    /// Return a reference to the rollup registry.
    pub fn rollup_registry(&self) -> &Arc<RollupRegistryLock<RollupRegistry>> {
        &self.rollup_registry
    }
}
