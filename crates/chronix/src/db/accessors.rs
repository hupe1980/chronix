//! Accessor methods (getters / setters) for [`Chronix`].

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::error::{DbError, Result};

use metrics::gauge;

use chronix_core::{ChronixConfig, MeasurementSchema, SchemaRegistry};
use chronix_engine::index::{SegmentCatalog, SeriesBloomFilter, TagInvertedIndex};
use chronix_engine::memtable::ShardRouter;
use chronix_engine::wal::WalWriter;
use chronix_streaming::cdc::{EventBus, FilteredSubscription, SubscriptionFilter};

use crate::lock_order::{BloomsLock, CatalogLock, RollupRegistryLock};
use crate::rollup::RollupRegistry;

use super::{Chronix, DatabaseStatistics};

impl Chronix {
    #[cfg(feature = "sql")]
    /// Register a custom scalar UDF that will be available in SQL queries.
    pub fn register_udf(&self, udf: Arc<datafusion::logical_expr::ScalarUDF>) {
        *self.sql_ctx.write() = None;
        self.custom_udfs.write().push(udf);
    }

    #[cfg(feature = "sql")]
    /// Register a custom aggregate UDF that will be available in SQL queries.
    pub fn register_udaf(&self, udaf: Arc<datafusion::logical_expr::AggregateUDF>) {
        *self.sql_ctx.write() = None;
        self.custom_udafs.write().push(udaf);
    }

    #[cfg(feature = "sql")]
    /// Run a SQL query and collect the result.
    ///
    /// The whole SQL surface — DataFusion with Chronix's measurements as
    /// tables, `time_bucket`, `rate`, `forecast` and the rest of the
    /// analytics functions — from one call:
    ///
    /// ```no_run
    /// # use chronix::Chronix;
    /// # let db = Chronix::open_small("/tmp/db").unwrap();
    /// let batches = db.sql(
    ///     "SELECT time_bucket('5m', _time) AS t, avg(usage) FROM cpu GROUP BY t ORDER BY t",
    /// )?;
    /// # Ok::<(), chronix::DbError>(())
    /// ```
    ///
    /// Queries are read-only, verified before they are planned: `SET`,
    /// DDL and `COPY` are rejected, exactly as on the server. Writes go
    /// through [`insert_batch`](Self::insert_batch).
    ///
    /// This call **blocks**: on a private runtime from synchronous code, or
    /// by stepping the current worker aside on a multi-thread tokio runtime.
    /// On a current-thread runtime it is an error rather than a deadlock —
    /// use [`sql_async`](Self::sql_async) there.
    ///
    /// # Errors
    ///
    /// Returns [`DbError::Sql`] for a query that does not parse, plan or
    /// execute, and [`DbError::Internal`] when called from inside a
    /// current-thread async runtime.
    pub fn sql(&self, query: &str) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // On a multi-thread runtime the worker can step aside for the
            // duration; on a current-thread runtime blocking would deadlock
            // the only worker, so that is an error rather than a hang.
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
                return tokio::task::block_in_place(|| handle.block_on(self.sql_async(query)));
            }
            return Err(DbError::Internal(
                "Chronix::sql() blocks and was called inside a current-thread async runtime; \
                 use sql_async()"
                    .into(),
            ));
        }
        let runtime = self.sql_runtime.get_or_init(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a current-thread tokio runtime can always be built")
        });
        runtime.block_on(self.sql_async(query))
    }

    #[cfg(feature = "sql")]
    /// [`sql`](Self::sql) for async callers.
    ///
    /// # Errors
    ///
    /// As [`sql`](Self::sql).
    pub async fn sql_async(&self, query: &str) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        let ctx = self.session_context();
        let df = crate::sql::sql_read_only(&ctx, query).await?;
        Ok(df.collect().await?)
    }

    #[cfg(feature = "sql")]
    /// The DataFusion [`SessionContext`](datafusion::execution::context::SessionContext)
    /// behind [`sql`](Self::sql): every measurement as a table, all
    /// analytics functions registered. For callers that want DataFusion's
    /// own API — `DataFrame`, `EXPLAIN`, streaming execution.
    ///
    /// Built on first use and shared; rebuilt after
    /// [`register_udf`](Self::register_udf) / [`register_udaf`](Self::register_udaf).
    /// Multi-tenant servers scope their own contexts with
    /// [`create_namespaced_session_context`](crate::sql::create_namespaced_session_context).
    pub fn session_context(&self) -> datafusion::execution::context::SessionContext {
        if let Some(ctx) = self.sql_ctx.read().as_ref() {
            return ctx.clone();
        }
        let mut slot = self.sql_ctx.write();
        if let Some(ctx) = slot.as_ref() {
            return ctx.clone();
        }
        let ctx = crate::sql::create_session_context(Arc::new(self.clone()));
        *slot = Some(ctx.clone());
        ctx
    }

    /// Evaluate a PromQL instant query at `at_ns`.
    ///
    /// Measurements are metrics and tags are labels, exactly as
    /// `chronixd` serves them to Grafana; the evaluator tracks Prometheus
    /// 3.x semantics.
    ///
    /// ```no_run
    /// # use chronix::Chronix;
    /// # let db = Chronix::open_small("/tmp/db").unwrap();
    /// let now = 1_700_000_000_000_000_000;
    /// let value = db.promql(r#"rate(requests{host="a"}[5m])"#, now)?;
    /// # Ok::<(), chronix::DbError>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`DbError::PromQl`] for a query that does not parse or
    /// evaluate.
    pub fn promql(&self, query: &str, at_ns: i64) -> Result<crate::promql::ast::PromQLValue> {
        let expr = crate::promql::parse(query).map_err(|e| DbError::PromQl(e.to_string()))?;
        let params = crate::promql::eval::QueryParams {
            time: at_ns,
            ..Default::default()
        };
        crate::promql::PromQLEvaluator::new(Arc::new(self.clone()))
            .instant_query(&expr, &params)
            .map_err(|e| DbError::PromQl(e.0))
    }

    /// Evaluate a PromQL range query over `[start_ns, end_ns]` at `step_ns`.
    ///
    /// The result is one series per label set, one sample per step; the
    /// window is read once, not once per step.
    ///
    /// # Errors
    ///
    /// As [`promql`](Self::promql); a non-positive step is an error.
    pub fn promql_range(
        &self,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
    ) -> Result<crate::promql::ast::PromQLValue> {
        let expr = crate::promql::parse(query).map_err(|e| DbError::PromQl(e.to_string()))?;
        let params = crate::promql::eval::QueryParams {
            time: end_ns,
            start: Some(start_ns),
            end: Some(end_ns),
            step: Some(step_ns),
            ..Default::default()
        };
        crate::promql::PromQLEvaluator::new(Arc::new(self.clone()))
            .range_query(&expr, &params)
            .map_err(|e| DbError::PromQl(e.0))
    }

    #[cfg(feature = "sql")]
    /// Return a snapshot of all runtime-registered scalar UDFs.
    pub fn custom_udfs(&self) -> Vec<Arc<datafusion::logical_expr::ScalarUDF>> {
        self.custom_udfs.read().clone()
    }

    #[cfg(feature = "sql")]
    /// Return a snapshot of all runtime-registered aggregate UDFs.
    pub fn custom_udafs(&self) -> Vec<Arc<datafusion::logical_expr::AggregateUDF>> {
        self.custom_udafs.read().clone()
    }

    /// Return the schema for a measurement, if it exists.
    ///
    /// `None` for a measurement pending a soft-delete, exactly as for one
    /// that never existed: a "drop" that leaves the data fully readable for
    /// its whole grace period is not a drop. The segments and the schema
    /// registry entry are untouched, so a restore before the deadline is
    /// instant and lossless.
    #[must_use]
    pub fn schema(&self, measurement: &str) -> Option<Arc<MeasurementSchema>> {
        if self.catalog.read().is_measurement_pending_drop(measurement) {
            return None;
        }
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
        let shard_count = self.catalog.read().shard_count();
        let memtable_memory_bytes = self.shards.total_memory();
        let interner_memory_bytes = self.shards.total_interner_memory();
        let wal_buffer_bytes = self.wal.memory_bytes();
        let catalog_memory_bytes = self.catalog.read().memory_bytes();
        let measurement_count = self.schema.measurement_count();
        let wal_sequence = self.wal.current_sequence();
        let tombstone_count = {
            let ts = self.tombstones.read();
            ts.len()
        };
        let leased_segments = self.segment_leases.len();

        // Emit gauge metrics so Prometheus/OTLP scrapers pick them up.
        gauge!("chronix_series_count").set(series_count as f64);
        gauge!("chronix_segment_count").set(segment_count as f64);
        gauge!("chronix_shard_count").set(shard_count as f64);
        gauge!("chronix_memtable_memory_bytes").set(memtable_memory_bytes as f64);
        gauge!("chronix_interner_memory_bytes").set(interner_memory_bytes as f64);
        gauge!("chronix_wal_buffer_bytes").set(wal_buffer_bytes as f64);
        gauge!("chronix_catalog_memory_bytes").set(catalog_memory_bytes as f64);
        gauge!("chronix_measurement_count").set(measurement_count as f64);
        gauge!("chronix_wal_sequence").set(wal_sequence as f64);
        gauge!("chronix_tombstone_count").set(tombstone_count as f64);
        gauge!("chronix_leased_segments").set(leased_segments as f64);
        gauge!("chronix_storage_disk_usage_bytes").set(self.disk_usage_bytes() as f64);

        DatabaseStatistics {
            series_count,
            segment_count,
            shard_count,
            memtable_memory_bytes,
            interner_memory_bytes,
            wal_buffer_bytes,
            catalog_memory_bytes,
            measurement_count,
            wal_sequence,
            tombstone_count,
            leased_segments,
        }
    }

    /// Bytes the database occupies on disk, cached for a minute.
    ///
    /// Disk usage is the storage number an operator actually alerts on, and
    /// nothing exported it. It needs a directory walk, so the result is
    /// cached: `statistics()` runs on every metrics scrape, and a walk per
    /// scrape would make the exporter the most expensive thing in the
    /// process once a deployment holds many segments.
    #[must_use]
    pub fn disk_usage_bytes(&self) -> u64 {
        const TTL: Duration = Duration::from_secs(60);

        {
            let cached = self.disk_usage.read();
            if let Some((measured_at, bytes)) = *cached {
                if measured_at.elapsed() < TTL {
                    return bytes;
                }
            }
        }

        let bytes = directory_size(self.data_dir());
        *self.disk_usage.write() = Some((Instant::now(), bytes));
        bytes
    }

    /// Measurement names this namespace holds series for.
    ///
    /// `None` means unscoped — every measurement, which is what a
    /// single-tenant deployment wants and what the embedded API always
    /// wants.
    ///
    /// The SQL catalog answers from this rather than from the process-wide
    /// schema registry, which listed **every** tenant's measurement names:
    /// `SELECT * FROM another_tenants_measurement` returned zero rows where a
    /// name that does not exist errors, so a tenant could enumerate the
    /// others by probing.
    /// A measurement pending a whole-measurement drop is filtered out of
    /// **both** branches. It used to be filtered out of the unscoped one
    /// only, so a drop with a grace period configured was invisible on a
    /// single-tenant server and fully listed on a tenanted one — one
    /// question, two answers, in one function.
    #[must_use]
    pub fn measurement_names_in(&self, namespace: Option<&str>) -> Vec<String> {
        let pending = self.catalog.read().pending_measurement_drops().clone();
        let Some(namespace) = namespace else {
            return self
                .schema
                .measurement_names()
                .into_iter()
                .filter(|m| !pending.contains_key(m))
                .collect();
        };
        let mut names: Vec<String> = self
            .namespace_measurements
            .get(namespace)
            .map(|set| {
                set.iter()
                    .filter(|m| !pending.contains_key(m.as_str()))
                    .map(|m| m.clone())
                    .collect()
            })
            .unwrap_or_default();
        names.sort_unstable();
        names
    }

    /// Whether `namespace` holds any series of `measurement`.
    ///
    /// `None` means unscoped, and then only the measurement's existence
    /// matters. A measurement pending a drop exists for nobody, on either
    /// branch — the scoped one used to say yes, so under tenancy a dropped
    /// table still planned and answered zero rows where a name that does not
    /// exist is a planning error.
    #[must_use]
    pub fn has_measurement_in(&self, namespace: Option<&str>, measurement: &str) -> bool {
        if self.is_measurement_pending_drop(measurement) {
            return false;
        }
        match namespace {
            None => self.schema(measurement).is_some(),
            Some(namespace) => self
                .namespace_measurements
                .get(namespace)
                .is_some_and(|set| set.contains(measurement)),
        }
    }

    /// Return the WAL writer.
    #[must_use]
    pub fn wal(&self) -> &Arc<WalWriter> {
        &self.wal
    }

    /// Return the path to the data directory.
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }

    /// Number of WAL records `open()` replayed into the memtable.
    ///
    /// Zero after a graceful `close()`: everything acknowledged was already
    /// in a segment, and the catalog records the WAL floor so replay has
    /// nothing to do.
    #[must_use]
    pub fn wal_replayed_records(&self) -> usize {
        self.replayed_records
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

/// Total size of every regular file under `root`, following no symlinks.
///
/// Errors are skipped rather than propagated: a file that vanished
/// mid-walk (a compaction finishing) is normal, and a disk-usage figure
/// that is a little stale is more useful than none.
fn directory_size(root: &Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                stack.push(entry.path());
            } else if meta.is_file() {
                total += meta.len();
            }
        }
    }
    total
}

/// Build the namespace → measurements index from a set of canonical series
/// keys.
///
/// A canonical key is `measurement\0k1\0v1\0k2\0v2…` with the tags sorted,
/// so both halves are already here — the index only makes the lookup cheap
/// enough for the SQL planner to ask on every table reference.
pub(super) fn index_by_namespace(
    known_series: &dashmap::DashSet<String>,
) -> dashmap::DashMap<String, dashmap::DashSet<String>> {
    let index = dashmap::DashMap::new();
    for key in known_series.iter() {
        record_series(&index, key.as_str());
    }
    index
}

/// Record one canonical series key in the index.
pub(super) fn record_series(
    index: &dashmap::DashMap<String, dashmap::DashSet<String>>,
    canonical: &str,
) {
    let Some((measurement, namespace)) = split_canonical(canonical) else {
        return;
    };
    index
        .entry(namespace.to_string())
        .or_default()
        .insert(measurement.to_string());
}

/// The measurement and namespace of a canonical series key.
///
/// A canonical key is `measurement` then, for each tag in sorted key order,
/// [`TAG_SEPARATOR`], the key, [`KV_SEPARATOR`], the value. Both separators
/// are control characters that [`SeriesKey::validate_name`] rejects in user
/// data, so splitting on them is unambiguous.
///
/// The namespace is `""` when the series carries no namespace tag, which is
/// every series on a single-tenant deployment.
///
/// [`TAG_SEPARATOR`]: chronix_core::TAG_SEPARATOR
/// [`KV_SEPARATOR`]: chronix_core::KV_SEPARATOR
/// [`SeriesKey::validate_name`]: chronix_core::SeriesKey::validate_name
pub(super) fn split_canonical(canonical: &str) -> Option<(&str, &str)> {
    let mut parts = canonical.split(chronix_core::TAG_SEPARATOR);
    let measurement = parts.next()?;
    let namespace = parts
        .find_map(|tag| {
            let (key, value) = tag.split_once(chronix_core::KV_SEPARATOR)?;
            (key == chronix_core::NAMESPACE_TAG).then_some(value)
        })
        .unwrap_or("");
    Some((measurement, namespace))
}
