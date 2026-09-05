//! gRPC service implementation for the Chronix API.
//!
//! Implements `chronix_service_server::ChronixService` backed by the
//! same `Arc<Chronix>` database instance shared with the REST layer.
//!
//! ## HTTP 504 vs gRPC `DeadlineExceeded` semantics
//!
//! Timeout behaviour differs between the HTTP and gRPC surfaces:
//!
//! | Surface   | Timeout mechanism                          | Error returned              |
//! |-----------|--------------------------------------------|-----------------------------||
//! | HTTP/REST | `tokio::time::timeout` → `ServerError`     | HTTP 500 with JSON body     |
//! | gRPC      | `tonic::Status::deadline_exceeded`         | gRPC `DEADLINE_EXCEEDED`    |
//! | Flight SQL| Same as gRPC (tonic)                       | gRPC `DEADLINE_EXCEEDED`    |
//!
//! Clients consuming both APIs should handle `HTTP 500` (timeout string)
//! and gRPC code `4` (`DEADLINE_EXCEEDED`) as equivalent timeout errors.
//! A future improvement could return HTTP **504 Gateway Timeout** from
//! the REST layer for clearer semantics.

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, warn};

use chronix::prelude::*;
use chronix::Chronix;

use crate::error::ServerError;
use crate::proto;

/// gRPC service handler with server-side batching and deduplication.
pub struct ChronixGrpcService {
    db: Arc<Chronix>,
    start_time: Instant,
    /// Per-namespace DataFusion SQL session contexts.
    ///
    /// One shared context would make every gRPC `SqlQuery` read every
    /// tenant's rows regardless of the `x-namespace` metadata.
    sql_contexts: crate::namespace::SqlContexts,
    /// Batch size threshold for `StreamWrite`.
    stream_batch_size: usize,
    /// Batch flush interval for `StreamWrite`.
    stream_batch_interval: Duration,
    /// Deduplication window (0 = disabled).
    dedup_window: Duration,
    /// Dedup cache: maps dedup_key → insertion instant.
    ///
    /// Uses full key strings to prevent silent write
    /// rejection on hash collision.
    dedup_cache: Arc<Mutex<HashMap<String, Instant>>>,
    /// Maximum number of dedup entries before eviction.
    max_dedup_entries: usize,
    /// SQL query timeout (0 = no timeout).
    sql_query_timeout: Duration,
    /// Maximum rows returned by a single SQL query.
    sql_max_rows: usize,
    /// Maximum duration for a single write batch (`Duration::ZERO` = no deadline).
    /// Prevents stuck writes from exhausting the thread-pool.
    write_timeout: Duration,
    /// Whether namespace isolation is enforced (`multi_tenancy` in the config).
    multi_tenancy: bool,
}

/// Default max dedup entries.
const DEFAULT_MAX_DEDUP_ENTRIES: usize = 1_000_000;

impl ChronixGrpcService {
    /// Create a new gRPC service backed by the given database.
    pub fn new(db: Arc<Chronix>, start_time: Instant) -> Self {
        let sql_contexts = crate::namespace::SqlContexts::new(db.clone());
        Self {
            db,
            start_time,
            sql_contexts,
            stream_batch_size: 10_000,
            stream_batch_interval: Duration::from_millis(100),
            dedup_window: Duration::from_secs(300),
            dedup_cache: Arc::new(Mutex::new(HashMap::new())),
            max_dedup_entries: DEFAULT_MAX_DEDUP_ENTRIES,
            sql_query_timeout: Duration::from_secs(30),
            sql_max_rows: 100_000,
            write_timeout: Duration::ZERO,
            multi_tenancy: false,
        }
    }

    /// Create a new gRPC service with custom streaming configuration.
    pub fn with_streaming_config(
        db: Arc<Chronix>,
        start_time: Instant,
        stream_batch_size: usize,
        stream_batch_interval_ms: u64,
        dedup_window_secs: u64,
    ) -> Self {
        let sql_contexts = crate::namespace::SqlContexts::new(db.clone());
        Self {
            db,
            start_time,
            sql_contexts,
            stream_batch_size,
            stream_batch_interval: Duration::from_millis(stream_batch_interval_ms),
            dedup_window: Duration::from_secs(dedup_window_secs),
            dedup_cache: Arc::new(Mutex::new(HashMap::new())),
            max_dedup_entries: DEFAULT_MAX_DEDUP_ENTRIES,
            sql_query_timeout: Duration::from_secs(30),
            sql_max_rows: 100_000,
            write_timeout: Duration::ZERO,
            multi_tenancy: false,
        }
    }

    /// Enforce namespace isolation, reading the namespace from `x-namespace`.
    #[must_use]
    pub fn with_multi_tenancy(mut self, enabled: bool) -> Self {
        self.multi_tenancy = enabled;
        self
    }

    /// Override the maximum number of dedup-cache entries.
    pub fn with_max_dedup_entries(mut self, max_entries: usize) -> Self {
        self.max_dedup_entries = max_entries;
        self
    }

    /// Set SQL query timeout and max rows for parity with the REST endpoint.
    pub fn with_sql_limits(mut self, timeout_secs: u64, max_rows: usize) -> Self {
        self.sql_query_timeout = Duration::from_secs(timeout_secs);
        self.sql_max_rows = max_rows;
        self
    }

    /// Set the write-batch timeout.
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }
}

type GrpcResult<T> = Result<Response<T>, Status>;
type QueryStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<proto::QueryResponse, Status>> + Send>>;

#[tonic::async_trait]
impl proto::chronix_service_server::ChronixService for ChronixGrpcService {
    /// Write a batch of points.
    async fn write(
        &self,
        request: Request<proto::WriteRequest>,
    ) -> GrpcResult<proto::WriteResponse> {
        let scope = crate::namespace::scope_from_request(self.multi_tenancy, &request)?;
        let req = request.into_inner();
        let points = req
            .points
            .iter()
            .cloned()
            .map(proto_point_to_point)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_grpc_status())?;

        let count = points.len() as u64;

        crate::util::insert_batch_with_mode(
            &self.db,
            scope.as_deref(),
            points,
            self.write_timeout,
            crate::util::WriteMode::from_flag(req.backfill),
        )
        .await
        .map_err(|e| e.to_grpc_status())?;

        debug!(count, "wrote points via gRPC");
        metrics::counter!("chronix_grpc_points_written_total").increment(count);

        Ok(Response::new(proto::WriteResponse { written: count }))
    }

    /// Client-streaming write with server-side batching, deduplication,
    /// and at-least-once delivery semantics.
    ///
    /// Points are accumulated in a buffer and flushed when either the batch
    /// size threshold or the batch interval is reached. When the client
    /// closes the stream, remaining buffered points are drained before the
    /// final acknowledgement is returned (graceful close).
    ///
    /// If a `dedup_key` is provided on a message and was already seen
    /// within the deduplication window, the message is silently skipped.
    async fn stream_write(
        &self,
        request: Request<Streaming<proto::StreamWriteRequest>>,
    ) -> GrpcResult<proto::StreamWriteResponse> {
        let scope = crate::namespace::scope_from_request(self.multi_tenancy, &request)?;
        let mut stream = request.into_inner();
        let mut total_written: u64 = 0;
        let mut last_batch_written: u64 = 0;
        let mut buffer: Vec<Point> = Vec::with_capacity(self.stream_batch_size);
        // Dedup keys of the messages sitting in `buffer`. They become
        // "already written" only when that buffer reaches the database.
        let mut pending_keys: Vec<String> = Vec::new();

        let mut interval = tokio::time::interval(self.stream_batch_interval);
        interval.tick().await; // consume initial immediate tick

        loop {
            tokio::select! {
                biased; // prefer messages over timer ticks

                msg = stream.message() => {
                    match msg? {
                        Some(req) => {
                            // ── Dedup check ────────────────────────────
                            if !req.dedup_key.is_empty() && !self.dedup_window.is_zero() {
                                let mut cache = self.dedup_cache.lock().await;
                                let now = Instant::now();
                                // Evict expired entries on each dedup check
                                cache.retain(|_, ts| now.duration_since(*ts) < self.dedup_window);

                                // Cap cache size to prevent unbounded memory growth
                                // under high-cardinality dedup keys.
                                if cache.len() >= self.max_dedup_entries {
                                    // Evict oldest quarter when cap is hit.
                                    let mut entries: Vec<_> = cache.iter().map(|(k, &v)| (k.clone(), v)).collect();
                                    entries.sort_by_key(|(_, ts)| *ts);
                                    let evict_count = entries.len() / 4;
                                    for (key, _) in entries.into_iter().take(evict_count) {
                                        cache.remove(&key);
                                    }
                                }

                                if cache.contains_key(&req.dedup_key)
                                    || pending_keys.iter().any(|k| k == &req.dedup_key)
                                {
                                    debug!(dedup_key = %req.dedup_key, "skipping duplicate stream message");
                                    continue;
                                }
                                // **Not committed yet.** The points go into a
                                // buffer and are flushed later, so recording
                                // the key here marks a write that has not
                                // happened. If a flush then failed, the
                                // stream errored, the client reconnected and
                                // resent — and every resent message was
                                // skipped as a duplicate, which turns
                                // at-least-once delivery into never.
                                pending_keys.push(req.dedup_key.clone());
                            }

                            // ── Parse points ───────────────────────────
                            let points = req
                                .points
                                .into_iter()
                                .map(proto_point_to_point)
                                .collect::<Result<Vec<_>, _>>()
                                .map_err(|e| e.to_grpc_status())?;
                            buffer.extend(points);

                            // ── Flush if batch threshold reached ───────
                            if buffer.len() >= self.stream_batch_size {
                                let count = flush_point_buffer(&self.db, scope.as_deref(), &mut buffer, self.write_timeout).await?;
                                total_written += count;
                                last_batch_written = count;
                                commit_dedup_keys(&self.dedup_cache, &mut pending_keys).await;
                            }
                        }
                        None => {
                            // ── Graceful close: drain remaining buffer ─
                            if !buffer.is_empty() {
                                let count = flush_point_buffer(&self.db, scope.as_deref(), &mut buffer, self.write_timeout).await?;
                                total_written += count;
                                last_batch_written = count;
                                commit_dedup_keys(&self.dedup_cache, &mut pending_keys).await;
                            }
                            break;
                        }
                    }
                }

                _ = interval.tick() => {
                    // ── Periodic flush ──────────────────────────────
                    if !buffer.is_empty() {
                        let count = flush_point_buffer(&self.db, scope.as_deref(), &mut buffer, self.write_timeout).await?;
                        total_written += count;
                        last_batch_written = count;
                        commit_dedup_keys(&self.dedup_cache, &mut pending_keys).await;
                    }
                }
            }
        }

        debug!(total_written, "stream write completed via gRPC");
        metrics::counter!("chronix_grpc_stream_points_written_total").increment(total_written);

        Ok(Response::new(proto::StreamWriteResponse {
            total_written,
            batch_written: last_batch_written,
        }))
    }

    /// Server-streaming query.
    type QueryStream = QueryStream;

    async fn query(&self, request: Request<proto::QueryRequest>) -> GrpcResult<Self::QueryStream> {
        let scope = crate::namespace::scope_from_request(self.multi_tenancy, &request)?;
        let req = request.into_inner();
        let db = self.db.clone();
        let measurement = req.measurement.clone();

        // Check measurement exists
        if db.schema(&measurement).is_none() {
            return Err(Status::not_found(format!(
                "measurement not found: {measurement}"
            )));
        }

        let (tx, rx) = mpsc::channel(64);

        tokio::task::spawn_blocking(move || {
            let mut builder = db
                .query()
                .measurement(&req.measurement)
                .namespace_scope(scope.as_deref());

            if let Some(ref range) = req.range {
                builder = builder.range(range.start, range.end);
            }

            for tag in &req.tag_filters {
                builder = builder.tag(&tag.key, &tag.value);
            }

            if !req.field_columns.is_empty() {
                for f in &req.field_columns {
                    builder = builder.field(f);
                }
            }

            let plan = match builder.build() {
                Ok(p) => p,
                Err(e) => {
                    if tx
                        .blocking_send(Err(Status::invalid_argument(e.to_string())))
                        .is_err()
                    {
                        warn!("query response channel closed before error could be sent");
                    }
                    return;
                }
            };
            // `execute_iter` reads one time bucket of segments per step, so
            // the server never holds the whole result set and a `limit` stops
            // the scan instead of merely truncating it.
            let stream = match db.execute_iter(&plan) {
                Ok(s) => s,
                Err(e) => {
                    if tx
                        .blocking_send(Err(ServerError::Db(e).to_grpc_status()))
                        .is_err()
                    {
                        warn!("query response channel closed before error could be sent");
                    }
                    return;
                }
            };

            let limit = req.limit as usize;
            let mut remaining = if limit > 0 { limit } else { usize::MAX };

            for batch in stream {
                if remaining == 0 {
                    break;
                }
                let batch = match batch {
                    Ok(b) => b,
                    Err(e) => {
                        if tx
                            .blocking_send(Err(ServerError::Db(e).to_grpc_status()))
                            .is_err()
                        {
                            warn!("query response channel closed before error could be sent");
                        }
                        return;
                    }
                };
                let rows = record_batch_to_proto_rows(&batch);

                // Apply global limit across all batches
                let rows: Vec<_> = if rows.len() > remaining {
                    rows.into_iter().take(remaining).collect()
                } else {
                    rows
                };
                remaining = remaining.saturating_sub(rows.len());

                let response = proto::QueryResponse { rows };
                if tx.blocking_send(Ok(response)).is_err() {
                    return; // Client disconnected
                }
            }
        });

        metrics::counter!("chronix_grpc_queries_total").increment(1);

        let stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::QueryStream))
    }

    /// Get measurement schema.
    async fn get_schema(
        &self,
        request: Request<proto::SchemaRequest>,
    ) -> GrpcResult<proto::SchemaResponse> {
        let measurement = request.into_inner().measurement;
        let schema = self
            .db
            .schema(&measurement)
            .ok_or_else(|| Status::not_found(format!("measurement not found: {measurement}")))?;

        let columns = schema_to_proto_columns(&schema);

        Ok(Response::new(proto::SchemaResponse {
            measurement,
            columns,
        }))
    }

    /// List all measurements.
    async fn list_measurements(
        &self,
        _request: Request<proto::ListMeasurementsRequest>,
    ) -> GrpcResult<proto::ListMeasurementsResponse> {
        let db = self.db.clone();
        // The schema registry is process-wide, so listing it verbatim told
        // every tenant what the others were writing, column names included.
        // A namespace sees the measurements it holds data for — the same
        // rule the HTTP and Prometheus listings follow.
        let scope = crate::namespace::scope_from_request(self.multi_tenancy, &_request)?;

        let measurements = tokio::task::spawn_blocking(move || {
            let registry = db.schema_registry();
            let names = match scope.as_deref() {
                None => registry.measurement_names(),
                Some(ns) => {
                    let now_ns = crate::util::now_nanos().unwrap_or(i64::MAX);
                    crate::namespace::measurements_in(&db, Some(ns), i64::MIN, now_ns, usize::MAX)
                }
            };

            let mut infos = Vec::new();
            for name in names {
                if let Some(schema) = registry.lookup(&name) {
                    let columns = schema_to_proto_columns(&schema);
                    infos.push(proto::MeasurementInfo { name, columns });
                }
            }

            infos
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::ListMeasurementsResponse {
            measurements,
        }))
    }

    /// Delete points matching predicates.
    async fn delete(
        &self,
        request: Request<proto::DeleteRequest>,
    ) -> GrpcResult<proto::DeleteResponse> {
        let scope = crate::namespace::scope_from_request(self.multi_tenancy, &request)?;
        let req = request.into_inner();
        let db = self.db.clone();

        #[allow(clippy::result_large_err)] // tonic::Status
        let outcome = tokio::task::spawn_blocking(move || {
            let mut builder = db.delete_builder().measurement(&req.measurement);
            if let Some(ref ns) = scope {
                builder = builder.tag(chronix_core::NAMESPACE_TAG, ns);
            }
            for tag in &req.tag_filters {
                builder = builder.tag(&tag.key, &tag.value);
            }
            if let Some(ref range) = req.range {
                builder = builder.range(range.start, range.end);
            }
            let request = builder
                .build()
                .map_err(|e| Status::invalid_argument(format!("delete build error: {e}")))?;
            db.execute_delete(&request)
                .map_err(|e| ServerError::Db(e).to_grpc_status())
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))??;

        Ok(Response::new(proto::DeleteResponse {
            deleted: outcome.series_tombstoned,
            segments_skipped: outcome.segments_skipped,
        }))
    }

    /// Drop an entire measurement.
    async fn drop_measurement(
        &self,
        request: Request<proto::DropMeasurementRequest>,
    ) -> GrpcResult<proto::DropMeasurementResponse> {
        let scope = crate::namespace::scope_from_request(self.multi_tenancy, &request)?;
        let name = request.into_inner().measurement;
        let db = self.db.clone();

        // A measurement is shared between tenants, so under multi-tenancy
        // "drop" means "delete this namespace's series of it" — dropping the
        // measurement itself would take every tenant's data and the schema.
        match scope {
            None => {
                tokio::task::spawn_blocking(move || db.drop_measurement(&name))
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
                    .map_err(|e| ServerError::Db(e).to_grpc_status())?;
            }
            Some(ns) => {
                let request =
                    crate::namespace::scoped_delete_request(&db, Some(&ns), &name, [], None)
                        .map_err(|e| e.to_grpc_status())?;
                tokio::task::spawn_blocking(move || db.execute_delete(&request))
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
                    .map_err(|e| ServerError::Db(e).to_grpc_status())?;
            }
        }

        Ok(Response::new(proto::DropMeasurementResponse {}))
    }

    /// Server metadata.
    async fn server_info(
        &self,
        _request: Request<proto::ServerInfoRequest>,
    ) -> GrpcResult<proto::ServerInfoResponse> {
        let uptime = self.start_time.elapsed().as_secs();
        let db = self.db.clone();

        let measurement_count =
            tokio::task::spawn_blocking(move || db.schema_registry().measurement_count() as u64)
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(Response::new(proto::ServerInfoResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds: uptime,
            measurement_count,
        }))
    }

    /// Execute a SQL query via DataFusion.
    ///
    /// Uses `DataFrame::execute_stream()` so that row-limit enforcement
    /// applies back-pressure upstream — no excess data is materialised.
    async fn execute_sql(
        &self,
        request: Request<proto::SqlRequest>,
    ) -> GrpcResult<proto::SqlResponse> {
        let scope = crate::namespace::scope_from_request(self.multi_tenancy, &request)?;
        let query = request.into_inner().query;
        debug!(%query, "gRPC ExecuteSql");

        // Plan and verify *before* executing. `SessionContext::sql`
        // would apply DDL/`SET` side effects to the shared session before we
        // ever got a chance to inspect the plan.
        let sql_ctx = self.sql_contexts.get(scope.as_deref());
        let df = chronix::sql::sql_read_only(&sql_ctx, &query)
            .await
            .map_err(|e| Status::invalid_argument(format!("SQL error: {e}")))?;

        // Stream batches with timeout instead of full collect().
        use futures::StreamExt;
        let stream_future = df.execute_stream();
        let mut stream = if self.sql_query_timeout.is_zero() {
            stream_future.await
        } else {
            tokio::time::timeout(self.sql_query_timeout, stream_future)
                .await
                .map_err(|_| Status::deadline_exceeded("SQL query timed out"))?
        }
        .map_err(|e| Status::internal(format!("SQL execution error: {e}")))?;

        // Get schema from the stream before consuming.
        let schema = stream.schema();
        let columns: Vec<proto::SqlColumnMeta> = schema
            .fields()
            .iter()
            .map(|f| proto::SqlColumnMeta {
                name: f.name().clone(),
                data_type: format!("{:?}", f.data_type()),
            })
            .collect();

        // Consume batches incrementally, enforcing row limit with
        // early termination (back-pressure).
        let mut rows = Vec::new();
        let mut truncated = false;
        while let Some(batch_result) = stream.next().await {
            let batch =
                batch_result.map_err(|e| Status::internal(format!("SQL execution error: {e}")))?;
            for row_idx in 0..batch.num_rows() {
                if rows.len() >= self.sql_max_rows {
                    truncated = true;
                    break;
                }
                let mut values = Vec::with_capacity(batch.num_columns());
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    values.push(arrow_to_sql_value(col, row_idx));
                }
                rows.push(proto::SqlRow { values });
            }
            if truncated {
                break;
            }
        }

        if truncated {
            warn!(
                max_rows = self.sql_max_rows,
                "gRPC SQL result truncated to max_rows"
            );
            metrics::counter!("chronix_sql_results_truncated_total").increment(1);
        }

        let row_count = rows.len() as u64;
        Ok(Response::new(proto::SqlResponse {
            columns,
            rows,
            row_count,
            truncated,
        }))
    }
}

// ── Conversion helpers ─────────────────────────────────────────────────

/// Convert an Arrow array cell to a protobuf `SqlValue`.
fn arrow_to_sql_value(col: &arrow::array::ArrayRef, row: usize) -> proto::SqlValue {
    use arrow::array::{
        Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, StringArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    };

    if col.is_null(row) {
        return proto::SqlValue {
            value: None,
            is_null: true,
        };
    }

    let value = if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        Some(proto::sql_value::Value::Float64(a.value(row)))
    } else if let Some(a) = col.as_any().downcast_ref::<Float32Array>() {
        Some(proto::sql_value::Value::Float64(a.value(row) as f64))
    } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        Some(proto::sql_value::Value::Int64(a.value(row)))
    } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        Some(proto::sql_value::Value::Int64(a.value(row) as i64))
    } else if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
        Some(proto::sql_value::Value::Int64(a.value(row) as i64))
    } else if let Some(a) = col.as_any().downcast_ref::<Int8Array>() {
        Some(proto::sql_value::Value::Int64(a.value(row) as i64))
    } else if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        Some(proto::sql_value::Value::Uint64(a.value(row)))
    } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        Some(proto::sql_value::Value::Uint64(a.value(row) as u64))
    } else if let Some(a) = col.as_any().downcast_ref::<UInt16Array>() {
        Some(proto::sql_value::Value::Uint64(a.value(row) as u64))
    } else if let Some(a) = col.as_any().downcast_ref::<UInt8Array>() {
        Some(proto::sql_value::Value::Uint64(a.value(row) as u64))
    } else if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
        Some(proto::sql_value::Value::Boolean(a.value(row)))
    } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
        Some(proto::sql_value::Value::String(a.value(row).to_string()))
    } else if let Some(a) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        Some(proto::sql_value::Value::Int64(a.value(row)))
    } else {
        // Fallback: render the data type rather than the entire array so we
        // don't accidentally Debug-print all N values for every single row,
        // which would allocate O(N²) memory and can OOM the server.
        Some(proto::sql_value::Value::String(format!(
            "<unsupported: {}>",
            col.data_type()
        )))
    };

    proto::SqlValue {
        value,
        is_null: false,
    }
}

/// Record the dedup keys of a batch that has just been written.
///
/// Called only after a successful flush: a key is a claim that a write
/// happened, and recording one for a write still sitting in a buffer is how
/// a reconnecting client's resent messages were skipped as duplicates.
async fn commit_dedup_keys(
    cache: &tokio::sync::Mutex<std::collections::HashMap<String, Instant>>,
    pending: &mut Vec<String>,
) {
    if pending.is_empty() {
        return;
    }
    let now = Instant::now();
    let mut cache = cache.lock().await;
    for key in pending.drain(..) {
        cache.insert(key, now);
    }
}

#[allow(clippy::result_large_err)] // tonic::Status is large by design
/// Flush buffered points to the database. Provides at-least-once delivery:
/// the caller receives the count only after `insert_batch` (including WAL write)
/// completes successfully.
async fn flush_point_buffer(
    db: &Arc<Chronix>,
    namespace: Option<&str>,
    buffer: &mut Vec<Point>,
    write_timeout: Duration,
) -> Result<u64, Status> {
    let points = std::mem::take(buffer);
    let count = points.len() as u64;

    crate::util::insert_with_timeout(db, namespace, points, write_timeout)
        .await
        .map_err(|e| e.to_grpc_status())?;

    debug!(count, "flushed stream write buffer");
    Ok(count)
}

/// Convert a protobuf [`proto::Point`] to a Chronix [`Point`].
fn proto_point_to_point(p: proto::Point) -> Result<Point, ServerError> {
    let tags: BTreeMap<String, String> = p.tags.into_iter().map(|t| (t.key, t.value)).collect();

    let fields: BTreeMap<String, FieldValue> = p
        .fields
        .into_iter()
        .map(|f| {
            let value = f.value.ok_or_else(|| {
                ServerError::BadRequest(format!("field '{}' has no value", f.key))
            })?;
            let inner = value.value.ok_or_else(|| {
                ServerError::BadRequest(format!("field '{}' has no inner value", f.key))
            })?;
            let fv = proto_field_to_field_value(inner)?;
            Ok((f.key, fv))
        })
        .collect::<Result<_, ServerError>>()?;

    let now = crate::util::now_nanos()?;
    let timestamp = if p.timestamp == 0 { now } else { p.timestamp };

    let key =
        SeriesKey::new(&p.measurement, tags).map_err(|e| ServerError::BadRequest(e.to_string()))?;

    Point::new(key, fields, timestamp).map_err(|e| ServerError::BadRequest(e.to_string()))
}

/// Convert a protobuf field value to a Chronix [`FieldValue`].
fn proto_field_to_field_value(v: proto::field_value::Value) -> Result<FieldValue, ServerError> {
    match v {
        proto::field_value::Value::Float64(f) => Ok(FieldValue::F64(f)),
        proto::field_value::Value::Int64(i) => Ok(FieldValue::I64(i)),
        proto::field_value::Value::Uint64(u) => Ok(FieldValue::U64(u)),
        proto::field_value::Value::Boolean(b) => Ok(FieldValue::Bool(b)),
        proto::field_value::Value::String(s) => Ok(FieldValue::String(s)),
    }
}

/// Convert a [`MeasurementSchema`] to protobuf column definitions.
fn schema_to_proto_columns(schema: &MeasurementSchema) -> Vec<proto::ColumnSchema> {
    let mut columns = Vec::new();

    columns.push(proto::ColumnSchema {
        name: chronix_core::TIME_COLUMN.to_string(),
        role: "timestamp".to_string(),
        data_type: "int64".to_string(),
    });

    for tag in schema.tag_names() {
        columns.push(proto::ColumnSchema {
            name: tag.to_string(),
            role: "tag".to_string(),
            data_type: "string".to_string(),
        });
    }

    for col in schema.columns() {
        if col.role == ColumnRole::Field {
            columns.push(proto::ColumnSchema {
                name: col.name.clone(),
                role: "field".to_string(),
                data_type: crate::util::column_type_to_str(col.column_type).to_string(),
            });
        }
    }

    columns
}

/// Convert an Arrow `RecordBatch` to protobuf query rows.
fn record_batch_to_proto_rows(batch: &arrow::record_batch::RecordBatch) -> Vec<proto::QueryRow> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    let schema = batch.schema();
    let mut rows = Vec::with_capacity(batch.num_rows());

    for row_idx in 0..batch.num_rows() {
        let mut timestamp = 0i64;
        let mut tags = Vec::new();
        let mut fields = Vec::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let name = field.name().clone();

            if name == chronix_core::TIME_COLUMN {
                if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                    timestamp = arr.value(row_idx);
                }
                continue;
            }

            match field.data_type() {
                DataType::Utf8 => {
                    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        if !arr.is_null(row_idx) {
                            let val = arr.value(row_idx).to_string();
                            // The scan stamps the role: a tag and a string
                            // field are the same Arrow type, so without it
                            // every tag was reported as a field.
                            if field
                                .metadata()
                                .get(chronix::db::ROLE_KEY)
                                .map(std::string::String::as_str)
                                == Some("tag")
                            {
                                tags.push(proto::Tag {
                                    key: name,
                                    value: val,
                                });
                            } else {
                                fields.push(proto::Field {
                                    key: name,
                                    value: Some(proto::FieldValue {
                                        value: Some(proto::field_value::Value::String(val)),
                                    }),
                                });
                            }
                        }
                    }
                }
                DataType::Float64 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                        if !arr.is_null(row_idx) {
                            fields.push(proto::Field {
                                key: name,
                                value: Some(proto::FieldValue {
                                    value: Some(proto::field_value::Value::Float64(
                                        arr.value(row_idx),
                                    )),
                                }),
                            });
                        }
                    }
                }
                DataType::Int64 => {
                    if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                        if !arr.is_null(row_idx) {
                            fields.push(proto::Field {
                                key: name,
                                value: Some(proto::FieldValue {
                                    value: Some(proto::field_value::Value::Int64(
                                        arr.value(row_idx),
                                    )),
                                }),
                            });
                        }
                    }
                }
                DataType::UInt64 => {
                    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                        if !arr.is_null(row_idx) {
                            fields.push(proto::Field {
                                key: name,
                                value: Some(proto::FieldValue {
                                    value: Some(proto::field_value::Value::Uint64(
                                        arr.value(row_idx),
                                    )),
                                }),
                            });
                        }
                    }
                }
                DataType::Boolean => {
                    if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
                        if !arr.is_null(row_idx) {
                            fields.push(proto::Field {
                                key: name,
                                value: Some(proto::FieldValue {
                                    value: Some(proto::field_value::Value::Boolean(
                                        arr.value(row_idx),
                                    )),
                                }),
                            });
                        }
                    }
                }
                _ => {
                    tracing::debug!(
                        column = name,
                        dtype = ?field.data_type(),
                        "skipping unsupported column type in gRPC response"
                    );
                }
            }
        }

        rows.push(proto::QueryRow {
            timestamp,
            tags,
            fields,
        });
    }

    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn proto_field_to_field_value_all_types() {
        assert!(matches!(
            proto_field_to_field_value(proto::field_value::Value::Float64(1.5)),
            Ok(FieldValue::F64(f)) if (f - 1.5).abs() < f64::EPSILON
        ));
        assert!(matches!(
            proto_field_to_field_value(proto::field_value::Value::Int64(42)),
            Ok(FieldValue::I64(42))
        ));
        assert!(matches!(
            proto_field_to_field_value(proto::field_value::Value::Uint64(100)),
            Ok(FieldValue::U64(100))
        ));
        assert!(matches!(
            proto_field_to_field_value(proto::field_value::Value::Boolean(true)),
            Ok(FieldValue::Bool(true))
        ));
        assert!(matches!(
            proto_field_to_field_value(proto::field_value::Value::String("hello".into())),
            Ok(FieldValue::String(s)) if s == "hello"
        ));
    }

    #[test]
    fn proto_point_to_point_basic() {
        let p = proto::Point {
            measurement: "cpu".into(),
            tags: vec![proto::Tag {
                key: "host".into(),
                value: "srv1".into(),
            }],
            fields: vec![proto::Field {
                key: "usage".into(),
                value: Some(proto::FieldValue {
                    value: Some(proto::field_value::Value::Float64(72.5)),
                }),
            }],
            timestamp: 1000,
        };
        let point = proto_point_to_point(p).unwrap();
        assert_eq!(point.series_key().measurement(), "cpu");
        assert_eq!(point.timestamp(), 1000);
        assert_eq!(point.tag("host"), Some("srv1"));
    }

    #[test]
    fn proto_point_missing_field_value_error() {
        let p = proto::Point {
            measurement: "cpu".into(),
            tags: vec![],
            fields: vec![proto::Field {
                key: "usage".into(),
                value: None,
            }],
            timestamp: 1000,
        };
        let err = proto_point_to_point(p).unwrap_err();
        assert!(err.to_string().contains("usage"));
    }

    #[test]
    fn proto_point_auto_timestamp() {
        let p = proto::Point {
            measurement: "cpu".into(),
            tags: vec![],
            fields: vec![proto::Field {
                key: "v".into(),
                value: Some(proto::FieldValue {
                    value: Some(proto::field_value::Value::Float64(1.0)),
                }),
            }],
            timestamp: 0, // should auto-assign current time
        };
        let point = proto_point_to_point(p).unwrap();
        assert!(point.timestamp() > 0);
    }

    #[test]
    fn schema_to_proto_columns_complete() {
        let mut schema = MeasurementSchema::new("test");
        let _ = schema.add_tag("host");
        schema.add_field("usage", &FieldValue::F64(0.0)).unwrap();

        let cols = schema_to_proto_columns(&schema);
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, chronix_core::TIME_COLUMN);
        assert_eq!(cols[0].role, "timestamp");
        assert_eq!(cols[1].name, "host");
        assert_eq!(cols[1].role, "tag");
        assert_eq!(cols[2].name, "usage");
        assert_eq!(cols[2].role, "field");
    }

    #[test]
    fn record_batch_to_proto_rows_basic() {
        let mut tag_metadata = std::collections::HashMap::new();
        tag_metadata.insert("role".to_string(), "tag".to_string());

        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, true).with_metadata(tag_metadata),
            Field::new("usage", DataType::Float64, true),
        ]));

        let ts = Int64Array::from(vec![1000]);
        let hosts = StringArray::from(vec!["srv1"]);
        let vals = Float64Array::from(vec![72.5]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(ts), Arc::new(hosts), Arc::new(vals)])
                .unwrap();

        let rows = record_batch_to_proto_rows(&batch);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp, 1000);
        assert_eq!(rows[0].tags.len(), 1);
        assert_eq!(rows[0].tags[0].key, "host");
        assert_eq!(rows[0].tags[0].value, "srv1");
        assert_eq!(rows[0].fields.len(), 1);
        assert_eq!(rows[0].fields[0].key, "usage");
    }

    #[test]
    fn record_batch_to_proto_rows_all_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("f", DataType::Float64, true),
            Field::new("i", DataType::Int64, true),
            Field::new("u", DataType::UInt64, true),
            Field::new("b", DataType::Boolean, true),
        ]));

        let ts = Int64Array::from(vec![1000]);
        let f = Float64Array::from(vec![1.5]);
        let i = Int64Array::from(vec![42]);
        let u = UInt64Array::from(vec![100u64]);
        let b = BooleanArray::from(vec![true]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ts),
                Arc::new(f),
                Arc::new(i),
                Arc::new(u),
                Arc::new(b),
            ],
        )
        .unwrap();

        let rows = record_batch_to_proto_rows(&batch);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].fields.len(), 4);
    }
}
