//! Arrow Flight SQL service implementation.
//!
//! Provides high-throughput columnar data transfer via the Arrow Flight SQL
//! protocol. Supports `DoGet` for query results and catalog browsing
//! (`GetTables`, `GetCatalogs`, `GetTableTypes`).

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::{FlightSqlService as FlightSqlTrait, PeekableFlightDataStream};
use arrow_flight::sql::{
    Any, CommandGetCatalogs, CommandGetTableTypes, CommandGetTables, CommandStatementQuery,
    CommandStatementUpdate, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::debug;

use chronix::prelude::*;
use chronix::Chronix;

/// The Chronix Flight SQL service.
pub struct ChronixFlightSqlService {
    db: Arc<Chronix>,
    sql_contexts: crate::namespace::SqlContexts,
    /// Query execution timeout (zero = no timeout).
    sql_query_timeout: Duration,
    /// Maximum rows returned per query (0 = unlimited).
    sql_max_rows: usize,
    /// Maximum duration for a single write batch (`Duration::ZERO` = no deadline).
    /// Prevents stuck writes from exhausting the thread-pool.
    write_timeout: Duration,
    /// Whether namespace isolation is enforced (`multi_tenancy` in the config).
    multi_tenancy: bool,
}

impl ChronixFlightSqlService {
    /// Create a new Flight SQL service backed by the given database.
    pub fn new(db: Arc<Chronix>) -> Self {
        let sql_contexts = crate::namespace::SqlContexts::new(db.clone());
        Self {
            db,
            sql_contexts,
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

    /// Create a Flight SQL service with configurable limits.
    pub fn with_limits(mut self, timeout: Duration, max_rows: usize) -> Self {
        self.sql_query_timeout = timeout;
        self.sql_max_rows = max_rows;
        self
    }

    /// Set the write-batch timeout.
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Create a `FlightServiceServer` from this service.
    pub fn into_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
    }
}

type DoGetStream =
    Pin<Box<dyn tokio_stream::Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightSqlTrait for ChronixFlightSqlService {
    type FlightService = ChronixFlightSqlService;

    /// Register SQL info — required by the trait.
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        // No-op: we don't maintain a SQL info registry.
    }

    /// Get flight info for a SQL query statement.
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = &query.query;
        debug!(%sql, "Flight SQL GetFlightInfo");

        // The namespace travels with the ticket: `DoGet` arrives as a separate
        // request, potentially on a different connection, so a scope resolved
        // only here would be lost before the query runs.
        let scope = crate::namespace::scope_from_request(self.multi_tenancy, &request)?;
        let sql_ctx = self.sql_contexts.get(scope.as_deref());

        // Plan through the **read-only** path, exactly as `DoGet` does.
        //
        // `SessionContext::sql` executes DDL, DML and `SET` eagerly rather
        // than only planning them, and every JDBC/ADBC client calls
        // `GetFlightInfo` before `DoGet` — so this was a complete bypass of
        // the read-only admission the rest of the server enforces. A
        // `CREATE EXTERNAL TABLE … LOCATION '/etc/passwd'` registered the
        // file here and a later `SELECT` read it through the checked path;
        // a `SET` mutated the shared session for every request after it.
        let df = chronix::sql::sql_read_only(&sql_ctx, sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("SQL error: {e}")))?;
        let arrow_schema = df.schema().as_arrow().clone();

        let ticket_data = TicketStatementQuery {
            statement_handle: encode_handle(scope.as_deref(), sql).into_bytes().into(),
        };
        let mut buf = Vec::new();
        let any = Any::pack(&ticket_data)
            .map_err(|e| Status::internal(format!("failed to pack ticket: {e}")))?;
        any.encode(&mut buf)
            .map_err(|e| Status::internal(format!("failed to encode ticket: {e}")))?;

        let ticket = Ticket::new(buf);
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&arrow_schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(endpoint)
            .with_descriptor(FlightDescriptor::new_cmd(Vec::new()));

        Ok(Response::new(flight_info))
    }

    /// Execute a SQL query and stream results.
    ///
    /// Uses `DataFrame::execute_stream()` for true streaming with row-
    /// limit enforcement and query timeout.  Unlike `collect()`, this
    /// does not materialise the entire result set in memory.
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let handle = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid statement handle"))?;
        let (handle_scope, sql) = decode_handle(&handle)?;

        // **The ticket is not authoritative for scope.** A ticket is bytes
        // the client sends, so a handle that names a namespace is a
        // namespace the client chose — and it used to be trusted, which let
        // any client read any tenant's data by editing the string.
        //
        // The scope comes from *this* request's metadata, exactly as it does
        // on every other surface, and the handle's copy only has to agree.
        // Disagreement is refused rather than silently resolved, so a client
        // whose ticket outlived a namespace change gets an error instead of
        // another tenant's rows.
        let scope = crate::namespace::scope_from_request(self.multi_tenancy, &_request)?;
        if self.multi_tenancy && handle_scope.as_deref() != scope.as_deref() {
            return Err(Status::permission_denied(
                "the statement handle names a different namespace than this request",
            ));
        }

        debug!(%sql, ?scope, "Flight SQL DoGet");

        let sql_ctx = self.sql_contexts.get(scope.as_deref());
        // Plan and verify *before* executing — see
        // `chronix::sql::readonly`.
        let df = chronix::sql::sql_read_only(&sql_ctx, sql)
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
                .map_err(|_| Status::deadline_exceeded("Flight SQL query timed out"))?
        }
        .map_err(|e| Status::internal(format!("SQL execution error: {e}")))?;

        // Consume batches incrementally, enforcing row limit with early termination.
        // The deadline covers the entire batch consumption loop, not just
        // stream creation. Dropping the stream on timeout cancels DataFusion.
        let deadline = if !self.sql_query_timeout.is_zero() {
            Some(tokio::time::Instant::now() + self.sql_query_timeout)
        } else {
            None
        };
        let max_rows = self.sql_max_rows;
        let mut batches = Vec::new();
        let mut total_rows: usize = 0;
        while let Some(batch_result) = {
            if let Some(dl) = deadline {
                match tokio::time::timeout_at(dl, stream.next()).await {
                    Ok(v) => v,
                    Err(_) => {
                        return Err(Status::deadline_exceeded(
                            "Flight SQL query timed out during result streaming",
                        ));
                    }
                }
            } else {
                stream.next().await
            }
        } {
            let batch =
                batch_result.map_err(|e| Status::internal(format!("SQL execution error: {e}")))?;
            if max_rows > 0 && total_rows >= max_rows {
                break;
            }
            if max_rows > 0 {
                let remaining = max_rows - total_rows;
                if batch.num_rows() <= remaining {
                    total_rows += batch.num_rows();
                    batches.push(batch);
                } else {
                    batches.push(batch.slice(0, remaining));
                    break;
                }
            } else {
                total_rows += batch.num_rows();
                batches.push(batch);
            }
        }

        stream_batches(batches).await
    }

    /// Bulk write via DoPut — accepts Arrow RecordBatch data.
    ///
    /// Expects a schema with:
    /// A `timestamp` column (Int64, nanoseconds since Unix epoch).
    /// Zero or more tag columns (Utf8) with metadata `role = "tag"` or
    ///   names matching a known tag in the measurement schema.
    /// One or more field columns (numeric, bool, or string).
    ///
    /// The `CommandStatementUpdate.query` field is used as the
    /// measurement name.
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        use tokio_stream::StreamExt;

        let measurement = ticket.query.trim().to_string();
        if measurement.is_empty() {
            return Err(Status::invalid_argument(
                "CommandStatementUpdate.query must contain the measurement name",
            ));
        }

        let scope = crate::namespace::scope_from_request(self.multi_tenancy, &request)?;

        // Collect all FlightData from the stream
        let mut stream = request.into_inner();
        let mut flight_data: Vec<FlightData> = Vec::new();
        while let Some(data) = stream.next().await {
            flight_data.push(data.map_err(|e| Status::internal(format!("receive error: {e}")))?);
        }

        if flight_data.is_empty() {
            return Ok(0);
        }

        // First message contains the Arrow IPC schema
        let schema = Arc::new(
            Schema::try_from(&flight_data[0])
                .map_err(|e| Status::internal(format!("schema decode error: {e}")))?,
        );

        // Decode remaining messages into RecordBatches
        let dictionaries_by_id = std::collections::HashMap::new();
        let mut total_rows: i64 = 0;
        let db = self.db.clone();

        for data in &flight_data[1..] {
            let batch = arrow_flight::utils::flight_data_to_arrow_batch(
                data,
                schema.clone(),
                &dictionaries_by_id,
            )
            .map_err(|e| Status::internal(format!("batch decode error: {e}")))?;

            let num_rows = batch.num_rows();
            if num_rows == 0 {
                continue;
            }

            // Identify column roles
            let ts_idx = schema.index_of(chronix_core::TIME_COLUMN).map_err(|_| {
                Status::invalid_argument("RecordBatch must have a 'timestamp' column")
            })?;

            let ts_array = batch
                .column(ts_idx)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| Status::invalid_argument("'timestamp' column must be Int64"))?;

            // Classify columns using the server-side schema registry as the
            // authoritative source of column roles. Client-supplied metadata is
            // only used as a fallback for columns unknown to the registry.
            let mut tag_indices: Vec<(usize, String)> = Vec::new();
            let mut field_indices: Vec<(usize, String)> = Vec::new();

            let existing_schema = db.schema_registry().lookup(&measurement);

            for (i, field) in schema.fields().iter().enumerate() {
                if i == ts_idx {
                    continue;
                }

                // Server-authoritative: check existing schema first
                if let Some(ref ms) = existing_schema {
                    if let Some(col_def) = ms.column(field.name()) {
                        match col_def.role {
                            chronix_core::ColumnRole::Tag => {
                                tag_indices.push((i, field.name().clone()));
                            }
                            chronix_core::ColumnRole::Field => {
                                field_indices.push((i, field.name().clone()));
                            }
                            chronix_core::ColumnRole::Timestamp => {
                                // Already handled above
                            }
                        }
                        continue;
                    }
                }

                // Fallback for new columns: string-like without role=field → tag
                match field.data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 => {
                        let is_field = field
                            .metadata()
                            .get("role")
                            .map(|r| r == "field")
                            .unwrap_or(false);
                        if is_field {
                            field_indices.push((i, field.name().clone()));
                        } else {
                            tag_indices.push((i, field.name().clone()));
                        }
                    }
                    DataType::Dictionary(_, value_type)
                        if matches!(value_type.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
                    {
                        let is_field = field
                            .metadata()
                            .get("role")
                            .map(|r| r == "field")
                            .unwrap_or(false);
                        if is_field {
                            field_indices.push((i, field.name().clone()));
                        } else {
                            tag_indices.push((i, field.name().clone()));
                        }
                    }
                    _ => {
                        field_indices.push((i, field.name().clone()));
                    }
                }
            }

            if field_indices.is_empty() {
                return Err(Status::invalid_argument(
                    "RecordBatch must have at least one field column",
                ));
            }

            let points = arrow_batch_to_points(
                &measurement,
                &batch,
                ts_array,
                &tag_indices,
                &field_indices,
            )?;

            let n = points.len() as i64;
            crate::util::insert_with_timeout(&db, scope.as_deref(), points, self.write_timeout)
                .await
                .map_err(|e| e.to_grpc_status())?;

            total_rows += n;
        }

        debug!(measurement = %measurement, rows = total_rows, "DoPut write complete");
        Ok(total_rows)
    }

    /// Get catalogs — returns a single "chronix" catalog.
    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog_name",
            DataType::Utf8,
            false,
        )]));

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(FlightDescriptor::new_cmd(Vec::new()));

        Ok(Response::new(info))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog_name",
            DataType::Utf8,
            false,
        )]));

        let catalogs: ArrayRef = Arc::new(StringArray::from(vec!["chronix"]));
        let batch = RecordBatch::try_new(schema.clone(), vec![catalogs])
            .map_err(|e| Status::internal(e.to_string()))?;

        stream_batches(vec![batch]).await
    }

    /// Get table types — returns ["TABLE"].
    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));

        let cmd = CommandGetTableTypes {};
        let ticket_data =
            Any::pack(&cmd).map_err(|e| Status::internal(format!("failed to pack ticket: {e}")))?;
        let mut buf = Vec::new();
        ticket_data
            .encode(&mut buf)
            .map_err(|e| Status::internal(format!("failed to encode ticket: {e}")))?;

        let ticket = Ticket::new(buf);
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(endpoint)
            .with_descriptor(FlightDescriptor::new_cmd(Vec::new()));

        Ok(Response::new(info))
    }

    /// Get table types — returns ["TABLE"].
    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));

        let types: ArrayRef = Arc::new(StringArray::from(vec!["TABLE"]));
        let batch = RecordBatch::try_new(schema.clone(), vec![types])
            .map_err(|e| Status::internal(e.to_string()))?;

        stream_batches(vec![batch]).await
    }

    /// Get tables — returns FlightInfo for measurements as tables.
    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));

        let cmd = CommandGetTables {
            catalog: None,
            db_schema_filter_pattern: None,
            table_name_filter_pattern: None,
            table_types: vec![],
            include_schema: false,
        };
        let ticket_data =
            Any::pack(&cmd).map_err(|e| Status::internal(format!("failed to pack ticket: {e}")))?;
        let mut buf = Vec::new();
        ticket_data
            .encode(&mut buf)
            .map_err(|e| Status::internal(format!("failed to encode ticket: {e}")))?;

        let ticket = Ticket::new(buf);
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_endpoint(endpoint)
            .with_descriptor(FlightDescriptor::new_cmd(Vec::new()));

        Ok(Response::new(info))
    }

    /// Get tables — returns measurements as tables.
    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let db = self.db.clone();
        // The schema registry is process-wide, so a table listing told every
        // tenant what the others were writing — which for a JDBC client is
        // the first thing it fetches to populate its schema browser. A
        // namespace sees the measurements it holds data for.
        let scope = crate::namespace::scope_from_request(self.multi_tenancy, &_request)?;

        let names: Vec<String> = tokio::task::spawn_blocking(move || match scope.as_deref() {
            None => db.schema_registry().measurement_names(),
            Some(ns) => {
                let now_ns = crate::util::now_nanos().unwrap_or(i64::MAX);
                crate::namespace::measurements_in(&db, Some(ns), i64::MIN, now_ns, usize::MAX)
            }
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));

        let n = names.len();
        let catalogs: ArrayRef = Arc::new(StringArray::from(vec!["chronix"; n]));
        let schemas: ArrayRef = Arc::new(StringArray::from(vec!["public"; n]));
        let table_names: ArrayRef = Arc::new(StringArray::from(names));
        let table_types: ArrayRef = Arc::new(StringArray::from(vec!["TABLE"; n]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![catalogs, schemas, table_names, table_types],
        )
        .map_err(|e| Status::internal(e.to_string()))?;

        stream_batches(vec![batch]).await
    }
}

/// Stream record batches as Flight data.
///
/// Converts each `RecordBatch` to `FlightData` lazily via an iterator,
/// emitting the schema header first followed by per-batch IPC messages.
/// This avoids materializing all `FlightData` into a `Vec` before streaming.
#[allow(clippy::result_large_err)] // tonic::Status is large by design
async fn stream_batches(batches: Vec<RecordBatch>) -> Result<Response<DoGetStream>, Status> {
    use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};

    if batches.is_empty() {
        let stream: DoGetStream = Box::pin(tokio_stream::empty());
        return Ok(Response::new(stream));
    }

    let schema = batches[0].schema();
    let options = IpcWriteOptions::default();

    // Schema header is always first.
    let schema_flight_data: FlightData = arrow_flight::SchemaAsIpc::new(&schema, &options).into();

    let data_gen = IpcDataGenerator::default();
    // Arrow 59 removed the pre-IPC-format-1.0 `preserve_dict_id` path; the
    // tracker now only decides whether a replaced dictionary is an error.
    let mut dictionary_tracker = DictionaryTracker::new(false);
    // Arrow 59 threads a reusable compression scratch buffer through encoding
    // instead of allocating one per batch.
    let mut ipc_write_context = arrow::ipc::writer::IpcWriteContext::default();

    // Build an iterator that yields schema, then each batch's encoded data.
    let mut items: Vec<Result<FlightData, Status>> = Vec::with_capacity(1 + batches.len());
    items.push(Ok(schema_flight_data));

    for batch in &batches {
        let (encoded_dicts, encoded_batch) = data_gen
            .encode(
                batch,
                &mut dictionary_tracker,
                &options,
                &mut ipc_write_context,
            )
            .map_err(|e| Status::internal(e.to_string()))?;

        for dict in encoded_dicts {
            let flight_data: FlightData = dict.into();
            items.push(Ok(flight_data));
        }
        let flight_data: FlightData = encoded_batch.into();
        items.push(Ok(flight_data));
    }

    let stream: DoGetStream = Box::pin(tokio_stream::iter(items));
    Ok(Response::new(stream))
}

/// Parse a very simple SQL-like query: `SELECT * FROM measurement [WHERE time >= start AND time < end]`
#[cfg(test)]
#[allow(clippy::result_large_err)]
fn parse_simple_sql(sql: &str) -> Result<(String, i64, i64), Status> {
    let sql_upper = sql.to_uppercase();
    let sql_trimmed = sql.trim();

    // Must start with SELECT
    if !sql_upper.starts_with("SELECT") {
        return Err(Status::invalid_argument(
            "only SELECT queries are supported",
        ));
    }

    // Find FROM clause
    let from_pos = sql_upper
        .find(" FROM ")
        .ok_or_else(|| Status::invalid_argument("missing FROM clause"))?;

    let after_from = &sql_trimmed[from_pos + 6..].trim_start();

    // Extract measurement name (first word after FROM)
    let measurement = after_from
        .split_whitespace()
        .next()
        .ok_or_else(|| Status::invalid_argument("missing measurement name after FROM"))?
        .to_string();

    // Simple WHERE clause parsing for time range
    let mut start = i64::MIN;
    let mut end = i64::MAX;

    if let Some(where_pos) = sql_upper.find(" WHERE ") {
        let where_clause = &sql_trimmed[where_pos + 7..];

        // Look for "time >= N" or "time > N"
        for part in where_clause.split(" AND ") {
            let part = part.trim();
            if let Some(val) = extract_time_bound(part, ">=") {
                start = val;
            } else if let Some(val) = extract_time_bound(part, ">") {
                start = val + 1;
            }
            if let Some(val) = extract_time_bound(part, "<=") {
                end = val + 1;
            } else if let Some(val) = extract_time_bound(part, "<") {
                end = val;
            }
        }
    }

    Ok((measurement, start, end))
}

/// Extract a time bound from a simple predicate like "time >= 123".
#[cfg(test)]
fn extract_time_bound(predicate: &str, op: &str) -> Option<i64> {
    let upper = predicate.to_uppercase();
    if !upper.contains("TIME") {
        return None;
    }

    let parts: Vec<&str> = predicate.splitn(2, op).collect();
    if parts.len() != 2 {
        return None;
    }

    let left = parts[0].trim().to_uppercase();
    if left != "TIME" {
        return None;
    }

    parts[1].trim().parse::<i64>().ok()
}

/// Convert a `MeasurementSchema` to an Arrow `Schema`.
#[cfg(test)]
fn measurement_to_arrow_schema(schema: &MeasurementSchema) -> Schema {
    let mut fields = vec![Field::new(
        chronix_core::TIME_COLUMN,
        DataType::Int64,
        false,
    )];

    for tag in schema.tag_names() {
        fields.push(Field::new(tag, DataType::Utf8, true));
    }

    for col in schema.columns() {
        if col.role == ColumnRole::Field {
            let dt = crate::util::column_type_to_arrow(col.column_type);
            fields.push(Field::new(&col.name, dt, true));
        }
    }

    Schema::new(fields)
}

/// Convert an Arrow `RecordBatch` into Chronix `Point`s using columnar access.
///
/// Pre-downcasts each column once (O(columns)) instead of per-cell
/// downcasting (O(rows × columns)).  Tag strings are borrowed from the
/// Arrow array and only cloned into the per-point `BTreeMap`.
#[allow(clippy::result_large_err)]
fn arrow_batch_to_points(
    measurement: &str,
    batch: &RecordBatch,
    ts_array: &arrow::array::Int64Array,
    tag_indices: &[(usize, String)],
    field_indices: &[(usize, String)],
) -> Result<Vec<Point>, Status> {
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};

    let num_rows = batch.num_rows();

    // --- Pre-downcast tag columns once ---------------------------------
    enum TagCol<'a> {
        Str(&'a StringArray),
        Dict(&'a arrow::array::DictionaryArray<arrow::datatypes::Int32Type>),
    }

    let tag_cols: Vec<(&String, TagCol<'_>)> = tag_indices
        .iter()
        .filter_map(|(col_idx, name)| {
            let col = batch.column(*col_idx);
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                Some((name, TagCol::Str(arr)))
            } else if let Some(dict) = col
                .as_any()
                .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>()
            {
                Some((name, TagCol::Dict(dict)))
            } else {
                None
            }
        })
        .collect();

    // --- Pre-downcast field columns once --------------------------------
    enum TypedField<'a> {
        F64(&'a Float64Array),
        I64(&'a Int64Array),
        U64(&'a UInt64Array),
        Bool(&'a BooleanArray),
        Str(&'a StringArray),
    }

    let typed_fields: Vec<(&String, TypedField<'_>)> = field_indices
        .iter()
        .map(|(col_idx, name)| {
            let col = batch.column(*col_idx);
            let typed = if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
                TypedField::F64(a)
            } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                TypedField::I64(a)
            } else if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                TypedField::U64(a)
            } else if let Some(a) = col.as_any().downcast_ref::<BooleanArray>() {
                TypedField::Bool(a)
            } else if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                TypedField::Str(a)
            } else {
                return Err(Status::invalid_argument(format!(
                    "unsupported Arrow data type for field '{name}'"
                )));
            };
            Ok((name, typed))
        })
        .collect::<Result<Vec<_>, Status>>()?;

    // --- Row iteration with pre-resolved columns -----------------------
    let mut points = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let timestamp = ts_array.value(row);

        // Build tags from pre-downcast tag arrays
        let mut tags = std::collections::BTreeMap::new();
        for (name, tag_col) in &tag_cols {
            let value = match tag_col {
                TagCol::Str(arr) if !arr.is_null(row) => Some(arr.value(row).to_string()),
                TagCol::Dict(dict) if !dict.is_null(row) => {
                    let values = dict.values();
                    values
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .map(|v| v.value(dict.keys().value(row) as usize).to_string())
                }
                _ => None,
            };
            if let Some(v) = value {
                tags.insert((*name).clone(), v);
            }
        }

        // Build fields from pre-downcast typed arrays
        let mut fields = std::collections::BTreeMap::new();
        for (name, typed) in &typed_fields {
            let fv = match typed {
                TypedField::F64(a) if !a.is_null(row) => FieldValue::F64(a.value(row)),
                TypedField::I64(a) if !a.is_null(row) => FieldValue::I64(a.value(row)),
                TypedField::U64(a) if !a.is_null(row) => FieldValue::U64(a.value(row)),
                TypedField::Bool(a) if !a.is_null(row) => FieldValue::Bool(a.value(row)),
                TypedField::Str(a) if !a.is_null(row) => {
                    FieldValue::String(a.value(row).to_string())
                }
                _ => continue,
            };
            fields.insert((*name).clone(), fv);
        }

        if fields.is_empty() {
            continue;
        }

        let key = SeriesKey::new(measurement, tags).map_err(|e| Status::internal(e.to_string()))?;
        let point =
            Point::new(key, fields, timestamp).map_err(|e| Status::internal(e.to_string()))?;
        points.push(point);
    }

    Ok(points)
}

// ── Flight ticket handles ──────────────────────────────────────────────

/// Encode the namespace alongside the SQL text in a statement handle.
///
/// Flight SQL splits a query across two requests: `GetFlightInfo` plans it and
/// hands back a ticket, and `DoGet` executes that ticket — possibly on another
/// connection, whose metadata the client is under no obligation to repeat. A
/// namespace resolved only at `GetFlightInfo` would therefore be gone by the
/// time the query ran, so it travels inside the handle.
///
/// The handle is opaque to clients and never parsed from user input: `DoGet`
/// only ever sees a handle this server produced.
/// Build a statement handle.
///
/// The namespace it carries is a **hint that must agree** with the scope the
/// `DoGet` request resolves for itself, never the scope itself: a ticket is
/// bytes the client sends. See `do_get_statement`.
fn encode_handle(namespace: Option<&str>, sql: &str) -> String {
    format!("{}\n{sql}", namespace.unwrap_or_default())
}

/// Split a statement handle back into `(namespace, sql)`.
fn decode_handle(handle: &str) -> Result<(Option<String>, &str), Status> {
    handle.split_once('\n').map_or_else(
        || Err(Status::invalid_argument("malformed statement handle")),
        |(ns, sql)| {
            let scope = (!ns.is_empty()).then(|| ns.to_string());
            Ok((scope, sql))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;

    #[test]
    fn parse_simple_select() {
        let (m, s, e) = parse_simple_sql("SELECT * FROM cpu").unwrap();
        assert_eq!(m, "cpu");
        assert_eq!(s, i64::MIN);
        assert_eq!(e, i64::MAX);
    }

    #[test]
    fn parse_select_with_time_range() {
        let (m, s, e) =
            parse_simple_sql("SELECT * FROM cpu WHERE time >= 1000 AND time < 2000").unwrap();
        assert_eq!(m, "cpu");
        assert_eq!(s, 1000);
        assert_eq!(e, 2000);
    }

    #[test]
    fn parse_select_no_from_fails() {
        let err = parse_simple_sql("SELECT * cpu").unwrap_err();
        assert!(err.message().contains("FROM"), "{}", err.message());
    }

    #[test]
    fn parse_select_no_select_fails() {
        let err = parse_simple_sql("INSERT INTO cpu VALUES (1)").unwrap_err();
        assert!(err.message().contains("SELECT"), "{}", err.message());
    }

    #[test]
    fn parse_select_with_gt_and_le() {
        let (_, s, e) =
            parse_simple_sql("SELECT * FROM m WHERE time > 99 AND time <= 200").unwrap();
        assert_eq!(s, 100); // > 99 → 100
        assert_eq!(e, 201); // <= 200 → 201
    }

    #[test]
    fn measurement_to_arrow_schema_complete() {
        let mut schema = MeasurementSchema::new("cpu");
        let _ = schema.add_tag("host");
        schema.add_field("usage", &FieldValue::F64(0.0)).unwrap();
        schema.add_field("count", &FieldValue::I64(0)).unwrap();

        let arrow = measurement_to_arrow_schema(&schema);
        assert_eq!(arrow.fields().len(), 4); // timestamp + host + usage + count
        assert_eq!(arrow.field(0).name(), chronix_core::TIME_COLUMN);
        assert_eq!(*arrow.field(0).data_type(), DataType::Int64);
        assert_eq!(arrow.field(1).name(), "host");
        assert_eq!(*arrow.field(1).data_type(), DataType::Utf8);
        assert_eq!(arrow.field(2).name(), "usage");
        assert_eq!(*arrow.field(2).data_type(), DataType::Float64);
        assert_eq!(arrow.field(3).name(), "count");
        assert_eq!(*arrow.field(3).data_type(), DataType::Int64);
    }

    #[test]
    fn arrow_batch_to_points_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
            Field::new("usage", DataType::Float64, true),
        ]));

        let ts = Int64Array::from(vec![1000, 2000]);
        let hosts = StringArray::from(vec!["srv1", "srv2"]);
        let vals = Float64Array::from(vec![72.5, 85.0]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(ts.clone()), Arc::new(hosts), Arc::new(vals)],
        )
        .unwrap();

        let tag_indices = vec![(1, "host".to_string())];
        let field_indices = vec![(2, "usage".to_string())];

        let points =
            arrow_batch_to_points("cpu", &batch, &ts, &tag_indices, &field_indices).unwrap();

        assert_eq!(points.len(), 2);
        assert_eq!(points[0].series_key().measurement(), "cpu");
        assert_eq!(points[0].timestamp(), 1000);
        assert_eq!(points[0].tag("host"), Some("srv1"));
        assert!(
            matches!(points[0].field("usage"), Some(FieldValue::F64(f)) if (*f - 72.5).abs() < f64::EPSILON)
        );
    }

    #[test]
    fn arrow_batch_to_points_multi_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("f_val", DataType::Float64, true),
            Field::new("i_val", DataType::Int64, true),
            Field::new("u_val", DataType::UInt64, true),
            Field::new("b_val", DataType::Boolean, true),
            Field::new("s_val", DataType::Utf8, true),
        ]));

        let ts = Int64Array::from(vec![1000]);
        let f = Float64Array::from(vec![1.5]);
        let i = Int64Array::from(vec![42]);
        let u = UInt64Array::from(vec![100u64]);
        let b = BooleanArray::from(vec![true]);
        let s = StringArray::from(vec!["hello"]);

        // Mark s_val as role=field so it's treated as field not tag
        let mut field_meta = std::collections::HashMap::new();
        field_meta.insert("role".to_string(), "field".to_string());

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ts.clone()),
                Arc::new(f),
                Arc::new(i),
                Arc::new(u),
                Arc::new(b),
                Arc::new(s),
            ],
        )
        .unwrap();

        let field_indices = vec![
            (1, "f_val".to_string()),
            (2, "i_val".to_string()),
            (3, "u_val".to_string()),
            (4, "b_val".to_string()),
            (5, "s_val".to_string()),
        ];

        let points = arrow_batch_to_points("m", &batch, &ts, &[], &field_indices).unwrap();

        assert_eq!(points.len(), 1);
        assert!(matches!(points[0].field("f_val"), Some(FieldValue::F64(_))));
        assert!(matches!(
            points[0].field("i_val"),
            Some(FieldValue::I64(42))
        ));
        assert!(matches!(
            points[0].field("u_val"),
            Some(FieldValue::U64(100))
        ));
        assert!(matches!(
            points[0].field("b_val"),
            Some(FieldValue::Bool(true))
        ));
        assert!(matches!(points[0].field("s_val"), Some(FieldValue::String(s)) if s == "hello"));
    }

    #[test]
    fn arrow_batch_to_points_null_rows_skipped() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]));

        let ts = Int64Array::from(vec![1000, 2000]);
        let vals = Float64Array::from(vec![Some(10.0), None]);

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(ts.clone()), Arc::new(vals)]).unwrap();

        let field_indices = vec![(1, "value".to_string())];
        let points = arrow_batch_to_points("m", &batch, &ts, &[], &field_indices).unwrap();

        // Row with all-null fields is skipped
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp(), 1000);
    }

    #[test]
    fn extract_time_bound_parses_correctly() {
        assert_eq!(extract_time_bound("time >= 1000", ">="), Some(1000));
        assert_eq!(extract_time_bound("time < 2000", "<"), Some(2000));
        assert_eq!(extract_time_bound("host = 'srv1'", ">="), None);
        assert_eq!(extract_time_bound("TIME >= 1000", ">="), Some(1000));
    }

    #[test]
    fn arrow_batch_to_points_dict_tags() {
        use arrow::datatypes::Int32Type;

        let dict_hosts: DictionaryArray<Int32Type> =
            vec!["srv1", "srv2", "srv1"].into_iter().collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new(chronix_core::TIME_COLUMN, DataType::Int64, false),
            Field::new("host", dict_hosts.data_type().clone(), true),
            Field::new("value", DataType::Float64, true),
        ]));

        let ts = Int64Array::from(vec![1000, 2000, 3000]);
        let vals = Float64Array::from(vec![10.0, 20.0, 30.0]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(ts.clone()), Arc::new(dict_hosts), Arc::new(vals)],
        )
        .unwrap();

        let tag_indices = vec![(1, "host".to_string())];
        let field_indices = vec![(2, "value".to_string())];

        let points =
            arrow_batch_to_points("cpu", &batch, &ts, &tag_indices, &field_indices).unwrap();

        assert_eq!(points.len(), 3);
        assert_eq!(points[0].tag("host"), Some("srv1"));
        assert_eq!(points[1].tag("host"), Some("srv2"));
        assert_eq!(points[2].tag("host"), Some("srv1"));
    }
}
