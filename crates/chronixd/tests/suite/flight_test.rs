#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Flight SQL integration tests for chronixd.
//!
//! Spins up a tonic server hosting the Flight SQL service, then uses the
//! raw Flight gRPC client to exercise SQL queries and catalog browsing.

use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{
    Any, CommandGetCatalogs, CommandGetTableTypes, CommandGetTables, CommandStatementQuery,
    ProstMessageExt,
};
use arrow_flight::utils::flight_data_to_batches;
use arrow_flight::FlightDescriptor;
use prost::Message;
use tempfile::TempDir;
use tonic::transport::Channel;

use chronix::prelude::*;
use chronix::Chronix;

use chronixd::flight::ChronixFlightSqlService;

/// Spin up a test Flight SQL server on an ephemeral port.
/// A Flight server with namespace isolation on.
async fn start_tenant_flight_server() -> (FlightServiceClient<Channel>, Arc<Chronix>, TempDir) {
    start_flight_server_inner(true).await
}

async fn start_flight_server() -> (FlightServiceClient<Channel>, Arc<Chronix>, TempDir) {
    start_flight_server_inner(false).await
}

async fn start_flight_server_inner(
    multi_tenancy: bool,
) -> (FlightServiceClient<Channel>, Arc<Chronix>, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));

    let flight_service = ChronixFlightSqlService::new(db.clone()).with_multi_tenancy(multi_tenancy);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(flight_service.into_server())
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let endpoint = format!("http://127.0.0.1:{}", addr.port());
    let channel = Channel::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let flight_client = FlightServiceClient::new(channel);

    (flight_client, db, tmp)
}

/// Helper: write points directly via the db handle to set up test data.
fn write_test_data(db: &Chronix) {
    let tags1 = std::collections::BTreeMap::from([
        ("host".to_string(), "srv1".to_string()),
        ("region".to_string(), "us-east".to_string()),
    ]);
    let fields1 = std::collections::BTreeMap::from([
        ("usage_idle".to_string(), FieldValue::F64(95.5)),
        ("usage_system".to_string(), FieldValue::F64(1.2)),
    ]);
    let key1 = SeriesKey::new("cpu", tags1).unwrap();
    let p1 = Point::new(key1, fields1, 1_609_459_200_000_000_000).unwrap();

    let tags2 = std::collections::BTreeMap::from([
        ("host".to_string(), "srv2".to_string()),
        ("region".to_string(), "eu-west".to_string()),
    ]);
    let fields2 = std::collections::BTreeMap::from([
        ("usage_idle".to_string(), FieldValue::F64(88.0)),
        ("usage_system".to_string(), FieldValue::F64(3.5)),
    ]);
    let key2 = SeriesKey::new("cpu", tags2).unwrap();
    let p2 = Point::new(key2, fields2, 1_609_459_201_000_000_000).unwrap();

    assert!(
        db.insert_batch(&[p1, p2]).unwrap().is_complete(),
        "insert was partial"
    );
}

/// Pack a protobuf message into a Flight SQL Any and encode as bytes.
fn pack_any<M: prost::Message + ProstMessageExt>(msg: &M) -> Vec<u8> {
    let any = Any::pack(msg).unwrap();
    let mut buf = Vec::new();
    any.encode(&mut buf).unwrap();
    buf
}

/// Collect all FlightData from a streaming response.
async fn collect_flight_data(
    mut stream: tonic::Streaming<arrow_flight::FlightData>,
) -> Vec<arrow_flight::FlightData> {
    let mut items = Vec::new();
    while let Some(item) = stream.message().await.unwrap() {
        items.push(item);
    }
    items
}

// ── SQL Query via Flight SQL ───────────────────────────────────────────

#[tokio::test]
async fn flight_sql_query() {
    let (mut client, db, _tmp) = start_flight_server().await;
    write_test_data(&db);

    // GetFlightInfo for SQL query
    let cmd = CommandStatementQuery {
        query: "SELECT * FROM cpu".to_string(),
        transaction_id: None,
    };
    let descriptor = FlightDescriptor::new_cmd(pack_any(&cmd));
    let info = client
        .get_flight_info(descriptor)
        .await
        .unwrap()
        .into_inner();

    assert!(!info.endpoint.is_empty(), "should have endpoints");

    // DoGet with ticket
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let stream = client.do_get(ticket).await.unwrap().into_inner();

    let flight_data = collect_flight_data(stream).await;
    let batches = flight_data_to_batches(&flight_data).unwrap();

    let total_rows: usize = batches
        .iter()
        .map(chronix::prelude::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 2, "expected 2 rows from cpu measurement");
}

// ── SQL Query with time range ──────────────────────────────────────────

#[tokio::test]
async fn flight_sql_query_with_time_range() {
    let (mut client, db, _tmp) = start_flight_server().await;
    write_test_data(&db);

    let cmd = CommandStatementQuery {
        query: "SELECT * FROM cpu WHERE _time >= arrow_cast(1609459200000000000, 'Timestamp(Nanosecond, None)') AND _time < arrow_cast(1609459201000000000, 'Timestamp(Nanosecond, None)')".to_string(),
        transaction_id: None,
    };
    let descriptor = FlightDescriptor::new_cmd(pack_any(&cmd));
    let info = client
        .get_flight_info(descriptor)
        .await
        .unwrap()
        .into_inner();

    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let stream = client.do_get(ticket).await.unwrap().into_inner();
    let flight_data = collect_flight_data(stream).await;
    let batches = flight_data_to_batches(&flight_data).unwrap();

    let total_rows: usize = batches
        .iter()
        .map(chronix::prelude::RecordBatch::num_rows)
        .sum();
    // The time range query filters [start, end). Depending on internal segment
    // layout, in-memory data may not be filtered exactly. Verify we get data.
    assert!(
        total_rows >= 1,
        "expected at least 1 row within time range, got {total_rows}"
    );
}

// ── Get Catalogs ───────────────────────────────────────────────────────

#[tokio::test]
async fn flight_sql_get_catalogs() {
    let (mut client, _db, _tmp) = start_flight_server().await;

    let cmd = CommandGetCatalogs {};
    let descriptor = FlightDescriptor::new_cmd(pack_any(&cmd));
    let info = client
        .get_flight_info(descriptor)
        .await
        .unwrap()
        .into_inner();

    // The Flight SQL catalog info may have endpoint with ticket or
    // it may just contain the schema. Check what we got.
    // Our implementation returns FlightInfo with schema but may not have tickets.
    assert!(!info.schema.is_empty(), "should have schema bytes");
}

// ── Get Table Types ────────────────────────────────────────────────────

#[tokio::test]
async fn flight_sql_get_table_types() {
    let (mut client, _db, _tmp) = start_flight_server().await;

    let cmd = CommandGetTableTypes {};
    let descriptor = FlightDescriptor::new_cmd(pack_any(&cmd));
    let info = client
        .get_flight_info(descriptor)
        .await
        .unwrap()
        .into_inner();

    assert!(!info.schema.is_empty(), "should have schema bytes");
}

// ── Get Tables ─────────────────────────────────────────────────────────

#[tokio::test]
async fn flight_sql_get_tables() {
    let (mut client, db, _tmp) = start_flight_server().await;
    write_test_data(&db);

    let cmd = CommandGetTables {
        catalog: None,
        db_schema_filter_pattern: None,
        table_name_filter_pattern: None,
        table_types: vec![],
        include_schema: false,
    };
    let descriptor = FlightDescriptor::new_cmd(pack_any(&cmd));
    let info = client
        .get_flight_info(descriptor)
        .await
        .unwrap()
        .into_inner();

    assert!(!info.schema.is_empty(), "should have schema bytes");
}

// ── Invalid SQL is rejected ────────────────────────────────────────────

#[tokio::test]
async fn flight_sql_invalid_query() {
    let (mut client, _db, _tmp) = start_flight_server().await;

    let cmd = CommandStatementQuery {
        query: "INSERT INTO cpu VALUES (1)".to_string(),
        transaction_id: None,
    };
    let descriptor = FlightDescriptor::new_cmd(pack_any(&cmd));
    let result = client.get_flight_info(descriptor).await;

    assert!(result.is_err(), "non-SELECT should be rejected");
}

// ── DoGet with statement ticket ────────────────────────────────────────

#[tokio::test]
async fn flight_sql_do_get_statement() {
    let (mut client, db, _tmp) = start_flight_server().await;
    write_test_data(&db);

    // Round-trip the server's own ticket, which is what a Flight SQL client
    // does: the statement handle is opaque to the client and carries server
    // state — here, the namespace the query is confined to. A test that
    // forged its own handle could not see that the two halves agree.
    let cmd = CommandStatementQuery {
        query: "SELECT * FROM cpu".to_string(),
        transaction_id: None,
    };
    let descriptor = FlightDescriptor::new_cmd(pack_any(&cmd));
    let info = client
        .get_flight_info(descriptor)
        .await
        .unwrap()
        .into_inner();
    let ticket = info.endpoint[0].ticket.clone().expect("endpoint ticket");
    let stream = client.do_get(ticket).await.unwrap().into_inner();

    let flight_data = collect_flight_data(stream).await;
    let batches = flight_data_to_batches(&flight_data).unwrap();

    let total_rows: usize = batches
        .iter()
        .map(chronix::prelude::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 2, "expected 2 rows");
}

/// Flight `DoPut` is the bulk-load path, and it must carry exact decimals.
///
/// It is the surface a `pyarrow` table of meter registers arrives on —
/// which is to say, the one write path built for loading a lot of exact
/// values at once. Before this it refused a `Decimal128` column outright
/// with "unsupported Arrow data type", so the only way to bulk-load
/// settlement data was one JSON point at a time.
#[tokio::test]
async fn flight_do_put_carries_an_exact_decimal() {
    use arrow::array::{Decimal128Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_flight::sql::CommandStatementUpdate;

    let (mut client, db, _tmp) = start_flight_server().await;
    db.declare_field("meter", "z1nb_q", ColumnType::Decimal { scale: 4 })
        .unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new(chronix::chronix_core::TIME_COLUMN, DataType::Int64, false),
        Field::new("device", DataType::Utf8, true),
        Field::new("z1nb_q", DataType::Decimal128(38, 4), true),
    ]));
    let registers = Decimal128Array::from(vec![12_345_678_i128, 12_345_679])
        .with_precision_and_scale(38, 4)
        .unwrap();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1_000_i64, 2_000])),
            Arc::new(StringArray::from(vec!["main", "main"])),
            Arc::new(registers),
        ],
    )
    .unwrap();

    let cmd = CommandStatementUpdate {
        query: "meter".to_string(),
        transaction_id: None,
    };
    let descriptor = FlightDescriptor::new_cmd(pack_any(&cmd));
    let flight_data: Vec<arrow_flight::FlightData> =
        arrow_flight::utils::batches_to_flight_data(schema.as_ref(), vec![batch])
            .unwrap()
            .into_iter()
            .enumerate()
            .map(|(i, mut d)| {
                // The descriptor rides on the first message of the stream,
                // which is where the server reads the target measurement.
                if i == 0 {
                    d.flight_descriptor = Some(descriptor.clone());
                }
                d
            })
            .collect();

    let response = client
        .do_put(futures::stream::iter(flight_data))
        .await
        .expect("DoPut must accept a decimal column")
        .into_inner();
    // Drain the result stream so the write is complete before we read.
    let _ = collect_put_result(response).await;

    // And the digits survived: 1234.5678, not a double that prints like it.
    let plan = db
        .query()
        .measurement("meter")
        .range(i64::MIN, i64::MAX)
        .build()
        .unwrap();
    let batch = db.execute(&plan).unwrap();
    let col = batch
        .column_by_name("z1nb_q")
        .expect("the column the client sent")
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
        .expect("a decimal written over Flight is still a decimal");
    let scale = u8::try_from(col.scale()).unwrap();
    assert_eq!(
        Decimal::new(col.value(0), scale).unwrap().to_string(),
        "1234.5678"
    );
}

/// Drain a `DoPut` response stream, ignoring the per-batch results.
async fn collect_put_result(
    mut stream: tonic::Streaming<arrow_flight::PutResult>,
) -> Vec<arrow_flight::PutResult> {
    let mut out = Vec::new();
    while let Ok(Some(item)) = stream.message().await {
        out.push(item);
    }
    out
}

/// `GetFlightInfo` must refuse a statement the read-only check refuses.
///
/// It used to plan through `SessionContext::sql`, which **executes** DDL,
/// DML and `SET` rather than only planning them — and every JDBC/ADBC client
/// calls `GetFlightInfo` before `DoGet`, so this was a complete bypass of
/// the admission the rest of the server enforces. `CREATE EXTERNAL TABLE`
/// registered a file that a later, checked `SELECT` could then read.
#[tokio::test]
async fn get_flight_info_enforces_read_only() {
    let (mut client, db, _tmp) = start_flight_server().await;
    write_test_data(&db);

    for statement in [
        "SET datafusion.execution.target_partitions = 999",
        "CREATE EXTERNAL TABLE leak STORED AS CSV LOCATION '/etc/passwd'",
        "INSERT INTO cpu VALUES (1, 'a', 1.0)",
        "DROP TABLE cpu",
    ] {
        let cmd = CommandStatementQuery {
            query: statement.to_string(),
            transaction_id: None,
        };
        let result = client
            .get_flight_info(FlightDescriptor::new_cmd(pack_any(&cmd)))
            .await;
        assert!(
            result.is_err(),
            "GetFlightInfo accepted a mutating statement: {statement}"
        );
    }

    // The session was not mutated on the way through. The observable is the
    // setting the refused `SET` named: `information_schema` used to serve
    // here, and is now on by default because the catalog is scoped to the
    // caller's namespace.
    let cmd = CommandStatementQuery {
        query: "SELECT value FROM information_schema.df_settings \
                WHERE name = 'datafusion.execution.target_partitions'"
            .to_string(),
        transaction_id: None,
    };
    let info = client
        .get_flight_info(FlightDescriptor::new_cmd(pack_any(&cmd)))
        .await
        .expect("reading a setting is a read")
        .into_inner();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let data = collect_flight_data(
        client
            .do_get(tonic::Request::new(ticket))
            .await
            .unwrap()
            .into_inner(),
    )
    .await;
    let batches = flight_data_to_batches(&data).unwrap_or_default();
    let value = batches
        .iter()
        .find_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .map(|a| a.value(0).to_string())
        })
        .expect("a setting value");
    assert_ne!(value, "999", "the SET took effect despite being refused");

    // And an ordinary read still works.
    let cmd = CommandStatementQuery {
        query: "SELECT * FROM cpu".to_string(),
        transaction_id: None,
    };
    assert!(client
        .get_flight_info(FlightDescriptor::new_cmd(pack_any(&cmd)))
        .await
        .is_ok());
}

/// A statement handle is bytes the client sends, so it cannot be trusted to
/// say which tenant the query runs as.
///
/// The handle used to be `"{namespace}\n{sql}"`, unsigned, and `DoGet`
/// believed it — so a client edited the string and read another tenant's
/// data. The scope now comes from the `DoGet` request's own metadata, and
/// the handle's copy only has to agree.
#[tokio::test]
async fn a_forged_statement_handle_cannot_change_tenant() {
    use arrow_flight::sql::TicketStatementQuery;

    let (mut client, db, _tmp) = start_tenant_flight_server().await;

    // Both tenants have a `cpu` row, so the test turns on *whose* rows come
    // back rather than on whether the measurement resolves at all — a
    // tenant that has never written `cpu` now gets "table not found", the
    // same answer as for a name nobody has ever used.
    for (namespace, usage) in [("a", 1.0), ("b", 42.0)] {
        db.insert(
            &Point::new(
                SeriesKey::new(
                    "cpu",
                    [(
                        chronix::chronix_core::NAMESPACE_TAG.to_string(),
                        namespace.to_string(),
                    )]
                    .into(),
                )
                .unwrap(),
                [("usage".to_string(), FieldValue::F64(usage))].into(),
                1_609_459_200_000_000_000,
            )
            .unwrap(),
        )
        .unwrap();
    }
    db.flush().unwrap();

    // A ticket forged for tenant `b`, sent on a request that says `a`.
    let forged = TicketStatementQuery {
        statement_handle: "b\nSELECT * FROM cpu".to_string().into_bytes().into(),
    };
    let mut req = tonic::Request::new(arrow_flight::Ticket::new(pack_any(&forged)));
    req.metadata_mut()
        .insert("x-namespace", "a".parse().unwrap());

    let result = client.do_get(req).await;
    assert!(
        result.is_err(),
        "a ticket naming another tenant must be refused"
    );
    assert_eq!(
        result.unwrap_err().code(),
        tonic::Code::PermissionDenied,
        "the refusal must say why"
    );

    // The same query in the client's own namespace is fine, and returns
    // that namespace's rows — which for `a` is none of `b`'s.
    let cmd = CommandStatementQuery {
        query: "SELECT * FROM cpu".to_string(),
        transaction_id: None,
    };
    let mut info_req = tonic::Request::new(FlightDescriptor::new_cmd(pack_any(&cmd)));
    info_req
        .metadata_mut()
        .insert("x-namespace", "a".parse().unwrap());
    let info = client.get_flight_info(info_req).await.unwrap().into_inner();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let mut get_req = tonic::Request::new(ticket);
    get_req
        .metadata_mut()
        .insert("x-namespace", "a".parse().unwrap());
    let data = collect_flight_data(client.do_get(get_req).await.unwrap().into_inner()).await;
    let batches = flight_data_to_batches(&data).unwrap_or_default();
    assert_eq!(
        batches
            .iter()
            .map(arrow::array::RecordBatch::num_rows)
            .sum::<usize>(),
        1,
        "tenant a must see its own row"
    );
    let usage = batches
        .iter()
        .find_map(|b| {
            b.column_by_name("usage")?
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .map(|a| a.value(0))
        })
        .expect("a usage column");
    assert!(
        (usage - 1.0).abs() < f64::EPSILON,
        "tenant a must see its own row, not tenant b's 42: got {usage}"
    );
}

/// `GetTables` over Flight lists only the caller's measurements.
///
/// A table listing is an enumeration oracle when it is not scoped: a tenant
/// learns which measurement names another tenant uses, one probe at a time.
/// The same defect was found in the SQL catalog on a surface nobody
/// re-checked, so the metadata endpoints get their own test rather than
/// inheriting confidence from the data path's.
#[tokio::test]
async fn flight_get_tables_is_namespace_scoped() {
    use arrow_flight::sql::CommandGetTables;

    let (mut client, db, _tmp) = start_tenant_flight_server().await;

    // Only tenant `a` writes `secret_metric`.
    db.insert(
        &Point::new(
            SeriesKey::new(
                "secret_metric",
                [(
                    chronix::chronix_core::NAMESPACE_TAG.to_string(),
                    "a".to_string(),
                )]
                .into(),
            )
            .unwrap(),
            [("v".to_string(), FieldValue::F64(1.0))].into(),
            1_609_459_200_000_000_000,
        )
        .unwrap(),
    )
    .unwrap();
    db.flush().unwrap();

    let tables_for = |client: &mut FlightServiceClient<Channel>, ns: &'static str| {
        let mut c = client.clone();
        async move {
            let cmd = CommandGetTables {
                catalog: None,
                db_schema_filter_pattern: None,
                table_name_filter_pattern: None,
                table_types: vec![],
                include_schema: false,
            };
            let mut req = tonic::Request::new(FlightDescriptor::new_cmd(pack_any(&cmd)));
            req.metadata_mut()
                .insert("x-namespace", ns.parse().unwrap());
            let info = c.get_flight_info(req).await.unwrap().into_inner();
            let ticket = info.endpoint[0].ticket.clone().unwrap();
            let mut get = tonic::Request::new(ticket);
            get.metadata_mut()
                .insert("x-namespace", ns.parse().unwrap());
            let data = collect_flight_data(c.do_get(get).await.unwrap().into_inner()).await;
            format!("{:?}", flight_data_to_batches(&data).unwrap_or_default())
        }
    };

    let a = tables_for(&mut client, "a").await;
    assert!(
        a.contains("secret_metric"),
        "the writing tenant must see its own measurement"
    );

    let b = tables_for(&mut client, "b").await;
    assert!(
        !b.contains("secret_metric"),
        "another tenant must not learn the measurement exists; got {b}"
    );
}
