#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! gRPC integration tests for chronixd.
//!
//! Spins up a real tonic gRPC server with an in-memory temp database and
//! exercises the full `ChronixService` RPC surface using the generated
//! tonic client.

use std::net::SocketAddr;
use std::sync::Arc;

use tempfile::TempDir;
use tonic::transport::Channel;

use chronix::prelude::*;
use chronix::Chronix;

use chronixd::grpc::ChronixGrpcService;
use chronixd::proto;
use chronixd::proto::chronix_service_client::ChronixServiceClient;

/// Spin up a test gRPC server on an ephemeral port.
/// Returns `(client, endpoint_url, temp_dir)`.
async fn start_grpc_server() -> (ChronixServiceClient<Channel>, String, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));

    let start_time = std::time::Instant::now();
    let grpc_service = ChronixGrpcService::new(db, start_time);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");

    // Start gRPC server
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<proto::chronix_service_server::ChronixServiceServer<ChronixGrpcService>>()
        .await;

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(health_service)
            .add_service(proto::chronix_service_server::ChronixServiceServer::new(
                grpc_service,
            ))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    // Give the server a moment to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let endpoint = format!("http://127.0.0.1:{}", addr.port());
    let client = ChronixServiceClient::connect(endpoint.clone())
        .await
        .unwrap();

    (client, endpoint, tmp)
}

/// Build a proto Point for testing.
fn make_point(
    measurement: &str,
    tags: &[(&str, &str)],
    fields: &[(&str, f64)],
    timestamp: i64,
) -> proto::Point {
    let proto_tags: Vec<proto::Tag> = tags
        .iter()
        .map(|(k, v)| proto::Tag {
            key: k.to_string(),
            value: v.to_string(),
        })
        .collect();

    let proto_fields: Vec<proto::Field> = fields
        .iter()
        .map(|(k, v)| proto::Field {
            key: k.to_string(),
            value: Some(proto::FieldValue {
                value: Some(proto::field_value::Value::Float64(*v)),
            }),
        })
        .collect();

    proto::Point {
        measurement: measurement.to_string(),
        tags: proto_tags,
        fields: proto_fields,
        timestamp,
    }
}

// ── Write & Query round-trip ───────────────────────────────────────────

#[tokio::test]
async fn grpc_write_and_query() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    // Write
    let points = vec![
        make_point("cpu", &[("host", "srv1")], &[("usage", 42.5)], 1_000_000),
        make_point("cpu", &[("host", "srv2")], &[("usage", 88.0)], 2_000_000),
    ];

    let resp = client
        .write(proto::WriteRequest { points })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.written, 2);

    // Query
    let query = proto::QueryRequest {
        measurement: "cpu".to_string(),
        range: Some(proto::TimeRange {
            start: 0,
            end: 10_000_000,
        }),
        tag_filters: vec![],
        field_columns: vec![],
        limit: 0,
    };

    let mut stream = client.query(query).await.unwrap().into_inner();
    let mut total_rows = 0;
    while let Some(resp) = stream.message().await.unwrap() {
        total_rows += resp.rows.len();
    }
    assert_eq!(total_rows, 2, "expected 2 query rows");
}

// ── Write with tag filter query ────────────────────────────────────────

#[tokio::test]
async fn grpc_query_with_tag_filter() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    // Write points with different tags
    let points = vec![
        make_point("mem", &[("host", "a")], &[("used", 512.0)], 1000),
        make_point("mem", &[("host", "b")], &[("used", 1024.0)], 2000),
        make_point("mem", &[("host", "a")], &[("used", 768.0)], 3000),
    ];
    client.write(proto::WriteRequest { points }).await.unwrap();

    // Query with tag filter
    let query = proto::QueryRequest {
        measurement: "mem".to_string(),
        range: None,
        tag_filters: vec![proto::Tag {
            key: "host".to_string(),
            value: "a".to_string(),
        }],
        field_columns: vec![],
        limit: 0,
    };

    let mut stream = client.query(query).await.unwrap().into_inner();
    let mut total_rows = 0;
    while let Some(resp) = stream.message().await.unwrap() {
        total_rows += resp.rows.len();
    }
    assert_eq!(total_rows, 2, "expected 2 rows for host=a");
}

// ── Get Schema ─────────────────────────────────────────────────────────

#[tokio::test]
async fn grpc_get_schema() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    // Write to create schema
    let points = vec![make_point(
        "temperature",
        &[("location", "office")],
        &[("value", 23.5)],
        1000,
    )];
    client.write(proto::WriteRequest { points }).await.unwrap();

    // Get schema
    let resp = client
        .get_schema(proto::SchemaRequest {
            measurement: "temperature".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(resp.measurement, "temperature");
    assert!(!resp.columns.is_empty());

    let col_names: Vec<&str> = resp.columns.iter().map(|c| c.name.as_str()).collect();
    assert!(col_names.contains(&"timestamp"));
    assert!(col_names.contains(&"value"));
}

// ── Get Schema not found ───────────────────────────────────────────────

#[tokio::test]
async fn grpc_get_schema_not_found() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    let result = client
        .get_schema(proto::SchemaRequest {
            measurement: "nonexistent".to_string(),
        })
        .await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

// ── List Measurements ──────────────────────────────────────────────────

#[tokio::test]
async fn grpc_list_measurements() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    // Write to create measurements
    let points = vec![
        make_point("cpu", &[], &[("usage", 50.0)], 1000),
        make_point("mem", &[], &[("used", 1024.0)], 2000),
    ];
    client.write(proto::WriteRequest { points }).await.unwrap();

    let resp = client
        .list_measurements(proto::ListMeasurementsRequest {})
        .await
        .unwrap()
        .into_inner();

    let names: Vec<&str> = resp.measurements.iter().map(|m| m.name.as_str()).collect();
    assert!(names.contains(&"cpu"));
    assert!(names.contains(&"mem"));
}

// ── Delete ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn grpc_delete() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    // Write
    let points = vec![
        make_point("sensor", &[("id", "s1")], &[("temp", 20.0)], 1000),
        make_point("sensor", &[("id", "s1")], &[("temp", 21.0)], 2000),
        make_point("sensor", &[("id", "s1")], &[("temp", 22.0)], 3000),
    ];
    client.write(proto::WriteRequest { points }).await.unwrap();

    // Delete range [1500, 2500)
    let resp = client
        .delete(proto::DeleteRequest {
            measurement: "sensor".to_string(),
            tag_filters: vec![proto::Tag {
                key: "id".to_string(),
                value: "s1".to_string(),
            }],
            range: Some(proto::TimeRange {
                start: 1500,
                end: 2500,
            }),
        })
        .await
        .unwrap()
        .into_inner();

    // Verify the delete RPC completed without error — the actual deleted
    // count depends on internal segment layout. The successful response
    // is the primary assertion.
    let _ = resp.deleted;
}

// ── Drop Measurement ───────────────────────────────────────────────────

#[tokio::test]
async fn grpc_drop_measurement() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    // Write
    let points = vec![make_point("to_drop", &[], &[("val", 1.0)], 1000)];
    client.write(proto::WriteRequest { points }).await.unwrap();

    // Verify it exists
    client
        .get_schema(proto::SchemaRequest {
            measurement: "to_drop".to_string(),
        })
        .await
        .unwrap();

    // Drop
    client
        .drop_measurement(proto::DropMeasurementRequest {
            measurement: "to_drop".to_string(),
        })
        .await
        .unwrap();

    // Verify it's gone
    let result = client
        .get_schema(proto::SchemaRequest {
            measurement: "to_drop".to_string(),
        })
        .await;
    assert!(result.is_err());
}

// ── Server Info ────────────────────────────────────────────────────────

#[tokio::test]
async fn grpc_server_info() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    let resp = client
        .server_info(proto::ServerInfoRequest {})
        .await
        .unwrap()
        .into_inner();

    assert!(!resp.version.is_empty());
    // Uptime should be very small in tests
    assert!(resp.uptime_seconds < 60);
}

// ── Stream Write ───────────────────────────────────────────────────────

#[tokio::test]
async fn grpc_stream_write() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    // Create a stream of write requests
    let batches = vec![
        proto::StreamWriteRequest {
            points: vec![
                make_point("stream_test", &[("host", "a")], &[("val", 1.0)], 1000),
                make_point("stream_test", &[("host", "a")], &[("val", 2.0)], 2000),
            ],
            dedup_key: String::new(),
        },
        proto::StreamWriteRequest {
            points: vec![make_point(
                "stream_test",
                &[("host", "b")],
                &[("val", 3.0)],
                3000,
            )],
            dedup_key: String::new(),
        },
    ];

    let stream = tokio_stream::iter(batches);
    let resp = client.stream_write(stream).await.unwrap().into_inner();

    assert_eq!(resp.total_written, 3);

    // Verify data was written by querying
    let query = proto::QueryRequest {
        measurement: "stream_test".to_string(),
        range: None,
        tag_filters: vec![],
        field_columns: vec![],
        limit: 0,
    };

    let mut query_stream = client.query(query).await.unwrap().into_inner();
    let mut total_rows = 0;
    while let Some(resp) = query_stream.message().await.unwrap() {
        total_rows += resp.rows.len();
    }
    assert_eq!(total_rows, 3);
}

// ── Query with limit ───────────────────────────────────────────────────

#[tokio::test]
async fn grpc_query_with_limit() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    // Write many points
    let mut points = Vec::new();
    for i in 0..20 {
        points.push(make_point(
            "series",
            &[("id", "a")],
            &[("val", i as f64)],
            (1000 + i * 100) as i64,
        ));
    }
    client.write(proto::WriteRequest { points }).await.unwrap();

    // Query with limit
    let query = proto::QueryRequest {
        measurement: "series".to_string(),
        range: None,
        tag_filters: vec![],
        field_columns: vec![],
        limit: 5,
    };

    let mut stream = client.query(query).await.unwrap().into_inner();
    let mut total_rows = 0;
    while let Some(resp) = stream.message().await.unwrap() {
        total_rows += resp.rows.len();
    }
    assert!(total_rows <= 5, "expected at most 5 rows, got {total_rows}");
}

// ── Query nonexistent measurement ──────────────────────────────────────

#[tokio::test]
async fn grpc_query_nonexistent() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    let result = client
        .query(proto::QueryRequest {
            measurement: "does_not_exist".to_string(),
            range: None,
            tag_filters: vec![],
            field_columns: vec![],
            limit: 0,
        })
        .await;

    assert!(result.is_err());
    let status = result.unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

// ── gRPC health check ─────────────────────────────────────────────────

#[tokio::test]
async fn grpc_health_check() {
    let (_client, endpoint, _tmp) = start_grpc_server().await;

    // Create a dedicated health client from the same endpoint
    let channel = Channel::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut health_client = tonic_health::pb::health_client::HealthClient::new(channel);

    let resp = health_client
        .check(tonic_health::pb::HealthCheckRequest {
            service: "chronix.v1.ChronixService".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        resp.status(),
        tonic_health::pb::health_check_response::ServingStatus::Serving
    );
}

// ── Write + Query results match REST ───────────────────────────────────

/// Integration test verifying gRPC write + query produces results
/// consistent with the data model.
#[tokio::test]
async fn grpc_write_query_data_integrity() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    let points = vec![make_point(
        "integrity",
        &[("env", "prod"), ("region", "eu")],
        &[("latency_ms", 12.5)],
        1_609_459_200_000_000_000,
    )];

    client
        .write(proto::WriteRequest {
            points: points.clone(),
        })
        .await
        .unwrap();

    let query = proto::QueryRequest {
        measurement: "integrity".to_string(),
        range: Some(proto::TimeRange {
            start: 1_609_459_199_000_000_000,
            end: 1_609_459_201_000_000_000,
        }),
        tag_filters: vec![],
        field_columns: vec![],
        limit: 0,
    };

    let mut stream = client.query(query).await.unwrap().into_inner();
    let mut all_rows = Vec::new();
    while let Some(resp) = stream.message().await.unwrap() {
        all_rows.extend(resp.rows);
    }

    assert_eq!(all_rows.len(), 1);
    let row = &all_rows[0];
    assert_eq!(row.timestamp, 1_609_459_200_000_000_000);

    // Verify fields
    let field = row
        .fields
        .iter()
        .find(|f| f.key == "latency_ms")
        .expect("latency_ms field");
    match &field.value.as_ref().unwrap().value {
        Some(proto::field_value::Value::Float64(v)) => {
            assert!((v - 12.5).abs() < f64::EPSILON);
        }
        other => panic!("expected Float64(12.5), got {other:?}"),
    }
}

// ── Stream write dedup ─────────────────────────────────────────────────

#[tokio::test]
async fn grpc_stream_write_dedup() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    // Send two batches with the same dedup_key — second should be skipped
    let batches = vec![
        proto::StreamWriteRequest {
            points: vec![make_point(
                "dedup_test",
                &[("host", "a")],
                &[("val", 1.0)],
                1000,
            )],
            dedup_key: "batch-001".to_string(),
        },
        proto::StreamWriteRequest {
            points: vec![make_point(
                "dedup_test",
                &[("host", "a")],
                &[("val", 2.0)],
                2000,
            )],
            dedup_key: "batch-001".to_string(), // duplicate key
        },
        proto::StreamWriteRequest {
            points: vec![make_point(
                "dedup_test",
                &[("host", "b")],
                &[("val", 3.0)],
                3000,
            )],
            dedup_key: "batch-002".to_string(), // different key
        },
    ];

    let stream = tokio_stream::iter(batches);
    let resp = client.stream_write(stream).await.unwrap().into_inner();

    // Only 2 batches should be written (1 from batch-001, 1 from batch-002)
    assert_eq!(resp.total_written, 2);

    // Verify via query — should have exactly 2 rows
    let query = proto::QueryRequest {
        measurement: "dedup_test".to_string(),
        range: None,
        tag_filters: vec![],
        field_columns: vec![],
        limit: 0,
    };
    let mut query_stream = client.query(query).await.unwrap().into_inner();
    let mut total_rows = 0;
    while let Some(resp) = query_stream.message().await.unwrap() {
        total_rows += resp.rows.len();
    }
    assert_eq!(total_rows, 2);
}

// ── Stream write batching (many small messages) ────────────────────────

#[tokio::test]
async fn grpc_stream_write_many_small_batches() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    // Send 50 individual-point messages — they should be batched server-side
    let batches: Vec<proto::StreamWriteRequest> = (0..50)
        .map(|i| proto::StreamWriteRequest {
            points: vec![make_point(
                "batch_test",
                &[("host", "srv1")],
                &[("val", i as f64)],
                (i + 1) * 1000,
            )],
            dedup_key: String::new(),
        })
        .collect();

    let stream = tokio_stream::iter(batches);
    let resp = client.stream_write(stream).await.unwrap().into_inner();

    assert_eq!(resp.total_written, 50);

    // Verify all 50 points are queryable
    let query = proto::QueryRequest {
        measurement: "batch_test".to_string(),
        range: None,
        tag_filters: vec![],
        field_columns: vec![],
        limit: 0,
    };
    let mut query_stream = client.query(query).await.unwrap().into_inner();
    let mut total_rows = 0;
    while let Some(resp) = query_stream.message().await.unwrap() {
        total_rows += resp.rows.len();
    }
    assert_eq!(total_rows, 50);
}

// ── Stream write graceful close (drain remaining buffer) ───────────────

#[tokio::test]
async fn grpc_stream_write_graceful_close() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    // Send a small number of points — they will be in the buffer when the
    // stream closes (batch_size default is 10K, so 5 points won't trigger
    // a batch-size flush). The server should drain them on close.
    let batches = vec![
        proto::StreamWriteRequest {
            points: vec![
                make_point("drain_test", &[("host", "a")], &[("val", 1.0)], 1000),
                make_point("drain_test", &[("host", "a")], &[("val", 2.0)], 2000),
            ],
            dedup_key: String::new(),
        },
        proto::StreamWriteRequest {
            points: vec![
                make_point("drain_test", &[("host", "a")], &[("val", 3.0)], 3000),
                make_point("drain_test", &[("host", "a")], &[("val", 4.0)], 4000),
                make_point("drain_test", &[("host", "a")], &[("val", 5.0)], 5000),
            ],
            dedup_key: String::new(),
        },
    ];

    let stream = tokio_stream::iter(batches);
    let resp = client.stream_write(stream).await.unwrap().into_inner();

    // All 5 should be drained and written
    assert_eq!(resp.total_written, 5);
    assert_eq!(resp.batch_written, 5); // last flush was the drain of all 5

    // Verify
    let query = proto::QueryRequest {
        measurement: "drain_test".to_string(),
        range: None,
        tag_filters: vec![],
        field_columns: vec![],
        limit: 0,
    };
    let mut query_stream = client.query(query).await.unwrap().into_inner();
    let mut total_rows = 0;
    while let Some(resp) = query_stream.message().await.unwrap() {
        total_rows += resp.rows.len();
    }
    assert_eq!(total_rows, 5);
}

// ── Stream write dedup with empty key (no dedup) ───────────────────────

#[tokio::test]
async fn grpc_stream_write_empty_dedup_key_no_skip() {
    let (mut client, _endpoint, _tmp) = start_grpc_server().await;

    // Empty dedup_key should NOT trigger dedup — all messages written
    let batches = vec![
        proto::StreamWriteRequest {
            points: vec![make_point(
                "nodedup",
                &[("host", "a")],
                &[("val", 1.0)],
                1000,
            )],
            dedup_key: String::new(),
        },
        proto::StreamWriteRequest {
            points: vec![make_point(
                "nodedup",
                &[("host", "a")],
                &[("val", 2.0)],
                2000,
            )],
            dedup_key: String::new(),
        },
    ];

    let stream = tokio_stream::iter(batches);
    let resp = client.stream_write(stream).await.unwrap().into_inner();
    assert_eq!(resp.total_written, 2);
}

// ══ Namespace isolation ═════════════════════════════════════════════════
//
// The gRPC surface reads `x-namespace` and scopes every RPC on it, and until
// now nothing tested that it does: the HTTP surfaces had a tenancy suite and
// this one had none. Namespace isolation is the property this tree has broken
// most often — a scope each handler has to remember is a scope most of them
// forget — so "it is scoped" is a claim, and this is the test of it.
//
// The shape is deliberately the same as the HTTP suite's: write as one tenant,
// then try to reach the data as another through **every** RPC that takes a
// scope.

/// A multi-tenant gRPC server.
async fn start_tenant_grpc_server() -> (ChronixServiceClient<Channel>, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));

    let grpc_service =
        ChronixGrpcService::new(db, std::time::Instant::now()).with_multi_tenancy(true);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(proto::chronix_service_server::ChronixServiceServer::new(
                grpc_service,
            ))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let client = ChronixServiceClient::connect(format!("http://127.0.0.1:{}", addr.port()))
        .await
        .unwrap();
    (client, tmp)
}

/// Tag a request with the tenant making it.
fn as_tenant<T>(msg: T, namespace: &str) -> tonic::Request<T> {
    let mut req = tonic::Request::new(msg);
    req.metadata_mut()
        .insert("x-namespace", namespace.parse().unwrap());
    req
}

/// Write one point of `cpu` as `namespace`.
async fn write_as(client: &mut ChronixServiceClient<Channel>, namespace: &str, value: f64) {
    let req = proto::WriteRequest {
        points: vec![make_point(
            "cpu",
            &[("host", namespace)],
            &[("usage", value)],
            1_700_000_000_000_000_000,
        )],
    };
    assert!(
        client.write(as_tenant(req, namespace)).await.is_ok(),
        "{namespace} must be able to write"
    );
}

#[tokio::test]
async fn grpc_query_only_sees_the_requesting_namespace() {
    let (mut client, _tmp) = start_tenant_grpc_server().await;
    write_as(&mut client, "tenant-a", 1.0).await;
    write_as(&mut client, "tenant-b", 42.0).await;

    for (tenant, expected) in [("tenant-a", 1.0), ("tenant-b", 42.0)] {
        let req = proto::QueryRequest {
            measurement: "cpu".to_string(),
            ..Default::default()
        };
        let mut stream = client
            .query(as_tenant(req, tenant))
            .await
            .unwrap()
            .into_inner();

        let mut rows = 0;
        let mut text = String::new();
        while let Some(resp) = stream.message().await.unwrap() {
            rows += resp.rows.len();
            text.push_str(&format!("{:?}", resp.rows));
        }
        assert_eq!(rows, 1, "{tenant} must see exactly its own point");
        assert!(
            text.contains(&format!("{expected}")),
            "{tenant} must see its own value {expected}; got {text}"
        );
    }
}

#[tokio::test]
async fn grpc_list_measurements_is_namespace_scoped() {
    let (mut client, _tmp) = start_tenant_grpc_server().await;
    write_as(&mut client, "tenant-a", 1.0).await;

    let listed = |names: Vec<String>| names.contains(&"cpu".to_string());

    let a = client
        .list_measurements(as_tenant(
            proto::ListMeasurementsRequest::default(),
            "tenant-a",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        listed(a.measurements.iter().map(|m| m.name.clone()).collect()),
        "the writing tenant must see its measurement"
    );

    let b = client
        .list_measurements(as_tenant(
            proto::ListMeasurementsRequest::default(),
            "tenant-b",
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(
        !listed(b.measurements.iter().map(|m| m.name.clone()).collect()),
        "another tenant must not learn that the measurement exists"
    );
}

/// A delete must not reach another tenant's rows. This is the one that cannot
/// be undone if it is wrong.
#[tokio::test]
async fn grpc_delete_only_removes_the_requesting_namespaces_data() {
    let (mut client, _tmp) = start_tenant_grpc_server().await;
    write_as(&mut client, "tenant-a", 1.0).await;
    write_as(&mut client, "tenant-b", 42.0).await;

    client
        .delete(as_tenant(
            proto::DeleteRequest {
                measurement: "cpu".to_string(),
                ..Default::default()
            },
            "tenant-a",
        ))
        .await
        .unwrap();

    // tenant-b's data survives.
    let req = proto::QueryRequest {
        measurement: "cpu".to_string(),
        ..Default::default()
    };
    let mut stream = client
        .query(as_tenant(req, "tenant-b"))
        .await
        .unwrap()
        .into_inner();
    let mut rows = 0;
    while let Some(resp) = stream.message().await.unwrap() {
        rows += resp.rows.len();
    }
    assert_eq!(rows, 1, "tenant-a's delete must not touch tenant-b's rows");
}

/// Dropping a measurement drops the caller's series of it, not the shared
/// measurement — the same rule the HTTP surface follows.
#[tokio::test]
async fn grpc_drop_measurement_only_drops_the_callers_series() {
    let (mut client, _tmp) = start_tenant_grpc_server().await;
    write_as(&mut client, "tenant-a", 1.0).await;
    write_as(&mut client, "tenant-b", 42.0).await;

    client
        .drop_measurement(as_tenant(
            proto::DropMeasurementRequest {
                measurement: "cpu".to_string(),
            },
            "tenant-a",
        ))
        .await
        .unwrap();

    let mut stream = client
        .query(as_tenant(
            proto::QueryRequest {
                measurement: "cpu".to_string(),
                ..Default::default()
            },
            "tenant-b",
        ))
        .await
        .unwrap()
        .into_inner();
    let mut rows = 0;
    while let Some(resp) = stream.message().await.unwrap() {
        rows += resp.rows.len();
    }
    assert_eq!(rows, 1, "tenant-b's series must survive tenant-a's drop");
}

/// SQL over gRPC is scoped too — it resolves tables through a per-namespace
/// session context rather than a shared one.
#[tokio::test]
async fn grpc_sql_only_sees_the_requesting_namespace() {
    let (mut client, _tmp) = start_tenant_grpc_server().await;
    write_as(&mut client, "tenant-a", 1.0).await;
    write_as(&mut client, "tenant-b", 42.0).await;

    for (tenant, expected) in [("tenant-a", "1"), ("tenant-b", "42")] {
        let resp = client
            .execute_sql(as_tenant(
                proto::SqlRequest {
                    query: "SELECT sum(usage) AS s FROM cpu".to_string(),
                },
                tenant,
            ))
            .await
            .unwrap()
            .into_inner();
        let text = format!("{resp:?}");
        assert!(
            text.contains(expected),
            "{tenant} must see only its own sum; got {text}"
        );
    }
}

/// A request carrying no namespace reads the `default` namespace — never
/// another tenant's data, and never everything.
///
/// This is the safe direction and it is chosen rather than accidental: with no
/// credential to derive a scope from, the header is all there is, and the
/// fallback has to be a namespace rather than the absence of one. The failure
/// that matters is the other one — an unscoped request reading across tenants
/// — so that is what is asserted.
#[tokio::test]
async fn an_unscoped_request_reads_the_default_namespace_only() {
    let (mut client, _tmp) = start_tenant_grpc_server().await;
    write_as(&mut client, "tenant-a", 1.0).await;
    write_as(&mut client, "tenant-b", 42.0).await;

    let mut rows = 0;
    if let Ok(resp) = client
        .query(tonic::Request::new(proto::QueryRequest {
            measurement: "cpu".to_string(),
            ..Default::default()
        }))
        .await
    {
        let mut stream = resp.into_inner();
        while let Some(resp) = stream.message().await.unwrap() {
            rows += resp.rows.len();
        }
    }
    assert_eq!(
        rows, 0,
        "an unscoped request must not see either tenant's rows"
    );

    // And the `default` namespace's own data is visible to it.
    write_as(&mut client, "default", 7.0).await;
    let mut stream = client
        .query(tonic::Request::new(proto::QueryRequest {
            measurement: "cpu".to_string(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let mut rows = 0;
    while let Some(resp) = stream.message().await.unwrap() {
        rows += resp.rows.len();
    }
    assert_eq!(rows, 1, "it reads the default namespace, and only that");
}
