#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! MQTT integration tests using testcontainers.
//!
//! These tests spin up a real Eclipse Mosquitto broker via Docker and
//! verify the end-to-end path: publish messages → `MqttSubscriber`
//! ingests → data appears in the Chronix database.
//!
//! # Requirements
//!
//! - Docker daemon running
//! - `--features mqtt` (or `all-connectors`)
//!
//! All tests are `#[ignore]` by default so `cargo test` doesn't require
//! Docker. Run them explicitly:
//!
//! ```sh
//! cargo test -p chronixd --features mqtt --test mqtt_integration -- --ignored
//! ```

#![cfg(feature = "mqtt")]

use std::sync::Arc;
use std::time::Duration;

use rumqttc::{AsyncClient, MqttOptions, QoS};
use tempfile::TempDir;

use testcontainers_modules::mosquitto::Mosquitto;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use chronix::Chronix;
use chronixd::connector::{ConnectorFormat, IngestionConnector, MqttConfig};
use chronixd::mqtt::MqttSubscriber;

// ── Helpers ────────────────────────────────────────────────────────────

/// Create a temp Chronix database for testing.
fn open_test_db() -> (Arc<Chronix>, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = chronix_core::ChronixConfig::builder()
        .data_dir(tmp.path())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));
    (db, tmp)
}

/// Build an `MqttConfig` for the given broker address and topic list.
fn make_mqtt_config(
    broker: &str,
    port: u16,
    topics: Vec<String>,
    format: chronixd::connector::ConnectorFormat,
) -> MqttConfig {
    MqttConfig {
        namespace: "default".to_string(),
        broker: broker.to_string(),
        port,
        client_id: format!("chronix-test-{}", rand::random::<u32>()),
        topics,
        qos: 1,
        format,
        ca_cert: None,
        client_cert: None,
        client_key: None,
        username: None,
        password: None,
        credential_file: None,
        topic_measurement_map: Default::default(),
    }
}

/// Create a rumqttc publisher client connected to the broker.
async fn make_publisher(broker: &str, port: u16) -> AsyncClient {
    let client_id = format!("test-pub-{}", rand::random::<u32>());
    let mut opts = MqttOptions::new(client_id, broker, port);
    opts.set_keep_alive(Duration::from_secs(30));
    opts.set_clean_session(true);

    let (client, mut eventloop) = AsyncClient::new(opts, 64);

    // Spawn event loop driver in background
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Publisher event loop error: {e}");
                    break;
                }
            }
        }
    });

    // Wait for connection
    tokio::time::sleep(Duration::from_millis(500)).await;
    client
}

// ── Tests ──────────────────────────────────────────────────────────────

/// Publish JSON messages via MQTT → verify the `MqttSubscriber`
/// ingests them into Chronix.
#[tokio::test]
#[ignore]
async fn mqtt_json_end_to_end() {
    let mosquitto_node = Mosquitto::default()
        .start()
        .await
        .expect("Failed to start Mosquitto container");

    let host_port = mosquitto_node
        .get_host_port_ipv4(1883)
        .await
        .expect("Mosquitto port");

    let broker = "127.0.0.1";
    let topic = "sensors/room1/temperature";

    // Set up Chronix + subscriber (must start before publishing so it
    // subscribes in time)
    let (db, _tmp) = open_test_db();
    let config = make_mqtt_config(
        broker,
        host_port,
        vec!["sensors/#".to_string()],
        ConnectorFormat::Json,
    );
    let subscriber = MqttSubscriber::new_arc("mqtt-json-test", config, db.clone());
    subscriber.start().await.expect("subscriber start");

    // Give the subscriber time to connect and subscribe
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Publish JSON messages
    let publisher = make_publisher(broker, host_port).await;

    let messages = vec![
        r#"{"fields":{"temp":22.5},"timestamp":1000}"#,
        r#"{"fields":{"temp":23.1},"timestamp":2000}"#,
        r#"{"fields":{"temp":21.8},"timestamp":3000}"#,
    ];

    for msg in &messages {
        publisher
            .publish(topic, QoS::AtLeastOnce, false, msg.as_bytes())
            .await
            .expect("MQTT publish failed");
    }

    // Wait for ingestion
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let metrics = subscriber.metrics().await;
        if metrics.points_total >= 3 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            let m = subscriber.metrics().await;
            panic!(
                "Timed out: messages={}, points={}, errors={}",
                m.messages_total, m.points_total, m.decode_errors
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    subscriber.stop().await.expect("subscriber stop");

    let metrics = subscriber.metrics().await;
    assert_eq!(metrics.points_total, 3, "expected 3 points ingested");
    assert_eq!(metrics.decode_errors, 0, "expected no decode errors");

    // Verify data — measurement from last topic segment = "temperature"
    let plan = db
        .query()
        .measurement("temperature")
        .range(0, i64::MAX)
        .build()
        .expect("build query plan");
    let batch = db.execute(&plan).expect("query failed");

    assert_eq!(batch.num_rows(), 3, "expected 3 points in database");
}

/// Publish InfluxDB Line Protocol messages via MQTT → verify ingestion.
#[tokio::test]
#[ignore]
async fn mqtt_line_protocol_end_to_end() {
    let mosquitto_node = Mosquitto::default()
        .start()
        .await
        .expect("Failed to start Mosquitto container");

    let host_port = mosquitto_node
        .get_host_port_ipv4(1883)
        .await
        .expect("Mosquitto port");

    let broker = "127.0.0.1";
    let topic = "metrics/cpu";

    let (db, _tmp) = open_test_db();
    let config = make_mqtt_config(
        broker,
        host_port,
        vec!["metrics/#".to_string()],
        ConnectorFormat::LineProtocol,
    );
    let subscriber = MqttSubscriber::new_arc("mqtt-lp-test", config, db.clone());
    subscriber.start().await.expect("subscriber start");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let publisher = make_publisher(broker, host_port).await;

    let lines = vec![
        "cpu,host=srv1 usage=87.5 1000000000",
        "cpu,host=srv2 usage=42.1 2000000000",
    ];

    for line in &lines {
        publisher
            .publish(topic, QoS::AtLeastOnce, false, line.as_bytes())
            .await
            .expect("MQTT publish failed");
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let metrics = subscriber.metrics().await;
        if metrics.points_total >= 2 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            let m = subscriber.metrics().await;
            panic!(
                "Timed out: messages={}, points={}, errors={}",
                m.messages_total, m.points_total, m.decode_errors
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    subscriber.stop().await.expect("subscriber stop");

    let metrics = subscriber.metrics().await;
    assert_eq!(metrics.points_total, 2);
    assert_eq!(metrics.decode_errors, 0);

    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .expect("build query plan");
    let batch = db.execute(&plan).expect("query failed");

    assert_eq!(batch.num_rows(), 2);
}

/// Verify that topic-derived tags are applied to ingested points.
#[tokio::test]
#[ignore]
async fn mqtt_topic_tags_applied() {
    let mosquitto_node = Mosquitto::default()
        .start()
        .await
        .expect("Failed to start Mosquitto container");

    let host_port = mosquitto_node
        .get_host_port_ipv4(1883)
        .await
        .expect("Mosquitto port");

    let broker = "127.0.0.1";

    let (db, _tmp) = open_test_db();
    let config = make_mqtt_config(
        broker,
        host_port,
        vec!["sensors/#".to_string()],
        ConnectorFormat::Json,
    );
    let subscriber = MqttSubscriber::new_arc("mqtt-tags-test", config, db.clone());
    subscriber.start().await.expect("subscriber start");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let publisher = make_publisher(broker, host_port).await;

    // Topic: sensors/building-1/temperature → tag topic_level_1=building-1
    publisher
        .publish(
            "sensors/building-1/temperature",
            QoS::AtLeastOnce,
            false,
            br#"{"tags":{"device":"sensor-42"},"fields":{"temp":22.5},"timestamp":1000}"#,
        )
        .await
        .expect("publish");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let metrics = subscriber.metrics().await;
        if metrics.points_total >= 1 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            let m = subscriber.metrics().await;
            panic!(
                "Timed out: messages={}, points={}, errors={}",
                m.messages_total, m.points_total, m.decode_errors
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    subscriber.stop().await.expect("subscriber stop");

    let plan = db
        .query()
        .measurement("temperature")
        .range(0, i64::MAX)
        .build()
        .expect("build query plan");
    let batch = db.execute(&plan).expect("query failed");

    assert_eq!(batch.num_rows(), 1);

    // Verify both payload tags and topic-derived tags are present
    // We need to check the original points, not the Arrow batch
    // since tags are columns in the batch
    let schema = batch.schema();
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        col_names.contains(&"device"),
        "column 'device' (from payload tag) should be present in schema, got: {col_names:?}"
    );
    assert!(
        col_names.contains(&"topic_level_1"),
        "column 'topic_level_1' (from topic path) should be present in schema, got: {col_names:?}"
    );
}

/// Publish malformed messages → verify decode errors are counted
/// and valid messages still ingested.
#[tokio::test]
#[ignore]
async fn mqtt_decode_error_resilience() {
    let mosquitto_node = Mosquitto::default()
        .start()
        .await
        .expect("Failed to start Mosquitto container");

    let host_port = mosquitto_node
        .get_host_port_ipv4(1883)
        .await
        .expect("Mosquitto port");

    let broker = "127.0.0.1";

    let (db, _tmp) = open_test_db();
    let config = make_mqtt_config(
        broker,
        host_port,
        vec!["test/#".to_string()],
        ConnectorFormat::Json,
    );
    let subscriber = MqttSubscriber::new_arc("mqtt-error-test", config, db.clone());
    subscriber.start().await.expect("subscriber start");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let publisher = make_publisher(broker, host_port).await;

    // Valid message
    publisher
        .publish(
            "test/metric",
            QoS::AtLeastOnce,
            false,
            br#"{"fields":{"value":1.0},"timestamp":1000}"#,
        )
        .await
        .expect("publish valid");

    // Malformed JSON
    publisher
        .publish("test/metric", QoS::AtLeastOnce, false, b"this is not json!")
        .await
        .expect("publish invalid");

    // Another valid message
    publisher
        .publish(
            "test/metric",
            QoS::AtLeastOnce,
            false,
            br#"{"fields":{"value":2.0},"timestamp":2000}"#,
        )
        .await
        .expect("publish valid 2");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let m = subscriber.metrics().await;
        if m.messages_total >= 3 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!(
                "Timed out: messages={}, points={}, errors={}",
                m.messages_total, m.points_total, m.decode_errors
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    subscriber.stop().await.expect("subscriber stop");

    let metrics = subscriber.metrics().await;
    assert_eq!(metrics.points_total, 2, "expected 2 valid points");
    assert!(
        metrics.decode_errors >= 1,
        "expected at least 1 decode error, got {}",
        metrics.decode_errors
    );
}

/// Verify connector lifecycle: start → running → stop → stopped,
/// with a real Mosquitto broker.
#[tokio::test]
#[ignore]
async fn mqtt_connector_lifecycle_live() {
    use chronixd::connector::ConnectorStatus;

    let mosquitto_node = Mosquitto::default()
        .start()
        .await
        .expect("Failed to start Mosquitto container");

    let host_port = mosquitto_node
        .get_host_port_ipv4(1883)
        .await
        .expect("Mosquitto port");

    let (db, _tmp) = open_test_db();
    let config = make_mqtt_config(
        "127.0.0.1",
        host_port,
        vec!["lifecycle/#".to_string()],
        ConnectorFormat::Json,
    );
    let subscriber = MqttSubscriber::new_arc("mqtt-lifecycle", config, db);

    assert_eq!(subscriber.status().await, ConnectorStatus::Stopped);

    subscriber.start().await.expect("start");
    assert_eq!(subscriber.status().await, ConnectorStatus::Running);
    assert!(subscriber.is_healthy().await);

    subscriber.stop().await.expect("stop");
    assert_eq!(subscriber.status().await, ConnectorStatus::Stopped);
}

/// Verify ingestion across multiple MQTT topics with different
/// measurements.
#[tokio::test]
#[ignore]
async fn mqtt_multi_topic_routing() {
    let mosquitto_node = Mosquitto::default()
        .start()
        .await
        .expect("Failed to start Mosquitto container");

    let host_port = mosquitto_node
        .get_host_port_ipv4(1883)
        .await
        .expect("Mosquitto port");

    let broker = "127.0.0.1";

    let (db, _tmp) = open_test_db();
    let config = make_mqtt_config(
        broker,
        host_port,
        vec!["devices/#".to_string()],
        ConnectorFormat::Json,
    );
    let subscriber = MqttSubscriber::new_arc("mqtt-multi-test", config, db.clone());
    subscriber.start().await.expect("subscriber start");

    tokio::time::sleep(Duration::from_secs(2)).await;

    let publisher = make_publisher(broker, host_port).await;

    // Different measurement names from different topic paths
    publisher
        .publish(
            "devices/temperature",
            QoS::AtLeastOnce,
            false,
            br#"{"fields":{"value":22.5},"timestamp":1000}"#,
        )
        .await
        .expect("publish temp");

    publisher
        .publish(
            "devices/humidity",
            QoS::AtLeastOnce,
            false,
            br#"{"fields":{"value":65.0},"timestamp":2000}"#,
        )
        .await
        .expect("publish humidity");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let metrics = subscriber.metrics().await;
        if metrics.points_total >= 2 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            let m = subscriber.metrics().await;
            panic!(
                "Timed out: messages={}, points={}, errors={}",
                m.messages_total, m.points_total, m.decode_errors
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    subscriber.stop().await.expect("subscriber stop");

    // Verify each measurement routed correctly
    let temp_plan = db
        .query()
        .measurement("temperature")
        .range(0, i64::MAX)
        .build()
        .expect("build temperature plan");
    let temp_batch = db.execute(&temp_plan).expect("query temperature");
    assert_eq!(temp_batch.num_rows(), 1, "expected 1 temperature point");

    let humidity_plan = db
        .query()
        .measurement("humidity")
        .range(0, i64::MAX)
        .build()
        .expect("build humidity plan");
    let humidity_batch = db.execute(&humidity_plan).expect("query humidity");
    assert_eq!(humidity_batch.num_rows(), 1, "expected 1 humidity point");
}
