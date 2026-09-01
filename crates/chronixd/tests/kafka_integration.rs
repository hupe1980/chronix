#![allow(clippy::unwrap_used, clippy::expect_used)]
// test code may unwrap
// `krafka`'s builder futures are large by construction and these run one at a
// time in a test, where stack size is not a constraint.
#![allow(clippy::large_futures)]
//! Kafka integration tests using testcontainers.
//!
//! These tests spin up a real Apache Kafka broker via Docker and verify
//! the end-to-end path: produce messages → `KafkaConsumer` ingests →
//! data appears in the Chronix database.
//!
//! # Requirements
//!
//! - Docker daemon running
//! - `--features kafka` (or `all-connectors`)
//!
//! All tests are `#[ignore]` by default so `cargo test` doesn't require
//! Docker. Run them explicitly:
//!
//! ```sh
//! cargo test -p chronixd --features kafka --test kafka_integration -- --ignored
//! ```

#![cfg(feature = "kafka")]

use std::sync::Arc;
use std::time::Duration;

use krafka::producer::Producer;
use tempfile::TempDir;

use testcontainers_modules::kafka::apache::{self, Kafka};
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use chronix::Chronix;
use chronixd::connector::{ConnectorFormat, IngestionConnector, KafkaConfig};
use chronixd::kafka::KafkaConsumer;

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

/// Create a `krafka` producer pointing at the given bootstrap server.
async fn make_producer(bootstrap_servers: &str) -> Producer {
    Producer::builder()
        .bootstrap_servers(bootstrap_servers)
        .request_timeout(Duration::from_secs(10))
        .build()
        .await
        .expect("Failed to create Kafka producer")
}

/// Build a `KafkaConfig` for the given broker and topic.
fn make_kafka_config(
    bootstrap_servers: &str,
    topics: Vec<String>,
    format: chronixd::connector::ConnectorFormat,
) -> KafkaConfig {
    KafkaConfig {
        namespace: "default".to_string(),
        brokers: bootstrap_servers.to_string(),
        group_id: "chronix-test".to_string(),
        topics,
        format,
        auto_offset_reset: "earliest".to_string(),
        topic_measurement_map: Default::default(),
        sasl_mechanism: None,
        sasl_username: None,
        sasl_password: None,
        security_protocol: None,
        ca_cert: None,
        credential_file: None,
    }
}

// ── Tests ──────────────────────────────────────────────────────────────

/// Produce InfluxDB Line Protocol messages to Kafka → verify the
/// `KafkaConsumer` ingests them into Chronix.
#[tokio::test]
#[ignore]
async fn kafka_line_protocol_end_to_end() {
    let kafka_node = Kafka::default()
        .start()
        .await
        .expect("Failed to start Kafka container");

    let bootstrap = format!(
        "127.0.0.1:{}",
        kafka_node
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("Kafka port")
    );

    let topic = "test-lp";
    let producer = make_producer(&bootstrap).await;

    // Produce line-protocol messages
    let lines = vec![
        "cpu,host=srv1 usage=87.5 1000000000",
        "cpu,host=srv2 usage=42.1 2000000000",
        "cpu,host=srv1 usage=91.3 3000000000",
    ];

    for line in &lines {
        let _ = producer
            .send(topic, Some(b"k"), Some(line.as_bytes()))
            .await
            .expect("Kafka produce failed");
    }

    // Set up Chronix + consumer
    let (db, _tmp) = open_test_db();
    let config = make_kafka_config(
        &bootstrap,
        vec![topic.to_string()],
        ConnectorFormat::LineProtocol,
    );
    let consumer = KafkaConsumer::new_arc("kafka-lp-test", config, db.clone());

    consumer.start().await.expect("consumer start");

    // Wait for ingestion (poll until data appears or timeout)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let metrics = consumer.metrics().await;
        if metrics.points_total >= 3 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            let m = consumer.metrics().await;
            panic!(
                "Timed out waiting for Kafka ingestion: messages={}, points={}, errors={}",
                m.messages_total, m.points_total, m.decode_errors
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    consumer.stop().await.expect("consumer stop");

    // Verify data in Chronix
    let metrics = consumer.metrics().await;
    assert_eq!(metrics.points_total, 3, "expected 3 points ingested");
    assert_eq!(metrics.decode_errors, 0, "expected no decode errors");

    // Query the database to verify
    let plan = db
        .query()
        .measurement("cpu")
        .range(0, i64::MAX)
        .build()
        .expect("build query plan");
    let batch = db.execute(&plan).expect("query failed");

    assert_eq!(batch.num_rows(), 3, "expected 3 points in database");
}

/// Produce JSON messages to Kafka → verify ingestion.
#[tokio::test]
#[ignore]
async fn kafka_json_end_to_end() {
    let kafka_node = Kafka::default()
        .start()
        .await
        .expect("Failed to start Kafka container");

    let bootstrap = format!(
        "127.0.0.1:{}",
        kafka_node
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("Kafka port")
    );

    let topic = "test-json";
    let producer = make_producer(&bootstrap).await;

    // Produce JSON messages (topic name becomes measurement)
    let messages = vec![
        r#"{"tags":{"host":"srv1"},"fields":{"temp":22.5},"timestamp":1000}"#,
        r#"{"tags":{"host":"srv2"},"fields":{"temp":18.3},"timestamp":2000}"#,
    ];

    for msg in &messages {
        let _ = producer
            .send(topic, Some(b"k"), Some(msg.as_bytes()))
            .await
            .expect("Kafka produce failed");
    }

    let (db, _tmp) = open_test_db();
    let config = make_kafka_config(&bootstrap, vec![topic.to_string()], ConnectorFormat::Json);
    let consumer = KafkaConsumer::new_arc("kafka-json-test", config, db.clone());

    consumer.start().await.expect("consumer start");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let metrics = consumer.metrics().await;
        if metrics.points_total >= 2 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            let m = consumer.metrics().await;
            panic!(
                "Timed out: messages={}, points={}, errors={}",
                m.messages_total, m.points_total, m.decode_errors
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    consumer.stop().await.expect("consumer stop");

    let metrics = consumer.metrics().await;
    assert_eq!(metrics.points_total, 2);
    assert_eq!(metrics.decode_errors, 0);

    let plan = db
        .query()
        .measurement("test-json")
        .range(0, i64::MAX)
        .build()
        .expect("build query plan");
    let batch = db.execute(&plan).expect("query failed");

    assert_eq!(batch.num_rows(), 2);
}

/// Produce to multiple topics → verify topic-to-measurement mapping.
#[tokio::test]
#[ignore]
async fn kafka_multi_topic_mapping() {
    let kafka_node = Kafka::default()
        .start()
        .await
        .expect("Failed to start Kafka container");

    let bootstrap = format!(
        "127.0.0.1:{}",
        kafka_node
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("Kafka port")
    );

    let topics = vec!["topic-a".to_string(), "topic-b".to_string()];
    let producer = make_producer(&bootstrap).await;

    // Produce to topic-a (line protocol)
    let _ = producer
        .send(
            "topic-a",
            Some(b"k"),
            Some(b"mem,host=srv1 used=1024 1000000000" as &[u8]),
        )
        .await
        .expect("produce topic-a");

    // Produce to topic-b
    let _ = producer
        .send(
            "topic-b",
            Some(b"k"),
            Some(b"disk,host=srv1 free=50000 2000000000" as &[u8]),
        )
        .await
        .expect("produce topic-b");

    let (db, _tmp) = open_test_db();
    let config = make_kafka_config(&bootstrap, topics, ConnectorFormat::LineProtocol);
    let consumer = KafkaConsumer::new_arc("kafka-multi-test", config, db.clone());

    consumer.start().await.expect("consumer start");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let metrics = consumer.metrics().await;
        if metrics.points_total >= 2 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            let m = consumer.metrics().await;
            panic!(
                "Timed out: messages={}, points={}, errors={}",
                m.messages_total, m.points_total, m.decode_errors
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    consumer.stop().await.expect("consumer stop");

    // Verify each measurement exists
    let mem_plan = db
        .query()
        .measurement("mem")
        .range(0, i64::MAX)
        .build()
        .expect("build mem plan");
    let mem_batch = db.execute(&mem_plan).expect("query mem");
    assert_eq!(mem_batch.num_rows(), 1, "expected 1 'mem' point");

    let disk_plan = db
        .query()
        .measurement("disk")
        .range(0, i64::MAX)
        .build()
        .expect("build disk plan");
    let disk_batch = db.execute(&disk_plan).expect("query disk");
    assert_eq!(disk_batch.num_rows(), 1, "expected 1 'disk' point");
}

/// Produce malformed messages → verify decode errors are counted
/// and valid messages still ingested.
#[tokio::test]
#[ignore]
async fn kafka_decode_error_resilience() {
    let kafka_node = Kafka::default()
        .start()
        .await
        .expect("Failed to start Kafka container");

    let bootstrap = format!(
        "127.0.0.1:{}",
        kafka_node
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("Kafka port")
    );

    let topic = "test-errors";
    let producer = make_producer(&bootstrap).await;

    // First: valid message
    let _ = producer
        .send(
            topic,
            Some(b"k"),
            Some(b"cpu,host=srv1 usage=50.0 1000000000" as &[u8]),
        )
        .await
        .expect("produce valid");

    // Second: malformed line protocol
    let _ = producer
        .send(
            topic,
            Some(b"k"),
            Some(b"this is not valid line protocol!!!" as &[u8]),
        )
        .await
        .expect("produce invalid");

    // Third: another valid message
    let _ = producer
        .send(
            topic,
            Some(b"k"),
            Some(b"cpu,host=srv2 usage=75.0 2000000000" as &[u8]),
        )
        .await
        .expect("produce valid 2");

    let (db, _tmp) = open_test_db();
    let config = make_kafka_config(
        &bootstrap,
        vec![topic.to_string()],
        ConnectorFormat::LineProtocol,
    );
    let consumer = KafkaConsumer::new_arc("kafka-error-test", config, db.clone());

    consumer.start().await.expect("consumer start");

    // Wait until all 3 messages are consumed (2 valid + 1 error)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let m = consumer.metrics().await;
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

    consumer.stop().await.expect("consumer stop");

    let metrics = consumer.metrics().await;
    assert_eq!(metrics.points_total, 2, "expected 2 valid points");
    assert!(
        metrics.decode_errors >= 1,
        "expected at least 1 decode error, got {}",
        metrics.decode_errors
    );
}

/// Verify connector lifecycle: start → running → stop → stopped,
/// with a real Kafka broker.
#[tokio::test]
#[ignore]
async fn kafka_connector_lifecycle_live() {
    use chronixd::connector::ConnectorStatus;

    let kafka_node = Kafka::default()
        .start()
        .await
        .expect("Failed to start Kafka container");

    let bootstrap = format!(
        "127.0.0.1:{}",
        kafka_node
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("Kafka port")
    );

    let (db, _tmp) = open_test_db();
    let config = make_kafka_config(
        &bootstrap,
        vec!["lifecycle-test".to_string()],
        ConnectorFormat::Json,
    );
    let consumer = KafkaConsumer::new_arc("kafka-lifecycle", config, db);

    assert_eq!(consumer.status().await, ConnectorStatus::Stopped);

    consumer.start().await.expect("start");
    assert_eq!(consumer.status().await, ConnectorStatus::Running);
    assert!(consumer.is_healthy().await);

    consumer.stop().await.expect("stop");
    assert_eq!(consumer.status().await, ConnectorStatus::Stopped);
}
