#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Native histograms end to end, over the wire a real sender uses.
//!
//! The unit tests in `wire::histogram` prove the span arithmetic. These prove
//! the thing that actually matters to a user: that a histogram **sent by
//! Prometheus or an OTel collector** is stored, queried and read back — over
//! HTTP, through Snappy, through the router, through the handler's content-type
//! negotiation, and out again through PromQL.
//!
//! That distinction is not pedantry. Every piece of this feature passed its own
//! tests while nothing on the outside could send chronix a histogram at all.

use std::net::SocketAddr;
use std::sync::Arc;

use prost::Message;
use serde_json::Value;
use tempfile::TempDir;
use tokio::net::TcpListener;

use chronix::Chronix;
use chronix::prelude::*;
use chronixd::http::{AppState, SharedState};
use chronixd::server::build_router;
use chronixd::{otlp, prom_proto, prom_proto_v2};

/// An empty server, ready to be written to over HTTP.
async fn server() -> (String, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = ChronixConfigBuilder::default()
        .data_dir(tmp.path().to_path_buf())
        .build()
        .expect("chronix config");
    let db = Arc::new(Chronix::open(config).expect("open db"));

    let sql_contexts = chronixd::namespace::SqlContexts::new(db.clone());
    let state: AppState = Arc::new(SharedState {
        db,
        start_time: std::time::Instant::now(),
        connector_manager: None,
        sql_contexts,
        auth_state: None,
        #[cfg(feature = "cluster")]
        meta_client: None,
        namespace_registry: std::sync::Arc::new(chronix_security::tenant::NamespaceRegistry::new()),
        model_catalog: Arc::new(parking_lot::RwLock::new(
            chronix::chronix_analytics::forecast::ModelCatalog::new(),
        )),
        authz_engine: None,
        audit_logger: None,
        config: chronixd::config::ServerConfig::default(),
        namespace_rate_limiter: chronixd::rate_limit::NamespaceRateLimiter::new(),
        sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
        write_dedup_cache: None,
        write_timeout: std::time::Duration::ZERO,
        pipeline: None,
        openapi_json: std::sync::OnceLock::new(),
    });

    let metrics_handle = chronixd::server::prometheus_builder()
        .expect("bucket config")
        .build_recorder()
        .handle();
    let app = build_router(state, "/metrics", metrics_handle, 10 * 1024 * 1024, None);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr: SocketAddr = listener.local_addr().expect("local_addr");
    let base = format!("http://{addr}");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (base, tmp)
}

/// A recent millisecond timestamp, so the sample lands inside the write window.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// POST a Snappy-compressed protobuf body, returning the response.
async fn post_proto(
    base: &str,
    path: &str,
    content_type: Option<&str>,
    body: Vec<u8>,
) -> reqwest::Response {
    chronixd::tls::ensure_crypto_provider();
    let compressed = snap::raw::Encoder::new().compress_vec(&body).unwrap();
    let mut req = reqwest::Client::new()
        .post(format!("{base}{path}"))
        .body(compressed);
    if let Some(ct) = content_type {
        req = req.header("content-type", ct);
    }
    req.send().await.unwrap()
}

/// Run a PromQL instant query and return `data`.
async fn promql(base: &str, query: &str) -> Value {
    chronixd::tls::ensure_crypto_provider();
    let resp = reqwest::Client::new()
        .get(format!("{base}/api/v1/query"))
        .query(&[("query", query)])
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: Value = resp.json().await.unwrap();
    assert!(status.is_success(), "{query} → {status}: {body}");
    assert_eq!(body["status"], "success", "{query}: {body}");
    body["data"].clone()
}

/// The single scalar a vector-of-one query returned.
fn one_value(data: &Value) -> f64 {
    let result = data["result"].as_array().expect("a result array");
    assert_eq!(result.len(), 1, "expected exactly one series: {data}");
    result[0]["value"][1]
        .as_str()
        .expect("a string value")
        .parse()
        .expect("a number")
}

/// A histogram of 12 observations, as Prometheus encodes one.
///
/// Schema 0 (each bucket twice the previous), buckets chosen by hand from the
/// `(2^(2^-0))^i` boundaries so the expected quantiles can be stated without
/// running the code:
///
/// | index | covers    | count |
/// |-------|-----------|-------|
/// | 1     | (1, 2]    | 2     |
/// | 2     | (2, 4]    | 4     |
/// | 3     | (4, 8]    | 6     |
///
/// One span `{offset: 1, length: 3}` with deltas `[2, 2, 2]` — cumulative, so
/// 2, 4, 6.
fn sample_histogram(timestamp: i64) -> prom_proto::Histogram {
    prom_proto::Histogram {
        count: Some(prom_proto::histogram::Count::CountInt(12)),
        sum: 54.0,
        schema: 0,
        zero_threshold: 0.0,
        zero_count: Some(prom_proto::histogram::ZeroCount::ZeroCountInt(0)),
        negative_spans: vec![],
        negative_deltas: vec![],
        negative_counts: vec![],
        positive_spans: vec![prom_proto::BucketSpan {
            offset: 1,
            length: 3,
        }],
        positive_deltas: vec![2, 2, 2],
        positive_counts: vec![],
        reset_hint: 0,
        timestamp,
        custom_values: vec![],
    }
}

/// Remote write 1.0 carrying a native histogram, queried back through PromQL.
///
/// 1.0 has carried histograms since Prometheus 2.40 and is still what a
/// `remote_write` block with no `protobuf_message` sends, so this is the path
/// most existing deployments take.
#[tokio::test]
async fn remote_write_v1_histogram_is_stored_and_queryable() {
    let (base, _tmp) = server().await;
    let ts = now_ms();

    let req = prom_proto::WriteRequest {
        timeseries: vec![prom_proto::TimeSeries {
            labels: vec![
                prom_proto::Label {
                    name: "__name__".into(),
                    value: "request_latency".into(),
                },
                prom_proto::Label {
                    name: "job".into(),
                    value: "api".into(),
                },
            ],
            samples: vec![],
            exemplars: vec![],
            histograms: vec![sample_histogram(ts)],
        }],
    };

    let resp = post_proto(
        &base,
        "/api/v1/prom/write",
        Some("application/x-protobuf"),
        req.encode_to_vec(),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NO_CONTENT,
        "a 1.0 write carrying only histograms must be accepted"
    );

    // The metric keeps its own name — `request_latency`, not
    // `request_latency_value` and not one series per bucket.
    let count = one_value(&promql(&base, "histogram_count(request_latency)").await);
    assert_eq!(count, 12.0);

    let sum = one_value(&promql(&base, "histogram_sum(request_latency)").await);
    assert_eq!(sum, 54.0);

    // 12 observations, cumulative 2 / 6 / 12 across (1,2] (2,4] (4,8]. The
    // median is the 6th, which is the last of the (2,4] bucket, so it lands at
    // that bucket's top: 4.
    let median = one_value(&promql(&base, "histogram_quantile(0.5, request_latency)").await);
    assert!(
        (2.0..=4.0).contains(&median),
        "the median falls in (2, 4], got {median}"
    );

    // Everything is somewhere.
    let all = one_value(&promql(&base, "histogram_fraction(0, 1e9, request_latency)").await);
    assert!(
        (all - 1.0).abs() < 1e-9,
        "expected the whole of it, got {all}"
    );
}

/// Remote write 2.0: same histogram, symbol table, accounting headers.
///
/// Two series in one request, sharing symbols — which is the point of 2.0:
/// `job` and `api` are each interned once no matter how many series carry
/// them.
#[tokio::test]
async fn remote_write_v2_histogram_is_stored_and_counted_in_the_response() {
    let (base, _tmp) = server().await;
    let ts = now_ms();

    // `symbols[0]` is the empty string by specification, so a zero reference
    // is unambiguously "absent".
    let symbols = vec![
        String::new(),
        "__name__".to_string(),
        "request_latency".to_string(),
        "job".to_string(),
        "api".to_string(),
        "request_total".to_string(),
    ];
    let h = sample_histogram(ts);
    let req = prom_proto_v2::Request {
        symbols,
        timeseries: vec![
            prom_proto_v2::TimeSeries {
                label_refs: vec![1, 2, 3, 4],
                samples: vec![],
                histograms: vec![prom_proto_v2::Histogram {
                    count: Some(prom_proto_v2::histogram::Count::CountInt(12)),
                    sum: h.sum,
                    schema: h.schema,
                    zero_threshold: h.zero_threshold,
                    zero_count: Some(prom_proto_v2::histogram::ZeroCount::ZeroCountInt(0)),
                    negative_spans: vec![],
                    negative_deltas: vec![],
                    negative_counts: vec![],
                    positive_spans: vec![prom_proto_v2::BucketSpan {
                        offset: 1,
                        length: 3,
                    }],
                    positive_deltas: vec![2, 2, 2],
                    positive_counts: vec![],
                    reset_hint: 0,
                    timestamp: ts,
                    custom_values: vec![],
                }],
                exemplars: vec![],
                metadata: None,
                created_timestamp: 0,
            },
            // A plain counter, sharing the `job="api"` symbols above.
            prom_proto_v2::TimeSeries {
                label_refs: vec![1, 5, 3, 4],
                samples: vec![prom_proto_v2::Sample {
                    value: 42.0,
                    timestamp: ts,
                }],
                histograms: vec![],
                exemplars: vec![],
                metadata: None,
                created_timestamp: 0,
            },
        ],
    };

    let resp = post_proto(
        &base,
        "/api/v1/prom/write",
        Some("application/x-protobuf;proto=io.prometheus.write.v2.Request"),
        req.encode_to_vec(),
    )
    .await;
    let st = resp.status();
    if st != reqwest::StatusCode::NO_CONTENT {
        panic!("{st}: {}", resp.text().await.unwrap());
    }

    // The accounting headers are the only thing separating "stored" from
    // "silently dropped", since both answer 204.
    let header = |name: &str| {
        resp.headers()
            .get(name)
            .unwrap_or_else(|| panic!("2.0 must answer with {name}"))
            .to_str()
            .unwrap()
            .to_string()
    };
    assert_eq!(header("x-prometheus-remote-write-histograms-written"), "1");
    assert_eq!(header("x-prometheus-remote-write-samples-written"), "1");
    assert_eq!(
        header("x-prometheus-remote-write-exemplars-written"),
        "0",
        "chronix stores no exemplars, and says so rather than omitting the header"
    );

    // Both series landed, and the labels were reassembled from the shared
    // symbol table rather than from whichever entry happened to be nearby.
    assert_eq!(
        one_value(&promql(&base, "histogram_count(request_latency{job=\"api\"})").await),
        12.0
    );
    assert_eq!(
        one_value(&promql(&base, "request_total{job=\"api\"}").await),
        42.0
    );
}

/// One metric cannot be a float series and a histogram series at once.
///
/// Both land in the same `value` column, and a column has one type. The
/// refusal is the right answer — a metric that is a gauge on Monday and a
/// distribution on Tuesday is two metrics — and what this pins is that the
/// message **names the column and both types**, because the sender has to
/// find which of its series is the odd one.
#[tokio::test]
async fn a_metric_cannot_be_both_a_float_and_a_histogram() {
    let (base, _tmp) = server().await;
    let ts = now_ms();

    let req = prom_proto::WriteRequest {
        timeseries: vec![prom_proto::TimeSeries {
            labels: vec![prom_proto::Label {
                name: "__name__".into(),
                value: "confused".into(),
            }],
            samples: vec![prom_proto::Sample {
                value: 1.0,
                timestamp: ts,
            }],
            exemplars: vec![],
            histograms: vec![sample_histogram(ts)],
        }],
    };
    let resp = post_proto(
        &base,
        "/api/v1/prom/write",
        Some("application/x-protobuf"),
        req.encode_to_vec(),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await.unwrap();
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("confused") && msg.contains("histogram") && msg.contains("f64"),
        "the refusal must name the measurement and both types: {msg}"
    );
}

/// A 1.0 write answers no accounting headers, because 1.0 does not define them.
#[tokio::test]
async fn remote_write_v1_does_not_invent_the_two_point_oh_headers() {
    let (base, _tmp) = server().await;
    let req = prom_proto::WriteRequest {
        timeseries: vec![prom_proto::TimeSeries {
            labels: vec![prom_proto::Label {
                name: "__name__".into(),
                value: "up".into(),
            }],
            samples: vec![prom_proto::Sample {
                value: 1.0,
                timestamp: now_ms(),
            }],
            exemplars: vec![],
            histograms: vec![],
        }],
    };
    let resp = post_proto(
        &base,
        "/api/v1/prom/write",
        Some("application/x-protobuf"),
        req.encode_to_vec(),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    assert!(
        resp.headers()
            .get("x-prometheus-remote-write-samples-written")
            .is_none()
    );
}

/// An unknown `proto=` is 415, naming what this receiver does speak.
///
/// The status is the contract: a sender configured for a message set the
/// receiver has never heard of uses 415 to fall back, and a 400 would tell it
/// to give up on the data instead.
#[tokio::test]
async fn an_unknown_write_proto_is_415_and_names_the_supported_ones() {
    let (base, _tmp) = server().await;
    let resp = post_proto(
        &base,
        "/api/v1/prom/write",
        Some("application/x-protobuf;proto=io.prometheus.write.v9.Request"),
        Vec::new(),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let body: Value = resp.json().await.unwrap();
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("io.prometheus.write.v2.Request") && msg.contains("prometheus.WriteRequest"),
        "the refusal must name both supported message sets so a sender can \
         pick one: {msg}"
    );
}

/// A histogram written over remote write comes back over remote read.
///
/// Half a round trip is not a round trip: a federating Prometheus reading its
/// own long-term storage must get the distribution back, not an empty series
/// where the handler skipped a column it did not recognise.
#[tokio::test]
async fn a_histogram_survives_remote_write_and_remote_read() {
    let (base, _tmp) = server().await;
    let ts = now_ms();

    let write = prom_proto::WriteRequest {
        timeseries: vec![prom_proto::TimeSeries {
            labels: vec![prom_proto::Label {
                name: "__name__".into(),
                value: "rt_latency".into(),
            }],
            samples: vec![],
            exemplars: vec![],
            histograms: vec![sample_histogram(ts)],
        }],
    };
    let resp = post_proto(
        &base,
        "/api/v1/prom/write",
        Some("application/x-protobuf"),
        write.encode_to_vec(),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);

    let read = prom_proto::ReadRequest {
        queries: vec![prom_proto::Query {
            start_timestamp_ms: ts - 60_000,
            end_timestamp_ms: ts + 60_000,
            matchers: vec![prom_proto::LabelMatcher {
                r#type: prom_proto::label_matcher::Type::Eq as i32,
                name: "__name__".into(),
                value: "rt_latency".into(),
            }],
        }],
    };
    let resp = post_proto(
        &base,
        "/api/v1/prom/read",
        Some("application/x-protobuf"),
        read.encode_to_vec(),
    )
    .await;
    assert!(resp.status().is_success(), "read → {}", resp.status());

    let body = resp.bytes().await.unwrap();
    let decompressed = snap::raw::Decoder::new().decompress_vec(&body).unwrap();
    let response = prom_proto::ReadResponse::decode(decompressed.as_slice()).unwrap();

    let series = &response.results[0].timeseries;
    assert_eq!(series.len(), 1, "one series back");
    assert!(
        series[0].samples.is_empty(),
        "a distribution must not be flattened into a float sample"
    );
    assert_eq!(series[0].histograms.len(), 1, "the histogram comes back");

    let back = &series[0].histograms[0];
    assert_eq!(back.schema, 0);
    assert_eq!(back.timestamp, ts);
    assert_eq!(
        back.count,
        Some(prom_proto::histogram::Count::CountFloat(12.0))
    );
    assert_eq!(back.sum, 54.0);
    // Re-decoding the response must land on the same three buckets, whatever
    // span layout the encoder chose.
    assert_eq!(back.positive_counts, vec![2.0, 4.0, 6.0]);
    assert_eq!(
        back.positive_spans
            .iter()
            .map(|s| (s.offset, s.length))
            .collect::<Vec<_>>(),
        vec![(1, 3)]
    );
}

/// An OTLP exponential histogram is stored and answers `histogram_quantile`.
#[tokio::test]
async fn otlp_exponential_histogram_is_stored_and_queryable() {
    let (base, _tmp) = server().await;
    let ts = now_ms() as u64 * 1_000_000;

    let req = otlp::ExportMetricsServiceRequest {
        resource_metrics: vec![otlp::ResourceMetrics {
            resource: None,
            scope_metrics: vec![otlp::ScopeMetrics {
                scope: None,
                metrics: vec![otlp::Metric {
                    name: "otel_latency".into(),
                    description: String::new(),
                    unit: String::new(),
                    data: Some(otlp::metric::Data::ExponentialHistogram(
                        otlp::ExponentialHistogram {
                            data_points: vec![otlp::ExponentialHistogramDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 0,
                                time_unix_nano: ts,
                                count: 12,
                                sum: Some(54.0),
                                scale: 0,
                                zero_count: 0,
                                // OTLP index 0 is (1,2], 1 is (2,4], 2 is (4,8]
                                // — one lower than the Prometheus indices the
                                // v1 fixture uses, for the same boundaries.
                                positive: Some(otlp::exponential_histogram_data_point::Buckets {
                                    offset: 0,
                                    bucket_counts: vec![2, 4, 6],
                                }),
                                negative: None,
                                flags: 0,
                                min: None,
                                max: None,
                                zero_threshold: 0.0,
                            }],
                            aggregation_temporality: 2,
                        },
                    )),
                }],
            }],
        }],
    };

    chronixd::tls::ensure_crypto_provider();
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/otlp/metrics"))
        .header("content-type", "application/x-protobuf")
        .body(req.encode_to_vec())
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "otlp → {}", resp.status());

    assert_eq!(
        one_value(&promql(&base, "histogram_count(otel_latency)").await),
        12.0
    );
    let median = one_value(&promql(&base, "histogram_quantile(0.5, otel_latency)").await);
    assert!(
        (2.0..=4.0).contains(&median),
        "the same distribution as the Prometheus fixture, so the same median \
         bucket — a missing +1 on the OTLP index would put it in (1, 2]: {median}"
    );
}

/// An OTLP explicit-bucket histogram becomes one queryable column.
///
/// It used to become `count`, `sum` and one `bucket_<bound>` field per
/// boundary — data that stored fine and that `histogram_quantile` could not
/// read, which is why a Grafana histogram panel over OTLP data rendered as a
/// summary.
#[tokio::test]
async fn otlp_explicit_buckets_answer_histogram_quantile() {
    let (base, _tmp) = server().await;
    let ts = now_ms() as u64 * 1_000_000;

    let req = otlp::ExportMetricsServiceRequest {
        resource_metrics: vec![otlp::ResourceMetrics {
            resource: None,
            scope_metrics: vec![otlp::ScopeMetrics {
                scope: None,
                metrics: vec![otlp::Metric {
                    name: "otel_classic".into(),
                    description: String::new(),
                    unit: String::new(),
                    data: Some(otlp::metric::Data::Histogram(otlp::Histogram {
                        data_points: vec![otlp::HistogramDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: 0,
                            time_unix_nano: ts,
                            count: 10,
                            sum: Some(12.0),
                            // (-inf,1] (1,2] (2,5] (5,inf)
                            bucket_counts: vec![1, 2, 3, 4],
                            explicit_bounds: vec![1.0, 2.0, 5.0],
                        }],
                        aggregation_temporality: 2,
                    })),
                }],
            }],
        }],
    };

    chronixd::tls::ensure_crypto_provider();
    let resp = reqwest::Client::new()
        .post(format!("{base}/api/v1/otlp/metrics"))
        .header("content-type", "application/x-protobuf")
        .body(req.encode_to_vec())
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "otlp → {}", resp.status());

    assert_eq!(
        one_value(&promql(&base, "histogram_count(otel_classic)").await),
        10.0
    );
    // 1 below 1, then 2 more through 2: the 30th percentile is the 3rd
    // observation, which is in (1, 2].
    let q = one_value(&promql(&base, "histogram_quantile(0.3, otel_classic)").await);
    assert!(
        (1.0..=2.0).contains(&q),
        "q(0.30) over counts 1,2,3,4 with bounds 1,2,5 is in (1, 2]: {q}"
    );
}

/// A staleness marker on the histogram wire is a signal, not a sample.
#[tokio::test]
async fn a_histogram_staleness_marker_is_dropped_rather_than_stored() {
    let (base, _tmp) = server().await;
    let ts = now_ms();

    let mut stale = sample_histogram(ts + 1);
    stale.sum = f64::from_bits(0x7ff0_0000_0000_0002);

    let req = prom_proto::WriteRequest {
        timeseries: vec![prom_proto::TimeSeries {
            labels: vec![prom_proto::Label {
                name: "__name__".into(),
                value: "stale_latency".into(),
            }],
            samples: vec![],
            exemplars: vec![],
            histograms: vec![sample_histogram(ts), stale],
        }],
    };
    let resp = post_proto(
        &base,
        "/api/v1/prom/write",
        Some("application/x-protobuf"),
        req.encode_to_vec(),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NO_CONTENT,
        "a staleness marker must not fail the batch that carries it"
    );

    // The real sample survived; the marker did not become a NaN-sum histogram
    // sitting on top of it.
    let sum = one_value(&promql(&base, "histogram_sum(stale_latency)").await);
    assert_eq!(sum, 54.0, "the marker must not have overwritten the sample");
}

/// A histogram whose spans and counts disagree is refused, by name.
#[tokio::test]
async fn a_malformed_histogram_is_refused_rather_than_half_stored() {
    let (base, _tmp) = server().await;
    let mut bad = sample_histogram(now_ms());
    // Three buckets promised, two counts sent.
    bad.positive_deltas = vec![2, 2];

    let req = prom_proto::WriteRequest {
        timeseries: vec![prom_proto::TimeSeries {
            labels: vec![prom_proto::Label {
                name: "__name__".into(),
                value: "bad_latency".into(),
            }],
            samples: vec![],
            exemplars: vec![],
            histograms: vec![bad],
        }],
    };
    let resp = post_proto(
        &base,
        "/api/v1/prom/write",
        Some("application/x-protobuf"),
        req.encode_to_vec(),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await.unwrap();
    let msg = body["error"].as_str().unwrap_or_default();
    assert!(
        msg.contains("bad_latency"),
        "the refusal must name the metric, or an operator has a 400 and no \
         way to find which series caused it: {msg}"
    );
}

/// A 2.0 label reference past the end of the symbol table is refused.
#[tokio::test]
async fn a_dangling_symbol_reference_is_refused() {
    let (base, _tmp) = server().await;
    let req = prom_proto_v2::Request {
        symbols: vec![String::new(), "__name__".into(), "m".into()],
        timeseries: vec![prom_proto_v2::TimeSeries {
            // 99 is not in a three-entry table.
            label_refs: vec![1, 2, 99, 3],
            samples: vec![prom_proto_v2::Sample {
                value: 1.0,
                timestamp: now_ms(),
            }],
            histograms: vec![],
            exemplars: vec![],
            metadata: None,
            created_timestamp: 0,
        }],
    };
    let resp = post_proto(
        &base,
        "/api/v1/prom/write",
        Some("application/x-protobuf;proto=io.prometheus.write.v2.Request"),
        req.encode_to_vec(),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "a series whose labels cannot be reconstructed must not be stored \
         under the labels that happened to resolve"
    );
}

/// `/series` and `/query` describe the same selector the same way.
///
/// This is the disagreement class this server has a history of: a discovery
/// endpoint that enumerates label sets from tag columns, and an evaluator that
/// invents a label the columns do not hold. The `le` of a classic bucket view
/// is exactly such a label — it is read out of the histogram's boundaries — so
/// `/series?match[]=foo_bucket` would have offered one label set with no `le`
/// while `/query` returned one per boundary, and `le="0.5"` pushed into the
/// scan as a tag filter would have matched nothing at all.
#[tokio::test]
async fn discovery_and_the_evaluator_agree_about_classic_bucket_series() {
    let (base, _tmp) = server().await;
    let ts = now_ms();

    let req = prom_proto::WriteRequest {
        timeseries: vec![prom_proto::TimeSeries {
            labels: vec![
                prom_proto::Label {
                    name: "__name__".into(),
                    value: "disc_latency".into(),
                },
                prom_proto::Label {
                    name: "job".into(),
                    value: "api".into(),
                },
            ],
            samples: vec![],
            exemplars: vec![],
            histograms: vec![sample_histogram(ts)],
        }],
    };
    let resp = post_proto(
        &base,
        "/api/v1/prom/write",
        Some("application/x-protobuf"),
        req.encode_to_vec(),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);

    // What the evaluator produces. `le` carries each bucket's **upper** bound,
    // so the fixture's indices 1, 2 and 3 at schema 0 — covering (1,2], (2,4]
    // and (4,8] — are le=2, le=4 and le=8, with counts accumulating 2, 6, 12.
    let result = promql(&base, "disc_latency_bucket").await;
    let mut from_query: Vec<(String, f64)> = result["result"]
        .as_array()
        .expect("an array")
        .iter()
        .map(|s| {
            (
                s["metric"]["le"].as_str().unwrap_or("<none>").to_string(),
                s["value"][1]
                    .as_str()
                    .unwrap_or("nan")
                    .parse()
                    .unwrap_or(f64::NAN),
            )
        })
        .collect();
    from_query.sort_by(|a, b| a.0.cmp(&b.0));
    let mut expected = vec![
        ("2".to_string(), 2.0),
        ("4".to_string(), 6.0),
        ("8".to_string(), 12.0),
        ("+Inf".to_string(), 12.0),
    ];
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        from_query, expected,
        "`le` is the bucket's upper bound and the counts are cumulative"
    );
    let from_query: std::collections::BTreeSet<String> =
        from_query.into_iter().map(|(le, _)| le).collect();

    // What discovery offers, for the same selector.
    chronixd::tls::ensure_crypto_provider();
    let resp = reqwest::get(format!("{base}/api/v1/series?match[]=disc_latency_bucket"))
        .await
        .unwrap();
    let body: Value = resp.json().await.unwrap();
    let from_series: std::collections::BTreeSet<String> = body["data"]
        .as_array()
        .expect("an array")
        .iter()
        .map(|s| s["le"].as_str().unwrap_or("<none>").to_string())
        .collect();
    assert_eq!(
        from_series, from_query,
        "/series must offer exactly the series /query returns"
    );

    // And an `le` matcher narrows both the same way.
    let one = promql(&base, r#"disc_latency_bucket{le="2"}"#).await;
    assert_eq!(one["result"].as_array().expect("an array").len(), 1);
    assert_eq!(
        one_value(&one),
        2.0,
        "the count at or below 2, not the whole histogram"
    );

    let resp = reqwest::get(format!(
        "{base}/api/v1/series?match[]=disc_latency_bucket%7Ble%3D%222%22%7D"
    ))
    .await
    .unwrap();
    let body: Value = resp.json().await.unwrap();
    assert_eq!(
        body["data"].as_array().expect("an array").len(),
        1,
        "an `le` equality must narrow /series too, not empty it: {body}"
    );
}
