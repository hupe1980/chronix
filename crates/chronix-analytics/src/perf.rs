//! Performance validation tests for streaming analytics (Stories 5.1 / 3.1 / 3.4).
//!
//! Validates:
//! - Streaming anomaly detection < 10 ms from write to anomaly score
//! - Alert firing < 20 ms from anomaly detection to dispatch

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    use crate::anomaly::DetectorType;
    use chronix_core::FieldValue;

    use crate::alerting::{AlertAction, AlertConfig, AlertEngine};
    use crate::streaming_anomaly::{StreamingAnomalyConfig, StreamingAnomalyEngine};

    // ── Streaming Anomaly Detection Latency ─────────────────────────

    #[test]
    fn anomaly_detection_latency_under_10ms() {
        let engine = StreamingAnomalyEngine::new();

        let config =
            StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 3.0).with_min_fit_points(10);
        engine.enable(config).unwrap();

        let tags: BTreeMap<String, String> =
            [("host".into(), "server1".into())].into_iter().collect();

        // Seed the detector with training data
        for i in 0..50 {
            let mut fields = BTreeMap::new();
            fields.insert("value".into(), FieldValue::F64(100.0 + (i as f64 * 0.1)));
            engine.process_write("cpu", &tags, &fields, i * 1_000_000_000);
        }

        // Measure per-point detection latency
        let iterations = 1_000;
        let mut latencies = Vec::with_capacity(iterations);

        for i in 0..iterations {
            let mut fields = BTreeMap::new();
            fields.insert(
                "value".into(),
                FieldValue::F64(100.0 + (i % 20) as f64 * 0.5),
            );

            let start = Instant::now();
            let _ = engine.process_write("cpu", &tags, &fields, (50 + i as i64) * 1_000_000_000);
            latencies.push(start.elapsed());
        }

        latencies.sort();
        let avg = latencies.iter().sum::<Duration>() / iterations as u32;
        let p99 = latencies[(iterations as f64 * 0.99) as usize];
        let target = Duration::from_millis(10);

        assert!(
            avg < target,
            "Anomaly detection average latency {avg:?} exceeds 10ms target"
        );
        assert!(
            p99 < target,
            "Anomaly detection p99 latency {p99:?} exceeds 10ms target"
        );
    }

    #[test]
    fn anomaly_detection_multiple_series() {
        let engine = StreamingAnomalyEngine::new();

        let config =
            StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 3.0).with_min_fit_points(10);
        engine.enable(config).unwrap();

        // Seed 100 different series
        for host_id in 0..100 {
            let tags: BTreeMap<String, String> = [("host".into(), format!("server{host_id}"))]
                .into_iter()
                .collect();
            for t in 0..20 {
                let mut fields = BTreeMap::new();
                fields.insert(
                    "value".into(),
                    FieldValue::F64(100.0 + host_id as f64 + t as f64 * 0.1),
                );
                engine.process_write("cpu", &tags, &fields, t * 1_000_000_000);
            }
        }

        // Measure detection across all series
        let mut total = Duration::ZERO;
        let events = 100;

        for host_id in 0..events {
            let tags: BTreeMap<String, String> = [("host".into(), format!("server{host_id}"))]
                .into_iter()
                .collect();
            let mut fields = BTreeMap::new();
            fields.insert("value".into(), FieldValue::F64(110.0));

            let start = Instant::now();
            let _ = engine.process_write("cpu", &tags, &fields, 20 * 1_000_000_000);
            total += start.elapsed();
        }

        let avg = total / events as u32;
        assert!(
            avg < Duration::from_millis(10),
            "Multi-series anomaly detection average {avg:?} exceeds 10ms"
        );
    }

    // ── Alert Firing Latency ────────────────────────────────────────

    #[test]
    fn alert_firing_latency_under_20ms() {
        let engine = AlertEngine::new();

        let config = AlertConfig::new("alert1", "cpu", 3.0)
            .with_cooldown(Duration::from_secs(0))
            .with_actions(vec![AlertAction::Log, AlertAction::Metric]);
        engine.register(config);

        let tags: BTreeMap<String, String> =
            [("host".into(), "server1".into())].into_iter().collect();

        // Warm up
        for _ in 0..100 {
            let _ = engine.evaluate("cpu", &tags, 5.0, 1_000_000_000);
        }

        // Measure alert evaluation + firing latency
        let iterations = 1_000;
        let mut latencies = Vec::with_capacity(iterations);

        for i in 0..iterations {
            let start = Instant::now();
            let _ = engine.evaluate("cpu", &tags, 5.0, (i as i64 + 100) * 1_000_000_000);
            latencies.push(start.elapsed());
        }

        latencies.sort();
        let avg = latencies.iter().sum::<Duration>() / iterations as u32;
        let p99 = latencies[(iterations as f64 * 0.99) as usize];
        let target = Duration::from_millis(20);

        assert!(
            avg < target,
            "Alert firing average latency {avg:?} exceeds 20ms target"
        );
        assert!(
            p99 < target,
            "Alert firing p99 latency {p99:?} exceeds 20ms target"
        );
    }

    #[test]
    fn alert_evaluation_no_match_fast() {
        let engine = AlertEngine::new();

        let config = AlertConfig::new("alert1", "cpu", 10.0)
            .with_cooldown(Duration::from_secs(0))
            .with_action(AlertAction::Log);
        engine.register(config);

        let tags: BTreeMap<String, String> =
            [("host".into(), "server1".into())].into_iter().collect();

        // Score below threshold → no alert
        let iterations = 10_000;
        let start = Instant::now();

        for i in 0..iterations {
            let alerts = engine.evaluate("cpu", &tags, 1.0, i * 1_000_000_000);
            assert!(alerts.is_empty());
        }

        let total = start.elapsed();
        let avg = total / iterations as u32;

        assert!(
            avg < Duration::from_millis(1),
            "No-match alert evaluation average {avg:?} exceeds 1ms"
        );
    }

    #[test]
    #[ignore = "wall-clock perf assertion — run with --ignored on an idle machine"]
    fn alert_multiple_configs_performance() {
        // Wall-clock latency assertion — meaningful only on an otherwise
        // idle machine. Kept out of the default run (thermal throttling and
        // parallel test load produce false failures); run explicitly with
        // `cargo test -- --ignored`. Migrating perf assertions to Criterion
        // is in the backlog.
        let engine = AlertEngine::new();

        // Register 50 alert configs on the same measurement
        for i in 0..50 {
            let config = AlertConfig::new(format!("alert_{i}"), "cpu", (i as f64 + 1.0) * 0.5)
                .with_cooldown(Duration::from_secs(0))
                .with_action(AlertAction::Metric);
            engine.register(config);
        }

        let tags: BTreeMap<String, String> =
            [("host".into(), "server1".into())].into_iter().collect();

        let iterations = 1_000;
        let mut latencies = Vec::with_capacity(iterations);

        for i in 0..iterations {
            let start = Instant::now();
            let _ = engine.evaluate("cpu", &tags, 25.0, (i as i64) * 1_000_000_000);
            latencies.push(start.elapsed());
        }

        latencies.sort();
        let p99 = latencies[(iterations as f64 * 0.99) as usize];

        assert!(
            p99 < Duration::from_millis(20),
            "50-config alert p99 {p99:?} exceeds 20ms"
        );
    }

    // ── Throughput ──────────────────────────────────────────────────

    #[test]
    fn anomaly_detection_throughput() {
        let engine = StreamingAnomalyEngine::new();

        let config =
            StreamingAnomalyConfig::new("cpu", DetectorType::ZScore, 3.0).with_min_fit_points(10);
        engine.enable(config).unwrap();

        let tags: BTreeMap<String, String> =
            [("host".into(), "server1".into())].into_iter().collect();

        // Seed
        for i in 0..20 {
            let mut fields = BTreeMap::new();
            fields.insert("value".into(), FieldValue::F64(100.0 + i as f64));
            engine.process_write("cpu", &tags, &fields, i * 1_000_000_000);
        }

        let count = 10_000;
        let start = Instant::now();

        for i in 0..count {
            let mut fields = BTreeMap::new();
            fields.insert("value".into(), FieldValue::F64(100.0 + (i % 10) as f64));
            engine.process_write("cpu", &tags, &fields, (20 + i as i64) * 1_000_000_000);
        }

        let elapsed = start.elapsed();
        let points_per_sec = count as f64 / elapsed.as_secs_f64();

        assert!(
            points_per_sec > 10_000.0,
            "Anomaly throughput {points_per_sec:.0} pts/s is below 10K target"
        );
    }
}
