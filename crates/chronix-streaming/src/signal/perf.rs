//! Performance validation tests for the signal system (Stories 5.1 / 8.2).
//!
//! Validates:
//! - Trigger evaluation < 1 ms per point (amortized)
//! - 100 concurrent triggers on same measurement: evaluation < 10 ms per point
//! - Signal SQL parse < 1 ms

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    use crate::cdc::CdcEvent;
    use chronix_core::FieldValue;

    use crate::signal::engine::{
        anomaly_threshold_trigger, ma_crossover_trigger, rate_of_change_trigger, TriggerEngine,
    };
    use crate::signal::model::{EventTrigger, ThresholdOp, TriggerCondition};
    use crate::signal::sql::parse_trigger_sql;

    // ── Single Trigger Evaluation ───────────────────────────────────

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn trigger_evaluation_under_1ms_per_point() {
        let engine = TriggerEngine::new();

        let trigger = anomaly_threshold_trigger("t1", "anomaly_alert", "cpu", 3.0);
        engine.register(trigger).unwrap();

        let mut tags = BTreeMap::new();
        tags.insert("host".into(), "server1".into());
        let mut fields = BTreeMap::new();
        fields.insert("value".into(), FieldValue::F64(42.0));

        // Warm up
        for i in 0..100 {
            let event = CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: i * 1_000_000_000,
                seq: i as u64,
            };
            engine.process_event(&event);
        }

        // Measure
        let iterations = 5_000;
        let mut latencies = Vec::with_capacity(iterations);

        for i in 0..iterations {
            let event = CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: (100 + i as i64) * 1_000_000_000,
                seq: (100 + i) as u64,
            };

            let start = Instant::now();
            let _ = engine.process_event(&event);
            latencies.push(start.elapsed());
        }

        latencies.sort();
        let avg = latencies.iter().sum::<Duration>() / iterations as u32;
        let p99 = latencies[(iterations as f64 * 0.99) as usize];
        let target = Duration::from_millis(1);

        assert!(
            avg < target,
            "Single trigger average eval latency {avg:?} exceeds 1ms"
        );
        assert!(
            p99 < target,
            "Single trigger p99 eval latency {p99:?} exceeds 1ms"
        );
    }

    // ── 100 Concurrent Triggers ─────────────────────────────────────

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn hundred_triggers_under_10ms_per_point() {
        let engine = TriggerEngine::new();

        // Register 100 triggers on the same measurement
        for i in 0..100 {
            let trigger = EventTrigger::new(
                format!("trigger_{i}"),
                format!("Alert {i}"),
                "cpu",
                TriggerCondition::FieldThreshold {
                    field: "value".into(),
                    op: ThresholdOp::Gt,
                    value: 90.0 + f64::from(i),
                },
            );
            engine.register(trigger).unwrap();
        }

        let mut tags = BTreeMap::new();
        tags.insert("host".into(), "server1".into());
        let mut fields = BTreeMap::new();
        fields.insert("value".into(), FieldValue::F64(200.0));

        // Warm up
        for i in 0..50 {
            let event = CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: i * 1_000_000_000,
                seq: i as u64,
            };
            engine.process_event(&event);
        }

        // Drain signals from warmup
        engine.drain_signals();

        // Measure evaluation of a single point across 100 triggers
        let iterations = 1_000;
        let mut latencies = Vec::with_capacity(iterations);

        for i in 0..iterations {
            let event = CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: (50 + i as i64) * 1_000_000_000,
                seq: (50 + i) as u64,
            };

            let start = Instant::now();
            let _ = engine.process_event(&event);
            latencies.push(start.elapsed());
        }

        latencies.sort();
        let avg = latencies.iter().sum::<Duration>() / iterations as u32;
        let p99 = latencies[(iterations as f64 * 0.99) as usize];
        let target = Duration::from_millis(10);

        assert!(
            avg < target,
            "100-trigger average eval latency {avg:?} exceeds 10ms"
        );
        assert!(
            p99 < target,
            "100-trigger p99 eval latency {p99:?} exceeds 10ms"
        );
    }

    // ── MA Crossover Trigger ────────────────────────────────────────

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn ma_crossover_evaluation_fast() {
        let engine = TriggerEngine::new();

        let trigger = ma_crossover_trigger("ma1", "MA Cross", "cpu", 5, 20);
        engine.register(trigger).unwrap();

        let tags: BTreeMap<String, String> =
            [("host".into(), "server1".into())].into_iter().collect();
        let mut fields = BTreeMap::new();
        fields.insert("value".into(), FieldValue::F64(100.0));

        let iterations = 5_000;
        let start = Instant::now();

        for i in 0..iterations {
            fields.insert(
                "value".into(),
                FieldValue::F64(100.0 + (i as f64 * 0.1).sin() * 10.0),
            );
            let event = CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: i * 1_000_000_000,
                seq: i as u64,
            };
            engine.process_event(&event);
        }

        let elapsed = start.elapsed();
        let per_point = elapsed / iterations as u32;

        assert!(
            per_point < Duration::from_millis(1),
            "MA crossover per-point latency {per_point:?} exceeds 1ms"
        );
    }

    // ── Rate of Change Trigger ──────────────────────────────────────

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn rate_of_change_evaluation_fast() {
        let engine = TriggerEngine::new();

        let trigger = rate_of_change_trigger("roc1", "RoC Alert", "cpu", 10.0, 5);
        engine.register(trigger).unwrap();

        let tags: BTreeMap<String, String> =
            [("host".into(), "server1".into())].into_iter().collect();

        let iterations = 5_000;
        let start = Instant::now();

        for i in 0..iterations {
            let mut fields = BTreeMap::new();
            fields.insert("value".into(), FieldValue::F64(100.0 + (i % 50) as f64));
            let event = CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: i * 1_000_000_000,
                seq: i as u64,
            };
            engine.process_event(&event);
        }

        let elapsed = start.elapsed();
        let per_point = elapsed / iterations as u32;

        assert!(
            per_point < Duration::from_millis(1),
            "Rate-of-change per-point latency {per_point:?} exceeds 1ms"
        );
    }

    // ── SQL Parse Performance ───────────────────────────────────────

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn sql_parse_under_1ms() {
        let sql =
            "CREATE TRIGGER alert_cpu ON cpu_usage WHEN anomaly_score > 3.0 DELIVER webhook('https://example.com/hook') COOLDOWN INTERVAL '5m'";

        // Warm up
        for _ in 0..100 {
            let _ = parse_trigger_sql(sql).unwrap();
        }

        let iterations = 10_000;
        let mut latencies = Vec::with_capacity(iterations);

        for _ in 0..iterations {
            let start = Instant::now();
            let _ = parse_trigger_sql(sql).unwrap();
            latencies.push(start.elapsed());
        }

        latencies.sort();
        let avg = latencies.iter().sum::<Duration>() / iterations as u32;
        let p99 = latencies[(iterations as f64 * 0.99) as usize];

        assert!(
            avg < Duration::from_millis(1),
            "SQL parse average {avg:?} exceeds 1ms"
        );
        assert!(
            p99 < Duration::from_millis(1),
            "SQL parse p99 {p99:?} exceeds 1ms"
        );
    }

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn sql_parse_show_drop_fast() {
        let sqls = [
            "SHOW TRIGGERS",
            "DROP TRIGGER alert_cpu",
            "ALTER TRIGGER alert_cpu ENABLE",
            "ALTER TRIGGER alert_cpu DISABLE",
        ];

        for sql in &sqls {
            let iterations = 10_000;
            let start = Instant::now();
            for _ in 0..iterations {
                let _ = parse_trigger_sql(sql).unwrap();
            }
            let avg = start.elapsed() / iterations as u32;

            assert!(
                avg < Duration::from_micros(100),
                "Parse '{sql}' average {avg:?} exceeds 100µs"
            );
        }
    }

    // ── Throughput ──────────────────────────────────────────────────

    #[test]
    #[ignore = "perf benchmark — run with --ignored"]
    fn trigger_throughput_100k_events() {
        let engine = TriggerEngine::new();

        let trigger = EventTrigger::new(
            "t1",
            "alert",
            "cpu",
            TriggerCondition::FieldThreshold {
                field: "value".into(),
                op: ThresholdOp::Gt,
                value: 999.0, // won't fire → pure evaluation cost
            },
        );
        engine.register(trigger).unwrap();

        let tags: BTreeMap<String, String> =
            [("host".into(), "server1".into())].into_iter().collect();

        let count = 100_000;
        let start = Instant::now();

        for i in 0..count {
            let mut fields = BTreeMap::new();
            fields.insert("value".into(), FieldValue::F64(42.0));
            let event = CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags: tags.clone(),
                fields,
                timestamp: i * 1_000_000_000,
                seq: i as u64,
            };
            engine.process_event(&event);
        }

        let elapsed = start.elapsed();
        let events_per_sec = count as f64 / elapsed.as_secs_f64();

        assert!(
            events_per_sec > 100_000.0,
            "Trigger throughput {events_per_sec:.0} events/s below 100K target"
        );
    }
}
