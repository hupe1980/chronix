//! Performance validation tests for CDC streaming (Story 5.1).
//!
//! Validates:
//! - CDC event delivery < 5 ms from publish to subscriber
//! - Continuous aggregation < 50 ms from write to aggregated row

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};

    use chronix_core::FieldValue;

    use crate::cdc::bus::EventBus;
    use crate::cdc::event::CdcEvent;

    // ── CDC Event Delivery Latency ──────────────────────────────────

    #[tokio::test]
    async fn cdc_delivery_latency_under_5ms() {
        let bus = EventBus::with_default_capacity();
        let mut sub = bus.subscribe();

        let mut tags = BTreeMap::new();
        tags.insert("host".into(), "server1".into());
        let mut fields = BTreeMap::new();
        fields.insert("value".into(), FieldValue::F64(42.0));

        // Warm up
        for seq in 0..100 {
            let event = CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: seq,
                seq: seq as u64,
            };
            bus.publish(event);
            let _ = sub.recv().await;
        }

        // Measure delivery latency over 1000 events
        let iterations = 1000;
        let mut total = Duration::ZERO;
        let mut max_latency = Duration::ZERO;

        for i in 0..iterations {
            let event = CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: 1000 + i,
                seq: 100 + i as u64,
            };

            let start = Instant::now();
            bus.publish(event);
            let _ = sub.recv().await.unwrap();
            let elapsed = start.elapsed();

            total += elapsed;
            if elapsed > max_latency {
                max_latency = elapsed;
            }
        }

        let avg = total / iterations as u32;
        let target = Duration::from_millis(5);

        assert!(
            avg < target,
            "CDC average delivery latency {avg:?} exceeds 5ms target"
        );
    }

    #[tokio::test]
    async fn cdc_delivery_p99_under_5ms() {
        let bus = EventBus::with_default_capacity();
        let mut sub = bus.subscribe();

        let mut tags = BTreeMap::new();
        tags.insert("host".into(), "server1".into());
        let mut fields = BTreeMap::new();
        fields.insert("value".into(), FieldValue::F64(42.0));

        let iterations = 1000;
        let mut latencies = Vec::with_capacity(iterations);

        for i in 0..iterations {
            let event = CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: i as i64,
                seq: i as u64,
            };

            let start = Instant::now();
            bus.publish(event);
            let _ = sub.recv().await.unwrap();
            latencies.push(start.elapsed());
        }

        latencies.sort();
        let p99 = latencies[(iterations as f64 * 0.99) as usize];
        let target = Duration::from_millis(5);

        assert!(
            p99 < target,
            "CDC p99 delivery latency {p99:?} exceeds 5ms target"
        );
    }

    // ── Continuous Aggregation Latency ──────────────────────────────

    #[test]
    fn aggregation_latency_under_50ms() {
        use crate::cdc::aggregation::{
            AggFunction, ContinuousAggregationConfig, ContinuousAggregationEngine,
        };

        let config = ContinuousAggregationConfig {
            name: "agg_cpu".into(),
            source_measurement: "cpu".into(),
            target_measurement: "cpu_1m".into(),
            source_field: "value".into(),
            interval: Duration::from_secs(60),
            functions: vec![AggFunction::Sum, AggFunction::Count, AggFunction::Mean],
            late_arrival_window: Duration::from_secs(5),
        };

        let mut engine = ContinuousAggregationEngine::new(config).unwrap();

        let mut tags = BTreeMap::new();
        tags.insert("host".into(), "server1".into());
        let mut fields = BTreeMap::new();
        fields.insert("value".into(), FieldValue::F64(42.0));

        // Process 120 events (2 minutes of 1-second data)
        let iterations = 120;
        let mut total = Duration::ZERO;
        let mut max_latency = Duration::ZERO;

        for i in 0..iterations {
            let event = CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: i * 1_000_000_000, // 1-second intervals in nanoseconds
                seq: i as u64,
            };

            let start = Instant::now();
            let _results = engine.process_event(&event);
            let elapsed = start.elapsed();

            total += elapsed;
            if elapsed > max_latency {
                max_latency = elapsed;
            }
        }

        let avg = total / iterations as u32;
        let target = Duration::from_millis(50);

        assert!(
            avg < target,
            "Aggregation average latency {avg:?} exceeds 50ms target"
        );
        assert!(
            max_latency < target,
            "Aggregation max latency {max_latency:?} exceeds 50ms target"
        );
    }

    // ── Throughput ──────────────────────────────────────────────────

    #[tokio::test]
    async fn cdc_throughput_10k_events_per_second() {
        let bus = EventBus::with_default_capacity();
        let mut sub = bus.subscribe();

        let mut tags = BTreeMap::new();
        tags.insert("host".into(), "server1".into());
        let mut fields = BTreeMap::new();
        fields.insert("value".into(), FieldValue::F64(42.0));

        let count = 10_000;
        let start = Instant::now();

        for i in 0..count {
            let event = CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags: tags.clone(),
                fields: fields.clone(),
                timestamp: i,
                seq: i as u64,
            };
            bus.publish(event);
        }

        for _ in 0..count {
            let _ = sub.recv().await.unwrap();
        }

        let elapsed = start.elapsed();
        let events_per_sec = count as f64 / elapsed.as_secs_f64();

        assert!(
            events_per_sec > 10_000.0,
            "CDC throughput {events_per_sec:.0} events/s is below 10K target"
        );
    }

    #[test]
    fn aggregation_handles_high_cardinality() {
        use crate::cdc::aggregation::{
            AggFunction, ContinuousAggregationConfig, ContinuousAggregationEngine,
        };

        let config = ContinuousAggregationConfig {
            name: "agg_cpu".into(),
            source_measurement: "cpu".into(),
            target_measurement: "cpu_1m".into(),
            source_field: "value".into(),
            interval: Duration::from_secs(60),
            functions: vec![AggFunction::Sum, AggFunction::Mean],
            late_arrival_window: Duration::from_secs(5),
        };

        let mut engine = ContinuousAggregationEngine::new(config).unwrap();

        // 100 unique tag combinations
        let start = Instant::now();
        for host_id in 0..100 {
            let mut tags = BTreeMap::new();
            tags.insert("host".into(), format!("server{host_id}"));
            let mut fields = BTreeMap::new();
            fields.insert("value".into(), FieldValue::F64(42.0 + host_id as f64));

            let event = CdcEvent::PointWritten {
                measurement: "cpu".into(),
                tags,
                fields,
                timestamp: 0,
                seq: host_id as u64,
            };
            engine.process_event(&event);
        }
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(50),
            "100-cardinality aggregation took {elapsed:?}, exceeds 50ms"
        );
    }
}
