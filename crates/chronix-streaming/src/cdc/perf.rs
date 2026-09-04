//! Performance validation tests for CDC streaming.
//!
//! Validates:
//! - CDC event delivery < 5 ms from publish to subscriber

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
}
