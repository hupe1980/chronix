//! Performance validation tests for audit logging (Story 8.3).
//!
//! Validates:
//! - Audit event emit latency < 1 ms per event
//! - MemorySink query latency < 10 ms for 10k events
//! - Multi-sink dispatch overhead acceptable
//! - Concurrent audit logging is safe

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::audit::model::{AuditAction, AuditDecision, AuditEvent};
    use crate::audit::{AuditLogger, AuditSink, MemorySink, TracingSink};

    // In debug mode tests run ~50x slower; adjust thresholds.
    #[cfg(debug_assertions)]
    const LATENCY_MULTIPLIER: u64 = 50;
    #[cfg(not(debug_assertions))]
    const LATENCY_MULTIPLIER: u64 = 1;

    fn make_event(i: usize) -> AuditEvent {
        AuditEvent::new(
            format!("user_{}", i % 100),
            AuditAction::Write,
            format!("measurement_{}", i % 10),
            if i.is_multiple_of(3) {
                AuditDecision::Deny
            } else {
                AuditDecision::Allow
            },
        )
        .with_source_ip("127.0.0.1")
        .with_request_id(format!("req-{i}"))
    }

    /// Wrapper to make `Arc<MemorySink>` implement `AuditSink`.
    struct SinkWrapper(Arc<MemorySink>);

    impl AuditSink for SinkWrapper {
        fn emit(&self, event: &AuditEvent) -> crate::audit::error::Result<()> {
            self.0.emit(event)
        }
        fn flush(&self) -> crate::audit::error::Result<()> {
            self.0.flush()
        }
    }

    // ── Emit latency ────────────────────────────────────────────

    #[test]
    fn audit_emit_latency_under_1ms() {
        let sink = Arc::new(MemorySink::new(100_000));
        let logger = AuditLogger::new();
        logger.add_sink(Box::new(SinkWrapper(Arc::clone(&sink))));

        // Warm up
        for i in 0..100 {
            logger.log(make_event(i));
        }

        let iterations = 5_000;
        let start = Instant::now();
        for i in 100..100 + iterations {
            logger.log(make_event(i));
        }
        let elapsed = start.elapsed();
        let per_event = elapsed / iterations as u32;

        let max = Duration::from_micros(1_000 * LATENCY_MULTIPLIER);
        assert!(
            per_event < max,
            "Audit emit took {per_event:?}/event, max {max:?}"
        );
    }

    // ── MemorySink query performance ────────────────────────────

    #[test]
    fn memory_sink_query_under_10ms() {
        let sink = MemorySink::new(20_000);

        // Fill with 10k events
        for i in 0..10_000 {
            sink.emit(&make_event(i)).unwrap();
        }

        // Query by principal
        let start = Instant::now();
        let results = sink.query_by_principal("user_42");
        let elapsed = start.elapsed();
        assert!(!results.is_empty());
        let max = Duration::from_millis(10 * LATENCY_MULTIPLIER);
        assert!(
            elapsed < max,
            "Query by principal took {elapsed:?}, max {max:?}"
        );

        // Query by decision
        let start = Instant::now();
        let results = sink.query_by_decision(AuditDecision::Deny);
        let elapsed = start.elapsed();
        assert!(!results.is_empty());
        assert!(
            elapsed < max,
            "Query by decision took {elapsed:?}, max {max:?}"
        );
    }

    // ── Multi-sink dispatch ─────────────────────────────────────

    #[test]
    fn multi_sink_dispatch_overhead() {
        let mem1 = Arc::new(MemorySink::new(10_000));
        let mem2 = Arc::new(MemorySink::new(10_000));

        let logger = AuditLogger::new();
        logger.add_sink(Box::new(SinkWrapper(Arc::clone(&mem1))));
        logger.add_sink(Box::new(SinkWrapper(Arc::clone(&mem2))));
        logger.add_sink(Box::new(TracingSink));

        let iterations = 2_000;
        let start = Instant::now();
        for i in 0..iterations {
            logger.log(make_event(i));
        }
        let elapsed = start.elapsed();
        let per_event = elapsed / iterations as u32;

        // Even with 3 sinks, should be under 2ms per event
        let max = Duration::from_micros(2_000 * LATENCY_MULTIPLIER);
        assert!(
            per_event < max,
            "Multi-sink emit took {per_event:?}/event, max {max:?}"
        );

        // Both memory sinks should have all events
        assert_eq!(mem1.events().len(), iterations);
        assert_eq!(mem2.events().len(), iterations);
    }

    // ── Concurrent logging ──────────────────────────────────────

    #[test]
    fn concurrent_audit_logging() {
        let sink = Arc::new(MemorySink::new(100_000));
        let logger = AuditLogger::new();
        logger.add_sink(Box::new(SinkWrapper(Arc::clone(&sink))));
        let logger = Arc::new(logger);

        let threads: Vec<_> = (0..4)
            .map(|t| {
                let logger = Arc::clone(&logger);
                std::thread::spawn(move || {
                    for i in 0..1_000 {
                        logger.log(make_event(t * 1_000 + i));
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(sink.events().len(), 4_000);
    }

    // ── Sequence number monotonicity ────────────────────────────

    #[test]
    fn sequence_numbers_unique_under_concurrency() {
        let sink = Arc::new(MemorySink::new(10_000));
        let logger = AuditLogger::new();
        logger.add_sink(Box::new(SinkWrapper(Arc::clone(&sink))));
        let logger = Arc::new(logger);

        let threads: Vec<_> = (0..4)
            .map(|t| {
                let logger = Arc::clone(&logger);
                std::thread::spawn(move || {
                    for i in 0..500 {
                        logger.log(make_event(t * 500 + i));
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        let events = sink.events();
        assert_eq!(events.len(), 2_000);

        // All IDs should be unique (assigned by AtomicU64)
        let mut ids: Vec<u64> = events.iter().map(|e| e.id).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 2_000, "All event IDs should be unique");
    }
}
