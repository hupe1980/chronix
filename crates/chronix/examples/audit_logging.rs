#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Audit Logging
//!
//! Demonstrates structured audit logging with multi-sink support,
//! queryable audit trail, and event lifecycle tracking.
//!
//! ```bash
//! cargo run --example audit_logging
//! ```

use std::sync::Arc;

use chronix::chronix_security::audit::{
    AuditAction, AuditDecision, AuditEvent, AuditLogger, AuditSink, MemorySink, TracingSink,
};

/// Wrapper to make `Arc<MemorySink>` implement `AuditSink`,
/// so we can share the sink between the logger and our query code.
struct SinkWrapper(Arc<MemorySink>);

impl AuditSink for SinkWrapper {
    fn emit(&self, event: &AuditEvent) -> chronix::chronix_security::audit::error::Result<()> {
        self.0.emit(event)
    }
    fn flush(&self) -> chronix::chronix_security::audit::error::Result<()> {
        self.0.flush()
    }
}

fn main() {
    println!("=== Chronix Audit Logging ===\n");

    // ── 1. Create Logger with Sinks ───────────────────────────────
    println!("--- Setting Up Audit Logger ---");
    let logger = AuditLogger::new();

    // In-memory sink for querying — wrap in Arc so we retain a handle
    let memory_sink = Arc::new(MemorySink::new(1000));
    logger.add_sink(Box::new(SinkWrapper(Arc::clone(&memory_sink))));

    // Tracing sink for structured logging
    logger.add_sink(Box::new(TracingSink));

    println!("Logger configured with {} sinks\n", logger.sink_count());

    // ── 2. Log Various Events ─────────────────────────────────────
    println!("--- Logging Events ---");

    // Successful read
    logger.log(
        AuditEvent::new(
            "alice",
            AuditAction::Read,
            "cpu_metrics",
            AuditDecision::Allow,
        )
        .with_source_ip("192.168.1.10")
        .with_request_id("req-001")
        .with_metadata("query_type", "range")
        .with_metadata("time_range", "last_1h"),
    );
    println!("  Logged: alice READ cpu_metrics → Allow");

    // Successful write
    logger.log(
        AuditEvent::new(
            "bob",
            AuditAction::Write,
            "temperature",
            AuditDecision::Allow,
        )
        .with_source_ip("192.168.1.20")
        .with_request_id("req-002")
        .with_metadata("points", "1000"),
    );
    println!("  Logged: bob WRITE temperature → Allow");

    // Denied delete
    logger.log(
        AuditEvent::new(
            "carol",
            AuditAction::Delete,
            "secret_metrics",
            AuditDecision::Deny,
        )
        .with_source_ip("10.0.0.5")
        .with_request_id("req-003")
        .with_metadata("reason", "insufficient_permissions"),
    );
    println!("  Logged: carol DELETE secret_metrics → Deny");

    // Admin action
    logger.log(
        AuditEvent::new("admin", AuditAction::Admin, "system", AuditDecision::Allow)
            .with_source_ip("127.0.0.1")
            .with_metadata("action_detail", "config_update"),
    );
    println!("  Logged: admin ADMIN system → Allow");

    // Something the built-in categories do not name. `Custom` is the escape
    // hatch, and it is why the enum holds only the categories the server
    // actually emits: a variant nothing constructs reads as coverage.
    logger.log(
        AuditEvent::new(
            "alice",
            AuditAction::Custom("forecast".into()),
            "cpu_metrics",
            AuditDecision::Allow,
        )
        .with_request_id("req-004")
        .with_metadata("model", "ARIMA"),
    );
    println!("  Logged: alice forecast cpu_metrics → Allow");

    // A schema change — permanent, because schema-on-write is additive-only.
    logger.log(
        AuditEvent::new(
            "bob",
            AuditAction::SchemaChange,
            "temperature",
            AuditDecision::Allow,
        )
        .with_request_id("req-005")
        .with_metadata("column", "celsius"),
    );
    println!("  Logged: bob SCHEMA_CHANGE temperature → Allow");

    // A refused credential. There is deliberately no `LoginSuccess`: a
    // database authenticates every request, so recording the successes would
    // make the trail a second request log.
    logger.log(
        AuditEvent::new(
            "eve",
            AuditAction::LoginFailure,
            "auth",
            AuditDecision::Deny,
        )
        .with_source_ip("203.0.113.50")
        .with_metadata("reason", "invalid_credentials"),
    );
    println!("  Logged: eve LOGIN_FAILURE → Deny");

    // A minted key, recorded against the caller who minted it.
    logger.log(
        AuditEvent::new(
            "admin",
            AuditAction::ApiKeyCreate,
            "api_key:ingest",
            AuditDecision::Allow,
        )
        .with_metadata("namespaces", "tenant-a"),
    );
    println!("  Logged: admin API_KEY_CREATE api_key:ingest → Allow");

    // Data leaving the database. `DataExport` is a built-in category, and
    // this said `Custom("DataExport")` beside it — the tell that the built-in
    // one had no producer and nobody had noticed it was there.
    logger.log(
        AuditEvent::new(
            "system",
            AuditAction::DataExport,
            "all_metrics",
            AuditDecision::Allow,
        )
        .with_metadata("format", "parquet")
        .with_metadata("destination", "s3://backup"),
    );
    println!("  Logged: system DATA_EXPORT all_metrics → Allow");

    logger.flush();

    // ── 3. Query the Audit Trail ──────────────────────────────────
    println!("\n--- Querying Audit Trail ---");

    let all_events = memory_sink.events();
    println!("Total events logged: {}", all_events.len());

    // Query by principal
    let alice_events = memory_sink.query_by_principal("alice");
    println!("\nAlice's events ({}):", alice_events.len());
    for event in &alice_events {
        println!(
            "  {:?} on {} → {:?}",
            event.action, event.resource, event.decision
        );
    }

    // Query by decision
    let denied = memory_sink.query_by_decision(AuditDecision::Deny);
    println!("\nDenied events ({}):", denied.len());
    for event in &denied {
        println!(
            "  {} attempted {:?} on {} (IP: {:?})",
            event.principal,
            event.action,
            event.resource,
            event.source_ip.as_deref().unwrap_or("unknown")
        );
    }

    // Query by resource
    let cpu_events = memory_sink.query_by_resource("cpu_metrics");
    println!("\nEvents on cpu_metrics ({}):", cpu_events.len());
    for event in &cpu_events {
        println!(
            "  {} → {:?} → {:?}",
            event.principal, event.action, event.decision
        );
    }

    // ── 4. Sequence Numbers ───────────────────────────────────────
    println!("\n--- Sequence Tracking ---");
    println!("Next sequence number: {}", logger.next_sequence());
    println!("All events have monotonically increasing IDs:");
    for (i, event) in all_events.iter().enumerate().take(5) {
        println!(
            "  Event {i}: id={}, principal={}",
            event.id, event.principal
        );
    }

    // ── 5. Metadata Inspection ────────────────────────────────────
    println!("\n--- Event Metadata ---");
    for event in &all_events {
        if !event.metadata.is_empty() {
            println!(
                "  {} {:?} → metadata: {:?}",
                event.principal, event.action, event.metadata
            );
        }
    }

    println!("\n✓ Audit logging complete");
}
