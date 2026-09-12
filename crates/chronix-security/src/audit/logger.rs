//! Audit logger — captures, stores, and exports audit events.

use std::collections::VecDeque;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use metrics::{counter, gauge};
use parking_lot::RwLock;
use tracing::{debug, error, warn};

use crate::audit::error::{AuditError, Result};
use crate::audit::model::{AuditDecision, AuditEvent};

// ── AuditSink trait ─────────────────────────────────────────────────

/// Pluggable sink for audit event output.
pub trait AuditSink: Send + Sync {
    /// Write an audit event.
    fn emit(&self, event: &AuditEvent) -> Result<()>;

    /// Flush any buffered output.
    fn flush(&self) -> Result<()> {
        Ok(())
    }

    /// Whether this sink is considered durable.
    ///
    /// Durable sinks persist events to stable storage (disk, remote
    /// service, etc.) so they survive process restarts. In-memory
    /// sinks are NOT durable. Production deployments should have at
    /// least one durable sink for compliance.
    fn is_durable(&self) -> bool {
        false
    }
}

// ── MemorySink ──────────────────────────────────────────────────────

/// In-memory audit sink, useful for testing and querying.
pub struct MemorySink {
    events: RwLock<VecDeque<AuditEvent>>,
    max_capacity: usize,
    /// Number of events dropped due to capacity overflow.
    dropped_count: AtomicU64,
}

impl MemorySink {
    /// Create a new memory sink with the given capacity.
    #[must_use]
    pub fn new(max_capacity: usize) -> Self {
        Self {
            events: RwLock::new(VecDeque::with_capacity(max_capacity)),
            max_capacity,
            dropped_count: AtomicU64::new(0),
        }
    }

    /// Number of events that were dropped due to overflow.
    #[must_use]
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }

    /// All stored events.
    #[must_use]
    pub fn events(&self) -> Vec<AuditEvent> {
        self.events.read().iter().cloned().collect()
    }

    /// Number of stored events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.read().len()
    }

    /// Whether the sink is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.read().is_empty()
    }

    /// Query events by principal.
    #[must_use]
    pub fn query_by_principal(&self, principal: &str) -> Vec<AuditEvent> {
        self.events
            .read()
            .iter()
            .filter(|e| e.principal == principal)
            .cloned()
            .collect()
    }

    /// Query events by decision.
    #[must_use]
    pub fn query_by_decision(&self, decision: AuditDecision) -> Vec<AuditEvent> {
        self.events
            .read()
            .iter()
            .filter(|e| e.decision == decision)
            .cloned()
            .collect()
    }

    /// Query events by resource.
    #[must_use]
    pub fn query_by_resource(&self, resource: &str) -> Vec<AuditEvent> {
        self.events
            .read()
            .iter()
            .filter(|e| e.resource == resource)
            .cloned()
            .collect()
    }

    /// Query events within a time range.
    #[must_use]
    pub fn query_by_time_range(&self, min_ts: i64, max_ts: i64) -> Vec<AuditEvent> {
        self.events
            .read()
            .iter()
            .filter(|e| e.timestamp >= min_ts && e.timestamp <= max_ts)
            .cloned()
            .collect()
    }

    /// Drain all events.
    pub fn drain(&self) -> Vec<AuditEvent> {
        let mut q = self.events.write();
        std::mem::take(&mut *q).into()
    }

    /// Fraction of capacity currently used (0.0 – 1.0).
    #[must_use]
    pub fn fill_ratio(&self) -> f64 {
        if self.max_capacity == 0 {
            return 1.0;
        }
        self.events.read().len() as f64 / self.max_capacity as f64
    }
}

impl Default for MemorySink {
    fn default() -> Self {
        Self::new(100_000)
    }
}

impl AuditSink for MemorySink {
    fn emit(&self, event: &AuditEvent) -> Result<()> {
        let mut q = self.events.write();
        if q.len() >= self.max_capacity {
            q.pop_front();
            let dropped = self.dropped_count.fetch_add(1, Ordering::Relaxed) + 1;
            counter!("chronix_audit_memory_sink_dropped_total").increment(1);
            // Warn at 1, 10, 100, 1000, then every 1000 drops to avoid
            // large silent gaps (the old `is_power_of_two()` pattern had a
            // 64K-event gap between the 65 536 and 131 072 warnings).
            if dropped == 1 || dropped == 10 || dropped == 100 || dropped.is_multiple_of(1000) {
                warn!(
                    dropped,
                    capacity = self.max_capacity,
                    "MemorySink overflow: oldest audit events being dropped"
                );
            }
        }
        q.push_back(event.clone());
        let fill = q.len() as f64 / self.max_capacity.max(1) as f64;
        gauge!("chronix_audit_memory_sink_fill_ratio").set(fill);
        Ok(())
    }
}

// ── WriterSink ──────────────────────────────────────────────────────

/// Writes audit events as JSON lines to an `io::Write` destination.
///
/// Suitable for file or stdout output.
pub struct WriterSink<W: Write + Send + Sync> {
    writer: parking_lot::Mutex<W>,
}

impl<W: Write + Send + Sync> WriterSink<W> {
    /// Create a new writer sink.
    pub fn new(writer: W) -> Self {
        Self {
            writer: parking_lot::Mutex::new(writer),
        }
    }
}

impl<W: Write + Send + Sync> AuditSink for WriterSink<W> {
    fn emit(&self, event: &AuditEvent) -> Result<()> {
        let json =
            serde_json::to_string(event).map_err(|e| AuditError::Serialization(e.to_string()))?;
        let mut w = self.writer.lock();
        writeln!(w, "{json}")?;
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        self.writer.lock().flush()?;
        Ok(())
    }

    /// WriterSink is durable — it writes to persistent output (file, etc.).
    fn is_durable(&self) -> bool {
        true
    }
}

// ── FileSink ────────────────────────────────────────────────────────

/// An append-only audit file, one JSON event per line.
///
/// This is the sink that makes an audit log an audit log: `WriterSink` over
/// a `File` writes through the same handle but never calls `sync_data`, so
/// a power loss takes the tail of the log with it — and the tail is where
/// the interesting events are, because an attacker's last act is what
/// crashed the box.
///
/// `sync_each` trades throughput for that guarantee. Leave it on unless
/// the log is also shipped somewhere else synchronously.
pub struct FileSink {
    file: parking_lot::Mutex<std::fs::File>,
    sync_each: bool,
    /// Bytes known to hold whole lines — the rewind point.
    ///
    /// A `writeln!` that fails part-way leaves a line with no terminator, and
    /// the *next* event is then appended to it: one line holding a fragment
    /// and a whole event, which parses as neither. So a failed write loses
    /// the event it failed on and takes the following one with it, which is
    /// the opposite of what a trail is for.
    good_len: std::sync::atomic::AtomicU64,
}

impl FileSink {
    /// Open (or create) an audit log at `path` in append mode.
    ///
    /// # Errors
    ///
    /// Returns an error if the file or its parent directory cannot be
    /// created or opened.
    pub fn open(path: impl AsRef<std::path::Path>, sync_each: bool) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let good_len = file.metadata()?.len();
        Ok(Self {
            file: parking_lot::Mutex::new(file),
            sync_each,
            good_len: std::sync::atomic::AtomicU64::new(good_len),
        })
    }
}

impl AuditSink for FileSink {
    fn emit(&self, event: &AuditEvent) -> Result<()> {
        use std::sync::atomic::Ordering;

        let mut line =
            serde_json::to_vec(event).map_err(|e| AuditError::Serialization(e.to_string()))?;
        line.push(b'\n');

        let mut f = self.file.lock();
        // One `write_all` of the whole line, and a rewind if it does not
        // land: a half-written line swallows the *next* event, because the
        // fragment carries no terminator and the following write continues
        // it. Losing the event that failed is unavoidable; losing the one
        // after it is not.
        match f.write_all(&line) {
            Ok(()) => {}
            Err(e) => {
                let good = self.good_len.load(Ordering::SeqCst);
                if let Err(t) = f.set_len(good) {
                    tracing::error!(error = %t, offset = good, "audit log rewind failed");
                }
                return Err(e.into());
            }
        }
        self.good_len.fetch_add(line.len() as u64, Ordering::SeqCst);
        if self.sync_each {
            f.sync_data()?;
        }
        Ok(())
    }

    fn flush(&self) -> Result<()> {
        let mut f = self.file.lock();
        f.flush()?;
        f.sync_data()?;
        Ok(())
    }

    fn is_durable(&self) -> bool {
        true
    }
}

/// The last event of an audit log, for anchoring a new chain onto it.
///
/// Returns `None` for a missing or empty file — a first start. A trailing
/// partial line (a crash mid-write) is skipped: it is not a sealed event,
/// so the chain continues from the last one that is.
///
/// # Errors
///
/// Returns an error if the file exists but cannot be read.
pub fn last_event(path: impl AsRef<std::path::Path>) -> Result<Option<AuditEvent>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(path)?;
    Ok(content
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<AuditEvent>(line).ok()))
}

/// Read every sealed event from an audit log, oldest first.
///
/// Partial trailing lines are skipped, as in [`last_event`].
///
/// # Errors
///
/// Returns an error if the file cannot be read.
pub fn read_events(path: impl AsRef<std::path::Path>) -> Result<Vec<AuditEvent>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = std::fs::read_to_string(path)?;
    Ok(content
        .lines()
        .filter_map(|line| serde_json::from_str::<AuditEvent>(line).ok())
        .collect())
}

// ── TracingSink ─────────────────────────────────────────────────────

/// Emits audit events as structured tracing events.
pub struct TracingSink;

impl AuditSink for TracingSink {
    fn emit(&self, event: &AuditEvent) -> Result<()> {
        match event.decision {
            AuditDecision::Allow => {
                tracing::info!(
                    target: "chronix::audit",
                    id = event.id,
                    principal = %event.principal,
                    action = %event.action,
                    resource = %event.resource,
                    decision = "allow",
                    source_ip = event.source_ip.as_deref().unwrap_or("-"),
                    request_id = event.request_id.as_deref().unwrap_or("-"),
                    "Audit event"
                );
            }
            AuditDecision::Deny => {
                tracing::warn!(
                    target: "chronix::audit",
                    id = event.id,
                    principal = %event.principal,
                    action = %event.action,
                    resource = %event.resource,
                    decision = "deny",
                    source_ip = event.source_ip.as_deref().unwrap_or("-"),
                    request_id = event.request_id.as_deref().unwrap_or("-"),
                    "Audit event (DENIED)"
                );
            }
        }
        Ok(())
    }
}

// ── AuditLogger ─────────────────────────────────────────────────────

/// The main audit logger, dispatching events to configured sinks.
///
/// Thread-safe and designed for high-throughput operation.
/// Sinks can be added at any time, even after wrapping in `Arc`.
///
/// # Retention Policy
///
/// `AuditLogger` itself does **not** enforce a retention policy — it
/// forwards events to sinks and forgets them.  Retention is the
/// responsibility of each sink:
///
/// - [`MemorySink`]: bounded by `max_capacity` (FIFO eviction).
/// - [`WriterSink`]: appends forever; pair with external log rotation
///   (e.g. `logrotate`, cloud blob lifecycle rules).
/// - [`TracingSink`]: inherits the tracing subscriber's retention.
///
/// For production deployments, configure a log rotation / archival
/// pipeline and monitor the `chronix_audit_events_total` counter to
/// ensure events are not silently dropped.
pub struct AuditLogger {
    sinks: RwLock<Vec<Box<dyn AuditSink>>>,
    sequence: AtomicU64,
    /// SHA-256 hash of the most recently emitted event.
    prev_hash: parking_lot::Mutex<Option<String>>,
    /// Optional HMAC key for tamper-resistant hash chain.
    /// When set, `seal_with_key` is used instead of plain SHA-256.
    hmac_key: Option<Vec<u8>>,
}

impl AuditLogger {
    /// Create a new audit logger with no sinks.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sinks: RwLock::new(Vec::new()),
            sequence: AtomicU64::new(1),
            prev_hash: parking_lot::Mutex::new(None),
            hmac_key: None,
        }
    }

    /// Create an audit logger with an HMAC key.
    ///
    /// When set, each event is sealed with HMAC-SHA-256 instead of
    /// plain SHA-256.  An attacker with database write access cannot
    /// recompute the hash chain without possessing the key.
    #[must_use]
    pub fn with_hmac_key(mut self, key: Vec<u8>) -> Self {
        self.hmac_key = Some(key);
        self
    }

    /// Continue an existing chain instead of starting a new one.
    ///
    /// A restart used to reset the sequence to 1 and the previous hash to
    /// `None`, so every restart began a fresh chain in the same file. A
    /// verifier then could not tell a restart from a truncation: both look
    /// like an event whose `prev_hash` is `None` in the middle of the log.
    /// Anchoring on the last event already on disk makes the file one
    /// chain across the whole life of the deployment.
    #[must_use]
    pub fn resume_from(self, last_id: u64, last_hash: Option<String>) -> Self {
        self.sequence.store(last_id + 1, Ordering::Relaxed);
        *self.prev_hash.lock() = last_hash;
        self
    }

    /// Whether the chain is sealed with a key rather than a bare hash.
    #[must_use]
    pub fn is_keyed(&self) -> bool {
        self.hmac_key.is_some()
    }

    /// Add a sink. Can be called at any time, including after
    /// wrapping in `Arc`.
    pub fn add_sink(&self, sink: Box<dyn AuditSink>) {
        self.sinks.write().push(sink);
    }

    /// **** Validate that at least one durable audit sink is configured.
    ///
    /// Returns `Ok(())` if at least one sink reports `is_durable() == true`.
    /// Returns an error if no durable sinks are present, which means audit
    /// events would be lost on process restart — a compliance violation in
    /// regulated environments.
    ///
    /// Call this at startup after configuring all sinks.
    pub fn validate_has_durable_sink(&self) -> Result<()> {
        let sinks = self.sinks.read();
        let has_durable = sinks.iter().any(|s| s.is_durable());
        if !has_durable {
            warn!(
                "No durable audit sink configured! Audit events will be \
                 lost on process restart. Add a WriterSink (file) or equivalent \
                 persistent sink for compliance."
            );
            return Err(AuditError::NoDurableSink);
        }
        Ok(())
    }

    /// Check if at least one durable sink is configured.
    #[must_use]
    pub fn has_durable_sink(&self) -> bool {
        self.sinks.read().iter().any(|s| s.is_durable())
    }

    /// Log an audit event to all sinks.
    ///
    /// Assigns a monotonic sequence number, seals the event into the hash
    /// chain, and writes it.
    ///
    /// # Sealing and writing are one step
    ///
    /// They were two, with the lock released in between — so two concurrent
    /// callers could seal in one order and write in the other, leaving a file
    /// in which `B` precedes `A` while `B.prev_hash` names `A`. That is
    /// exactly what `verify_hash_chain` reports as **tampering**, and it
    /// happened with no write failing and nothing logged: an ordinary pair of
    /// simultaneous requests made a tamper-evident trail permanently
    /// unverifiable. A trail whose whole purpose is to be checkable cannot
    /// have a benign reason to fail its own check, because then no failure of
    /// it means anything.
    ///
    /// So the chain lock is held across the emission. Audit events are rare
    /// relative to requests, and the file sink serialises on its own mutex
    /// anyway, so the lock is not a new bottleneck — it is the existing one,
    /// now covering the invariant it always should have.
    pub fn log(&self, mut event: AuditEvent) {
        event.id = self.sequence.fetch_add(1, Ordering::Relaxed);

        // Seal the event into the hash chain.
        //
        // With the key, when one is configured: sealing with plain SHA-256
        // here while `log_strict` sealed with HMAC produced a file whose
        // links were computed two different ways, so it verified under
        // neither — and a plain hash is recomputable by anyone who can
        // write the file, which is the whole threat the key exists for.
        let mut prev = self.prev_hash.lock();
        event.seal_with_key(prev.as_deref(), self.hmac_key.as_deref());

        counter!(
            "chronix_audit_events_total",
            "action" => event.action.to_string(),
            "decision" => event.decision.to_string()
        )
        .increment(1);

        let sinks = self.sinks.read();
        let mut durable_failed = false;
        let mut durable_wrote = false;
        for sink in sinks.iter() {
            if let Err(e) = sink.emit(&event) {
                warn!(
                    sink = std::any::type_name_of_val(sink),
                    error = %e,
                    "Failed to emit audit event"
                );
                counter!("chronix_audit_emit_errors_total").increment(1);
                // Track dropped events per-sink so operators can
                // alert on audit pipeline failures. The event is lost for
                // this sink but may succeed on others.
                counter!(
                    "chronix_audit_events_dropped_total",
                    "sink" => std::any::type_name_of_val(sink).to_string(),
                )
                .increment(1);
                if sink.is_durable() {
                    durable_failed = true;
                }
            } else if sink.is_durable() {
                durable_wrote = true;
            }
        }

        // **The chain advances only if the event reached a durable sink.**
        // It advanced unconditionally, so an event the file never received
        // still became the `prev_hash` of the next one — and every later
        // verification of that file failed, reporting a full disk as
        // tampering, for ever. A dropped event is gone either way; what this
        // decides is whether the events *after* it remain checkable.
        if durable_failed && !durable_wrote {
            error!(
                id = event.id,
                action = %event.action,
                "audit event reached no durable sink; it is dropped and the \
                 chain continues from the last one that landed"
            );
            counter!("chronix_audit_chain_gaps_total").increment(1);
        } else {
            *prev = event.event_hash.clone();
        }
        drop(prev);

        debug!(id = event.id, action = %event.action, "Audit event logged");
    }

    /// Current sequence number (next ID to be assigned).
    #[must_use]
    pub fn next_sequence(&self) -> u64 {
        self.sequence.load(Ordering::Relaxed)
    }

    /// Number of sinks.
    #[must_use]
    pub fn sink_count(&self) -> usize {
        self.sinks.read().len()
    }

    /// Flush all sinks.
    pub fn flush(&self) {
        let sinks = self.sinks.read();
        for sink in sinks.iter() {
            if let Err(e) = sink.flush() {
                warn!(error = %e, "Failed to flush audit sink");
            }
        }
    }

    /// Strict audit logging — fail-closed mode for regulated environments.
    ///
    /// Unlike [`log`](Self::log), this attempts **all** sinks even when some
    /// fail, and returns a [`PartialDelivery`](crate::audit::error::AuditError::PartialDelivery)
    /// error if any sink could not emit. This gives the caller full visibility
    /// into which sinks succeeded vs failed so it can make an informed decision
    /// on whether to abort the protected operation.
    pub fn log_strict(&self, mut event: AuditEvent) -> Result<()> {
        event.id = self.sequence.fetch_add(1, Ordering::Relaxed);

        // Seal the event into the hash chain.
        {
            let mut prev = self.prev_hash.lock();
            event.seal_with_key(prev.as_deref(), self.hmac_key.as_deref());
            *prev = event.event_hash.clone();
        }

        counter!(
            "chronix_audit_events_total",
            "action" => event.action.to_string(),
            "decision" => event.decision.to_string()
        )
        .increment(1);

        let sinks = self.sinks.read();
        let total = sinks.len();
        let mut failures: Vec<(String, String)> = Vec::new();

        for sink in sinks.iter() {
            if let Err(e) = sink.emit(&event) {
                counter!("chronix_audit_emit_errors_total").increment(1);
                // Track dropped events per-sink for alerting.
                counter!(
                    "chronix_audit_events_dropped_total",
                    "sink" => std::any::type_name_of_val(sink).to_string(),
                )
                .increment(1);
                let sink_name = std::any::type_name_of_val(sink).to_string();
                warn!(
                    sink = %sink_name,
                    error = %e,
                    "Strict audit emit failed"
                );
                failures.push((sink_name, e.to_string()));
            }
        }

        if failures.is_empty() {
            debug!(id = event.id, action = %event.action, "Audit event logged (strict)");
            Ok(())
        } else {
            let succeeded = total - failures.len();
            Err(AuditError::PartialDelivery {
                succeeded,
                total,
                failures,
            })
        }
    }

    /// Strict flush — tries **all** sinks, reports partial failures.
    ///
    /// Use this together with [`log_strict`](Self::log_strict) for
    /// fail-closed audit contexts where all buffered events must be
    /// durably persisted before acknowledging the protected operation.
    pub fn flush_strict(&self) -> Result<()> {
        let sinks = self.sinks.read();
        let total = sinks.len();
        let mut failures: Vec<(String, String)> = Vec::new();

        for sink in sinks.iter() {
            if let Err(e) = sink.flush() {
                let sink_name = std::any::type_name_of_val(sink).to_string();
                warn!(sink = %sink_name, error = %e, "Strict audit flush failed");
                failures.push((sink_name, e.to_string()));
            }
        }

        if failures.is_empty() {
            Ok(())
        } else {
            let succeeded = total - failures.len();
            Err(AuditError::PartialDelivery {
                succeeded,
                total,
                failures,
            })
        }
    }
}

impl Default for AuditLogger {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::model::{AuditAction, AuditDecision, AuditEvent};
    use std::sync::Arc;

    fn make_event(action: AuditAction, decision: AuditDecision) -> AuditEvent {
        AuditEvent::new("alice", action, "cpu", decision).with_timestamp(1_700_000_000_000)
    }

    /// **Reproduction.** Two threads logging at once must leave a file that
    /// verifies.
    ///
    /// `log()` seals the event under the `prev_hash` lock and then *releases
    /// it* before the sinks write. So two concurrent callers can seal in one
    /// order and write in the other, and the file then holds `B` before `A`
    /// while `B.prev_hash` names `A` — which is precisely what
    /// `verify_hash_chain` reports as tampering. No write fails, nothing is
    /// logged, and the trail whose entire purpose is to be checkable is
    /// permanently uncheckable.
    #[test]
    fn concurrent_events_leave_a_verifiable_chain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");

        let logger = Arc::new(AuditLogger::new());
        logger.add_sink(Box::new(FileSink::open(&path, true).unwrap()));

        const THREADS: usize = 8;
        const PER_THREAD: usize = 25;
        std::thread::scope(|scope| {
            for t in 0..THREADS {
                let logger = Arc::clone(&logger);
                scope.spawn(move || {
                    for i in 0..PER_THREAD {
                        logger.log(
                            AuditEvent::new(
                                format!("worker-{t}"),
                                AuditAction::Read,
                                format!("m{i}"),
                                AuditDecision::Allow,
                            )
                            .with_timestamp(1_700_000_000_000),
                        );
                    }
                });
            }
        });
        logger.flush();

        let events = read_events(&path).unwrap();
        assert_eq!(
            events.len(),
            THREADS * PER_THREAD,
            "every event must reach the file"
        );
        crate::audit::model::verify_hash_chain(&events).expect(
            "a log written by concurrent callers must verify; a chain that does \
             not is indistinguishable from a tampered one",
        );
    }

    /// Lets a test keep a handle on a sink the logger owns.
    struct MemoryHandle(Arc<MemorySink>);

    impl AuditSink for MemoryHandle {
        fn emit(&self, event: &AuditEvent) -> Result<()> {
            self.0.emit(event)
        }
        fn flush(&self) -> Result<()> {
            self.0.flush()
        }
    }

    /// A sink that accepts `fail_after` events and then refuses every one.
    #[derive(Debug)]
    struct BudgetedSink {
        fail_after: std::sync::atomic::AtomicUsize,
        durable: bool,
    }

    impl AuditSink for BudgetedSink {
        fn emit(&self, _event: &AuditEvent) -> Result<()> {
            use std::sync::atomic::Ordering;
            if self.fail_after.load(Ordering::SeqCst) == 0 {
                return Err(AuditError::Serialization("disk full".into()));
            }
            self.fail_after.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
        fn flush(&self) -> Result<()> {
            Ok(())
        }
        fn is_durable(&self) -> bool {
            self.durable
        }
    }

    /// **An event that reaches no durable sink must not advance the chain.**
    ///
    /// It did, so an event the file never received still became the
    /// `prev_hash` of the next one — and every later verification of that
    /// file failed, reporting a full disk as *tampering*, for ever. The
    /// dropped event is gone either way; what this decides is whether the
    /// events after it stay checkable.
    #[test]
    fn a_dropped_event_does_not_break_the_chain_for_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");

        let logger = AuditLogger::new();
        logger.add_sink(Box::new(FileSink::open(&path, true).unwrap()));
        // Fails from the third event on, and it is the only *other* durable
        // sink — so events 3.. reach the file but not this one, which is the
        // partial-failure case the chain must survive.
        let failing = Box::new(BudgetedSink {
            fail_after: std::sync::atomic::AtomicUsize::new(2),
            durable: false,
        });
        logger.add_sink(failing);

        for i in 0..5 {
            logger.log(
                AuditEvent::new(
                    "alice",
                    AuditAction::Read,
                    format!("m{i}"),
                    AuditDecision::Allow,
                )
                .with_timestamp(1_700_000_000_000),
            );
        }
        logger.flush();

        let events = read_events(&path).unwrap();
        assert_eq!(events.len(), 5, "the file sink accepted every event");
        crate::audit::model::verify_hash_chain(&events)
            .expect("a non-durable sink failing must not touch the chain");
    }

    /// The same question when **no** durable sink accepts the event.
    #[test]
    fn the_chain_skips_an_event_no_durable_sink_took() {
        let logger = AuditLogger::new();
        let memory = Arc::new(MemorySink::new(100));
        logger.add_sink(Box::new(MemoryHandle(Arc::clone(&memory))));
        logger.add_sink(Box::new(BudgetedSink {
            fail_after: std::sync::atomic::AtomicUsize::new(2),
            durable: true,
        }));

        for i in 0..5 {
            logger.log(
                AuditEvent::new(
                    "alice",
                    AuditAction::Read,
                    format!("m{i}"),
                    AuditDecision::Allow,
                )
                .with_timestamp(1_700_000_000_000),
            );
        }

        // The memory sink saw all five, but the chain only links the two the
        // durable sink accepted plus the ones after — so verifying the
        // *durable* subset is what must hold. Here: events 0 and 1 landed,
        // 2..5 did not, and each of those left the chain where it was.
        let seen = memory.events();
        assert_eq!(seen.len(), 5);
        assert_eq!(
            seen[2].prev_hash, seen[3].prev_hash,
            "two events that reached no durable sink must hang from the same link"
        );
        assert_eq!(
            seen[1].event_hash.as_deref(),
            seen[2].prev_hash.as_deref(),
            "the first dropped event still links to the last one that landed"
        );
    }

    // ── MemorySink tests ────────

    #[test]
    fn memory_sink_stores_events() {
        let sink = MemorySink::new(100);
        assert!(sink.is_empty());

        let event = make_event(AuditAction::Write, AuditDecision::Allow);
        sink.emit(&event).unwrap();
        assert_eq!(sink.len(), 1);
    }

    #[test]
    fn memory_sink_evicts_on_capacity() {
        let sink = MemorySink::new(2);
        for i in 0..3 {
            let mut event = make_event(AuditAction::Read, AuditDecision::Allow);
            event.timestamp = i;
            sink.emit(&event).unwrap();
        }
        assert_eq!(sink.len(), 2);
        let events = sink.events();
        assert_eq!(events[0].timestamp, 1);
        assert_eq!(events[1].timestamp, 2);
    }

    #[test]
    fn memory_sink_query_by_principal() {
        let sink = MemorySink::new(100);
        sink.emit(&AuditEvent::new(
            "alice",
            AuditAction::Write,
            "cpu",
            AuditDecision::Allow,
        ))
        .unwrap();
        sink.emit(&AuditEvent::new(
            "bob",
            AuditAction::Read,
            "cpu",
            AuditDecision::Deny,
        ))
        .unwrap();
        sink.emit(&AuditEvent::new(
            "alice",
            AuditAction::Delete,
            "logs",
            AuditDecision::Allow,
        ))
        .unwrap();

        let alice = sink.query_by_principal("alice");
        assert_eq!(alice.len(), 2);
        let bob = sink.query_by_principal("bob");
        assert_eq!(bob.len(), 1);
    }

    #[test]
    fn memory_sink_query_by_decision() {
        let sink = MemorySink::new(100);
        sink.emit(&make_event(AuditAction::Write, AuditDecision::Allow))
            .unwrap();
        sink.emit(&make_event(AuditAction::Read, AuditDecision::Deny))
            .unwrap();
        sink.emit(&make_event(AuditAction::Delete, AuditDecision::Deny))
            .unwrap();

        let denied = sink.query_by_decision(AuditDecision::Deny);
        assert_eq!(denied.len(), 2);
    }

    #[test]
    fn memory_sink_query_by_resource() {
        let sink = MemorySink::new(100);
        sink.emit(&AuditEvent::new(
            "a",
            AuditAction::Write,
            "cpu",
            AuditDecision::Allow,
        ))
        .unwrap();
        sink.emit(&AuditEvent::new(
            "b",
            AuditAction::Write,
            "memory",
            AuditDecision::Allow,
        ))
        .unwrap();
        sink.emit(&AuditEvent::new(
            "c",
            AuditAction::Write,
            "cpu",
            AuditDecision::Allow,
        ))
        .unwrap();

        let cpu = sink.query_by_resource("cpu");
        assert_eq!(cpu.len(), 2);
    }

    #[test]
    fn memory_sink_query_by_time_range() {
        let sink = MemorySink::new(100);
        for ts in [100, 200, 300] {
            let mut event = make_event(AuditAction::Read, AuditDecision::Allow);
            event.timestamp = ts;
            sink.emit(&event).unwrap();
        }

        let range = sink.query_by_time_range(150, 250);
        assert_eq!(range.len(), 1);
        assert_eq!(range[0].timestamp, 200);
    }

    #[test]
    fn memory_sink_drain() {
        let sink = MemorySink::new(100);
        sink.emit(&make_event(AuditAction::Write, AuditDecision::Allow))
            .unwrap();
        sink.emit(&make_event(AuditAction::Read, AuditDecision::Allow))
            .unwrap();

        let drained = sink.drain();
        assert_eq!(drained.len(), 2);
        assert!(sink.is_empty());
    }

    // ── WriterSink tests ────────

    #[test]
    fn writer_sink_json_lines() {
        let buf: Vec<u8> = Vec::new();
        let sink = WriterSink::new(buf);

        let event1 = make_event(AuditAction::Write, AuditDecision::Allow);
        let event2 = make_event(AuditAction::Read, AuditDecision::Deny);
        sink.emit(&event1).unwrap();
        sink.emit(&event2).unwrap();
        sink.flush().unwrap();

        let output = sink.writer.lock();
        let text = String::from_utf8(output.clone()).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);

        let parsed: AuditEvent = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed.principal, "alice");
        assert_eq!(parsed.decision, AuditDecision::Allow);

        let parsed2: AuditEvent = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(parsed2.decision, AuditDecision::Deny);
    }

    // ── TracingSink tests ───────

    #[test]
    fn tracing_sink_emits_ok() {
        let sink = TracingSink;
        let event = make_event(AuditAction::Write, AuditDecision::Allow);
        assert!(sink.emit(&event).is_ok());

        let deny = make_event(AuditAction::Delete, AuditDecision::Deny);
        assert!(sink.emit(&deny).is_ok());
    }

    // ── AuditLogger tests ───────

    #[test]
    fn logger_assigns_sequence_numbers() {
        let mem = Arc::new(MemorySink::new(100));
        let logger = AuditLogger::new();
        logger.add_sink(Box::new(SinkWrapper(Arc::clone(&mem))));

        logger.log(make_event(AuditAction::Write, AuditDecision::Allow));
        logger.log(make_event(AuditAction::Read, AuditDecision::Allow));
        logger.log(make_event(AuditAction::Delete, AuditDecision::Deny));

        let events = mem.events();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].id, 1);
        assert_eq!(events[1].id, 2);
        assert_eq!(events[2].id, 3);
        assert_eq!(logger.next_sequence(), 4);
    }

    #[test]
    fn logger_dispatches_to_multiple_sinks() {
        let mem1 = Arc::new(MemorySink::new(100));
        let mem2 = Arc::new(MemorySink::new(100));

        let logger = AuditLogger::new();
        logger.add_sink(Box::new(SinkWrapper(Arc::clone(&mem1))));
        logger.add_sink(Box::new(SinkWrapper(Arc::clone(&mem2))));
        assert_eq!(logger.sink_count(), 2);

        logger.log(make_event(AuditAction::Write, AuditDecision::Allow));
        assert_eq!(mem1.len(), 1);
        assert_eq!(mem2.len(), 1);
    }

    #[test]
    fn logger_all_actions_logged() {
        let mem = Arc::new(MemorySink::new(100));
        let logger = AuditLogger::new();
        logger.add_sink(Box::new(SinkWrapper(Arc::clone(&mem))));

        // Log both allow and deny
        logger.log(make_event(AuditAction::Write, AuditDecision::Allow));
        logger.log(make_event(AuditAction::Write, AuditDecision::Deny));
        logger.log(make_event(AuditAction::LoginFailure, AuditDecision::Allow));
        logger.log(make_event(AuditAction::LoginFailure, AuditDecision::Deny));
        logger.log(make_event(AuditAction::ApiKeyCreate, AuditDecision::Allow));

        assert_eq!(mem.len(), 5);
        let denied = mem.query_by_decision(AuditDecision::Deny);
        assert_eq!(denied.len(), 2);
    }

    #[test]
    fn logger_with_file_sink() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let file = std::fs::File::create(&path).unwrap();

        let logger = AuditLogger::new();
        logger.add_sink(Box::new(WriterSink::new(file)));

        logger.log(
            AuditEvent::new("alice", AuditAction::Write, "cpu", AuditDecision::Allow)
                .with_source_ip("10.0.0.1")
                .with_request_id("req-001")
                .with_timestamp(1_700_000_000_000),
        );
        logger.log(
            AuditEvent::new("bob", AuditAction::Read, "memory", AuditDecision::Deny)
                .with_timestamp(1_700_000_000_001),
        );
        logger.flush();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);

        let e1: AuditEvent = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(e1.principal, "alice");
        assert_eq!(e1.source_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(e1.request_id.as_deref(), Some("req-001"));
        assert_eq!(e1.id, 1);

        let e2: AuditEvent = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(e2.principal, "bob");
        assert_eq!(e2.decision, AuditDecision::Deny);
        assert_eq!(e2.id, 2);
    }

    // ── Helper: AuditSink wrapper for Arc<MemorySink> ─

    /// Wrapper to make Arc<MemorySink> implement AuditSink.
    struct SinkWrapper(Arc<MemorySink>);

    impl AuditSink for SinkWrapper {
        fn emit(&self, event: &AuditEvent) -> Result<()> {
            self.0.emit(event)
        }

        fn flush(&self) -> Result<()> {
            self.0.flush()
        }
    }

    // ── Failing sink for strict-mode tests ─

    /// A sink that always fails on emit.
    struct FailingSink;

    impl AuditSink for FailingSink {
        fn emit(&self, _event: &AuditEvent) -> Result<()> {
            Err(AuditError::Internal("sink unavailable".into()))
        }

        fn flush(&self) -> Result<()> {
            Err(AuditError::Internal("flush failed".into()))
        }
    }

    // ── log_strict / flush_strict tests ─

    #[test]
    fn log_strict_succeeds_with_healthy_sinks() {
        let mem = Arc::new(MemorySink::new(100));
        let logger = AuditLogger::new();
        logger.add_sink(Box::new(SinkWrapper(Arc::clone(&mem))));

        logger
            .log_strict(make_event(AuditAction::Write, AuditDecision::Allow))
            .unwrap();
        assert_eq!(mem.len(), 1);
    }

    #[test]
    fn log_strict_returns_error_on_sink_failure() {
        let logger = AuditLogger::new();
        logger.add_sink(Box::new(FailingSink));

        let result = logger.log_strict(make_event(AuditAction::Write, AuditDecision::Allow));
        assert!(result.is_err());
    }

    #[test]
    fn log_strict_fails_if_any_sink_fails() {
        let mem = Arc::new(MemorySink::new(100));
        let logger = AuditLogger::new();
        logger.add_sink(Box::new(SinkWrapper(Arc::clone(&mem))));
        logger.add_sink(Box::new(FailingSink)); // second sink fails

        let result = logger.log_strict(make_event(AuditAction::Delete, AuditDecision::Deny));
        // All sinks attempted — first sink received the event
        assert_eq!(mem.len(), 1);
        // PartialDelivery reports exactly one success and one failure
        match result {
            Err(AuditError::PartialDelivery {
                succeeded,
                total,
                failures,
            }) => {
                assert_eq!(succeeded, 1);
                assert_eq!(total, 2);
                assert_eq!(failures.len(), 1);
            }
            other => panic!("Expected PartialDelivery, got: {other:?}"),
        }
    }

    #[test]
    fn flush_strict_propagates_failure() {
        let logger = AuditLogger::new();
        logger.add_sink(Box::new(FailingSink));

        let result = logger.flush_strict();
        assert!(result.is_err());
    }

    #[test]
    fn flush_strict_succeeds_with_healthy_sinks() {
        let mem = Arc::new(MemorySink::new(100));
        let logger = AuditLogger::new();
        logger.add_sink(Box::new(SinkWrapper(Arc::clone(&mem))));

        logger.flush_strict().unwrap();
    }

    #[test]
    fn log_best_effort_continues_on_sink_failure() {
        let mem = Arc::new(MemorySink::new(100));
        let logger = AuditLogger::new();
        logger.add_sink(Box::new(FailingSink)); // first sink fails
        logger.add_sink(Box::new(SinkWrapper(Arc::clone(&mem)))); // second succeeds

        // Best-effort log should still emit to the healthy sink
        logger.log(make_event(AuditAction::Write, AuditDecision::Allow));
        assert_eq!(mem.len(), 1);
    }

    // ── Durable sink enforcement ────────────────────────

    #[test]
    fn memory_sink_is_not_durable() {
        let sink = MemorySink::new(100);
        assert!(!sink.is_durable());
    }

    #[test]
    fn writer_sink_is_durable() {
        let sink = WriterSink::new(Vec::<u8>::new());
        assert!(sink.is_durable());
    }

    #[test]
    fn validate_no_durable_sink_fails() {
        let logger = AuditLogger::new();
        logger.add_sink(Box::new(MemorySink::new(100)));
        let result = logger.validate_has_durable_sink();
        assert!(matches!(result, Err(AuditError::NoDurableSink)));
        assert!(!logger.has_durable_sink());
    }

    #[test]
    fn validate_with_durable_sink_succeeds() {
        let logger = AuditLogger::new();
        logger.add_sink(Box::new(MemorySink::new(100)));
        logger.add_sink(Box::new(WriterSink::new(Vec::<u8>::new())));
        logger.validate_has_durable_sink().unwrap();
        assert!(logger.has_durable_sink());
    }

    #[test]
    fn validate_empty_logger_fails() {
        let logger = AuditLogger::new();
        let result = logger.validate_has_durable_sink();
        assert!(matches!(result, Err(AuditError::NoDurableSink)));
    }

    // ── MemorySink overflow observability ─────────────────

    fn make_named_event(principal: &str) -> AuditEvent {
        AuditEvent::new(principal, AuditAction::Write, "cpu", AuditDecision::Allow)
            .with_timestamp(1_700_000_000_000)
    }

    #[test]
    fn overflow_increments_dropped_count() {
        let sink = MemorySink::new(3);
        for i in 0..5 {
            sink.emit(&make_named_event(&format!("user{i}"))).unwrap();
        }
        assert_eq!(sink.dropped_count(), 2);
        assert_eq!(sink.len(), 3);
    }

    #[test]
    fn fill_ratio_reports_correctly() {
        let sink = MemorySink::new(10);
        assert!((sink.fill_ratio() - 0.0).abs() < f64::EPSILON);
        for i in 0..5 {
            sink.emit(&make_named_event(&format!("u{i}"))).unwrap();
        }
        assert!((sink.fill_ratio() - 0.5).abs() < f64::EPSILON);
        for i in 5..10 {
            sink.emit(&make_named_event(&format!("u{i}"))).unwrap();
        }
        assert!((sink.fill_ratio() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn fill_ratio_stays_at_one_when_overflowing() {
        let sink = MemorySink::new(2);
        for i in 0..5 {
            sink.emit(&make_named_event(&format!("u{i}"))).unwrap();
        }
        assert!((sink.fill_ratio() - 1.0).abs() < f64::EPSILON);
        assert_eq!(sink.dropped_count(), 3);
    }

    #[test]
    fn overflow_preserves_newest_events() {
        let sink = MemorySink::new(3);
        for i in 0..6 {
            sink.emit(&make_named_event(&format!("user{i}"))).unwrap();
        }
        let events = sink.events();
        let principals: Vec<&str> = events.iter().map(|e| e.principal.as_str()).collect();
        assert_eq!(principals, vec!["user3", "user4", "user5"]);
    }
}

#[cfg(test)]
mod durability_tests {
    use super::*;
    use crate::audit::model::verify_chain_from;
    use crate::audit::{AuditAction, AuditDecision};

    fn event(principal: &str) -> AuditEvent {
        AuditEvent::new(principal, AuditAction::Delete, "cpu", AuditDecision::Allow)
    }

    #[test]
    fn a_restart_continues_one_chain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");

        {
            let logger = AuditLogger::new();
            logger.add_sink(Box::new(FileSink::open(&path, true).unwrap()));
            logger.log(event("alice"));
            logger.log(event("bob"));
        }

        // Restart: anchor on what is already on disk.
        let last = last_event(&path).unwrap().expect("a previous event");
        assert_eq!(last.id, 2);
        {
            let logger = AuditLogger::new().resume_from(last.id, last.event_hash.clone());
            logger.add_sink(Box::new(FileSink::open(&path, true).unwrap()));
            logger.log(event("carol"));
        }

        let events = read_events(&path).unwrap();
        assert_eq!(events.len(), 3, "every event survives the restart");
        assert_eq!(
            events.iter().map(|e| e.id).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "sequence numbers continue rather than restarting at 1"
        );
        verify_chain_from(&events, None, None)
            .expect("the chain must verify straight through the restart");
    }

    /// Without the key, the chain detects corruption but not tampering:
    /// anyone who can rewrite the file can recompute a plain SHA-256 chain.
    /// `log` used to seal without the key while `log_strict` sealed with it,
    /// so a mixed log verified under neither.
    #[test]
    fn a_keyed_chain_verifies_only_with_its_key() {
        let key = b"seal".to_vec();
        let logger = AuditLogger::new().with_hmac_key(key.clone());
        let handle = std::sync::Arc::new(MemorySink::new(16));
        logger.add_sink(Box::new(SharedMemorySink(handle.clone())));

        logger.log(event("alice"));
        logger.log_strict(event("bob")).unwrap();

        let events = handle.events();
        assert_eq!(events.len(), 2);
        verify_chain_from(&events, None, Some(&key))
            .expect("both `log` and `log_strict` must seal the same way");
        assert!(
            verify_chain_from(&events, None, None).is_err(),
            "a keyed chain must not verify as an unkeyed one"
        );
    }

    /// A sink that shares one buffer with the test.
    struct SharedMemorySink(std::sync::Arc<MemorySink>);

    impl AuditSink for SharedMemorySink {
        fn emit(&self, event: &AuditEvent) -> Result<()> {
            self.0.emit(event)
        }
        fn flush(&self) -> Result<()> {
            self.0.flush()
        }
    }

    /// A crash mid-write leaves a partial line. It is not a sealed event,
    /// so the chain continues from the last one that is, rather than
    /// refusing to start.
    #[test]
    fn a_partial_trailing_line_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.jsonl");
        {
            let logger = AuditLogger::new();
            logger.add_sink(Box::new(FileSink::open(&path, true).unwrap()));
            logger.log(event("alice"));
        }
        {
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            write!(f, "{{\"id\":2,\"principal\":\"tr").unwrap();
        }
        let last = last_event(&path).unwrap().expect("the complete event");
        assert_eq!(last.id, 1);
        assert_eq!(read_events(&path).unwrap().len(), 1);
    }
}
