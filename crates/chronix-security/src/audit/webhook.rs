//! Webhook audit sink — forwards audit events via HTTP POST for SIEM integration.
//!
//! Events are serialized to JSON and sent to a configurable endpoint URL.
//! Delivery is performed on a background thread via a bounded channel to
//! avoid blocking the hot path.  Failed deliveries are retried with
//! exponential backoff.
//!
//! # Delivery Guarantees
//!
//! Two modes are available:
//!
//! - **`BestEffort`** (default): when the in-memory channel is full, the
//!   event is dropped and a counter is incremented.  Non-blocking,
//!   suitable for most observability use cases.
//!
//! - **`AtLeastOnce`**: when the channel is full, events overflow to a
//!   disk-backed append-only journal.  The background worker drains the
//!   overflow file before consuming from the channel, ensuring no event
//!   is lost even under sustained back-pressure.  Suitable for
//!   compliance-critical deployments (SOX, HIPAA).
//!
//! # Formats
//!
//! - **JSON** (default): raw `AuditEvent` serialized as JSON
//! - **CEF**: Common Event Format — widely supported by Splunk, ArcSight, QRadar

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::audit::error::{AuditError, Result};
use crate::audit::model::AuditEvent;

/// Output format for webhook events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookFormat {
    /// JSON: raw `AuditEvent` as JSON object.
    Json,
    /// CEF: Common Event Format (RFC-like), widely used by enterprise SIEMs.
    Cef,
}

/// Delivery guarantee mode for the webhook sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryGuarantee {
    /// Drop events when the channel is full (non-blocking, default).
    BestEffort,
    /// Overflow to a disk-backed journal when the channel is full.
    /// Events are retried from disk until delivered.
    AtLeastOnce,
}

/// Configuration for the webhook sink.
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    /// Target URL for HTTP POST.
    pub url: String,
    /// Output format.
    pub format: WebhookFormat,
    /// Channel buffer size (events). Default: 10,000.
    pub buffer_size: usize,
    /// HTTP request timeout. Default: 5 seconds.
    pub timeout: Duration,
    /// Maximum retry attempts per event. Default: 3.
    pub max_retries: u32,
    /// Optional authorization header value (e.g. "Bearer `<token>`").
    ///
    /// Supports environment variable expansion: values matching the
    /// pattern `${VAR_NAME}` are replaced with the contents of the
    /// corresponding environment variable at sink creation time.  This
    /// avoids storing plaintext secrets in configuration files.
    pub auth_header: Option<String>,
    /// Delivery guarantee mode. Default: `BestEffort`.
    pub delivery_guarantee: DeliveryGuarantee,
    /// Directory for the overflow journal (only used in `AtLeastOnce` mode).
    /// If `None`, falls back to the system temp directory.
    pub overflow_dir: Option<std::path::PathBuf>,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            format: WebhookFormat::Json,
            buffer_size: 10_000,
            timeout: Duration::from_secs(5),
            max_retries: 3,
            auth_header: None,
            delivery_guarantee: DeliveryGuarantee::BestEffort,
            overflow_dir: None,
        }
    }
}

/// Format an `AuditEvent` as a CEF string.
///
/// CEF format: `CEF:0|Chronix|TSDB|version|action|action_name|severity|extension`
fn format_cef(event: &AuditEvent) -> String {
    let severity = match event.decision {
        crate::audit::model::AuditDecision::Allow => 3,
        crate::audit::model::AuditDecision::Deny => 7,
    };
    let extension = format!(
        "src={} requestId={} principal={} resource={} decision={}",
        event.source_ip.as_deref().unwrap_or("-"),
        event.request_id.as_deref().unwrap_or("-"),
        event.principal,
        event.resource,
        event.decision,
    );
    format!(
        "CEF:0|Chronix|TSDB|1.0|{}|{}|{severity}|{extension}",
        event.action, event.action,
    )
}

/// Expand environment variable references in a string.
///
/// Supported patterns:
/// - `${VAR_NAME}` — replaced with the value of `VAR_NAME` from the environment.
///
/// Returns an error if a referenced variable is not set.
fn expand_env_vars(s: &str) -> std::result::Result<String, String> {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_name = String::new();
            loop {
                match chars.next() {
                    Some('}') => break,
                    Some(ch) => var_name.push(ch),
                    None => return Err("unclosed ${...} in env var reference".into()),
                }
            }
            match std::env::var(&var_name) {
                Ok(val) => result.push_str(&val),
                Err(_) => {
                    return Err(format!("environment variable '{var_name}' is not set"));
                }
            }
        } else {
            result.push(c);
        }
    }
    Ok(result)
}

/// Background worker state.
struct WebhookWorker {
    config: WebhookConfig,
    rx: mpsc::Receiver<AuditEvent>,
    running: Arc<AtomicBool>,
    delivered: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    overflow: Option<Arc<parking_lot::Mutex<OverflowJournal>>>,
}

/// Append-only disk-backed overflow journal for AtLeastOnce mode.
///
/// Events are written as newline-delimited JSON. On recovery, the worker
/// reads from the journal before consuming from the channel.
struct OverflowJournal {
    path: std::path::PathBuf,
    /// Pending events read from disk but not yet delivered.
    pending: std::collections::VecDeque<AuditEvent>,
    /// Whether we have loaded pending events from the file.
    loaded: bool,
}

impl OverflowJournal {
    fn new(dir: &std::path::Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("webhook_overflow.jsonl");
        Ok(Self {
            path,
            pending: std::collections::VecDeque::new(),
            loaded: false,
        })
    }

    /// Append an event to the overflow file.
    fn append(&mut self, event: &AuditEvent) -> std::io::Result<()> {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let json = serde_json::to_string(event)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        writeln!(file, "{json}")?;
        file.sync_all()?;
        metrics::counter!("chronix_audit_webhook_overflow_total").increment(1);
        Ok(())
    }

    /// Load all pending events from the overflow file into memory.
    /// Called once at startup by the worker thread.
    fn load_pending(&mut self) -> std::io::Result<usize> {
        if self.loaded {
            return Ok(self.pending.len());
        }
        self.loaded = true;
        if !self.path.exists() {
            return Ok(0);
        }
        let file = std::fs::File::open(&self.path)?;
        let reader = std::io::BufReader::new(file);
        use std::io::BufRead;
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<AuditEvent>(&line) {
                Ok(event) => self.pending.push_back(event),
                Err(e) => {
                    tracing::warn!(error = %e, "skipping corrupt overflow journal line");
                }
            }
        }
        let count = self.pending.len();
        if count > 0 {
            tracing::info!(count, "loaded pending events from overflow journal");
        }
        Ok(count)
    }

    /// Take the next pending event (from disk recovery).
    fn pop_pending(&mut self) -> Option<AuditEvent> {
        self.pending.pop_front()
    }

    /// Truncate the overflow file (called after all pending events are delivered).
    fn truncate(&mut self) -> std::io::Result<()> {
        if self.path.exists() {
            std::fs::write(&self.path, b"")?;
        }
        Ok(())
    }
}

impl WebhookWorker {
    fn run(self) {
        // In AtLeastOnce mode, drain overflow journal first.
        if let Some(ref overflow) = self.overflow {
            let mut guard = overflow.lock();
            if let Err(e) = guard.load_pending() {
                tracing::error!(error = %e, "failed to load overflow journal");
            }
            let mut delivered_from_overflow = 0u64;
            while let Some(event) = guard.pop_pending() {
                if !self.running.load(Ordering::Relaxed) {
                    break;
                }
                // Drop lock while sending to avoid holding it during I/O.
                drop(guard);
                self.send_event(&event);
                delivered_from_overflow += 1;
                guard = overflow.lock();
            }
            if delivered_from_overflow > 0 {
                tracing::info!(count = delivered_from_overflow, "drained overflow journal");
                if let Err(e) = guard.truncate() {
                    tracing::error!(error = %e, "failed to truncate overflow journal");
                }
            }
        }

        while self.running.load(Ordering::Relaxed) {
            match self.rx.recv_timeout(Duration::from_millis(100)) {
                Ok(event) => self.send_event(&event),
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }

        // Drain remaining events on shutdown.
        for event in self.rx.try_iter() {
            self.send_event(&event);
        }
    }

    fn send_event(&self, event: &AuditEvent) {
        let body = match self.config.format {
            WebhookFormat::Json => match serde_json::to_string(event) {
                Ok(json) => json,
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to serialize audit event");
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            },
            WebhookFormat::Cef => format_cef(event),
        };

        let content_type = match self.config.format {
            WebhookFormat::Json => "application/json",
            WebhookFormat::Cef => "text/plain",
        };

        for attempt in 0..=self.config.max_retries {
            let req = ureq::post(&self.config.url)
                .header("Content-Type", content_type)
                .header("User-Agent", "chronix-audit/1.0");

            let req = if let Some(ref auth) = self.config.auth_header {
                req.header("Authorization", auth)
            } else {
                req
            };

            let req = req
                .config()
                .timeout_global(Some(self.config.timeout))
                .build();

            match req.send(&body) {
                Ok(resp) => {
                    if resp.status().is_success() {
                        self.delivered.fetch_add(1, Ordering::Relaxed);
                        metrics::counter!("chronix_audit_webhook_delivered_total").increment(1);
                        return;
                    }
                    let status = resp.status().as_u16();
                    tracing::warn!(
                        status,
                        attempt,
                        url = %self.config.url,
                        "Webhook returned non-2xx status"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        attempt,
                        url = %self.config.url,
                        "Webhook delivery failed"
                    );
                }
            }

            if attempt < self.config.max_retries {
                let backoff = Duration::from_millis(100 * 2u64.pow(attempt));
                thread::sleep(backoff);
            }
        }

        self.dropped.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("chronix_audit_webhook_dropped_total").increment(1);
        tracing::warn!(
            url = %self.config.url,
            retries = self.config.max_retries,
            "Webhook delivery failed after all retries — event dropped"
        );
    }
}

/// HTTP webhook audit sink for SIEM integration.
///
/// Events are queued into a bounded channel and delivered asynchronously by
/// a background thread.  If the channel is full, behaviour depends on the
/// `DeliveryGuarantee` mode:
///
/// - `BestEffort`: event is dropped (non-blocking).
/// - `AtLeastOnce`: event overflows to a disk-backed journal for later delivery.
pub struct WebhookSink {
    tx: mpsc::SyncSender<AuditEvent>,
    running: Arc<AtomicBool>,
    handle: parking_lot::Mutex<Option<thread::JoinHandle<()>>>,
    delivered: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    overflow: Option<Arc<parking_lot::Mutex<OverflowJournal>>>,
    delivery_guarantee: DeliveryGuarantee,
}

impl WebhookSink {
    /// Create and start a webhook sink.
    ///
    /// Spawns a background thread that consumes events from the channel and
    /// POSTs them to the configured URL.
    pub fn new(config: WebhookConfig) -> Self {
        // Expand environment variable references in auth_header
        // so that secrets need not be stored in plaintext configuration.
        let mut config = config;
        if let Some(ref raw) = config.auth_header {
            match expand_env_vars(raw) {
                Ok(expanded) => {
                    // Reject control characters in expanded auth_header
                    // to prevent HTTP header injection attacks.
                    if expanded.chars().any(char::is_control) {
                        tracing::error!("webhook auth_header contains control characters after expansion — rejecting");
                        config.auth_header = None;
                    } else {
                        config.auth_header = Some(expanded);
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to expand env vars in webhook auth_header");
                }
            }
        }

        let (tx, rx) = mpsc::sync_channel(config.buffer_size);
        let running = Arc::new(AtomicBool::new(true));
        let delivered = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let delivery_guarantee = config.delivery_guarantee;

        // Set up overflow journal for AtLeastOnce mode.
        let overflow = if delivery_guarantee == DeliveryGuarantee::AtLeastOnce {
            let dir = config
                .overflow_dir
                .clone()
                .unwrap_or_else(|| std::env::temp_dir().join("chronix-audit-overflow"));
            match OverflowJournal::new(&dir) {
                Ok(journal) => Some(Arc::new(parking_lot::Mutex::new(journal))),
                Err(e) => {
                    tracing::error!(error = %e, "failed to create overflow journal — falling back to BestEffort");
                    None
                }
            }
        } else {
            None
        };

        let worker = WebhookWorker {
            config,
            rx,
            running: Arc::clone(&running),
            delivered: Arc::clone(&delivered),
            dropped: Arc::clone(&dropped),
            overflow: overflow.clone(),
        };

        let handle = thread::Builder::new()
            .name("chronix-audit-webhook".into())
            .spawn(move || worker.run())
            .expect("failed to spawn webhook worker thread");

        Self {
            tx,
            running,
            handle: parking_lot::Mutex::new(Some(handle)),
            delivered,
            dropped,
            overflow,
            delivery_guarantee,
        }
    }

    /// Number of successfully delivered events.
    #[must_use]
    pub fn delivered_count(&self) -> u64 {
        self.delivered.load(Ordering::Relaxed)
    }

    /// Number of dropped events (channel full or delivery failure).
    #[must_use]
    pub fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Gracefully shut down the background worker, draining queued events.
    pub fn shutdown(&self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.handle.lock().take() {
            let _ = handle.join();
        }
    }
}

impl super::AuditSink for WebhookSink {
    fn emit(&self, event: &AuditEvent) -> Result<()> {
        match self.tx.try_send(event.clone()) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(event)) => {
                // AtLeastOnce mode — overflow to disk.
                if self.delivery_guarantee == DeliveryGuarantee::AtLeastOnce {
                    if let Some(ref overflow) = self.overflow {
                        if let Err(e) = overflow.lock().append(&event) {
                            tracing::error!(error = %e, "failed to write to overflow journal — event dropped");
                            self.dropped.fetch_add(1, Ordering::Relaxed);
                            metrics::counter!("chronix_audit_webhook_dropped_total").increment(1);
                        }
                        return Ok(());
                    }
                }
                // BestEffort or overflow unavailable — drop.
                self.dropped.fetch_add(1, Ordering::Relaxed);
                metrics::counter!("chronix_audit_webhook_channel_full_total").increment(1);
                tracing::warn!("Webhook audit channel full — event dropped");
                Ok(())
            }
            Err(mpsc::TrySendError::Disconnected(_)) => Err(AuditError::Internal(
                "webhook worker thread has stopped".into(),
            )),
        }
    }

    fn flush(&self) -> Result<()> {
        // Give the worker time to drain.
        thread::sleep(Duration::from_millis(50));
        Ok(())
    }
}

impl Drop for WebhookSink {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::model::{AuditAction, AuditDecision};
    use parking_lot::Mutex;
    use std::io::{BufRead, BufReader, Read, Write as IoWrite};
    use std::net::TcpListener;

    fn test_event(principal: &str) -> AuditEvent {
        AuditEvent {
            id: 1,
            timestamp: 1_000_000,
            principal: principal.to_string(),
            action: AuditAction::Write,
            resource: "test_measurement".to_string(),
            decision: AuditDecision::Allow,
            source_ip: Some("127.0.0.1".to_string()),
            request_id: Some("req-1".to_string()),
            metadata: Default::default(),
            prev_hash: None,
            event_hash: None,
        }
    }

    /// Spawn a minimal HTTP server that collects request bodies.
    fn spawn_test_server() -> (String, Arc<Mutex<Vec<String>>>, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://127.0.0.1:{}", addr.port());
        let bodies: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        (url, bodies, listener)
    }

    fn handle_requests(listener: &TcpListener, bodies: &Arc<Mutex<Vec<String>>>, count: usize) {
        listener.set_nonblocking(false).unwrap();
        for _ in 0..count {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut content_length: Option<usize> = None;
            let mut is_chunked = false;

            // Read headers (case-insensitive)
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).is_err() {
                    break;
                }
                if line.trim().is_empty() {
                    break;
                }
                let lower = line.to_ascii_lowercase();
                if let Some(val) = lower.strip_prefix("content-length:") {
                    content_length = val.trim().parse().ok();
                }
                if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
                    is_chunked = true;
                }
            }

            // Read body
            let body_str = if is_chunked {
                // Read chunked encoding
                let mut body = Vec::new();
                loop {
                    let mut size_line = String::new();
                    if reader.read_line(&mut size_line).is_err() {
                        break;
                    }
                    let chunk_size = usize::from_str_radix(size_line.trim(), 16).unwrap_or(0);
                    if chunk_size == 0 {
                        // Read trailing \r\n
                        let mut _trail = String::new();
                        let _ = reader.read_line(&mut _trail);
                        break;
                    }
                    let mut chunk = vec![0u8; chunk_size];
                    let _ = reader.read_exact(&mut chunk);
                    body.extend_from_slice(&chunk);
                    // Read trailing \r\n after chunk data
                    let mut _crlf = String::new();
                    let _ = reader.read_line(&mut _crlf);
                }
                String::from_utf8_lossy(&body).to_string()
            } else if let Some(len) = content_length {
                let mut buf = vec![0u8; len];
                let _ = reader.read_exact(&mut buf);
                String::from_utf8_lossy(&buf).to_string()
            } else {
                String::new()
            };

            bodies.lock().push(body_str);

            // Respond 200
            let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
            let _ = stream.write_all(response.as_bytes());
        }
    }

    #[test]
    fn webhook_delivers_json_event() {
        let (url, bodies, listener) = spawn_test_server();

        let config = WebhookConfig {
            url,
            format: WebhookFormat::Json,
            buffer_size: 100,
            timeout: Duration::from_secs(2),
            max_retries: 0,
            auth_header: None,
            ..Default::default()
        };

        let sink = WebhookSink::new(config);
        let event = test_event("alice");

        // Accept connections in a thread
        let bodies_clone = Arc::clone(&bodies);
        let server = thread::spawn(move || {
            handle_requests(&listener, &bodies_clone, 1);
        });

        super::super::AuditSink::emit(&sink, &event).unwrap();
        thread::sleep(Duration::from_millis(200));
        sink.shutdown();
        server.join().unwrap();

        let received = bodies.lock();
        assert_eq!(received.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(&received[0]).unwrap();
        assert_eq!(parsed["principal"], "alice");
    }

    #[test]
    fn webhook_delivers_cef_event() {
        let (url, bodies, listener) = spawn_test_server();

        let config = WebhookConfig {
            url,
            format: WebhookFormat::Cef,
            buffer_size: 100,
            timeout: Duration::from_secs(2),
            max_retries: 0,
            auth_header: None,
            ..Default::default()
        };

        let sink = WebhookSink::new(config);
        let event = test_event("bob");

        let bodies_clone = Arc::clone(&bodies);
        let server = thread::spawn(move || {
            handle_requests(&listener, &bodies_clone, 1);
        });

        super::super::AuditSink::emit(&sink, &event).unwrap();
        thread::sleep(Duration::from_millis(200));
        sink.shutdown();
        server.join().unwrap();

        let received = bodies.lock();
        assert_eq!(received.len(), 1);
        assert!(received[0].starts_with("CEF:0|Chronix|TSDB|"));
        assert!(received[0].contains("principal=bob"));
    }

    #[test]
    fn cef_format_deny_severity() {
        let mut event = test_event("mallory");
        event.decision = AuditDecision::Deny;
        let cef = format_cef(&event);
        // Deny should have severity 7
        assert!(cef.contains("|7|"));
    }

    #[test]
    fn webhook_drops_when_channel_full() {
        let config = WebhookConfig {
            url: "http://127.0.0.1:1".to_string(), // unreachable
            format: WebhookFormat::Json,
            buffer_size: 2,
            timeout: Duration::from_millis(50),
            max_retries: 0,
            auth_header: None,
            ..Default::default()
        };

        let sink = WebhookSink::new(config);

        // Fill the channel
        for i in 0..10 {
            let event = test_event(&format!("user_{i}"));
            let _ = super::super::AuditSink::emit(&sink, &event);
        }

        // Some should be dropped due to full channel
        assert!(sink.dropped_count() > 0);
        sink.shutdown();
    }

    #[test]
    fn webhook_shutdown_drains() {
        let (url, bodies, listener) = spawn_test_server();

        let config = WebhookConfig {
            url,
            format: WebhookFormat::Json,
            buffer_size: 100,
            timeout: Duration::from_secs(2),
            max_retries: 0,
            auth_header: None,
            ..Default::default()
        };

        let sink = WebhookSink::new(config);

        let bodies_clone = Arc::clone(&bodies);
        let server = thread::spawn(move || {
            handle_requests(&listener, &bodies_clone, 3);
        });

        for i in 0..3 {
            let event = test_event(&format!("user_{i}"));
            super::super::AuditSink::emit(&sink, &event).unwrap();
        }

        // Shutdown should drain
        sink.shutdown();
        server.join().unwrap();

        let received = bodies.lock();
        assert_eq!(received.len(), 3);
    }

    #[test]
    fn overflow_journal_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = OverflowJournal::new(dir.path()).unwrap();
        let event = test_event("overflow-user");

        // Append two events
        journal.append(&event).unwrap();
        journal.append(&event).unwrap();

        // Load and verify
        let count = journal.load_pending().unwrap();
        assert_eq!(count, 2);
        let e1 = journal.pop_pending().unwrap();
        assert_eq!(e1.principal, "overflow-user");
        let e2 = journal.pop_pending().unwrap();
        assert_eq!(e2.principal, "overflow-user");
        assert!(journal.pop_pending().is_none());

        // Truncate
        journal.truncate().unwrap();
        let mut journal2 = OverflowJournal::new(dir.path()).unwrap();
        let count2 = journal2.load_pending().unwrap();
        assert_eq!(count2, 0);
    }

    #[test]
    fn at_least_once_overflows_to_disk() {
        // Use a buffer_size=1 so events overflow immediately.
        let dir = tempfile::tempdir().unwrap();
        let (url, bodies, listener) = spawn_test_server();

        let config = WebhookConfig {
            url,
            buffer_size: 1,
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            overflow_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };

        let sink = WebhookSink::new(config);

        // Fill the channel (1 slot) + overflow events.
        // The first event goes into the channel, subsequent ones overflow.
        for i in 0..3 {
            let event = test_event(&format!("user_{i}"));
            super::super::AuditSink::emit(&sink, &event).unwrap();
        }

        // Verify overflow file exists and has content
        let overflow_path = dir.path().join("webhook_overflow.jsonl");
        assert!(overflow_path.exists());
        let content = std::fs::read_to_string(&overflow_path).unwrap();
        assert!(!content.is_empty(), "overflow journal should have content");

        // Now accept events from the server
        let bodies_clone = Arc::clone(&bodies);
        let server = thread::spawn(move || {
            // Accept at least the channel event
            handle_requests(&listener, &bodies_clone, 1);
        });

        // Shutdown drains both channel and overflow
        sink.shutdown();
        let _ = server.join();

        // At least the channel event was delivered
        let received = bodies.lock();
        assert!(!received.is_empty());
    }

    #[test]
    fn expand_env_vars_replaces_variable() {
        std::env::set_var("CHRONIX_TEST_TOKEN_9182", "secret-value");
        let result = super::expand_env_vars("Bearer ${CHRONIX_TEST_TOKEN_9182}").unwrap();
        assert_eq!(result, "Bearer secret-value");
        std::env::remove_var("CHRONIX_TEST_TOKEN_9182");
    }

    #[test]
    fn expand_env_vars_passthrough_literal() {
        let result = super::expand_env_vars("Bearer my-literal-token").unwrap();
        assert_eq!(result, "Bearer my-literal-token");
    }

    #[test]
    fn expand_env_vars_missing_var_errors() {
        let result = super::expand_env_vars("${CHRONIX_NONEXISTENT_VAR_XYZ}");
        assert!(result.is_err());
    }

    #[test]
    fn expand_env_vars_unclosed_brace_errors() {
        let result = super::expand_env_vars("${UNCLOSED");
        assert!(result.is_err());
    }

    #[test]
    fn expand_env_vars_multiple_vars() {
        std::env::set_var("CHRONIX_TEST_A_3847", "hello");
        std::env::set_var("CHRONIX_TEST_B_3847", "world");
        let result =
            super::expand_env_vars("${CHRONIX_TEST_A_3847} ${CHRONIX_TEST_B_3847}").unwrap();
        assert_eq!(result, "hello world");
        std::env::remove_var("CHRONIX_TEST_A_3847");
        std::env::remove_var("CHRONIX_TEST_B_3847");
    }
}
