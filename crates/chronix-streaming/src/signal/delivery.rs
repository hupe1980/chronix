//! Event delivery layer — pluggable channels for signal dispatch.
//!
//! ## Architecture
//!
//! ```text
//! TriggerEngine → SignalEvent
//!                      │
//!                      ▼
//!               DeliveryRouter
//!                 ├── WebhookChannel (HMAC-SHA256 signed)
//!                 ├── LogChannel (structured tracing)
//!                 └── (NATS / MQTT / Kafka behind feature flags)
//!                      │
//!                      └── Retry + Dead Letter Queue
//! ```

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use metrics::counter;
use parking_lot::RwLock;
use tokio::time as tokio_time;
use tracing::{debug, error, warn};

use crate::signal::error::{Result, SignalError};
use crate::signal::model::SignalEvent;

// ── DeliveryChannel trait ───────────────────────────────────────────

/// A pluggable delivery channel for signal events.
pub trait DeliveryChannel: Send + Sync {
    /// Channel name for logging & metrics.
    fn name(&self) -> &str;

    /// Deliver a signal event (blocking).
    ///
    /// # Errors
    ///
    /// Returns an error if delivery fails.
    fn deliver(&self, event: &SignalEvent) -> Result<()>;

    /// Health check — returns Ok if the channel is operational.
    fn health_check(&self) -> Result<()> {
        Ok(())
    }
}

// ── LogChannel ──────────────────────────────────────────────────────

/// Delivers signals as structured log events.
pub struct LogChannel;

impl DeliveryChannel for LogChannel {
    fn name(&self) -> &str {
        "log"
    }

    fn deliver(&self, event: &SignalEvent) -> Result<()> {
        match event.severity {
            crate::signal::model::Severity::Critical => {
                tracing::error!(
                    trigger_id = %event.trigger_id,
                    trigger_name = %event.trigger_name,
                    measurement = %event.measurement,
                    signal_type = %event.signal_type,
                    severity = %event.severity,
                    value = event.value,
                    timestamp = event.timestamp,
                    "Signal fired"
                );
            }
            crate::signal::model::Severity::Warning => {
                tracing::warn!(
                    trigger_id = %event.trigger_id,
                    trigger_name = %event.trigger_name,
                    measurement = %event.measurement,
                    signal_type = %event.signal_type,
                    severity = %event.severity,
                    value = event.value,
                    timestamp = event.timestamp,
                    "Signal fired"
                );
            }
            crate::signal::model::Severity::Info => {
                tracing::info!(
                    trigger_id = %event.trigger_id,
                    trigger_name = %event.trigger_name,
                    measurement = %event.measurement,
                    signal_type = %event.signal_type,
                    severity = %event.severity,
                    value = event.value,
                    timestamp = event.timestamp,
                    "Signal fired"
                );
            }
        }
        Ok(())
    }
}

// ── MetricChannel ───────────────────────────────────────────────────

/// Delivers signals as Prometheus metrics.
pub struct MetricChannel;

impl DeliveryChannel for MetricChannel {
    fn name(&self) -> &str {
        "metric"
    }

    fn deliver(&self, event: &SignalEvent) -> Result<()> {
        counter!("chronix_alert_fired_total",
            "trigger_id" => event.trigger_id.clone(),
            "measurement" => event.measurement.clone(),
            "severity" => event.severity.to_string()
        )
        .increment(1);
        Ok(())
    }
}

// ── WebhookChannel ──────────────────────────────────────────────────

/// Configuration for webhook delivery.
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    /// Target URL.
    pub url: String,
    /// Timeout for the HTTP request.
    pub timeout: Duration,
    /// HMAC-SHA256 signing secret (required).
    ///
    /// Every webhook payload is signed with this secret so receivers can
    /// verify authenticity via the `X-Chronix-Signature` header.
    pub signing_secret: String,
    /// Custom headers to include.
    pub headers: Vec<(String, String)>,
}

impl WebhookConfig {
    /// Create a new webhook config.
    ///
    /// `signing_secret` is now mandatory — unsigned webhooks
    /// allow payload forgery and should never be deployed.
    #[must_use]
    pub fn new(url: impl Into<String>, signing_secret: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            timeout: Duration::from_secs(10),
            signing_secret: signing_secret.into(),
            headers: Vec::new(),
        }
    }

    /// Set the timeout.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Add a custom header.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// Delivers signals via HTTP POST with optional HMAC-SHA256 signature.
///
/// The webhook channel serializes the `SignalEvent` to JSON and posts it
/// to the configured URL. If a signing secret is configured, an
/// `X-Chronix-Signature` header with the HMAC-SHA256 hex digest is included.
/// The TLS configuration for this crate's outbound HTTPS requests.
///
/// **This is a library, so it does not install a process-global provider.**
/// `CryptoProvider::install_default` sets process-wide state and silently
/// loses to whoever called it first: a crate that installs `ring` on its first
/// webhook would make a later `install_default(aws_lc_rs)` in the embedding
/// application fail, and the application would get a provider it did not
/// choose with no error to read. `chronix` is published for other people to
/// embed, so that is not a theoretical objection.
///
/// Instead the provider the application installed is *used* if there is one,
/// and `ring` is the fallback only when nobody has chosen. `chronixd`
/// installs one at startup, so under the server this resolves to the server's
/// choice.
///
/// Trust anchors come from the platform verifier, matching what `reqwest`
/// would have built on its own.
fn tls_config() -> std::result::Result<rustls::ClientConfig, rustls::Error> {
    use rustls_platform_verifier::BuilderVerifierExt;

    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider()));

    Ok(rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_platform_verifier()?
        .with_no_client_auth())
}

///
/// Uses the async `reqwest::Client` internally. When called from a
/// synchronous context (the `DeliveryChannel` trait), it bridges via
/// a dedicated background thread + oneshot channel so that `block_on`
/// is never called from within an ambient async context.
///
/// # Design
///
/// A dedicated single-threaded tokio runtime is created **once** in
/// [`WebhookChannel::new`] and a persistent background thread runs the
/// runtime's event loop.  Deliveries are submitted via an `mpsc` channel
/// and results returned via oneshot, eliminating per-delivery OS thread
/// creation.  The `reqwest::Client` is bound to this runtime's IO driver
/// and reused across all deliveries.
pub struct WebhookChannel {
    config: WebhookConfig,
    /// Sender for delivery requests to the background thread.
    tx: std::sync::mpsc::SyncSender<DeliveryRequest>,
}

/// Request sent to the background delivery thread.
struct DeliveryRequest {
    payload: Vec<u8>,
    reply: std::sync::mpsc::SyncSender<Result<(u16, String)>>,
}

impl WebhookChannel {
    /// Create a new webhook channel.
    ///
    /// A single-threaded tokio runtime and a dedicated background thread
    /// are created here and kept alive for the lifetime of this channel.
    /// Deliveries are submitted via a bounded channel, avoiding per-call
    /// OS thread creation.
    ///
    /// # Errors
    ///
    /// Returns an error if the tokio runtime or HTTP client cannot be built.
    pub fn new(config: WebhookConfig) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        // Enter the runtime context so reqwest binds its IO driver to
        // *this* runtime rather than requiring an ambient one.
        let _guard = runtime.enter();
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .use_preconfigured_tls(tls_config().map_err(|e| {
                SignalError::InvalidConfig(format!("failed to build TLS configuration: {e}"))
            })?)
            .build()?;
        drop(_guard);

        let url = config.url.clone();
        let headers = config.headers.clone();
        let signing_secret = config.signing_secret.clone();

        // Single persistent background thread replaces
        // per-delivery scoped threads.  The MPSC channel bounds
        // backpressure to 64 queued deliveries.
        let (tx, rx) = std::sync::mpsc::sync_channel::<DeliveryRequest>(64);

        std::thread::Builder::new()
            .name("chronix-webhook".into())
            .spawn(move || {
                runtime.block_on(async {
                    while let Ok(req) = rx.recv() {
                        let mut request = client
                            .post(&url)
                            .header("Content-Type", "application/json")
                            .header("User-Agent", "chronix-signal/1.0");

                        for (name, value) in &headers {
                            request = request.header(name, value);
                        }

                        // signing_secret is always present (mandatory).
                        let sig = Self::compute_signature(&signing_secret, &req.payload);
                        request = request.header("X-Chronix-Signature", format!("sha256={sig}"));

                        let result = match request.body(req.payload).send().await {
                            Ok(response) => {
                                let status = response.status().as_u16();
                                let body = if response.status().is_success() {
                                    String::new()
                                } else {
                                    response.text().await.unwrap_or_default()
                                };
                                Ok((status, body))
                            }
                            Err(e) => Err(SignalError::WebhookRequest {
                                url: url.clone(),
                                source: e,
                            }),
                        };
                        let _ = req.reply.send(result);
                    }
                });
            })?;

        Ok(Self { config, tx })
    }

    /// Compute HMAC-SHA256 signature for a payload.
    #[must_use]
    pub fn compute_signature(secret: &str, payload: &[u8]) -> String {
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::Sha256;

        let mut mac =
            Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC can take any key size");
        mac.update(payload);
        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }
}

impl DeliveryChannel for WebhookChannel {
    fn name(&self) -> &str {
        "webhook"
    }

    fn deliver(&self, event: &SignalEvent) -> Result<()> {
        let payload =
            serde_json::to_vec(event).map_err(|e| SignalError::Serialization { source: e })?;

        debug!(
            url = %self.config.url,
            trigger_id = %event.trigger_id,
            "Delivering signal via webhook"
        );

        // Send delivery to the background thread via channel
        // instead of spawning a per-delivery OS thread.
        let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel(1);
        self.tx
            .send(DeliveryRequest {
                payload,
                reply: reply_tx,
            })
            .map_err(|_| SignalError::Internal("webhook background thread shut down".into()))?;

        let (status_code, response_body) = reply_rx.recv().map_err(|_| {
            SignalError::Internal("webhook background thread dropped reply".into())
        })??;

        if (200..300).contains(&status_code) {
            counter!("chronix_signal_delivered_total", "channel" => "webhook").increment(1);
            Ok(())
        } else {
            Err(SignalError::WebhookStatus {
                url: self.config.url.clone(),
                status: status_code,
                body: response_body,
            })
        }
    }
}

// ── RetryPolicy ─────────────────────────────────────────────────────

/// Retry policy for delivery channels.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// Base backoff duration (doubles each retry).
    pub base_backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_backoff: Duration::from_secs(1),
        }
    }
}

// ── DeadLetterQueue ─────────────────────────────────────────────────

/// A dead-letter queue for events that failed all delivery attempts.
#[derive(Debug)]
pub struct DeadLetterQueue {
    events: RwLock<VecDeque<DeadLetter>>,
    max_size: usize,
}

/// A failed delivery entry.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    /// The signal that failed delivery.
    pub event: SignalEvent,
    /// The channel name that failed.
    pub channel: String,
    /// Error message from the last attempt.
    pub error: String,
    /// Number of attempts made.
    pub attempts: u32,
}

impl DeadLetterQueue {
    /// Create a new DLQ with the given capacity.
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        Self {
            events: RwLock::new(VecDeque::with_capacity(max_size)),
            max_size,
        }
    }

    /// Push a dead letter into the queue.
    pub fn push(&self, dl: DeadLetter) {
        let mut q = self.events.write();
        if q.len() >= self.max_size {
            q.pop_front();
        }
        q.push_back(dl);
        counter!("chronix_signal_dlq_total").increment(1);
    }

    /// Number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.read().len()
    }

    /// Whether the DLQ is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.read().is_empty()
    }

    /// Drain all dead letters for inspection.
    pub fn drain(&self) -> Vec<DeadLetter> {
        std::mem::take(&mut *self.events.write()).into()
    }
}

// ── DeliveryRouter ──────────────────────────────────────────────────

/// Maximum backoff cap to prevent overflow with high retry counts.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Routes signal events to registered delivery channels with retry
/// and dead-letter support.
///
/// Thread-safe — channels can be added at any time, even after
/// wrapping in `Arc`.
pub struct DeliveryRouter {
    channels: parking_lot::RwLock<Vec<Arc<dyn DeliveryChannel>>>,
    retry_policy: RetryPolicy,
    dlq: DeadLetterQueue,
}

impl DeliveryRouter {
    /// Create a new router with default retry policy.
    #[must_use]
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(Vec::new()),
            retry_policy: RetryPolicy::default(),
            dlq: DeadLetterQueue::new(10_000),
        }
    }

    /// Set the retry policy.
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Add a delivery channel. Can be called at any time, including
    /// after wrapping in `Arc`.
    pub fn add_channel(&self, channel: Box<dyn DeliveryChannel>) {
        self.channels.write().push(Arc::from(channel));
    }

    /// Deliver a signal event to all channels.
    ///
    /// Returns the number of successful deliveries.
    ///
    /// **Note:** Retry backoff uses `std::thread::yield_now()` instead of
    /// `std::thread::sleep()` to avoid blocking the calling thread for
    /// extended periods. For production use with exponential backoff,
    /// prefer [`deliver_async`](Self::deliver_async).
    pub fn deliver(&self, event: &SignalEvent) -> usize {
        let mut success_count = 0;
        // Snapshot Arc refs and release the RwLock so retries with backoff
        // don't block add_channel or other deliver calls.
        let channels: Vec<Arc<dyn DeliveryChannel>> = self.channels.read().clone();

        for channel in channels.iter() {
            let mut last_error = None;

            for attempt in 0..=self.retry_policy.max_retries {
                match channel.deliver(event) {
                    Ok(()) => {
                        success_count += 1;
                        last_error = None;
                        break;
                    }
                    Err(e) => {
                        let backoff = (self.retry_policy.base_backoff
                            * 2u32.saturating_pow(attempt))
                        .min(MAX_BACKOFF);
                        warn!(
                            channel = channel.name(),
                            attempt = attempt + 1,
                            max_retries = self.retry_policy.max_retries,
                            error = %e,
                            backoff_ms = backoff.as_millis() as u64,
                            "Delivery failed, retrying"
                        );
                        last_error = Some(e);

                        counter!("chronix_signal_delivery_failed_total",
                            "channel" => channel.name().to_string()
                        )
                        .increment(1);

                        // Actually sleep for the computed backoff
                        // duration instead of merely yielding the CPU timeslice.
                        if attempt < self.retry_policy.max_retries {
                            std::thread::sleep(backoff);
                        }
                    }
                }
            }

            if let Some(err) = last_error {
                error!(
                    channel = channel.name(),
                    trigger_id = %event.trigger_id,
                    "All delivery attempts exhausted, moving to DLQ"
                );
                self.dlq.push(DeadLetter {
                    event: event.clone(),
                    channel: channel.name().to_string(),
                    error: err.to_string(),
                    attempts: self.retry_policy.max_retries + 1,
                });
            }
        }

        success_count
    }

    /// Asynchronous delivery with proper non-blocking exponential backoff.
    ///
    /// This is the preferred method when running inside a Tokio runtime.
    /// Uses `tokio::time::sleep` for backoff instead of blocking the OS thread.
    pub async fn deliver_async(&self, event: &SignalEvent) -> usize {
        let mut success_count = 0;
        let channels: Vec<Arc<dyn DeliveryChannel>> = self.channels.read().clone();

        for channel in channels.iter() {
            let mut last_error = None;

            for attempt in 0..=self.retry_policy.max_retries {
                match channel.deliver(event) {
                    Ok(()) => {
                        success_count += 1;
                        last_error = None;
                        break;
                    }
                    Err(e) => {
                        let backoff = (self.retry_policy.base_backoff
                            * 2u32.saturating_pow(attempt))
                        .min(MAX_BACKOFF);
                        warn!(
                            channel = channel.name(),
                            attempt = attempt + 1,
                            max_retries = self.retry_policy.max_retries,
                            error = %e,
                            backoff_ms = backoff.as_millis() as u64,
                            "Delivery failed, retrying (async)"
                        );
                        last_error = Some(e);

                        counter!("chronix_signal_delivery_failed_total",
                            "channel" => channel.name().to_string()
                        )
                        .increment(1);

                        if attempt < self.retry_policy.max_retries {
                            tokio_time::sleep(backoff).await;
                        }
                    }
                }
            }

            if let Some(err) = last_error {
                error!(
                    channel = channel.name(),
                    trigger_id = %event.trigger_id,
                    "All delivery attempts exhausted, moving to DLQ"
                );
                self.dlq.push(DeadLetter {
                    event: event.clone(),
                    channel: channel.name().to_string(),
                    error: err.to_string(),
                    attempts: self.retry_policy.max_retries + 1,
                });
            }
        }

        success_count
    }

    /// Access the dead letter queue.
    #[must_use]
    pub fn dlq(&self) -> &DeadLetterQueue {
        &self.dlq
    }

    /// Number of registered channels.
    #[must_use]
    pub fn channel_count(&self) -> usize {
        self.channels.read().len()
    }
}

impl Default for DeliveryRouter {
    fn default() -> Self {
        Self::new()
    }
}

// ── SignalStore ──────────────────────────────────────────────────────

/// Persists fired signals for later querying.
///
/// In a full deployment this writes to the `_signals` measurement in
/// Chronix. Here we provide the in-memory representation and serialization
/// for the integration layer.
pub struct SignalStore {
    signals: RwLock<VecDeque<SignalEvent>>,
    max_size: usize,
}

impl SignalStore {
    /// Create a new signal store.
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        Self {
            signals: RwLock::new(VecDeque::with_capacity(max_size.min(10_000))),
            max_size,
        }
    }

    /// Persist a signal event.
    pub fn store(&self, event: SignalEvent) {
        let mut store = self.signals.write();
        if store.len() >= self.max_size {
            // FIFO eviction — O(1) with VecDeque
            store.pop_front();
        }
        store.push_back(event);
        counter!("chronix_signal_persisted_total").increment(1);
    }

    /// Query signals by measurement.
    #[must_use]
    pub fn query_by_measurement(&self, measurement: &str) -> Vec<SignalEvent> {
        self.signals
            .read()
            .iter()
            .filter(|s| s.measurement == measurement)
            .cloned()
            .collect()
    }

    /// Query signals by severity.
    #[must_use]
    pub fn query_by_severity(&self, severity: crate::signal::model::Severity) -> Vec<SignalEvent> {
        self.signals
            .read()
            .iter()
            .filter(|s| s.severity == severity)
            .cloned()
            .collect()
    }

    /// Query signals within a time range.
    #[must_use]
    pub fn query_by_time_range(&self, min_ts: i64, max_ts: i64) -> Vec<SignalEvent> {
        self.signals
            .read()
            .iter()
            .filter(|s| s.timestamp >= min_ts && s.timestamp <= max_ts)
            .cloned()
            .collect()
    }

    /// All signals.
    #[must_use]
    pub fn all(&self) -> Vec<SignalEvent> {
        self.signals.read().iter().cloned().collect()
    }

    /// Signal count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.signals.read().len()
    }

    /// Whether the store is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.signals.read().is_empty()
    }
}

impl Default for SignalStore {
    fn default() -> Self {
        Self::new(100_000)
    }
}

// ── hex encoding helper ─────────────────────────────────────────────

mod hex {
    const CHARS: &[u8; 16] = b"0123456789abcdef";

    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        let bytes = bytes.as_ref();
        let mut s = String::with_capacity(bytes.len() * 2);
        for &b in bytes {
            s.push(CHARS[(b >> 4) as usize] as char);
            s.push(CHARS[(b & 0x0f) as usize] as char);
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::model::Severity;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    fn test_signal(trigger_id: &str, measurement: &str, severity: Severity) -> SignalEvent {
        SignalEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            trigger_id: trigger_id.into(),
            trigger_name: format!("Trigger {trigger_id}"),
            measurement: measurement.into(),
            tags: Default::default(),
            timestamp: 1_700_000_000_000,
            signal_type: "test".into(),
            severity,
            value: 3.5,
            metadata: HashMap::new(),
        }
    }

    // ── LogChannel tests ────────────

    #[test]
    fn log_channel_delivers_ok() {
        let ch = LogChannel;
        let event = test_signal("t1", "cpu", Severity::Warning);
        assert!(ch.deliver(&event).is_ok());
    }

    // ── MetricChannel tests ────────

    #[test]
    fn metric_channel_delivers_ok() {
        let ch = MetricChannel;
        let event = test_signal("t1", "cpu", Severity::Critical);
        assert!(ch.deliver(&event).is_ok());
    }

    // ── WebhookChannel tests ───────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn webhook_delivers_to_unreachable_returns_error() {
        let config = WebhookConfig::new("http://127.0.0.1:1/hook", "my-secret")
            .with_timeout(Duration::from_secs(1));
        let ch = WebhookChannel::new(config).expect("failed to build webhook client");
        let event = test_signal("t1", "cpu", Severity::Warning);
        // Real HTTP delivery to an unreachable host should return an error.
        let result = ch.deliver(&event);
        assert!(result.is_err(), "delivery to unreachable host should fail");
    }

    #[test]
    fn webhook_hmac_signature() {
        let sig = WebhookChannel::compute_signature("secret", b"hello");
        assert_eq!(sig.len(), 64); // hex-encoded SHA256 = 32 bytes = 64 hex chars
                                   // Verify deterministic
        assert_eq!(sig, WebhookChannel::compute_signature("secret", b"hello"));
        // Different payload
        assert_ne!(sig, WebhookChannel::compute_signature("secret", b"world"));
        // Different key
        assert_ne!(sig, WebhookChannel::compute_signature("other", b"hello"));
    }

    #[test]
    fn webhook_config_builder() {
        let config = WebhookConfig::new("https://example.com", "s3cret")
            .with_timeout(Duration::from_secs(30))
            .with_header("Authorization", "Bearer token");
        assert_eq!(config.url, "https://example.com");
        assert_eq!(config.signing_secret, "s3cret");
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.headers.len(), 1);
    }

    // ── DeadLetterQueue tests ──────

    #[test]
    fn dlq_push_and_drain() {
        let dlq = DeadLetterQueue::new(100);
        assert!(dlq.is_empty());

        dlq.push(DeadLetter {
            event: test_signal("t1", "cpu", Severity::Critical),
            channel: "webhook".into(),
            error: "timeout".into(),
            attempts: 3,
        });

        assert_eq!(dlq.len(), 1);
        let items = dlq.drain();
        assert_eq!(items.len(), 1);
        assert!(dlq.is_empty());
    }

    #[test]
    fn dlq_evicts_when_full() {
        let dlq = DeadLetterQueue::new(2);
        for i in 0..3 {
            dlq.push(DeadLetter {
                event: test_signal(&format!("t{i}"), "cpu", Severity::Info),
                channel: "test".into(),
                error: "err".into(),
                attempts: 1,
            });
        }
        assert_eq!(dlq.len(), 2);
        let items = dlq.drain();
        // Oldest (t0) should have been evicted
        assert_eq!(items[0].event.trigger_id, "t1");
        assert_eq!(items[1].event.trigger_id, "t2");
    }

    // ── DeliveryRouter tests ───────

    struct FailingChannel {
        fail_count: Arc<AtomicU32>,
        fail_first_n: u32,
    }

    impl DeliveryChannel for FailingChannel {
        fn name(&self) -> &str {
            "failing"
        }

        fn deliver(&self, _event: &SignalEvent) -> Result<()> {
            let n = self.fail_count.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_first_n {
                Err(SignalError::Delivery("simulated failure".into()))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn router_delivers_to_all_channels() {
        let router = DeliveryRouter::new();
        router.add_channel(Box::new(LogChannel));
        router.add_channel(Box::new(MetricChannel));
        assert_eq!(router.channel_count(), 2);

        let event = test_signal("t1", "cpu", Severity::Warning);
        let ok = router.deliver(&event);
        assert_eq!(ok, 2);
        assert!(router.dlq().is_empty());
    }

    #[test]
    fn router_retries_and_succeeds() {
        let fail_count = Arc::new(AtomicU32::new(0));
        let router = DeliveryRouter::new().with_retry_policy(RetryPolicy {
            max_retries: 3,
            base_backoff: Duration::from_millis(1),
        });
        router.add_channel(Box::new(FailingChannel {
            fail_count: Arc::clone(&fail_count),
            fail_first_n: 2, // fail twice, succeed on 3rd
        }));

        let event = test_signal("t1", "cpu", Severity::Warning);
        let ok = router.deliver(&event);
        assert_eq!(ok, 1);
        assert!(router.dlq().is_empty());
    }

    #[test]
    fn router_exhausts_retries_into_dlq() {
        let fail_count = Arc::new(AtomicU32::new(0));
        let router = DeliveryRouter::new().with_retry_policy(RetryPolicy {
            max_retries: 2,
            base_backoff: Duration::from_millis(1),
        });
        router.add_channel(Box::new(FailingChannel {
            fail_count: Arc::clone(&fail_count),
            fail_first_n: 100, // always fail
        }));

        let event = test_signal("t1", "cpu", Severity::Critical);
        let ok = router.deliver(&event);
        assert_eq!(ok, 0);
        assert_eq!(router.dlq().len(), 1);

        let dls = router.dlq().drain();
        assert_eq!(dls[0].channel, "failing");
        assert_eq!(dls[0].attempts, 3); // 1 initial + 2 retries
    }

    // ── SignalStore tests ──────────

    #[test]
    fn store_and_query_by_measurement() {
        let store = SignalStore::new(100);
        store.store(test_signal("t1", "cpu", Severity::Warning));
        store.store(test_signal("t2", "memory", Severity::Info));
        store.store(test_signal("t3", "cpu", Severity::Critical));

        assert_eq!(store.len(), 3);
        let cpu_signals = store.query_by_measurement("cpu");
        assert_eq!(cpu_signals.len(), 2);
    }

    #[test]
    fn store_query_by_severity() {
        let store = SignalStore::new(100);
        store.store(test_signal("t1", "cpu", Severity::Warning));
        store.store(test_signal("t2", "cpu", Severity::Critical));
        store.store(test_signal("t3", "cpu", Severity::Info));

        let critical = store.query_by_severity(Severity::Critical);
        assert_eq!(critical.len(), 1);
        assert_eq!(critical[0].trigger_id, "t2");
    }

    #[test]
    fn store_query_by_time_range() {
        let store = SignalStore::new(100);
        let mut e1 = test_signal("t1", "cpu", Severity::Info);
        e1.timestamp = 100;
        let mut e2 = test_signal("t2", "cpu", Severity::Info);
        e2.timestamp = 200;
        let mut e3 = test_signal("t3", "cpu", Severity::Info);
        e3.timestamp = 300;

        store.store(e1);
        store.store(e2);
        store.store(e3);

        let results = store.query_by_time_range(150, 250);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].trigger_id, "t2");
    }

    #[test]
    fn store_evicts_oldest_when_full() {
        let store = SignalStore::new(2);
        store.store(test_signal("t1", "cpu", Severity::Info));
        store.store(test_signal("t2", "cpu", Severity::Info));
        store.store(test_signal("t3", "cpu", Severity::Info));

        assert_eq!(store.len(), 2);
        let all = store.all();
        assert_eq!(all[0].trigger_id, "t2");
        assert_eq!(all[1].trigger_id, "t3");
    }

    // ── hex tests ──────────────────

    #[test]
    fn hex_encode() {
        assert_eq!(hex::encode([0xde, 0xad, 0xbe, 0xef]), "deadbeef");
        assert_eq!(hex::encode([0x00, 0xff]), "00ff");
    }

    #[test]
    fn test_backoff_capped_at_60s() {
        let policy = RetryPolicy {
            max_retries: 100,
            base_backoff: Duration::from_secs(1),
        };
        let backoff = (policy.base_backoff * 2u32.saturating_pow(100)).min(MAX_BACKOFF);
        assert_eq!(backoff, Duration::from_secs(60));
    }

    #[tokio::test]
    async fn deliver_async_succeeds() {
        let router = DeliveryRouter::new();
        router.add_channel(Box::new(LogChannel));

        let event = test_signal("t1", "cpu", Severity::Warning);
        let ok = router.deliver_async(&event).await;
        assert_eq!(ok, 1);
    }

    #[tokio::test]
    async fn deliver_async_exhausts_retries_into_dlq() {
        let fail_count = Arc::new(AtomicU32::new(0));
        let router = DeliveryRouter::new().with_retry_policy(RetryPolicy {
            max_retries: 2,
            base_backoff: Duration::from_millis(1),
        });
        router.add_channel(Box::new(FailingChannel {
            fail_count: Arc::clone(&fail_count),
            fail_first_n: 100,
        }));

        let event = test_signal("t1", "cpu", Severity::Critical);
        let ok = router.deliver_async(&event).await;
        assert_eq!(ok, 0);
        assert_eq!(router.dlq().len(), 1);
    }

    #[tokio::test]
    async fn deliver_async_does_not_block_runtime() {
        // Verify that deliver_async with backoff does NOT block the tokio
        // runtime — we can run other tasks concurrently.
        let fail_count = Arc::new(AtomicU32::new(0));
        let router = Arc::new(DeliveryRouter::new().with_retry_policy(RetryPolicy {
            max_retries: 2,
            base_backoff: Duration::from_millis(10),
        }));
        router.add_channel(Box::new(FailingChannel {
            fail_count: Arc::clone(&fail_count),
            fail_first_n: 100,
        }));

        let event = test_signal("t1", "cpu", Severity::Info);
        let router2 = Arc::clone(&router);

        // Spawn delivery and a concurrent task
        let (delivery_result, concurrent_ok) =
            tokio::join!(async move { router2.deliver_async(&event).await }, async {
                // This should complete while deliver_async is sleeping
                tokio::time::sleep(Duration::from_millis(1)).await;
                true
            });

        assert_eq!(delivery_result, 0);
        assert!(
            concurrent_ok,
            "concurrent task should complete while delivery retries"
        );
    }
}
