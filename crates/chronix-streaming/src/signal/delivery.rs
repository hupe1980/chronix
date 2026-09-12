//! Event delivery layer — pluggable channels for signal dispatch.
//!
//! ## Architecture
//!
//! ```text
//! TriggerEngine → SignalEvent
//!                      │
//!                      ▼
//!               DeliveryRouter
//!                 ├── WebhookChannel (CloudEvents body, Standard Webhooks signature)
//!                 ├── LogChannel (structured tracing)
//!                 └── (NATS / MQTT / Kafka behind feature flags)
//!                      │
//!                      └── Retry + Dead Letter Queue
//! ```
//!
//! ## The wire format is two open standards, not one of our own
//!
//! A webhook receiver is somebody else's code, so the bar is the same one
//! the wire protocols are held to: conformance with what a receiver already
//! knows how to verify, not a bespoke scheme with one implementation. The
//! body is a [CloudEvents](https://cloudevents.io) 1.0 structured-mode JSON
//! envelope —
//! `specversion`, `id`, `source`, `type`, `time`, `data` — so any
//! CloudEvents-aware router (Knative, EventBridge, an OTel Collector
//! receiver) can consume a fired signal without knowing chronix's own shape;
//! `data` carries the [`SignalEvent`] verbatim, so nothing already reading it
//! for `trigger_id`, `measurement` or `value` has to change. The signature is
//! [Standard Webhooks](https://www.standardwebhooks.com) `v1`: `webhook-id`,
//! `webhook-timestamp` and `webhook-signature` headers over
//! `{id}.{timestamp}.{body}`, which is what turns a payload signature into a
//! request signature — the old `X-Chronix-Signature: sha256=<hex>` covered
//! the body alone, so a captured request could be replayed indefinitely with
//! nothing to bound its age. A receiver that already speaks Standard
//! Webhooks — Svix, Stripe-alikes, the reference libraries the spec ships —
//! verifies a chronix signal with no code specific to chronix at all.
//!
//! ## Delivery does not block evaluation
//!
//! Each channel owns a **bounded queue and a worker thread**, and
//! [`DeliveryRouter::deliver`] only enqueues. It used to run the retry
//! schedule inline, on the same task that processes the CDC event — so one
//! unreachable webhook backing off for seven seconds delayed every trigger
//! evaluation behind it, and a sustained write rate overran the event bus.
//! Evaluation stays ordered because the CDC listener is; delivery is
//! per-channel, so a broken webhook cannot slow a healthy one.
//!
//! A full queue **drops the oldest event** and counts it. That is the honest
//! failure: the alternative is blocking ingestion on a channel nobody can
//! reach.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use metrics::{counter, gauge};
use parking_lot::RwLock;
use serde::Serialize;
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

// ── CloudEvent envelope ───────────────────────────────────────────────

/// A [CloudEvents 1.0](https://cloudevents.io) structured-mode JSON envelope
/// around a fired [`SignalEvent`].
///
/// `data` carries the event exactly as chronix's own APIs return it — a
/// receiver that already parses `SignalEvent` does not have to change — and
/// the envelope is what makes the body recognisable to anything that speaks
/// CloudEvents without knowing chronix at all: `source` is the trigger that
/// fired, `id` is the event's own idempotency key (so a redelivered signal
/// keeps the id a receiver already deduplicated on), and `time` is the data
/// point's own timestamp, not the delivery attempt's — the two differ across
/// a retry, and `time` is defined as when the thing described *happened*.
#[derive(Debug, Serialize)]
struct CloudEvent<'a> {
    specversion: &'static str,
    id: &'a str,
    source: String,
    #[serde(rename = "type")]
    ty: &'static str,
    time: String,
    datacontenttype: &'static str,
    data: &'a SignalEvent,
}

impl<'a> CloudEvent<'a> {
    fn from_signal(event: &'a SignalEvent) -> Self {
        Self {
            specversion: "1.0",
            id: &event.event_id,
            source: format!("chronix:trigger/{}", event.trigger_id),
            ty: "io.chronix.signal.fired",
            time: chrono::DateTime::from_timestamp_nanos(event.timestamp).to_rfc3339(),
            datacontenttype: "application/json",
            data: event,
        }
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
    /// HMAC-SHA256 signing secrets, newest first. At least one is required.
    ///
    /// Every delivery is signed per the [Standard
    /// Webhooks](https://www.standardwebhooks.com) `v1` scheme and carried in
    /// the `webhook-id` / `webhook-timestamp` / `webhook-signature` headers,
    /// so receivers can verify authenticity — and the request's age — with
    /// any Standard Webhooks-compatible library rather than code specific to
    /// chronix.
    ///
    /// **A list, because rotating one secret is otherwise an outage.** The
    /// header is space-delimited by design: a delivery signed with every
    /// active secret verifies against a receiver that holds *any* of them, so
    /// the two sides can be updated in either order and in their own time.
    /// With a single secret there is no such window — every in-flight request
    /// fails from the moment one side changes, which is why a rotation that
    /// should be routine gets postponed until it is urgent.
    ///
    /// The ordinary case is one entry. Add the new secret, deploy, let the
    /// receivers pick it up, then drop the old one.
    pub signing_secrets: Vec<String>,
    /// Custom headers to include.
    pub headers: Vec<(String, String)>,
    /// Permit a target that resolves inside the deployment's own network.
    ///
    /// Off by default. A webhook URL reaching loopback, RFC1918 or a
    /// link-local address is server-side request forgery when the URL came
    /// from anywhere but the operator — the cloud metadata service, an
    /// unauthenticated Docker socket and a service mesh all live there — so
    /// the address rule is enforced at the connection regardless of what
    /// built the channel.
    ///
    /// An operator wiring a webhook to a sink on their own host is the one
    /// legitimate case, and it is spelled out here rather than left as a hole
    /// in the check. The trigger DSL never sets it.
    pub allow_private_targets: bool,
}

impl WebhookConfig {
    /// Create a new webhook config.
    ///
    /// A signing secret is mandatory — unsigned webhooks allow payload
    /// forgery and should never be deployed.
    ///
    /// Use [`with_secrets`](Self::with_secrets) to sign with more than one
    /// during a rotation.
    #[must_use]
    pub fn new(url: impl Into<String>, signing_secret: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            timeout: Duration::from_secs(10),
            signing_secrets: vec![signing_secret.into()],
            headers: Vec::new(),
            allow_private_targets: false,
        }
    }

    /// Sign with every secret in `secrets`, newest first.
    ///
    /// The `webhook-signature` header carries one `v1,<sig>` per secret,
    /// space-delimited, so a receiver holding any one of them verifies.
    #[must_use]
    pub fn with_secrets(url: impl Into<String>, secrets: Vec<String>) -> Self {
        Self {
            url: url.into(),
            timeout: Duration::from_secs(10),
            signing_secrets: secrets,
            headers: Vec::new(),
            allow_private_targets: false,
        }
    }

    /// The `webhook-signature` header value for one delivery.
    ///
    /// One `v1,<base64>` per active secret, space-delimited — the format the
    /// Standard Webhooks spec defines precisely so that rotation has a
    /// window.
    #[must_use]
    pub fn signature_header(&self, msg_id: &str, timestamp: u64, payload: &[u8]) -> String {
        self.signing_secrets
            .iter()
            .map(|secret| {
                format!(
                    "v1,{}",
                    WebhookChannel::sign(secret, msg_id, timestamp, payload)
                )
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Permit a target inside the deployment's own network.
    ///
    /// See [`allow_private_targets`](Self::allow_private_targets) for what
    /// this gives up.
    #[must_use]
    pub fn allow_private_targets(mut self, allow: bool) -> Self {
        self.allow_private_targets = allow;
        self
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

/// The TLS configuration for this crate's outbound HTTPS requests.
///
/// **This is a library, so it installs no process-global provider.**
/// `CryptoProvider::install_default` sets process-wide state and silently
/// loses to whoever called it first, which would hand an embedding
/// application a provider it did not choose with no error to read. The
/// provider the application installed is used when there is one, and `ring`
/// is the fallback only when nobody has chosen; `chronixd` installs one at
/// startup.
///
/// Trust anchors come from the platform verifier, matching what `reqwest`
/// builds on its own.
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
    /// Channel name, `webhook:<url>`.
    ///
    /// The URL is part of the name because the name is what a trigger's
    /// `DELIVER` clause routes on. A constant `"webhook"` made two endpoints
    /// one channel, so a trigger asking for either got both.
    name: String,
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
    /// # The target is resolved once, here, and the client is pinned to it
    ///
    /// [`ssrf::resolve_allowed_addrs`](crate::signal::ssrf::resolve_allowed_addrs)
    /// resolves the host and refuses every address that is not routable on the
    /// public internet; the client is then told to use exactly those addresses
    /// for that host. Checking the URL and then letting the client resolve it
    /// again is a DNS-rebinding hole — the name can answer with a public
    /// address for the check and a link-local one for the request — and it is
    /// the reason the check lives at the connection rather than only at
    /// `CREATE TRIGGER`.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is not an allowed webhook target, if its
    /// host does not resolve, or if the tokio runtime or HTTP client cannot be
    /// built.
    pub fn new(config: WebhookConfig) -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let addrs =
            crate::signal::ssrf::resolve_allowed_addrs(&config.url, config.allow_private_targets)?;
        let host = reqwest::Url::parse(&config.url)?
            .host_str()
            .ok_or_else(|| SignalError::InvalidConfig("webhook URL has no host".into()))?
            .to_string();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        // Enter the runtime context so reqwest binds its IO driver to
        // *this* runtime rather than requiring an ambient one.
        let _guard = runtime.enter();
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .resolve_to_addrs(&host, &addrs)
            .use_preconfigured_tls(tls_config().map_err(|e| {
                SignalError::InvalidConfig(format!("failed to build TLS configuration: {e}"))
            })?)
            .build()?;
        drop(_guard);

        let url = config.url.clone();
        let headers = config.headers.clone();
        let signing_secrets = config.signing_secrets.clone();

        // Single persistent background thread replaces
        // per-delivery scoped threads.  The MPSC channel bounds
        // backpressure to 64 queued deliveries.
        let (tx, rx) = std::sync::mpsc::sync_channel::<DeliveryRequest>(64);

        std::thread::Builder::new()
            .name("chronix-webhook".into())
            .spawn(move || {
                runtime.block_on(async {
                    while let Ok(req) = rx.recv() {
                        // Standard Webhooks' own replay-attack mitigation:
                        // the timestamp is signed alongside the body, so a
                        // captured request has an age a receiver can refuse.
                        // `msg_` is the spec's own convention for the id
                        // prefix.
                        let msg_id = format!("msg_{}", uuid::Uuid::new_v4().simple());
                        let timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);

                        let mut request = client
                            .post(&url)
                            .header("Content-Type", "application/cloudevents+json")
                            .header("User-Agent", "chronix-signal/1.0")
                            .header("webhook-id", &msg_id)
                            .header("webhook-timestamp", timestamp.to_string());

                        for (name, value) in &headers {
                            request = request.header(name, value);
                        }

                        // One signature per active secret, space-delimited,
                        // so a receiver holding any of them verifies and a
                        // rotation is not an outage.
                        let sigs: Vec<String> = signing_secrets
                            .iter()
                            .map(|secret| {
                                format!(
                                    "v1,{}",
                                    Self::sign(secret, &msg_id, timestamp, &req.payload)
                                )
                            })
                            .collect();
                        request = request.header("webhook-signature", sigs.join(" "));

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

        let name = format!("webhook:{}", config.url);
        Ok(Self { config, name, tx })
    }

    /// Standard Webhooks `v1` signature: `base64(HMAC-SHA256(secret,
    /// "{msg_id}.{timestamp}.{payload}"))`.
    ///
    /// Signing the id and the timestamp alongside the body — not the body
    /// alone, which is what `X-Chronix-Signature: sha256=<hex>` did — is
    /// what lets a receiver refuse a captured request that is replayed
    /// later: nothing about a bare payload signature says how old the
    /// request is.
    #[must_use]
    pub fn sign(secret: &str, msg_id: &str, timestamp: u64, payload: &[u8]) -> String {
        use base64::Engine as _;
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::Sha256;

        let mut mac =
            Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC can take any key size");
        mac.update(msg_id.as_bytes());
        mac.update(b".");
        mac.update(timestamp.to_string().as_bytes());
        mac.update(b".");
        mac.update(payload);
        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }
}

impl DeliveryChannel for WebhookChannel {
    fn name(&self) -> &str {
        &self.name
    }

    fn deliver(&self, event: &SignalEvent) -> Result<()> {
        let envelope = CloudEvent::from_signal(event);
        let payload =
            serde_json::to_vec(&envelope).map_err(|e| SignalError::Serialization { source: e })?;

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

/// A webhook channel that builds itself on first delivery.
///
/// [`WebhookChannel::new`] resolves the target host — a network call, and the
/// channel is registered from `CREATE TRIGGER`, which must not do network I/O.
/// So the URL's syntactic check happens at parse time and resolution happens
/// here, on the first signal. A failure is retried on the next signal rather
/// than cached.
pub struct DeferredWebhookChannel {
    config: WebhookConfig,
    name: String,
    inner: RwLock<Option<Arc<WebhookChannel>>>,
}

impl DeferredWebhookChannel {
    /// Name a webhook target without connecting to it.
    #[must_use]
    pub fn new(config: WebhookConfig) -> Self {
        let name = format!("webhook:{}", config.url);
        Self {
            config,
            name,
            inner: RwLock::new(None),
        }
    }

    /// The built channel, building it if this is the first delivery.
    fn channel(&self) -> Result<Arc<WebhookChannel>> {
        if let Some(existing) = self.inner.read().clone() {
            return Ok(existing);
        }
        let mut slot = self.inner.write();
        // Another thread may have built it while this one waited.
        if let Some(existing) = slot.clone() {
            return Ok(existing);
        }
        let built = Arc::new(WebhookChannel::new(self.config.clone()).map_err(|e| {
            SignalError::InvalidConfig(format!("webhook channel for {}: {e}", self.config.url))
        })?);
        *slot = Some(Arc::clone(&built));
        Ok(built)
    }
}

impl DeliveryChannel for DeferredWebhookChannel {
    fn name(&self) -> &str {
        &self.name
    }

    fn deliver(&self, event: &SignalEvent) -> Result<()> {
        self.channel()?.deliver(event)
    }

    fn health_check(&self) -> Result<()> {
        self.channel().map(|_| ())
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

/// How many events one channel may have waiting.
///
/// Per channel, so a broken webhook cannot consume a healthy one's headroom.
/// Deep enough to ride out a retry schedule (a minute at the backoff cap),
/// shallow enough that a channel nobody can reach is not a memory leak.
const DEFAULT_QUEUE_CAPACITY: usize = 1024;

/// One registered channel, with the queue and worker that serve it.
struct ChannelWorker {
    channel: Arc<dyn DeliveryChannel>,
    /// Pending events, oldest first, plus a flag the worker sets while it is
    /// mid-delivery so `flush` waits for the event in flight too.
    queue: Arc<(Mutex<WorkerState>, Condvar)>,
    handle: Option<std::thread::JoinHandle<()>>,
}

/// What the worker and its producers share.
struct WorkerState {
    pending: VecDeque<SignalEvent>,
    /// Set while an event is being delivered, so "empty queue" does not mean
    /// "nothing in flight".
    in_flight: bool,
    /// Set on drop so the worker stops rather than blocking for ever.
    stopping: bool,
}

impl ChannelWorker {
    fn spawn(
        channel: Arc<dyn DeliveryChannel>,
        retry_policy: RetryPolicy,
        dlq: Arc<DeadLetterQueue>,
        capacity: usize,
    ) -> Self {
        let queue = Arc::new((
            Mutex::new(WorkerState {
                pending: VecDeque::new(),
                in_flight: false,
                stopping: false,
            }),
            Condvar::new(),
        ));
        let worker_queue = Arc::clone(&queue);
        let worker_channel = Arc::clone(&channel);
        let name = channel.name().to_string();
        let handle = std::thread::Builder::new()
            .name(format!("chronix::deliver::{name}"))
            .spawn(move || {
                run_worker(&worker_channel, &worker_queue, &retry_policy, &dlq);
            })
            .ok();
        let _ = capacity;
        Self {
            channel,
            queue,
            handle,
        }
    }

    /// Queue an event. Returns `false` when the queue was full and the oldest
    /// waiting event was dropped to make room.
    fn enqueue(&self, event: SignalEvent, capacity: usize) -> bool {
        let (lock, cv) = &*self.queue;
        let mut state = match lock.lock() {
            Ok(s) => s,
            // A worker that panicked mid-delivery poisons the lock. The queue
            // is still structurally sound, so keep serving rather than
            // dropping every later signal.
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut accepted = true;
        while state.pending.len() >= capacity {
            state.pending.pop_front();
            accepted = false;
        }
        state.pending.push_back(event);
        let depth = state.pending.len();
        drop(state);
        cv.notify_one();
        gauge!("chronix_signal_delivery_queue_depth", "channel" => self.channel.name().to_string())
            .set(depth as f64);
        if !accepted {
            counter!("chronix_signal_delivery_dropped_total",
                "channel" => self.channel.name().to_string())
            .increment(1);
            warn!(
                channel = self.channel.name(),
                capacity, "delivery queue full — dropped the oldest waiting signal"
            );
        }
        accepted
    }

    /// Whether nothing is waiting and nothing is being delivered.
    fn is_idle(&self) -> bool {
        let (lock, _) = &*self.queue;
        let state = match lock.lock() {
            Ok(s) => s,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.pending.is_empty() && !state.in_flight
    }
}

impl Drop for ChannelWorker {
    fn drop(&mut self) {
        {
            let (lock, cv) = &*self.queue;
            let mut state = match lock.lock() {
                Ok(s) => s,
                Err(poisoned) => poisoned.into_inner(),
            };
            state.stopping = true;
            cv.notify_all();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// The retry schedule, run off the evaluation path.
///
/// One implementation. There were two — `deliver` with `std::thread::sleep`
/// and `deliver_async` with `tokio::time::sleep` — and they had already
/// drifted: only the blocking one recorded
/// `chronix_signal_delivery_duration_seconds`, so the documented latency
/// histogram was blank for anything that used the async path.
fn run_worker(
    channel: &Arc<dyn DeliveryChannel>,
    queue: &Arc<(Mutex<WorkerState>, Condvar)>,
    retry_policy: &RetryPolicy,
    dlq: &Arc<DeadLetterQueue>,
) {
    let (lock, cv) = &**queue;
    loop {
        let event = {
            let mut state = match lock.lock() {
                Ok(s) => s,
                Err(poisoned) => poisoned.into_inner(),
            };
            loop {
                if let Some(event) = state.pending.pop_front() {
                    state.in_flight = true;
                    break event;
                }
                if state.stopping {
                    return;
                }
                state = match cv.wait(state) {
                    Ok(s) => s,
                    Err(poisoned) => poisoned.into_inner(),
                };
            }
        };

        deliver_with_retries(channel, &event, retry_policy, dlq);

        let (lock, cv) = &**queue;
        let mut state = match lock.lock() {
            Ok(s) => s,
            Err(poisoned) => poisoned.into_inner(),
        };
        state.in_flight = false;
        cv.notify_all();
    }
}

/// Deliver one event to one channel, retrying on the policy's schedule.
fn deliver_with_retries(
    channel: &Arc<dyn DeliveryChannel>,
    event: &SignalEvent,
    retry_policy: &RetryPolicy,
    dlq: &Arc<DeadLetterQueue>,
) {
    let mut last_error = None;
    // Delivery latency including retries: a channel that succeeds only on its
    // third attempt is healthy by the success counter and useless in practice,
    // and nothing measured the difference.
    let started = std::time::Instant::now();

    for attempt in 0..=retry_policy.max_retries {
        match channel.deliver(event) {
            Ok(()) => {
                counter!("chronix_signal_delivery_total",
                    "channel" => channel.name().to_string())
                .increment(1);
                metrics::histogram!(
                    "chronix_signal_delivery_duration_seconds",
                    "channel" => channel.name().to_string()
                )
                .record(started.elapsed().as_secs_f64());
                return;
            }
            Err(e) => {
                let backoff =
                    (retry_policy.base_backoff * 2u32.saturating_pow(attempt)).min(MAX_BACKOFF);
                warn!(
                    channel = channel.name(),
                    attempt = attempt + 1,
                    max_retries = retry_policy.max_retries,
                    error = %e,
                    backoff_ms = backoff.as_millis() as u64,
                    "Delivery failed, retrying"
                );
                last_error = Some(e);
                counter!("chronix_signal_delivery_failed_total",
                    "channel" => channel.name().to_string())
                .increment(1);
                // Sleeping here parks the *channel's own* worker thread and
                // nothing else — which is the whole point of the worker.
                if attempt < retry_policy.max_retries {
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
        dlq.push(DeadLetter {
            event: event.clone(),
            channel: channel.name().to_string(),
            error: err.to_string(),
            attempts: retry_policy.max_retries + 1,
        });
    }
}

/// Routes signal events to registered delivery channels with retry
/// and dead-letter support.
///
/// Thread-safe — channels can be added at any time, even after
/// wrapping in `Arc`.
pub struct DeliveryRouter {
    channels: parking_lot::RwLock<Vec<Arc<ChannelWorker>>>,
    retry_policy: RetryPolicy,
    dlq: Arc<DeadLetterQueue>,
    queue_capacity: usize,
    /// Events queued since start, for the flush-on-shutdown log line.
    queued: AtomicUsize,
}

impl DeliveryRouter {
    /// Create a new router with default retry policy.
    #[must_use]
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(Vec::new()),
            retry_policy: RetryPolicy::default(),
            dlq: Arc::new(DeadLetterQueue::new(10_000)),
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            queued: AtomicUsize::new(0),
        }
    }

    /// Set the retry policy. Applies to channels added afterwards.
    #[must_use]
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Set the per-channel queue capacity. Applies to channels added
    /// afterwards.
    #[must_use]
    pub fn with_queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity.max(1);
        self
    }

    /// Add a delivery channel. Can be called at any time, including
    /// after wrapping in `Arc`.
    pub fn add_channel(&self, channel: Box<dyn DeliveryChannel>) {
        let channel: Arc<dyn DeliveryChannel> = Arc::from(channel);
        let worker = ChannelWorker::spawn(
            channel,
            self.retry_policy.clone(),
            Arc::clone(&self.dlq),
            self.queue_capacity,
        );
        self.channels.write().push(Arc::new(worker));
    }

    /// Whether a channel of this name is registered.
    ///
    /// Used to make channel registration idempotent: a second trigger
    /// delivering to the same webhook URL must reuse the channel, which owns a
    /// runtime, a thread and a connection pool.
    #[must_use]
    pub fn has_channel(&self, name: &str) -> bool {
        self.channels
            .read()
            .iter()
            .any(|c| c.channel.name() == name)
    }

    /// The channels a signal is for.
    ///
    /// See [`deliver`](Self::deliver) for why this exists.
    fn route(&self, event: &SignalEvent) -> Vec<Arc<ChannelWorker>> {
        let all: Vec<Arc<ChannelWorker>> = self.channels.read().clone();
        if event.delivery_targets.is_empty() {
            return all;
        }

        let mut routed = Vec::with_capacity(event.delivery_targets.len());
        for target in &event.delivery_targets {
            match all.iter().find(|c| c.channel.name() == target) {
                Some(worker) => routed.push(Arc::clone(worker)),
                None => {
                    counter!("chronix_signal_delivery_unrouted_total").increment(1);
                    warn!(
                        trigger = %event.trigger_name,
                        target = %target,
                        "signal names a delivery channel that is not registered — \
                         the signal fired and went nowhere"
                    );
                }
            }
        }
        routed
    }

    /// Queue a signal event for the channels its trigger named.
    ///
    /// [`SignalEvent::delivery_targets`] is matched against
    /// [`DeliveryChannel::name`]. An empty list means every channel, which is
    /// what a signal with no trigger behind it — an anomaly alert — wants.
    ///
    /// A target naming a channel that is not registered is not silently
    /// dropped: it increments `chronix_signal_delivery_unrouted_total` and is
    /// logged, because a webhook that was configured and never fires is the
    /// failure this subsystem exists to avoid.
    ///
    /// **This returns as soon as the event is queued.** Retries run on each
    /// channel's own worker, so an unreachable webhook delays nothing but
    /// itself. Use [`flush`](Self::flush) to wait for delivery — a test, or a
    /// shutdown.
    ///
    /// Returns the number of channels the event was queued to.
    pub fn deliver(&self, event: &SignalEvent) -> usize {
        let workers = self.route(event);
        for worker in &workers {
            worker.enqueue(event.clone(), self.queue_capacity);
        }
        self.queued.fetch_add(workers.len(), Ordering::Relaxed);
        counter!("chronix_signal_delivery_queued_total").increment(workers.len() as u64);
        workers.len()
    }

    /// Wait until every channel has drained, or `timeout` elapses.
    ///
    /// Returns `true` if everything drained. Used by shutdown — a signal that
    /// fired a millisecond before `SIGTERM` should still go out — and by tests,
    /// which is what keeps [`deliver`](Self::deliver) free of a second
    /// synchronous code path.
    pub fn flush(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let workers: Vec<Arc<ChannelWorker>> = self.channels.read().clone();
            if workers.iter().all(|w| w.is_idle()) {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                debug!("delivery flush timed out with events still queued");
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
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

    /// Events queued for delivery since start.
    #[must_use]
    pub fn queued_total(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }
}

impl Default for DeliveryRouter {
    fn default() -> Self {
        Self::new()
    }
}

// ── SignalStore ──────────────────────────────────────────────────────

/// Persists fired signals for later querying, **one bounded ring per
/// namespace**.
///
/// The capacity is per namespace, not shared. It used to be one process-wide
/// deque filtered on the way out, so a tenant firing signals quickly evicted
/// a quiet tenant's — and the quiet tenant saw fewer of its own signals with
/// nothing to say why. Total memory is `max_size × namespaces`, which is what
/// isolation costs and what the namespace registry bounds.
///
/// A signal's namespace is the `__namespace__` tag on the series it is about —
/// the same thing the trigger's own scoping matched on. A deployment that is
/// not multi-tenant has one unnamed bucket and behaves exactly as before.
pub struct SignalStore {
    /// `None` is the unnamed bucket a single-tenant deployment uses.
    signals: RwLock<std::collections::BTreeMap<Option<String>, VecDeque<SignalEvent>>>,
    max_size: usize,
}

impl SignalStore {
    /// Create a new signal store holding `max_size` signals **per namespace**.
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        Self {
            signals: RwLock::new(std::collections::BTreeMap::new()),
            max_size,
        }
    }

    /// The namespace a signal belongs to.
    fn namespace_of(event: &SignalEvent) -> Option<String> {
        event.tags.get(chronix_core::NAMESPACE_TAG).cloned()
    }

    /// Persist a signal event in its namespace's ring.
    pub fn store(&self, event: SignalEvent) {
        let ns = Self::namespace_of(&event);
        let mut store = self.signals.write();
        let ring = store.entry(ns).or_default();
        if ring.len() >= self.max_size {
            // FIFO eviction — O(1) with VecDeque, and confined to the
            // namespace that overflowed.
            ring.pop_front();
            counter!("chronix_signal_evicted_total").increment(1);
        }
        ring.push_back(event);
        counter!("chronix_signal_persisted_total").increment(1);
    }

    /// Every signal matching `predicate`, in the given scope.
    fn query(
        &self,
        scope: Option<&str>,
        predicate: impl Fn(&SignalEvent) -> bool,
    ) -> Vec<SignalEvent> {
        let store = self.signals.read();
        let rings: Vec<&VecDeque<SignalEvent>> = match scope {
            Some(ns) => store.get(&Some(ns.to_string())).into_iter().collect(),
            None => store.values().collect(),
        };
        rings
            .into_iter()
            .flat_map(|r| r.iter())
            .filter(|s| predicate(s))
            .cloned()
            .collect()
    }

    /// Query signals by measurement, across every namespace.
    #[must_use]
    pub fn query_by_measurement(&self, measurement: &str) -> Vec<SignalEvent> {
        self.query(None, |s| s.measurement == measurement)
    }

    /// Query signals by severity, across every namespace.
    #[must_use]
    pub fn query_by_severity(&self, severity: crate::signal::model::Severity) -> Vec<SignalEvent> {
        self.query(None, |s| s.severity == severity)
    }

    /// Query signals within a time range, across every namespace.
    #[must_use]
    pub fn query_by_time_range(&self, min_ts: i64, max_ts: i64) -> Vec<SignalEvent> {
        self.query(None, |s| s.timestamp >= min_ts && s.timestamp <= max_ts)
    }

    /// All signals, across every namespace.
    #[must_use]
    pub fn all(&self) -> Vec<SignalEvent> {
        self.query(None, |_| true)
    }

    /// All signals in one namespace, or — with `None` — across every one.
    ///
    /// This is what the `/api/v1/signals` handler calls: reading one tenant's
    /// ring rather than reading everything and filtering is the difference
    /// between isolation and a filter that hides a capacity it still shares.
    #[must_use]
    pub fn all_in(&self, scope: Option<&str>) -> Vec<SignalEvent> {
        self.query(scope, |_| true)
    }

    /// Signal count across every namespace.
    #[must_use]
    pub fn len(&self) -> usize {
        self.signals.read().values().map(VecDeque::len).sum()
    }

    /// Whether the store is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many namespaces have signals.
    #[must_use]
    pub fn namespace_count(&self) -> usize {
        self.signals.read().len()
    }
}

impl Default for SignalStore {
    fn default() -> Self {
        Self::new(100_000)
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
            delivery_targets: Vec::new(),
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
        // Loopback is a forbidden target by default, so this test has to say
        // it means it — which is the whole point of the flag being named.
        let config = WebhookConfig::new("http://127.0.0.1:1/hook", "my-secret")
            .with_timeout(Duration::from_secs(1))
            .allow_private_targets(true);
        let ch = WebhookChannel::new(config).expect("failed to build webhook client");
        let event = test_signal("t1", "cpu", Severity::Warning);
        // Real HTTP delivery to an unreachable host should return an error.
        let result = ch.deliver(&event);
        assert!(result.is_err(), "delivery to unreachable host should fail");
    }

    /// The default refuses the same target, and says how to mean it.
    #[test]
    fn a_webhook_channel_refuses_a_private_target_by_default() {
        let config = WebhookConfig::new("http://127.0.0.1:1/hook", "my-secret");
        let Err(err) = WebhookChannel::new(config) else {
            panic!("loopback must be refused unless asked for");
        };
        assert!(
            err.to_string().contains("allow_private_targets"),
            "the error must name the way out, got: {err}"
        );
    }

    #[test]
    fn webhook_standard_signature() {
        let sig = WebhookChannel::sign("secret", "msg_1", 1_700_000_000, b"hello");
        // base64 of a 32-byte HMAC-SHA256 digest, no padding stripped.
        assert_eq!(sig.len(), 44);
        // Deterministic.
        assert_eq!(
            sig,
            WebhookChannel::sign("secret", "msg_1", 1_700_000_000, b"hello")
        );
        // Every signed field changes the signature: the payload,
        assert_ne!(
            sig,
            WebhookChannel::sign("secret", "msg_1", 1_700_000_000, b"world")
        );
        // the message id (so one delivery's signature can't cover another's),
        assert_ne!(
            sig,
            WebhookChannel::sign("secret", "msg_2", 1_700_000_000, b"hello")
        );
        // the timestamp (so a captured request can't be replayed silently),
        assert_ne!(
            sig,
            WebhookChannel::sign("secret", "msg_1", 1_700_000_001, b"hello")
        );
        // and the key.
        assert_ne!(
            sig,
            WebhookChannel::sign("other", "msg_1", 1_700_000_000, b"hello")
        );
    }

    #[test]
    fn signal_event_wraps_in_a_cloudevents_envelope() {
        let event = test_signal("t1", "cpu", Severity::Critical);
        let envelope = CloudEvent::from_signal(&event);
        let json = serde_json::to_value(&envelope).unwrap();
        assert_eq!(json["specversion"], "1.0");
        assert_eq!(json["id"], event.event_id);
        assert_eq!(json["source"], "chronix:trigger/t1");
        assert_eq!(json["type"], "io.chronix.signal.fired");
        assert_eq!(json["data"]["trigger_id"], "t1");
        // `time` is the data point's own timestamp, RFC 3339, round-trippable.
        let time = json["time"].as_str().unwrap();
        let parsed = chrono::DateTime::parse_from_rfc3339(time).unwrap();
        assert_eq!(parsed.timestamp_nanos_opt(), Some(event.timestamp));
    }

    #[test]
    fn webhook_config_builder() {
        let config = WebhookConfig::new("https://example.com", "s3cret")
            .with_timeout(Duration::from_secs(30))
            .with_header("Authorization", "Bearer token");
        assert_eq!(config.url, "https://example.com");
        assert_eq!(config.signing_secrets, vec!["s3cret".to_string()]);
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
        assert_eq!(router.deliver(&event), 2);
        assert!(router.flush(Duration::from_secs(5)));
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
        // `deliver` queues; `flush` is how a caller waits for the outcome.
        assert_eq!(router.deliver(&event), 1, "queued to one channel");
        assert!(router.flush(Duration::from_secs(5)));
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
        assert_eq!(router.deliver(&event), 1, "queued to one channel");
        assert!(router.flush(Duration::from_secs(5)));
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

    #[test]
    fn test_backoff_capped_at_60s() {
        let policy = RetryPolicy {
            max_retries: 100,
            base_backoff: Duration::from_secs(1),
        };
        let backoff = (policy.base_backoff * 2u32.saturating_pow(100)).min(MAX_BACKOFF);
        assert_eq!(backoff, Duration::from_secs(60));
    }

    /// A noisy tenant cannot evict a quiet one's signals.
    ///
    /// The store was one process-wide ring filtered on the way out, so its
    /// capacity was shared: a tenant firing signals quickly pushed out a quiet
    /// tenant's, and the quiet tenant simply saw fewer of its own with nothing
    /// to say why.
    #[test]
    fn one_namespace_cannot_evict_anothers_signals() {
        let store = SignalStore::new(4);

        let with_ns = |ns: &str, trigger: &str| {
            let mut e = test_signal(trigger, "cpu", Severity::Warning);
            e.tags
                .insert(chronix_core::NAMESPACE_TAG.to_string(), ns.to_string());
            e
        };

        // A quiet tenant stores one signal.
        store.store(with_ns("quiet", "q1"));
        // A noisy one fills and overflows its own ring many times over.
        for i in 0..50 {
            store.store(with_ns("noisy", &format!("n{i}")));
        }

        let quiet = store.all_in(Some("quiet"));
        assert_eq!(quiet.len(), 1, "the quiet tenant's signal was evicted");
        assert!(
            quiet[0].trigger_name.ends_with("q1"),
            "{}",
            quiet[0].trigger_name
        );

        // The noisy tenant is bounded by its own capacity, not by the total.
        assert_eq!(store.all_in(Some("noisy")).len(), 4);
        assert_eq!(store.namespace_count(), 2);
    }

    /// A single-tenant deployment has one unnamed ring and behaves as before.
    #[test]
    fn an_untagged_signal_uses_the_unnamed_ring() {
        let store = SignalStore::new(2);
        for i in 0..5 {
            store.store(test_signal(&format!("t{i}"), "cpu", Severity::Info));
        }
        assert_eq!(store.len(), 2);
        assert_eq!(store.all_in(None).len(), 2);
        assert_eq!(store.namespace_count(), 1);
    }

    /// Delivery is queued, and `flush` is how a caller waits for it.
    #[test]
    fn deliver_queues_and_flush_waits() {
        let router = DeliveryRouter::new();
        router.add_channel(Box::new(LogChannel));

        let event = test_signal("t1", "cpu", Severity::Warning);
        assert_eq!(router.deliver(&event), 1, "queued to one channel");
        assert!(router.flush(Duration::from_secs(5)), "the queue must drain");
    }

    /// Retries happen on the channel's worker and end in the DLQ.
    #[test]
    fn exhausted_retries_reach_the_dlq_without_blocking_the_caller() {
        let fail_count = Arc::new(AtomicU32::new(0));
        let router = DeliveryRouter::new().with_retry_policy(RetryPolicy {
            max_retries: 2,
            base_backoff: Duration::from_millis(400),
        });
        router.add_channel(Box::new(FailingChannel {
            fail_count: Arc::clone(&fail_count),
            fail_first_n: 100,
        }));

        let event = test_signal("t1", "cpu", Severity::Critical);
        let started = std::time::Instant::now();
        assert_eq!(router.deliver(&event), 1);
        // The backoff schedule is 400 ms + 800 ms and `deliver` must not have
        // paid for any of it — this is the defect the worker exists for. The
        // budget is an order of magnitude under the first sleep rather than
        // just under it, because a wall-clock assertion with a thin margin is
        // a test that fails on a busy machine and teaches people to re-run.
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "deliver blocked for {:?}",
            started.elapsed()
        );

        assert!(router.flush(Duration::from_secs(5)));
        assert_eq!(router.dlq().len(), 1);
    }

    /// A slow channel does not hold up a healthy one.
    #[test]
    fn a_failing_channel_does_not_delay_a_healthy_one() {
        let delivered = Arc::new(AtomicU32::new(0));
        let router = DeliveryRouter::new().with_retry_policy(RetryPolicy {
            max_retries: 3,
            base_backoff: Duration::from_millis(50),
        });
        router.add_channel(Box::new(FailingChannel {
            fail_count: Arc::new(AtomicU32::new(0)),
            fail_first_n: 100,
        }));
        router.add_channel(Box::new(CountingChannel {
            name: "healthy".to_string(),
            count: Arc::clone(&delivered),
        }));

        // Targets both channels.
        let event = test_signal("t1", "cpu", Severity::Info);
        router.deliver(&event);

        // The healthy channel lands long before the failing one has finished
        // its 50+100+200 ms schedule.
        let deadline = std::time::Instant::now() + Duration::from_millis(200);
        while delivered.load(std::sync::atomic::Ordering::Relaxed) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the healthy channel waited on the failing one"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// A queue that fills drops the oldest rather than blocking ingestion.
    #[test]
    fn a_full_queue_drops_the_oldest_and_keeps_accepting() {
        let router = DeliveryRouter::new()
            .with_queue_capacity(4)
            .with_retry_policy(RetryPolicy {
                max_retries: 0,
                base_backoff: Duration::from_millis(1),
            });
        router.add_channel(Box::new(BlockingChannel {
            release: Arc::new((Mutex::new(false), Condvar::new())),
        }));

        // The worker is stuck on the first event; the rest pile up behind it.
        for _ in 0..50 {
            let event = test_signal("t1", "cpu", Severity::Info);
            assert_eq!(
                router.deliver(&event),
                1,
                "a full queue must keep accepting"
            );
        }
    }

    /// A channel that blocks until told otherwise, for the queue tests.
    struct BlockingChannel {
        release: Arc<(Mutex<bool>, Condvar)>,
    }

    impl DeliveryChannel for BlockingChannel {
        fn name(&self) -> &str {
            "blocking"
        }
        fn deliver(&self, _event: &SignalEvent) -> Result<()> {
            let (lock, cv) = &*self.release;
            let mut released = lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut waited = Duration::ZERO;
            while !*released && waited < Duration::from_secs(2) {
                let (guard, _) = cv
                    .wait_timeout(released, Duration::from_millis(20))
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                released = guard;
                waited += Duration::from_millis(20);
            }
            Ok(())
        }
    }

    /// A channel that counts what reaches it.
    struct CountingChannel {
        name: String,
        count: Arc<AtomicU32>,
    }

    impl DeliveryChannel for CountingChannel {
        fn name(&self) -> &str {
            &self.name
        }
        fn deliver(&self, _event: &SignalEvent) -> Result<()> {
            self.count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }
}

#[cfg(test)]
mod rotation_tests {
    use super::*;

    /// During a rotation both secrets verify, which is the whole point.
    ///
    /// Standard Webhooks makes `webhook-signature` a **space-delimited list**
    /// for exactly this: a sender signing with the old and the new secret can
    /// be updated before, after, or at the same time as its receivers. With
    /// one secret there is no window at all — every in-flight delivery fails
    /// the moment either side changes — which is how a routine rotation gets
    /// postponed until it is urgent.
    #[test]
    fn a_rotation_signs_with_every_active_secret() {
        let config = WebhookConfig::with_secrets(
            "https://example.test/hook",
            vec!["new-secret".to_string(), "old-secret".to_string()],
        );
        let header = config.signature_header("msg_1", 1_700_000_000, b"{}");

        let parts: Vec<&str> = header.split(' ').collect();
        assert_eq!(parts.len(), 2, "one signature per secret: {header}");

        // Each is the signature a receiver holding that one secret computes.
        for (part, secret) in parts.iter().zip(["new-secret", "old-secret"]) {
            let expected = format!(
                "v1,{}",
                WebhookChannel::sign(secret, "msg_1", 1_700_000_000, b"{}")
            );
            assert_eq!(*part, expected);
        }

        // A receiver that has only ever seen the old secret still verifies —
        // it looks for its own signature among the list.
        let old_only = format!(
            "v1,{}",
            WebhookChannel::sign("old-secret", "msg_1", 1_700_000_000, b"{}")
        );
        assert!(header.split(' ').any(|p| p == old_only));
    }

    /// The ordinary case is one secret and one signature.
    #[test]
    fn one_secret_produces_one_signature() {
        let config = WebhookConfig::new("https://example.test/hook", "only");
        let header = config.signature_header("m", 1, b"body");
        assert!(
            !header.contains(' '),
            "no list for a single secret: {header}"
        );
        assert!(header.starts_with("v1,"));
    }

    /// A signature covers the id and the timestamp, not the body alone.
    #[test]
    fn the_signature_binds_the_id_and_the_timestamp() {
        let config = WebhookConfig::new("https://example.test/hook", "k");
        let base = config.signature_header("m1", 100, b"body");
        assert_ne!(base, config.signature_header("m2", 100, b"body"));
        assert_ne!(base, config.signature_header("m1", 101, b"body"));
        assert_ne!(base, config.signature_header("m1", 100, b"other"));
    }
}
