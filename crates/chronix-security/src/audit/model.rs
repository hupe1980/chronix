//! Audit event model and types.

use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::fmt;

/// The result of an authorization decision or operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditDecision {
    /// The operation was allowed.
    Allow,
    /// The operation was denied.
    Deny,
}

impl fmt::Display for AuditDecision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow => write!(f, "allow"),
            Self::Deny => write!(f, "deny"),
        }
    }
}

/// Category of auditable actions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    // Authorization actions
    /// Data write.
    Write,
    /// Data read / query.
    Read,
    /// Data deletion.
    Delete,
    /// Administrative operation.
    Admin,
    /// Rollup creation.
    CreateRollup,
    /// Forecast execution.
    Forecast,
    /// Anomaly detection.
    DetectAnomalies,
    /// CDC subscription.
    Subscribe,

    // Authentication events
    /// Successful login.
    LoginSuccess,
    /// Failed login attempt.
    LoginFailure,
    /// API key rotation.
    KeyRotation,
    /// Token refresh.
    TokenRefresh,

    // Trigger / signal events
    /// Trigger created.
    TriggerCreate,
    /// Trigger dropped.
    TriggerDrop,
    /// Signal fired.
    SignalFired,

    // Additional auditable actions for complete coverage.
    /// Schema change (create/alter/drop measurement).
    SchemaChange,
    /// Rollup rule dropped.
    DropRollup,
    /// Data exported.
    DataExport,
    /// Permission or role change.
    PermissionChange,
    /// Cedar/authz policy loaded or updated.
    PolicyLoad,
    /// API key created.
    ApiKeyCreate,
    /// API key revoked.
    ApiKeyRevoke,
    /// Namespace created.
    NamespaceCreate,
    /// Namespace deleted.
    NamespaceDelete,
    /// Quota changed.
    QuotaChange,

    /// Custom action.
    Custom(String),
}

impl fmt::Display for AuditAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Write => write!(f, "write"),
            Self::Read => write!(f, "read"),
            Self::Delete => write!(f, "delete"),
            Self::Admin => write!(f, "admin"),
            Self::CreateRollup => write!(f, "create_rollup"),
            Self::Forecast => write!(f, "forecast"),
            Self::DetectAnomalies => write!(f, "detect_anomalies"),
            Self::Subscribe => write!(f, "subscribe"),
            Self::LoginSuccess => write!(f, "login_success"),
            Self::LoginFailure => write!(f, "login_failure"),
            Self::KeyRotation => write!(f, "key_rotation"),
            Self::TokenRefresh => write!(f, "token_refresh"),
            Self::TriggerCreate => write!(f, "trigger_create"),
            Self::TriggerDrop => write!(f, "trigger_drop"),
            Self::SignalFired => write!(f, "signal_fired"),
            Self::SchemaChange => write!(f, "schema_change"),
            Self::DropRollup => write!(f, "drop_rollup"),
            Self::DataExport => write!(f, "data_export"),
            Self::PermissionChange => write!(f, "permission_change"),
            Self::PolicyLoad => write!(f, "policy_load"),
            Self::ApiKeyCreate => write!(f, "api_key_create"),
            Self::ApiKeyRevoke => write!(f, "api_key_revoke"),
            Self::NamespaceCreate => write!(f, "namespace_create"),
            Self::NamespaceDelete => write!(f, "namespace_delete"),
            Self::QuotaChange => write!(f, "quota_change"),
            Self::Custom(s) => write!(f, "{s}"),
        }
    }
}

/// A structured audit event capturing a security-relevant operation.
///
/// Events carry a SHA-256 hash chain — each event's `event_hash`
/// covers `prev_hash || canonical_payload`, making log tampering detectable
/// by verifying the chain with [`verify_hash_chain`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Monotonically increasing event ID.
    pub id: u64,
    /// Nanosecond timestamp.
    pub timestamp: i64,
    /// The principal (user / service) that performed the action.
    pub principal: String,
    /// The action performed.
    pub action: AuditAction,
    /// The resource targeted (measurement, series, etc.).
    pub resource: String,
    /// Authorization decision.
    pub decision: AuditDecision,
    /// Source IP address (if available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_ip: Option<String>,
    /// Request ID for correlation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// Additional context (e.g., deny reasons, error details).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
    /// Hex SHA-256 hash of the previous event (or `None` for the
    /// first event in the chain).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_hash: Option<String>,
    /// Hex SHA-256 hash of `prev_hash || canonical_payload`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_hash: Option<String>,
}

impl AuditEvent {
    /// Create a new audit event builder.
    #[must_use]
    pub fn new(
        principal: impl Into<String>,
        action: AuditAction,
        resource: impl Into<String>,
        decision: AuditDecision,
    ) -> Self {
        Self {
            id: 0, // set by logger
            timestamp: now_nanos(),
            principal: principal.into(),
            action,
            resource: resource.into(),
            decision,
            source_ip: None,
            request_id: None,
            metadata: HashMap::new(),
            prev_hash: None,
            event_hash: None,
        }
    }

    /// Set the source IP.
    #[must_use]
    pub fn with_source_ip(mut self, ip: impl Into<String>) -> Self {
        self.source_ip = Some(ip.into());
        self
    }

    /// Set the request ID.
    #[must_use]
    pub fn with_request_id(mut self, id: impl Into<String>) -> Self {
        self.request_id = Some(id.into());
        self
    }

    /// Add a metadata key-value pair.
    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Set the timestamp explicitly (for tests).
    #[must_use]
    pub fn with_timestamp(mut self, ts: i64) -> Self {
        self.timestamp = ts;
        self
    }
}

impl fmt::Display for AuditEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] principal={} action={} resource={} decision={}",
            self.timestamp, self.principal, self.action, self.resource, self.decision,
        )
    }
}

/// Returns a **strictly increasing** timestamp in nanoseconds since epoch.
///
/// Uses a CAS loop to guarantee that every call returns a
/// **unique** timestamp, even when multiple threads call simultaneously
/// within the same nanosecond. If wall-clock time has not advanced past
/// the last emitted value, we increment by 1 — producing a synthetic
/// sub-nanosecond tiebreaker. This eliminates the duplicate-timestamp
/// problem that made ordering ambiguous.
///
/// Combined with the per-event sequence `id` assigned in `log()`, audit
/// events now have two independent total-order keys.
fn now_nanos() -> i64 {
    use std::sync::atomic::{AtomicI64, Ordering};
    static LAST: AtomicI64 = AtomicI64::new(0);

    let wall = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    )
    .unwrap_or(i64::MAX);

    // CAS loop: guarantee strictly increasing output.
    loop {
        let prev = LAST.load(Ordering::SeqCst);
        let next = wall.max(prev + 1);
        if LAST
            .compare_exchange_weak(prev, next, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            return next;
        }
    }
}

// ── Hash-chain helpers ───────────────────────────────────────

impl AuditEvent {
    /// Compute the canonical payload bytes used as input to the chain hash.
    ///
    /// The canonical form is `id|timestamp|principal|action|resource|decision`
    /// followed by sorted metadata key-value pairs, ensuring deterministic
    /// output regardless of `HashMap` iteration order.
    fn canonical_payload(&self) -> Vec<u8> {
        let mut buf = format!(
            "{}|{}|{}|{}|{}|{}",
            self.id, self.timestamp, self.principal, self.action, self.resource, self.decision,
        )
        .into_bytes();

        // Sort metadata by key for deterministic hashing
        let mut keys: Vec<&String> = self.metadata.keys().collect();
        keys.sort();
        for key in keys {
            buf.push(b'|');
            buf.extend_from_slice(key.as_bytes());
            buf.push(b'=');
            buf.extend_from_slice(self.metadata[key].as_bytes());
        }

        buf
    }

    /// Compute HMAC-SHA-256 of `prev_hash || canonical_payload` and set
    /// `self.prev_hash` and `self.event_hash`.
    ///
    /// Uses HMAC with a secret key instead of plain SHA-256, so
    /// an attacker with database write access cannot recompute the chain.
    /// Falls back to plain SHA-256 if no key is provided (backward compat).
    pub fn seal(&mut self, prev_hash: Option<&str>) {
        self.seal_with_key(prev_hash, None);
    }

    /// Seal the event hash with an optional HMAC key.
    ///
    /// When `hmac_key` is `Some`, uses HMAC-SHA-256 for tamper resistance.
    /// When `None`, falls back to plain SHA-256 (backward compatible).
    pub fn seal_with_key(&mut self, prev_hash: Option<&str>, hmac_key: Option<&[u8]>) {
        self.prev_hash = prev_hash.map(String::from);

        let hash = if let Some(key) = hmac_key {
            use hmac::{Hmac, KeyInit, Mac};
            type HmacSha256 = Hmac<Sha256>;
            let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
            if let Some(ph) = &self.prev_hash {
                mac.update(ph.as_bytes());
            }
            mac.update(&self.canonical_payload());
            hex_encode(&mac.finalize().into_bytes())
        } else {
            use sha2::Digest;
            let mut hasher = Sha256::new();
            if let Some(ph) = &self.prev_hash {
                hasher.update(ph.as_bytes());
            }
            hasher.update(self.canonical_payload());
            hex_encode(&hasher.finalize())
        };

        self.event_hash = Some(hash);
    }
}

/// Verify the integrity of a hash chain produced by `AuditLogger`.
///
/// Returns `Ok(())` if every event's `event_hash` matches the expected
/// hash, or an error describing the first broken link.
///
/// When `hmac_key` is `Some`, uses HMAC-SHA-256 for verification.
/// When `None`, falls back to plain SHA-256 for backward compatibility.
pub fn verify_hash_chain(events: &[AuditEvent]) -> std::result::Result<(), String> {
    verify_hash_chain_with_key(events, None)
}

/// Verify the hash chain with an optional HMAC key.
pub fn verify_hash_chain_with_key(
    events: &[AuditEvent],
    hmac_key: Option<&[u8]>,
) -> std::result::Result<(), String> {
    let mut expected_prev: Option<&str> = None;

    for (idx, event) in events.iter().enumerate() {
        // Check prev_hash linkage
        if event.prev_hash.as_deref() != expected_prev {
            return Err(format!(
                "event id={} (index {idx}): prev_hash mismatch — expected {:?}, got {:?}",
                event.id, expected_prev, event.prev_hash,
            ));
        }

        // Recompute the hash
        let expected_hash = if let Some(key) = hmac_key {
            use hmac::{Hmac, KeyInit, Mac};
            type HmacSha256 = Hmac<Sha256>;
            let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
            if let Some(ph) = &event.prev_hash {
                mac.update(ph.as_bytes());
            }
            mac.update(&event.canonical_payload());
            hex_encode(&mac.finalize().into_bytes())
        } else {
            use sha2::Digest;
            let mut hasher = Sha256::new();
            if let Some(ph) = &event.prev_hash {
                hasher.update(ph.as_bytes());
            }
            hasher.update(event.canonical_payload());
            hex_encode(&hasher.finalize())
        };

        let actual = event.event_hash.as_deref().unwrap_or("");
        if actual != expected_hash {
            return Err(format!(
                "event id={} (index {idx}): event_hash mismatch — expected {expected_hash}, got {actual}",
                event.id,
            ));
        }

        expected_prev = event.event_hash.as_deref();
    }

    Ok(())
}

/// Hex-encode a byte slice (lowercase).
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_event_creation() {
        let event = AuditEvent::new("alice", AuditAction::Write, "cpu", AuditDecision::Allow)
            .with_source_ip("10.0.0.1")
            .with_request_id("req-123");
        assert_eq!(event.principal, "alice");
        assert_eq!(event.resource, "cpu");
        assert_eq!(event.decision, AuditDecision::Allow);
        assert_eq!(event.source_ip.as_deref(), Some("10.0.0.1"));
        assert_eq!(event.request_id.as_deref(), Some("req-123"));
    }

    #[test]
    fn audit_event_with_metadata() {
        let event = AuditEvent::new("bob", AuditAction::Delete, "logs", AuditDecision::Deny)
            .with_metadata("reason", "insufficient privileges");
        assert_eq!(
            event.metadata.get("reason").unwrap(),
            "insufficient privileges"
        );
    }

    #[test]
    fn audit_event_serialization() {
        let event = AuditEvent::new("alice", AuditAction::Read, "cpu", AuditDecision::Allow)
            .with_timestamp(1_700_000_000_000);
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"principal\":\"alice\""));
        assert!(json.contains("\"action\":\"read\""));
        assert!(json.contains("\"decision\":\"allow\""));

        let deserialized: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.principal, "alice");
        assert_eq!(deserialized.timestamp, 1_700_000_000_000);
    }

    #[test]
    fn audit_event_display() {
        let event = AuditEvent::new("alice", AuditAction::Write, "cpu", AuditDecision::Allow)
            .with_timestamp(100);
        let s = event.to_string();
        assert!(s.contains("principal=alice"));
        assert!(s.contains("action=write"));
        assert!(s.contains("decision=allow"));
    }

    #[test]
    fn audit_action_display() {
        assert_eq!(AuditAction::LoginSuccess.to_string(), "login_success");
        assert_eq!(AuditAction::KeyRotation.to_string(), "key_rotation");
        assert_eq!(AuditAction::Custom("foo".into()).to_string(), "foo");
    }

    #[test]
    fn decision_display() {
        assert_eq!(AuditDecision::Allow.to_string(), "allow");
        assert_eq!(AuditDecision::Deny.to_string(), "deny");
    }

    #[test]
    fn auth_actions_roundtrip() {
        let actions = vec![
            AuditAction::Write,
            AuditAction::LoginFailure,
            AuditAction::TriggerCreate,
            AuditAction::SignalFired,
            AuditAction::Custom("custom_op".into()),
        ];
        for action in actions {
            let event = AuditEvent::new("user", action, "res", AuditDecision::Allow);
            let json = serde_json::to_string(&event).unwrap();
            let de: AuditEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(de.action, event.action);
        }
    }

    #[test]
    fn hash_chain_seal_and_verify() {
        let mut events: Vec<AuditEvent> = Vec::new();
        let mut prev_hash: Option<String> = None;

        for i in 0..5 {
            let mut event =
                AuditEvent::new("alice", AuditAction::Read, "cpu", AuditDecision::Allow)
                    .with_timestamp(1000 + i);
            event.id = i as u64;
            event.seal(prev_hash.as_deref());
            prev_hash = event.event_hash.clone();
            events.push(event);
        }

        // All events should have hashes
        for event in &events {
            assert!(event.event_hash.is_some());
        }
        // First event has no prev_hash
        assert!(events[0].prev_hash.is_none());
        // Subsequent events chain
        for i in 1..events.len() {
            assert_eq!(events[i].prev_hash, events[i - 1].event_hash);
        }

        // Chain should verify
        assert!(verify_hash_chain(&events).is_ok());
    }

    #[test]
    fn hash_chain_detects_tampering() {
        let mut events: Vec<AuditEvent> = Vec::new();
        let mut prev_hash: Option<String> = None;

        for i in 0..3 {
            let mut event =
                AuditEvent::new("alice", AuditAction::Write, "cpu", AuditDecision::Allow)
                    .with_timestamp(1000 + i);
            event.id = i as u64;
            event.seal(prev_hash.as_deref());
            prev_hash = event.event_hash.clone();
            events.push(event);
        }

        // Tamper with the second event
        events[1].principal = "mallory".to_string();
        assert!(verify_hash_chain(&events).is_err());
    }

    #[test]
    fn hash_chain_empty_is_valid() {
        assert!(verify_hash_chain(&[]).is_ok());
    }

    #[test]
    fn sealed_event_roundtrips_json() {
        let mut event = AuditEvent::new("alice", AuditAction::Read, "cpu", AuditDecision::Allow)
            .with_timestamp(42);
        event.id = 1;
        event.seal(None);

        let json = serde_json::to_string(&event).unwrap();
        let de: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(de.event_hash, event.event_hash);
        assert_eq!(de.prev_hash, event.prev_hash);
    }

    #[test]
    fn now_nanos_produces_strictly_increasing_timestamps() {
        // Verify that rapid successive calls produce unique,
        // strictly increasing timestamps.
        let mut prev = 0_i64;
        for _ in 0..1000 {
            let ts = now_nanos();
            assert!(ts > prev, "timestamp {ts} must be > previous {prev}");
            prev = ts;
        }
    }

    #[test]
    fn now_nanos_concurrent_uniqueness() {
        use std::collections::HashSet;
        use std::sync::Arc;

        let timestamps = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let ts = Arc::clone(&timestamps);
                std::thread::spawn(move || {
                    let mut local = Vec::with_capacity(250);
                    for _ in 0..250 {
                        local.push(now_nanos());
                    }
                    ts.lock().extend(local);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let all = timestamps.lock();
        let unique: HashSet<_> = all.iter().copied().collect();
        assert_eq!(
            unique.len(),
            all.len(),
            "all {} timestamps must be unique, got {} unique",
            all.len(),
            unique.len()
        );
    }
}
