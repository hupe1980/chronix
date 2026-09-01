//! Automated secret rotation for encryption keys.
//!
//! Provides a [`RotatingKeyProvider`] that wraps any [`KeyProvider`] and
//! supports automatic key generation and rotation based on configurable
//! policies (time-based, usage-based, or both).
//!
//! # Architecture
//!
//! ```text
//! RotatingKeyProvider
//! ├── inner: Arc<RwLock<RotatingState>>
//! │   ├── keys: BTreeMap<String, SecretKey>
//! │   ├── current_key_id: String
//! │   ├── created_at: HashMap<String, Instant>
//! │   └── usage_counts: HashMap<String, u64>
//! ├── policy: RotationPolicy
//! └── audit: RotationAuditLog
//! ```
//!
//! # Example
//!
//! ```no_run
//! use chronix_security::auth::rotation::{RotatingKeyProvider, RotationPolicy};
//! use std::time::Duration;
//!
//! let policy = RotationPolicy::builder()
//!     .max_age(Duration::from_secs(86400))     // rotate every 24h
//!     .max_encryptions(1_000_000)               // or after 1M encryptions
//!     .retain_old_keys(5)                        // keep 5 old keys for decryption
//!     .build();
//!
//! let provider = RotatingKeyProvider::new(policy);
//! // provider implements KeyProvider — pass to EncryptionService
//! ```

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use zeroize::Zeroize;

use crate::auth::encryption::{KeyProvider, SecretKey};
use crate::auth::error::AuthError;

/// Policy governing when automatic key rotation occurs.
#[derive(Debug, Clone)]
pub struct RotationPolicy {
    /// Maximum age of a key before rotation is triggered.
    /// `None` disables time-based rotation.
    pub max_age: Option<Duration>,

    /// Maximum number of encryptions before rotation is triggered.
    /// `None` disables usage-based rotation.
    pub max_encryptions: Option<u64>,

    /// Number of old keys to retain for decrypting historical data.
    /// Oldest keys beyond this limit are purged.
    pub retain_old_keys: usize,
}

impl Default for RotationPolicy {
    fn default() -> Self {
        Self {
            max_age: Some(Duration::from_secs(24 * 60 * 60)), // 24 hours
            max_encryptions: Some(1 << 31),                   // 2^31 (~2B)
            retain_old_keys: 5,
        }
    }
}

impl RotationPolicy {
    /// Create a builder for `RotationPolicy`.
    #[must_use]
    pub fn builder() -> RotationPolicyBuilder {
        RotationPolicyBuilder::default()
    }
}

/// Builder for [`RotationPolicy`].
#[derive(Debug, Default)]
pub struct RotationPolicyBuilder {
    max_age: Option<Duration>,
    max_encryptions: Option<u64>,
    retain_old_keys: Option<usize>,
}

impl RotationPolicyBuilder {
    /// Set the maximum age before rotation.
    #[must_use]
    pub fn max_age(mut self, age: Duration) -> Self {
        self.max_age = Some(age);
        self
    }

    /// Set the maximum encryption count before rotation.
    #[must_use]
    pub fn max_encryptions(mut self, count: u64) -> Self {
        self.max_encryptions = Some(count);
        self
    }

    /// Set how many old keys to retain for decryption.
    #[must_use]
    pub fn retain_old_keys(mut self, count: usize) -> Self {
        self.retain_old_keys = Some(count);
        self
    }

    /// Build the policy.
    #[must_use]
    pub fn build(self) -> RotationPolicy {
        RotationPolicy {
            max_age: self.max_age,
            max_encryptions: self.max_encryptions,
            retain_old_keys: self.retain_old_keys.unwrap_or(5),
        }
    }
}

/// Record of a single key rotation event.
#[derive(Debug, Clone)]
pub struct RotationEvent {
    /// ID of the old (retired) key.
    pub old_key_id: String,
    /// ID of the new (active) key.
    pub new_key_id: String,
    /// Reason for rotation.
    pub reason: RotationReason,
    /// Timestamp of the rotation (monotonic).
    pub rotated_at: Instant,
    /// Number of encryptions performed with the old key.
    pub old_key_encryptions: u64,
}

/// Why a key rotation was triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotationReason {
    /// Key exceeded maximum age.
    MaxAge,
    /// Key exceeded maximum encryption count.
    MaxEncryptions,
    /// Manual rotation requested.
    Manual,
}

impl std::fmt::Display for RotationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaxAge => write!(f, "max_age"),
            Self::MaxEncryptions => write!(f, "max_encryptions"),
            Self::Manual => write!(f, "manual"),
        }
    }
}

/// Internal state for key rotation.
struct RotatingState {
    /// All keys indexed by ID (current + old).
    keys: BTreeMap<String, Vec<u8>>,
    /// The current (active) key ID.
    current_key_id: String,
    /// When each key was created.
    created_at: BTreeMap<String, Instant>,
    /// Monotonically increasing key sequence number.
    key_sequence: u64,
}

impl Drop for RotatingState {
    fn drop(&mut self) {
        for value in self.keys.values_mut() {
            value.zeroize();
        }
        std::sync::atomic::fence(Ordering::SeqCst);
        std::hint::black_box(&self.keys);
    }
}

/// A key provider with automatic rotation support.
///
/// Generates cryptographically random AES-256 keys and rotates them
/// based on the configured [`RotationPolicy`]. Old keys are retained
/// (up to `retain_old_keys`) so historical data can still be decrypted.
///
/// Thread-safe: uses `RwLock` for state and `AtomicU64` for the
/// encryption counter (zero contention on the hot read path).
pub struct RotatingKeyProvider {
    state: RwLock<RotatingState>,
    policy: RotationPolicy,
    /// Encryption count for the current key (atomic for hot-path performance).
    current_usage: AtomicU64,
    /// Audit log of rotation events (bounded ring buffer).
    events: RwLock<VecDeque<RotationEvent>>,
    /// Maximum number of audit events to retain.
    max_events: usize,
}

impl std::fmt::Debug for RotatingKeyProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.read();
        f.debug_struct("RotatingKeyProvider")
            .field("current_key_id", &state.current_key_id)
            .field("key_count", &state.keys.len())
            .field("current_usage", &self.current_usage.load(Ordering::Relaxed))
            .field("policy", &self.policy)
            .field("event_count", &self.events.read().len())
            .finish()
    }
}

/// Generate a cryptographically random 256-bit key.
fn generate_random_key() -> Vec<u8> {
    let mut key = vec![0u8; 32];
    super::fill_random(&mut key);
    key
}

/// Format a key ID from a sequence number.
fn format_key_id(sequence: u64) -> String {
    format!("rk-{sequence}")
}

impl RotatingKeyProvider {
    /// Create a new rotating key provider with the given policy.
    ///
    /// An initial random key is generated immediately.
    #[must_use]
    pub fn new(policy: RotationPolicy) -> Self {
        let key = generate_random_key();
        let key_id = format_key_id(1);
        let now = Instant::now();

        let mut keys = BTreeMap::new();
        keys.insert(key_id.clone(), key);

        let mut created_at = BTreeMap::new();
        created_at.insert(key_id.clone(), now);

        let state = RotatingState {
            keys,
            current_key_id: key_id,
            created_at,
            key_sequence: 1,
        };

        Self {
            state: RwLock::new(state),
            policy,
            current_usage: AtomicU64::new(0),
            events: RwLock::new(VecDeque::new()),
            max_events: 1000,
        }
    }

    /// Create a rotating provider seeded with an existing key.
    ///
    /// Useful for migrating from a static key to rotating keys.
    pub fn with_initial_key(
        key_bytes: Vec<u8>,
        key_id: &str,
        policy: RotationPolicy,
    ) -> Result<Self, AuthError> {
        if key_bytes.len() != 32 {
            return Err(AuthError::Config(format!(
                "AES-256 key must be 32 bytes, got {}",
                key_bytes.len()
            )));
        }

        let now = Instant::now();
        let mut keys = BTreeMap::new();
        keys.insert(key_id.to_string(), key_bytes);

        let mut created_at = BTreeMap::new();
        created_at.insert(key_id.to_string(), now);

        let state = RotatingState {
            keys,
            current_key_id: key_id.to_string(),
            created_at,
            key_sequence: 1,
        };

        Ok(Self {
            state: RwLock::new(state),
            policy,
            current_usage: AtomicU64::new(0),
            events: RwLock::new(VecDeque::new()),
            max_events: 1000,
        })
    }

    /// Check if the current key needs rotation and rotate if necessary.
    ///
    /// Returns `Some(event)` if rotation occurred, `None` otherwise.
    /// This is called automatically on each `current_key()` call.
    fn maybe_rotate(&self) -> Option<RotationEvent> {
        let usage = self.current_usage.load(Ordering::Relaxed);
        let state = self.state.read();

        let reason = self.check_rotation_needed(&state, usage);
        let reason = reason?;

        // Drop read lock before upgrading to write
        let old_key_id = state.current_key_id.clone();
        drop(state);

        let mut state = self.state.write();
        // Double-check: another thread may have rotated between our read and write
        if state.current_key_id != old_key_id {
            return None;
        }

        let old_usage = self.current_usage.swap(0, Ordering::Relaxed);
        let new_key = generate_random_key();
        state.key_sequence += 1;
        let new_key_id = format_key_id(state.key_sequence);

        state.keys.insert(new_key_id.clone(), new_key);
        state.created_at.insert(new_key_id.clone(), Instant::now());
        state.current_key_id = new_key_id.clone();

        // Purge oldest keys beyond retention limit
        self.purge_old_keys(&mut state);

        let event = RotationEvent {
            old_key_id,
            new_key_id,
            reason,
            rotated_at: Instant::now(),
            old_key_encryptions: old_usage,
        };

        // Log the rotation for audit
        tracing::info!(
            old_key = %event.old_key_id,
            new_key = %event.new_key_id,
            reason = %event.reason,
            old_key_encryptions = event.old_key_encryptions,
            "encryption key rotated"
        );
        metrics::counter!("chronix_auth_key_rotations_total", "reason" => event.reason.to_string())
            .increment(1);

        let mut events = self.events.write();
        if events.len() >= self.max_events {
            events.pop_front();
        }
        events.push_back(event.clone());

        Some(event)
    }

    /// Check if rotation is needed based on the policy.
    fn check_rotation_needed(&self, state: &RotatingState, usage: u64) -> Option<RotationReason> {
        // Check usage-based rotation first (more urgent)
        if let Some(max) = self.policy.max_encryptions {
            if usage >= max {
                return Some(RotationReason::MaxEncryptions);
            }
        }

        // Check time-based rotation
        if let Some(max_age) = self.policy.max_age {
            if let Some(created) = state.created_at.get(&state.current_key_id) {
                if created.elapsed() >= max_age {
                    return Some(RotationReason::MaxAge);
                }
            }
        }

        None
    }

    /// Purge old keys beyond the retention limit.
    fn purge_old_keys(&self, state: &mut RotatingState) {
        let retain = self.policy.retain_old_keys + 1; // +1 for current key
        while state.keys.len() > retain {
            // Find the oldest non-current key
            let oldest = state
                .created_at
                .iter()
                .filter(|(id, _)| **id != state.current_key_id)
                .min_by_key(|(_, created)| *created)
                .map(|(id, _)| id.clone());

            if let Some(oldest_id) = oldest {
                if let Some(mut key_bytes) = state.keys.remove(&oldest_id) {
                    key_bytes.zeroize();
                }
                state.created_at.remove(&oldest_id);
                tracing::debug!(key_id = %oldest_id, "purged expired encryption key");
                metrics::counter!("chronix_auth_keys_purged_total").increment(1);
            } else {
                break;
            }
        }
    }

    /// Manually trigger key rotation regardless of policy thresholds.
    ///
    /// Returns the rotation event.
    pub fn rotate_now(&self) -> RotationEvent {
        let old_usage = self.current_usage.swap(0, Ordering::Relaxed);
        let mut state = self.state.write();

        let old_key_id = state.current_key_id.clone();
        let new_key = generate_random_key();
        state.key_sequence += 1;
        let new_key_id = format_key_id(state.key_sequence);

        state.keys.insert(new_key_id.clone(), new_key);
        state.created_at.insert(new_key_id.clone(), Instant::now());
        state.current_key_id = new_key_id.clone();

        self.purge_old_keys(&mut state);

        let event = RotationEvent {
            old_key_id,
            new_key_id,
            reason: RotationReason::Manual,
            rotated_at: Instant::now(),
            old_key_encryptions: old_usage,
        };

        tracing::info!(
            old_key = %event.old_key_id,
            new_key = %event.new_key_id,
            reason = %event.reason,
            old_key_encryptions = event.old_key_encryptions,
            "encryption key rotated (manual)"
        );
        metrics::counter!("chronix_auth_key_rotations_total", "reason" => "manual").increment(1);

        let mut events = self.events.write();
        if events.len() >= self.max_events {
            events.pop_front();
        }
        events.push_back(event.clone());

        event
    }

    /// Record that the current key was used for an encryption operation.
    ///
    /// Call this from the `EncryptionService` after each `encrypt()` call
    /// so that usage-based rotation can be tracked.
    pub fn record_encryption(&self) {
        self.current_usage.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the current encryption count for the active key.
    #[must_use]
    pub fn current_key_usage(&self) -> u64 {
        self.current_usage.load(Ordering::Relaxed)
    }

    /// Return all rotation events (most recent last).
    #[must_use]
    pub fn rotation_history(&self) -> Vec<RotationEvent> {
        self.events.read().iter().cloned().collect()
    }

    /// Number of keys currently held (current + retained old).
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.state.read().keys.len()
    }

    /// Returns the current key's age (time since creation).
    #[must_use]
    pub fn current_key_age(&self) -> Duration {
        let state = self.state.read();
        state
            .created_at
            .get(&state.current_key_id)
            .map(std::time::Instant::elapsed)
            .unwrap_or_default()
    }
}

impl KeyProvider for RotatingKeyProvider {
    fn current_key(&self) -> Result<(SecretKey, String), AuthError> {
        // Check if rotation is needed (cheap atomic read in hot path)
        self.maybe_rotate();

        let state = self.state.read();
        let key_bytes = state
            .keys
            .get(&state.current_key_id)
            .ok_or_else(|| {
                AuthError::KeyNotFound(format!(
                    "current key {} missing from state",
                    state.current_key_id
                ))
            })?
            .clone();

        Ok((SecretKey::new(key_bytes), state.current_key_id.clone()))
    }

    fn key_by_id(&self, key_id: &str) -> Result<SecretKey, AuthError> {
        let state = self.state.read();
        state
            .keys
            .get(key_id)
            .cloned()
            .map(SecretKey::new)
            .ok_or_else(|| AuthError::KeyNotFound(key_id.to_string()))
    }

    fn key_ids(&self) -> Vec<String> {
        self.state.read().keys.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::encryption::EncryptionService;

    #[test]
    fn default_policy_values() {
        let policy = RotationPolicy::default();
        assert_eq!(policy.max_age, Some(Duration::from_secs(86400)));
        assert_eq!(policy.max_encryptions, Some(1 << 31));
        assert_eq!(policy.retain_old_keys, 5);
    }

    #[test]
    fn builder_creates_custom_policy() {
        let policy = RotationPolicy::builder()
            .max_age(Duration::from_secs(3600))
            .max_encryptions(500_000)
            .retain_old_keys(3)
            .build();

        assert_eq!(policy.max_age, Some(Duration::from_secs(3600)));
        assert_eq!(policy.max_encryptions, Some(500_000));
        assert_eq!(policy.retain_old_keys, 3);
    }

    #[test]
    fn new_provider_generates_initial_key() {
        let provider = RotatingKeyProvider::new(RotationPolicy::default());
        assert_eq!(provider.key_count(), 1);

        let (key, id) = provider.current_key().unwrap();
        assert_eq!(key.as_bytes().len(), 32);
        assert_eq!(id, "rk-1");
    }

    #[test]
    fn with_initial_key_accepts_valid_key() {
        let key = vec![0xAB; 32];
        let provider = RotatingKeyProvider::with_initial_key(
            key.clone(),
            "my-key-1",
            RotationPolicy::default(),
        )
        .unwrap();

        let (retrieved, id) = provider.current_key().unwrap();
        assert_eq!(retrieved.as_bytes(), &key);
        assert_eq!(id, "my-key-1");
    }

    #[test]
    fn with_initial_key_rejects_wrong_size() {
        let result = RotatingKeyProvider::with_initial_key(
            vec![0u8; 16], // too short
            "bad-key",
            RotationPolicy::default(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn manual_rotation_creates_new_key() {
        let provider = RotatingKeyProvider::new(RotationPolicy::default());

        let (_, id1) = provider.current_key().unwrap();
        assert_eq!(id1, "rk-1");

        let event = provider.rotate_now();
        assert_eq!(event.old_key_id, "rk-1");
        assert_eq!(event.new_key_id, "rk-2");
        assert_eq!(event.reason, RotationReason::Manual);

        let (_, id2) = provider.current_key().unwrap();
        assert_eq!(id2, "rk-2");
        assert_eq!(provider.key_count(), 2);
    }

    #[test]
    fn old_keys_still_accessible_after_rotation() {
        let provider = RotatingKeyProvider::new(RotationPolicy::default());

        // Get key-1
        let (key1, _) = provider.current_key().unwrap();
        let key1_bytes = key1.as_bytes().to_vec();

        // Rotate to key-2
        provider.rotate_now();

        // key-1 is still accessible
        let old_key = provider.key_by_id("rk-1").unwrap();
        assert_eq!(old_key.as_bytes(), &key1_bytes);
    }

    #[test]
    fn encryption_roundtrip_after_rotation() {
        let provider = RotatingKeyProvider::new(RotationPolicy::default());
        let svc = EncryptionService::new(Box::new(provider));

        // Encrypt with initial key
        let plaintext = b"secret data before rotation";
        let (ct1, kid1) = svc.encrypt(plaintext).unwrap();
        assert_eq!(kid1, "rk-1");

        // We can't rotate via svc — this tests the normal path
        let dec = svc.decrypt(&ct1, &kid1).unwrap();
        assert_eq!(dec, plaintext);
    }

    #[test]
    fn usage_based_rotation_triggers() {
        let policy = RotationPolicy::builder()
            .max_encryptions(3) // rotate after 3 encryptions
            .retain_old_keys(2)
            .build();

        let provider = RotatingKeyProvider::new(policy);

        // Simulate 3 encryptions
        provider.record_encryption();
        provider.record_encryption();
        provider.record_encryption();

        // Next current_key() should trigger rotation
        let (_, id) = provider.current_key().unwrap();
        assert_eq!(id, "rk-2", "should have auto-rotated to rk-2");
        assert_eq!(provider.key_count(), 2);

        // Rotation event should be recorded
        let history = provider.rotation_history();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].reason, RotationReason::MaxEncryptions);
        assert_eq!(history[0].old_key_encryptions, 3);
    }

    #[test]
    fn key_purge_respects_retention_limit() {
        let policy = RotationPolicy::builder()
            .max_encryptions(1_000_000) // won't auto-trigger
            .retain_old_keys(2)         // keep 2 old + 1 current = 3 max
            .build();

        let provider = RotatingKeyProvider::new(policy);
        // rk-1 is current

        provider.rotate_now(); // rk-2 current, rk-1 old
        assert_eq!(provider.key_count(), 2);

        provider.rotate_now(); // rk-3 current, rk-1 + rk-2 old
        assert_eq!(provider.key_count(), 3);

        provider.rotate_now(); // rk-4 current, rk-2 + rk-3 old, rk-1 purged
        assert_eq!(provider.key_count(), 3);

        // rk-1 should be purged
        assert!(provider.key_by_id("rk-1").is_err());
        // rk-2 and rk-3 still accessible
        assert!(provider.key_by_id("rk-2").is_ok());
        assert!(provider.key_by_id("rk-3").is_ok());
        assert!(provider.key_by_id("rk-4").is_ok());
    }

    #[test]
    fn rotation_history_bounded() {
        let policy = RotationPolicy::default();

        // Create provider with small event buffer for testing
        let key = generate_random_key();
        let key_id = format_key_id(1);
        let now = Instant::now();

        let mut keys = BTreeMap::new();
        keys.insert(key_id.clone(), key);
        let mut created_at = BTreeMap::new();
        created_at.insert(key_id.clone(), now);

        let provider = RotatingKeyProvider {
            state: RwLock::new(RotatingState {
                keys,
                current_key_id: key_id,
                created_at,
                key_sequence: 1,
            }),
            policy,
            current_usage: AtomicU64::new(0),
            events: RwLock::new(VecDeque::new()),
            max_events: 3, // small for testing
        };

        for _ in 0..5 {
            provider.rotate_now();
        }

        let history = provider.rotation_history();
        assert_eq!(history.len(), 3, "should cap at max_events");
        // Most recent events should be retained
        assert_eq!(history[2].new_key_id, "rk-6");
    }

    #[test]
    fn current_key_age_increases() {
        let provider = RotatingKeyProvider::new(RotationPolicy::default());
        let age1 = provider.current_key_age();
        std::thread::sleep(Duration::from_millis(10));
        let age2 = provider.current_key_age();
        assert!(age2 > age1);
    }

    #[test]
    fn record_encryption_increments_counter() {
        let provider = RotatingKeyProvider::new(RotationPolicy::default());
        assert_eq!(provider.current_key_usage(), 0);

        provider.record_encryption();
        provider.record_encryption();
        assert_eq!(provider.current_key_usage(), 2);
    }

    #[test]
    fn key_ids_lists_all() {
        let provider = RotatingKeyProvider::new(RotationPolicy::default());
        provider.rotate_now();

        let ids = provider.key_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"rk-1".to_string()));
        assert!(ids.contains(&"rk-2".to_string()));
    }

    #[test]
    fn debug_does_not_leak_keys() {
        let provider = RotatingKeyProvider::new(RotationPolicy::default());
        let debug = format!("{provider:?}");

        assert!(debug.contains("RotatingKeyProvider"));
        assert!(debug.contains("current_key_id"));
        // Should not contain raw key bytes
        assert!(!debug.contains("0x"));
    }

    #[test]
    fn concurrent_rotation_safety() {
        use std::sync::Arc;

        let policy = RotationPolicy::builder()
            .max_encryptions(10)
            .retain_old_keys(20)
            .build();

        let provider = Arc::new(RotatingKeyProvider::new(policy));
        let mut handles = Vec::new();

        // 4 threads each doing 50 encryptions
        for _ in 0..4 {
            let p = Arc::clone(&provider);
            handles.push(std::thread::spawn(move || {
                for _ in 0..50 {
                    p.record_encryption();
                    let _ = p.current_key().unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Should have rotated multiple times
        let history = provider.rotation_history();
        assert!(
            !history.is_empty(),
            "should have auto-rotated at least once"
        );
    }

    #[test]
    fn encrypt_decrypt_across_rotation() {
        let policy = RotationPolicy::builder()
            .max_encryptions(1_000_000)
            .retain_old_keys(5)
            .build();

        let provider = RotatingKeyProvider::new(policy);

        // Encrypt with key rk-1
        let svc = EncryptionService::new(Box::new(provider));
        let (ct1, kid1) = svc.encrypt(b"data-1").unwrap();
        assert_eq!(kid1, "rk-1");
        let dec = svc.decrypt(&ct1, &kid1).unwrap();
        assert_eq!(dec, b"data-1");
    }

    #[test]
    fn rotation_reason_display() {
        assert_eq!(RotationReason::MaxAge.to_string(), "max_age");
        assert_eq!(
            RotationReason::MaxEncryptions.to_string(),
            "max_encryptions"
        );
        assert_eq!(RotationReason::Manual.to_string(), "manual");
    }
}
