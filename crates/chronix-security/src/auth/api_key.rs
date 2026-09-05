//! API key authentication with Argon2 password hashing.
//!
//! Keys are stored as Argon2 hashes. Each key has a name, optional expiry,
//! and optional role. Multiple keys can be valid simultaneously for rotation.
//!
//! # Example
//!
//! ```no_run
//! use chronix_security::auth::api_key::ApiKeyStore;
//!
//! let mut store = ApiKeyStore::new();
//! let raw_key = store.create_key("admin", None).unwrap();
//! assert!(store.validate(&raw_key).is_ok());
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use dashmap::DashMap;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tracing::debug;
use zeroize::Zeroize;

use crate::auth::error::AuthError;

/// Compute the 8-byte prefix fingerprint of a raw API key.
///
/// This is a fast, non-cryptographic lookup key used to narrow the
/// candidate set before running the expensive Argon2 verification.
/// With 8 bytes (64 bits) the probability of a false prefix match
/// is ~$5.4 \times 10^{-20}$ per key pair, so the Argon2 scan runs
/// on at most 1 candidate in practice.
fn key_prefix(raw_key: &str) -> [u8; 8] {
    let hash = Sha256::digest(raw_key.as_bytes());
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&hash[..8]);
    prefix
}

/// A stored API key entry with metadata.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ApiKeyEntry {
    /// Human-readable name for this key.
    pub name: String,
    /// Argon2 hash of the key.
    pub hash: String,
    /// Optional Unix timestamp (seconds) when this key expires.
    pub expires_at: Option<u64>,
    /// When the key was created (Unix seconds).
    pub created_at: u64,
    /// Namespaces this key may act in.
    ///
    /// Empty means **unrestricted** — the key may name any namespace. That is
    /// the right default for a single-tenant deployment, where the header is
    /// always `default` and binding it would be ceremony. It is the wrong
    /// default for a multi-tenant one, which is why `chronixd` refuses to
    /// start multi-tenant with an unrestricted key rather than silently
    /// handing every tenant's data to every key.
    #[serde(default)]
    pub namespaces: Vec<String>,
    /// Whether this key may perform administrative operations.
    ///
    /// Restore, namespace management and key management are all reachable
    /// with an ordinary key otherwise, because the Cedar authorization
    /// engine is optional and its absence used to mean "permit".
    #[serde(default)]
    pub admin: bool,
}

impl ApiKeyEntry {
    /// Whether this key may act in `namespace`.
    #[must_use]
    pub fn allows_namespace(&self, namespace: &str) -> bool {
        self.namespaces.is_empty() || self.namespaces.iter().any(|n| n == namespace)
    }
}

/// In-memory store for API keys with Argon2 hashing.
///
/// Validation uses a two-stage lookup: a fast SHA-256 prefix narrows
/// the candidate set to (usually) one entry, avoiding a linear Argon2
/// scan that would otherwise take `O(N * ~100ms)` and create a
/// CPU-exhaustion denial-of-service vector.
///
/// **** Rate limiting is applied **per key prefix**, so brute-force
/// attempts targeting one key cannot lock out unrelated keys. A global
/// counter provides an additional safety net against distributed attacks.
#[derive(Clone)]
pub struct ApiKeyStore {
    keys: HashMap<String, ApiKeyEntry>,
    /// SHA-256 prefix → entry name(s) for O(1) candidate narrowing.
    prefix_index: HashMap<[u8; 8], Vec<String>>,
    /// Per-prefix failure counters — `(failure_count, window_start_epoch_secs)`.
    per_prefix_failures: Arc<DashMap<[u8; 8], (u64, u64)>>,
    /// Global consecutive failed validation count in the current window.
    failure_count: Arc<AtomicU64>,
    /// Epoch-second at which the current global failure window started.
    failure_window_start: Arc<AtomicU64>,
}

impl std::fmt::Debug for ApiKeyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKeyStore")
            .field("keys", &self.keys.len())
            .finish()
    }
}

/// Maximum failed validations allowed per sliding window (per-prefix and global).
const MAX_FAILURES_PER_WINDOW: u64 = 20;
/// Per-prefix failures threshold (lower than global for targeted brute-force isolation).
const MAX_PREFIX_FAILURES_PER_WINDOW: u64 = 5;
/// Sliding window duration in seconds.
const FAILURE_WINDOW_SECS: u64 = 60;

impl ApiKeyStore {
    /// Create a new empty key store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            keys: HashMap::new(),
            prefix_index: HashMap::new(),
            per_prefix_failures: Arc::new(DashMap::new()),
            failure_count: Arc::new(AtomicU64::new(0)),
            failure_window_start: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Check whether a key with the given name is registered (not revoked).
    ///
    /// This is an O(1) lookup that avoids the expensive Argon2 re-hash
    /// performed by [`validate`](Self::validate). Use this for runtime
    /// revocation checks when the key has already been authenticated.
    #[must_use]
    pub fn key_exists(&self, name: &str) -> bool {
        self.keys.contains_key(name)
    }

    /// Create a new API key with the given name and optional expiry.
    ///
    /// Returns the raw key string that should be given to the user.
    /// The store only keeps the Argon2 hash.
    pub fn create_key(&mut self, name: &str, expires_at: Option<u64>) -> Result<String, AuthError> {
        if self.keys.contains_key(name) {
            return Err(AuthError::Config(format!(
                "API key with name '{name}' already exists"
            )));
        }
        // Generate 256-bit (32-byte) random key for full
        // cryptographic strength instead of UUID v4 (122 bits).
        let mut key_bytes = [0u8; 32];
        super::fill_random(&mut key_bytes);
        use base64::Engine;
        let raw_key = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key_bytes);
        let argon2 = Argon2::default();
        // `hash_password` draws its own salt from the OS. It is the salt this
        // tree used to build by hand, from the same source, at the same
        // length — the crate simply owns it now.
        let hash = argon2
            .hash_password(raw_key.as_bytes())
            .map_err(|e| AuthError::Config(format!("hash error: {e}")))?
            .to_string();

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = ApiKeyEntry {
            name: name.to_string(),
            hash,
            expires_at,
            created_at: now,
            namespaces: Vec::new(),
            admin: false,
        };

        self.keys.insert(name.to_string(), entry);
        self.prefix_index
            .entry(key_prefix(&raw_key))
            .or_default()
            .push(name.to_string());
        debug!(name, "created API key");
        Ok(raw_key)
    }

    /// Confine an existing key to a set of namespaces.
    ///
    /// An empty list leaves the key unrestricted. Returns `false` when no
    /// key of that name is stored.
    pub fn bind_namespaces(&mut self, name: &str, namespaces: Vec<String>) -> bool {
        match self.keys.get_mut(name) {
            Some(entry) => {
                entry.namespaces = namespaces;
                true
            }
            None => false,
        }
    }

    /// Grant or revoke the administrative capability on an existing key.
    ///
    /// Returns `false` when no key of that name is stored.
    pub fn set_admin(&mut self, name: &str, admin: bool) -> bool {
        match self.keys.get_mut(name) {
            Some(entry) => {
                entry.admin = admin;
                true
            }
            None => false,
        }
    }

    /// Whether a key carries the administrative capability.
    #[must_use]
    pub fn is_admin(&self, name: &str) -> bool {
        self.keys.get(name).is_some_and(|e| e.admin)
    }

    /// The namespaces a key is confined to; empty means unrestricted.
    #[must_use]
    pub fn namespaces_for(&self, name: &str) -> &[String] {
        self.keys
            .get(name)
            .map_or(&[][..], |entry| entry.namespaces.as_slice())
    }

    /// Names of keys that may act in **any** namespace.
    ///
    /// A multi-tenant server refuses to start while this is non-empty: an
    /// unconfined key is the whole isolation boundary gone, and it fails
    /// open, so nothing in normal operation would reveal it.
    #[must_use]
    pub fn unconfined_keys(&self) -> Vec<&str> {
        self.keys
            .values()
            .filter(|e| e.namespaces.is_empty())
            .map(|e| e.name.as_str())
            .collect()
    }

    /// Add a pre-hashed key entry (for loading from config).
    ///
    /// **Note:** pre-hashed entries are not indexed by prefix and will
    /// only be found via the fallback linear Argon2 scan. Use
    /// [`register_plaintext`](Self::register_plaintext) when the raw
    /// key is available.
    pub fn add_entry(&mut self, entry: ApiKeyEntry) {
        self.keys.insert(entry.name.clone(), entry);
    }

    /// Register a plaintext key (hashing it with Argon2).
    ///
    /// Used to pre-configure keys from server configuration files
    /// where the raw key is specified in cleartext.
    ///
    /// # Errors
    ///
    /// Returns an error if a key with the same name already exists.
    pub fn register_plaintext(
        &mut self,
        name: &str,
        raw_key: &str,
        expires_at: Option<u64>,
    ) -> Result<(), AuthError> {
        if self.keys.contains_key(name) {
            return Err(AuthError::Config(format!(
                "API key with name '{name}' already exists"
            )));
        }
        let argon2 = Argon2::default();
        let mut hash = argon2
            .hash_password(raw_key.as_bytes())
            .map_err(|e| AuthError::Config(format!("hash error: {e}")))?
            .to_string();

        // Log a production warning — plaintext keys in config are
        // a deployment smell.  The raw_key &str is caller-owned so we cannot
        // zeroize it here, but we do zeroize the intermediate hash clone
        // after it is moved into the entry.
        tracing::warn!(
            name,
            "registering API key from plaintext config — prefer runtime key generation",
        );

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = ApiKeyEntry {
            name: name.to_string(),
            hash: hash.clone(),
            expires_at,
            created_at: now,
            namespaces: Vec::new(),
            admin: false,
        };

        // Zeroize the local hash copy now that it is stored in the entry.
        hash.zeroize();

        self.keys.insert(name.to_string(), entry);
        self.prefix_index
            .entry(key_prefix(raw_key))
            .or_default()
            .push(name.to_string());
        debug!(name, "registered plaintext API key");
        Ok(())
    }

    /// Validate a raw API key against the store.
    ///
    /// Uses a two-stage strategy:
    /// 1. **Fast path** – look up the SHA-256 prefix of `raw_key` in
    ///    the prefix index and Argon2-verify only the (usually ≤ 1)
    ///    matching candidates.  This covers keys created via
    ///    [`create_key`](Self::create_key) and
    ///    [`register_plaintext`](Self::register_plaintext).
    /// 2. **Fallback** – if no indexed candidate matched, fall back to
    ///    a linear Argon2 scan for entries loaded via
    ///    [`add_entry`](Self::add_entry) (pre-hashed, no prefix).
    ///
    /// Returns the key name on success.
    pub fn validate(&self, raw_key: &str) -> Result<String, AuthError> {
        let prefix = key_prefix(raw_key);
        self.check_rate_limit(prefix)?;

        let argon2 = Argon2::default();

        // Constant-time prefix scan: avoid timing oracle from HashMap lookup.
        // We iterate all prefix entries and use `subtle::ConstantTimeEq` so
        // the comparison time is independent of the input prefix value.
        let mut matched_names: Vec<&str> = Vec::new();
        for (stored_prefix, names) in &self.prefix_index {
            if bool::from(stored_prefix.ct_eq(&prefix)) {
                matched_names.extend(names.iter().map(String::as_str));
            }
        }

        // Verify matched candidates
        for name in &matched_names {
            if let Some(entry) = self.keys.get(*name) {
                match self.try_verify(&argon2, entry, raw_key) {
                    Ok(()) => return Ok(entry.name.clone()),
                    Err(AuthError::InvalidApiKey(_)) => continue,
                    Err(other) => return Err(other),
                }
            }
        }

        // Fallback: linear scan for add_entry (pre-hashed) keys not in prefix index
        for entry in self.keys.values() {
            // Skip entries already checked via prefix
            if matched_names.contains(&entry.name.as_str()) {
                continue;
            }
            if let Err(e) = self.try_verify(&argon2, entry, raw_key) {
                match e {
                    AuthError::InvalidApiKey(_) => continue,
                    other => return Err(other),
                }
            } else {
                return Ok(entry.name.clone());
            }
        }

        self.record_failure(prefix);
        Err(AuthError::InvalidApiKey("key not found".into()))
    }

    /// Check both per-prefix and global rate limits.
    fn check_rate_limit(&self, prefix: [u8; 8]) -> Result<(), AuthError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Per-prefix rate limit (isolates brute-force per key)
        if let Some(entry) = self.per_prefix_failures.get(&prefix) {
            let (count, window_start) = *entry;
            if now.saturating_sub(window_start) < FAILURE_WINDOW_SECS
                && count >= MAX_PREFIX_FAILURES_PER_WINDOW
            {
                tracing::warn!("API key validation per-prefix rate limit exceeded");
                return Err(AuthError::InvalidApiKey("rate limit exceeded".into()));
            }
        }

        // Global rate limit (safety net against distributed attacks)
        let window_start = self.failure_window_start.load(Ordering::Relaxed);
        if now.saturating_sub(window_start) >= FAILURE_WINDOW_SECS {
            // Window expired — reset.
            self.failure_window_start.store(now, Ordering::Relaxed);
            self.failure_count.store(0, Ordering::Relaxed);
        }
        if self.failure_count.load(Ordering::Relaxed) >= MAX_FAILURES_PER_WINDOW {
            tracing::warn!("API key validation global rate limit exceeded");
            return Err(AuthError::InvalidApiKey("rate limit exceeded".into()));
        }
        Ok(())
    }

    /// Record a failed validation attempt against both per-prefix and global counters.
    fn record_failure(&self, prefix: [u8; 8]) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Per-prefix counter
        let mut entry = self.per_prefix_failures.entry(prefix).or_insert((0, now));
        let (ref mut count, ref mut window_start) = *entry;
        if now.saturating_sub(*window_start) >= FAILURE_WINDOW_SECS {
            *count = 1;
            *window_start = now;
        } else {
            *count += 1;
        }

        // Global counter
        self.failure_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Attempt to verify `raw_key` against a single entry.
    ///
    /// Returns `Ok(())` on match, `Err(ExpiredApiKey)` if the key is
    /// expired, or `Err(InvalidApiKey)` if the hash doesn't match or
    /// is corrupted.
    fn try_verify(
        &self,
        argon2: &Argon2<'_>,
        entry: &ApiKeyEntry,
        raw_key: &str,
    ) -> Result<(), AuthError> {
        let parsed_hash = match PasswordHash::new(&entry.hash) {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(
                    name = %entry.name,
                    error = %e,
                    "skipping API key with corrupted hash"
                );
                return Err(AuthError::InvalidApiKey("corrupted hash".into()));
            }
        };

        if argon2
            .verify_password(raw_key.as_bytes(), &parsed_hash)
            .is_ok()
        {
            // Check expiry
            if let Some(expires) = entry.expires_at {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if now > expires {
                    return Err(AuthError::ExpiredApiKey(entry.name.clone()));
                }
            }
            return Ok(());
        }

        Err(AuthError::InvalidApiKey("no match".into()))
    }

    /// Revoke a key by name.
    ///
    /// Also cleans up the prefix index. Stale prefix entries from
    /// pre-hashed keys (added via [`add_entry`](Self::add_entry)) are
    /// harmless — they simply won't find a matching `keys` entry
    /// during validation.
    pub fn revoke(&mut self, name: &str) -> bool {
        let removed = self.keys.remove(name).is_some();
        if removed {
            // Remove name from every prefix bucket it appears in.
            self.prefix_index.retain(|_prefix, names| {
                names.retain(|n| n != name);
                !names.is_empty()
            });
        }
        removed
    }

    /// List all key names.
    pub fn list_keys(&self) -> Vec<&str> {
        self.keys.keys().map(String::as_str).collect()
    }

    /// Get the number of stored keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Check if the store is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

impl Default for ApiKeyStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_validate() {
        let mut store = ApiKeyStore::new();
        let key = store.create_key("test", None).unwrap();
        assert_eq!(store.len(), 1);

        let name = store.validate(&key).unwrap();
        assert_eq!(name, "test");
    }

    #[test]
    fn invalid_key_rejected() {
        let mut store = ApiKeyStore::new();
        let _key = store.create_key("test", None).unwrap();

        let result = store.validate("wrong-key");
        assert!(result.is_err());
        assert!(matches!(result, Err(AuthError::InvalidApiKey(_))));
    }

    #[test]
    fn expired_key_rejected() {
        let mut store = ApiKeyStore::new();
        // Expire in the past
        let key = store.create_key("expired", Some(0)).unwrap();

        let result = store.validate(&key);
        assert!(result.is_err());
        assert!(matches!(result, Err(AuthError::ExpiredApiKey(_))));
    }

    #[test]
    fn revoke_key() {
        let mut store = ApiKeyStore::new();
        let key = store.create_key("revokable", None).unwrap();

        assert!(store.revoke("revokable"));
        assert!(store.validate(&key).is_err());
        assert!(!store.revoke("revokable")); // already revoked
    }

    #[test]
    fn multiple_keys() {
        let mut store = ApiKeyStore::new();
        let key1 = store.create_key("admin", None).unwrap();
        let key2 = store.create_key("reader", None).unwrap();
        assert_eq!(store.len(), 2);

        assert_eq!(store.validate(&key1).unwrap(), "admin");
        assert_eq!(store.validate(&key2).unwrap(), "reader");
    }

    #[test]
    fn list_keys() {
        let mut store = ApiKeyStore::new();
        let _k1 = store.create_key("a", None).unwrap();
        let _k2 = store.create_key("b", None).unwrap();

        let mut names = store.list_keys();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn register_plaintext_duplicate_rejected() {
        let mut store = ApiKeyStore::new();
        store
            .register_plaintext("admin", "my-secret", None)
            .unwrap();
        let err = store
            .register_plaintext("admin", "other-secret", None)
            .unwrap_err();
        assert!(matches!(err, AuthError::Config(_)));
    }

    #[test]
    fn per_prefix_rate_limit_isolates_keys() {
        let mut store = ApiKeyStore::new();
        let good_key = store.create_key("good", None).unwrap();

        // Exhaust per-prefix limit with a specific bad key
        let bad_key = "definitely-wrong-key";
        for _ in 0..MAX_PREFIX_FAILURES_PER_WINDOW {
            let _ = store.validate(bad_key);
        }

        // The bad key prefix is now rate-limited
        let result = store.validate(bad_key);
        assert!(result.is_err());

        // But the good key still works (different prefix)
        let result = store.validate(&good_key);
        assert!(result.is_ok());
    }
}
