//! AES-256-GCM authenticated encryption for data at rest.
//!
//! Provides transparent encryption/decryption of segment data blocks
//! and WAL entries with per-entry nonces. Supports key rotation —
//! new data uses the latest key, old data remains readable with
//! previous keys.
//!
//! # Example
//!
//! ```no_run
//! use chronix_security::auth::encryption::{EncryptionService, FileKeyProvider};
//!
//! let provider = FileKeyProvider::from_hex_key(
//!     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
//!     "key-1",
//! ).unwrap();
//! let service = EncryptionService::new(Box::new(provider));
//!
//! let plaintext = b"hello world";
//! let (ciphertext, key_id) = service.encrypt(plaintext).unwrap();
//! let decrypted = service.decrypt(&ciphertext, &key_id).unwrap();
//! assert_eq!(plaintext.as_slice(), &decrypted);
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use zeroize::Zeroize;

use crate::auth::error::AuthError;

/// 12-byte nonce for AES-256-GCM.
const NONCE_SIZE: usize = 12;

/// A secret key that is automatically zeroized when dropped.
///
/// Wraps raw key bytes to ensure they are erased from memory
/// when the value goes out of scope, even for clones returned by
/// [`KeyProvider::current_key`] and [`KeyProvider::key_by_id`].
#[derive(Clone)]
pub struct SecretKey(Vec<u8>);

impl SecretKey {
    /// Create a new `SecretKey` from raw bytes.
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// View the key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Drop for SecretKey {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretKey([REDACTED; {} bytes])", self.0.len())
    }
}

impl AsRef<[u8]> for SecretKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Trait for providing encryption keys.
///
/// Implementations can load keys from files, environment variables,
/// or external KMS services.
///
/// All key material is returned as [`SecretKey`] to ensure automatic
/// zeroization when the consumer drops the value.
pub trait KeyProvider: Send + Sync + std::fmt::Debug {
    /// Get the current (latest) encryption key and its ID.
    fn current_key(&self) -> Result<(SecretKey, String), AuthError>;

    /// Get a key by its ID (for decrypting old data).
    fn key_by_id(&self, key_id: &str) -> Result<SecretKey, AuthError>;

    /// List all available key IDs.
    fn key_ids(&self) -> Vec<String>;
}

/// Key provider that reads keys from a file or config.
///
/// **Security:** Implements `Drop` to zeroize key material from memory.
/// Intentionally does not implement `Clone` to prevent accidental
/// duplication of key material that would bypass zeroization.
pub struct FileKeyProvider {
    keys: HashMap<String, Vec<u8>>,
    current_key_id: String,
}

impl Drop for FileKeyProvider {
    fn drop(&mut self) {
        // Zeroize all key material on drop to minimize in-memory exposure.
        // Use black_box to prevent the compiler from optimising the zeroing away.
        for value in self.keys.values_mut() {
            value.fill(0);
        }
        // Compiler fence + black_box to prevent dead-store elimination.
        std::sync::atomic::fence(std::sync::atomic::Ordering::SeqCst);
        std::hint::black_box(&self.keys);
    }
}

impl std::fmt::Debug for FileKeyProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileKeyProvider")
            .field("current_key_id", &self.current_key_id)
            .field("key_count", &self.keys.len())
            .finish()
    }
}

impl FileKeyProvider {
    /// Create a key provider from a hex-encoded 256-bit key.
    pub fn from_hex_key(hex_key: &str, key_id: &str) -> Result<Self, AuthError> {
        let key = hex_decode(hex_key)?;
        if key.len() != 32 {
            return Err(AuthError::Config(format!(
                "AES-256 key must be 32 bytes, got {}",
                key.len()
            )));
        }

        let mut keys = HashMap::new();
        keys.insert(key_id.to_string(), key);

        Ok(Self {
            keys,
            current_key_id: key_id.to_string(),
        })
    }

    /// Add a key for rotation. The last added key becomes current.
    pub fn add_key(&mut self, hex_key: &str, key_id: &str) -> Result<(), AuthError> {
        let key = hex_decode(hex_key)?;
        if key.len() != 32 {
            return Err(AuthError::Config(format!(
                "AES-256 key must be 32 bytes, got {}",
                key.len()
            )));
        }
        self.keys.insert(key_id.to_string(), key);
        self.current_key_id = key_id.to_string();
        Ok(())
    }
}

impl KeyProvider for FileKeyProvider {
    fn current_key(&self) -> Result<(SecretKey, String), AuthError> {
        let key = self
            .keys
            .get(&self.current_key_id)
            .ok_or_else(|| AuthError::KeyNotFound(self.current_key_id.clone()))?
            .clone();
        Ok((SecretKey::new(key), self.current_key_id.clone()))
    }

    fn key_by_id(&self, key_id: &str) -> Result<SecretKey, AuthError> {
        self.keys
            .get(key_id)
            .cloned()
            .map(SecretKey::new)
            .ok_or_else(|| AuthError::KeyNotFound(key_id.to_string()))
    }

    fn key_ids(&self) -> Vec<String> {
        self.keys.keys().cloned().collect()
    }
}

/// Key provider that reads from environment variables.
///
/// # Design Note
///
/// The environment variable is read on **every call** to
/// [`current_key()`](KeyProvider::current_key) rather than being cached at
/// construction time. This is intentional: it allows key rotation by
/// updating the env var (e.g. via a sidecar or init-container) without
/// restarting the process. The overhead of `std::env::var` is negligible
/// compared to the AES-GCM operations that follow.
///
/// # Cached Fallback
///
/// If the environment variable is temporarily unset (e.g. during key
/// rotation), the provider falls back to the last successfully loaded
/// key for up to [`grace_period`](EnvKeyProvider::with_grace_period)
/// (default **5 s**). After the grace period expires, the
/// missing-variable error is propagated.
///
/// # Hardening
///
/// * Grace period reduced from 30 s → 5 s (configurable).
/// * [`clear_cache`](EnvKeyProvider::clear_cache) lets operators
///   explicitly discard the cached key once rotation is complete.
/// * Every cache-fallback hit emits the `chronix_auth_env_key_grace_hit`
///   counter so operators can detect prolonged rotation windows.
pub struct EnvKeyProvider {
    env_var: String,
    key_id: String,
    /// Cached last-good key with the timestamp it was fetched.
    cache: parking_lot::Mutex<Option<(SecretKey, std::time::Instant)>>,
    /// Maximum age of a cached key after the env var disappears.
    grace_period: std::time::Duration,
}

impl std::fmt::Debug for EnvKeyProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvKeyProvider")
            .field("env_var", &self.env_var)
            .field("key_id", &self.key_id)
            .field("has_cached_key", &self.cache.lock().is_some())
            .finish()
    }
}

/// Grace period during which a cached key is returned if the env var
/// becomes temporarily unavailable (e.g. during key rotation).
///
/// Reduced from 30 s → 5 s to shrink the stale-key window.
const DEFAULT_ENV_KEY_GRACE_PERIOD: std::time::Duration = std::time::Duration::from_secs(5);

impl EnvKeyProvider {
    /// Create a key provider from an environment variable name.
    ///
    /// The environment variable should contain a hex-encoded 256-bit key.
    /// The default grace period is 5 seconds.
    #[must_use]
    pub fn new(env_var: &str, key_id: &str) -> Self {
        Self {
            env_var: env_var.to_string(),
            key_id: key_id.to_string(),
            cache: parking_lot::Mutex::new(None),
            grace_period: DEFAULT_ENV_KEY_GRACE_PERIOD,
        }
    }

    /// Override the grace period for cached key fallback.
    #[must_use]
    pub fn with_grace_period(mut self, duration: std::time::Duration) -> Self {
        self.grace_period = duration;
        self
    }

    /// Explicitly discard the cached key, e.g. after rotation is complete.
    pub fn clear_cache(&self) {
        *self.cache.lock() = None;
    }
}

impl KeyProvider for EnvKeyProvider {
    fn current_key(&self) -> Result<(SecretKey, String), AuthError> {
        match std::env::var(&self.env_var) {
            Ok(hex) => {
                let key = hex_decode(&hex)?;
                if key.len() != 32 {
                    return Err(AuthError::Config(format!(
                        "AES-256 key must be 32 bytes, got {}",
                        key.len()
                    )));
                }
                let secret = SecretKey::new(key);
                // Update the cache with the fresh key.
                *self.cache.lock() = Some((secret.clone(), std::time::Instant::now()));
                Ok((secret, self.key_id.clone()))
            }
            Err(_) => {
                // Env var missing — fall back to cached key within grace period.
                let guard = self.cache.lock();
                if let Some((ref cached_key, fetched_at)) = *guard {
                    if fetched_at.elapsed() < self.grace_period {
                        tracing::warn!(
                            env_var = %self.env_var,
                            grace_remaining_secs = (self.grace_period.saturating_sub(fetched_at.elapsed())).as_secs(),
                            "env var missing, using cached key within grace period"
                        );
                        metrics::counter!("chronix_auth_env_key_grace_hit").increment(1);
                        return Ok((cached_key.clone(), self.key_id.clone()));
                    }
                }
                drop(guard);
                Err(AuthError::Config(format!(
                    "env var {} not set",
                    self.env_var
                )))
            }
        }
    }

    fn key_by_id(&self, key_id: &str) -> Result<SecretKey, AuthError> {
        if key_id == self.key_id {
            self.current_key().map(|(k, _)| k)
        } else {
            Err(AuthError::KeyNotFound(key_id.to_string()))
        }
    }

    fn key_ids(&self) -> Vec<String> {
        vec![self.key_id.clone()]
    }
}

/// AES-256-GCM encryption service for data at rest.
///
/// Encrypted format: `[12-byte nonce][ciphertext + 16-byte tag]`
///
/// Tracks invocation count per NIST SP 800-38D recommendation to
/// rotate keys before 2^32 encryptions with random nonces.
#[derive(Debug)]
pub struct EncryptionService {
    provider: Box<dyn KeyProvider>,
    /// Number of `encrypt()` calls since creation. NIST recommends
    /// key rotation before 2^32 encryptions with random nonces.
    invocation_count: Arc<AtomicU64>,
}

/// NIST SP 800-38D recommended maximum encryptions per key with random nonces.
const NIST_KEY_USAGE_LIMIT: u64 = 1 << 32;

impl EncryptionService {
    /// Create a new encryption service with the given key provider.
    #[must_use]
    pub fn new(provider: Box<dyn KeyProvider>) -> Self {
        Self {
            provider,
            invocation_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Returns the number of `encrypt()` calls since creation.
    #[must_use]
    pub fn invocation_count(&self) -> u64 {
        self.invocation_count.load(Ordering::Relaxed)
    }

    /// Derive a domain-separated subkey from the master key
    /// using HMAC-SHA256 as a PRF (per NIST SP 800-108 counter mode KDF).
    /// This ensures the AES-256-GCM encryption key and HMAC-SHA256
    /// integrity key are cryptographically independent.
    ///
    /// Returns a [`SecretKey`] so the derived key is automatically zeroized.
    fn derive_subkey(master: &[u8], domain: &[u8]) -> SecretKey {
        let mut mac =
            <Hmac<Sha256> as KeyInit>::new_from_slice(master).expect("HMAC accepts any key size");
        mac.update(domain);
        SecretKey::new(mac.finalize().into_bytes().to_vec())
    }

    /// Encrypt data using the current key.
    ///
    /// Returns `(ciphertext_with_nonce, key_id)`.
    /// Format: `[12-byte nonce][AES-256-GCM ciphertext + 16-byte auth tag]`
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<(Vec<u8>, String), AuthError> {
        let count = self.invocation_count.fetch_add(1, Ordering::Relaxed);
        // Enforce NIST SP 800-38D limit — reject encryption past 2^32
        // invocations to prevent nonce collision risk with random nonces.
        if count >= NIST_KEY_USAGE_LIMIT {
            return Err(AuthError::Encryption(format!(
                "AES-GCM key usage limit reached ({count} encryptions); \
                 rotate key to avoid nonce collision risk (NIST SP 800-38D)"
            )));
        }
        if count == NIST_KEY_USAGE_LIMIT - 1 {
            tracing::warn!(
                count = count + 1,
                "AES-GCM key approaching 2^32 encryption limit; rotate key soon"
            );
        }

        let (master_key, key_id) = self.provider.current_key()?;
        let enc_key = Self::derive_subkey(master_key.as_bytes(), b"chronix-aes-gcm-enc-v1");

        let cipher = Aes256Gcm::new_from_slice(enc_key.as_bytes())
            .map_err(|e| AuthError::Encryption(format!("key error: {e}")))?;

        // Generate random nonce
        let nonce_bytes = generate_nonce();
        let nonce = Nonce::try_from(&nonce_bytes[..])
            .map_err(|_| AuthError::Config("nonce must be 12 bytes".into()))?;

        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| AuthError::Encryption(e.to_string()))?;

        // Prepend nonce to ciphertext
        let mut result = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
        result.extend_from_slice(&nonce_bytes);
        result.extend_from_slice(&ciphertext);

        Ok((result, key_id))
    }

    /// Decrypt data using the specified key.
    ///
    /// Input format: `[12-byte nonce][AES-256-GCM ciphertext + 16-byte auth tag]`
    pub fn decrypt(&self, data: &[u8], key_id: &str) -> Result<Vec<u8>, AuthError> {
        if data.len() < NONCE_SIZE {
            return Err(AuthError::Decryption("data too short for nonce".into()));
        }

        let master_key = self.provider.key_by_id(key_id)?;
        let enc_key = Self::derive_subkey(master_key.as_bytes(), b"chronix-aes-gcm-enc-v1");

        let cipher = Aes256Gcm::new_from_slice(enc_key.as_bytes())
            .map_err(|e| AuthError::Decryption(format!("key error: {e}")))?;

        let nonce = Nonce::try_from(&data[..NONCE_SIZE])
            .map_err(|_| AuthError::Config("nonce must be 12 bytes".into()))?;
        let ciphertext = &data[NONCE_SIZE..];

        cipher
            .decrypt(&nonce, ciphertext)
            .map_err(|e| AuthError::Decryption(e.to_string()))
    }

    /// Compute HMAC-SHA256 over data for integrity verification.
    pub fn hmac(&self, data: &[u8]) -> Result<Vec<u8>, AuthError> {
        let (master_key, _) = self.provider.current_key()?;
        let hmac_key = Self::derive_subkey(master_key.as_bytes(), b"chronix-hmac-sha256-v1");

        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(hmac_key.as_bytes())
            .map_err(|e| AuthError::Encryption(format!("HMAC error: {e}")))?;
        mac.update(data);
        Ok(mac.finalize().into_bytes().to_vec())
    }

    /// Verify HMAC-SHA256 integrity.
    pub fn verify_hmac(
        &self,
        data: &[u8],
        expected: &[u8],
        key_id: &str,
    ) -> Result<bool, AuthError> {
        let master_key = self.provider.key_by_id(key_id)?;
        let hmac_key = Self::derive_subkey(master_key.as_bytes(), b"chronix-hmac-sha256-v1");

        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(hmac_key.as_bytes())
            .map_err(|e| AuthError::Encryption(format!("HMAC error: {e}")))?;
        mac.update(data);

        Ok(mac.verify_slice(expected).is_ok())
    }

    /// Get the current key ID.
    pub fn current_key_id(&self) -> Result<String, AuthError> {
        self.provider.current_key().map(|(_, id)| id)
    }

    /// List all available key IDs.
    #[must_use]
    pub fn key_ids(&self) -> Vec<String> {
        self.provider.key_ids()
    }
}

/// Generate a random 12-byte nonce.
fn generate_nonce() -> [u8; NONCE_SIZE] {
    let mut nonce = [0u8; NONCE_SIZE];
    super::fill_random(&mut nonce);
    nonce
}

/// Decode a hex string to bytes.
fn hex_decode(hex: &str) -> Result<Vec<u8>, AuthError> {
    if !hex.len().is_multiple_of(2) {
        return Err(AuthError::Config("hex string must have even length".into()));
    }

    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16)
                .map_err(|e| AuthError::Config(format!("invalid hex: {e}")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_KEY_HEX: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn test_service() -> EncryptionService {
        let provider = FileKeyProvider::from_hex_key(TEST_KEY_HEX, "key-1").unwrap();
        EncryptionService::new(Box::new(provider))
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let svc = test_service();
        let plaintext = b"The quick brown fox jumps over the lazy dog";

        let (ciphertext, key_id) = svc.encrypt(plaintext).unwrap();
        assert_ne!(ciphertext.as_slice(), plaintext.as_slice());
        assert_eq!(key_id, "key-1");

        let decrypted = svc.decrypt(&ciphertext, &key_id).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn empty_plaintext_roundtrip() {
        let svc = test_service();
        let plaintext = b"";

        let (ciphertext, key_id) = svc.encrypt(plaintext).unwrap();
        let decrypted = svc.decrypt(&ciphertext, &key_id).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn large_data_roundtrip() {
        let svc = test_service();
        let plaintext: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();

        let (ciphertext, key_id) = svc.encrypt(&plaintext).unwrap();
        let decrypted = svc.decrypt(&ciphertext, &key_id).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn tampered_data_fails() {
        let svc = test_service();
        let plaintext = b"sensitive data";

        let (mut ciphertext, key_id) = svc.encrypt(plaintext).unwrap();
        // Flip a bit in the ciphertext (after nonce)
        if let Some(byte) = ciphertext.get_mut(NONCE_SIZE + 1) {
            *byte ^= 0xFF;
        }

        let err = svc.decrypt(&ciphertext, &key_id).unwrap_err();
        assert!(matches!(err, AuthError::Decryption(_)));
    }

    #[test]
    fn wrong_key_fails() {
        let svc = test_service();
        let plaintext = b"test data";

        let (ciphertext, _) = svc.encrypt(plaintext).unwrap();

        // Try to decrypt with wrong key ID
        let err = svc.decrypt(&ciphertext, "nonexistent-key").unwrap_err();
        assert!(matches!(err, AuthError::KeyNotFound(_)));
    }

    #[test]
    fn key_rotation() {
        let provider1 = FileKeyProvider::from_hex_key(TEST_KEY_HEX, "key-1").unwrap();

        // Encrypt with key-1
        let svc1 = EncryptionService::new(Box::new(provider1));
        let plaintext = b"data with key 1";
        let (ct1, kid1) = svc1.encrypt(plaintext).unwrap();
        assert_eq!(kid1, "key-1");

        // Create a fresh provider with key-1, then add key-2 (becomes current)
        let mut provider2 = FileKeyProvider::from_hex_key(TEST_KEY_HEX, "key-1").unwrap();
        let key2_hex = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
        provider2.add_key(key2_hex, "key-2").unwrap();

        let svc2 = EncryptionService::new(Box::new(provider2));

        // New encryption uses key-2
        let (ct2, kid2) = svc2.encrypt(b"data with key 2").unwrap();
        assert_eq!(kid2, "key-2");

        // Can still decrypt old data with key-1
        let dec1 = svc2.decrypt(&ct1, "key-1").unwrap();
        assert_eq!(dec1, plaintext);

        // Can decrypt new data with key-2
        let dec2 = svc2.decrypt(&ct2, "key-2").unwrap();
        assert_eq!(dec2, b"data with key 2");
    }

    #[test]
    fn hmac_verify() {
        let svc = test_service();
        let data = b"manifest data to verify";

        let mac = svc.hmac(data).unwrap();
        assert!(svc.verify_hmac(data, &mac, "key-1").unwrap());

        // Tampered data
        let mut tampered = data.to_vec();
        tampered[0] ^= 0xFF;
        assert!(!svc.verify_hmac(&tampered, &mac, "key-1").unwrap());
    }

    #[test]
    fn hmac_tampered_mac_fails() {
        let svc = test_service();
        let data = b"important data";

        let mut mac = svc.hmac(data).unwrap();
        mac[0] ^= 0xFF; // tamper with MAC
        assert!(!svc.verify_hmac(data, &mac, "key-1").unwrap());
    }

    #[test]
    fn short_key_rejected() {
        let result = FileKeyProvider::from_hex_key("0123456789abcdef", "k");
        assert!(result.is_err());
    }

    #[test]
    fn invalid_hex_rejected() {
        let result = FileKeyProvider::from_hex_key("zzzz", "k");
        assert!(result.is_err());
    }

    #[test]
    fn data_too_short_for_nonce() {
        let svc = test_service();
        let err = svc.decrypt(&[0u8; 5], "key-1").unwrap_err();
        assert!(matches!(err, AuthError::Decryption(_)));
    }

    #[test]
    fn hex_decode_works() {
        assert_eq!(hex_decode("48656c6c6f").unwrap(), b"Hello");
        assert_eq!(hex_decode("").unwrap(), b"");
        assert!(hex_decode("0").is_err()); // odd length
    }

    // ── Hardening tests ─────────────────────────

    #[test]
    fn env_key_default_grace_period_is_5s() {
        let provider = EnvKeyProvider::new("CHRONIX_TEST_KEY_NOT_SET", "k1");
        assert_eq!(provider.grace_period, std::time::Duration::from_secs(5));
    }

    #[test]
    fn env_key_custom_grace_period() {
        let provider = EnvKeyProvider::new("CHRONIX_TEST_KEY_NOT_SET", "k1")
            .with_grace_period(std::time::Duration::from_secs(2));
        assert_eq!(provider.grace_period, std::time::Duration::from_secs(2));
    }

    #[test]
    fn env_key_clear_cache_discards_key() {
        // Manually seed the cache
        let provider = EnvKeyProvider::new("CHRONIX_NEVER_SET_12345", "k1");
        {
            let key_bytes = vec![0u8; 32];
            *provider.cache.lock() = Some((SecretKey::new(key_bytes), std::time::Instant::now()));
        }

        // Cache is populated → current_key would succeed via grace fallback
        assert!(provider.current_key().is_ok());

        // clear_cache → must fail now
        provider.clear_cache();
        assert!(provider.current_key().is_err());
    }

    #[test]
    fn env_key_expired_grace_returns_error() {
        let provider = EnvKeyProvider::new("CHRONIX_NEVER_SET_67890", "k1")
            .with_grace_period(std::time::Duration::from_millis(0));

        // Seed cache with an "instant now" key
        {
            let key_bytes = vec![0u8; 32];
            *provider.cache.lock() = Some((SecretKey::new(key_bytes), std::time::Instant::now()));
        }

        // 0 ms grace → already expired
        assert!(provider.current_key().is_err());
    }
}
