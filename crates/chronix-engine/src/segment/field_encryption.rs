//! Field-level encryption for columnar segment blocks.
//!
//! Provides per-column AES-256-GCM encryption with per-block random nonces.
//! Each encrypted block is stored as `[12-byte nonce][ciphertext + 16-byte tag]`.
//!
//! # Security Properties
//!
//! - **Per-block random nonces**: every column block gets a fresh 12-byte nonce
//!   from `OsRng`, preventing nonce reuse even if the same data is written twice.
//! - **Authenticated encryption**: AES-256-GCM provides both confidentiality
//!   and integrity. Tampered ciphertext is detected and rejected.
//! - **Stats suppression**: encrypted columns have their min/max/sum/distinct
//!   statistics zeroed to prevent information leakage via zone maps.
//! - **Bloom filter suppression**: tag columns with encryption skip bloom
//!   filter construction.
//!
//! # Performance Tradeoff: Statistics Suppression
//!
//! Encrypting a column **suppresses all zone-map statistics** (min, max, sum,
//! distinct count). This is a deliberate security measure — publishing
//! plaintext min/max values would leak information about the encrypted data.
//!
//! **Consequence:** Predicate pushdown cannot prune row groups based on
//! encrypted columns. Queries that filter primarily on an encrypted column
//! will read and decrypt every segment instead of skipping non-matching
//! row groups. This can cause **significant read amplification** when most
//! data would otherwise be pruned (e.g., 10× for a 90%-selective filter).
//!
//! **Mitigation strategies:**
//!
//! 1. **Filter on non-encrypted columns first.** Time-range and plaintext
//!    tag filters still benefit from zone-map pruning and bloom filters.
//!    Apply encrypted-column predicates as a post-decryption filter.
//!
//! 2. **Encrypt only sensitive fields.** Keep frequently-filtered columns
//!    (e.g., `host`, `region`) in plaintext and encrypt only payload
//!    fields (e.g., `patient_id`, `credit_card`).
//!
//! 3. **Partition by sensitive dimension.** If you need to filter on an
//!    encrypted tag, consider using it as a partition key so that each
//!    segment contains exactly one distinct value.
//!
//! For a full discussion see the [encryption section](../../docs/security.md)
//! in the user guide.
//!
//! # Key Management
//!
//! Keys are identified by string IDs, enabling key rotation. The writer
//! receives a mapping of column names to [`FieldEncryptionKey`] values.
//! The reader must provide a [`FieldKeyProvider`] that resolves key IDs
//! to the actual 256-bit key material.
//!
//! # Format
//!
//! ```text
//! Encrypted block layout:
//!   [nonce: 12 bytes][ciphertext: N bytes][auth_tag: 16 bytes]
//!
//! Where N = len(encoded_and_possibly_compressed_block)
//! ```

use crate::segment::error::{Result, SegmentError};

use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, Nonce};

/// AES-256-GCM nonce size (96 bits).
pub(crate) const NONCE_SIZE: usize = 12;

/// AES-256-GCM authentication tag size (128 bits).
pub(crate) const TAG_SIZE: usize = 16;

/// A 256-bit encryption key with its ID for key rotation tracking.
#[derive(Clone)]
pub struct FieldEncryptionKey {
    /// Opaque key identifier for rotation / lookup.
    pub key_id: String,
    /// 32-byte AES-256 key material.
    key: [u8; 32],
}

impl FieldEncryptionKey {
    /// Create a new field encryption key.
    ///
    /// # Panics
    ///
    /// Panics if `key_bytes` length is not exactly 32.
    #[must_use]
    pub fn new(key_id: impl Into<String>, key_bytes: [u8; 32]) -> Self {
        Self {
            key_id: key_id.into(),
            key: key_bytes,
        }
    }

    /// Access the raw key bytes.
    #[must_use]
    pub fn key_bytes(&self) -> &[u8; 32] {
        &self.key
    }
}

impl std::fmt::Debug for FieldEncryptionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldEncryptionKey")
            .field("key_id", &self.key_id)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

/// Configuration for field-level encryption in the segment writer.
///
/// Maps column names to encryption keys. Only columns listed here are
/// encrypted; all other columns remain in plaintext.
#[derive(Debug, Clone, Default)]
pub struct FieldEncryptionConfig {
    /// Column name → encryption key mapping.
    pub columns: std::collections::HashMap<String, FieldEncryptionKey>,
}

impl FieldEncryptionConfig {
    /// Create an empty config (no columns encrypted).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a column to encrypt.
    pub fn encrypt_column(&mut self, column_name: impl Into<String>, key: FieldEncryptionKey) {
        self.columns.insert(column_name.into(), key);
    }

    /// Check whether a column should be encrypted.
    #[must_use]
    pub fn is_encrypted(&self, column_name: &str) -> bool {
        self.columns.contains_key(column_name)
    }

    /// Get the encryption key for a column, if any.
    #[must_use]
    pub fn key_for(&self, column_name: &str) -> Option<&FieldEncryptionKey> {
        self.columns.get(column_name)
    }
}

/// Trait for resolving encryption key IDs to key material during reads.
pub trait FieldKeyProvider: Send + Sync {
    /// Look up an encryption key by its ID.
    ///
    /// Returns the 32-byte AES-256 key material, or `None` if the key ID
    /// is unknown (e.g. after key deletion or misconfiguration).
    fn get_key(&self, key_id: &str) -> Option<[u8; 32]>;
}

/// Simple in-memory key provider for testing and embedded use.
#[derive(Debug, Clone, Default)]
pub struct StaticKeyProvider {
    keys: std::collections::HashMap<String, [u8; 32]>,
}

impl StaticKeyProvider {
    /// Create an empty provider.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a key.
    pub fn add_key(&mut self, key_id: impl Into<String>, key_bytes: [u8; 32]) {
        self.keys.insert(key_id.into(), key_bytes);
    }
}

impl FieldKeyProvider for StaticKeyProvider {
    fn get_key(&self, key_id: &str) -> Option<[u8; 32]> {
        self.keys.get(key_id).copied()
    }
}

/// Encrypt a column block using AES-256-GCM.
///
/// Returns `[nonce (12)][ciphertext + tag]`.
pub(crate) fn encrypt_block(plaintext: &[u8], key: &[u8; 32]) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| SegmentError::CorruptFile {
        detail: format!("invalid encryption key: {e}"),
    })?;

    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let ciphertext = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|e| SegmentError::CorruptFile {
            detail: format!("field encryption failed: {e}"),
        })?;

    let mut result = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
    result.extend_from_slice(&nonce);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Decrypt a column block using AES-256-GCM.
///
/// Expects `[nonce (12)][ciphertext + tag]`.
pub(crate) fn decrypt_block(encrypted: &[u8], key: &[u8; 32]) -> Result<Vec<u8>> {
    if encrypted.len() < NONCE_SIZE + TAG_SIZE {
        return Err(SegmentError::CorruptFile {
            detail: format!(
                "encrypted block too short: {} bytes (need at least {})",
                encrypted.len(),
                NONCE_SIZE + TAG_SIZE,
            ),
        });
    }

    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| SegmentError::CorruptFile {
        detail: format!("invalid decryption key: {e}"),
    })?;

    let nonce = Nonce::from_slice(&encrypted[..NONCE_SIZE]);
    let ciphertext = &encrypted[NONCE_SIZE..];

    cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| SegmentError::CorruptFile {
            detail: format!("field decryption failed (wrong key or tampered data): {e}"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        for (i, b) in key.iter_mut().enumerate() {
            *b = i as u8;
        }
        key
    }

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let key = test_key();
        let plaintext = b"Hello, field-level encryption!";
        let encrypted = encrypt_block(plaintext, &key).unwrap();
        assert_ne!(encrypted, plaintext);
        assert!(encrypted.len() > plaintext.len()); // nonce + tag overhead
        let decrypted = decrypt_block(&encrypted, &key).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn different_nonces_produce_different_ciphertext() {
        let key = test_key();
        let plaintext = b"same data";
        let enc1 = encrypt_block(plaintext, &key).unwrap();
        let enc2 = encrypt_block(plaintext, &key).unwrap();
        // Nonces should differ (with overwhelming probability)
        assert_ne!(&enc1[..NONCE_SIZE], &enc2[..NONCE_SIZE]);
    }

    #[test]
    fn wrong_key_fails_decryption() {
        let key1 = test_key();
        let mut key2 = test_key();
        key2[0] ^= 0xFF; // flip a bit
        let encrypted = encrypt_block(b"secret", &key1).unwrap();
        let result = decrypt_block(&encrypted, &key2);
        assert!(result.is_err());
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let key = test_key();
        let mut encrypted = encrypt_block(b"important data", &key).unwrap();
        // Flip a byte in the ciphertext
        let last = encrypted.len() - 1;
        encrypted[last] ^= 0x01;
        let result = decrypt_block(&encrypted, &key);
        assert!(result.is_err());
    }

    #[test]
    fn too_short_encrypted_block_fails() {
        let key = test_key();
        let result = decrypt_block(&[0u8; 10], &key);
        assert!(result.is_err());
    }

    #[test]
    fn field_encryption_config_basic() {
        let mut config = FieldEncryptionConfig::new();
        assert!(!config.is_encrypted("temperature"));

        let key = FieldEncryptionKey::new("key-1", test_key());
        config.encrypt_column("temperature", key);

        assert!(config.is_encrypted("temperature"));
        assert!(!config.is_encrypted("humidity"));
        assert_eq!(config.key_for("temperature").unwrap().key_id, "key-1");
    }

    #[test]
    fn static_key_provider_lookup() {
        let mut provider = StaticKeyProvider::new();
        let key = test_key();
        provider.add_key("key-1", key);

        assert_eq!(provider.get_key("key-1"), Some(key));
        assert_eq!(provider.get_key("key-2"), None);
    }

    #[test]
    fn field_encryption_key_debug_redacts() {
        let key = FieldEncryptionKey::new("key-1", test_key());
        let debug = format!("{key:?}");
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains(&format!("{:?}", test_key())));
    }

    #[test]
    fn encrypt_empty_block() {
        let key = test_key();
        let encrypted = encrypt_block(b"", &key).unwrap();
        assert_eq!(encrypted.len(), NONCE_SIZE + TAG_SIZE); // nonce + tag, no ciphertext
        let decrypted = decrypt_block(&encrypted, &key).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn encrypt_large_block() {
        let key = test_key();
        let plaintext = vec![0xABu8; 1024 * 1024]; // 1 MiB
        let encrypted = encrypt_block(&plaintext, &key).unwrap();
        let decrypted = decrypt_block(&encrypted, &key).unwrap();
        assert_eq!(decrypted, plaintext);
    }
}
