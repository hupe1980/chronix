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

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};

/// AES-256-GCM nonce size (96 bits).
pub(crate) const NONCE_SIZE: usize = 12;

/// Fill `dest` from the operating system's random source.
///
/// A GCM nonce must never repeat under one key, so there is no fallback worth
/// having: if the kernel has no entropy source this panics rather than
/// producing a nonce that could collide.
fn fill_random(dest: &mut [u8]) {
    use rand::TryRng;
    rand::rngs::SysRng
        .try_fill_bytes(dest)
        .expect("the operating system random source must be available");
}

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

/// Where an encrypted block belongs, bound into its authentication tag.
///
/// AES-GCM proves a ciphertext was written by someone holding the key. It
/// says nothing about **which** ciphertext this is, so without associated
/// data any encrypted block decrypts correctly in any other block's place:
/// one column reads as another, or one segment's values as another's, by
/// swapping bytes between two files that share a key. Nothing detects it,
/// because the tag verifies.
///
/// Binding the column and the segment makes a moved block fail to
/// authenticate.
///
/// The segment is identified by its header's creation timestamp rather than
/// by a catalog ID: the catalog assigns an ID only after the file is
/// written, whereas the header is written before the first block, so it is
/// the one segment-unique value both the writer and the reader hold at the
/// moment they need it. Compaction writes a new segment with its own
/// timestamp and re-encrypts, so a compacted block is bound to where it
/// actually ended up.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockContext<'a> {
    /// Column name.
    pub column: &'a str,
    /// The `created_at` of the segment holding this block.
    pub segment_created_at: i64,
}

impl BlockContext<'_> {
    /// The associated-data bytes. Length-prefixed, so that a measurement
    /// and column pair cannot be re-split to produce the same bytes.
    fn associated_data(self) -> Vec<u8> {
        let mut aad = Vec::with_capacity(self.column.len() + 32);
        aad.extend_from_slice(b"chronix-field-v1");
        aad.extend_from_slice(&(self.column.len() as u32).to_le_bytes());
        aad.extend_from_slice(self.column.as_bytes());
        aad.extend_from_slice(&self.segment_created_at.to_le_bytes());
        aad
    }
}

/// Encrypt a column block using AES-256-GCM, bound to where it belongs.
///
/// Returns `[nonce (12)][ciphertext + tag]`.
pub(crate) fn encrypt_block(
    plaintext: &[u8],
    key: &[u8; 32],
    context: BlockContext<'_>,
) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| SegmentError::CorruptFile {
        detail: format!("invalid encryption key: {e}"),
    })?;

    // `aead` no longer re-exports an OS generator, and `generate_nonce` went
    // with it; the 96 bits come from the same source either way.
    let mut nonce_bytes = [0u8; NONCE_SIZE];
    fill_random(&mut nonce_bytes);
    let nonce = Nonce::try_from(&nonce_bytes[..]).map_err(|_| SegmentError::CorruptFile {
        detail: "nonce must be 12 bytes".into(),
    })?;
    let aad = context.associated_data();
    let ciphertext = cipher
        .encrypt(
            &nonce,
            aes_gcm::aead::Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
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
/// Expects `[nonce (12)][ciphertext + tag]`, and the same context the
/// block was encrypted under. A block moved to another column or another
/// segment fails to authenticate.
pub(crate) fn decrypt_block(
    encrypted: &[u8],
    key: &[u8; 32],
    context: BlockContext<'_>,
) -> Result<Vec<u8>> {
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

    let nonce =
        Nonce::try_from(&encrypted[..NONCE_SIZE]).map_err(|_| SegmentError::CorruptFile {
            detail: "nonce must be 12 bytes".into(),
        })?;
    let ciphertext = &encrypted[NONCE_SIZE..];
    let aad = context.associated_data();

    cipher
        .decrypt(
            &nonce,
            aes_gcm::aead::Payload {
                msg: ciphertext,
                aad: &aad,
            },
        )
        .map_err(|e| SegmentError::CorruptFile {
            detail: format!(
                "field decryption failed (wrong key, moved block, or tampered data): {e}"
            ),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixed context for the round-trip tests.
    fn ctx() -> BlockContext<'static> {
        BlockContext {
            column: "value",
            segment_created_at: 1_700_000_000_000_000_000,
        }
    }

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
        let encrypted = encrypt_block(plaintext, &key, ctx()).unwrap();
        assert_ne!(encrypted, plaintext);
        assert!(encrypted.len() > plaintext.len()); // nonce + tag overhead
        let decrypted = decrypt_block(&encrypted, &key, ctx()).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn different_nonces_produce_different_ciphertext() {
        let key = test_key();
        let plaintext = b"same data";
        let enc1 = encrypt_block(plaintext, &key, ctx()).unwrap();
        let enc2 = encrypt_block(plaintext, &key, ctx()).unwrap();
        // Nonces should differ (with overwhelming probability)
        assert_ne!(&enc1[..NONCE_SIZE], &enc2[..NONCE_SIZE]);
    }

    #[test]
    fn wrong_key_fails_decryption() {
        let key1 = test_key();
        let mut key2 = test_key();
        key2[0] ^= 0xFF; // flip a bit
        let encrypted = encrypt_block(b"secret", &key1, ctx()).unwrap();
        let result = decrypt_block(&encrypted, &key2, ctx());
        assert!(result.is_err());
    }

    #[test]
    fn tampered_ciphertext_fails() {
        let key = test_key();
        let mut encrypted = encrypt_block(b"important data", &key, ctx()).unwrap();
        // Flip a byte in the ciphertext
        let last = encrypted.len() - 1;
        encrypted[last] ^= 0x01;
        let result = decrypt_block(&encrypted, &key, ctx());
        assert!(result.is_err());
    }

    #[test]
    fn too_short_encrypted_block_fails() {
        let key = test_key();
        let result = decrypt_block(&[0u8; 10], &key, ctx());
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
        let encrypted = encrypt_block(b"", &key, ctx()).unwrap();
        assert_eq!(encrypted.len(), NONCE_SIZE + TAG_SIZE); // nonce + tag, no ciphertext
        let decrypted = decrypt_block(&encrypted, &key, ctx()).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn encrypt_large_block() {
        let key = test_key();
        let plaintext = vec![0xABu8; 1024 * 1024]; // 1 MiB
        let encrypted = encrypt_block(&plaintext, &key, ctx()).unwrap();
        let decrypted = decrypt_block(&encrypted, &key, ctx()).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    /// AES-GCM proves *who* wrote a block, never *which* block it is. With
    /// no associated data, one column's ciphertext decrypted cleanly in
    /// another column's place — so a file holding a public column and a
    /// private one under the same key leaked the private values to anyone
    /// who could swap the bytes, and the authentication tag still verified.
    #[test]
    fn a_block_moved_to_another_column_fails_to_authenticate() {
        let key = test_key();
        let salary = encrypt_block(
            b"180000",
            &key,
            BlockContext {
                column: "salary",
                segment_created_at: 42,
            },
        )
        .unwrap();

        let err = decrypt_block(
            &salary,
            &key,
            BlockContext {
                column: "public_note",
                segment_created_at: 42,
            },
        )
        .expect_err("a block must not decrypt under another column's name");
        assert!(err.to_string().contains("decryption failed"), "{err}");
    }

    /// The same for a block lifted into another segment: the header's
    /// creation timestamp is part of what the tag covers.
    #[test]
    fn a_block_moved_to_another_segment_fails_to_authenticate() {
        let key = test_key();
        let block = encrypt_block(
            b"readings",
            &key,
            BlockContext {
                column: "value",
                segment_created_at: 1_000,
            },
        )
        .unwrap();

        assert!(
            decrypt_block(
                &block,
                &key,
                BlockContext {
                    column: "value",
                    segment_created_at: 2_000,
                },
            )
            .is_err(),
            "a block must not decrypt inside a different segment"
        );

        // And it still reads correctly where it belongs.
        let plain = decrypt_block(
            &block,
            &key,
            BlockContext {
                column: "value",
                segment_created_at: 1_000,
            },
        )
        .expect("the block reads in its own place");
        assert_eq!(plain, b"readings");
    }
}
