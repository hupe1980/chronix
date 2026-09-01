//! Encrypting storage backend — transparent AES-256-GCM encryption layer.
//!
//! Wraps any [`StorageBackend`] to provide transparent encryption and
//! decryption of segment data at rest, using the `EncryptionService`
//! from `chronix_security::auth`.
//!
//! # Envelope Format
//!
//! ```text
//! [4-byte LE key_id_len][key_id bytes][12-byte nonce][AES-256-GCM ciphertext + tag]
//! ```
//!
//! The key ID is embedded in each encrypted segment so that key rotation
//! is seamless — segments encrypted with an older key can still be
//! decrypted as long as the key provider retains the retired key.
//!
//! # Thread Safety
//!
//! `EncryptingBackend<B>` is `Send + Sync` whenever `B` is, making it
//! usable as a drop-in replacement in async contexts.

use std::sync::Arc;

use tracing::debug;

use chronix_core::{NamespaceId, ShardId};

use crate::storage::backend::{SegmentPath, StorageBackend};
use crate::storage::error::{Result, StorageError};

/// Transparent encryption wrapper around any [`StorageBackend`].
///
/// Encrypts segment data before writing and decrypts after reading,
/// using AES-256-GCM via [`chronix_security::auth::EncryptionService`].
#[derive(Debug, Clone)]
pub struct EncryptingBackend<B> {
    inner: B,
    encryption: Arc<chronix_security::auth::EncryptionService>,
}

impl<B> EncryptingBackend<B> {
    /// Wrap `inner` backend with transparent encryption.
    pub fn new(inner: B, encryption: Arc<chronix_security::auth::EncryptionService>) -> Self {
        Self { inner, encryption }
    }

    /// Returns a reference to the inner (unencrypted) backend.
    #[must_use]
    pub fn inner(&self) -> &B {
        &self.inner
    }
}

/// Envelope: `[4-byte LE key_id_len][key_id][encrypted_data]`
fn seal(
    encryption: &chronix_security::auth::EncryptionService,
    plaintext: &[u8],
) -> Result<Vec<u8>> {
    let (ciphertext, key_id) = encryption
        .encrypt(plaintext)
        .map_err(|e| StorageError::Io(std::io::Error::other(e.to_string())))?;

    let key_id_bytes = key_id.as_bytes();
    let key_id_len = u32::try_from(key_id_bytes.len()).map_err(|_| {
        StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "key_id too long",
        ))
    })?;

    let mut envelope = Vec::with_capacity(4 + key_id_bytes.len() + ciphertext.len());
    envelope.extend_from_slice(&key_id_len.to_le_bytes());
    envelope.extend_from_slice(key_id_bytes);
    envelope.extend_from_slice(&ciphertext);
    Ok(envelope)
}

/// Parse envelope and decrypt.
fn unseal(
    encryption: &chronix_security::auth::EncryptionService,
    envelope: &[u8],
) -> Result<Vec<u8>> {
    if envelope.len() < 4 {
        return Err(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "encrypted envelope too short",
        )));
    }

    let key_id_len = u32::from_le_bytes(envelope[..4].try_into().expect("4 bytes")) as usize;

    if envelope.len() < 4 + key_id_len {
        return Err(StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "encrypted envelope truncated at key_id",
        )));
    }

    let key_id = std::str::from_utf8(&envelope[4..4 + key_id_len]).map_err(|_| {
        StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "key_id is not valid UTF-8",
        ))
    })?;

    let ciphertext = &envelope[4 + key_id_len..];

    encryption.decrypt(ciphertext, key_id).map_err(|e| {
        StorageError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("decryption failed: {e}"),
        ))
    })
}

impl<B: StorageBackend> StorageBackend for EncryptingBackend<B> {
    async fn put_segment(&self, path: &SegmentPath, data: &[u8]) -> Result<()> {
        let envelope = seal(&self.encryption, data)?;
        debug!(
            path = %path,
            plaintext_bytes = data.len(),
            encrypted_bytes = envelope.len(),
            "encrypting segment"
        );
        self.inner.put_segment(path, &envelope).await
    }

    async fn get_segment(&self, path: &SegmentPath) -> Result<Vec<u8>> {
        let envelope = self.inner.get_segment(path).await?;
        let plaintext = unseal(&self.encryption, &envelope)?;
        debug!(
            path = %path,
            encrypted_bytes = envelope.len(),
            plaintext_bytes = plaintext.len(),
            "decrypting segment"
        );
        Ok(plaintext)
    }

    async fn get_range(&self, path: &SegmentPath, offset: u64, length: usize) -> Result<Vec<u8>> {
        // Range reads on encrypted data require decrypting the entire segment
        // because AES-GCM is authenticated — partial reads cannot be verified.
        // This is the correct trade-off: security > performance for range reads.
        let plaintext = self.get_segment(path).await?;
        let end = offset as usize + length;
        if end > plaintext.len() {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "range [{offset}..{end}) exceeds decrypted segment size {}",
                    plaintext.len()
                ),
            )));
        }
        Ok(plaintext[offset as usize..end].to_vec())
    }

    async fn delete_segment(&self, path: &SegmentPath) -> Result<()> {
        self.inner.delete_segment(path).await
    }

    async fn list_segments(
        &self,
        namespace: &NamespaceId,
        shard_id: &ShardId,
    ) -> Result<Vec<SegmentPath>> {
        self.inner.list_segments(namespace, shard_id).await
    }

    async fn exists(&self, path: &SegmentPath) -> Result<bool> {
        self.inner.exists(path).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::local::LocalFsBackend;
    use chronix_security::auth::error::AuthError;
    use chronix_security::auth::{EncryptionService, KeyProvider, SecretKey};

    /// Simple in-memory key provider for tests.
    struct TestKeyProvider {
        key: Vec<u8>,
        key_id: String,
    }

    impl TestKeyProvider {
        fn new() -> Self {
            Self {
                key: vec![0xAB; 32], // 256-bit key
                key_id: "test-key-1".to_string(),
            }
        }
    }

    impl std::fmt::Debug for TestKeyProvider {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestKeyProvider")
                .field("key_id", &self.key_id)
                .finish()
        }
    }

    impl KeyProvider for TestKeyProvider {
        fn current_key(&self) -> std::result::Result<(SecretKey, String), AuthError> {
            Ok((SecretKey::new(self.key.clone()), self.key_id.clone()))
        }

        fn key_by_id(&self, key_id: &str) -> std::result::Result<SecretKey, AuthError> {
            if key_id == self.key_id {
                Ok(SecretKey::new(self.key.clone()))
            } else {
                Err(AuthError::KeyNotFound(key_id.to_string()))
            }
        }

        fn key_ids(&self) -> Vec<String> {
            vec![self.key_id.clone()]
        }
    }

    fn make_backend(dir: &std::path::Path) -> EncryptingBackend<LocalFsBackend> {
        let local = LocalFsBackend::new(dir);
        let enc = Arc::new(EncryptionService::new(Box::new(TestKeyProvider::new())));
        EncryptingBackend::new(local, enc)
    }

    fn test_path() -> SegmentPath {
        SegmentPath::new(
            NamespaceId::default_namespace(),
            ShardId(1),
            "test_segment.csx",
        )
        .unwrap()
    }

    #[tokio::test]
    async fn encrypt_decrypt_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let backend = make_backend(dir.path());
        let path = test_path();

        let data = b"Hello, encrypted segment!";
        backend.put_segment(&path, data).await.unwrap();

        // Verify on-disk data is NOT plaintext
        let raw = LocalFsBackend::new(dir.path())
            .get_segment(&path)
            .await
            .unwrap();
        assert_ne!(&raw, data, "on-disk data must be encrypted");

        // Verify decrypted data matches original
        let decrypted = backend.get_segment(&path).await.unwrap();
        assert_eq!(decrypted, data);
    }

    #[tokio::test]
    async fn range_read_after_encryption() {
        let dir = tempfile::tempdir().unwrap();
        let backend = make_backend(dir.path());
        let path = test_path();

        let data = b"0123456789ABCDEF";
        backend.put_segment(&path, data).await.unwrap();

        let range = backend.get_range(&path, 4, 8).await.unwrap();
        assert_eq!(&range, &data[4..12]);
    }

    #[tokio::test]
    async fn range_read_out_of_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let backend = make_backend(dir.path());
        let path = test_path();

        backend.put_segment(&path, b"short").await.unwrap();

        let err = backend.get_range(&path, 0, 100).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn delete_encrypted_segment() {
        let dir = tempfile::tempdir().unwrap();
        let backend = make_backend(dir.path());
        let path = test_path();

        backend.put_segment(&path, b"delete me").await.unwrap();
        assert!(backend.exists(&path).await.unwrap());

        backend.delete_segment(&path).await.unwrap();
        assert!(!backend.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn list_encrypted_segments() {
        let dir = tempfile::tempdir().unwrap();
        let backend = make_backend(dir.path());
        let path = test_path();

        backend.put_segment(&path, b"data").await.unwrap();

        let segments = backend
            .list_segments(&NamespaceId::default_namespace(), &ShardId(1))
            .await
            .unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].segment_name(), "test_segment.csx");
    }

    #[tokio::test]
    async fn tampered_data_fails_decrypt() {
        let dir = tempfile::tempdir().unwrap();
        let backend = make_backend(dir.path());
        let path = test_path();

        backend.put_segment(&path, b"sensitive data").await.unwrap();

        // Tamper with the encrypted data on disk
        let raw_backend = LocalFsBackend::new(dir.path());
        let mut raw = raw_backend.get_segment(&path).await.unwrap();
        if let Some(last) = raw.last_mut() {
            *last ^= 0xFF; // flip bits in the auth tag
        }
        raw_backend.put_segment(&path, &raw).await.unwrap();

        // Decryption must fail due to authentication tag mismatch
        let result = backend.get_segment(&path).await;
        assert!(result.is_err(), "tampered segment must fail decryption");
    }

    #[tokio::test]
    async fn wrong_key_fails_decrypt() {
        let dir = tempfile::tempdir().unwrap();
        let backend = make_backend(dir.path());
        let path = test_path();

        backend.put_segment(&path, b"secret").await.unwrap();

        // Create a different backend with a different key
        struct OtherKey;
        impl std::fmt::Debug for OtherKey {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "OtherKey")
            }
        }
        impl KeyProvider for OtherKey {
            fn current_key(&self) -> std::result::Result<(SecretKey, String), AuthError> {
                Ok((SecretKey::new(vec![0xCD; 32]), "test-key-1".into()))
            }
            fn key_by_id(&self, _: &str) -> std::result::Result<SecretKey, AuthError> {
                Ok(SecretKey::new(vec![0xCD; 32]))
            }
            fn key_ids(&self) -> Vec<String> {
                vec!["test-key-1".into()]
            }
        }

        let wrong_backend = EncryptingBackend::new(
            LocalFsBackend::new(dir.path()),
            Arc::new(EncryptionService::new(Box::new(OtherKey))),
        );

        let result = wrong_backend.get_segment(&path).await;
        assert!(result.is_err(), "wrong key must fail decryption");
    }

    #[tokio::test]
    async fn large_segment_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let backend = make_backend(dir.path());
        let path = test_path();

        // 1 MiB of pseudo-random data
        let data: Vec<u8> = (0..1_048_576u32).map(|i| (i % 256) as u8).collect();
        backend.put_segment(&path, &data).await.unwrap();

        let decrypted = backend.get_segment(&path).await.unwrap();
        assert_eq!(decrypted, data);
    }

    // ── Additional edge-case tests ────────────────────────────────

    #[tokio::test]
    async fn exists_nonexistent_encrypted() {
        let dir = tempfile::tempdir().unwrap();
        let backend = make_backend(dir.path());
        let path = test_path();
        assert!(!backend.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn empty_segment_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let backend = make_backend(dir.path());
        let path = test_path();

        backend.put_segment(&path, b"").await.unwrap();
        let data = backend.get_segment(&path).await.unwrap();
        assert!(data.is_empty());
    }

    #[tokio::test]
    async fn range_read_full_segment() {
        let dir = tempfile::tempdir().unwrap();
        let backend = make_backend(dir.path());
        let path = test_path();

        let original = b"complete data";
        backend.put_segment(&path, original).await.unwrap();

        let range = backend.get_range(&path, 0, original.len()).await.unwrap();
        assert_eq!(&range, original);
    }
}
