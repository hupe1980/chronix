//! Resolving the field-encryption declaration into keys, at `open()`.
//!
//! The configuration names columns and **environment variables**; it never
//! holds key material. This is the only place in the tree that reads a
//! field-encryption key — a key written beside the data would add nothing
//! over full-disk encryption, whose key is on the machine too.
//!
//! **Fail closed, at startup.** Every declared key must resolve when the
//! database opens, including one no column currently names: that is what a
//! half-finished rotation looks like, and the segments written under it are
//! unreadable without it. A missing variable, a value that is not base64, or
//! one that is not exactly 32 bytes is a startup error naming the
//! **variable**, never the value.

use std::sync::Arc;

use base64::Engine as _;
use chronix_core::FieldEncryption;
use chronix_engine::segment::field_encryption::{
    FieldEncryptionConfig, FieldEncryptionKey, FieldKeyProvider, StaticKeyProvider,
};

use crate::error::{DbError, Result};

/// What the engine needs in order to read and write encrypted columns.
///
/// `None` where nothing is declared, so a database with no encryption pays
/// nothing and carries no provider.
#[derive(Clone)]
pub(crate) struct ResolvedFieldEncryption {
    /// Column → key, for the segment writer.
    pub(crate) writer: FieldEncryptionConfig,
    /// Key id → key, for every segment reader.
    pub(crate) reader: Arc<dyn FieldKeyProvider>,
}

impl std::fmt::Debug for ResolvedFieldEncryption {
    /// Prints what is configured and never what it is configured *with*.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedFieldEncryption")
            .field("columns", &"<redacted>")
            .finish()
    }
}

/// Read every declared key out of the environment.
///
/// # Errors
///
/// [`DbError::InvalidRequest`] naming the environment variable if it is
/// unset, not base64, or not 32 bytes. The value never appears in the
/// message — an error that prints the key is a key in the log.
pub(crate) fn resolve(settings: &FieldEncryption) -> Result<Option<ResolvedFieldEncryption>> {
    if settings.is_empty() && settings.keys.is_empty() {
        return Ok(None);
    }

    let mut provider = StaticKeyProvider::new();
    let mut keys: std::collections::BTreeMap<String, FieldEncryptionKey> =
        std::collections::BTreeMap::new();

    for (key_id, var) in &settings.keys {
        let encoded = std::env::var(var).map_err(|_| {
            DbError::InvalidRequest(format!(
                "field encryption key '{key_id}' reads ${var}, which is not set — every \
                 declared key must resolve at startup, including one no column names yet, \
                 because segments written under it are unreadable without it"
            ))
        })?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded.trim())
            .map_err(|_| {
                DbError::InvalidRequest(format!(
                    "field encryption key '{key_id}': ${var} is not valid base64"
                ))
            })?;
        let bytes: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
            DbError::InvalidRequest(format!(
                "field encryption key '{key_id}': ${var} decodes to {} bytes, and AES-256 \
                 needs exactly 32",
                bytes.len()
            ))
        })?;
        provider.add_key(key_id.clone(), bytes);
        keys.insert(
            key_id.clone(),
            FieldEncryptionKey::new(key_id.clone(), bytes),
        );
    }

    let mut writer = FieldEncryptionConfig::new();
    for (column, key_id) in &settings.columns {
        let key = keys.get(key_id).ok_or_else(|| {
            DbError::InvalidRequest(format!(
                "field encryption: column '{column}' names key '{key_id}', which is not declared"
            ))
        })?;
        writer.encrypt_column(column.clone(), key.clone());
    }

    Ok(Some(ResolvedFieldEncryption {
        writer,
        reader: Arc::new(provider),
    }))
}
