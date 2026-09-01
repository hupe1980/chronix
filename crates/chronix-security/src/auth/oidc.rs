//! OIDC discovery and JWKS caching.
//!
//! Fetches JSON Web Key Sets from `OpenID` Connect providers and caches
//! the parsed decoding keys with a configurable TTL.
//!
//! # TLS Trust Model
//!
//! OIDC/JWKS requests use the system's default TLS trust store (via
//! `reqwest` / `rustls` / `native-tls`) rather than certificate pinning.
//! This is the industry-standard approach for OIDC because:
//!
//! - Identity providers rotate TLS certificates independently of the
//!   relying party, making static pins fragile and operationally risky.
//! - The security guarantee comes from the **signed JWT** itself (RS256/
//!   ES256 signature verified against JWKS keys), not from the transport
//!   that delivered the JWKS.
//! - Certificate Transparency and CAA records provide equivalent
//!   protection against mis-issuance without the brittleness of pinning.
//!
//! Operators who require additional transport-layer assurance can
//! configure a custom `reqwest::Client` with certificate pinning or
//! mutual TLS outside this crate.
//!
//! # Example
//!
//! ```no_run
//! use chronix_security::auth::oidc::JwksCache;
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let cache = JwksCache::from_oidc_issuer(
//!     "https://accounts.google.com",
//!     Duration::from_secs(3600),
//! ).await?;
//!
//! let key = cache.get_key("my-kid").await?;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::{Jwk, JwkSet};
use jsonwebtoken::{Algorithm, DecodingKey};
use parking_lot::RwLock;
use serde::Deserialize;
use tokio::sync::Mutex as AsyncMutex;

use crate::auth::error::AuthError;

/// Default HTTP timeout for OIDC/JWKS requests.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// OIDC discovery
// ---------------------------------------------------------------------------

/// Subset of the `OpenID` Connect discovery document we care about.
#[derive(Debug, Deserialize)]
struct OidcDiscoveryDocument {
    /// The JWKS endpoint URL.
    jwks_uri: String,
    /// The issuer identifier — validated per OIDC Discovery §4.3.
    issuer: String,
}

/// Fetch the JWKS URI from an OIDC issuer's discovery endpoint.
///
/// Performs `GET {issuer}/.well-known/openid-configuration` and extracts
/// the `jwks_uri` field.
pub async fn discover_jwks_url(issuer: &str) -> Result<String, AuthError> {
    let url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );

    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|e| AuthError::Config(format!("failed to build HTTP client: {e}")))?;

    let resp =
        client.get(&url).send().await.map_err(|e| {
            AuthError::Config(format!("OIDC discovery request failed for {url}: {e}"))
        })?;

    if !resp.status().is_success() {
        return Err(AuthError::Config(format!(
            "OIDC discovery returned HTTP {}",
            resp.status()
        )));
    }

    let doc: OidcDiscoveryDocument = resp
        .json()
        .await
        .map_err(|e| AuthError::Config(format!("failed to parse OIDC discovery document: {e}")))?;

    // OIDC Discovery §4.3: issuer in the document MUST match the expected
    // issuer URL (with trailing-slash normalization).
    let expected = issuer.trim_end_matches('/');
    let actual = doc.issuer.trim_end_matches('/');
    if expected != actual {
        return Err(AuthError::Config(format!(
            "OIDC issuer mismatch: expected {expected}, document reports {actual}"
        )));
    }

    Ok(doc.jwks_uri)
}

// ---------------------------------------------------------------------------
// JWKS fetching & parsing
// ---------------------------------------------------------------------------

/// Fetch a [`JwkSet`] from a URL.
pub async fn fetch_jwks(url: &str) -> Result<JwkSet, AuthError> {
    let client = reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .map_err(|e| AuthError::Config(format!("failed to build HTTP client: {e}")))?;

    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| AuthError::Config(format!("JWKS fetch failed for {url}: {e}")))?;

    if !resp.status().is_success() {
        return Err(AuthError::Config(format!(
            "JWKS endpoint returned HTTP {}",
            resp.status()
        )));
    }

    resp.json::<JwkSet>()
        .await
        .map_err(|e| AuthError::Config(format!("failed to parse JWKS: {e}")))
}

/// Convert a [`Jwk`] into a [`DecodingKey`].
fn decoding_key_from_jwk(jwk: &Jwk) -> Result<DecodingKey, AuthError> {
    DecodingKey::from_jwk(jwk)
        .map_err(|e| AuthError::Config(format!("failed to create decoding key from JWK: {e}")))
}

/// Determine the [`Algorithm`] for a JWK, defaulting to RS256.
fn algorithm_for_jwk(jwk: &Jwk) -> Algorithm {
    jwk.common
        .key_algorithm
        .map_or(Algorithm::RS256, |ka| match ka {
            jsonwebtoken::jwk::KeyAlgorithm::RS256 => Algorithm::RS256,
            jsonwebtoken::jwk::KeyAlgorithm::RS384 => Algorithm::RS384,
            jsonwebtoken::jwk::KeyAlgorithm::RS512 => Algorithm::RS512,
            jsonwebtoken::jwk::KeyAlgorithm::ES256 => Algorithm::ES256,
            jsonwebtoken::jwk::KeyAlgorithm::ES384 => Algorithm::ES384,
            _ => Algorithm::RS256,
        })
}

// ---------------------------------------------------------------------------
// Cached key entry
// ---------------------------------------------------------------------------

/// A single cached key with its algorithm.
#[derive(Clone)]
pub struct CachedKey {
    /// The decoding key.
    pub key: DecodingKey,
    /// The algorithm associated with this key.
    pub algorithm: Algorithm,
}

impl std::fmt::Debug for CachedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedKey")
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// JwksCache
// ---------------------------------------------------------------------------

/// Inner cache state protected by a lock.
struct CacheInner {
    /// `kid` → cached key mapping.
    keys: HashMap<String, CachedKey>,
    /// When the cache was last populated, `None` if never fetched.
    fetched_at: Option<Instant>,
}

/// Caches JWKS decoding keys with a configurable TTL.
///
/// Thread-safe: all access goes through a [`parking_lot::RwLock`].
/// When the cache expires, the next call to [`get_key`](JwksCache::get_key)
/// triggers a background-safe refresh. A `tokio::sync::Mutex` serialises
/// concurrent refresh attempts to prevent thundering-herd behaviour.
pub struct JwksCache {
    /// The URL from which to fetch the JWKS.
    jwks_url: String,
    /// How long fetched keys remain valid.
    ttl: Duration,
    /// Maximum duration stale keys may be served after a failed refresh.
    max_stale: Duration,
    /// The cached keys.
    inner: Arc<RwLock<CacheInner>>,
    /// Serialises concurrent refresh attempts.
    refresh_mutex: Arc<AsyncMutex<()>>,
}

impl std::fmt::Debug for JwksCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwksCache")
            .field("jwks_url", &self.jwks_url)
            .field("ttl", &self.ttl)
            .field("cached_keys", &self.inner.read().keys.len())
            .finish()
    }
}

impl JwksCache {
    /// Create a new cache that will fetch keys from `jwks_url`.
    ///
    /// Does **not** fetch keys eagerly — the first call to
    /// [`get_key`](Self::get_key) or [`refresh`](Self::refresh) will
    /// populate the cache.
    #[must_use]
    pub fn new(jwks_url: String, ttl: Duration) -> Self {
        Self {
            jwks_url,
            ttl,
            max_stale: Duration::from_secs(24 * 3600), // 24 hours
            inner: Arc::new(RwLock::new(CacheInner {
                keys: HashMap::new(),
                fetched_at: None,
            })),
            refresh_mutex: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Create a cache by first performing OIDC discovery on the given issuer
    /// URL, then eagerly fetching the JWKS.
    pub async fn from_oidc_issuer(issuer: &str, ttl: Duration) -> Result<Self, AuthError> {
        let jwks_url = discover_jwks_url(issuer).await?;
        let cache = Self::new(jwks_url, ttl);
        cache.refresh().await?;
        Ok(cache)
    }

    /// Create a cache pre-populated with the given keys (useful for testing).
    #[must_use]
    pub fn from_keys(keys: HashMap<String, CachedKey>) -> Self {
        Self {
            jwks_url: String::new(),
            ttl: Duration::from_secs(3600),
            max_stale: Duration::from_secs(24 * 3600),
            inner: Arc::new(RwLock::new(CacheInner {
                keys,
                fetched_at: Some(Instant::now()),
            })),
            refresh_mutex: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Whether the cached keys have expired (or were never fetched).
    #[must_use]
    pub fn is_expired(&self) -> bool {
        let inner = self.inner.read();
        match inner.fetched_at {
            None => true,
            Some(t) => t.elapsed() > self.ttl,
        }
    }

    /// Number of keys currently cached.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.read().keys.len()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.read().keys.is_empty()
    }

    /// Fetch the JWKS from the configured URL and update the cache.
    ///
    /// On failure, previously cached keys are preserved (stale-on-error)
    /// so that transient network failures don't immediately break JWT
    /// validation for all requests.
    pub async fn refresh(&self) -> Result<(), AuthError> {
        if self.jwks_url.is_empty() {
            return Err(AuthError::Config(
                "cannot refresh: no JWKS URL configured (cache was created via from_keys)".into(),
            ));
        }

        let jwk_set = match fetch_jwks(&self.jwks_url).await {
            Ok(set) => set,
            Err(e) => {
                // If we have cached keys and they haven't exceeded max_stale,
                // log the error but keep serving stale.
                let inner = self.inner.read();
                if !inner.keys.is_empty() {
                    let too_old = inner
                        .fetched_at
                        .is_none_or(|t| t.elapsed() > self.max_stale);
                    if too_old {
                        tracing::error!(
                            url = %self.jwks_url,
                            error = %e,
                            max_stale_secs = self.max_stale.as_secs(),
                            "JWKS refresh failed and stale keys exceeded max_stale — rejecting"
                        );
                        return Err(e);
                    }
                    tracing::warn!(
                        url = %self.jwks_url,
                        error = %e,
                        "JWKS refresh failed, serving stale keys"
                    );
                    return Ok(());
                }
                return Err(e);
            }
        };

        let mut new_keys = HashMap::new();
        for jwk in &jwk_set.keys {
            let kid = jwk
                .common
                .key_id
                .clone()
                .unwrap_or_else(|| "default".to_string());
            let dk = decoding_key_from_jwk(jwk)?;
            let alg = algorithm_for_jwk(jwk);
            new_keys.insert(
                kid,
                CachedKey {
                    key: dk,
                    algorithm: alg,
                },
            );
        }

        let mut inner = self.inner.write();
        inner.keys = new_keys;
        inner.fetched_at = Some(Instant::now());

        tracing::debug!(
            keys = inner.keys.len(),
            url = %self.jwks_url,
            "JWKS cache refreshed"
        );

        Ok(())
    }

    /// Ensure the cache is fresh. Serialises concurrent refresh attempts
    /// so only one HTTP request is made even under high concurrency.
    async fn ensure_fresh(&self) -> Result<(), AuthError> {
        if !self.is_expired() {
            return Ok(());
        }
        // Serialise refreshes — only one task fetches at a time.
        let _guard = self.refresh_mutex.lock().await;
        // Double-check after acquiring the lock — another task may have
        // already refreshed while we were waiting.
        if !self.is_expired() {
            return Ok(());
        }
        self.refresh().await
    }

    /// Get a cached key by its `kid` (key ID).
    ///
    /// If the cache has expired, it is refreshed first. Concurrent callers
    /// share a single refresh request (thundering-herd protection).
    pub async fn get_key(&self, kid: &str) -> Result<CachedKey, AuthError> {
        self.ensure_fresh().await?;

        let inner = self.inner.read();
        inner
            .keys
            .get(kid)
            .cloned()
            .ok_or_else(|| AuthError::Config(format!("no JWKS key with kid={kid}")))
    }

    /// Get all cached keys (useful when the JWT has no `kid` header).
    ///
    /// If the cache has expired, it is refreshed first. Concurrent callers
    /// share a single refresh request (thundering-herd protection).
    pub async fn get_all_keys(&self) -> Result<Vec<CachedKey>, AuthError> {
        self.ensure_fresh().await?;

        let inner = self.inner.read();
        Ok(inner.keys.values().cloned().collect())
    }

    /// Returns the JWKS URL this cache is configured to fetch from.
    #[must_use]
    pub fn jwks_url(&self) -> &str {
        &self.jwks_url
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_from_keys_not_expired() {
        let mut keys = HashMap::new();
        keys.insert(
            "kid-1".to_string(),
            CachedKey {
                key: DecodingKey::from_secret(b"test"),
                algorithm: Algorithm::HS256,
            },
        );
        let cache = JwksCache::from_keys(keys);

        assert!(!cache.is_expired());
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
    }

    #[test]
    fn empty_cache_is_expired() {
        let cache = JwksCache::new("https://example.com/jwks".into(), Duration::from_secs(60));
        assert!(cache.is_expired());
        assert!(cache.is_empty());
    }

    #[test]
    fn cache_expires_after_ttl() {
        let cache = JwksCache {
            jwks_url: String::new(),
            ttl: Duration::from_millis(0), // immediate expiry
            max_stale: Duration::from_secs(24 * 3600),
            inner: Arc::new(RwLock::new(CacheInner {
                keys: HashMap::new(),
                fetched_at: Some(Instant::now().checked_sub(Duration::from_secs(1)).unwrap()),
            })),
            refresh_mutex: Arc::new(AsyncMutex::new(())),
        };
        assert!(cache.is_expired());
    }

    #[tokio::test]
    async fn get_key_from_prepopulated_cache() {
        let mut keys = HashMap::new();
        keys.insert(
            "my-kid".to_string(),
            CachedKey {
                key: DecodingKey::from_secret(b"secret"),
                algorithm: Algorithm::HS256,
            },
        );
        let cache = JwksCache::from_keys(keys);

        let k = cache.get_key("my-kid").await.unwrap();
        assert_eq!(k.algorithm, Algorithm::HS256);
    }

    #[tokio::test]
    async fn get_key_missing_kid_returns_error() {
        let cache = JwksCache::from_keys(HashMap::new());
        let err = cache.get_key("nonexistent").await.unwrap_err();
        assert!(matches!(err, AuthError::Config(_)));
    }

    #[tokio::test]
    async fn get_all_keys_from_prepopulated_cache() {
        let mut keys = HashMap::new();
        keys.insert(
            "kid-a".to_string(),
            CachedKey {
                key: DecodingKey::from_secret(b"a"),
                algorithm: Algorithm::RS256,
            },
        );
        keys.insert(
            "kid-b".to_string(),
            CachedKey {
                key: DecodingKey::from_secret(b"b"),
                algorithm: Algorithm::RS256,
            },
        );
        let cache = JwksCache::from_keys(keys);

        let all = cache.get_all_keys().await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn discover_url_construction() {
        // Verify the URL we would construct for OIDC discovery
        let issuer = "https://accounts.google.com";
        let url = format!(
            "{}/.well-known/openid-configuration",
            issuer.trim_end_matches('/')
        );
        assert_eq!(
            url,
            "https://accounts.google.com/.well-known/openid-configuration"
        );

        // With trailing slash
        let issuer = "https://example.com/";
        let url = format!(
            "{}/.well-known/openid-configuration",
            issuer.trim_end_matches('/')
        );
        assert_eq!(url, "https://example.com/.well-known/openid-configuration");
    }

    #[test]
    fn algorithm_for_jwk_defaults_to_rs256() {
        let jwk: Jwk = serde_json::from_value(serde_json::json!({
            "kty": "RSA",
            "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
            "e": "AQAB",
            "kid": "test-kid"
        }))
        .unwrap();

        let alg = algorithm_for_jwk(&jwk);
        assert_eq!(alg, Algorithm::RS256);
    }

    #[test]
    fn decoding_key_from_valid_jwk() {
        let jwk: Jwk = serde_json::from_value(serde_json::json!({
            "kty": "RSA",
            "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
            "e": "AQAB",
            "kid": "test-kid"
        }))
        .unwrap();

        let result = decoding_key_from_jwk(&jwk);
        assert!(result.is_ok());
    }
}
