//! JWT and OIDC token validation.
//!
//! Supports the HMAC (`HS*`), RSA (`RS*`, `PS*`), elliptic-curve (`ES*`)
//! and Edwards-curve (`EdDSA`) families.
//! Can fetch JWKS from an OIDC discovery endpoint or use a static secret.
//!
//! # Example
//!
//! ```no_run
//! use chronix_security::auth::jwt::{JwtValidator, JwtConfig};
//! use zeroize::Zeroizing;
//!
//! let config = JwtConfig {
//!     secret: Some(Zeroizing::new("my-secret-key-at-least-32-bytes!".to_string())),
//!     issuer: Some("https://auth.example.com".to_string()),
//!     audience: Some("chronix".to_string()),
//!     ..Default::default()
//! };
//! let validator = JwtValidator::new(config).unwrap();
//! ```

use std::collections::HashMap;

use std::sync::Arc;

use dashmap::DashMap;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, TokenData, Validation};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::auth::error::AuthError;
use crate::auth::oidc::JwksCache;

/// Serialize `Option<Zeroizing<String>>` as `Option<String>`.
fn ser_zeroizing_opt<S: serde::Serializer>(
    val: &Option<Zeroizing<String>>,
    ser: S,
) -> Result<S::Ok, S::Error> {
    match val {
        Some(z) => ser.serialize_some(z.as_str()),
        None => ser.serialize_none(),
    }
}

/// Deserialize `Option<String>` into `Option<Zeroizing<String>>`.
fn de_zeroizing_opt<'de, D: serde::Deserializer<'de>>(
    de: D,
) -> Result<Option<Zeroizing<String>>, D::Error> {
    let opt: Option<String> = Option::deserialize(de)?;
    Ok(opt.map(Zeroizing::new))
}

/// Serde default helper that returns `true`.
fn default_true() -> bool {
    true
}

/// JWT validation configuration.
///
/// **Security:** The `secret` and `public_key_pem` fields are redacted
/// in `Debug` output to prevent credential leakage in logs.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct JwtConfig {
    /// HMAC secret for HS256/HS384/HS512 (base64-encoded or raw).
    #[serde(
        serialize_with = "ser_zeroizing_opt",
        deserialize_with = "de_zeroizing_opt",
        default
    )]
    pub secret: Option<Zeroizing<String>>,
    /// Public key PEM for the asymmetric families.
    ///
    /// RSA (`RS*`, `PS*`) and elliptic-curve (`ES*`) keys both live here:
    /// the family comes from `algorithm`, and mixing the two used to mean
    /// an EC key was parsed as RSA, failed, and left the validator with no
    /// key at all — which every token then failed against.
    #[serde(
        serialize_with = "ser_zeroizing_opt",
        deserialize_with = "de_zeroizing_opt",
        default
    )]
    pub public_key_pem: Option<Zeroizing<String>>,
    /// Expected issuer (`iss` claim).
    pub issuer: Option<String>,
    /// Expected audience (`aud` claim).
    pub audience: Option<String>,
    /// Whether to require audience validation.  When `true` (default)
    /// and `audience` is `None`, validator construction fails.
    /// Set to `false` only if tokens legitimately carry no audience.
    #[serde(default = "default_true")]
    pub require_audience: bool,
    /// JWKS URL for fetching public keys (OIDC discovery).
    pub jwks_url: Option<String>,
    /// Claim name for the principal identity (default: `sub`).
    pub subject_claim: Option<String>,
    /// Algorithm to use. Default: HS256.
    pub algorithm: Option<String>,
    /// Claim name or dot-separated path for role extraction (default: `roles`).
    /// Examples: `"roles"`, `"realm_access.roles"`, `"https://example.com/roles"`.
    pub role_claim: Option<String>,
}

impl std::fmt::Debug for JwtConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtConfig")
            .field("secret", &self.secret.as_ref().map(|_| "[REDACTED]"))
            .field(
                "public_key_pem",
                &self.public_key_pem.as_ref().map(|_| "[REDACTED]"),
            )
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("jwks_url", &self.jwks_url)
            .field("subject_claim", &self.subject_claim)
            .field("role_claim", &self.role_claim)
            .field("algorithm", &self.algorithm)
            .finish()
    }
}

/// Resolve a dot-separated claim path against a claim map.
///
/// `realm_access.roles` is Keycloak, `https://example.com/roles` is Auth0 —
/// the second contains dots that are not path separators, so a path that
/// looks like a URI is treated as one key.
#[must_use]
pub fn resolve_claim_path<'a>(
    claims: &'a std::collections::HashMap<String, serde_json::Value>,
    path: &str,
) -> Option<&'a serde_json::Value> {
    if !path.contains('.') || path.contains("://") {
        return claims.get(path);
    }
    let segments: Vec<&str> = path.split('.').collect();
    let mut current: &serde_json::Value = claims.get(*segments.first()?)?;
    for segment in &segments[1..] {
        current = current.as_object()?.get(*segment)?;
    }
    Some(current)
}

/// Validated JWT claims.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Subject — the principal identity.
    #[serde(default)]
    pub sub: Option<String>,
    /// Issuer.
    #[serde(default)]
    pub iss: Option<String>,
    /// Audience.
    #[serde(default)]
    pub aud: Option<JwtAudience>,
    /// Expiration time (Unix timestamp).
    #[serde(default)]
    pub exp: Option<u64>,
    /// Issued at (Unix timestamp).
    #[serde(default)]
    pub iat: Option<u64>,
    /// Email claim.
    #[serde(default)]
    pub email: Option<String>,
    /// JWT ID for replay detection. Optional per RFC 7519.
    #[serde(default)]
    pub jti: Option<String>,
    /// Custom claims.
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// JWT audience can be a single string or an array.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JwtAudience {
    /// Single audience string.
    Single(String),
    /// Multiple audience strings.
    Multiple(Vec<String>),
}

/// JWT token validator.
///
/// Supports three key sources (checked in order):
/// 1. Static secret or RSA PEM key
/// 2. JWKS cache (fetched from OIDC discovery or direct JWKS URL)
#[derive(Clone)]
pub struct JwtValidator {
    config: JwtConfig,
    decoding_key: Option<DecodingKey>,
    validation: Validation,
    /// Optional JWKS cache for OIDC / dynamic key rotation.
    jwks_cache: Option<Arc<JwksCache>>,
    /// Bounded `jti` replay cache. Key is the jti string,
    /// value is the token expiry timestamp (Unix seconds). Entries
    /// are evicted when they exceed `exp`.
    jti_cache: Arc<DashMap<String, u64>>,
}

impl std::fmt::Debug for JwtValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtValidator")
            .field("config", &self.config)
            .field("has_decoding_key", &self.decoding_key.is_some())
            .field("has_jwks_cache", &self.jwks_cache.is_some())
            .finish()
    }
}

impl JwtValidator {
    /// Create a new JWT validator from configuration.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Config`] if the configured algorithm string
    /// is unrecognized (fail-closed).
    pub fn new(config: JwtConfig) -> Result<Self, AuthError> {
        let algorithm = match config.algorithm.as_deref() {
            Some(s) => parse_algorithm(s)?,
            None => Algorithm::HS256,
        };

        let mut validation = Validation::new(algorithm);

        if let Some(ref iss) = config.issuer {
            validation.set_issuer(&[iss]);
        }

        if let Some(ref aud) = config.audience {
            validation.set_audience(&[aud]);
        } else if config.require_audience {
            return Err(AuthError::Config(
                "audience is required but not configured; set `audience` or \
                 set `require_audience = false` to opt out"
                    .into(),
            ));
        } else {
            validation.validate_aud = false;
        }

        // Require exp claim
        validation.set_required_spec_claims(&["exp"]);

        // Validate `nbf` (not-before) claim when present.
        // The claim is not required — many IdPs omit it — but when
        // present, the token must not be accepted before that time.
        validation.validate_nbf = true;

        // **The key is chosen by the algorithm's family, not by which
        // field happens to be set.** A configuration naming RS256 while a
        // `secret` was present built an HMAC key from the secret and
        // verified an RSA signature against it, so every token was
        // rejected — a 401 with nothing in the log to say why. Picking the
        // key from the algorithm makes the mismatch a startup error.
        // **The key is chosen by the algorithm's family, not by whichever
        // field happens to be set.** A configuration naming RS256 while a
        // `secret` was present built an HMAC key from that secret and
        // verified an RSA signature against it, so every token was
        // rejected — a 401 with nothing in the log to say why.
        //
        // No key material at all stays `None`: a JWKS cache may be attached
        // afterwards, and that is the whole point of an OIDC deployment.
        // Material that cannot belong to this algorithm is a startup error,
        // because it can never verify anything.
        let hmac_family = matches!(
            algorithm,
            Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512
        );
        if hmac_family && config.public_key_pem.is_some() {
            return Err(AuthError::Config(format!(
                "{algorithm:?} is an HMAC algorithm and takes `secret`, not `public_key_pem`"
            )));
        }
        if !hmac_family && config.secret.is_some() && config.public_key_pem.is_none() {
            return Err(AuthError::Config(format!(
                "{algorithm:?} verifies with a public key; set `public_key_pem` \
                 (a `secret` cannot verify an asymmetric signature)"
            )));
        }

        let decoding_key = match (
            algorithm,
            config.secret.as_ref(),
            config.public_key_pem.as_ref(),
        ) {
            (_, None, None) => None,
            (Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512, Some(secret), _) => {
                // NIST SP 800-107: the key is at least as long as the hash.
                if secret.len() < 32 {
                    return Err(AuthError::Config(
                        "HMAC secret must be at least 32 bytes".into(),
                    ));
                }
                Some(DecodingKey::from_secret(secret.as_bytes()))
            }
            (
                Algorithm::RS256
                | Algorithm::RS384
                | Algorithm::RS512
                | Algorithm::PS256
                | Algorithm::PS384
                | Algorithm::PS512,
                _,
                Some(pem),
            ) => Some(DecodingKey::from_rsa_pem(pem.as_bytes()).map_err(|e| {
                AuthError::Config(format!(
                    "{algorithm:?} public key is not valid RSA PEM: {e}"
                ))
            })?),
            (Algorithm::ES256 | Algorithm::ES384, _, Some(pem)) => {
                Some(DecodingKey::from_ec_pem(pem.as_bytes()).map_err(|e| {
                    AuthError::Config(format!("{algorithm:?} public key is not valid EC PEM: {e}"))
                })?)
            }
            (Algorithm::EdDSA, _, Some(pem)) => {
                Some(DecodingKey::from_ed_pem(pem.as_bytes()).map_err(|e| {
                    AuthError::Config(format!("EdDSA public key is not valid PEM: {e}"))
                })?)
            }
            (other, _, _) => {
                return Err(AuthError::Config(format!(
                    "JWT algorithm {other:?} is not supported"
                )))
            }
        };

        Ok(Self {
            config,
            decoding_key,
            validation,
            jwks_cache: None,
            jti_cache: Arc::new(DashMap::new()),
        })
    }

    /// Attach a [`JwksCache`] for dynamic OIDC key resolution.
    ///
    /// When set, [`validate_async`](Self::validate_async) will try the JWKS
    /// cache if no static key is configured (or if static validation fails).
    #[must_use]
    pub fn with_jwks_cache(mut self, cache: Arc<JwksCache>) -> Self {
        self.jwks_cache = Some(cache);
        self
    }

    /// Returns a reference to the attached JWKS cache, if any.
    #[must_use]
    pub fn jwks_cache(&self) -> Option<&Arc<JwksCache>> {
        self.jwks_cache.as_ref()
    }

    /// Validate a JWT token string and return the claims.
    pub fn validate(&self, token: &str) -> Result<JwtClaims, AuthError> {
        let key = self
            .decoding_key
            .as_ref()
            .ok_or_else(|| AuthError::Config("no decoding key configured".into()))?;

        let token_data: TokenData<JwtClaims> =
            decode(token, key, &self.validation).map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::ExpiredJwt,
                _ => AuthError::InvalidJwt(e.to_string()),
            })?;

        // Replay protection — if `jti` is present, reject duplicates.
        self.check_jti_replay(&token_data.claims)?;

        Ok(token_data.claims)
    }

    /// Check for JWT replay attacks using the `jti` claim.
    ///
    /// If a `jti` is present, ensures it hasn't been seen before. The cache
    /// is bounded: expired entries are evicted on each check to prevent
    /// unbounded growth.
    fn check_jti_replay(&self, claims: &JwtClaims) -> Result<(), AuthError> {
        // Evict expired entries (bounded maintenance).
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.jti_cache.retain(|_, exp| *exp > now);

        if let Some(ref jti) = claims.jti {
            // Hard cap to prevent DoS via large cache.
            // Fail-closed when cache is exhausted — reject the
            // token instead of silently skipping replay detection.
            const MAX_JTI_CACHE: usize = 100_000;
            if self.jti_cache.len() >= MAX_JTI_CACHE {
                tracing::error!(
                    cache_size = self.jti_cache.len(),
                    "jti replay cache exhausted — rejecting token (fail-closed)"
                );
                return Err(AuthError::InvalidJwt(
                    "replay cache exhausted — try again later".into(),
                ));
            }
            let exp = claims.exp.unwrap_or(now + 3600);
            if self.jti_cache.insert(jti.clone(), exp).is_some() {
                return Err(AuthError::InvalidJwt(
                    "token replay detected (duplicate jti)".into(),
                ));
            }
        }
        Ok(())
    }

    /// Validate a JWT token with JWKS cache support (async).
    ///
    /// Resolution order:
    /// 1. Try static key (secret / RSA PEM) if configured.
    /// 2. Fall back to the JWKS cache: decode the JWT header to extract `kid`,
    ///    fetch the matching key from the cache, and validate.
    /// 3. If no `kid` header, try all cached keys.
    pub async fn validate_async(&self, token: &str) -> Result<JwtClaims, AuthError> {
        // 1. Try static key first — if configured, errors are authoritative.
        if self.decoding_key.is_some() {
            match self.validate(token) {
                ok @ Ok(_) => return ok,
                Err(e) => {
                    // If we also have a JWKS cache, give it a chance.
                    // But for expiry errors there is no point retrying.
                    if self.jwks_cache.is_none() || matches!(e, AuthError::ExpiredJwt) {
                        return Err(e);
                    }
                    tracing::debug!(error = %e, "static key validation failed, falling back to JWKS");
                }
            }
        }

        // 2. Try JWKS cache.
        let cache = self
            .jwks_cache
            .as_ref()
            .ok_or_else(|| AuthError::Config("no decoding key or JWKS cache configured".into()))?;

        // Decode header to get kid.
        let header = decode_header(token)
            .map_err(|e| AuthError::InvalidJwt(format!("failed to decode JWT header: {e}")))?;

        if let Some(kid) = &header.kid {
            let cached = cache.get_key(kid).await?;
            let mut validation = self.validation.clone();
            validation.algorithms = vec![cached.algorithm];
            let token_data: TokenData<JwtClaims> = decode(token, &cached.key, &validation)
                .map_err(|e| match e.kind() {
                    jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::ExpiredJwt,
                    _ => AuthError::InvalidJwt(e.to_string()),
                })?;
            return Ok(token_data.claims);
        }

        // 3. No kid — try all cached keys.
        let all_keys = cache.get_all_keys().await?;
        let mut last_err = AuthError::Config("no JWKS keys available".into());
        for cached in &all_keys {
            let mut validation = self.validation.clone();
            validation.algorithms = vec![cached.algorithm];
            match decode::<JwtClaims>(token, &cached.key, &validation) {
                Ok(td) => return Ok(td.claims),
                Err(e) => {
                    match e.kind() {
                        jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
                            // The signature matched this key but the token is
                            // expired — return immediately. Trying further keys
                            // cannot fix an expired `exp` claim, and continuing
                            // would risk overwriting this informative error with
                            // a misleading `InvalidJwt` from a non-matching key.
                            return Err(AuthError::ExpiredJwt);
                        }
                        _ => {
                            last_err = AuthError::InvalidJwt(e.to_string());
                        }
                    }
                }
            }
        }

        Err(last_err)
    }

    /// Extract the principal identity from JWT claims.
    /// The claim path roles are read from (`roles` unless configured).
    #[must_use]
    pub fn role_claim(&self) -> &str {
        self.config.role_claim.as_deref().unwrap_or("roles")
    }

    /// The roles this token carries, as `Chronix::Role` names.
    ///
    /// Resolved here rather than at a gate, so every gate sees the same set.
    /// The administrative gate used to do it alone, which is why a role
    /// granted an admin operation and matched nothing in a namespace policy.
    ///
    /// Accepts an array of strings, or a single string — identity providers
    /// emit both, and a one-element list is often flattened on the way
    /// through. A space-separated string is *not* split: `scope` is
    /// space-separated and `roles` is not, and guessing turns one role named
    /// `"data science"` into two that do not exist.
    #[must_use]
    pub fn extract_roles(&self, claims: &JwtClaims) -> Vec<String> {
        match resolve_claim_path(&claims.extra, self.role_claim()) {
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect(),
            Some(serde_json::Value::String(s)) => vec![s.clone()],
            _ => Vec::new(),
        }
    }

    /// The principal this token names, from `subject_claim` (default `sub`).
    #[must_use]
    pub fn extract_principal(&self, claims: &JwtClaims) -> Option<String> {
        let claim_name = self.config.subject_claim.as_deref().unwrap_or("sub");

        match claim_name {
            "sub" => claims.sub.clone(),
            "email" => claims.email.clone(),
            other => claims
                .extra
                .get(other)
                .and_then(|v| v.as_str())
                .map(String::from),
        }
    }
}

/// Parse algorithm string to `jsonwebtoken::Algorithm`.
///
/// Returns an error for unrecognized algorithm strings so that
/// configuration typos are caught at startup (fail-closed).
fn parse_algorithm(s: &str) -> Result<Algorithm, AuthError> {
    match s.to_uppercase().as_str() {
        "HS256" => Ok(Algorithm::HS256),
        "HS384" => Ok(Algorithm::HS384),
        "HS512" => Ok(Algorithm::HS512),
        "RS256" => Ok(Algorithm::RS256),
        "RS384" => Ok(Algorithm::RS384),
        "RS512" => Ok(Algorithm::RS512),
        "ES256" => Ok(Algorithm::ES256),
        "ES384" => Ok(Algorithm::ES384),
        "PS256" => Ok(Algorithm::PS256),
        "PS384" => Ok(Algorithm::PS384),
        "PS512" => Ok(Algorithm::PS512),
        "EDDSA" => Ok(Algorithm::EdDSA),
        unknown => Err(AuthError::Config(format!(
            "unrecognized JWT algorithm: {unknown}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};

    fn make_token(claims: &JwtClaims, secret: &str) -> String {
        let header = Header::new(Algorithm::HS256);
        let key = EncodingKey::from_secret(secret.as_bytes());
        encode(&header, claims, &key).unwrap()
    }

    fn future_exp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600
    }

    #[test]
    fn reject_short_hmac_secret() {
        let config = JwtConfig {
            secret: Some(Zeroizing::new("too-short".into())),
            ..Default::default()
        };
        let err = JwtValidator::new(config).unwrap_err();
        assert!(matches!(err, AuthError::Config(_)));
    }

    #[test]
    fn reject_missing_audience_when_required() {
        let config = JwtConfig {
            secret: Some(Zeroizing::new("test-secret-at-least-32-bytes!!!".into())),
            require_audience: true,
            audience: None,
            ..Default::default()
        };
        let err = JwtValidator::new(config).unwrap_err();
        assert!(matches!(err, AuthError::Config(_)));
    }

    #[test]
    fn allow_missing_audience_when_not_required() {
        let config = JwtConfig {
            secret: Some(Zeroizing::new("test-secret-at-least-32-bytes!!!".into())),
            require_audience: false,
            audience: None,
            ..Default::default()
        };
        assert!(JwtValidator::new(config).is_ok());
    }

    #[test]
    fn validate_valid_token() {
        let secret = "test-secret-key-for-unit-tests!!"; // 32 bytes
        let config = JwtConfig {
            secret: Some(Zeroizing::new(secret.into())),
            ..Default::default()
        };
        let validator = JwtValidator::new(config).unwrap();

        let claims = JwtClaims {
            sub: Some("user-123".into()),
            exp: Some(future_exp()),
            iss: None,
            aud: None,
            iat: None,
            email: None,
            jti: None,
            extra: HashMap::new(),
        };

        let token = make_token(&claims, secret);
        let validated = validator.validate(&token).unwrap();
        assert_eq!(validated.sub, Some("user-123".into()));
    }

    #[test]
    fn reject_expired_token() {
        let secret = "test-secret-at-least-32-bytes!!!";
        let config = JwtConfig {
            secret: Some(Zeroizing::new(secret.into())),
            ..Default::default()
        };
        let validator = JwtValidator::new(config).unwrap();

        let claims = JwtClaims {
            sub: Some("user".into()),
            exp: Some(0), // expired
            iss: None,
            aud: None,
            iat: None,
            email: None,
            jti: None,
            extra: HashMap::new(),
        };

        let token = make_token(&claims, secret);
        let err = validator.validate(&token).unwrap_err();
        assert!(matches!(err, AuthError::ExpiredJwt));
    }

    #[test]
    fn reject_wrong_secret() {
        let config = JwtConfig {
            secret: Some(Zeroizing::new("correct-secret-at-least-32bytes!".into())),
            ..Default::default()
        };
        let validator = JwtValidator::new(config).unwrap();

        let claims = JwtClaims {
            sub: Some("user".into()),
            exp: Some(future_exp()),
            iss: None,
            aud: None,
            iat: None,
            email: None,
            jti: None,
            extra: HashMap::new(),
        };

        let token = make_token(&claims, "wrong-secret--at-least-32bytes!");
        let err = validator.validate(&token).unwrap_err();
        assert!(matches!(err, AuthError::InvalidJwt(_)));
    }

    #[test]
    fn validate_with_issuer() {
        let secret = "test-secret-at-least-32-bytes!!!";
        let config = JwtConfig {
            secret: Some(Zeroizing::new(secret.into())),
            issuer: Some("https://auth.example.com".into()),
            ..Default::default()
        };
        let validator = JwtValidator::new(config).unwrap();

        let claims = JwtClaims {
            sub: Some("user".into()),
            iss: Some("https://auth.example.com".into()),
            exp: Some(future_exp()),
            aud: None,
            iat: None,
            email: None,
            jti: None,
            extra: HashMap::new(),
        };

        let token = make_token(&claims, secret);
        validator.validate(&token).unwrap();
    }

    #[test]
    fn reject_wrong_issuer() {
        let secret = "test-secret-at-least-32-bytes!!!";
        let config = JwtConfig {
            secret: Some(Zeroizing::new(secret.into())),
            issuer: Some("https://auth.example.com".into()),
            ..Default::default()
        };
        let validator = JwtValidator::new(config).unwrap();

        let claims = JwtClaims {
            sub: Some("user".into()),
            iss: Some("https://evil.com".into()),
            exp: Some(future_exp()),
            aud: None,
            iat: None,
            email: None,
            jti: None,
            extra: HashMap::new(),
        };

        let token = make_token(&claims, secret);
        assert!(validator.validate(&token).is_err());
    }

    #[test]
    fn extract_principal_from_sub() {
        let config = JwtConfig::default();
        let validator = JwtValidator::new(config).unwrap();

        let claims = JwtClaims {
            sub: Some("principal-1".into()),
            email: Some("user@example.com".into()),
            exp: None,
            iss: None,
            aud: None,
            iat: None,
            jti: None,
            extra: HashMap::new(),
        };

        assert_eq!(
            validator.extract_principal(&claims),
            Some("principal-1".into())
        );
    }

    #[test]
    fn extract_principal_from_email() {
        let config = JwtConfig {
            subject_claim: Some("email".into()),
            ..Default::default()
        };
        let validator = JwtValidator::new(config).unwrap();

        let claims = JwtClaims {
            sub: Some("user-id".into()),
            email: Some("admin@chronix.io".into()),
            exp: None,
            iss: None,
            aud: None,
            iat: None,
            jti: None,
            extra: HashMap::new(),
        };

        assert_eq!(
            validator.extract_principal(&claims),
            Some("admin@chronix.io".into())
        );
    }

    #[test]
    fn extract_principal_from_custom_claim() {
        let config = JwtConfig {
            subject_claim: Some("username".into()),
            ..Default::default()
        };
        let validator = JwtValidator::new(config).unwrap();

        let mut extra = HashMap::new();
        extra.insert("username".into(), serde_json::json!("admin"));

        let claims = JwtClaims {
            sub: None,
            email: None,
            exp: None,
            iss: None,
            aud: None,
            iat: None,
            jti: None,
            extra,
        };

        assert_eq!(validator.extract_principal(&claims), Some("admin".into()));
    }

    // ------------------------------------------------------------------
    // JWKS cache integration tests
    // ------------------------------------------------------------------

    fn make_token_with_kid(claims: &JwtClaims, secret: &str, kid: &str) -> String {
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(kid.to_string());
        let key = EncodingKey::from_secret(secret.as_bytes());
        encode(&header, claims, &key).unwrap()
    }

    #[tokio::test]
    async fn validate_async_with_static_key() {
        let secret = "static-secret-at-least-32-bytes!";
        let config = JwtConfig {
            secret: Some(Zeroizing::new(secret.into())),
            ..Default::default()
        };
        let validator = JwtValidator::new(config).unwrap();

        let claims = JwtClaims {
            sub: Some("user-static".into()),
            exp: Some(future_exp()),
            iss: None,
            aud: None,
            iat: None,
            email: None,
            jti: None,
            extra: HashMap::new(),
        };

        let token = make_token(&claims, secret);
        let validated = validator.validate_async(&token).await.unwrap();
        assert_eq!(validated.sub, Some("user-static".into()));
    }

    #[tokio::test]
    async fn validate_async_with_jwks_cache() {
        let secret = "jwks-secret-at-least-32-bytes!!!";

        // Pre-populate JWKS cache with the HS256 key under kid="key-1"
        let mut keys = std::collections::HashMap::new();
        keys.insert(
            "key-1".to_string(),
            crate::auth::oidc::CachedKey {
                key: DecodingKey::from_secret(secret.as_bytes()),
                algorithm: Algorithm::HS256,
            },
        );
        let cache = Arc::new(crate::auth::oidc::JwksCache::from_keys(keys));

        // No static key configured — only JWKS cache
        let config = JwtConfig::default();
        let validator = JwtValidator::new(config).unwrap().with_jwks_cache(cache);

        let claims = JwtClaims {
            sub: Some("oidc-user".into()),
            exp: Some(future_exp()),
            iss: None,
            aud: None,
            iat: None,
            email: None,
            jti: None,
            extra: HashMap::new(),
        };

        let token = make_token_with_kid(&claims, secret, "key-1");
        let validated = validator.validate_async(&token).await.unwrap();
        assert_eq!(validated.sub, Some("oidc-user".into()));
    }

    #[tokio::test]
    async fn validate_async_jwks_no_kid_tries_all() {
        let secret = "no-kid-secret-at-least-32bytes!!";

        let mut keys = std::collections::HashMap::new();
        keys.insert(
            "default".to_string(),
            crate::auth::oidc::CachedKey {
                key: DecodingKey::from_secret(secret.as_bytes()),
                algorithm: Algorithm::HS256,
            },
        );
        let cache = Arc::new(crate::auth::oidc::JwksCache::from_keys(keys));

        let config = JwtConfig::default();
        let validator = JwtValidator::new(config).unwrap().with_jwks_cache(cache);

        let claims = JwtClaims {
            sub: Some("anon".into()),
            exp: Some(future_exp()),
            iss: None,
            aud: None,
            iat: None,
            email: None,
            jti: None,
            extra: HashMap::new(),
        };

        // Token without kid header
        let token = make_token(&claims, secret);
        let validated = validator.validate_async(&token).await.unwrap();
        assert_eq!(validated.sub, Some("anon".into()));
    }

    #[tokio::test]
    async fn validate_async_jwks_wrong_kid_fails() {
        let secret = "wrong-kid-secret-at-least-32byte";

        let mut keys = std::collections::HashMap::new();
        keys.insert(
            "real-kid".to_string(),
            crate::auth::oidc::CachedKey {
                key: DecodingKey::from_secret(secret.as_bytes()),
                algorithm: Algorithm::HS256,
            },
        );
        let cache = Arc::new(crate::auth::oidc::JwksCache::from_keys(keys));

        let config = JwtConfig::default();
        let validator = JwtValidator::new(config).unwrap().with_jwks_cache(cache);

        let claims = JwtClaims {
            sub: Some("user".into()),
            exp: Some(future_exp()),
            iss: None,
            aud: None,
            iat: None,
            email: None,
            jti: None,
            extra: HashMap::new(),
        };

        let token = make_token_with_kid(&claims, secret, "unknown-kid");
        let err = validator.validate_async(&token).await.unwrap_err();
        assert!(matches!(err, AuthError::Config(_)));
    }

    #[tokio::test]
    async fn validate_async_no_key_no_cache_fails() {
        let config = JwtConfig::default();
        let validator = JwtValidator::new(config).unwrap();

        let claims = JwtClaims {
            sub: Some("user".into()),
            exp: Some(future_exp()),
            iss: None,
            aud: None,
            iat: None,
            email: None,
            jti: None,
            extra: HashMap::new(),
        };

        let token = make_token(&claims, "any-secret");
        let err = validator.validate_async(&token).await.unwrap_err();
        assert!(matches!(err, AuthError::Config(_)));
    }
}

#[cfg(test)]
mod algorithm_family_tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use std::collections::HashMap;
    use zeroize::Zeroizing;

    /// A throwaway P-256 keypair, generated for this test alone and used
    /// nowhere else. It is in the source so the test needs no fixtures.
    const EC_PRIVATE_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgftsCo6PP58Bktonn\nIQQdJB0xhCZr5vaXReMirLL6vfmhRANCAATI63Cp8AxIz8oL4Sfzwp/1r5j8o5v5\nQ2O3+N41eKFkFT/bk1oF8zP6Zz6OaOU7nfw77Pcn+DZz9GFp3ISD9rJK\n-----END PRIVATE KEY-----";
    const EC_PUBLIC_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEyOtwqfAMSM/KC+En88Kf9a+Y/KOb\n+UNjt/jeNXihZBU/25NaBfMz+mc+jmjlO538O+z3J/g2c/RhadyEg/aySg==\n-----END PUBLIC KEY-----";

    fn claims() -> JwtClaims {
        JwtClaims {
            sub: Some("ec-user".into()),
            exp: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
            iss: None,
            aud: None,
            iat: None,
            email: None,
            jti: None,
            extra: HashMap::new(),
        }
    }

    /// ES256 had no key path at all: the only asymmetric branch parsed the
    /// PEM as RSA, an EC key failed that, and the validator was left with
    /// no key — so every token from an ES256 issuer got a 401.
    #[test]
    fn es256_verifies_a_token_signed_with_its_key() {
        let config = JwtConfig {
            algorithm: Some("ES256".into()),
            public_key_pem: Some(Zeroizing::new(EC_PUBLIC_PEM.to_string())),
            ..Default::default()
        };
        let validator = JwtValidator::new(config).expect("ES256 config must build");

        let token = encode(
            &Header::new(Algorithm::ES256),
            &claims(),
            &EncodingKey::from_ec_pem(EC_PRIVATE_PEM.as_bytes()).unwrap(),
        )
        .unwrap();

        let validated = validator.validate(&token).expect("a valid ES256 token");
        assert_eq!(validated.sub, Some("ec-user".into()));
    }

    /// The shape that used to fail silently: an asymmetric algorithm with a
    /// `secret`. It built an HMAC key and rejected every real token, so the
    /// operator saw 401s with a configuration that looked right.
    #[test]
    fn an_asymmetric_algorithm_with_a_secret_is_a_startup_error() {
        let config = JwtConfig {
            algorithm: Some("RS256".into()),
            secret: Some(Zeroizing::new("a-secret-that-is-at-least-32-bytes!".into())),
            ..Default::default()
        };
        let err = JwtValidator::new(config).expect_err("a secret cannot verify an RSA signature");
        assert!(
            err.to_string().contains("public key"),
            "the error must say what to set instead: {err}"
        );
    }

    /// The mirror case, so neither family can be configured with the
    /// other's key material.
    #[test]
    fn an_hmac_algorithm_with_a_public_key_is_a_startup_error() {
        let config = JwtConfig {
            algorithm: Some("HS256".into()),
            public_key_pem: Some(Zeroizing::new(EC_PUBLIC_PEM.to_string())),
            ..Default::default()
        };
        assert!(JwtValidator::new(config).is_err());
    }

    /// Neither kind of material is fine: an OIDC deployment attaches a
    /// JWKS cache afterwards and resolves each token by its `kid`.
    #[test]
    fn no_key_material_is_allowed_for_jwks() {
        let config = JwtConfig {
            algorithm: Some("RS256".into()),
            ..Default::default()
        };
        assert!(JwtValidator::new(config).is_ok());
    }
}
