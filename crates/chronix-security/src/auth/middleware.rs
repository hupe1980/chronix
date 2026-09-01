//! Unified authentication middleware.
//!
//! Chains multiple authentication methods: mTLS → JWT → API Key.
//! The first successful method wins. If no method succeeds, returns 401.
//!
//! When no authentication is configured, all requests pass through
//! with a warning log message.

use crate::auth::api_key::ApiKeyStore;
use crate::auth::error::AuthError;
use crate::auth::jwt::{JwtConfig, JwtValidator};
use crate::auth::mtls::{MtlsConfig, MtlsValidator};
use percent_encoding::percent_decode_str;

/// The authentication method that was used.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMethod {
    /// Authenticated via API key.
    ApiKey,
    /// Authenticated via JWT token.
    Jwt,
    /// Authenticated via mTLS client certificate.
    Mtls,
    /// No authentication configured — unauthenticated passthrough.
    None,
}

/// Authentication context attached to each request.
#[derive(Debug, Clone)]
pub struct AuthContext {
    /// The principal name (e.g., key name, JWT subject, certificate CN).
    pub principal: String,
    /// Which authentication method was used.
    pub method: AuthMethod,
    /// Additional claims from JWT (empty for API key / mTLS).
    pub claims: std::collections::HashMap<String, serde_json::Value>,
}

/// Authentication middleware configuration.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// API key store (optional).
    pub api_key_store: Option<ApiKeyStore>,
    /// JWT configuration (optional).
    pub jwt_config: Option<JwtConfig>,
    /// mTLS configuration (optional).
    pub mtls_config: Option<MtlsConfig>,
    /// Paths exempt from authentication (e.g., `/health`, `/ready`, `/metrics`).
    pub exempt_paths: Vec<String>,
    /// Allow anonymous access when no auth providers are configured.
    /// Defaults to `false` — the middleware rejects all requests when
    /// no provider is configured unless this is explicitly set.
    pub allow_anonymous: bool,
}

/// The authentication middleware.
///
/// Checks all configured authentication methods in order:
/// mTLS → JWT → API key. First success wins.
#[derive(Debug, Clone)]
pub struct AuthMiddleware {
    api_key_store: Option<ApiKeyStore>,
    jwt_validator: Option<JwtValidator>,
    mtls_validator: Option<MtlsValidator>,
    exempt_paths: Vec<String>,
    any_configured: bool,
    allow_anonymous: bool,
}

impl AuthMiddleware {
    /// Create a new authentication middleware from configuration.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Config`] if the JWT configuration contains
    /// an unrecognized algorithm string.
    pub fn new(config: AuthConfig) -> Result<Self, crate::auth::error::AuthError> {
        let any_configured = config.api_key_store.is_some()
            || config.jwt_config.is_some()
            || config.mtls_config.as_ref().is_some_and(|c| c.enabled);

        let jwt_validator = config.jwt_config.map(JwtValidator::new).transpose()?;
        let mtls_validator = config.mtls_config.map(MtlsValidator::new);

        Ok(Self {
            api_key_store: config.api_key_store,
            jwt_validator,
            mtls_validator,
            exempt_paths: config.exempt_paths,
            any_configured,
            allow_anonymous: config.allow_anonymous,
        })
    }

    /// Check if a path is exempt from authentication.
    ///
    /// Uses **segment-boundary matching** instead of raw prefix matching
    /// to prevent bypass via path manipulation (e.g., `/healthz`,
    /// `/health/../api/v1/write`, `/health%2F..`).
    ///
    /// **** Percent-decodes the path before normalization to prevent
    /// bypass via `%2F` (/) and `%2E` (.) encoded characters.
    ///
    /// A path matches an exempt entry if it is either an exact match
    /// or the exempt path is a proper directory prefix (path starts with
    /// `exempt + "/"`).
    #[must_use]
    pub fn is_exempt(&self, path: &str) -> bool {
        // Strip query string and normalize the path.
        let path = path.split('?').next().unwrap_or(path);
        let normalized = Self::normalize_path(path);
        self.exempt_paths
            .iter()
            .any(|exempt| normalized == *exempt || normalized.starts_with(&format!("{exempt}/")))
    }

    /// Normalize a URL path by resolving `.`, `..`, and double-slash
    /// segments. Returns a canonical path that starts with `/`.
    ///
    /// **** Percent-decodes the path first, so `%2F` → `/` and
    /// `%2E` → `.` are resolved before segment matching.
    fn normalize_path(path: &str) -> String {
        // Decode percent-encoded characters first
        let decoded = percent_decode_str(path).decode_utf8_lossy();
        let mut segments: Vec<&str> = Vec::new();
        for seg in decoded.split('/') {
            match seg {
                "" | "." => {}
                ".." => {
                    segments.pop();
                }
                s => segments.push(s),
            }
        }
        format!("/{}", segments.join("/"))
    }

    /// Authenticate a request.
    ///
    /// Parameters:
    /// - `bearer_token`: The value from `Authorization: Bearer <token>` header
    /// - `client_cert_cn`: The client certificate CN (from mTLS handshake)
    ///
    /// Returns the authentication context on success.
    pub fn authenticate(
        &self,
        bearer_token: Option<&str>,
        client_cert_cn: Option<&str>,
    ) -> Result<AuthContext, AuthError> {
        if !self.any_configured {
            if self.allow_anonymous {
                tracing::warn!(
                    "no authentication method configured and allow_anonymous=true -- \
                     allowing unauthenticated access"
                );
                return Ok(AuthContext {
                    principal: "anonymous".into(),
                    method: AuthMethod::None,
                    claims: std::collections::HashMap::new(),
                });
            }
            tracing::warn!(
                "no authentication method configured — rejecting request; \
                 configure at least one auth provider (JWT, API key, or mTLS) \
                 or set 'auth.allow_anonymous = true' to permit unauthenticated access"
            );
            return Err(AuthError::Config(
                "no authentication method configured".into(),
            ));
        }

        // 1. Try mTLS — if a client certificate is presented and mTLS is
        //    enabled, it MUST succeed.  Falling through to weaker methods
        //    (JWT / API-key) would be a security downgrade.
        if let Some(ref mtls) = self.mtls_validator {
            if let Some(cn) = client_cert_cn {
                if mtls.is_enabled() {
                    let identity = mtls.extract_identity_from_cn(cn).map_err(|e| {
                        AuthError::InvalidCertificate(format!(
                            "mTLS certificate validation failed: {e}"
                        ))
                    })?;
                    return Ok(AuthContext {
                        principal: identity.principal().to_string(),
                        method: AuthMethod::Mtls,
                        claims: std::collections::HashMap::new(),
                    });
                }
            }
        }

        // 2. Try JWT
        if let Some(ref jwt) = self.jwt_validator {
            if let Some(token) = bearer_token {
                match jwt.validate(token) {
                    Ok(claims) => {
                        let principal = jwt
                            .extract_principal(&claims)
                            .unwrap_or_else(|| "unknown".into());
                        return Ok(AuthContext {
                            principal,
                            method: AuthMethod::Jwt,
                            claims: claims.extra,
                        });
                    }
                    Err(AuthError::ExpiredJwt) => return Err(AuthError::ExpiredJwt),
                    Err(e) => {
                        // If the token has JWT structure (2+ dots),
                        // treat it as a failed JWT — never fall through to
                        // API key auth. This prevents auth downgrade attacks.
                        let looks_like_jwt = token.bytes().filter(|&b| b == b'.').count() >= 2;
                        if looks_like_jwt {
                            return Err(e);
                        }
                        // Non-JWT-structured token — fall through to API key
                    }
                }
            }
        }

        // 3. Try API key — skip if token looks like a JWT (has 2+ dot separators)
        if let Some(ref store) = self.api_key_store {
            if let Some(token) = bearer_token {
                let looks_like_jwt = token.bytes().filter(|&b| b == b'.').count() >= 2;
                if !looks_like_jwt {
                    match store.validate(token) {
                        Ok(name) => {
                            return Ok(AuthContext {
                                principal: name,
                                method: AuthMethod::ApiKey,
                                claims: std::collections::HashMap::new(),
                            });
                        }
                        Err(AuthError::ExpiredApiKey(name)) => {
                            return Err(AuthError::ExpiredApiKey(name));
                        }
                        Err(_) => {
                            // API key validation failed
                        }
                    }
                }
            }
        }

        Err(AuthError::NoAuth)
    }

    /// Check whether any authentication method is configured.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.any_configured
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zeroize::Zeroizing;

    #[test]
    fn no_auth_configured_rejects_by_default() {
        let middleware = AuthMiddleware::new(AuthConfig::default()).unwrap();
        let result = middleware.authenticate(None, None);
        assert!(result.is_err(), "should reject when no auth is configured");
    }

    #[test]
    fn no_auth_configured_allows_with_anonymous() {
        let middleware = AuthMiddleware::new(AuthConfig {
            allow_anonymous: true,
            ..Default::default()
        })
        .unwrap();
        let ctx = middleware.authenticate(None, None).unwrap();
        assert_eq!(ctx.method, AuthMethod::None);
        assert_eq!(ctx.principal, "anonymous");
    }

    #[test]
    fn api_key_authentication() {
        let mut store = ApiKeyStore::new();
        let key = store.create_key("test-key", None).unwrap();

        let middleware = AuthMiddleware::new(AuthConfig {
            api_key_store: Some(store),
            ..Default::default()
        })
        .unwrap();

        let ctx = middleware.authenticate(Some(&key), None).unwrap();
        assert_eq!(ctx.method, AuthMethod::ApiKey);
        assert_eq!(ctx.principal, "test-key");
    }

    #[test]
    fn api_key_invalid_rejected() {
        let mut store = ApiKeyStore::new();
        let _key = store.create_key("k", None).unwrap();

        let middleware = AuthMiddleware::new(AuthConfig {
            api_key_store: Some(store),
            ..Default::default()
        })
        .unwrap();

        let err = middleware.authenticate(Some("wrong"), None).unwrap_err();
        assert!(matches!(err, AuthError::NoAuth));
    }

    #[test]
    fn jwt_authentication() {
        let secret = "test-jwt-secret-for-middleware!!"; // 32 bytes
        let config = JwtConfig {
            secret: Some(Zeroizing::new(secret.into())),
            ..Default::default()
        };

        let claims = crate::auth::jwt::JwtClaims {
            sub: Some("jwt-user".into()),
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
            extra: std::collections::HashMap::new(),
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();

        let middleware = AuthMiddleware::new(AuthConfig {
            jwt_config: Some(config),
            ..Default::default()
        })
        .unwrap();

        let ctx = middleware.authenticate(Some(&token), None).unwrap();
        assert_eq!(ctx.method, AuthMethod::Jwt);
        assert_eq!(ctx.principal, "jwt-user");
    }

    #[test]
    fn mtls_authentication() {
        let middleware = AuthMiddleware::new(AuthConfig {
            mtls_config: Some(MtlsConfig {
                enabled: true,
                use_cn: true,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();

        let ctx = middleware
            .authenticate(None, Some("client.internal"))
            .unwrap();
        assert_eq!(ctx.method, AuthMethod::Mtls);
        assert_eq!(ctx.principal, "client.internal");
    }

    #[test]
    fn mtls_takes_priority_over_jwt() {
        let secret = "test-secret-at-least-32-bytes!!!";
        let claims = crate::auth::jwt::JwtClaims {
            sub: Some("jwt-user".into()),
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
            extra: std::collections::HashMap::new(),
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();

        let middleware = AuthMiddleware::new(AuthConfig {
            jwt_config: Some(JwtConfig {
                secret: Some(Zeroizing::new(secret.into())),
                ..Default::default()
            }),
            mtls_config: Some(MtlsConfig {
                enabled: true,
                use_cn: true,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();

        // Both JWT and mTLS provided — mTLS wins
        let ctx = middleware
            .authenticate(Some(&token), Some("mtls-client"))
            .unwrap();
        assert_eq!(ctx.method, AuthMethod::Mtls);
    }

    #[test]
    fn exempt_paths() {
        let middleware = AuthMiddleware::new(AuthConfig {
            exempt_paths: vec!["/health".into(), "/ready".into(), "/metrics".into()],
            ..Default::default()
        })
        .unwrap();

        // Exact matches
        assert!(middleware.is_exempt("/health"));
        assert!(middleware.is_exempt("/ready"));
        assert!(middleware.is_exempt("/metrics"));

        // Sub-paths under exempt directories
        assert!(middleware.is_exempt("/health/live"));
        assert!(middleware.is_exempt("/metrics/prometheus"));

        // Non-exempt paths
        assert!(!middleware.is_exempt("/api/v1/write"));

        // Segment-boundary: /healthz is NOT under /health
        assert!(!middleware.is_exempt("/healthz"));
        assert!(!middleware.is_exempt("/health-admin"));

        // Path traversal attacks must NOT bypass
        assert!(!middleware.is_exempt("/health/../api/v1/write"));
        assert!(!middleware.is_exempt("/health/../../secret"));

        // Normalized double-slashes
        assert!(middleware.is_exempt("/health//live"));

        // Query string stripping
        assert!(middleware.is_exempt("/health?foo=bar"));
        assert!(!middleware.is_exempt("/healthz?x=1"));
    }

    /// Percent-encoded path segments must be decoded before
    /// normalization to prevent auth bypass via `%2F` and `%2E`.
    #[test]
    fn percent_encoding_bypass_prevented() {
        let middleware = AuthMiddleware::new(AuthConfig {
            exempt_paths: vec!["/health".into(), "/metrics".into()],
            ..Default::default()
        })
        .unwrap();

        // %2F = '/' — must not bypass path traversal protection
        assert!(!middleware.is_exempt("/health%2F..%2Fapi%2Fv1%2Fwrite"));
        assert!(!middleware.is_exempt("/health%2F..%2F..%2Fsecret"));

        // %2E = '.' — must be decoded and resolved
        assert!(!middleware.is_exempt("/health/%2E%2E/api/v1/write"));
        assert!(!middleware.is_exempt("/health/%2e%2e/secret"));

        // Double-encoded %252F must NOT be decoded twice (stays literal)
        // (we only do one decode pass — double-decode is a separate vuln)
        assert!(middleware.is_exempt("/health/%252E%252E"));

        // Normal exempt paths still work
        assert!(middleware.is_exempt("/health"));
        assert!(middleware.is_exempt("/metrics"));
        assert!(middleware.is_exempt("/health/sub"));
    }

    #[test]
    fn no_bearer_with_auth_configured_fails() {
        let mut store = ApiKeyStore::new();
        let _key = store.create_key("k", None).unwrap();

        let middleware = AuthMiddleware::new(AuthConfig {
            api_key_store: Some(store),
            ..Default::default()
        })
        .unwrap();

        let err = middleware.authenticate(None, None).unwrap_err();
        assert!(matches!(err, AuthError::NoAuth));
    }
}
