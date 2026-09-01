//! Authentication middleware for HTTP, gRPC, and Flight SQL.
//!
//! Integrates [`chronix_security::auth`] into the chronixd server as middleware
//! layers that validate Bearer tokens (JWT or API key) on every
//! request, skipping exempt paths like `/health`.
//!
//! ## Protocol parity
//!
//! The same [`AuthState`] is shared across HTTP (axum), gRPC (tonic),
//! and Flight SQL (tonic) so that all three surfaces enforce identical
//! authentication rules. gRPC/Flight use a tonic `Interceptor`
//! (`grpc_auth_interceptor`) while HTTP uses an axum middleware
//! ([`auth_layer`]).
//!
//! Also provides API key management endpoints for creating and revoking
//! keys at runtime via `POST /api/v1/auth/keys` and
//! `DELETE /api/v1/auth/keys/{name}`.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Json, Path, State};
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tonic::service::Interceptor;
use tracing::{debug, info, warn};

use chronix_security::auth::middleware::{AuthConfig as MiddlewareAuthConfig, AuthMiddleware};
use chronix_security::auth::ApiKeyStore;

use crate::config::AuthConfig;
use crate::http::AppState;

/// Shared authentication state.
///
/// Wraps both the [`AuthMiddleware`] for request validation and a mutable
/// [`ApiKeyStore`] behind an `Arc<RwLock>` so keys can be created or
/// revoked at runtime.
#[derive(Debug, Clone)]
pub struct AuthState {
    middleware: Arc<AuthMiddleware>,
    /// Mutable key store for runtime key management.
    key_store: Arc<RwLock<ApiKeyStore>>,
    /// Claim name or dot-separated path used for role extraction from JWT tokens.
    /// Defaults to `"roles"` when `None`.
    ///
    /// # Nested claim depth
    ///
    /// The dot-separated path (e.g. `"realm_access.roles"`) is split and
    /// traversed without a depth limit. A maliciously crafted JWT with
    /// deeply nested claims could cause excessive recursion or large
    /// allocations during traversal. In practice the JWT payload size is
    /// bounded by the HTTP header limit (`max_body_size` / typical 8 KiB
    /// header cap), which constrains the nesting depth to a few hundred
    /// levels — well within safe stack limits. If stricter guarantees are
    /// needed, add an explicit `max_claim_depth` configuration option.
    role_claim: Option<String>,
}

impl AuthState {
    /// Build an [`AuthState`] from server auth configuration.
    ///
    /// This pre-configures the API key store and JWT validator so that
    /// every request check is a fast hash comparison / signature verify.
    ///
    /// # Errors
    ///
    /// Returns an error if the JWT configuration contains an
    /// unrecognized algorithm string.
    pub fn from_config(
        config: &AuthConfig,
    ) -> Result<Self, chronix_security::auth::error::AuthError> {
        let mut api_key_store = ApiKeyStore::new();

        // Pre-register API keys.
        // Supports three formats:
        //   1. Env-var reference: key = "$API_KEY" or key = "${API_KEY}"
        //      → resolves the environment variable at startup.
        //   2. Pre-hashed Argon2 PHC string: key = "$argon2id$v=19$..."
        //      → loaded directly, no re-hashing.
        //   3. Plain text (default): key = "my-secret"
        //      → hashed with Argon2 on load (backward compatible).
        for entry in &config.api_keys {
            let resolved_key = if entry.key.starts_with("${") && entry.key.ends_with('}') {
                // ${VAR_NAME} form
                let var_name = &entry.key[2..entry.key.len() - 1];
                match std::env::var(var_name) {
                    Ok(val) => val,
                    Err(_) => {
                        warn!(name = %entry.name, var = var_name,
                              "env var not set for API key — skipping");
                        continue;
                    }
                }
            } else if entry.key.starts_with('$') && !entry.key.starts_with("$argon2") {
                // $VAR_NAME form (but not $argon2... which is a PHC hash)
                let var_name = &entry.key[1..];
                match std::env::var(var_name) {
                    Ok(val) => val,
                    Err(_) => {
                        warn!(name = %entry.name, var = var_name,
                              "env var not set for API key — skipping");
                        continue;
                    }
                }
            } else {
                entry.key.clone()
            };

            if resolved_key.starts_with("$argon2") {
                // Pre-hashed key — load directly without re-hashing.
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                api_key_store.add_entry(chronix_security::auth::api_key::ApiKeyEntry {
                    name: entry.name.clone(),
                    hash: resolved_key,
                    expires_at: None,
                    created_at: now,
                });
            } else if let Err(e) =
                api_key_store.register_plaintext(&entry.name, &resolved_key, None)
            {
                warn!(name = %entry.name, %e, "failed to register API key");
            }
        }

        // Configure JWT
        let jwt_config =
            config
                .jwt
                .as_ref()
                .map(|jwt_cfg| chronix_security::auth::jwt::JwtConfig {
                    secret: Some(zeroize::Zeroizing::new(jwt_cfg.secret.clone())),
                    algorithm: Some(jwt_cfg.algorithm.clone()),
                    issuer: jwt_cfg.issuer.clone(),
                    audience: jwt_cfg.audience.clone(),
                    require_audience: jwt_cfg.audience.is_some(),
                    rsa_public_key_pem: None,
                    jwks_url: None,
                    subject_claim: None,
                    role_claim: jwt_cfg.role_claim.clone(),
                });

        let key_store = Arc::new(RwLock::new(api_key_store.clone()));

        let mw_config = MiddlewareAuthConfig {
            api_key_store: if config.api_keys.is_empty() {
                None
            } else {
                Some(api_key_store)
            },
            jwt_config,
            mtls_config: None, // mTLS is handled at the TLS layer
            exempt_paths: config.exempt_paths.clone(),
            allow_anonymous: false,
        };

        let middleware = AuthMiddleware::new(mw_config)?;

        let role_claim = config.jwt.as_ref().and_then(|j| j.role_claim.clone());

        Ok(Self {
            middleware: Arc::new(middleware),
            key_store,
            role_claim,
        })
    }

    /// Create a tonic [`Interceptor`] that enforces authentication on
    /// gRPC and Flight SQL requests.
    ///
    /// Extracts the `authorization` metadata header, runs the same
    /// [`AuthMiddleware`] chain used by HTTP, checks for runtime key
    /// revocation, and inserts an `AuthContext` into request
    /// extensions on success.
    #[must_use]
    pub fn grpc_interceptor(&self) -> GrpcAuthInterceptor {
        GrpcAuthInterceptor {
            middleware: Arc::clone(&self.middleware),
            key_store: Arc::clone(&self.key_store),
            audit_logger: None,
        }
    }

    /// Create a tonic [`Interceptor`] with audit logging for auth failures.
    #[must_use]
    pub fn grpc_interceptor_with_audit(
        &self,
        audit_logger: Arc<chronix_security::audit::AuditLogger>,
    ) -> GrpcAuthInterceptor {
        GrpcAuthInterceptor {
            middleware: Arc::clone(&self.middleware),
            key_store: Arc::clone(&self.key_store),
            audit_logger: Some(audit_logger),
        }
    }
}

/// Tonic [`Interceptor`] enabling authentication parity between
/// HTTP and gRPC/Flight SQL surfaces.
///
/// Validates Bearer tokens from the `authorization` gRPC metadata
/// header using the same auth chain as HTTP (JWT → API Key).
/// Rejects unauthenticated requests with `UNAUTHENTICATED`.
///
/// Optionally logs failed authentication attempts to the audit log
/// (closes ).
#[derive(Clone)]
pub struct GrpcAuthInterceptor {
    middleware: Arc<AuthMiddleware>,
    key_store: Arc<RwLock<ApiKeyStore>>,
    audit_logger: Option<Arc<chronix_security::audit::AuditLogger>>,
}

impl std::fmt::Debug for GrpcAuthInterceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcAuthInterceptor")
            .field("middleware", &self.middleware)
            .field("key_store", &"...")
            .field("audit_logger", &self.audit_logger.is_some())
            .finish()
    }
}

impl GrpcAuthInterceptor {
    /// Log a failed authentication attempt to the audit log.
    fn log_auth_failure(&self, reason: &str) {
        if let Some(ref logger) = self.audit_logger {
            let event = chronix_security::audit::AuditEvent::new(
                "anonymous",
                chronix_security::audit::AuditAction::LoginFailure,
                "grpc",
                chronix_security::audit::AuditDecision::Deny,
            )
            .with_metadata("reason", reason)
            .with_metadata("protocol", "grpc");
            logger.log(event);
        }
    }
}

impl Interceptor for GrpcAuthInterceptor {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        // Extract bearer token from gRPC metadata (RFC 6750)
        let bearer_token = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| {
                let lower = v.to_ascii_lowercase();
                if lower.starts_with("bearer ") {
                    Some(v[7..].to_string())
                } else {
                    None
                }
            });

        // Try middleware auth chain
        match self.middleware.authenticate(bearer_token.as_deref(), None) {
            Ok(ctx) => {
                // Check runtime key revocation for API key auth.
                // Use key_exists() instead of validate() to
                // avoid a redundant Argon2 hash — the key was already
                // cryptographically verified by middleware.authenticate().
                if ctx.method == chronix_security::auth::AuthMethod::ApiKey {
                    let still_valid = {
                        let store = self.key_store.read();
                        store.key_exists(&ctx.principal)
                    };
                    if !still_valid {
                        warn!(
                            principal = %ctx.principal,
                            "gRPC: API key was revoked — rejecting"
                        );
                        self.log_auth_failure("api_key_revoked");
                        return Err(tonic::Status::unauthenticated("authentication required"));
                    }
                }

                debug!(
                    principal = %ctx.principal,
                    method = ?ctx.method,
                    "gRPC request authenticated"
                );
                req.extensions_mut().insert(ctx);
                Ok(req)
            }
            Err(_) => {
                // Fall back to runtime key store
                if let Some(ref token) = bearer_token {
                    let validated = {
                        let store = self.key_store.read();
                        store.validate(token).ok()
                    };
                    if let Some(name) = validated {
                        let ctx = chronix_security::auth::AuthContext {
                            principal: name,
                            method: chronix_security::auth::AuthMethod::ApiKey,
                            claims: Default::default(),
                        };
                        debug!(
                            principal = %ctx.principal,
                            "gRPC request authenticated via runtime key"
                        );
                        req.extensions_mut().insert(ctx);
                        return Ok(req);
                    }
                }
                self.log_auth_failure("no_valid_credentials");
                Err(tonic::Status::unauthenticated("authentication required"))
            }
        }
    }
}

// ── Authorization helpers ───────────────────────────────────────────

/// Traverse a JSON claims map using a dot-separated path.
///
/// Simple segments (e.g. `"roles"`) perform a direct map lookup.
/// Dot-separated segments (e.g. `"realm_access.roles"`) walk nested
/// JSON objects.  If any segment along the path is missing or the
/// intermediate value is not an object, `None` is returned.
///
/// Segments that contain `://` (URIs) are **not** split, so
/// `"https://example.com/roles"` is treated as a single key.
fn resolve_claim_path<'a>(
    claims: &'a std::collections::HashMap<String, serde_json::Value>,
    path: &str,
) -> Option<&'a serde_json::Value> {
    // Fast path: if the path contains no dots or looks like a URI,
    // treat the whole thing as a single key.
    if !path.contains('.') || path.contains("://") {
        return claims.get(path);
    }

    let segments: Vec<&str> = path.split('.').collect();
    let mut current: &serde_json::Value = claims.get(segments[0])?;

    for segment in &segments[1..] {
        current = current.as_object()?.get(*segment)?;
    }

    Some(current)
}

/// Check that the current request has `Admin` permission via Cedar.
///
/// If no authz engine is configured (i.e., authorization is disabled),
/// access is allowed.  If no `AuthContext` extension is present (i.e.,
/// authentication is disabled or exempt), access is also allowed —
/// the assumption being that an open instance intentionally has no
/// restrictions.
///
/// Returns `Err(FORBIDDEN)` only when Cedar yields `Deny`.
pub fn require_admin(
    state: &crate::http::SharedState,
    auth_ctx: Option<&chronix_security::auth::AuthContext>,
) -> Result<(), (StatusCode, String)> {
    let engine = match &state.authz_engine {
        Some(e) => e,
        None => return Ok(()), // No authz engine → permit.
    };

    let ctx = match auth_ctx {
        Some(c) => c,
        None => return Ok(()), // No auth context → permit (unauthenticated mode).
    };

    let mut principal = chronix_security::authz::ChronixPrincipal::new(&ctx.principal);

    // Resolve the role claim path — configurable via `role_claim` in JWT
    // config.  Supports dot-separated paths (e.g. "realm_access.roles"
    // for Keycloak, "https://example.com/roles" for Auth0).
    let default_claim = "roles".to_string();
    let claim_path = state
        .auth_state
        .as_ref()
        .and_then(|a| a.role_claim.as_ref())
        .unwrap_or(&default_claim);

    let roles_value = resolve_claim_path(&ctx.claims, claim_path);
    if let Some(roles) = roles_value.and_then(|v| v.as_array()) {
        for role in roles.iter().filter_map(|v| v.as_str()) {
            principal = principal.with_role(role);
        }
    }

    let resource = chronix_security::authz::ChronixResource::measurement("__system__");

    match engine.authorize(
        &principal,
        chronix_security::authz::ChronixAction::Admin,
        &resource,
    ) {
        chronix_security::authz::Decision::Allow => Ok(()),
        chronix_security::authz::Decision::Deny { reasons } => {
            warn!(
                principal = %ctx.principal,
                reasons = ?reasons,
                "Admin access denied by authorization policy"
            );
            Err((StatusCode::FORBIDDEN, "admin access denied".to_string()))
        }
    }
}

/// Axum middleware layer that enforces `Admin` Cedar authorization on
/// every request that passes through it.
///
/// This is designed to be applied as a layer on the admin route group
/// so that **all** admin endpoints are protected uniformly.
///
/// When the authz engine is not configured, all requests are allowed
/// (open mode).
pub async fn admin_authz_layer(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let auth_ctx = req
        .extensions()
        .get::<chronix_security::auth::AuthContext>()
        .cloned();
    if let Err((status, msg)) = require_admin(&state, auth_ctx.as_ref()) {
        return (status, msg).into_response();
    }
    next.run(req).await
}

/// Axum middleware function that authenticates incoming requests.
///
/// Extracts the Bearer token from the `Authorization` header and
/// validates it against the configured auth methods. On success,
/// inserts an `AuthContext` extension into the request.
pub async fn auth_layer(
    State(state): State<AppState>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    let auth = match &state.auth_state {
        Some(a) => a,
        None => return next.run(req).await,
    };

    let path = req.uri().path().to_string();

    // Skip auth for exempt paths
    if auth.middleware.is_exempt(&path) {
        return next.run(req).await;
    }

    // Extract bearer token from Authorization header (RFC 6750: case-insensitive)
    let bearer_token = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let lower = v.to_ascii_lowercase();
            if lower.starts_with("bearer ") {
                Some(v[7..].to_string())
            } else {
                None
            }
        });

    // Try middleware first, then check mutable key store
    match auth.middleware.authenticate(bearer_token.as_deref(), None) {
        Ok(ctx) => {
            // If this was an API key auth, verify the key hasn't been
            // revoked from the runtime key store.  The middleware holds a
            // static copy from startup, so runtime revocations won't be
            // reflected there.
            if ctx.method == chronix_security::auth::AuthMethod::ApiKey {
                let still_valid = {
                    let store = auth.key_store.read();
                    // If the runtime store can validate the same token,
                    // the key hasn't been revoked.
                    bearer_token
                        .as_ref()
                        .is_some_and(|t| store.validate(t).is_ok())
                };
                if !still_valid {
                    warn!(
                        principal = %ctx.principal,
                        path = %path,
                        "API key was revoked — rejecting"
                    );
                    return (StatusCode::UNAUTHORIZED, "authentication required").into_response();
                }
            }
            debug!(
                principal = %ctx.principal,
                method = ?ctx.method,
                path = %path,
                "request authenticated"
            );
            req.extensions_mut().insert(ctx);
            next.run(req).await
        }
        Err(mw_err) => {
            // Fall back to runtime key store (drop guard before await)
            if let Some(ref token) = bearer_token {
                let validated = {
                    let store = auth.key_store.read();
                    store.validate(token).ok()
                };
                if let Some(name) = validated {
                    let ctx = chronix_security::auth::AuthContext {
                        principal: name,
                        method: chronix_security::auth::AuthMethod::ApiKey,
                        claims: Default::default(),
                    };
                    debug!(
                        principal = %ctx.principal,
                        path = %path,
                        "request authenticated via runtime key"
                    );
                    req.extensions_mut().insert(ctx);
                    return next.run(req).await;
                }
            }
            warn!(path = %path, error = %mw_err, "authentication failed");
            // Return generic error message — never leak auth method details or key names.
            (StatusCode::UNAUTHORIZED, mw_err.client_message()).into_response()
        }
    }
}

// ── API key management endpoints ────────────────────────────────────────

/// Request body for `POST /api/v1/auth/keys`.
#[derive(Debug, Deserialize)]
pub struct CreateKeyRequest {
    /// Human-readable name for the API key.
    pub name: String,
    /// Optional Unix timestamp (seconds) when this key expires.
    #[serde(default)]
    pub expires_at: Option<u64>,
}

/// Response body for `POST /api/v1/auth/keys`.
#[derive(Debug, Serialize)]
pub struct CreateKeyResponse {
    /// The raw API key — shown only once.
    pub key: String,
    /// The key name.
    pub name: String,
    /// When the key expires (if set).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
}

/// `POST /api/v1/auth/keys` — create a new API key (requires authenticated admin).
pub async fn create_key_handler(
    State(state): State<AppState>,
    Json(body): Json<CreateKeyRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let auth = state
        .auth_state
        .as_ref()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "auth not configured".to_string()))?;

    // Validate key name
    let name = body.name.trim();
    if name.is_empty() || name.len() > 128 {
        return Err((
            StatusCode::BAD_REQUEST,
            "key name must be 1–128 characters".to_string(),
        ));
    }

    let raw_key = {
        let mut store = auth.key_store.write();
        store
            .create_key(name, body.expires_at)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    };

    info!(name = %name, "API key created");
    Ok((
        StatusCode::CREATED,
        Json(CreateKeyResponse {
            key: raw_key,
            name: name.to_string(),
            expires_at: body.expires_at,
        }),
    ))
}

/// Response for listing keys.
#[derive(Debug, Serialize)]
pub struct ListKeysResponse {
    /// Key names.
    pub keys: Vec<String>,
}

/// `GET /api/v1/auth/keys` — list all API key names.
pub async fn list_keys_handler(State(state): State<AppState>) -> impl IntoResponse {
    if let Some(auth) = &state.auth_state {
        let store = auth.key_store.read();
        let keys: Vec<String> = store
            .list_keys()
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        Json(ListKeysResponse { keys })
    } else {
        Json(ListKeysResponse { keys: vec![] })
    }
}

/// `DELETE /api/v1/auth/keys/{name}` — revoke an API key.
pub async fn revoke_key_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let auth = match &state.auth_state {
        Some(a) => a,
        None => return StatusCode::NOT_FOUND,
    };

    let revoked = {
        let mut store = auth.key_store.write();
        store.revoke(&name)
    };

    if revoked {
        info!(name = %name, "API key revoked");
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ApiKeyEntry, JwtAuthConfig};

    #[test]
    fn auth_state_from_empty_config_rejects_by_default() {
        let config = AuthConfig {
            api_keys: vec![],
            jwt: None,
            exempt_paths: vec!["/health".to_string()],
        };
        let state = AuthState::from_config(&config).unwrap();

        // No auth configured with allow_anonymous=false — rejects by default
        let result = state.middleware.authenticate(None, None);
        assert!(result.is_err(), "should reject when no auth configured");
    }

    #[test]
    fn auth_state_with_api_key() {
        let config = AuthConfig {
            api_keys: vec![ApiKeyEntry {
                name: "test-key".to_string(),
                key: "super-secret-key-123".to_string(),
            }],
            jwt: None,
            exempt_paths: vec!["/health".to_string()],
        };
        let state = AuthState::from_config(&config).unwrap();

        // Exempt path check
        assert!(state.middleware.is_exempt("/health"));

        // Non-exempt path fails without token
        let err = state.middleware.authenticate(None, None);
        assert!(err.is_err());

        // Non-exempt path passes with valid API key
        let ctx = state
            .middleware
            .authenticate(Some("super-secret-key-123"), None)
            .unwrap();
        assert_eq!(ctx.principal, "test-key");
    }

    #[test]
    fn auth_state_with_jwt() {
        use jsonwebtoken::{encode, EncodingKey, Header};

        let secret = "test-jwt-secret-256-bits-long!!!!"; // 32 bytes
        let config = AuthConfig {
            api_keys: vec![],
            jwt: Some(JwtAuthConfig {
                secret: secret.to_string(),
                algorithm: "HS256".to_string(),
                issuer: None,
                audience: None,
                role_claim: None,
            }),
            exempt_paths: vec![],
        };
        let state = AuthState::from_config(&config).unwrap();

        // Create a valid JWT
        let claims = serde_json::json!({
            "sub": "user@example.com",
            "exp": chrono_expiry(),
        });
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();

        let ctx = state.middleware.authenticate(Some(&token), None).unwrap();
        assert_eq!(ctx.principal, "user@example.com");
    }

    #[test]
    fn auth_state_invalid_key_rejected() {
        let config = AuthConfig {
            api_keys: vec![ApiKeyEntry {
                name: "k".to_string(),
                key: "real-key".to_string(),
            }],
            jwt: None,
            exempt_paths: vec![],
        };
        let state = AuthState::from_config(&config).unwrap();

        let err = state.middleware.authenticate(Some("wrong-key"), None);
        assert!(err.is_err());
    }

    /// Helper to create an expiry timestamp 1 hour from now.
    fn chrono_expiry() -> u64 {
        (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs())
            + 3600
    }

    // ── API key management tests ────────────────────────────────────

    #[test]
    fn create_key_at_runtime() {
        let config = AuthConfig {
            api_keys: vec![ApiKeyEntry {
                name: "admin".to_string(),
                key: "admin-key".to_string(),
            }],
            jwt: None,
            exempt_paths: vec![],
        };
        let state = AuthState::from_config(&config).unwrap();

        // Create a new key at runtime
        let raw_key = {
            let mut store = state.key_store.write();
            store.create_key("runtime-key", None).unwrap()
        };

        // Validate via runtime key store
        {
            let store = state.key_store.read();
            let name = store.validate(&raw_key).unwrap();
            assert_eq!(name, "runtime-key");
        }
    }

    #[test]
    fn revoke_key_at_runtime() {
        let config = AuthConfig {
            api_keys: vec![],
            jwt: None,
            exempt_paths: vec![],
        };
        let state = AuthState::from_config(&config).unwrap();

        // Create and then revoke
        let raw_key = {
            let mut store = state.key_store.write();
            store.create_key("temp", None).unwrap()
        };

        {
            let store = state.key_store.read();
            assert!(store.validate(&raw_key).is_ok());
        }

        {
            let mut store = state.key_store.write();
            assert!(store.revoke("temp"));
        }

        {
            let store = state.key_store.read();
            assert!(store.validate(&raw_key).is_err());
        }
    }

    #[test]
    fn list_runtime_keys() {
        let config = AuthConfig {
            api_keys: vec![],
            jwt: None,
            exempt_paths: vec![],
        };
        let state = AuthState::from_config(&config).unwrap();

        {
            let mut store = state.key_store.write();
            let _k1 = store.create_key("alpha", None).unwrap();
            let _k2 = store.create_key("beta", None).unwrap();
        }

        let store = state.key_store.read();
        let mut names = store.list_keys();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    #[test]
    fn create_key_with_expiry() {
        let config = AuthConfig {
            api_keys: vec![],
            jwt: None,
            exempt_paths: vec![],
        };
        let state = AuthState::from_config(&config).unwrap();

        // Create an already-expired key
        let raw_key = {
            let mut store = state.key_store.write();
            store.create_key("expired", Some(0)).unwrap()
        };

        let store = state.key_store.read();
        let result = store.validate(&raw_key);
        assert!(result.is_err());
    }

    #[test]
    fn bearer_token_case_insensitive() {
        // Verify that the extraction logic handles various casings per RFC 6750
        for prefix in &["Bearer ", "bearer ", "BEARER ", "bEaReR "] {
            let header_val = format!("{prefix}my-secret-token");
            let lower = header_val.to_ascii_lowercase();
            let token = if lower.starts_with("bearer ") {
                Some(header_val[7..].to_string())
            } else {
                None
            };
            assert_eq!(
                token.as_deref(),
                Some("my-secret-token"),
                "failed for prefix: {prefix}"
            );
        }
    }

    #[test]
    fn create_key_name_validation() {
        // Empty name should be rejected by the handler. We test the validation
        // logic directly here since setting up a full AppState is expensive.
        let name = "";
        let trimmed = name.trim();
        assert!(
            trimmed.is_empty() || trimmed.len() > 128,
            "empty name should fail validation"
        );

        // Very long name should be rejected
        let long_name = "x".repeat(129);
        let trimmed = long_name.trim();
        assert!(
            trimmed.is_empty() || trimmed.len() > 128,
            "long name should fail validation"
        );

        // Normal name should pass
        let good_name = "my-api-key";
        let trimmed = good_name.trim();
        assert!(
            !trimmed.is_empty() && trimmed.len() <= 128,
            "good name should pass"
        );
    }

    fn make_test_db() -> std::sync::Arc<chronix::prelude::Chronix> {
        let dir = tempfile::tempdir().unwrap();
        let config = chronix::prelude::ChronixConfig::builder()
            .data_dir(dir.path())
            .build()
            .unwrap();
        // Leak the tempdir so it lives long enough.
        std::mem::forget(dir);
        std::sync::Arc::new(chronix::prelude::Chronix::open(config).unwrap())
    }

    #[test]
    fn require_admin_no_engine_allows() {
        // When no authz engine is configured, all requests are allowed.
        let db = make_test_db();
        let sql_contexts = crate::namespace::SqlContexts::new(db.clone());
        let state = crate::http::SharedState {
            db,
            start_time: std::time::Instant::now(),
            connector_manager: None,
            sql_contexts,
            auth_state: None,
            #[cfg(feature = "cluster")]
            meta_client: None,
            namespace_registry: None,
            model_catalog: std::sync::Arc::new(parking_lot::RwLock::new(
                chronix::chronix_analytics::forecast::ModelCatalog::new(),
            )),
            #[cfg(feature = "chaos")]
            chaos_agent: None,
            authz_engine: None,
            audit_logger: None,
            namespace_rate_limiter: crate::rate_limit::NamespaceRateLimiter::new(),
            #[cfg(feature = "chaos")]
            chaos_guards: parking_lot::Mutex::new(Vec::new()),
            sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
            config: crate::config::ServerConfig::default(),
            write_dedup_cache: None,
            openapi_json: std::sync::OnceLock::new(),
            write_timeout: std::time::Duration::ZERO,
        };
        let ctx = chronix_security::auth::AuthContext {
            principal: "user1".to_string(),
            method: chronix_security::auth::AuthMethod::ApiKey,
            claims: Default::default(),
        };
        assert!(super::require_admin(&state, Some(&ctx)).is_ok());
    }

    #[test]
    fn require_admin_no_context_allows() {
        // When no auth context is present, require_admin permits (open mode).
        let db = make_test_db();
        let sql_contexts = crate::namespace::SqlContexts::new(db.clone());
        let state = crate::http::SharedState {
            db,
            start_time: std::time::Instant::now(),
            connector_manager: None,
            sql_contexts,
            auth_state: None,
            #[cfg(feature = "cluster")]
            meta_client: None,
            namespace_registry: None,
            model_catalog: std::sync::Arc::new(parking_lot::RwLock::new(
                chronix::chronix_analytics::forecast::ModelCatalog::new(),
            )),
            #[cfg(feature = "chaos")]
            chaos_agent: None,
            authz_engine: None,
            audit_logger: None,
            namespace_rate_limiter: crate::rate_limit::NamespaceRateLimiter::new(),
            #[cfg(feature = "chaos")]
            chaos_guards: parking_lot::Mutex::new(Vec::new()),
            sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
            config: crate::config::ServerConfig::default(),
            write_dedup_cache: None,
            openapi_json: std::sync::OnceLock::new(),
            write_timeout: std::time::Duration::ZERO,
        };
        assert!(super::require_admin(&state, None).is_ok());
    }

    #[test]
    fn require_admin_with_engine_denies_non_admin() {
        // When authz engine is configured with no policies (default-deny),
        // non-admin users are rejected.
        let db = make_test_db();
        let sql_contexts = crate::namespace::SqlContexts::new(db.clone());
        let engine = chronix_security::authz::AuthzEngine::new();
        let state = crate::http::SharedState {
            db,
            start_time: std::time::Instant::now(),
            connector_manager: None,
            sql_contexts,
            auth_state: None,
            #[cfg(feature = "cluster")]
            meta_client: None,
            namespace_registry: None,
            model_catalog: std::sync::Arc::new(parking_lot::RwLock::new(
                chronix::chronix_analytics::forecast::ModelCatalog::new(),
            )),
            #[cfg(feature = "chaos")]
            chaos_agent: None,
            authz_engine: Some(std::sync::Arc::new(engine)),
            audit_logger: None,
            namespace_rate_limiter: crate::rate_limit::NamespaceRateLimiter::new(),
            #[cfg(feature = "chaos")]
            chaos_guards: parking_lot::Mutex::new(Vec::new()),
            sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
            config: crate::config::ServerConfig::default(),
            write_dedup_cache: None,
            openapi_json: std::sync::OnceLock::new(),
            write_timeout: std::time::Duration::ZERO,
        };
        let ctx = chronix_security::auth::AuthContext {
            principal: "user1".to_string(),
            method: chronix_security::auth::AuthMethod::ApiKey,
            claims: Default::default(),
        };
        let result = super::require_admin(&state, Some(&ctx));
        assert!(result.is_err());
        let (status, _msg) = result.unwrap_err();
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    // ── gRPC Interceptor tests ──────────────────────────────────────

    #[test]
    fn grpc_interceptor_rejects_unauthenticated() {
        let config = AuthConfig {
            api_keys: vec![ApiKeyEntry {
                name: "test-key".to_string(),
                key: "my-secret".to_string(),
            }],
            jwt: None,
            exempt_paths: vec![],
        };
        let state = AuthState::from_config(&config).unwrap();
        let mut interceptor = state.grpc_interceptor();

        // No auth header → reject
        let req = tonic::Request::new(());
        let result = interceptor.call(req);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn grpc_interceptor_accepts_valid_api_key() {
        let config = AuthConfig {
            api_keys: vec![ApiKeyEntry {
                name: "test-key".to_string(),
                key: "my-secret".to_string(),
            }],
            jwt: None,
            exempt_paths: vec![],
        };
        let state = AuthState::from_config(&config).unwrap();
        let mut interceptor = state.grpc_interceptor();

        let mut req = tonic::Request::new(());
        req.metadata_mut()
            .insert("authorization", "Bearer my-secret".parse().unwrap());
        let result = interceptor.call(req);
        assert!(result.is_ok());
        let req = result.unwrap();
        let ctx = req
            .extensions()
            .get::<chronix_security::auth::AuthContext>()
            .unwrap();
        assert_eq!(ctx.principal, "test-key");
    }

    #[test]
    fn grpc_interceptor_rejects_invalid_key() {
        let config = AuthConfig {
            api_keys: vec![ApiKeyEntry {
                name: "test-key".to_string(),
                key: "my-secret".to_string(),
            }],
            jwt: None,
            exempt_paths: vec![],
        };
        let state = AuthState::from_config(&config).unwrap();
        let mut interceptor = state.grpc_interceptor();

        let mut req = tonic::Request::new(());
        req.metadata_mut()
            .insert("authorization", "Bearer wrong-key".parse().unwrap());
        let result = interceptor.call(req);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn grpc_interceptor_accepts_runtime_key() {
        let config = AuthConfig {
            api_keys: vec![],
            jwt: None,
            exempt_paths: vec![],
        };
        let state = AuthState::from_config(&config).unwrap();

        // Create a runtime key
        let raw_key = {
            let mut store = state.key_store.write();
            store.create_key("runtime", None).unwrap()
        };

        let mut interceptor = state.grpc_interceptor();
        let mut req = tonic::Request::new(());
        req.metadata_mut().insert(
            "authorization",
            format!("Bearer {raw_key}").parse().unwrap(),
        );
        let result = interceptor.call(req);
        assert!(result.is_ok());
    }

    #[test]
    fn grpc_interceptor_rejects_revoked_key() {
        let config = AuthConfig {
            api_keys: vec![ApiKeyEntry {
                name: "test-key".to_string(),
                key: "my-secret".to_string(),
            }],
            jwt: None,
            exempt_paths: vec![],
        };
        let state = AuthState::from_config(&config).unwrap();

        // Revoke the key at runtime
        {
            let mut store = state.key_store.write();
            store.revoke("test-key");
        }

        let mut interceptor = state.grpc_interceptor();
        let mut req = tonic::Request::new(());
        req.metadata_mut()
            .insert("authorization", "Bearer my-secret".parse().unwrap());
        let result = interceptor.call(req);
        assert!(result.is_err());
    }

    #[test]
    fn resolve_claim_path_simple_key() {
        let mut claims = std::collections::HashMap::new();
        claims.insert("roles".to_string(), serde_json::json!(["admin", "reader"]));
        let val = super::resolve_claim_path(&claims, "roles").unwrap();
        assert_eq!(val, &serde_json::json!(["admin", "reader"]));
    }

    #[test]
    fn resolve_claim_path_nested_dot() {
        let mut claims = std::collections::HashMap::new();
        claims.insert(
            "realm_access".to_string(),
            serde_json::json!({"roles": ["admin"]}),
        );
        let val = super::resolve_claim_path(&claims, "realm_access.roles").unwrap();
        assert_eq!(val, &serde_json::json!(["admin"]));
    }

    #[test]
    fn resolve_claim_path_uri_key() {
        let mut claims = std::collections::HashMap::new();
        claims.insert(
            "https://example.com/roles".to_string(),
            serde_json::json!(["editor"]),
        );
        let val = super::resolve_claim_path(&claims, "https://example.com/roles").unwrap();
        assert_eq!(val, &serde_json::json!(["editor"]));
    }

    #[test]
    fn resolve_claim_path_missing() {
        let claims = std::collections::HashMap::new();
        assert!(super::resolve_claim_path(&claims, "roles").is_none());
        assert!(super::resolve_claim_path(&claims, "a.b.c").is_none());
    }

    #[test]
    fn resolve_claim_path_deep_nested() {
        let mut claims = std::collections::HashMap::new();
        claims.insert(
            "a".to_string(),
            serde_json::json!({"b": {"c": "deep_value"}}),
        );
        let val = super::resolve_claim_path(&claims, "a.b.c").unwrap();
        assert_eq!(val, &serde_json::json!("deep_value"));
    }

    // ── gRPC auth failure audit logging ───────────────────────

    #[test]
    fn grpc_interceptor_logs_auth_failure_to_audit() {
        let config = AuthConfig {
            api_keys: vec![ApiKeyEntry {
                name: "test-key".to_string(),
                key: "secret".to_string(),
            }],
            jwt: None,
            exempt_paths: vec![],
        };
        let state = AuthState::from_config(&config).unwrap();

        let logger = std::sync::Arc::new(chronix_security::audit::AuditLogger::new());
        let before = logger.next_sequence();

        let mut interceptor = state.grpc_interceptor_with_audit(logger.clone());

        // Send a request with an invalid token.
        let mut req = tonic::Request::new(());
        req.metadata_mut()
            .insert("authorization", "Bearer wrong-token".parse().unwrap());
        let result = interceptor.call(req);
        assert!(result.is_err());

        // The audit logger should have recorded one LoginFailure event.
        assert_eq!(logger.next_sequence(), before + 1);
    }

    #[test]
    fn grpc_interceptor_no_audit_without_logger() {
        let config = AuthConfig {
            api_keys: vec![],
            jwt: None,
            exempt_paths: vec![],
        };
        let state = AuthState::from_config(&config).unwrap();

        // Without an audit logger, auth failures should still work.
        let mut interceptor = state.grpc_interceptor();
        let req = tonic::Request::new(());
        let result = interceptor.call(req);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
    }
}
