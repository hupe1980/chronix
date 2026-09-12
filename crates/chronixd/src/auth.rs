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
    /// Where the durable overlay lives, when the server has a data
    /// directory. `None` in tests that do not care.
    overlay_path: Option<std::path::PathBuf>,
}

/// The key changes that outlive the process, beside the configured ones.
///
/// **`[[auth.api_keys]]` is a *declaration*, not a record**, and it is
/// re-read on every start — so both halves of runtime key management used to
/// last exactly as long as the process. A key minted through the admin API
/// was shown once, written into a deployment, and stopped working at the next
/// restart with no way to recover it. A **revocation** was worse: an operator
/// revoking a leaked credential during an incident was told it was done, and
/// a restart handed the key back.
///
/// This is the record. Revocations are names, so a configured key can be
/// revoked without editing the config; creations carry the Argon2 PHC hash,
/// which is the same form `[[auth.api_keys]]` already accepts and is designed
/// to sit on disk. The raw key is never here — it does not exist after the
/// response that showed it.
#[derive(Debug, Default, Serialize, Deserialize)]
struct KeyOverlay {
    /// Keys minted through the admin API.
    #[serde(default)]
    created: Vec<chronix_security::auth::api_key::ApiKeyEntry>,
    /// Names revoked through the admin API, configured or minted.
    #[serde(default)]
    revoked: Vec<String>,
}

impl KeyOverlay {
    /// The overlay file inside `dir`.
    fn path(dir: &std::path::Path) -> std::path::PathBuf {
        dir.join("api_keys.json")
    }

    /// Read the overlay, or an empty one when there is none.
    fn load(dir: &std::path::Path) -> Result<Self, chronix_security::auth::error::AuthError> {
        let path = Self::path(dir);
        let Ok(bytes) = std::fs::read(&path) else {
            return Ok(Self::default());
        };
        serde_json::from_slice(&bytes).map_err(|e| {
            chronix_security::auth::error::AuthError::Config(format!(
                "cannot read the API key overlay at {}: {e}",
                path.display()
            ))
        })
    }

    /// Write the overlay durably: temp file, fsync, rename.
    ///
    /// Synchronous and fallible on purpose. A revocation the caller is told
    /// succeeded and the disk did not take is the failure this whole file
    /// exists to remove.
    fn store(&self, dir: &std::path::Path) -> Result<(), chronix_security::auth::error::AuthError> {
        let fail = |e: String| chronix_security::auth::error::AuthError::Config(e);
        std::fs::create_dir_all(dir)
            .map_err(|e| fail(format!("cannot create the auth directory: {e}")))?;
        let json = serde_json::to_vec_pretty(self)
            .map_err(|e| fail(format!("cannot serialise the API key overlay: {e}")))?;
        let tmp = dir.join("api_keys.json.tmp");
        let written = (|| -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            std::io::Write::write_all(&mut f, &json)?;
            f.sync_all()
        })();
        if let Err(e) = written {
            let _ = std::fs::remove_file(&tmp);
            return Err(fail(format!("cannot write the API key overlay: {e}")));
        }
        if let Err(e) = std::fs::rename(&tmp, Self::path(dir)) {
            let _ = std::fs::remove_file(&tmp);
            return Err(fail(format!("cannot rename the API key overlay: {e}")));
        }
        Ok(())
    }
}

impl AuthState {
    /// Attach the issuer's published keys, when a JWKS URL is configured.
    ///
    /// `jwks_url` used to be accepted by the config and then dropped on the
    /// floor: nothing built a cache, so an OIDC deployment authenticated
    /// nobody and the config gave no hint why. Fetching once at startup
    /// also turns an unreachable issuer into a startup failure instead of a
    /// wall of 401s.
    ///
    /// # Errors
    ///
    /// Returns an error if the endpoint cannot be fetched or holds no keys.
    pub async fn attach_jwks(
        &mut self,
        jwks_url: &str,
        ttl: std::time::Duration,
    ) -> Result<(), chronix_security::auth::error::AuthError> {
        let cache = std::sync::Arc::new(chronix_security::auth::oidc::JwksCache::new(
            jwks_url.to_string(),
            ttl,
        ));
        cache.refresh().await?;
        if cache.is_empty() {
            return Err(chronix_security::auth::error::AuthError::Config(format!(
                "the JWKS endpoint {jwks_url} published no keys"
            )));
        }
        info!(jwks_url, keys = cache.len(), "JWKS keys loaded");
        match std::sync::Arc::get_mut(&mut self.middleware) {
            Some(mw) => mw.set_jwks_cache(cache),
            None => {
                return Err(chronix_security::auth::error::AuthError::Config(
                    "JWKS must be attached before the auth state is shared".into(),
                ))
            }
        }
        Ok(())
    }

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
        Self::from_config_at(config, None)
    }

    /// [`from_config`](Self::from_config), with the directory that holds the
    /// durable record of runtime key changes.
    ///
    /// `None` keeps the old behaviour — configured keys only, nothing
    /// persisted — which is what a test that never mints one wants.
    ///
    /// # Errors
    ///
    /// As [`from_config`](Self::from_config), plus an unreadable overlay.
    pub fn from_config_at(
        config: &AuthConfig,
        dir: Option<&std::path::Path>,
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
            let resolved_key = match crate::config::resolve_env_reference(&entry.key) {
                Ok(val) => val,
                Err(var_name) => {
                    warn!(name = %entry.name, var = %var_name,
                          "env var not set for API key — skipping");
                    continue;
                }
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
                    namespaces: entry.namespaces.clone(),
                    admin: entry.admin,
                    roles: entry.roles.clone(),
                });
            } else if let Err(e) =
                api_key_store.register_plaintext(&entry.name, &resolved_key, None)
            {
                warn!(name = %entry.name, %e, "failed to register API key");
            } else {
                api_key_store.bind_namespaces(&entry.name, entry.namespaces.clone());
                api_key_store.set_admin(&entry.name, entry.admin);
                api_key_store.set_roles(&entry.name, entry.roles.clone());
            }
        }

        // Configure JWT
        // The key material follows the algorithm family. `public_key_pem`
        // used to be hard-coded to `None` here, so an RS256 or ES256
        // deployment had no key to verify with and answered 401 to every
        // token — configuration the operator could write and the server
        // would accept while never authenticating anyone.
        let jwt_config = match config.jwt.as_ref() {
            None => None,
            Some(jwt_cfg) => {
                let public_key_pem = match jwt_cfg.public_key_pem_file.as_ref() {
                    None => None,
                    Some(path) => {
                        let pem = std::fs::read_to_string(path).map_err(|e| {
                            chronix_security::auth::error::AuthError::Config(format!(
                                "cannot read the JWT public key at {}: {e}",
                                path.display()
                            ))
                        })?;
                        Some(zeroize::Zeroizing::new(pem))
                    }
                };
                let secret = if jwt_cfg.secret.is_empty() {
                    None
                } else {
                    Some(zeroize::Zeroizing::new(jwt_cfg.secret.clone()))
                };
                Some(chronix_security::auth::jwt::JwtConfig {
                    secret,
                    algorithm: Some(jwt_cfg.algorithm.clone()),
                    issuer: jwt_cfg.issuer.clone(),
                    audience: jwt_cfg.audience.clone(),
                    require_audience: jwt_cfg.audience.is_some(),
                    public_key_pem,
                    jwks_url: jwt_cfg.jwks_url.clone(),
                    subject_claim: None,
                    role_claim: jwt_cfg.role_claim.clone(),
                })
            }
        };

        // The overlay is applied **after** the configured keys, so a
        // revocation wins over a declaration: the config says which keys
        // exist, the overlay says which of them were taken away and which
        // were added since.
        let overlay_path = dir.map(std::path::Path::to_path_buf);
        if let Some(dir) = dir {
            let overlay = KeyOverlay::load(dir)?;
            for entry in overlay.created {
                api_key_store.add_entry(entry);
            }
            for name in &overlay.revoked {
                if api_key_store.revoke(name) {
                    info!(name = %name, "API key revoked (from the durable record)");
                }
            }
        }

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

        Ok(Self {
            middleware: Arc::new(middleware),
            key_store,
            overlay_path,
        })
    }

    /// Mint a key and record it, returning the raw value **once**.
    ///
    /// Durable before it returns: the response is the only place the raw key
    /// ever appears, so an operator writes it into a deployment immediately
    /// and a key that does not survive a restart cannot be recovered.
    ///
    /// # Errors
    ///
    /// The store's error, or the overlay's if it cannot be written — in
    /// which case the key is rolled back rather than left working until the
    /// next restart.
    pub fn create_key(
        &self,
        name: &str,
        expires_at: Option<u64>,
        namespaces: Vec<String>,
        roles: Vec<String>,
    ) -> Result<String, chronix_security::auth::error::AuthError> {
        let (raw, entry) = {
            let mut store = self.key_store.write();
            let raw = store.create_key(name, expires_at)?;
            store.bind_namespaces(name, namespaces);
            store.set_roles(name, roles);
            let entry = store.entry(name).cloned();
            (raw, entry)
        };

        if let (Some(dir), Some(entry)) = (self.overlay_path.as_deref(), entry) {
            let mut overlay = KeyOverlay::load(dir)?;
            overlay.revoked.retain(|n| n != name);
            overlay.created.retain(|e| e.name != name);
            overlay.created.push(entry);
            if let Err(e) = overlay.store(dir) {
                self.key_store.write().revoke(name);
                return Err(e);
            }
        }
        Ok(raw)
    }

    /// Revoke a key and record it, so it stays revoked across a restart.
    ///
    /// Returns whether the key was there. A **configured** key can be
    /// revoked too: the overlay holds the name, and startup applies it after
    /// reading the config — otherwise revoking a leaked credential would
    /// require a config change and a deploy, during an incident.
    ///
    /// # Errors
    ///
    /// The overlay's, if it cannot be written. The in-memory revocation is
    /// undone in that case, so the caller is not told a lie.
    pub fn revoke(&self, name: &str) -> Result<bool, chronix_security::auth::error::AuthError> {
        let removed = {
            let mut store = self.key_store.write();
            // The whole entry, kept so the revocation can be undone if the
            // record cannot be written.
            let entry = store.entry(name).cloned();
            if entry.is_some() {
                store.revoke(name);
            }
            entry
        };
        let Some(entry) = removed else {
            return Ok(false);
        };

        if let Some(dir) = self.overlay_path.as_deref() {
            let mut overlay = KeyOverlay::load(dir)?;
            overlay.created.retain(|e| e.name != name);
            if !overlay.revoked.iter().any(|n| n == name) {
                overlay.revoked.push(name.to_string());
            }
            if let Err(e) = overlay.store(dir) {
                self.key_store.write().add_entry(entry);
                return Err(e);
            }
        }
        Ok(true)
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
    /// Record a failed authentication attempt.
    ///
    /// The counter is unconditional; the audit event is not, because an audit
    /// logger is optional. Without the counter a deployment that has not
    /// configured one had no signal at all that credentials were being
    /// refused — which is the signal the security checklist asks for.
    fn log_auth_failure(&self, reason: &str) {
        metrics::counter!("chronix_auth_failures_total", "protocol" => "grpc").increment(1);
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
                        store.authenticate(token).ok()
                    };
                    if let Some(ctx) = validated {
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

/// May this request perform `action`?
///
/// The **capability and the policy**, in that order, and both of them:
///
/// 1. the credential must be marked `admin` — carried on the key or the
///    token, because the policy engine is optional and its absence must not
///    mean "everyone is an administrator";
/// 2. when a policy engine *is* configured, it must also permit `action` on
///    `Chronix::System`.
///
/// Requiring both is why adding Cedar can only ever narrow access. It used
/// to be an either/or — a configured engine replaced the capability check —
/// so pointing `authz_policy_dir` at a permissive file widened what every
/// non-admin credential could do.
///
/// `action` is the granular capability the route needs, not `Admin`: a
/// backup credential should not be able to mint API keys. Every
/// administrative route group names its own, and
/// `every_admin_route_asks_for_its_own_capability` checks that none of them
/// shares one by accident. A policy grants all of them at once with
/// `action in Chronix::Action::"Admin"`.
///
/// # Errors
///
/// `401` when authentication is configured and the request carried no
/// credential, `403` when the credential lacks the capability or the policy
/// denies it.
pub fn require_capability(
    state: &crate::http::SharedState,
    auth_ctx: Option<&chronix_security::auth::AuthContext>,
    action: chronix_security::authz::ChronixAction,
) -> Result<(), (StatusCode, String)> {
    debug_assert!(
        action.is_administrative(),
        "{action} is a data action; the namespace gate asks about those"
    );

    let ctx = match auth_ctx {
        Some(c) => c,
        None => {
            // No principal at all. That happens only when the operator
            // configured no authentication, in which case every endpoint is
            // already open and refusing here would be theatre.
            if state.auth_state.is_some() {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    "administrative operations require authentication".to_string(),
                ));
            }
            return Ok(());
        }
    };

    // A refusal is the event the trail exists for, and neither of the two
    // below was recorded: an operator investigating an attempt on the
    // control plane found a `warn!` in the process log, which does not
    // survive a restart and cannot be shown to be unedited.
    let refuse = |reason: &str, message: String| {
        crate::audit::record(
            state,
            &ctx.principal,
            chronix_security::audit::AuditAction::Admin,
            action.to_string(),
            chronix_security::audit::AuditDecision::Deny,
            &[("reason", reason.to_string())],
        );
        Err((StatusCode::FORBIDDEN, message))
    };

    if !ctx.admin {
        warn!(
            principal = %ctx.principal,
            %action,
            "administrative operation refused: credential has no admin capability"
        );
        return refuse(
            "credential has no admin capability",
            "administrative operations require a credential marked `admin`".to_string(),
        );
    }

    let Some(engine) = &state.authz_engine else {
        return Ok(());
    };

    match engine.authorize_system(&ctx.principal(), action) {
        chronix_security::authz::Decision::Allow => Ok(()),
        chronix_security::authz::Decision::Deny { reasons } => {
            warn!(
                principal = %ctx.principal,
                %action,
                reasons = ?reasons,
                "administrative operation denied by policy"
            );
            refuse(
                "denied by policy",
                format!("access denied: {action} is not permitted for this principal"),
            )
        }
    }
}

/// Axum layer enforcing one administrative capability over a route group.
///
/// Applied per group rather than once over `/api/v1/admin`, so the action is
/// a property of the routes it guards instead of something a handler has to
/// remember. Every group had the same `Admin` action before, which is why
/// nine granular capabilities existed in the model, were tested, and were
/// asked for by nothing.
pub async fn capability_layer(
    State((state, action)): State<(AppState, chronix_security::authz::ChronixAction)>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let auth_ctx = req
        .extensions()
        .get::<chronix_security::auth::AuthContext>()
        .cloned();
    if let Err((status, msg)) = require_capability(&state, auth_ctx.as_ref(), action) {
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

    // Try middleware first, then check mutable key store.
    //
    // The async form so a JWKS-backed deployment can fetch and cache the
    // issuer's keys; it delegates to the sync form whenever no cache is
    // attached, which is every other deployment.
    match auth
        .middleware
        .authenticate_async(bearer_token.as_deref(), None)
        .await
    {
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
                    store.authenticate(token).ok()
                };
                if let Some(ctx) = validated {
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
            // A refused credential is a *rate*, not just a log line: the
            // security checklist tells an operator to watch this for
            // brute-force attempts, and the metric it named did not exist, so
            // the alert was silently dead. Labelled by protocol rather than by
            // principal — a failed credential has no trustworthy principal,
            // and labelling by one would let an attacker mint cardinality.
            metrics::counter!("chronix_auth_failures_total", "protocol" => "http").increment(1);
            // A refused credential is exactly the event an audit trail
            // exists for, and only the gRPC side was recording it.
            crate::audit::record(
                &state,
                "anonymous",
                chronix_security::audit::AuditAction::LoginFailure,
                path.clone(),
                chronix_security::audit::AuditDecision::Deny,
                &[
                    ("reason", mw_err.to_string()),
                    ("protocol", "http".to_string()),
                ],
            );
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
    /// Namespaces the key may act in.
    ///
    /// Required under multi-tenancy: a key minted at runtime with no
    /// confinement reaches every tenant, which is the same hole the
    /// startup check closes for configured keys.
    #[serde(default)]
    pub namespaces: Vec<String>,
    /// Cedar roles the key carries.
    ///
    /// A key minted here had none and no way to be given any, so it could
    /// not match a single policy in the security guide. There is
    /// deliberately no `admin` field: the administrative *capability* is
    /// configuration, because a holder of `ManageKeys` who could mint an
    /// admin key would hold every capability.
    #[serde(default)]
    pub roles: Vec<String>,
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
    req_extensions: axum::http::Extensions,
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

    if state.config.server.multi_tenancy && body.namespaces.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "a key must name the namespaces it may act in when multi-tenancy is on".to_string(),
        ));
    }

    // Through `AuthState`, which records it: the raw key below appears in
    // this response and nowhere else ever again, so an operator writes it
    // into a deployment immediately — and one that does not survive a
    // restart cannot be recovered.
    let raw_key = auth
        .create_key(
            name,
            body.expires_at,
            body.namespaces.clone(),
            body.roles.clone(),
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    info!(name = %name, "API key created");
    // The **caller**, not the key that was just minted. Recording the new
    // key's own name as the principal answers "who did this?" with the thing
    // that was done, which is the one question the trail exists for.
    crate::audit::record(
        &state,
        &crate::audit::principal_of(&req_extensions),
        chronix_security::audit::AuditAction::ApiKeyCreate,
        format!("api_key:{name}"),
        chronix_security::audit::AuditDecision::Allow,
        &[
            ("namespaces", body.namespaces.join(",")),
            ("roles", body.roles.join(",")),
        ],
    );
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
    req_extensions: axum::http::Extensions,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let auth = match &state.auth_state {
        Some(a) => a,
        None => return StatusCode::NOT_FOUND,
    };

    // Through `AuthState`, which records it: a revocation that lasts only
    // as long as the process is one a restart undoes, and revoking a leaked
    // credential is exactly what nobody can afford to have undone.
    let revoked = match auth.revoke(&name) {
        Ok(revoked) => revoked,
        Err(e) => {
            warn!(name = %name, error = %e, "could not record the revocation");
            crate::audit::record(
                &state,
                &crate::audit::principal_of(&req_extensions),
                chronix_security::audit::AuditAction::ApiKeyRevoke,
                format!("api_key:{name}"),
                chronix_security::audit::AuditDecision::Deny,
                &[("reason", "could not be made durable".to_string())],
            );
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };

    if revoked {
        info!(name = %name, "API key revoked");
        crate::audit::record(
            &state,
            &crate::audit::principal_of(&req_extensions),
            chronix_security::audit::AuditAction::ApiKeyRevoke,
            format!("api_key:{name}"),
            chronix_security::audit::AuditDecision::Allow,
            &[],
        );
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

    /// **Reproduction.** Revoking a key must outlive the process.
    ///
    /// `DELETE /api/v1/admin/auth/keys/{name}` answers `204` and removes the
    /// key from an **in-memory** store. Every `[[auth.api_keys]]` entry is
    /// re-registered from the config at the next start, so revoking a leaked
    /// credential lasts exactly as long as the process does — and nothing
    /// says so. An operator revokes a compromised key during an incident,
    /// is told it is done, and a restart hands it back.
    #[test]
    fn a_revoked_key_stays_revoked_across_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let config = AuthConfig {
            api_keys: vec![ApiKeyEntry {
                name: "leaked".to_string(),
                key: "leaked-secret".to_string(),
                namespaces: Vec::new(),
                admin: false,
                roles: Vec::new(),
            }],
            jwt: None,
            exempt_paths: vec![],
        };

        let state = AuthState::from_config_at(&config, Some(dir.path())).unwrap();
        assert!(state.key_store.read().validate("leaked-secret").is_ok());
        assert!(state.revoke("leaked").unwrap());
        assert!(
            state.key_store.read().validate("leaked-secret").is_err(),
            "the revocation takes effect immediately"
        );

        // The same config, a new process.
        let restarted = AuthState::from_config_at(&config, Some(dir.path())).unwrap();
        assert!(
            restarted
                .key_store
                .read()
                .validate("leaked-secret")
                .is_err(),
            "a revoked credential must not come back when the server restarts"
        );
    }

    /// A key minted through the admin API must outlive the process too.
    ///
    /// It is shown **once**, in the creation response, and cannot be
    /// recovered — so an operator writes it into a deployment and it stops
    /// working at the next restart, with no way to get it back.
    #[test]
    fn a_minted_key_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let config = AuthConfig {
            api_keys: vec![],
            jwt: None,
            exempt_paths: vec![],
        };

        let state = AuthState::from_config_at(&config, Some(dir.path())).unwrap();
        let raw = state
            .create_key(
                "ingest",
                None,
                vec!["tenant-a".into()],
                vec!["writer".into()],
            )
            .unwrap();

        let restarted = AuthState::from_config_at(&config, Some(dir.path())).unwrap();
        let store = restarted.key_store.read();
        let name = store
            .validate(&raw)
            .expect("a key the API said it created must still work");
        assert_eq!(name, "ingest");
        assert_eq!(store.namespaces_for("ingest"), ["tenant-a"]);
        assert_eq!(store.roles("ingest"), ["writer"]);
    }

    #[test]
    fn auth_state_with_api_key() {
        let config = AuthConfig {
            api_keys: vec![ApiKeyEntry {
                name: "test-key".to_string(),
                key: "super-secret-key-123".to_string(),
                namespaces: Vec::new(),
                admin: false,
                roles: Vec::new(),
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
                public_key_pem_file: None,
                jwks_url: None,
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
                namespaces: Vec::new(),
                admin: false,
                roles: Vec::new(),
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
                namespaces: Vec::new(),
                admin: false,
                roles: Vec::new(),
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
    fn a_capability_is_required_even_without_an_engine() {
        // No policy engine is the default deployment, and it used to mean
        // "permit" — so any authenticated key could restore a backup over
        // the live data directory. The capability lives on the credential.
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
            namespace_registry: Arc::new(chronix_security::tenant::NamespaceRegistry::new()),
            model_catalog: std::sync::Arc::new(parking_lot::RwLock::new(
                chronix::chronix_analytics::forecast::ModelCatalog::new(),
            )),
            authz_engine: None,
            audit_logger: None,
            namespace_rate_limiter: crate::rate_limit::NamespaceRateLimiter::new(),
            sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
            config: crate::config::ServerConfig::default(),
            write_dedup_cache: None,
            pipeline: None,
            openapi_json: std::sync::OnceLock::new(),
            write_timeout: std::time::Duration::ZERO,
        };
        let ctx = chronix_security::auth::AuthContext {
            principal: "user1".to_string(),
            method: chronix_security::auth::AuthMethod::ApiKey,
            claims: Default::default(),
            namespaces: Vec::new(),
            admin: false,
            roles: Vec::new(),
        };
        let (status, _) = super::require_capability(
            &state,
            Some(&ctx),
            chronix_security::authz::ChronixAction::ManageKeys,
        )
        .expect_err("an ordinary key must not be an administrator");
        assert_eq!(status, StatusCode::FORBIDDEN);

        let admin_ctx = chronix_security::auth::AuthContext { admin: true, ..ctx };
        assert!(
            super::require_capability(
                &state,
                Some(&admin_ctx),
                chronix_security::authz::ChronixAction::ManageKeys
            )
            .is_ok(),
            "a key marked admin must still be able to administer"
        );
    }

    #[test]
    fn no_credential_and_no_auth_configured_is_open() {
        // With no auth context, require_capability permits (open mode).
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
            namespace_registry: Arc::new(chronix_security::tenant::NamespaceRegistry::new()),
            model_catalog: std::sync::Arc::new(parking_lot::RwLock::new(
                chronix::chronix_analytics::forecast::ModelCatalog::new(),
            )),
            authz_engine: None,
            audit_logger: None,
            namespace_rate_limiter: crate::rate_limit::NamespaceRateLimiter::new(),
            sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
            config: crate::config::ServerConfig::default(),
            write_dedup_cache: None,
            pipeline: None,
            openapi_json: std::sync::OnceLock::new(),
            write_timeout: std::time::Duration::ZERO,
        };
        assert!(super::require_capability(
            &state,
            None,
            chronix_security::authz::ChronixAction::ManageKeys
        )
        .is_ok());
    }

    #[test]
    fn an_engine_cannot_widen_what_the_credential_carries() {
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
            namespace_registry: Arc::new(chronix_security::tenant::NamespaceRegistry::new()),
            model_catalog: std::sync::Arc::new(parking_lot::RwLock::new(
                chronix::chronix_analytics::forecast::ModelCatalog::new(),
            )),
            authz_engine: Some(std::sync::Arc::new(engine)),
            audit_logger: None,
            namespace_rate_limiter: crate::rate_limit::NamespaceRateLimiter::new(),
            sql_plan_cache: parking_lot::Mutex::new(std::collections::HashMap::new()),
            config: crate::config::ServerConfig::default(),
            write_dedup_cache: None,
            pipeline: None,
            openapi_json: std::sync::OnceLock::new(),
            write_timeout: std::time::Duration::ZERO,
        };
        let ctx = chronix_security::auth::AuthContext {
            principal: "user1".to_string(),
            method: chronix_security::auth::AuthMethod::ApiKey,
            claims: Default::default(),
            namespaces: Vec::new(),
            admin: false,
            roles: Vec::new(),
        };
        let result = super::require_capability(
            &state,
            Some(&ctx),
            chronix_security::authz::ChronixAction::ManageKeys,
        );
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
                namespaces: Vec::new(),
                admin: false,
                roles: Vec::new(),
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
                namespaces: Vec::new(),
                admin: false,
                roles: Vec::new(),
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
                namespaces: Vec::new(),
                admin: false,
                roles: Vec::new(),
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
                namespaces: Vec::new(),
                admin: false,
                roles: Vec::new(),
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
        let val = chronix_security::auth::jwt::resolve_claim_path(&claims, "roles").unwrap();
        assert_eq!(val, &serde_json::json!(["admin", "reader"]));
    }

    #[test]
    fn resolve_claim_path_nested_dot() {
        let mut claims = std::collections::HashMap::new();
        claims.insert(
            "realm_access".to_string(),
            serde_json::json!({"roles": ["admin"]}),
        );
        let val =
            chronix_security::auth::jwt::resolve_claim_path(&claims, "realm_access.roles").unwrap();
        assert_eq!(val, &serde_json::json!(["admin"]));
    }

    #[test]
    fn resolve_claim_path_uri_key() {
        let mut claims = std::collections::HashMap::new();
        claims.insert(
            "https://example.com/roles".to_string(),
            serde_json::json!(["editor"]),
        );
        let val =
            chronix_security::auth::jwt::resolve_claim_path(&claims, "https://example.com/roles")
                .unwrap();
        assert_eq!(val, &serde_json::json!(["editor"]));
    }

    #[test]
    fn resolve_claim_path_missing() {
        let claims = std::collections::HashMap::new();
        assert!(chronix_security::auth::jwt::resolve_claim_path(&claims, "roles").is_none());
        assert!(chronix_security::auth::jwt::resolve_claim_path(&claims, "a.b.c").is_none());
    }

    #[test]
    fn resolve_claim_path_deep_nested() {
        let mut claims = std::collections::HashMap::new();
        claims.insert(
            "a".to_string(),
            serde_json::json!({"b": {"c": "deep_value"}}),
        );
        let val = chronix_security::auth::jwt::resolve_claim_path(&claims, "a.b.c").unwrap();
        assert_eq!(val, &serde_json::json!("deep_value"));
    }

    // ── gRPC auth failure audit logging ───────────────────────

    #[test]
    fn grpc_interceptor_logs_auth_failure_to_audit() {
        let config = AuthConfig {
            api_keys: vec![ApiKeyEntry {
                name: "test-key".to_string(),
                key: "secret".to_string(),
                namespaces: Vec::new(),
                admin: false,
                roles: Vec::new(),
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
