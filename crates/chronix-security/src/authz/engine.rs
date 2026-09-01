//! Cedar policy authorizer engine.
//!
//! This module wraps the Cedar policy engine and provides the core
//! `authorize()` function for Chronix.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use cedar_policy::{
    Authorizer, Context, Entities, PolicySet, Request, Schema, ValidationMode, Validator,
};
use metrics::counter;
use parking_lot::RwLock;
use tracing::{debug, error, info, warn};

use crate::authz::error::{AuthzError, Result};
use crate::authz::model::{
    ChronixAction, ChronixNamespace, ChronixPrincipal, ChronixResource, Decision,
};

/// Maximum number of policy versions retained in the history ring buffer.
const MAX_POLICY_VERSIONS: usize = 10;

/// A snapshot of a policy set at a particular version.
#[derive(Debug, Clone)]
pub struct PolicyVersion {
    /// Monotonically increasing version number.
    pub version: u64,
    /// The policy set at this version.
    pub policies: Arc<PolicySet>,
    /// Human-readable description of the change (e.g. "full reload", "add policy foo").
    pub source: String,
    /// Unix timestamp (seconds) when this version was created.
    pub timestamp: i64,
}

/// The Cedar-based authorization engine.
///
/// Thread-safe — can be shared across threads via `Arc<AuthzEngine>`.
///
/// # Policy Versioning & Rollback
///
/// The engine maintains an in-memory ring buffer of the last
/// `MAX_POLICY_VERSIONS` policy snapshots. Every mutating operation
/// (`load_policies`, `add_policy`, `remove_policy`) bumps the version
/// counter and pushes the *previous* state into the history before
/// swapping in the new set. Rollback to any retained version is
/// available via [`revert_to_version()`](Self::revert_to_version).
///
/// A dry-run mode ([`dryrun_load_policies()`](Self::dryrun_load_policies))
/// parses and validates a policy string without modifying the active set.
///
/// # Cross-Tenant Isolation
///
/// There is no implicit "deny cross-tenant access" policy built into
/// the engine. Tenant isolation is enforced at two layers:
///
/// 1. **Namespace scoping** — every request carries a namespace context
///    (see `ChronixNamespace`). The storage and query layers filter by
///    namespace before authorization is even checked.
/// 2. **Explicit Cedar policies** — operators must deploy `forbid`
///    policies that deny access when `resource.namespace != principal.namespace`.
///    See `docs/security.md` for a reference policy template.
///
/// This explicit-policy approach avoids magic built-in rules that are
/// invisible to policy auditors.
pub struct AuthzEngine {
    /// The Cedar authorizer (stateless, thread-safe).
    authorizer: Authorizer,
    /// The active policy set — wrapped in Arc so authorize() can
    /// snapshot the pointer without holding the lock during evaluation
    /// (Lock-free read path.)
    policies: RwLock<Arc<PolicySet>>,
    /// Optional Cedar schema for policy validation.
    schema: Option<Schema>,
    /// When true, policy loads fail if no schema is set.
    require_schema: bool,
    /// Monotonically increasing version counter.
    version_counter: AtomicU64,
    /// Ring buffer of previous policy versions for rollback.
    history: RwLock<VecDeque<PolicyVersion>>,
}

impl AuthzEngine {
    /// Create a new authorizer with the built-in cross-tenant isolation
    /// policy auto-loaded (default-deny + namespace isolation).
    ///
    /// Schema validation is automatically enabled when a schema
    /// is loaded via `with_schema()` or `load_schema()`. Without a
    /// schema, policies are accepted without type-level validation.
    /// Call `.with_require_schema(true)` to force a schema requirement.
    #[must_use]
    pub fn new() -> Self {
        let engine = Self {
            authorizer: Authorizer::new(),
            policies: RwLock::new(Arc::new(PolicySet::new())),
            schema: None,
            require_schema: false,
            version_counter: AtomicU64::new(0),
            history: RwLock::new(VecDeque::new()),
        };

        // Auto-load the cross-tenant isolation policy so that
        // namespace-scoped principals are always denied access to
        // resources in a different namespace by default.
        if let Err(e) = engine.load_cross_tenant_isolation_policy() {
            tracing::error!(error = %e, "failed to auto-load cross-tenant isolation policy");
        }

        engine
    }

    /// Create an authorizer with the given policy set.
    pub fn with_policies(policies: PolicySet) -> Self {
        let count = policies.policies().count();
        info!(
            policy_count = count,
            "AuthzEngine initialized with policies"
        );
        Self {
            authorizer: Authorizer::new(),
            policies: RwLock::new(Arc::new(policies)),
            schema: None,
            require_schema: false,
            version_counter: AtomicU64::new(1),
            history: RwLock::new(VecDeque::new()),
        }
    }

    /// Set the Cedar schema for policy validation.
    ///
    /// When a schema is set, `load_policies()` and `add_policy()` will
    /// validate policies against it and reject those that reference
    /// unknown entity types, actions, or attributes.
    ///
    /// Setting a schema automatically enables `require_schema`,
    /// which means all policies MUST validate against the schema.
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self.require_schema = true;
        self
    }

    /// Load a Cedar schema from its string representation.
    ///
    /// Loading a schema automatically enables `require_schema`.
    pub fn load_schema(&mut self, schema_src: &str) -> Result<()> {
        let (schema, _warnings) = Schema::from_cedarschema_str(schema_src)
            .map_err(|e| AuthzError::SchemaParse(format!("{e}")))?;
        self.schema = Some(schema);
        self.require_schema = true;
        info!("Cedar schema loaded (require_schema auto-enabled)");
        Ok(())
    }

    /// Require a Cedar schema for all policy operations.
    /// When enabled, `load_policies()` and `add_policy()` will fail
    /// if no schema has been loaded.
    pub fn with_require_schema(mut self, require: bool) -> Self {
        self.require_schema = require;
        self
    }

    /// Validate a policy set against the schema (if present).
    ///
    /// Logs a warning when no schema is set. If `require_schema`
    /// is true, returns an error instead.
    fn validate_policies(&self, ps: &PolicySet) -> Result<()> {
        if let Some(ref schema) = self.schema {
            let validator = Validator::new(schema.clone());
            let result = validator.validate(ps, ValidationMode::default());
            let errors: Vec<String> = result.validation_errors().map(|e| format!("{e}")).collect();
            if !errors.is_empty() {
                return Err(AuthzError::PolicyValidation(errors.join("; ")));
            }
            let warnings: Vec<String> = result
                .validation_warnings()
                .map(|w| format!("{w}"))
                .collect();
            for w in &warnings {
                warn!(warning = %w, "Cedar policy validation warning");
            }
        } else if self.require_schema {
            return Err(AuthzError::PolicyValidation(
                "no Cedar schema loaded but require_schema is enabled; \
                 call load_schema() or with_schema() before loading policies"
                    .into(),
            ));
        } else {
            tracing::warn!(
                "no Cedar schema loaded — policies are not validated \
                 against a schema; typos in entity/action names will be silently accepted"
            );
        }
        Ok(())
    }

    /// Load policies from a Cedar policy string.
    ///
    /// Replaces all existing policies. If a schema is set, policies
    /// are validated against it before being accepted.
    ///
    /// The previous policy set is pushed into the version history ring
    /// buffer before being replaced.
    pub fn load_policies(&self, policy_src: &str) -> Result<usize> {
        let ps = policy_src
            .parse::<PolicySet>()
            .map_err(|e| AuthzError::PolicyParse(format!("{e}")))?;
        self.validate_policies(&ps)?;
        let count = ps.policies().count();
        let policy_ids: Vec<String> = ps.policies().map(|p| p.id().to_string()).collect();

        let new_version = self.version_counter.fetch_add(1, Ordering::Relaxed) + 1;
        self.push_history("full reload");

        *self.policies.write() = Arc::new(ps);
        info!(count, version = new_version, policies = ?policy_ids, "Policies loaded (full reload)");
        counter!("chronix_authz_policy_load", "operation" => "load_all").increment(1);
        Ok(count)
    }

    /// Dry-run policy loading: parse and validate without modifying the
    /// active policy set.
    ///
    /// Returns the number of policies that would be loaded, or an error
    /// if parsing or schema validation fails. The active policy set is
    /// **not** modified.
    pub fn dryrun_load_policies(&self, policy_src: &str) -> Result<usize> {
        let ps = policy_src
            .parse::<PolicySet>()
            .map_err(|e| AuthzError::PolicyParse(format!("{e}")))?;
        self.validate_policies(&ps)?;
        let count = ps.policies().count();
        info!(count, "Dry-run policy validation passed");
        Ok(count)
    }

    /// Add a single policy from source.
    ///
    /// Uses a clone-and-swap strategy: clones the current policy set
    /// under a brief read lock, builds and validates the candidate set
    /// without holding any lock, and only acquires a write lock for
    /// the final atomic swap.  If parsing or validation fails, the
    /// current set is left untouched (automatic rollback).
    pub fn add_policy(&self, id: &str, policy_src: &str) -> Result<()> {
        let policy = cedar_policy::Policy::parse(Some(cedar_policy::PolicyId::new(id)), policy_src)
            .map_err(|e| AuthzError::PolicyParse(format!("{e}")))?;

        // 1. Snapshot current policies under read lock (brief)
        let mut new_ps = {
            let ps = self.policies.read();
            let mut candidate = PolicySet::new();
            for p in ps.policies() {
                candidate
                    .add(p.clone())
                    .map_err(|e| AuthzError::Internal(format!("Duplicate policy: {e}")))?;
            }
            candidate
        };

        // 2. Build and validate candidate (no lock held)
        new_ps
            .add(policy)
            .map_err(|e| AuthzError::Internal(format!("Failed to add policy: {e}")))?;
        self.validate_policies(&new_ps)?;

        // 3. Atomic swap under write lock
        let total = new_ps.policies().count();
        let new_version = self.version_counter.fetch_add(1, Ordering::Relaxed) + 1;
        self.push_history(&format!("add policy {id}"));
        *self.policies.write() = Arc::new(new_ps);
        info!(
            policy_id = id,
            total_policies = total,
            version = new_version,
            "Policy added"
        );
        counter!("chronix_authz_policy_load", "operation" => "add").increment(1);
        Ok(())
    }

    /// Remove a policy by ID.
    ///
    /// Returns `true` if the policy was found and removed.
    pub fn remove_policy(&self, id: &str) -> bool {
        let mut guard = self.policies.write();
        // Clone the policy set out of the Arc so we can mutate it
        let mut ps = PolicySet::new();
        for p in guard.policies() {
            // Log rather than silently dropping add failures.
            if let Err(e) = ps.add(p.clone()) {
                tracing::error!(error = %e, "failed to re-add policy during remove_policy rebuild");
            }
        }
        let policy_id = cedar_policy::PolicyId::new(id);
        match ps.remove_static(policy_id) {
            Ok(_) => {
                let remaining = ps.policies().count();
                // Push history *before* swapping (guard is held, so
                // snapshot is the current live set).
                let prev = Arc::clone(&guard);
                let ver = self.version_counter.fetch_add(1, Ordering::Relaxed) + 1;
                {
                    let mut hist = self.history.write();
                    if hist.len() >= MAX_POLICY_VERSIONS {
                        hist.pop_front();
                    }
                    hist.push_back(PolicyVersion {
                        version: ver.saturating_sub(1),
                        policies: prev,
                        source: format!("before remove policy {id}"),
                        timestamp: now_unix(),
                    });
                }
                *guard = Arc::new(ps);
                info!(
                    policy_id = id,
                    remaining_policies = remaining,
                    version = ver,
                    "Policy removed"
                );
                counter!("chronix_authz_policy_load", "operation" => "remove").increment(1);
                true
            }
            Err(_) => {
                warn!(id, "Policy not found for removal");
                false
            }
        }
    }

    /// Push the current active policy set into the history ring buffer.
    ///
    /// Called internally before every mutating operation.
    fn push_history(&self, source: &str) {
        let current = Arc::clone(&self.policies.read());
        let ver = self.version_counter.load(Ordering::Relaxed);
        let mut hist = self.history.write();
        if hist.len() >= MAX_POLICY_VERSIONS {
            hist.pop_front();
        }
        hist.push_back(PolicyVersion {
            // The version being *replaced* is the current counter
            // before the caller incremented it. Since the caller does
            // fetch_add(1) + 1, the version of the old set is (new - 1).
            version: ver.saturating_sub(1),
            policies: current,
            source: source.to_string(),
            timestamp: now_unix(),
        });
    }

    /// Return the current policy version number.
    #[must_use]
    pub fn current_version(&self) -> u64 {
        self.version_counter.load(Ordering::Relaxed)
    }

    /// List all retained policy versions (oldest first).
    #[must_use]
    pub fn policy_versions(&self) -> Vec<PolicyVersion> {
        self.history.read().iter().cloned().collect()
    }

    /// Revert the active policy set to a previously retained version
    ///.
    ///
    /// The version must exist in the history ring buffer. On success
    /// the reverted-to snapshot becomes the active set and the
    /// *pre-revert* state is pushed into history (so it can itself be
    /// rolled back).
    pub fn revert_to_version(&self, version: u64) -> Result<()> {
        let snapshot = {
            let hist = self.history.read();
            hist.iter()
                .find(|v| v.version == version)
                .map(|v| Arc::clone(&v.policies))
        };
        let snapshot = snapshot.ok_or_else(|| {
            AuthzError::Internal(format!(
                "Policy version {version} not found in history (retained: {})",
                self.history
                    .read()
                    .iter()
                    .map(|v| v.version.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;

        let new_version = self.version_counter.fetch_add(1, Ordering::Relaxed) + 1;
        self.push_history(&format!("before revert to v{version}"));
        let count = snapshot.policies().count();
        *self.policies.write() = snapshot;
        info!(
            reverted_to = version,
            new_version = new_version,
            policy_count = count,
            "Policy set reverted"
        );
        counter!("chronix_authz_policy_load", "operation" => "revert").increment(1);
        Ok(())
    }

    /// Load policies from all `.cedar` files in a directory.
    pub fn load_policies_from_dir(&self, dir: &Path) -> Result<usize> {
        let mut combined = String::new();
        let mut file_count = 0;

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "cedar") {
                let content = std::fs::read_to_string(&path)?;
                combined.push_str(&content);
                combined.push('\n');
                file_count += 1;
                debug!(path = %path.display(), "Loaded policy file");
            }
        }

        if file_count == 0 {
            info!(dir = %dir.display(), "No .cedar files found");
            return Ok(0);
        }

        self.load_policies(&combined)
    }

    /// Evaluate an authorization request.
    ///
    /// Returns `Decision::Allow` if any policy explicitly permits the request,
    /// or `Decision::Deny` with reasons otherwise (default-deny).
    pub fn authorize(
        &self,
        principal: &ChronixPrincipal,
        action: ChronixAction,
        resource: &ChronixResource,
    ) -> Decision {
        let principal_entity = match principal.to_entity() {
            Ok(e) => e,
            Err(e) => {
                error!(error = %e, "Cedar principal entity construction failed");
                return Decision::Deny {
                    reasons: vec!["Internal authorization error".into()],
                };
            }
        };
        let resource_entity = match resource.to_entity() {
            Ok(e) => e,
            Err(e) => {
                error!(error = %e, "Cedar resource entity construction failed");
                return Decision::Deny {
                    reasons: vec!["Internal authorization error".into()],
                };
            }
        };

        // Build role entities (for membership) using cached type names
        #[allow(clippy::result_large_err)] // cedar_policy error type; not on a hot path
        let role_entities: std::result::Result<Vec<cedar_policy::Entity>, _> = principal
            .roles
            .iter()
            .map(|role| {
                let uid = cedar_policy::EntityUid::from_type_name_and_id(
                    crate::authz::model::role_type_name().clone(),
                    cedar_policy::EntityId::new(role),
                );
                cedar_policy::Entity::new(uid, HashMap::new(), HashSet::new())
            })
            .collect();

        let role_entities = match role_entities {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "Cedar role entity construction failed");
                return Decision::Deny {
                    reasons: vec!["Internal authorization error".into()],
                };
            }
        };

        let mut all_entities = vec![principal_entity, resource_entity];
        all_entities.extend(role_entities);

        // Include namespace entity so Cedar can resolve `resource in Chronix::Namespace::"..."`.
        if let Some(ns_entity) = resource.namespace_entity() {
            all_entities.push(ns_entity);
        }

        let entities = match Entities::from_entities(all_entities, None) {
            Ok(e) => e,
            Err(e) => {
                error!(error = %e, "Cedar entity set construction failed");
                return Decision::Deny {
                    reasons: vec!["Internal authorization error".into()],
                };
            }
        };

        let request = match Request::new(
            principal.to_entity_uid(),
            action.to_entity_uid(),
            resource.to_entity_uid(),
            Context::empty(),
            None,
        ) {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "Cedar authorization request construction failed");
                return Decision::Deny {
                    reasons: vec!["Internal authorization error".into()],
                };
            }
        };

        // Snapshot the policy set via Arc clone (brief read lock),
        // then drop the lock before the expensive authorization
        // evaluation. This prevents policy hot-reloads from blocking
        // concurrent authorize() calls.
        let policies = Arc::clone(&self.policies.read());
        let response = self
            .authorizer
            .is_authorized(&request, &policies, &entities);

        let decision_label = match response.decision() {
            cedar_policy::Decision::Allow => "allow",
            cedar_policy::Decision::Deny => "deny",
        };

        counter!("chronix_authz_decisions_total", "action" => action.to_string(), "decision" => decision_label)
            .increment(1);

        match response.decision() {
            cedar_policy::Decision::Allow => Decision::Allow,
            cedar_policy::Decision::Deny => {
                // Log detailed diagnostics server-side for debugging,
                // but only return a generic reason to callers to avoid
                // leaking policy IDs or evaluation internals to clients.
                let eval_errors: Vec<String> = response
                    .diagnostics()
                    .errors()
                    .map(std::string::ToString::to_string)
                    .collect();

                let policy_reasons: Vec<String> = response
                    .diagnostics()
                    .reason()
                    .map(std::string::ToString::to_string)
                    .collect();

                if !policy_reasons.is_empty() || !eval_errors.is_empty() {
                    tracing::debug!(
                        ?policy_reasons,
                        ?eval_errors,
                        %action,
                        principal = %principal.id,
                        "authorization denied with diagnostics"
                    );
                }

                Decision::Deny {
                    reasons: vec!["access denied".into()],
                }
            }
        }
    }

    /// Evaluate a granular admin action, falling back to the `Admin`
    /// super-action if the specific action is denied.
    ///
    /// Policies that grant `Action::"Admin"` automatically cover
    /// all granular admin actions (`ManageNodes`, `ManageKeys`, etc.).
    /// This avoids breaking existing policies while enabling least-privilege
    /// grants for new deployments.
    pub fn authorize_admin(
        &self,
        principal: &ChronixPrincipal,
        action: ChronixAction,
        resource: &ChronixResource,
    ) -> Decision {
        let result = self.authorize(principal, action, resource);
        if result.is_allowed() || action == ChronixAction::Admin {
            return result;
        }
        // Granular action was denied — try the super-action.
        self.authorize(principal, ChronixAction::Admin, resource)
    }

    /// Evaluate an authorization request against a namespace resource.
    ///
    /// This is used for namespace-level policies, e.g.:
    /// ```cedar
    /// permit(
    ///   principal in Chronix::Role::"ns_admin",
    ///   action,
    ///   resource == Chronix::Namespace::"production"
    /// );
    /// ```
    pub fn authorize_namespace(
        &self,
        principal: &ChronixPrincipal,
        action: ChronixAction,
        namespace: &ChronixNamespace,
    ) -> Decision {
        let principal_entity = match principal.to_entity() {
            Ok(e) => e,
            Err(e) => {
                error!(error = %e, "Cedar principal entity construction failed");
                return Decision::Deny {
                    reasons: vec!["Internal authorization error".into()],
                };
            }
        };
        let namespace_entity = match namespace.to_entity() {
            Ok(e) => e,
            Err(e) => {
                error!(error = %e, "Cedar namespace entity construction failed");
                return Decision::Deny {
                    reasons: vec!["Internal authorization error".into()],
                };
            }
        };

        #[allow(clippy::result_large_err)] // cedar_policy error type; not on a hot path
        let role_entities: std::result::Result<Vec<cedar_policy::Entity>, _> = principal
            .roles
            .iter()
            .map(|role| {
                let uid = cedar_policy::EntityUid::from_type_name_and_id(
                    crate::authz::model::role_type_name().clone(),
                    cedar_policy::EntityId::new(role),
                );
                cedar_policy::Entity::new(uid, HashMap::new(), HashSet::new())
            })
            .collect();

        let role_entities = match role_entities {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "Cedar role entity construction failed");
                return Decision::Deny {
                    reasons: vec!["Internal authorization error".into()],
                };
            }
        };

        let mut all_entities = vec![principal_entity, namespace_entity];
        all_entities.extend(role_entities);

        let entities = match Entities::from_entities(all_entities, None) {
            Ok(e) => e,
            Err(e) => {
                error!(error = %e, "Cedar entity set construction failed");
                return Decision::Deny {
                    reasons: vec!["Internal authorization error".into()],
                };
            }
        };

        let request = match Request::new(
            principal.to_entity_uid(),
            action.to_entity_uid(),
            namespace.to_entity_uid(),
            Context::empty(),
            None,
        ) {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "Cedar authorization request construction failed");
                return Decision::Deny {
                    reasons: vec!["Internal authorization error".into()],
                };
            }
        };

        // Snapshot policy set via Arc clone for lock-free evaluation.
        let policies = Arc::clone(&self.policies.read());
        let response = self
            .authorizer
            .is_authorized(&request, &policies, &entities);

        let decision_label = match response.decision() {
            cedar_policy::Decision::Allow => "allow",
            cedar_policy::Decision::Deny => "deny",
        };

        counter!(
            "chronix_authz_decisions_total",
            "action" => action.to_string(),
            "resource_type" => "namespace",
            "decision" => decision_label
        )
        .increment(1);

        match response.decision() {
            cedar_policy::Decision::Allow => Decision::Allow,
            cedar_policy::Decision::Deny => {
                let eval_errors: Vec<String> = response
                    .diagnostics()
                    .errors()
                    .map(std::string::ToString::to_string)
                    .collect();
                let policy_reasons: Vec<String> = response
                    .diagnostics()
                    .reason()
                    .map(std::string::ToString::to_string)
                    .collect();

                if !policy_reasons.is_empty() || !eval_errors.is_empty() {
                    tracing::debug!(
                        ?policy_reasons,
                        ?eval_errors,
                        %action,
                        principal = %principal.id,
                        namespace = %namespace.name,
                        "namespace authorization denied with diagnostics"
                    );
                }

                Decision::Deny {
                    reasons: vec!["access denied".into()],
                }
            }
        }
    }

    /// Returns the number of active policies.
    #[must_use]
    pub fn policy_count(&self) -> usize {
        self.policies.read().policies().count()
    }

    /// List all policy IDs.
    #[must_use]
    pub fn policy_ids(&self) -> Vec<String> {
        self.policies
            .read()
            .policies()
            .map(|p| p.id().to_string())
            .collect()
    }

    /// **** Load the default cross-tenant isolation policy.
    ///
    /// This policy forbids any principal from accessing resources in a
    /// namespace that differs from the principal's own namespace attribute.
    /// Without this, data is visible across tenants unless explicit Cedar
    /// `forbid` policies are deployed.
    ///
    /// The policy is idempotent — calling this multiple times is safe
    /// (the existing built-in policy is removed before re-adding).
    pub fn load_cross_tenant_isolation_policy(&self) -> Result<()> {
        const POLICY_ID: &str = "chronix-builtin-cross-tenant-deny";
        // Remove if already present (idempotent)
        self.remove_policy(POLICY_ID);
        self.add_policy(POLICY_ID, Self::CROSS_TENANT_ISOLATION_POLICY)
    }

    /// **** Check if any namespace-scoped policies are loaded.
    ///
    /// Logs a warning if no policies reference `Chronix::Namespace`,
    /// which likely means cross-tenant isolation is not enforced.
    pub fn warn_if_no_namespace_policies(&self) {
        let policies = self.policies.read();
        let has_ns = policies.policies().any(|p| {
            let src = format!("{p}");
            src.contains("Namespace")
        });
        if !has_ns {
            warn!(
                "No namespace-scoped Cedar policies detected! \
                 Cross-tenant data isolation may not be enforced. \
                 Call `load_cross_tenant_isolation_policy()` or deploy \
                 explicit namespace policies. See docs/security.md."
            );
        }
    }

    /// Built-in Cedar policy that denies cross-tenant resource access.
    ///
    /// Uses Cedar's `forbid` with `unless` to deny any request where
    /// the resource's namespace does not match the principal's namespace,
    /// unless the principal has the `admin` role.
    pub const CROSS_TENANT_ISOLATION_POLICY: &'static str = r#"
    forbid(
        principal,
        action,
        resource
    )
    when {
        resource has namespace &&
        principal has namespace &&
        resource.namespace != principal.namespace
    }
    unless {
        principal in Chronix::Role::"admin"
    };
    "#;
}

/// Returns the current Unix timestamp in seconds.
fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

impl Default for AuthzEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for AuthzEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthzEngine")
            .field("policy_count", &self.policy_count())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin_allow_all_policy() -> &'static str {
        r#"
        permit(
            principal in Chronix::Role::"admin",
            action,
            resource
        );
        "#
    }

    fn read_only_policy() -> &'static str {
        r#"
        permit(
            principal,
            action == Chronix::Action::"Read",
            resource
        );
        "#
    }

    fn measurement_scoped_policy() -> &'static str {
        r#"
        permit(
            principal in Chronix::Role::"data-science",
            action == Chronix::Action::"Forecast",
            resource == Chronix::Measurement::"energy"
        );
        "#
    }

    #[test]
    fn default_deny_no_policies() {
        let engine = AuthzEngine::new();
        let principal = ChronixPrincipal::new("alice");
        let resource = ChronixResource::measurement("cpu");
        let decision = engine.authorize(&principal, ChronixAction::Read, &resource);
        assert!(decision.is_denied());
    }

    #[test]
    fn admin_role_allows_all() {
        let engine = AuthzEngine::new();
        engine.load_policies(admin_allow_all_policy()).unwrap();

        let admin = ChronixPrincipal::new("alice").with_role("admin");
        let resource = ChronixResource::measurement("cpu");

        assert!(engine
            .authorize(&admin, ChronixAction::Write, &resource)
            .is_allowed());
        assert!(engine
            .authorize(&admin, ChronixAction::Read, &resource)
            .is_allowed());
        assert!(engine
            .authorize(&admin, ChronixAction::Delete, &resource)
            .is_allowed());
        assert!(engine
            .authorize(&admin, ChronixAction::Admin, &resource)
            .is_allowed());
    }

    #[test]
    fn non_admin_denied_without_matching_policy() {
        let engine = AuthzEngine::new();
        engine.load_policies(admin_allow_all_policy()).unwrap();

        let user = ChronixPrincipal::new("bob"); // no role
        let resource = ChronixResource::measurement("cpu");

        assert!(engine
            .authorize(&user, ChronixAction::Write, &resource)
            .is_denied());
    }

    #[test]
    fn read_only_allows_read_denies_write() {
        let engine = AuthzEngine::new();
        engine.load_policies(read_only_policy()).unwrap();

        let user = ChronixPrincipal::new("bob");
        let resource = ChronixResource::measurement("cpu");

        assert!(engine
            .authorize(&user, ChronixAction::Read, &resource)
            .is_allowed());
        assert!(engine
            .authorize(&user, ChronixAction::Write, &resource)
            .is_denied());
    }

    #[test]
    fn measurement_scoped_authorization() {
        let engine = AuthzEngine::new();
        engine.load_policies(measurement_scoped_policy()).unwrap();

        let ds = ChronixPrincipal::new("carol").with_role("data-science");

        // Allowed: forecast on energy
        assert!(engine
            .authorize(
                &ds,
                ChronixAction::Forecast,
                &ChronixResource::measurement("energy")
            )
            .is_allowed());

        // Denied: forecast on cpu (wrong measurement)
        assert!(engine
            .authorize(
                &ds,
                ChronixAction::Forecast,
                &ChronixResource::measurement("cpu")
            )
            .is_denied());

        // Denied: write on energy (wrong action)
        assert!(engine
            .authorize(
                &ds,
                ChronixAction::Write,
                &ChronixResource::measurement("energy")
            )
            .is_denied());
    }

    #[test]
    fn add_and_remove_policy() {
        let engine = AuthzEngine::new();
        // Engine starts with 1 built-in cross-tenant isolation policy
        let builtin_count = engine.policy_count();
        assert!(builtin_count >= 1);

        engine
            .add_policy(
                "read-all",
                r#"permit(principal, action == Chronix::Action::"Read", resource);"#,
            )
            .unwrap();
        assert_eq!(engine.policy_count(), builtin_count + 1);

        let user = ChronixPrincipal::new("alice");
        let resource = ChronixResource::measurement("cpu");
        assert!(engine
            .authorize(&user, ChronixAction::Read, &resource)
            .is_allowed());

        assert!(engine.remove_policy("read-all"));
        assert_eq!(engine.policy_count(), builtin_count);
        assert!(engine
            .authorize(&user, ChronixAction::Read, &resource)
            .is_denied());
    }

    #[test]
    fn remove_nonexistent_policy() {
        let engine = AuthzEngine::new();
        assert!(!engine.remove_policy("does-not-exist"));
    }

    #[test]
    fn policy_ids_listing() {
        let engine = AuthzEngine::new();
        // Engine starts with built-in cross-tenant isolation policy
        let builtin_count = engine.policy_ids().len();
        engine
            .add_policy(
                "p1",
                r#"permit(principal, action == Chronix::Action::"Read", resource);"#,
            )
            .unwrap();
        engine
            .add_policy(
                "p2",
                r#"permit(principal, action == Chronix::Action::"Write", resource);"#,
            )
            .unwrap();

        let ids = engine.policy_ids();
        assert_eq!(ids.len(), builtin_count + 2);
        assert!(ids.contains(&"p1".to_string()));
        assert!(ids.contains(&"p2".to_string()));
    }

    #[test]
    fn invalid_policy_rejected() {
        let engine = AuthzEngine::new();
        let result = engine.load_policies("this is not valid cedar");
        assert!(result.is_err());
    }

    #[test]
    fn hot_reload_replaces_policies() {
        let engine = AuthzEngine::new();
        engine.load_policies(read_only_policy()).unwrap();

        let user = ChronixPrincipal::new("alice");
        let resource = ChronixResource::measurement("cpu");
        assert!(engine
            .authorize(&user, ChronixAction::Read, &resource)
            .is_allowed());
        assert!(engine
            .authorize(&user, ChronixAction::Write, &resource)
            .is_denied());

        // Hot-reload with admin policy
        engine.load_policies(admin_allow_all_policy()).unwrap();

        // Now only admin role has access
        assert!(engine
            .authorize(&user, ChronixAction::Read, &resource)
            .is_denied());
        let admin = ChronixPrincipal::new("alice").with_role("admin");
        assert!(engine
            .authorize(&admin, ChronixAction::Write, &resource)
            .is_allowed());
    }

    #[test]
    fn load_from_directory() {
        let dir = tempfile::tempdir().unwrap();
        let policy_path = dir.path().join("allow_read.cedar");
        std::fs::write(
            &policy_path,
            r#"permit(principal, action == Chronix::Action::"Read", resource);"#,
        )
        .unwrap();

        // Non-cedar files should be ignored
        std::fs::write(dir.path().join("notes.txt"), "not a policy").unwrap();

        let engine = AuthzEngine::new();
        let count = engine.load_policies_from_dir(dir.path()).unwrap();
        assert_eq!(count, 1);

        let user = ChronixPrincipal::new("alice");
        let resource = ChronixResource::measurement("cpu");
        assert!(engine
            .authorize(&user, ChronixAction::Read, &resource)
            .is_allowed());
    }

    #[test]
    fn empty_directory_loads_zero() {
        let dir = tempfile::tempdir().unwrap();
        let engine = AuthzEngine::new();
        let count = engine.load_policies_from_dir(dir.path()).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn multiple_actions_with_combined_policies() {
        let policies = r#"
            permit(
                principal in Chronix::Role::"ops",
                action == Chronix::Action::"Read",
                resource
            );
            permit(
                principal in Chronix::Role::"ops",
                action == Chronix::Action::"Write",
                resource
            );
            permit(
                principal in Chronix::Role::"admin",
                action,
                resource
            );
        "#;

        let engine = AuthzEngine::new();
        engine.load_policies(policies).unwrap();
        assert_eq!(engine.policy_count(), 3);

        let ops = ChronixPrincipal::new("dave").with_role("ops");
        let admin = ChronixPrincipal::new("eve").with_role("admin");
        let resource = ChronixResource::measurement("cpu");

        // ops can read and write
        assert!(engine
            .authorize(&ops, ChronixAction::Read, &resource)
            .is_allowed());
        assert!(engine
            .authorize(&ops, ChronixAction::Write, &resource)
            .is_allowed());
        // ops cannot delete or admin
        assert!(engine
            .authorize(&ops, ChronixAction::Delete, &resource)
            .is_denied());
        assert!(engine
            .authorize(&ops, ChronixAction::Admin, &resource)
            .is_denied());
        // admin can do everything
        assert!(engine
            .authorize(&admin, ChronixAction::Delete, &resource)
            .is_allowed());
        assert!(engine
            .authorize(&admin, ChronixAction::Forecast, &resource)
            .is_allowed());
    }

    #[test]
    fn forbid_policy_overrides_permit() {
        let policies = r#"
            permit(principal, action == Chronix::Action::"Read", resource);
            forbid(principal, action == Chronix::Action::"Read", resource == Chronix::Measurement::"secret");
        "#;

        let engine = AuthzEngine::new();
        engine.load_policies(policies).unwrap();

        let user = ChronixPrincipal::new("alice");

        // Can read normal measurements
        assert!(engine
            .authorize(
                &user,
                ChronixAction::Read,
                &ChronixResource::measurement("cpu")
            )
            .is_allowed());
        // Cannot read secret measurement (forbid overrides permit)
        assert!(engine
            .authorize(
                &user,
                ChronixAction::Read,
                &ChronixResource::measurement("secret")
            )
            .is_denied());
    }

    #[test]
    fn analytics_authorization() {
        let policies = r#"
            permit(
                principal in Chronix::Role::"data-science",
                action == Chronix::Action::"Forecast",
                resource
            );
            permit(
                principal in Chronix::Role::"data-science",
                action == Chronix::Action::"DetectAnomalies",
                resource
            );
            permit(
                principal in Chronix::Role::"ops",
                action == Chronix::Action::"Read",
                resource
            );
        "#;

        let engine = AuthzEngine::new();
        engine.load_policies(policies).unwrap();

        let ds = ChronixPrincipal::new("data-alice").with_role("data-science");
        let ops = ChronixPrincipal::new("ops-bob").with_role("ops");
        let resource = ChronixResource::measurement("energy");

        // data-science can forecast and detect anomalies
        assert!(engine
            .authorize(&ds, ChronixAction::Forecast, &resource)
            .is_allowed());
        assert!(engine
            .authorize(&ds, ChronixAction::DetectAnomalies, &resource)
            .is_allowed());
        // data-science cannot write
        assert!(engine
            .authorize(&ds, ChronixAction::Write, &resource)
            .is_denied());

        // ops can read but not forecast
        assert!(engine
            .authorize(&ops, ChronixAction::Read, &resource)
            .is_allowed());
        assert!(engine
            .authorize(&ops, ChronixAction::Forecast, &resource)
            .is_denied());
    }

    #[test]
    fn subscribe_action_authorization() {
        let policies = r#"
            permit(
                principal in Chronix::Role::"subscriber",
                action == Chronix::Action::"Subscribe",
                resource
            );
        "#;

        let engine = AuthzEngine::new();
        engine.load_policies(policies).unwrap();

        let sub = ChronixPrincipal::new("stream-user").with_role("subscriber");
        let non_sub = ChronixPrincipal::new("basic-user");
        let resource = ChronixResource::measurement("cpu");

        assert!(engine
            .authorize(&sub, ChronixAction::Subscribe, &resource)
            .is_allowed());
        assert!(engine
            .authorize(&non_sub, ChronixAction::Subscribe, &resource)
            .is_denied());
    }

    #[test]
    fn decision_deny_includes_reasons() {
        let engine = AuthzEngine::new();
        let decision = engine.authorize(
            &ChronixPrincipal::new("nobody"),
            ChronixAction::Write,
            &ChronixResource::measurement("cpu"),
        );
        match decision {
            Decision::Deny { reasons } => {
                assert!(!reasons.is_empty());
                // Only a generic message should be exposed — no policy IDs
                assert!(reasons[0].contains("access denied"));
            }
            Decision::Allow => panic!("Expected deny"),
        }
    }

    #[test]
    fn namespace_scoped_measurement_policy() {
        // Policy: allow reads on any resource that is *in* the "prod" namespace.
        let policies = r#"
            permit(
                principal in Chronix::Role::"reader",
                action == Chronix::Action::"Read",
                resource in Chronix::Namespace::"prod"
            );
        "#;

        let engine = AuthzEngine::new();
        engine.load_policies(policies).unwrap();

        let reader = ChronixPrincipal::new("alice").with_role("reader");

        // Measurement in "prod" namespace → allowed
        let cpu_prod = ChronixResource::measurement("cpu").with_namespace("prod");
        assert!(
            engine
                .authorize(&reader, ChronixAction::Read, &cpu_prod)
                .is_allowed(),
            "measurement in prod namespace should be readable"
        );

        // Measurement in "staging" namespace → denied
        let cpu_staging = ChronixResource::measurement("cpu").with_namespace("staging");
        assert!(
            engine
                .authorize(&reader, ChronixAction::Read, &cpu_staging)
                .is_denied(),
            "measurement in staging namespace should be denied"
        );

        // Measurement with no namespace → denied (not in any namespace)
        let cpu_none = ChronixResource::measurement("cpu");
        assert!(
            engine
                .authorize(&reader, ChronixAction::Read, &cpu_none)
                .is_denied(),
            "measurement without namespace should be denied"
        );

        // Write is not permitted even in prod
        assert!(
            engine
                .authorize(&reader, ChronixAction::Write, &cpu_prod)
                .is_denied(),
            "write should be denied even in prod namespace"
        );
    }

    // ── Namespace authorization tests ────────────────────────────────

    #[test]
    fn namespace_default_deny() {
        let engine = AuthzEngine::new();
        let user = ChronixPrincipal::new("alice");
        let ns = ChronixNamespace::new("production");
        let decision = engine.authorize_namespace(&user, ChronixAction::Read, &ns);
        assert!(decision.is_denied());
    }

    #[test]
    fn namespace_scoped_permit() {
        let policies = r#"
            permit(
                principal in Chronix::Role::"ns_admin",
                action,
                resource == Chronix::Namespace::"production"
            );
        "#;

        let engine = AuthzEngine::new();
        engine.load_policies(policies).unwrap();

        let ns_admin = ChronixPrincipal::new("alice").with_role("ns_admin");
        let production = ChronixNamespace::new("production");
        let staging = ChronixNamespace::new("staging");

        // ns_admin can access "production"
        assert!(engine
            .authorize_namespace(&ns_admin, ChronixAction::Read, &production)
            .is_allowed());
        assert!(engine
            .authorize_namespace(&ns_admin, ChronixAction::Write, &production)
            .is_allowed());
        // ns_admin cannot access other namespaces
        assert!(engine
            .authorize_namespace(&ns_admin, ChronixAction::Read, &staging)
            .is_denied());
    }

    #[test]
    fn namespace_forbid_cross_namespace() {
        let policies = r#"
            permit(
                principal in Chronix::Role::"team_a",
                action,
                resource == Chronix::Namespace::"team_a_ns"
            );
            forbid(
                principal in Chronix::Role::"team_a",
                action,
                resource == Chronix::Namespace::"team_b_ns"
            );
        "#;

        let engine = AuthzEngine::new();
        engine.load_policies(policies).unwrap();

        let team_a = ChronixPrincipal::new("dev1").with_role("team_a");
        let ns_a = ChronixNamespace::new("team_a_ns");
        let ns_b = ChronixNamespace::new("team_b_ns");

        assert!(engine
            .authorize_namespace(&team_a, ChronixAction::Write, &ns_a)
            .is_allowed());
        assert!(engine
            .authorize_namespace(&team_a, ChronixAction::Read, &ns_b)
            .is_denied());
    }

    /// Cross-tenant isolation policy can be loaded and denies
    /// cross-namespace access.
    #[test]
    fn cross_tenant_isolation_policy_loads() {
        let engine = AuthzEngine::new();
        engine.load_cross_tenant_isolation_policy().unwrap();
        assert!(engine
            .policy_ids()
            .contains(&"chronix-builtin-cross-tenant-deny".to_string()));
    }

    /// Cross-tenant isolation policy is idempotent.
    #[test]
    fn cross_tenant_isolation_policy_idempotent() {
        let engine = AuthzEngine::new();
        engine.load_cross_tenant_isolation_policy().unwrap();
        engine.load_cross_tenant_isolation_policy().unwrap();
        // Still only 1 policy (replaced)
        assert_eq!(engine.policy_count(), 1);
    }

    /// Full cross-tenant integration test.
    ///
    /// Creates two namespaces with independent principals and verifies that
    /// the Cedar cross-tenant isolation policy prevents any principal from
    /// accessing resources in another namespace — for all actions.
    #[test]
    fn cross_tenant_full_isolation_integration() {
        let engine = AuthzEngine::new();
        // Use add_policy (not load_policies) to preserve the auto-loaded
        // cross-tenant isolation forbid policy.
        engine
            .add_policy(
                "team_a_permit",
                r#"permit(principal in Chronix::Role::"team_a_dev", action, resource);"#,
            )
            .unwrap();
        engine
            .add_policy(
                "team_b_permit",
                r#"permit(principal in Chronix::Role::"team_b_dev", action, resource);"#,
            )
            .unwrap();

        // Team A principal in namespace "team_a"
        let alice = ChronixPrincipal::new("alice")
            .with_role("team_a_dev")
            .with_namespace("team_a");

        // Team B principal in namespace "team_b"
        let bob = ChronixPrincipal::new("bob")
            .with_role("team_b_dev")
            .with_namespace("team_b");

        // Resources in each namespace
        let cpu_a = ChronixResource::measurement("cpu").with_namespace("team_a");
        let cpu_b = ChronixResource::measurement("cpu").with_namespace("team_b");
        let mem_a = ChronixResource::measurement("memory").with_namespace("team_a");
        let mem_b = ChronixResource::measurement("memory").with_namespace("team_b");

        // ── Same-namespace access: ALLOWED ──
        for action in [
            ChronixAction::Read,
            ChronixAction::Write,
            ChronixAction::Delete,
        ] {
            assert!(
                engine.authorize(&alice, action, &cpu_a).is_allowed(),
                "Alice should access team_a resources ({action:?})"
            );
            assert!(
                engine.authorize(&bob, action, &cpu_b).is_allowed(),
                "Bob should access team_b resources ({action:?})"
            );
        }

        // ── Cross-namespace access: DENIED ──
        for action in [
            ChronixAction::Read,
            ChronixAction::Write,
            ChronixAction::Delete,
        ] {
            assert!(
                engine.authorize(&alice, action, &cpu_b).is_denied(),
                "Alice must NOT access team_b cpu ({action:?})"
            );
            assert!(
                engine.authorize(&alice, action, &mem_b).is_denied(),
                "Alice must NOT access team_b memory ({action:?})"
            );
            assert!(
                engine.authorize(&bob, action, &cpu_a).is_denied(),
                "Bob must NOT access team_a cpu ({action:?})"
            );
            assert!(
                engine.authorize(&bob, action, &mem_a).is_denied(),
                "Bob must NOT access team_a memory ({action:?})"
            );
        }

        // ── Admin override: admin principal CAN cross namespaces ──
        let admin = ChronixPrincipal::new("superadmin")
            .with_role("admin")
            .with_namespace("team_a");
        // Need an admin-allow policy
        engine
            .add_policy(
                "admin_allow",
                r#"permit(principal in Chronix::Role::"admin", action, resource);"#,
            )
            .unwrap();
        assert!(
            engine
                .authorize(&admin, ChronixAction::Read, &cpu_b)
                .is_allowed(),
            "Admin should cross namespace boundaries"
        );
    }

    /// warn_if_no_namespace_policies doesn't panic.
    #[test]
    fn warn_if_no_namespace_policies_works() {
        let engine = AuthzEngine::new();
        // Should log warning but not panic
        engine.warn_if_no_namespace_policies();

        // After loading cross-tenant policy, no warning
        engine.load_cross_tenant_isolation_policy().unwrap();
        engine.warn_if_no_namespace_policies();
    }

    // ── Policy Versioning & Rollback ─────────────────────────

    #[test]
    fn version_increments_on_load() {
        let engine = AuthzEngine::new();
        // Version starts at 1 after auto-loading isolation policy
        let base = engine.current_version();

        engine.load_policies(read_only_policy()).unwrap();
        assert_eq!(engine.current_version(), base + 1);

        engine.load_policies(admin_allow_all_policy()).unwrap();
        assert_eq!(engine.current_version(), base + 2);
    }

    #[test]
    fn version_increments_on_add_and_remove() {
        let engine = AuthzEngine::new();
        let base = engine.current_version();
        engine
            .add_policy(
                "p1",
                r#"permit(principal, action == Chronix::Action::"Read", resource);"#,
            )
            .unwrap();
        assert_eq!(engine.current_version(), base + 1);

        assert!(engine.remove_policy("p1"));
        assert_eq!(engine.current_version(), base + 2);
    }

    #[test]
    fn history_contains_previous_versions() {
        let engine = AuthzEngine::new();
        let base = engine.current_version();
        engine.load_policies(read_only_policy()).unwrap();
        engine.load_policies(admin_allow_all_policy()).unwrap();

        let versions = engine.policy_versions();
        // History contains entries from auto-loaded policy + 2 loads
        assert!(versions.len() >= 2);
        // The last two entries should be the ones from our loads
        let last_two = &versions[versions.len() - 2..];
        assert_eq!(last_two[0].version, base);
        assert_eq!(last_two[1].version, base + 1);
    }

    #[test]
    fn revert_to_previous_version() {
        let engine = AuthzEngine::new();
        let base = engine.current_version();
        engine.load_policies(read_only_policy()).unwrap();
        engine.load_policies(admin_allow_all_policy()).unwrap();

        let user = ChronixPrincipal::new("alice");
        let resource = ChronixResource::measurement("cpu");
        // Currently admin_allow_all — non-admin denied
        assert!(engine
            .authorize(&user, ChronixAction::Read, &resource)
            .is_denied());

        // Revert to the read_only_policy version
        engine.revert_to_version(base + 1).unwrap();
        assert!(engine
            .authorize(&user, ChronixAction::Read, &resource)
            .is_allowed());
        assert!(engine
            .authorize(&user, ChronixAction::Write, &resource)
            .is_denied());
    }

    #[test]
    fn revert_to_nonexistent_version_fails() {
        let engine = AuthzEngine::new();
        engine.load_policies(read_only_policy()).unwrap();
        let err = engine.revert_to_version(999);
        assert!(err.is_err());
        let msg = format!("{}", err.unwrap_err());
        assert!(msg.contains("999"));
    }

    #[test]
    fn revert_itself_creates_history_entry() {
        let engine = AuthzEngine::new();
        let base = engine.current_version();
        engine.load_policies(read_only_policy()).unwrap();
        engine.load_policies(admin_allow_all_policy()).unwrap();
        engine.revert_to_version(base + 1).unwrap();

        let versions = engine.policy_versions();
        // Should have at least 3 entries from our operations
        assert!(versions.len() >= 3);

        // Can revert back to admin_allow_all version
        engine.revert_to_version(base + 2).unwrap();
        let admin = ChronixPrincipal::new("alice").with_role("admin");
        let resource = ChronixResource::measurement("cpu");
        assert!(engine
            .authorize(&admin, ChronixAction::Write, &resource)
            .is_allowed());
    }

    #[test]
    fn history_ring_buffer_bounded() {
        let engine = AuthzEngine::new();
        // Load 15 times — history should only retain MAX_POLICY_VERSIONS
        for _ in 0..15 {
            engine.load_policies(read_only_policy()).unwrap();
        }
        let versions = engine.policy_versions();
        assert!(versions.len() <= super::MAX_POLICY_VERSIONS);
    }

    #[test]
    fn dryrun_does_not_modify_active_set() {
        let engine = AuthzEngine::new();
        engine.load_policies(read_only_policy()).unwrap();
        let v_before = engine.current_version();
        let count_before = engine.policy_count();

        let dryrun_count = engine
            .dryrun_load_policies(admin_allow_all_policy())
            .unwrap();
        assert!(dryrun_count > 0);

        // Version and active set unchanged
        assert_eq!(engine.current_version(), v_before);
        assert_eq!(engine.policy_count(), count_before);

        // Authorization still uses old policy
        let user = ChronixPrincipal::new("alice");
        let resource = ChronixResource::measurement("cpu");
        assert!(engine
            .authorize(&user, ChronixAction::Read, &resource)
            .is_allowed());
    }

    #[test]
    fn dryrun_rejects_invalid_policy() {
        let engine = AuthzEngine::new();
        let result = engine.dryrun_load_policies("not valid cedar syntax {{}}");
        assert!(result.is_err());
    }

    #[test]
    fn policy_version_has_timestamp() {
        let engine = AuthzEngine::new();
        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        engine.load_policies(read_only_policy()).unwrap();

        let versions = engine.policy_versions();
        // At least 1 entry from auto-loaded policy + our load
        assert!(!versions.is_empty());
        // The latest entry should be from our load
        let latest = versions.last().unwrap();
        assert!(latest.timestamp >= before);
    }

    // ── granular admin action tests ─────────────────────────

    #[test]
    fn ent06_admin_super_action_covers_granular() {
        let policies = r#"
            permit(
                principal in Chronix::Role::"admin",
                action == Chronix::Action::"Admin",
                resource
            );
        "#;
        let engine = AuthzEngine::new();
        engine.load_policies(policies).unwrap();

        let admin = ChronixPrincipal::new("alice").with_role("admin");
        let resource = ChronixResource::measurement("cpu");

        // Admin super-action is allowed.
        assert!(engine
            .authorize_admin(&admin, ChronixAction::Admin, &resource)
            .is_allowed());
        // All granular admin actions are covered via fallback.
        assert!(engine
            .authorize_admin(&admin, ChronixAction::ManageNodes, &resource)
            .is_allowed());
        assert!(engine
            .authorize_admin(&admin, ChronixAction::ManageKeys, &resource)
            .is_allowed());
        assert!(engine
            .authorize_admin(&admin, ChronixAction::ManageBackups, &resource)
            .is_allowed());
        assert!(engine
            .authorize_admin(&admin, ChronixAction::ManageModels, &resource)
            .is_allowed());
        assert!(engine
            .authorize_admin(&admin, ChronixAction::ViewCluster, &resource)
            .is_allowed());
    }

    #[test]
    fn ent06_granular_action_without_admin_super() {
        // Grant only ManageModels — must NOT grant other admin ops.
        let policies = r#"
            permit(
                principal in Chronix::Role::"ml-eng",
                action == Chronix::Action::"ManageModels",
                resource
            );
        "#;
        let engine = AuthzEngine::new();
        engine.load_policies(policies).unwrap();

        let ml_eng = ChronixPrincipal::new("bob").with_role("ml-eng");
        let resource = ChronixResource::measurement("cpu");

        assert!(engine
            .authorize_admin(&ml_eng, ChronixAction::ManageModels, &resource)
            .is_allowed());
        // Must NOT have other admin capabilities.
        assert!(engine
            .authorize_admin(&ml_eng, ChronixAction::ManageKeys, &resource)
            .is_denied());
        assert!(engine
            .authorize_admin(&ml_eng, ChronixAction::ManageBackups, &resource)
            .is_denied());
        assert!(engine
            .authorize_admin(&ml_eng, ChronixAction::Admin, &resource)
            .is_denied());
    }

    #[test]
    fn ent06_is_admin_action_classification() {
        assert!(ChronixAction::Admin.is_admin_action());
        assert!(ChronixAction::ManageNodes.is_admin_action());
        assert!(ChronixAction::ManageKeys.is_admin_action());
        assert!(ChronixAction::ManageNamespaces.is_admin_action());
        // Non-admin actions should return false.
        assert!(!ChronixAction::Read.is_admin_action());
        assert!(!ChronixAction::Write.is_admin_action());
        assert!(!ChronixAction::Subscribe.is_admin_action());
    }
}
