//! The Cedar policy engine, and the two questions `chronixd` asks it.
//!
//! * **May this principal do this to this namespace?** — the data plane. The
//!   namespace gate every HTTP, gRPC and Flight SQL request passes through
//!   asks it with `Read`, `Write` or `Delete`.
//! * **May this principal do this to the server?** — the control plane. Each
//!   administrative route group asks it with its own capability.
//!
//! There is no third question, and the model in [`super::model`] has no third
//! resource. A policy language whose vocabulary is wider than the set of
//! questions asked is one that accepts rules nothing ever consults.
//!
//! # The schema is not optional
//!
//! [`SCHEMA_SRC`] is compiled in and loaded by [`AuthzEngine::new`], so every
//! policy is validated before it can be installed: an unknown action, a
//! misspelt entity type, an administrative action aimed at a namespace are
//! all load-time errors. Without it Cedar accepts anything syntactically
//! well-formed, and a `permit` that names something the server never asks
//! about is indistinguishable from a lockout while a `forbid` that does is
//! indistinguishable from protection.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::LazyLock;

use cedar_policy::{
    Authorizer, Context, Entities, Entity, EntityUid, PolicySet, Request, Schema, ValidationMode,
    Validator,
};
use metrics::counter;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::authz::error::{AuthzError, Result};
use crate::authz::model::{
    ChronixAction, ChronixNamespace, ChronixPrincipal, ChronixSystem, Decision,
};

/// The Chronix Cedar schema, compiled into the binary.
///
/// Published as well as used: an operator writing policies needs it, and a
/// copy in the documentation is a copy that drifts, so the security guide
/// points at this constant and the site renders it.
pub const SCHEMA_SRC: &str = include_str!("chronix.cedarschema");

/// The parsed schema. Parsed once; a failure here is a build-time mistake in
/// a compiled-in constant, which `the_builtin_schema_parses` catches.
static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    let (schema, warnings) = Schema::from_cedarschema_str(SCHEMA_SRC)
        .expect("the compiled-in Chronix schema must parse");
    for w in warnings {
        warn!(warning = %w, "Chronix Cedar schema warning");
    }
    schema
});

/// The Cedar-based authorization engine.
///
/// Thread-safe — share it as `Arc<AuthzEngine>`.
///
/// # Default-deny
///
/// A request no policy permits is denied. An engine with no policies at all
/// therefore denies everything, which is why `chronixd` treats "no policy
/// directory configured" as "no policy engine" rather than as an empty one.
///
/// # Cross-tenant isolation is not a policy
///
/// It is a property of the credential: a key or token carries the namespaces
/// it may act in, and the gate refuses a header naming any other *before*
/// Cedar is consulted. There used to be a built-in `forbid` policy for this
/// which nothing loaded and which compared a `principal.namespace` attribute
/// nothing set — an isolation guarantee that existed only as a string
/// constant. Policies narrow what a credential may do; they are not what
/// keeps tenants apart.
pub struct AuthzEngine {
    /// The Cedar authorizer (stateless, thread-safe).
    authorizer: Authorizer,
    /// The active policy set, behind an `Arc` so `authorize` can snapshot the
    /// pointer under a brief read lock and evaluate without holding it.
    policies: RwLock<Arc<PolicySet>>,
}

impl AuthzEngine {
    /// Create an engine with no policies and the built-in schema.
    #[must_use]
    pub fn new() -> Self {
        Self {
            authorizer: Authorizer::new(),
            policies: RwLock::new(Arc::new(PolicySet::new())),
        }
    }

    /// Validate a candidate policy set against the schema.
    ///
    /// Warnings are surfaced as errors for one class only — a policy Cedar
    /// can prove never matches — because that is precisely the failure this
    /// whole mechanism exists to catch, and it is reported as a warning
    /// rather than an error.
    fn validate(ps: &PolicySet) -> Result<()> {
        let validator = Validator::new(SCHEMA.clone());
        let result = validator.validate(ps, ValidationMode::Strict);
        let errors: Vec<String> = result.validation_errors().map(|e| format!("{e}")).collect();
        if !errors.is_empty() {
            return Err(AuthzError::PolicyValidation(errors.join("; ")));
        }
        let mut never_matches: Vec<String> = Vec::new();
        for w in result.validation_warnings() {
            let text = format!("{w}");
            if text.contains("impossible") || text.contains("will never") {
                never_matches.push(text);
            } else {
                warn!(warning = %text, "Cedar policy validation warning");
            }
        }
        if !never_matches.is_empty() {
            return Err(AuthzError::PolicyValidation(never_matches.join("; ")));
        }
        Ok(())
    }

    /// Parse and validate `policy_src` without installing it.
    ///
    /// # Errors
    ///
    /// [`AuthzError::PolicyParse`] if it is not Cedar,
    /// [`AuthzError::PolicyValidation`] if it does not fit the schema.
    pub fn check_policies(policy_src: &str) -> Result<usize> {
        let ps = policy_src
            .parse::<PolicySet>()
            .map_err(|e| AuthzError::PolicyParse(format!("{e}")))?;
        Self::validate(&ps)?;
        Ok(ps.policies().count())
    }

    /// Replace the active policy set.
    ///
    /// # Errors
    ///
    /// As [`check_policies`](Self::check_policies); the active set is left
    /// untouched when either fails.
    pub fn load_policies(&self, policy_src: &str) -> Result<usize> {
        let ps = policy_src
            .parse::<PolicySet>()
            .map_err(|e| AuthzError::PolicyParse(format!("{e}")))?;
        Self::validate(&ps)?;
        let count = ps.policies().count();
        let policy_ids: Vec<String> = ps.policies().map(|p| p.id().to_string()).collect();
        *self.policies.write() = Arc::new(ps);
        info!(count, policies = ?policy_ids, "Cedar policies loaded");
        counter!("chronix_authz_policy_load", "operation" => "load_all").increment(1);
        Ok(count)
    }

    /// Add one policy, keeping the rest.
    ///
    /// Clone-and-swap: the candidate is built and validated without a lock
    /// held, and the write lock is taken only for the swap, so a rejected
    /// policy leaves the active set exactly as it was.
    ///
    /// # Errors
    ///
    /// As [`check_policies`](Self::check_policies), or
    /// [`AuthzError::Internal`] if `id` is already taken.
    pub fn add_policy(&self, id: &str, policy_src: &str) -> Result<()> {
        let policy = cedar_policy::Policy::parse(Some(cedar_policy::PolicyId::new(id)), policy_src)
            .map_err(|e| AuthzError::PolicyParse(format!("{e}")))?;

        let mut candidate = {
            let ps = self.policies.read();
            let mut candidate = PolicySet::new();
            for p in ps.policies() {
                candidate
                    .add(p.clone())
                    .map_err(|e| AuthzError::Internal(format!("Duplicate policy: {e}")))?;
            }
            candidate
        };
        candidate
            .add(policy)
            .map_err(|e| AuthzError::Internal(format!("Failed to add policy: {e}")))?;
        Self::validate(&candidate)?;

        let total = candidate.policies().count();
        *self.policies.write() = Arc::new(candidate);
        info!(policy_id = id, total_policies = total, "Cedar policy added");
        counter!("chronix_authz_policy_load", "operation" => "add").increment(1);
        Ok(())
    }

    /// Remove a policy by id. Returns whether it was there.
    pub fn remove_policy(&self, id: &str) -> bool {
        let mut guard = self.policies.write();
        let mut ps = PolicySet::new();
        for p in guard.policies() {
            if let Err(e) = ps.add(p.clone()) {
                tracing::error!(error = %e, "failed to re-add policy while removing another");
            }
        }
        match ps.remove_static(cedar_policy::PolicyId::new(id)) {
            Ok(_) => {
                let remaining = ps.policies().count();
                *guard = Arc::new(ps);
                info!(
                    policy_id = id,
                    remaining_policies = remaining,
                    "Cedar policy removed"
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

    /// Load every `.cedar` file in `dir` as one policy set.
    ///
    /// # Errors
    ///
    /// The directory cannot be read, or the combined text does not parse or
    /// validate.
    pub fn load_policies_from_dir(&self, dir: &Path) -> Result<usize> {
        let mut combined = String::new();
        let mut file_count = 0;

        // Sorted, so a policy id collision between two files is reported the
        // same way on every machine. `read_dir` order is the filesystem's.
        let mut paths: Vec<_> = std::fs::read_dir(dir)?
            .filter_map(std::result::Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "cedar"))
            .collect();
        paths.sort();

        for path in paths {
            combined.push_str(&std::fs::read_to_string(&path)?);
            combined.push('\n');
            file_count += 1;
            debug!(path = %path.display(), "Loaded policy file");
        }

        if file_count == 0 {
            info!(dir = %dir.display(), "No .cedar files found");
            return Ok(0);
        }
        self.load_policies(&combined)
    }

    /// Number of active policies.
    #[must_use]
    pub fn policy_count(&self) -> usize {
        self.policies.read().policies().count()
    }

    /// The ids of the active policies.
    #[must_use]
    pub fn policy_ids(&self) -> Vec<String> {
        self.policies
            .read()
            .policies()
            .map(|p| p.id().to_string())
            .collect()
    }

    /// May `principal` perform `action` in `namespace`?
    ///
    /// `action` must be a data action; an administrative one cannot reach a
    /// namespace resource under the schema, so asking would be a decision
    /// nobody can write a policy for. Debug builds assert it.
    #[must_use]
    pub fn authorize_namespace(
        &self,
        principal: &ChronixPrincipal,
        action: ChronixAction,
        namespace: &ChronixNamespace,
    ) -> Decision {
        debug_assert!(
            !action.is_administrative(),
            "{action} applies to the system, not to a namespace"
        );
        match namespace.to_entity() {
            Ok(resource) => self.evaluate(
                principal,
                action,
                resource,
                namespace.to_entity_uid(),
                "namespace",
            ),
            Err(e) => Self::internal_deny(&e),
        }
    }

    /// May `principal` perform the administrative `action`?
    #[must_use]
    pub fn authorize_system(
        &self,
        principal: &ChronixPrincipal,
        action: ChronixAction,
    ) -> Decision {
        debug_assert!(
            action.is_administrative(),
            "{action} applies to a namespace, not to the system"
        );
        match ChronixSystem::to_entity() {
            Ok(resource) => self.evaluate(
                principal,
                action,
                resource,
                ChronixSystem::to_entity_uid(),
                "system",
            ),
            Err(e) => Self::internal_deny(&e),
        }
    }

    /// An entity the authorizer could not build is a denial, never a pass.
    fn internal_deny(e: &AuthzError) -> Decision {
        tracing::error!(error = %e, "Cedar entity construction failed");
        Decision::Deny {
            reasons: vec!["internal authorization error".into()],
        }
    }

    fn evaluate(
        &self,
        principal: &ChronixPrincipal,
        action: ChronixAction,
        resource: Entity,
        resource_uid: EntityUid,
        resource_label: &'static str,
    ) -> Decision {
        let principal_entity = match principal.to_entity() {
            Ok(e) => e,
            Err(e) => return Self::internal_deny(&e),
        };
        let role_entities = match principal.role_entities() {
            Ok(v) => v,
            Err(e) => return Self::internal_deny(&e),
        };

        let mut all = vec![principal_entity, resource];
        all.extend(role_entities);
        let entities = match Entities::from_entities(all, Some(&SCHEMA)) {
            Ok(e) => e,
            Err(e) => {
                return Self::internal_deny(&AuthzError::Internal(format!("entity set: {e}")))
            }
        };

        let request = match Request::new(
            principal.to_entity_uid(),
            action.to_entity_uid(),
            resource_uid,
            Context::empty(),
            Some(&SCHEMA),
        ) {
            Ok(r) => r,
            Err(e) => return Self::internal_deny(&AuthzError::Internal(format!("request: {e}"))),
        };

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
            "resource_type" => resource_label,
            "decision" => decision_label,
        )
        .increment(1);

        match response.decision() {
            cedar_policy::Decision::Allow => Decision::Allow,
            cedar_policy::Decision::Deny => {
                // Diagnostics name policy ids and evaluation internals, so
                // they go to the log and the caller gets "access denied".
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
                    debug!(
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

/// The action names the schema declares, group included.
///
/// Read out of the parsed schema rather than out of the text: the point of
/// the inventory check is that the two lists cannot drift, and a regex over
/// the source would drift with it.
///
/// # Errors
///
/// Returns [`AuthzError::Internal`] if Cedar cannot enumerate the actions.
pub fn schema_action_names() -> Result<HashSet<String>> {
    let entities = SCHEMA
        .action_entities()
        .map_err(|e| AuthzError::Internal(format!("schema actions: {e}")))?;
    Ok(entities
        .iter()
        .map(|e| e.uid().id().escaped().trim_matches('"').to_string())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alice(role: &str) -> ChronixPrincipal {
        ChronixPrincipal::new("alice").with_role(role)
    }

    fn prod() -> ChronixNamespace {
        ChronixNamespace::new("prod")
    }

    #[test]
    fn the_builtin_schema_parses() {
        // `SCHEMA` is a `LazyLock` that panics on a malformed schema, so
        // forcing it is the assertion.
        let _ = &*SCHEMA;
        assert!(!schema_action_names().unwrap().is_empty());
    }

    #[test]
    fn the_schema_describes_exactly_the_actions_we_issue() {
        // The two inventories drift in both directions and both are silent:
        // an action in the schema that nothing requests is a policy clause
        // that never fires, and an action requested but absent from the
        // schema fails validation on every policy that names it.
        let mut declared = schema_action_names().unwrap();
        // `Admin` is a group, deliberately never requested.
        assert!(declared.remove("Admin"), "the Admin group is gone");
        let issued: HashSet<String> = ChronixAction::ALL
            .iter()
            .map(|a| a.name().to_string())
            .collect();
        assert_eq!(declared, issued);
    }

    #[test]
    fn a_policy_naming_an_unknown_action_is_refused() {
        // The exact shape the security guide shipped: Prometheus-flavoured
        // action names, unqualified entity types, a `Group` that is a `Role`.
        for bad in [
            r#"permit(principal, action == Chronix::Action::"Query", resource);"#,
            r#"permit(principal, action == Action::"Write", resource);"#,
            r#"permit(principal in Group::"platform-team", action, resource);"#,
            r#"permit(principal, action, resource in Namespace::"team-platform");"#,
            r#"permit(principal, action == Chronix::Action::"CreateMeasurement", resource);"#,
        ] {
            assert!(
                AuthzEngine::check_policies(bad).is_err(),
                "should have been refused: {bad}"
            );
        }
    }

    #[test]
    fn an_admin_action_aimed_at_a_namespace_is_refused() {
        let bad = r#"
            permit(
              principal,
              action == Chronix::Action::"ManageKeys",
              resource == Chronix::Namespace::"prod"
            );
        "#;
        assert!(AuthzEngine::check_policies(bad).is_err());
    }

    #[test]
    fn default_deny() {
        let engine = AuthzEngine::new();
        assert!(engine
            .authorize_namespace(&alice("reader"), ChronixAction::Read, &prod())
            .is_denied());
        assert!(engine
            .authorize_system(&alice("reader"), ChronixAction::ManageKeys)
            .is_denied());
    }

    #[test]
    fn a_role_grants_within_one_namespace_only() {
        let engine = AuthzEngine::new();
        engine
            .load_policies(
                r#"
                permit(
                  principal in Chronix::Role::"prod_reader",
                  action == Chronix::Action::"Read",
                  resource == Chronix::Namespace::"prod"
                );
                "#,
            )
            .unwrap();
        let p = alice("prod_reader");
        assert!(engine
            .authorize_namespace(&p, ChronixAction::Read, &prod())
            .is_allowed());
        assert!(engine
            .authorize_namespace(&p, ChronixAction::Read, &ChronixNamespace::new("staging"))
            .is_denied());
        assert!(engine
            .authorize_namespace(&p, ChronixAction::Write, &prod())
            .is_denied());
    }

    #[test]
    fn a_principal_without_the_role_is_denied() {
        let engine = AuthzEngine::new();
        engine
            .load_policies(
                r#"permit(principal in Chronix::Role::"writer",
                          action == Chronix::Action::"Write", resource);"#,
            )
            .unwrap();
        assert!(engine
            .authorize_namespace(&alice("reader"), ChronixAction::Write, &prod())
            .is_denied());
        assert!(engine
            .authorize_namespace(&alice("writer"), ChronixAction::Write, &prod())
            .is_allowed());
    }

    #[test]
    fn the_admin_group_grants_every_capability_and_one_action_grants_one() {
        let engine = AuthzEngine::new();
        engine
            .load_policies(
                r#"
                permit(
                  principal in Chronix::Role::"root",
                  action in Chronix::Action::"Admin",
                  resource
                );
                permit(
                  principal in Chronix::Role::"backup_operator",
                  action == Chronix::Action::"ManageBackups",
                  resource
                );
                "#,
            )
            .unwrap();

        let root = alice("root");
        for action in ChronixAction::ALL
            .iter()
            .copied()
            .filter(|a| a.is_administrative())
        {
            assert!(
                engine.authorize_system(&root, action).is_allowed(),
                "the Admin group should cover {action}"
            );
        }

        // Least privilege is the point: the capability that was granted, and
        // nothing else. Every administrative route asked for `Admin` before
        // this, so a backup credential could mint API keys.
        let backup = alice("backup_operator");
        assert!(engine
            .authorize_system(&backup, ChronixAction::ManageBackups)
            .is_allowed());
        assert!(engine
            .authorize_system(&backup, ChronixAction::ManageKeys)
            .is_denied());
        assert!(engine
            .authorize_system(&backup, ChronixAction::ManageNamespaces)
            .is_denied());
    }

    #[test]
    fn a_forbid_overrides_a_permit() {
        let engine = AuthzEngine::new();
        engine
            .load_policies(
                r#"
                permit(principal, action == Chronix::Action::"Read", resource);
                forbid(principal, action, resource == Chronix::Namespace::"secret");
                "#,
            )
            .unwrap();
        assert!(engine
            .authorize_namespace(&alice("any"), ChronixAction::Read, &prod())
            .is_allowed());
        assert!(engine
            .authorize_namespace(
                &alice("any"),
                ChronixAction::Read,
                &ChronixNamespace::new("secret")
            )
            .is_denied());
    }

    #[test]
    fn a_principal_named_directly_needs_no_role() {
        let engine = AuthzEngine::new();
        engine
            .load_policies(
                r#"permit(principal == Chronix::User::"ingest-key",
                          action == Chronix::Action::"Write", resource);"#,
            )
            .unwrap();
        assert!(engine
            .authorize_namespace(
                &ChronixPrincipal::new("ingest-key"),
                ChronixAction::Write,
                &prod()
            )
            .is_allowed());
        assert!(engine
            .authorize_namespace(
                &ChronixPrincipal::new("other-key"),
                ChronixAction::Write,
                &prod()
            )
            .is_denied());
    }

    #[test]
    fn a_rejected_policy_leaves_the_active_set_alone() {
        let engine = AuthzEngine::new();
        engine
            .load_policies(r#"permit(principal, action == Chronix::Action::"Read", resource);"#)
            .unwrap();
        assert_eq!(engine.policy_count(), 1);
        assert!(engine
            .add_policy(
                "bad",
                r#"permit(principal, action == Chronix::Action::"Query", resource);"#
            )
            .is_err());
        assert_eq!(engine.policy_count(), 1);
        assert!(engine
            .authorize_namespace(&alice("any"), ChronixAction::Read, &prod())
            .is_allowed());
    }

    #[test]
    fn add_and_remove_a_policy() {
        let engine = AuthzEngine::new();
        engine
            .add_policy(
                "readers",
                r#"permit(principal, action == Chronix::Action::"Read", resource);"#,
            )
            .unwrap();
        assert_eq!(engine.policy_ids(), vec!["readers".to_string()]);
        assert!(engine.remove_policy("readers"));
        assert!(!engine.remove_policy("readers"));
        assert_eq!(engine.policy_count(), 0);
    }

    #[test]
    fn policies_load_from_a_directory_in_a_stable_order() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("b.cedar"),
            r#"permit(principal, action == Chronix::Action::"Write", resource);"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("a.cedar"),
            r#"permit(principal, action == Chronix::Action::"Read", resource);"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("notes.txt"), "ignored").unwrap();

        let engine = AuthzEngine::new();
        assert_eq!(engine.load_policies_from_dir(dir.path()).unwrap(), 2);
        assert!(engine
            .authorize_namespace(&alice("any"), ChronixAction::Read, &prod())
            .is_allowed());
    }

    #[test]
    fn an_empty_directory_loads_nothing_and_denies_everything() {
        let dir = tempfile::tempdir().unwrap();
        let engine = AuthzEngine::new();
        assert_eq!(engine.load_policies_from_dir(dir.path()).unwrap(), 0);
        assert!(engine
            .authorize_namespace(&alice("any"), ChronixAction::Read, &prod())
            .is_denied());
    }
}
