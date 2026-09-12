//! Chronix authorization model types.
//!
//! Defines the actions, resources and principals used for Cedar policy
//! evaluation. Everything here has a producer in `chronixd` and a line in
//! [`chronix.cedarschema`](../chronix.cedarschema); nothing is modelled that
//! no request carries.
//!
//! That was not always true. The model once described measurements with tag
//! constraints, seventeen actions and a super-action, and `chronixd` issued
//! exactly two of them — so a policy written against a measurement, or
//! against `ManageModels`, was never consulted by anything. A model wider
//! than its enforcement reads as protection.
//!
//! Entity type names are parsed once and cached via `LazyLock` to avoid
//! per-request `from_str` + `expect()` overhead on the hot path.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::LazyLock;

use cedar_policy::{Entity, EntityId, EntityTypeName, EntityUid, RestrictedExpression};
use serde::{Deserialize, Serialize};

use crate::authz::error::AuthzError;

// ── Cached entity type names ────────────────────────────────────────

/// Pre-parsed entity type name for `Chronix::Action`.
static TYPE_ACTION: LazyLock<EntityTypeName> =
    LazyLock::new(|| EntityTypeName::from_str("Chronix::Action").expect("valid type"));

/// Pre-parsed entity type name for `Chronix::User`.
static TYPE_USER: LazyLock<EntityTypeName> =
    LazyLock::new(|| EntityTypeName::from_str("Chronix::User").expect("valid type"));

/// Pre-parsed entity type name for `Chronix::Role`.
static TYPE_ROLE: LazyLock<EntityTypeName> =
    LazyLock::new(|| EntityTypeName::from_str("Chronix::Role").expect("valid type"));

/// Pre-parsed entity type name for `Chronix::Namespace`.
static TYPE_NAMESPACE: LazyLock<EntityTypeName> =
    LazyLock::new(|| EntityTypeName::from_str("Chronix::Namespace").expect("valid type"));

/// Pre-parsed entity type name for `Chronix::System`.
static TYPE_SYSTEM: LazyLock<EntityTypeName> =
    LazyLock::new(|| EntityTypeName::from_str("Chronix::System").expect("valid type"));

// ── Actions ─────────────────────────────────────────────────────────

/// An action `chronixd` asks about.
///
/// Every variant is issued by a request path, and every action in the schema
/// is a variant here — the two lists are checked against each other by
/// `the_schema_describes_exactly_the_actions_we_issue`. An action nothing
/// issues is a policy clause that silently never fires, which is the whole
/// failure mode this enum exists to avoid.
///
/// `Admin` is **not** here on purpose. It is an action *group* in the schema:
/// the granular administrative actions are members of it, so
/// `action in Chronix::Action::"Admin"` grants all of them while
/// `action == Chronix::Action::"ManageBackups"` grants one. Nothing requests
/// the group, so nothing can be granted by naming it as an equality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChronixAction {
    /// Read data from a namespace (`GET`, `HEAD`, `OPTIONS`, and every query
    /// RPC).
    Read,
    /// Write data to a namespace, or change something in it.
    Write,
    /// Delete data from a namespace.
    Delete,

    // ── administrative actions, all members of the `Admin` group ────
    /// Create, list and revoke API keys.
    ManageKeys,
    /// Take, verify and restore backups.
    ManageBackups,
    /// Change runtime configuration, such as the log level.
    ManageConfig,
    /// List, inspect, delete and retrain analytics models.
    ManageModels,
    /// Create, delete and inspect namespaces.
    ManageNamespaces,
    /// Register, deregister and decommission cluster nodes.
    ManageNodes,
    /// Create regions and drive their state transitions.
    ManageRegions,
    /// Trigger cluster rebalancing.
    ManageCluster,
    /// Read cluster topology, routing and health.
    ViewCluster,
}

impl ChronixAction {
    /// Every action, in schema order. The inventory the schema is checked
    /// against.
    pub const ALL: &'static [Self] = &[
        Self::Read,
        Self::Write,
        Self::Delete,
        Self::ManageKeys,
        Self::ManageBackups,
        Self::ManageConfig,
        Self::ManageModels,
        Self::ManageNamespaces,
        Self::ManageNodes,
        Self::ManageRegions,
        Self::ManageCluster,
        Self::ViewCluster,
    ];

    /// The Cedar action name, which is also its `Display`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Read => "Read",
            Self::Write => "Write",
            Self::Delete => "Delete",
            Self::ManageKeys => "ManageKeys",
            Self::ManageBackups => "ManageBackups",
            Self::ManageConfig => "ManageConfig",
            Self::ManageModels => "ManageModels",
            Self::ManageNamespaces => "ManageNamespaces",
            Self::ManageNodes => "ManageNodes",
            Self::ManageRegions => "ManageRegions",
            Self::ManageCluster => "ManageCluster",
            Self::ViewCluster => "ViewCluster",
        }
    }

    /// Convert to the Cedar action entity UID.
    #[must_use]
    pub fn to_entity_uid(self) -> EntityUid {
        EntityUid::from_type_name_and_id(TYPE_ACTION.clone(), EntityId::new(self.name()))
    }

    /// Whether this action's resource is the server rather than a namespace.
    ///
    /// The split is the schema's: an administrative action `appliesTo`
    /// `System`, a data action `appliesTo` `Namespace`. Asking the wrong one
    /// is a validation error at policy-load time rather than a decision that
    /// silently never matches.
    #[must_use]
    pub const fn is_administrative(self) -> bool {
        !matches!(self, Self::Read | Self::Write | Self::Delete)
    }
}

impl std::fmt::Display for ChronixAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

// ── Namespace resource ──────────────────────────────────────────────

/// A Chronix namespace — the resource of every data-plane decision.
///
/// ```cedar
/// permit(
///   principal in Chronix::Role::"ns_admin",
///   action,
///   resource == Chronix::Namespace::"production"
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChronixNamespace {
    /// Namespace identifier.
    pub name: String,
}

impl ChronixNamespace {
    /// Create a namespace resource.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// Convert to a Cedar entity.
    ///
    /// # Errors
    ///
    /// Returns [`AuthzError::Internal`] if Cedar rejects the entity.
    pub fn to_entity(&self) -> Result<Entity, AuthzError> {
        let uid =
            EntityUid::from_type_name_and_id(TYPE_NAMESPACE.clone(), EntityId::new(&self.name));

        let mut attrs: HashMap<String, RestrictedExpression> = HashMap::new();
        attrs.insert(
            "name".into(),
            RestrictedExpression::new_string(self.name.clone()),
        );

        Entity::new(uid, attrs, HashSet::new())
            .map_err(|e| AuthzError::Internal(format!("Namespace entity: {e}")))
    }

    /// Convert to the Cedar entity UID.
    #[must_use]
    pub fn to_entity_uid(&self) -> EntityUid {
        EntityUid::from_type_name_and_id(TYPE_NAMESPACE.clone(), EntityId::new(&self.name))
    }
}

// ── System resource ─────────────────────────────────────────────────

/// The server itself — the resource of every administrative decision.
///
/// A singleton, so a policy names it as `Chronix::System::"chronix"` or
/// leaves `resource` unconstrained. It replaced a `Measurement` entity called
/// `"__system__"`, which validated against nothing and read as a measurement
/// somebody might also have data in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChronixSystem;

impl ChronixSystem {
    /// The singleton's entity id.
    pub const ID: &'static str = "chronix";

    /// Convert to a Cedar entity.
    ///
    /// # Errors
    ///
    /// Returns [`AuthzError::Internal`] if Cedar rejects the entity.
    pub fn to_entity() -> Result<Entity, AuthzError> {
        let mut attrs: HashMap<String, RestrictedExpression> = HashMap::new();
        attrs.insert(
            "name".into(),
            RestrictedExpression::new_string(Self::ID.into()),
        );
        Entity::new(Self::to_entity_uid(), attrs, HashSet::new())
            .map_err(|e| AuthzError::Internal(format!("System entity: {e}")))
    }

    /// Convert to the Cedar entity UID.
    #[must_use]
    pub fn to_entity_uid() -> EntityUid {
        EntityUid::from_type_name_and_id(TYPE_SYSTEM.clone(), EntityId::new(Self::ID))
    }
}

// ── Principal ───────────────────────────────────────────────────────

/// A Chronix principal (authenticated identity).
///
/// Built in one place — `chronixd`'s authentication middleware, from the
/// credential — so the roles a policy matches on are the same set whichever
/// gate asks. They used to be resolved inside the admin gate alone, so the
/// *same* role granted an administrative operation and nothing in a
/// namespace policy, and an API key had no roles at all.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChronixPrincipal {
    /// The principal identifier (an API key name, a JWT subject, a cert CN).
    pub id: String,
    /// Roles assigned to this principal, as `Chronix::Role` parents.
    #[serde(default)]
    pub roles: Vec<String>,
}

impl ChronixPrincipal {
    /// Create a new principal.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            roles: Vec::new(),
        }
    }

    /// Add a role to the principal.
    #[must_use]
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles.push(role.into());
        self
    }

    /// Add several roles.
    #[must_use]
    pub fn with_roles<I, S>(mut self, roles: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.roles.extend(roles.into_iter().map(Into::into));
        self
    }

    /// Convert to a Cedar entity (roles become parent entities).
    ///
    /// # Errors
    ///
    /// Returns [`AuthzError::Internal`] if Cedar rejects the entity.
    pub fn to_entity(&self) -> Result<Entity, AuthzError> {
        let uid = EntityUid::from_type_name_and_id(TYPE_USER.clone(), EntityId::new(&self.id));

        let parents: HashSet<EntityUid> = self
            .roles
            .iter()
            .map(|role| EntityUid::from_type_name_and_id(TYPE_ROLE.clone(), EntityId::new(role)))
            .collect();

        let mut attrs: HashMap<String, RestrictedExpression> = HashMap::new();
        attrs.insert(
            "name".into(),
            RestrictedExpression::new_string(self.id.clone()),
        );

        Entity::new(uid, attrs, parents)
            .map_err(|e| AuthzError::Internal(format!("User entity: {e}")))
    }

    /// The role entities this principal claims membership of.
    ///
    /// Cedar needs them in the entity set for `principal in
    /// Chronix::Role::"…"` to resolve; a role with no entity is a membership
    /// that silently does not hold.
    ///
    /// # Errors
    ///
    /// Returns [`AuthzError::Internal`] if Cedar rejects an entity.
    pub fn role_entities(&self) -> Result<Vec<Entity>, AuthzError> {
        self.roles
            .iter()
            .map(|role| {
                let uid = EntityUid::from_type_name_and_id(TYPE_ROLE.clone(), EntityId::new(role));
                Entity::new(uid, HashMap::new(), HashSet::new())
                    .map_err(|e| AuthzError::Internal(format!("Role entity: {e}")))
            })
            .collect()
    }

    /// Convert to the Cedar entity UID.
    #[must_use]
    pub fn to_entity_uid(&self) -> EntityUid {
        EntityUid::from_type_name_and_id(TYPE_USER.clone(), EntityId::new(&self.id))
    }
}

// ── Decision ────────────────────────────────────────────────────────

/// The result of an authorization decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decision {
    /// Access is allowed.
    Allow,
    /// Access is denied.
    Deny {
        /// Human-readable reasons for the denial.
        reasons: Vec<String>,
    },
}

impl Decision {
    /// Returns `true` if the decision is `Allow`.
    #[must_use]
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow)
    }

    /// Returns `true` if the decision is `Deny`.
    #[must_use]
    pub fn is_denied(&self) -> bool {
        !self.is_allowed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_display_is_the_cedar_name() {
        assert_eq!(ChronixAction::Write.to_string(), "Write");
        assert_eq!(ChronixAction::ManageBackups.to_string(), "ManageBackups");
    }

    #[test]
    fn action_to_entity_uid() {
        let uid = ChronixAction::Read.to_entity_uid();
        assert_eq!(uid.to_string(), r#"Chronix::Action::"Read""#);
    }

    #[test]
    fn data_actions_are_not_administrative() {
        for a in [
            ChronixAction::Read,
            ChronixAction::Write,
            ChronixAction::Delete,
        ] {
            assert!(!a.is_administrative(), "{a} should apply to a namespace");
        }
        for a in ChronixAction::ALL.iter().copied().filter(|a| {
            !matches!(
                a,
                ChronixAction::Read | ChronixAction::Write | ChronixAction::Delete
            )
        }) {
            assert!(a.is_administrative(), "{a} should apply to the system");
        }
    }

    #[test]
    fn namespace_to_entity() {
        let entity = ChronixNamespace::new("prod").to_entity().unwrap();
        assert_eq!(entity.uid().to_string(), r#"Chronix::Namespace::"prod""#);
    }

    #[test]
    fn system_is_a_singleton() {
        assert_eq!(
            ChronixSystem::to_entity_uid().to_string(),
            r#"Chronix::System::"chronix""#
        );
    }

    #[test]
    fn principal_roles_become_parents_and_entities() {
        let p = ChronixPrincipal::new("alice").with_roles(["admin", "data-science"]);
        assert_eq!(p.roles.len(), 2);
        let entity = p.to_entity().unwrap();
        assert_eq!(entity.uid().to_string(), r#"Chronix::User::"alice""#);
        assert_eq!(p.role_entities().unwrap().len(), 2);
    }

    #[test]
    fn decision_is_allowed_or_denied() {
        assert!(Decision::Allow.is_allowed());
        assert!(Decision::Deny { reasons: vec![] }.is_denied());
    }
}
