//! Chronix authorization model types.
//!
//! Defines the actions, resources, and principals used for Cedar policy
//! evaluation.
//!
//! Entity type names are parsed once and cached via `LazyLock` to avoid
//! per-request `from_str` + `expect()` overhead on the hot path.

use std::collections::{BTreeMap, HashMap, HashSet};
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

/// Pre-parsed entity type name for `Chronix::Measurement`.
static TYPE_MEASUREMENT: LazyLock<EntityTypeName> =
    LazyLock::new(|| EntityTypeName::from_str("Chronix::Measurement").expect("valid type"));

/// Pre-parsed entity type name for `Chronix::Namespace`.
static TYPE_NAMESPACE: LazyLock<EntityTypeName> =
    LazyLock::new(|| EntityTypeName::from_str("Chronix::Namespace").expect("valid type"));

/// Returns the cached `Chronix::Role` entity type name.
pub(crate) fn role_type_name() -> &'static EntityTypeName {
    &TYPE_ROLE
}

// ── Actions ─────────────────────────────────────────────────────────

/// Actions that can be authorized in Chronix.
///
/// The original `Admin` variant was a catch-all for all
/// administrative operations.  Granular admin actions (below) allow
/// policies to grant least-privilege access — e.g. granting
/// `ManageModels` without also granting `ManageKeys`.  The `Admin`
/// variant is retained as a super-action that implies all granular
/// admin actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChronixAction {
    /// Write data points.
    Write,
    /// Read / query data.
    Read,
    /// Delete data (series or measurement).
    Delete,
    /// Super-admin — implies all granular admin actions below.
    Admin,
    /// Create or manage rollups.
    CreateRollup,
    /// Execute forecast queries.
    Forecast,
    /// Execute anomaly detection.
    DetectAnomalies,
    /// Subscribe to CDC events.
    Subscribe,

    // ── granular admin actions ──────────────────────────────
    /// Manage cluster nodes (register, decommission, heartbeat).
    ManageNodes,
    /// Manage regions (create, state transitions).
    ManageRegions,
    /// View cluster topology and health (read-only admin).
    ViewCluster,
    /// Trigger cluster rebalancing.
    ManageCluster,
    /// Manage analytics models (list, delete, retrain).
    ManageModels,
    /// Manage chaos injection experiments.
    ManageChaos,
    /// Manage API keys (create, list, revoke).
    ManageKeys,
    /// Manage runtime configuration (e.g. log levels).
    ManageConfig,
    /// Manage backups and point-in-time restores.
    ManageBackups,
    /// Create, list, delete namespaces and view usage.
    ManageNamespaces,
}

impl ChronixAction {
    /// Convert to the Cedar action entity UID.
    #[must_use]
    pub fn to_entity_uid(&self) -> EntityUid {
        let name = match self {
            Self::Write => "Write",
            Self::Read => "Read",
            Self::Delete => "Delete",
            Self::Admin => "Admin",
            Self::CreateRollup => "CreateRollup",
            Self::Forecast => "Forecast",
            Self::DetectAnomalies => "DetectAnomalies",
            Self::Subscribe => "Subscribe",
            Self::ManageNodes => "ManageNodes",
            Self::ManageRegions => "ManageRegions",
            Self::ViewCluster => "ViewCluster",
            Self::ManageCluster => "ManageCluster",
            Self::ManageModels => "ManageModels",
            Self::ManageChaos => "ManageChaos",
            Self::ManageKeys => "ManageKeys",
            Self::ManageConfig => "ManageConfig",
            Self::ManageBackups => "ManageBackups",
            Self::ManageNamespaces => "ManageNamespaces",
        };
        EntityUid::from_type_name_and_id(TYPE_ACTION.clone(), EntityId::new(name))
    }

    /// Returns `true` if this is a granular admin action (or `Admin` itself).
    ///
    /// Useful for authorization engines that want to grant `Admin` as a
    /// super-action implying all granular admin sub-actions.
    #[must_use]
    pub fn is_admin_action(&self) -> bool {
        matches!(
            self,
            Self::Admin
                | Self::ManageNodes
                | Self::ManageRegions
                | Self::ViewCluster
                | Self::ManageCluster
                | Self::ManageModels
                | Self::ManageChaos
                | Self::ManageKeys
                | Self::ManageConfig
                | Self::ManageBackups
                | Self::ManageNamespaces
        )
    }
}

impl std::fmt::Display for ChronixAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Write => "Write",
            Self::Read => "Read",
            Self::Delete => "Delete",
            Self::Admin => "Admin",
            Self::CreateRollup => "CreateRollup",
            Self::Forecast => "Forecast",
            Self::DetectAnomalies => "DetectAnomalies",
            Self::Subscribe => "Subscribe",
            Self::ManageNodes => "ManageNodes",
            Self::ManageRegions => "ManageRegions",
            Self::ViewCluster => "ViewCluster",
            Self::ManageCluster => "ManageCluster",
            Self::ManageModels => "ManageModels",
            Self::ManageChaos => "ManageChaos",
            Self::ManageKeys => "ManageKeys",
            Self::ManageConfig => "ManageConfig",
            Self::ManageBackups => "ManageBackups",
            Self::ManageNamespaces => "ManageNamespaces",
        };
        write!(f, "{s}")
    }
}

// ── Resource ────────────────────────────────────────────────────────

/// A Chronix resource subject to authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChronixResource {
    /// Measurement name.
    pub measurement: String,
    /// Optional namespace the measurement belongs to.
    ///
    /// When set, the Cedar entity for this resource will have the
    /// corresponding `Chronix::Namespace` as a parent, enabling
    /// namespace-scoped policies such as:
    ///
    /// ```cedar
    /// permit(
    ///   principal in Chronix::Role::"ns_reader",
    ///   action == Chronix::Action::"Read",
    ///   resource in Chronix::Namespace::"production"
    /// );
    /// ```
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Optional tag constraints (for fine-grained access).
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

impl ChronixResource {
    /// Create a new resource for a measurement.
    #[must_use]
    pub fn measurement(name: impl Into<String>) -> Self {
        Self {
            measurement: name.into(),
            namespace: None,
            tags: BTreeMap::new(),
        }
    }

    /// Set the namespace this measurement belongs to.
    ///
    /// This causes the measurement entity to have the namespace as a
    /// Cedar parent, enabling `resource in Chronix::Namespace::"..."` policies.
    #[must_use]
    pub fn with_namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = Some(ns.into());
        self
    }

    /// Maximum number of tags allowed on a resource entity.
    /// Prevents Cedar entity attribute explosion on high-cardinality tags.
    const MAX_TAGS: usize = 64;

    /// Add a tag constraint.
    #[must_use]
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        if self.tags.len() < Self::MAX_TAGS {
            self.tags.insert(key.into(), value.into());
        } else {
            tracing::warn!(
                max = Self::MAX_TAGS,
                "tag limit reached on resource entity; \
                 additional tags are ignored for Cedar evaluation"
            );
        }
        self
    }

    /// Convert to a Cedar entity.
    ///
    /// If a namespace is set, the namespace entity is included as a
    /// parent so that `resource in Chronix::Namespace::"..."` policies
    /// evaluate correctly.
    pub fn to_entity(&self) -> Result<Entity, AuthzError> {
        let uid = EntityUid::from_type_name_and_id(
            TYPE_MEASUREMENT.clone(),
            EntityId::new(&self.measurement),
        );

        let mut attrs: HashMap<String, RestrictedExpression> = HashMap::new();
        attrs.insert(
            "name".into(),
            RestrictedExpression::new_string(self.measurement.clone()),
        );

        // Add tags as a record attribute
        if !self.tags.is_empty() {
            let tag_pairs: Vec<(String, RestrictedExpression)> = self
                .tags
                .iter()
                .map(|(k, v)| (k.clone(), RestrictedExpression::new_string(v.clone())))
                .collect();
            attrs.insert(
                "tags".into(),
                RestrictedExpression::new_record(tag_pairs)
                    .map_err(|e| AuthzError::Internal(format!("record construction: {e}")))?,
            );
        }

        // Add namespace as a parent entity for hierarchical policies.
        let mut parents = HashSet::new();
        if let Some(ns) = &self.namespace {
            attrs.insert(
                "namespace".into(),
                RestrictedExpression::new_string(ns.clone()),
            );
            parents.insert(EntityUid::from_type_name_and_id(
                TYPE_NAMESPACE.clone(),
                EntityId::new(ns),
            ));
        }

        Entity::new(uid, attrs, parents)
            .map_err(|e| AuthzError::Internal(format!("Measurement entity: {e}")))
    }

    /// Build the namespace parent entity (if set).
    ///
    /// Call this to include in the Cedar entity set so the authorizer
    /// can resolve the parent relationship.
    pub fn namespace_entity(&self) -> Option<Entity> {
        self.namespace
            .as_ref()
            .and_then(|ns| ChronixNamespace::new(ns).to_entity().ok())
    }

    /// Convert to the Cedar entity UID.
    #[must_use]
    pub fn to_entity_uid(&self) -> EntityUid {
        EntityUid::from_type_name_and_id(TYPE_MEASUREMENT.clone(), EntityId::new(&self.measurement))
    }
}

// ── Namespace resource ──────────────────────────────────────────────

/// A Chronix namespace subject to authorization.
///
/// Cedar policies can reference `Chronix::Namespace` entities with
/// a `name` attribute, enabling rules like:
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

// ── Principal ───────────────────────────────────────────────────────

/// A Chronix principal (authenticated identity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChronixPrincipal {
    /// The principal identifier (e.g., API key name, JWT subject, cert CN).
    pub id: String,
    /// Roles assigned to this principal.
    #[serde(default)]
    pub roles: Vec<String>,
    /// Namespace the principal belongs to (for cross-tenant isolation).
    /// When set, the Cedar cross-tenant isolation policy will forbid access
    /// to resources in a different namespace unless the principal is an admin.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

impl ChronixPrincipal {
    /// Create a new principal.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            roles: Vec::new(),
            namespace: None,
        }
    }

    /// Add a role to the principal.
    #[must_use]
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles.push(role.into());
        self
    }

    /// Set the namespace for this principal (for cross-tenant isolation).
    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Convert to a Cedar entity (includes role membership as parents).
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

        // Include namespace attribute so the cross-tenant isolation
        // Cedar policy (`principal has namespace`) can evaluate correctly.
        if let Some(ref ns) = self.namespace {
            attrs.insert(
                "namespace".into(),
                RestrictedExpression::new_string(ns.clone()),
            );
        }

        Entity::new(uid, attrs, parents)
            .map_err(|e| AuthzError::Internal(format!("User entity: {e}")))
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
    fn action_display() {
        assert_eq!(ChronixAction::Write.to_string(), "Write");
        assert_eq!(
            ChronixAction::DetectAnomalies.to_string(),
            "DetectAnomalies"
        );
    }

    #[test]
    fn action_to_entity_uid() {
        let uid = ChronixAction::Read.to_entity_uid();
        assert!(uid.to_string().contains("Read"));
    }

    #[test]
    fn resource_builder() {
        let res = ChronixResource::measurement("cpu").with_tag("host", "server-01");
        assert_eq!(res.measurement, "cpu");
        assert_eq!(res.tags.get("host").unwrap(), "server-01");
    }

    #[test]
    fn resource_to_entity() {
        let res = ChronixResource::measurement("cpu");
        let entity = res.to_entity().unwrap();
        assert!(entity.uid().to_string().contains("cpu"));
    }

    #[test]
    fn principal_builder() {
        let p = ChronixPrincipal::new("alice")
            .with_role("admin")
            .with_role("data-science");
        assert_eq!(p.id, "alice");
        assert_eq!(p.roles.len(), 2);
    }

    #[test]
    fn principal_to_entity() {
        let p = ChronixPrincipal::new("bob").with_role("reader");
        let entity = p.to_entity().unwrap();
        assert!(entity.uid().to_string().contains("bob"));
    }

    #[test]
    fn decision_is_allowed() {
        assert!(Decision::Allow.is_allowed());
        assert!(!Decision::Deny { reasons: vec![] }.is_allowed());
    }

    #[test]
    fn decision_is_denied() {
        assert!(Decision::Deny {
            reasons: vec!["no policy".into()]
        }
        .is_denied());
        assert!(!Decision::Allow.is_denied());
    }

    #[test]
    fn action_serde_roundtrip() {
        let action = ChronixAction::Forecast;
        let json = serde_json::to_string(&action).unwrap();
        let parsed: ChronixAction = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, action);
    }

    #[test]
    fn resource_serde_roundtrip() {
        let res = ChronixResource::measurement("cpu").with_tag("host", "s1");
        let json = serde_json::to_string(&res).unwrap();
        let parsed: ChronixResource = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, res);
    }

    #[test]
    fn principal_serde_roundtrip() {
        let p = ChronixPrincipal::new("alice").with_role("admin");
        let json = serde_json::to_string(&p).unwrap();
        let parsed: ChronixPrincipal = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, p);
    }

    #[test]
    fn namespace_builder() {
        let ns = ChronixNamespace::new("production");
        assert_eq!(ns.name, "production");
    }

    #[test]
    fn namespace_to_entity() {
        let ns = ChronixNamespace::new("staging");
        let entity = ns.to_entity().unwrap();
        assert!(entity.uid().to_string().contains("staging"));
    }

    #[test]
    fn namespace_serde_roundtrip() {
        let ns = ChronixNamespace::new("production");
        let json = serde_json::to_string(&ns).unwrap();
        let parsed: ChronixNamespace = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, ns);
    }

    #[test]
    fn resource_with_namespace_builder() {
        let res = ChronixResource::measurement("cpu")
            .with_namespace("production")
            .with_tag("host", "s1");
        assert_eq!(res.measurement, "cpu");
        assert_eq!(res.namespace.as_deref(), Some("production"));
        assert_eq!(res.tags.get("host").unwrap(), "s1");
    }

    #[test]
    fn resource_with_namespace_entity_has_parent() {
        let res = ChronixResource::measurement("cpu").with_namespace("prod");
        let entity = res.to_entity().unwrap();
        // Verify the entity UID is correct
        assert!(entity.uid().to_string().contains("cpu"));
        // Verify the namespace entity helper returns the right thing
        let ns = res
            .namespace_entity()
            .expect("should have namespace entity");
        assert!(ns.uid().to_string().contains("prod"));
        assert!(ns.uid().to_string().contains("Namespace"));
    }

    #[test]
    fn resource_without_namespace_has_no_parent() {
        let res = ChronixResource::measurement("cpu");
        let entity = res.to_entity().unwrap();
        assert!(entity.uid().to_string().contains("cpu"));
        assert!(res.namespace_entity().is_none());
    }

    #[test]
    fn resource_namespace_entity_helper() {
        let res = ChronixResource::measurement("cpu").with_namespace("staging");
        let ns_entity = res
            .namespace_entity()
            .expect("should produce namespace entity");
        assert!(ns_entity.uid().to_string().contains("staging"));

        let res_no_ns = ChronixResource::measurement("cpu");
        assert!(res_no_ns.namespace_entity().is_none());
    }

    #[test]
    fn resource_with_namespace_serde_roundtrip() {
        let res = ChronixResource::measurement("cpu")
            .with_namespace("prod")
            .with_tag("host", "s1");
        let json = serde_json::to_string(&res).unwrap();
        let parsed: ChronixResource = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, res);
        assert_eq!(parsed.namespace.as_deref(), Some("prod"));
    }
}
