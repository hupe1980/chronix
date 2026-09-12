//! Recording security-relevant events in the audit trail.
//!
//! The trail answers one question after the fact: *who did what, and was it
//! allowed?* That makes the interesting events the ones nobody watches in
//! normal operation — a refused credential, a delete, an admin action — so
//! they are recorded at the point where the decision is actually made
//! rather than inferred later from request logs.

use chronix_security::audit::{AuditAction, AuditDecision, AuditEvent};

use crate::http::SharedState;

/// Record one event, if an audit logger is configured.
///
/// Takes the principal, what was attempted, what it was attempted on, and
/// how it was decided. Metadata is free-form and goes into the sealed
/// payload, so it is covered by the hash chain like everything else.
///
/// `&SharedState` rather than `&AppState`: a caller holding either can pass
/// it, because `AppState` is an `Arc` and derefs — and the gates that need to
/// record a *refusal* hold the inner reference.
pub fn record(
    state: &SharedState,
    principal: &str,
    action: AuditAction,
    resource: impl Into<String>,
    decision: AuditDecision,
    metadata: &[(&str, String)],
) {
    let Some(logger) = state.audit_logger.as_ref() else {
        return;
    };
    let mut event = AuditEvent::new(principal, action, resource, decision);
    for (key, value) in metadata {
        event = event.with_metadata(*key, value.clone());
    }
    logger.log(event);
}

/// The principal on a request, or `"anonymous"` when it carries none.
///
/// An unauthenticated attempt is still worth recording — a refusal with no
/// principal is the shape a credential-stuffing run leaves behind.
#[must_use]
pub fn principal_of(extensions: &axum::http::Extensions) -> String {
    extensions
        .get::<chronix_security::auth::AuthContext>()
        .map_or_else(|| "anonymous".to_string(), |c| c.principal.clone())
}
