//! Per-namespace limits the database enforces at admission.
//!
//! A namespace is the value of a series' `__namespace__` tag. The database
//! admits every new series against its namespace's limits in the same step,
//! and from the same exact set of known series, as the database-wide
//! cardinality budget — so usage is never an estimate and never a second
//! counter beside the data. A whole-series delete, a dropped measurement and
//! a retention pass release it.
//!
//! The limits themselves are configuration the host pushes in
//! ([`Chronix::set_namespace_limits`](crate::Chronix::set_namespace_limits));
//! `chronixd` pushes every namespace's quota at startup and on every change.
//! A namespace without limits is bounded only by the database budget, and a
//! series with no namespace tag is never subject to namespace limits.

/// The limits the database enforces for one namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceLimits {
    /// Distinct series the namespace may hold.
    pub max_series: u64,
    /// Measurements the namespace may write to. A measurement counts until
    /// it is dropped; retention emptying it does not release it, because
    /// the measurement is still resolvable by the namespace's queries.
    pub max_measurements: u32,
}

/// A namespace's usage, exact, from the set admission compares against.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NamespaceUsage {
    /// Distinct series tagged with the namespace.
    pub series: u64,
    /// Measurements the namespace has written to and not dropped.
    pub measurements: u32,
}
