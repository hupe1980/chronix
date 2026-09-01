//! Property-based invariants for distributed system verification.
//!
//! Invariants are checked throughout a simulation run to detect safety
//! violations as soon as they occur, rather than only at the end.

use std::collections::HashMap;

/// Error returned when an invariant is violated.
#[derive(Debug, Clone, thiserror::Error)]
pub enum InvariantError {
    /// A safety invariant was violated.
    #[error("invariant violation: {name} — {detail}")]
    Violation {
        /// Name of the violated invariant.
        name: String,
        /// Human-readable description of the violation.
        detail: String,
    },
}

/// A checkable property of the distributed system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invariant {
    /// At most one leader per Raft term.
    LeaderUniqueness,
    /// A committed write must be visible to subsequent reads on any node.
    ReadYourWrites,
    /// No data loss — every acknowledged write must be durable.
    Durability,
    /// Quorum consistency — only the majority partition can make progress.
    QuorumSafety,
    /// Monotonic reads — once a value is seen, newer values may replace it
    /// but the old value must not reappear after a newer one.
    MonotonicReads,
}

/// State tracked during invariant checking.
#[derive(Debug, Default)]
pub struct InvariantChecker {
    /// Which invariants are enabled.
    enabled: Vec<Invariant>,
    /// Leader seen per term: term → node_id.
    leaders_by_term: HashMap<u64, u64>,
    /// Last value read per (client, key) — for monotonic reads.
    last_read: HashMap<(u64, String), String>,
    /// Acknowledged writes: key → value set.
    acked_writes: HashMap<String, Vec<String>>,
    /// Total violations found.
    violations: Vec<InvariantError>,
}

impl InvariantChecker {
    /// Create a checker with the given invariants enabled.
    #[must_use]
    pub fn new(invariants: Vec<Invariant>) -> Self {
        Self {
            enabled: invariants,
            ..Default::default()
        }
    }

    /// Create a checker with all invariants enabled.
    #[must_use]
    pub fn all() -> Self {
        Self::new(vec![
            Invariant::LeaderUniqueness,
            Invariant::ReadYourWrites,
            Invariant::Durability,
            Invariant::QuorumSafety,
            Invariant::MonotonicReads,
        ])
    }

    /// Record a leader observation: node `node_id` claims leadership in `term`.
    pub fn observe_leader(&mut self, term: u64, node_id: u64) {
        if !self.enabled.contains(&Invariant::LeaderUniqueness) {
            return;
        }

        if let Some(&existing) = self.leaders_by_term.get(&term) {
            if existing != node_id {
                self.violations.push(InvariantError::Violation {
                    name: "LeaderUniqueness".into(),
                    detail: format!(
                        "term {term}: node {existing} and node {node_id} both claim leadership"
                    ),
                });
            }
        } else {
            self.leaders_by_term.insert(term, node_id);
        }
    }

    /// Record an acknowledged write.
    pub fn ack_write(&mut self, key: &str, value: &str) {
        self.acked_writes
            .entry(key.to_owned())
            .or_default()
            .push(value.to_owned());
    }

    /// Record a read result and check monotonic reads invariant.
    pub fn observe_read(&mut self, client_id: u64, key: &str, value: Option<&str>) {
        if let Some(val) = value {
            if self.enabled.contains(&Invariant::MonotonicReads) {
                let rkey = (client_id, key.to_owned());
                if let Some(prev) = self.last_read.get(&rkey) {
                    // Check if we're reading an older value after seeing a newer one.
                    // In a register model, if values are versioned (write order),
                    // we check against the acked_writes ordering.
                    if let Some(writes) = self.acked_writes.get(key) {
                        let prev_idx = writes.iter().position(|w| w == prev);
                        let curr_idx = writes.iter().position(|w| w == val);
                        if let (Some(pi), Some(ci)) = (prev_idx, curr_idx) {
                            if ci < pi {
                                self.violations.push(InvariantError::Violation {
                                    name: "MonotonicReads".into(),
                                    detail: format!(
                                        "client {client_id} read '{val}' (write #{ci}) \
                                         after previously reading '{prev}' (write #{pi}) \
                                         on key '{key}'"
                                    ),
                                });
                            }
                        }
                    }
                }
                self.last_read.insert(rkey, val.to_owned());
            }

            // Durability check: the read value must be among acked writes (or initial state).
            if self.enabled.contains(&Invariant::Durability) {
                if let Some(writes) = self.acked_writes.get(key) {
                    if !writes.contains(&val.to_owned()) {
                        self.violations.push(InvariantError::Violation {
                            name: "Durability".into(),
                            detail: format!(
                                "read '{val}' on key '{key}' but this value was never \
                                 acknowledged — possible phantom read"
                            ),
                        });
                    }
                }
                // If no writes to this key yet, but we read something, that's also a phantom.
                else {
                    self.violations.push(InvariantError::Violation {
                        name: "Durability".into(),
                        detail: format!(
                            "read '{val}' on key '{key}' but no writes have been acknowledged"
                        ),
                    });
                }
            }
        }
    }

    /// Check quorum safety: only the majority partition should accept writes.
    ///
    /// Call with `total_nodes` and `nodes_in_partition` — if the partition
    /// is a minority, writes should fail.
    pub fn check_quorum(&mut self, total_nodes: u64, partition_size: u64, write_succeeded: bool) {
        if !self.enabled.contains(&Invariant::QuorumSafety) {
            return;
        }
        let majority = total_nodes / 2 + 1;
        if partition_size < majority && write_succeeded {
            self.violations.push(InvariantError::Violation {
                name: "QuorumSafety".into(),
                detail: format!(
                    "write succeeded in partition of size {partition_size} \
                     (majority requires {majority} of {total_nodes})"
                ),
            });
        }
    }

    /// Return all violations found so far.
    #[must_use]
    pub fn violations(&self) -> &[InvariantError] {
        &self.violations
    }

    /// Check if any violations have been recorded.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.violations.is_empty()
    }

    /// Consume the checker, returning `Ok(())` if no violations or
    /// `Err` with the first violation.
    pub fn finish(self) -> Result<(), InvariantError> {
        if let Some(v) = self.violations.into_iter().next() {
            Err(v)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_violations_when_correct() {
        let mut checker = InvariantChecker::all();
        checker.observe_leader(1, 1);
        checker.ack_write("x", "1");
        checker.observe_read(1, "x", Some("1"));
        assert!(checker.is_valid());
        assert!(checker.finish().is_ok());
    }

    #[test]
    fn leader_uniqueness_violation() {
        let mut checker = InvariantChecker::new(vec![Invariant::LeaderUniqueness]);
        checker.observe_leader(1, 1);
        checker.observe_leader(1, 2); // Same term, different leader!
        assert!(!checker.is_valid());
        assert_eq!(checker.violations().len(), 1);
    }

    #[test]
    fn leader_different_terms_ok() {
        let mut checker = InvariantChecker::new(vec![Invariant::LeaderUniqueness]);
        checker.observe_leader(1, 1);
        checker.observe_leader(2, 2); // Different term, different leader = fine.
        assert!(checker.is_valid());
    }

    #[test]
    fn durability_phantom_read() {
        let mut checker = InvariantChecker::new(vec![Invariant::Durability]);
        // Read "ghost" without any writes.
        checker.observe_read(1, "x", Some("ghost"));
        assert!(!checker.is_valid());
    }

    #[test]
    fn durability_valid_read() {
        let mut checker = InvariantChecker::new(vec![Invariant::Durability]);
        checker.ack_write("x", "1");
        checker.observe_read(1, "x", Some("1"));
        assert!(checker.is_valid());
    }

    #[test]
    fn monotonic_reads_violation() {
        let mut checker =
            InvariantChecker::new(vec![Invariant::MonotonicReads, Invariant::Durability]);
        checker.ack_write("x", "old");
        checker.ack_write("x", "new");
        checker.observe_read(1, "x", Some("new")); // See "new" first.
        checker.observe_read(1, "x", Some("old")); // Then see "old" — violation!
        assert!(!checker.is_valid());
    }

    #[test]
    fn quorum_safety_minority_write_fails() {
        let mut checker = InvariantChecker::new(vec![Invariant::QuorumSafety]);
        // 5-node cluster, partition with 2 nodes accepts a write — violation!
        checker.check_quorum(5, 2, true);
        assert!(!checker.is_valid());
    }

    #[test]
    fn quorum_safety_majority_write_ok() {
        let mut checker = InvariantChecker::new(vec![Invariant::QuorumSafety]);
        checker.check_quorum(5, 3, true);
        assert!(checker.is_valid());
    }

    #[test]
    fn quorum_safety_minority_write_rejected_ok() {
        let mut checker = InvariantChecker::new(vec![Invariant::QuorumSafety]);
        checker.check_quorum(5, 2, false); // Write correctly rejected.
        assert!(checker.is_valid());
    }

    #[test]
    fn read_none_does_not_trigger_durability() {
        let mut checker = InvariantChecker::new(vec![Invariant::Durability]);
        checker.observe_read(1, "x", None); // Reading None is fine.
        assert!(checker.is_valid());
    }
}
