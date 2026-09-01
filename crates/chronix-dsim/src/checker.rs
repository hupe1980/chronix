//! Linearizability checker for distributed operations.
//!
//! Implements a history-based linearizability verification algorithm inspired
//! by Wing & Gong (1993). Records invocations and responses of operations on
//! a shared register, then checks that there exists a legal sequential ordering
//! consistent with real-time ordering.
//!
//! This is the core correctness verification tool for the simulation framework —
//! it proves that the distributed system behaves as if operations execute
//! atomically on a single copy, even under concurrent access and fault injection.

use std::collections::HashMap;

/// The kind of operation performed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpKind {
    /// Write a key-value pair.
    Write {
        /// The key written.
        key: String,
        /// The value written.
        value: String,
    },
    /// Read a key, expecting a specific value (or None if not found).
    Read {
        /// The key read.
        key: String,
        /// The value returned (None = key not found).
        result: Option<String>,
    },
    /// Compare-and-swap: set key to new_value if current == expected.
    Cas {
        /// The key to CAS.
        key: String,
        /// The expected current value.
        expected: Option<String>,
        /// The new value to write.
        new_value: String,
        /// Whether the CAS succeeded.
        ok: bool,
    },
}

/// A single entry in the operation history.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    /// Unique operation ID.
    pub op_id: u64,
    /// The client/thread that performed the operation.
    pub client_id: u64,
    /// The operation performed.
    pub op: OpKind,
    /// Invocation timestamp (virtual clock ms).
    pub invoke_at: u64,
    /// Response timestamp (virtual clock ms).
    pub return_at: u64,
}

/// Linearizability checker.
///
/// Records a history of concurrent operations and verifies that a legal
/// sequential ordering exists. Uses a brute-force search over possible
/// linearization points — efficient for the small histories generated
/// in simulation tests (typically < 1000 operations).
#[derive(Debug, Default)]
pub struct Linearizer {
    history: Vec<HistoryEntry>,
    next_id: u64,
}

impl Linearizer {
    /// Create a new empty linearizer.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an operation in the history.
    pub fn record(&mut self, client_id: u64, op: OpKind, invoke_at: u64, return_at: u64) -> u64 {
        let op_id = self.next_id;
        self.next_id += 1;
        self.history.push(HistoryEntry {
            op_id,
            client_id,
            op,
            invoke_at,
            return_at,
        });
        op_id
    }

    /// Number of recorded operations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.history.len()
    }

    /// Whether the history is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.history.is_empty()
    }

    /// Check if the recorded history is linearizable with respect to a
    /// single-key register model.
    ///
    /// Returns `Ok(())` if linearizable, `Err(description)` if a violation
    /// is found.
    ///
    /// # Algorithm
    ///
    /// For each key, extracts all operations touching that key, sorts by
    /// invocation time, and verifies that reads return the value of the
    /// most recent preceding write in some legal ordering that respects
    /// real-time precedence (return_at of A < invoke_at of B ⟹ A before B).
    pub fn check(&self) -> Result<(), String> {
        // Group operations by key.
        let mut by_key: HashMap<String, Vec<&HistoryEntry>> = HashMap::new();
        for entry in &self.history {
            let key = match &entry.op {
                OpKind::Write { key, .. } | OpKind::Read { key, .. } | OpKind::Cas { key, .. } => {
                    key.clone()
                }
            };
            by_key.entry(key).or_default().push(entry);
        }

        for (key, ops) in &by_key {
            self.check_key(key, ops)?;
        }
        Ok(())
    }

    /// Check linearizability for a single key's operations.
    fn check_key(&self, key: &str, ops: &[&HistoryEntry]) -> Result<(), String> {
        // Sort by invocation time, then by return time for ties.
        let mut sorted: Vec<&HistoryEntry> = ops.to_vec();
        sorted.sort_by_key(|e| (e.invoke_at, e.return_at));

        // Try to find a valid linearization using backtracking.
        let mut used = vec![false; sorted.len()];
        let mut current_value: Option<String> = None;

        if self.try_linearize(key, &sorted, &mut used, &mut current_value, 0) {
            Ok(())
        } else {
            Err(format!(
                "linearizability violation on key '{key}': no valid sequential ordering exists \
                 for {} operations",
                sorted.len()
            ))
        }
    }

    /// Backtracking search for a valid linearization order.
    ///
    /// At each step, picks an operation whose invocation is not after
    /// any un-linearized operation's return (respecting real-time order),
    /// checks if it's consistent with the current register state, and recurses.
    fn try_linearize(
        &self,
        key: &str,
        ops: &[&HistoryEntry],
        used: &mut [bool],
        current_value: &mut Option<String>,
        depth: usize,
    ) -> bool {
        if depth == ops.len() {
            return true; // All operations linearized.
        }

        // Find the minimum return_at among unused operations — any candidate
        // must have invoke_at <= this value (otherwise it definitely started
        // after some operation that hasn't been linearized yet, which would
        // violate real-time ordering if we linearize it first).
        let min_return = ops
            .iter()
            .enumerate()
            .filter(|(i, _)| !used[*i])
            .map(|(_, e)| e.return_at)
            .min()
            .unwrap_or(u64::MAX);

        for i in 0..ops.len() {
            if used[i] {
                continue;
            }
            // Real-time constraint: can only linearize ops that overlap with
            // the earliest-returning un-linearized operation.
            if ops[i].invoke_at > min_return {
                continue;
            }

            let saved_value = current_value.clone();

            if self.is_consistent(key, ops[i], current_value) {
                used[i] = true;
                if self.try_linearize(key, ops, used, current_value, depth + 1) {
                    return true;
                }
                used[i] = false;
            }
            *current_value = saved_value;
        }
        false
    }

    /// Check if an operation is consistent with the current register state
    /// and update the state if it is.
    #[allow(clippy::option_if_let_else)]
    fn is_consistent(
        &self,
        _key: &str,
        entry: &HistoryEntry,
        current_value: &mut Option<String>,
    ) -> bool {
        match &entry.op {
            OpKind::Write { value, .. } => {
                *current_value = Some(value.clone());
                true
            }
            OpKind::Read { result, .. } => *result == *current_value,
            OpKind::Cas {
                expected,
                new_value,
                ok,
                ..
            } => {
                if *ok {
                    // CAS succeeded — expected must match current.
                    if *expected == *current_value {
                        *current_value = Some(new_value.clone());
                        true
                    } else {
                        false
                    }
                } else {
                    // CAS failed — expected must NOT match current.
                    *expected != *current_value
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_history_is_linearizable() {
        let checker = Linearizer::new();
        assert!(checker.check().is_ok());
    }

    #[test]
    fn single_write_read_is_linearizable() {
        let mut checker = Linearizer::new();
        checker.record(
            1,
            OpKind::Write {
                key: "x".into(),
                value: "1".into(),
            },
            0,
            1,
        );
        checker.record(
            2,
            OpKind::Read {
                key: "x".into(),
                result: Some("1".into()),
            },
            2,
            3,
        );
        assert!(checker.check().is_ok());
    }

    #[test]
    fn stale_read_is_not_linearizable() {
        let mut checker = Linearizer::new();
        // Write x=1 at t=[0,1]
        checker.record(
            1,
            OpKind::Write {
                key: "x".into(),
                value: "1".into(),
            },
            0,
            1,
        );
        // Write x=2 at t=[2,3]
        checker.record(
            1,
            OpKind::Write {
                key: "x".into(),
                value: "2".into(),
            },
            2,
            3,
        );
        // Read x=1 at t=[4,5] — stale! Should see "2".
        checker.record(
            2,
            OpKind::Read {
                key: "x".into(),
                result: Some("1".into()),
            },
            4,
            5,
        );
        assert!(checker.check().is_err());
    }

    #[test]
    fn concurrent_ops_can_reorder() {
        let mut checker = Linearizer::new();
        // Write x=1 at t=[0,10] (long-running)
        checker.record(
            1,
            OpKind::Write {
                key: "x".into(),
                value: "1".into(),
            },
            0,
            10,
        );
        // Write x=2 at t=[5,8] (overlapping)
        checker.record(
            2,
            OpKind::Write {
                key: "x".into(),
                value: "2".into(),
            },
            5,
            8,
        );
        // Read x=1 at t=[9,11] — valid because write(1) could linearize after write(2)
        checker.record(
            3,
            OpKind::Read {
                key: "x".into(),
                result: Some("1".into()),
            },
            9,
            11,
        );
        assert!(checker.check().is_ok());
    }

    #[test]
    fn read_before_any_write_must_return_none() {
        let mut checker = Linearizer::new();
        checker.record(
            1,
            OpKind::Read {
                key: "x".into(),
                result: Some("phantom".into()),
            },
            0,
            1,
        );
        assert!(checker.check().is_err());
    }

    #[test]
    fn cas_success_linearizable() {
        let mut checker = Linearizer::new();
        checker.record(
            1,
            OpKind::Write {
                key: "x".into(),
                value: "1".into(),
            },
            0,
            1,
        );
        checker.record(
            1,
            OpKind::Cas {
                key: "x".into(),
                expected: Some("1".into()),
                new_value: "2".into(),
                ok: true,
            },
            2,
            3,
        );
        checker.record(
            2,
            OpKind::Read {
                key: "x".into(),
                result: Some("2".into()),
            },
            4,
            5,
        );
        assert!(checker.check().is_ok());
    }

    #[test]
    fn cas_failure_must_not_mutate() {
        let mut checker = Linearizer::new();
        checker.record(
            1,
            OpKind::Write {
                key: "x".into(),
                value: "1".into(),
            },
            0,
            1,
        );
        // CAS expects "2" but current is "1" — must fail.
        checker.record(
            2,
            OpKind::Cas {
                key: "x".into(),
                expected: Some("2".into()),
                new_value: "3".into(),
                ok: false,
            },
            2,
            3,
        );
        // Value should still be "1".
        checker.record(
            2,
            OpKind::Read {
                key: "x".into(),
                result: Some("1".into()),
            },
            4,
            5,
        );
        assert!(checker.check().is_ok());
    }

    #[test]
    fn multiple_keys_independent() {
        let mut checker = Linearizer::new();
        checker.record(
            1,
            OpKind::Write {
                key: "x".into(),
                value: "1".into(),
            },
            0,
            1,
        );
        checker.record(
            2,
            OpKind::Write {
                key: "y".into(),
                value: "2".into(),
            },
            0,
            1,
        );
        checker.record(
            1,
            OpKind::Read {
                key: "x".into(),
                result: Some("1".into()),
            },
            2,
            3,
        );
        checker.record(
            2,
            OpKind::Read {
                key: "y".into(),
                result: Some("2".into()),
            },
            2,
            3,
        );
        assert!(checker.check().is_ok());
    }

    #[test]
    fn read_none_before_write_is_valid() {
        let mut checker = Linearizer::new();
        checker.record(
            1,
            OpKind::Read {
                key: "x".into(),
                result: None,
            },
            0,
            1,
        );
        checker.record(
            2,
            OpKind::Write {
                key: "x".into(),
                value: "1".into(),
            },
            2,
            3,
        );
        assert!(checker.check().is_ok());
    }
}
