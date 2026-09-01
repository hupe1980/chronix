//! Simulated network layer for deterministic fault injection.
//!
//! Wraps the in-process Raft router with controllable network partitions,
//! message delays, and link failures. Partitions are symmetric — if node A
//! cannot reach node B, then B also cannot reach A.

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::RwLock;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// A network action applied to the simulated cluster.
#[derive(Debug, Clone, PartialEq)]
pub enum NetworkAction {
    /// Partition the network into two groups — no messages cross the boundary.
    Partition {
        /// Nodes in the minority partition.
        minority: Vec<u64>,
        /// Nodes in the majority partition.
        majority: Vec<u64>,
    },
    /// Heal all partitions — full connectivity restored.
    Heal,
    /// Isolate a single node from all others.
    Isolate(u64),
    /// Drop a percentage of messages between any two nodes.
    PacketLoss {
        /// Drop ratio in [0.0, 1.0].
        ratio: f64,
    },
}

/// State of the simulated network.
#[derive(Debug)]
struct NetworkState {
    /// Set of (from, to) pairs that are blocked.
    blocked: HashSet<(u64, u64)>,
    /// Global packet loss ratio.
    loss_ratio: f64,
    /// History of actions applied.
    history: Vec<NetworkAction>,
}

/// Simulated network with controllable partitions and message loss.
///
/// Thread-safe — can be shared across async tasks via `Arc`.
#[derive(Debug, Clone)]
pub struct SimNetwork {
    state: Arc<RwLock<NetworkState>>,
    node_count: u64,
    rng: Arc<parking_lot::Mutex<StdRng>>,
}

impl SimNetwork {
    /// Create a new fully-connected simulated network.
    #[must_use]
    pub fn new(node_count: u64, seed: u64) -> Self {
        Self {
            state: Arc::new(RwLock::new(NetworkState {
                blocked: HashSet::new(),
                loss_ratio: 0.0,
                history: Vec::new(),
            })),
            node_count,
            rng: Arc::new(parking_lot::Mutex::new(StdRng::seed_from_u64(seed))),
        }
    }

    /// Apply a network action.
    pub fn apply(&self, action: NetworkAction) {
        let mut state = self.state.write();
        match &action {
            NetworkAction::Partition { minority, majority } => {
                // Block all cross-partition links (symmetric).
                for &a in minority {
                    for &b in majority {
                        state.blocked.insert((a, b));
                        state.blocked.insert((b, a));
                    }
                }
            }
            NetworkAction::Heal => {
                state.blocked.clear();
                state.loss_ratio = 0.0;
            }
            NetworkAction::Isolate(node_id) => {
                for id in 1..=self.node_count {
                    if id != *node_id {
                        state.blocked.insert((*node_id, id));
                        state.blocked.insert((id, *node_id));
                    }
                }
            }
            NetworkAction::PacketLoss { ratio } => {
                state.loss_ratio = ratio.clamp(0.0, 1.0);
            }
        }
        state.history.push(action);
    }

    /// Check if a message from `from` to `to` should be delivered.
    ///
    /// Returns `false` if the link is partitioned or the message is
    /// randomly dropped due to packet loss.
    #[must_use]
    pub fn can_deliver(&self, from: u64, to: u64) -> bool {
        let state = self.state.read();
        if state.blocked.contains(&(from, to)) {
            return false;
        }
        if state.loss_ratio > 0.0 {
            let mut rng = self.rng.lock();
            if rng.random::<f64>() < state.loss_ratio {
                return false;
            }
        }
        true
    }

    /// Check if any partition is currently active.
    #[must_use]
    pub fn is_partitioned(&self) -> bool {
        !self.state.read().blocked.is_empty()
    }

    /// Get the number of blocked links.
    #[must_use]
    pub fn blocked_link_count(&self) -> usize {
        self.state.read().blocked.len()
    }

    /// Get history of all network actions applied.
    #[must_use]
    pub fn history(&self) -> Vec<NetworkAction> {
        self.state.read().history.clone()
    }

    /// Reset to fully-connected state.
    pub fn reset(&self) {
        let mut state = self.state.write();
        state.blocked.clear();
        state.loss_ratio = 0.0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fully_connected_by_default() {
        let net = SimNetwork::new(3, 42);
        assert!(net.can_deliver(1, 2));
        assert!(net.can_deliver(2, 3));
        assert!(net.can_deliver(3, 1));
        assert!(!net.is_partitioned());
    }

    #[test]
    fn partition_blocks_cross_group_messages() {
        let net = SimNetwork::new(5, 42);
        net.apply(NetworkAction::Partition {
            minority: vec![1, 2],
            majority: vec![3, 4, 5],
        });

        // Cross-partition blocked (symmetric).
        assert!(!net.can_deliver(1, 3));
        assert!(!net.can_deliver(3, 1));
        assert!(!net.can_deliver(2, 4));

        // Within-partition allowed.
        assert!(net.can_deliver(1, 2));
        assert!(net.can_deliver(3, 4));
        assert!(net.can_deliver(4, 5));
    }

    #[test]
    fn heal_restores_connectivity() {
        let net = SimNetwork::new(3, 42);
        net.apply(NetworkAction::Isolate(2));
        assert!(!net.can_deliver(1, 2));

        net.apply(NetworkAction::Heal);
        assert!(net.can_deliver(1, 2));
        assert!(net.can_deliver(2, 1));
        assert!(!net.is_partitioned());
    }

    #[test]
    fn isolate_blocks_all_links_to_node() {
        let net = SimNetwork::new(4, 42);
        net.apply(NetworkAction::Isolate(3));

        assert!(!net.can_deliver(3, 1));
        assert!(!net.can_deliver(1, 3));
        assert!(!net.can_deliver(3, 4));
        assert!(!net.can_deliver(4, 3));
        // Other links unaffected.
        assert!(net.can_deliver(1, 2));
        assert!(net.can_deliver(2, 4));
    }

    #[test]
    fn packet_loss_drops_probabilistically() {
        let net = SimNetwork::new(2, 42);
        net.apply(NetworkAction::PacketLoss { ratio: 0.5 });

        let mut delivered = 0;
        let trials = 1000;
        for _ in 0..trials {
            if net.can_deliver(1, 2) {
                delivered += 1;
            }
        }
        // With 50% loss, expect roughly 400-600 deliveries.
        assert!(
            (350..=650).contains(&delivered),
            "delivered {delivered}/1000 — outside expected range for 50% loss"
        );
    }

    #[test]
    fn history_tracks_actions() {
        let net = SimNetwork::new(3, 42);
        net.apply(NetworkAction::Isolate(1));
        net.apply(NetworkAction::Heal);
        assert_eq!(net.history().len(), 2);
    }
}
