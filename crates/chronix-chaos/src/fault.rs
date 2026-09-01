//! Fault types and configuration.

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// A fault that can be injected into the system.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Fault {
    /// Kill a node after `delay`.
    KillNode {
        /// Delay before killing.
        delay: Duration,
    },

    /// Network partition — isolate specific nodes.
    NetworkPartition {
        /// Node IDs to isolate from the rest of the cluster.
        isolated_nodes: Vec<u64>,
    },

    /// Simulate disk full on a node.
    DiskFull,

    /// Add artificial latency to disk I/O.
    SlowDisk {
        /// Extra latency added to each I/O operation.
        latency: Duration,
    },

    /// Add latency to all operations.
    LatencySpike {
        /// Delay added to each operation.
        delay: Duration,
    },

    /// Drop a percentage of write requests.
    WriteDropper {
        /// Fraction of writes to drop (0.0 to 1.0).
        drop_ratio: f64,
    },

    /// Corrupt data on reads (return random bytes).
    ReadCorruption {
        /// Fraction of reads to corrupt (0.0 to 1.0).
        corruption_ratio: f64,
    },
}

impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KillNode { delay } => write!(f, "KillNode(delay={delay:?})"),
            Self::NetworkPartition { isolated_nodes } => {
                write!(f, "NetworkPartition(nodes={isolated_nodes:?})")
            }
            Self::DiskFull => write!(f, "DiskFull"),
            Self::SlowDisk { latency } => write!(f, "SlowDisk(latency={latency:?})"),
            Self::LatencySpike { delay } => write!(f, "LatencySpike(delay={delay:?})"),
            Self::WriteDropper { drop_ratio } => {
                write!(f, "WriteDropper(ratio={drop_ratio:.2})")
            }
            Self::ReadCorruption { corruption_ratio } => {
                write!(f, "ReadCorruption(ratio={corruption_ratio:.2})")
            }
        }
    }
}

/// Configuration for injecting a fault.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultConfig {
    /// The fault to inject.
    pub fault: Fault,

    /// How long the fault remains active. Auto-expires after this duration.
    pub duration: Duration,

    /// Human-readable description for logging / admin API.
    pub description: String,
}

impl FaultConfig {
    /// Maximum acceptable delay / latency for fault injection (1 hour).
    const MAX_DELAY: Duration = Duration::from_secs(3600);

    /// Validate the fault configuration.
    ///
    /// Checks:
    /// - `WriteDropper::drop_ratio` and `ReadCorruption::corruption_ratio`
    ///   must be in `[0.0, 1.0]`.
    /// - `NetworkPartition::isolated_nodes` must not be empty.
    /// - `KillNode::delay`, `SlowDisk::latency`, `LatencySpike::delay`
    ///   must not exceed `MAX_DELAY` (1 hour).
    /// - `duration` must not be zero (use `clear()` to remove faults).
    /// - `LatencySpike::delay` and `SlowDisk::latency` must not be
    ///   `Duration::ZERO`.
    ///
    /// # Errors
    ///
    /// Returns a descriptive [`String`] on the first failing check.
    pub fn validate(&self) -> Result<(), String> {
        match &self.fault {
            Fault::WriteDropper { drop_ratio } if !(0.0..=1.0).contains(drop_ratio) => {
                return Err(format!(
                    "drop_ratio must be in [0.0, 1.0], got {drop_ratio}"
                ));
            }
            Fault::ReadCorruption { corruption_ratio }
                if !(0.0..=1.0).contains(corruption_ratio) =>
            {
                return Err(format!(
                    "corruption_ratio must be in [0.0, 1.0], got {corruption_ratio}"
                ));
            }
            Fault::NetworkPartition { isolated_nodes } if isolated_nodes.is_empty() => {
                return Err("NetworkPartition requires at least one node in isolated_nodes".into());
            }
            Fault::KillNode { delay } if *delay > Self::MAX_DELAY => {
                return Err(format!(
                    "KillNode delay must not exceed {:?}, got {delay:?}",
                    Self::MAX_DELAY
                ));
            }
            Fault::SlowDisk { latency } if *latency > Self::MAX_DELAY => {
                return Err(format!(
                    "SlowDisk latency must not exceed {:?}, got {latency:?}",
                    Self::MAX_DELAY
                ));
            }
            Fault::LatencySpike { delay } if *delay > Self::MAX_DELAY => {
                return Err(format!(
                    "LatencySpike delay must not exceed {:?}, got {delay:?}",
                    Self::MAX_DELAY
                ));
            }
            // Zero-duration delays/latencies are no-ops and likely a
            // configuration mistake.  Reject them early.
            Fault::LatencySpike { delay } if delay.is_zero() => {
                return Err("LatencySpike delay must not be zero".into());
            }
            Fault::SlowDisk { latency } if latency.is_zero() => {
                return Err("SlowDisk latency must not be zero".into());
            }
            Fault::KillNode { delay } if delay.is_zero() => {
                return Err("KillNode delay must not be zero".into());
            }
            _ => {}
        }
        // Zero-duration fault injection is a no-op.
        if self.duration.is_zero() {
            return Err("fault duration must not be zero — use clear() to remove faults".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fault_display() {
        assert_eq!(
            Fault::KillNode {
                delay: Duration::from_secs(5)
            }
            .to_string(),
            "KillNode(delay=5s)"
        );
        assert_eq!(Fault::DiskFull.to_string(), "DiskFull");
        assert!(Fault::WriteDropper { drop_ratio: 0.5 }
            .to_string()
            .contains("0.50"));
    }

    #[test]
    fn fault_serde_roundtrip() {
        let faults = vec![
            Fault::KillNode {
                delay: Duration::from_secs(3),
            },
            Fault::NetworkPartition {
                isolated_nodes: vec![1, 2],
            },
            Fault::DiskFull,
            Fault::SlowDisk {
                latency: Duration::from_millis(100),
            },
            Fault::LatencySpike {
                delay: Duration::from_millis(200),
            },
            Fault::WriteDropper { drop_ratio: 0.3 },
            Fault::ReadCorruption {
                corruption_ratio: 0.1,
            },
        ];

        for fault in &faults {
            let json = serde_json::to_string(fault).unwrap();
            let back: Fault = serde_json::from_str(&json).unwrap();
            assert_eq!(&back, fault);
        }
    }

    #[test]
    fn fault_config_serde() {
        let config = FaultConfig {
            fault: Fault::LatencySpike {
                delay: Duration::from_millis(500),
            },
            duration: Duration::from_secs(30),
            description: "test".into(),
        };
        let json = serde_json::to_string(&config).unwrap();
        let back: FaultConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.description, "test");
    }

    #[test]
    fn validate_network_partition_empty() {
        let config = FaultConfig {
            fault: Fault::NetworkPartition {
                isolated_nodes: vec![],
            },
            duration: Duration::from_secs(10),
            description: "empty partition".into(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_network_partition_non_empty() {
        let config = FaultConfig {
            fault: Fault::NetworkPartition {
                isolated_nodes: vec![1, 2],
            },
            duration: Duration::from_secs(10),
            description: "valid partition".into(),
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_kill_node_delay_too_large() {
        let config = FaultConfig {
            fault: Fault::KillNode {
                delay: Duration::from_secs(7200),
            },
            duration: Duration::from_secs(10),
            description: "huge delay".into(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_slow_disk_latency_too_large() {
        let config = FaultConfig {
            fault: Fault::SlowDisk {
                latency: Duration::from_secs(7200),
            },
            duration: Duration::from_secs(10),
            description: "huge latency".into(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_latency_spike_delay_too_large() {
        let config = FaultConfig {
            fault: Fault::LatencySpike {
                delay: Duration::from_secs(7200),
            },
            duration: Duration::from_secs(10),
            description: "huge delay".into(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_valid_config() {
        let config = FaultConfig {
            fault: Fault::LatencySpike {
                delay: Duration::from_millis(500),
            },
            duration: Duration::from_secs(30),
            description: "valid".into(),
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn validate_write_dropper_out_of_range() {
        let config = FaultConfig {
            fault: Fault::WriteDropper { drop_ratio: 1.5 },
            duration: Duration::from_secs(10),
            description: "bad ratio".into(),
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_read_corruption_out_of_range() {
        let config = FaultConfig {
            fault: Fault::ReadCorruption {
                corruption_ratio: -0.1,
            },
            duration: Duration::from_secs(10),
            description: "bad ratio".into(),
        };
        assert!(config.validate().is_err());
    }
}
