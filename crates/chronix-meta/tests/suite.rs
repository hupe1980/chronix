//! Consolidated integration test suite for chronix-meta (Raft + Admin).
//!
//! Merges admin and cluster integration tests into a single binary to
//! reduce link-time overhead.

#[path = "suite/admin_integration.rs"]
mod admin_integration;
#[path = "suite/cluster_integration.rs"]
mod cluster_integration;
