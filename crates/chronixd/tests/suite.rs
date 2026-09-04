#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Consolidated integration test suite for chronixd.
//!
//! Merges integration, TLS, gRPC and Flight SQL tests into a single
//! binary to reduce link-time overhead (saves ~3 link cycles).

#[path = "suite/client_compat.rs"]
mod client_compat;

#[path = "suite/dashboards.rs"]
mod dashboards;
#[path = "suite/flight_test.rs"]
mod flight_test;
#[path = "suite/grpc_test.rs"]
mod grpc_test;
#[path = "suite/integration.rs"]
mod integration;
#[path = "suite/prom_discovery.rs"]
mod prom_discovery;
#[path = "suite/tenancy_test.rs"]
mod tenancy_test;
#[path = "suite/tls_test.rs"]
mod tls_test;
#[path = "suite/triggers_test.rs"]
mod triggers_test;
