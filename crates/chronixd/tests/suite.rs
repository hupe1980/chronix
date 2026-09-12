#![allow(clippy::unwrap_used, clippy::expect_used)] // test code may unwrap
//! Consolidated integration test suite for chronixd.
//!
//! Merges integration, TLS, gRPC and Flight SQL tests into a single
//! binary to reduce link-time overhead (saves ~3 link cycles).

// Declared once for the whole binary: two modules read the router's table,
// and `#[path]`-including it twice compiles it twice.
#[path = "suite/route_scan.rs"]
mod route_scan;

#[path = "suite/route_scoping.rs"]
mod route_scoping;

#[path = "suite/authz_test.rs"]
mod authz_test;

#[path = "suite/client_compat.rs"]
mod client_compat;

#[path = "suite/decimal_wire.rs"]
mod decimal_wire;

#[path = "suite/errors_test.rs"]
mod errors_test;

#[path = "suite/explain_agreement.rs"]
mod explain_agreement;

#[path = "suite/flight_test.rs"]
mod flight_test;
#[path = "suite/grpc_test.rs"]
mod grpc_test;
#[path = "suite/integration.rs"]
mod integration;
#[path = "suite/partial_answers.rs"]
mod partial_answers;
#[path = "suite/prom_discovery.rs"]
mod prom_discovery;
#[path = "suite/tenancy_test.rs"]
mod tenancy_test;
#[path = "suite/tls_test.rs"]
mod tls_test;
#[path = "suite/triggers_test.rs"]
mod triggers_test;

/// Every Arrow type a query can produce reaches the client as data,
/// and a mistake in a query says what it was.
#[path = "suite/value_fidelity.rs"]
mod value_fidelity;

#[path = "suite/soft_delete.rs"]
mod soft_delete;

#[path = "suite/stopping.rs"]
mod stopping;
