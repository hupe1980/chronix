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

/// `server.max_concurrent_queries` across SQL, PromQL and Flight SQL.
#[path = "suite/concurrency.rs"]
mod concurrency;
#[path = "suite/console_test.rs"]
#[cfg(feature = "console")]
mod console_test;

/// The MCP server, driven by the official SDK's client.
#[path = "suite/mcp_test.rs"]
mod mcp_test;

/// Hints, spans, typo warnings and `explain_empty`.
#[path = "suite/diagnostics.rs"]
mod diagnostics;

#[path = "suite/decimal_wire.rs"]
mod decimal_wire;

#[path = "suite/errors_test.rs"]
mod errors_test;

#[path = "suite/explain_agreement.rs"]
mod explain_agreement;

#[path = "suite/extension_seam.rs"]
mod extension_seam;

#[path = "suite/flight_test.rs"]
mod flight_test;
#[path = "suite/grpc_test.rs"]
mod grpc_test;
/// `chronixd inspect` through the real binary.
#[path = "suite/inspect_cli.rs"]
mod inspect_cli;

#[path = "suite/demo_test.rs"]
mod demo_test;
#[path = "suite/integration.rs"]
mod integration;
#[path = "suite/partial_answers.rs"]
mod partial_answers;
#[path = "suite/prom_discovery.rs"]
mod prom_discovery;

/// Native histograms over the wire a real Prometheus or OTel collector uses.
#[path = "suite/native_histogram_wire.rs"]
mod native_histogram_wire;
#[path = "suite/tenancy_test.rs"]
mod tenancy_test;

/// Namespace quotas on every write surface the router mounts.
#[path = "suite/quota_enforcement.rs"]
mod quota_enforcement;
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
