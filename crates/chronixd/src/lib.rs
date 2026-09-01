//! # chronixd
//!
//! The Chronix time-series database server daemon.
//!
//! Provides REST, gRPC, and Arrow Flight SQL endpoints backed by the
//! embedded [`chronix`] database engine. Supports InfluxDB Line Protocol
//! ingestion, Prometheus metrics export, TLS, and graceful shutdown.
//!
//! ## Endpoints
//!
//! | Protocol    | Default Port | Description                          |
//! |-------------|-------------|--------------------------------------|
//! | HTTP/REST   | 8086        | JSON write/query/management API      |
//! | gRPC        | 8087        | Protobuf write/query/schema API      |
//! | Flight SQL  | 8817        | Arrow columnar data transfer         |
//! | Prometheus  | 8086        | `/metrics` endpoint on HTTP server   |

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod admin;
pub mod auth;
#[cfg(feature = "cluster")]
pub mod cluster;
pub mod config;
pub mod connector;
pub mod error;
pub mod flight;
pub mod grpc;
pub mod http;
pub mod http_metrics;
pub mod influx;
pub mod kafka;
pub mod mqtt;
pub mod namespace;
pub mod openapi;
pub mod otel;
pub mod rate_limit;
#[cfg(feature = "cluster")]
pub mod region_storage;
pub mod request_id;
pub mod server;
pub mod tls;
pub mod tls_watcher;
pub mod util;

/// Generated protobuf types and gRPC service trait.
#[allow(missing_docs)]
#[allow(clippy::result_large_err)] // tonic::Status is large by design
pub mod proto {
    tonic::include_proto!("chronix.v1");

    /// File descriptor set for gRPC reflection.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("chronix_descriptor");
}

/// Prometheus remote write/read protobuf types.
#[allow(missing_docs)]
pub mod prom_proto {
    include!(concat!(env!("OUT_DIR"), "/prometheus.rs"));
}

/// OpenTelemetry OTLP protobuf types.
#[allow(missing_docs)]
pub mod otlp {
    include!(concat!(env!("OUT_DIR"), "/otlp.rs"));
}

pub mod wire;
