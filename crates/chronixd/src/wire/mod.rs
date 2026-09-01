//! Wire protocol handlers for Prometheus remote write/read and OTLP metrics.
//!
//! These endpoints accept data in the native Prometheus and OpenTelemetry
//! protobuf formats (with Snappy compression), enabling direct integration
//! with Prometheus servers and OpenTelemetry Collectors.

pub mod otlp;
pub mod prometheus;

#[cfg(test)]
mod tests;
