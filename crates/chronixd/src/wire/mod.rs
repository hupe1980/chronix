//! Wire protocol handlers, and the one encoding every response shares.
//!
//! The Prometheus and OTLP endpoints accept data in the native Prometheus and
//! OpenTelemetry protobuf formats (with Snappy compression), enabling direct
//! integration with Prometheus servers and OpenTelemetry Collectors.
//!
//! [`value`] is the other direction: the single Arrow → JSON encoding used by
//! every surface that answers a query in JSON or protobuf.

pub mod otlp;
pub mod prometheus;
pub mod value;

#[cfg(test)]
mod tests;
