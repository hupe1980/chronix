//! # Chronix Audit — Structured Audit Logging
//!
//! Captures security-relevant operations as structured audit events with
//! pluggable sinks (memory, file, tracing) and queryable storage.
//!
//! ## Architecture
//!
//! ```text
//! Authorization Decision  ──┐
//! Authentication Event    ──┤
//! Admin Operation         ──┤──→ AuditLogger ──→ AuditSink(s)
//! Signal / Trigger Event  ──┘       │               ├── MemorySink (queryable)
//!                                   │               ├── WriterSink (JSON lines)
//!                                   │               ├── TracingSink (structured logs)
//!                                   │               └── WebhookSink (HTTP POST, JSON/CEF)
//!                                   │
//!                                   └── Sequence numbers + metrics
//! ```
//!
//! ## SIEM Integration
//!
//! Built-in [`WebhookSink`] forwards events via HTTP POST (JSON or CEF
//! format) with configurable auth, retries, and backpressure. Additional
//! integration options:
//!
//! 1. **Webhook**: use [`WebhookSink`] (feature `webhook`) to POST
//!    events directly to Splunk HEC, Elastic, Sentinel, or any HTTP
//!    endpoint. Supports JSON and CEF formats.
//! 2. **File → agent**: use [`WriterSink`] with JSON-lines output and
//!    configure a log shipper (Filebeat, Fluentd, Vector) to ingest
//!    the file into your SIEM pipeline.
//! 3. **Custom sink**: implement [`AuditSink`] and push events
//!    directly via the SIEM's HTTP/gRPC ingest API.
//! 4. **Tracing → OTLP**: use [`TracingSink`] combined with the
//!    `chronixd`'s OTLP exporter; many SIEMs accept OTLP.
//!
//! Option 1 is recommended for most deployments. Option 2 is preferred
//! when you need to decouple audit durability from network availability.
//!
//! ## Example
//!
//! ```no_run
//! use chronix_security::audit::{AuditLogger, AuditEvent, AuditAction, AuditDecision, MemorySink};
//! use std::sync::Arc;
//!
//! // Create logger with memory sink
//! let sink = Arc::new(MemorySink::new(10_000));
//! let mut logger = AuditLogger::new();
//!
//! // Log an authorization decision
//! let event = AuditEvent::new("alice", AuditAction::Write, "cpu", AuditDecision::Allow)
//!     .with_source_ip("10.0.0.1")
//!     .with_request_id("req-001");
//! // logger.log(event);
//! ```

#![warn(missing_docs)]
#![deny(unsafe_code)]

pub mod error;
mod logger;
mod model;
#[cfg(test)]
mod perf;
#[cfg(feature = "webhook")]
mod webhook;

pub use error::AuditError;
pub use logger::{
    last_event, read_events, AuditLogger, AuditSink, FileSink, MemorySink, TracingSink, WriterSink,
};
pub use model::{
    verify_chain_from, verify_hash_chain, verify_hash_chain_with_key, AuditAction, AuditDecision,
    AuditEvent,
};
#[cfg(feature = "webhook")]
pub use webhook::{WebhookConfig, WebhookFormat, WebhookSink};
