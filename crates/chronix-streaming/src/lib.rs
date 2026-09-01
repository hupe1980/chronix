#![deny(unsafe_code)]
//! # Chronix Streaming
//!
//! The reactive layer of Chronix:
//!
//! - [`cdc`] — change data capture: bounded broadcast event bus, filtered and
//!   resumable subscriptions, durable event log, incrementally maintained
//!   continuous aggregations, and (feature `flight`) Arrow Flight CDC export.
//! - [`signal`] — programmable signals: trigger engine (anomaly score,
//!   forecast deviation, thresholds, moving-average crossover, rate of
//!   change), SQL trigger management, and delivery with retry + dead-letter
//!   queue via webhook (HMAC-SHA256 signed), CDC bus, or in-process callback.

pub mod cdc;
pub mod signal;
