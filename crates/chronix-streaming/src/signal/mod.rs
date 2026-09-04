//! # Chronix Signal — Streaming Signal System
//!
//! Programmable triggers, condition evaluation, and signal event generation
//! for the Chronix time-series database.
//!
//! ## Architecture
//!
//! ```text
//! CDC EventBus (writes)
//!       │
//!       ▼
//!   TriggerEngine
//!     ├── Registered EventTriggers (per measurement)
//!     ├── Condition Evaluation (field threshold, anomaly, forecast, MA, RoC)
//!     ├── Cooldown Deduplication (per trigger × series)
//!     └── SignalEvent emission → delivery layer
//! ```
//!
//! ## Built-in Trigger Templates
//!
//! - [`anomaly_threshold_trigger`] — fires when anomaly score exceeds threshold
//! - [`forecast_deviation_trigger`] — fires when actual deviates from forecast
//! - [`ma_crossover_trigger`] — fires on golden/death cross (short MA vs long MA)
//! - [`rate_of_change_trigger`] — fires when value changes rapidly
//!
//! ## Example
//!
//! ```no_run
//! use chronix_streaming::signal::{TriggerEngine, anomaly_threshold_trigger};
//! use std::time::Duration;
//!
//! let engine = TriggerEngine::new();
//! let trigger = anomaly_threshold_trigger("alert-1", "CPU Anomaly", "cpu", 3.0)
//!     .with_cooldown(Duration::from_secs(60));
//! engine.register(trigger).unwrap();
//!
//! // Process CDC events — fires signals when conditions are met
//! // let fired = engine.process_event(&cdc_event);
//! ```

#![warn(missing_docs)]
#![deny(unsafe_code)]

mod delivery;
mod engine;
pub mod error;
mod manager;
mod model;
#[cfg(test)]
mod perf;
pub mod sql;
pub mod ssrf;

pub use delivery::{
    DeadLetter, DeadLetterQueue, DeferredWebhookChannel, DeliveryChannel, DeliveryRouter,
    LogChannel, MetricChannel, RetryPolicy, SignalStore, WebhookChannel, WebhookConfig,
};
pub use engine::{
    anomaly_threshold_trigger, forecast_deviation_trigger, ma_crossover_trigger,
    rate_of_change_trigger, TriggerEngine,
};
pub use error::SignalError;
pub use manager::TriggerManager;
pub use model::{
    CrossoverDirection, EventTrigger, Severity, SignalEvent, ThresholdOp, TriggerCondition,
};
pub use sql::{
    execute_trigger_sql, parse_trigger_sql, AlterTrigger, CatalogEntry, CreateTrigger,
    DeliveryTarget, ParsedCondition, SqlResult, TriggerCatalog, TriggerInfo, TriggerStatement,
};
