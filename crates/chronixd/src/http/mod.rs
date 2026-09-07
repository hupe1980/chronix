//! REST API handlers for the chronixd HTTP server.
//!
//! # API Versioning Strategy
//!
//! All data-plane and management endpoints live under **`/api/v1/`** and use
//! JSON request/response bodies.  Health (`/health`) and readiness (`/ready`)
//! endpoints are deliberately **un-versioned** so load-balancers and
//! orchestrators can probe them without knowing the API version.
//!
//! Versioning is **path-based**: breaking changes will be introduced under
//! `/api/v2/`, allowing clients to migrate at their own pace while the
//! previous version remains available.  Non-breaking additions (new fields,
//! new optional query parameters) are made in-place on the current version.
//!
//! The shared database handle is passed via Axum state.

mod management;
mod prom;
mod prom_params;
mod query;
mod streaming;
mod triggers;
mod types;
mod write;

// Re-export shared types used by other modules in the crate.
pub use types::{
    AppState, PaginatedResponse, PaginationParams, SharedState, WriteDedupCache, DEFAULT_LIST_LIMIT,
};

// Re-export all handler functions so server.rs can reference them as `http::handler_name`.
pub use management::{
    create_rollup_handler, declare_field_handler, delete_batch_handler, delete_handler,
    delete_rollup_handler, drop_measurement_handler, export_dashboards_handler,
    export_parquet_handler, get_schema_handler, health_handler, list_connectors_handler,
    list_measurements_handler, list_rollups_handler, ready_handler, refresh_rollup_handler,
    restore_measurement_handler, update_log_level_handler,
};
pub use prom::{
    prom_buildinfo_handler, prom_empty_alerts_handler, prom_empty_exemplars_handler,
    prom_empty_rules_handler, prom_instant_query_handler, prom_label_values_handler,
    prom_labels_handler, prom_metadata_handler, prom_range_query_handler, prom_series_handler,
};
pub use prom_params::{parse_duration_ns, parse_time_ns, PromParams};
pub use query::{query_explain_handler, query_handler, sql_handler};
pub use streaming::{annotations_handler, annotations_stream_handler, cdc_stream_handler};
pub use triggers::{
    drop_trigger_handler, get_trigger_handler, list_signals_handler, list_triggers_handler,
    trigger_sql_handler, SignalView, TriggerResponse, TriggerSqlRequest, TriggerView,
};
pub use write::{write_handler, write_influx_handler};
