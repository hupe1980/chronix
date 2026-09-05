//! OpenAPI specification for the Chronix REST API.
//!
//! Provides a programmatically built OpenAPI 3.1 document that describes
//! all public HTTP endpoints.  Served at `GET /api/v1/openapi.json`.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use utoipa::openapi::path::HttpMethod;
use utoipa::openapi::path::{OperationBuilder, ParameterBuilder, ParameterIn};
use utoipa::openapi::request_body::RequestBodyBuilder;
use utoipa::openapi::response::ResponseBuilder;
use utoipa::openapi::{
    ContactBuilder, Content, InfoBuilder, LicenseBuilder, OpenApi, OpenApiBuilder, PathItem,
    PathsBuilder, RefOr, Schema, ServerBuilder,
};

use crate::http::AppState;

/// Serve the OpenAPI JSON document.
///
/// Memoized per server on first request.
pub async fn openapi_handler(State(state): State<AppState>) -> impl IntoResponse {
    let json = state.openapi_json.get_or_init(|| {
        build_openapi(&server_url(&state.config))
            .to_json()
            .unwrap_or_else(|e| format!("{{\"error\": \"failed to serialize OpenAPI spec: {e}\"}}"))
    });
    (
        StatusCode::OK,
        [("content-type", "application/json")],
        json.clone(),
    )
}

/// The OpenAPI document as JSON, for a given server URL.
///
/// Exposed so the route-inventory test can compare the document against the
/// router without standing a server up.
///
/// # Panics
///
/// If the document cannot be serialised, which is a construction bug rather
/// than a runtime condition.
#[must_use]
pub fn spec_json(server_url: &str) -> String {
    build_openapi(server_url)
        .to_json()
        .expect("the OpenAPI document must serialise")
}

/// Resolve the `servers[0].url` the spec should advertise.
///
/// A bind address is not a client-reachable URL: the common `0.0.0.0:8086`
/// resolves to nothing useful in a generated client, and behind a reverse
/// proxy neither the address nor the scheme reflects how callers reach the
/// server. So the default is the relative URL `/`, which OpenAPI 3.x resolves
/// against the location the document was fetched from — always correct, no
/// configuration required. Deployments that need an absolute URL (publishing
/// the spec to a portal, say) set `public_url`.
fn server_url(config: &crate::config::ServerConfig) -> String {
    if let Some(url) = &config.server.public_url {
        return url.clone();
    }
    "/".to_string()
}

fn json_content(schema: RefOr<Schema>) -> Content {
    Content::new(Some(schema))
}

fn json_body(desc: &str, schema: RefOr<Schema>) -> utoipa::openapi::request_body::RequestBody {
    RequestBodyBuilder::new()
        .description(Some(desc))
        .content("application/json", json_content(schema))
        .required(Some(utoipa::openapi::Required::True))
        .build()
}

fn ok_json(desc: &str, schema: RefOr<Schema>) -> utoipa::openapi::response::Response {
    ResponseBuilder::new()
        .description(desc)
        .content("application/json", json_content(schema))
        .build()
}

fn ok_empty(desc: &str) -> utoipa::openapi::response::Response {
    ResponseBuilder::new().description(desc).build()
}

/// An optional query parameter, described.
fn query_param(name: &str, desc: &str) -> utoipa::openapi::path::Parameter {
    ParameterBuilder::new()
        .name(name)
        .parameter_in(ParameterIn::Query)
        .required(utoipa::openapi::Required::False)
        .description(Some(desc))
        .build()
}

/// The `?backfill=` parameter, on every write route that accepts it.
fn backfill_param() -> utoipa::openapi::path::Parameter {
    query_param(
        "backfill",
        "`true` writes points **outside** the out-of-order window — importing \
         history rather than ingesting live. Live writes are held to \
         ±`ooo_shard_tolerance` shards of the newest write; anything older is \
         refused with a 400 naming the window. Everything else (admission, the \
         cardinality budget, the schema, the future-timestamp bound) is \
         unchanged.",
    )
}

/// The `?limit=` parameter, as Prometheus defines it.
fn limit_param() -> utoipa::openapi::path::Parameter {
    query_param(
        "limit",
        "Maximum number of results; `0` or absent means no limit. When it \
         cuts the answer, the response carries \
         `warnings: [\"results truncated due to limit\"]`.",
    )
}

fn obj_schema() -> RefOr<Schema> {
    RefOr::T(Schema::Object(
        utoipa::openapi::schema::ObjectBuilder::new().build(),
    ))
}

/// Build the full OpenAPI 3.1 spec.
fn build_openapi(server_url: &str) -> OpenApi {
    let info = InfoBuilder::new()
        .title("Chronix Time-Series Database REST API")
        .version(env!("CARGO_PKG_VERSION"))
        .description(Some(
            "High-performance embedded time-series database with SQL, PromQL, and streaming support.",
        ))
        .license(Some(
            LicenseBuilder::new().name("MIT").build(),
        ))
        .contact(Some(
            ContactBuilder::new()
                .name(Some("Chronix Project"))
                .build(),
        ))
        .build();

    let server = ServerBuilder::new()
        .url(server_url)
        .description(Some("Chronix server"))
        .build();

    let mut paths = PathsBuilder::new();

    // ── Health ─────────────────────────────────────────────────────
    paths = paths.path(
        "/health",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Health")
                .summary(Some("Health check"))
                .description(Some("Returns 200 if the server is running."))
                .response("200", ok_empty("Server is healthy"))
                .build(),
        ),
    );
    paths = paths.path(
        "/ready",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Health")
                .summary(Some("Readiness check"))
                .description(Some(
                    "Returns 200 when the server is ready to accept requests.",
                ))
                .response("200", ok_empty("Server is ready"))
                .build(),
        ),
    );

    // ── Write ──────────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/write",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Write")
                .summary(Some("Write points"))
                .description(Some(
                    "Write one or more data points in JSON format. Supports single point or batch.",
                ))
                .parameter(backfill_param())
                .request_body(Some(json_body("Write point(s)", obj_schema())))
                .response("204", ok_empty("Points written successfully"))
                .response("400", ok_json("Bad request", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/write/influx",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Write")
                .summary(Some("Write points (InfluxDB line protocol)"))
                .description(Some("Write data using InfluxDB line protocol format."))
                .parameter(backfill_param())
                .response("204", ok_empty("Points written successfully"))
                .response("400", ok_json("Bad request", obj_schema()))
                .build(),
        ),
    );

    // ── Query ──────────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/chronix/query",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Query")
                .summary(Some("Query measurement data"))
                .description(Some(
                    "Execute a structured query with time range, tag filters, and field projection.",
                ))
                .request_body(Some(json_body("Query request", obj_schema())))
                .response("200", ok_json("Query results", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/chronix/query/explain",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Query")
                .summary(Some("Explain query execution plan"))
                .description(Some(
                    "Returns the query plan and pruning statistics without executing.",
                ))
                .request_body(Some(json_body("Query request", obj_schema())))
                .response("200", ok_json("Query explanation", obj_schema()))
                .build(),
        ),
    );

    // ── SQL ────────────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/chronix/sql",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("SQL")
                .summary(Some("Execute SQL query"))
                .description(Some(
                    "Execute a SQL query using the DataFusion engine. Supports SELECT, SHOW, and analytical queries.",
                ))
                .request_body(Some(json_body("SQL query", obj_schema())))
                .response("200", ok_json("SQL results with columns and rows", obj_schema()))
                .build(),
        ),
    );

    // ── Prometheus ─────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/prom/query",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Prometheus")
                .summary(Some("Instant query (PromQL)"))
                .description(Some(
                    "Evaluate a PromQL expression at a single point in time.",
                ))
                .parameter(
                    ParameterBuilder::new()
                        .name("query")
                        .parameter_in(ParameterIn::Query)
                        .required(utoipa::openapi::Required::True)
                        .build(),
                )
                .response("200", ok_json("Prometheus response", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/prom/query_range",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Prometheus")
                .summary(Some("Range query (PromQL)"))
                .description(Some("Evaluate a PromQL expression over a time range."))
                .parameter(
                    ParameterBuilder::new()
                        .name("query")
                        .parameter_in(ParameterIn::Query)
                        .required(utoipa::openapi::Required::True)
                        .build(),
                )
                .parameter(
                    ParameterBuilder::new()
                        .name("start")
                        .parameter_in(ParameterIn::Query)
                        .required(utoipa::openapi::Required::True)
                        .build(),
                )
                .parameter(
                    ParameterBuilder::new()
                        .name("end")
                        .parameter_in(ParameterIn::Query)
                        .required(utoipa::openapi::Required::True)
                        .build(),
                )
                .parameter(
                    ParameterBuilder::new()
                        .name("step")
                        .parameter_in(ParameterIn::Query)
                        .required(utoipa::openapi::Required::True)
                        .build(),
                )
                .response("200", ok_json("Prometheus range response", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/prom/labels",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Prometheus")
                .summary(Some("List label names"))
                .response("200", ok_json("Label names", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/prom/label/{name}/values",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Prometheus")
                .summary(Some("List label values"))
                .parameter(
                    ParameterBuilder::new()
                        .name("name")
                        .parameter_in(ParameterIn::Path)
                        .required(utoipa::openapi::Required::True)
                        .build(),
                )
                .parameter(limit_param())
                .response("200", ok_json("Label values", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/prom/series",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Prometheus")
                .summary(Some("Find series"))
                .parameter(limit_param())
                .response("200", ok_json("Matching series", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/prom/metadata",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Prometheus")
                .summary(Some("Metric metadata"))
                .response("200", ok_json("Metric metadata", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/prom/write",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Prometheus")
                .summary(Some("Prometheus remote write"))
                .description(Some(
                    "Accepts Prometheus remote write v1 protocol (Snappy-compressed protobuf).",
                ))
                .response("204", ok_empty("Write successful"))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/prom/read",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Prometheus")
                .summary(Some("Prometheus remote read"))
                .description(Some("Responds to Prometheus remote read queries."))
                .response("200", ok_empty("Read response (protobuf)"))
                .build(),
        ),
    );

    // ── OTLP ───────────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/otlp/metrics",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("OTLP")
                .summary(Some("OpenTelemetry metrics ingestion"))
                .description(Some("Accepts OTLP/HTTP metrics in JSON format."))
                .response("200", ok_json("Ingestion result", obj_schema()))
                .build(),
        ),
    );

    // ── Measurements ───────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/measurements",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Measurements")
                .summary(Some("List measurements"))
                .description(Some("List all measurements with pagination support."))
                .parameter(
                    ParameterBuilder::new()
                        .name("offset")
                        .parameter_in(ParameterIn::Query)
                        .build(),
                )
                .parameter(
                    ParameterBuilder::new()
                        .name("limit")
                        .parameter_in(ParameterIn::Query)
                        .build(),
                )
                .response("200", ok_json("Paginated measurement list", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/measurements/{name}/schema",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Measurements")
                .summary(Some("Get measurement schema"))
                .parameter(
                    ParameterBuilder::new()
                        .name("name")
                        .parameter_in(ParameterIn::Path)
                        .required(utoipa::openapi::Required::True)
                        .build(),
                )
                .response("200", ok_json("Measurement schema", obj_schema()))
                .response("404", ok_json("Not found", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/measurements/{name}",
        PathItem::new(
            HttpMethod::Delete,
            OperationBuilder::new()
                .tag("Measurements")
                .summary(Some("Drop measurement"))
                .parameter(
                    ParameterBuilder::new()
                        .name("name")
                        .parameter_in(ParameterIn::Path)
                        .required(utoipa::openapi::Required::True)
                        .build(),
                )
                .response("200", ok_json("Measurement dropped", obj_schema()))
                .build(),
        ),
    );

    // ── Delete ─────────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/delete",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Delete")
                .summary(Some("Delete data points"))
                .description(Some(
                    "Delete points matching measurement, tag filters, and optional time range.",
                ))
                .request_body(Some(json_body("Delete request", obj_schema())))
                .response("200", ok_json("Delete result", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/delete_batch",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Delete")
                .summary(Some("Batch delete data points"))
                .description(Some(
                    "Delete multiple sets of data points in a single request.",
                ))
                .request_body(Some(json_body("Batch delete request", obj_schema())))
                .response("200", ok_json("Batch delete results", obj_schema()))
                .build(),
        ),
    );

    // ── Export ──────────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/export/parquet",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Export")
                .summary(Some("Export to Parquet"))
                .description(Some("Export measurement data to a Parquet file."))
                .request_body(Some(json_body("Export request", obj_schema())))
                .response("200", ok_json("Export result", obj_schema()))
                .build(),
        ),
    );

    // ── Rollups ────────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/rollups",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Rollups")
                .summary(Some("List rollup rules"))
                .response("200", ok_json("Rollup rules", obj_schema()))
                .build(),
        ),
    );

    // ── CDC / Streaming ────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/cdc/stream",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Streaming")
                .summary(Some("CDC event stream (SSE)"))
                .description(Some(
                    "Server-Sent Events stream of change data capture events.",
                ))
                .response("200", ok_empty("SSE stream"))
                .build(),
        ),
    );

    // ── Admin ──────────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/admin/nodes",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Register a node"))
                .request_body(Some(json_body("Node registration", obj_schema())))
                .response("201", ok_json("Node registered", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/admin/routing",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Get routing table"))
                .response("200", ok_json("Routing table", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/admin/health",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Cluster health"))
                .response("200", ok_json("Cluster health status", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/admin/topology",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Cluster topology"))
                .response("200", ok_json("Full cluster topology", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/admin/backup",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Create backup"))
                .description(Some("Create a point-in-time backup of the database."))
                .request_body(Some(json_body("Backup target directory", obj_schema())))
                .response("200", ok_json("Backup manifest", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/admin/restore",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Restore from backup"))
                .description(Some("Restore a database from a backup directory."))
                .request_body(Some(json_body(
                    "Restore source and target directories",
                    obj_schema(),
                )))
                .response("200", ok_json("Restore manifest", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/admin/rebalance",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Trigger cluster rebalance"))
                .response("202", ok_json("Rebalance accepted", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/admin/log-level",
        PathItem::new(
            HttpMethod::Put,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Update log level"))
                .description(Some("Dynamically adjust the tracing log-level filter."))
                .request_body(Some(json_body("Log level filter", obj_schema())))
                .response("200", ok_json("Log level updated", obj_schema()))
                .build(),
        ),
    );

    // ── Connectors ─────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/connectors",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Connectors")
                .summary(Some("List ingestion connectors"))
                .response("200", ok_json("Active connectors", obj_schema()))
                .build(),
        ),
    );

    // ── OpenAPI ────────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/openapi.json",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Meta")
                .summary(Some("OpenAPI specification"))
                .description(Some("Returns this OpenAPI 3.1 JSON document."))
                .response("200", ok_json("OpenAPI spec", obj_schema()))
                .build(),
        ),
    );

    // ── Signal triggers ────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/triggers",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Triggers")
                .summary(Some("List the caller's triggers"))
                .response("200", ok_json("Triggers", obj_schema()))
                .response("404", ok_json("Triggers are not configured", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/triggers",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Triggers")
                .summary(Some("Run one trigger DSL statement"))
                .description(Some(
                    "CREATE TRIGGER, ALTER TRIGGER, DROP TRIGGER or SHOW TRIGGERS. \
                     Scoped to the caller's namespace.",
                ))
                .response("200", ok_json("Statement executed", obj_schema()))
                .response("400", ok_json("The statement was refused", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/triggers/{name}",
        PathItem::new(
            HttpMethod::Delete,
            OperationBuilder::new()
                .tag("Triggers")
                .summary(Some("Drop one of the caller's triggers"))
                .response("200", ok_json("Trigger dropped", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/signals",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Triggers")
                .summary(Some("Recently fired signals for the caller"))
                .response("200", ok_json("Signals", obj_schema()))
                .build(),
        ),
    );

    // ── Prometheus discovery ───────────────────────────────────────
    for (path, tag, summary, desc) in [
        (
            "/api/v1/prom/series",
            "Prometheus",
            "Series matching a selector",
            "The label sets present in the window, for each repeated `match[]` selector.",
        ),
        (
            "/api/v1/prom/labels",
            "Prometheus",
            "Label names",
            "Every label name in the window, narrowed by any `match[]` selectors.",
        ),
        (
            "/api/v1/prom/metadata",
            "Prometheus",
            "Metric metadata",
            "One entry per metric name — the shape Grafana's metric browser reads.",
        ),
        (
            "/api/v1/status/buildinfo",
            "Prometheus",
            "Build information",
            "Version and revision, in the shape a Prometheus client expects.",
        ),
        (
            "/api/v1/rules",
            "Prometheus",
            "Recording and alerting rules",
            "Always an empty group list: chronix has no rules, and its triggers are a \
             different feature with its own API.",
        ),
        (
            "/api/v1/alerts",
            "Prometheus",
            "Active alerts",
            "Always empty, for the same reason as `/api/v1/rules`.",
        ),
        (
            "/api/v1/query_exemplars",
            "Prometheus",
            "Exemplars",
            "Always empty: exemplars are not stored.",
        ),
    ] {
        paths = paths.path(
            path,
            PathItem::new(
                HttpMethod::Get,
                OperationBuilder::new()
                    .tag(tag)
                    .summary(Some(summary))
                    .description(Some(desc))
                    .response("200", ok_json("Prometheus response", obj_schema()))
                    .build(),
            ),
        );
    }
    paths = paths.path(
        "/api/v1/prom/label/{name}/values",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Prometheus")
                .summary(Some("Values of one label"))
                .description(Some(
                    "The values `name` takes in the window. `__name__` answers the metric \
                     names — `measurement_field`, or the measurement alone when the field \
                     is called `value`.",
                ))
                .parameter(
                    ParameterBuilder::new()
                        .name("name")
                        .parameter_in(ParameterIn::Path)
                        .required(utoipa::openapi::Required::True)
                        .build(),
                )
                .parameter(limit_param())
                .response("200", ok_json("Label values", obj_schema()))
                .build(),
        ),
    );

    // ── Rollups, annotations, dashboards ───────────────────────────
    paths = paths.path(
        "/api/v1/rollups/{name}",
        PathItem::new(
            HttpMethod::Delete,
            OperationBuilder::new()
                .tag("Rollups")
                .summary(Some("Drop a rollup rule"))
                .response("200", ok_json("Rollup dropped", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/rollups/{name}/refresh",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Rollups")
                .summary(Some("Materialise a rollup now"))
                .description(Some(
                    "Runs the rule's materialisation immediately instead of waiting for the \
                     next maintenance pass.",
                ))
                .response("200", ok_json("Refresh result", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/annotations",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Streaming")
                .summary(Some("Annotations for a time range"))
                .description(Some("Fired signals in the Grafana annotation shape."))
                .response("200", ok_json("Annotations", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/annotations/stream",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Streaming")
                .summary(Some("Annotation stream (SSE)"))
                .response("200", ok_empty("SSE stream"))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/dashboards/export",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Streaming")
                .summary(Some("Bundled Grafana dashboards"))
                .description(Some(
                    "The dashboards shipped with the server, as a Grafana provisioning payload.",
                ))
                .response("200", ok_json("Dashboards", obj_schema()))
                .build(),
        ),
    );

    // ── Namespaces ─────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/namespaces",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("List namespaces"))
                .response("200", ok_json("Namespaces", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/namespaces/{name}",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Read one namespace"))
                .response("200", ok_json("Namespace", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/namespaces/{name}/usage",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Namespace resource usage"))
                .response("200", ok_json("Usage", obj_schema()))
                .build(),
        ),
    );

    // ── Admin: keys, models, PITR ──────────────────────────────────
    paths = paths.path(
        "/api/v1/admin/auth/keys",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("List API keys"))
                .response("200", ok_json("Keys", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/admin/auth/keys/{name}",
        PathItem::new(
            HttpMethod::Delete,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Revoke an API key"))
                .response("200", ok_json("Revoked", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/admin/analytics/models",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("List fitted models"))
                .response("200", ok_json("Models", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/admin/analytics/models/{measurement}/{name}",
        PathItem::new(
            HttpMethod::Get,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Read or delete one fitted model"))
                .response("200", ok_json("Model", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/admin/analytics/retrain",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Refit a model now"))
                .response("200", ok_json("Retrain result", obj_schema()))
                .build(),
        ),
    );
    paths = paths.path(
        "/api/v1/admin/restore/pitr",
        PathItem::new(
            HttpMethod::Post,
            OperationBuilder::new()
                .tag("Admin")
                .summary(Some("Point-in-time restore"))
                .response("200", ok_json("Restore result", obj_schema()))
                .build(),
        ),
    );

    // ── Aliases ────────────────────────────────────────────────────
    //
    // A client derives these rather than being told them: a Prometheus
    // datasource appends `/api/v1/query` to its base URL, Telegraf and the
    // Influx clients post to `/write` or `/api/v2/write`, and the OTel
    // Collector's `otlphttp` exporter posts to `/v1/metrics`. They are the
    // paths real traffic arrives on, so a document that omits them describes
    // an API nobody calls. Cloned from the canonical entry rather than
    // rewritten, so the two cannot drift.
    let mut built = paths.build();
    for (alias, canonical) in [
        ("/api/v1/query", "/api/v1/prom/query"),
        ("/api/v1/query_range", "/api/v1/prom/query_range"),
        ("/api/v1/series", "/api/v1/prom/series"),
        ("/api/v1/labels", "/api/v1/prom/labels"),
        (
            "/api/v1/label/{name}/values",
            "/api/v1/prom/label/{name}/values",
        ),
        ("/api/v1/metadata", "/api/v1/prom/metadata"),
        ("/write", "/api/v1/write/influx"),
        ("/api/v2/write", "/api/v1/write/influx"),
        ("/v1/metrics", "/api/v1/otlp/metrics"),
    ] {
        if let Some(item) = built.paths.get(canonical).cloned() {
            built.paths.insert(alias.to_string(), item);
        }
    }

    OpenApiBuilder::new()
        .info(info)
        .servers(Some(vec![server]))
        .paths(built)
        .build()
}
