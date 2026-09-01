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
    if let Some(url) = &config.public_url {
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
                .response("204", ok_empty("Points written successfully"))
                .response("400", ok_json("Bad request", obj_schema()))
                .build(),
        ),
    );

    // ── Query ──────────────────────────────────────────────────────
    paths = paths.path(
        "/api/v1/query",
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
        "/api/v1/query/explain",
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
        "/api/v1/sql",
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

    OpenApiBuilder::new()
        .info(info)
        .servers(Some(vec![server]))
        .paths(paths.build())
        .build()
}
