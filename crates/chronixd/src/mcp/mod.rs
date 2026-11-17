//! The MCP server: chronix's read path as tools an AI agent can call.
//!
//! Two transports, one handler:
//!
//! - **`/mcp`** on the HTTP server (streamable HTTP). It sits inside the
//!   same authentication, namespace and Cedar layers as every route and maps
//!   to `Read`, so a tool sees exactly what the caller's key may read.
//! - **`chronixd mcp --data-dir DIR`** over stdio, against a directory opened
//!   read-only — for a directory copied off a device, asked questions on a
//!   laptop. A local process: the OS user is the authority.
//!
//! Every tool reads; none writes. The tools are adapters over calls the
//! HTTP API already makes (the `tools` module), and `forecast` and `detect_anomalies`
//! run in the engine.

pub mod time;
mod tools;

use std::sync::Arc;

use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation,
    JsonObject, ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool,
    ToolAnnotations,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData as McpError, RoleServer, ServerHandler};
use serde_json::Value;

use crate::http::AppState;
use crate::namespace::NamespaceContext;

pub use crate::config::MCP_TOOLS;

/// Read once, when an agent connects.
const INSTRUCTIONS: &str = "chronix is a time-series database. Start with list_metrics, then \
label_values to see how series are labelled, then query_promql or query_sql. forecast and \
detect_anomalies run in the database on one series; give labels when a metric has several. \
Times are \"now\", \"now-7d\", RFC 3339 or Unix seconds. An empty query result says why in \
`warnings`. Every tool only reads.";

/// One tool's definition: name, description, input schema.
fn define<A: rmcp::schemars::JsonSchema + 'static>(
    name: &'static str,
    description: &'static str,
) -> Tool {
    Tool::new(name, description, Arc::new(JsonObject::new()))
        .with_input_schema::<A>()
        .with_annotations(ToolAnnotations::new().read_only(true).open_world(false))
}

/// Every tool, in [`MCP_TOOLS`] order.
fn definitions() -> Vec<Tool> {
    vec![
        define::<tools::ListMetricsArgs>(
            "list_metrics",
            "List the metrics, with their type (gauge, counter, histogram) and the \
             measurement and field each one reads. Start here. Example: {\"match\": \"^pv_\"}",
        ),
        define::<tools::LabelValuesArgs>(
            "label_values",
            "List the values of one label, optionally only for one metric — how series are \
             told apart. Example: {\"label\": \"device\", \"metric\": \"pv_power_w\"}",
        ),
        define::<tools::QueryPromqlArgs>(
            "query_promql",
            "Run a PromQL query at one time, or over a range with start and end. An empty \
             result explains itself in `warnings`. Example: {\"query\": \
             \"avg_over_time(pv_power_w[1h])\", \"start\": \"now-1d\", \"end\": \"now\"}",
        ),
        define::<tools::QuerySqlArgs>(
            "query_sql",
            "Run a read-only SQL query. Tables are measurements, `_time` is the timestamp, \
             and forecast(), anomaly_score() and the other analytics functions are \
             available. Example: {\"query\": \"SELECT device, max(power_w) FROM pv GROUP BY \
             device\"}",
        ),
        define::<tools::ExplainArgs>(
            "explain",
            "Show how a PromQL or SQL query runs. With analyze, PromQL reports the series and \
             samples every sub-expression produced — the way to find the step that lost the \
             data. Example: {\"query\": \"sum(rate(m[5m]))\", \"language\": \"promql\", \
             \"analyze\": true}",
        ),
        define::<tools::CardinalityArgs>(
            "cardinality",
            "Count the series, show which metrics and labels hold the most, and how much of \
             the series budget is left. Example: {\"top\": 10}",
        ),
        define::<tools::ForecastArgs>(
            "forecast",
            "Forecast one series, in the database, with a prediction interval. The model is \
             chosen by cross-validation unless named. For windows longer than a day, pass \
             `step` so the fit sees the season; pass `min` for a quantity that cannot go \
             below it. Example: {\"metric\": \"pv_power_w\", \"start\": \"now-7d\", \
             \"step\": \"15m\", \"horizon\": 96, \"min\": 0}",
        ),
        define::<tools::DetectArgs>(
            "detect_anomalies",
            "Find the anomalous points of one series, in the database, strongest first. \
             Example: {\"metric\": \"heat_pump_power_w\", \"start\": \"now-7d\"}",
        ),
    ]
}

/// The MCP server for one chronixd, or one read-only data directory.
#[derive(Clone)]
pub struct ChronixMcp {
    state: AppState,
}

impl ChronixMcp {
    /// A server over `state`, offering `[mcp] tools`.
    #[must_use]
    pub fn new(state: AppState) -> Self {
        Self { state }
    }

    fn offered(&self, name: &str) -> bool {
        self.state.config.mcp.tools.iter().any(|t| t == name)
    }

    /// The caller's namespace, from the HTTP request the tool call arrived
    /// in; `None` over stdio.
    fn namespace(context: &RequestContext<RoleServer>) -> Option<NamespaceContext> {
        context
            .extensions
            .get::<axum::http::request::Parts>()
            .and_then(|parts| parts.extensions.get::<NamespaceContext>().cloned())
    }

    async fn call(&self, name: &str, args: Value, ns: Option<NamespaceContext>) -> CallToolResult {
        let ctx = tools::Ctx {
            state: self.state.clone(),
            ns,
            max_bytes: self.state.config.mcp.max_result_bytes,
        };
        macro_rules! run {
            ($f:path) => {
                match serde_json::from_value(args) {
                    Ok(a) => $f(&ctx, a).await,
                    Err(e) => Err(tools::ToolError {
                        message: format!("{name}: {e}"),
                        hint: Some("tools/list shows each tool's arguments".into()),
                    }),
                }
            };
        }
        let result = match name {
            "list_metrics" => run!(tools::list_metrics),
            "label_values" => run!(tools::label_values),
            "query_promql" => run!(tools::query_promql),
            "query_sql" => run!(tools::query_sql),
            "explain" => run!(tools::explain),
            "cardinality" => run!(tools::cardinality),
            "forecast" => run!(tools::forecast),
            "detect_anomalies" => run!(tools::detect_anomalies),
            other => Err(tools::ToolError {
                message: format!("there is no tool called {other:?}"),
                hint: None,
            }),
        };
        metrics::counter!(
            "chronix_mcp_tool_calls_total",
            "tool" => name.to_owned(),
            "outcome" => if result.is_ok() { "ok" } else { "error" }
        )
        .increment(1);
        match result {
            Ok(out) => {
                let mut r = CallToolResult::structured(out.structured);
                r.content = vec![ContentBlock::text(out.summary)];
                r
            }
            Err(e) => {
                let text = match &e.hint {
                    Some(h) => format!("{} — {h}", e.message),
                    None => e.message.clone(),
                };
                let mut r = CallToolResult::structured_error(
                    serde_json::json!({ "error": e.message, "hint": e.hint }),
                );
                r.content = vec![ContentBlock::text(text)];
                r
            }
        }
    }
}

impl ServerHandler for ChronixMcp {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("chronix", env!("CARGO_PKG_VERSION")))
            .with_instructions(INSTRUCTIONS)
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let tools = definitions()
            .into_iter()
            .filter(|t| self.offered(&t.name))
            .collect();
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        // Refused here as well as left out of the list: a client may call a
        // tool by name without listing.
        if !self.offered(&request.name) {
            return Err(McpError::invalid_params(
                format!("tool {:?} is not offered by this server", request.name),
                None,
            ));
        }
        let args = Value::Object(request.arguments.unwrap_or_default());
        let ns = Self::namespace(&context);
        Ok(self.call(&request.name, args, ns).await.into())
    }
}

/// The `/mcp` service for the HTTP router.
#[must_use]
pub fn http_service(
    state: AppState,
) -> rmcp::transport::StreamableHttpService<
    ChronixMcp,
    rmcp::transport::streamable_http_server::session::never::NeverSessionManager,
> {
    use rmcp::transport::streamable_http_server::StreamableHttpServerConfig;
    let hosts = match (&state.config.mcp.allowed_hosts, state.auth_state.is_some()) {
        (Some(hosts), _) => hosts.clone(),
        // Behind authentication a rebinding page has no key to send.
        (None, true) => Vec::new(),
        (None, false) => vec!["localhost".into(), "127.0.0.1".into(), "::1".into()],
    };
    let config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_allowed_hosts(hosts);
    let mcp = ChronixMcp::new(state);
    rmcp::transport::StreamableHttpService::new(move || Ok(mcp.clone()), Arc::default(), config)
}

/// Serve MCP over stdin and stdout until the client disconnects.
///
/// # Errors
///
/// The transport failed.
pub async fn serve_stdio(state: AppState) -> Result<(), Box<dyn std::error::Error>> {
    use rmcp::ServiceExt;
    let running = ChronixMcp::new(state)
        .serve(rmcp::transport::stdio())
        .await?;
    running.waiting().await?;
    Ok(())
}

/// Arguments of `chronixd mcp`.
#[derive(clap::Args, Debug)]
pub struct McpArgs {
    /// The data directory to answer from, opened read-only.
    #[arg(long, value_name = "DIR")]
    pub data_dir: std::path::PathBuf,

    /// Server configuration for the database settings (field-encryption
    /// keys, limits) the directory needs; its listen addresses are ignored.
    #[arg(long, short = 'c')]
    pub config: Option<std::path::PathBuf>,
}

impl McpArgs {
    /// Serve MCP over stdio from the directory until the client hangs up.
    ///
    /// Nothing in the directory is written: it is opened read-only, and one a
    /// running process holds is refused — connect to that server's `/mcp`.
    pub async fn run(&self) -> std::process::ExitCode {
        // stdout is the protocol; anything else goes to stderr.
        let _ = tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .try_init();
        match self.serve().await {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e}");
                std::process::ExitCode::FAILURE
            }
        }
    }

    async fn serve(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut config = match &self.config {
            Some(path) => crate::config::ServerConfig::from_toml(path)?,
            None => crate::config::ServerConfig::default(),
        };
        config.validate_mcp()?;
        config.database.data_dir.clone_from(&self.data_dir);
        // A local process sees every namespace in the directory.
        config.server.multi_tenancy = false;
        let mut db_config = config.to_chronix_config()?;
        db_config.read_only = true;
        let db = Arc::new(chronix::Chronix::open(db_config)?);
        let state = crate::http::SharedState::standalone(db.clone(), config);
        let served = serve_stdio(state).await;
        db.close()?;
        served
    }
}
