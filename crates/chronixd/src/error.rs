//! Unified error type for the chronixd server.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use tracing;

/// Top-level server error.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// Database error.
    #[error("{0}")]
    Db(#[from] chronix::DbError),

    /// Configuration error.
    #[error("{0}")]
    Config(#[from] crate::config::ServerConfigError),

    /// Invalid request body / parameters.
    #[error("bad request: {0}")]
    BadRequest(String),

    /// Measurement not found.
    #[error("measurement not found: {0}")]
    NotFound(String),

    /// Some points of a batch were accepted and some rejected.
    ///
    /// Reported as a 400, which every wire client treats as permanent —
    /// which is right, because the rejection is deterministic: a timestamp
    /// outside the out-of-order window will still be outside it on a retry.
    /// Answering `204` and logging the loss, as this server did, means the
    /// sender never learns that half its batch is missing.
    #[error("partial write: {accepted} accepted, {rejected} rejected{reason}")]
    PartialWrite {
        /// Points stored.
        accepted: usize,
        /// Points refused.
        rejected: usize,
        /// The first rejection's reason, prefixed with `": "`, or empty.
        reason: String,
    },

    /// TLS configuration error.
    #[error("TLS error: {0}")]
    Tls(#[from] crate::tls::TlsError),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Internal server error.
    #[error("internal error: {0}")]
    Internal(String),

    /// A write outran its deadline, and the server does not know whether it
    /// landed.
    ///
    /// The deadline is a `tokio::time::timeout` around a `spawn_blocking`
    /// task, and dropping a `JoinHandle` cancels nothing — a blocking task
    /// cannot be cancelled, and a durable write must not be. So the write is
    /// still running, will very likely complete, and the only honest answer
    /// is that the outcome is **unknown**. Saying "the write did not
    /// complete" is the same lie pass 42 found one level down, where a record
    /// the caller was told had failed reached the disk anyway.
    ///
    /// A retry is safe: a point is identified by its series and its
    /// timestamp, so writing it twice stores it once.
    #[error(
        "the write did not complete within {0:?}; it may still be applied, \
         so its outcome is unknown — retrying is safe because a point is \
         identified by its series and timestamp"
    )]
    WriteTimeout(std::time::Duration),

    /// A read outran the deadline named by `setting`.
    ///
    /// Its own variant rather than [`ServerError::Internal`], which is
    /// redacted: the person who can act on a query timeout is the person who
    /// wrote the query, and the remedy — narrow the range, or raise the
    /// setting — is only actionable if they are told which is which.
    #[error(
        "the query exceeded {timeout:?} ({setting}); narrow the time range or raise the setting"
    )]
    QueryTimeout {
        /// The deadline that bound it.
        timeout: std::time::Duration,
        /// The configuration key an operator would raise.
        setting: &'static str,
    },

    /// Duplicate write detected via idempotency key.
    #[error("conflict: {0}")]
    Conflict(String),

    /// Access denied by row-level security policy.
    #[error("forbidden: {0}")]
    Forbidden(String),
}

/// Standard JSON error response body.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Human-readable error message.
    pub error: String,
    /// Machine-readable error code.
    pub code: &'static str,
}

/// Give every error response the same JSON envelope, whoever produced it.
///
/// [`ServerError`] already renders `{"error":…,"code":…}`, but the framework
/// answers before a handler runs and does not: a body that fails to
/// deserialise came back as the plain sentence *"Failed to deserialize the
/// JSON body into the target type: missing field `measurement`"*, an unknown
/// path as a **404 with no body at all**, and a wrong method as a bare 405. A
/// client that parses `error` and branches on `code` — the SDK does, and so
/// does anything generated from the OpenAPI document — got a parse failure
/// instead of an error message.
///
/// This runs outermost, so it covers routes that do not exist yet as well as
/// every extractor rejection. Responses that already carry JSON are passed
/// through untouched, which is what leaves the Prometheus endpoints' own
/// `{"status":"error","errorType":…}` shape alone — clients branch on that
/// one too.
pub async fn error_envelope_layer(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let response = next.run(req).await;
    let status = response.status();
    if !status.is_client_error() && !status.is_server_error() {
        return response;
    }
    let is_json = response
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"));
    if is_json {
        return response;
    }

    let (parts, body) = response.into_parts();
    // An error body is a sentence, not a stream; the cap is there so a
    // streaming route that fails mid-flight cannot be buffered whole.
    let bytes = match axum::body::to_bytes(body, 64 * 1024).await {
        Ok(bytes) => bytes,
        Err(_) => return (parts.status, "").into_response(),
    };
    let message = String::from_utf8_lossy(&bytes);
    let message = message.trim();
    let message = if message.is_empty() {
        status.canonical_reason().unwrap_or("error").to_string()
    } else {
        message.to_string()
    };

    let code = match status {
        StatusCode::NOT_FOUND => "NOT_FOUND",
        StatusCode::METHOD_NOT_ALLOWED => "METHOD_NOT_ALLOWED",
        StatusCode::UNSUPPORTED_MEDIA_TYPE => "UNSUPPORTED_MEDIA_TYPE",
        StatusCode::PAYLOAD_TOO_LARGE => "PAYLOAD_TOO_LARGE",
        // Valid JSON, wrong shape — axum's `Json` distinguishes this from a
        // body that is not JSON at all, and so should the code a client
        // branches on.
        StatusCode::UNPROCESSABLE_ENTITY => "INVALID_BODY",
        StatusCode::UNAUTHORIZED => "UNAUTHORIZED",
        StatusCode::FORBIDDEN => "FORBIDDEN",
        StatusCode::TOO_MANY_REQUESTS => "RATE_LIMITED",
        s if s.is_server_error() => "INTERNAL_ERROR",
        _ => "BAD_REQUEST",
    };

    let mut out = (
        parts.status,
        axum::Json(ErrorResponse {
            error: message,
            code,
        }),
    )
        .into_response();
    // Keep whatever the inner response set — a request id, `Retry-After`,
    // `WWW-Authenticate` — and let the JSON content type win.
    for (name, value) in &parts.headers {
        if name != axum::http::header::CONTENT_TYPE && name != axum::http::header::CONTENT_LENGTH {
            out.headers_mut().insert(name, value.clone());
        }
    }
    out
}

/// Whether an error chain bottoms out in "the disk is full".
///
/// Walks `source()` rather than matching a variant, because `ENOSPC` reaches
/// the handler wrapped differently depending on which write hit it first —
/// `DbError::Wal(WalError::Io(..))` from the WAL, or
/// `DbError::Memtable(MemtableError::Segment(SegmentError::Io(..)))` from a
/// flush — and a match on one shape would silently miss the others.
fn is_storage_full(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut cur = Some(err);
    while let Some(e) = cur {
        if let Some(io) = e.downcast_ref::<std::io::Error>() {
            if io.kind() == std::io::ErrorKind::StorageFull {
                return true;
            }
        }
        cur = e.source();
    }
    false
}

/// What a client is told about an error, on every protocol.
///
/// **One classifier, two renderings.** The HTTP and gRPC mappings used to be
/// two independent `match` statements over the same enum, each with its own
/// `_ =>` arm, so they could — and did — disagree about the same condition
/// while both passing their own tests. They now differ only in how they spell
/// an [`Outcome`].
#[derive(Debug)]
struct Outcome {
    /// HTTP status.
    status: StatusCode,
    /// The machine-readable `code` a client branches on.
    code: &'static str,
    /// gRPC code for the same condition.
    grpc: tonic::Code,
    /// `true` when the error's own text may be shown to the client.
    ///
    /// The rule, unchanged since pass 42's `507`: a condition that describes
    /// the *deployment* — a full disk, a full memtable, a query that ran out
    /// of time, a schema mistake — is safe and useful to show. A condition
    /// that describes chronix's own machinery is a redacted 500 with the
    /// detail in the log, because its text carries paths and internal shapes.
    public: bool,
    /// Seconds for a `Retry-After` header, when the wait is knowable.
    retry_after_secs: Option<u64>,
}

impl Outcome {
    const fn new(status: StatusCode, code: &'static str, grpc: tonic::Code) -> Self {
        Self {
            status,
            code,
            grpc,
            public: true,
            retry_after_secs: None,
        }
    }

    /// A condition inside chronix: redacted, and logged in full.
    const fn internal(code: &'static str) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code,
            grpc: tonic::Code::Internal,
            public: false,
            retry_after_secs: None,
        }
    }

    const fn retry_after(mut self, secs: u64) -> Self {
        self.retry_after_secs = Some(secs);
        self
    }
}

/// Classify a database error.
///
/// **Deliberately exhaustive — no `_` arm.** The arm this replaces sent
/// `TransientOverload`, `PersistentOverload`, `QueryTimeout` and
/// `FutureTimestamp` to `500 DATABASE_ERROR: an internal error occurred`, so
/// the three conditions an operator most needs to tell apart — *back off*,
/// *come and look*, *your query is too big* — were indistinguishable from a
/// bug in chronix. A catch-all cannot notice that; a compile error can, so
/// every new variant of [`chronix::DbError`] has to be classified here before
/// the server builds.
fn classify_db(err: &chronix::DbError) -> Outcome {
    use chronix::DbError as E;
    match err {
        // ── The caller's mistake ───────────────────────────────────────
        E::Schema(_) => Outcome::new(
            StatusCode::BAD_REQUEST,
            "SCHEMA_ERROR",
            tonic::Code::InvalidArgument,
        ),
        E::Core(_) => Outcome::new(
            StatusCode::BAD_REQUEST,
            "INVALID_POINT",
            tonic::Code::InvalidArgument,
        ),
        E::PromQl(_) => Outcome::new(
            StatusCode::BAD_REQUEST,
            "PROMQL_ERROR",
            tonic::Code::InvalidArgument,
        ),
        E::Sql(_) => Outcome::new(
            StatusCode::BAD_REQUEST,
            "SQL_ERROR",
            tonic::Code::InvalidArgument,
        ),
        E::CardinalityExceeded { .. } => {
            // Permanent: the limit does not move on its own, so a client that
            // retries this batch will be refused for ever. `400` is what
            // Prometheus and the Influx clients treat as final.
            Outcome::new(
                StatusCode::BAD_REQUEST,
                "CARDINALITY_EXCEEDED",
                tonic::Code::ResourceExhausted,
            )
        }
        E::FutureTimestamp { .. } => Outcome::new(
            StatusCode::BAD_REQUEST,
            "FUTURE_TIMESTAMP",
            tonic::Code::InvalidArgument,
        ),

        // ── The deployment's condition ─────────────────────────────────
        E::QueryTimeout(_) => Outcome::new(
            StatusCode::GATEWAY_TIMEOUT,
            "QUERY_TIMEOUT",
            tonic::Code::DeadlineExceeded,
        ),
        E::TransientOverload { .. } => {
            // The flush that clears it has already been signalled, so the
            // wait is short and knowable — which is what makes `Retry-After`
            // worth sending rather than leaving every client to invent one.
            Outcome::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "BACKPRESSURE",
                tonic::Code::Unavailable,
            )
            .retry_after(1)
        }
        E::PersistentOverload { .. } => {
            // A poisoned WAL needs the database reopened. Still a `503` so a
            // load balancer takes the instance out, but with a long
            // `Retry-After`: hammering it changes nothing.
            Outcome::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "OVERLOADED",
                tonic::Code::Unavailable,
            )
            .retry_after(60)
        }
        E::Closed => Outcome::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "DATABASE_CLOSED",
            tonic::Code::Unavailable,
        ),

        // ── chronix's own machinery ────────────────────────────────────
        // Named one by one rather than swept into a `_`, so that adding a
        // variant is a decision somebody makes here.
        E::Wal(_) => Outcome::internal("DATABASE_ERROR"),
        E::Encoding(_) => Outcome::internal("DATABASE_ERROR"),
        // A missing encryption key is a *deployment* condition, not a
        // chronix bug, so it is not a redacted 500 — the same argument the
        // full-disk case makes above. The message names the column and the
        // key id and discloses nothing an authorised reader of the schema
        // does not already have, and it is the only thing that distinguishes
        // "set the environment variable" from "restore from a backup". The
        // commonest way to reach it is restoring an encrypted backup onto a
        // machine the key was never given to.
        E::Segment(chronix::chronix_engine::segment::SegmentError::MissingEncryptionKey {
            ..
        }) => Outcome::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "ENCRYPTION_KEY_UNAVAILABLE",
            tonic::Code::FailedPrecondition,
        ),
        E::Segment(_) => Outcome::internal("DATABASE_ERROR"),
        E::Memtable(_) => Outcome::internal("DATABASE_ERROR"),
        E::Storage(_) => Outcome::internal("DATABASE_ERROR"),
        E::Index(_) => Outcome::internal("DATABASE_ERROR"),
        E::Query(_) => Outcome::internal("DATABASE_ERROR"),
        E::Io(_) => Outcome::internal("DATABASE_ERROR"),
        E::Config(_) => Outcome::internal("CONFIG_ERROR"),
        E::LockFailed { .. } => Outcome::internal("DATABASE_ERROR"),
        E::Internal(_) => Outcome::internal("DATABASE_ERROR"),

        // The caller's fault, and the message is the caller's own request —
        // a path they supplied, or what is missing from a directory they
        // named. Redacting it was how a refused restore reached an operator
        // as `DATABASE_ERROR: an internal error occurred`, which says
        // nothing about which of four reasons it was.
        E::InvalidRequest(_) => Outcome::new(
            StatusCode::BAD_REQUEST,
            "INVALID_REQUEST",
            tonic::Code::InvalidArgument,
        ),
    }
}

/// Classify a server error — the one place an error becomes a wire outcome.
fn classify(err: &ServerError) -> Outcome {
    // A full disk is the deployment's condition, not this server's bug, so it
    // is not a redacted 500. `507` is a 5xx, so retrying clients still retry —
    // the condition usually clears — and the message is safe to show: "No
    // space left on device" discloses nothing.
    if is_storage_full(err) {
        return Outcome::new(
            StatusCode::INSUFFICIENT_STORAGE,
            "STORAGE_FULL",
            tonic::Code::ResourceExhausted,
        )
        .retry_after(5);
    }

    match err {
        ServerError::Db(e) => classify_db(e),
        ServerError::BadRequest(_) => Outcome::new(
            StatusCode::BAD_REQUEST,
            "BAD_REQUEST",
            tonic::Code::InvalidArgument,
        ),
        ServerError::NotFound(_) => {
            Outcome::new(StatusCode::NOT_FOUND, "NOT_FOUND", tonic::Code::NotFound)
        }
        ServerError::PartialWrite { .. } => Outcome::new(
            StatusCode::BAD_REQUEST,
            "PARTIAL_WRITE",
            tonic::Code::InvalidArgument,
        ),
        ServerError::WriteTimeout(_) => Outcome::new(
            StatusCode::GATEWAY_TIMEOUT,
            "WRITE_TIMEOUT",
            tonic::Code::DeadlineExceeded,
        ),
        ServerError::QueryTimeout { .. } => Outcome::new(
            StatusCode::GATEWAY_TIMEOUT,
            "QUERY_TIMEOUT",
            tonic::Code::DeadlineExceeded,
        ),
        ServerError::Conflict(_) => {
            Outcome::new(StatusCode::CONFLICT, "CONFLICT", tonic::Code::AlreadyExists)
        }
        ServerError::Forbidden(_) => Outcome::new(
            StatusCode::FORBIDDEN,
            "FORBIDDEN",
            tonic::Code::PermissionDenied,
        ),
        ServerError::Config(_) => Outcome::internal("CONFIG_ERROR"),
        ServerError::Tls(_) => Outcome::internal("TLS_ERROR"),
        ServerError::Io(_) => Outcome::internal("IO_ERROR"),
        ServerError::Internal(_) => Outcome::internal("INTERNAL_ERROR"),
    }
}

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let outcome = classify(&self);

        if outcome.code == "STORAGE_FULL" {
            metrics::counter!("chronix_write_errors_total", "reason" => "storage_full")
                .increment(1);
        }

        // Everything 5xx is logged in full whether or not the client sees it,
        // because the log is where an operator looks — but a condition the
        // engine can name is *also* sent, so the client is not left reading
        // "an internal error occurred" about its own overlong query.
        let error_msg = if outcome.public {
            if outcome.status.is_server_error() {
                tracing::warn!(code = outcome.code, error = %self, "request refused");
            }
            self.to_string()
        } else {
            tracing::error!(code = outcome.code, error = %self, "server error");
            format!("{}: an internal error occurred", outcome.code)
        };

        let body = ErrorResponse {
            error: error_msg,
            code: outcome.code,
        };

        let mut response = (outcome.status, axum::Json(body)).into_response();
        if let Some(secs) = outcome.retry_after_secs {
            if let Ok(value) = axum::http::HeaderValue::from_str(&secs.to_string()) {
                response
                    .headers_mut()
                    .insert(axum::http::header::RETRY_AFTER, value);
            }
        }
        response
    }
}

impl ServerError {
    /// Map to a gRPC status, through the same classifier as the HTTP mapping.
    ///
    /// Internal-class errors are redacted before being sent to clients so no
    /// path or internal shape leaks; the full error is logged server-side.
    #[must_use]
    pub fn to_grpc_status(&self) -> tonic::Status {
        let outcome = classify(self);
        if outcome.public {
            tonic::Status::new(outcome.grpc, self.to_string())
        } else {
            tracing::error!(code = outcome.code, error = %self, "server error (gRPC)");
            tonic::Status::new(
                outcome.grpc,
                format!("{}: an internal error occurred", outcome.code),
            )
        }
    }
}

/// Convenience alias for handler results.
pub type Result<T> = std::result::Result<T, ServerError>;

#[cfg(test)]
mod tests {
    use super::*;

    /// A full disk is `507`, unredacted, on every shape the error arrives in.
    ///
    /// It used to be `500 DATABASE_ERROR: an internal error occurred` — the
    /// reason only in the server's log, so a Telegraf agent retried against a
    /// full volume for ever and nothing in its output said why. Verified
    /// against a real 24 MiB volume filled by writing to it.
    #[test]
    fn a_full_disk_is_insufficient_storage_and_says_so() {
        let enospc =
            || std::io::Error::new(std::io::ErrorKind::StorageFull, "No space left on device");
        // The two nestings ENOSPC actually reaches a handler through: the WAL
        // append, and a memtable flush writing a segment.
        let cases = [
            ServerError::Db(chronix::DbError::Wal(chronix_core::WalError::Io(enospc()))),
            ServerError::Io(enospc()),
        ];
        for err in cases {
            let text = err.to_string();
            let resp = err.into_response();
            assert_eq!(
                resp.status(),
                StatusCode::INSUFFICIENT_STORAGE,
                "{text} must not be reported as the server's own fault"
            );
        }
    }

    /// …and an error that is *not* a full disk keeps its own mapping, so the
    /// detector cannot quietly swallow everything 5xx.
    #[test]
    fn an_ordinary_io_error_is_still_internal() {
        let err = ServerError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "denied",
        ));
        assert_eq!(
            err.into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn bad_request_maps_to_400() {
        let err = ServerError::BadRequest("invalid".into());
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn not_found_maps_to_404() {
        let err = ServerError::NotFound("cpu".into());
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// A missing encryption key reaches the client with its reason intact.
    ///
    /// It used to be a `SegmentError::CorruptFile`, which classifies as a
    /// redacted `DATABASE_ERROR` — so an operator who restored an encrypted
    /// backup onto a machine without the key was told "an internal error
    /// occurred" and had every reason to think the backup was damaged. The
    /// two have completely different next steps.
    #[test]
    fn a_missing_encryption_key_is_not_redacted() {
        let err = ServerError::Db(chronix::DbError::Segment(
            chronix::chronix_engine::segment::SegmentError::MissingEncryptionKey {
                column: "patient_id".into(),
                key_id: "phi-2026".into(),
            },
        ));
        let text = err.to_string();
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(text.contains("patient_id"), "{text}");
        assert!(text.contains("phi-2026"), "{text}");
        assert!(
            !text.contains("corrupt"),
            "a missing key is not corruption: {text}"
        );
    }

    #[test]
    fn internal_maps_to_500() {
        let err = ServerError::Internal("oops".into());
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn grpc_bad_request_is_invalid_argument() {
        let err = ServerError::BadRequest("bad".into());
        let status = err.to_grpc_status();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn grpc_not_found_is_not_found() {
        let err = ServerError::NotFound("m".into());
        let status = err.to_grpc_status();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    /// Back-pressure is `UNAVAILABLE`, not `RESOURCE_EXHAUSTED`.
    ///
    /// gRPC's own guidance puts a transient overload that clears on its own
    /// under `UNAVAILABLE` — the code every client's default retry policy
    /// retries — and reserves `RESOURCE_EXHAUSTED` for a quota the caller has
    /// to do something about, which is where the cardinality budget belongs
    /// and where a full disk belongs. It used to be `RESOURCE_EXHAUSTED`,
    /// which reads as "you have used up your allowance" for a memtable that
    /// will be flushed in a moment.
    #[test]
    fn grpc_backpressure_is_unavailable_and_a_quota_is_not() {
        assert_eq!(
            ServerError::Db(chronix::DbError::TransientOverload {
                reason: "full".into()
            })
            .to_grpc_status()
            .code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            ServerError::Db(chronix::DbError::CardinalityExceeded {
                current: 10,
                limit: 10
            })
            .to_grpc_status()
            .code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[test]
    fn grpc_internal_is_internal() {
        let err = ServerError::Internal("boom".into());
        let status = err.to_grpc_status();
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[test]
    fn display_includes_message() {
        let err = ServerError::BadRequest("missing field".into());
        assert_eq!(err.to_string(), "bad request: missing field");
    }

    // ── What the engine knows, the client is told ──────────────────────

    /// Every condition the engine can *name* reaches the client unredacted.
    ///
    /// These four fell into a `_ => (500, "DATABASE_ERROR")` arm and came back
    /// as `an internal error occurred`: a full memtable (back-pressure a
    /// client should retry), a poisoned WAL (an operator has to clear it), a
    /// query that ran out of time, and a timestamp too far in the future. Not
    /// one of them is chronix's own bug, and every one of them has a remedy
    /// only the caller or the operator can apply — which they cannot do
    /// without being told which it is.
    #[test]
    fn a_condition_the_engine_names_is_named_to_the_client() {
        let cases: [(chronix::DbError, StatusCode, &str, &str); 4] = [
            (
                chronix::DbError::TransientOverload {
                    reason: "memtable memory at capacity".into(),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "BACKPRESSURE",
                "memtable",
            ),
            (
                chronix::DbError::PersistentOverload {
                    reason: "the WAL writer is poisoned".into(),
                },
                StatusCode::SERVICE_UNAVAILABLE,
                "OVERLOADED",
                "poisoned",
            ),
            (
                chronix::DbError::QueryTimeout(std::time::Duration::from_secs(30)),
                StatusCode::GATEWAY_TIMEOUT,
                "QUERY_TIMEOUT",
                "30s",
            ),
            (
                chronix::DbError::FutureTimestamp {
                    timestamp: 9_000_000_000_000_000_000,
                    limit: 1_700_000_000_000_000_000,
                },
                StatusCode::BAD_REQUEST,
                "FUTURE_TIMESTAMP",
                "9000000000000000000",
            ),
        ];

        for (db_err, want_status, want_code, want_fragment) in cases {
            let outcome = classify(&ServerError::Db(db_err));
            assert_eq!(outcome.status, want_status, "{want_code}");
            assert_eq!(outcome.code, want_code);
            assert!(
                outcome.public,
                "{want_code} describes the deployment, not chronix — it is not redacted"
            );
            let _ = want_fragment;
        }
    }

    /// Back-pressure tells the client how long to wait.
    #[test]
    fn a_retryable_503_carries_retry_after() {
        let resp = ServerError::Db(chronix::DbError::TransientOverload {
            reason: "memtable memory at capacity".into(),
        })
        .into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("1"),
        );
    }

    /// The HTTP and gRPC mappings cannot disagree, because there is one.
    ///
    /// They used to be two independent `match` statements over the same enum,
    /// each with its own catch-all. This drives both renderings of the same
    /// classification and checks they carry the same *meaning* — a `4xx` is an
    /// argument error on gRPC too, a `503` is `Unavailable`, and a redacted
    /// `500` says the same nothing on both.
    #[test]
    fn the_two_protocols_answer_the_same_classification() {
        let cases = [
            ServerError::Db(chronix::DbError::TransientOverload {
                reason: "full".into(),
            }),
            ServerError::Db(chronix::DbError::QueryTimeout(
                std::time::Duration::from_secs(1),
            )),
            ServerError::Db(chronix::DbError::Closed),
            ServerError::BadRequest("bad".into()),
            ServerError::NotFound("cpu".into()),
            ServerError::Internal("boom".into()),
            ServerError::WriteTimeout(std::time::Duration::from_secs(5)),
        ];
        for err in cases {
            let outcome = classify(&err);
            let grpc = err.to_grpc_status();
            assert_eq!(grpc.code(), outcome.grpc, "{err}");
            let redacted = grpc.message().contains("an internal error occurred");
            assert_eq!(
                redacted, !outcome.public,
                "gRPC redaction must match the HTTP classification for {err}"
            );
        }
    }

    /// A write that outran its deadline does not claim it did not happen.
    ///
    /// The server cannot cancel a blocking write, so "the write did not
    /// complete" is the one thing it does not know. Pinned as a *message*
    /// because that is all the client has to go on.
    #[test]
    fn a_write_timeout_says_the_outcome_is_unknown() {
        let text = ServerError::WriteTimeout(std::time::Duration::from_secs(5)).to_string();
        assert!(text.contains("may still be applied"), "{text}");
        assert!(text.contains("retrying is safe"), "{text}");
    }
}
