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

    /// Server backpressure — too many in-flight writes.
    #[error("server busy: {0}")]
    Backpressure(String),

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

    /// Write operation timed out.
    #[error("write timeout after {0:?}")]
    WriteTimeout(std::time::Duration),

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

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        // A full disk is the deployment's condition, not this server's bug,
        // so it is not a redacted 500. `507` is a 5xx, so retrying clients
        // still retry — the condition usually clears — and the message is
        // safe to show: "No space left on device" discloses nothing.
        if is_storage_full(&self) {
            tracing::error!(error = %self, "write failed: no space left on device");
            metrics::counter!("chronix_write_errors_total", "reason" => "storage_full")
                .increment(1);
            return (
                StatusCode::INSUFFICIENT_STORAGE,
                axum::Json(ErrorResponse {
                    error: format!("no space left on the data volume: {self}"),
                    code: "STORAGE_FULL",
                }),
            )
                .into_response();
        }

        let (status, code) = match &self {
            ServerError::BadRequest(_) => (StatusCode::BAD_REQUEST, "BAD_REQUEST"),
            ServerError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
            ServerError::Backpressure(_) => (StatusCode::SERVICE_UNAVAILABLE, "BACKPRESSURE"),
            ServerError::PartialWrite { .. } => (StatusCode::BAD_REQUEST, "PARTIAL_WRITE"),
            ServerError::Db(e) => match e {
                chronix::DbError::CardinalityExceeded { .. } => {
                    (StatusCode::BAD_REQUEST, "CARDINALITY_EXCEEDED")
                }
                chronix::DbError::Schema(_) => (StatusCode::BAD_REQUEST, "SCHEMA_ERROR"),
                chronix::DbError::Closed => (StatusCode::SERVICE_UNAVAILABLE, "DATABASE_CLOSED"),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "DATABASE_ERROR"),
            },
            ServerError::Config(_) => (StatusCode::INTERNAL_SERVER_ERROR, "CONFIG_ERROR"),
            ServerError::Tls(_) => (StatusCode::INTERNAL_SERVER_ERROR, "TLS_ERROR"),
            ServerError::Io(_) => (StatusCode::INTERNAL_SERVER_ERROR, "IO_ERROR"),
            ServerError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR"),
            ServerError::WriteTimeout(_) => (StatusCode::GATEWAY_TIMEOUT, "WRITE_TIMEOUT"),
            ServerError::Conflict(_) => (StatusCode::CONFLICT, "CONFLICT"),
            ServerError::Forbidden(_) => (StatusCode::FORBIDDEN, "FORBIDDEN"),
        };

        // For 5xx errors, redact internal details from the client response
        // and log the full error server-side.
        let error_msg = if status.is_server_error() {
            tracing::error!(code, error = %self, "server error");
            format!("{code}: an internal error occurred")
        } else {
            self.to_string()
        };

        let body = ErrorResponse {
            error: error_msg,
            code,
        };

        (status, axum::Json(body)).into_response()
    }
}

impl ServerError {
    /// Map to gRPC status code.
    ///
    /// Internal-class errors are redacted before being sent to clients
    /// to avoid leaking paths, stack traces, or implementation details.
    /// The full error is logged server-side for diagnostics.
    pub fn to_grpc_status(&self) -> tonic::Status {
        // Same reasoning as the HTTP mapping: a full disk is the deployment's
        // condition, `RESOURCE_EXHAUSTED` is what a gRPC client retries, and
        // the message is not redacted because it discloses nothing.
        if is_storage_full(self) {
            tracing::error!(error = %self, "write failed: no space left on device");
            return tonic::Status::resource_exhausted(format!(
                "no space left on the data volume: {self}"
            ));
        }
        match self {
            ServerError::BadRequest(msg) => tonic::Status::invalid_argument(msg),
            ServerError::NotFound(msg) => tonic::Status::not_found(msg),
            ServerError::Backpressure(msg) => tonic::Status::resource_exhausted(msg),
            ServerError::PartialWrite { .. } => tonic::Status::invalid_argument(self.to_string()),
            ServerError::Db(e) => match e {
                chronix::DbError::CardinalityExceeded { .. } => {
                    tonic::Status::resource_exhausted(e.to_string())
                }
                chronix::DbError::Schema(_) => tonic::Status::invalid_argument(e.to_string()),
                chronix::DbError::Closed => tonic::Status::unavailable("database is closed"),
                _ => {
                    tracing::error!(error = %e, "database error (gRPC)");
                    tonic::Status::internal("DATABASE_ERROR: an internal error occurred")
                }
            },
            ServerError::WriteTimeout(d) => {
                tonic::Status::deadline_exceeded(format!("write timeout after {d:?}"))
            }
            ServerError::Config(e) => {
                tracing::error!(error = %e, "config error (gRPC)");
                tonic::Status::internal("CONFIG_ERROR: an internal error occurred")
            }
            ServerError::Tls(e) => {
                tracing::error!(error = %e, "TLS error (gRPC)");
                tonic::Status::internal("TLS_ERROR: an internal error occurred")
            }
            ServerError::Io(e) => {
                tracing::error!(error = %e, "I/O error (gRPC)");
                tonic::Status::internal("IO_ERROR: an internal error occurred")
            }
            ServerError::Internal(msg) => {
                tracing::error!(error = %msg, "internal error (gRPC)");
                tonic::Status::internal("INTERNAL_ERROR: an internal error occurred")
            }
            ServerError::Conflict(msg) => tonic::Status::already_exists(msg),
            ServerError::Forbidden(msg) => tonic::Status::permission_denied(msg),
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

    #[test]
    fn backpressure_maps_to_503() {
        let err = ServerError::Backpressure("overloaded".into());
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
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

    #[test]
    fn grpc_backpressure_is_resource_exhausted() {
        let err = ServerError::Backpressure("busy".into());
        let status = err.to_grpc_status();
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
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
}
