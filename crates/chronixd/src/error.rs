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

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            ServerError::BadRequest(_) => (StatusCode::BAD_REQUEST, "BAD_REQUEST"),
            ServerError::NotFound(_) => (StatusCode::NOT_FOUND, "NOT_FOUND"),
            ServerError::Backpressure(_) => (StatusCode::SERVICE_UNAVAILABLE, "BACKPRESSURE"),
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
        match self {
            ServerError::BadRequest(msg) => tonic::Status::invalid_argument(msg),
            ServerError::NotFound(msg) => tonic::Status::not_found(msg),
            ServerError::Backpressure(msg) => tonic::Status::resource_exhausted(msg),
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
