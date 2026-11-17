//! REST admin API handlers for cluster management and analytics.
//!
//! Provides HTTP endpoints under `/api/v1/admin/` that mirror the gRPC
//! admin service.  Cluster operations delegate to the `MetaClient` trait,
//! which is backed by either an in-process Raft client (meta-node) or a
//! remote gRPC client (data-node).
//!
//! Analytics model management endpoints allow listing, inspecting, and
//! deleting fitted forecast models, plus triggering model re-training.
//!

use std::sync::Arc;

use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::error::ServerError;
use crate::http::AppState;

// ── Request / Response types ──────────────────────────────────────────

/// Generic acknowledgement response.
#[derive(Debug, Serialize)]
pub struct AckResponse {
    /// Whether the operation succeeded.
    pub ok: bool,
    /// Optional descriptive message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

// ── Helper ────────────────────────────────────────────────────────────

// ── Handlers ──────────────────────────────────────────────────────────

// ── Cluster Topology ──────────────────────────────────────────

// ── Analytics Model Management ────────────────────────────────────────

/// Response for a single model's metadata.
#[derive(Debug, Serialize)]
pub struct ModelInfoResponse {
    /// Model name.
    pub name: String,
    /// Measurement this model is trained on.
    pub measurement: String,
    /// Model type (SES, HoltLinear, HoltWinters, etc.).
    pub model_type: String,
    /// Model version (auto-incremented on re-save).
    pub version: u32,
    /// Training data point count.
    pub training_points: usize,
    /// Mean squared error on training data.
    pub mse: f64,
    /// Mean absolute error on training data.
    pub mae: f64,
    /// R-squared on training data.
    pub r_squared: f64,
}

/// Response for model listing.
#[derive(Debug, Serialize)]
pub struct ModelListResponse {
    /// All models matching the query.
    pub models: Vec<ModelInfoResponse>,
    /// Number of models returned.
    pub count: usize,
}

/// Request body for `POST /api/v1/admin/analytics/retrain`.
#[derive(Debug, Deserialize)]
pub struct RetrainRequest {
    /// Measurement to retrain models for.
    pub measurement: String,
    /// Optional: specific model name. If absent, all models for the
    /// measurement are retrained.
    #[serde(default)]
    pub model_name: Option<String>,
}

/// Response body for retrain.
#[derive(Debug, Serialize)]
pub struct RetrainResponse {
    /// Whether the retrain was accepted.
    pub accepted: bool,
    /// Number of models queued for retraining.
    pub models_queued: usize,
    /// Descriptive message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// `GET /api/v1/admin/analytics/models` — list all trained models.
///
/// Optional query parameter `?measurement=cpu` to filter by measurement.
pub async fn list_models_handler(
    State(state): State<AppState>,
    query: axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<impl IntoResponse, ServerError> {
    let catalog = state.model_catalog.read();

    let models: Vec<ModelInfoResponse> = if let Some(measurement) = query.get("measurement") {
        catalog
            .list_models(measurement)
            .into_iter()
            .map(model_metadata_to_response)
            .collect()
    } else {
        catalog
            .list_all()
            .into_iter()
            .map(model_metadata_to_response)
            .collect()
    };

    let count = models.len();
    Ok(Json(ModelListResponse { models, count }))
}

/// `GET /api/v1/admin/analytics/models/{measurement}/{name}` — get
/// metadata for a specific model.
pub async fn get_model_handler(
    State(state): State<AppState>,
    Path((measurement, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ServerError> {
    let catalog = state.model_catalog.read();
    let meta = catalog
        .get_metadata(&measurement, &name)
        .ok_or_else(|| ServerError::NotFound(format!("model {measurement}/{name} not found")))?;
    Ok(Json(model_metadata_to_response(meta)))
}

/// `DELETE /api/v1/admin/analytics/models/{measurement}/{name}` — delete
/// a model from the catalog.
pub async fn delete_model_handler(
    State(state): State<AppState>,
    Path((measurement, name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ServerError> {
    let mut catalog = state.model_catalog.write();
    if catalog.delete_model(&measurement, &name) {
        info!(
            measurement = %measurement,
            model = %name,
            "audit: model deleted"
        );
        Ok(Json(AckResponse {
            ok: true,
            message: Some(format!("model {measurement}/{name} deleted")),
        }))
    } else {
        Err(ServerError::NotFound(format!(
            "model {measurement}/{name} not found"
        )))
    }
}

/// `POST /api/v1/admin/analytics/retrain` — trigger model re-training.
///
/// This is a best-effort trigger.  The actual retraining happens
/// asynchronously via the `ContinuousForecastEngine`.  This endpoint
/// returns immediately with the count of models queued for retraining.
pub async fn retrain_handler(
    State(state): State<AppState>,
    Json(body): Json<RetrainRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let catalog = state.model_catalog.read();
    let models = if let Some(ref name) = body.model_name {
        catalog
            .get_metadata(&body.measurement, name)
            .map_or_else(Vec::new, |m| vec![m.model_name.clone()])
    } else {
        catalog
            .list_models(&body.measurement)
            .into_iter()
            .map(|m| m.model_name.clone())
            .collect()
    };
    drop(catalog);

    let queued = models.len();
    if queued == 0 {
        return Ok((
            StatusCode::OK,
            Json(RetrainResponse {
                accepted: false,
                models_queued: 0,
                message: Some("no matching models found".into()),
            }),
        ));
    }

    // Mark models for retraining by deleting them from the catalog.
    // The ContinuousForecastEngine will re-fit on the next data batch.
    let mut catalog = state.model_catalog.write();
    for name in &models {
        catalog.delete_model(&body.measurement, name);
    }

    info!(
        measurement = %body.measurement,
        models_queued = queued,
        "audit: model retraining triggered"
    );

    Ok((
        StatusCode::ACCEPTED,
        Json(RetrainResponse {
            accepted: true,
            models_queued: queued,
            message: Some(format!(
                "{queued} model(s) cleared for retraining on '{}'",
                body.measurement
            )),
        }),
    ))
}

fn model_metadata_to_response(
    meta: &chronix::chronix_analytics::forecast::ModelMetadata,
) -> ModelInfoResponse {
    ModelInfoResponse {
        name: meta.model_name.clone(),
        measurement: meta.measurement.clone(),
        model_type: format!("{:?}", meta.model_type),
        version: meta.version,
        training_points: meta.training_points,
        mse: meta.mse,
        mae: meta.mae,
        r_squared: meta.r_squared,
    }
}

// ── Backup / Restore ──────────────────────────────────────────────────

/// The directory every admin filesystem path is confined to.
///
/// Configurable, because a backup usually belongs on a different volume
/// from the data. Defaults to `backups/` beside the data.
fn admin_root(state: &AppState) -> std::path::PathBuf {
    state
        .config
        .server
        .backup_root
        .clone()
        .unwrap_or_else(|| state.db.data_dir().join("backups"))
}

/// Resolve a caller-supplied backup path inside `root`.
///
/// The old check rejected `..` and demanded an absolute path, and an
/// absolute path was then accepted **anywhere on the filesystem** — so a
/// restore read any directory the server user could read, and a backup
/// wrote over any directory it could write. Rejecting `..` is not
/// confinement; a root is.
///
/// A relative path is taken as relative to `root`. An absolute path is
/// accepted only when it is already inside `root`. Either way the deepest
/// existing ancestor is canonicalised before the check, so a symlink
/// planted inside `root` cannot point out of it.
fn confine_to_root(
    user_path: &str,
    root: &std::path::Path,
) -> Result<std::path::PathBuf, ServerError> {
    let requested = std::path::PathBuf::from(user_path);

    for component in requested.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(ServerError::BadRequest(
                "path must not contain '..' components".into(),
            ));
        }
    }

    // The root has to exist before it can anchor anything; creating it is
    // part of serving the endpoint, not a caller's job.
    std::fs::create_dir_all(root).map_err(|e| {
        ServerError::Internal(format!(
            "cannot create the backup root {}: {e}",
            root.display()
        ))
    })?;
    let canonical_root = root.canonicalize().map_err(|e| {
        ServerError::Internal(format!(
            "cannot resolve the backup root {}: {e}",
            root.display()
        ))
    })?;

    let candidate = if requested.is_absolute() {
        requested
    } else {
        canonical_root.join(&requested)
    };

    // Canonicalise the deepest ancestor that exists — the leaf usually does
    // not yet, since a restore target must be new — then re-attach the
    // remainder. Checking the string alone would miss a symlink.
    let mut existing = candidate.as_path();
    let mut tail = std::path::PathBuf::new();
    let resolved = loop {
        match existing.canonicalize() {
            Ok(base) => break base.join(&tail),
            Err(_) => match existing.parent() {
                Some(parent) => {
                    let name = existing
                        .file_name()
                        .ok_or_else(|| ServerError::BadRequest("invalid path".into()))?;
                    tail = std::path::PathBuf::from(name).join(&tail);
                    existing = parent;
                }
                None => {
                    return Err(ServerError::BadRequest(
                        "path does not resolve inside the backup root".into(),
                    ));
                }
            },
        }
    };

    if !resolved.starts_with(&canonical_root) {
        return Err(ServerError::BadRequest(format!(
            "path must stay inside the backup root {}",
            canonical_root.display()
        )));
    }

    Ok(resolved)
}

/// Request body for `POST /api/v1/admin/backup`.
#[derive(Debug, Deserialize)]
pub struct BackupRequest {
    /// Target directory for the backup.
    pub target_dir: String,
}

/// Request body for `POST /api/v1/admin/restore`.
#[derive(Debug, Deserialize)]
pub struct RestoreRequest {
    /// Directory containing the backup.
    pub backup_dir: String,
    /// Target directory to restore into (must not exist).
    pub target_dir: String,
}

/// `POST /api/v1/admin/backup` — create a point-in-time backup.
pub async fn backup_handler(
    State(state): State<AppState>,
    Json(body): Json<BackupRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let target = confine_to_root(&body.target_dir, &admin_root(&state))?;
    let manifest = tokio::task::spawn_blocking({
        let db = Arc::clone(&state.db);
        move || db.backup(&target)
    })
    .await
    .map_err(|e| ServerError::Internal(format!("backup task panicked: {e}")))?
    .map_err(ServerError::Db)?;

    info!(
        target_dir = %body.target_dir,
        files = manifest.file_count,
        bytes = manifest.total_bytes,
        wal_seq = manifest.wal_sequence,
        "audit: backup created"
    );
    crate::audit::record(
        &state,
        "admin",
        chronix_security::audit::AuditAction::Admin,
        format!("backup:{}", body.target_dir),
        chronix_security::audit::AuditDecision::Allow,
        &[("files", manifest.file_count.to_string())],
    );

    Ok((StatusCode::OK, Json(manifest)))
}

/// Request body for `POST /api/v1/admin/backup/verify`.
#[derive(Debug, Deserialize)]
pub struct VerifyBackupRequest {
    /// Directory holding the backup to check.
    pub backup_dir: String,
}

/// `POST /api/v1/admin/backup/verify` — check a backup without restoring it.
///
/// A backup that can only be checked by restoring it is one nobody checks,
/// and a backup nobody has checked is the one that turns out to be
/// incomplete on the day it is needed. This runs the same verification a
/// restore runs — every segment the backup's own catalog names, present and
/// at its recorded size — and writes nothing.
pub async fn verify_backup_handler(
    State(state): State<AppState>,
    Json(body): Json<VerifyBackupRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let backup_dir = confine_to_root(&body.backup_dir, &admin_root(&state))?;
    let manifest =
        tokio::task::spawn_blocking(move || chronix::Chronix::verify_backup(&backup_dir))
            .await
            .map_err(|e| ServerError::Internal(format!("verify task panicked: {e}")))?
            .map_err(ServerError::Db)?;

    info!(
        backup_dir = %body.backup_dir,
        segments = manifest.segments,
        "audit: backup verified"
    );
    Ok((StatusCode::OK, Json(manifest)))
}

/// `POST /api/v1/admin/restore` — restore a database from backup.
pub async fn restore_handler(
    State(state): State<AppState>,
    Json(body): Json<RestoreRequest>,
) -> Result<impl IntoResponse, ServerError> {
    let root = admin_root(&state);
    let backup_dir = confine_to_root(&body.backup_dir, &root)?;
    let target_dir = confine_to_root(&body.target_dir, &root)?;
    let manifest =
        tokio::task::spawn_blocking(move || chronix::Chronix::restore(&backup_dir, &target_dir))
            .await
            .map_err(|e| ServerError::Internal(format!("restore task panicked: {e}")))?
            .map_err(ServerError::Db)?;

    info!(
        backup_dir = %body.backup_dir,
        target_dir = %body.target_dir,
        wal_seq = manifest.wal_sequence,
        "audit: restore completed"
    );
    crate::audit::record(
        &state,
        "admin",
        chronix_security::audit::AuditAction::Admin,
        format!("restore:{}", body.target_dir),
        chronix_security::audit::AuditDecision::Allow,
        &[
            ("backup_dir", body.backup_dir.clone()),
            ("wal_sequence", manifest.wal_sequence.to_string()),
        ],
    );

    Ok((StatusCode::OK, Json(manifest)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_response_serialization() {
        let ack = AckResponse {
            ok: true,
            message: None,
        };
        let json = serde_json::to_string(&ack).unwrap();
        assert!(json.contains("\"ok\":true"));
        assert!(!json.contains("message"));
    }

    #[test]
    fn ack_response_with_message() {
        let ack = AckResponse {
            ok: true,
            message: Some("done".into()),
        };
        let json = serde_json::to_string(&ack).unwrap();
        assert!(json.contains("\"message\":\"done\""));
    }

    // ── Analytics model response tests ────────────────────────────────

    #[test]
    fn model_info_response_serialization() {
        let resp = ModelInfoResponse {
            name: "ses_v1".into(),
            measurement: "cpu".into(),
            model_type: "Ses".into(),
            version: 3,
            training_points: 1000,
            mse: 0.5,
            mae: 0.3,
            r_squared: 0.95,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["name"], "ses_v1");
        assert_eq!(json["measurement"], "cpu");
        assert_eq!(json["version"], 3);
        assert_eq!(json["training_points"], 1000);
    }

    #[test]
    fn model_list_response_serialization() {
        let resp = ModelListResponse {
            models: vec![
                ModelInfoResponse {
                    name: "m1".into(),
                    measurement: "cpu".into(),
                    model_type: "Ses".into(),
                    version: 1,
                    training_points: 100,
                    mse: 1.0,
                    mae: 0.8,
                    r_squared: 0.9,
                },
                ModelInfoResponse {
                    name: "m2".into(),
                    measurement: "mem".into(),
                    model_type: "HoltWinters".into(),
                    version: 2,
                    training_points: 500,
                    mse: 0.2,
                    mae: 0.1,
                    r_squared: 0.99,
                },
            ],
            count: 2,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["count"], 2);
        assert!(json["models"].as_array().unwrap().len() == 2);
    }

    #[test]
    fn retrain_request_deserialize() {
        let json = r#"{"measurement": "cpu"}"#;
        let req: RetrainRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.measurement, "cpu");
        assert!(req.model_name.is_none());

        let json = r#"{"measurement": "cpu", "model_name": "ses_v1"}"#;
        let req: RetrainRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.model_name.as_deref(), Some("ses_v1"));
    }

    #[test]
    fn retrain_response_serialization() {
        let resp = RetrainResponse {
            accepted: true,
            models_queued: 3,
            message: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"accepted\":true"));
        assert!(json.contains("\"models_queued\":3"));
        assert!(!json.contains("message"));
    }

    #[test]
    fn model_metadata_to_response_mapping() {
        let meta = chronix::chronix_analytics::forecast::ModelMetadata {
            model_type: chronix::chronix_analytics::forecast::ModelType::Ses,
            model_name: "test_model".into(),
            measurement: "disk".into(),
            version: 5,
            created_at: 0,
            training_data_start: 100,
            training_data_end: 200,
            training_points: 50,
            mse: 0.1,
            mae: 0.2,
            r_squared: 0.98,
        };
        let resp = model_metadata_to_response(&meta);
        assert_eq!(resp.name, "test_model");
        assert_eq!(resp.measurement, "disk");
        assert_eq!(resp.version, 5);
        assert_eq!(resp.training_points, 50);
        assert!((resp.mse - 0.1).abs() < f64::EPSILON);
        assert!((resp.r_squared - 0.98).abs() < f64::EPSILON);
    }

    #[test]
    fn model_catalog_list_all() {
        use chronix::chronix_analytics::forecast::{
            ForecastModel, ModelCatalog, ModelType, SesModel,
        };

        let ts: Vec<i64> = (0..50).map(|i| i * 1_000_000_000).collect();
        let vals: Vec<f64> = vec![42.0; 50];
        let mut model = SesModel::new(Some(0.5));
        model.fit(&ts, &vals).unwrap();

        let mut catalog = ModelCatalog::new();
        catalog
            .save_model(
                "cpu",
                "ses1",
                &model,
                ModelType::Ses,
                (0, 49),
                50,
                0.0,
                0.0,
                1.0,
            )
            .unwrap();
        catalog
            .save_model(
                "mem",
                "ses2",
                &model,
                ModelType::Ses,
                (0, 49),
                50,
                0.0,
                0.0,
                1.0,
            )
            .unwrap();

        let all = catalog.list_all();
        assert_eq!(all.len(), 2);

        let cpu_models = catalog.list_models("cpu");
        assert_eq!(cpu_models.len(), 1);
        assert_eq!(cpu_models[0].model_name, "ses1");
    }
}

#[cfg(test)]
mod path_confinement_tests {
    use super::confine_to_root;

    /// The old check rejected `..` and required an absolute path, which
    /// meant *any* absolute path was accepted: restore read whatever the
    /// server user could read, and backup wrote wherever it could write.
    #[test]
    fn an_absolute_path_outside_the_root_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("backups");
        let err =
            confine_to_root("/etc", &root).expect_err("a path outside the root must be refused");
        assert!(
            err.to_string().contains("backup root"),
            "the error must say why: {err}"
        );
    }

    #[test]
    fn a_relative_path_resolves_inside_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("backups");
        let resolved = confine_to_root("nightly/2026-09-03", &root).unwrap();
        assert!(
            resolved.starts_with(root.canonicalize().unwrap()),
            "{} must be inside the root",
            resolved.display()
        );
    }

    #[test]
    fn parent_traversal_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("backups");
        assert!(confine_to_root("../../etc", &root).is_err());
    }

    /// Rejecting `..` in the string is not confinement. A symlink planted
    /// inside the root points out of it without any `..` anywhere, so the
    /// deepest existing ancestor is canonicalised before the check.
    #[cfg(unix)]
    #[test]
    fn a_symlink_out_of_the_root_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("backups");
        std::fs::create_dir_all(&root).unwrap();
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("escape")).unwrap();

        let err = confine_to_root("escape/loot", &root)
            .expect_err("a symlink leaving the root must be refused");
        assert!(err.to_string().contains("backup root"), "{err}");
    }
}
