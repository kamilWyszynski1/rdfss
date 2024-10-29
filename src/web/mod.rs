use crate::web::master_router::MasterAppError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

pub mod master_router;
pub mod worker_router;

// Make our own error that wraps `anyhow::Error`.
pub(crate) struct AppError(anyhow::Error);

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        // How we want errors responses to be serialized
        #[derive(Serialize)]
        struct ErrorResponse {
            error: String,
        }

        tracing::error!(err = format!("{:#}", self.0), "error occurred");

        match self.0.downcast::<MasterAppError>() {
            Ok(e) => match e {
                MasterAppError::FileExists => (
                    StatusCode::CONFLICT,
                    Json(ErrorResponse {
                        error: "File already exists".to_string(),
                    }),
                ),
            },
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ),
        }
        .into_response()
    }
}

// This enables using `?` on functions that return `Result<_, anyhow::Error>` to turn them into
// `Result<_, AppError>`. That way you don't need to do that manually.
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}
