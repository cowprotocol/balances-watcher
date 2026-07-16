use crate::services::session_manager::SessionError;
use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Too many requests: {0}")]
    TooManyRequests(String),
}

#[derive(Serialize, ToSchema)]
pub struct ErrorBody {
    /// Mirrors the HTTP status code (400 / 404 / 429).
    pub code: u16,
    /// Human-readable explanation; safe to surface to end users.
    pub message: String,
}

impl From<SessionError> for AppError {
    fn from(e: SessionError) -> Self {
        match e {
            SessionError::SessionIsNotCreated => AppError::NotFound(e.to_string()),
            SessionError::TokenLimitExceeded(_, _) => AppError::BadRequest(e.to_string()),
            SessionError::TokenListNotFound(_) => AppError::BadRequest(e.to_string()),
            SessionError::TokenListUrlNotAllowed(_, _) => AppError::BadRequest(e.to_string()),
            SessionError::OwnerClientLimitExceeded(_) => AppError::TooManyRequests(e.to_string()),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::TooManyRequests(_) => StatusCode::TOO_MANY_REQUESTS,
        };

        (
            status,
            Json(ErrorBody {
                code: status.as_u16(),
                message: self.to_string(),
            }),
        )
            .into_response()
    }
}
