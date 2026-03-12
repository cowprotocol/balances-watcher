use crate::services::session_manager::SessionError;
use axum::{http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

#[derive(Serialize)]
pub struct ErrorBody {
    code: u16,
    message: String,
}

impl From<SessionError> for AppError {
    fn from(e: SessionError) -> Self {
        match e {
            SessionError::SessionIsNotCreated => AppError::NotFound(e.to_string()),
            SessionError::ProviderIsNotDefined => AppError::Internal(e.to_string()),
            SessionError::WsProviderIsNotDefined => AppError::Internal(e.to_string()),
            SessionError::TokenLimitExceeded(_, _) => AppError::BadRequest(e.to_string()),
            SessionError::TokenListNotFound(_) => AppError::BadRequest(e.to_string()),
            SessionError::TooManyClients => AppError::BadRequest(e.to_string()),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
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
