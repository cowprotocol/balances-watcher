use std::sync::Arc;

use axum::{extract::State, http::StatusCode};

use crate::app_state::AppState;

// todo implement proper check via rpc call
pub async fn health_handler(State(_): State<Arc<AppState>>) -> StatusCode {
    StatusCode::OK
}