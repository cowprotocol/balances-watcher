use std::sync::Arc;

use axum::{extract::State, http::StatusCode};

use crate::app_state::AppState;

/// `GET /health` — see `openapi.yml` for the contract. Active probe that
/// calls `eth_blockNumber` on the upstream RPC; returns 503 on failure.
pub async fn health_handler(State(state): State<Arc<AppState>>) -> StatusCode {
    match state.session_manager.healthcheck().await {
        Ok(_) => StatusCode::OK,
        Err(err) => {
            tracing::error!(
                error = %err,
                "/health failed"
            );
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}
