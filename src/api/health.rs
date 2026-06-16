use std::sync::Arc;

use axum::{extract::State, http::StatusCode};

use crate::app_state::AppState;

#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses(
        (status = 200, description = "Upstream HTTP RPC reachable; this instance is serving"),
        (status = 503, description = "Upstream RPC unreachable or this instance unhealthy"),
    ),
)]
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
