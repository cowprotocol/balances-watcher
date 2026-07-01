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
    if state.session_manager.is_healthy() {
        StatusCode::OK
    } else {
        tracing::error!("/health failed");
        StatusCode::SERVICE_UNAVAILABLE
    }
}
