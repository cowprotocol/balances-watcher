use std::sync::Arc;

use axum::{extract::State, http::StatusCode};

use crate::app_state::AppState;

#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses(
        (status = 200, description = "All subsystems healthy"),
        (status = 503, description = "One or more subsystems unhealthy; see server logs for the reason"),
    ),
)]
pub async fn health_handler(State(state): State<Arc<AppState>>) -> StatusCode {
    let health = state.session_manager.health_status();
    if health.is_healthy() {
        return StatusCode::OK;
    }

    tracing::error!(
        block_watcher_reason = health.block_watcher.reason(),
        event_dispatcher_reason = health.event_dispatcher.reason(),
        "/health failed"
    );
    StatusCode::SERVICE_UNAVAILABLE
}
