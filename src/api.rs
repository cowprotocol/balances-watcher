mod create_session;
mod create_sse_session;
mod extractors;
mod health;
mod update_session;

use crate::api::health::health_handler;
use crate::app_state::AppState;
use axum::routing::{get, post, put};
use axum::Router;
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;

use self::create_session::create_session;
use self::create_sse_session::create_sse_connection;
use self::update_session::update_session;

/// Build the application's `Router`.
///
/// Routes registered:
/// - `GET  /health`
/// - `GET  /metrics`
/// - `GET  /sse/{chain_id}/balances/{owner}`
/// - `POST /{chain_id}/sessions/{owner}`
/// - `PUT  /{chain_id}/sessions/{owner}`
///
/// The HTTP API contract is documented in `openapi.yml` at the repository
/// root (kept in sync with handlers by code review + CI lint).
pub fn create_router(app_state: Arc<AppState>, prometheus_handler: PrometheusHandle) -> Router {
    Router::new()
        .route("/health", get(health_handler))
        .route(
            "/metrics",
            get(move || async move { prometheus_handler.render() }),
        )
        .route(
            "/sse/{chain_id}/balances/{owner}",
            get(create_sse_connection),
        )
        .route("/{chain_id}/sessions/{owner}", post(create_session))
        .route("/{chain_id}/sessions/{owner}", put(update_session))
        .with_state(app_state)
}
