mod chain_extractor;
mod create_session;
mod create_sse_session;
mod health;
mod openapi;
mod update_session;

use crate::api::health::health_handler;
use crate::api::openapi::ApiDoc;
use crate::app_state::AppState;
use axum::routing::{get, post, put};
use axum::Router;
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use self::create_session::create_session;
use self::create_sse_session::create_sse_connection;
use self::update_session::update_session;

/// Build the application's `Router`.
///
/// Routes registered:
/// - `GET  /health`
/// - `GET  /metrics`
/// - `GET  /openapi.json`
/// - `GET  /docs`
/// - `GET  /sse/{chain_id}/balances/{owner}`
/// - `POST /{chain_id}/sessions/{owner}`
/// - `PUT  /{chain_id}/sessions/{owner}`
pub fn create_router(app_state: Arc<AppState>, prometheus_handler: PrometheusHandle) -> Router {
    Router::new()
        .merge(SwaggerUi::new("/docs").url("/openapi.json", ApiDoc::openapi()))
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
