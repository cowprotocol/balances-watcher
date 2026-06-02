mod create_session;
mod create_sse_session;
mod extractors;
mod update_session;

use crate::app_state::AppState;
use axum::routing::{get, post, put};
use axum::Router;
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

use self::create_session::create_session;
use self::create_sse_session::create_sse_connection;
use self::update_session::update_session;

/// Build the application's `Router`.
///
/// Routes registered:
/// - `GET  /metrics`                          — Prometheus scrape endpoint
/// - `GET  /sse/{chain_id}/balances/{owner}`  — SSE stream of balance diffs
/// - `POST /{chain_id}/sessions/{owner}`      — create session
/// - `PUT  /{chain_id}/sessions/{owner}`      — extend session's token set
///
pub fn create_router(
    app_state: Arc<AppState>,
    prometheus_handler: PrometheusHandle,
    allowed_origins: Vec<String>,
) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(move |origin, _| {
            // if there no allowed origins in env, allow all origins
            if allowed_origins.is_empty() {
                return true;
            }

            let origin = origin.to_str().unwrap_or("");

            allowed_origins.iter().any(|allowed| {
                if allowed.contains('*') {
                    // allow urls from vercel for testing dev environment for frontend
                    let pattern = allowed.replace("*", "");
                    origin.contains(&pattern)
                } else {
                    origin == allowed
                }
            })
        }))
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
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
        .layer(cors)
        .with_state(app_state)
}
