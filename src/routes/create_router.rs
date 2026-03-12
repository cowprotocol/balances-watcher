use crate::api::create_session::create_session;
use crate::api::create_sse_session::create_sse_connection;
use crate::api::update_session::update_session;
use crate::app_state::AppState;
use axum::routing::put;
use axum::{
    routing::{get, post},
    Router,
};
use metrics_exporter_prometheus::PrometheusHandle;
use std::sync::Arc;
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

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
