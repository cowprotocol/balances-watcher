//! Library entry point. `main.rs` is a thin wrapper around this; integration
//! tests import from here to spin up the full stack against a controlled
//! backend (Anvil, in-process token-list server).

pub mod api;
pub mod app_error;
pub mod app_state;
pub mod args;
pub mod config;
pub mod domain;
pub mod evm;
pub mod graceful_shutdown;
pub mod metrics;
pub mod services;
pub mod tracing;
pub mod ws_connection;

use crate::api::create_router;
use crate::app_state::AppState;
use crate::config::network_config::NetworkConfig;
use crate::graceful_shutdown::LifeCycle;
use crate::metrics::Metrics;
use metrics_exporter_prometheus::PrometheusHandle;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

/// Everything the running server needs. Split out from `main` so tests can
/// bind the router to `127.0.0.1:0` and grab the concrete port without
/// re-implementing wiring.
pub struct ServerHandle {
    pub app_state: Arc<AppState>,
    pub lifecycle: LifeCycle,
    pub metrics: Arc<Metrics>,
    pub local_addr: SocketAddr,
    /// `serve` future. Awaiting it drives the axum server; dropping cancels.
    pub serve: tokio::task::JoinHandle<Result<(), std::io::Error>>,
}

/// Build router + spawn the axum server on `bind`. The returned handle exposes
/// the bound address (useful when `bind` uses port 0) and a JoinHandle for
/// the server task. Graceful shutdown is wired to `lifecycle.cancel_token`.
pub async fn spawn_server(
    bind: &str,
    network_cfg: NetworkConfig,
    ws_url: String,
    metrics: Arc<Metrics>,
    metrics_handler: PrometheusHandle,
    lifecycle: LifeCycle,
) -> Result<ServerHandle, Box<dyn std::error::Error>> {
    let app_state =
        AppState::build(network_cfg, ws_url, Arc::clone(&metrics), lifecycle.clone()).await?;
    let app = create_router(Arc::clone(&app_state), metrics_handler);

    let address: SocketAddr = bind.parse()?;
    let listener = TcpListener::bind(address).await?;
    let local_addr = listener.local_addr()?;

    let shutdown_token = lifecycle.cancel_token.clone();
    let serve = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown_token.cancelled().await })
            .await
    });

    Ok(ServerHandle {
        app_state,
        lifecycle,
        metrics,
        local_addr,
        serve,
    })
}

/// Wait for spawned background work to drain after the server has stopped
/// accepting new requests. Bounded to 10s so a stuck subsystem cannot hold
/// shutdown open indefinitely.
pub async fn drain(lifecycle: LifeCycle) {
    lifecycle.task_tracker.close();
    let _ = tokio::time::timeout(Duration::from_secs(10), lifecycle.task_tracker.wait())
        .await
        .map_err(|_| {
            ::tracing::warn!(
                pending = lifecycle.task_tracker.len(),
                "graceful shutdown timed out, killing remaining tasks"
            )
        });
}
