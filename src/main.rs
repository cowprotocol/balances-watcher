mod api;
mod app_error;
mod app_state;
mod args;
mod config;
mod domain;
mod evm;
mod graceful_shutdown;
mod metrics;
mod services;
mod tracing;
mod ws_connection;

use crate::api::create_router;
use crate::args::Args;
use crate::graceful_shutdown::LifeCycle;
use crate::metrics::Metrics;
use crate::tracing::init_tracing::init_tracing;
use app_state::AppState;
use config::network_config::NetworkConfig;
use config::ws_pool_config::WsPoolConfig;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    init_tracing();

    let cfg = Args::from_env();
    let network_cfg = NetworkConfig::from_args(&cfg);
    let ws_pool_cfg = WsPoolConfig::from_args(&cfg);

    ::tracing::info!(
        network = %network_cfg.network,
        bind = %cfg.bind,
        "starting balances-watcher",
    );

    let metrics_handler = PrometheusBuilder::new().install_recorder()?;
    let metrics = Arc::new(Metrics::install());

    let lifecycle = LifeCycle::spawn();

    let app_state = AppState::build(
        network_cfg,
        ws_pool_cfg,
        Arc::clone(&metrics),
        lifecycle.clone(),
    )
    .await?;

    let app = create_router(app_state, metrics_handler);

    let address: SocketAddr = cfg.bind.parse()?;
    ::tracing::info!("Listening to http://{}", address);

    let listener = TcpListener::bind(address).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { lifecycle.cancel_token.cancelled().await })
        .await?;

    lifecycle.task_tracker.close();

    let _ = tokio::time::timeout(Duration::from_secs(10), lifecycle.task_tracker.wait())
        .await
        .map_err(|_| {
            ::tracing::warn!(
                pending = lifecycle.task_tracker.len(),
                "graceful shutdown timed out, killing remaining tasks"
            )
        });

    Ok(())
}
