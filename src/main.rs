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

use crate::api::create_router;
use crate::args::Args;
use crate::metrics::Metrics;
use crate::tracing::init_tracing::init_tracing;
use app_state::AppState;
use config::network_config::NetworkConfig;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::task::TaskTracker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    init_tracing();

    let cfg = Args::from_env();
    let network_cfg = NetworkConfig::from_args(&cfg);

    ::tracing::info!(
        network = %network_cfg.network,
        bind = %cfg.bind,
        "starting balances-watcher",
    );

    let metrics_handler = PrometheusBuilder::new().install_recorder()?;
    let metrics = Arc::new(Metrics::install());

    let shutdown_token = graceful_shutdown::get_token();
    let task_tracker = TaskTracker::new();
    let token_for_app_state = shutdown_token.clone();
    let app_state = AppState::build(
        network_cfg,
        Arc::clone(&metrics),
        task_tracker.clone(),
        token_for_app_state,
    )
    .await?;

    let app = create_router(app_state, metrics_handler);

    let address: SocketAddr = cfg.bind.parse()?;
    ::tracing::info!("Listening to http://{}", address);

    let listener = TcpListener::bind(address).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown_token.cancelled().await })
        .await?;

    task_tracker.close();

    let _ = tokio::time::timeout(Duration::from_secs(10), task_tracker.wait())
        .await
        .map_err(|_| {
            ::tracing::warn!(
                pending = task_tracker.len(),
                "graceful shutdown timed out, killing remaining tasks"
            )
        });

    Ok(())
}
