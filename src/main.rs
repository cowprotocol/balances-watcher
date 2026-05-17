mod api;
mod app_error;
mod app_state;
mod args;
mod config;
mod domain;
mod evm;
mod graceful_shutdown;
mod routes;
mod services;
mod tracing;

use crate::args::Args;
use crate::routes::create_router::create_router;
use crate::tracing::init_tracing::init_tracing;
use app_state::AppState;
use config::network_config::NetworkConfig;
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    init_tracing();

    let cfg = Args::from_env();
    if cfg.alchemy_api_key.is_empty() {
        return Err("ALCHEMY_API_KEY is required".into());
    }

    let network_cfg = NetworkConfig::init(&cfg);

    let metrics_handler = PrometheusBuilder::new().install_recorder()?;

    let allowed_origins = network_cfg.allowed_origins.clone();
    let shutdown_token = graceful_shutdown::get_token();
    let token_for_app_state = shutdown_token.clone();
    let app_state = AppState::build(network_cfg, token_for_app_state).await;

    let app = create_router(app_state, metrics_handler, allowed_origins);

    let address: SocketAddr = cfg.bind.parse()?;
    ::tracing::info!("Listening to http://{}", address);

    let listener = TcpListener::bind(address).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown_token.cancelled().await })
        .await?;

    Ok(())
}
