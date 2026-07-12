use balances_watcher::args::Args;
use balances_watcher::config::network_config::NetworkConfig;
use balances_watcher::graceful_shutdown::LifeCycle;
use balances_watcher::metrics::Metrics;
use balances_watcher::tracing::init_tracing::init_tracing;
use balances_watcher::{drain, spawn_server};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    init_tracing();

    let cfg = Args::from_env();
    let network_cfg = NetworkConfig::from_args(&cfg);
    let ws_url = cfg.rpc_ws_url.clone();

    tracing::info!(
        network = %network_cfg.network,
        bind = %cfg.bind,
        "starting balances-watcher",
    );

    let metrics_handler = PrometheusBuilder::new().install_recorder()?;
    let metrics = Arc::new(Metrics::install());

    let lifecycle = LifeCycle::spawn();

    let server = spawn_server(
        &cfg.bind,
        network_cfg,
        ws_url,
        metrics,
        metrics_handler,
        lifecycle.clone(),
    )
    .await?;
    tracing::info!("Listening to http://{}", server.local_addr);

    server.serve.await.expect("axum server task panicked")?;

    drain(lifecycle).await;

    Ok(())
}
