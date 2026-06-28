use crate::config::network_config::NetworkConfig;
use crate::domain::EvmNetwork;
use crate::graceful_shutdown::LifeCycle;
use crate::metrics::Metrics;
use crate::services::rpc_client::RpcClient;
use crate::services::session_manager::{SessionConfig, SessionManager};
use alloy::providers::{Provider, ProviderBuilder};
use std::sync::Arc;

/// Application state for the single-network instance.
///
/// The service is intentionally **chain-scoped**: one process serves exactly
/// one network (set via `NETWORK` env). Multi-network fan-out is achieved by
/// deploying N replicas (one per chain) behind a path-based ingress.
#[derive(Clone)]
pub struct AppState {
    pub session_manager: Arc<SessionManager>,
    /// Network this instance serves. Used by API handlers to reject requests
    /// addressed to a different chain.
    pub network: EvmNetwork,
    pub metrics: Arc<Metrics>,
}

impl AppState {
    pub async fn build(
        network_config: NetworkConfig,
        ws_url: String,
        metrics: Arc<Metrics>,
        life_cycle: LifeCycle,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let network = network_config.network;

        let http_provider = ProviderBuilder::new()
            .connect(&network_config.rpc_http_url)
            .await?
            .erased();
        let rpc_client = Arc::new(RpcClient::new(
            Arc::new(http_provider),
            Arc::clone(&metrics),
        ));
        tracing::info!(%network, "http provider connected");

        let session_manager = SessionManager::spawn(
            rpc_client,
            Arc::clone(&metrics),
            life_cycle.clone(),
            SessionConfig {
                snapshot_interval: network_config.snapshot_interval,
                token_limit: network_config.max_watched_tokens_limit,
                active_network: network,
            },
            ws_url,
        );

        Ok(Arc::new(Self {
            session_manager,
            network,
            metrics,
        }))
    }
}
