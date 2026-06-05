use crate::config::constants::MAX_CLIENTS_PER_WS_CONNECTION;
use crate::config::network_config::NetworkConfig;
use crate::domain::EvmNetwork;
use crate::metrics::Metrics;
use crate::services::rpc_client::RpcClient;
use crate::services::session_manager::{SessionConfig, SessionManager};
use crate::services::ws_connection_pool::WsConnectionPool;
use alloy::providers::{Provider, ProviderBuilder};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

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
        metrics: Arc<Metrics>,
        task_tracker: TaskTracker,
        shutdown_token: CancellationToken,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let network = network_config.network;

        let http_provider = ProviderBuilder::new()
            .connect(&network_config.rpc_http_url)
            .await?
            .erased();
        let rpc_client = Arc::new(RpcClient::new(
            Arc::new(http_provider),
            network,
            Arc::clone(&metrics),
        ));
        tracing::info!(%network, "http provider connected");

        let ws_pool = Arc::new(WsConnectionPool::new(
            network_config.rpc_ws_url.clone(),
            MAX_CLIENTS_PER_WS_CONNECTION,
        ));
        tracing::info!(%network, "ws connection pool ready");

        let session_manager = Arc::new(SessionManager::new(
            rpc_client,
            ws_pool,
            Arc::clone(&metrics),
            task_tracker,
            shutdown_token,
            SessionConfig {
                snapshot_interval: network_config.snapshot_interval,
                token_limit: network_config.max_watched_tokens_limit,
                active_network: network,
            },
        ));

        Ok(Arc::new(Self {
            session_manager,
            network,
            metrics,
        }))
    }
}
