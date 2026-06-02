use crate::config::constants::MAX_CLIENTS_PER_WS_CONNECTION;
use crate::config::network_config::NetworkConfig;
use crate::domain::EvmNetwork;
use crate::services::balance_fetcher::BalanceFetcher;
use crate::services::session_manager::SessionManager;
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
}

impl AppState {
    pub async fn build(
        network_config: NetworkConfig,
        task_tracker: TaskTracker,
        shutdown_token: CancellationToken,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let network = network_config.network;

        let http_url = network_config.alchemy_http_url(network);
        let http_provider = ProviderBuilder::new().connect(&http_url).await?.erased();
        let balance_fetcher = Arc::new(BalanceFetcher::new(Arc::new(http_provider), network));
        tracing::info!(%network, "http provider connected");

        let ws_url = network_config.alchemy_ws_url(network);
        let ws_pool = Arc::new(WsConnectionPool::new(ws_url, MAX_CLIENTS_PER_WS_CONNECTION));
        tracing::info!(%network, "ws connection pool ready");

        let session_manager = Arc::new(SessionManager::new(
            balance_fetcher,
            ws_pool,
            network_config.snapshot_interval,
            network_config.max_watched_tokens_limit,
            task_tracker,
            shutdown_token,
        ));

        Ok(Arc::new(Self {
            session_manager,
            network,
        }))
    }
}
