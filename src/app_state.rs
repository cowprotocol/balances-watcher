use crate::config::constants::MAX_CLIENTS_PER_WS_CONNECTION;
use crate::config::network_config::NetworkConfig;
use crate::domain::EvmNetwork;
use crate::services::balance_fetcher::BalanceFetcher;
use crate::services::session_manager::SessionManager;
use crate::services::ws_connection_pool::WsConnectionPool;
use alloy::providers::{Provider, ProviderBuilder};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct AppState {
    pub session_manager: Arc<SessionManager>,
}

impl AppState {
    pub async fn build(
        network_config: NetworkConfig,
        shutdown_token: CancellationToken,
    ) -> Arc<Self> {
        let providers = Self::build_rpc_fetchers_map(&network_config).await;
        let ws_connection_pools = Self::build_ws_rpc_providers(&network_config).await;

        let session_manager = Arc::new(SessionManager::new(
            providers,
            ws_connection_pools,
            network_config.snapshot_interval,
            network_config.max_watched_tokens_limit,
            shutdown_token,
        ));

        Arc::new(Self { session_manager })
    }

    async fn build_rpc_fetchers_map(
        cfg: &NetworkConfig,
    ) -> HashMap<EvmNetwork, Arc<BalanceFetcher>> {
        let mut fetchers: HashMap<EvmNetwork, Arc<BalanceFetcher>> = HashMap::new();

        for network in EvmNetwork::ALL {
            let rpc = &cfg.alchemy_http_url(network);
            match ProviderBuilder::new().connect(rpc).await {
                Ok(provider) => {
                    let fetcher = BalanceFetcher::new(Arc::new(provider.erased()), network);
                    fetchers.insert(network, Arc::new(fetcher));
                    tracing::info!("Provider for network {} is registered", network);
                }
                Err(e) => {
                    tracing::error!("Error to init http rpc connection {:?}", e);
                }
            };
        }

        fetchers
    }

    async fn build_ws_rpc_providers(
        cfg: &NetworkConfig,
    ) -> HashMap<EvmNetwork, Arc<WsConnectionPool>> {
        let mut pool: HashMap<EvmNetwork, Arc<WsConnectionPool>> = HashMap::new();

        for network in EvmNetwork::ALL {
            let rpc = cfg.alchemy_ws_url(network);
            let ws_connection_pool = WsConnectionPool::new(rpc, MAX_CLIENTS_PER_WS_CONNECTION);
            pool.insert(network, Arc::new(ws_connection_pool));

            tracing::info!("WS provider for network {} is registered", network);
        }

        pool
    }
}
