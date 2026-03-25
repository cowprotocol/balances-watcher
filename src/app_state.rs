use crate::config::constants::MAX_CLIENTS_PER_WS_CONNECTION;
use crate::config::network_config::NetworkConfig;
use crate::domain::EvmNetwork;
use crate::services::session_manager::SessionManager;
use crate::services::ws_connection_pool::WsConnectionPool;
use alloy::network::Ethereum;
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub session_manager: Arc<SessionManager>,
}

impl AppState {
    pub async fn build(network_config: NetworkConfig) -> Arc<Self> {
        let providers = Self::build_rpc_roviders_map(&network_config).await;
        let ws_connection_pools = Self::build_ws_rpc_providers(&network_config).await;

        let session_manager = Arc::new(SessionManager::new(
            providers,
            ws_connection_pools,
            network_config.snapshot_interval,
            network_config.max_watched_tokens_limit,
        ));

        Arc::new(Self { session_manager })
    }

    async fn build_rpc_roviders_map(
        cfg: &NetworkConfig,
    ) -> HashMap<EvmNetwork, DynProvider<Ethereum>> {
        let mut providers: HashMap<EvmNetwork, DynProvider<Ethereum>> = HashMap::new();

        for network in EvmNetwork::ALL {
            let rpc = &cfg.alchemy_http_url(network);
            match ProviderBuilder::new().connect(rpc).await {
                Ok(provider) => {
                    providers.insert(network, provider.erased());
                    tracing::info!("Provider for network {} is registered", network);
                }
                Err(e) => {
                    tracing::error!("Error to init http rpc connection {:?}", e);
                }
            };
        }

        providers
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
