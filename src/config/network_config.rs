use super::constants::{DEFAULT_MAX_WATCHED_TOKENS_LIMIT, DEFAULT_SNAPSHOT_INTERVAL_SECS};
use crate::args::Args;
use crate::domain::EvmNetwork;

#[derive(Debug)]
pub struct NetworkConfig {
    api_key: String,
    pub snapshot_interval: usize,
    pub max_watched_tokens_limit: usize,
    pub allowed_origins: Vec<String>,
}

impl NetworkConfig {
    pub fn init(args: &Args) -> Self {
        let api_key = args.alchemy_api_key.clone();

        let snapshot_interval: usize = args
            .snapshot_interval
            .parse()
            .inspect_err(|err| {
                tracing::warn!("Invalid snapshot interval value: {}", err);
            })
            .unwrap_or(DEFAULT_SNAPSHOT_INTERVAL_SECS);

        let max_watched_tokens_limit: usize = args
            .max_watched_tokens_limit
            .parse()
            .inspect_err(|err| {
                tracing::warn!("Invalid MAX_WATCHED_TOKENS_LIMIT value: {}", err);
            })
            .unwrap_or(DEFAULT_MAX_WATCHED_TOKENS_LIMIT);

        let allowed_origins: Vec<String> = args
            .allowed_origins
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        tracing::info!(origins = %allowed_origins.join(", "), "init origins from env");

        Self {
            api_key,
            snapshot_interval,
            max_watched_tokens_limit,
            allowed_origins,
        }
    }

    fn network_subdomain(network: EvmNetwork) -> &'static str {
        match network {
            EvmNetwork::Eth => "eth-mainnet",
            EvmNetwork::Arbitrum => "arb-mainnet",
            EvmNetwork::Sepolia => "eth-sepolia",
            EvmNetwork::Bnb => "bnb-mainnet",
            EvmNetwork::Poligon => "poligon-mainnet",
            EvmNetwork::Gnosis => "gnosis-mainnet",
        }
    }

    pub fn alchemy_http_url(&self, network: EvmNetwork) -> String {
        let subdomain = Self::network_subdomain(network);
        format!("https://{}.g.alchemy.com/v2/{}", subdomain, self.api_key)
    }

    pub fn alchemy_ws_url(&self, network: EvmNetwork) -> String {
        let subdomain = Self::network_subdomain(network);
        format!("wss://{}.g.alchemy.com/v2/{}", subdomain, self.api_key)
    }
}
