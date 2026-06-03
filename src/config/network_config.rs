use super::constants::{DEFAULT_MAX_WATCHED_TOKENS_LIMIT, DEFAULT_SNAPSHOT_INTERVAL_SECS};
use crate::args::Args;
use crate::domain::EvmNetwork;

#[derive(Debug)]
pub struct NetworkConfig {
    api_key: String,
    /// The single EVM network this instance serves. Set via `NETWORK` env (chain id).
    pub network: EvmNetwork,
    pub snapshot_interval: usize,
    pub max_watched_tokens_limit: usize,
    pub allowed_origins: Vec<String>,
}

impl NetworkConfig {
    /// build a `NetworkConfig` from parsed CLI/env args.
    pub fn from_args(args: &Args) -> Self {
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

        tracing::info!(
            network = %args.network,
            origins = %allowed_origins.join(", "),
            "network config initialised",
        );

        Self {
            api_key: args.alchemy_api_key.clone(),
            network: args.network,
            snapshot_interval,
            max_watched_tokens_limit,
            allowed_origins,
        }
    }

    fn network_subdomain(network: EvmNetwork) -> &'static str {
        match network {
            EvmNetwork::Eth => "eth-mainnet",
            EvmNetwork::Bnb => "bnb-mainnet",
            EvmNetwork::Gnosis => "gnosis-mainnet",
            EvmNetwork::Polygon => "polygon-mainnet",
            EvmNetwork::Base => "base-mainnet",
            EvmNetwork::Plasma => "plasma-mainnet",
            EvmNetwork::Arbitrum => "arb-mainnet",
            EvmNetwork::Avalanche => "avax-mainnet",
            EvmNetwork::Ink => "ink-mainnet",
            EvmNetwork::Linea => "linea-mainnet",
            EvmNetwork::Sepolia => "eth-sepolia",
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
