use super::constants::{DEFAULT_MAX_WATCHED_TOKENS_LIMIT, DEFAULT_SNAPSHOT_INTERVAL_SECS};
use crate::args::Args;
use crate::domain::errors::EvmError;
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
    /// Build a `NetworkConfig` from parsed CLI/env args. Fails fast if `NETWORK`
    /// is missing or refers to an unsupported chain id — the process should not
    /// start in a half-configured state.
    pub fn from_args(args: &Args) -> Result<Self, EvmError> {
        let network: EvmNetwork = args.network.parse()?;

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
            network = %network,
            origins = %allowed_origins.join(", "),
            "network config initialised",
        );

        Ok(Self {
            api_key: args.alchemy_api_key.clone(),
            network,
            snapshot_interval,
            max_watched_tokens_limit,
            allowed_origins,
        })
    }

    fn network_subdomain(network: EvmNetwork) -> &'static str {
        match network {
            EvmNetwork::Eth => "eth-mainnet",
            EvmNetwork::Arbitrum => "arb-mainnet",
            EvmNetwork::Sepolia => "eth-sepolia",
            EvmNetwork::Bnb => "bnb-mainnet",
            EvmNetwork::Polygon => "polygon-mainnet",
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
