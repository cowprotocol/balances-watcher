use super::constants::{DEFAULT_MAX_WATCHED_TOKENS_LIMIT, DEFAULT_SNAPSHOT_INTERVAL_SECS};
use crate::args::Args;
use crate::domain::EvmNetwork;

#[derive(Debug)]
pub struct NetworkConfig {
    /// The single EVM network this instance serves. Set via `NETWORK` env (chain id).
    pub network: EvmNetwork,
    pub rpc_http_url: String,
    pub snapshot_interval: usize,
    pub max_watched_tokens_limit: usize,
    /// Allow token-list URLs on private / loopback hosts (SSRF escape hatch
    /// for local dev and tests). `false` in production.
    pub allow_private_token_lists: bool,
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

        tracing::info!(
            network = %args.network,
            "network config initialised",
        );

        Self {
            network: args.network,
            rpc_http_url: args.rpc_http_url.clone(),
            snapshot_interval,
            max_watched_tokens_limit,
            allow_private_token_lists: args.allow_private_token_lists,
        }
    }
}
