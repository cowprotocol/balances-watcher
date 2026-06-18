use crate::domain::EvmNetwork;
use clap::Parser;

const DEFAULT_TOKEN_LIST_PATH: &str = "configs/tokens_list.json";

#[derive(Parser, Debug, Clone)]
pub struct Args {
    #[arg(long, env = "HTTP_BIND", default_value = "0.0.0.0:8080")]
    pub bind: String,

    /// Target EVM network for this instance (chain id, e.g. `1`, `42161`, `100`).
    #[arg(long, env = "NETWORK")]
    pub network: EvmNetwork,

    /// RPC HTTP endpoint (e.g. `http://mainnet-proxy.rpc-nodes.svc.cluster.local`).
    #[arg(long, env = "RPC_HTTP_URL")]
    pub rpc_http_url: String,

    /// RPC WebSocket endpoint (e.g. `ws://mainnet-proxy.rpc-nodes.svc.cluster.local`).
    #[arg(long, env = "RPC_WS_URL")]
    pub rpc_ws_url: String,

    #[arg(long, env="TOKEN_LIST_PATH", default_value=DEFAULT_TOKEN_LIST_PATH)]
    pub token_list_path: String,

    #[arg(long, env = "SNAPSHOT_INTERVAL", default_value = "60")]
    pub snapshot_interval: String,

    #[arg(long, env = "MAX_WATCHED_TOKENS_LIMIT", default_value = "1500")]
    pub max_watched_tokens_limit: String,

    /// Hard cap on sessions sharing a single WS provider pipe. When exceeded,
    /// the pool opens a new pipe.
    #[arg(long, env = "MAX_CLIENTS_PER_WS_CONNECTION", default_value = "300")]
    pub max_clients_per_ws_connection: String,

    /// Concurrency cap on in-flight `eth_subscribe` calls. Held across the
    /// whole `subscribe_with_retries` backoff window, not per attempt.
    #[arg(long, env = "WS_SUBSCRIPTION_PERMITS_COUNT", default_value = "40")]
    pub ws_subscription_permits_count: String,
}

impl Args {
    pub fn from_env() -> Self {
        Self::parse()
    }
}
