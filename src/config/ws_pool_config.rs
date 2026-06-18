use super::constants::{
    DEFAULT_MAX_CLIENTS_PER_WS_CONNECTION, DEFAULT_WS_SUBSCRIPTION_PERMITS_COUNT,
};
use crate::args::Args;

/// Tunables for [`WsConnectionPool`](crate::services::ws_connection_pool::WsConnectionPool).
///
/// Sourced from env via [`Self::from_args`]; bad/missing values fall back to
/// the `DEFAULT_*` constants and a warn-level log so prod misconfigurations
/// don't crash the binary at start-up.
#[derive(Debug, Clone)]
pub struct WsPoolConfig {
    /// Upstream WebSocket RPC URL (mirrors `RPC_WS_URL`).
    pub ws_url: String,
    /// Capacity per WS pipe before a new one is opened.
    pub max_clients_per_ws_connection: usize,
    /// Cap on concurrent `subscribe_logs` operations.
    pub subscribe_permits_count: usize,
}

impl WsPoolConfig {
    pub fn from_args(args: &Args) -> Self {
        let max_clients_per_ws_connection: usize = args
            .max_clients_per_ws_connection
            .parse()
            .inspect_err(|err| {
                tracing::warn!("Invalid MAX_CLIENTS_PER_WS_CONNECTION value: {}", err);
            })
            .unwrap_or(DEFAULT_MAX_CLIENTS_PER_WS_CONNECTION);

        let subscribe_permits_count: usize = args
            .ws_subscription_permits_count
            .parse()
            .inspect_err(|err| {
                tracing::warn!("Invalid WS_SUBSCRIPTION_PERMITS_COUNT value: {}", err);
            })
            .unwrap_or(DEFAULT_WS_SUBSCRIPTION_PERMITS_COUNT);

        tracing::info!(
            max_clients_per_ws_connection,
            subscribe_permits_count,
            "ws pool config initialised",
        );

        Self {
            ws_url: args.rpc_ws_url.clone(),
            max_clients_per_ws_connection,
            subscribe_permits_count,
        }
    }
}
