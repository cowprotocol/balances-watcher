use std::time::Duration;

/// Capacity of the broadcast channel for balance events per subscription
pub const BROADCAST_CHANNEL_CAPACITY: usize = 256;

/// Default interval (seconds) between full balance snapshot updates
pub const DEFAULT_SNAPSHOT_INTERVAL_SECS: usize = 60;

pub const DEFAULT_MAX_WATCHED_TOKENS_LIMIT: usize = 1500;

pub const CALL_QUEUE_DELAY: Duration = Duration::from_millis(300);

pub const MAX_QUEUE_SIZE: usize = 256;

/// Default hard cap on sessions sharing a single WS provider. When exceeded,
/// the pool opens a new pipe (see [`WsConnectionPool::acquire`]). Tuned
/// together with [`MULTICALL_PERMITS_COUNT`] — at full capacity each session
/// can hold one multicall permit, so raising one usually means raising the
/// other. Overridable via `MAX_CLIENTS_PER_WS_CONNECTION` env var.
pub const DEFAULT_MAX_CLIENTS_PER_WS_CONNECTION: usize = 300;

/// Concurrency cap on `RpcClient::fetch_balances_via_multicall`.
pub const MULTICALL_PERMITS_COUNT: usize = 300;

/// Default concurrency cap on `WsConnectionPool::subscribe`. Keeps a session
/// burst from fanning out into a stampede of `eth_subscribe` on the shared
/// WS pipe, which is what historically caused upstream RSTs under load. Held
/// across the whole backoff retry window of `subscribe_with_retries`, not per
/// attempt. Overridable via `WS_SUBSCRIPTION_PERMITS_COUNT` env var.
pub const DEFAULT_WS_SUBSCRIPTION_PERMITS_COUNT: usize = 40;
