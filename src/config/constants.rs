use std::time::Duration;

/// Capacity of the broadcast channel for balance events per subscription
pub const BROADCAST_CHANNEL_CAPACITY: usize = 256;

/// Default interval (seconds) between full balance snapshot updates
pub const DEFAULT_SNAPSHOT_INTERVAL_SECS: usize = 60;

pub const DEFAULT_MAX_WATCHED_TOKENS_LIMIT: usize = 1500;

pub const CALL_QUEUE_DELAY: Duration = Duration::from_millis(300);

pub const MAX_QUEUE_SIZE: usize = 256;

/// Concurrency cap on `RpcClient::fetch_balances_via_multicall`.
pub const MULTICALL_PERMITS_COUNT: usize = 600;

/// Max concurrent sessions sharing the same `(chain, owner)`. Sessions are
/// keyed by `(chain, owner, client_id)` — this bounds how many distinct
/// devices/tabs can watch one wallet at once, so a POST storm with unique
/// UUIDs cannot spawn unbounded snapshot pipelines on a single wallet.
pub const MAX_CLIENTS_PER_OWNER: usize = 5;

/// Idle window after which a session with zero SSE subscribers is dropped by
/// [`crate::services::subscription_manager::SubscriptionManager`]'s
/// background cleanup task. Cleanup ticks at the same cadence, so an idle
/// session dies somewhere in `[SESSION_TTL, 2 * SESSION_TTL)` after its last
/// subscriber left.
pub const SESSION_TTL: Duration = Duration::from_secs(5);
