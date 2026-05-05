use std::time::Duration;

/// Capacity of the broadcast channel for balance events per subscription
pub const BROADCAST_CHANNEL_CAPACITY: usize = 256;

/// Default interval (seconds) between full balance snapshot updates
pub const DEFAULT_SNAPSHOT_INTERVAL_SECS: usize = 60;

pub const DEFAULT_MAX_WATCHED_TOKENS_LIMIT: usize = 1500;

pub const CALL_QUEUE_DELAY: Duration = Duration::from_millis(300);

pub const MAX_CLIENTS_PER_WS_CONNECTION: usize = 300;

pub const MULTICALL_PERMITS_COUNT: usize = 200;