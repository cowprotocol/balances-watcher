use metrics::{counter, gauge, histogram, Counter, Gauge, Histogram};

pub struct Metrics {
    /// new session created
    pub sessions_created_total: Counter,
    /// existing session got new tokens
    pub sessions_updated_total: Counter,
    /// session removed (idle ttl or last client left)
    pub sessions_expired_total: Counter,
    /// currently live sessions
    pub active_sessions: Gauge,

    /// sse stream opened
    pub sse_connections_total: Counter,
    /// currently open sse streams
    pub sse_connections_active: Gauge,
    /// sse client fell behind, broadcast slot dropped
    pub broadcast_lagged_total: Counter,
    /// balance diff broadcast to sse clients
    pub balance_updates_sent_total: Counter,

    /// multicall request issued
    pub multicall_total: Counter,
    /// multicall attempt failed (per retry)
    pub multicall_failed_total: Counter,
    /// multicall round-trip latency, ms
    pub multicall_duration_ms: Histogram,
    /// multicall gave up after backoff exhausted
    pub provider_exhausted_with_retries_total: Counter,
    /// per-token subcall inside a successful multicall failed
    /// (revert / missing slot / abi-decode failure). The token is skipped,
    /// the rest of the batch still flows to the client.
    pub multicall_subcall_failed_total: Counter,

    /// erc20 transfer log received
    pub erc20_event_received_total: Counter,
    /// weth9 deposit or withdrawal log received
    pub weth9_events_received_total: Counter,

    /// ws stream ended, will resubscribe
    pub ws_provider_disconnected_total: Counter,
    /// ws resubscribe attempted
    pub ws_reconnect_attempts_total: Counter,
    /// time spent in backoff between a failed ws connect/subscribe attempt and the next retry
    pub ws_reconnect_attempt_duration_ms: Histogram,

    /// blocks accepted from the ws subscription. Rate ≈ chain block rate when healthy
    pub block_accepted_total: Counter,
    /// block channel (BlockWatcher → EventDispatcher) overflowed on `try_send`.
    /// Non-zero = dispatcher catastrophically behind, health should already be red.
    pub block_channel_overflow_total: Counter,

    /// event dispatcher successfully processed a block (both eth_getLogs calls
    /// completed, results fanned out). Rate ≈ chain block rate when healthy.
    pub event_dispatcher_blocks_processed_total: Counter,
    /// current dispatcher lag in blocks (`latest_block - last_processed`).
    /// Set on every processed block. Prod alerting can page on lag values
    /// above `EvmNetwork::max_block_lag()` for the deployment's chain.
    pub event_dispatcher_lag_blocks: Gauge,
    /// eth_getLogs attempt failed (per-attempt, bumped from the retry `.notify`
    /// hook — same semantics as `multicall_failed_total`). Sum across erc20 and
    /// weth9 paths. A single logical getLogs call can bump this up to N times
    /// before the retry chain either succeeds or exhausts.
    pub eth_get_logs_failed_total: Counter,
    /// eth_getLogs latency per block (dispatcher path). Sum across erc20 and
    /// weth9; tag with `kind = erc20 | weth9` when moving to labelled histograms.
    pub eth_get_logs_duration_ms: Histogram,
    /// eth_getLogs for a block exhausted retries — that block's logs are
    /// permanently lost (dispatcher still bumps `latest_processed_block` to
    /// keep lag health honest). Sum across erc20 and weth9 paths; one failed
    /// source on one block bumps by 1. **Data-loss counter — page on any
    /// non-zero rate.** Snapshot loop (60s cadence) is the recovery path.
    pub event_dispatcher_missed_block_logs_total: Counter,

    /// token list fetched ok
    pub token_list_loaded_total: Counter,
    /// token list fetch failed after retries
    pub token_list_load_failed_total: Counter,
    /// token list fetch latency, ms
    pub token_list_loaded_time_in_ms: Histogram,

    /// periodic snapshot multicall fired
    pub snapshot_updater_runs_total: Counter,
    /// session rejected, token limit exceeded
    pub tokens_limit_exceeded_total: Counter,
    /// session rejected, per-owner client_id cap hit
    pub owner_client_limit_exceeded_total: Counter,
    /// distribution of active client_id count per owner, sampled at insert
    pub sessions_per_owner: Histogram,
    /// post sessions handler latency, ms
    pub create_session_duration_ms: Histogram,
}

impl Metrics {
    pub fn install() -> Self {
        Self {
            sessions_created_total: counter!("sessions_created_total"),
            sessions_updated_total: counter!("sessions_updated_total"),
            sessions_expired_total: counter!("sessions_expired_total"),
            active_sessions: gauge!("active_sessions"),

            sse_connections_total: counter!("sse_connections_total"),
            sse_connections_active: gauge!("sse_connections_active"),
            broadcast_lagged_total: counter!("broadcast_lagged_total"),
            balance_updates_sent_total: counter!("balance_updates_sent_total"),

            multicall_total: counter!("multicall_total"),
            multicall_failed_total: counter!("multicall_failed_total"),
            multicall_duration_ms: histogram!("multicall_duration_ms"),
            provider_exhausted_with_retries_total: counter!(
                "provider_exhausted_with_retries_total"
            ),
            multicall_subcall_failed_total: counter!("multicall_subcall_failed_total"),

            erc20_event_received_total: counter!("erc20_event_received_total"),
            weth9_events_received_total: counter!("weth9_events_received_total"),

            ws_provider_disconnected_total: counter!("ws_provider_disconnected_total"),
            ws_reconnect_attempts_total: counter!("ws_reconnect_attempts_total"),
            ws_reconnect_attempt_duration_ms: histogram!("ws_reconnect_attempt_duration_ms"),

            block_accepted_total: counter!("block_accepted_total"),
            block_channel_overflow_total: counter!("block_channel_overflow_total"),

            event_dispatcher_blocks_processed_total: counter!(
                "event_dispatcher_blocks_processed_total"
            ),
            event_dispatcher_lag_blocks: gauge!("event_dispatcher_lag_blocks"),
            eth_get_logs_failed_total: counter!("eth_get_logs_failed_total"),
            eth_get_logs_duration_ms: histogram!("eth_get_logs_duration_ms"),
            event_dispatcher_missed_block_logs_total: counter!(
                "event_dispatcher_missed_block_logs_total"
            ),

            token_list_loaded_total: counter!("token_list_loaded_total"),
            token_list_load_failed_total: counter!("token_list_load_failed_total"),
            token_list_loaded_time_in_ms: histogram!("token_list_loaded_time_in_ms"),

            snapshot_updater_runs_total: counter!("snapshot_updater_runs_total"),
            tokens_limit_exceeded_total: counter!("tokens_limit_exceeded_total"),
            owner_client_limit_exceeded_total: counter!("owner_client_limit_exceeded_total"),
            sessions_per_owner: histogram!("sessions_per_owner"),
            create_session_duration_ms: histogram!("create_session_duration_ms"),
        }
    }
}
