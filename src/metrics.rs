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

    /// log event received from any ws subscription
    pub events_received_total: Counter,
    /// erc20 transfer log received
    pub erc20_event_received_total: Counter,
    /// weth9 deposit or withdrawal log received
    pub weth9_events_received_total: Counter,
    /// erc20 transfer log decode failed
    pub parse_erc20_log_errors_total: Counter,
    /// weth9 log decode failed
    pub parse_weth9_logs_failed_total: Counter,

    /// ws stream ended, will resubscribe
    pub ws_provider_disconnected_total: Counter,
    /// ws resubscribe attempted
    pub ws_reconnect_attempts_total: Counter,
    /// ws subscribe call errored (per retry)
    pub ws_subscribe_errors_total: Counter,
    /// ws subscribe gave up after backoff exhausted
    pub ws_subscribe_is_down_total: Counter,

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
    /// post sessions handler latency, ms
    pub create_session_duration_ms: Histogram,

    /// full upsert latency, ms
    pub upsert_total_ms: Histogram,
    /// upsert phase: fetch token lists, ms
    pub upsert_fetch_tokens_ms: Histogram,
    /// upsert phase: resolve existing subscription, ms
    pub upsert_get_subscription_ms: Histogram,
    /// upsert phase: sub manager update, ms
    pub upsert_sub_manager_upsert_ms: Histogram,
    /// upsert phase: spawn watchers, ms
    pub upsert_spawn_watchers_ms: Histogram,
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

            events_received_total: counter!("events_received_total"),
            erc20_event_received_total: counter!("erc20_event_received_total"),
            weth9_events_received_total: counter!("weth9_events_received_total"),
            parse_erc20_log_errors_total: counter!("parse_erc20_log_errors_total"),
            parse_weth9_logs_failed_total: counter!("parse_weth9_logs_failed_total"),

            ws_provider_disconnected_total: counter!("ws_provider_disconnected_total"),
            ws_reconnect_attempts_total: counter!("ws_reconnect_attempts_total"),
            ws_subscribe_errors_total: counter!("ws_subscribe_errors_total"),
            ws_subscribe_is_down_total: counter!("ws_subscribe_is_down_total"),

            token_list_loaded_total: counter!("token_list_loaded_total"),
            token_list_load_failed_total: counter!("token_list_load_failed_total"),
            token_list_loaded_time_in_ms: histogram!("token_list_loaded_time_in_ms"),

            snapshot_updater_runs_total: counter!("snapshot_updater_runs_total"),
            tokens_limit_exceeded_total: counter!("tokens_limit_exceeded_total"),
            create_session_duration_ms: histogram!("create_session_duration_ms"),

            upsert_total_ms: histogram!("upsert_total_ms"),
            upsert_fetch_tokens_ms: histogram!("upsert_fetch_tokens_ms"),
            upsert_get_subscription_ms: histogram!("upsert_get_subscription_ms"),
            upsert_sub_manager_upsert_ms: histogram!("upsert_sub_manager_upsert_ms"),
            upsert_spawn_watchers_ms: histogram!("upsert_spawn_watchers_ms"),
        }
    }
}
