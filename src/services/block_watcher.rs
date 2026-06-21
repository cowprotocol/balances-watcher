//! WebSocket health canary: a dedicated `eth_subscribe("newHeads")` subscription
//! that powers `/health` via [`BlockWatcher::is_healthy`].
//!
//! Runs in its own task with infinite-retry exponential backoff
//! (`MIN_RECONNECT_ATTEMPT_DELAY` → `MAX_RECONNECT_ATTEMPT_DELAY`, jitter) and a
//! stall watchdog (`block_time × STALL_TIMEOUT_BLOCKS`, floored at `MIN_STALL_DURATION`) that forces
//! a reconnect when headers stop arriving. The provider is **not** shared with the per-session pool
//! ([`crate::services::ws_connection_pool`]) — a dedicated socket keeps the health
//! signal isolated from data-plane churn (event load, session reconnect storms).
//!
//! Health is a single [`std::sync::atomic::AtomicBool`]: flipped `true` on the
//! first header received, `false` on disconnect, stall, or shutdown.

use crate::domain::EvmNetwork;
use crate::metrics::Metrics;
use alloy::providers::{DynProvider, Provider, ProviderBuilder, WsConnect};
use alloy::pubsub::SubscriptionStream;
use alloy::rpc::types::Header;
use backon::{ExponentialBuilder, Retryable};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const MIN_STALL_DURATION: Duration = Duration::from_secs(2);
const STALL_TIMEOUT_BLOCKS: u32 = 3;
const MIN_RECONNECT_ATTEMPT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_ATTEMPT_DELAY: Duration = Duration::from_secs(30);
const POST_DISCONNECT_DELAY: Duration = Duration::from_millis(200);

type BlockStream = SubscriptionStream<Header>;

/// Process-wide WS subscription to `newHeads`. See module docs for the reconnect
/// strategy and the rationale for a dedicated provider.
pub struct BlockWatcher {
    network: EvmNetwork,
    metrics: Arc<Metrics>,
    connected: AtomicBool,
}

impl BlockWatcher {
    /// Spawns the watcher task on `task_tracker` and returns a shared handle.
    /// The task runs until `cancellation_token` is cancelled.
    pub fn spawn(
        network: EvmNetwork,
        metrics: Arc<Metrics>,
        task_tracker: TaskTracker,
        cancellation_token: CancellationToken,
        ws_url: String,
    ) -> Arc<Self> {
        let watcher = Arc::new(Self {
            network,
            metrics,
            connected: AtomicBool::new(false),
        });

        let watcher_for_spawn = Arc::clone(&watcher);
        task_tracker.spawn(async move {
            watcher_for_spawn.run(cancellation_token, ws_url).await;
        });

        watcher
    }

    /// `true` iff the watcher has received at least one header since the most
    /// recent reconnect and the stream has not since closed or stalled.
    pub fn is_healthy(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    async fn run(self: Arc<Self>, cancel: CancellationToken, ws_url: String) {
        loop {
            if cancel.is_cancelled() {
                break;
            }

            let Some((provider, stream)) = self.connect_with_retry(&cancel, &ws_url).await else {
                break;
            };
            tracing::info!("block subscription established, waiting for first header");

            self.consume_until_disconnect(provider, stream, &cancel).await;
            self.connected.store(false, Ordering::Relaxed);

            tracing::info!(
                delay_ms = POST_DISCONNECT_DELAY.as_millis() as u64,
                "block subscription ended, will resubscribe after delay"
            );
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tokio::time::sleep(POST_DISCONNECT_DELAY) => {}
            }
        }
    }

    async fn consume_until_disconnect(
        &self,
        _provider: DynProvider,
        mut stream: BlockStream,
        cancel: &CancellationToken,
    ) {
        let stall_timeout = Self::stall_timeout(self.network.block_time());

        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                next = tokio::time::timeout(stall_timeout, stream.next()) => {
                    match next {
                        Ok(Some(_)) => self.record_connected(),
                        Ok(None) => {
                            self.metrics.ws_provider_disconnected_total.increment(1);
                            tracing::warn!("stream terminated, subscription closed by server");
                            return;
                        },
                        Err(_) => {
                            self.metrics.ws_provider_disconnected_total.increment(1);
                            tracing::warn!(stall_timeout_s = stall_timeout.as_secs(), "stream stalled");
                            return;
                        }
                    }
                }
            }
        }
    }

    fn notify_reconnect_error(&self, err: &anyhow::Error, reconnect_duration: Duration) {
        self.metrics.ws_reconnect_attempts_total.increment(1);
        self.metrics
            .ws_reconnect_attempt_duration_ms
            .record(reconnect_duration);
        tracing::warn!(err = %err, "ws connect/subscribe failed, backing off");
    }

    fn record_connected(&self) {
        self.metrics.block_accepted_total.increment(1);
        self.connected.store(true, Ordering::Relaxed);
    }

    async fn connect_with_retry(
        &self,
        cancel: &CancellationToken,
        ws_url: &str,
    ) -> Option<(DynProvider, BlockStream)> {
        let backoff = Self::backoff();

        tokio::select! {
            _ = cancel.cancelled() => None,
            res = (|| {
                let url = ws_url.to_owned();
                async move { Self::attempt_to_connect(url).await }
            })
            .retry(backoff)
            .notify(|err, duration| self.notify_reconnect_error(err, duration))
            .when(|_| !cancel.is_cancelled()) => res.ok()
        }
    }

    async fn attempt_to_connect(
        ws_url: String,
    ) -> Result<(DynProvider, BlockStream), anyhow::Error> {
        let ws = WsConnect::new(ws_url);
        let provider = ProviderBuilder::new().connect_ws(ws).await?;
        let sub = provider.subscribe_blocks().await?;
        Ok((provider.erased(), sub.into_stream()))
    }

    fn stall_timeout(block_time: Duration) -> Duration {
        (block_time * STALL_TIMEOUT_BLOCKS).max(MIN_STALL_DURATION)
    }

    fn backoff() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(MIN_RECONNECT_ATTEMPT_DELAY)
            .with_max_delay(MAX_RECONNECT_ATTEMPT_DELAY)
            .with_jitter()
            .with_max_times(usize::MAX)
    }
}
