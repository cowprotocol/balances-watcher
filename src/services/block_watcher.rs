//! WebSocket health canary: a dedicated `eth_subscribe("newHeads")` subscription
//! that powers `/health` via [`BlockWatcher::is_healthy`].
//!
//! Connect/subscribe and infinite-retry reconnect live in
//! [`crate::ws_connection::WsConnection`]. This module only owns the
//! consume-loop: a stall watchdog
//! (`block_time × STALL_TIMEOUT_BLOCKS`, floored at `MIN_STALL_DURATION`)
//! that forces a fresh subscription when headers stop arriving, and the
//! single-bit health flag. The WS provider is **not** shared with the
//! per-session pool ([`crate::services::ws_connection_pool`]) — a
//! dedicated socket keeps the health signal isolated from data-plane
//! churn (event load, session reconnect storms).
//!
//! Health is a single [`std::sync::atomic::AtomicBool`]: flipped `true` on the
//! first header received, `false` on disconnect, stall, or shutdown.

use crate::domain::EvmNetwork;
use crate::graceful_shutdown::LifeCycle;
use crate::metrics::Metrics;
use crate::ws_connection::{ManagedWsSubscription, WsConnection};
use alloy::primitives::BlockNumber;
use alloy::rpc::types::Header;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

const MIN_STALL_DURATION: Duration = Duration::from_secs(2);
const STALL_TIMEOUT_BLOCKS: u32 = 3;
const POST_DISCONNECT_DELAY: Duration = Duration::from_millis(200);

/// Process-wide WS subscription to `newHeads`. See module docs for the reconnect
/// strategy and the rationale for a dedicated provider.
pub struct BlockWatcher {
    network: EvmNetwork,
    metrics: Arc<Metrics>,
    connected: AtomicBool,
    ws_connection: WsConnection,
    on_connect_tx: watch::Sender<bool>,
    latest_block_tx: watch::Sender<Option<BlockNumber>>,
}

impl BlockWatcher {
    /// Spawns the watcher task on `task_tracker` and returns a shared handle.
    /// The task runs until `cancellation_token` is cancelled.
    pub fn spawn(
        network: EvmNetwork,
        metrics: Arc<Metrics>,
        lifecycle: LifeCycle,
        ws_connection: WsConnection,
    ) -> Arc<Self> {
        let (on_connect_tx, _) = watch::channel(false);
        let (latest_block_tx, _) = watch::channel(None);

        let watcher = Arc::new(Self {
            network,
            metrics,
            connected: AtomicBool::new(false),
            ws_connection,
            on_connect_tx,
            latest_block_tx,
        });

        let watcher_for_spawn = Arc::clone(&watcher);
        lifecycle.task_tracker.spawn(async move {
            watcher_for_spawn.run(lifecycle.cancel_token).await;
        });

        watcher
    }

    /// `true` iff the watcher has received at least one header since the most
    /// recent reconnect and the stream has not since closed or stalled.
    pub fn is_healthy(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub fn watch_connected(&self) -> watch::Receiver<bool> {
        self.on_connect_tx.subscribe()
    }

    /// Emits the block number of every received `newHeads` notification.
    /// Initial value is `None` until the first header lands; on disconnect
    /// the latest observed number is retained.
    pub fn watch_latest_block(&self) -> watch::Receiver<Option<BlockNumber>> {
        self.latest_block_tx.subscribe()
    }

    async fn run(self: Arc<Self>, cancel: CancellationToken) {
        loop {
            if cancel.is_cancelled() {
                break;
            }

            let Some(sub) = self.ws_connection.subscribe_blocks().await else {
                tracing::info!("block subscription cancelled, exiting");
                break;
            };

            tracing::info!("block subscription established, waiting for first header");
            self.on_connect_tx.send_replace(true);

            self.consume_until_disconnect(sub, &cancel).await;
            self.connected.store(false, Ordering::Relaxed);
            self.on_connect_tx.send_replace(false);

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
        mut stream: ManagedWsSubscription<Header>,
        cancel: &CancellationToken,
    ) {
        let stall_timeout = Self::stall_timeout(self.network.block_time());

        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                next = tokio::time::timeout(stall_timeout, stream.next()) => {
                    match next {
                        Ok(Some(header)) => self.record_connected(header.number),
                        Ok(None) => {
                            self.metrics.ws_provider_disconnected_total.increment(1);
                            tracing::warn!("block stream terminated, subscription closed by server");
                            return;
                        },
                        Err(_) => {
                            tracing::warn!(stall_timeout_s = stall_timeout.as_secs(), "stream stalled");
                            return;
                        }
                    }
                }
            }
        }
    }

    fn record_connected(&self, block_number: BlockNumber) {
        self.metrics.block_accepted_total.increment(1);
        self.connected.store(true, Ordering::Relaxed);
        self.latest_block_tx.send_replace(Some(block_number));
    }

    fn stall_timeout(block_time: Duration) -> Duration {
        (block_time * STALL_TIMEOUT_BLOCKS).max(MIN_STALL_DURATION)
    }
}
