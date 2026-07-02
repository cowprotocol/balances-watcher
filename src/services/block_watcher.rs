//! Process-wide WS subscription to `newHeads`. Serves three roles:
//!
//! 1. **Health canary** — [`BlockWatcher::health_status`] backs `/health`. A
//!    single [`std::sync::atomic::AtomicBool`] flipped `true` on first header,
//!    `false` on disconnect/stall/shutdown. Downstream health checks
//!    (dispatcher lag) also read `latest_block()` from here.
//!
//! 2. **Block distributor** — every accepted header is pushed into a bounded
//!    [`tokio::sync::mpsc::Sender<BlockNumber>`] (capacity
//!    [`BLOCK_CHANNEL_CAP`]) via `try_send`. The consumer is the
//!    process-wide
//!    [`crate::services::event_dispatcher::Erc20TransferEventDispatcher`]
//!    which reads FIFO and fires per-block `eth_getLogs`.
//!
//! 3. **Reconnect trigger** — [`BlockWatcher::watch_connected`] exposes a
//!    `watch::Receiver<bool>` observed by
//!    [`crate::services::snapshot_updater`] to force a fresh snapshot when
//!    the WS canary comes back up.
//!
//! Connect/subscribe and infinite-retry reconnect live in
//! [`crate::ws_connection::WsConnection`]. This module owns only the
//! consume-loop: a stall watchdog
//! (`block_time × STALL_TIMEOUT_BLOCKS`, floored at `MIN_STALL_DURATION`)
//! that forces a fresh subscription when headers stop arriving, plus the
//! state atomics.

use crate::domain::EvmNetwork;
use crate::graceful_shutdown::LifeCycle;
use crate::metrics::Metrics;
use crate::services::health::SubsystemHealth;
use crate::ws_connection::{ManagedWsSubscription, WsConnection};
use alloy::primitives::BlockNumber;
use alloy::rpc::types::Header;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

const MIN_STALL_DURATION: Duration = Duration::from_secs(2);
const STALL_TIMEOUT_BLOCKS: u32 = 3;
const POST_DISCONNECT_DELAY: Duration = Duration::from_millis(200);

const BLOCK_CHANNEL_CAP: usize = 256;

/// Process-wide WS subscription to `newHeads`. See module docs for the reconnect
/// strategy and the rationale for a dedicated provider.
pub struct BlockWatcher {
    network: EvmNetwork,
    metrics: Arc<Metrics>,
    connected: AtomicBool,
    latest_block: AtomicU64,
    ws_connection: WsConnection,
    on_connect_tx: watch::Sender<bool>,
    latest_block_tx: mpsc::Sender<BlockNumber>,
}

impl BlockWatcher {
    /// Spawns the watcher task on `task_tracker` and returns a shared handle.
    /// The task runs until `cancellation_token` is cancelled.
    pub fn spawn(
        network: EvmNetwork,
        metrics: Arc<Metrics>,
        lifecycle: LifeCycle,
        ws_connection: WsConnection,
    ) -> (Arc<Self>, mpsc::Receiver<BlockNumber>) {
        let (on_connect_tx, _) = watch::channel(false);
        let (latest_block_tx, latest_block_rx) = mpsc::channel::<BlockNumber>(BLOCK_CHANNEL_CAP);

        let watcher = Arc::new(Self {
            network,
            metrics,
            connected: AtomicBool::new(false),
            latest_block: AtomicU64::new(0),
            ws_connection,
            on_connect_tx,
            latest_block_tx,
        });

        let watcher_for_spawn = Arc::clone(&watcher);
        lifecycle.task_tracker.spawn(async move {
            watcher_for_spawn.run(lifecycle.cancel_token).await;
        });

        (watcher, latest_block_rx)
    }

    /// Structured health for `/health`. Healthy iff the watcher has received
    /// at least one header since the most recent reconnect and the stream has
    /// not since closed or stalled. The `Unhealthy` variant carries a concrete
    /// reason string.
    pub fn health_status(&self) -> SubsystemHealth {
        if self.connected.load(Ordering::Relaxed) {
            SubsystemHealth::Healthy
        } else {
            SubsystemHealth::Unhealthy(format!(
                "ws newHeads disconnected (last observed block={})",
                self.latest_block.load(Ordering::Relaxed)
            ))
        }
    }

    pub fn watch_connected(&self) -> watch::Receiver<bool> {
        self.on_connect_tx.subscribe()
    }

    /// Returns the latest block number observed via `newHeads`, or `0` if no
    /// header has arrived yet.
    pub fn latest_block(&self) -> BlockNumber {
        self.latest_block.load(Ordering::Relaxed)
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
        self.latest_block.store(block_number, Ordering::Relaxed);
        match self.latest_block_tx.try_send(block_number) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.block_channel_overflow_total.increment(1);
                tracing::error!(
                    block = block_number,
                    cap = BLOCK_CHANNEL_CAP,
                    "block channel overflow — dispatcher catastrophically behind, dropping"
                )
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("block channel receiver dropped — event dispatcher gone");
            }
        }
    }

    fn stall_timeout(block_time: Duration) -> Duration {
        (block_time * STALL_TIMEOUT_BLOCKS).max(MIN_STALL_DURATION)
    }
}
