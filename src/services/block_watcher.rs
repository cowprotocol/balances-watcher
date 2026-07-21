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
//!
//! **Stall cross-check.** "No header for N seconds" is not proof the
//! stream is dead — chains with dynamic block production (Linea batches,
//! bursty low-activity periods) legitimately go quiet for longer than any
//! fixed timeout. When the watchdog fires, one HTTP `eth_blockNumber`
//! settles it: head still at our last observed block → the chain is idle,
//! keep the stream and stay healthy; head moved past us → the stream
//! really stalled, resubscribe (the dispatcher's gap backfill recovers
//! the missed range). Check unavailable → assume the worst, resubscribe.

use crate::domain::EvmNetwork;
use crate::graceful_shutdown::LifeCycle;
use crate::metrics::Metrics;
use crate::services::health::SubsystemHealth;
use crate::services::rpc_client::RpcClient;
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

/// Outcome of cross-checking a stalled `newHeads` stream against the HTTP
/// head. See the module docs for the decision table.
#[derive(Debug, PartialEq, Eq)]
enum StallVerdict {
    /// HTTP head has not moved past our last observed block — the chain is
    /// simply not producing; the stream is presumed fine.
    ChainIdle,
    /// HTTP head is ahead of the stream — the subscription genuinely
    /// stalled and must be re-established.
    StreamStalled { http_head: BlockNumber },
    /// No baseline yet (no header this process) or the HTTP check itself
    /// failed — cannot tell, so assume the worst.
    Unconfirmed,
}

/// Process-wide WS subscription to `newHeads`. See module docs for the reconnect
/// strategy and the rationale for a dedicated provider.
pub struct BlockWatcher {
    network: EvmNetwork,
    metrics: Arc<Metrics>,
    connected: AtomicBool,
    latest_block: AtomicU64,
    ws_connection: WsConnection,
    /// HTTP client for the stall check (`eth_blockNumber` cross-check).
    rpc_client: Arc<RpcClient>,
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
        rpc_client: Arc<RpcClient>,
    ) -> (Arc<Self>, mpsc::Receiver<BlockNumber>) {
        let (on_connect_tx, _) = watch::channel(false);
        let (latest_block_tx, latest_block_rx) = mpsc::channel::<BlockNumber>(BLOCK_CHANNEL_CAP);

        let watcher = Arc::new(Self {
            network,
            metrics,
            connected: AtomicBool::new(false),
            latest_block: AtomicU64::new(0),
            ws_connection,
            rpc_client,
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
    /// at least one header since the most recent reconnect — or the stall
    /// check has since confirmed the chain is idle at our last observed
    /// head — and the stream has not since closed or stalled for real. The
    /// `Unhealthy` variant carries a concrete reason string.
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
                            match self.confirm_stall().await {
                                StallVerdict::ChainIdle => {
                                    tracing::debug!(
                                        stall_timeout_s = stall_timeout.as_secs(),
                                        last_observed = self.latest_block.load(Ordering::Relaxed),
                                        "no header within stall timeout but http head has not \
                                         advanced — chain idle, keeping stream"
                                    );
                                    // Confirmed in sync with the chain head, so the
                                    // canary may report healthy even if no header
                                    // arrived since the last resubscribe.
                                    self.connected.store(true, Ordering::Relaxed);
                                }
                                StallVerdict::StreamStalled { http_head } => {
                                    tracing::warn!(
                                        stall_timeout_s = stall_timeout.as_secs(),
                                        http_head,
                                        last_observed = self.latest_block.load(Ordering::Relaxed),
                                        "stream stalled: chain advanced past last observed \
                                         header, resubscribing"
                                    );
                                    return;
                                }
                                StallVerdict::Unconfirmed => {
                                    tracing::warn!(
                                        stall_timeout_s = stall_timeout.as_secs(),
                                        "stream stalled (head check unavailable or no baseline), \
                                         resubscribing"
                                    );
                                    return;
                                }
                            }
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

    fn stall_verdict(last_observed: BlockNumber, http_head: BlockNumber) -> StallVerdict {
        if http_head <= last_observed {
            StallVerdict::ChainIdle
        } else {
            StallVerdict::StreamStalled { http_head }
        }
    }

    /// Cross-check a fired stall watchdog against the HTTP head. Requires a
    /// baseline (`latest_block != 0` — at least one header seen this
    /// process); without one, or when the HTTP check fails or times out,
    /// the verdict is [`StallVerdict::Unconfirmed`] and the caller falls
    /// back to the plain reconnect path.
    async fn confirm_stall(&self) -> StallVerdict {
        let last_observed = self.latest_block.load(Ordering::Relaxed);
        if last_observed == 0 {
            return StallVerdict::Unconfirmed;
        }

        match self.rpc_client.latest_block_number().await {
            Ok(http_head) => Self::stall_verdict(last_observed, http_head),
            Err(err) => {
                tracing::warn!(error = %err, "stall check: eth_blockNumber failed");
                StallVerdict::Unconfirmed
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_at_last_observed_means_idle() {
        assert_eq!(
            BlockWatcher::stall_verdict(100, 100),
            StallVerdict::ChainIdle
        );
    }

    #[test]
    fn head_behind_last_observed_means_idle() {
        // http node briefly behind the ws view, or a reorg to a lower head —
        // neither is evidence the stream is broken
        assert_eq!(
            BlockWatcher::stall_verdict(100, 97),
            StallVerdict::ChainIdle
        );
    }

    #[test]
    fn head_past_last_observed_means_stalled() {
        assert_eq!(
            BlockWatcher::stall_verdict(100, 106),
            StallVerdict::StreamStalled { http_head: 106 }
        );
    }
}
