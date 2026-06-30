//! Per-session background workers. One `Watcher` owns a tree of spawned
//! Tokio tasks that keep balances in sync for a single `(owner, network)`:
//!
//! - **Snapshot updater** — periodic full multicall + on-demand resync on
//!   cold-start, WS reconnect, and watched-token-list extension.
//! - **ERC20 / WETH9 log listeners** — WS subscriptions that filter events
//!   client-side and enqueue refresh requests into `BalanceRefreshQueue`.
//! - **Queue result receiver** — drains multicall results from the queue
//!   into the broadcast stream feeding SSE clients.
//!
//! Lifecycle is gated by the per-session `CancellationToken` carried inside
//! [`Subscription`]. All workers exit when that token fires.
//!
//! ```text
//!     WS pool                              snapshot loop
//!        │                                       ▲
//!        ▼                                       │ Notify::notify_one
//!     log listeners ─► BalanceRefreshQueue ─► queue receiver ─► broadcast
//! ```

use alloy::eips::BlockId;
use futures::StreamExt;
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, watch};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::domain::BalanceEvent;
use crate::domain::Session;
use crate::metrics::Metrics;
use crate::services::block_watcher::BlockWatcher;
use crate::services::rpc_client::{BalancesWithBlock, RpcClient, RpcError};
use crate::services::subscription::Subscription;

/// Per-session worker. Wraps the session-scoped state and RPC handles, and
/// spawns the background tasks via [`Self::spawn_watchers`].
pub struct SnapshotUpdater {
    task_tracker: TaskTracker,
    session: Session,
    sub: Arc<Subscription>,
    rpc_client: Arc<RpcClient>,
    metrics: Arc<Metrics>,
    block_watcher: Arc<BlockWatcher>,
}

impl SnapshotUpdater {
    /// Construct a watcher. Does not start any background tasks — call
    /// [`Self::spawn_watchers`] for that.
    pub fn new(
        task_tracker: TaskTracker,
        rpc_client: Arc<RpcClient>,
        subscription: Arc<Subscription>,
        metrics: Arc<Metrics>,
        session: Session,
        block_watcher: Arc<BlockWatcher>,
    ) -> Self {
        Self {
            task_tracker,
            session,
            sub: subscription,
            rpc_client,
            metrics,
            block_watcher,
        }
    }

    /// Spawn the three background tasks that keep balances in sync for the
    /// session:
    ///
    /// - **snapshot updater** — periodic full multicall + reconnect-driven
    ///   resync (see [`Self::spawn_snapshot_updater`]).
    /// - **ERC20 transfer listeners** — WS subscriptions for `Transfer` /
    ///   WETH9 events.
    /// - **queue result receiver** — drains `BalanceRefreshQueue` results
    ///   into the broadcast stream.
    ///
    /// `refresh_queue` is the producer side of the refresh queue, owned upstream
    /// by [`crate::services::subscription_manager`] — Watcher holds it only to
    /// let its WS listeners enqueue refreshes. It is a transient dependency
    /// that disappears once the shared `EventDispatcher` takes over routing in
    /// a later migration phase.
    ///
    /// Idempotent at the session level via
    /// [`crate::services::subscription_manager::SubscriptionManager::upsert`],
    /// which hands back the queue endpoints only on first creation —
    /// `SessionManager` calls this exactly once per session lifetime.
    pub async fn spawn_watchers(
        self: Arc<Self>,
        rx: mpsc::Receiver<Result<BalancesWithBlock, RpcError>>,
        interval_secs: usize,
    ) {
        Arc::clone(&self)
            .spawn_snapshot_updater(interval_secs)
            .await;
        Arc::clone(&self).spawn_queue_result_receiver(rx).await;
    }

    // Periodic full balance snapshot + on-demand resync.
    //
    // Stays parked until the first refresh signal arrives (cold-start race
    // closed: log subscriptions must be live before we issue a multicall —
    // otherwise Transfer events between the multicall block and the WS
    // subscribe handshake would be silently dropped). After the first
    // signal:
    // - `interval.tick()` — periodic refresh every `interval_secs`.
    // - notifier — on-demand resync (new tokens via `extend_tokens`, WS
    //   resubscribe after disconnect, cold start).
    async fn spawn_snapshot_updater(self: Arc<Self>, interval_secs: usize) {
        let sub = Arc::clone(&self.sub);
        let cancel = sub.cancellable();
        let owner = self.session.owner;

        Arc::clone(&self).task_tracker.spawn(async move {
            // Wait for node ws connection is being established
            let mut rx_connected = self.block_watcher.watch_connected();
            loop {
                if !*rx_connected.borrow_and_update() {
                    tracing::debug!(owner = %owner, "snapshot updater blocked, waiting for BlockWatcher connection");

                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        res = rx_connected.changed() => {
                            if let Err(err) = res {
                                tracing::error!(err = ?err, "rx_connected change failed");
                                return;
                            }
                        }
                    }
                }

                tracing::debug!(
                    owner = %owner,
                    "snapshot updater unblocked, starting periodic refresh loop"
                );

                Arc::clone(&self)
                    .run_snapshot_update_by_interval(&cancel, &mut rx_connected, interval_secs)
                    .await;
            }
        });
    }

    async fn run_snapshot_update_by_interval(
        self: Arc<Self>,
        cancel: &CancellationToken,
        rx_connected: &mut watch::Receiver<bool>,
        interval_secs: usize,
    ) {
        let interval_duration = Duration::from_secs(interval_secs as u64);
        let mut interval = interval(interval_duration);
        let refresh_balance_notifier = self.sub.snapshot_refresh_notifier();
        let owner = self.session.owner;

        loop {
            let this = Arc::clone(&self);

            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::debug!(
                        owner = %owner,
                        "snapshot updater cancelled"
                    );
                    break;
                }
                _ = rx_connected.changed() => {
                    if !*rx_connected.borrow_and_update() {
                        tracing::debug!("BlockWatcher disconnected, exiting interval");
                        return;
                    }
                }
                _ = interval.tick() => {
                    this.metrics.snapshot_updater_runs_total.increment(1);
                    this.fetch_balances_and_broadcast(cancel).await;
                },
                _ = refresh_balance_notifier.notified() => {
                    interval.reset();
                    this.metrics.snapshot_updater_runs_total.increment(1);
                    this.fetch_balances_and_broadcast(cancel).await;
                }
            }
        }
    }

    async fn spawn_queue_result_receiver(
        self: Arc<Self>,
        mut rx: mpsc::Receiver<Result<BalancesWithBlock, RpcError>>,
    ) {
        let cancel = self.sub.cancellable();

        Arc::clone(&self).task_tracker.spawn(async move {
            let this = Arc::clone(&self);
            loop {
                let this = Arc::clone(&this);
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::info!("cancelled spawn_queue_result_receiver watcher");
                        break;
                    },
                    msg = rx.recv() => {
                        if let Some(result) = msg {
                            match result {
                                Ok(balances) => {
                                    tracing::debug!(
                                        owner = %this.session.owner,
                                        "balances received from queue, updating snapshot"
                                    );

                                    this.update_balances_and_send_event(balances).await;
                                },
                                Err(err) => {
                                    tracing::error!(
                                        owner = %this.session.owner,
                                        error = %err,
                                        "error from watcher: close session"
                                    );

                                    this.sub.send_event(BalanceEvent::Error {
                                        code: 503,
                                        message: "RPC provider connection lost permanently".to_string(),
                                    }, this.session);

                                    cancel.cancel();
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    async fn update_balances_and_send_event(self: Arc<Self>, balances: BalancesWithBlock) {
        let event = {
            let diff = self.sub.update_balances_and_take_diff(balances).await;

            if !diff.is_empty() {
                Some(BalanceEvent::BalanceUpdate(diff))
            } else {
                tracing::info!(owner = %self.session.owner, "diff is empty, skipping broadcast");
                None
            }
        };

        self.send_balance_update_event(event);
    }

    // request all balances for a list of watched tokens via multicall and broadcast them to clients
    async fn fetch_balances_and_broadcast(self: Arc<Self>, cancel: &CancellationToken) {
        let tokens = {
            self.sub
                .clone_watched_tokens()
                .await
                .into_iter()
                .collect::<Vec<_>>()
        };

        tracing::info!(
            tokens_count = tokens.len(),
            "snapshot updater fetching balances"
        );

        let mut results = Arc::clone(&self.rpc_client).fetch_balances(
            self.session.owner,
            &tokens,
            BlockId::latest(),
        );

        loop {
            let this = Arc::clone(&self);
            tokio::select! {
                _ = cancel.cancelled() => {
                    break;
                },
                maybe_result = results.next() => {
                    let Some(result) = maybe_result else { return; };
                    match result {
                        Ok(balances) => {
                            this.update_balances_and_send_event(balances).await;
                        }
                        Err(e) => {
                            tracing::error!(
                                owner = %self.session.owner,
                                error = %e,
                                "failed to get balances"
                            );

                            let event = Some(BalanceEvent::Error {
                                code: 500,
                                message: "Error when make multicall3 request".to_string(),
                            });

                            this.send_balance_update_event(event)
                        }
                    }
                }
            }
        }
    }

    fn send_balance_update_event(self: Arc<Self>, event: Option<BalanceEvent>) {
        let Some(event) = event else {
            tracing::info!(owner = %self.session.owner, "no balance update to send (empty diff)");
            return;
        };

        self.sub.send_event(event, self.session);
    }
}
