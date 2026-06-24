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
use alloy::primitives::BlockNumber;
use alloy::{
    primitives::Address,
    rpc::types::{Filter, Log, Topic},
    sol_types::SolEvent,
};
use futures::future::BoxFuture;
use futures::StreamExt;
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_util::task::TaskTracker;

use crate::domain::Session;
use crate::metrics::Metrics;
use crate::services::balance_refresh_queue::BalanceRefreshQueueHandle;
use crate::services::rpc_client::{BalancesWithBlock, RpcClient, RpcError};
use crate::services::subscription::Subscription;
use crate::services::ws_connection_pool::WsConnectionPool;
use crate::{
    domain::{BalanceEvent, EvmNetwork},
    evm::wrapped::WrappedToken,
};

enum WethEvents {
    Deposit(Option<BlockNumber>),
    Withdrawal(Option<BlockNumber>),
}

/// Errors surfaced from the watcher's RPC interactions.
#[derive(Error, Debug, Clone)]
pub enum WatcherError {
    #[error("unable to get balance for owner{0} in network{1}: {2}")]
    GettingBalance(Address, EvmNetwork, String),
}

/// Errors from decoding raw `Log` items into known event shapes.
#[derive(Error, Debug, Clone)]
pub enum ParseWeb3LogsError {
    #[error("log.topic0() is none")]
    Topic0IsNone,

    #[error("event HASH_SIGNATURE is not expected")]
    UnexpectedHashSignature,
}

/// Per-session worker. Wraps the session-scoped state and RPC handles, and
/// spawns the background tasks via [`Self::spawn_watchers`].
pub struct Watcher {
    task_tracker: TaskTracker,
    session: Session,
    sub: Arc<Subscription>,
    ws_connection_pool: Arc<WsConnectionPool>,
    rpc_client: Arc<RpcClient>,
    metrics: Arc<Metrics>,
}

impl Watcher {
    /// Construct a watcher. Does not start any background tasks — call
    /// [`Self::spawn_watchers`] for that.
    pub fn new(
        task_tracker: TaskTracker,
        rpc_client: Arc<RpcClient>,
        subscription: Arc<Subscription>,
        ws_connection_pool: Arc<WsConnectionPool>,
        metrics: Arc<Metrics>,
        session: Session,
    ) -> Self {
        Self {
            task_tracker,
            session,
            sub: subscription,
            rpc_client,
            ws_connection_pool,
            metrics,
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
        refresh_queue: BalanceRefreshQueueHandle,
        interval_secs: usize,
    ) {
        Arc::clone(&self)
            .spawn_snapshot_updater(interval_secs)
            .await;
        Arc::clone(&self)
            .spawn_erc20_transfer_listeners(refresh_queue)
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
        let refresh_notifier = sub.snapshot_refresh_notifier();
        let owner = self.session.owner;

        Arc::clone(&self).task_tracker.spawn(async move {
            // Wait for the first refresh request before starting the interval.
            // The interval is intentionally created AFTER this wait so its
            // deadline is anchored to the moment the loop actually starts,
            // not to spawn time.
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = refresh_notifier.notified() => {}
            }
            tracing::debug!(
                owner = %owner,
                "snapshot updater unblocked, starting periodic refresh loop"
            );

            let interval_duration = Duration::from_secs(interval_secs as u64);
            let mut interval = interval(interval_duration);

            let this = Arc::clone(&self);

            loop {
                let this = Arc::clone(&this);
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::debug!(
                            owner = %owner,
                            "snapshot updater cancelled"
                        );
                        break;
                    }
                    _ = interval.tick() => {
                        this.metrics.snapshot_updater_runs_total.increment(1);
                        this.fetch_balances_and_broadcast().await;
                    }
                    _ = refresh_notifier.notified() => {
                        interval.reset();
                        tracing::debug!(
                            owner = %owner,
                            "snapshot refresh requested"
                        );
                        this.metrics.snapshot_updater_runs_total.increment(1);
                        this.fetch_balances_and_broadcast().await;
                    }
                }
            }
        });
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
    async fn fetch_balances_and_broadcast(self: Arc<Self>) {
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
        let result = Arc::clone(&self)
            .get_tokens_balance(&tokens, BlockId::latest())
            .await;

        match result {
            Ok(balances) => {
                self.update_balances_and_send_event(balances).await;
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
                self.send_balance_update_event(event)
            }
        };
    }

    // request balances via multicall for a list of tokens and map error
    async fn get_tokens_balance(
        self: Arc<Self>,
        tokens: &[Address],
        block_id: BlockId,
    ) -> Result<BalancesWithBlock, WatcherError> {
        let owner = self.session.owner;
        self.rpc_client
            .fetch_balances_via_multicall(owner, tokens, block_id)
            .await
            .map_err(|e| WatcherError::GettingBalance(owner, self.session.network, e.to_string()))
    }

    /**
     * Listen Deposit/Withdrawal events
     *
     * Need to sync wrap/unwrap txs to handle wrapped token balance
     */
    async fn spawn_weth9_events_listener(
        self: Arc<Self>,
        refresh_queue: BalanceRefreshQueueHandle,
    ) {
        let session = self.session;
        let weth9_address = session.network.weth9_address();

        let event_signatures = vec![
            WrappedToken::Deposit::SIGNATURE_HASH,
            WrappedToken::Withdrawal::SIGNATURE_HASH,
        ];
        let filter = Filter::new()
            .address(weth9_address)
            .event_signature(event_signatures)
            .topic1(Topic::from(session.owner));

        let this = Arc::clone(&self);
        self.task_tracker.spawn(async move {
            let this_on_log = Arc::clone(&this);
            let this_on_drop = Arc::clone(&this);

            let _ = Arc::clone(&this_on_log)
                .run_log_subscription_loop(
                    filter,
                    move |log: Log| {
                        let this = Arc::clone(&this_on_log);
                        let refresh_queue = refresh_queue.clone();

                        Box::pin(async move {
                            this.metrics.weth9_events_received_total.increment(1);
                            this.parse_weth9_logs_and_fetch_balance(refresh_queue, &log)
                                .await;
                        })
                    },
                    move || {
                        let event = Some(BalanceEvent::Error {
                            code: 503,
                            message: "WebSocket connection lost permanently".to_string(),
                        });
                        let this = Arc::clone(&this_on_drop);
                        this.send_balance_update_event(event);
                    },
                )
                .await;
        });
    }

    fn send_balance_update_event(self: Arc<Self>, event: Option<BalanceEvent>) {
        let Some(event) = event else {
            tracing::info!(owner = %self.session.owner, "no balance update to send (empty diff)");
            return;
        };

        self.sub.send_event(event, self.session);
    }

    // create a subscription to ws provider and run a loop to listen to logs
    // if log is received - call on_log callback
    // if ws provider disconnects - reconnect and continue listening
    async fn run_log_subscription_loop(
        self: Arc<Self>,
        filter: Filter,
        mut on_web3_log_received: impl FnMut(Log) -> BoxFuture<'static, ()> + Send + Sync + 'static,
        mut on_drop: impl FnMut() + Send + 'static,
    ) {
        let cancel = self.sub.cancellable();

        let mut is_reconnect = false;

        let this = self.clone();
        'ws_connection_loop: loop {
            let this = Arc::clone(&this);
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::debug!("log subscription cancelled");
                    break 'ws_connection_loop;
                },
                _ = async {
                    match Arc::clone(&this.ws_connection_pool).subscribe(&filter).await {
                        Ok(sub) => {
                            // notify to refresh full snapshot
                            this.sub.emit_balance_snapshot_refresh();
                            if is_reconnect {
                                this.metrics.ws_resubscribed_total.increment(1);
                            }

                            is_reconnect = true;
                            tracing::debug!("ws subscribed");

                            let mut stream = sub.ws_sub.into_stream();
                             'web3_logs_loop: loop {
                                tokio::select! {
                                    _ = cancel.cancelled() => {
                                        tracing::debug!("cancelled log subscription");
                                        return;
                                    },
                                    item = stream.next() => {
                                        match item {
                                            Some(log) => {
                                                on_web3_log_received(log).await;
                                            },
                                            None => {
                                                this.metrics.ws_provider_disconnected_total.increment(1);
                                                tracing::warn!("ws stream ended (disconnect). will resubscribe");
                                                break 'web3_logs_loop;
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        Err(err) => {
                            this.metrics.ws_subscribe_is_down_total.increment(1);
                            tracing::error!(error = %err, "ws subscribe exhausted retries, cancelling");
                            on_drop();
                            cancel.cancel();
                            return;
                        }
                    }

                    this.metrics.ws_reconnect_attempts_total.increment(1);
                } => {}
            }
        }
    }

    async fn parse_weth9_logs_and_fetch_balance(
        self: Arc<Self>,
        refresh_queue: BalanceRefreshQueueHandle,
        log: &Log,
    ) {
        tracing::debug!("got weth9 log: {:?}", log);

        let Ok(parsed_log) = Self::parse_weth9_logs(log) else {
            self.metrics.parse_weth9_logs_failed_total.increment(1);
            return;
        };

        let block_number = match parsed_log {
            Some(WethEvents::Deposit(block_id)) => block_id,
            Some(WethEvents::Withdrawal(block_id)) => block_id,
            _ => None,
        };

        let weth9_address = self.session.network.weth9_address();
        tracing::debug!(
            token = %weth9_address,
            block_number = block_number,
            "weth9 log is parsed, send fetching balance request to a queue"
        );
        refresh_queue.enqueue(weth9_address, block_number).await;
    }

    // parse WETH logs, search DEPOSIT/WITHDRAWAL events
    // if there is no DEPOSIT/WITHDRAWAL event signature in a log - return Error
    // otherwise return parsed event data
    fn parse_weth9_logs(log: &Log) -> Result<Option<WethEvents>, ParseWeb3LogsError> {
        let topic0 = match log.topic0() {
            Some(topic0) => topic0,
            None => {
                tracing::warn!("topic0 is None for log(WETH event): {:#?}", log);
                return Err(ParseWeb3LogsError::Topic0IsNone);
            }
        };

        let block_number = log.block_number.or_else(|| {
            // Falls back to `BlockId::latest()` at fetch time — recoverable, but
            // worth flagging since pinned-block refresh is preferable.
            tracing::warn!(
                ?log,
                "weth9 event log has no block_number, will refresh at latest"
            );
            None
        });

        if *topic0 == WrappedToken::Deposit::SIGNATURE_HASH {
            let result = log
                .log_decode::<WrappedToken::Deposit>()
                .inspect_err(|err| {
                    tracing::error!(
                        error = %err,
                        "error when decode DEPOSIT event"
                    );
                })
                .map(|log| {
                    let data = log.inner.data;
                    tracing::debug!(dst = %data.dst, wad = %data.wad, "weth9 deposit event");

                    WethEvents::Deposit(block_number)
                })
                .ok();

            return Ok(result);
        }

        if *topic0 == WrappedToken::Withdrawal::SIGNATURE_HASH {
            let result = log
                .log_decode::<WrappedToken::Withdrawal>()
                .inspect_err(|err| {
                    tracing::error!(
                        error = %err,
                        "error when decode Withdrawal event"
                    );
                })
                .map(|log| {
                    let data = log.inner.data;
                    tracing::debug!(src = %data.src, wad = %data.wad, "weth9 withdrawal event");
                    WethEvents::Withdrawal(block_number)
                })
                .ok();

            return Ok(result);
        };

        tracing::error!("unexpected topic0(WETH9 event): {:#?}", topic0);
        Err(ParseWeb3LogsError::UnexpectedHashSignature)
    }

    // Two WS subscriptions — Transfer(from=owner) and Transfer(to=owner) — plus
    // the WETH9 listener. The filter is **address-less** by design: we match on
    // `event_signature(Transfer)` + the owner topic, then drop unwatched tokens
    // client-side in `parse_transfer_event` via `Subscription::is_watched`.
    //
    // This sidesteps any provider-side cap on addresses-per-filter (Alchemy and
    // Infura both impose one, in the low thousands), and keeps the WS
    // subscription set bounded — three per session, regardless of how large the
    // watched-token list grows.
    async fn spawn_erc20_transfer_listeners(
        self: Arc<Self>,
        refresh_queue: BalanceRefreshQueueHandle,
    ) {
        self.spawn_weth9_events_listener(refresh_queue).await;
    }
}
