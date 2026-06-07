use crate::evm::erc20::ERC20;
use alloy::eips::BlockId;
use alloy::primitives::BlockNumber;
use alloy::{
    primitives::Address,
    providers::{DynProvider, Provider},
    rpc::types::{Filter, Log, Topic},
    sol_types::SolEvent,
};
use backon::{ExponentialBuilder, Retryable};
use futures::future::BoxFuture;
use futures::StreamExt;
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::interval;
use tokio_util::task::TaskTracker;

use crate::domain::Session;
use crate::metrics::Metrics;
use crate::services::calls_queue::BalanceRefreshQueueHandle;
use crate::services::errors::ServiceError;
use crate::services::rpc_client::{BalancesWithBlock, RpcClient};
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

#[derive(Error, Debug, Clone)]
pub enum WatcherError {
    #[error("unable to get balance for owner{0} in network{1}: {2}")]
    GettingBalance(Address, EvmNetwork, String),

    #[error("WS subscription is exhausted with retires")]
    WsSubscriptionExhausted,
}

#[derive(Error, Debug, Clone)]
pub enum ParseWeb3LogsError {
    #[error("log.topic0() is none")]
    Topic0IsNone,

    #[error("event HASH_SIGNATURE is not expected")]
    UnexpectedHashSignature,
}

pub struct Watcher {
    task_tracker: TaskTracker,
    session: Session,
    sub: Arc<Subscription>,
    ws_connection_pool: Arc<WsConnectionPool>,
    refresh_queue: BalanceRefreshQueueHandle,
    rpc_client: Arc<RpcClient>,
    metrics: Arc<Metrics>,
}

impl Watcher {
    pub fn new(
        task_tracker: TaskTracker,
        rpc_client: Arc<RpcClient>,
        subscription: Arc<Subscription>,
        refresh_queue: BalanceRefreshQueueHandle,
        ws_connection_pool: Arc<WsConnectionPool>,
        metrics: Arc<Metrics>,
        session: Session,
    ) -> Self {
        Self {
            task_tracker,
            session,
            refresh_queue,
            sub: subscription,
            rpc_client,
            ws_connection_pool,
            metrics,
        }
    }

    // create all necessary watchers to sync balances
    // spawn_erc20_transfer_listeners - spawn listener for erc20 transfer events
    // spawn_wrapped_events_listener - spawn listener for wrapped token events (deposit/withdrawal)
    // spawn_snapshot_updater - spawn listener for snapshot update (every interval_secs)
    pub async fn spawn_watchers(
        self: Arc<Self>,
        rx: mpsc::Receiver<Result<BalancesWithBlock, ServiceError>>,
        interval_secs: usize,
    ) {
        Arc::clone(&self)
            .spawn_snapshot_updater(interval_secs)
            .await;
        Arc::clone(&self).spawn_erc20_transfer_listeners().await;
        Arc::clone(&self).spawn_queue_result_receiver(rx).await;
    }

    // watcher to request balances via multicall every interval_secs to have an actual state
    // it updates the whole state of balances and then send event to clients
    // could be removed if we check more ws subscriptions for updates
    async fn spawn_snapshot_updater(self: Arc<Self>, interval_secs: usize) {
        let sub = Arc::clone(&self.sub);
        let cancel = sub.cancellable();
        let sync_balance_notifier = sub.take_sync_notifier();
        let owner = self.session.owner;

        Arc::clone(&self).task_tracker.spawn(async move {
            let mut interval = interval(Duration::from_secs(interval_secs as u64));
            let this = Arc::clone(&self);

            loop {
                let this = Arc::clone(&this);
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::info!(
                            owner = %owner,
                            "cancelled watcher"
                        );
                        break;
                    }
                    _ = interval.tick() => {
                        // every n secs we make a multicall to sync all token balances
                        this.metrics.snapshot_updater_runs_total.increment(1);
                        this.fetch_balances_and_broadcast().await;
                    }
                    // there are few cases when we need to request balances immediately
                    // 1 - when ws is exhausted for a while, and then we got reconnect - we should get
                    // new balance snapshot to be sure we don't lost any events
                    // 2 - when we got new tokens (client sends new tokens list or new custom tokens)
                    // todo - it's better to request new tokens in a different multicall without full snapshot
                    _ = sync_balance_notifier.notified() => {
                        // if there are new tokens that were added to a subscription, we should immediately update a snapshot to not
                        //  wait for the next interval
                        tracing::info!(
                            owner = %owner,
                            "watched tokens were updated - force sync and reset interval"
                        );
                        this.metrics.snapshot_updater_runs_total.increment(1);
                        this.fetch_balances_and_broadcast().await;
                        interval.reset();
                    }
                }
            }
        });
    }

    async fn spawn_queue_result_receiver(
        self: Arc<Self>,
        mut rx: mpsc::Receiver<Result<BalancesWithBlock, ServiceError>>,
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
            .map_err(|e| {
                tracing::error!(
                    error = %e,
                    owner = %owner,
                    "Failed to fetch balance"
                );
                WatcherError::GettingBalance(owner, self.session.network, e.to_string())
            })
    }

    /**
     * Listen Deposit/Withdrawal events
     *
     * Need to sync wrap/unwrap txs to handle wrapped token balance
     */
    async fn spawn_weth9_events_listener(self: Arc<Self>) {
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
                        Box::pin(async move {
                            this.metrics.weth9_events_received_total.increment(1);
                            this.parse_weth9_logs_and_fetch_balance(&log).await;
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
        let pool_guard = match self.ws_connection_pool.acquire().await {
            Ok(pool_guard) => pool_guard,
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "failed to acquire connection pool guard, cancel watchers"
                );
                self.sub.cancellable().cancel();
                cancel.cancel();
                return;
            }
        };

        let this = self.clone();
        'ws_connection_loop: loop {
            let this = Arc::clone(&this);
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("cancelled log subscription");
                    break 'ws_connection_loop;
                },
                _ = async {
                    match Arc::clone(&this).subscribe_with_retries(&pool_guard.provider, filter.clone()).await {
                        Ok(sub) => {
                            tracing::debug!("subscribed to logs");

                            let mut stream = sub.into_stream();
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

    // subscribe to events with backoff
    async fn subscribe_with_retries(
        self: Arc<Self>,
        provider: &DynProvider,
        filter: Filter,
    ) -> Result<alloy::pubsub::Subscription<Log>, WatcherError> {
        let backoff = Self::create_ws_sub_backoff();
        (|| async { provider.subscribe_logs(&filter).await })
            .retry(backoff)
            .notify(move |err, duration| {
                tracing::error!(
                    error = %err,
                    duration = ?duration,
                    "failed to subscribe logs"
                );
                self.metrics.ws_subscribe_errors_total.increment(1);
            })
            .await
            .map_err(|_| WatcherError::WsSubscriptionExhausted)
    }

    fn create_ws_sub_backoff() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(Duration::from_secs(1))
            .with_max_delay(Duration::from_secs(5))
            .with_max_times(5)
            .with_jitter()
    }

    async fn parse_weth9_logs_and_fetch_balance(self: Arc<Self>, log: &Log) {
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
        self.refresh_queue
            .enqueue(weth9_address, block_number)
            .await;
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
            tracing::error!("block_number is None for log(WETH event): {:#?}", log);
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
                    tracing::info!("Deposit event dst={}, wad={}", data.dst, data.wad);

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
                    tracing::info!("Withdrawal event: src={}, wad={}", data.src, data.wad);
                    WethEvents::Withdrawal(block_number)
                })
                .ok();

            return Ok(result);
        };

        tracing::error!("unexpected topic0(WETH9 event): {:#?}", topic0);
        Err(ParseWeb3LogsError::UnexpectedHashSignature)
    }

    async fn spawn_erc20_transfer_listeners(self: Arc<Self>) {
        let session = self.session;
        let base = Filter::new().event_signature(ERC20::Transfer::SIGNATURE_HASH);
        let from = base.clone().topic1(Topic::from(session.owner));
        let to = base.clone().topic2(Topic::from(session.owner));

        Arc::clone(&self)
            .spawn_erc20_transfer_listener_with_filter(from)
            .await;
        Arc::clone(&self)
            .spawn_erc20_transfer_listener_with_filter(to)
            .await;
        self.spawn_weth9_events_listener().await;
    }

    async fn spawn_erc20_transfer_listener_with_filter(self: Arc<Self>, filter: Filter) {
        let session = self.session;
        let sub = Arc::clone(&self.sub);

        let this = Arc::clone(&self);
        self.task_tracker.spawn(async move {
            let _ = Arc::clone(&this)
                .run_log_subscription_loop(
                    filter,
                    move |log: Log| {
                        tracing::info!("received erc20 transfer event: {:#?}", log);
                        this.metrics.erc20_event_received_total.increment(1);

                        let this = Arc::clone(&this);
                        Box::pin(async move {
                            this.parse_transfer_event_and_fetch_balance(&log).await;
                        })
                    },
                    move || {
                        sub.send_event(
                            BalanceEvent::Error {
                                code: 503,
                                message: "WebSocket connection lost permanently".to_string(),
                            },
                            session,
                        );
                    },
                )
                .await;
        });
    }

    async fn parse_transfer_event_and_fetch_balance(self: Arc<Self>, log: &Log) {
        let block_number = log.block_number;

        let decoded_log: Log<ERC20::Transfer> = match log.log_decode() {
            Ok(log) => log,
            Err(err) => {
                self.metrics.parse_erc20_log_errors_total.increment(1);
                tracing::error!(
                    error = %err,
                    owner = %self.session.owner,
                    "error when parse log",
                );
                return;
            }
        };

        // service listens to all transfer events
        // skip all tokens that are not in the watched token list
        let token_address = decoded_log.address();
        if !self.sub.is_watched(&token_address).await {
            tracing::info!(
                token_address = %token_address,
                "token is not watched, skip"
            );
            return;
        }

        tracing::debug!(
            token = %token_address,
            block_number = block_number,
            "erc20 event is parsed, send it to fetch queue"
        );
        self.refresh_queue
            .enqueue(token_address, block_number)
            .await;
    }
}
