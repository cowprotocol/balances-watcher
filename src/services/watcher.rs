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
use tokio::time::interval;
use tokio_util::task::TaskTracker;

use crate::domain::Session;
use crate::metrics::Metrics;
use crate::services::calls_queue::{CallsQueue, QueueMessage};
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

use crate::services::calls_queue::Receiver;

pub struct Watcher {
    task_tracker: TaskTracker,
    session: Session,
    sub: Arc<Subscription>,
    ws_connection_pool: Arc<WsConnectionPool>,
    calls_queue: Arc<CallsQueue>,
    rpc_client: Arc<RpcClient>,
    metrics: Arc<Metrics>,
}

impl Watcher {
    pub fn new(
        task_tracker: TaskTracker,
        rpc_client: Arc<RpcClient>,
        subscription: Arc<Subscription>,
        calls_queue: CallsQueue,
        ws_connection_pool: Arc<WsConnectionPool>,
        metrics: Arc<Metrics>,
        session: Session,
    ) -> Self {
        Self {
            task_tracker,
            session,
            calls_queue: Arc::new(calls_queue),
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
    pub async fn spawn_watchers(self: Arc<Self>, rx: Receiver, interval_secs: usize) {
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
        let session = self.session;
        let fetcher = Arc::clone(&self.rpc_client);
        let metrics = Arc::clone(&self.metrics);

        self.task_tracker.spawn(async move {
            let mut interval = interval(Duration::from_secs(interval_secs as u64));

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::info!(
                            session = %session,
                            "cancelled watcher"
                        );
                        break;
                    }
                    _ = interval.tick() => {
                        // every n secs we make a multicall to sync all token balances
                        metrics.snapshot_updater_runs_total.increment(1);
                        Self::fetch_balances_and_broadcast(Arc::clone(&fetcher), session, Arc::clone(&sub)).await;
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
                            session = %session,
                            "watched tokens were updated - force sync and reset interval"
                        );
                        metrics.snapshot_updater_runs_total.increment(1);
                        Self::fetch_balances_and_broadcast(Arc::clone(&fetcher), session, Arc::clone(&sub)).await;
                        interval.reset();
                    }
                }
            }
        });
    }

    async fn spawn_queue_result_receiver(self: Arc<Self>, mut rx: Receiver) {
        let cancel = self.sub.cancellable();
        let sub = Arc::clone(&self.sub);

        let session = self.session;
        self.task_tracker.spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::info!("cancelled spawn_queue_result_receiver watcher");
                        break;
                    },
                    msg = rx.recv() => {
                        if let Some(msg) = msg {
                            match msg {
                                QueueMessage::Success(balances) => {
                                    tracing::debug!(
                                        session = %session,
                                        "balances received from queue, updating snapshot"
                                    );

                                    Self::update_balances_and_send_event(Arc::clone(&sub), balances, session).await;
                                },
                                QueueMessage::Error(err) => {
                                    tracing::error!(
                                        session = %session,
                                        error = %err,
                                        "error from watcher: close session"
                                    );

                                    sub.send_event(BalanceEvent::Error {
                                        code: 503,
                                        message: "RPC provider connection lost permanently".to_string(),
                                    }, session);

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

    async fn update_balances_and_send_event(
        sub: Arc<Subscription>,
        balances: BalancesWithBlock,
        session: Session,
    ) {
        let event = {
            let diff = sub.update_balances_and_take_diff(balances).await;

            if !diff.is_empty() {
                Some(BalanceEvent::BalanceUpdate(diff))
            } else {
                tracing::info!(session = %session, "diff is empty, skipping broadcast");
                None
            }
        };

        Self::send_balance_update_event(event, Arc::clone(&sub), session);
    }

    // request all balances for a list of watched tokens via multicall and broadcast them to clients
    async fn fetch_balances_and_broadcast(
        fetcher: Arc<RpcClient>,
        session: Session,
        sub: Arc<Subscription>,
    ) {
        let tokens = {
            sub.clone_watched_tokens()
                .await
                .into_iter()
                .collect::<Vec<_>>()
        };
        tracing::info!(
            tokens_count = tokens.len(),
            "snapshot updater fetching balances"
        );
        let result =
            Self::get_tokens_balance(Arc::clone(&fetcher), session, &tokens, BlockId::latest())
                .await;

        match result {
            Ok(balances) => {
                Self::update_balances_and_send_event(Arc::clone(&sub), balances, session).await;
            }
            Err(e) => {
                tracing::error!(
                    session = %session,
                    error = %e,
                    "failed to get balances"
                );

                let event = Some(BalanceEvent::Error {
                    code: 500,
                    message: "Error when make multicall3 request".to_string(),
                });
                Self::send_balance_update_event(event, Arc::clone(&sub), session)
            }
        };
    }

    // request balances via multicall for a list of tokens and map error
    async fn get_tokens_balance(
        fetcher: Arc<RpcClient>,
        session: Session,
        tokens: &[Address],
        block_id: BlockId,
    ) -> Result<BalancesWithBlock, WatcherError> {
        fetcher
            .fetch_balances_via_multicall(session.owner, tokens, block_id)
            .await
            .map_err(|e| {
                tracing::error!(
                    error = %e,
                    session = %session,
                    "Failed to fetch balance"
                );
                WatcherError::GettingBalance(session.owner, session.network, e.to_string())
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

        let sub: Arc<Subscription> = Arc::clone(&self.sub);

        let calls_queue = Arc::clone(&self.calls_queue);
        let metrics = Arc::clone(&self.metrics);

        let this = Arc::clone(&self);
        self.task_tracker.spawn(async move {
            let sub = Arc::clone(&sub);
            let calls_queue = Arc::clone(&calls_queue);
            let metrics_for_log = Arc::clone(&metrics);

            let _ = this
                .run_log_subscription_loop(
                    filter,
                    sub.cancellable(),
                    move |log: Log| {
                        let calls_queue = Arc::clone(&calls_queue);
                        let metrics_for_log = Arc::clone(&metrics_for_log);

                        Box::pin(async move {
                            metrics_for_log.weth9_events_received_total.increment(1);

                            Self::parse_weth9_logs_and_fetch_balance(
                                session,
                                &log,
                                calls_queue,
                                metrics_for_log,
                            )
                            .await;
                        })
                    },
                    move || {
                        let sub = Arc::clone(&sub);
                        let event = Some(BalanceEvent::Error {
                            code: 503,
                            message: "WebSocket connection lost permanently".to_string(),
                        });
                        Self::send_balance_update_event(event, sub, session);
                    },
                )
                .await;
        });
    }

    fn send_balance_update_event(
        event: Option<BalanceEvent>,
        sub: Arc<Subscription>,
        session: Session,
    ) {
        let Some(event) = event else {
            tracing::info!(session = %session, "no balance update to send (empty diff)");
            return;
        };

        sub.send_event(event, session);
    }

    // create a subscription to ws provider and run a loop to listen to logs
    // if log is received - call on_log callback
    // if ws provider disconnects - reconnect and continue listening
    async fn run_log_subscription_loop(
        self: Arc<Self>,
        filter: Filter,
        cancel: tokio_util::sync::CancellationToken,
        mut on_web3_log_received: impl FnMut(Log) -> BoxFuture<'static, ()> + Send + Sync + 'static,
        mut on_drop: impl FnMut() + Send + 'static,
    ) {
        let pool_guard = match self.ws_connection_pool.acquire().await {
            Ok(pool_guard) => pool_guard,
            Err(err) => {
                tracing::error!(
                    error = %err,
                    "failed to acquire connection pool guard, cancel watchers"
                );
                cancel.cancel();
                return;
            }
        };

        'ws_connection_loop: loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("cancelled log subscription");
                    break 'ws_connection_loop;
                },
                _ = async {
                    match Self::subscribe_with_retries(&pool_guard.provider, filter.clone(), Arc::clone(&self.metrics)).await {
                        Ok(sub) => {
                            tracing::info!("subscribed to logs");

                            let mut stream = sub.into_stream();
                             'logs_loop: loop {
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
                                                self.metrics.ws_provider_disconnected_total.increment(1);
                                                tracing::warn!("ws stream ended (disconnect). will resubscribe");
                                                break 'logs_loop;
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        Err(err) => {
                            self.metrics.ws_subscribe_is_down_total.increment(1);
                            tracing::error!(error = %err, "ws subscribe exhausted retries, cancelling");
                            on_drop();
                            cancel.cancel();
                            return;
                        }
                    }

                    self.metrics.ws_reconnect_attempts_total.increment(1);
                } => {}
            }
        }
    }

    // subscribe to events with backoff
    async fn subscribe_with_retries(
        provider: &DynProvider,
        filter: Filter,
        metrics: Arc<Metrics>,
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
                metrics.ws_subscribe_errors_total.increment(1);
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

    async fn parse_weth9_logs_and_fetch_balance(
        session: Session,
        log: &Log,
        calls_queue: Arc<CallsQueue>,
        metrics: Arc<Metrics>,
    ) {
        let Ok(parsed_log) = Self::parse_weth9_logs(log).inspect_err(move |_| {
            metrics.parse_weth9_logs_failed_total.increment(1);
        }) else {
            return;
        };

        let block_number = match parsed_log {
            Some(WethEvents::Deposit(block_id)) => block_id,
            Some(WethEvents::Withdrawal(block_id)) => block_id,
            _ => None,
        };

        // always put native address into queue to keep it synced
        let tokens = vec![
            session.network.weth9_address(),
            session.network.native_token_address(),
        ];
        calls_queue.upsert_delayed_call(&tokens, block_number).await;
    }

    // parse WETH logs, search DEPOSIT/WITHDRAWAL events
    // if there is no DEPOSIT/WITHDRAWAL event signature in a log - return Error
    // otherwise return parsed event data
    fn parse_weth9_logs(log: &Log) -> Result<Option<WethEvents>, ParseWeb3LogsError> {
        let topic0 = match log.topic0() {
            Some(topic0) => topic0,
            None => {
                tracing::error!("topic0 is None for log(WETH event): {:#?}", log);
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
        let calls_queue = Arc::clone(&self.calls_queue);
        let metrics = Arc::clone(&self.metrics);

        let this = Arc::clone(&self);
        self.task_tracker.spawn(async move {
            let sub_for_log_handler = Arc::clone(&sub);
            let metrics_for_log = Arc::clone(&metrics);

            let _ = this
                .run_log_subscription_loop(
                    filter,
                    sub_for_log_handler.cancellable(),
                    move |log: Log| {
                        let calls_queue = Arc::clone(&calls_queue);
                        let metrics_for_log = Arc::clone(&metrics_for_log);
                        let sub_for_parsing_tokens = Arc::clone(&sub_for_log_handler);

                        tracing::info!("received erc20 transfer event: {:#?}", log);
                        metrics_for_log.erc20_event_received_total.increment(1);

                        Box::pin(async move {
                            Self::parse_transfer_event_and_fetch_balance(
                                session,
                                calls_queue,
                                sub_for_parsing_tokens,
                                &log,
                                metrics_for_log,
                            )
                            .await;
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

    async fn parse_transfer_event_and_fetch_balance(
        session: Session,
        calls_queue: Arc<CallsQueue>,
        sub: Arc<Subscription>,
        log: &Log,
        metrics: Arc<Metrics>,
    ) {
        let block_number = log.block_number;

        let decoded_log: Log<ERC20::Transfer> = match log.log_decode() {
            Ok(log) => log,
            Err(err) => {
                metrics.parse_erc20_log_errors_total.increment(1);
                tracing::error!(
                    error = %err,
                    netowrk = %session.network,
                    owner = %session.owner,
                    "error when parse log",
                );
                return;
            }
        };

        // service listens to all transfer events
        // skip all tokens that are not in the watched token list
        let token_address = decoded_log.address();
        if !sub.is_watched(&token_address).await {
            tracing::info!(
                token_address = %token_address,
                "token is not watched, skip"
            );
            return;
        }

        // always put native address into queue to keep it synced
        let tokens = vec![token_address, session.network.native_token_address()];

        calls_queue.upsert_delayed_call(&tokens, block_number).await;
    }
}
