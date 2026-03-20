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
use metrics::counter;
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::time::interval;

use crate::domain::Session;
use crate::services::calls_queue::{CallsQueue, QueueMessage};
use crate::services::fetch_balances_via_multicall::{BalanceCallCtx, BalancesWithBlock};
use crate::services::subscription::Subscription;
use crate::{
    domain::{BalanceEvent, EvmNetwork},
    evm::wrapped::WrappedToken,
    services::fetch_balances_via_multicall,
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

pub struct WatcherContext {
    pub session: Session,
    pub provider: DynProvider,
    pub ws_provider: DynProvider,
}

type Receiver = tokio::sync::mpsc::Receiver<QueueMessage>;

pub struct Watcher {
    ctx: Arc<WatcherContext>,
    sub: Arc<Subscription>,
    calls_queue: Arc<CallsQueue>,
    // accepts data from calls_queue
    rx: Receiver,
}

impl Watcher {
    pub fn new(ctx: WatcherContext, subscription: Arc<Subscription>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let calls_queue = CallsQueue::new(ctx.session, Arc::new(ctx.provider.clone()), tx);

        Self {
            ctx: Arc::new(ctx),
            calls_queue: Arc::new(calls_queue),
            sub: subscription,
            rx,
        }
    }

    // create all necessary watchers to sync balances
    // spawn_erc20_transfer_listeners - spawn listener for erc20 transfer events
    // spawn_wrapped_events_listener - spawn listener for wrapped token events (deposit/withdrawal)
    // spawn_snapshot_updater - spawn listener for snapshot update (every interval_secs)
    pub async fn spawn_watchers(self, interval_secs: usize) {
        self.spawn_snapshot_updater(interval_secs).await;
        self.spawn_erc20_transfer_listeners().await;
        Self::spawn_queue_result_receiver(Arc::clone(&self.sub), self.rx, self.ctx.session).await;
    }

    // watcher to request balances via multicall every interval_secs to have an actual state
    // it updates the whole state of balances and then send event to clients
    // could be removed if we check more ws subscriptions for updates
    async fn spawn_snapshot_updater(&self, interval_secs: usize) {
        let sub = Arc::clone(&self.sub);
        let ctx = Arc::clone(&self.ctx);
        let cancel = sub.cancellable();
        let notifier = sub.take_sync_notifier();

        let balance_call_ctx = {
            let balance_call_ctx = BalanceCallCtx {
                session: ctx.session,
                provider: Arc::new(ctx.provider.clone()),
            };

            Arc::new(balance_call_ctx)
        };

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(interval_secs as u64));
            let session = ctx.session;

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
                        counter!("snapshot_updater_runs_total").increment(1);
                        Self::fetch_balances_and_broadcast(Arc::clone(&balance_call_ctx), Arc::clone(&sub)).await;
                    }
                    _ = notifier.notified() => {
                        // if there are new tokens that were added to a subscription, we should immediately update a snapshot to not
                        //  wait for the next interval
                        tracing::info!(
                            session = %session,
                            "watched tokens were updated - force sync and reset interval"
                        );
                        counter!("snapshot_updater_runs_total").increment(1);
                        Self::fetch_balances_and_broadcast(Arc::clone(&balance_call_ctx), Arc::clone(&sub)).await;
                        interval.reset();
                    }
                }
            }
        });
    }

    async fn spawn_queue_result_receiver(
        sub: Arc<Subscription>,
        mut rx: Receiver,
        session: Session,
    ) {
        let cancel = sub.cancellable();

        tokio::spawn(async move {
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
                                    tracing::info!(
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
    async fn fetch_balances_and_broadcast(ctx: Arc<BalanceCallCtx>, sub: Arc<Subscription>) {
        let owner = ctx.session.owner;
        let network = ctx.session.network;
        let session = ctx.session;
        let tokens = { sub.watched_tokens().await.into_iter().collect::<Vec<_>>() };
        tracing::info!(
            tokens_count = tokens.len(),
            "snapshot updater fetching balances"
        );
        let result = Self::get_tokens_balance(ctx, &tokens, BlockId::latest()).await;

        match result {
            Ok(balances) => {
                Self::update_balances_and_send_event(Arc::clone(&sub), balances, session).await;
            }
            Err(e) => {
                tracing::error!(
                    owner = %owner,
                    error = %e,
                    "failed to get balances"
                );

                let event = Some(BalanceEvent::Error {
                    code: 500,
                    message: "Error when make multicall3 request".to_string(),
                });
                Self::send_balance_update_event(event, Arc::clone(&sub), Session { owner, network })
            }
        };
    }

    // request balances via multicall for a list of tokens and map error
    async fn get_tokens_balance(
        ctx: Arc<BalanceCallCtx>,
        tokens: &[Address],
        block_id: BlockId,
    ) -> Result<BalancesWithBlock, WatcherError> {
        let owner = ctx.session.owner;
        let network = ctx.session.network;
        fetch_balances_via_multicall::fetch_balances_via_multicall(ctx, tokens, block_id)
            .await
            .map_err(|e| {
                tracing::error!("Failed to get balances for {}: {}", owner, e);
                WatcherError::GettingBalance(owner, network, e.to_string())
            })
    }

    /**
     * Listen Deposit/Withdrawal events
     *
     * Need to sync wrap/unwrap txs to handle wrapped token balance
     */
    async fn spawn_weth9_events_listener(&self) {
        let ctx = Arc::clone(&self.ctx);
        let weth9_address = ctx.session.network.weth9_address();

        let event_signatures = vec![
            WrappedToken::Deposit::SIGNATURE_HASH,
            WrappedToken::Withdrawal::SIGNATURE_HASH,
        ];
        let filter = Filter::new()
            .address(weth9_address)
            .event_signature(event_signatures)
            .topic1(Topic::from(ctx.session.owner));

        let sub: Arc<Subscription> = Arc::clone(&self.sub);

        let balance_call_ctx = {
            let ctx = BalanceCallCtx {
                session: ctx.session,
                provider: Arc::new(self.ctx.provider.clone()),
            };

            Arc::new(ctx)
        };

        let ws_provider = self.ctx.ws_provider.clone();
        let calls_queue = Arc::clone(&self.calls_queue);

        tokio::spawn(async move {
            let sub = Arc::clone(&sub);
            let calls_queue = Arc::clone(&calls_queue);

            Self::run_log_subscription_loop(
                ws_provider,
                filter,
                sub.cancellable(),
                move |log: Log| {
                    let ctx = Arc::clone(&balance_call_ctx);
                    let calls_queue = Arc::clone(&calls_queue);

                    Box::pin(async move {
                        counter!("weth9_events_received_total").increment(1);

                        Self::parse_weth9_logs_and_fetch_balance(ctx.session, &log, calls_queue)
                            .await;
                    })
                },
                move || {
                    let sub = Arc::clone(&sub);
                    let event = Some(BalanceEvent::Error {
                        code: 503,
                        message: "WebSocket connection lost permanently".to_string(),
                    });
                    Self::send_balance_update_event(event, sub, ctx.session);
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
        ws_provider: DynProvider,
        filter: Filter,
        cancel: tokio_util::sync::CancellationToken,
        mut on_log: impl FnMut(Log) -> BoxFuture<'static, ()> + Send + Sync + 'static,
        mut on_drop: impl FnMut() + Send + 'static,
    ) {
        'ws_connection_loop: loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("cancelled log subscription");
                    break 'ws_connection_loop;
                },
                _ = async {
                    match Self::ws_sub_with_retries(ws_provider.clone(), filter.clone()).await {
                        Ok(sub) => {
                            tracing::info!("subscribed to logs");

                            let mut stream = sub.into_stream();
                             'logs_loop: loop {
                                tokio::select! {
                                    _ = cancel.cancelled() => {
                                        tracing::info!("cancelled log subscription");
                                        return;
                                    },
                                    item = stream.next() => {
                                        match item {
                                            Some(log) => {
                                                counter!("events_received_total").increment(1);
                                                on_log(log).await;
                                            },
                                            None => {
                                                counter!("ws_provider_disconnected_total").increment(1);
                                                tracing::warn!("ws stream ended (disconnect). will resubscribe");
                                                break 'logs_loop;
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        Err(err) => {
                            counter!("ws_subscribe_is_down_total").increment(1);
                            tracing::error!(error = %err, "ws subscribe exhausted retries, cancelling");
                            on_drop();
                            cancel.cancel();
                            return;
                        }
                    }

                    counter!("ws_reconnect_attempts_total").increment(1);
                } => {}
            }
        }
    }

    // subscribe to events with backoff
    async fn ws_sub_with_retries(
        ws_provider: DynProvider,
        filter: Filter,
    ) -> Result<alloy::pubsub::Subscription<Log>, WatcherError> {
        let backoff = Self::create_ws_sub_backoff();
        (|| async { ws_provider.subscribe_logs(&filter).await })
            .retry(backoff)
            .notify(|err, duration| {
                tracing::error!(
                    error = %err,
                    duration = ?duration,
                    "failed to subscribe logs"
                );
                counter!("ws_subscribe_errors_total").increment(1);
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
    ) {
        let Ok(parsed_log) = Self::parse_weth9_logs(log).inspect_err(|_| {
            counter!("parse_weth9_logs_failed_total").increment(1);
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

    async fn spawn_erc20_transfer_listeners(&self) {
        let ctx = Arc::clone(&self.ctx);
        let base = Filter::new().event_signature(ERC20::Transfer::SIGNATURE_HASH);
        let from = base.clone().topic1(Topic::from(ctx.session.owner));
        let to = base.clone().topic2(Topic::from(ctx.session.owner));

        self.spawn_erc20_transfer_listener_with_filter(from).await;
        self.spawn_erc20_transfer_listener_with_filter(to).await;
        self.spawn_weth9_events_listener().await;
    }

    // listent to erc20 transfer events for owner (in/out)
    // if an event is received - get balance for token(+ eth balance) and send it to clients
    async fn spawn_erc20_transfer_listener_with_filter(&self, filter: Filter) {
        let ctx = Arc::clone(&self.ctx);
        let sub = Arc::clone(&self.sub);
        let calls_queue = Arc::clone(&self.calls_queue);

        let balance_call_ctx = {
            let ctx = BalanceCallCtx {
                session: ctx.session,
                provider: Arc::new(ctx.provider.clone()),
            };

            Arc::new(ctx)
        };

        let ws_provider = self.ctx.ws_provider.clone();

        tokio::spawn(async move {
            let sub_for_session = Arc::clone(&sub);
            Self::run_log_subscription_loop(
                ws_provider,
                filter,
                sub_for_session.cancellable(),
                move |log: Log| {
                    let call_queue = Arc::clone(&calls_queue);
                    let ctx = Arc::clone(&balance_call_ctx);

                    tracing::info!("received erc20 transfer event: {:#?}", log);
                    counter!("erc20_event_received_total").increment(1);

                    Box::pin(async move {
                        // parse event and send it to calls_queue to update balance
                        Self::parse_transfer_event_and_fetch_balance(
                            Arc::clone(&ctx),
                            Arc::clone(&call_queue),
                            &log,
                        )
                        .await;
                    })
                },
                move || {
                    sub_for_session.send_event(
                        BalanceEvent::Error {
                            code: 503,
                            message: "WebSocket connection lost permanently".to_string(),
                        },
                        ctx.session,
                    );
                },
            )
            .await;
        });
    }

    async fn parse_transfer_event_and_fetch_balance(
        ctx: Arc<BalanceCallCtx>,
        calls_queue: Arc<CallsQueue>,
        log: &Log,
    ) {
        let block_number = log.block_number;

        let decoded_log: Log<ERC20::Transfer> = match log.log_decode() {
            Ok(log) => log,
            Err(err) => {
                counter!("parse_erc20_log_errors_total").increment(1);
                tracing::error!(
                    error = %err,
                    netowrk = %ctx.session.network,
                    owner = %ctx.session.owner,
                    "error when parse log",
                );
                return;
            }
        };

        // always put native address into queue to keep it synced
        let tokens = vec![
            decoded_log.address(),
            ctx.session.network.native_token_address(),
        ];
        calls_queue.upsert_delayed_call(&tokens, block_number).await;
    }
}
