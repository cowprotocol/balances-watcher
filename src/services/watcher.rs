use crate::evm::erc20::ERC20;
use alloy::eips::BlockId;
use alloy::{
    primitives::Address,
    providers::{DynProvider, Provider},
    rpc::types::{Filter, Log, Topic},
    sol_types::SolEvent,
};
use futures::future::BoxFuture;
use futures::StreamExt;
use metrics::counter;
use std::collections::HashMap;
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::sync::RwLockWriteGuard;
use tokio::time::interval;

use crate::domain::Session;
use crate::services::fetch_balances_via_multicall::{BalanceCallCtx, BalancesWithBlock};
use crate::services::subscription_manager::{Balance, BalanceSnapshot};
use crate::{
    domain::{BalanceEvent, EvmNetwork},
    evm::wrapped::WrappedToken,
    services::{fetch_balances_via_multicall, subscription_manager::Subscription},
};

enum WethEvents {
    Deposit(Option<BlockId>),
    Withdrawal(Option<BlockId>),
}

#[derive(Error, Debug, Clone)]
pub enum WatcherError {
    #[error("unable to get balance for owner{0} in network{1}: {2}")]
    GettingBalance(Address, EvmNetwork, String),

    #[error("Parse log error for network: {1}, owner: {2}: {0}")]
    ParseLog(EvmNetwork, Address, String),
}

#[derive(Error, Debug, Clone)]
pub enum ParseWeb3LogsError {
    #[error("log.topic0() is none")]
    Topic0IsNone,

    #[error("event HASH_SIGNATURE is not expected")]
    UnexpectedHashSignature,
}

pub struct WatcherContext {
    pub owner: Address,
    pub provider: DynProvider,
    pub network: EvmNetwork,
    pub ws_provider: DynProvider,
}

pub struct Watcher {
    ctx: Arc<WatcherContext>,
    sub: Arc<Subscription>,
}

impl Watcher {
    pub fn new(ctx: WatcherContext, subscription: Arc<Subscription>) -> Self {
        Self {
            ctx: Arc::new(ctx),
            sub: subscription,
        }
    }

    // create all necessary watchers to sync balances
    // spawn_erc20_transfer_listeners - spawn listener for erc20 transfer events
    // spawn_wrapped_events_listener - spawn listener for wrapped token events (deposit/withdrawal)
    // spawn_snapshot_updater - spawn listener for snapshot update (every interval_secs)
    pub async fn spawn_watchers(&self, interval_secs: usize) {
        self.spawn_snapshot_updater(interval_secs).await;
        self.spawn_erc20_transfer_listeners().await;
        self.spawn_weth9_events_listener().await;
    }

    // watcher to request balances via multicall every interval_secs to have an actual state
    // it update the whole state of balances and then send event to clients
    // could be removed if we check more ws subscriptions for updates
    async fn spawn_snapshot_updater(&self, interval_secs: usize) {
        let sub = Arc::clone(&self.sub);
        let ctx = Arc::clone(&self.ctx);
        let cancel = sub.cancel_token.clone();

        let balance_call_ctx = {
            let balance_call_ctx = BalanceCallCtx {
                session: Session {
                    owner: ctx.owner,
                    network: ctx.network,
                },
                provider: Arc::new(ctx.provider.clone()),
            };

            Arc::new(balance_call_ctx)
        };

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(interval_secs as u64));

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => { break; }
                    _ = interval.tick() => {
                        counter!("snapshot_updater_runs_total").increment(1);
                        Self::fetch_balances_and_broadcast(Arc::clone(&balance_call_ctx), Arc::clone(&sub)).await;
                    }
                }
            }
        });
    }

    // request all balances for a list of watched tokens via multicall and broadcast them to clients
    async fn fetch_balances_and_broadcast(ctx: Arc<BalanceCallCtx>, sub: Arc<Subscription>) {
        let owner = ctx.session.owner;
        let network = ctx.session.network;
        let tokens = {
            sub.tokens
                .read()
                .await
                .clone()
                .into_iter()
                .collect::<Vec<_>>()
        };
        tracing::info!(
            tokens_count = tokens.len(),
            "snapshot updater fetching balances"
        );
        let result = Self::get_tokens_balance(ctx, &tokens, BlockId::latest()).await;

        let event = match result {
            Ok(balances) => {
                let diff = {
                    let balance_snapshot = sub.balances_snapshot.write().await;
                    Self::update_balances_and_take_diff(balance_snapshot, balances)
                };

                if !diff.is_empty() {
                    Some(BalanceEvent::BalanceUpdate(diff))
                } else {
                    None
                }
            }
            Err(e) => {
                tracing::error!(
                    owner = %owner,
                    error = %e,
                    "failed to get balances"
                );
                Some(BalanceEvent::Error {
                    code: 500,
                    message: "Error when make multicall3 request".to_string(),
                })
            }
        };

        Self::send_balance_update_event(event, Arc::clone(&sub), Session { owner, network })
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
        let weth9_address = ctx.network.weth9_address();

        let event_signatures = vec![
            WrappedToken::Deposit::SIGNATURE_HASH,
            WrappedToken::Withdrawal::SIGNATURE_HASH,
        ];
        let filter = Filter::new()
            .address(weth9_address)
            .event_signature(event_signatures)
            .topic1(Topic::from(ctx.owner));

        let sub: Arc<Subscription> = Arc::clone(&self.sub);
        let cancel = sub.cancel_token.clone();

        let balance_call_ctx = {
            let ctx = BalanceCallCtx {
                session: Session {
                    owner: ctx.owner,
                    network: ctx.network,
                },
                provider: Arc::new(self.ctx.provider.clone()),
            };

            Arc::new(ctx)
        };

        let ws_provider = self.ctx.ws_provider.clone();

        tokio::spawn(async move {
            Self::run_log_subscription_loop(ws_provider, filter, cancel, move |log: Log| {
                let sub = Arc::clone(&sub);
                let ctx = Arc::clone(&balance_call_ctx);

                Box::pin(async move {
                    counter!("weth9_events_received_total").increment(1);

                    let event = match Self::parse_weth9_logs_and_fetch_balance(
                        ctx.clone(),
                        &log,
                        weth9_address,
                    )
                    .await
                    {
                        Ok(balances) => {
                            let balance_snapshot = sub.balances_snapshot.write().await;
                            counter!("partial_snapshot_updater_runs_total").increment(1);
                            let diff =
                                Self::update_balances_and_take_diff(balance_snapshot, balances);

                            (!diff.is_empty()).then_some(BalanceEvent::BalanceUpdate(diff))
                        }
                        Err(err) => Some(BalanceEvent::Error {
                            code: 500,
                            message: err.to_string(),
                        }),
                    };

                    Self::send_balance_update_event(event, Arc::clone(&sub), ctx.session);
                })
            })
            .await;
        });
    }

    fn send_balance_update_event(
        event: Option<BalanceEvent>,
        sub: Arc<Subscription>,
        session: Session,
    ) {
        if let Some(event) = event {
            let _ = sub
                .sender
                .send(event)
                .inspect(|_| {
                    counter!("balance_updates_sent_total").increment(1);
                })
                .inspect_err(|err| {
                    tracing::info!(
                        error = %err,
                        sub = %session,
                        "failed to send balance update event"
                    )
                });
        }
    }

    // create a subscription to ws provider and run a loop to listen to logs
    // if log is received - call on_log callback
    // if ws provider disconnects - reconnect and continue listening
    async fn run_log_subscription_loop(
        ws_provider: DynProvider,
        filter: Filter,
        cancel: tokio_util::sync::CancellationToken,
        mut on_log: impl FnMut(Log) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) {
        let mut attempt: u32 = 0;

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    tracing::info!("cancelled log subscription");
                    break;
                },
                _ = async {
                    match ws_provider.clone().subscribe_logs(&filter).await {
                        Ok(sub) => {
                            tracing::info!("subscribed to logs");
                            attempt = 0;

                            let mut stream = sub.into_stream();
                            loop {
                                tokio::select! {
                                    _ = cancel.cancelled() => {
                                        tracing::info!("cancelled log subscription");
                                        break;
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
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        Err(err) => {
                            counter!("ws_subscribe_errors_total").increment(1);
                            tracing::error!(error = %err, "error to subscribe on logs");
                        }
                    }

                    // TODO make it more configurable
                    let delay = Duration::from_secs(1);
                    attempt = attempt.saturating_add(1);
                    tokio::time::sleep(delay).await;
                    counter!("ws_reconnect_attempts_total").increment(1);
                } => {}
            }
        }
    }

    // this function is requesting balance per token + eth balance via multicall
    // the main reason to take both of them - rpc providers usually take the same compute units for balanceOf
    // and for multicall3 (depends on chunks, but for both tokens it would be 1 chunk)
    // so we can get both balances in one request (in the future it would be great to have a list of
    // frequently used tokens to sync their balances more often
    async fn fetch_erc20_and_eth_balance(
        ctx: Arc<BalanceCallCtx>,
        token: Address,
        block_id: BlockId,
    ) -> Result<BalancesWithBlock, WatcherError> {
        let network = ctx.session.network;
        let owner = ctx.session.owner;
        let native_address = network.native_token_address();
        let tokens = vec![token, network.native_token_address()];

        fetch_balances_via_multicall::fetch_balances_via_multicall(ctx, &tokens, block_id)
            .await
            .map_err(|err| {
                tracing::error!(
                    error = %err,
                    network = %network,
                    "error when get balance for tokens: {token}, {native_address}"
                );
                WatcherError::GettingBalance(owner, network, err.to_string())
            })
    }

    async fn parse_weth9_logs_and_fetch_balance(
        ctx: Arc<BalanceCallCtx>,
        log: &Log,
        weth9_address: Address,
    ) -> Result<BalancesWithBlock, WatcherError> {
        let parsed_log = Self::parse_weth9_logs(log).map_err(|err| {
            counter!("parse_weth9_logs_failed_total").increment(1);
            WatcherError::ParseLog(ctx.session.network, ctx.session.owner, err.to_string())
        })?;

        let block_id = match parsed_log {
            Some(WethEvents::Deposit(block_id)) => block_id,
            Some(WethEvents::Withdrawal(block_id)) => block_id,
            _ => None,
        }
        .unwrap_or(BlockId::latest());

        Self::fetch_erc20_and_eth_balance(ctx, weth9_address, block_id).await
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

        let block_id = block_number.map(BlockId::from);

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

                    WethEvents::Deposit(block_id)
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
                    WethEvents::Withdrawal(block_id)
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
        let from = base.clone().topic1(Topic::from(ctx.owner));
        let to = base.clone().topic2(Topic::from(ctx.owner));

        self.spawn_erc20_transfer_listener_with_filter(from).await;
        self.spawn_erc20_transfer_listener_with_filter(to).await;
        self.spawn_weth9_events_listener().await;
    }

    // listent to erc20 transfer events for owner (in/out)
    // if an event is received - get balance for token(+ eth balance) and send it to clients
    async fn spawn_erc20_transfer_listener_with_filter(&self, filter: Filter) {
        let ctx = Arc::clone(&self.ctx);
        let sub = Arc::clone(&self.sub);
        let cancel = sub.cancel_token.clone();

        let balance_call_ctx = {
            let ctx = BalanceCallCtx {
                session: Session {
                    owner: ctx.owner,
                    network: ctx.network,
                },
                provider: Arc::new(ctx.provider.clone()),
            };

            Arc::new(ctx)
        };

        let ws_provider = self.ctx.ws_provider.clone();

        tokio::spawn(async move {
            Self::run_log_subscription_loop(ws_provider, filter, cancel, move |log: Log| {
                let sub = Arc::clone(&sub);
                let ctx = Arc::clone(&balance_call_ctx);

                tracing::info!("received erc20 transfer event: {:#?}", log);
                counter!("erc20_event_received_total").increment(1);

                Box::pin(async move {
                    let token_balance =
                        Self::parse_transfer_event_and_fetch_balance(Arc::clone(&ctx), &log).await;

                    let event = match token_balance {
                        Some(token_balance) => {
                            let balance_snapshot = sub.balances_snapshot.write().await;
                            counter!("partial_snapshot_updater_runs_total").increment(1);
                            let diff = Self::update_balances_and_take_diff(
                                balance_snapshot,
                                token_balance,
                            );

                            (!diff.is_empty()).then_some(BalanceEvent::BalanceUpdate(diff))
                        }
                        None => Some(BalanceEvent::Error {
                            code: 500,
                            message: "unable to parse erc20 tranfer event".to_string(),
                        }),
                    };

                    Self::send_balance_update_event(event, Arc::clone(&sub), ctx.session);
                })
            })
            .await;
        });
    }

    // update snapshot with new balances
    // first compare block_number, if it is bigger than in snapshot - update it
    // if the balance is different - put it in diff
    // return diff
    fn update_balances_and_take_diff(
        mut snapshot: RwLockWriteGuard<BalanceSnapshot>,
        (new_balances, block_number): BalancesWithBlock,
    ) -> HashMap<Address, String> {
        let mut diff: HashMap<Address, String> = HashMap::new();
        if new_balances.is_empty() {
            tracing::warn!("balances is empty, nothing to update");
            return diff;
        }

        for (address, new_balance) in new_balances {
            let current_balance = snapshot.get_mut(&address);
            if let Some(current_balance) = current_balance {
                if current_balance.block_number < block_number {
                    if current_balance.amount != new_balance {
                        diff.insert(address, new_balance.to_string());
                    }

                    *current_balance = Balance {
                        amount: new_balance,
                        block_number,
                    };
                }
            } else {
                diff.insert(address, new_balance.to_string());
                snapshot.insert(
                    address,
                    Balance {
                        amount: new_balance,
                        block_number,
                    },
                );
            }
        }

        diff
    }

    async fn parse_transfer_event_and_fetch_balance(
        ctx: Arc<BalanceCallCtx>,
        log: &Log,
    ) -> Option<BalancesWithBlock> {
        let Some(block_number) = log.block_number else {
            tracing::warn!(
                network = %ctx.session.network,
                "block number is undefined"
            );
            return None;
        };

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
                return None;
            }
        };

        Self::fetch_erc20_and_eth_balance(ctx, decoded_log.address(), BlockId::from(block_number))
            .await
            .ok()
    }
}
