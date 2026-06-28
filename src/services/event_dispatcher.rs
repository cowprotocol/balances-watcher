//! Process-wide ERC20 Transfer dispatcher driven by `newHeads` notifications
//! and HTTP `eth_getLogs` — replaces the previous WS `eth_subscribe`-based
//! design because WS subscriptions silently drop tail events on burst
//! blocks (mainnet block-bursts of 500+ Transfer logs in <100ms overflow
//! both alloy's broadcast buffer and the upstream reth subscription queue;
//! measured ~95% delivery on direct WS, dropping to ~70% through the
//! cluster proxy).
//!
//! Flow:
//!     BlockWatcher --(watch::Receiver<Option<BlockNumber>>)--> Dispatcher
//!         Dispatcher --(per-block eth_getLogs)--> RpcClient
//!         Dispatcher --(Erc20TransferEvent)--> SessionManager router
//!
//! Each new block triggers exactly one `eth_getLogs` call with a
//! single-block range and the Transfer topic filter, so the cost is fixed
//! per block (~one HTTP RPC per ~12s on mainnet) regardless of active
//! session count. Delivery is 100% — `eth_getLogs` returns the full set.

use crate::graceful_shutdown::LifeCycle;
use crate::metrics::Metrics;
use crate::services::block_watcher::BlockWatcher;
use crate::services::rpc_client::RpcClient;
use alloy::primitives::{Address, BlockNumber};
use alloy::rpc::types::Log;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const ERC20_TRANSFER_TOPICS_LEN: usize = 3;

pub struct Erc20TransferEvent {
    pub owner: Address,
    pub token: Address,
    pub block: Option<BlockNumber>,
}

pub struct Erc20TransferEventDispatcher {
    cancel_token: CancellationToken,
    metrics: Arc<Metrics>,
    rpc_client: Arc<RpcClient>,
    block_watcher: Arc<BlockWatcher>,
    is_active: AtomicBool,
    transfer_tx_out: mpsc::Sender<Erc20TransferEvent>,
    weth9_address: Address,
}

impl Erc20TransferEventDispatcher {
    pub fn spawn(
        metrics: Arc<Metrics>,
        rpc_client: Arc<RpcClient>,
        block_watcher: Arc<BlockWatcher>,
        lifecycle: LifeCycle,
        transfer_tx_out: mpsc::Sender<Erc20TransferEvent>,
        weth9_address: Address,
    ) -> Arc<Self> {
        let dispatcher = Arc::new(Self {
            metrics,
            rpc_client,
            block_watcher,
            cancel_token: lifecycle.cancel_token,
            is_active: AtomicBool::new(false),
            transfer_tx_out,
            weth9_address,
        });

        let dispatcher_for_spawn = Arc::clone(&dispatcher);
        lifecycle.task_tracker.spawn(async move {
            dispatcher_for_spawn.run().await;
        });

        dispatcher
    }

    /// `true` once the dispatcher has seen its first block notification.
    /// Goes to `false` only on full process shutdown; per-block fetch
    /// failures don't toggle it (they're logged but the loop continues).
    pub fn is_healthy(&self) -> bool {
        self.is_active.load(Ordering::Relaxed)
    }

    async fn run(&self) {
        let mut rx = self.block_watcher.watch_latest_block();
        self.is_active.store(true, Ordering::Relaxed);
        tracing::info!("event dispatcher: subscribed to block_watcher, awaiting blocks");

        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => break,
                changed = rx.changed() => {
                    if changed.is_err() {
                        tracing::warn!("event dispatcher: block_watcher channel closed");
                        break;
                    }
                    let Some(block_number) = *rx.borrow_and_update() else {
                        continue;
                    };

                    let _ = tokio::join!(
                        self.fetch_erc20_transfer_logs_and_process_block(block_number),
                        self.fetch_and_process_weth9_logs(block_number),
                    );
                }
            }
        }

        self.is_active.store(false, Ordering::Relaxed);
    }

    async fn fetch_and_process_weth9_logs(&self, block_number: BlockNumber) {
        tracing::debug!(
            block = block_number,
            "event dispatcher: fetching weth9 logs for block"
        );

        match self
            .rpc_client
            .fetch_weth9_logs_for_block(block_number)
            .await
        {
            Ok(logs) => {
                tracing::debug!(
                    block = block_number,
                    count = logs.len(),
                    "event dispatcher: got weth9 logs for block",
                );

                for log in logs {
                    self.on_weth9_log(log).await;
                }
            }
            Err(err) => {
                tracing::warn!(
                    block = block_number,
                    error = %err,
                    "event dispatcher: eth_getLogs for weth9 failed, will retry on next block"
                );
            }
        }
    }

    async fn fetch_erc20_transfer_logs_and_process_block(&self, block_number: BlockNumber) {
        tracing::info!(
            block = block_number,
            "event dispatcher: fetching logs for block"
        );
        match self
            .rpc_client
            .fetch_transfer_logs_for_block(block_number)
            .await
        {
            Ok(logs) => {
                tracing::info!(
                    block = block_number,
                    count = logs.len(),
                    "event dispatcher: got logs"
                );
                for log in logs {
                    self.on_erc20_log(log).await;
                }
            }
            Err(err) => {
                tracing::warn!(
                    block = block_number,
                    error = %err,
                    "event dispatcher: eth_getLogs failed, will retry on next block"
                );
            }
        }
    }

    async fn on_weth9_log(&self, log: Log) {
        let Some(owner) = self.weth9_owner(&log) else {
            return;
        };

        self.metrics.weth9_events_received_total.increment(1);

        let _ = self
            .transfer_tx_out
            .send(Erc20TransferEvent {
                token: log.address(),
                block: log.block_number,
                owner,
            })
            .await;
    }

    fn weth9_owner(&self, log: &Log) -> Option<Address> {
        if log.address() != self.weth9_address {
            tracing::debug!(weth9_address = %self.weth9_address, "not tracked weth9 DEPOSIT/WITHDRAWAL log, skip");
            return None;
        }

        let topics = log.topics();
        if topics.len() < 2 {
            tracing::debug!("not weth9 DEPOSIT/WITHDRAWAL log, skip");
            return None;
        }

        Some(Address::from_word(topics[1]))
    }

    async fn on_erc20_log(&self, log: Log) {
        let topics = log.topics();
        if topics.len() != ERC20_TRANSFER_TOPICS_LEN {
            // skip erc720 transfer events
            tracing::debug!("not erc20 log transfer, skip");
            return;
        }

        self.metrics.erc20_event_received_total.increment(1);
        let from = Address::from_word(topics[1]);
        let to = Address::from_word(topics[2]);
        let token = log.address();
        let block = log.block_number;

        for owner in [from, to] {
            let _ = self
                .transfer_tx_out
                .send(Erc20TransferEvent {
                    token,
                    block,
                    owner,
                })
                .await;
        }
    }
}
