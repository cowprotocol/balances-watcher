//! Process-wide balance-change event dispatcher driven by `newHeads`
//! notifications and HTTP `eth_getLogs`
//!
//! Two log sources are fetched **in parallel** per block:
//! 1. ERC20 `Transfer` (global, topic-filter only) — covers most token
//!    balance changes.
//! 2. WETH9 `Deposit` / `Withdrawal` (address-filtered on the WETH9
//!    contract) — covers wrap/unwrap, which the canonical WETH9 impl
//!    does NOT emit a Transfer for, so they'd be invisible to source #1.
//!
//! Flow:
//!     BlockWatcher --(watch::Receiver<Option<BlockNumber>>)--> Dispatcher
//!         Dispatcher --(per-range eth_getLogs × 2)--> RpcClient
//!         Dispatcher --(Erc20TransferEvent)--> SessionManager router
//!
//! On the happy path each new head triggers exactly two `eth_getLogs`
//! calls with a single-block range, so the cost is fixed per block (~two
//! HTTP RPCs per ~12s on mainnet) regardless of active session count.
//!
//! **Gap backfill.** WS `newHeads` skips heads across reconnects (and can
//! skip under load). When an incoming head is more than one block past
//! `latest_processed_block`, the dispatcher fetches the missing range too:
//! sequential `eth_getLogs` chunks sized to [`BACKFILL_CHUNK_SECONDS`] of
//! chain activity (see [`Erc20TransferEventDispatcher::backfill_chunk_blocks`]), advancing the cursor
//! after each chunk — so a chunk that exhausts retries leaves a smaller
//! residual gap that the next head re-triggers. The backfill is capped at
//! the chain's `max_block_lag()` blocks; anything older is deliberately
//! skipped (the 60s snapshot loop is the recovery path, and processing a
//! log only schedules a balance refresh, so replays and reorg leftovers
//! are harmless — refreshes read current chain state).

use crate::domain::EvmNetwork;
use crate::evm::wrapped::WrappedToken;
use crate::graceful_shutdown::LifeCycle;
use crate::metrics::Metrics;
use crate::services::block_watcher::BlockWatcher;
use crate::services::health::SubsystemHealth;
use crate::services::rpc_client::RpcClient;
use alloy::primitives::{Address, BlockNumber};
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const ERC20_TRANSFER_TOPICS_LEN: usize = 3;

/// Budget per backfill `eth_getLogs` range, expressed in chain time: log
/// volume scales with seconds of chain activity, not block count, so a
/// fixed block-count chunk would be huge on a 12s-block chain and trivial
/// on a 250ms one. Providers cap response size; sequential chunks
/// self-throttle.
const BACKFILL_CHUNK_SECONDS: Duration = Duration::from_secs(10);

/// Fetch work derived from one incoming head vs the dispatcher's cursor.
/// Produced by [`Erc20TransferEventDispatcher::plan_fetch`], executed by
/// the dispatcher loop.
#[derive(Debug, PartialEq, Eq)]
struct FetchPlan {
    /// Blocks deliberately dropped because the gap exceeded the backfill
    /// cap. Healed by the snapshot loop; logged, not replayed.
    skipped_blocks: u64,
    /// Inclusive `(from, to)` ranges to fetch, oldest first, each at most
    /// one backfill chunk wide (see [`Erc20TransferEventDispatcher::backfill_chunk_blocks`]). Empty =
    /// stale or duplicate head.
    ranges: Vec<(BlockNumber, BlockNumber)>,
}

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
    latest_processed_block: AtomicU64,
    weth9_address: Address,
    /// Snapshot of `EvmNetwork::max_block_lag()` for the chain this
    /// dispatcher was spawned for. Kept as a plain field so the health
    /// check stays lock-free.
    max_block_lag: u64,
    /// Blocks per backfill `eth_getLogs` chunk, derived once from the
    /// chain's block time (see [`Erc20TransferEventDispatcher::backfill_chunk_blocks`]).
    backfill_chunk_blocks: u64,
}

impl Erc20TransferEventDispatcher {
    pub fn spawn(
        network: EvmNetwork,
        metrics: Arc<Metrics>,
        rpc_client: Arc<RpcClient>,
        block_watcher: Arc<BlockWatcher>,
        block_number_rx: mpsc::Receiver<BlockNumber>,
        lifecycle: LifeCycle,
        transfer_tx_out: mpsc::Sender<Erc20TransferEvent>,
    ) -> Arc<Self> {
        let dispatcher = Arc::new(Self {
            metrics,
            rpc_client,
            block_watcher,
            cancel_token: lifecycle.cancel_token,
            is_active: AtomicBool::new(false),
            transfer_tx_out,
            weth9_address: network.weth9_address(),
            latest_processed_block: AtomicU64::new(0),
            max_block_lag: network.max_block_lag(),
            backfill_chunk_blocks: Self::backfill_chunk_blocks(network.block_time()),
        });

        let dispatcher_for_spawn = Arc::clone(&dispatcher);
        lifecycle.task_tracker.spawn(async move {
            dispatcher_for_spawn.run(block_number_rx).await;
        });

        dispatcher
    }

    /// Blocks per backfill chunk for a chain with the given block time —
    /// [`BACKFILL_CHUNK_SECONDS`] worth of blocks, at least 1 (chains whose
    /// block time exceeds the budget, e.g. mainnet's 12s, go block-by-block).
    fn backfill_chunk_blocks(block_time: Duration) -> u64 {
        ((BACKFILL_CHUNK_SECONDS.as_millis() / block_time.as_millis().max(1)) as u64).max(1)
    }

    /// Plan the fetch for one incoming head: skip stale heads, fetch the head
    /// itself, and chunk any gap since `latest_processed` — capped at
    /// `max_backfill` blocks counted back from `incoming` (the newest blocks
    /// win; stale history is the snapshot loop's job).
    ///
    /// `latest_processed == 0` means nothing was processed yet: fetch just the
    /// incoming head, there is no baseline to backfill from.
    fn plan_fetch(
        latest_processed: BlockNumber,
        incoming: BlockNumber,
        max_backfill: u64,
        chunk_blocks: u64,
    ) -> FetchPlan {
        if latest_processed != 0 && incoming <= latest_processed {
            return FetchPlan {
                skipped_blocks: 0,
                ranges: Vec::new(),
            };
        }

        let fetch_from = if latest_processed == 0 {
            incoming
        } else {
            let cap_floor = incoming.saturating_sub(max_backfill) + 1;
            (latest_processed + 1).max(cap_floor)
        };

        let mut ranges = Vec::new();
        let mut from = fetch_from;
        while from <= incoming {
            let to = incoming.min(from + chunk_blocks - 1);
            ranges.push((from, to));
            from = to + 1;
        }

        let skipped_blocks = if latest_processed == 0 {
            0
        } else {
            fetch_from - (latest_processed + 1)
        };

        FetchPlan {
            skipped_blocks,
            ranges,
        }
    }

    /// Structured health for `/health`. Healthy from the moment the dispatcher
    /// has seen its first block notification, unless block lag has climbed
    /// past `self.max_block_lag`. Per-block fetch failures do NOT toggle this —
    /// they're logged and the loop continues; only sustained lag is unhealthy.
    pub fn health_status(&self) -> SubsystemHealth {
        if !self.is_active.load(Ordering::Relaxed) {
            return SubsystemHealth::Unhealthy(
                "dispatcher not active yet (startup) or shut down".into(),
            );
        }

        let current_head = self.block_watcher.latest_block();
        if current_head == 0 {
            // warm up
            return SubsystemHealth::Healthy;
        }

        let latest_processed_block = self.latest_processed_block.load(Ordering::Relaxed);
        if latest_processed_block == 0 {
            return SubsystemHealth::Healthy;
        }

        let lag = current_head.saturating_sub(latest_processed_block);
        let max_block_lag = self.max_block_lag;
        if lag > max_block_lag {
            tracing::warn!(
                current_head = current_head,
                lag,
                latest_processed_block,
                max_block_lag,
                "event dispatcher: lag exceeded threshold, dispatcher unhealthy"
            );
            return SubsystemHealth::Unhealthy(format!(
                "block lag {lag} exceeds max {max_block_lag} \
                 (head={current_head}, latest_processed={latest_processed_block})"
            ));
        }
        SubsystemHealth::Healthy
    }

    async fn run(&self, mut block_number_rx: mpsc::Receiver<BlockNumber>) {
        self.is_active.store(true, Ordering::Relaxed);
        tracing::info!("event dispatcher: subscribed to block_watcher, awaiting blocks");

        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => break,
                maybe_latest_block = block_number_rx.recv() => {
                    let Some(block_number) = maybe_latest_block else {
                        tracing::info!("event dispatcher: block_number_rx senders are dropped, stop dispatcher");
                        return;
                    };

                    let latest_processed = self.latest_processed_block.load(Ordering::Relaxed);
                    let plan = Self::plan_fetch(
                        latest_processed,
                        block_number,
                        self.max_block_lag,
                        self.backfill_chunk_blocks,
                    );

                    if plan.ranges.is_empty() {
                        tracing::debug!(
                            block = block_number,
                            latest_processed,
                            "event dispatcher: stale or duplicate head, skipping"
                        );
                        continue;
                    }

                    if plan.skipped_blocks > 0 {
                        tracing::warn!(
                            block = block_number,
                            latest_processed,
                            skipped = plan.skipped_blocks,
                            max_backfill = self.max_block_lag,
                            "event dispatcher: gap exceeds backfill cap, skipping oldest blocks"
                        );
                    }

                    let total_blocks: u64 = plan.ranges.iter().map(|(from, to)| to - from + 1).sum();
                    let backfilled = total_blocks - 1; // the head itself is the ordinary path
                    if backfilled > 0 {
                        self.metrics.event_dispatcher_backfilled_blocks_total.increment(backfilled);
                        tracing::info!(
                            block = block_number,
                            latest_processed,
                            backfilled,
                            "event dispatcher: backfilling gap left by ws heads"
                        );
                    }

                    for (from, to) in plan.ranges {
                        let _ = tokio::join!(
                            self.fetch_erc20_transfer_logs_and_process(from, to),
                            self.fetch_and_process_weth9_logs(from, to),
                        );

                        // Advance after every chunk: a later chunk failing
                        // leaves only the residual gap for the next head.
                        self.latest_processed_block.store(to, Ordering::Relaxed);
                        self.metrics.event_dispatcher_blocks_processed_total.increment(to - from + 1);
                    }

                    // update dispatcher lag gauge
                    let latest_head = self.block_watcher.latest_block();
                    let lag = latest_head.saturating_sub(block_number);
                    self.metrics.event_dispatcher_lag_blocks.set(lag as f64);
                }
            }
        }

        self.is_active.store(false, Ordering::Relaxed);
    }

    async fn fetch_and_process_weth9_logs(&self, from: BlockNumber, to: BlockNumber) {
        tracing::debug!(from, to, "event dispatcher: fetching weth9 logs for range");

        let t0 = std::time::Instant::now();
        match self
            .rpc_client
            .fetch_weth9_logs_in_range(self.weth9_address, from, to)
            .await
        {
            Ok(logs) => {
                let elapsed_ms = t0.elapsed().as_millis() as f64;
                self.metrics.eth_get_logs_duration_ms.record(elapsed_ms);
                tracing::debug!(
                    from,
                    to,
                    count = logs.len(),
                    duration_ms = elapsed_ms as u64,
                    "event dispatcher: got weth9 logs for range",
                );

                for log in logs {
                    self.on_weth9_log(log).await;
                }
            }
            Err(err) => {
                self.metrics
                    .event_dispatcher_missed_block_logs_total
                    .increment(to - from + 1);
                tracing::warn!(
                    from,
                    to,
                    error = %err,
                    "event dispatcher: eth_getLogs for weth9 exhausted retries, range logs lost"
                );
            }
        }
    }

    async fn fetch_erc20_transfer_logs_and_process(&self, from: BlockNumber, to: BlockNumber) {
        tracing::debug!(
            from,
            to,
            "event dispatcher: fetching erc20 transfer logs for range"
        );

        let t0 = std::time::Instant::now();
        match self.rpc_client.fetch_transfer_logs_in_range(from, to).await {
            Ok(logs) => {
                let elapsed_ms = t0.elapsed().as_millis() as f64;
                self.metrics.eth_get_logs_duration_ms.record(elapsed_ms);
                tracing::debug!(
                    from,
                    to,
                    count = logs.len(),
                    duration_ms = elapsed_ms as u64,
                    "event dispatcher: got erc20 transfer logs for range"
                );
                for log in logs {
                    self.on_erc20_log(log).await;
                }
            }
            Err(err) => {
                self.metrics
                    .event_dispatcher_missed_block_logs_total
                    .increment(to - from + 1);
                tracing::warn!(
                    from,
                    to,
                    error = %err,
                    "event dispatcher: eth_getLogs for erc20 exhausted retries, range logs lost"
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
            return None;
        }

        let topics = log.topics();
        if topics.len() < 2 {
            return None;
        }

        // Defensive topic0 check — guards against unexpected 2+ topic events
        // emitted by the WETH9 contract itself (none in canonical impl, but
        // some forks have admin events). Both Deposit(indexed dst, wad) and
        // Withdrawal(indexed src, wad) carry the owner in topic1.
        let topic0 = topics[0];
        if topic0 != WrappedToken::Deposit::SIGNATURE_HASH
            && topic0 != WrappedToken::Withdrawal::SIGNATURE_HASH
        {
            return None;
        }

        Some(Address::from_word(topics[1]))
    }

    async fn on_erc20_log(&self, log: Log) {
        let topics = log.topics();
        if topics.len() != ERC20_TRANSFER_TOPICS_LEN {
            // ERC721 also emits `Transfer(from, to, tokenId)` but with
            // `tokenId` as a third indexed topic (4 topics total). Skip those
            // — `balanceOf(owner)` on an ERC721 returns a count, not a token
            // balance, and we have no schema to make sense of it here.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_head_fetches_only_that_block() {
        let plan = Erc20TransferEventDispatcher::plan_fetch(0, 100, 30, 10);
        assert_eq!(plan.skipped_blocks, 0);
        assert_eq!(plan.ranges, vec![(100, 100)]);
    }

    #[test]
    fn stale_and_duplicate_heads_are_skipped() {
        assert!(Erc20TransferEventDispatcher::plan_fetch(100, 100, 30, 10)
            .ranges
            .is_empty());
        assert!(Erc20TransferEventDispatcher::plan_fetch(100, 97, 30, 10)
            .ranges
            .is_empty());
    }

    #[test]
    fn next_head_is_a_single_block_range() {
        let plan = Erc20TransferEventDispatcher::plan_fetch(100, 101, 30, 10);
        assert_eq!(plan.skipped_blocks, 0);
        assert_eq!(plan.ranges, vec![(101, 101)]);
    }

    #[test]
    fn small_gap_backfills_from_cursor_in_one_range() {
        let plan = Erc20TransferEventDispatcher::plan_fetch(100, 105, 30, 10);
        assert_eq!(plan.skipped_blocks, 0);
        assert_eq!(plan.ranges, vec![(101, 105)]);
    }

    #[test]
    fn wide_gap_is_chunked_with_remainder() {
        let plan = Erc20TransferEventDispatcher::plan_fetch(100, 125, 30, 10);
        assert_eq!(plan.skipped_blocks, 0);
        assert_eq!(plan.ranges, vec![(101, 110), (111, 120), (121, 125)]);
    }

    #[test]
    fn gap_at_exact_chunk_boundary() {
        let plan = Erc20TransferEventDispatcher::plan_fetch(100, 120, 30, 10);
        assert_eq!(plan.skipped_blocks, 0);
        assert_eq!(plan.ranges, vec![(101, 110), (111, 120)]);
    }

    #[test]
    fn gap_equal_to_cap_backfills_everything() {
        // gap == max_backfill: nothing skipped
        let plan = Erc20TransferEventDispatcher::plan_fetch(100, 130, 30, 10);
        assert_eq!(plan.skipped_blocks, 0);
        assert_eq!(plan.ranges, vec![(101, 110), (111, 120), (121, 130)]);
    }

    #[test]
    fn gap_over_cap_skips_oldest_blocks() {
        // gap = 100, cap = 30: fetch the newest 30 blocks, skip the 70 oldest
        let plan = Erc20TransferEventDispatcher::plan_fetch(100, 200, 30, 10);
        assert_eq!(plan.skipped_blocks, 70);
        assert_eq!(plan.ranges, vec![(171, 180), (181, 190), (191, 200)]);
        let fetched: u64 = plan.ranges.iter().map(|(f, t)| t - f + 1).sum();
        assert_eq!(fetched, 30);
    }

    #[test]
    fn tight_cap_still_fetches_the_head() {
        // mainnet-style cap of 3: only the newest 3 blocks survive the cap
        let plan = Erc20TransferEventDispatcher::plan_fetch(10, 20, 3, 10);
        assert_eq!(plan.skipped_blocks, 7);
        assert_eq!(plan.ranges, vec![(18, 20)]);
    }

    #[test]
    fn chunk_is_time_normalized_per_network() {
        // ~BACKFILL_CHUNK_SECONDS of chain activity per request, floor 1
        assert_eq!(
            Erc20TransferEventDispatcher::backfill_chunk_blocks(EvmNetwork::Eth.block_time()),
            1
        );
        assert_eq!(
            Erc20TransferEventDispatcher::backfill_chunk_blocks(EvmNetwork::Arbitrum.block_time()),
            40
        );
        assert_eq!(
            Erc20TransferEventDispatcher::backfill_chunk_blocks(EvmNetwork::Bnb.block_time()),
            13
        );
        assert_eq!(
            Erc20TransferEventDispatcher::backfill_chunk_blocks(EvmNetwork::Plasma.block_time()),
            10
        );
        assert_eq!(
            Erc20TransferEventDispatcher::backfill_chunk_blocks(EvmNetwork::Linea.block_time()),
            2
        );
    }
}
