//! Debounced, coalescing fetcher for ERC20 balances of a single owner.
//!
//! # Why
//!
//! Each on-chain event (`Transfer`, `Deposit`, `Withdrawal`) signals that one
//! token's balance for our owner is now stale. Naïvely calling `balanceOf` per
//! event would mean N RPC round-trips per block. Instead, events flow into
//! this queue: it waits a short debounce window ([`CALL_QUEUE_DELAY`]),
//! coalesces duplicates by token address (keeping the highest observed block),
//! and emits a single Multicall3 batch covering everything seen during the
//! window.
//!
//! # Topology
//!
//! ```text
//!     producer ─┐                                 multicall result
//!     producer ─┼──► tx_in ──► [worker task] ──► tx_out ──► consumer
//!     producer ─┘    (mpsc)    chunks_timeout    (mpsc)
//!                              + coalesce
//!                              + Multicall3
//! ```
//!
//! [`BalanceRefreshQueue::spawn`] spawns the worker into the provided
//! [`TaskTracker`] and returns:
//!  * a [`BalanceRefreshQueueHandle`] producers use to submit tokens via
//!    [`BalanceRefreshQueueHandle::enqueue`]
//!  * a `mpsc::Receiver<Result<BalancesWithBlock, RpcError>>` the consumer
//!    polls for batched multicall results
//!
//! # Backpressure & lifecycle
//!
//! Both internal mpsc channels are bounded (capacity 64). If the result
//! consumer falls behind, producers eventually block on `enqueue().await`. The
//! worker exits when every [`BalanceRefreshQueueHandle`] clone is dropped (the
//! inbound channel closes) or when the result consumer is dropped (the
//! outbound channel closes).

use crate::config::constants::{CALL_QUEUE_DELAY, MAX_QUEUE_SIZE};
use crate::services::rpc_client::{BalancesWithBlock, RpcClient, RpcError};
use alloy::eips::BlockId;
use alloy::primitives::{Address, BlockNumber};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::task::TaskTracker;

/// Token → highest block number we've seen for it inside the current debounce
/// window. `None` means "the caller didn't pin a block, query at `latest`."
type LatestBlockByToken = HashMap<Address, Option<BlockNumber>>;

/// One pending refresh request flowing from a producer to the worker.
///
/// `block` carries the block number observed on the triggering event when
/// available; `None` falls back to `BlockId::latest()` at fetch time.
type FetchRequest = (Address, Option<BlockNumber>);

/// Producer handle returned by [`BalanceRefreshQueue::spawn`].
///
/// Every event handler that needs a balance refresh holds a clone and calls
/// [`Self::enqueue`]. When the last clone is dropped the worker task exits.
#[derive(Clone)]
pub struct BalanceRefreshQueueHandle {
    tx_in: mpsc::Sender<FetchRequest>,
}

impl BalanceRefreshQueueHandle {
    /// Submit a token for refresh.
    ///
    /// * `token` — the ERC20 address whose balance is now stale.
    /// * `block` — the block at which the triggering event was observed. Pass
    ///   `None` to let the worker query `latest` instead.
    ///
    /// Awaits until the request is accepted into the bounded inbound channel
    /// (this is the project's only backpressure point on producers).
    pub async fn enqueue(&self, token: Address, block: Option<BlockNumber>) {
        if let Err(err) = self.tx_in.send((token, block)).await {
            tracing::warn!(
                token = %token,
                block = ?block,
                error = %err,
                "balance_refresh_queue: failed to enqueue, worker is gone",
            );
        }
    }
}

/// Builder for the balance refresh worker.
///
/// Construct with [`Self::new`] and immediately call [`Self::spawn`] to start
/// the worker and obtain a producer handle plus result receiver.
pub struct BalanceRefreshQueue {
    task_tracker: TaskTracker,
    owner: Address,
    rpc_client: Arc<RpcClient>,
}

impl BalanceRefreshQueue {
    /// Create a builder bound to a single `owner` address.
    ///
    /// `task_tracker` is the lifecycle owner of the spawned worker; it must
    /// outlive the queue. `rpc_client` is the RPC dependency the worker uses
    /// to issue Multicall3 batches.
    pub fn new(task_tracker: TaskTracker, owner: Address, rpc_client: Arc<RpcClient>) -> Self {
        Self {
            task_tracker,
            owner,
            rpc_client,
        }
    }

    /// Spawn the worker task and return the producer/consumer ends.
    ///
    /// The returned [`BalanceRefreshQueueHandle`] is meant to be shared with
    /// every producer that needs to enqueue a refresh. The receiver should be
    /// polled by exactly one consumer task.
    ///
    /// The worker task uses `chunks_timeout` to debounce inbound requests by
    /// [`CALL_QUEUE_DELAY`] (capped at [`MAX_QUEUE_SIZE`] events per chunk),
    /// coalesces duplicate tokens via [`Self::coalesce_by_token`], and fires
    /// one Multicall3 per chunk through [`Self::process_batch`].
    pub fn spawn(
        self,
    ) -> (
        BalanceRefreshQueueHandle,
        mpsc::Receiver<Result<BalancesWithBlock, RpcError>>,
    ) {
        let (tx_in, rx_in) = mpsc::channel::<FetchRequest>(64);
        let (tx_out, rx_out) = mpsc::channel::<Result<BalancesWithBlock, RpcError>>(64);

        self.task_tracker.clone().spawn(async move {
            let stream =
                ReceiverStream::new(rx_in).chunks_timeout(MAX_QUEUE_SIZE, CALL_QUEUE_DELAY);
            tokio::pin!(stream);

            while let Some(batch) = stream.next().await {
                let coalesced = Self::coalesce_by_token(batch);
                let result = self.process_batch(coalesced).await;
                if tx_out.send(result).await.is_err() {
                    tracing::info!(
                        owner = %self.owner,
                        "balance_refresh_queue: result consumer dropped, worker exiting",
                    );
                    break;
                }
            }
        });

        (BalanceRefreshQueueHandle { tx_in }, rx_out)
    }

    /// Collapse duplicates inside a debounce window: for each token, keep the
    /// **highest** block we've seen (or `None` if any request was unpinned).
    ///
    /// `Some(n)` then `None` collapses to `None` (we'll query at `latest`,
    /// which is at least as recent as `n`).
    fn coalesce_by_token(batch: Vec<FetchRequest>) -> LatestBlockByToken {
        batch.into_iter().fold(
            LatestBlockByToken::new(),
            |mut acc, (token, current_block)| {
                acc.entry(token)
                    .and_modify(|saved_block| {
                        *saved_block = match (*saved_block, current_block) {
                            (None, _) | (_, None) => None,
                            (Some(saved), Some(current)) => Some(saved.max(current)),
                        };
                    })
                    .or_insert(current_block);
                acc
            },
        )
    }

    /// Fire one Multicall3 covering every token in `batch` and return the
    /// result. The caller is responsible for logging / reacting to errors.
    async fn process_batch(
        &self,
        batch: LatestBlockByToken,
    ) -> Result<BalancesWithBlock, RpcError> {
        let tokens: Vec<Address> = batch.keys().copied().collect();
        let block_id = Self::resolve_block_id(&batch);

        tracing::debug!(
            owner = %self.owner,
            tokens = ?tokens,
            block = ?block_id,
            "balance_refresh_queue: firing multicall",
        );

        self.rpc_client
            .fetch_balances_via_multicall(self.owner, &tokens, block_id)
            .await
    }

    /// Pick the block at which the next multicall should be issued.
    ///
    /// Returns `BlockId::latest()` if any entry in the batch is unpinned
    /// (`None`) or the resolved max is zero; otherwise the highest block in
    /// the batch.
    fn resolve_block_id(batch: &LatestBlockByToken) -> BlockId {
        let max_block = batch.values().try_fold(0u64, |max_so_far, block_opt| {
            block_opt.map(|block| max_so_far.max(block))
        });

        match max_block {
            Some(max_block) if max_block > 0 => BlockId::from(max_block),
            _ => BlockId::latest(),
        }
    }
}
