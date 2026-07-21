//! HTTP-side RPC client. Two roles:
//!
//! 1. **Balance reads via Multicall3** — two public entrypoints:
//!    - [`RpcClient::fetch_balances_via_multicall`] — all-or-nothing single-shot,
//!      used by [`crate::services::balance_refresh_queue`] where event-driven
//!      batches are small (debounced coalesce).
//!    - [`RpcClient::fetch_balances_by_chunks`] — chunked streaming, used by
//!      [`crate::services::snapshot_updater`]. Splits the token list into
//!      [`MULTICALL_CHUNK_SIZE`]-sized chunks and yields each chunk's result
//!      as an [`impl Stream`] as it completes — so partial diffs can flow to
//!      SSE clients without waiting for the whole snapshot.
//!
//!    Both share the same per-chunk pipeline:
//!    ```text
//!        semaphore permit
//!            → try_block_and_aggregate_with_retries (backon + is_multicall_retryable)
//!                → build_balance_of_multicall (rebuilt per attempt, MulticallBuilder is !Clone)
//!                    → alloy → eth_call → RPC provider
//!    ```
//!
//! 2. **Log fetches for the process-wide event dispatcher** —
//!    [`RpcClient::fetch_transfer_logs_in_range`] (ERC20 Transfer, global topic
//!    filter) and [`RpcClient::fetch_weth9_logs_in_range`] (WETH9 Deposit /
//!    Withdrawal, address-filtered). Called per head block (single-block range)
//!    or per backfill chunk after a gap by
//!    [`crate::services::event_dispatcher::Erc20TransferEventDispatcher`].

use crate::config::constants::MULTICALL_PERMITS_COUNT;
use crate::evm::erc20::ERC20;
use crate::evm::wrapped::WrappedToken;
use crate::metrics::Metrics;
use alloy::eips::BlockId;
use alloy::network::Ethereum;
use alloy::primitives::{Address, BlockNumber, U256};
use alloy::providers::{DynProvider, Dynamic, Failure, MulticallBuilder, MulticallError, Provider};
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::{SolCall, SolEvent};
use backon::{ExponentialBuilder, Retryable};
use futures::stream::{self, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_stream::Stream;

const MULTICALL_CHUNK_SIZE: usize = 500;

/// Upper bound on the `eth_blockNumber` round-trip in
/// [`RpcClient::latest_block_number`]. The block-watcher stall check must
/// not be held hostage by a slow node — on timeout the caller treats the
/// head as unknown and falls back to the plain reconnect path.
const HEADER_TIMEOUT: Duration = Duration::from_secs(3);

/// `(token → balance, block_number_the_batch_was_read_at)`.
pub type BalancesWithBlock = (HashMap<Address, U256>, BlockNumber);

type DynMulticallBuilder<D> = MulticallBuilder<Dynamic<D>, Arc<DynProvider>, Ethereum>;

/// Error surface for this module.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RpcError {
    /// Multicall retry path: backoff exhausted, or short-circuited on a
    /// permanent error by [`RpcClient::is_multicall_retryable`].
    #[error("Provider exhausted after retries: {0}")]
    Exhausted(String),
}

/// HTTP-side RPC client shared across the service.
pub struct RpcClient {
    provider: Arc<DynProvider>,
    request_semaphore: tokio::sync::Semaphore,
    metrics: Arc<Metrics>,
}

impl RpcClient {
    /// Construct a client around an already-connected HTTP provider.
    pub fn new(provider: Arc<DynProvider>, metrics: Arc<Metrics>) -> Self {
        Self {
            provider,
            request_semaphore: tokio::sync::Semaphore::new(MULTICALL_PERMITS_COUNT),
            metrics,
        }
    }

    /// Current chain head via HTTP `eth_blockNumber`. Single attempt, no
    /// retry/backoff, bounded by [`HEADER_TIMEOUT`] — the only caller is
    /// the block-watcher stall check, which needs a fast verdict and treats
    /// any error as "unconfirmed" (falls back to the plain reconnect path).
    pub async fn latest_block_number(&self) -> Result<BlockNumber, RpcError> {
        match tokio::time::timeout(HEADER_TIMEOUT, self.provider.get_block_number()).await {
            Ok(result) => result.map_err(|err| RpcError::Exhausted(err.to_string())),
            Err(_) => Err(RpcError::Exhausted(format!(
                "eth_blockNumber timed out after {}s",
                HEADER_TIMEOUT.as_secs()
            ))),
        }
    }

    /// Fetch every ERC20 `Transfer` log emitted in blocks `from..=to`. Uses
    /// HTTP `eth_getLogs` (server-side topic filter); returns 100% of matching
    /// logs in the range, in contrast to WS `eth_subscribe` whose internal
    /// buffer drops the tail of burst blocks.
    ///
    /// The happy path is a single-block range (`from == to`, one call per
    /// head); wider ranges are used by the dispatcher's gap backfill and must
    /// stay small enough for the provider's per-response log limit — the
    /// caller owns chunking.
    ///
    /// Transient RPC failures are retried per [`Self::backoff`]; the final
    /// `Err` surfaces as [`RpcError::Exhausted`].
    pub async fn fetch_transfer_logs_in_range(
        &self,
        from: BlockNumber,
        to: BlockNumber,
    ) -> Result<Vec<Log>, RpcError> {
        let filter = Filter::new()
            .from_block(from)
            .to_block(to)
            .event_signature(ERC20::Transfer::SIGNATURE_HASH);

        self.get_logs_with_retries(filter).await
    }

    /// Fetch WETH9 `Deposit` and `Withdrawal` logs emitted in blocks
    /// `from..=to`, address-filtered to the canonical WETH9 contract for this
    /// chain. Needed alongside [`Self::fetch_transfer_logs_in_range`] because
    /// the canonical WETH9 impl does not emit `Transfer` on wrap / unwrap.
    ///
    /// Same retry policy and chunking contract as
    /// [`Self::fetch_transfer_logs_in_range`].
    pub async fn fetch_weth9_logs_in_range(
        &self,
        weth9_address: Address,
        from: BlockNumber,
        to: BlockNumber,
    ) -> Result<Vec<Log>, RpcError> {
        let event_signatures = vec![
            WrappedToken::Deposit::SIGNATURE_HASH,
            WrappedToken::Withdrawal::SIGNATURE_HASH,
        ];

        let filter = Filter::new()
            .from_block(from)
            .to_block(to)
            .address(weth9_address)
            .event_signature(event_signatures);

        self.get_logs_with_retries(filter).await
    }

    /// Run `eth_getLogs` with retry/backoff. Retries every error unconditionally
    /// — we construct all filters ourselves, so "permanent" errors (bad filter,
    /// unknown topic) are not expected here. Real-world failures are transport
    /// hiccups (5xx, connection reset, timeout) and "block not found" while
    /// the node is briefly behind the head; both resolve within one backoff
    /// window.
    async fn get_logs_with_retries(&self, filter: Filter) -> Result<Vec<Log>, RpcError> {
        let metrics = Arc::clone(&self.metrics);
        (|| {
            let filter = filter.clone();
            async move { self.provider.get_logs(&filter).await }
        })
        .retry(Self::backoff())
        .notify(move |err, duration| {
            metrics.eth_get_logs_failed_total.increment(1);
            tracing::warn!(
                error = %err,
                duration = ?duration,
                "eth_getLogs attempt failed, will retry"
            );
        })
        .await
        .map_err(|err| RpcError::Exhausted(err.to_string()))
    }

    /// Read ERC20 balances for `owner` at `block_id`, one Multicall3
    /// round-trip per call.
    ///
    /// Sent with `requireSuccess=false`: a single failing `balanceOf` does
    /// not poison the snapshot. This matters in practice — upstream token
    /// lists regularly include dead / migrated / proxy-broken ERC20s whose
    /// `balanceOf` reverts. Those tokens are warned + counted and dropped from
    /// the result map; healthy tokens still flow to the client.
    ///
    /// `Err` is returned only when the multicall itself never completed
    /// (transient errors exhausted retries, or a permanent error such as
    /// ABI mismatch / wrong multicall3 address).
    pub async fn fetch_balances_via_multicall(
        &self,
        owner: Address,
        tokens: &[Address],
        block_id: BlockId,
    ) -> Result<BalancesWithBlock, RpcError> {
        let mut erc20_tokens: Vec<Address> = tokens.to_vec();
        erc20_tokens.sort();

        let t0 = Instant::now();

        let (block_number, subcalls_result) = {
            let _permit = self.request_semaphore.acquire().await;
            let metrics = Arc::clone(&self.metrics);
            let erc20_tokens = erc20_tokens.clone();

            self.try_block_and_aggregate_with_retries(move || {
                Self::build_balance_of_multicall(&self.provider, &erc20_tokens, block_id, owner)
            })
            .await
            .inspect(move |_| {
                self.metrics.multicall_total.increment(1);
                metrics
                    .multicall_duration_ms
                    .record(t0.elapsed().as_millis() as f64);
            })
            .inspect_err(|_| {
                self.metrics
                    .provider_exhausted_with_retries_total
                    .increment(1);
            })?
        };

        tracing::debug!(
            time_ms = t0.elapsed().as_millis(),
            "tryBlockAndAggregate balances complete"
        );

        let mut balances: HashMap<Address, U256> = HashMap::new();

        // We call multicall with `requireSuccess=false`, so per-subcall failures
        // are expected and non-fatal: dead/migrated/proxy-broken ERC20s appear
        // routinely in large token lists. Log, count, and skip the offending
        // token; the rest of the batch still flows to the client. A full-batch
        // failure is a different path — handled above by `provider_exhausted_with_retries_total`.
        for (i, erc20_token) in erc20_tokens.iter().enumerate() {
            let Some(sub_call_result) = subcalls_result.get(i) else {
                self.metrics.multicall_subcall_failed_total.increment(1);
                tracing::warn!(
                    token = %erc20_token,
                    index = i,
                    "multicall response is not matched to current token list size"
                );
                continue;
            };

            match sub_call_result {
                Ok(balance) => {
                    balances.insert(*erc20_token, *balance);
                }
                Err(failure) => {
                    self.metrics.multicall_subcall_failed_total.increment(1);
                    // `Failure` covers both subcall revert and abi-decode mismatch;
                    // distinguish heuristically via return_data length when triaging.
                    tracing::warn!(
                        token = %erc20_token,
                        return_data_len = failure.return_data.len(),
                        "multicall subcall failed, skipping token"
                    );
                }
            }
        }

        Ok((balances, block_number))
    }

    /// Streaming variant of [`Self::fetch_balances_via_multicall`]: splits
    /// `tokens` into [`MULTICALL_CHUNK_SIZE`]-sized chunks, fires one multicall
    /// per chunk in parallel via [`FuturesUnordered`], and yields each chunk's
    /// result as it lands.
    ///
    /// Enables partial-first delivery to SSE clients: the snapshot updater
    /// broadcasts a diff after every chunk instead of waiting for the whole
    /// token list to resolve. On a 1000-token list this drops
    /// `session → first_snapshot` roughly by a factor of `ceil(N / chunk_size)`.
    ///
    /// **Ordering.** Chunks are `FuturesUnordered` — items arrive out of
    /// submission order. Callers must not assume any particular chunk lands
    /// first.
    ///
    /// **Block number per item.** Each chunk carries its own `block_number`
    /// from `tryBlockAndAggregate`. Under normal head advancement two chunks
    /// in the same call may land on adjacent blocks — the per-token diff
    /// path in `Subscription::update_balances_and_take_diff` is block-guarded
    /// (`current.block_number < new.block_number`), so this is safe.
    ///
    /// **Failure isolation.** An `Err` item means that specific chunk's
    /// multicall exhausted retries or hit a permanent error — sibling chunks
    /// are unaffected. Callers decide whether to broadcast the failure or
    /// silently skip. Counter: `provider_exhausted_with_retries_total`
    /// (bumped inside the chunk future on final failure).
    ///
    /// **Concurrency cap.** Each chunk future acquires one permit from
    /// `request_semaphore` (capacity [`MULTICALL_PERMITS_COUNT`]) before
    /// issuing its multicall.
    pub fn fetch_balances_by_chunks(
        self: Arc<Self>,
        owner: Address,
        tokens: &[Address],
        block_id: BlockId,
    ) -> impl Stream<Item = Result<BalancesWithBlock, RpcError>> {
        let chunks = {
            let mut sorted = tokens.to_vec();
            sorted.sort();
            sorted
                .chunks(MULTICALL_CHUNK_SIZE)
                .map(|chunk| chunk.to_vec())
                .collect::<Vec<_>>()
        };

        stream::iter(chunks).then(move |chunk| {
            let this = Arc::clone(&self);

            async move {
                let _permit = this.request_semaphore.acquire().await;
                let t0 = Instant::now();
                let metrics = Arc::clone(&this.metrics);

                let (block_number, call_result) = this
                    .try_block_and_aggregate_with_retries(|| {
                        Self::build_balance_of_multicall(&this.provider, &chunk, block_id, owner)
                    })
                    .await
                    .inspect(move |_| {
                        let metrics = Arc::clone(&metrics);
                        metrics.multicall_total.increment(1);
                        metrics
                            .multicall_duration_ms
                            .record(t0.elapsed().as_millis() as f64);
                    })
                    .inspect_err(|_| {
                        this.metrics
                            .provider_exhausted_with_retries_total
                            .increment(1);
                    })?;

                tracing::debug!(
                    time_ms = t0.elapsed().as_millis(),
                    owner = %owner,
                    chunk_size = chunk.len(),
                    block = block_number,
                    "tryBlockAndAggregate chunk complete"
                );

                let balances_map = this.map_balance_result(&chunk, call_result);
                Ok((balances_map, block_number))
            }
        })
    }

    fn map_balance_result(
        &self,
        erc20_tokens_chunk: &[Address],
        subcalls_result: Vec<Result<U256, Failure>>,
    ) -> HashMap<Address, U256> {
        let mut balances: HashMap<Address, U256> = HashMap::new();

        // We call multicall with `requireSuccess=false`, so per-subcall failures
        // are expected and non-fatal: dead/migrated/proxy-broken ERC20s appear
        // routinely in large token lists. Log, count, and skip the offending
        // token; the rest of the batch still flows to the client. A full-batch
        // failure is a different path — handled above by `provider_exhausted_with_retries_total`.
        for (i, erc20_token) in erc20_tokens_chunk.iter().enumerate() {
            let Some(sub_call_result) = subcalls_result.get(i) else {
                self.metrics.multicall_subcall_failed_total.increment(1);
                tracing::warn!(
                    token = %erc20_token,
                    index = i,
                    "multicall response is not matched to current token list size"
                );
                continue;
            };

            match sub_call_result {
                Ok(balance) => {
                    balances.insert(*erc20_token, *balance);
                }
                Err(failure) => {
                    self.metrics.multicall_subcall_failed_total.increment(1);
                    // `Failure` covers both subcall revert and abi-decode mismatch;
                    // distinguish heuristically via return_data length when triaging.
                    tracing::warn!(
                        token = %erc20_token,
                        return_data_len = failure.return_data.len(),
                        "multicall subcall failed, skipping token"
                    );
                }
            }
        }

        balances
    }

    /// Build a `MulticallBuilder` of `balanceOf(owner)` for every `token`,
    /// pinned to `block_id`.
    fn build_balance_of_multicall(
        provider: &Arc<DynProvider>,
        tokens: &[Address],
        block_id: BlockId,
        owner: Address,
    ) -> DynMulticallBuilder<ERC20::balanceOfCall> {
        let multicall = tokens.iter().fold(
            MulticallBuilder::new(Arc::clone(provider)).dynamic::<ERC20::balanceOfCall>(),
            |builder, token| builder.add_dynamic(ERC20::new(*token, provider).balanceOf(owner)),
        );

        multicall.block(block_id)
    }

    /// Run `tryBlockAndAggregate(false, …)` with retries from [`Self::backoff`].
    ///
    /// Transient errors are retried; permanent errors
    /// ([`Self::is_multicall_retryable`] returns `false`) short-circuit.
    /// Both flavours of final failure surface as [`RpcError::Exhausted`].
    ///
    /// Returns `(block_number, per-subcall results)`. `Err(Failure)` in the
    /// inner `Vec` means the individual subcall reverted or its return data
    /// could not be decoded.
    async fn try_block_and_aggregate_with_retries<D, F>(
        &self,
        build_multicall: F,
    ) -> Result<(BlockNumber, Vec<Result<D::Return, Failure>>), RpcError>
    where
        D: SolCall + Send + Sync + Unpin + 'static,
        F: Fn() -> DynMulticallBuilder<D> + Clone + Send + Sync,
    {
        let backoff = Self::backoff();
        let metrics = Arc::clone(&self.metrics);
        (|| {
            let build_multicall = build_multicall.clone();
            async move {
                build_multicall()
                    .try_block_and_aggregate(false)
                    .await
                    .map(|(block_number, _block_hash, results)| (block_number, results))
            }
        })
        .retry(backoff)
        .when(Self::is_multicall_retryable)
        .notify(move |err, duration| {
            tracing::warn!(
                error = %err,
                duration = ?duration,
                "multicall attempt failed, will retry"
            );
            metrics.multicall_failed_total.increment(1);
        })
        .await
        .map_err(|err| {
            if !Self::is_multicall_retryable(&err) {
                tracing::warn!(
                    error = %err,
                    "multicall returned a permanent error, not retrying"
                );
            }
            RpcError::Exhausted(err.to_string())
        })
    }

    /// Classify a `MulticallError` from `alloy::providers::MulticallBuilder`:
    /// which ones are worth retrying.
    ///
    /// Transient (retry): transport-layer failures — backend gone, network
    /// timeouts, 5xx from the RPC. A `backon` round may recover.
    ///
    /// Permanent (give up immediately):
    /// - `TransportError` that carries on-chain revert payload — multicall3
    ///   itself reverted (wrong address / out-of-gas on the whole batch),
    ///   retrying with the same input will fail the same way.
    /// - `DecodeError` / `NoReturnData` / `CallFailed` / `ValueTx` — encoder
    ///   bug or ABI mismatch on our side; no retry will fix it.
    fn is_multicall_retryable(err: &MulticallError) -> bool {
        use MulticallError::*;
        match err {
            TransportError(transport_err) => transport_err
                .as_error_resp()
                .and_then(|e| e.as_revert_data())
                .is_none(),
            DecodeError(_) | NoReturnData | CallFailed(_) | ValueTx => false,
        }
    }

    fn backoff() -> ExponentialBuilder {
        ExponentialBuilder::new()
            .with_min_delay(Duration::from_secs(2))
            .with_max_delay(Duration::from_secs(10))
            .with_max_times(3)
            .with_jitter()
    }
}
