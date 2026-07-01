//! HTTP-side RPC client. Two roles:
//!
//! 1. **Balance reads via Multicall3** — two public entrypoints:
//!    - [`RpcClient::fetch_balances_via_multicall`] — all-or-nothing single-shot,
//!      used by [`crate::services::balance_refresh_queue`] where event-driven
//!      batches are small (debounced coalesce).
//!    - [`RpcClient::fetch_balances`] — chunked streaming, used by
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
//!    [`RpcClient::fetch_transfer_logs_for_block`] (ERC20 Transfer, global topic
//!    filter) and [`RpcClient::fetch_weth9_logs_for_block`] (WETH9 Deposit /
//!    Withdrawal, address-filtered). Called once per block by
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
use futures::stream::FuturesUnordered;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_stream::Stream;

const MULTICALL_CHUNK_SIZE: usize = 500;

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

    /// Fetch every ERC20 `Transfer` log emitted in `block_number`. Uses HTTP
    /// `eth_getLogs` (server-side topic filter); returns 100% of matching
    /// logs in the block, in contrast to WS `eth_subscribe` whose internal
    /// buffer drops the tail of burst blocks.
    pub async fn fetch_transfer_logs_for_block(
        &self,
        block_number: BlockNumber,
    ) -> Result<Vec<Log>, RpcError> {
        let filter = Filter::new()
            .from_block(block_number)
            .to_block(block_number)
            .event_signature(ERC20::Transfer::SIGNATURE_HASH);

        // todo implement backoff
        self.provider
            .get_logs(&filter)
            .await
            .map_err(|err| RpcError::Exhausted(err.to_string()))
    }

    pub async fn fetch_weth9_logs_for_block(
        &self,
        weth9_address: Address,
        block_number: BlockNumber,
    ) -> Result<Vec<Log>, RpcError> {
        let event_signatures = vec![
            WrappedToken::Deposit::SIGNATURE_HASH,
            WrappedToken::Withdrawal::SIGNATURE_HASH,
        ];

        let filter = Filter::new()
            .from_block(block_number)
            .to_block(block_number)
            .address(weth9_address)
            .event_signature(event_signatures);

        // todo implement backoff
        self.provider
            .get_logs(&filter)
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

    pub fn fetch_balances(
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

        chunks
            .into_iter()
            .map(move |chunk| {
                let this = Arc::clone(&self);

                async move {
                    let _permit = this.request_semaphore.acquire().await;
                    let t0 = Instant::now();
                    let metrics = Arc::clone(&this.metrics);

                    let (block_number, call_result) = this
                        .try_block_and_aggregate_with_retries(|| {
                            Self::build_balance_of_multicall(
                                &this.provider,
                                &chunk,
                                block_id,
                                owner,
                            )
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
            .collect::<FuturesUnordered<_>>()
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
