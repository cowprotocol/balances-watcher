use crate::config::constants::MULTICALL_PERMITS_COUNT;
use crate::evm::erc20::ERC20;
use crate::metrics::Metrics;
use crate::services::errors::ServiceError;
use alloy::eips::BlockId;
use alloy::network::Ethereum;
use alloy::primitives::{Address, BlockNumber, U256};
use alloy::providers::{DynProvider, Dynamic, Failure, MulticallBuilder, MulticallError, Provider};
use alloy::sol_types::SolCall;
use backon::{ExponentialBuilder, Retryable};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type BalancesWithBlock = (HashMap<Address, U256>, BlockNumber);

type DynMulticallBuilder<D> = MulticallBuilder<Dynamic<D>, Arc<DynProvider>, Ethereum>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum RpcError {
    #[error("RPC call failed: {0}")]
    Call(String),
    #[error("Provider exhausted after retries: {0}")]
    Exhausted(String),
}

pub struct RpcClient {
    provider: Arc<DynProvider>,
    request_semaphore: tokio::sync::Semaphore,
    metrics: Arc<Metrics>,
}

impl RpcClient {
    pub fn new(provider: Arc<DynProvider>, metrics: Arc<Metrics>) -> Self {
        Self {
            provider,
            request_semaphore: tokio::sync::Semaphore::new(MULTICALL_PERMITS_COUNT),
            metrics,
        }
    }

    pub async fn fetch_balances_via_multicall(
        &self,
        owner: Address,
        tokens: &[Address],
        block_id: BlockId,
    ) -> Result<BalancesWithBlock, ServiceError> {
        let mut erc20_tokens: Vec<Address> = tokens.to_vec();
        erc20_tokens.sort();

        let t0 = Instant::now();

        let (block_number, subcalls_result) = {
            let _permit = self.request_semaphore.acquire().await;
            let metrics = Arc::clone(&self.metrics);
            let erc20_tokens = erc20_tokens.clone();

            self.try_block_and_aggregate_with_retries(move || {
                let provider = Arc::clone(&self.provider);
                Self::build_balance_of_multicall(provider, &erc20_tokens, block_id, owner)
            })
            .await
            .inspect(move |_| {
                self.metrics.multicall_total.increment(1);
                metrics
                    .multicall_duration_ms
                    .record(t0.elapsed().as_millis() as f64);
            })
            .map_err(|err| {
                self.metrics
                    .provider_exhausted_with_retries_total
                    .increment(1);
                ServiceError::MultiCallError(err.to_string())
            })?
        };

        tracing::info!(
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
                Err(err) => {
                    self.metrics.multicall_subcall_failed_total.increment(1);
                    tracing::warn!(
                        error = %err,
                        token = %erc20_token,
                        "multicall: abi_decode failed, skipping token"
                    );
                }
            }
        }

        Ok((balances, block_number))
    }

    fn build_balance_of_multicall(
        provider: Arc<DynProvider>,
        tokens: &[Address],
        block_id: BlockId,
        owner: Address,
    ) -> DynMulticallBuilder<ERC20::balanceOfCall> {
        let multicall = tokens.iter().fold(
            MulticallBuilder::new(provider.clone()).dynamic::<ERC20::balanceOfCall>(),
            |builder, token| builder.add_dynamic(ERC20::new(*token, &provider).balanceOf(owner)),
        );

        multicall.block(block_id)
    }

    async fn try_block_and_aggregate_with_retries<D, F>(
        &self,
        build_multicall: F,
    ) -> Result<(u64, Vec<Result<D::Return, Failure>>), RpcError>
    where
        D: SolCall + Send + Sync + Unpin + 'static,
        F: Fn() -> DynMulticallBuilder<D> + Clone + Send + Sync,
    {
        let backoff = Self::backoff();
        let metrics = Arc::clone(&self.metrics);
        (|| async {
            let build_multicall = build_multicall.clone();
            build_multicall()
                .try_block_and_aggregate(false)
                .await
                .map(|(block_number, _block_hash, results)| (block_number, results))
        })
        .retry(backoff)
        .when(Self::is_multicall_retryable)
        .notify(move |err, duration| {
            tracing::error!(
                error = %err,
                duration = ?duration,
                "failed to execute multicall"
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

    pub async fn get_block_number(&self) -> Result<BlockNumber, RpcError> {
        self.provider
            .get_block_number()
            .await
            .map_err(|err| RpcError::Call(err.to_string()))
    }
}
