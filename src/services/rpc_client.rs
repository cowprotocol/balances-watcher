use crate::config::constants::MULTICALL_PERMITS_COUNT;
use crate::domain::EvmNetwork;
use crate::evm::erc20::ERC20;
use crate::evm::multicall3::Multicall3;
use crate::evm::multicall3::Multicall3::Multicall3Instance;
use crate::metrics::Metrics;
use crate::services::errors::ServiceError;
use alloy::eips::BlockId;
use alloy::primitives::{Address, BlockNumber, U256};
use alloy::providers::{DynProvider, Provider};
use alloy::sol_types::{SolCall, SolValue};
use backon::{ExponentialBuilder, Retryable};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type BalancesWithBlock = (HashMap<Address, U256>, U256);

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
    network: EvmNetwork,
    metrics: Arc<Metrics>,
}

impl RpcClient {
    pub fn new(provider: Arc<DynProvider>, network: EvmNetwork, metrics: Arc<Metrics>) -> Self {
        Self {
            provider,
            request_semaphore: tokio::sync::Semaphore::new(MULTICALL_PERMITS_COUNT),
            network,
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

        let multicall3 = Multicall3::new(self.network.multicall3_address(), self.provider.clone());
        let mut calls: Vec<Multicall3::Call> = Vec::new();

        for address in &erc20_tokens {
            let call = ERC20::balanceOfCall { owner };
            let calldata = call.abi_encode();
            calls.push(Multicall3::Call {
                target: *address,
                callData: calldata.into(),
            });
        }

        let t0 = Instant::now();

        let call_result = {
            let _permit = self.request_semaphore.acquire().await;
            let metrics = Arc::clone(&self.metrics);
            self.multicall_with_backoff(&multicall3, &calls, block_id)
                .await
                .inspect(move |_| {
                    self.metrics.multicall_total.increment(1);
                    metrics
                        .multicall_duration_ms
                        .record(t0.elapsed().as_millis() as f64);
                })
                .map_err(|e| {
                    self.metrics
                        .provider_exhausted_with_retries_total
                        .increment(1);
                    ServiceError::BalancesMultiCallError(e.to_string())
                })?
        };

        tracing::info!(
            time_ms = t0.elapsed().as_millis(),
            "tryBlockAndAggregate balances complete"
        );

        let mut balances: HashMap<Address, U256> = HashMap::new();
        let return_data = &call_result.returnData;

        // We call multicall with `requireSuccess=false`, so per-subcall failures
        // are expected and non-fatal: dead/migrated/proxy-broken ERC20s appear
        // routinely in large token lists. Log, count, and skip the offending
        // token; the rest of the batch still flows to the client. A full-batch
        // failure is a different path — handled above by `provider_exhausted_with_retries_total`.
        for (i, erc20_token) in erc20_tokens.iter().enumerate() {
            let Some(resp) = return_data.get(i) else {
                self.metrics.multicall_subcall_failed_total.increment(1);
                tracing::warn!(
                    token = %erc20_token,
                    index = i,
                    "multicall: missing response slot, skipping token"
                );
                continue;
            };

            if !resp.success {
                self.metrics.multicall_subcall_failed_total.increment(1);
                tracing::warn!(
                    token = %erc20_token,
                    index = i,
                    return_data_len = resp.returnData.len(),
                    "multicall3 subcall reverted, skipping token"
                );
                continue;
            }

            match <U256 as SolValue>::abi_decode(&resp.returnData) {
                Ok(balance) => {
                    balances.insert(*erc20_token, balance);
                }
                Err(e) => {
                    self.metrics.multicall_subcall_failed_total.increment(1);
                    tracing::warn!(
                        error = %e,
                        token = %erc20_token,
                        "multicall: abi_decode failed, skipping token"
                    );
                }
            }
        }

        Ok((balances, call_result.blockNumber))
    }

    async fn multicall_with_backoff(
        &self,
        multicall3: &Multicall3Instance<Arc<DynProvider>>,
        calls: &[Multicall3::Call],
        block_id: BlockId,
    ) -> Result<Multicall3::tryBlockAndAggregateReturn, RpcError> {
        let backoff = Self::backoff();
        let metrics = Arc::clone(&self.metrics);
        (|| {
            let calls = calls.to_owned();
            let mc = multicall3.clone();

            async move {
                mc.tryBlockAndAggregate(false, calls)
                    .block(block_id)
                    .call()
                    .await
            }
        })
        .retry(backoff)
        .when(Self::is_retryable)
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
            if !Self::is_retryable(&err) {
                tracing::warn!(
                    error = %err,
                    "multicall returned a permanent error, not retrying"
                );
            }
            RpcError::Exhausted(err.to_string())
        })
    }

    /// Classify a contract-call error: which ones are worth retrying.
    ///
    /// Transient (retry): transport-layer failures — backend gone, missing
    /// response, network timeouts, 5xx from the RPC. A `backon` round may
    /// recover.
    ///
    /// Permanent (give up immediately): caller-side bugs that no amount of
    /// retrying will fix — multicall3 not deployed at the configured address,
    /// ABI mismatch, unknown selector, on-chain revert with payload.
    fn is_retryable(err: &alloy::contract::Error) -> bool {
        use alloy::contract::Error;
        match err {
            Error::UnknownFunction(_)
            | Error::UnknownSelector(_)
            | Error::NotADeploymentTransaction
            | Error::ContractNotDeployed
            | Error::ZeroData(_, _)
            | Error::AbiError(_)
            | Error::PendingTransactionError(_) => false,
            Error::TransportError(transport_err) => {
                // A transport error carrying revert payload is a contract-level
                // failure (e.g. multicall3 itself reverted) — no point retrying.
                transport_err
                    .as_error_resp()
                    .and_then(|e| e.as_revert_data())
                    .is_none()
            }
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
