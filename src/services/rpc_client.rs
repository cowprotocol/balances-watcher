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
        let native_address = self.network.native_token_address();
        let mut erc20_tokens: Vec<Address> = tokens
            .iter()
            .cloned()
            .filter(|a| *a != native_address)
            .collect();
        erc20_tokens.sort();

        let multicall3 = Multicall3::new(self.network.multicall3_address(), self.provider.clone());
        // one for erc balances
        let mut calls: Vec<Multicall3::Call> = Vec::with_capacity(erc20_tokens.len() + 1);

        for address in &erc20_tokens {
            let call = ERC20::balanceOfCall { owner };
            let calldata = call.abi_encode();
            calls.push(Multicall3::Call {
                target: *address,
                callData: calldata.into(),
            });
        }

        let eth_balance_call = Multicall3::getEthBalanceCall { addr: owner };
        let eth_balance_call_data = eth_balance_call.abi_encode();
        calls.push(Multicall3::Call {
            target: self.network.multicall3_address(),
            callData: eth_balance_call_data.into(),
        });

        let t0 = Instant::now();
        self.metrics.multicall_total.increment(1);

        let call_result = {
            let _permit = self.request_semaphore.acquire().await;
            let metrics = Arc::clone(&self.metrics);
            self.multicall_with_backoff(&multicall3, &calls, block_id)
                .await
                .inspect(move |_| {
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

        let mut balances: HashMap<Address, U256> = HashMap::with_capacity(erc20_tokens.len() + 1);
        let return_data = &call_result.returnData;

        for (i, erc20_token) in erc20_tokens.iter().enumerate() {
            let resp = return_data.get(i).ok_or_else(|| {
                ServiceError::BalancesMultiCallError(format!(
                    "multicall: missing response at index={i} for token={erc20_token}"
                ))
            })?;

            if !resp.success {
                tracing::error!(
                    token = %erc20_token,
                    index = i,
                    return_data_len = resp.returnData.len(),
                    "multicall3 subcall failed (success=false)"
                );

                return Err(ServiceError::BalancesMultiCallError(format!(
                    "multicall3 subcall failed: token={erc20_token}, index={i}, return_data_len={}",
                    resp.returnData.len()
                )));
            }

            match <U256 as SolValue>::abi_decode(&resp.returnData) {
                Ok(balance) => {
                    balances.insert(*erc20_token, balance);
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        token = %erc20_token,
                        "abi_decode failed"
                    );
                }
            }
        }

        let eth_balance_resp = return_data.get(erc20_tokens.len()).ok_or_else(|| {
            ServiceError::BalancesMultiCallError(
                "multicall3: missing response for token=ETH".into(),
            )
        })?;

        match <U256 as SolValue>::abi_decode(&eth_balance_resp.returnData) {
            Ok(balance) => {
                balances.insert(native_address, balance);
            }
            Err(e) => {
                tracing::error!(error = %e, "abi_decode failed for getEthBalance");
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
        .notify(move |err, duration| {
            tracing::error!(
                error = %err,
                duration = ?duration,
                "failed to execute multicall"
            );
            metrics.multicall_failed_total.increment(1);
        })
        .await
        .map_err(|err| RpcError::Exhausted(err.to_string()))
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
