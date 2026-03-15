use crate::domain::Session;
use crate::evm::multicall3::Multicall3::Multicall3Instance;
use crate::evm::{erc20::ERC20, multicall3::Multicall3};
use crate::services::errors::ServiceError;
use alloy::eips::BlockId;
use alloy::primitives::{Address, U256};
use alloy::providers::DynProvider;
use alloy::sol_types::{SolCall, SolValue};
use backon::{ExponentialBuilder, Retryable};
use metrics::{counter, histogram};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, thiserror::Error)]
pub enum MulticallError {
    #[error("Provider exhausted with retries")]
    ProviderExhausted,
}

pub struct BalanceCallCtx {
    pub session: Session,
    pub provider: Arc<DynProvider>,
}

pub type BalancesWithBlock = (HashMap<Address, U256>, U256);

pub async fn fetch_balances_via_multicall(
    ctx: Arc<BalanceCallCtx>,
    tokens: &[Address],
    block_id: BlockId,
) -> Result<BalancesWithBlock, ServiceError> {
    let native_address = ctx.session.network.native_token_address();
    let mut erc20_tokens: Vec<Address> = tokens
        .iter()
        .cloned()
        .filter(|a| *a != native_address)
        .collect();
    erc20_tokens.sort();

    let multicall3 = Multicall3::new(
        ctx.session.network.multicall3_address(),
        ctx.provider.clone(),
    );
    // one for erc balances
    let mut calls: Vec<Multicall3::Call> = Vec::with_capacity(erc20_tokens.len() + 1);
    let owner = ctx.session.owner;

    for address in &erc20_tokens {
        let call = ERC20::balanceOfCall { owner };
        let calldata = call.abi_encode();
        calls.push(Multicall3::Call {
            target: *address,
            callData: calldata.into(),
        });
    }

    let eth_balance_call = Multicall3::getEthBalanceCall {
        addr: ctx.session.owner,
    };
    let eth_balance_call_data = eth_balance_call.abi_encode();
    calls.push(Multicall3::Call {
        target: ctx.session.network.multicall3_address(),
        callData: eth_balance_call_data.into(),
    });

    let t0 = Instant::now();
    counter!("multicall_total").increment(1);

    let call_result = multicall_with_backoff(&multicall3, &calls, block_id)
        .await
        .inspect(move |_| {
            histogram!("multicall_duration_ms").record(t0.elapsed().as_millis() as f64);
        })
        .map_err(|e| {
            counter!("provider_exhausted_with_retires_total", "network" => ctx.session.network.to_string()).increment(1);
            histogram!("multicall_duration_ms").record(t0.elapsed().as_millis() as f64);
            ServiceError::BalancesMultiCallError(e.to_string())
        })?;

    tracing::info!(
        time_ms = t0.elapsed().as_millis(),
        "tryBlockAndAggregate balances complete"
    );

    let mut balances: HashMap<Address, U256> = HashMap::with_capacity(erc20_tokens.len() + 1);
    let return_data = &call_result.returnData;

    for (i, erc20_token) in erc20_tokens.iter().enumerate() {
        let resp = return_data.get(i).ok_or_else(|| {
            ServiceError::BalancesMultiCallError(
                "multicall: missing response at index={i} for token={token}".to_string(),
            )
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
            "multicall3: missing response at index={i} for token={token}".to_string(),
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
    multicall3: &Multicall3Instance<Arc<DynProvider>>,
    calls: &[Multicall3::Call],
    block_id: BlockId,
) -> Result<Multicall3::tryBlockAndAggregateReturn, MulticallError> {
    let backoff = backoff();
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
    .notify(|err, duration| {
        tracing::error!(
            error = %err,
            duration = ?duration,
            "failed to execute multicall"
        );
        counter!("multicall_failed_total").increment(1);
    })
    .await
    .map_err(|_| MulticallError::ProviderExhausted)
}

fn backoff() -> ExponentialBuilder {
    ExponentialBuilder::new()
        .with_min_delay(Duration::from_secs(2))
        .with_max_delay(Duration::from_secs(10))
        .with_max_times(3)
        .with_jitter()
}
