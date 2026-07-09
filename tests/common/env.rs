//! The [`Env`] handle stitches anvil + service + token-list server together.
//! Each test spawns its own [`Env`] to keep on-chain state isolated.

use alloy::network::EthereumWallet;
use alloy::node_bindings::AnvilInstance;
use alloy::primitives::{Address, U256};
use alloy::providers::DynProvider;
use alloy::signers::local::PrivateKeySigner;
use balances_watcher::config::network_config::NetworkConfig;
use balances_watcher::domain::EvmNetwork;
use balances_watcher::graceful_shutdown::LifeCycle;
use balances_watcher::metrics::Metrics;
use balances_watcher::spawn_server;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use super::api::start_token_list_server;
use super::onchain::{install_infrastructure, spawn_anvil, Weth9, WETH9_ADDRESS};

/// Prometheus recorder is a process-wide singleton — `install_recorder`
/// after the first successful call returns Err, but there's no way to fish
/// out the existing handle. Cache it here so every `Env::spawn` (and hence
/// every parallel test's `/metrics` endpoint) is backed by the same handle
/// that was actually installed.
static METRICS_HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

fn shared_metrics_handle() -> PrometheusHandle {
    METRICS_HANDLE
        .get_or_init(|| {
            PrometheusBuilder::new()
                .install_recorder()
                .expect("first-ever install_recorder should succeed")
        })
        .clone()
}

/// A live integration environment. Dropping it cancels the balances-watcher
/// service; the anvil child process is killed by `AnvilInstance::Drop`.
pub struct Env {
    pub anvil: AnvilInstance,
    pub provider: DynProvider,
    pub deployer: PrivateKeySigner,
    pub deployer_wallet: EthereumWallet,
    pub owner: Address,
    /// Base URL of the balances-watcher service (e.g. `http://127.0.0.1:XXXX`).
    pub service_url: String,
    /// Base URL of the in-process token-list server.
    pub token_list_url: String,
    _token_list_cancel: CancellationToken,
    service_lifecycle: LifeCycle,
}

impl Env {
    /// Full startup: anvil → infra bytecode → token-list server →
    /// balances-watcher service. Anything failing aborts the test with a
    /// pointed panic message.
    pub async fn spawn() -> Self {
        let (anvil, provider, deployer, deployer_wallet) = spawn_anvil().await;
        install_infrastructure(&provider).await;

        let owner = deployer.address();
        let (token_list_url, _token_list_cancel) = start_token_list_server(WETH9_ADDRESS).await;

        let service_lifecycle = LifeCycle::spawn();
        let metrics_handler = shared_metrics_handle();
        let metrics = Arc::new(Metrics::install());

        let network_cfg = NetworkConfig {
            network: EvmNetwork::Eth,
            rpc_http_url: anvil.endpoint(),
            snapshot_interval: 5,
            max_watched_tokens_limit: 1500,
        };

        let server = spawn_server(
            "127.0.0.1:0",
            network_cfg,
            anvil.ws_endpoint(),
            metrics,
            metrics_handler,
            service_lifecycle.clone(),
        )
        .await
        .expect("spawn_server");

        // Let BlockWatcher establish its `newHeads` subscription before the
        // first POST, otherwise the snapshot updater parks and the initial
        // SSE `balance_update` races the connect.
        tokio::time::sleep(Duration::from_millis(1000)).await;

        Self {
            service_url: format!("http://{}", server.local_addr),
            anvil,
            provider,
            deployer,
            deployer_wallet,
            owner,
            token_list_url,
            _token_list_cancel,
            service_lifecycle,
        }
    }

    /// `WETH.deposit()` with `amount` wei attached. Triggers a Deposit log.
    pub async fn weth_deposit(&self, amount: U256) {
        Weth9::new(WETH9_ADDRESS, &self.provider)
            .deposit()
            .value(amount)
            .send()
            .await
            .expect("weth deposit send")
            .get_receipt()
            .await
            .expect("weth deposit receipt");
    }

    /// `WETH.withdraw(amount)`. Triggers a Withdrawal log.
    pub async fn weth_withdraw(&self, amount: U256) {
        Weth9::new(WETH9_ADDRESS, &self.provider)
            .withdraw(amount)
            .send()
            .await
            .expect("weth withdraw send")
            .get_receipt()
            .await
            .expect("weth withdraw receipt");
    }

    /// `WETH.transfer(to, amount)`. Triggers a Transfer log.
    pub async fn weth_transfer(&self, to: Address, amount: U256) {
        Weth9::new(WETH9_ADDRESS, &self.provider)
            .transfer(to, amount)
            .send()
            .await
            .expect("weth transfer send")
            .get_receipt()
            .await
            .expect("weth transfer receipt");
    }

    /// A second EOA to use as a Transfer destination — anvil pre-funds
    /// `keys()[1]` so we can use its address safely.
    pub fn peer_address(&self) -> Address {
        let signer: PrivateKeySigner = self.anvil.keys()[1].clone().into();
        signer.address()
    }

    /// Second pre-funded address distinct from `owner` — useful when a test
    /// wants two independent owners on the same chain.
    pub fn second_owner(&self) -> Address {
        let signer: PrivateKeySigner = self.anvil.keys()[2].clone().into();
        signer.address()
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        self.service_lifecycle.cancel_token.cancel();
    }
}
