//! Integration-test harness.
//!
//! Boots a real [anvil](https://book.getfoundry.sh/anvil/) node with
//! `chain_id=1`, `anvil_setCode`-installs canonical Multicall3 and WETH9
//! at their mainnet addresses (fetched from a public RPC on first run and
//! cached under `target/test-cache/` for offline reruns), then spins up an
//! in-process token-list HTTP server and the balances-watcher service.
//!
//! Tests use WETH9 as their sole ERC20 — no need for a hand-rolled test
//! contract because WETH9 covers every transport we care about
//! (Transfer via `transfer`, Deposit / Withdrawal via `deposit` / `withdraw`).
//!
//! All integration tests carry `#[ignore]` so `cargo test` on a checkout with
//! no `anvil` in PATH stays green; run the suite explicitly with
//! `cargo test --ignored -- --test-threads=1`.

#![allow(dead_code)]

use alloy::hex;
use alloy::network::EthereumWallet;
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{address, Address, Bytes, U256};
use alloy::providers::ext::AnvilApi;
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use axum::{routing::get, Json, Router};
use balances_watcher::config::network_config::NetworkConfig;
use balances_watcher::domain::EvmNetwork;
use balances_watcher::graceful_shutdown::LifeCycle;
use balances_watcher::metrics::Metrics;
use balances_watcher::spawn_server;
use metrics_exporter_prometheus::PrometheusBuilder;
use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// Canonical Multicall3 — hardcoded inside alloy's `MulticallBuilder`, so
/// tests must place it here or every balance read reverts.
pub const MULTICALL3_ADDRESS: Address = address!("0xcA11bde05977b3631167028862bE2a173976CA11");

/// Canonical WETH9 — hardcoded in `EvmNetwork::Eth::weth9_address`, so the
/// WETH9 Deposit / Withdrawal dispatch path is address-filtered against
/// exactly this constant.
pub const WETH9_ADDRESS: Address = address!("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");

/// Public read-only RPC used solely to source deployed bytecode for
/// Multicall3 and WETH9 the first time the suite runs. Results are cached
/// under `target/test-cache/`, so subsequent runs are offline. Override via
/// `INTEGRATION_TEST_RPC_URL` if this endpoint ever drops out.
const DEFAULT_BYTECODE_SOURCE_RPC: &str = "https://ethereum-rpc.publicnode.com";

sol! {
    /// WETH9 surface used by tests. The bytecode lives on-chain via
    /// `anvil_setCode`; here we only need the ABI to build calls.
    #[sol(rpc)]
    contract Weth9 {
        function deposit() external payable;
        function withdraw(uint256 wad) external;
        function transfer(address dst, uint256 wad) external returns (bool);
        function balanceOf(address) external view returns (uint256);
        event Transfer(address indexed src, address indexed dst, uint256 wad);
        event Deposit(address indexed dst, uint256 wad);
        event Withdrawal(address indexed src, uint256 wad);
    }
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
        let anvil = Anvil::new()
            .chain_id(1)
            .args(["--block-time", "1"])
            .try_spawn()
            .expect("anvil failed to start — is `anvil` on PATH? install foundry via foundryup");

        let deployer: PrivateKeySigner = anvil.keys()[0].clone().into();
        let deployer_wallet = EthereumWallet::from(deployer.clone());

        let provider = ProviderBuilder::new()
            .wallet(deployer_wallet.clone())
            .connect(&anvil.endpoint())
            .await
            .expect("connect to anvil HTTP")
            .erased();

        fetch_and_setcode(&provider, MULTICALL3_ADDRESS, "multicall3")
            .await
            .expect("install Multicall3 bytecode");
        fetch_and_setcode(&provider, WETH9_ADDRESS, "weth9")
            .await
            .expect("install WETH9 bytecode");

        let owner = deployer.address();
        let (token_list_url, _token_list_cancel) = start_token_list_server(WETH9_ADDRESS).await;

        let service_lifecycle = LifeCycle::spawn();
        // install_recorder is process-wide — allow parallel tests to race
        // by falling back to a detached builder on the second caller.
        let metrics_handler = PrometheusBuilder::new()
            .install_recorder()
            .unwrap_or_else(|_| PrometheusBuilder::new().build_recorder().handle());
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
}

impl Drop for Env {
    fn drop(&mut self) {
        self.service_lifecycle.cancel_token.cancel();
    }
}

fn cache_dir() -> PathBuf {
    let dir = std::env::var("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target"));
    dir.join("test-cache")
}

async fn fetch_and_setcode(
    provider: &DynProvider,
    addr: Address,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let cache_path = cache_dir().join(format!("{label}.hex"));
    std::fs::create_dir_all(cache_dir())?;

    let bytecode: Bytes = if let Ok(cached) = std::fs::read_to_string(&cache_path) {
        Bytes::from(hex::decode(cached.trim().trim_start_matches("0x"))?)
    } else {
        let rpc_url = std::env::var("INTEGRATION_TEST_RPC_URL")
            .unwrap_or_else(|_| DEFAULT_BYTECODE_SOURCE_RPC.to_string());
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getCode",
            "params": [format!("{addr:#x}"), "latest"],
        });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        let resp: serde_json::Value = client
            .post(&rpc_url)
            .json(&body)
            .send()
            .await?
            .json()
            .await?;
        let raw = resp
            .get("result")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("no result field in eth_getCode response: {resp}"))?;
        if raw == "0x" {
            return Err(format!("empty bytecode for {addr:#x} on {rpc_url}").into());
        }
        std::fs::write(&cache_path, raw)?;
        Bytes::from(hex::decode(raw.trim_start_matches("0x"))?)
    };

    provider.anvil_set_code(addr, bytecode).await?;
    Ok(())
}

/// A minimal token-list HTTP endpoint serving exactly one token — WETH9.
/// The service adds WETH9 automatically at session-time regardless, but
/// having it in the list keeps the request body meaningful and exercises
/// the token-list fetcher path.
async fn start_token_list_server(token: Address) -> (String, CancellationToken) {
    let cancel = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind token-list server");
    let addr = listener.local_addr().expect("token-list local_addr");
    let url = format!("http://{addr}/list.json");

    let token_hex = format!("{token:#x}");
    let router = Router::new().route(
        "/list.json",
        get(move || {
            let token_hex = token_hex.clone();
            async move {
                Json(json!({
                    "name": "test list",
                    "tokens": [
                        {
                            "chainId": 1,
                            "address": token_hex,
                            "symbol": "WETH",
                            "decimals": 18,
                            "name": "Wrapped Ether"
                        }
                    ]
                }))
            }
        }),
    );

    let shutdown = cancel.clone();
    tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await
            .ok();
    });

    (url, cancel)
}

/// POST /1/sessions/{owner} with a fresh `X-Client-Id` and the harness's
/// token-list URL.
pub async fn post_session(
    service_url: &str,
    owner: Address,
    client_id: &str,
    token_list_url: &str,
) -> reqwest::Response {
    let owner_hex = format!("{owner:#x}");
    let url = format!("{service_url}/1/sessions/{owner_hex}");
    let body = json!({
        "tokensListsUrls": [token_list_url],
        "customTokens": []
    });
    reqwest::Client::new()
        .post(&url)
        .header("X-Client-Id", client_id)
        .json(&body)
        .send()
        .await
        .expect("POST /sessions")
}

/// A minimal parsed `balance_update` event — just the address→amount map.
#[derive(Debug, Clone)]
pub struct BalanceUpdate {
    pub balances: std::collections::HashMap<Address, U256>,
}

/// Open the SSE stream and yield each `balance_update` event as it arrives.
pub async fn open_sse(
    service_url: &str,
    owner: Address,
    client_id: &str,
) -> impl futures::Stream<Item = BalanceUpdate> {
    use eventsource_stream::Eventsource;
    use futures::StreamExt;

    let owner_hex = format!("{owner:#x}");
    let url = format!("{service_url}/sse/1/balances/{owner_hex}?client_id={client_id}");
    let response = reqwest::Client::new()
        .get(&url)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("SSE connect");
    assert!(
        response.status().is_success(),
        "SSE request failed with status {}",
        response.status()
    );

    response
        .bytes_stream()
        .eventsource()
        .filter_map(|ev| async move {
            let ev = ev.ok()?;
            if ev.event != "balance_update" {
                return None;
            }
            let raw: serde_json::Value = serde_json::from_str(&ev.data).ok()?;
            let balances_obj = raw.get("balances")?.as_object()?;
            let mut balances = std::collections::HashMap::new();
            for (k, v) in balances_obj {
                let addr: Address = k.parse().ok()?;
                // amounts are decimal strings in the SSE payload
                let amount = U256::from_str_radix(v.as_str()?, 10).ok()?;
                balances.insert(addr, amount);
            }
            Some(BalanceUpdate { balances })
        })
}
