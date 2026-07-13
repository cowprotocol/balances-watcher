//! On-chain half of the test harness: anvil bootstrap, canonical-address
//! bytecode installation for Multicall3 + WETH9, and the tiny alloy `sol!`
//! ABI wrapper for WETH9 calls tests issue.

use alloy::hex;
use alloy::network::EthereumWallet;
use alloy::node_bindings::{Anvil, AnvilInstance};
use alloy::primitives::{address, Address, Bytes};
use alloy::providers::ext::AnvilApi;
use alloy::providers::{DynProvider, Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use balances_watcher::domain::EvmNetwork;
use serde_json::json;
use std::path::PathBuf;
use std::time::Duration;

/// Deterministic block cadence — matches what mainnet feels like well enough
/// for the dispatcher's per-block eth_getLogs loop and keeps test wall-time
/// low. Kept as `&str` because `Anvil::args` takes CLI-style strings.
const ANVIL_BLOCK_TIME_SECS: &str = "1";

/// Canonical Multicall3 — hardcoded inside alloy's `MulticallBuilder`, so
/// tests must place it here or every balance read reverts.
pub const MULTICALL3_ADDRESS: Address = address!("0xcA11bde05977b3631167028862bE2a173976CA11");

/// Canonical WETH9 — hardcoded in `EvmNetwork::Eth::weth9_address`, so the
/// WETH9 Deposit / Withdrawal dispatch path is address-filtered against
/// exactly this constant.
pub const WETH9_ADDRESS: Address = address!("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");

/// Second ERC20 used by the token-list-update test. The server treats it as
/// any other ERC20 (no special dispatch), so we reuse WETH9 bytecode at a
/// throwaway address — `deposit()` becomes a convenient way to mint balance
/// for the owner and `transfer` still fires a standard Transfer event.
pub const CUSTOM_TOKEN_ADDRESS: Address = address!("0x00000000000000000000000000000000C0FFee00");

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

/// Start anvil with a fixed chain-id=1 (our `EvmNetwork::Eth` constants
/// assume mainnet addresses) and a 1s auto-mine. Returns the instance plus
/// a ready-to-use provider signed by the first pre-funded key.
pub async fn spawn_anvil() -> (AnvilInstance, DynProvider, PrivateKeySigner, EthereumWallet) {
    let anvil = Anvil::new()
        .chain_id(EvmNetwork::Eth.chain_id())
        .args(["--block-time", ANVIL_BLOCK_TIME_SECS])
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

    (anvil, provider, deployer, deployer_wallet)
}

/// `anvil_setCode`-install Multicall3 and WETH9 at their canonical mainnet
/// addresses, plus a second WETH9 clone at [`CUSTOM_TOKEN_ADDRESS`] used by
/// the token-list-update test as a stand-in "any other ERC20". Bytecode is
/// fetched from a public RPC the first time the suite runs and cached under
/// `target/test-cache/`, so subsequent runs are offline.
pub async fn install_infrastructure(provider: &DynProvider) {
    fetch_and_setcode(provider, MULTICALL3_ADDRESS, "multicall3")
        .await
        .expect("install Multicall3 bytecode");
    fetch_and_setcode(provider, WETH9_ADDRESS, "weth9")
        .await
        .expect("install WETH9 bytecode");
    // Reuse the cached weth9 bytecode — this second address is just a
    // throwaway ERC20 with independent storage.
    fetch_and_setcode(provider, CUSTOM_TOKEN_ADDRESS, "weth9")
        .await
        .expect("install custom-token bytecode");
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
