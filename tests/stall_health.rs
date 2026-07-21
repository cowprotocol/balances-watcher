//! Stall cross-check behaviour on an idle chain, against a real anvil node.
//!
//! Anvil (automine) mines a block only when a transaction lands, so an
//! empty chain produces no `newHeads` at all — we don't delay header
//! delivery, we withhold the headers by not submitting transactions. From
//! the watcher's side this is identical to a quiet chain: `stream.next()`
//! yields nothing within the stall timeout. That reproduces the idle
//! condition that bites Linea in production (its sequencer batches
//! transactions and goes quiet for longer than the timeout).
//!
//! The behaviour under test is chain-agnostic, so we spawn as Arbitrum —
//! the fastest timings in the table (250ms block time → stall timeout
//! floored at `MIN_STALL_DURATION` = 2s) — purely to keep the test short.
//!
//! This exercises only the `ChainIdle` branch (HTTP head has not advanced
//! either, since automine stops both the WS and HTTP views together). The
//! `StreamStalled` branch — socket silent while the chain keeps moving —
//! needs a TCP proxy between the service and anvil to desync the WS stream
//! from the HTTP head, and is not covered here.
//!
//! `#[ignore]` per the suite convention; run with
//! `cargo test --ignored -- --test-threads=1`.

use alloy::primitives::U256;
use balances_watcher::domain::EvmNetwork;
use std::time::Duration;

mod common;
use common::{fetch_metric, Env};

async fn health_status(service_url: &str) -> reqwest::StatusCode {
    reqwest::get(format!("{service_url}/health"))
        .await
        .expect("GET /health")
        .status()
}

/// An idle chain must not flip `/health`: the stall watchdog fires (2s on
/// Arbitrum timings), the head check sees the HTTP head unchanged, the
/// stream is kept and the canary stays green. Without the check this test
/// fails — the watchdog tears the subscription down, no new header ever
/// arrives on the idle chain, and `/health` sits at 503 for the whole
/// quiet window.
#[tokio::test]
#[ignore]
async fn idle_chain_stays_healthy_past_stall_timeout() {
    let env = Env::spawn_with_network(EvmNetwork::Arbitrum).await;

    // A single transaction makes anvil mine exactly one block, so the
    // watcher observes one header (its health baseline) and the stall check
    // gets a non-zero last_observed to compare against.
    env.weth_deposit(U256::from(1)).await;

    // Wait out startup races: subscription + first header.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if health_status(&env.service_url).await == reqwest::StatusCode::OK {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "service never became healthy after the first mined block"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Idle: no blocks well past the 2s stall timeout, so the watchdog fires
    // at least twice; every firing must resolve to ChainIdle and keep
    // health green.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(
        health_status(&env.service_url).await,
        reqwest::StatusCode::OK,
        "idle chain flipped /health — stall check did not hold the stream"
    );

    // The kept stream must still be live: one more transaction mines one
    // more block, and that header has to arrive through the very same
    // subscription (no resubscribe happened).
    let accepted_before = fetch_metric(&env.service_url, "block_accepted_total").await;
    env.weth_deposit(U256::from(1)).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let accepted_now = fetch_metric(&env.service_url, "block_accepted_total").await;
        if accepted_now > accepted_before {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no header consumed after idle window — stream was not kept alive"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    assert_eq!(
        health_status(&env.service_url).await,
        reqwest::StatusCode::OK,
        "health must stay green after the chain resumes"
    );
}
