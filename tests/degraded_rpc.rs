//! Degraded-provider scenarios for the event dispatcher, against a real
//! anvil chain with an [`RpcProxy`] fault-injection layer on the HTTP leg
//! (see `common::rpc_proxy` for why anvil alone cannot produce these).
//!
//! This is the regression suite for the 2026-07-29 Polygon crashloop: a
//! fallback node slightly slower than the chain's block time grew the
//! dispatcher's lag past `max_block_lag`, `/health` flipped, and k8s
//! crash-looped the pod. The fix — queue drain into ranged fetches plus
//! range bisect — is exercised here end-to-end:
//!
//! - positive: a backlog built under a slow provider is drained with
//!   ranged `eth_getLogs` and survives a provider that rejects wide
//!   ranges, with zero lost blocks;
//! - negative: a permanently failing block loses only itself and does not
//!   flip `/health`;
//! - incident replay: a slow provider flips `/health`, and recovery
//!   happens through the drain alone — no restart.
//!
//! Anvil mines every second (`--block-time 1`, see `common::onchain`), so
//! heads keep flowing over WS exactly like a live chain while the proxy
//! degrades only the HTTP side.
//!
//! `#[ignore]` per the suite convention; run with
//! `cargo test --test degraded_rpc -- --ignored --test-threads=1`.

use alloy::primitives::U256;
use alloy::providers::Provider;
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

async fn wait_healthy(service_url: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if health_status(service_url).await == reqwest::StatusCode::OK {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "service never became healthy after spawn"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll `metric` until it reaches at least `baseline + expected_delta`.
/// Panics with the observed value on timeout.
async fn wait_metric_delta(
    service_url: &str,
    metric: &str,
    baseline: f64,
    expected_delta: f64,
    budget: Duration,
) {
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let now = fetch_metric(service_url, metric).await;
        if now >= baseline + expected_delta {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{metric}: expected +{expected_delta} over {baseline}, still at {now} \
             after {budget:?}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// A backlog built under a slow provider must be drained with ranged
/// fetches, and a provider that then rejects wide ranges must not cost a
/// single block: the bisect refetches the halves until the provider
/// accepts. Every Transfer emitted during the degradation reaches the
/// dispatcher; `missed_block_logs` stays flat.
#[tokio::test]
#[ignore]
async fn degraded_provider_backlog_is_bisected_without_losses() {
    let (env, proxy) = Env::spawn_with_rpc_proxy(EvmNetwork::Polygon).await;
    env.custom_deposit(U256::from(100)).await;
    wait_healthy(&env.service_url).await;

    let erc20_before = fetch_metric(&env.service_url, "erc20_event_received_total").await;
    let missed_before =
        fetch_metric(&env.service_url, "event_dispatcher_missed_block_logs_total").await;

    // Slow provider: each eth_getLogs takes 5s against a 1s block time, so
    // heads queue while cycles crawl — the incident profile.
    proxy.set_get_logs_delay(Some(Duration::from_secs(5)));

    // Emit Transfers while degraded; each lands in some queued block.
    let peer = env.peer_address();
    for _ in 0..6 {
        env.custom_transfer(peer, U256::from(1)).await;
    }

    // Provider "recovers" but with a range cap: the drained backlog comes
    // out as a wide range, gets rejected, and must be bisected down.
    proxy.set_max_get_logs_blocks(Some(2));
    proxy.set_get_logs_delay(None);

    wait_metric_delta(
        &env.service_url,
        "erc20_event_received_total",
        erc20_before,
        6.0,
        Duration::from_secs(30),
    )
    .await;

    let missed_after =
        fetch_metric(&env.service_url, "event_dispatcher_missed_block_logs_total").await;
    assert_eq!(
        missed_after, missed_before,
        "no block may lose logs: wide-range rejections must be healed by the bisect"
    );

    let widest_seen = proxy
        .get_logs_ranges()
        .iter()
        .map(|(from, to)| to - from + 1)
        .max()
        .unwrap_or(0);
    assert!(
        widest_seen >= 3,
        "queue drain never produced a ranged fetch (widest getLogs range: {widest_seen} blocks)"
    );
    assert!(
        !proxy.rejected_get_logs_ranges().is_empty(),
        "the range cap was never hit — bisect path untested"
    );

    assert_eq!(
        health_status(&env.service_url).await,
        reqwest::StatusCode::OK,
        "a drained backlog must not leave health red"
    );
}

/// A block whose `eth_getLogs` fails deterministically (every range
/// containing it is rejected) loses exactly its own logs — the bisect
/// isolates it, neighbours still deliver, `/health` stays green (per-block
/// fetch failures are logged, not fatal), and the service keeps processing
/// once the block heals.
#[tokio::test]
#[ignore]
async fn permanently_failing_block_loses_only_itself() {
    let (env, proxy) = Env::spawn_with_rpc_proxy(EvmNetwork::Polygon).await;
    env.custom_deposit(U256::from(100)).await;
    wait_healthy(&env.service_url).await;

    let erc20_before = fetch_metric(&env.service_url, "erc20_event_received_total").await;
    let missed_before =
        fetch_metric(&env.service_url, "event_dispatcher_missed_block_logs_total").await;

    // Poison a near-future block. Anvil mines it within ~2s; both per-block
    // fetches (erc20 + weth9) will fail on it and count one missed block each.
    let poisoned = env.provider.get_block_number().await.expect("head") + 2;
    proxy.set_reject_range_containing(Some(poisoned));

    wait_metric_delta(
        &env.service_url,
        "event_dispatcher_missed_block_logs_total",
        missed_before,
        2.0,
        Duration::from_secs(15),
    )
    .await;

    assert_eq!(
        health_status(&env.service_url).await,
        reqwest::StatusCode::OK,
        "a single lost block must not flip /health"
    );

    // Heal the block; a fresh Transfer must flow through the same dispatcher.
    proxy.set_reject_range_containing(None);
    env.custom_transfer(env.peer_address(), U256::from(1)).await;
    wait_metric_delta(
        &env.service_url,
        "erc20_event_received_total",
        erc20_before,
        1.0,
        Duration::from_secs(15),
    )
    .await;

    let missed_after =
        fetch_metric(&env.service_url, "event_dispatcher_missed_block_logs_total").await;
    assert_eq!(
        missed_after,
        missed_before + 2.0,
        "exactly the poisoned block may be lost (once per fetch path), nothing else"
    );
}

/// Incident replay: a provider slower than the block time grows the lag
/// past `max_block_lag` and `/health` flips to 503 — and once the provider
/// recovers, the queue drain catches the dispatcher up and health returns
/// to 200 **without a restart**. Pre-fix, the second half fails: the
/// backlog is worked off one block per cycle and lag keeps growing as long
/// as blocks keep coming.
#[tokio::test]
#[ignore]
async fn slow_provider_flips_health_and_drain_recovers_without_restart() {
    // Eth timings: max_block_lag = 3, so a 4s-per-fetch provider against
    // 1s blocks overruns the budget within a couple of cycles.
    let (env, proxy) = Env::spawn_with_rpc_proxy(EvmNetwork::Eth).await;
    wait_healthy(&env.service_url).await;

    proxy.set_get_logs_delay(Some(Duration::from_secs(4)));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if health_status(&env.service_url).await == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "slow provider never flipped /health — lag guard broken"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Provider recovers. The dispatcher must drain the queued heads in one
    // ranged plan and bring lag back under the budget on its own.
    proxy.set_get_logs_delay(None);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if health_status(&env.service_url).await == reqwest::StatusCode::OK {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "health never recovered after the provider healed — drain did not catch up"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}
