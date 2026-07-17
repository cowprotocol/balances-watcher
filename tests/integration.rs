//! Integration tests against a real anvil node.
//!
//! All are `#[ignore]` so `cargo test` on a plain checkout stays green; run
//! this suite with `cargo test --ignored -- --test-threads=1` (parallelism
//! is disabled because the global metrics recorder is a process-wide
//! singleton that gets re-installed per test).

use alloy::primitives::U256;
use std::time::Duration;
use uuid::Uuid;

mod common;
use common::{open_sse, post_session, wait_for, Env, WETH9_ADDRESS};

/// Case 1 — snapshot after POST.
///
/// After `POST /sessions`, the very first SSE `balance_update` is the full
/// snapshot: it must contain WETH9 (added automatically by the server) with
/// a zero balance for a fresh owner.
#[tokio::test]
#[ignore]
async fn snapshot_after_post() {
    let env = Env::spawn().await;
    let client_id = Uuid::new_v4().to_string();

    let resp = post_session(&env.service_url, env.owner, &client_id, &env.token_list_url).await;
    assert!(
        resp.status().is_success(),
        "POST /sessions status {}",
        resp.status()
    );

    let stream = open_sse(&env.service_url, env.owner, &client_id).await;
    tokio::pin!(stream);

    let first = wait_for(&mut stream, Duration::from_secs(15), |_| true)
        .await
        .expect("timed out waiting for the initial snapshot");
    let weth_balance = first
        .balances
        .get(&WETH9_ADDRESS)
        .expect("WETH9 missing from initial snapshot");
    assert_eq!(
        *weth_balance,
        U256::ZERO,
        "fresh owner should have zero WETH"
    );
}

/// Case 2 — WETH `deposit()` and `withdraw()` drive SSE updates.
///
/// `deposit()` emits a WETH9 Deposit log; the dispatcher's WETH9 branch
/// routes it into the session's refresh queue and the snapshot updater
/// broadcasts the diff. Same for `withdraw()`.
#[tokio::test]
#[ignore]
async fn weth_deposit_and_withdraw_are_broadcast() {
    let env = Env::spawn().await;
    let client_id = Uuid::new_v4().to_string();

    let resp = post_session(&env.service_url, env.owner, &client_id, &env.token_list_url).await;
    assert!(resp.status().is_success());

    let stream = open_sse(&env.service_url, env.owner, &client_id).await;
    tokio::pin!(stream);

    // Drain initial snapshot.
    let _ = wait_for(&mut stream, Duration::from_secs(15), |_| true)
        .await
        .expect("no initial snapshot");

    let deposit_amount = U256::from(500_000_000_000_000_000u128); // 0.5 ETH
    env.weth_deposit(deposit_amount).await;

    let after_deposit = wait_for(&mut stream, Duration::from_secs(30), |event| {
        event
            .balances
            .get(&WETH9_ADDRESS)
            .map(|v| *v >= deposit_amount)
            .unwrap_or(false)
    })
    .await
    .expect("no update reflecting deposit");
    let after_deposit_bal = after_deposit.balances[&WETH9_ADDRESS];
    assert!(after_deposit_bal >= deposit_amount);

    let withdraw_amount = U256::from(200_000_000_000_000_000u128); // 0.2 ETH
    env.weth_withdraw(withdraw_amount).await;

    let after_withdraw = wait_for(&mut stream, Duration::from_secs(30), |event| {
        event
            .balances
            .get(&WETH9_ADDRESS)
            .map(|v| *v < after_deposit_bal)
            .unwrap_or(false)
    })
    .await
    .expect("no update reflecting withdraw");
    let after_withdraw_bal = after_withdraw.balances[&WETH9_ADDRESS];
    assert_eq!(after_withdraw_bal, after_deposit_bal - withdraw_amount);
}

/// Case 3 — ERC20 Transfer drives SSE updates.
///
/// Sends WETH from the owner to a second EOA and asserts the owner's SSE
/// stream reflects the reduced balance. Uses the same WETH9 contract for
/// simplicity — its `transfer` emits a standard ERC20 Transfer event so
/// the global Transfer dispatcher path fires.
#[tokio::test]
#[ignore]
async fn erc20_transfer_is_broadcast() {
    let env = Env::spawn().await;
    let client_id = Uuid::new_v4().to_string();

    // Pre-fund the owner with WETH so there's something to send.
    let seed = U256::from(1_000_000_000_000_000_000u128); // 1 WETH
    env.weth_deposit(seed).await;

    let resp = post_session(&env.service_url, env.owner, &client_id, &env.token_list_url).await;
    assert!(resp.status().is_success());

    let stream = open_sse(&env.service_url, env.owner, &client_id).await;
    tokio::pin!(stream);

    let initial = wait_for(&mut stream, Duration::from_secs(15), |event| {
        event.balances.contains_key(&WETH9_ADDRESS)
    })
    .await
    .expect("no initial snapshot");
    let initial_bal = initial.balances[&WETH9_ADDRESS];
    assert!(initial_bal >= seed, "seeded WETH not visible in snapshot");

    let transfer_amount = U256::from(300_000_000_000_000_000u128); // 0.3 WETH
    env.weth_transfer(env.peer_address(), transfer_amount).await;

    let after = wait_for(&mut stream, Duration::from_secs(30), |event| {
        event
            .balances
            .get(&WETH9_ADDRESS)
            .map(|v| *v < initial_bal)
            .unwrap_or(false)
    })
    .await
    .expect("no update reflecting transfer");
    assert_eq!(
        after.balances[&WETH9_ADDRESS],
        initial_bal - transfer_amount
    );
}

/// Case 4 — MAX_CLIENTS_PER_OWNER cap enforcement.
///
/// The `(N+1)`-th distinct client_id targeting the same `(chain, owner)`
/// must be rejected with `429 Too Many Requests`. Uses `MAX_CLIENTS_PER_OWNER`
/// from `constants.rs` (currently 5).
#[tokio::test]
#[ignore]
async fn owner_client_limit_returns_429() {
    let env = Env::spawn().await;

    // MAX_CLIENTS_PER_OWNER is the cap value under test.
    let limit = balances_watcher::config::constants::MAX_CLIENTS_PER_OWNER;

    // First `limit` distinct client_ids succeed.
    for _ in 0..limit {
        let cid = Uuid::new_v4().to_string();
        let resp = post_session(&env.service_url, env.owner, &cid, &env.token_list_url).await;
        assert!(
            resp.status().is_success(),
            "expected 2xx below the cap, got {}",
            resp.status()
        );
    }

    // The (limit+1)-th distinct client_id must be rejected.
    let over_cid = Uuid::new_v4().to_string();
    let resp = post_session(&env.service_url, env.owner, &over_cid, &env.token_list_url).await;
    assert_eq!(
        resp.status().as_u16(),
        429,
        "expected 429 at cap, got {}",
        resp.status()
    );
}

/// Case 5 — a client attaching late to a live session is seeded with the
/// full, up-to-date snapshot (not just future diffs).
///
/// Multiple SSE connections on the same `(chain, owner, client_id)` share one
/// subscription. A connection that joins *after* the balance has already moved
/// must still receive the current snapshot on connect — `create_sse_connection`
/// seeds each new client from `current_snapshot()` before switching it to
/// broadcast diffs. We prove it by moving the balance while two clients watch,
/// then attaching a third and asserting its first event already reflects the
/// change.
#[tokio::test]
#[ignore]
async fn late_joining_client_gets_full_snapshot() {
    let env = Env::spawn().await;
    let client_id = Uuid::new_v4().to_string();

    // Seed WETH so the snapshot carries a non-zero balance worth re-sending.
    let seed = U256::from(1_000_000_000_000_000_000u128); // 1 WETH
    env.weth_deposit(seed).await;

    let resp = post_session(&env.service_url, env.owner, &client_id, &env.token_list_url).await;
    assert!(resp.status().is_success());

    // Two clients attach up front and drain their initial snapshot.
    let stream_a = open_sse(&env.service_url, env.owner, &client_id).await;
    tokio::pin!(stream_a);
    let _ = wait_for(&mut stream_a, Duration::from_secs(15), |event| {
        event.balances.contains_key(&WETH9_ADDRESS)
    })
    .await
    .expect("client A got no initial snapshot");

    let stream_b = open_sse(&env.service_url, env.owner, &client_id).await;
    tokio::pin!(stream_b);
    let _ = wait_for(&mut stream_b, Duration::from_secs(15), |event| {
        event.balances.contains_key(&WETH9_ADDRESS)
    })
    .await
    .expect("client B got no initial snapshot");

    // Advance the balance past what A and B saw at connect time.
    let extra = U256::from(500_000_000_000_000_000u128); // +0.5 WETH
    env.weth_deposit(extra).await;
    let expected = seed + extra;

    // Wait until A observes the diff — this guarantees the server-side snapshot
    // has actually advanced before the late client connects.
    let _ = wait_for(&mut stream_a, Duration::from_secs(30), |event| {
        event
            .balances
            .get(&WETH9_ADDRESS)
            .map(|v| *v == expected)
            .unwrap_or(false)
    })
    .await
    .expect("client A never saw the post-join deposit");

    // The late joiner's very first event must be the full current snapshot,
    // already carrying the advanced balance.
    let stream_c = open_sse(&env.service_url, env.owner, &client_id).await;
    tokio::pin!(stream_c);
    let snapshot_c = wait_for(&mut stream_c, Duration::from_secs(15), |event| {
        event.balances.contains_key(&WETH9_ADDRESS)
    })
    .await
    .expect("late client C got no initial snapshot");
    assert_eq!(
        snapshot_c.balances[&WETH9_ADDRESS], expected,
        "late joiner must be seeded with the full up-to-date snapshot"
    );
}

/// Case 6 — a single balance change fans out to every client on the session.
///
/// Three SSE connections share one `(chain, owner, client_id)` session. One
/// on-chain WETH deposit must be delivered to all three broadcast receivers,
/// proving the fan-out reaches every attached client, not just one.
#[tokio::test]
#[ignore]
async fn balance_update_fans_out_to_all_session_clients() {
    let env = Env::spawn().await;
    let client_id = Uuid::new_v4().to_string();

    let seed = U256::from(1_000_000_000_000_000_000u128); // 1 WETH
    env.weth_deposit(seed).await;

    let resp = post_session(&env.service_url, env.owner, &client_id, &env.token_list_url).await;
    assert!(resp.status().is_success());

    // Three clients on the same session.
    let stream_a = open_sse(&env.service_url, env.owner, &client_id).await;
    let stream_b = open_sse(&env.service_url, env.owner, &client_id).await;
    let stream_c = open_sse(&env.service_url, env.owner, &client_id).await;
    tokio::pin!(stream_a);
    tokio::pin!(stream_b);
    tokio::pin!(stream_c);

    // Drain each client's initial snapshot.
    let _ = wait_for(&mut stream_a, Duration::from_secs(15), |event| {
        event.balances.contains_key(&WETH9_ADDRESS)
    })
    .await
    .expect("client A got no initial snapshot");
    let _ = wait_for(&mut stream_b, Duration::from_secs(15), |event| {
        event.balances.contains_key(&WETH9_ADDRESS)
    })
    .await
    .expect("client B got no initial snapshot");
    let _ = wait_for(&mut stream_c, Duration::from_secs(15), |event| {
        event.balances.contains_key(&WETH9_ADDRESS)
    })
    .await
    .expect("client C got no initial snapshot");

    // A single on-chain change must reach all three clients.
    let extra = U256::from(400_000_000_000_000_000u128); // +0.4 WETH
    env.weth_deposit(extra).await;
    let expected = seed + extra;

    let after_a = wait_for(&mut stream_a, Duration::from_secs(30), |event| {
        event
            .balances
            .get(&WETH9_ADDRESS)
            .map(|v| *v == expected)
            .unwrap_or(false)
    })
    .await
    .expect("client A missed the fan-out update");
    let after_b = wait_for(&mut stream_b, Duration::from_secs(30), |event| {
        event
            .balances
            .get(&WETH9_ADDRESS)
            .map(|v| *v == expected)
            .unwrap_or(false)
    })
    .await
    .expect("client B missed the fan-out update");
    let after_c = wait_for(&mut stream_c, Duration::from_secs(30), |event| {
        event
            .balances
            .get(&WETH9_ADDRESS)
            .map(|v| *v == expected)
            .unwrap_or(false)
    })
    .await
    .expect("client C missed the fan-out update");

    assert_eq!(after_a.balances[&WETH9_ADDRESS], expected);
    assert_eq!(after_b.balances[&WETH9_ADDRESS], expected);
    assert_eq!(after_c.balances[&WETH9_ADDRESS], expected);
}
