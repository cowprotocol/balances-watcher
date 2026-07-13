//! End-to-end test for the token-list `PUT` flow.
//!
//! Walks a session through the full "add token → transfer → remove token →
//! transfer" cycle and asserts each step is reflected on the SSE stream:
//!
//! 1. `POST` a session that watches only WETH9.
//! 2. Drain the initial snapshot — nothing else should be there.
//! 3. Mint balance on a second ERC20 (custom token).
//! 4. `PUT` to add the custom token to the watched list. Expect an SSE
//!    update carrying a non-zero balance for that address.
//! 5. `custom_transfer` a slice of the balance away. Expect an SSE update
//!    showing the reduced balance for that address.
//! 6. `PUT` to remove the custom token (send only WETH9). Expect the next
//!    snapshot cycle to no longer include the custom token address.
//! 7. `custom_transfer` again — the Transfer event fires on-chain but the
//!    server no longer watches this token, so **no SSE update mentioning
//!    the custom token address may arrive** for the duration of the probe
//!    window.

use alloy::primitives::U256;
use std::time::Duration;
use uuid::Uuid;

mod common;
use common::{
    open_sse, post_session, put_session, wait_for, Env, CUSTOM_TOKEN_ADDRESS, WETH9_ADDRESS,
};

#[tokio::test]
#[ignore]
async fn put_add_transfer_remove_flow() {
    let env = Env::spawn().await;
    let client_id = Uuid::new_v4().to_string();

    // Step 1 — create the session watching only WETH9 (via the token list;
    // no custom tokens yet).
    let resp = post_session(&env.service_url, env.owner, &client_id, &env.token_list_url).await;
    assert!(
        resp.status().is_success(),
        "POST /sessions: {}",
        resp.status()
    );

    // Step 2 — drain the initial snapshot. It must include WETH9 but not
    // the custom token address, since the session hasn't asked for it yet.
    let mut stream = Box::pin(open_sse(&env.service_url, env.owner, &client_id).await);
    let initial = wait_for(&mut stream, Duration::from_secs(15), |event| {
        event.balances.contains_key(&WETH9_ADDRESS)
    })
    .await
    .expect("no initial snapshot");
    assert!(
        !initial.balances.contains_key(&CUSTOM_TOKEN_ADDRESS),
        "initial snapshot must not contain the custom token before PUT-add"
    );

    // Step 3 — mint balance on the custom token so there's something to
    // report once it becomes watched.
    let mint_amount = U256::from(1_000_000_000_000_000_000u128); // 1e18
    env.custom_deposit(mint_amount).await;

    // Step 4 — PUT the custom token in. The server's set_watched_tokens
    // fires a snapshot refresh whenever the set actually changes, so we
    // should get a fresh chunk containing the new address with the balance
    // we just minted.
    let resp = put_session(
        &env.service_url,
        env.owner,
        &client_id,
        &env.token_list_url,
        &[CUSTOM_TOKEN_ADDRESS],
    )
    .await;
    assert!(
        resp.status().is_success(),
        "PUT /sessions add: {}",
        resp.status()
    );

    let event = wait_for(&mut stream, Duration::from_secs(30), |event| {
        event
            .balances
            .get(&CUSTOM_TOKEN_ADDRESS)
            .map(|v| *v >= mint_amount)
            .unwrap_or(false)
    })
    .await
    .expect("no SSE update after PUT-add");
    let balance = event.balances[&CUSTOM_TOKEN_ADDRESS];

    // Step 5 — transfer some of the custom-token balance away. Standard
    // ERC20 Transfer log fires the dispatcher path.
    let transfer_amount = U256::from(250_000_000_000_000_000u128); // 0.25
    env.custom_transfer(env.peer_address(), transfer_amount)
        .await;

    let event = wait_for(&mut stream, Duration::from_secs(30), |event| {
        event
            .balances
            .get(&CUSTOM_TOKEN_ADDRESS)
            .map(|v| *v < balance)
            .unwrap_or(false)
    })
    .await
    .expect("no SSE update after custom transfer");
    assert_eq!(
        event.balances[&CUSTOM_TOKEN_ADDRESS],
        balance - transfer_amount
    );

    // Step 6 — PUT the custom token out. WETH9 stays because the server
    // always adds it back. We can't cleanly assert "the next snapshot is
    // without the custom token" because after the change WETH9's balance
    // hasn't moved and the custom token is no longer measured, so no diff
    // is broadcast at all. Step 7 verifies the removal took effect by the
    // stronger negative-assertion route.
    let resp = put_session(
        &env.service_url,
        env.owner,
        &client_id,
        &env.token_list_url,
        &[],
    )
    .await;
    assert!(
        resp.status().is_success(),
        "PUT /sessions remove: {}",
        resp.status()
    );

    // Step 7 — transfer the (still non-zero) custom-token balance again.
    // On-chain this fires a Transfer log, but the server no longer watches
    // this token, so nothing about `CUSTOM_TOKEN_ADDRESS` may show up on
    // the SSE stream during the probe window. Assert the negative: no
    // arriving event within `probe` may mention the custom token address.
    let transfer_amount = U256::from(50_000_000_000_000_000u128); // 0.05
    env.custom_transfer(env.peer_address(), transfer_amount)
        .await;

    // Give the dispatcher generous time — one CALL_QUEUE_DELAY window
    // (300 ms) + a block-time tick + snapshot_interval (5 s in the test
    // NetworkConfig) with margin.
    let probe = Duration::from_secs(8);
    let leaked = wait_for(&mut stream, probe, |event| {
        event.balances.contains_key(&CUSTOM_TOKEN_ADDRESS)
    })
    .await;
    assert!(
        leaked.is_none(),
        "server broadcast a balance for the removed custom token: {leaked:?}"
    );
}
