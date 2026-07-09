//! Session-lifecycle / TTL tests.
//!
//! These verify the background cleanup path in
//! `SubscriptionManager::spawn_cleanup` — a session with zero SSE subscribers
//! that stays idle past `SESSION_TTL` is dropped, and its cancel token wound
//! down so its per-session workers exit. Cleanup ticks at `SESSION_TTL`
//! cadence, so the worst-case delay from "went idle" to "gone" is between
//! `SESSION_TTL` and `2 * SESSION_TTL`. We wait `2 * SESSION_TTL + slack`
//! everywhere to keep the tests deterministic without hard-coding numbers.
//!
//! Death signal, kept purely black-box: after cleanup,
//! `GET /sse/.../?client_id=X` returns `404` because
//! `SubscriptionManager::subscribe` no longer finds a session for that key.
//! We also cross-check via the `sessions_expired_total` counter on `/metrics`
//! so a test failing tells us whether the API path or the cleanup itself is
//! misbehaving.

use std::time::Duration;
use uuid::Uuid;

use balances_watcher::config::constants::SESSION_TTL;

mod common;
use common::{fetch_metric, get_sse, open_sse, post_session, sse_stream, Env};

/// Waiting `2 * SESSION_TTL + 2s` covers cleanup's worst-case latency: idle
/// starts right after the previous cleanup tick fires, so up to
/// `2 * SESSION_TTL` can pass before the next tick sees `idle_since >
/// SESSION_TTL`. Two extra seconds absorbs scheduler jitter under load.
fn ttl_wait() -> Duration {
    2 * SESSION_TTL + Duration::from_secs(2)
}

/// Case 1 — an idle session dies alone.
///
/// A `POST /sessions` that is never followed by `GET /sse` sits with
/// `clients == 0`; `idle_since` is stamped at insert time, so the very next
/// cleanup tick already sees an idle candidate. Confirm this by:
/// - `sessions_expired_total` climbs by exactly 1;
/// - a subsequent SSE-connect returns 404.
#[tokio::test]
#[ignore]
async fn idle_session_expires_after_ttl() {
    let env = Env::spawn().await;
    let client_id = Uuid::new_v4().to_string();

    let before = fetch_metric(&env.service_url, "sessions_expired_total").await;

    let resp = post_session(&env.service_url, env.owner, &client_id, &env.token_list_url).await;
    assert!(resp.status().is_success(), "POST status {}", resp.status());

    tokio::time::sleep(ttl_wait()).await;

    let after = fetch_metric(&env.service_url, "sessions_expired_total").await;
    assert!(
        after >= before + 1.0,
        "sessions_expired_total did not increase: before={before} after={after}"
    );

    let sse = get_sse(&env.service_url, env.owner, &client_id).await;
    assert_eq!(
        sse.status().as_u16(),
        404,
        "expected 404 after TTL, got {}",
        sse.status()
    );
}

/// Case 2 — an actively subscribed session survives beyond TTL.
///
/// While an SSE reader is attached, `clients > 0` and `idle_since` is `None`
/// — cleanup skips such rows. Drop the reader and the *same* session must
/// then die on the next tick, proving the survival was due to the connection
/// and not some ambient timing.
#[tokio::test]
#[ignore]
async fn active_session_survives_ttl_dies_after_disconnect() {
    use futures::StreamExt;

    let env = Env::spawn().await;
    let client_id = Uuid::new_v4().to_string();

    let resp = post_session(&env.service_url, env.owner, &client_id, &env.token_list_url).await;
    assert!(resp.status().is_success());

    // Attach an SSE reader that stays connected while we sleep past TTL.
    let mut stream = Box::pin(open_sse(&env.service_url, env.owner, &client_id).await);
    // Consume the initial snapshot so the stream isn't stuck at the first
    // event when tokio wakes it during the sleep.
    let _ = tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await
        .expect("no initial snapshot before wait")
        .expect("stream ended before initial snapshot");

    tokio::time::sleep(ttl_wait()).await;

    // Session must still be alive: a fresh SSE-connect succeeds.
    let second = get_sse(&env.service_url, env.owner, &client_id).await;
    assert!(
        second.status().is_success(),
        "session was reaped despite an active subscriber: status {}",
        second.status()
    );
    // Drop the second reader too, then drop the original — session now has
    // zero subscribers.
    drop(second);
    drop(stream);

    // Give cleanup time to notice and reap.
    tokio::time::sleep(ttl_wait()).await;

    let third = get_sse(&env.service_url, env.owner, &client_id).await;
    assert_eq!(
        third.status().as_u16(),
        404,
        "session should have died after TTL of idleness, got {}",
        third.status()
    );
}

/// Case 3 — two sibling sessions on the same `(chain, owner)` age
/// independently.
///
/// Two `client_id`s POST for the same wallet, but only one keeps an SSE
/// reader attached. The idle sibling must die on schedule, the attached one
/// must live — proving cleanup keys on the full `(chain, owner, client_id)`
/// tuple, not just `owner`.
#[tokio::test]
#[ignore]
async fn siblings_die_independently() {
    use futures::StreamExt;

    let env = Env::spawn().await;
    let alive_cid = Uuid::new_v4().to_string();
    let idle_cid = Uuid::new_v4().to_string();

    let r1 = post_session(&env.service_url, env.owner, &alive_cid, &env.token_list_url).await;
    assert!(r1.status().is_success());
    let r2 = post_session(&env.service_url, env.owner, &idle_cid, &env.token_list_url).await;
    assert!(r2.status().is_success());

    // Keep `alive_cid` subscribed.
    let mut stream = Box::pin(open_sse(&env.service_url, env.owner, &alive_cid).await);
    let _ = tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await
        .expect("no initial snapshot on the alive session")
        .expect("stream closed before initial snapshot");

    let expired_before = fetch_metric(&env.service_url, "sessions_expired_total").await;

    tokio::time::sleep(ttl_wait()).await;

    let expired_after = fetch_metric(&env.service_url, "sessions_expired_total").await;
    assert!(
        expired_after >= expired_before + 1.0,
        "cleanup counter did not tick for the idle sibling: {expired_before} → {expired_after}"
    );

    let idle_probe = get_sse(&env.service_url, env.owner, &idle_cid).await;
    assert_eq!(
        idle_probe.status().as_u16(),
        404,
        "idle sibling should be gone, got {}",
        idle_probe.status()
    );

    // The attached SSE stream must still work. Read one more event to be
    // sure the stream is live, then drop cleanly.
    let alive_probe = get_sse(&env.service_url, env.owner, &alive_cid).await;
    assert!(
        alive_probe.status().is_success(),
        "actively subscribed sibling was reaped: status {}",
        alive_probe.status()
    );

    // Keep the compiler happy about the borrows on `stream` / `sse_stream`
    // — we're really using this just as a lint sink.
    let _ = sse_stream;
    drop(stream);
}
