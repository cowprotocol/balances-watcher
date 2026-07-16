//! Integration tests for the token-list URL hardening (SSRF guard).
//!
//! `tokens_lists_urls` is caller-supplied and fetched server-side, so the
//! service must refuse URLs that point into private address space and cap
//! how much it is willing to download. These tests drive the real HTTP
//! surface (`POST /{chain_id}/sessions/{owner}`) against a service spawned
//! without anvil — no on-chain data is needed, so unlike the other suites
//! they run under plain `cargo test` with no `#[ignore]`.

mod common;

use alloy::primitives::Address;
use axum::{routing::get, Router};
use balances_watcher::config::constants::MAX_TOKEN_LIST_RESPONSE_BYTES;
use common::{post_session, start_token_list_server, ServiceEnv};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

const CLIENT_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

#[tokio::test]
async fn guard_rejects_private_ip_literal_urls() {
    let env = ServiceEnv::spawn(false).await;
    let owner = Address::random();

    for url in [
        "http://127.0.0.1:8080/list.json",
        "http://169.254.169.254/latest/meta-data",
        "http://10.0.0.1/list.json",
        "http://[::1]:8080/list.json",
    ] {
        let t0 = Instant::now();
        let resp = post_session(&env.service_url, owner, CLIENT_ID, url).await;
        assert_eq!(resp.status(), 400, "{url} must be rejected");

        let body = resp.text().await.expect("error body");
        assert!(
            body.contains("not allowed"),
            "unexpected error body for {url}: {body}"
        );

        // Pre-flight rejection: no socket is ever opened, so there must be
        // no connect attempts or retry backoff in the request path.
        assert!(
            t0.elapsed() < Duration::from_secs(2),
            "{url} rejection was not fast: {:?}",
            t0.elapsed()
        );
    }
}

#[tokio::test]
async fn guard_rejects_non_http_schemes() {
    let env = ServiceEnv::spawn(false).await;
    let owner = Address::random();

    let resp = post_session(
        &env.service_url,
        owner,
        CLIENT_ID,
        "ftp://tokens.example.com/list.json",
    )
    .await;
    assert_eq!(resp.status(), 400);

    let body = resp.text().await.expect("error body");
    assert!(
        body.contains("not allowed"),
        "unexpected error body: {body}"
    );
}

#[tokio::test]
async fn guard_rejects_hostnames_resolving_to_private_addresses() {
    let env = ServiceEnv::spawn(false).await;
    let owner = Address::random();

    // `localhost` passes the pre-flight check (it is a name, not an IP
    // literal) — the filtering DNS resolver must refuse it at connect time.
    let resp = post_session(
        &env.service_url,
        owner,
        CLIENT_ID,
        "http://localhost:9/list.json",
    )
    .await;
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn guard_disabled_allows_local_token_lists() {
    let env = ServiceEnv::spawn(true).await;
    let owner = Address::random();
    let (list_url, _cancel) = start_token_list_server(Address::random()).await;

    let resp = post_session(&env.service_url, owner, CLIENT_ID, &list_url).await;
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn oversized_token_list_response_is_rejected() {
    let env = ServiceEnv::spawn(true).await;
    let owner = Address::random();
    let (huge_url, _cancel) = start_oversized_list_server().await;

    let resp = post_session(&env.service_url, owner, CLIENT_ID, &huge_url).await;
    assert_eq!(resp.status(), 400);
}

/// Serve a body one byte past the response cap on `127.0.0.1:0`. The
/// content is irrelevant — the size check must fire before JSON parsing.
async fn start_oversized_list_server() -> (String, CancellationToken) {
    let cancel = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind oversized-list server");
    let addr = listener.local_addr().expect("oversized-list local_addr");
    let url = format!("http://{addr}/huge.json");

    let router = Router::new().route(
        "/huge.json",
        get(|| async { vec![b' '; MAX_TOKEN_LIST_RESPONSE_BYTES + 1] }),
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
