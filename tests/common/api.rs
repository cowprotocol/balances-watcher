//! HTTP-side half of the harness: in-process token-list server, the two API
//! calls tests make (`post_session`, `open_sse`), a Prometheus scraper, and
//! the parsed SSE event type.

use alloy::primitives::{Address, U256};
use axum::{routing::get, Json, Router};
use serde_json::json;
use std::collections::HashMap;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// Serve a single-token JSON list on `127.0.0.1:0`. Returns `(url, cancel)`;
/// drop `cancel` to shut the server down.
pub async fn start_token_list_server(token: Address) -> (String, CancellationToken) {
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

/// POST /1/sessions/{owner} with the given `X-Client-Id` and token-list URL.
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

/// GET /sse/1/balances/{owner}?client_id=... — returns the raw response so
/// tests can either read the stream (see [`sse_stream`]) or just inspect the
/// status code (used by the TTL tests to assert 404 after expiry).
pub async fn get_sse(service_url: &str, owner: Address, client_id: &str) -> reqwest::Response {
    let owner_hex = format!("{owner:#x}");
    let url = format!("{service_url}/sse/1/balances/{owner_hex}?client_id={client_id}");
    reqwest::Client::new()
        .get(&url)
        .header("Accept", "text/event-stream")
        .send()
        .await
        .expect("SSE connect")
}

/// Fetch the Prometheus `/metrics` endpoint and return the numeric value of
/// the given counter/gauge sample. Prometheus doesn't emit counters that
/// have never been incremented, so a missing metric maps to `0.0` — that's
/// the value tests want anyway (comparing "before" against "after").
pub async fn fetch_metric(service_url: &str, name: &str) -> f64 {
    let url = format!("{service_url}/metrics");
    let body = reqwest::get(&url)
        .await
        .expect("GET /metrics")
        .text()
        .await
        .expect("read /metrics body");
    for line in body.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        // Format: `metric_name [labels] <value>` — we assume no labels, which
        // is true for every counter/gauge Metrics::install builds.
        if let Some(rest) = line.strip_prefix(name) {
            let trimmed = rest.trim_start();
            if let Some(value_str) = trimmed.split_whitespace().next() {
                return value_str.parse().unwrap_or_else(|_| {
                    panic!("metric {name}: could not parse value from line: {line:?}")
                });
            }
        }
    }
    0.0
}

/// Parsed `balance_update` payload — just the address→amount map.
#[derive(Debug, Clone)]
pub struct BalanceUpdate {
    pub balances: HashMap<Address, U256>,
}

/// Turn a successful SSE response into a stream of `balance_update` events.
/// Panics if the response status isn't 2xx — tests that expect 404 should
/// use [`get_sse`] directly and inspect the status.
pub fn sse_stream(response: reqwest::Response) -> impl futures::Stream<Item = BalanceUpdate> {
    use eventsource_stream::Eventsource;
    use futures::StreamExt;

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
            let mut balances = HashMap::new();
            for (k, v) in balances_obj {
                let addr: Address = k.parse().ok()?;
                let amount = U256::from_str_radix(v.as_str()?, 10).ok()?;
                balances.insert(addr, amount);
            }
            Some(BalanceUpdate { balances })
        })
}

/// Convenience: post + open SSE + return only successful stream (asserts 2xx).
pub async fn open_sse(
    service_url: &str,
    owner: Address,
    client_id: &str,
) -> impl futures::Stream<Item = BalanceUpdate> {
    let resp = get_sse(service_url, owner, client_id).await;
    sse_stream(resp)
}
