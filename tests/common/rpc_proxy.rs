//! Programmable JSON-RPC fault-injection proxy for the HTTP leg of the
//! service. Anvil always answers honestly and fast, so degraded-provider
//! scenarios — the 2026-07-29 Polygon incident class — cannot be produced
//! by the node itself. This proxy sits between the service and anvil and
//! selectively degrades `eth_getLogs` while passing every other method
//! through untouched:
//!
//! - **delay** — each successful `eth_getLogs` sleeps first, emulating a
//!   provider slower than the chain's block time (the incident profile);
//! - **range cap** — ranges wider than N blocks are rejected with a
//!   provider-style "query exceeds max results" error (reth/Alchemy
//!   class), exercising the dispatcher's bisect;
//! - **poisoned block** — any range containing a given block is rejected,
//!   exercising the single-block-loss path.
//!
//! Rejections short-circuit before the delay (a provider computes "too
//! big" cheaply), and every observed / rejected range is recorded so tests
//! can assert that the drain actually produced ranged queries.
//!
//! The WS `newHeads` leg is NOT proxied — tests wire it straight to anvil,
//! so heads keep flowing while HTTP degrades, exactly like production
//! (separate connections through the rpc-proxy).

use axum::body::Bytes;
use axum::http::header::CONTENT_TYPE;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::{CancellationToken, DropGuard};

#[derive(Default)]
struct Rules {
    get_logs_delay: Option<Duration>,
    max_get_logs_blocks: Option<u64>,
    reject_range_containing: Option<u64>,
    seen_get_logs_ranges: Vec<(u64, u64)>,
    rejected_get_logs_ranges: Vec<(u64, u64)>,
}

/// Handle to the running proxy. Dropping it shuts the listener down.
pub struct RpcProxy {
    /// Base URL to hand the service as `rpc_http_url`.
    pub url: String,
    rules: Arc<Mutex<Rules>>,
    _shutdown: DropGuard,
}

impl RpcProxy {
    /// Bind on `127.0.0.1:0` and forward everything to `upstream` (anvil's
    /// HTTP endpoint), subject to the currently configured rules.
    pub async fn spawn(upstream: String) -> Self {
        let rules: Arc<Mutex<Rules>> = Arc::default();
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind rpc proxy");
        let addr = listener.local_addr().expect("rpc proxy local_addr");
        let cancel = CancellationToken::new();
        let client = reqwest::Client::new();

        let router = Router::new().route(
            "/",
            post({
                let rules = Arc::clone(&rules);
                move |body: Bytes| {
                    handle(Arc::clone(&rules), client.clone(), upstream.clone(), body)
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

        Self {
            url: format!("http://{addr}"),
            rules,
            _shutdown: cancel.drop_guard(),
        }
    }

    /// Sleep this long before answering each (non-rejected) `eth_getLogs`.
    /// `None` restores full speed.
    pub fn set_get_logs_delay(&self, delay: Option<Duration>) {
        self.rules.lock().unwrap().get_logs_delay = delay;
    }

    /// Reject `eth_getLogs` ranges wider than `max` blocks with a
    /// provider-style "query exceeds max results" error. `None` lifts the cap.
    pub fn set_max_get_logs_blocks(&self, max: Option<u64>) {
        self.rules.lock().unwrap().max_get_logs_blocks = max;
    }

    /// Reject every `eth_getLogs` whose range contains `block`. `None`
    /// heals the block.
    pub fn set_reject_range_containing(&self, block: Option<u64>) {
        self.rules.lock().unwrap().reject_range_containing = block;
    }

    /// Every `eth_getLogs` `(from, to)` observed so far, in arrival order.
    pub fn get_logs_ranges(&self) -> Vec<(u64, u64)> {
        self.rules.lock().unwrap().seen_get_logs_ranges.clone()
    }

    /// The subset of ranges that were rejected by the configured rules.
    pub fn rejected_get_logs_ranges(&self) -> Vec<(u64, u64)> {
        self.rules.lock().unwrap().rejected_get_logs_ranges.clone()
    }
}

async fn handle(
    rules: Arc<Mutex<Rules>>,
    client: reqwest::Client,
    upstream: String,
    body: Bytes,
) -> Response {
    let request: Option<Value> = serde_json::from_slice(&body).ok();
    let id = request
        .as_ref()
        .and_then(|request| request.get("id"))
        .cloned()
        .unwrap_or(Value::Null);

    if let Some((from, to)) = request.as_ref().and_then(parse_get_logs_range) {
        let (reject, delay) = {
            let mut rules = rules.lock().unwrap();
            rules.seen_get_logs_ranges.push((from, to));
            let too_wide = rules
                .max_get_logs_blocks
                .is_some_and(|max| to - from + 1 > max);
            let poisoned = rules
                .reject_range_containing
                .is_some_and(|block| (from..=to).contains(&block));
            if too_wide || poisoned {
                rules.rejected_get_logs_ranges.push((from, to));
            }
            (too_wide || poisoned, rules.get_logs_delay)
        };

        if reject {
            return Json(json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32602, "message": "query exceeds max results 20000" }
            }))
            .into_response();
        }
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await;
        }
    }

    let forwarded = client
        .post(&upstream)
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await;
    match forwarded {
        Ok(response) => match response.bytes().await {
            Ok(bytes) => ([(CONTENT_TYPE, "application/json")], bytes).into_response(),
            Err(err) => upstream_error(id, err).into_response(),
        },
        Err(err) => upstream_error(id, err).into_response(),
    }
}

/// Transport-style JSON-RPC error: the message deliberately avoids the
/// service's "query too big" patterns so it stays retryable.
fn upstream_error(id: Value, err: reqwest::Error) -> Json<Value> {
    Json(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": -32000, "message": format!("proxy: upstream unreachable: {err}") }
    }))
}

/// `Some((from, to))` iff the request is an `eth_getLogs` with a concrete
/// numeric block range (the only shape the service sends).
fn parse_get_logs_range(request: &Value) -> Option<(u64, u64)> {
    if request.get("method")?.as_str()? != "eth_getLogs" {
        return None;
    }
    let filter = request.get("params")?.get(0)?;
    let from = hex_block_number(filter.get("fromBlock")?)?;
    let to = hex_block_number(filter.get("toBlock")?)?;
    Some((from, to))
}

fn hex_block_number(value: &Value) -> Option<u64> {
    u64::from_str_radix(value.as_str()?.strip_prefix("0x")?, 16).ok()
}
