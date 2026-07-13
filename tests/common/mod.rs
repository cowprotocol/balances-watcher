//! Integration-test harness for balances-watcher.
//!
//! Split into three modules:
//!
//! - [`onchain`] — anvil bootstrap, canonical Multicall3 / WETH9 bytecode
//!   installation, WETH9 ABI wrapper.
//! - [`api`] — in-process token-list HTTP server, POST/SSE helpers, `/metrics`
//!   scraper, SSE event parser.
//! - [`env`] — the [`Env`] handle that stitches everything together.
//!
//! Tests carry `#[ignore]` so `cargo test` on a checkout with no `anvil` on
//! PATH stays green; run the suite explicitly with
//! `cargo test --test integration -- --ignored --test-threads=1` (or
//! `--test session_lifecycle` for the TTL-focused suite).

#![allow(dead_code, unused_imports)]

pub mod api;
pub mod env;
pub mod onchain;

pub use api::{
    fetch_metric, get_sse, open_sse, post_session, put_session, sse_stream,
    start_token_list_server, wait_for, BalanceUpdate,
};
pub use env::Env;
pub use onchain::{Weth9, CUSTOM_TOKEN_ADDRESS, MULTICALL3_ADDRESS, WETH9_ADDRESS};
