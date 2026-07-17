//! Regression tests for the create-session race.
//!
//! `SubscriptionManager::upsert` used to check session existence under a
//! read lock and insert under a separate write lock. Two concurrent POSTs
//! for the same `(chain, owner, client_id)` could both pass the read check;
//! the second insert then silently replaced the first registry entry, so
//! the first subscription's cancellation token became unreachable and its
//! watcher tasks leaked until process shutdown (both callers also received
//! a queue receiver, so both spawned snapshot pipelines).

mod common;

use alloy::primitives::Address;
use alloy::providers::{Provider, ProviderBuilder};
use balances_watcher::domain::{EvmNetwork, Session};
use balances_watcher::graceful_shutdown::LifeCycle;
use balances_watcher::metrics::Metrics;
use balances_watcher::services::rpc_client::RpcClient;
use balances_watcher::services::subscription_manager::SubscriptionManager;
use common::{get_sse, post_session, start_token_list_server, ServiceEnv};
use futures::future::join_all;
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

const CLIENT_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

async fn make_manager() -> Arc<SubscriptionManager> {
    let metrics = Arc::new(Metrics::install());
    // http provider is lazy — port 9 (discard) is never actually dialled
    let provider = ProviderBuilder::new()
        .connect("http://127.0.0.1:9")
        .await
        .expect("lazy http provider")
        .erased();
    let rpc_client = Arc::new(RpcClient::new(Arc::new(provider), Arc::clone(&metrics)));

    Arc::new(SubscriptionManager::new(
        metrics,
        rpc_client,
        LifeCycle::spawn(),
    ))
}

/// The core invariant: however many upserts for the same brand-new session
/// run concurrently, exactly one of them wins the create (gets
/// `Some(result_rx)`); the rest must take the update path (`None`).
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_upserts_create_exactly_one_subscription() {
    let manager = make_manager().await;

    // Repeat with fresh sessions to hunt the read-check → insert window;
    // the invariant must hold on every round regardless of interleaving.
    for round in 0..50 {
        let session = Session {
            owner: Address::random(),
            network: EvmNetwork::Eth,
            client_id: Uuid::from_u128(round),
        };
        let tokens: HashSet<Address> = [Address::random()].into_iter().collect();

        let calls = (0..8).map(|_| {
            let manager = Arc::clone(&manager);
            let tokens = tokens.clone();
            tokio::spawn(async move { manager.upsert(session, tokens).await })
        });

        // A caller "won the create" iff it got back a queue receiver
        // (`Some`); losers take the update path and get `None`. Panics or
        // upsert errors are genuine test failures — surface them loudly.
        let mut creates = 0;
        for join_result in join_all(calls).await {
            let upsert_result = join_result.expect("upsert task panicked");
            let (_subscription, maybe_result_rx) = upsert_result.expect("upsert returned an error");
            if maybe_result_rx.is_some() {
                creates += 1;
            }
        }

        assert_eq!(
            creates, 1,
            "round {round}: exactly one concurrent upsert must win the create"
        );
    }
}

/// Same race, driven through the real HTTP surface: a burst of identical
/// POSTs must all succeed and leave one attachable session behind.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_posts_with_same_client_id_yield_one_working_session() {
    let env = ServiceEnv::spawn(true).await;
    let owner = Address::random();
    let (list_url, _cancel) = start_token_list_server(Address::random()).await;

    let posts = (0..10).map(|_| post_session(&env.service_url, owner, CLIENT_ID, &list_url));
    for resp in join_all(posts).await {
        assert_eq!(resp.status(), 200);
    }

    // The surviving session must be attachable via SSE.
    let resp = get_sse(&env.service_url, owner, CLIENT_ID).await;
    assert_eq!(resp.status(), 200);
}
