//! Server-side registry of live SSE sessions, keyed by [`Session`]
//! (`(network, owner, client_id)`). Owns every [`Subscription`] the process holds,
//! spawns its [`BalanceRefreshQueue`] worker on first creation, and
//! arbitrates between HTTP handlers and a background cleanup task.
//!
//! - [`upsert`](SubscriptionManager::upsert) — create a session (spawning a
//!   fresh refresh-queue worker and returning its result receiver to the
//!   caller), or PUT-replace its watched-token set via
//!   [`Subscription::set_watched_tokens`]. Forces a fresh multicall via
//!   [`Subscription::emit_balance_snapshot_refresh`] only when the set
//!   actually changed, so re-PUT'ing the same list is a no-op.
//! - [`subscribe`](SubscriptionManager::subscribe) — hand a
//!   `broadcast::Receiver` to an SSE handler and bump the per-session client
//!   counter.
//! - [`unsubscribe`](SubscriptionManager::unsubscribe) — decrement the
//!   counter; when it hits zero, stamp `idle_since` so cleanup can reap the
//!   session later.
//! - [`spawn_cleanup`](SubscriptionManager::spawn_cleanup) — background task
//!   ticking every `SESSION_TTL`. Drops sessions with zero clients idle past
//!   the TTL, cancelling their per-session token (which unwinds every
//!   snapshot updater worker in [`crate::services::snapshot_updater`] and,
//!   transitively, the refresh-queue worker once its last handle is gone). On
//!   shutdown,
//!   broadcasts a 503 close event to every client and clears the map.
//!
//! `SubWithCounter` is the per-session bookkeeping cell — `clients`,
//! `idle_since`, `Arc<Subscription>` — that the registry inspects to decide
//! "is anyone still listening?" without touching subscription internals.

use crate::config::constants::{MAX_CLIENTS_PER_OWNER, SESSION_TTL};
use crate::domain::{BalanceEvent, Session};
use crate::graceful_shutdown::LifeCycle;
use crate::metrics::Metrics;
use crate::services::balance_refresh_queue::{BalanceRefreshQueue, BalanceRefreshQueueHandle};
use crate::services::rpc_client::{BalancesWithBlock, RpcClient, RpcError};
use crate::services::subscription::Subscription;
use alloy::primitives::{Address, BlockNumber, U256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, mpsc, RwLock};

/// Errors surfaced by [`SubscriptionManager`].
#[derive(Debug, Clone, thiserror::Error)]
pub enum SubscriptionError {
    /// The lookup hit a session that was never created, was already cleaned
    /// up, or had its client counter underflow on `unsubscribe`. With
    /// `Session = (chain, owner, client_id)`, the mismatch is usually a
    /// `client_id` that doesn't match any session for this `(chain, owner)`.
    #[error("no session registered for this (chain, owner, client_id)")]
    SessionNotRegistered,

    /// A new `client_id` would push the count of active sessions for this
    /// `(chain, owner)` past [`crate::config::constants::MAX_CLIENTS_PER_OWNER`].
    #[error("too many active client_ids for this owner (limit: {limit})")]
    OwnerClientLimitExceeded { limit: usize },
}

struct SubWithCounter {
    pub clients: u32,
    pub subscription: Arc<Subscription>,
    pub idle_since: Option<Instant>,
    pub refresh_queue: BalanceRefreshQueueHandle,
}

#[derive(Debug, Clone)]
pub struct Balance {
    pub amount: U256,
    pub block_number: BlockNumber,
}

pub type BalanceSnapshot = HashMap<Address, Balance>;

pub struct SubscriptionManager {
    subscriptions: RwLock<HashMap<Session, SubWithCounter>>,
    metrics: Arc<Metrics>,
    rpc_client: Arc<RpcClient>,
    lifecycle: LifeCycle,
}

impl SubscriptionManager {
    pub fn new(metrics: Arc<Metrics>, rpc_client: Arc<RpcClient>, lifecycle: LifeCycle) -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            lifecycle,
            metrics,
            rpc_client,
        }
    }

    /// Create or update a session.
    ///
    /// On **create**: spawns a fresh [`BalanceRefreshQueue`] worker and returns
    /// `Some(result_rx)` so the caller can wire up its
    /// [`crate::services::snapshot_updater::SnapshotUpdater`].
    /// On **update** (the session already exists): replaces the watched-token
    /// set, optionally forces a snapshot refresh, and returns `None` in the
    /// second slot — the worker is already running.
    ///
    /// Fails with [`SubscriptionError::OwnerClientLimitExceeded`] when the
    /// caller is opening a **new** session and this `(chain, owner)` already
    /// hosts [`MAX_CLIENTS_PER_OWNER`] sessions. Updates to an existing
    /// `client_id` bypass the cap — a client already inside the map can
    /// keep rotating its watched-token list regardless of pressure.
    pub async fn upsert(
        &self,
        session: Session,
        tokens: HashSet<Address>,
    ) -> Result<
        (
            Arc<Subscription>,
            Option<mpsc::Receiver<Result<BalancesWithBlock, RpcError>>>,
        ),
        SubscriptionError,
    > {
        let tokens_len = tokens.len();
        let maybe_sub = {
            let subs = self.subscriptions.read().await;
            subs.get(&session).map(|sub| Arc::clone(&sub.subscription))
        };

        if let Some(sub) = maybe_sub {
            let changed = sub.set_watched_tokens(tokens).await;

            self.metrics.sessions_updated_total.increment(1);
            tracing::info!(
                session = %session,
                tokens_len,
                watched_tokens_changed = changed,
                "watched token list updated"
            );

            if changed {
                // PUT had a real effect — force a fresh multicall so balances
                // reflect the new set right away instead of waiting for the
                // next snapshot tick.
                sub.emit_balance_snapshot_refresh();
            }

            return Ok((sub, None));
        }

        let shutdown_token = self.lifecycle.cancel_token.clone();
        let subscription = Arc::new(Subscription::new(
            tokens,
            shutdown_token.child_token(),
            Arc::clone(&self.metrics),
        ));

        let (refresh_queue, result_rx) = BalanceRefreshQueue::new(
            self.lifecycle.task_tracker.clone(),
            session.owner,
            Arc::clone(&self.rpc_client),
        )
        .spawn();

        let sub_with_counter = SubWithCounter {
            clients: 0,
            subscription: Arc::clone(&subscription),
            idle_since: Some(Instant::now()),
            refresh_queue: refresh_queue.clone(),
        };

        // Cap check + insert must share one write lock — otherwise concurrent
        // POSTs with fresh UUIDs could each read a stale count and jointly
        // overshoot the limit.
        let mut subs = self.subscriptions.write().await;

        // LEGACY: full-map scan. Goes away with the process-wide-queue
        // migration once storage becomes `HashMap<Address, HashMap<Uuid, _>>`.
        let existing_for_owner = subs.keys().filter(|s| s.owner == session.owner).count();
        if existing_for_owner >= MAX_CLIENTS_PER_OWNER {
            self.metrics.owner_client_limit_exceeded_total.increment(1);
            tracing::warn!(
                owner = %session.owner,
                limit = MAX_CLIENTS_PER_OWNER,
                existing = existing_for_owner,
                "client request rejected: per-owner client limit exceeded",
            );
            return Err(SubscriptionError::OwnerClientLimitExceeded {
                limit: MAX_CLIENTS_PER_OWNER,
            });
        }
        subs.insert(session, sub_with_counter);
        let sessions_for_owner = (existing_for_owner + 1) as f64;
        drop(subs);
        self.metrics.sessions_per_owner.record(sessions_for_owner);

        self.metrics.sessions_created_total.increment(1);
        self.metrics.active_sessions.increment(1);
        tracing::info!(
            tokens_len = %tokens_len,
            session = %session,
            "session is created"
        );

        Ok((subscription, Some(result_rx)))
    }

    pub async fn subscribe(
        &self,
        session: Session,
    ) -> Result<(broadcast::Receiver<BalanceEvent>, Arc<Subscription>), SubscriptionError> {
        let mut subs = self.subscriptions.write().await;

        if let Some(existing) = subs.get_mut(&session) {
            // saturating_add — u32::MAX SSE-connections on one session is
            // unreachable in practice; better to silently cap than to panic
            // (debug) or wrap (release).
            existing.clients = existing.clients.saturating_add(1);
            existing.idle_since = None;
            let receiver = existing.subscription.subscribe();

            self.metrics.sse_connections_total.increment(1);
            self.metrics.sse_connections_active.increment(1);
            tracing::info!(
                session = %session,
                "sse connection created"
            );

            return Ok((receiver, Arc::clone(&existing.subscription)));
        }

        Err(SubscriptionError::SessionNotRegistered)
    }

    // For every session on this `owner` whose watched-token set contains
    // `token`, return the session's refresh-queue handle. Since sessions are
    // keyed by `(network, owner, client_id)`, one owner may map to N sessions
    // (one per device/tab) — each gets its own enqueue.
    //
    // Two-phase to avoid holding the outer read-lock across inner-lock awaits:
    //   1) under `read()`, cheaply collect (Subscription, handle) for the
    //      owner — no awaits between candidate rows;
    //   2) drop the lock, then do the `is_watched_token` await per candidate.
    // Otherwise every extra client_id on this owner would stretch the outer
    // read-lock hold time, starving POST/PUT of the write lock.
    pub async fn owned_queues_watching(
        &self,
        owner: &Address,
        token: &Address,
    ) -> Vec<BalanceRefreshQueueHandle> {
        let candidates: Vec<(Arc<Subscription>, BalanceRefreshQueueHandle)> = {
            let subs = self.subscriptions.read().await;
            subs.iter()
                .filter(|(session, _)| session.owner == *owner)
                .map(|(_, entry)| (Arc::clone(&entry.subscription), entry.refresh_queue.clone()))
                .collect()
        };

        let mut queues = Vec::new();
        for (sub, queue) in candidates {
            if sub.is_watched_token(token).await {
                queues.push(queue);
            }
        }
        queues
    }

    // true - if it was the last client
    pub async fn unsubscribe(&self, session: &Session) -> Result<bool, SubscriptionError> {
        let mut subs = self.subscriptions.write().await;

        if let Some(existing) = subs.get_mut(session) {
            existing.clients = existing
                .clients
                .checked_sub(1)
                .ok_or(SubscriptionError::SessionNotRegistered)?;

            if existing.clients == 0 {
                existing.idle_since = Some(Instant::now());

                self.metrics.sessions_expired_total.increment(1);
                self.metrics.sse_connections_active.decrement(1);
                tracing::info!(
                    session = %session,
                    "session expired"
                );

                return Ok(true);
            }

            self.metrics.sse_connections_active.decrement(1);
            tracing::info!(
                session = %session,
                "sse connection is closed"
            );

            return Ok(false);
        }

        Err(SubscriptionError::SessionNotRegistered)
    }

    pub fn spawn_cleanup(self: Arc<Self>) {
        Arc::clone(&self).lifecycle.task_tracker.spawn(async move {
            let mut interval = tokio::time::interval(SESSION_TTL);
            loop {
                tokio::select! {
                    _ = self.lifecycle.cancel_token.cancelled() => {
                        tracing::info!("shutdown cleanup_subs");
                        self.close_sse_connections().await;
                        break;
                    },
                    _ = interval.tick() => self.cleanup_subs().await
                }
            }
        });
    }

    // send 503 error to clients and close all sse connections
    async fn close_sse_connections(&self) {
        let mut subs = self.subscriptions.write().await;
        for (session, sub_with_counter) in subs.iter() {
            let close_event = BalanceEvent::Error {
                code: 503,
                message: "server is shutting down".into(),
            };
            sub_with_counter
                .subscription
                .send_event(close_event, session.to_owned());
        }
        // drain all subscriptions after
        subs.clear();
    }

    async fn cleanup_subs(&self) {
        let mut subs = self.subscriptions.write().await;

        let now = Instant::now();

        subs.retain(|session, sub| {
            let should_remove = if sub.clients == 0 {
                match sub.idle_since {
                    Some(idle_since) => now.duration_since(idle_since) > SESSION_TTL,
                    None => false,
                }
            } else {
                false
            };

            if should_remove {
                sub.subscription.cancellable().cancel();
                self.metrics.sessions_expired_total.increment(1);
                self.metrics.active_sessions.decrement(1);
                tracing::info!(
                    session = %session,
                    "subscription cleanup"
                );
            }

            !should_remove
        })
    }
}
