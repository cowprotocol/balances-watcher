use crate::domain::{BalanceEvent, Session};
use crate::metrics::Metrics;
use crate::services::errors::SubscriptionError;
use crate::services::subscription::Subscription;
use alloy::primitives::{Address, BlockNumber, U256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

struct SubWithCounter {
    pub clients: u32,
    pub subscription: Arc<Subscription>,
    pub idle_since: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct Balance {
    pub amount: U256,
    pub block_number: BlockNumber,
}

pub type BalanceSnapshot = HashMap<Address, Balance>;

pub struct SubscriptionManager {
    subscriptions: RwLock<HashMap<Session, SubWithCounter>>,
    task_tracker: TaskTracker,
    shutdown_token: CancellationToken,
    metrics: Arc<Metrics>,
}

const SESSION_TTL: Duration = Duration::from_secs(5);

impl SubscriptionManager {
    pub fn new(
        task_tracker: TaskTracker,
        shutdown_token: CancellationToken,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            task_tracker,
            shutdown_token,
            metrics,
        }
    }

    // create or update subscriptions clients count and watched token list
    pub async fn upsert(&self, session: Session, tokens: HashSet<Address>) -> Arc<Subscription> {
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

            return sub;
        }

        let shutdown_token = self.shutdown_token.clone();
        let subscription = Arc::new(Subscription::new(
            tokens,
            shutdown_token.child_token(),
            Arc::clone(&self.metrics),
        ));

        let sub_with_counter = SubWithCounter {
            clients: 0,
            subscription: Arc::clone(&subscription),
            idle_since: Some(Instant::now()),
        };

        self.subscriptions
            .write()
            .await
            .insert(session, sub_with_counter);

        self.metrics.sessions_created_total.increment(1);
        self.metrics.active_sessions.increment(1);
        tracing::info!(
            tokens_len = %tokens_len,
            session = %session,
            "session is created"
        );

        subscription
    }

    pub async fn get_subscription(&self, session: Session) -> Option<Arc<Subscription>> {
        let subs = self.subscriptions.read().await;
        subs.get(&session).map(|sub| Arc::clone(&sub.subscription))
    }

    pub async fn subscribe(
        &self,
        session: Session,
    ) -> Result<(broadcast::Receiver<BalanceEvent>, Arc<Subscription>), SubscriptionError> {
        let mut subs = self.subscriptions.write().await;

        if let Some(existing) = subs.get_mut(&session) {
            existing.clients = existing
                .clients
                .checked_add(1)
                .ok_or(SubscriptionError::TooManySubscriptions)?;
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

        Err(SubscriptionError::ThereArentCreatedSubscriptions)
    }

    // true - if it was the last client
    pub async fn unsubscribe(&self, session: &Session) -> Result<bool, SubscriptionError> {
        let mut subs = self.subscriptions.write().await;

        if let Some(existing) = subs.get_mut(session) {
            existing.clients = existing
                .clients
                .checked_sub(1)
                .ok_or(SubscriptionError::ThereArentCreatedSubscriptions)?;

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

        Err(SubscriptionError::ThereArentCreatedSubscriptions)
    }

    pub fn spawn_cleanup(self: Arc<Self>) {
        Arc::clone(&self).task_tracker.spawn(async move {
            let mut interval = tokio::time::interval(SESSION_TTL);
            loop {
                tokio::select! {
                    _ = self.shutdown_token.cancelled() => {
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
