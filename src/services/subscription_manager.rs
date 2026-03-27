use crate::domain::{BalanceEvent, Session};
use crate::services::errors::SubscriptionError;
use crate::services::subscription::Subscription;
use alloy::primitives::{Address, U256};
use dashmap::DashMap;
use metrics::{counter, gauge};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

struct SubWithCounter {
    pub clients: u32,
    pub subscription: Arc<Subscription>,
    pub idle_since: Option<Instant>,
}

#[derive(Debug, Clone)]
pub struct Balance {
    pub amount: U256,
    pub block_number: U256,
}

pub type BalanceSnapshot = HashMap<Address, Balance>;

pub struct SubscriptionManager {
    subscriptions: DashMap<Session, SubWithCounter>,
}

const SESSION_TTL: Duration = Duration::from_secs(5);

impl SubscriptionManager {
    pub fn new() -> Self {
        Self {
            subscriptions: DashMap::new(),
        }
    }

    // create or update subscriptions clients count and watched token list
    pub async fn upsert(&self, session: Session, tokens: HashSet<Address>) -> Arc<Subscription> {
        let new_tokens_len = tokens.len();
        let maybe_sub = self
            .subscriptions
            .get(&session)
            .map(|sub| Arc::clone(&sub.subscription));
        if let Some(sub) = maybe_sub {
            let watched_tokens_len = sub.extend_tokens(tokens).await;

            counter!("sessions_updated_total").increment(1);
            tracing::info!(
                session = %session,
                tokens_len = watched_tokens_len,
                "session is updated"
            );

            if new_tokens_len > 0 {
                // if there are new tokens -> we should immediately make multicall
                // to update a balance snapshot for the current subscription
                sub.sync_balance();
            }

            return Arc::clone(&sub);
        }

        let tokens_len = tokens.len();
        let subscription = Arc::new(Subscription::new(tokens));

        let sub_with_counter = SubWithCounter {
            clients: 0,
            subscription: Arc::clone(&subscription),
            idle_since: Some(Instant::now()),
        };

        self.subscriptions.insert(session, sub_with_counter);

        counter!("sessions_created_total").increment(1);
        gauge!("active_sessions").increment(1);
        tracing::info!(
            tokens_len = %tokens_len,
            session = %session,
            "session is created"
        );

        subscription
    }

    pub async fn get_subscription(&self, session: Session) -> Option<Arc<Subscription>> {
        self.subscriptions
            .get(&session)
            .map(|sub| Arc::clone(&sub.subscription))
    }

    pub async fn subscribe(
        &self,
        session: Session,
    ) -> Result<(broadcast::Receiver<BalanceEvent>, Arc<Subscription>), SubscriptionError> {
        if let Some(mut existing) = self.subscriptions.get_mut(&session) {
            existing.clients = existing
                .clients
                .checked_add(1)
                .ok_or(SubscriptionError::TooManySubscriptions)?;
            existing.idle_since = None;
            let receiver = existing.subscription.subscribe();

            counter!("sse_connections_total").increment(1);
            gauge!("sse_connections_active").increment(1);
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
        if let Some(mut existing) = self.subscriptions.get_mut(session) {
            existing.clients = existing
                .clients
                .checked_sub(1)
                .ok_or(SubscriptionError::ThereArentCreatedSubscriptions)?;

            if existing.clients == 0 {
                existing.idle_since = Some(Instant::now());

                counter!("sessions_expired_total").increment(1);
                gauge!("sse_connections_active").decrement(1);
                tracing::info!(
                    session = %session,
                    "session expired"
                );

                return Ok(true);
            }

            gauge!("sse_connections_active").decrement(1);
            tracing::info!(
                session = %session,
                "sse connection is closed"
            );

            return Ok(false);
        }

        Err(SubscriptionError::ThereArentCreatedSubscriptions)
    }

    pub fn spawn_cleanup(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(SESSION_TTL);
            loop {
                interval.tick().await;
                self.cleanup_subs().await;
            }
        });
    }

    async fn cleanup_subs(&self) {
        let now = Instant::now();

        self.subscriptions.retain(|session, sub| {
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
                counter!("sessions_expired_total").increment(1);
                gauge!("active_sessions").decrement(1);
                tracing::info!(
                    session = %session,
                    "subscription cleanup"
                );
            }

            !should_remove
        })
    }
}
