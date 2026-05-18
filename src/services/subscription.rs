use crate::config::constants::BROADCAST_CHANNEL_CAPACITY;
use crate::domain::{BalanceEvent, Session};
use crate::services::balance_fetcher::BalancesWithBlock;
use crate::services::subscription_manager::{Balance, BalanceSnapshot};
use alloy::primitives::Address;
use metrics::counter;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, Notify, RwLock};
use tokio_util::sync::CancellationToken;

pub struct Subscription {
    balances_snapshot: RwLock<BalanceSnapshot>,
    sender: broadcast::Sender<BalanceEvent>,
    tokens: RwLock<HashSet<Address>>,
    watchers_spawned: AtomicBool,
    cancellation_token: CancellationToken,
    sync_notify: Arc<Notify>,
}

impl Subscription {
    pub fn new(tokens: HashSet<Address>, cancellation_token: CancellationToken) -> Self {
        let (sender, _) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
        Self {
            // snapshot of all watched tokens
            balances_snapshot: RwLock::new(HashMap::new()),
            // signal to cancel all watchers for this sub
            cancellation_token,
            // watched tokens
            tokens: RwLock::new(tokens),
            // flag to detect if the watcher was spawned for this subscription
            watchers_spawned: AtomicBool::new(false),
            // notifier to force balance snapshot update (if watched token list was updated)
            sync_notify: Arc::new(Notify::new()),
            // send events to clients
            sender,
        }
    }

    pub fn sync_balance(&self) {
        self.sync_notify.notify_one();
    }

    pub fn take_sync_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.sync_notify)
    }

    pub fn cancellable(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    // check the flag that watcher was spawned and switched it if it wasn't
    // return true if the flag was switched
    pub fn try_mark_watchers_spawned(&self) -> bool {
        self.watchers_spawned
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub async fn clone_watched_tokens(&self) -> HashSet<Address> {
        self.tokens.read().await.clone()
    }

    // update watched token list
    pub async fn extend_tokens(&self, tokens: HashSet<Address>) -> usize {
        let mut watched_tokens = self.tokens.write().await;
        watched_tokens.extend(tokens);
        watched_tokens.len()
    }

    pub async fn is_watched(&self, token: &Address) -> bool {
        self.tokens.read().await.contains(token)
    }

    pub fn send_event(&self, event: BalanceEvent, session: Session) {
        match self.sender.send(event) {
            Ok(receivers) => {
                counter!("balance_updates_sent_total").increment(1);
                tracing::info!(
                    session = %session,
                    receivers,
                    "balance update sent"
                );
            }
            Err(err) => {
                // no need to log it as error because clients could close their connection
                // before accepting events and it just spam with errors
                tracing::debug!(
                    error = %err,
                    session = %session,
                    "failed to send balance update event: no receivers"
                );
            }
        }
    }

    // subscribe new client to events
    pub fn subscribe(&self) -> broadcast::Receiver<BalanceEvent> {
        self.sender.subscribe()
    }

    // update a snapshot with new balances
    // first compare block_number, if it is bigger than in the snapshot - update it
    // if the balance is different - put it in diff (only if the block_number the same or bigger)
    // return diff
    pub async fn update_balances_and_take_diff(
        &self,
        (new_balances, block_number): BalancesWithBlock,
    ) -> HashMap<Address, String> {
        let mut diff: HashMap<Address, String> = HashMap::new();
        if new_balances.is_empty() {
            tracing::warn!("balances is empty, nothing to update");
            return diff;
        }

        let mut snapshot = self.balances_snapshot.write().await;

        for (token, new_balance) in new_balances {
            let current_balance = snapshot.get_mut(&token);
            if let Some(current_balance) = current_balance {
                // first check the block_number it should be higher than in the snapshot
                if current_balance.block_number < block_number {
                    // if balance is different - add it to diff and update
                    if current_balance.amount != new_balance {
                        diff.insert(token, new_balance.to_string());
                    }

                    *current_balance = Balance {
                        amount: new_balance,
                        block_number,
                    };
                }
            } else {
                // if the token is not in the snapshot -> add it to diff and update the snapshot
                diff.insert(token, new_balance.to_string());
                snapshot.insert(
                    token,
                    Balance {
                        amount: new_balance,
                        block_number,
                    },
                );
            }
        }

        diff
    }

    // return the cloned snapshot
    pub async fn current_snapshot(&self) -> BalanceSnapshot {
        self.balances_snapshot.read().await.clone()
    }
}
