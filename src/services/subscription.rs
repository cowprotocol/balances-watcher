use crate::config::constants::BROADCAST_CHANNEL_CAPACITY;
use crate::domain::{BalanceEvent, Session};
use crate::services::fetch_balances_via_multicall::BalancesWithBlock;
use crate::services::subscription_manager::{Balance, BalanceSnapshot};
use alloy::primitives::Address;
use metrics::counter;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::broadcast::Receiver;
use tokio::sync::{broadcast, RwLock};
use tokio_util::sync::CancellationToken;

pub struct Subscription {
    balances_snapshot: RwLock<BalanceSnapshot>,
    sender: broadcast::Sender<BalanceEvent>,
    tokens: RwLock<HashSet<Address>>,
    watchers_spawned: AtomicBool,
    cancel_token: CancellationToken,
}

impl Subscription {
    pub fn new(tokens: HashSet<Address>) -> Self {
        let (sender, _) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
        Self {
            balances_snapshot: RwLock::new(HashMap::new()),
            sender,
            cancel_token: CancellationToken::new(),
            tokens: RwLock::new(tokens),
            watchers_spawned: AtomicBool::new(false),
        }
    }

    pub fn cancellable(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    pub fn try_mark_watchers_spawned(&self) -> bool {
        self.watchers_spawned
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub async fn watched_tokens(&self) -> HashSet<Address> {
        self.tokens.read().await.clone()
    }

    pub async fn extend_tokens(&self, tokens: HashSet<Address>) -> usize {
        let mut watched_tokens = self.tokens.write().await;
        watched_tokens.extend(tokens);
        watched_tokens.len()
    }

    pub fn send_event(&self, event: BalanceEvent, session: Session) {
        let _ = self
            .sender
            .send(event)
            .inspect(|receivers| {
                counter!("balance_updates_sent_total").increment(1);
                tracing::info!(
                    session = %session,
                    receivers,
                    "balance update sent"
                );
            })
            .inspect_err(|err| {
                tracing::error!(
                    error = %err,
                    session = %session,
                    "failed to send balance update event: no receivers"
                );
            });
    }

    pub fn subscribe(&self) -> Receiver<BalanceEvent> {
        self.sender.subscribe()
    }

    // update snapshot with new balances
    // first compare block_number, if it is bigger than in snapshot - update it
    // if the balance is different - put it in diff
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

        for (address, new_balance) in new_balances {
            let current_balance = snapshot.get_mut(&address);
            if let Some(current_balance) = current_balance {
                if current_balance.block_number < block_number {
                    if current_balance.amount != new_balance {
                        diff.insert(address, new_balance.to_string());
                    }

                    *current_balance = Balance {
                        amount: new_balance,
                        block_number,
                    };
                }
            } else {
                diff.insert(address, new_balance.to_string());
                snapshot.insert(
                    address,
                    Balance {
                        amount: new_balance,
                        block_number,
                    },
                );
            }
        }

        diff
    }

    pub async fn current_snapshot(&self) -> BalanceSnapshot {
        self.balances_snapshot.read().await.clone()
    }
}
