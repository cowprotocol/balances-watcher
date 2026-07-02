//! Per-session shared state: watched-token set, balance snapshot, cancel
//! token, SSE broadcast fan-out, snapshot-refresh signal.
//!
//! One [`Subscription`] is created per `(owner, network)` pair and shared by
//! every component that touches that session — watchers, SSE handlers,
//! manager cleanup. All mutable state goes through `RwLock` / `AtomicBool` /
//! `Notify`, so the struct is held by `Arc<Subscription>` everywhere.
//!
//! ```text
//!         producers                     consumers
//!     ┌──────────────────┐          ┌─────────────────────┐
//!     │ set_watched…     │──►       │ is_watched          │
//!     │ update_balances… │──►       │ current_snapshot    │
//!     │ emit_refresh     │──Notify─►│ snapshot_updater    │
//!     │ send_event       │─broadcast│ SSE clients         │
//!     │ cancel           │─token───►│ all worker tasks    │
//!     └──────────────────┘          └─────────────────────┘
//! ```

use crate::config::constants::BROADCAST_CHANNEL_CAPACITY;
use crate::domain::{BalanceEvent, Session};
use crate::metrics::Metrics;
use crate::services::rpc_client::BalancesWithBlock;
use crate::services::subscription_manager::{Balance, BalanceSnapshot};
use alloy::primitives::Address;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, Notify, RwLock};
use tokio_util::sync::CancellationToken;

/// Per-session shared state. Always held by `Arc<Subscription>`.
pub struct Subscription {
    balances_snapshot: RwLock<BalanceSnapshot>,
    sender: broadcast::Sender<BalanceEvent>,
    tokens: RwLock<HashSet<Address>>,
    cancellation_token: CancellationToken,
    snapshot_refresh_notify: Arc<Notify>,
    metrics: Arc<Metrics>,
}

impl Subscription {
    /// Create a fresh subscription pre-seeded with `tokens`. `cancellation_token`
    /// is owned by `SubscriptionManager`; cancelling it tears down every
    /// watcher spawned for this session.
    pub fn new(
        tokens: HashSet<Address>,
        cancellation_token: CancellationToken,
        metrics: Arc<Metrics>,
    ) -> Self {
        let (sender, _) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
        Self {
            balances_snapshot: RwLock::new(HashMap::new()),
            cancellation_token,
            tokens: RwLock::new(tokens),
            // wakes the snapshot updater on any of: cold-start (first
            // BlockWatcher connect), BlockWatcher reconnect, watched-token
            // list change.
            snapshot_refresh_notify: Arc::new(Notify::new()),
            sender,
            metrics,
        }
    }

    /// Signal that watchers should refresh the full balance snapshot.
    ///
    /// Backed by `Notify::notify_one`, so bursty calls (e.g. all WS log
    /// listeners resubscribing in lockstep after a node restart) collapse
    /// into at most one extra wake of the snapshot updater.
    pub fn emit_balance_snapshot_refresh(&self) {
        self.snapshot_refresh_notify.notify_one();
    }

    /// Get a shared handle to the notifier that fires on
    /// [`Self::emit_balance_snapshot_refresh`]. The receiver `.await`s on
    /// `.notified()`.
    pub fn snapshot_refresh_notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.snapshot_refresh_notify)
    }

    /// Cloned token that watchers `.cancelled().await` on.
    pub fn cancellable(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// Snapshot copy of the watched set.
    pub async fn clone_watched_tokens(&self) -> HashSet<Address> {
        self.tokens.read().await.clone()
    }

    /// Replace the watched set with `new_tokens`. Evicts any cached balances
    /// for tokens that drop out so SSE clients stop seeing stale entries.
    ///
    /// Returns `true` if the watched set actually changed, `false` if
    /// `new_tokens` was identical to the current set (no-op fast path that
    /// lets the caller skip a forced snapshot refresh).
    pub async fn set_watched_tokens(&self, new_tokens: HashSet<Address>) -> bool {
        let mut watched_tokens = self.tokens.write().await;
        if *watched_tokens == new_tokens {
            return false;
        }
        *watched_tokens = new_tokens;
        // Lock ordering: tokens (write) -> snapshot (write). Hold both at
        // once so a concurrent reader can never observe a watched set that
        // has been swapped while the snapshot still carries ghost entries.
        let mut snapshot = self.balances_snapshot.write().await;
        snapshot.retain(|token, _| watched_tokens.contains(token));
        true
    }

    /// Fan out a balance event to all SSE subscribers. Disconnected clients
    /// are logged at debug and never escalate to error.
    pub fn send_event(&self, event: BalanceEvent, session: Session) {
        match self.sender.send(event) {
            Ok(receivers) => {
                self.metrics.balance_updates_sent_total.increment(1);
                tracing::debug!(
                    session = %session,
                    receivers,
                    "balance update sent"
                );
            }
            Err(err) => {
                tracing::debug!(
                    error = %err,
                    session = %session,
                    "failed to send balance update event: no receivers"
                );
            }
        }
    }

    pub async fn is_watched_token(&self, address: &Address) -> bool {
        self.tokens.read().await.contains(address)
    }

    /// Attach a new SSE client to the broadcast stream.
    pub fn subscribe(&self) -> broadcast::Receiver<BalanceEvent> {
        self.sender.subscribe()
    }

    /// Merge `new_balances` (read at `block_number`) into the snapshot and
    /// return the diff for clients.
    ///
    /// Block-guarded: a per-token update only applies if `block_number` is
    /// strictly higher than what's currently stored — protects against an
    /// in-flight stale snapshot overwriting fresher per-event updates.
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

    /// Cloned full snapshot. Used by the SSE handler to seed a new client
    /// with the initial state before switching it to broadcast diffs.
    pub async fn current_snapshot(&self) -> BalanceSnapshot {
        self.balances_snapshot.read().await.clone()
    }
}
