use crate::domain::EvmNetwork;
use alloy::primitives::Address;
use std::fmt::Display;
use uuid::Uuid;

/// Primary key of a live watcher in [`crate::services::subscription_manager`].
///
/// A session is scoped to a **single caller device** rather than a wallet:
/// two browsers, two devices, or an incognito tab open independent sessions
/// on the same `(network, owner)` because their `client_id`s differ. Within a
/// device the `client_id` is stable (per-origin `localStorage` in the browser,
/// per-process in CLIs), so extra tabs on the same origin reuse the session.
///
/// Consequences worth knowing:
/// - Each session runs its own snapshot updater / refresh queue — one `Transfer`
///   for the same owner fans out into N per-session multicalls (see
///   [`crate::services::session_manager::SessionManager::spawn_erc20_transfer_listener`]).
/// - `POST` / `PUT` from one `client_id` cannot touch another `client_id`'s
///   watched-token list; that isolation is the reason `client_id` was added.
/// - `client_id` is required on every session-facing endpoint (POST/PUT header,
///   SSE query). Missing/invalid → 400.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Copy)]
pub struct Session {
    pub owner: Address,
    pub network: EvmNetwork,
    pub client_id: Uuid,
}

impl Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.owner, self.network)
    }
}
