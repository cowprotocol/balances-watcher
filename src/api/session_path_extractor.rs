//! Path extractor for session-scoped routes.
//!
//! Parses `{chain_id}` and `{owner}` in one pass and rejects requests targeted
//! at a chain this instance doesn't serve with `404 Not Found`, before the
//! handler body runs.

use crate::app_error::AppError;
use crate::app_state::AppState;
use crate::domain::EvmNetwork;
use alloy::primitives::Address;
use axum::extract::{FromRequestParts, Path};
use axum::http::request::Parts;
use std::sync::Arc;

/// Parsed `(chain_id, owner)` pair, chain validated against
/// [`AppState::network`]. Handlers consume it as
/// `SessionPath(network, owner): SessionPath`.
pub struct SessionPath(pub EvmNetwork, pub Address);

impl FromRequestParts<Arc<AppState>> for SessionPath {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let Path((network, owner)) =
            Path::<(EvmNetwork, Address)>::from_request_parts(parts, state)
                .await
                .map_err(|e| AppError::BadRequest(format!("invalid path: {e}")))?;

        if network != state.network {
            return Err(AppError::NotFound(format!(
                "chain_id {network} is not served by this instance (configured: {})",
                state.network
            )));
        }

        Ok(SessionPath(network, owner))
    }
}
