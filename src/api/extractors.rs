//! Request extractor
//!
//! Currently exposes [`ChainId`], a typed-extractor that validates the
//! `{chain_id}` path segment against the chain this instance serves.

use crate::app_error::AppError;
use crate::app_state::AppState;
use crate::domain::EvmNetwork;
use axum::extract::{FromRequestParts, RawPathParams};
use axum::http::request::Parts;
use std::sync::Arc;

/// Pulls `{chain_id}` from the request path and validates it against
/// [`AppState::network`].
///
/// Handlers consume it as `ChainId(network): ChainId` — the extractor rejects mismatched requests with
/// `404 Not Found` before the handler body runs.
pub struct ChainId(pub EvmNetwork);

impl FromRequestParts<Arc<AppState>> for ChainId {
    type Rejection = AppError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let params = RawPathParams::from_request_parts(parts, state)
            .await
            .map_err(|_| AppError::BadRequest("missing path parameters".into()))?;

        // extract chain_id from params
        let raw = params
            .iter()
            .find_map(|(name, value)| (name == "chain_id").then_some(value))
            .ok_or_else(|| AppError::BadRequest("missing chain_id path parameter".into()))?;

        let chain: EvmNetwork = raw
            .parse()
            .map_err(|_| AppError::BadRequest(format!("invalid chain_id: {raw}")))?;

        // check that the current instance handle this chain_id, if it doesn't - 404
        if chain != state.network {
            return Err(AppError::NotFound(format!(
                "chain_id {chain} is not served by this instance (configured: {})",
                state.network
            )));
        }

        Ok(ChainId(chain))
    }
}
