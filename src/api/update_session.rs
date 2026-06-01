use std::sync::Arc;

use alloy::primitives::Address;
use axum::{
    extract::{Path, State},
    Json,
};
use serde::Deserialize;

use crate::api::extractors::ChainId;
use crate::services::session_manager::SessionContext;
use crate::{
    app_error::AppError,
    app_state::AppState,
    domain::{EvmNetwork, Session},
};

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSessionRequest {
    #[serde(default)]
    tokens_lists_urls: Vec<String>,

    #[serde(default)]
    custom_tokens: Vec<Address>,
}

/// `PUT /{chain_id}/sessions/{owner}`
///
/// Adds more tokens to an existing session on `chain_id` for `owner` (token
/// lists and/or custom token addresses). The session must already have been
/// created via `POST /{chain_id}/sessions/{owner}`.
///
/// Chain mismatch is rejected with `404 Not Found` by the `ChainId` extractor.
pub async fn update_session(
    ChainId(network): ChainId,
    Path((_, owner)): Path<(EvmNetwork, Address)>,
    State(state): State<Arc<AppState>>,
    Json(body): Json<UpdateSessionRequest>,
) -> Result<(), AppError> {
    if body.custom_tokens.is_empty() && body.tokens_lists_urls.is_empty() {
        return Err(AppError::BadRequest(
            "tokens_lists_urls && custom_tokens are empty".to_string(),
        ));
    }

    let ctx = SessionContext {
        session: Session { network, owner },
        tokens_lists_urls: body.tokens_lists_urls,
        custom_tokens: body.custom_tokens,
    };

    state
        .session_manager
        .upsert(ctx)
        .await
        .map_err(AppError::from)
}
