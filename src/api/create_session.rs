use alloy::primitives::Address;
use axum::{
    extract::{Path, State},
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Instant;

use crate::api::extractors::ChainId;
use crate::services::session_manager::SessionContext;
use crate::{
    app_error::AppError,
    app_state::AppState,
    domain::{EvmNetwork, Session},
};

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CreateSessionRequest {
    tokens_lists_urls: Vec<String>,

    #[serde(default)]
    custom_tokens: Vec<Address>,
}

/// `POST /{chain_id}/sessions/{owner}`
///
/// creates a new watcher session for `owner` on `chain_id` and spawns the
/// underlying web3 listeners + snapshot updaters. Must be called before the
/// SSE stream is opened.
///
/// Chain mismatch is rejected with `404 Not Found` by the `ChainId` extractor.
pub async fn create_session(
    ChainId(network): ChainId,
    Path((_, owner)): Path<(EvmNetwork, Address)>,
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<(), AppError> {
    if body.tokens_lists_urls.is_empty() && body.custom_tokens.is_empty() {
        return Err(AppError::BadRequest(
            "tokens_lists_urls or custom_tokens should not be empty both".into(),
        ));
    }

    let session = Session { network, owner };
    let t0 = Instant::now();
    tracing::info!(session = %session, "handler: create_session START");

    let ctx = SessionContext {
        session,
        tokens_lists_urls: body.tokens_lists_urls,
        custom_tokens: body.custom_tokens,
    };

    let result = state
        .session_manager
        .upsert(ctx)
        .await
        .map_err(AppError::from);

    let elapsed = t0.elapsed().as_millis() as f64;
    state.metrics.create_session_duration_ms.record(elapsed);
    tracing::info!(session = %session, time_ms = elapsed, success = result.is_ok(), "handler: create_session END");
    result
}
