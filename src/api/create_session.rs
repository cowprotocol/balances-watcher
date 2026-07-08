use alloy::primitives::Address;
use axum::{
    extract::{Path, State},
    Json,
};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Instant;
use utoipa::ToSchema;

use crate::api::chain_extractor::ChainId;
use crate::api::client_id_extractor::ClientId;
use crate::services::session_manager::SessionContext;
use crate::{
    app_error::AppError,
    app_state::AppState,
    domain::{EvmNetwork, Session},
};

#[derive(Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateSessionRequest {
    /// HTTPS URLs of token-list JSON files (Uniswap-style `tokens.json`).
    /// At least one of `tokensListsUrls` or `customTokens` must be non-empty.
    #[schema(example = json!(["https://tokens.coingecko.com/uniswap/all.json"]))]
    tokens_lists_urls: Vec<String>,

    /// Extra ERC20 addresses to watch beyond the token lists.
    #[serde(default)]
    #[schema(value_type = Vec<String>, example = json!(["0xdAC17F958D2ee523a2206206994597C13D831ec7"]))]
    custom_tokens: Vec<Address>,
}

#[utoipa::path(
    post,
    path = "/{chain_id}/sessions/{owner}",
    tag = "sessions",
    params(
        ("chain_id" = u64, Path, description = "EVM chain id; must match the instance's configured NETWORK", example = 1),
        ("owner"    = String, Path, description = "0x-prefixed owner address (20 bytes)", example = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"),
        ("X-Client-Id" = String, Header, description = "Required. UUID identifying the calling device/browser. Sessions are keyed by (chain_id, owner, client_id) — a distinct client_id gets its own isolated session and its own snapshot cycle for the same owner.", example = "550e8400-e29b-41d4-a716-446655440000"),
    ),
    request_body = CreateSessionRequest,
    responses(
        (status = 200, description = "Session created or watched list replaced if it already existed"),
        (status = 400, description = "Empty token lists, token limit exceeded, or missing/invalid X-Client-Id", body = crate::app_error::ErrorBody),
        (status = 404, description = "chain_id does not match this instance's NETWORK", body = crate::app_error::ErrorBody),
    ),
)]
pub async fn create_session(
    ChainId(network): ChainId,
    path: Path<(EvmNetwork, Address)>,
    ClientId(client_id): ClientId,
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<(), AppError> {
    let (_, owner) = path.0;
    if body.tokens_lists_urls.is_empty() && body.custom_tokens.is_empty() {
        return Err(AppError::BadRequest(
            "tokens_lists_urls or custom_tokens should not be empty both".into(),
        ));
    }

    let session = Session {
        client_id,
        network,
        owner,
    };
    let t0 = Instant::now();

    let ctx = SessionContext {
        session,
        tokens_lists_urls: body.tokens_lists_urls,
        custom_tokens: body.custom_tokens,
    };

    let result = Arc::clone(&state.session_manager)
        .upsert(ctx)
        .await
        .map_err(AppError::from);

    let elapsed = t0.elapsed().as_millis() as f64;
    state.metrics.create_session_duration_ms.record(elapsed);
    tracing::info!(session = %session, time_ms = elapsed, success = result.is_ok(), "handler: create_session END");
    result
}
