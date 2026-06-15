use std::sync::Arc;

use alloy::primitives::Address;
use axum::{
    extract::{Path, State},
    Json,
};
use serde::Deserialize;
use utoipa::ToSchema;

use crate::api::extractors::ChainId;
use crate::services::session_manager::SessionContext;
use crate::{
    app_error::AppError,
    app_state::AppState,
    domain::{EvmNetwork, Session},
};

#[derive(Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateSessionRequest {
    /// HTTPS URLs of token-list JSON files. At least one of
    /// `tokensListsUrls` or `customTokens` must be non-empty.
    #[serde(default)]
    #[schema(example = json!(["https://tokens.coingecko.com/uniswap/all.json"]))]
    tokens_lists_urls: Vec<String>,

    /// Extra ERC20 addresses to watch beyond the token lists.
    #[serde(default)]
    #[schema(value_type = Vec<String>, example = json!(["0xdAC17F958D2ee523a2206206994597C13D831ec7"]))]
    custom_tokens: Vec<Address>,
}

#[utoipa::path(
    put,
    path = "/{chain_id}/sessions/{owner}",
    tag = "sessions",
    params(
        ("chain_id" = u64, Path, description = "EVM chain id; must match the instance's configured NETWORK", example = 1),
        ("owner"    = String, Path, description = "0x-prefixed owner address (20 bytes)", example = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"),
    ),
    request_body = UpdateSessionRequest,
    responses(
        (status = 200, description = "Watched token list replaced (REPLACE semantics, not extend; tokens absent from the new list are evicted)"),
        (status = 400, description = "Empty body or new list exceeds token limit", body = crate::app_error::ErrorBody),
        (status = 404, description = "chain_id mismatch or session not created",   body = crate::app_error::ErrorBody),
    ),
)]
pub async fn update_session(
    ChainId(network): ChainId,
    path: Path<(EvmNetwork, Address)>,
    State(state): State<Arc<AppState>>,
    Json(body): Json<UpdateSessionRequest>,
) -> Result<(), AppError> {
    let (_, owner) = path.0;
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
