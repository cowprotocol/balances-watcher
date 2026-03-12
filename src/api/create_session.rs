use alloy::primitives::Address;
use axum::{
    extract::{Path, State},
    Json,
};
use serde::Deserialize;
use std::sync::Arc;

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

// handler to create a session - this endpoint should be called before sse request
// it creates necessary web3 listeners and snapshot updaters
pub async fn create_session(
    Path((network, owner)): Path<(EvmNetwork, Address)>,
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<(), AppError> {
    if body.tokens_lists_urls.is_empty() {
        return Err(AppError::BadRequest(
            "tokens_lists_urls should not be empty".into(),
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
