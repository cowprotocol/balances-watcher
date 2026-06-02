use crate::api::extractors::ChainId;
use crate::app_state::AppState;
use crate::domain::EvmNetwork;
use crate::services::session_manager::StreamError;
use alloy::primitives::Address;
use axum::{
    extract::{Path, State},
    response::sse::{Event, Sse},
};
use futures::Stream;
use std::{convert::Infallible, sync::Arc};

/// `GET /sse/{chain_id}/balances/{owner}`
///
/// opens a long-lived Server-Sent Events stream that pushes balance updates
/// for `owner` on `chain_id`. The first event is a full snapshot, every
/// subsequent event is a diff (only changed balances). Requires that a
/// session has already been created via `POST /{chain_id}/sessions/{owner}` —
/// otherwise returns `404 Not Found`.
///
/// Chain mismatch is rejected with `404 Not Found` by the `ChainId` extractor
pub async fn create_sse_connection(
    ChainId(network): ChainId,
    Path((_, owner)): Path<(EvmNetwork, Address)>,
    State(state): State<Arc<AppState>>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StreamError> {
    state
        .session_manager
        .create_sse_connection(owner, network)
        .await
}
