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

/// `GET /sse/{chain_id}/balances/{owner}` — see `openapi.yml` for the full
/// contract. Long-lived SSE stream: first event is the full snapshot,
/// subsequent events are diffs.
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
