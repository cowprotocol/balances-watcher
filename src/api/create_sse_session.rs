use crate::api::chain_extractor::ChainId;
use crate::api::client_id_extractor::ClientId;
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

#[utoipa::path(
    get,
    path = "/sse/{chain_id}/balances/{owner}",
    tag = "streaming",
    params(
        ("chain_id" = u64, Path, description = "EVM chain id; must match the instance's configured NETWORK", example = 1),
        ("owner"    = String, Path, description = "0x-prefixed owner address (20 bytes)", example = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"),
    ),
    responses(
        (status = 200,
         description = "Long-lived SSE stream. First event is the full snapshot \
                        (`event: balance_update`, JSON `{ balances: { address: amount } }`); \
                        every subsequent `balance_update` event is a diff (only changed entries). \
                        Terminal errors are emitted as `event: error` with `{ code, message }`.",
         content_type = "text/event-stream"),
        (status = 404, description = "chain_id mismatch or session not created"),
    ),
)]
pub async fn create_sse_connection(
    ChainId(network): ChainId,
    ClientId(client_id): ClientId,
    path: Path<(EvmNetwork, Address)>,
    State(state): State<Arc<AppState>>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StreamError> {
    let (_, owner) = path.0;
    Arc::clone(&state.session_manager)
        .create_sse_connection(owner, network, client_id)
        .await
}
