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

pub async fn create_sse_connection(
    Path((network, owner)): Path<(EvmNetwork, Address)>,
    State(state): State<Arc<AppState>>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StreamError> {
    if network != state.network {
        return Err(StreamError::new(
            404,
            format!(
                "chain_id {network} is not served by this instance (configured: {})",
                state.network
            ),
        ));
    }

    state
        .session_manager
        .create_sse_connection(owner, network)
        .await
}
