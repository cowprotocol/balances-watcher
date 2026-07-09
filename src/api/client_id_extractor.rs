//! [`ClientId`] extractor — pulls the caller's device UUID from either the
//! `X-Client-Id` request header (`POST` / `PUT`) or the `client_id` query
//! parameter (SSE, where the browser `EventSource` API cannot set headers).
//!
//! The header wins if both are present; missing or malformed input → `400`.
//! The extractor is state-agnostic (`impl FromRequestParts<S>`) so it can be
//! reused in tests and any future router that doesn't own `AppState`.

use crate::app_error::AppError;
use axum::extract::{FromRequestParts, Query};
use axum::http::request::Parts;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Deserialize)]
struct ClientIdQuery {
    client_id: Option<Uuid>,
}

/// Device-scoped session identifier — see [`crate::domain::Session`] for the
/// full semantics. Handlers consume it as `ClientId(client_id): ClientId` and
/// carry it into [`crate::domain::Session::client_id`].
pub struct ClientId(pub Uuid);

impl ClientId {
    fn try_from_headers(parts: &Parts) -> Result<Option<Uuid>, AppError> {
        let Some(raw_client_id) = parts.headers.get("x-client-id") else {
            return Ok(None);
        };

        let client_id_as_str = raw_client_id
            .to_str()
            .map_err(|_| AppError::BadRequest("X-Client-Id is not valid ASCII".to_string()))?;

        let client_id = Uuid::parse_str(client_id_as_str)
            .map_err(|_| AppError::BadRequest("X-Client-Id is not a valid UUID".to_string()))?;

        Ok(Some(client_id))
    }

    async fn try_from_query<S: Send + Sync>(
        parts: &mut Parts,
        state: &S,
    ) -> Result<Option<Uuid>, AppError> {
        let Query(q) = Query::<ClientIdQuery>::from_request_parts(parts, state)
            .await
            .map_err(|e| AppError::BadRequest(format!("invalid client_id query: {e}")))?;
        Ok(q.client_id)
    }
}

impl<S: Send + Sync> FromRequestParts<S> for ClientId {
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        if let Some(client_id) = ClientId::try_from_headers(parts)? {
            return Ok(ClientId(client_id));
        } else if let Some(client_id) = ClientId::try_from_query(parts, state).await? {
            return Ok(ClientId(client_id));
        }

        Err(AppError::BadRequest(
            "missing client id (send X-Client-Id header or ?client_id= query)".to_string(),
        ))
    }
}
