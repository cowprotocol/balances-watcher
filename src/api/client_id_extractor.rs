use crate::app_error::AppError;
use axum::extract::{FromRequestParts, Query};
use axum::http::request::Parts;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct ClientIdQuery {
    pub client_id: Option<Uuid>,
}

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
            .map_err(|_| {
                AppError::BadRequest("Query failed for client ID extractor".to_string())
            })?;
        Ok(q.client_id)
    }
}

impl<S: Send + Sync> FromRequestParts<S> for ClientId {
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        if let Some(client_id) = ClientId::try_from_headers(parts)? {
            return Ok(ClientId(client_id));
        } else if let Some(client_id) = ClientId::try_from_query(parts, &state).await? {
            return Ok(ClientId(client_id));
        }

        Err(AppError::BadRequest(
            "No client ID found in headers".to_string(),
        ))
    }
}
