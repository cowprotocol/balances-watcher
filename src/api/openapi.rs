//! OpenAPI specification for the public HTTP surface.
//!
//! Built at compile time by [`utoipa`]. The generated spec is served at
//! `/openapi.json` and rendered through Swagger UI at `/docs`.
//!
//! Schema policy:
//! - `EvmNetwork` and `Address` are rendered as JSON strings (the wire format
//!   the service actually accepts), not as Rust enums / byte arrays. The
//!   underlying types live outside this crate (`alloy`) so we attach
//!   `value_type = String` on the field rather than implementing `ToSchema`
//!   for them.

use utoipa::OpenApi;

use crate::api::create_session::{CreateSessionRequest, __path_create_session};
use crate::api::create_sse_session::__path_create_sse_connection;
use crate::api::health::__path_health_handler;
use crate::api::update_session::{UpdateSessionRequest, __path_update_session};
use crate::app_error::ErrorBody;

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Balances Watcher",
        version = env!("CARGO_PKG_VERSION"),
        description = "Real-time ERC20 balance tracking service for EVM chains. \
                       One process per network — the `{chain_id}` path segment is \
                       validated against the instance's configured chain and \
                       rejected with 404 if mismatched.\n\n\
                       Typical flow: `POST /{chain_id}/sessions/{owner}` to create \
                       a session, then open the SSE stream at \
                       `GET /sse/{chain_id}/balances/{owner}`. Use `PUT` to replace \
                       the watched token list (it is a full replace, not extend).",
        contact(name = "balances-watcher", url = "https://github.com/cowprotocol/balances-watcher"),
    ),
    paths(
        health_handler,
        create_session,
        update_session,
        create_sse_connection,
    ),
    components(schemas(CreateSessionRequest, UpdateSessionRequest, ErrorBody)),
    tags(
        (name = "sessions",  description = "Create / replace watched-token sessions."),
        (name = "streaming", description = "Server-Sent Events delivering balance diffs."),
        (name = "health",    description = "Active liveness probe."),
    )
)]
pub struct ApiDoc;
