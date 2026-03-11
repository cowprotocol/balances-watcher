use alloy::primitives::Address;
use serde::Serialize;
use std::collections::HashMap;

/// Events sent to SSE clients
#[derive(Debug, Clone, Serialize)]
pub enum BalanceEvent {
    /// Full balance snapshot (all tokens)
    BalanceUpdate(HashMap<Address, String>),
    /// Error event
    Error { code: u16, message: String },
}
