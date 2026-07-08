use crate::domain::EvmNetwork;
use alloy::primitives::Address;
use std::fmt::Display;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Copy)]
pub struct Session {
    pub owner: Address,
    pub network: EvmNetwork,
    pub client_id: Uuid,
}

impl Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.owner, self.network)
    }
}
