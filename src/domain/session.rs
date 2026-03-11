use crate::domain::EvmNetwork;
use alloy::primitives::Address;
use std::fmt::Display;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Copy)]
pub struct Session {
    pub owner: Address,
    pub network: EvmNetwork,
}

impl Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.owner, self.network)
    }
}
