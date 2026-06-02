use crate::domain::errors::EvmError;
use alloy::primitives::{address, Address};
use serde::{Deserialize, Deserializer};
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u64)]
pub enum EvmNetwork {
    Eth = 1,
    Arbitrum = 42161,
    Sepolia = 11155111,
    Bnb = 56,
    Gnosis = 100,
    Polygon = 137,
}

impl EvmNetwork {
    pub fn chain_id(self) -> u64 {
        self as u64
    }

    pub fn native_token_address(self) -> Address {
        address!("0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE")
    }

    pub fn multicall3_address(self) -> Address {
        address!("0xcA11bde05977b3631167028862bE2a173976CA11")
    }

    pub fn weth9_address(self) -> Address {
        match self {
            EvmNetwork::Eth => address!("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"),
            EvmNetwork::Sepolia => address!("0xfFf9976782d46CC05630D1f6eBAb18b2324d6B14"),
            EvmNetwork::Arbitrum => address!("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"),
            EvmNetwork::Gnosis => address!("0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d"),
            EvmNetwork::Bnb => address!("0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"),
            EvmNetwork::Polygon => address!("0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"),
        }
    }
}

impl TryFrom<u64> for EvmNetwork {
    type Error = EvmError;

    fn try_from(id: u64) -> Result<Self, EvmError> {
        match id {
            1 => Ok(EvmNetwork::Eth),
            42161 => Ok(EvmNetwork::Arbitrum),
            11155111 => Ok(EvmNetwork::Sepolia),
            56 => Ok(EvmNetwork::Bnb),
            100 => Ok(EvmNetwork::Gnosis),
            137 => Ok(EvmNetwork::Polygon),
            _ => Err(EvmError::UnsupportedNetwork(id)),
        }
    }
}

impl FromStr for EvmNetwork {
    type Err = EvmError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let chain_id = s.parse::<u64>().map_err(|_| EvmError::InvalidNetworkId)?;
        EvmNetwork::try_from(chain_id)
    }
}

impl Display for EvmNetwork {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.chain_id())
    }
}

impl<'de> Deserialize<'de> for EvmNetwork {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let id: u64 = s.parse().map_err(serde::de::Error::custom)?;
        EvmNetwork::try_from(id).map_err(serde::de::Error::custom)
    }
}
