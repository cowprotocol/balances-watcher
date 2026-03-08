use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum ServiceError {
    #[error("Error getting balances from multicall")]
    BalancesMultiCallError(String),
}

#[derive(Debug, Clone, Error)]
pub enum SubscriptionError {
    #[error("Too many subscriptions")]
    TooManySubscriptions,

    #[error("There aren't created subscriptions")]
    ThereArentCreatedSubscriptions,
}

#[derive(Debug, Clone, Error)]
pub enum FetcherError {
    #[error("Unable to load token list, url: {0}, error: {1}")]
    UnableToLoadList(String, String),
}
