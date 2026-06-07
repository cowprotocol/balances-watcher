use thiserror;

#[derive(Debug, Clone, thiserror::Error)]
pub enum ServiceError {
    #[error("Multicall error: {0}")]
    MultiCallError(String),

    #[error("Error when init the ws provider: {0}")]
    ErrorInitWsProvider(String),
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum SubscriptionError {
    #[error("Too many subscriptions")]
    TooManySubscriptions,

    #[error("There aren't created subscriptions")]
    ThereArentCreatedSubscriptions,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum FetcherError {
    #[error("Unable to load token list, url: {0}, error: {1}")]
    UnableToLoadList(String, String),
}
