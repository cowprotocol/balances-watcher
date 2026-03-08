use crate::config::constants::DEFAULT_MAX_WATCHED_TOKENS_LIMIT;
use crate::domain::{BalanceEvent, EvmNetwork, SubscriptionKey};
use crate::services::cleanup_stream;
use crate::services::errors::{FetcherError, SubscriptionError};
use crate::services::subscription_manager::SubscriptionManager;
use crate::services::token_list_fetcher::TokenListFetcher;
use crate::services::watcher::{Watcher, WatcherContext};
use alloy::primitives::Address;
use alloy::providers::DynProvider;
use alloy::transports::http::reqwest::StatusCode;
use axum::response::sse::Event;
use axum::response::{IntoResponse, Response, Sse};
use axum::Json;
use futures::Stream;
use futures::StreamExt;
use metrics::counter;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio_stream::wrappers::BroadcastStream;

// handle subscriptions: fetch token lists, spawn watchers, update watched tokens
pub struct SessionManager {
    sub_manager: Arc<SubscriptionManager>,
    providers: Arc<HashMap<EvmNetwork, DynProvider>>,
    ws_providers: Arc<HashMap<EvmNetwork, DynProvider>>,
    fetcher: Arc<TokenListFetcher>,
    snapshot_interval: usize,
    token_limit: usize,
}

pub struct SessionContext {
    pub owner: Address,
    pub network: EvmNetwork,
    pub custom_tokens: Vec<Address>,
    pub tokens_lists_urls: Vec<String>,
}

#[derive(Serialize)]
pub struct StreamError {
    pub code: u16,
    pub message: String,
}

impl StreamError {
    pub fn new(code: u16, message: String) -> StreamError {
        StreamError { code, message }
    }
    pub fn from(err: SessionError) -> StreamError {
        match err {
            SessionError::SessionIsNotCreated => {
                StreamError::new(404, String::from("Session is not created"))
            }
            _ => StreamError::new(500, String::from("Internal server error")),
        }
    }
}

impl IntoResponse for StreamError {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

        (status, Json(self)).into_response()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum SessionError {
    #[error("Token list not found: {0}")]
    TokenListNotFound(String),

    #[error("Token limit exceeded, max count is {0}, current count is {1}")]
    TokenLimitExceeded(usize, usize),

    #[error("Session is not created")]
    SessionIsNotCreated,

    #[error("Provider is not defined")]
    ProviderIsNotDefined,

    #[error("Ws provider is not defined")]
    WsProviderIsNotDefined,

    #[error("Too many clients")]
    TooManyClients,
}

// TODO create own error type
impl SessionManager {
    pub fn new(
        providers: HashMap<EvmNetwork, DynProvider>,
        ws_providers: HashMap<EvmNetwork, DynProvider>,
        snapshot_interval: usize,
        token_limit: usize,
    ) -> Self {
        let token_list_fetcher = TokenListFetcher::new();

        let sub_manager = SubscriptionManager::new();
        Self {
            sub_manager: Arc::new(sub_manager),
            providers: Arc::new(providers),
            ws_providers: Arc::new(ws_providers),
            fetcher: Arc::new(token_list_fetcher),
            snapshot_interval,
            token_limit,
        }
    }

    pub async fn create(&self, ctx: SessionContext) -> Result<(), SessionError> {
        let sub_key = SubscriptionKey {
            network: ctx.network,
            owner: ctx.owner,
        };

        let tokens = self
            .fetch_and_enriched_tokens(sub_key, ctx.tokens_lists_urls, ctx.custom_tokens)
            .await?;

        if tokens.len() > DEFAULT_MAX_WATCHED_TOKENS_LIMIT {
            return Err(SessionError::TokenLimitExceeded(
                tokens.len(),
                self.token_limit,
            ));
        }

        let provider = match self.providers.get(&sub_key.network) {
            Some(provider) => provider.clone(),
            None => return Err(SessionError::ProviderIsNotDefined),
        };

        let ws_provider = match self.ws_providers.get(&sub_key.network) {
            None => {
                return Err(SessionError::WsProviderIsNotDefined);
            }
            Some(ws_provider) => ws_provider.clone(),
        };

        let _ = self.sub_manager.create_or_update(sub_key, tokens).await;

        let (_, subscription) = self
            .sub_manager
            .subscribe(sub_key)
            .await
            .map_err(Self::map_subscription_error)?;

        // if there aren't spawners yet - spawn them and create a first subscription
        let should_spawn_watchers = subscription
            .watchers_spawned
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();

        if should_spawn_watchers {
            let ctx = WatcherContext {
                provider,
                owner: sub_key.owner,
                network: sub_key.network,
                ws_provider,
            };

            tracing::info!(
                sub = %sub_key,
                "create first sse subscription and spawn watchers"
            );

            Watcher::new(ctx, Arc::clone(&subscription))
                .spawn_watchers(self.snapshot_interval)
                .await;
        }

        tracing::warn!(
            sub = %sub_key,
            "session was created",
        );

        Ok(())
    }

    pub async fn update(&self, ctx: SessionContext) -> Result<(), SessionError> {
        let sub_key = SubscriptionKey {
            network: ctx.network,
            owner: ctx.owner,
        };

        let sub = self
            .sub_manager
            .get_subscription(sub_key)
            .await
            .ok_or(SessionError::SessionIsNotCreated)?;

        let tokens = self
            .fetch_and_enriched_tokens(sub_key, ctx.tokens_lists_urls, ctx.custom_tokens)
            .await?;

        let mut watched_tokens = sub.tokens.write().await;
        let prev_count = watched_tokens.len();

        // count how many new unique tokens would be added
        let new_unique = tokens
            .iter()
            .filter(|t| !watched_tokens.contains(*t))
            .count();

        let total_unique = prev_count + new_unique;
        if total_unique > self.token_limit {
            counter!("tokens_limit_exceeded_total").increment(1);
            tracing::error!(
                tokens_len = total_unique,
                previous_tokens_len = prev_count,
                "limit of watched tokens was exceeded",
            );
            return Err(SessionError::TokenLimitExceeded(
                total_unique,
                self.token_limit,
            ));
        }

        watched_tokens.extend(tokens);
        let new_count = watched_tokens.len();

        tracing::info!(
            tokens_len_before = prev_count,
            current_tokens_len = new_count,
            sub = %sub_key,
            "session was updated",
        );

        Ok(())
    }

    async fn fetch_and_enriched_tokens(
        &self,
        sub_key: SubscriptionKey,
        token_lists_urls: Vec<String>,
        custom_tokens: Vec<Address>,
    ) -> Result<HashSet<Address>, SessionError> {
        let token_list_fetcher = Arc::clone(&self.fetcher);

        let mut tokens = token_list_fetcher
            .get_tokens(&token_lists_urls, sub_key.network)
            .await
            .map_err(|err| match err {
                FetcherError::UnableToLoadList(url, _) => SessionError::TokenListNotFound(url),
            })?;
        tokens.extend(custom_tokens);
        tokens.insert(sub_key.network.weth9_address());
        tokens.insert(sub_key.network.native_token_address());

        Ok(tokens)
    }

    fn balance_event_to_sse(event: BalanceEvent) -> Result<Event, axum::Error> {
        #[derive(Serialize)]
        struct BalancesResponse {
            balances: HashMap<Address, String>,
        }
        #[derive(Serialize)]
        struct ErrorEvent {
            code: u16,
            message: String,
        }

        match event {
            BalanceEvent::BalanceUpdate(balances) => Event::default()
                .event("balance_update")
                .json_data(BalancesResponse { balances }),
            BalanceEvent::Error { code, message } => Event::default()
                .event("error")
                .json_data(ErrorEvent { code, message }),
        }
    }

    pub async fn create_sse_connection(
        &self,
        owner: Address,
        network: EvmNetwork,
    ) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StreamError> {
        let sub_key = SubscriptionKey { network, owner };
        let (rx, subscription) = self.sub_manager.subscribe(sub_key).await.map_err(|err| {
            let err = Self::map_subscription_error(err);
            StreamError::from(err)
        })?;

        let balance_snapshot = subscription.balances_snapshot.read().await;

        // if it's a first sse connection, the watcher should send updates when it fetches balances
        // otherwise, send balance snapshot
        let event = if balance_snapshot.is_empty() {
            tracing::info!(
                sub = %sub_key,
                "balance snapshot is empty"
            );
            None
        } else {
            let balance_snapshot: HashMap<Address, String> = balance_snapshot
                .clone()
                .into_iter()
                .map(|(address, balance)| (address, balance.amount.to_string()))
                .collect();
            Some(BalanceEvent::BalanceUpdate(balance_snapshot))
        };

        if let Some(event) = event {
            tracing::info!(
                sub = %sub_key,
                "sending first balance snapshot to new sse connection (full)"
            );

            let _ = subscription.sender.send(event).inspect_err(|err| {
                tracing::info!(
                    error = %err,
                    sub = %sub_key,
                    "error when send balance_snapshot update"
                );
            });
        }

        let manager_for_cleanup = Arc::clone(&self.sub_manager);

        let sse_stream = BroadcastStream::new(rx).filter_map(|result| async move {
            match result {
                Ok(event) => {
                    let sse_event = match Self::balance_event_to_sse(event) {
                        Ok(sse_event) => Some(Ok(sse_event)),
                        Err(err) => {
                            tracing::error!(
                                error = %err,
                                "error when convert balance event to sse event",
                            );
                            None
                        }
                    };
                    sse_event
                }
                Err(err) => {
                    counter!("broadcast_lagged_total").increment(1);
                    tracing::error!(
                        error = %err,
                        "broadcast stream error",
                    );
                    None
                }
            }
        });

        let cleanup_stream =
            cleanup_stream::CleanupStream::new(sse_stream, manager_for_cleanup, sub_key);

        Ok(Sse::new(cleanup_stream))
    }

    fn map_subscription_error(sub_error: SubscriptionError) -> SessionError {
        match sub_error {
            SubscriptionError::TooManySubscriptions => SessionError::TooManyClients,
            SubscriptionError::ThereArentCreatedSubscriptions => SessionError::SessionIsNotCreated,
        }
    }
}
