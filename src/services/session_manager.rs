use crate::config::back_off_config::get_token_list_fetcher_backoff;
use crate::domain::{BalanceEvent, EvmNetwork, Session};
use crate::services::balance_fetcher::BalanceFetcher;
use crate::services::cleanup_stream;
use crate::services::errors::{FetcherError, SubscriptionError};
use crate::services::subscription_manager::SubscriptionManager;
use crate::services::token_list_fetcher::TokenListFetcher;
use crate::services::watcher::Watcher;
use crate::services::ws_connection_pool::WsConnectionPool;
use alloy::primitives::Address;
use alloy::transports::http::reqwest::StatusCode;
use axum::response::sse::{Event, KeepAlive};
use axum::response::{IntoResponse, Response, Sse};
use axum::Json;
use futures::stream;
use futures::Stream;
use futures::StreamExt;
use metrics::{counter, histogram};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_stream::wrappers::BroadcastStream;

const TOKEN_LIST_CACHE_TTL: Duration = Duration::from_hours(5);

// handle subscriptions: fetch token lists, spawn watchers, update watched tokens
pub struct SessionManager {
    sub_manager: Arc<SubscriptionManager>,
    multicall_fetchers: Arc<HashMap<EvmNetwork, Arc<BalanceFetcher>>>,
    ws_providers_pool: Arc<HashMap<EvmNetwork, Arc<WsConnectionPool>>>,
    fetcher: Arc<TokenListFetcher>,
    snapshot_interval: usize,
    token_limit: usize,
}

pub struct SessionContext {
    pub session: Session,
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
        multicall_fetchers: HashMap<EvmNetwork, Arc<BalanceFetcher>>,
        ws_providers_pool: HashMap<EvmNetwork, Arc<WsConnectionPool>>,
        snapshot_interval: usize,
        token_limit: usize,
    ) -> Self {
        let token_list_fetcher =
            TokenListFetcher::new(TOKEN_LIST_CACHE_TTL, get_token_list_fetcher_backoff());

        let sub_manager = Arc::new(SubscriptionManager::new());
        Arc::clone(&sub_manager).spawn_cleanup();

        Self {
            sub_manager: Arc::clone(&sub_manager),
            ws_providers_pool: Arc::new(ws_providers_pool),
            fetcher: Arc::new(token_list_fetcher),
            multicall_fetchers: Arc::new(multicall_fetchers),
            snapshot_interval,
            token_limit,
        }
    }

    pub async fn upsert(&self, ctx: SessionContext) -> Result<(), SessionError> {
        let session = ctx.session;
        let upsert_start = Instant::now();

        tracing::info!(session = %session, "upsert: resolving providers");
        let provider = match self.multicall_fetchers.get(&session.network) {
            Some(provider) => provider.clone(),
            None => return Err(SessionError::ProviderIsNotDefined),
        };

        let ws_pool = match self.ws_providers_pool.get(&session.network) {
            None => {
                return Err(SessionError::WsProviderIsNotDefined);
            }
            Some(ws_pool) => Arc::clone(ws_pool),
        };

        let t0 = Instant::now();
        tracing::info!(session = %session, "upsert: fetching tokens");
        let tokens = self
            .fetch_and_enriched_tokens(session, ctx.tokens_lists_urls, ctx.custom_tokens)
            .await?;
        let elapsed = t0.elapsed().as_millis() as f64;
        histogram!("upsert_fetch_tokens_ms").record(elapsed);
        tracing::info!(session = %session, tokens_len = tokens.len(), time_ms = elapsed, "upsert: tokens fetched");

        let t0 = Instant::now();
        tracing::info!(session = %session, "upsert: get_subscription");
        let subscription = self.sub_manager.get_subscription(session).await;
        // if the sub already exists - check if there are new tokens to watch and check limits
        let (updated_tokens, new_uniq_tokens) = if let Some(sub) = subscription {
            let mut watched_tokens = sub.watched_tokens().await;

            let new_tokens = tokens
                .iter()
                .filter(|t| !watched_tokens.contains(*t))
                .copied()
                .collect::<HashSet<_>>();

            watched_tokens.extend(new_tokens.clone());

            (watched_tokens, Some(new_tokens))
        } else {
            (tokens, None)
        };
        let elapsed = t0.elapsed().as_millis() as f64;
        histogram!("upsert_get_subscription_ms").record(elapsed);

        if updated_tokens.len() > self.token_limit {
            counter!("tokens_limit_exceeded_total").increment(1);
            tracing::error!(
                tokens_len = updated_tokens.len(),
                "limit of watched tokens was exceeded",
            );
            return Err(SessionError::TokenLimitExceeded(
                updated_tokens.len(),
                self.token_limit,
            ));
        }

        let t0 = Instant::now();
        tracing::info!(session = %session, "upsert: sub_manager.upsert");
        let new_tokens = new_uniq_tokens.unwrap_or(updated_tokens);
        let sub = self.sub_manager.upsert(session, new_tokens).await;
        let elapsed = t0.elapsed().as_millis() as f64;
        histogram!("upsert_sub_manager_upsert_ms").record(elapsed);

        // if there aren't spawners yet - spawn them and create a first subscription
        let should_spawn_watchers = sub.try_mark_watchers_spawned();

        if should_spawn_watchers {
            let t0 = Instant::now();
            tracing::info!(
                session = %session,
                "upsert: spawning watchers"
            );

            Watcher::new(session, provider, Arc::clone(&sub), ws_pool)
                .spawn_watchers(self.snapshot_interval)
                .await;

            let elapsed = t0.elapsed().as_millis() as f64;
            histogram!("upsert_spawn_watchers_ms").record(elapsed);
            tracing::info!(
                session = %session,
                time_ms = elapsed,
                "upsert: watchers spawned"
            );
        }

        let elapsed = upsert_start.elapsed().as_millis() as f64;
        histogram!("upsert_total_ms").record(elapsed);
        tracing::info!(
            session = %session,
            time_ms = elapsed,
            "upsert: done",
        );

        Ok(())
    }

    // fetch tokens from lists and add eth/weth9 as watched
    async fn fetch_and_enriched_tokens(
        &self,
        session: Session,
        token_lists_urls: Vec<String>,
        custom_tokens: Vec<Address>,
    ) -> Result<HashSet<Address>, SessionError> {
        let token_list_fetcher = Arc::clone(&self.fetcher);

        let mut tokens = token_list_fetcher
            .get_tokens(&token_lists_urls, session.network)
            .await
            .map_err(|err| match err {
                FetcherError::UnableToLoadList(url, _) => SessionError::TokenListNotFound(url),
            })?;
        tokens.extend(custom_tokens);
        tokens.insert(session.network.weth9_address());
        tokens.insert(session.network.native_token_address());

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
        let session = Session { network, owner };
        let (rx, subscription) = self.sub_manager.subscribe(session).await.map_err(|err| {
            let err = Self::map_subscription_error(err);
            StreamError::from(err)
        })?;

        let balance_snapshot = subscription.current_snapshot().await;

        // Build an initial stream that delivers the snapshot directly to this client only,
        // avoiding a broadcast that would redundantly push to all existing subscribers.
        let initial_stream = if balance_snapshot.is_empty() {
            tracing::info!(
                session = %session,
                "balance snapshot is empty"
            );
            stream::iter(vec![])
        } else {
            tracing::info!(
                session = %session,
                "sending first balance snapshot to new sse connection (full)"
            );

            let balance_snapshot: HashMap<Address, String> = balance_snapshot
                .into_iter()
                .map(|(address, balance)| (address, balance.amount.to_string()))
                .collect();
            let event = BalanceEvent::BalanceUpdate(balance_snapshot);

            match Self::balance_event_to_sse(event) {
                Ok(sse_event) => stream::iter(vec![Ok(sse_event)]),
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        "error when converting initial snapshot to sse event",
                    );
                    stream::iter(vec![])
                }
            }
        };

        let manager_for_cleanup = Arc::clone(&self.sub_manager);

        let broadcast_stream = BroadcastStream::new(rx).filter_map(|result| async move {
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

        let sse_stream = initial_stream.chain(broadcast_stream);

        let cleanup_stream =
            cleanup_stream::CleanupStream::new(sse_stream, manager_for_cleanup, session);

        Ok(Sse::new(cleanup_stream).keep_alive(KeepAlive::default()))
    }

    fn map_subscription_error(sub_error: SubscriptionError) -> SessionError {
        match sub_error {
            SubscriptionError::TooManySubscriptions => SessionError::TooManyClients,
            SubscriptionError::ThereArentCreatedSubscriptions => SessionError::SessionIsNotCreated,
        }
    }
}
