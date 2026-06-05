use crate::config::back_off_config::get_token_list_fetcher_backoff;
use crate::domain::{BalanceEvent, EvmNetwork, Session};
use crate::metrics::Metrics;
use crate::services::calls_queue::CallsQueue;
use crate::services::cleanup_stream;
use crate::services::errors::{FetcherError, SubscriptionError};
use crate::services::rpc_client::{RpcClient, RpcError};
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
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const TOKEN_LIST_CACHE_TTL: Duration = Duration::from_hours(5);

pub struct SessionConfig {
    // interval for multicall for the whole watched token list
    pub snapshot_interval: usize,
    // how many tokens we watch regarding session
    pub token_limit: usize,
    // network that manager handles
    pub active_network: EvmNetwork,
}

/// Per-network session orchestrator.
///
/// One `SessionManager` exists per process — the service is single-network, so
/// the balance fetcher and WS connection pool are owned directly here (no
/// network → resource lookup map). Responsibilities:
/// - fetch & cache token lists
/// - spawn watchers for new sessions
/// - update watched token sets on session updates
/// - bridge subscription events to SSE clients
pub struct SessionManager {
    sub_manager: Arc<SubscriptionManager>,
    rpc_client: Arc<RpcClient>,
    ws_connection_pool: Arc<WsConnectionPool>,
    fetcher: Arc<TokenListFetcher>,
    task_tracker: TaskTracker,
    metrics: Arc<Metrics>,
    config: SessionConfig,
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

    #[error("Too many clients")]
    TooManyClients,
}

impl SessionManager {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        ws_connection_pool: Arc<WsConnectionPool>,
        metrics: Arc<Metrics>,
        task_tracker: TaskTracker,
        shutdown_token: CancellationToken,
        config: SessionConfig,
    ) -> Self {
        let token_list_fetcher = TokenListFetcher::new(
            TOKEN_LIST_CACHE_TTL,
            get_token_list_fetcher_backoff(),
            Arc::clone(&metrics),
            config.active_network,
        );

        let sub_manager = Arc::new(SubscriptionManager::new(
            task_tracker.clone(),
            shutdown_token,
            Arc::clone(&metrics),
        ));
        Arc::clone(&sub_manager).spawn_cleanup();

        Self {
            sub_manager: Arc::clone(&sub_manager),
            ws_connection_pool,
            fetcher: Arc::new(token_list_fetcher),
            rpc_client,
            task_tracker,
            metrics,
            config,
        }
    }

    pub async fn upsert(&self, ctx: SessionContext) -> Result<(), SessionError> {
        let session = ctx.session;

        // Single-network instance: the fetcher & ws pool are pre-bound. No
        // session-time lookup, no "provider not defined" branch.
        let rpc_client = Arc::clone(&self.rpc_client);
        let ws_pool = Arc::clone(&self.ws_connection_pool);

        let tokens = self
            .fetch_and_extend_tokens(session, ctx.tokens_lists_urls, ctx.custom_tokens)
            .await?;

        let subscription = self.sub_manager.get_subscription(session).await;
        // if the sub already exists - check if there are new tokens to watch and check limits
        let (updated_tokens, new_uniq_tokens) = if let Some(sub) = subscription {
            let mut watched_tokens = sub.clone_watched_tokens().await;

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

        if updated_tokens.len() > self.config.token_limit {
            self.metrics.tokens_limit_exceeded_total.increment(1);
            tracing::error!(
                tokens_len = updated_tokens.len(),
                "limit of watched tokens was exceeded",
            );
            return Err(SessionError::TokenLimitExceeded(
                updated_tokens.len(),
                self.config.token_limit,
            ));
        }

        let new_tokens = new_uniq_tokens.unwrap_or(updated_tokens);
        let sub = self.sub_manager.upsert(session, new_tokens).await;

        // if there aren't spawners yet - spawn them and create a first subscription
        let should_spawn_watchers = sub.try_mark_watchers_spawned();

        if should_spawn_watchers {
            tracing::info!(
                session = %session,
                "upsert: spawning watchers"
            );

            let (calls_queue, rx) = CallsQueue::new(
                self.task_tracker.clone(),
                session.owner,
                Arc::clone(&rpc_client),
            );

            let watcher = Arc::new(Watcher::new(
                self.task_tracker.clone(),
                rpc_client,
                sub,
                calls_queue,
                ws_pool,
                Arc::clone(&self.metrics),
                session,
            ));

            watcher
                .spawn_watchers(rx, self.config.snapshot_interval)
                .await;

            tracing::info!(
                session = %session,
                "upsert: watchers spawned"
            );
        }

        Ok(())
    }

    // fetch tokens from lists and add eth/weth9 as watched
    async fn fetch_and_extend_tokens(
        &self,
        session: Session,
        token_lists_urls: Vec<String>,
        custom_tokens: Vec<Address>,
    ) -> Result<HashSet<Address>, SessionError> {
        let token_list_fetcher = Arc::clone(&self.fetcher);

        let mut tokens = token_list_fetcher
            .get_tokens(&token_lists_urls)
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

    pub async fn healthcheck(&self) -> Result<(), RpcError> {
        self.rpc_client.get_block_number().await.map(|_| Ok(()))?
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
        let metrics = Arc::clone(&self.metrics);

        let broadcast_stream = BroadcastStream::new(rx).filter_map(move |result| {
            let metrics = Arc::clone(&metrics);
            async move {
                match result {
                    Ok(event) => match Self::balance_event_to_sse(event) {
                        Ok(sse_event) => Some(Ok(sse_event)),
                        Err(err) => {
                            tracing::error!(
                                error = %err,
                                "error when convert balance event to sse event",
                            );
                            None
                        }
                    },
                    Err(err) => {
                        metrics.broadcast_lagged_total.increment(1);
                        tracing::error!(
                            error = %err,
                            "broadcast stream error",
                        );
                        None
                    }
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
