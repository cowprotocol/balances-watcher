use crate::config::back_off_config::get_token_list_fetcher_backoff;
use crate::domain::{BalanceEvent, EvmNetwork, Session};
use crate::graceful_shutdown::LifeCycle;
use crate::metrics::Metrics;
use crate::services::block_watcher::BlockWatcher;
use crate::services::cleanup_stream;
use crate::services::event_dispatcher::{Erc20TransferEvent, EventDispatcher};
use crate::services::rpc_client::RpcClient;
use crate::services::subscription_manager::{SubscriptionError, SubscriptionManager};
use crate::services::token_list_fetcher::{FetcherError, TokenListFetcher};
use crate::services::watcher::SnapshotUpdater;
use crate::services::ws_connection_pool::WsConnectionPool;
use crate::ws_connection::WsConnection;
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
use tokio::sync::mpsc;
use tokio_stream::wrappers::BroadcastStream;

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
    event_dispatcher: Arc<EventDispatcher>,
    block_watcher: Arc<BlockWatcher>,
    lifecycle: LifeCycle,
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
    pub fn spawn(
        rpc_client: Arc<RpcClient>,
        ws_connection_pool: Arc<WsConnectionPool>,
        metrics: Arc<Metrics>,
        lifecycle: LifeCycle,
        config: SessionConfig,
        ws_url: String,
    ) -> Arc<Self> {
        let token_list_fetcher = TokenListFetcher::new(
            TOKEN_LIST_CACHE_TTL,
            get_token_list_fetcher_backoff(),
            Arc::clone(&metrics),
            config.active_network,
        );

        let sub_manager = Arc::new(SubscriptionManager::new(
            Arc::clone(&metrics),
            Arc::clone(&rpc_client),
            lifecycle.clone(),
        ));
        Arc::clone(&sub_manager).spawn_cleanup();

        // Dedicated WS sockets — per "one socket per logical signal" rule.
        let block_ws = WsConnection::new(
            ws_url.clone(),
            Arc::clone(&metrics),
            lifecycle.cancel_token.clone(),
        );
        let transfer_ws =
            WsConnection::new(ws_url, Arc::clone(&metrics), lifecycle.cancel_token.clone());

        let block_watcher = BlockWatcher::spawn(
            config.active_network,
            Arc::clone(&metrics),
            lifecycle.clone(),
            block_ws,
        );

        let (tx, rx) = mpsc::channel::<Erc20TransferEvent>(256);
        let event_dispatcher =
            EventDispatcher::spawn(Arc::clone(&metrics), transfer_ws, lifecycle.clone(), tx);

        let manager = Arc::new(Self {
            sub_manager: Arc::clone(&sub_manager),
            ws_connection_pool,
            event_dispatcher,
            block_watcher,
            fetcher: Arc::new(token_list_fetcher),
            rpc_client,
            lifecycle,
            metrics,
            config,
        });

        Arc::clone(&manager).spawn_erc20_transfer_listener(rx);

        manager
    }

    pub async fn upsert(self: Arc<Self>, ctx: SessionContext) -> Result<(), SessionError> {
        let session = ctx.session;

        // Single-network instance: the fetcher & ws pool are pre-bound. No
        // session-time lookup, no "provider not defined" branch.
        let rpc_client = Arc::clone(&self.rpc_client);
        let ws_pool = Arc::clone(&self.ws_connection_pool);

        let new_watched_tokens = Arc::clone(&self)
            .fetch_and_extend_tokens(session, ctx.tokens_lists_urls, ctx.custom_tokens)
            .await?;

        if new_watched_tokens.len() > self.config.token_limit {
            self.metrics.tokens_limit_exceeded_total.increment(1);
            tracing::warn!(
                tokens_len = new_watched_tokens.len(),
                limit = self.config.token_limit,
                "client request rejected: watched-token limit exceeded",
            );
            return Err(SessionError::TokenLimitExceeded(
                new_watched_tokens.len(),
                self.config.token_limit,
            ));
        }

        let (sub, maybe_queue_endpoints) =
            self.sub_manager.upsert(session, new_watched_tokens).await;

        // `upsert` returns the queue endpoints only for a brand-new session —
        // re-PUT'ing tokens for an already-live session yields `None` here,
        // so we spawn the per-session watchers exactly once over its lifetime.
        if let Some(queue_endpoints) = maybe_queue_endpoints {
            tracing::info!(
                session = %session,
                "upsert: spawning watchers"
            );

            let watcher = Arc::new(SnapshotUpdater::new(
                self.lifecycle.task_tracker.clone(),
                rpc_client,
                sub,
                ws_pool,
                Arc::clone(&self.metrics),
                session,
                Arc::clone(&self.block_watcher),
            ));

            watcher
                .spawn_watchers(
                    queue_endpoints.result_rx,
                    queue_endpoints.refresh_queue,
                    self.config.snapshot_interval,
                )
                .await;

            tracing::info!(
                session = %session,
                "upsert: watchers spawned"
            );
        }

        Ok(())
    }

    pub fn is_healthy(&self) -> bool {
        self.block_watcher.is_healthy() && self.event_dispatcher.is_healthy()
    }

    fn spawn_erc20_transfer_listener(
        self: Arc<Self>,
        mut receiver: mpsc::Receiver<Erc20TransferEvent>,
    ) {
        Arc::clone(&self).lifecycle.task_tracker.spawn(async move {
            let this = Arc::clone(&self);
            loop {
                let this = Arc::clone(&this);

                tokio::select! {
                    _ = this.lifecycle.cancel_token.cancelled() => break,
                    next = receiver.recv() => {
                        match next {
                            Some(event) => {
                                if let Some(queue_handle) = this.sub_manager.get_owned_queue_if_watched(&event.owner, &event.token).await {
                                    queue_handle.enqueue(event.token, event.block).await;
                                }
                            },
                            None => break,
                        }
                    }
                }
            }
        });
    }

    // fetch tokens from lists and add weth9 as watched
    async fn fetch_and_extend_tokens(
        self: Arc<Self>,
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
        // token lists doesn't contain weth9 address, we always should insert it
        tokens.insert(session.network.weth9_address());
        // native token tracking is intentionally not supported by this service.
        // Clients (and some token lists) commonly include the native sentinel
        // (0xEee…EEeE) alongside ERC20 addresses; strip it once here so it
        // never reaches `balanceOf` downstream.
        if tokens.remove(&session.network.native_token_address()) {
            tracing::debug!(
                session = %session,
                "dropped native token sentinel from watched set"
            );
        }

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
        self: Arc<Self>,
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
                        "failed to serialize initial snapshot as sse event",
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
                                "failed to serialize balance event as sse event",
                            );
                            None
                        }
                    },
                    Err(err) => {
                        metrics.broadcast_lagged_total.increment(1);
                        tracing::warn!(
                            error = %err,
                            "sse client lagged behind broadcast, dropping event",
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
            SubscriptionError::ClientLimitExceeded => SessionError::TooManyClients,
            SubscriptionError::SessionNotRegistered => SessionError::SessionIsNotCreated,
        }
    }
}
