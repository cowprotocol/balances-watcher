use crate::config::constants::WS_SUBSCRIPTION_PERMITS_COUNT;
use crate::metrics::Metrics;
use alloy::providers::{DynProvider, Provider, ProviderBuilder, WsConnect};
use alloy::rpc::types::{Filter, Log};
use backon::{ExponentialBuilder, Retryable};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};

/// Errors surfaced by [`WsConnectionPool`].
#[derive(Debug, Clone, thiserror::Error)]
pub enum WsPoolError {
    #[error("failed to init WS provider: {0}")]
    InitProvider(String),

    #[error("failed to acquire WS connection pool: {0}")]
    FailedToAcquireConnectionPool(String),

    #[error("WS subscription is exhausted with retires")]
    WsSubscriptionExhausted,
}

struct Connection {
    provider: DynProvider,
    clients: usize,
}

// guard to handle drop to release the provider
pub struct PoolGuard {
    pub provider: DynProvider,
    id: uuid::Uuid,
    pool: Arc<WsConnectionPool>,
}

impl Drop for PoolGuard {
    fn drop(&mut self) {
        let pool = Arc::clone(&self.pool);
        let id = self.id;
        tokio::spawn(async move {
            pool.release(id).await;
        });
    }
}

// handle active connections per a provider instance
pub struct WsConnectionPool {
    ws_url: String,
    connections: Mutex<HashMap<uuid::Uuid, Connection>>,
    max_clients_per_connection: usize,
    semaphore: Arc<Semaphore>,
    metrics: Arc<Metrics>,
}

pub struct GuardedWsSubscription {
    pub ws_sub: alloy::pubsub::Subscription<Log>,
    _guard: PoolGuard,
}

impl WsConnectionPool {
    pub fn new(ws_url: String, metrics: Arc<Metrics>, max_connections: usize) -> WsConnectionPool {
        Self {
            ws_url,
            connections: Mutex::new(HashMap::new()),
            max_clients_per_connection: max_connections,
            semaphore: Arc::new(Semaphore::new(WS_SUBSCRIPTION_PERMITS_COUNT)),
            metrics,
        }
    }

    pub async fn subscribe(
        self: Arc<Self>,
        filter: &Filter,
    ) -> Result<GuardedWsSubscription, WsPoolError> {
        let _permit = self.semaphore.acquire().await;

        let pool_guard = Arc::clone(&self).acquire().await;

        match pool_guard {
            Ok(pool) => Self::subscribe_with_retries(pool, filter).await,
            Err(err) => Err(WsPoolError::FailedToAcquireConnectionPool(err.to_string())),
        }
    }

    async fn subscribe_with_retries(
        pool_guard: PoolGuard,
        filter: &Filter,
    ) -> Result<GuardedWsSubscription, WsPoolError> {
        let backoff = Self::create_ws_sub_backoff();
        let provider = &pool_guard.provider;
        let metrics = Arc::clone(&pool_guard.pool.metrics);
        let ws_sub = (|| async { provider.subscribe_logs(filter).await })
            .retry(backoff)
            .notify(move |err, duration| {
                tracing::warn!(
                    error = %err,
                    duration = ?duration,
                    "ws subscribe attempt failed, will retry"
                );
                metrics.ws_subscribe_errors_total.increment(1);
            })
            .await
            .map_err(|_| WsPoolError::WsSubscriptionExhausted)?;

        Ok(GuardedWsSubscription {
            ws_sub,
            _guard: pool_guard,
        })
    }

    // check if there is a provider with free connections, if there is -> increase clients' count
    // and return the cloned provider, otherwise - creates a new provider instance
    //
    // Uses Mutex held across connect_ws to prevent thundering herd:
    // without this, under load all tasks see an empty pool simultaneously and
    // each creates its own WS connection, flooding the RPC provider.
    async fn acquire(self: Arc<Self>) -> Result<PoolGuard, WsPoolError> {
        let mut conns = self.connections.lock().await;

        // check if there is a connection with free capacity
        let maybe_entry = conns
            .iter_mut()
            .find(|(_, conn)| conn.clients < self.max_clients_per_connection);

        if let Some((id, conn)) = maybe_entry {
            conn.clients += 1;
            let id = *id;
            return Ok(PoolGuard {
                pool: Arc::clone(&self),
                provider: conn.provider.clone(),
                id,
            });
        }

        // no free connection — create a new one while holding the lock
        // so other tasks wait and reuse this connection instead of creating duplicates
        let ws = WsConnect::new(self.ws_url.clone());
        let provider = ProviderBuilder::new()
            .connect_ws(ws)
            .await
            .map_err(|err| WsPoolError::InitProvider(err.to_string()))?
            .erased();
        let new_con = Connection {
            provider: provider.clone(),
            clients: 1,
        };

        let id = uuid::Uuid::new_v4();
        conns.insert(id, new_con);

        Ok(PoolGuard {
            pool: Arc::clone(&self),
            provider,
            id,
        })
    }

    fn create_ws_sub_backoff() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(Duration::from_secs(1))
            .with_max_delay(Duration::from_secs(5))
            .with_max_times(5)
            .with_jitter()
    }

    pub async fn release(&self, id: uuid::Uuid) {
        let mut conns = self.connections.lock().await;
        let should_remove = if let Some(con) = conns.get_mut(&id) {
            con.clients = con.clients.saturating_sub(1);
            con.clients == 0
        } else {
            tracing::warn!(id = %id, "connection pool is already dropped");
            false
        };

        if should_remove {
            conns.remove(&id);
        }
    }
}
