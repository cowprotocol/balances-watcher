use crate::services::errors::ServiceError;
use alloy::providers::{DynProvider, Provider, ProviderBuilder, WsConnect};
use dashmap::DashMap;
use std::sync::Arc;

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
    connections: DashMap<uuid::Uuid, Connection>,
    max_clients_per_connection: usize,
}

impl WsConnectionPool {
    pub fn new(ws_url: String, max_connections: usize) -> WsConnectionPool {
        Self {
            ws_url,
            connections: DashMap::new(),
            max_clients_per_connection: max_connections,
        }
    }

    // check if there is a provider with free connections, if there is -> increase clients' count
    // and return the cloned provider, otherwise - creates a new provider instance
    pub async fn acquire(self: &Arc<Self>) -> Result<PoolGuard, ServiceError> {
        {
            let maybe_entry = self
                .connections
                .iter_mut()
                .find(|entry| entry.value().clients < self.max_clients_per_connection);

            if let Some(mut entry) = maybe_entry {
                // we have con.clients < self.max_connections above this check, so it's safe
                entry.clients += 1;
                return Ok(PoolGuard {
                    pool: Arc::clone(self),
                    provider: entry.provider.clone(),
                    id: *entry.key(),
                });
            }
        }

        let ws = WsConnect::new(self.ws_url.clone());
        let provider = ProviderBuilder::new()
            .connect_ws(ws)
            .await
            .map_err(|err| ServiceError::ErrorInitWsProvider(err.to_string()))?
            .erased();
        let new_con = Connection {
            provider: provider.clone(),
            clients: 1,
        };

        let id = uuid::Uuid::new_v4();
        self.connections.insert(id, new_con);
        Ok(PoolGuard {
            pool: Arc::clone(self),
            provider,
            id,
        })
    }

    pub async fn release(&self, id: uuid::Uuid) {
        if let Some(mut con) = self.connections.get_mut(&id) {
            con.clients = con.clients.saturating_sub(1);
            if con.clients == 0 {
                self.connections.remove(&id);
            }
        } else {
            tracing::warn!(id = %id, "connection pool is already dropped");
        }
    }
}
