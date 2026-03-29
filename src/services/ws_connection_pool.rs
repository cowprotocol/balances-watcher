use crate::services::errors::ServiceError;
use alloy::providers::{DynProvider, Provider, ProviderBuilder, WsConnect};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

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
    connections: RwLock<HashMap<uuid::Uuid, Connection>>,
    max_clients_per_connection: usize,
}

impl WsConnectionPool {
    pub fn new(ws_url: String, max_connections: usize) -> WsConnectionPool {
        Self {
            ws_url,
            connections: RwLock::new(HashMap::new()),
            max_clients_per_connection: max_connections,
        }
    }

    // check if there is a provider with free connections, if there is -> increase clients' count
    // and return the cloned provider, otherwise - creates a new provider instance
    pub async fn acquire(self: &Arc<Self>) -> Result<PoolGuard, ServiceError> {
        {
            let mut conns = self.connections.write().await;
            let maybe_entry = conns
                .iter_mut()
                .find(|(_, conn)| conn.clients < self.max_clients_per_connection);

            if let Some((id, conn)) = maybe_entry {
                conn.clients += 1;
                let id = *id;
                return Ok(PoolGuard {
                    pool: Arc::clone(self),
                    provider: conn.provider.clone(),
                    id,
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
        self.connections.write().await.insert(id, new_con);
        Ok(PoolGuard {
            pool: Arc::clone(self),
            provider,
            id,
        })
    }

    pub async fn release(&self, id: uuid::Uuid) {
        let mut connections = self.connections.write().await;
        let should_remove = if let Some(con) = connections.get_mut(&id) {
            con.clients = con.clients.saturating_sub(1);
            con.clients == 0
        } else {
            tracing::warn!(id = %id, "connection pool is already dropped");
            false
        };

        if should_remove {
            connections.remove(&id);
        }
    }
}
