use crate::services::errors::ServiceError;
use alloy::providers::{DynProvider, Provider, ProviderBuilder, WsConnect};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

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
}

impl WsConnectionPool {
    pub fn new(ws_url: String, max_connections: usize) -> WsConnectionPool {
        Self {
            ws_url,
            connections: Mutex::new(HashMap::new()),
            max_clients_per_connection: max_connections,
        }
    }

    // check if there is a provider with free connections, if there is -> increase clients' count
    // and return the cloned provider, otherwise - creates a new provider instance
    //
    // Uses Mutex held across connect_ws to prevent thundering herd:
    // without this, under load all tasks see an empty pool simultaneously and
    // each creates its own WS connection, flooding the RPC provider.
    pub async fn acquire(self: &Arc<Self>) -> Result<PoolGuard, ServiceError> {
        let mut conns = self.connections.lock().await;

        // check if there is a connection with free capacity
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

        // no free connection — create a new one while holding the lock
        // so other tasks wait and reuse this connection instead of creating duplicates
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
        conns.insert(id, new_con);

        Ok(PoolGuard {
            pool: Arc::clone(self),
            provider,
            id,
        })
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
