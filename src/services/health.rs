//! Structured health status for the `/health` endpoint.
//!
//! Each health-owning subsystem (currently `BlockWatcher` and
//! `Erc20TransferEventDispatcher`) returns a [`SubsystemHealth`] describing
//! either `Healthy` or `Unhealthy` with a concrete reason. The aggregate
//! [`AppHealth`] is what the HTTP handler logs before returning 503, so on any
//! outage we can read the exact cause straight from stdout / VictoriaLogs
//! without correlating against separate metrics.

#[derive(Debug, Clone)]
pub enum SubsystemHealth {
    Healthy,
    Unhealthy(String),
}

impl SubsystemHealth {
    pub fn is_healthy(&self) -> bool {
        match self {
            SubsystemHealth::Healthy => true,
            SubsystemHealth::Unhealthy(_) => false,
        }
    }

    pub fn reason(&self) -> Option<&str> {
        match self {
            SubsystemHealth::Unhealthy(r) => Some(r.as_str()),
            SubsystemHealth::Healthy => None,
        }
    }
}

/// Full app health snapshot returned by
/// [`crate::services::session_manager::SessionManager::health_status`].
#[derive(Debug, Clone)]
pub struct AppHealth {
    pub block_watcher: SubsystemHealth,
    pub event_dispatcher: SubsystemHealth,
}

impl AppHealth {
    pub fn is_healthy(&self) -> bool {
        self.block_watcher.is_healthy() && self.event_dispatcher.is_healthy()
    }
}
