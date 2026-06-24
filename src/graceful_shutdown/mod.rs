use tokio::{signal, signal::unix::SignalKind};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub fn spawn_shutdown_token() -> CancellationToken {
    let shutdown_token = CancellationToken::new();

    let shutdown_token_cloned = shutdown_token.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        shutdown_token_cloned.cancel();
    });

    shutdown_token
}

async fn shutdown_signal() {
    let mut sigint = signal::unix::signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal::unix::signal(SignalKind::terminate()).unwrap();

    tokio::select! {
        _ = sigint.recv() => {
            tracing::info!("sigint received");
        },
        _ = sigterm.recv() => {
            tracing::info!("sigterm received");
        }
    }
}

#[derive(Clone)]
pub struct LifeCycle {
    pub task_tracker: TaskTracker,
    pub cancel_token: CancellationToken,
}

impl LifeCycle {
    pub fn spawn() -> LifeCycle {
        Self {
            task_tracker: TaskTracker::new(),
            cancel_token: spawn_shutdown_token(),
        }
    }
}
