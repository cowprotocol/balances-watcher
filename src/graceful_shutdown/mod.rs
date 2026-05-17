use tokio::{signal, signal::unix::SignalKind};
use tokio_util::sync::CancellationToken;

pub fn get_token() -> CancellationToken {
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
