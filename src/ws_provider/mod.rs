use crate::metrics::Metrics;
use alloy::providers::{DynProvider, Provider, ProviderBuilder, WsConnect};
use alloy::pubsub::SubscriptionStream;
use alloy::rpc::types::Header;
use backon::{ExponentialBuilder, Retryable};
use serde::de::DeserializeOwned;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio_util::future::FutureExt;
use tokio_util::sync::CancellationToken;

const MIN_RECONNECT_ATTEMPT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_ATTEMPT_DELAY: Duration = Duration::from_secs(30);

pub struct ManagedWsSubscription<T> {
    _provider: DynProvider,
    stream: SubscriptionStream<T>,
}

impl<T: Unpin + 'static> futures::Stream for ManagedWsSubscription<T>
where
    T: DeserializeOwned + Send + 'static,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

pub struct WsProvider {
    ws_url: String,
    metrics: Arc<Metrics>,
    cancel_token: CancellationToken,
}

impl WsProvider {
    pub fn new(ws_url: String, metrics: Arc<Metrics>, cancel_token: CancellationToken) -> Self {
        Self {
            ws_url,
            metrics,
            cancel_token,
        }
    }

    pub async fn subscribe_blocks(&self) -> Option<ManagedWsSubscription<Header>> {
        self.with_retry(|provider| async move {
            let sub = provider.subscribe_blocks().await?;
            Ok(ManagedWsSubscription {
                _provider: provider,
                stream: sub.into_stream(),
            })
        })
        .await
    }

    async fn with_retry<T, F, Fut>(&self, subscribe_fn: F) -> Option<ManagedWsSubscription<T>>
    where
        T: Send + 'static,
        F: Fn(DynProvider) -> Fut + Send + Sync + Clone + 'static,
        Fut: Future<Output = Result<ManagedWsSubscription<T>, anyhow::Error>> + Send + 'static,
    {
        let backoff = Self::backoff();
        let ws_url = &self.ws_url;
        let attempt_to_connect = async || {
            let ws = WsConnect::new(ws_url.to_owned());
            let provider = ProviderBuilder::new().connect_ws(ws).await?;
            subscribe_fn(provider.erased()).await
        };

        let replied = (|| async { attempt_to_connect().await })
            .retry(backoff)
            .notify(|err, duration| self.notify_reconnect_error(err, duration))
            .when(|_| !self.cancel_token.is_cancelled());

        tokio::select! {
            _ = self.cancel_token.cancelled() => None,
            res = replied => res.ok(),
        }
    }

    fn notify_reconnect_error(&self, err: &anyhow::Error, reconnect_duration: Duration) {
        self.metrics.ws_reconnect_attempts_total.increment(1);
        self.metrics
            .ws_reconnect_attempt_duration_ms
            .record(reconnect_duration);
        tracing::warn!(err = %err, "ws connect/subscribe failed, backing off");
    }

    fn backoff() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(MIN_RECONNECT_ATTEMPT_DELAY)
            .with_max_delay(MAX_RECONNECT_ATTEMPT_DELAY)
            .with_jitter()
            .with_max_times(usize::MAX)
    }
}
