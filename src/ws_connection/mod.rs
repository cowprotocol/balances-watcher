//! Dedicated WebSocket connection factory with infinite-retry reconnect.
//!
//! [`WsConnection`] owns a single WS URL, its metrics handle, and the
//! shutdown cancellation token. Each `subscribe_*` method establishes a
//! fresh `connect_ws` + `eth_subscribe` pair, retrying with exponential
//! backoff ([`MIN_RECONNECT_ATTEMPT_DELAY`] → [`MAX_RECONNECT_ATTEMPT_DELAY`],
//! jitter) until either the subscription succeeds or the cancellation
//! token fires.
//!
//! Subscriptions are returned as [`ManagedWsSubscription`], a thin RAII
//! wrapper that holds both the `DynProvider` and the
//! `SubscriptionStream<T>` together. Dropping the wrapper drops both,
//! preventing the Phase 0 "Pubsub service request channel closed" bug:
//! `connect_ws` returns a provider whose pubsub service stays alive only
//! as long as **any** `Provider` clone is held, so the stream's lifetime
//! must be bounded by the provider's.

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
use tokio_util::sync::CancellationToken;

const MIN_RECONNECT_ATTEMPT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_ATTEMPT_DELAY: Duration = Duration::from_secs(30);

/// RAII wrapper binding a [`SubscriptionStream`] to the [`DynProvider`]
/// that produced it. The provider is held privately for the lifetime of
/// the wrapper so the underlying pubsub service stays alive; dropping the
/// wrapper drops both halves together.
///
/// Implements [`futures::Stream`], so callers can poll items directly
/// without unwrapping the inner stream.
pub struct ManagedWsSubscription<T> {
    _provider: DynProvider,
    stream: SubscriptionStream<T>,
}

impl<T> futures::Stream for ManagedWsSubscription<T>
where
    T: DeserializeOwned + Send + 'static,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

/// Factory for dedicated WS subscriptions on a single URL.
///
/// Each call to a `subscribe_*` method establishes a brand-new WS
/// connection (separate socket) and returns a [`ManagedWsSubscription`].
/// Suitable for the "one socket per logical signal" pattern — give every
/// consumer its own `WsConnection` so failures and reconnect storms stay
/// isolated.
pub struct WsConnection {
    ws_url: String,
    metrics: Arc<Metrics>,
    cancel_token: CancellationToken,
}

impl WsConnection {
    /// Build a factory bound to `ws_url`. `cancel_token` is observed across
    /// the whole retry chain (in-flight connect/subscribe attempts are
    /// aborted on cancel, not just sleeps between retries).
    pub fn new(ws_url: String, metrics: Arc<Metrics>, cancel_token: CancellationToken) -> Self {
        Self {
            ws_url,
            metrics,
            cancel_token,
        }
    }

    /// Establish a `newHeads` subscription with infinite-retry reconnect.
    ///
    /// Returns `Some(subscription)` on success, or `None` if the cancel
    /// token fired before a connection could be established.
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

    /// Drive a `subscribe_fn` closure under infinite-retry exponential
    /// backoff, returning the first successful subscription or `None` on
    /// cancellation.
    ///
    /// The closure is handed a fresh [`DynProvider`] per attempt; it must
    /// return a [`ManagedWsSubscription`] holding both the provider and
    /// the resulting stream. The whole chain (in-flight attempts + sleeps)
    /// is aborted promptly when `self.cancel_token` fires.
    async fn with_retry<T, F, Fut>(&self, subscribe_fn: F) -> Option<ManagedWsSubscription<T>>
    where
        F: Fn(DynProvider) -> Fut + Send,
        Fut: Future<Output = Result<ManagedWsSubscription<T>, anyhow::Error>> + Send,
    {
        let ws_url = &self.ws_url;
        let attempt_to_connect = async || {
            let ws = WsConnect::new(ws_url.to_owned());
            let provider = ProviderBuilder::new().connect_ws(ws).await?;
            subscribe_fn(provider.erased()).await
        };

        let retried = (|| async { attempt_to_connect().await })
            .retry(Self::backoff())
            .notify(|err, duration| self.notify_reconnect_error(err, duration));

        tokio::select! {
            _ = self.cancel_token.cancelled() => None,
            res = retried => res.ok(),
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
