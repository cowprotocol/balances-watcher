use crate::domain::EvmNetwork;
use crate::metrics::Metrics;
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::pubsub::SubscriptionStream;
use alloy::rpc::types::Header;
use backon::{ExponentialBuilder, Retryable};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

const MIN_STALL_DURATION: Duration = Duration::from_secs(2);
const MIN_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);

type BlockStream = SubscriptionStream<Header>;

pub struct BlockWatcher {
    network: EvmNetwork,
    metrics: Arc<Metrics>,
    connected: AtomicBool,
    last_block_at_sec: AtomicU64,
}

impl BlockWatcher {
    pub fn spawn(
        network: EvmNetwork,
        metrics: Arc<Metrics>,
        task_tracker: TaskTracker,
        cancellation_token: CancellationToken,
        ws_url: String,
    ) -> Arc<Self> {
        let watcher = Arc::new(Self {
            network,
            metrics,
            connected: AtomicBool::new(false),
            last_block_at_sec: AtomicU64::new(0),
        });

        let watcher_for_spawn = Arc::clone(&watcher);
        task_tracker.spawn(async move {
            watcher_for_spawn.run(cancellation_token, ws_url).await;
        });

        watcher
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    async fn run(self: Arc<Self>, cancel: CancellationToken, ws_url: String) {
        loop {
            if cancel.is_cancelled() {
                break;
            }

            let ws_url = ws_url.clone();
            let Some(stream) = self.connect_with_retry(&cancel, ws_url).await else {
                break;
            };

            self.consume_until_disconnect(stream, &cancel).await;
            self.connected.store(false, Ordering::Relaxed);
        }
    }

    async fn consume_until_disconnect(&self, mut stream: BlockStream, cancel: &CancellationToken) {
        let stall_timeout = Self::stall_timeout(self.network.block_time());

        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                next = tokio::time::timeout(stall_timeout, stream.next()) => {
                    match next {
                        Ok(Some(header)) => self.record_header(header),
                        Ok(None) => {
                            self.metrics.ws_subscribe_is_down_total.increment(1);
                            tracing::warn!("stream terminated, subscription closed by server");
                            return;
                        },
                        Err(_) => {
                            tracing::warn!(stall_timeout = stall_timeout.as_secs(), "stream stalled");
                            return;
                        }
                    }
                }
            }
        }
    }

    fn notify_connection_error(&self, err: &anyhow::Error, reconnect_duration: Duration) {
        self.metrics.ws_provider_disconnected_total.increment(1);
        self.metrics
            .ws_reconnect_attempt_duration_ms
            .record(reconnect_duration);
        tracing::warn!(err = %err, "ws connect/subscribe failed, backing off");
    }

    fn record_header(&self, _: Header) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("SystemTime before UNIX EPOCH!?")
            .as_secs();

        self.last_block_at_sec.store(now, Ordering::Relaxed);
        self.connected.store(true, Ordering::Relaxed);
    }

    async fn connect_with_retry(
        &self,
        cancel: &CancellationToken,
        ws_url: String,
    ) -> Option<BlockStream> {
        let backoff = Self::backoff();

        tokio::select! {
            _ = cancel.cancelled() => None,
            res = (|| {
                let ws_url = ws_url.clone();
                async move { Self::attempt_to_connect(ws_url).await }
            })
            .retry(backoff)
            .notify(|err, duration| self.notify_connection_error(err, duration))
            .when(|_| !cancel.is_cancelled()) => res.ok()
        }
    }

    async fn attempt_to_connect(ws_url: String) -> Result<BlockStream, anyhow::Error> {
        let ws = WsConnect::new(ws_url);
        let provider = ProviderBuilder::new().connect_ws(ws).await?;
        let sub = provider.subscribe_blocks().await?;
        Ok::<_, anyhow::Error>(sub.into_stream())
    }

    fn stall_timeout(block_time: Duration) -> Duration {
        (block_time * 3).max(MIN_STALL_DURATION)
    }

    fn backoff() -> ExponentialBuilder {
        ExponentialBuilder::default()
            .with_min_delay(MIN_RECONNECT_DELAY)
            .with_max_delay(MAX_RECONNECT_DELAY)
            .with_jitter()
            // never stop
            .with_max_times(usize::MAX)
    }
}
