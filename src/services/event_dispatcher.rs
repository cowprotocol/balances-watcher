use crate::evm::erc20::ERC20;
use crate::graceful_shutdown::LifeCycle;
use crate::metrics::Metrics;
use crate::ws_connection::{ManagedWsSubscription, WsConnection};
use alloy::primitives::{Address, BlockNumber};
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

const ERC20_TRANSFER_TOPICS_LEN: usize = 3;

const POST_DISCONNECT_DELAY: Duration = Duration::from_millis(200);

pub struct Erc20TransferEvent {
    pub owner: Address,
    pub token: Address,
    pub block: Option<BlockNumber>,
}

pub struct EventDispatcher {
    cancel_token: CancellationToken,
    metrics: Arc<Metrics>,
    ws_connection: WsConnection,
    is_erc20_sub_connected: AtomicBool,
    transfer_tx_out: mpsc::Sender<Erc20TransferEvent>,
}

impl EventDispatcher {
    pub fn spawn(
        metrics: Arc<Metrics>,
        ws_connection: WsConnection,
        lifecycle: LifeCycle,
        transfer_tx_out: mpsc::Sender<Erc20TransferEvent>,
    ) -> Arc<Self> {
        let dispatcher = Arc::new(Self {
            metrics,
            ws_connection,
            cancel_token: lifecycle.cancel_token,
            is_erc20_sub_connected: AtomicBool::new(false),
            transfer_tx_out,
        });

        let dispatcher_for_spawn = Arc::clone(&dispatcher);
        lifecycle.task_tracker.spawn(async move {
            dispatcher_for_spawn.run_erc20_transfer_dispatcher().await;
        });

        dispatcher
    }

    pub async fn run_erc20_transfer_dispatcher(&self) {
        loop {
            if self.cancel_token.is_cancelled() {
                break;
            }

            let Some(stream) = self.subscribe_erc20_transfer().await else {
                tracing::info!("erc20 transfer subscription cancelled, exiting");
                break;
            };
            tracing::info!("erc20 subscription established, waiting for first header");

            tracing::info!("erc20 transfer subscription established, waiting");
            self.is_erc20_sub_connected.store(true, Ordering::Relaxed);

            self.consume_until_disconnect(stream).await;
            self.is_erc20_sub_connected.store(false, Ordering::Relaxed);

            tracing::info!(
                delay_ms = POST_DISCONNECT_DELAY.as_millis() as u64,
                "erc20 transfer subscription ended, will resubscribe after delay"
            );
            tokio::select! {
                _ = self.cancel_token.cancelled() => break,
                _ = tokio::time::sleep(POST_DISCONNECT_DELAY) => {},
            }
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.is_erc20_sub_connected.load(Ordering::Relaxed)
    }

    async fn consume_until_disconnect(&self, mut stream: ManagedWsSubscription<Log>) {
        loop {
            tokio::select! {
                _ = self.cancel_token.cancelled() => return,
                next = stream.next() => {
                    match next {
                        Some(log) => self.on_erc20_log(log).await,
                        None => {
                            self.metrics.ws_provider_disconnected_total.increment(1);
                            tracing::warn!("erc20 transfer stream terminated, subscription closed by server");
                            return;
                        }
                    }
                }
            }
        }
    }

    async fn on_erc20_log(&self, log: Log) {
        let topics = log.topics();
        if topics.len() != ERC20_TRANSFER_TOPICS_LEN {
            return;
        }

        self.metrics.erc20_event_received_total.increment(1);
        let from = Address::from_word(topics[1]);
        let to = Address::from_word(topics[2]);
        let token = log.address();
        let block = log.block_number;

        for owner in [from, to] {
            let _ = self
                .transfer_tx_out
                .send(Erc20TransferEvent {
                    token,
                    block,
                    owner,
                })
                .await;
        }
    }

    async fn subscribe_erc20_transfer(&self) -> Option<ManagedWsSubscription<Log>> {
        let filter = Filter::new().event_signature(ERC20::Transfer::SIGNATURE_HASH);
        self.ws_connection.subscribe_logs(&filter).await
    }
}
