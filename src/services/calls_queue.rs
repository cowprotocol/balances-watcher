use crate::config::constants::{CALL_QUEUE_DELAY, MAX_QUEUE_SIZE};
use crate::services::errors::ServiceError;
use crate::services::rpc_client::{BalancesWithBlock, RpcClient};
use alloy::eips::BlockId;
use alloy::primitives::{Address, BlockNumber};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::task::TaskTracker;

type TokenMap = HashMap<Address, BlockNumber>;

pub enum QueueMessage {
    Success(BalancesWithBlock),
    Error(ServiceError),
}

pub type QueueResultReceiver = mpsc::Receiver<QueueMessage>;

type TokenToFetch = (Address, Option<BlockNumber>);

pub struct CallsQueueHandle {
    tx_in: Arc<mpsc::Sender<TokenToFetch>>,
}

impl CallsQueueHandle {
    pub async fn upsert_delayed_rpc_call(&self, token: Address, block_number: Option<BlockNumber>) {
        let _ = self.tx_in.send((token, block_number)).await;
    }
}

pub struct CallsQueue {
    task_tracker: TaskTracker,
    owner: Address,
    rpc_client: Arc<RpcClient>,
}

impl CallsQueue {
    pub fn new(task_tracker: TaskTracker, owner: Address, rpc_client: Arc<RpcClient>) -> Self {
        Self {
            task_tracker,
            owner,
            rpc_client,
        }
    }

    pub fn run_queue(self) -> (CallsQueueHandle, mpsc::Receiver<QueueMessage>) {
        let (tx_in, rx_in) = mpsc::channel(64);
        let (tx_out, rx_out) = mpsc::channel::<QueueMessage>(64);

        self.task_tracker.clone().spawn(async move {
            let stream =
                ReceiverStream::new(rx_in).chunks_timeout(MAX_QUEUE_SIZE, CALL_QUEUE_DELAY);
            tokio::pin!(stream);

            while let Some(batch) = stream.next().await {
                let token_map = Self::fold_batch_vec_to_token_map(batch);
                let result = self.process_batch(token_map).await;
                let _ = tx_out.send(result).await;
            }
        });

        (
            CallsQueueHandle {
                tx_in: Arc::new(tx_in),
            },
            rx_out,
        )
    }

    fn fold_batch_vec_to_token_map(tokens_to_fetch_batch: Vec<TokenToFetch>) -> TokenMap {
        tokens_to_fetch_batch
            .into_iter()
            .fold(TokenMap::new(), |mut acc, item| {
                let (token_address, block_number) = item;
                let block_number = block_number.unwrap_or(0);

                // we should always keep the latest block number result
                acc.entry(token_address)
                    .and_modify(|saved_block_n| *saved_block_n = (*saved_block_n).max(block_number))
                    .or_insert(block_number);

                acc
            })
    }

    async fn process_batch(&self, batch: TokenMap) -> QueueMessage {
        let (latest_block, tokens) = batch.iter().fold(
            (0, Vec::<Address>::with_capacity(batch.len())),
            |(latest_block, mut tokens), (token, curr)| {
                tokens.push(*token);
                (std::cmp::max(latest_block, *curr), tokens)
            },
        );

        let block_id = if latest_block == 0 {
            BlockId::latest()
        } else {
            BlockId::from(latest_block)
        };

        tracing::debug!(
            "request balances for tokens {:?}, with block number: {:?}",
            tokens,
            block_id
        );

        let result = self
            .rpc_client
            .fetch_balances_via_multicall(self.owner, &tokens, block_id)
            .await;

        match result {
            Ok(response) => QueueMessage::Success(response),
            Err(err) => {
                tracing::error!(
                    error = ?err,
                    owner = %self.owner,
                    "calls_queue: failed to fetch balances"
                );

                QueueMessage::Error(err)
            }
        }
    }
}
