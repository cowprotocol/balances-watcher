use crate::config::constants::CALL_QUEUE_DELAY;
use crate::domain::Session;
use crate::services::errors::ServiceError;
use crate::services::fetch_balances_via_multicall::{
    fetch_balances_via_multicall, BalanceCallCtx, BalancesWithBlock,
};
use alloy::eips::BlockId;
use alloy::primitives::{Address, BlockNumber};
use alloy::providers::DynProvider;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::time::sleep;

type TokenMap = HashMap<Address, BlockNumber>;

pub enum QueueMessage {
    Success(BalancesWithBlock),
    Error(ServiceError),
}

type Sender = mpsc::Sender<QueueMessage>;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Status {
    None,
    InFlight,
    Scheduled,
}

struct QueueState {
    pending: TokenMap,
    status: Status,
}

pub struct CallsQueue {
    session: Session,
    http_provider: Arc<DynProvider>,
    state: RwLock<QueueState>,
    tx: Arc<Sender>,
}

impl CallsQueue {
    pub fn new(session: Session, provider: Arc<DynProvider>, tx: Sender) -> Self {
        Self {
            session,
            http_provider: provider,
            state: RwLock::new(QueueState {
                pending: HashMap::new(),
                status: Status::None,
            }),
            tx: Arc::new(tx),
        }
    }

    // upsert tokens to the queue with block_number for a delayed call
    pub async fn upsert_delayed_call(
        self: Arc<Self>,
        tokens: &[Address],
        block_number: Option<BlockNumber>,
    ) {
        let should_schedule = {
            let mut state = self.state.write().await;

            // put zero if there is no block_number (if its zero - it means we should request them with "Latest" block id)
            // otherwise we always use latest block_number
            let block_number = block_number.unwrap_or(0);

            for token in tokens {
                state
                    .pending
                    .entry(*token)
                    .and_modify(|curr_block| {
                        *curr_block = std::cmp::max(*curr_block, block_number);
                    })
                    .or_insert(block_number);
            }

            match state.status {
                Status::None => {
                    state.status = Status::Scheduled;
                    true
                }
                _ => false,
            }
        };

        if should_schedule {
            let this = Arc::clone(&self);
            let session = self.session;

            let task = tokio::spawn(async move {
                let _ = this.flush().await.inspect_err(|err| {
                    tracing::error!(
                        error = %err,
                        session = %session,
                        "Error upserting delayed call"
                    );
                });
            });
            // we don't need to wait response from this task
            // we should fire it and goes further without waiting the delay
            drop(task);
        }
    }

    // request balances for tokens in queue (per session) with delay
    async fn flush(self: Arc<Self>) -> Result<(), ServiceError> {
        loop {
            sleep(CALL_QUEUE_DELAY).await;
            let tokens = {
                let mut state = self.state.write().await;

                if state.pending.is_empty() {
                    state.status = Status::None;
                    return Ok(());
                }

                state.status = Status::InFlight;
                std::mem::take(&mut state.pending)
            };

            self.process_batch(tokens).await?;

            let should_reschedule = {
                let mut state = self.state.write().await;

                if !state.pending.is_empty() {
                    state.status = Status::Scheduled;
                    true
                } else {
                    state.status = Status::None;
                    false
                }
            };

            if !should_reschedule {
                return Ok(());
            }
        }
    }

    async fn process_batch(&self, batch: TokenMap) -> Result<(), ServiceError> {
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

        let call_ctx = Arc::new(BalanceCallCtx {
            session: self.session,
            provider: Arc::clone(&self.http_provider),
        });

        let result = fetch_balances_via_multicall(call_ctx, &tokens, block_id).await;
        let msg = match result {
            Ok(response) => QueueMessage::Success(response),
            Err(err) => {
                tracing::error!(
                    error = ?err,
                    session = %self.session,
                    "calls_queue: failed to send result"
                );

                QueueMessage::Error(err)
            }
        };

        self.tx
            .send(msg)
            .await
            .map_err(|err| ServiceError::ErrorToSend(err.to_string()))
    }
}
