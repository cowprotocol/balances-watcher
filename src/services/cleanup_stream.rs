use crate::domain::Session;
use crate::services::subscription_manager::SubscriptionManager;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct CleanupStream<S> {
    inner: Pin<Box<S>>,
    manager: Arc<SubscriptionManager>,
    session: Session,
    cleaned_up: bool,
}

impl<S> CleanupStream<S> {
    pub fn new(inner: S, manager: Arc<SubscriptionManager>, session: Session) -> Self {
        Self {
            inner: Box::pin(inner),
            manager,
            session,
            cleaned_up: false,
        }
    }
}

impl<S: Stream> Stream for CleanupStream<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl<S> Drop for CleanupStream<S> {
    fn drop(&mut self) {
        if !self.cleaned_up {
            self.cleaned_up = true;
            let manager = Arc::clone(&self.manager);
            let session = self.session;
            tokio::spawn(async move {
                let _ = manager.unsubscribe(&session).await.inspect_err(|err| {
                    tracing::error!(
                        error = %err,
                        session = %session,
                        "error when unsubscribe",
                    );
                });
            });
        }
    }
}
