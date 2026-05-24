use std::{fmt, sync::Arc};
use tokio::sync::broadcast;

/// Configuration for a topic.
#[derive(Clone, Copy, Debug)]
pub struct TopicConfig {
    /// Size of the broadcast ring buffer.
    pub capacity: usize,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self { capacity: 1024 }
    }
}

/// Typed publish/subscribe channel (replaces the old untyped Bus).
///
/// Under the hood this is `tokio::sync::broadcast` and is best for:
/// - "events" where only recent history matters
/// - fan-out to many consumers
///
/// For state propagation, prefer `tokio::sync::watch` (you can wrap similarly if you want).
#[derive(Clone)]
pub struct Topic<E> {
    tx: broadcast::Sender<Arc<E>>,
}

impl<E> Topic<E>
where
    E: Send + Sync + 'static,
{
    pub fn new(cfg: TopicConfig) -> Self {
        let (tx, _) = broadcast::channel(cfg.capacity);
        Self { tx }
    }

    pub fn publish(&self, event: E) -> Result<usize, TopicClosed> {
        self.tx
            .send(Arc::new(event))
            .map_err(|_| TopicClosed)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<E>> {
        self.tx.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TopicClosed;

impl fmt::Display for TopicClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("topic is closed")
    }
}

impl std::error::Error for TopicClosed {}