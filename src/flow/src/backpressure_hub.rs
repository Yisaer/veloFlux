use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};

#[derive(Debug)]
struct BackpressureHubState<T> {
    closed: bool,
    next_subscriber_id: usize,
    subscribers: HashMap<usize, mpsc::Sender<T>>,
}

/// A no-drop in-process fan-out hub.
///
/// Every subscriber owns an independent bounded queue. Delivery waits for each active subscriber
/// so lagging consumers apply backpressure upstream instead of losing messages.
pub(crate) struct BackpressureHub<T> {
    capacity: usize,
    state: Arc<Mutex<BackpressureHubState<T>>>,
}

impl<T> BackpressureHub<T>
where
    T: Clone + Send + 'static,
{
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            state: Arc::new(Mutex::new(BackpressureHubState {
                closed: false,
                next_subscriber_id: 0,
                subscribers: HashMap::new(),
            })),
        }
    }

    pub(crate) async fn subscribe(&self) -> Result<mpsc::Receiver<T>, BackpressureHubClosed> {
        let mut state = self.state.lock().await;
        if state.closed {
            return Err(BackpressureHubClosed);
        }

        let subscriber_id = state.next_subscriber_id;
        state.next_subscriber_id = state.next_subscriber_id.saturating_add(1);
        let (tx, rx) = mpsc::channel(self.capacity);
        let cleanup_tx = tx.clone();
        state.subscribers.insert(subscriber_id, tx);
        let state = Arc::downgrade(&self.state);
        tokio::spawn(async move {
            cleanup_tx.closed().await;
            let Some(state) = state.upgrade() else {
                return;
            };

            let mut state = state.lock().await;
            state.subscribers.remove(&subscriber_id);
        });
        Ok(rx)
    }

    pub(crate) async fn send(&self, item: T) -> Result<(), BackpressureHubClosed> {
        let subscribers = {
            let state = self.state.lock().await;
            if state.closed {
                return Err(BackpressureHubClosed);
            }

            state
                .subscribers
                .iter()
                .map(|(subscriber_id, sender)| (*subscriber_id, sender.clone()))
                .collect::<Vec<_>>()
        };

        let mut closed_ids = Vec::new();
        for (subscriber_id, sender) in subscribers {
            if sender.send(item.clone()).await.is_err() {
                closed_ids.push(subscriber_id);
            }
        }

        if !closed_ids.is_empty() {
            let mut state = self.state.lock().await;
            for subscriber_id in closed_ids {
                state.subscribers.remove(&subscriber_id);
            }
        }
        Ok(())
    }

    pub(crate) async fn close(&self, terminal: Option<T>) {
        let subscribers = {
            let mut state = self.state.lock().await;
            if state.closed {
                return;
            }
            state.closed = true;
            state.subscribers.drain().collect::<Vec<_>>()
        };

        if let Some(item) = terminal {
            for (_, sender) in subscribers {
                let _ = sender.send(item.clone()).await;
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BackpressureHubClosed;
