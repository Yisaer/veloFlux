use std::collections::HashMap;

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
    state: Mutex<BackpressureHubState<T>>,
}

impl<T> BackpressureHub<T>
where
    T: Clone + Send + 'static,
{
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            state: Mutex::new(BackpressureHubState {
                closed: false,
                next_subscriber_id: 0,
                subscribers: HashMap::new(),
            }),
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
        state.subscribers.insert(subscriber_id, tx);
        Ok(rx)
    }

    pub(crate) async fn send(&self, item: T) -> Result<(), BackpressureHubClosed> {
        let mut state = self.state.lock().await;
        if state.closed {
            return Err(BackpressureHubClosed);
        }

        let subscriber_ids = state.subscribers.keys().copied().collect::<Vec<_>>();
        let mut closed_ids = Vec::new();
        for subscriber_id in subscriber_ids {
            let Some(sender) = state.subscribers.get(&subscriber_id).cloned() else {
                continue;
            };
            if sender.send(item.clone()).await.is_err() {
                closed_ids.push(subscriber_id);
            }
        }

        for subscriber_id in closed_ids {
            state.subscribers.remove(&subscriber_id);
        }
        Ok(())
    }

    pub(crate) async fn close(&self, terminal: Option<T>) {
        let mut state = self.state.lock().await;
        if state.closed {
            return;
        }
        state.closed = true;

        let subscriber_ids = state.subscribers.keys().copied().collect::<Vec<_>>();
        if let Some(item) = terminal {
            let mut closed_ids = Vec::new();
            for subscriber_id in subscriber_ids {
                let Some(sender) = state.subscribers.get(&subscriber_id).cloned() else {
                    continue;
                };
                if sender.send(item.clone()).await.is_err() {
                    closed_ids.push(subscriber_id);
                }
            }
            for subscriber_id in closed_ids {
                state.subscribers.remove(&subscriber_id);
            }
        }

        state.subscribers.clear();
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BackpressureHubClosed;
