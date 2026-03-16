use std::sync::Arc;
use std::collections::BinaryHeap;
use std::cmp::{Ordering, Reverse};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::common::Message;
use crate::storage::SegmentStore;

pub struct DelayQueue {
    inner: Mutex<DelayQueueInner>,
    notify: mpsc::Sender<()>,
    notify_rx: Option<mpsc::Receiver<()>>,
}

struct DelayQueueInner {
    heap: BinaryHeap<Reverse<DelayedMessage>>,
}

#[derive(Debug)]
struct DelayedMessage {
    message: Message,
    deliver_at: Instant,
}

impl PartialEq for DelayedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at == other.deliver_at
    }
}

impl Eq for DelayedMessage {}

impl PartialOrd for DelayedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DelayedMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        self.deliver_at.cmp(&other.deliver_at)
    }
}

impl DelayQueue {
    pub fn new() -> Self {
        let (notify, notify_rx) = mpsc::channel(1);
        
        Self {
            inner: Mutex::new(DelayQueueInner {
                heap: BinaryHeap::new(),
            }),
            notify,
            notify_rx: Some(notify_rx),
        }
    }

    pub fn schedule(&self, message: Message, delay: Duration) {
        let deliver_at = Instant::now() + delay;
        
        let delayed = DelayedMessage {
            message,
            deliver_at,
        };

        self.inner.lock().heap.push(Reverse(delayed));
        
        let _ = self.notify.try_send(());
    }

    pub async fn run(&self, storage: Arc<SegmentStore>) {
        let mut notify_rx = self.notify_rx.take().unwrap();

        loop {
            let wait_duration = {
                let inner = self.inner.lock();
                if let Some(Reverse(top)) = inner.heap.peek() {
                    let now = Instant::now();
                    if top.deliver_at <= now {
                        Duration::ZERO
                    } else {
                        top.deliver_at - now
                    }
                } else {
                    Duration::from_secs(60)
                }
            };

            if wait_duration == Duration::ZERO {
                let delayed = {
                    let mut inner = self.inner.lock();
                    inner.heap.pop()
                };

                if let Some(Reverse(mut delayed)) = delayed {
                    delayed.message.deliver_at = 0;
                    if let Err(e) = storage.append(&delayed.message) {
                        tracing::error!("Failed to deliver delayed message: {}", e);
                    }
                }
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(wait_duration) => {}
                    _ = notify_rx.recv() => {}
                }
            }
        }
    }
}
