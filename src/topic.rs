use async_broadcast::{Receiver, Sender, broadcast};
use dashmap::DashMap;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

/// Represents a single topic.
/// Contains information about how many subscribers it has and the inner broadcast sender & receivers
pub(crate) struct Topic {
    subscribers: AtomicUsize,
    sender: Sender<Arc<[u8]>>,
}

/// A map keeping track of the existing topics.
///
/// It contains the logic that understands when to create or delete a topic.
///
/// It makes use of a concurrent map that can be safely shared between threads.
pub(crate) struct TopicMap {
    inner: DashMap<String, Topic>,
    topic_capacity: usize,
}

impl TopicMap {
    pub(crate) fn new(topic_capacity: usize) -> Self {
        Self {
            inner: DashMap::new(),
            topic_capacity,
        }
    }

    pub(crate) fn get_sender(&self, topic_name: &str) -> Option<Sender<Arc<[u8]>>> {
        self.inner.get(topic_name).map(|topic| topic.sender.clone())
    }

    pub(crate) fn new_subscriber(&self, topic_name: &str) -> Receiver<Arc<[u8]>> {
        if let Some(topic) = self.inner.get(topic_name) {
            topic
                .subscribers
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            return topic.sender.new_receiver();
        }

        let (tx, rx) = broadcast::<Arc<[u8]>>(self.topic_capacity);

        let topic = Topic {
            subscribers: AtomicUsize::new(1),
            sender: tx,
        };

        self.inner.insert(topic_name.to_string(), topic);

        rx
    }

    pub(crate) fn remove_subscriber(&self, topic_name: &str) {
        // Scope the guard so it drops before we call `remove`.
        //
        // This is done because trying to call `remove` while we hold a read guard
        // from the DashMap will result in a deadlock.
        let subscribers_count = {
            let Some(topic_ref) = self.inner.get(topic_name) else {
                return;
            };

            topic_ref.subscribers.fetch_sub(1, Ordering::Relaxed)
        };

        if subscribers_count <= 1 {
            let _ = self.inner.remove(topic_name);
        }
    }
}
