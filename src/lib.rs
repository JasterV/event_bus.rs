//! An implementation of a simple event bus.
//!
//! It provides the following features:
//!
//! - Users can publish messages to a topic
//! - Users can subscribe to events published to a certain topic.
//!
//! Messages are published as bytes, it is responsibility of the user to perform the encoding and decoding.
use dashmap::DashMap;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::broadcast;

const TOPIC_CAPACITY: usize = 20;

type Rx = broadcast::Receiver<Arc<[u8]>>;
type Tx = broadcast::Sender<Arc<[u8]>>;

/// Represents a single topic.
/// Contains information about how many subscribers it has and the inner broadcast sender & receivers
struct Topic {
    subscribers: AtomicUsize,
    sender: Tx,
    receiver: Rx,
}

/// A map keeping track of the existing topics.
///
/// It contains the logic that understands when to create or delete a topic completely.
///
/// It makes use of a concurrent map that can be safely shared between threads.
struct TopicMap {
    inner: Arc<DashMap<String, Topic>>,
}

impl TopicMap {
    pub(crate) fn new_subscriber(&self, topic_name: &str) -> Rx {
        if let Some(topic) = self.inner.get(topic_name) {
            topic
                .subscribers
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            return topic.receiver.resubscribe();
        }

        let (tx, rx) = broadcast::channel(TOPIC_CAPACITY);

        let topic = Topic {
            subscribers: AtomicUsize::new(1),
            sender: tx,
            receiver: rx.resubscribe(),
        };

        self.inner.insert(topic_name.to_string(), topic);

        rx
    }

    pub(crate) fn remove_subscriber(&self, topic_name: &str) {
        let Some(topic) = self.inner.get(topic_name) else {
            return ();
        };

        let count = topic.subscribers.fetch_sub(1, Ordering::Relaxed);

        if count <= 1 {
            let _ = topic.sender.downgrade();
            let _ = self.inner.remove(topic_name);
        }
    }
}

pub struct Subscription {
    topic: Arc<str>,
    inner_topic_map: TopicMap,
    receiver: Rx,
}

// Implementation ideas:
//
// I.
//
// The event bus contains a map of (&str, Topic)
//
// A Topic could contain:
//
// - Arc<The `sender` used to send message>s
// - The `receiver` that gets cloned
// - A counter that keeps track of how many subscribers there are.
//
// Every time someone subscribes to a topic, it get's created in case it doesn't exist and the subscribers counter is sself.inner.get
// If the topic already exists, the counter increases.
// If the subscribers counter goes down to 0, the topic is removed.
//
// For the counter to go down, a subcription reference has to be dropped.
//
// We need tofind a way to decrease the counter automatically once the "subscription" gets dropped.
// Then, somehow, when the counter get's down to 0, the topics map entry must be removed.
