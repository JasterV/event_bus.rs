//! An implementation of a thread-safe event bus.
//!
//! It provides the following features:
//!
//! - Users can publish messages to a topic
//! - Users can subscribe a topic and listen for incoming events.
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
/// It contains the logic that understands when to create or delete a topic.
///
/// It makes use of a concurrent map that can be safely shared between threads.
struct TopicMap(DashMap<String, Topic>);

impl TopicMap {
    fn get_sender(&self, topic_name: &str) -> Option<Tx> {
        self.0.get(topic_name).map(|topic| topic.sender.clone())
    }

    fn new_subscriber(&self, topic_name: &str) -> Rx {
        if let Some(topic) = self.0.get(topic_name) {
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

        self.0.insert(topic_name.to_string(), topic);

        rx
    }

    fn remove_subscriber(&self, topic_name: &str) {
        let Some(topic) = self.0.get(topic_name) else {
            return ();
        };

        let count = topic.subscribers.fetch_sub(1, Ordering::Relaxed);

        if count <= 1 {
            let _ = topic.sender.downgrade();
            let _ = self.0.remove(topic_name);
        }
    }
}

pub struct Subscription {
    topic: Arc<str>,
    inner_topics: Weak<TopicMap>,
    receiver: Rx,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(topics) = self.inner_topics.upgrade() {
            topics.remove_subscriber(&self.topic);
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PublishError {
    #[error("Failed to publish message to topic: '{0}': '{1}")]
    BroadcastError(Arc<str>, broadcast::error::SendError<Arc<[u8]>>),
}

/// A thread-safe event bus.
///
/// Users can subscribe to topics and publish to them.
///
/// When all the subscriptions to a topic get dropped, the topic itself is dropped from memory.
pub struct EventBus {
    topics: Arc<TopicMap>,
}

impl EventBus {
    /// Subscribes to a topic and returns a `Subscription`.
    ///
    /// This operation will never fail, if the topic doesn't exist it gets internally created.
    /// Once the subscription goes out of scope and there are no more references to the given topic,
    /// the topic will automatically be dropped from memory.
    pub fn subscribe(&self, topic: &str) -> Subscription {
        let rx = self.topics.new_subscriber(topic);

        Subscription {
            topic: topic.into(),
            inner_topics: Arc::downgrade(&self.topics),
            receiver: rx,
        }
    }

    /// Publishes a bunch of bytes to a topic.
    ///
    /// If the topic doesn't exist, nothing will happen and it won't be considered an error.
    /// This method will only fail if, in case a topic with the given name exists, the internal `send` operation fails.
    pub fn publish(&self, topic: &str, data: &[u8]) -> Result<(), PublishError> {
        let Some(sx) = self.topics.get_sender(topic) else {
            return Ok(());
        };

        let _ = sx
            .send(Arc::from(data))
            .map_err(|err| PublishError::BroadcastError(topic.into(), err))?;

        Ok(())
    }
}
