#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))]

use crate::subscription::Subscription;
use async_broadcast::{Sender, broadcast};
use dashmap::DashMap;
use std::sync::{Arc, atomic::AtomicUsize};

mod subscription;

const DEFAULT_TOPIC_CAPACITY: usize = 1000;

/// Represents a single topic.
/// Contains information about how many subscribers it has and the inner broadcast sender & receivers
pub struct Topic {
    subscribers: AtomicUsize,
    sender: Sender<Arc<[u8]>>,
}

/// Error type returned by the `publish` method.
#[derive(thiserror::Error, Debug)]
pub enum PublishError {
    #[error("Failed to publish message to topic, it was unexpectedly closed: '{0}'")]
    ChannelClosed(Arc<str>),
    #[error(
        "Failed to publish message to topic, the topic is full and can't handle more messages: '{0}'"
    )]
    CapacityOverflow(Arc<str>),
}

/// EventBus builder.
///
/// It is used to configure the event bus before building it.
pub struct EventBusBuilder {
    /// Topics are bounded to a capacity, if the capacity is overflown, messages can't be published.
    topic_capacity: usize,
}

impl EventBusBuilder {
    fn new() -> Self {
        Self {
            topic_capacity: DEFAULT_TOPIC_CAPACITY,
        }
    }

    pub fn with_topic_capacity(self, topic_capacity: usize) -> Self {
        Self { topic_capacity }
    }

    pub fn build(self) -> EventBus {
        EventBus {
            inner: Arc::new(DashMap::new()),
            topic_capacity: self.topic_capacity,
        }
    }
}

/// A thread-safe event bus.
///
/// Users can subscribe to topics and publish to them.
///
/// When all the subscriptions to a topic get dropped, the topic itself is dropped from memory.
#[derive(Clone)]
pub struct EventBus {
    inner: Arc<DashMap<String, Topic>>,
    topic_capacity: usize,
}

impl Default for EventBus {
    fn default() -> Self {
        EventBus::builder().build()
    }
}

impl EventBus {
    pub fn new() -> EventBus {
        Self::default()
    }

    pub fn builder() -> EventBusBuilder {
        EventBusBuilder::new()
    }

    /// Subscribes to a topic and returns a `Subscription`.
    ///
    /// This operation will never fail, if the topic doesn't exist it gets internally created.
    /// Once the subscription goes out of scope and there are no more references to the given topic,
    /// the topic will automatically be dropped from memory.
    pub fn subscribe(&self, topic_name: &str) -> Subscription {
        if let Some(topic) = self.inner.get(topic_name) {
            topic
                .subscribers
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            return Subscription {
                topic: topic_name.into(),
                topics_ref: Arc::downgrade(&self.inner),
                rx: topic.sender.new_receiver(),
            };
        }

        let (tx, rx) = broadcast::<Arc<[u8]>>(self.topic_capacity);

        let topic = Topic {
            subscribers: AtomicUsize::new(1),
            sender: tx,
        };

        self.inner.insert(topic_name.to_string(), topic);

        Subscription {
            topic: topic_name.into(),
            topics_ref: Arc::downgrade(&self.inner),
            rx,
        }
    }

    /// Publishes a bunch of bytes to a topic.
    ///
    /// If the topic doesn't exist, nothing will happen and it won't be considered an error.
    /// This method will only fail if, in case a topic with the given name exists, the internal `send` operation fails.
    pub fn publish(&self, topic_name: &str, data: &[u8]) -> Result<(), PublishError> {
        let Some(topic) = self.inner.get(topic_name) else {
            return Ok(());
        };

        let result = topic.sender.try_broadcast(Arc::from(data));

        match result {
            Ok(_) => Ok(()),
            // There are no active receivers, we do not consider this an error
            Err(async_broadcast::TrySendError::Inactive(_)) => Ok(()),
            // The channel is closed, we return an error as this is unexpected
            Err(async_broadcast::TrySendError::Closed(_)) => {
                Err(PublishError::ChannelClosed(topic_name.into()))
            }
            // The channel is overflown, we return an error
            Err(async_broadcast::TrySendError::Full(_)) => {
                Err(PublishError::CapacityOverflow(topic_name.into()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    // Helper function to safely extract a message from a stream
    async fn get_next_message(sub: &mut Subscription) -> String {
        let payload = sub.next().await.expect("Stream unexpectedly closed");
        String::from_utf8_lossy(&payload).to_string()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_simple_pub_sub() {
        let event_bus = EventBus::new();
        let topic = "test_simple";
        let expected_message = "Hello EventBus";

        let mut subscription = event_bus.subscribe(topic);

        let task_handle = tokio::spawn(async move { get_next_message(&mut subscription).await });

        event_bus
            .publish(topic, expected_message.as_bytes())
            .unwrap();

        let received = task_handle
            .await
            .expect("Failed to receive result from task");

        assert_eq!(received, expected_message);
    }
}
