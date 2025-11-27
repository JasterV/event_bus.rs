#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))]

use crate::{rc_map::RcMap, subscription::Subscription};
use async_broadcast::{Receiver, Sender, broadcast};
use std::sync::Arc;

const DEFAULT_TOPIC_CAPACITY: usize = 1000;

mod rc_map;
mod subscription;

// Wrapper around a sender and a receiver.
//
// This type is useful to make sure that a channel is never closed.
// By holding always a strong reference to a Sender and a Receiver, the channel will never close.
#[derive(Clone)]
#[allow(dead_code)]
struct Channel(pub Sender<Arc<[u8]>>, pub Receiver<Arc<[u8]>>);

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

/// A thread-safe event bus.
///
/// Users can subscribe to topics and publish to them.
/// When all the subscriptions to a topic get dropped, the topic itself is dropped from memory.
#[derive(Clone)]
pub struct EventBus {
    inner: RcMap<Arc<str>, Channel>,
    topic_capacity: usize,
}

impl Default for EventBus {
    fn default() -> Self {
        EventBus {
            inner: RcMap::new(),
            topic_capacity: DEFAULT_TOPIC_CAPACITY,
        }
    }
}

impl EventBus {
    pub fn new() -> EventBus {
        Self::default()
    }

    pub fn new_with_topic_capacity(topic_capacity: usize) -> EventBus {
        let mut bus = EventBus::new();
        bus.topic_capacity = topic_capacity;
        bus
    }

    /// Subscribes to a topic and returns a `Subscription`.
    ///
    /// This operation will never fail: If the topic doesn't exist it gets internally created.
    ///
    /// Once the subscription goes out of scope and there are no more references to the given topic,
    /// the topic will automatically be dropped from memory.
    pub fn subscribe(&self, topic: &str) -> Subscription {
        if let Some(object_ref) = self.inner.get(topic.into()) {
            return Subscription::from(object_ref);
        }

        let (tx, rx) = broadcast(self.topic_capacity);

        let object_ref = self.inner.insert(topic.into(), Channel(tx, rx));

        Subscription::from(object_ref)
    }

    /// Publishes a bunch of bytes to a topic.
    ///
    /// If the topic doesn't exist, nothing will happen and it won't be considered an error.
    /// This method will only fail if, in case a topic with the given name exists, the internal `send` operation fails.
    pub fn publish(&self, topic: &str, data: &[u8]) -> Result<(), PublishError> {
        let Some(object_ref) = self.inner.get(topic.into()) else {
            return Ok(());
        };

        let Channel(tx, _) = object_ref.value();

        let result = tx.try_broadcast(Arc::from(data));

        match result {
            Ok(_) => Ok(()),
            // There are no active receivers, we do not consider this an error
            Err(async_broadcast::TrySendError::Inactive(_)) => Ok(()),
            // The channel is closed, we return an error as this is unexpected
            Err(async_broadcast::TrySendError::Closed(_)) => {
                Err(PublishError::ChannelClosed(topic.into()))
            }
            // The channel is overflown, we return an error
            Err(async_broadcast::TrySendError::Full(_)) => {
                Err(PublishError::CapacityOverflow(topic.into()))
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
