//! Runtime agnostic async implementation of a thread-safe event bus.
//!
//! It provides the following features:
//!
//! - Users can publish messages to a topic
//! - Users can subscribe a topic and listen for incoming events.
//!
//! Messages are published as bytes, it is responsibility of the user to perform the encoding and decoding.
use async_broadcast::{Receiver, Sender, broadcast};
use dashmap::DashMap;
use futures::Stream;
use std::{
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

const DEFAULT_TOPIC_CAPACITY: usize = 1000;

type Payload = Arc<[u8]>;
type Tx = Sender<Payload>;
type Rx = Receiver<Payload>;

/// Represents a single topic.
/// Contains information about how many subscribers it has and the inner broadcast sender & receivers
struct Topic {
    subscribers: AtomicUsize,
    sender: Tx,
}

/// A map keeping track of the existing topics.
///
/// It contains the logic that understands when to create or delete a topic.
///
/// It makes use of a concurrent map that can be safely shared between threads.
struct TopicMap {
    inner: DashMap<String, Topic>,
    topic_capacity: usize,
}

impl TopicMap {
    fn new(topic_capacity: usize) -> Self {
        Self {
            inner: DashMap::new(),
            topic_capacity,
        }
    }

    fn get_sender(&self, topic_name: &str) -> Option<Tx> {
        self.inner.get(topic_name).map(|topic| topic.sender.clone())
    }

    fn new_subscriber(&self, topic_name: &str) -> Rx {
        if let Some(topic) = self.inner.get(topic_name) {
            topic
                .subscribers
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            return topic.sender.new_receiver();
        }

        let (tx, rx) = broadcast::<Payload>(self.topic_capacity);

        let topic = Topic {
            subscribers: AtomicUsize::new(1),
            sender: tx,
        };

        self.inner.insert(topic_name.to_string(), topic);

        rx
    }

    fn remove_subscriber(&self, topic_name: &str) {
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

/// Holds a subscription to a topic.
///
/// `Subscription` implements `Stream`, which means that the user can consume it as a stream
/// to listen for incoming messages.
///
/// Given the nature of the pub-sub architecture and the fact that anyone might be able to publish
/// to any topic at any time, the right to close the channel remains on the subscription side.
///
/// This means, a subscription will always remain open and it won't be closed from the publishers side.
///
/// The internal channel will only be closed when all the subscriptions for a given topic have been dropped.
///
/// When the subscription is dropped, the parent topic might be deallocated from memory if no other subscriptions to it exist.
pub struct Subscription {
    topic: Arc<str>,
    inner_topics: Weak<TopicMap>,
    rx: Rx,
}

impl Stream for Subscription {
    type Item = Payload;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.rx).poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(topics) = self.inner_topics.upgrade() {
            topics.remove_subscriber(&self.topic);
        }
    }
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
    pub fn new() -> Self {
        Self {
            topic_capacity: DEFAULT_TOPIC_CAPACITY,
        }
    }

    pub fn with_topic_capacity(self, topic_capacity: usize) -> Self {
        Self { topic_capacity }
    }

    pub fn build(self) -> EventBus {
        EventBus {
            topics: Arc::new(TopicMap::new(self.topic_capacity)),
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
            rx,
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

        let result = sx.try_broadcast(Arc::from(data));

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
        let event_bus = EventBusBuilder::new().build();
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
