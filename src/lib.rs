//! An implementation of a thread-safe event bus.
//!
//! It provides the following features:
//!
//! - Users can publish messages to a topic
//! - Users can subscribe a topic and listen for incoming events.
//!
//! Messages are published as bytes, it is responsibility of the user to perform the encoding and decoding.
use dashmap::DashMap;
use std::{
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, ready},
};
use tokio::sync::broadcast;
use tokio_stream::{
    Stream,
    wrappers::{BroadcastStream, errors::BroadcastStreamRecvError},
};

const TOPIC_CAPACITY: usize = 20;

type Payload = Arc<[u8]>;
type Rx = broadcast::Receiver<Payload>;
type Tx = broadcast::Sender<Payload>;

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
struct TopicMap(DashMap<String, Topic>);

impl TopicMap {
    fn new() -> Self {
        Self(DashMap::new())
    }

    fn get_sender(&self, topic_name: &str) -> Option<Tx> {
        self.0.get(topic_name).map(|topic| topic.sender.clone())
    }

    fn new_subscriber(&self, topic_name: &str) -> Rx {
        if let Some(topic) = self.0.get(topic_name) {
            topic
                .subscribers
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            return topic.sender.subscribe();
        }

        let (tx, rx) = broadcast::channel(TOPIC_CAPACITY);

        let topic = Topic {
            subscribers: AtomicUsize::new(1),
            sender: tx,
        };

        self.0.insert(topic_name.to_string(), topic);

        rx
    }

    fn remove_subscriber(&self, topic_name: &str) {
        // scope the guard so it drops before we call `remove`
        let should_remove = {
            let Some(topic_ref) = self.0.get(topic_name) else {
                return;
            };

            // decrement and decide if we need to remove the topic
            let prev = topic_ref.subscribers.fetch_sub(1, Ordering::Relaxed);
            prev <= 1
            // `topic_ref` is dropped here at the end of the block!
        };

        if should_remove {
            if let Some((_, topic)) = self.0.remove(topic_name) {
                // explicitly drop the sender (optional - remove already drops it)
                drop(topic.sender);
            }
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
    stream: BroadcastStream<Payload>,
}

/// An error returned from the subscription stream.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SubscriptionStreamRecvError {
    /// The receiver lagged too far behind. Attempting to receive again will
    /// return the oldest message still retained by the channel.
    ///
    /// Includes the number of skipped messages.
    Lagged(u64),
}

impl Stream for Subscription {
    type Item = Result<Payload, SubscriptionStreamRecvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Delegate polling to the inner BroadcastStream
        let item = ready!(Pin::new(&mut self.stream).poll_next(cx));

        match item {
            Some(Ok(payload)) => Poll::Ready(Some(Ok(payload))),

            Some(Err(BroadcastStreamRecvError::Lagged(n))) => {
                Poll::Ready(Some(Err(SubscriptionStreamRecvError::Lagged(n))))
            }

            None => Poll::Ready(None),
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

#[derive(thiserror::Error, Debug)]
pub enum PublishError {
    #[error("Failed to publish message to topic: '{0}': '{1}")]
    BroadcastError(Arc<str>, broadcast::error::SendError<Payload>),
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
    /// EventBus main constructor.
    pub fn new() -> Self {
        Self {
            topics: Arc::new(TopicMap::new()),
        }
    }

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
            stream: BroadcastStream::new(rx),
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;

    // Helper function to safely extract a message from a stream
    async fn get_next_message(
        sub: &mut Subscription,
    ) -> Result<String, SubscriptionStreamRecvError> {
        let result = sub
            .next()
            .await
            .expect("Stream unexpectedly closed")
            .map(|payload| String::from_utf8_lossy(&payload).to_string());

        result
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_simple_pub_sub() {
        let event_bus = EventBus::new();
        let topic = "test_simple";
        let expected_message = "Hello EventBus";

        let mut subscription = event_bus.subscribe(topic);

        let task_handle = tokio::spawn(async move { get_next_message(&mut subscription).await });

        // 3. Publish the message. This happens after the receiver is active.
        event_bus
            .publish(topic, expected_message.as_bytes())
            .unwrap();

        let received = task_handle
            .await
            .expect("Failed to receive result from task")
            .unwrap();

        assert_eq!(received, expected_message);
    }
}
