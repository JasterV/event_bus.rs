#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))]

use crate::{
    rc_map::{InsertError, RcMap},
    subscription::Subscription,
};
use async_broadcast::{Sender, broadcast};
use std::sync::Arc;

/// The default topic capacity, it has been set to this value
/// for no particular reason, it is recommended that users
/// set their preferred value.
pub const DEFAULT_TOPIC_CAPACITY: usize = 1000;

mod rc_map;
mod subscription;

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
    inner: RcMap<Arc<str>, Sender<Arc<[u8]>>>,
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
        let (tx, rx) = broadcast(self.topic_capacity);

        match self.inner.insert(topic.into(), tx) {
            Ok(object_ref) => {
                // If in the moment of the channel creation, either the sender or the receiver get dropped, the channel will immediately be closed.
                //
                // This is why we are not using `Subscription::from(object_ref)` in this scenario
                // but rather we must make use of the receiver created or the channel will be closed.
                Subscription::new_with_rx(object_ref, rx)
            }
            // In this case we are fine with the new channel we created being dropped.
            // Since a channel already exists for this key we don't need to store the receiver and we can let the channel be closed.
            Err(InsertError::AlreadyExists(_key, object_ref)) => Subscription::from(object_ref),
        }
    }

    /// Publishes a bunch of bytes to a topic.
    ///
    /// If the topic doesn't exist, nothing will happen and it won't be considered an error.
    /// This method will only fail if, in case a topic with the given name exists, the internal `send` operation fails.
    pub fn publish(&self, topic: &str, data: &[u8]) -> Result<(), PublishError> {
        let Some(object_ref) = self.inner.get(topic.into()) else {
            return Ok(());
        };

        let tx = object_ref.value();
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
    use rand::{RngCore, SeedableRng, rngs::StdRng};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multithreaded_pub_sub() {
        let event_bus = EventBus::new();
        let topic = "test_simple";
        let expected_message = b"Hello EventBus";

        let mut subscription = event_bus.subscribe(topic);

        let task_handle = tokio::spawn(async move { subscription.next().await.unwrap() });

        event_bus.publish(topic, expected_message).unwrap();

        let received = task_handle
            .await
            .expect("Failed to receive result from task");

        assert_eq!(&*received, expected_message);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_publish_to_nonexistent_topic() {
        let bus = EventBus::new();
        let result = bus.publish("missing_topic", b"ignored");
        // Should not error, just ignored silently
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_subscribers_receive() {
        let topic = "multi_subs";
        let bus = EventBus::new();

        let mut s1 = bus.subscribe(topic);
        let mut s2 = bus.subscribe(topic);

        bus.publish(topic, b"msg").unwrap();

        let r1 = s1.next().await.unwrap();
        let r2 = s2.next().await.unwrap();

        assert_eq!(&*r1, b"msg");
        assert_eq!(&*r2, b"msg");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_topic_removed_when_no_subscribers() {
        let bus = EventBus::new();
        let topic = "temp_topic";

        {
            let mut sub = bus.subscribe(topic);
            // Topic exists so publish succeeds
            bus.publish(topic, b"hello").unwrap();
            // Assert the message was received
            let r1 = sub.next().await.unwrap();
            assert_eq!(&*r1, b"hello");
            // The topic will be cleaned up here
        }

        // The topic doesn't exist anymore → publish should silently no-op
        let result = bus.publish(topic, b"nobody_listens");
        // No err: behaves like non-existent topic
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_capacity_overflow() {
        let topic = "overflow_test";
        let bus = EventBus::new_with_topic_capacity(1);

        let mut sub = bus.subscribe(topic);

        // Fill buffer with one message
        bus.publish(topic, b"A").unwrap();

        // Second publish should overflow → Err(CapacityOverflow)
        let err = bus.publish(topic, b"B").unwrap_err();

        matches!(err, PublishError::CapacityOverflow(_));

        // Drain the first message to help debugging if needed
        let _ = sub.next().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_topics_isolation() {
        let bus = EventBus::new();

        let mut sub_a = bus.subscribe("A");
        let mut sub_b = bus.subscribe("B");

        bus.publish("A", b"msgA").unwrap();
        bus.publish("A", b"msgC").unwrap();
        bus.publish("B", b"msgB").unwrap();
        bus.publish("B", b"msgD").unwrap();

        let recv_a = sub_a.next().await.unwrap();
        let recv_c = sub_a.next().await.unwrap();
        let recv_b = sub_b.next().await.unwrap();
        let recv_d = sub_b.next().await.unwrap();

        assert_eq!(&*recv_a, b"msgA");
        assert_eq!(&*recv_c, b"msgC");
        assert_eq!(&*recv_b, b"msgB");
        assert_eq!(&*recv_d, b"msgD");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stress_test_concurrent_publishers() {
        const TOPIC: &str = "stress_topic";
        const PUBLISHERS: usize = 20;
        const MSGS_PER_PUBLISHER: usize = 200;
        const TOTAL_MSGS: usize = PUBLISHERS * MSGS_PER_PUBLISHER;

        let bus = EventBus::new();
        let mut sub = bus.subscribe(TOPIC);

        // Deterministic RNG for reproducibility
        let mut rng = StdRng::seed_from_u64(12345);

        // Pre-generate all messages and expected results
        let messages: Vec<Arc<[u8]>> = (0..TOTAL_MSGS)
            .map(|_| rng.next_u64().to_le_bytes().into())
            .collect();

        // Spawn publisher tasks
        let handles: Vec<_> = (0..PUBLISHERS)
            .map(|id| {
                let start = id * MSGS_PER_PUBLISHER;
                let end = start + MSGS_PER_PUBLISHER;
                let bus = bus.clone();
                let slice = messages[start..end].to_vec();

                tokio::spawn(async move {
                    for msg in slice {
                        bus.publish(TOPIC, &msg).unwrap();
                    }
                })
            })
            .collect();

        // Collect all messages in receiver
        let mut received = Vec::new();

        for _ in 0..TOTAL_MSGS {
            let msg = sub
                .next()
                .await
                .expect("Channel closed unexpectedly during stress test");

            received.push(msg.to_vec());
        }

        // Ensure all send tasks complete
        for h in handles {
            h.await.unwrap();
        }

        // Check we got all messages, sorted by content because async order varies
        let mut expected_sorted: Vec<_> = messages.clone().iter().map(|v| v.to_vec()).collect();
        expected_sorted.sort();
        received.sort();

        assert_eq!(
            received, expected_sorted,
            "Message mismatch under stress load!"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stress_test_multiple_subscribers() {
        const TOPIC: &str = "stress_multi_subs";
        const PUBLISHERS: usize = 10;
        const SUBSCRIBERS: usize = 15;
        const MSGS_PER_PUBLISHER: usize = 150;
        const TOTAL: usize = PUBLISHERS * MSGS_PER_PUBLISHER;

        let bus = EventBus::new();

        // Deterministic RNG seed
        let mut rng = StdRng::seed_from_u64(9999);

        // Random messages corpus
        let messages: Vec<Arc<[u8]>> = (0..TOTAL)
            .map(|_| rng.next_u64().to_le_bytes().into())
            .collect();

        // Create subscribers
        let subs: Vec<Subscription> = (0..SUBSCRIBERS).map(|_| bus.subscribe(TOPIC)).collect();

        // Spawn publishers
        let pub_handles = (0..PUBLISHERS)
            .map(|id| {
                let start = id * MSGS_PER_PUBLISHER;
                let end = start + MSGS_PER_PUBLISHER;
                let bus = bus.clone();
                let slice = messages[start..end].to_vec();

                tokio::spawn(async move {
                    for msg in slice {
                        bus.publish(TOPIC, &msg).unwrap();
                        // Bring the task randomly back to the runtime
                        if rand::random::<bool>() {
                            tokio::task::yield_now().await;
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        // Spawn subscriber collectors
        let sub_handles = subs
            .into_iter()
            .map(|mut sub| {
                tokio::spawn(async move {
                    let mut collected = Vec::with_capacity(TOTAL);

                    for _ in 0..TOTAL {
                        let msg = sub
                            .next()
                            .await
                            .expect("Channel closed unexpectedly during stress test");

                        collected.push(msg.to_vec());
                    }

                    collected
                })
            })
            .collect::<Vec<_>>();

        // Ensure publishers finish
        for h in pub_handles {
            h.await.unwrap();
        }

        // Collect all subscriber results
        let mut sub_results = Vec::new();
        for h in sub_handles {
            sub_results.push(h.await.unwrap());
        }

        // Validation (for each subscriber)
        for mut received in sub_results {
            // Asynchronous race means ordering is not enforced → sort
            received.sort();

            let mut expected: Vec<_> = messages.clone().into_iter().map(|v| v.to_vec()).collect();
            expected.sort();

            assert_eq!(
                received, expected,
                "Subscriber missed or corrupted messages",
            );
        }
    }
}
