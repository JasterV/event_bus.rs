#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))]

mod publisher;
mod rc_map;
mod subscription;

use crate::{
    publisher::Publisher,
    rc_map::{InsertError, RcMap},
};
use async_broadcast::{InactiveReceiver, Sender, broadcast};
use std::sync::Arc;

/// Re-export Subscription
pub use subscription::Subscription;

/// The default topic capacity, it has been set to this value
/// for no particular reason, it is recommended that users
/// set their preferred value.
pub const DEFAULT_TOPIC_CAPACITY: usize = 1000;

/// Utility typed used to keep at least an instance of both a sender and a receiver
/// in memory so the channel doesn't get closed.
///
/// `async_broadcast` will drop a channel if either all the receivers or all the senders get dropped.
#[derive(Clone)]
struct Channel(pub Sender<Arc<[u8]>>, pub InactiveReceiver<Arc<[u8]>>);

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
    /// Once the subscription goes out of scope and there are no more references (either subscriptions or publishers) to the given topic,
    /// the topic will automatically be dropped from memory.
    pub fn subscribe(&self, topic: &str) -> Subscription {
        let (tx, rx) = broadcast(self.topic_capacity);
        let channel = Channel(tx, rx.deactivate());

        match self.inner.insert(topic.into(), channel) {
            Ok(object_ref) => Subscription::from(object_ref),
            // If the topic already exists, the previously created channel will be dropped and closed.
            Err(InsertError::AlreadyExists(_key, object_ref)) => Subscription::from(object_ref),
        }
    }

    /// Builds a `Publisher`.
    ///
    /// A `Publisher` holds a reference to the internal topic and is able to publish messages to it.
    ///
    /// Once the `Publisher` goes out of scope and there are no more references (either subscriptions or publishers) to the given topic,
    /// the topic will automatically be dropped from memory.
    pub fn publisher(&self, topic: &str) -> Publisher {
        let (tx, rx) = broadcast(self.topic_capacity);
        let channel = Channel(tx, rx.deactivate());

        match self.inner.insert(topic.into(), channel) {
            Ok(object_ref) => Publisher::from(object_ref),
            // If the topic already exists, the previously created channel will be dropped and closed.
            Err(InsertError::AlreadyExists(_key, object_ref)) => Publisher::from(object_ref),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::publisher::PublishError;

    use super::*;
    use fake::{Fake, Faker};
    use futures::StreamExt;
    use futures::stream;
    use proptest::prop_assert_eq;
    use test_strategy::proptest;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multithreaded_pub_sub() {
        let event_bus = EventBus::new();
        let topic = "test_simple";
        let expected_message = b"Hello EventBus";

        let mut subscription = event_bus.subscribe(topic);
        let publisher = event_bus.publisher(topic);

        let task_handle = tokio::spawn(async move { subscription.next().await.unwrap() });

        publisher.publish(expected_message).unwrap();

        let received = task_handle
            .await
            .expect("Failed to receive result from task");

        assert_eq!(&*received, expected_message);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_subscribers_receive() {
        let topic = "multi_subs";
        let bus = EventBus::new();

        let mut s1 = bus.subscribe(topic);
        let mut s2 = bus.subscribe(topic);
        let publisher = bus.publisher(topic);

        publisher.publish(b"msg").unwrap();

        let r1 = s1.next().await.unwrap();
        let r2 = s2.next().await.unwrap();

        assert_eq!(&*r1, b"msg");
        assert_eq!(&*r2, b"msg");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_topic_removed_when_no_refs() {
        let bus = EventBus::new();
        let topic = "temp_topic";

        {
            let mut sub = bus.subscribe(topic);
            let publisher = bus.publisher(topic);
            publisher.publish(b"hello").unwrap();
            // Assert the message was received
            let r1 = sub.next().await.unwrap();
            assert_eq!(&*r1, b"hello");
            // The topic will be cleaned up here
        }

        let publisher = bus.publisher(topic);
        let result = publisher.publish(b"nobody_listens");
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_capacity_overflow() {
        let topic = "overflow_test";
        let bus = EventBus::new_with_topic_capacity(1);

        let mut sub = bus.subscribe(topic);
        let publisher = bus.publisher(topic);

        // Fill buffer with one message
        publisher.publish(b"A").unwrap();

        // Second publish should overflow → Err(CapacityOverflow)
        let err = publisher.publish(b"B").unwrap_err();

        matches!(err, PublishError::CapacityOverflow(_));

        // Drain the first message to help debugging if needed
        let _ = sub.next().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_topics_isolation() {
        let bus = EventBus::new();

        let mut sub_a = bus.subscribe("A");
        let mut sub_b = bus.subscribe("B");
        let publisher_a = bus.publisher("A");
        let publisher_b = bus.publisher("B");

        publisher_a.publish(b"msgA").unwrap();
        publisher_a.publish(b"msgC").unwrap();

        publisher_b.publish(b"msgB").unwrap();
        publisher_b.publish(b"msgD").unwrap();

        let recv_a = sub_a.next().await.unwrap();
        let recv_c = sub_a.next().await.unwrap();
        let recv_b = sub_b.next().await.unwrap();
        let recv_d = sub_b.next().await.unwrap();

        assert_eq!(&*recv_a, b"msgA");
        assert_eq!(&*recv_c, b"msgC");
        assert_eq!(&*recv_b, b"msgB");
        assert_eq!(&*recv_d, b"msgD");
    }

    #[proptest(async = "tokio")]
    async fn stress_test_multiple_subscribers_prop(
        #[strategy(1usize..100)] subscribers: usize,
        #[strategy(20usize..1_000)] total_msgs: usize,
    ) {
        const TOPIC: &str = "stress_multi_subs";
        let bus = EventBus::new_with_topic_capacity(total_msgs);

        // Generate messages to publish
        let mut messages: Vec<Arc<[u8]>> = (0..total_msgs)
            .map(|_| Faker.fake::<u64>().to_le_bytes().into())
            .collect();

        // Register all subscribers
        let subs: Vec<_> = (0..subscribers).map(|_| bus.subscribe(TOPIC)).collect();
        let publisher = bus.publisher(TOPIC);

        // Run all subscribers concurrently
        let subs_handles = tokio::spawn(async move {
            stream::iter(subs)
                .map(|sub| sub.take(total_msgs).collect::<Vec<_>>())
                .buffer_unordered(subscribers)
                .collect::<Vec<_>>()
                .await
        });

        // Publish all messages
        for msg in &messages {
            publisher.publish(msg.as_ref()).unwrap();
        }

        // Wait for all subscribers to complete
        let sub_results = subs_handles.await.unwrap();

        // Sort input messages to be able to assert against the results
        messages.sort();

        for mut result in sub_results {
            result.sort();
            prop_assert_eq!(&result, &messages, "subscriber missed or altered messages");
        }
    }
}
