use crate::Topic;
use async_broadcast::Receiver;
use dashmap::DashMap;
use futures::Stream;
use std::{
    pin::Pin,
    sync::{Arc, Weak, atomic::Ordering},
    task::{Context, Poll},
};

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
    pub(crate) topic: Arc<str>,
    pub(crate) topics_ref: Weak<DashMap<String, Topic>>,
    pub(crate) rx: Receiver<Arc<[u8]>>,
}

impl Stream for Subscription {
    type Item = Arc<[u8]>;

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
        let Some(topics) = self.topics_ref.upgrade() else {
            return;
        };

        // Scope the guard so it drops before we call `remove`.
        //
        // This is done because trying to call `remove` while we hold a read guard
        // from the DashMap will result in a deadlock.
        let subscribers_count = {
            let Some(topic_ref) = topics.get(self.topic.as_ref()) else {
                return;
            };

            topic_ref.subscribers.fetch_sub(1, Ordering::Relaxed)
        };

        if subscribers_count <= 1 {
            let _ = topics.remove(self.topic.as_ref());
        }
    }
}
