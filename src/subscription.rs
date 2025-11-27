use crate::{Channel, rc_map::ObjectRef};
use async_broadcast::Receiver;
use futures::Stream;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// Holds a subscription to a channel.
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
    // We need to keep the ownership of the object ref
    // Otherwise if the object ref gets dropped, it might cleanup the topic
    // And the channel would get closed
    _object_ref: ObjectRef<Arc<str>, Channel>,
    rx: Receiver<Arc<[u8]>>,
}

impl From<ObjectRef<Arc<str>, Channel>> for Subscription {
    fn from(object_ref: ObjectRef<Arc<str>, Channel>) -> Self {
        let Channel(tx, _) = object_ref.value();

        let rx = tx.new_receiver();

        Self {
            _object_ref: object_ref,
            rx,
        }
    }
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
