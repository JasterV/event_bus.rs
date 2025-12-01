use crate::{Channel, rc_map::ObjectRef};
use std::sync::Arc;

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

/// Holds a reference to the internal topic and knows how to publish to it.
///
/// When the publisher is dropped, the parent topic might be deallocated
/// from memory if no other subscriptions or publishers exist.
pub struct Publisher {
    // We need to keep the ownership of the object ref
    // Otherwise if the object ref gets dropped, it might cleanup the topic
    // And the channel would get closed
    topic: Arc<str>,
    object_ref: ObjectRef<Arc<str>, Channel>,
}

impl From<ObjectRef<Arc<str>, Channel>> for Publisher {
    fn from(object_ref: ObjectRef<Arc<str>, Channel>) -> Self {
        let topic = object_ref.key().clone();

        Self {
            topic,
            object_ref: object_ref,
        }
    }
}

impl Publisher {
    /// Publishes a bunch of bytes to a topic.
    ///
    /// This method will only fail if:
    ///
    /// - The internal channel got closed unexpectedly (It should never happen while there is still at least one subscriber or publisher)
    /// - The internal channel is full.
    pub fn publish(&self, data: &[u8]) -> Result<(), PublishError> {
        let Channel(tx, _rx) = self.object_ref.value();

        let result = tx.try_broadcast(Arc::from(data));

        match result {
            Ok(_) => Ok(()),
            // There are no active receivers, we do not consider this an error
            Err(async_broadcast::TrySendError::Inactive(_)) => Ok(()),
            // The channel is closed, we return an error as this is unexpected
            Err(async_broadcast::TrySendError::Closed(_)) => {
                Err(PublishError::ChannelClosed(self.topic.clone()))
            }
            // The channel is overflown, we return an error
            Err(async_broadcast::TrySendError::Full(_)) => {
                Err(PublishError::CapacityOverflow(self.topic.clone()))
            }
        }
    }
}
