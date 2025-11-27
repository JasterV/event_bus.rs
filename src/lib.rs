//! An implementation of a simple event bus.
//!
//! It provides the following features:
//!
//! - Users can publish messages to a topic
//! - Users can subscribe to events published to a certain topic.
//!
//! Messages are published as bytes, it is responsibility of the user to perform the encoding and decoding.
use std::sync::mpsc;

pub struct EventBus {}

// Implementation ideas:
//
// I.
//
// The event bus contains a map of (&str, Topic)
//
// A Topic could contain:
//
// - The `sender` used to send messages
// - The `receiver` that gets cloned
// - A counter that keeps track of how many subscribers there are.
//
// Every time someone subscribes to a topic, it get's created in case it doesn't exist and the subscribers counter is set to 1.
// If the topic already exists, the counter increases.
// If the subscribers counter goes down to 0, the topic is removed.
//
// For the counter to go down, a subcription reference has to be dropped.
//
// We need tofind a way to decrease the counter automatically once the "subscription" gets dropped.
// Then, somehow, when the counter get's down to 0, the topics map entry must be removed.

impl EventBus {
    pub fn subscribe(&self, _topic: impl AsRef<str>) -> Option<mpsc::Receiver<&[u8]>> {
        todo!()
    }

    pub fn publish(&self, _topic: impl AsRef<str>) {}
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        assert!(true)
    }
}
