# event_bus.rs

A **runtime-agnostic**, **async**, and **thread-safe** event bus for Rust.
Designed to be **efficient**, **simple**, and **easy to use**, allowing you to publish and subscribe to messages across threads and async tasks.

---

## Features

- **Runtime-agnostic**: works with any async runtime (Tokio, async-std, smol, etc.)
- **Thread-safe**: multiple publishers and subscribers can safely coexist
- **Async & Stream-based**: subscribers implement `futures::Stream`
- **Automatic cleanup**: topics are removed when the last subscriber drops
- **Minimal & simple API**: just `EventBus::subscribe` and `EventBus::publish`

## Topic capacity

The EventBus is build on top of bounded channels, which means that each time a topic is created, we need to specify a capacity.

The default one is set to an arbitrary value which is available and documented in the docs.

To know more about how the bounded channels work, check [async_broadcast](https://docs.rs/async-broadcast/0.7.2/async_broadcast/index.html)

---

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
event_bus_rs = "0.1.0"
futures = "0.3"
````

---
## Usage Example

```rust
use event_bus_rs::EventBus;
use futures::StreamExt;

#[tokio::main]
async fn main() {
    let bus = EventBus::new_with_topic_capacity(50);

    // Subscribe to a topic
    let mut sub = bus.subscribe("my_topic");

    // Spawn a subscriber task
    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            println!("Received: {}", String::from_utf8_lossy(&msg));
        }
    });

    // Publish a message
    bus.publish("my_topic", b"Hello, EventBus!").unwrap();
}
```

**Notes:**

* Messages are published as `&[u8]`; encoding/decoding is the user's responsibility.
* Multiple subscribers to the same topic each get a copy of every message.
* When all subscribers of a topic are dropped, the topic is automatically cleaned up.

---

## API Overview

* `EventBus::new() -> EventBus` – create a new bus
* `EventBus::new_with_topic_capacity() -> EventBus` - create a new but with a configure topic capacity
* `EventBus::subscribe(&self, topic: &str) -> Subscription` – subscribe to a topic
* `EventBus::publish(&self, topic: &str, data: &[u8]) -> Result<(), PublishError>` – publish a message
* `Subscription` implements `futures::Stream<Item = Arc<[u8]>>`

---

## License

MIT OR Apache-2.0

