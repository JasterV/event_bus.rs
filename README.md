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
    let bus = EventBus::builder()
          .with_topic_capacity(50)
          .build();

    // Subscribe to a topic
    let mut sub = bus.subscribe("my_topic");

    // Spawn a subscriber task
    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            match msg {
                Ok(payload) => {
                    println!("Received: {}", String::from_utf8_lossy(&payload));
                }
                Err(err) => {
                    eprintln!("Error receiving message: {:?}", err);
                }
            }
        }
    });

    // Publish a message
    bus.publish("my_topic", b"Hello, EventBus!").await.unwrap();
}
```

**Notes:**

* Messages are published as `&[u8]`; encoding/decoding is the user's responsibility.
* Multiple subscribers to the same topic each get a copy of every message.
* When all subscribers of a topic are dropped, the topic is automatically cleaned up.

---

## API Overview

* `EventBus::new() -> EventBus` – create a new bus
* `EventBus::builder() -> EventBusBuilder` – create a new bus builder
* `EventBus::subscribe(&self, topic: &str) -> Subscription` – subscribe to a topic
* `EventBus::publish(&self, topic: &str, data: &[u8]) -> Result<(), PublishError>` – publish a message
* `Subscription` implements `futures::Stream<Item = Result<Payload, SubscriptionStreamRecvError>>`

---

## License

MIT OR Apache-2.0

