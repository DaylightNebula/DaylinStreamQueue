//! Submits and consumes events of a daylin-sq server.
//!
//! Use [`StreamQueueClient`] to submit events and [`StreamQueueConsumer`] to consume them.

mod client;
mod consumer;
mod messages;

pub use client::{StatusError, StreamQueueClient};
pub use consumer::{StreamQueueConsumer, StreamQueueConsumerBuilder};
pub use messages::{EventMessage, HealthResponse, StreamContact, StreamEventResponse, Subscription};
