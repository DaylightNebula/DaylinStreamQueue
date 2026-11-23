use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The body posted to a subscriber's event endpoint, which marks the event as received by returning 200.
///
/// Fields:
/// * id: Used by the subscriber to mark the event as completed
/// * event_type: The type of the event
/// * message: The event's message, exactly as it was submitted
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventMessage {
    pub id: Uuid,
    pub event_type: String,
    pub message: String
}

/// How the events of a type are handled.
///
/// Variants:
/// * Simple: Events are only sent to a subscriber
/// * Response: The subscriber must return a [`StreamEventResponse`] that is passed on to the producer
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventMode {
    Simple,
    Response
}

/// What a subscriber's health endpoint responds with.
///
/// Fields:
/// * is_active: If the subscriber should stay subscribed, when false its subscription is removed
/// * accepting_events: If the subscriber can be sent an event now, false means that it is busy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthResponse {
    pub is_active: bool,
    pub accepting_events: bool
}

/// The body posted to the contact address of a producer once a subscriber has received its response event.
///
/// Fields:
/// * message: The event's message, exactly as it was submitted
/// * contact: How the producer may contact the subscriber, the producer does not have to
/// * extra_info: The extra info from the subscriber, if there is any
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamContact {
    pub message: String,
    pub contact: String,
    #[serde(rename = "extra-info", default, skip_serializing_if = "Option::is_none")]
    pub extra_info: Option<String>
}

/// What a subscriber's event endpoint must respond with when the mode of the event is response.
///
/// Fields:
/// * contact_address: How the producer may contact the subscriber while it processes the event
/// * extra_info: Passed on to the producer as it is, if there is any
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamEventResponse {
    #[serde(rename = "contactAddress")]
    pub contact_address: String,
    #[serde(rename = "extra-info", default, skip_serializing_if = "Option::is_none")]
    pub extra_info: Option<String>
}

/// A request to be sent every event of one type.
///
/// The subscriber must be fully set up before it subscribes: the server calls the health endpoint during the request and
/// rejects the subscription unless it responds as active.  Whether it is busy does not matter.
///
/// Fields:
/// * name: Not unique, just used for debugging
/// * event_type: What event type this subscription is subscribing to, which is registered if it does not exist yet
/// * mode: How the events are handled, must match the mode that the event type was registered with
/// * health_endpoint: Url called with a GET for a [`HealthResponse`]
/// * event_endpoint: Url called with a POST of an [`EventMessage`] for each event
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    pub name: String,
    pub event_type: String,
    pub mode: EventMode,
    pub health_endpoint: String,
    pub event_endpoint: String
}
