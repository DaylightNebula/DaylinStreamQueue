use std::fmt::{Display, Formatter};
use std::time::Duration;

use anyhow::{Context, anyhow};
use reqwest::{Client, RequestBuilder, Url};
use uuid::Uuid;

use crate::Subscription;

/// The error of a call that the server responded to with a failure, found by downcasting the error of the call.  Calls
/// that the server never responded to have other errors.
///
/// Fields:
/// * url: The url that was called
/// * status: The HTTP status that the server returned
/// * body: The body that the server returned
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusError {
    pub url: String,
    pub status: u16,
    pub body: String
}

impl Display for StatusError {
    /// Writes the url, status and body.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} returned {}: {}", self.url, self.status, self.body)
    }
}

impl std::error::Error for StatusError {}

/// Calls a daylin-sq server.  Cloning this is cheap.
///
/// Fields:
/// * api: The url that every call is under
/// * http: Makes the calls
#[derive(Debug, Clone)]
pub struct StreamQueueClient {
    api: Url,
    http: Client
}

impl StreamQueueClient {
    /// Creates a client, without calling the server.
    ///
    /// # Parameters
    /// * server_address: The address of the server, such as `http://localhost:5000`.
    ///
    /// # Errors
    /// * The address is not a valid http or https url.
    /// * The HTTP client cannot be set up, such as when the `https` feature is on and the system has no CA certificates.
    pub fn new(server_address: &str) -> anyhow::Result<Self> {
        let address = format!("{}/api", server_address.trim_end_matches('/'));
        let api = Url::parse(&address).with_context(|| format!("parsing the server address {server_address}"))?;
        anyhow::ensure!(
            matches!(api.scheme(), "http" | "https"),
            "the server address {server_address} is not an http or https url"
        );

        let http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .context("setting up the HTTP client")?;
        Ok(Self { api, http })
    }

    /// Submits an event to be sent to a subscriber.
    ///
    /// Returns the id of the event.
    ///
    /// # Parameters
    /// * event_type: The type of the event.
    /// * message: The message of the event.
    /// * contact: Url called with a POST of a [`StreamContact`](crate::StreamContact) once a subscriber receives the
    ///   event, or `None` to not be called.  Only allowed if the mode of the event type is stream.
    ///
    /// # Errors
    /// * The server cannot be called.
    /// * The server rejects the event, as a [`StatusError`].
    /// * The server does not return an id.
    pub async fn submit_event(&self, event_type: &str, message: &str, contact: Option<&str>) -> anyhow::Result<Uuid> {
        let mut url = self.url(&["submit-event", event_type])?;
        if let Some(contact) = contact {
            url.query_pairs_mut().append_pair("contact", contact);
        }

        let request = self
            .http
            .post(url)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(message.to_owned());
        parse_id(&send(request).await?)
    }

    /// Subscribes to an event type.
    ///
    /// The subscriber must be fully set up before calling this, the server calls its health endpoint before responding
    /// and rejects the subscription if it is not active.  [`StreamQueueConsumer`](crate::StreamQueueConsumer) does
    /// this.  Returns the id of the subscription.
    ///
    /// # Parameters
    /// * subscription: The subscription to add.
    ///
    /// # Errors
    /// * The server cannot be called.
    /// * The server rejects the subscription, as a [`StatusError`].
    /// * The server does not return an id.
    pub async fn subscribe(&self, subscription: &Subscription) -> anyhow::Result<Uuid> {
        let url = self.url(&["subscribe", &subscription.event_type])?;
        let request = self
            .http
            .post(url)
            .json(subscription);
        parse_id(&send(request).await?)
    }

    /// Marks an event as completed, removing it from the server.
    ///
    /// Returns false if the event is no longer held.
    ///
    /// # Parameters
    /// * id: The id of the event.
    ///
    /// # Errors
    /// * The server cannot be called.
    /// * The server fails to complete the event, as a [`StatusError`].
    pub async fn complete_event(&self, id: Uuid) -> anyhow::Result<bool> {
        let url = self.url(&["complete-event", &id.to_string()])?;
        match send(self.http.post(url)).await {
            Ok(_) => Ok(true),
            Err(e) if e.downcast_ref::<StatusError>().is_some_and(|e| e.status == 404) => Ok(false),
            Err(e) => Err(e)
        }
    }

    /// Finds the url of a path in the api, encoding each segment.
    fn url(&self, segments: &[&str]) -> anyhow::Result<Url> {
        let mut url = self.api.clone();
        url.path_segments_mut()
            .map_err(|()| anyhow!("{} cannot have a path", self.api))?
            .extend(segments);
        Ok(url)
    }
}

/// Sends a request and returns the body of its response.
async fn send(request: RequestBuilder) -> anyhow::Result<String> {
    let response = request
        .send()
        .await
        .context("calling the server")?;
    let url = response.url().to_string();
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("reading the response of {url}"))?;

    if !status.is_success() {
        return Err(StatusError { url, status: status.as_u16(), body }.into());
    }
    Ok(body)
}

/// Parses an id returned by the server.
fn parse_id(body: &str) -> anyhow::Result<Uuid> {
    Uuid::parse_str(body.trim()).with_context(|| format!("the server returned {body} instead of an id"))
}
