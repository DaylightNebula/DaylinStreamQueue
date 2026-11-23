use std::future::Future;
use std::net::Ipv4Addr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use log::{error, warn};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::{EventMessage, HealthResponse, StreamEventResponse, StreamQueueClient, Subscription};

/// How long closing a consumer waits for the calls to its endpoints to finish.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

type BoxFuture<T> = Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send>>;
type EventFn<T> = Box<dyn Fn(EventMessage) -> BoxFuture<T> + Send + Sync>;
type AddressFn = Box<dyn Fn(u16) -> String + Send + Sync>;

/// Sets the options of a [`StreamQueueConsumer`], created by [`StreamQueueConsumer::builder`].
///
/// Fields:
/// * client: The server to consume events from
/// * name: The name of the subscription
/// * event_type: The type of event to consume
/// * port: The port to host the endpoints on
/// * public_address: Finds the address that the server can reach the consumer at
/// * max_running: How many events to consume at once
/// * stream_response: Finds how the producer may contact the consumer
/// * handler: Consumes an event
pub struct StreamQueueConsumerBuilder {
    client: StreamQueueClient,
    name: String,
    event_type: String,
    port: u16,
    public_address: AddressFn,
    max_running: usize,
    stream_response: Option<EventFn<StreamEventResponse>>,
    handler: EventFn<()>
}

impl StreamQueueConsumerBuilder {
    /// Sets the port to host the endpoints on.  Any free port is used by default.
    ///
    /// # Parameters
    /// * port: The port, or 0 to use any free port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets how to find the address that the server can reach the consumer at.  This is `http://localhost:<port>` by
    /// default.
    ///
    /// # Parameters
    /// * public_address: Finds the address from the port that is used.
    pub fn public_address(mut self, public_address: impl Fn(u16) -> String + Send + Sync + 'static) -> Self {
        self.public_address = Box::new(public_address);
        self
    }

    /// Sets how many events to consume at once before responding as busy.  This is 1 by default.
    ///
    /// # Parameters
    /// * max_running: How many events to consume at once.
    pub fn max_running(mut self, max_running: usize) -> Self {
        self.max_running = max_running;
        self
    }

    /// Sets how the producer may contact the consumer about an event.  Required if the mode of the event type is stream.
    ///
    /// # Parameters
    /// * stream_response: Called before the handler, and must finish within 5 seconds.  If this fails then the event is
    ///   rejected and the server removes the subscription.
    pub fn stream_response<F, Fut>(mut self, stream_response: F) -> Self
    where
        F: Fn(EventMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<StreamEventResponse>> + Send + 'static
    {
        self.stream_response = Some(Box::new(move |event| Box::pin(stream_response(event))));
        self
    }

    /// Creates the consumer.  Call [`StreamQueueConsumer::start`] to begin.
    pub fn build(self) -> StreamQueueConsumer {
        StreamQueueConsumer {
            name: self.name,
            event_type: self.event_type,
            port: self.port,
            public_address: self.public_address,
            shared: Arc::new(Shared {
                client: self.client,
                max_running: self.max_running,
                stream_response: self.stream_response,
                handler: self.handler,
                active: AtomicBool::new(false),
                running: AtomicUsize::new(0),
                tasks: TaskTracker::new()
            }),
            server: None
        }
    }
}

/// Hosts the endpoints of a subscriber and consumes its events.
///
/// Dropping this stops the endpoints, but does not wait for running events like [`StreamQueueConsumer::close`] does.
///
/// Fields:
/// * name: The name of the subscription
/// * event_type: The type of event to consume
/// * port: The port to host the endpoints on, or 0 to use any free port
/// * public_address: Finds the address that the server can reach this at
/// * shared: What the endpoints use
/// * server: The hosted endpoints, once started
pub struct StreamQueueConsumer {
    name: String,
    event_type: String,
    port: u16,
    public_address: AddressFn,
    shared: Arc<Shared>,
    server: Option<Server>
}

impl StreamQueueConsumer {
    /// Begins setting up a consumer.
    ///
    /// # Parameters
    /// * client: The server to consume events from.
    /// * name: The name of the subscription, not unique and just used for debugging.
    /// * event_type: The type of event to consume.
    /// * handler: Consumes an event, which is completed when this succeeds.  If this fails, then the event is sent again
    ///   after the server's max processing duration.
    pub fn builder<F, Fut>(
        client: StreamQueueClient,
        name: impl Into<String>,
        event_type: impl Into<String>,
        handler: F
    ) -> StreamQueueConsumerBuilder
    where
        F: Fn(EventMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static
    {
        StreamQueueConsumerBuilder {
            client,
            name: name.into(),
            event_type: event_type.into(),
            port: 0,
            public_address: Box::new(|port| format!("http://localhost:{port}")),
            max_running: 1,
            stream_response: None,
            handler: Box::new(move |event| Box::pin(handler(event)))
        }
    }

    /// Hosts the endpoints and then subscribes.
    ///
    /// If subscribing fails then the endpoints stay up, and [`StreamQueueConsumer::resume`] can be called to try again.
    /// Returns the id of the subscription.
    ///
    /// # Errors
    /// * The consumer was already started.
    /// * The port cannot be used.
    /// * Subscribing fails, see [`StreamQueueClient::subscribe`].
    pub async fn start(&mut self) -> anyhow::Result<Uuid> {
        anyhow::ensure!(self.server.is_none(), "the consumer was already started");
        let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, self.port))
            .await
            .with_context(|| format!("hosting the consumer {} on port {}", self.name, self.port))?;
        let port = listener
            .local_addr()
            .context("finding the port of the consumer")?
            .port();

        // events can be any size
        let router = Router::new()
            .route("/health", get(health))
            .route("/event", post(event))
            .layer(DefaultBodyLimit::disable())
            .with_state(self.shared.clone());

        // host until the sender is used or dropped
        let (shutdown, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let serving = axum::serve(listener, router).with_graceful_shutdown(async {
                stopped.await.ok();
            });
            if let Err(e) = serving.await {
                error!("Failed to host the consumer: {e}");
            }
        });
        self.server = Some(Server { port, shutdown, task });
        self.resume().await
    }

    /// Responds as inactive, so that the server removes the subscription.  Running events are still finished.
    pub fn pause(&self) {
        self.shared.active.store(false, Ordering::SeqCst);
    }

    /// Subscribes again, after [`StreamQueueConsumer::pause`] or if the server removed the subscription.
    ///
    /// Returns the id of the new subscription.
    ///
    /// # Errors
    /// * The consumer was not started.
    /// * Subscribing fails, see [`StreamQueueClient::subscribe`].
    pub async fn resume(&self) -> anyhow::Result<Uuid> {
        let server = self.server.as_ref().context("the consumer was not started")?;
        let public_address = (self.public_address)(server.port);
        let address = public_address.trim_end_matches('/');
        let subscription = Subscription {
            name: self.name.clone(),
            event_type: self.event_type.clone(),
            health_endpoint: format!("{address}/health"),
            event_endpoint: format!("{address}/event")
        };

        // the server checks that this is active before it accepts the subscription
        self.shared.active.store(true, Ordering::SeqCst);
        self.shared.client.subscribe(&subscription).await
    }

    /// Stops taking events and waits for the running ones to finish.
    pub async fn close(mut self) {
        self.pause();
        if let Some(server) = self.server.take() {
            // the receiver is only gone if the server already stopped
            server.shutdown.send(()).ok();
            if tokio::time::timeout(CLOSE_TIMEOUT, server.task).await.is_err() {
                warn!("The consumer {} still has open calls to its endpoints", self.name);
            }
        }

        self.shared.tasks.close();
        self.shared.tasks.wait().await;
    }
}

impl Drop for StreamQueueConsumer {
    /// Stops taking events, as calls that are already open can outlive this.
    fn drop(&mut self) {
        self.pause();
    }
}

/// The hosted endpoints of a consumer.
///
/// Fields:
/// * port: The port that is used
/// * shutdown: Stops hosting when used or dropped
/// * task: Hosts the endpoints
struct Server {
    port: u16,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>
}

/// What the endpoints of a consumer use.
///
/// Fields:
/// * client: The server to complete events on
/// * max_running: How many events to consume at once
/// * stream_response: Finds how the producer may contact the consumer
/// * handler: Consumes an event
/// * active: If the consumer takes events
/// * running: How many events are being consumed
/// * tasks: The tasks that consume events
struct Shared {
    client: StreamQueueClient,
    max_running: usize,
    stream_response: Option<EventFn<StreamEventResponse>>,
    handler: EventFn<()>,
    active: AtomicBool,
    running: AtomicUsize,
    tasks: TaskTracker
}

/// Counts an event as running until this is dropped, so that a handler that panics is still uncounted.
///
/// Fields:
/// * 0: Holds the count
struct Running(Arc<Shared>);

impl Running {
    /// Counts an event as running.
    fn new(shared: Arc<Shared>) -> Self {
        shared.running.fetch_add(1, Ordering::SeqCst);
        Self(shared)
    }
}

impl Drop for Running {
    /// Stops counting the event.
    fn drop(&mut self) {
        self.0.running.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The health endpoint.
async fn health(State(shared): State<Arc<Shared>>) -> Response {
    let is_active = shared.active.load(Ordering::SeqCst);
    let accepting_events = is_active && shared.running.load(Ordering::SeqCst) < shared.max_running;
    json(&HealthResponse { is_active, accepting_events })
}

/// The event endpoint.
async fn event(State(shared): State<Arc<Shared>>, body: Bytes) -> Response {
    if !shared.active.load(Ordering::SeqCst) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let event = match serde_json::from_slice::<EventMessage>(&body) {
        Ok(event) => event,
        Err(e) => {
            warn!("Received an invalid event: {e}");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };

    let response = match &shared.stream_response {
        None => StatusCode::OK.into_response(),
        Some(stream_response) => match stream_response(event.clone()).await {
            Ok(response) => json(&response),
            Err(e) => {
                error!("Failed to find the stream response of event {}: {e:#}", event.id);
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }
    };

    // count before responding, so that the server's next health check sees it
    let running = Running::new(shared.clone());
    shared.tasks.spawn(consume(running, event));
    response
}

/// Handles an event and then completes it.
async fn consume(running: Running, event: EventMessage) {
    let shared = running.0.clone();
    let id = event.id;
    let handled = (shared.handler)(event).await;

    // before completing, as that makes the server check if this can take another event
    drop(running);
    if let Err(e) = handled {
        error!("Failed to consume event {id}: {e:#}");
        return;
    }

    match shared.client.complete_event(id).await {
        Ok(true) => {}
        Ok(false) => warn!("Event {id} is no longer held by this consumer"),
        Err(e) => error!("Failed to complete event {id}: {e:#}")
    }
}

/// Responds with JSON.
fn json(body: &impl Serialize) -> Response {
    match serde_json::to_string(body) {
        Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
        Err(e) => {
            error!("Failed to write a response: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}
