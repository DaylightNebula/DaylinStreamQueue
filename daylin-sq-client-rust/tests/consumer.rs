//! Uses a fake server that records the calls made to it.

use std::future::Future;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, bail};
use axum::Router;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::routing::any;
use daylin_sq_client_rust::{
    EventMessage, HealthResponse, StatusError, StreamEventResponse, StreamQueueClient, StreamQueueConsumer,
    StreamQueueConsumerBuilder, Subscription
};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use uuid::{Uuid, uuid};

const ID: Uuid = uuid!("5f0c9a52-6f0e-4a35-9d8e-0d8b3f1f6f11");

/// The calls made to the fake server.
///
/// Fields:
/// * calls: The path and body of each call
/// * status: What the server responds with
/// * health: The health of the subscriber during each call to subscribe or complete
#[derive(Default)]
struct Recorded {
    calls: Mutex<Vec<(String, String)>>,
    status: Mutex<u16>,
    health: Mutex<Vec<HealthResponse>>
}

/// A fake server and a client of it.
///
/// Fields:
/// * recorded: The calls made to the server
/// * client: Calls the server
struct Fixture {
    recorded: Arc<Recorded>,
    client: StreamQueueClient
}

impl Fixture {
    /// Hosts a fake server that responds with 200.
    async fn new() -> anyhow::Result<Self> {
        let recorded = Arc::new(Recorded { status: Mutex::new(200), ..Default::default() });
        let router = Router::new()
            .route("/api/{*path}", any(record))
            .with_state(recorded.clone());
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let client = StreamQueueClient::new(&format!("http://{}/", listener.local_addr()?))?;
        tokio::spawn(async move { axum::serve(listener, router).await });
        Ok(Self { recorded, client })
    }

    /// Sets what the server responds with.
    fn respond_with(&self, status: u16) {
        *self.recorded.status.lock().unwrap() = status;
    }

    /// Copies the calls made to the server.
    fn calls(&self) -> Vec<(String, String)> {
        self.recorded.calls.lock().unwrap().clone()
    }

    /// Counts the calls made to a path.
    fn count(&self, path: &str) -> usize {
        self.calls()
            .iter()
            .filter(|(called, _)| called == path)
            .count()
    }

    /// Sets up a consumer.
    fn consumer<F, Fut>(&self, handler: F) -> StreamQueueConsumerBuilder
    where
        F: Fn(EventMessage) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = anyhow::Result<()>> + Send + 'static
    {
        StreamQueueConsumer::builder(self.client.clone(), "test", "test-event", handler)
            .public_address(|port| format!("http://127.0.0.1:{port}/"))
    }

    /// The last subscription that was made.
    fn subscription(&self) -> anyhow::Result<Subscription> {
        last_subscription(&self.recorded)
    }

    /// Calls the health endpoint of the subscriber.
    async fn health(&self) -> anyhow::Result<HealthResponse> {
        health(&self.subscription()?).await
    }

    /// Posts an event to the subscriber.
    async fn send_event(&self) -> anyhow::Result<reqwest::Response> {
        let event = EventMessage { id: ID, event_type: "test-event".into(), message: "hello".into() };
        self.send(serde_json::to_string(&event)?).await
    }

    /// Posts a body to the event endpoint of the subscriber.
    async fn send(&self, body: String) -> anyhow::Result<reqwest::Response> {
        let request = reqwest::Client::new()
            .post(self.subscription()?.event_endpoint)
            .body(body);
        Ok(request.send().await?)
    }

    /// Waits for the event to be completed.
    async fn wait_for_complete(&self) -> anyhow::Result<()> {
        let path = format!("/complete-event/{ID}");
        let wait = async {
            while self.count(&path) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::time::timeout(Duration::from_secs(10), wait)
            .await
            .context("the event was not completed")
    }
}

/// Finds the last subscription made to the server.
fn last_subscription(recorded: &Recorded) -> anyhow::Result<Subscription> {
    let calls = recorded.calls.lock().unwrap();
    let (_, body) = calls
        .iter()
        .rfind(|(path, _)| path == "/subscribe/test-event")
        .context("there is no subscription")?;
    Ok(serde_json::from_str(body)?)
}

/// Calls the health endpoint of a subscription.
async fn health(subscription: &Subscription) -> anyhow::Result<HealthResponse> {
    let response = reqwest::get(&subscription.health_endpoint).await?;
    assert_eq!(200, response.status());
    Ok(response.json().await?)
}

/// Records a call to the fake server.
async fn record(State(recorded): State<Arc<Recorded>>, request: Request) -> (StatusCode, String) {
    let uri = request.uri().to_string();
    let path = uri.trim_start_matches("/api").to_owned();
    let body = axum::body::to_bytes(request.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    recorded.calls.lock().unwrap().push((path.clone(), body));

    // like the server, which checks the health of a subscriber during these calls
    if !path.starts_with("/submit-event")
        && let Ok(subscription) = last_subscription(&recorded)
        && let Ok(health) = health(&subscription).await
    {
        recorded.health.lock().unwrap().push(health);
    }

    let status = *recorded.status.lock().unwrap();
    (StatusCode::from_u16(status).unwrap(), ID.to_string())
}

/// The status in an error.
fn status_of<T>(result: anyhow::Result<T>) -> Option<u16> {
    let error = result.err()?;
    Some(error.downcast_ref::<StatusError>()?.status)
}

/// A handler that does nothing.
async fn ignore(_: EventMessage) -> anyhow::Result<()> {
    Ok(())
}

const FREE: HealthResponse = HealthResponse { is_active: true, accepting_events: true };
const BUSY: HealthResponse = HealthResponse { is_active: true, accepting_events: false };
const INACTIVE: HealthResponse = HealthResponse { is_active: false, accepting_events: false };

#[test]
fn messages_match_the_json_of_the_server() -> anyhow::Result<()> {
    let event = EventMessage { id: ID, event_type: "a".into(), message: "b".into() };
    assert_eq!(format!(r#"{{"id":"{ID}","eventType":"a","message":"b"}}"#), serde_json::to_string(&event)?);
    assert_eq!(r#"{"isActive":true,"acceptingEvents":false}"#, serde_json::to_string(&BUSY)?);

    let without_info = StreamEventResponse { contact_address: "a".into(), extra_info: None };
    assert_eq!(r#"{"contactAddress":"a"}"#, serde_json::to_string(&without_info)?);
    let with_info = StreamEventResponse { contact_address: "a".into(), extra_info: Some("b".into()) };
    assert_eq!(r#"{"contactAddress":"a","extra-info":"b"}"#, serde_json::to_string(&with_info)?);

    let subscription = Subscription {
        name: "a".into(),
        event_type: "b".into(),
        health_endpoint: "c".into(),
        event_endpoint: "d".into()
    };
    assert_eq!(
        r#"{"name":"a","eventType":"b","healthEndpoint":"c","eventEndpoint":"d"}"#,
        serde_json::to_string(&subscription)?
    );
    Ok(())
}

#[test]
fn client_rejects_invalid_addresses() {
    assert!(StreamQueueClient::new("not a url").is_err());
    assert!(StreamQueueClient::new("ftp://localhost").is_err());
}

#[tokio::test]
async fn client_submits_events() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    assert_eq!(ID, fixture.client.submit_event("test-event", "héllo", None).await?);
    assert_eq!(vec![("/submit-event/test-event".to_owned(), "héllo".to_owned())], fixture.calls());
    Ok(())
}

#[tokio::test]
async fn client_submits_events_with_an_encoded_contact_address() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let contact = Some("http://producer:3245/contact?a=b&c=é");
    fixture.client.submit_event("test-event", "hello", contact).await?;

    let query = "contact=http%3A%2F%2Fproducer%3A3245%2Fcontact%3Fa%3Db%26c%3D%C3%A9";
    assert_eq!(1, fixture.count(&format!("/submit-event/test-event?{query}")));
    Ok(())
}

#[tokio::test]
async fn client_returns_if_an_event_is_still_held() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    assert!(fixture.client.complete_event(ID).await?);

    fixture.respond_with(404);
    assert!(!fixture.client.complete_event(ID).await?);
    Ok(())
}

#[tokio::test]
async fn client_fails_when_a_call_fails() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    fixture.respond_with(500);
    assert_eq!(Some(500), status_of(fixture.client.submit_event("test-event", "", None).await));
    assert_eq!(Some(500), status_of(fixture.client.complete_event(ID).await));

    // a port that nothing is hosted on
    let port = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?.local_addr()?.port();
    let unreachable = StreamQueueClient::new(&format!("http://127.0.0.1:{port}"))?;
    let result = unreachable.submit_event("test-event", "", None).await;
    assert!(result.is_err());
    assert_eq!(None, status_of(result));
    Ok(())
}

#[tokio::test]
async fn consumer_is_active_before_it_subscribes() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut consumer = fixture.consumer(ignore).build();
    assert_eq!(ID, consumer.start().await?);
    assert_eq!(vec![FREE], *fixture.recorded.health.lock().unwrap());
    consumer.close().await;
    Ok(())
}

#[tokio::test]
async fn consumer_cannot_start_twice_or_resume_before_it_starts() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut consumer = fixture.consumer(ignore).build();
    assert!(consumer.resume().await.is_err());
    consumer.start().await?;
    assert!(consumer.start().await.is_err());
    Ok(())
}

#[tokio::test]
async fn consumer_handles_and_completes_an_event() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let handled = Arc::new(Mutex::new(Vec::new()));
    let handler_handled = handled.clone();
    let mut consumer = fixture
        .consumer(move |event| {
            handler_handled.lock().unwrap().push(event.message);
            async { Ok(()) }
        })
        .build();
    consumer.start().await?;

    let response = fixture.send_event().await?;
    assert_eq!(200, response.status());
    assert_eq!("", response.text().await?);
    fixture.wait_for_complete().await?;
    consumer.close().await;
    assert_eq!(vec!["hello"], *handled.lock().unwrap());
    Ok(())
}

#[tokio::test]
async fn consumer_returns_its_stream_response_before_the_event_is_handled() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let finish = Arc::new(Semaphore::new(0));
    let handler_finish = finish.clone();
    let mut consumer = fixture
        .consumer(move |_| {
            let finish = handler_finish.clone();
            async move {
                finish.acquire().await?.forget();
                Ok(())
            }
        })
        .stream_response(|event| async move {
            Ok(StreamEventResponse {
                contact_address: format!("http://consumer:3244/{}", event.id),
                extra_info: Some("info".into())
            })
        })
        .build();
    consumer.start().await?;

    let response = fixture.send_event().await?;
    assert_eq!(200, response.status());
    assert_eq!(
        format!(r#"{{"contactAddress":"http://consumer:3244/{ID}","extra-info":"info"}}"#),
        response.text().await?
    );
    assert_eq!(0, fixture.count(&format!("/complete-event/{ID}")));

    finish.add_permits(1);
    fixture.wait_for_complete().await?;
    consumer.close().await;
    Ok(())
}

#[tokio::test]
async fn consumer_rejects_an_event_when_its_stream_response_fails_and_stays_available() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut consumer = fixture
        .consumer(ignore)
        .stream_response(|_| async { bail!("expected by the test") })
        .build();
    consumer.start().await?;

    assert_eq!(500, fixture.send_event().await?.status());
    assert_eq!(FREE, fixture.health().await?);
    consumer.close().await;
    assert_eq!(0, fixture.count(&format!("/complete-event/{ID}")));
    Ok(())
}

#[tokio::test]
async fn consumer_is_busy_until_its_events_are_handled_and_free_before_they_are_completed() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let finish = Arc::new(Semaphore::new(0));
    let handler_finish = finish.clone();
    let mut consumer = fixture
        .consumer(move |_| {
            let finish = handler_finish.clone();
            async move {
                finish.acquire().await?.forget();
                Ok(())
            }
        })
        .max_running(2)
        .build();
    consumer.start().await?;

    fixture.send_event().await?;
    assert_eq!(FREE, fixture.health().await?);
    fixture.send_event().await?;
    assert_eq!(BUSY, fixture.health().await?);

    // the fake server checks the health when the first event is completed
    finish.add_permits(1);
    fixture.wait_for_complete().await?;
    finish.add_permits(1);
    consumer.close().await;
    assert_eq!(2, fixture.count(&format!("/complete-event/{ID}")));
    assert_eq!(Some(&FREE), fixture.recorded.health.lock().unwrap().get(1));
    Ok(())
}

#[tokio::test]
async fn consumer_does_not_complete_an_event_when_the_handler_fails_and_stays_available() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut failing = fixture.consumer(|_| async { bail!("expected by the test") }).build();
    failing.start().await?;
    assert_eq!(200, fixture.send_event().await?.status());
    wait_until_free(&fixture).await?;
    failing.close().await;

    let mut panicking = fixture.consumer(|_| async { panic!("expected by the test") }).build();
    panicking.start().await?;
    assert_eq!(200, fixture.send_event().await?.status());
    wait_until_free(&fixture).await?;
    panicking.close().await;

    assert_eq!(0, fixture.count(&format!("/complete-event/{ID}")));
    Ok(())
}

/// Waits for the subscriber to take events.
async fn wait_until_free(fixture: &Fixture) -> anyhow::Result<()> {
    for _ in 0..1_000 {
        if fixture.health().await? == FREE {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    bail!("the consumer stayed busy")
}

#[tokio::test]
async fn a_paused_consumer_is_inactive_until_it_resumes() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut consumer = fixture.consumer(ignore).build();
    consumer.start().await?;

    consumer.pause();
    assert_eq!(INACTIVE, fixture.health().await?);
    assert_eq!(503, fixture.send_event().await?.status());

    consumer.resume().await?;
    assert_eq!(FREE, fixture.health().await?);
    assert_eq!(2, fixture.count("/subscribe/test-event"));
    consumer.close().await;
    Ok(())
}

#[tokio::test]
async fn invalid_events_are_rejected() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut consumer = fixture.consumer(ignore).build();
    consumer.start().await?;
    assert_eq!(400, fixture.send("not json".into()).await?.status());
    assert_eq!(405, reqwest::get(fixture.subscription()?.event_endpoint).await?.status());
    assert_eq!(FREE, fixture.health().await?);
    consumer.close().await;
    Ok(())
}

#[tokio::test]
async fn large_events_are_accepted() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut consumer = fixture.consumer(ignore).build();
    consumer.start().await?;

    let event = EventMessage { id: ID, event_type: "test-event".into(), message: "a".repeat(5_000_000) };
    assert_eq!(200, fixture.send(serde_json::to_string(&event)?).await?.status());
    fixture.wait_for_complete().await?;
    consumer.close().await;
    Ok(())
}

#[tokio::test]
async fn a_rejected_subscription_can_be_tried_again() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut consumer = fixture.consumer(ignore).build();
    fixture.respond_with(422);
    assert_eq!(Some(422), status_of(consumer.start().await));

    fixture.respond_with(200);
    assert_eq!(ID, consumer.resume().await?);
    consumer.close().await;
    Ok(())
}

#[tokio::test]
async fn closed_and_dropped_consumers_stop_hosting() -> anyhow::Result<()> {
    let fixture = Fixture::new().await?;
    let mut closed = fixture.consumer(ignore).build();
    closed.start().await?;
    closed.close().await;
    assert!(fixture.health().await.is_err());

    let mut dropped = fixture.consumer(ignore).build();
    dropped.start().await?;
    drop(dropped);
    for _ in 0..1_000 {
        if fixture.health().await.is_err() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    bail!("the dropped consumer is still hosted")
}
