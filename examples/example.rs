//! Consumes and submits a simple event, then streams the alphabet from the consumer to the producer of a stream event.
//!
//! Needs a server on port 5000 that uses daylin-sq-server/config.yml, and the dependencies below:
//!
//! ```toml
//! anyhow = "1"
//! axum = "0.8"
//! daylin-sq-client-rust = { path = "../daylin-sq-client-rust" }
//! reqwest = "0.13"
//! tokio = { version = "1", features = ["full"] }
//! ```

use anyhow::Context;
use axum::routing::{get, post};
use axum::{Json, Router};
use daylin_sq_client_rust::{StreamContact, StreamEventResponse, StreamQueueClient, StreamQueueConsumer};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

const STREAM_PORT: u16 = 3244;
const CONTACT_PORT: u16 = 3245;

/// Runs both examples.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = StreamQueueClient::new("http://localhost:5000")?;
    simple_event(&client).await?;
    stream_event(&client).await
}

/// Consumes one test event that prints its message.
async fn simple_event(client: &StreamQueueClient) -> anyhow::Result<()> {
    let (handled, mut wait) = mpsc::channel(1);
    let mut consumer = StreamQueueConsumer::builder(client.clone(), "example", "test-event", move |event| {
        let handled = handled.clone();
        async move {
            println!("Received: {}", event.message);
            handled.send(()).await.context("the example already finished")
        }
    })
    .build();
    consumer.start().await?;

    // submit an event and wait for the consumer to handle it
    client.submit_event("test-event", "Hello from Rust", None).await?;
    wait.recv().await;
    consumer.close().await;
    Ok(())
}

/// Consumes one alphabet event, whose producer reads the alphabet from the consumer.
async fn stream_event(client: &StreamQueueClient) -> anyhow::Result<()> {
    // consumer: host the stream, this can be anything that the producer knows how to contact
    let alphabet = ('A'..='Z').map(|letter| format!("{letter}\n")).collect::<String>();
    let streams = Router::new().route("/alphabet/{id}", get(move || async { alphabet }));
    host(STREAM_PORT, streams).await?;

    // consumer: tell the producer where the stream is, the handler could keep working on the event while it is read
    let mut consumer = StreamQueueConsumer::builder(
        client.clone(),
        "example",
        "alphabet",
        |event| async move {
            println!("Consuming {}", event.id);
            Ok(())
        })
        .stream_response(|event| async move {
            Ok(StreamEventResponse {
                contact_address: format!("http://localhost:{STREAM_PORT}/alphabet/{}", event.id),
                extra_info: Some("one letter on each line".into())
            })
        })
        .build();
    consumer.start().await?;

    // producer: host the endpoint that is told how to contact the consumer
    let (contacted, mut contacts) = mpsc::channel(1);
    let contact = Router::new().route("/contact", post(move |Json(contact): Json<StreamContact>| async move {
        contacted.send(contact).await.ok();
    }));
    host(CONTACT_PORT, contact).await?;

    // producer: submit the event, then read the stream
    let address = format!("http://localhost:{CONTACT_PORT}/contact");
    client.submit_event("alphabet", "the ABC's please", Some(&address)).await?;
    let contact = contacts.recv().await.context("the consumer never made contact")?;
    println!("Streaming from {} with {:?}", contact.contact, contact.extra_info);
    let response = reqwest::get(&contact.contact).await?;
    print!("{}", response.text().await?);

    consumer.close().await;
    Ok(())
}

/// Hosts a router in the background.
async fn host(port: u16, router: Router) -> anyhow::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port))
        .await
        .with_context(|| format!("hosting on port {port}"))?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            eprintln!("Failed to host on port {port}: {e}");
        }
    });
    Ok(())
}
