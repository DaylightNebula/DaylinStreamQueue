# Daylin Stream Queue
The goal of this project is to build a system that allows event producers to connect to this service, send an event,
then allow the consumer to respond to the producer.  This allows for systems where a producer needs to stream data and 
talk to some kind of interactive services consuming the event.  This is nice for things like a build service for remotely 
building projects where you need to stream logs, results, and keyboard events to and from the user.

## What it does
Producers submit events to the server over HTTP, and the server queues and sends each one to a consumer that is free.
The architecture is similar to that of a more complex version of RabbitMQ:

- An event type is registered by the first subscription to it, which gives it a mode. It stays registered with that mode,
  and events cannot be submitted to it before then.
- Events are kept in a database until they are completed, so they survive restarts. An event that its consumer does not
  finish in time is sent again.
- Consumers subscribe to an event type, and its events are sent to the free ones in turn.
- A `Simple` event is sent to a consumer and that is all.
- A `Response` event also lets its producer and consumer talk to each other while it is processed. The server passes on how
  to contact the consumer, and how they then talk is up to them.

### Projects
Each folder is its own project.

- `daylin-sq-server`: The server, it keeps its `data` folder and finds `config.yml` where it is run
- `daylin-sq-client-java`: Library for submitting and consuming events from Java and Kotlin
- `daylin-sq-client-rust`: Library for submitting and consuming events from Rust
- `daylin-sq-client-js`: Library for submitting and consuming events from JavaScript and TypeScript
- `daylin-sq-core`: Classes shared by the server and the Java client
- `java-tests`: A test consumer and producer
- `examples`: A full example of each client, each is one file

### HTTP API
The clients do all of this, so it is only needed to use the server without one.

To submit an event, post its raw message to `/api/submit-event/{eventType}`, which returns the id of the event.
It is rejected with a 404 if nothing has ever subscribed to the event type.

To subscribe, post a subscription to `/api/subscribe/{eventType}`, which returns the id of the subscription:

```json
{
  "name": "my-consumer",
  "eventType": "test-event",
  "mode": "Simple",
  "healthEndpoint": "http://my-consumer:3242/health",
  "eventEndpoint": "http://my-consumer:3242/event"
}
```

- The mode is `Simple` or `Response`. The first subscription to an event type registers it with its mode, and later ones
  are rejected with a 409 if their mode is different.
- A subscriber must be fully set up before it subscribes. Its health endpoint is called during the request, and the
  subscription is rejected with a 422 unless it responds as active. It may be busy.
- The health endpoint returns a 200 with `{"isActive": true, "acceptingEvents": true}`, where `acceptingEvents` is false
  when it is busy. It is called at least once every 30 seconds and before each event.
- A subscription is removed when a health check fails or is not active, or when an event fails to send. Subscribe again
  to rejoin. A failed event goes back to the front of the queue.
- Only one subscription to an event type can use a health endpoint, the oldest is ejected.
- Events are posted to the event endpoint as `{"id": "...", "eventType": "...", "message": "..."}`. Return a 200 within 5
  seconds to mark the event as received, then post to `/api/complete-event/{id}` once finished.

When the mode of an event type is `Response`:

1. The producer posts the event to `/api/submit-event/{eventType}?contact=...`, where `contact` is an optional url encoded
   address that it can be called on.
2. The subscriber must return `{"contactAddress": "...", "extra-info": "..."}` from its event endpoint, where `extra-info`
   is optional. Anything else is treated as a failed event.
3. If the producer gave an address, it is posted `{"message": "...", "contact": "...", "extra-info": "..."}`, where
   `contact` is the address from the subscriber. Its response is ignored, and nothing happens if the call fails.

The producer does not have to connect.

## Build
### Server
Java 25 and Gradle 9.9 are required.

```
gradle -p daylin-sq-server test
gradle -p daylin-sq-server bootRun
docker build -f daylin-sq-server/Dockerfile .
```

The server can be configured with a `config.yml` file where it is run. The file is optional, and so is every value in it.
The time limits are ISO 8601 durations, with these defaults:

```yaml
durations:
  maxNoReceiveDuration: PT10S
  maxProcessingDuration: PT20M
  maxTimeInQueue: PT24H
  healthCheckInterval: PT30S
  subscriberTimeout: PT5S
```

Web pages from other origins can only call the server if their origin is in `corsOrigins`, which is empty by default. Any
page can call it when this contains `"*"`, so only use that if everyone who can reach the server may use all of it:

```yaml
corsOrigins:
  - "https://my-page.com"
```

To run the test network, which should print "Hello from the producer" and 6.5, then stream the alphabet from the consumer
to the producer two letters at a time:

```
docker compose -f java-tests/docker-compose.yml up --build
```

### Java and Kotlin client
Java 25 and Gradle 9.9 are required. Test it with `gradle -p daylin-sq-client-java test`.

To use it, add `includeBuild("../daylin-sq-client-java")` to `settings.gradle.kts`, with the path to this repository, and
this to `build.gradle.kts`:

```kotlin
dependencies {
    implementation("com.daylin:daylin-sq-client-java:0.0.1-SNAPSHOT")
}
```

### Rust client
Rust 1.89 or newer is required. Test it with `cargo test` in `daylin-sq-client-rust`.

To use it, add this to `Cargo.toml`, with the path to this repository. It runs on tokio.

```toml
[dependencies]
daylin-sq-client-rust = { path = "../daylin-sq-client-rust" }
```

The `https` feature is on by default, and needs the system to have CA certificates even when only http is used. Some small
Docker images have none, so either install them or turn the feature off with `default-features = false`.

### JavaScript and TypeScript client
Node 22 or newer is required. Test it with `npm test` in `daylin-sq-client-js`. There is nothing to build, and it has no
dependencies.

To use it, install it with the path to this repository:

```
npm install ../daylin-sq-client-js
```

`daylin-sq-client-js` also works in browsers, while `daylin-sq-client-js/consumer` hosts endpoints and so only works in
Node. A web page must be from the same origin as the server, or have its origin in `corsOrigins` in `config.yml`.

## Examples
Each of these consumes a simple event and submits it, then does the same with a response event and tells the producer how
to contact the consumer. A consumer uses the `Response` mode if it is given a stream response, and `Simple` if not.
The files in `examples` also host the stream and read it.

### Kotlin
```kotlin
val client = StreamQueueClient("http://localhost:5000")

StreamQueueConsumer(client, "my-consumer", "test-event") { event -> println(event.message) }.start()
client.submitEvent("test-event", "hello")

StreamQueueConsumer(
    client, "my-consumer", "alphabet",
    streamResponse = { event -> StreamEventResponse("http://my-consumer:3244/${event.id}", "two letters on each line") }
) { event -> println(event.message) }.start()
client.submitEvent("alphabet", "hello", contact = "http://my-producer:3245/contact")
```

### Java
```java
StreamQueueClient client = new StreamQueueClient("http://localhost:5000");

// the arguments are the port, public address, max running events, stream response and handler
new StreamQueueConsumer(client, "my-consumer", "test-event", 0, port -> "http://localhost:" + port, 1, null, event -> {
    System.out.println(event.getMessage());
    return Unit.INSTANCE;
}).start();
client.submitEvent("test-event", "hello", null);

new StreamQueueConsumer(
    client, "my-consumer", "alphabet", 0, port -> "http://localhost:" + port, 1,
    event -> new StreamEventResponse("http://my-consumer:3244/" + event.getId(), "two letters on each line"),
    event -> {
        System.out.println(event.getMessage());
        return Unit.INSTANCE;
    }
).start();
client.submitEvent("alphabet", "hello", "http://my-producer:3245/contact");
```

### Rust
```rust
let client = StreamQueueClient::new("http://localhost:5000")?;

let mut simple = StreamQueueConsumer::builder(client.clone(), "my-consumer", "test-event", |event| async move {
    println!("{}", event.message);
    Ok(())
})
.build();
simple.start().await?;
client.submit_event("test-event", "hello", None).await?;

let mut stream = StreamQueueConsumer::builder(client.clone(), "my-consumer", "alphabet", |event| async move {
    println!("{}", event.message);
    Ok(())
})
.stream_response(|event| async move {
    Ok(StreamEventResponse {
        contact_address: format!("http://my-consumer:3244/{}", event.id),
        extra_info: Some("two letters on each line".into())
    })
})
.build();
stream.start().await?;
client.submit_event("alphabet", "hello", Some("http://my-producer:3245/contact")).await?;
```

### JavaScript and TypeScript
```ts
import { StreamQueueClient } from "daylin-sq-client-js";
import { StreamQueueConsumer } from "daylin-sq-client-js/consumer";

const client = new StreamQueueClient("http://localhost:5000");

await new StreamQueueConsumer(client, {
  name: "my-consumer",
  eventType: "test-event",
  handler: (event) => console.log(event.message)
}).start();
await client.submitEvent("test-event", "hello");

await new StreamQueueConsumer(client, {
  name: "my-consumer",
  eventType: "alphabet",
  streamResponse: (event) => ({
    contactAddress: `http://my-consumer:3244/${event.id}`,
    "extra-info": "two letters on each line"
  }),
  handler: (event) => console.log(event.message)
}).start();
await client.submitEvent("alphabet", "hello", "http://my-producer:3245/contact");
```
