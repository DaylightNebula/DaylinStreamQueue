# Daylin Stream Queue
The goal of this project is to build a system that allows event producers to connect to this service, send an event,
then establish a webstream link that the two can stream data over.  This allows for systems where a producer needs to
stream data and talk to some kind of interactive services consuming the event.  This is nice for things like a build
service for remotely building projects where you need to stream logs, results, and keyboard events to and from the user.

## Architecture
This architecture of this project is similar to that of a more complex version of RabbitMQ.  For this, messages are
described via a config.yml file that determines what the types of events that can be sent, how and where they are sent,
and what mode the event is handled with.

Subscribers are sent events over HTTP. To subscribe, post a subscription to `/api/subscribe/{eventType}`:

```json
{
  "name": "my-consumer",
  "eventType": "test-event",
  "healthEndpoint": "http://my-consumer:3242/health",
  "eventEndpoint": "http://my-consumer:3242/event"
}
```

- A subscriber must be fully set up before it subscribes. Its health endpoint is called during the request, and the
  subscription is rejected with a 422 unless it responds as active. It may be busy.
- The health endpoint returns a 200 with `{"isActive": true, "acceptingEvents": true}`, where `acceptingEvents` is false
  when it is busy. It is called at least once every 30 seconds and before each event.
- A subscription is removed when a health check fails or is not active, or when an event fails to send. Subscribe again
  to rejoin. A failed event goes back to the front of the queue.
- Only one subscription to an event type can use a health endpoint, the oldest is ejected.
- Events are posted to the event endpoint as `{"id": "...", "eventType": "...", "message": "..."}`. Respond within 5
  seconds, then post to `/api/recv-event/{id}` and, once finished, to `/api/complete-event/{id}`.

## Projects
Each folder is its own Gradle project, and includes the ones it needs as composite builds.

- `daylin-sq-core`: Classes shared by the server and client
- `daylin-sq-server`: The server, run it from its folder so that it finds `config.yml`
- `daylin-sq-client`: Library for submitting and consuming events
- `java-tests`: A test consumer and producer

## Build
Java 25 and Gradle 9.9 are required.

```
gradle -p daylin-sq-server test
gradle -p daylin-sq-client test
gradle -p daylin-sq-server bootRun
docker build -f daylin-sq-server/Dockerfile .
```

To run the test network, which should print "Hello from the producer" and 6.5:

```
docker compose -f java-tests/docker-compose.yml up --build
```

## Example
```kotlin
val client = StreamQueueClient("http://localhost:8080")

StreamQueueConsumer(client, "my-consumer", "test-event") { event -> println(event.message) }.start()

client.submitEvent("test-event", "hello")
```
