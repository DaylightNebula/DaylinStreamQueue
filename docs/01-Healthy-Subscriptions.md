# Health Subscriptions
The goal of this plan is to implement subscriptions that consumers can consume via a common client library.

## Step 1: Split up Project
The existing code in the project should be moved into a sub folder named "daylin-sq-server", then another Gradle project
setup called "daylin-sq-client", and another named "daylin-sq-core" to act as a common core between the two projects.  
All projects should be setup with kotlin.  The project should not be setup in a normal Gradle multi-module project.

## Step 2: Server-Side Subscriptions
Add the post endpoint "/subscribe/{eventType}" that allows clients to register a subscription.  That subscription should
look like the data class below.  The subscription should be saved in the database, but also cached in-memory for fast
look-ups.  Each is required to supply a health http endpoint that returns a `HealthResponse`.  A subscription is removed 
from the active subscriptions table and cache if the health check ever fails to return 200, or it returns a response
with isActive being false (this allows subscribers to temporarily take themselves from the subscription pool).  Health
checks may occur at any time, but must happen at least once every 30 seconds (timer recent each time called, regardless
if it occurred from the timer or not).  The `HealthResponse` class should be moved to core.  Inside the server, all
subscriptions should be tracked with a UUID ID.  When a subscription occurs, it must be fully set up and its responses
will return active (busy state does not matter), add this to documentation.  If the health endpoint is the same for
subscriptions on different event types, any health check that occurs from the 30s timer should mark as healthy for all
event types.  No subscriptions to one event type may share the same health point, eject the oldest if this occurs.

```kotlin
data class Subscription(
    val name: String, // not unique, just used for debugging
    val eventType: String, // what event type this subscription is subscribing too
    val healthEndpoint: String, // called for a HealthResponse
    val eventEndpoint: String   // 
)
```

## Step 3: Sending Events to Subscribers
The goal of this step is to automatically send out events to consumers.  

Two rules to establish first:
1. a health check that returns active and not busy is required before sending an event to a consumer/subscriber
2. events are sent in a round robbin fashion to the first to satisfy rule 1

When there are events of a specific type in their queue, they should be sent out under the following conditions:
1. a health check showed a subscriber was active and not busy
2. an event was completed
3. an event was sent out (any event of the same type)
4. a new subscriber was added

When any of the above conditions are meant, send them the event via the event endpoint, this should be a post
where the "message" field contains the events message.  If this endpoint times out or fails, the event should be added
back to the front of the queue, then the subscription should be treated as if it just failed a health check.

## Step 4: Testing
Write a test consumer into a new Gradle project located in this projects "/java-tests" directory named "consumer" that
consumes a test event that supplies a string and two numbers, consuming the event means printing the string then the
result of summing the two numbers.  Then make another project named "producer" that simply starts, sends the
event described above to a daylin-sq server which should in turn send the event to the consumer.  The producer may shut
down after sending the event.  A docker compose file in "/java-tests" should spin up a test network with the producer
started at the end via using depends on (and maybe a timer inside the producer to add extra delay if needed).

## Note: Documentation
During this step, please go through and make sure all public or package private functions are marked with proper java
doc comments like this:

```kotlin
/**
 * <what the function does>
 * 
 * @param <all params with what they do>
 * @return <what is returned>
 */
```
