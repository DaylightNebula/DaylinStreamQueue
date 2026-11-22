# Streams
The goal of this plan is to design a system by which producers negotiate connection parameters so the consumer and
producer can talk to each-other while an event is processed by the consumer.  When an event contains the mode stream
and the policy is http, the event endpoint must return an instance of the below data class.  This lays out how the
producer may contact the consumer, however, the producer is in no way required to connect to the consumer (consumers
should handle this on a case-by-case basis on their own, not handled by daylin-sq).  When an event is submitted for an
event type marked as stream, the producer may optionally provide an endpoint post address they may be contacted on 
(field named "contact"), and when an address is received from the consumer that endpoint is called with two fields: the
events message named "message" and the contact address named "contact".  The consumer and producer are then allowed to
talk to each-other however they may like, it is assumed that they will know of each-other and how to contact each-other.
A consumer may include in there response containing the contact address with another field named "extra-info", that
field should be copied into the post endpoint call to the producer with a field of the same name.  If the producer fails
to accept the post endpoint call, nothing should occur, any response they give should just be ignored.

```kotlin
data class StreamEventResponse {
    contactAddress: String
    extraInfo: String
}
```

### Submitting an Event with a Contact Address
The "/api/submit-event/{eventType}" endpoint takes the producer's contact address as an optional query parameter named
"contact".  The request body is still only the raw message, so existing producers do not need to change.  The address
must be url encoded and is saved with the event, so that it is kept through retries and restarts.

```
POST /api/submit-event/alphabet?contact=http%3A%2F%2Fproducer%3A3243%2Fcontact

<the raw message>
```

A bad request is returned if the contact address is not a valid http or https url, or if one is given for an event type
that is not marked as stream.

## Testing
Update the java test consumers and producers to stream the ABC's in two letter increments back to the producer with a
new event type named "alphabet".

## Extra: Remove Command Policy
If it has not already been done, remove the command policy.

## Extra: Default Port
Right now the default port is 8080, switch to port 5000.

# Extra: Event Received
Events should be marked as received if the events endpoint returns 200 OK.  The old endpoint for this in daylin-sq
should be removed.
