package io.daylightnebula.streamqueue.core

import kotlinx.serialization.Serializable

/**
 * A request to be sent every event of one type.
 *
 * The subscriber must be fully set up before it subscribes: the server calls the health endpoint during the request and
 * rejects the subscription unless it responds as active. Whether it is busy does not matter.
 *
 * @param name not unique, just used for debugging
 * @param eventType what event type this subscription is subscribing to, which is registered if it does not exist yet
 * @param mode how the events are handled, must match the mode that the event type was registered with
 * @param healthEndpoint url called with a GET for a [HealthResponse]
 * @param eventEndpoint url called with a POST of an [EventMessage] for each event
 */
@Serializable
data class Subscription(
    val name: String,
    val eventType: String,
    val mode: EventMode,
    val healthEndpoint: String,
    val eventEndpoint: String
)
