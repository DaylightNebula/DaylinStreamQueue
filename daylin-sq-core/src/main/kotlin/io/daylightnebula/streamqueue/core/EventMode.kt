package io.daylightnebula.streamqueue.core

import kotlinx.serialization.Serializable

/**
 * How the events of a type are handled. Simple events are only sent to a subscriber, while the subscriber of a response
 * event must return a [StreamEventResponse] that is passed on to its producer.
 */
@Serializable
enum class EventMode {
    Simple,
    Response
}
