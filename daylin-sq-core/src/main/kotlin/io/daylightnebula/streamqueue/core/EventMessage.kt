package io.daylightnebula.streamqueue.core

import kotlinx.serialization.Serializable
import kotlin.uuid.Uuid

/**
 * The body posted to a subscriber's event endpoint, which marks the event as received by returning 200. If the mode of
 * the event is response, then it must return a [StreamEventResponse].
 *
 * @param id used by the subscriber to mark the event as completed
 * @param eventType the type of the event
 * @param message the event's message, exactly as it was submitted
 */
@Serializable
data class EventMessage(
    val id: Uuid,
    val eventType: String,
    val message: String
)
