package io.daylightnebula.streamqueue.core

import kotlinx.serialization.Serializable
import kotlin.uuid.Uuid

/**
 * The body posted to a subscriber's event endpoint.
 *
 * @param id used by the subscriber to mark the event as received and completed
 * @param eventType the type of the event
 * @param message the event's message, exactly as it was submitted
 */
@Serializable
data class EventMessage(
    val id: Uuid,
    val eventType: String,
    val message: String
)
