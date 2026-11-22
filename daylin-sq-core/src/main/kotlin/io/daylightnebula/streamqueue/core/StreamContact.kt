package io.daylightnebula.streamqueue.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * The body posted to the contact address of a producer once a subscriber has received its stream event.
 *
 * @param message the event's message, exactly as it was submitted
 * @param contact how the producer may contact the subscriber, the producer does not have to
 * @param extraInfo the extra info from the subscriber, if there is any
 */
@Serializable
data class StreamContact(
    val message: String,
    val contact: String,
    @SerialName("extra-info")
    val extraInfo: String? = null
)
