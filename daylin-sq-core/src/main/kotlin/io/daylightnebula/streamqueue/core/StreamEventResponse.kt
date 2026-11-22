package io.daylightnebula.streamqueue.core

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * What a subscriber's event endpoint must respond with when the mode of the event is stream.
 *
 * @param contactAddress how the producer may contact the subscriber while it processes the event
 * @param extraInfo passed on to the producer as it is, if there is any
 */
@Serializable
data class StreamEventResponse(
    val contactAddress: String,
    @SerialName("extra-info")
    val extraInfo: String? = null
)
