package io.daylightnebula.streamqueue.core

import kotlinx.serialization.Serializable

/**
 * What a subscriber's health endpoint responds with.
 *
 * @param isActive if the subscriber should stay subscribed, when false its subscription is removed
 * @param acceptingEvents if the subscriber can be sent an event now, false means that it is busy
 */
@Serializable
data class HealthResponse(
    val isActive: Boolean,
    val acceptingEvents: Boolean
)
