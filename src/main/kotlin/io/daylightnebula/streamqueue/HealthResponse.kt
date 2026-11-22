package io.daylightnebula.streamqueue

import kotlinx.serialization.Serializable

@Serializable
data class HealthResponse(
    val isActive: Boolean,
    val acceptingEvents: Boolean
)