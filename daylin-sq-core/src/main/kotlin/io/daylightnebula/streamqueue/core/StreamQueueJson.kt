package io.daylightnebula.streamqueue.core

import kotlinx.serialization.json.Json

/** The JSON format shared by the server and its clients, unknown fields are ignored so that either can be newer. */
val streamQueueJson = Json { ignoreUnknownKeys = true }
