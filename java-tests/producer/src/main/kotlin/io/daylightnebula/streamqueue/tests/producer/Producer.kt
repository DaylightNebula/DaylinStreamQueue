package io.daylightnebula.streamqueue.tests.producer

import io.daylightnebula.streamqueue.client.StreamQueueClient
import io.daylightnebula.streamqueue.core.streamQueueJson
import kotlinx.serialization.Serializable

/**
 * The message of a test event, must match the one in the consumer.
 *
 * @param text printed by the consumer
 * @param first added to second
 * @param second added to first
 */
@Serializable
data class TestEvent(val text: String, val first: Double, val second: Double)

/** Sends one test event after START_DELAY_SECONDS, then stops. */
fun main() {
    val delay = System.getenv("START_DELAY_SECONDS")?.toLong() ?: 0
    Thread.sleep(delay * 1000)

    val client = StreamQueueClient(System.getenv("SERVER_ADDRESS") ?: "http://localhost:8080")
    val message = streamQueueJson.encodeToString(TestEvent("Hello from the producer", 2.5, 4.0))
    println("Sent event ${client.submitEvent("test-event", message)}")
}
