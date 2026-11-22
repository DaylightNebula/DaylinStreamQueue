package io.daylightnebula.streamqueue.tests.consumer

import io.daylightnebula.streamqueue.client.StreamQueueClient
import io.daylightnebula.streamqueue.client.StreamQueueConsumer
import io.daylightnebula.streamqueue.client.StreamQueueException
import io.daylightnebula.streamqueue.core.streamQueueJson
import kotlinx.serialization.Serializable
import kotlin.uuid.Uuid

/**
 * The message of a test event, must match the one in the producer.
 *
 * @param text printed by the consumer
 * @param first added to second
 * @param second added to first
 */
@Serializable
data class TestEvent(val text: String, val first: Double, val second: Double)

/** Prints the text and the sum of the numbers of every test event, until it is stopped. */
fun main() {
    val client = StreamQueueClient(System.getenv("SERVER_ADDRESS") ?: "http://localhost:8080")
    val host = System.getenv("PUBLIC_HOST") ?: "localhost"
    val consumer = StreamQueueConsumer(
        client = client,
        name = "test-consumer",
        eventType = "test-event",
        port = 3242,
        publicAddress = { "http://$host:$it" }
    ) { event ->
        val test = streamQueueJson.decodeFromString<TestEvent>(event.message)
        println(test.text)
        println(test.first + test.second)
    }

    // the server may not be up, or may not be able to look up this container yet
    var id = subscribe { consumer.start() }
    while (id == null) {
        Thread.sleep(2_000)
        id = subscribe { consumer.resume() }
    }
    println("Subscribed as $id")
    Thread.currentThread().join()
}

private fun subscribe(call: () -> Uuid): Uuid? = try {
    call()
} catch (e: StreamQueueException) {
    println("Failed to subscribe: ${e.message}")
    null
}
