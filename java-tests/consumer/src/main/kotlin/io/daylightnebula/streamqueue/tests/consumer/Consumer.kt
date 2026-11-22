package io.daylightnebula.streamqueue.tests.consumer

import com.sun.net.httpserver.HttpServer
import io.daylightnebula.streamqueue.client.StreamQueueClient
import io.daylightnebula.streamqueue.client.StreamQueueConsumer
import io.daylightnebula.streamqueue.client.StreamQueueException
import io.daylightnebula.streamqueue.core.StreamEventResponse
import io.daylightnebula.streamqueue.core.streamQueueJson
import kotlinx.serialization.Serializable
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
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

private const val STREAM_PORT = 3244

/** The alphabet events that are being consumed, counted down once they have been streamed. */
private val streams = ConcurrentHashMap<Uuid, CountDownLatch>()

/**
 * Prints the text and the sum of the numbers of every test event, and streams the alphabet to the producer of every
 * alphabet event, until it is stopped.
 */
fun main() {
    val client = StreamQueueClient(System.getenv("SERVER_ADDRESS") ?: "http://localhost:5000")
    val host = System.getenv("PUBLIC_HOST") ?: "localhost"
    val testConsumer = StreamQueueConsumer(
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
    val alphabetConsumer = StreamQueueConsumer(
        client = client,
        name = "alphabet-consumer",
        eventType = "alphabet",
        port = 3243,
        publicAddress = { "http://$host:$it" },
        streamResponse = { event ->
            streams[event.id] = CountDownLatch(1)
            StreamEventResponse("http://$host:$STREAM_PORT/alphabet/${event.id}", "two letters on each line")
        }
    ) { event ->
        // the producer does not have to connect
        val streamed = streams[event.id]?.await(30, TimeUnit.SECONDS) == true
        streams.remove(event.id)
        println(if (streamed) "Streamed the alphabet" else "The alphabet was not streamed")
    }

    startStreams()
    subscribe(testConsumer)
    subscribe(alphabetConsumer)
    Thread.currentThread().join()
}

/** Hosts the streams of the alphabet events that are being consumed. */
private fun startStreams() {
    val server = HttpServer.create(InetSocketAddress(STREAM_PORT), 0)
    server.executor = Executors.newVirtualThreadPerTaskExecutor()
    server.createContext("/alphabet/") { exchange ->
        exchange.use {
            val id = Uuid.parseOrNull(exchange.requestURI.path.substringAfterLast('/'))
            val stream = id?.let { streams[it] } ?: return@use exchange.sendResponseHeaders(404, -1)

            // a length of 0 sends each part as it is flushed
            exchange.sendResponseHeaders(200, 0)
            ('A'..'Z').chunked(2).forEach { letters ->
                exchange.responseBody.write("${letters.joinToString("")}\n".encodeToByteArray())
                exchange.responseBody.flush()
                Thread.sleep(100)
            }
            stream.countDown()
        }
    }
    server.start()
}

/** Keeps trying to subscribe, as the server may not be up or may not be able to look up this container yet. */
private fun subscribe(consumer: StreamQueueConsumer) {
    var id = trySubscribe { consumer.start() }
    while (id == null) {
        Thread.sleep(2_000)
        id = trySubscribe { consumer.resume() }
    }
    println("Subscribed as $id")
}

private fun trySubscribe(call: () -> Uuid): Uuid? = try {
    call()
} catch (e: StreamQueueException) {
    println("Failed to subscribe: ${e.message}")
    null
}
