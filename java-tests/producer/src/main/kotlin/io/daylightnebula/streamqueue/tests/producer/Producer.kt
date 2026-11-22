package io.daylightnebula.streamqueue.tests.producer

import com.sun.net.httpserver.HttpServer
import io.daylightnebula.streamqueue.client.StreamQueueClient
import io.daylightnebula.streamqueue.core.StreamContact
import io.daylightnebula.streamqueue.core.streamQueueJson
import kotlinx.serialization.Serializable
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

/**
 * The message of a test event, must match the one in the consumer.
 *
 * @param text printed by the consumer
 * @param first added to second
 * @param second added to first
 */
@Serializable
data class TestEvent(val text: String, val first: Double, val second: Double)

private const val CONTACT_PORT = 3245

/** Sends one test event after START_DELAY_SECONDS, then prints the stream of one alphabet event and stops. */
fun main() {
    val delay = System.getenv("START_DELAY_SECONDS")?.toLong() ?: 0
    Thread.sleep(delay * 1000)

    val client = StreamQueueClient(System.getenv("SERVER_ADDRESS") ?: "http://localhost:5000")
    val message = streamQueueJson.encodeToString(TestEvent("Hello from the producer", 2.5, 4.0))
    println("Sent event ${client.submitEvent("test-event", message)}")

    streamAlphabet(client, System.getenv("PUBLIC_HOST") ?: "localhost")
}

/** Sends an alphabet event, waits to be told how to contact its consumer, then prints what it streams. */
private fun streamAlphabet(client: StreamQueueClient, host: String) {
    val contacted = CompletableFuture<StreamContact>()
    val server = HttpServer.create(InetSocketAddress(CONTACT_PORT), 0)
    server.createContext("/contact") { exchange ->
        exchange.use {
            contacted.complete(streamQueueJson.decodeFromString(exchange.requestBody.readBytes().decodeToString()))
            exchange.sendResponseHeaders(200, -1)
        }
    }
    server.start()

    try {
        val id = client.submitEvent("alphabet", "the ABC's please", "http://$host:$CONTACT_PORT/contact")
        println("Sent event $id")
        val contact = contacted.get(30, TimeUnit.SECONDS)
        println("Streaming \"${contact.message}\" from ${contact.contact} with ${contact.extraInfo}")

        val request = HttpRequest.newBuilder(URI.create(contact.contact)).build()
        val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofLines())
        check(response.statusCode() == 200) { "${contact.contact} returned ${response.statusCode()}" }
        response.body().forEach { println(it) }
    } finally {
        server.stop(0)
    }
}
