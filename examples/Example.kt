// Needs a server on port 5000 that uses daylin-sq-server/config.yml, and the com.daylin:daylin-sq-client-java dependency.

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.daylightnebula.streamqueue.client.StreamQueueClient
import io.daylightnebula.streamqueue.client.StreamQueueConsumer
import io.daylightnebula.streamqueue.core.StreamContact
import io.daylightnebula.streamqueue.core.StreamEventResponse
import io.daylightnebula.streamqueue.core.streamQueueJson
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

private const val STREAM_PORT = 3244
private const val CONTACT_PORT = 3245

/**
 * Consumes and submits a simple event, then streams the alphabet from the consumer to the producer of a stream event.
 */
fun main() {
    val client = StreamQueueClient("http://localhost:5000")
    simpleEvent(client)
    streamEvent(client)
}

/** Consumes one test event that prints its message. */
private fun simpleEvent(client: StreamQueueClient) {
    val handled = CountDownLatch(1)
    val consumer = StreamQueueConsumer(client, "example", "test-event") { event ->
        println("Received: ${event.message}")
        handled.countDown()
    }

    // submit an event and wait for the consumer to handle it
    consumer.use {
        it.start()
        client.submitEvent("test-event", "Hello from Kotlin")
        handled.await()
    }
}

/** Consumes one alphabet event, whose producer reads the alphabet from the consumer. */
private fun streamEvent(client: StreamQueueClient) {
    // consumer: host the stream, this can be anything that the producer knows how to contact
    val streams = host(STREAM_PORT) { exchange ->
        exchange.sendResponseHeaders(200, 0)
        ('A'..'Z').forEach { exchange.responseBody.write("$it\n".encodeToByteArray()) }
    }

    // consumer: tell the producer where the stream is, the handler could keep working on the event while it is read
    val consumer = StreamQueueConsumer(
        client = client,
        name = "example",
        eventType = "alphabet",
        streamResponse = { event ->
            StreamEventResponse("http://localhost:$STREAM_PORT/alphabet/${event.id}", "one letter on each line")
        }
    ) { event -> println("Consuming ${event.id}") }

    // producer: host the endpoint that is told how to contact the consumer
    val contacted = CompletableFuture<StreamContact>()
    val contacts = host(CONTACT_PORT) { exchange ->
        contacted.complete(streamQueueJson.decodeFromString(exchange.requestBody.readBytes().decodeToString()))
        exchange.sendResponseHeaders(200, -1)
    }

    // producer: submit the event, then read the stream
    consumer.use {
        it.start()
        client.submitEvent("alphabet", "the ABC's please", "http://localhost:$CONTACT_PORT/contact")
        val contact = contacted.get(30, TimeUnit.SECONDS)
        println("Streaming from ${contact.contact} with ${contact.extraInfo}")
        val request = HttpRequest.newBuilder(URI.create(contact.contact)).build()
        val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofLines())
        response.body().forEach { line -> println(line) }
    }
    streams.stop(0)
    contacts.stop(0)
}

/** Hosts a server. */
private fun host(port: Int, handle: (HttpExchange) -> Unit): HttpServer {
    val server = HttpServer.create(InetSocketAddress(port), 0)
    server.createContext("/") { exchange -> exchange.use(handle) }
    server.start()
    return server
}
