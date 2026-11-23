package io.daylightnebula.streamqueue.client

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.daylightnebula.streamqueue.core.EventMessage
import io.daylightnebula.streamqueue.core.EventMode
import io.daylightnebula.streamqueue.core.HealthResponse
import io.daylightnebula.streamqueue.core.StreamEventResponse
import io.daylightnebula.streamqueue.core.Subscription
import io.daylightnebula.streamqueue.core.streamQueueJson
import kotlinx.serialization.SerializationException
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.uuid.Uuid

/**
 * Hosts the endpoints of a subscriber and consumes its events. Call [start] to begin.
 *
 * @param client the server to consume events from
 * @param name the name of the subscription, not unique and just used for debugging
 * @param eventType the type of event to consume
 * @param port the port to host the endpoints on, or 0 to use any free port
 * @param publicAddress finds the address that the server can reach this consumer at, from the port that is used
 * @param maxRunning how many events to consume at once before responding as busy
 * @param streamResponse finds how the producer may contact this consumer about an event, called before the handler and
 * must return within 5 seconds. The mode of the subscription is response if this is given and simple if not. If this
 * throws then the event is rejected and the server removes the subscription
 * @param handler consumes an event, which is completed when this returns. If this throws, then the event is sent again
 * after the server's max processing duration
 */
class StreamQueueConsumer(
    private val client: StreamQueueClient,
    private val name: String,
    private val eventType: String,
    private val port: Int = 0,
    private val publicAddress: (port: Int) -> String = { "http://localhost:$it" },
    private val maxRunning: Int = 1,
    private val streamResponse: ((EventMessage) -> StreamEventResponse)? = null,
    private val handler: (EventMessage) -> Unit
) : AutoCloseable {

    private val logger = System.getLogger(StreamQueueConsumer::class.java.name)
    private val executor = Executors.newVirtualThreadPerTaskExecutor()
    private val running = AtomicInteger()
    private var server: HttpServer? = null

    @Volatile
    private var active = false

    /**
     * Hosts the endpoints and then subscribes. If subscribing fails then the endpoints stay up, and [resume] can be
     * called to try again.
     *
     * @return the id of the subscription
     * @throws StreamQueueException if subscribing fails
     */
    fun start(): Uuid {
        check(server == null) { "The consumer was already started" }
        server = HttpServer.create(InetSocketAddress(port), 0).also {
            it.executor = executor
            it.createContext("/health") { exchange -> exchange.use(::health) }
            it.createContext("/event") { exchange -> exchange.use(::event) }
            it.start()
        }
        return resume()
    }

    /** Responds as inactive, so that the server removes the subscription. Running events are still finished. */
    fun pause() {
        active = false
    }

    /**
     * Subscribes again, after [pause] or if the server removed the subscription.
     *
     * @return the id of the new subscription
     * @throws StreamQueueException if subscribing fails
     */
    fun resume(): Uuid {
        val address = publicAddress(checkNotNull(server) { "The consumer was not started" }.address.port).trimEnd('/')

        // the server checks that this is active before it accepts the subscription
        active = true
        val mode = if (streamResponse != null) EventMode.Response else EventMode.Simple
        return client.subscribe(Subscription(name, eventType, mode, "$address/health", "$address/event"))
    }

    /** Stops taking events and waits for the running ones to finish. */
    override fun close() {
        active = false
        server?.stop(0)
        executor.close()
    }

    private fun health(exchange: HttpExchange) {
        val health = HealthResponse(isActive = active, acceptingEvents = active && running.get() < maxRunning)
        exchange.responseHeaders["Content-Type"] = "application/json"
        exchange.respond(200, streamQueueJson.encodeToString(health))
    }

    private fun event(exchange: HttpExchange) {
        if (exchange.requestMethod != "POST") return exchange.respond(405)
        if (!active) return exchange.respond(503)
        val event = try {
            streamQueueJson.decodeFromString<EventMessage>(exchange.requestBody.readBytes().decodeToString())
        } catch (e: SerializationException) {
            logger.log(System.Logger.Level.WARNING, "Received an invalid event", e)
            return exchange.respond(400)
        }

        val response = try {
            streamResponse?.invoke(event)
        } catch (e: Exception) {
            logger.log(System.Logger.Level.ERROR, "Failed to find the stream response of event ${event.id}", e)
            return exchange.respond(500)
        }

        // count before responding, so that the server's next health check sees it
        running.incrementAndGet()
        executor.submit { consume(event) }
        exchange.responseHeaders["Content-Type"] = "application/json"
        exchange.respond(200, response?.let { streamQueueJson.encodeToString(it) }.orEmpty())
    }

    private fun consume(event: EventMessage) {
        try {
            try {
                handler(event)
            } finally {
                // before completing, as that makes the server check if this can take another event
                running.decrementAndGet()
            }
            if (!client.completeEvent(event.id))
                logger.log(System.Logger.Level.WARNING, "Event ${event.id} is no longer held by this consumer")
        } catch (e: Exception) {
            logger.log(System.Logger.Level.ERROR, "Failed to consume event ${event.id}", e)
        }
    }

    private fun HttpExchange.respond(status: Int, body: String = "") {
        val bytes = body.encodeToByteArray()
        sendResponseHeaders(status, if (bytes.isEmpty()) -1 else bytes.size.toLong())
        if (bytes.isNotEmpty()) responseBody.write(bytes)
    }
}
