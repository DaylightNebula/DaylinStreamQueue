package io.daylightnebula.streamqueue

import io.daylightnebula.streamqueue.core.EventMessage
import io.daylightnebula.streamqueue.core.HealthResponse
import io.daylightnebula.streamqueue.core.StreamContact
import io.daylightnebula.streamqueue.core.StreamEventResponse
import io.daylightnebula.streamqueue.core.streamQueueJson
import kotlinx.serialization.SerializationException
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/** Makes the HTTP calls to subscribers and producers. */
object SubscriberClient {

    private val logger = LoggerFactory.getLogger(SubscriberClient::class.java)
    private val client = HttpClient
        .newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .build()

    /**
     * Checks if an endpoint is something that can be called.
     *
     * @param endpoint the url to check
     * @return true if the endpoint is a valid http or https url
     */
    fun isValidEndpoint(endpoint: String): Boolean =
        runCatching { HttpRequest.newBuilder(URI.create(endpoint)) }.isSuccess

    /**
     * Calls a health endpoint.
     *
     * @param endpoint the url of the health endpoint
     * @return the health of the subscriber, or null if the call failed, timed out or did not return 200
     */
    fun checkHealth(endpoint: String): HealthResponse? {
        val request = HttpRequest
            .newBuilder(URI.create(endpoint))
            .GET()
            .build()
        val response = send(request) ?: return null
        if (response.statusCode() != 200) return null

        return try {
            streamQueueJson.decodeFromString<HealthResponse>(response.body())
        } catch (e: SerializationException) {
            logger.warn("{} did not return a health response: {}", endpoint, e.message)
            null
        }
    }

    /**
     * Posts an event to an event endpoint.
     *
     * @param endpoint the url of the event endpoint
     * @param event the event to send
     * @return true if the subscriber returned 200 in time
     */
    fun sendEvent(endpoint: String, event: EventMessage): Boolean = postEvent(endpoint, event) != null

    /**
     * Posts a stream event to an event endpoint.
     *
     * @param endpoint the url of the event endpoint
     * @param event the event to send
     * @return how to contact the subscriber, or null if the call failed, timed out or did not return 200 with a valid
     * response
     */
    fun sendStreamEvent(endpoint: String, event: EventMessage): StreamEventResponse? {
        val body = postEvent(endpoint, event) ?: return null
        return try {
            streamQueueJson.decodeFromString<StreamEventResponse>(body)
        } catch (e: SerializationException) {
            logger.warn("{} did not return a stream event response: {}", endpoint, e.message)
            null
        }
    }

    /**
     * Tells a producer how to contact the subscriber that received its event. This does not wait for the call, and
     * nothing happens if it fails.
     *
     * @param endpoint the contact address of the producer
     * @param contact how to contact the subscriber
     */
    fun contactProducer(endpoint: String, contact: StreamContact) {
        Thread.startVirtualThread {
            runCatching { send(postOf(endpoint, streamQueueJson.encodeToString(contact))) }
                .onFailure { logger.warn("Failed to contact the producer at {}", endpoint, it) }
        }
    }

    /** Returns the body of the response, or null if the event was not received. */
    private fun postEvent(endpoint: String, event: EventMessage): String? =
        send(postOf(endpoint, streamQueueJson.encodeToString(event)))
            ?.takeIf { it.statusCode() == 200 }
            ?.body()

    private fun postOf(endpoint: String, body: String): HttpRequest = HttpRequest
        .newBuilder(URI.create(endpoint))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build()

    // the timeout is on the future because a timeout on the request may stop counting once the headers arrive
    private fun send(request: HttpRequest): HttpResponse<String>? {
        val future = client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        val timeout = Config.get().durations.subscriberTimeout
        return try {
            future.get(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
        } catch (_: TimeoutException) {
            future.cancel(true)
            logger.warn("{} did not respond within {}", request.uri(), timeout)
            null
        } catch (e: ExecutionException) {
            if (e.cause !is IOException) throw IllegalStateException("calling ${request.uri()}", e)
            logger.warn("Failed to call {}: {}", request.uri(), e.cause.toString())
            null
        }
    }
}
