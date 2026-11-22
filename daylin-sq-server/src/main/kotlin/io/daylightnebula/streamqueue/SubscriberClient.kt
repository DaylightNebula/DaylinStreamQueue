package io.daylightnebula.streamqueue

import io.daylightnebula.streamqueue.core.EventMessage
import io.daylightnebula.streamqueue.core.HealthResponse
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

/** Makes the HTTP calls to subscribers. */
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
     * @return true if the subscriber returned a success status in time
     */
    fun sendEvent(endpoint: String, event: EventMessage): Boolean {
        val request = HttpRequest
            .newBuilder(URI.create(endpoint))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(streamQueueJson.encodeToString(event)))
            .build()
        val response = send(request) ?: return false
        return response.statusCode() in 200..299
    }

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
