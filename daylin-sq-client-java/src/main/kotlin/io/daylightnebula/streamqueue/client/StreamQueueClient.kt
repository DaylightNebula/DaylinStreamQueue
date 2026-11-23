package io.daylightnebula.streamqueue.client

import io.daylightnebula.streamqueue.core.StreamContact
import io.daylightnebula.streamqueue.core.Subscription
import io.daylightnebula.streamqueue.core.streamQueueJson
import java.io.IOException
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import kotlin.uuid.Uuid

/**
 * Thrown when a call to the server fails.
 *
 * @param message what failed
 * @param status the HTTP status that the server returned, or null if it never responded
 * @param cause what made the call fail, if there was no response
 */
class StreamQueueException(
    message: String,
    val status: Int? = null,
    cause: Throwable? = null
): RuntimeException(message, cause)

/**
 * Calls a daylin-sq server.
 *
 * @param serverAddress the address of the server, such as http://localhost:5000
 */
class StreamQueueClient(serverAddress: String) {

    private val api = serverAddress.trimEnd('/') + "/api"
    private val http = HttpClient
        .newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofSeconds(5))
        .build()

    /**
     * Submits an event to be sent to a subscriber.
     *
     * @param eventType the type of the event
     * @param message the message of the event
     * @param contact url called with a POST of a [StreamContact] once a subscriber receives the event, or null to not be
     * called. Only allowed if the mode of the event type is stream
     * @return the id of the event
     * @throws StreamQueueException if the call fails
     */
    fun submitEvent(eventType: String, message: String, contact: String? = null): Uuid {
        val query = contact?.let { "?contact=" + URLEncoder.encode(it, Charsets.UTF_8) }.orEmpty()
        return Uuid.parse(post("/submit-event/$eventType$query", message, "text/plain; charset=utf-8"))
    }

    /**
     * Subscribes to an event type. The subscriber must be fully set up before calling this, the server calls its health
     * endpoint before responding and rejects the subscription if it is not active. [StreamQueueConsumer] does this.
     *
     * @param subscription the subscription to add
     * @return the id of the subscription
     * @throws StreamQueueException if the call fails or the subscription is rejected
     */
    fun subscribe(subscription: Subscription): Uuid {
        val body = streamQueueJson.encodeToString(subscription)
        return Uuid.parse(post("/subscribe/${subscription.eventType}", body, "application/json"))
    }

    /**
     * Marks an event as completed, removing it from the server.
     *
     * @param id the id of the event
     * @return false if the event is no longer held
     * @throws StreamQueueException if the call fails
     */
    fun completeEvent(id: Uuid): Boolean = try {
        post("/complete-event/$id", "", "text/plain")
        true
    } catch (e: StreamQueueException) {
        if (e.status != 404) throw e
        false
    }

    private fun post(path: String, body: String, contentType: String): String {
        val request = HttpRequest
            .newBuilder(URI.create(api + path))
            .timeout(Duration.ofSeconds(30))
            .header("Content-Type", contentType)
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
        val response = try {
            http.send(request, HttpResponse.BodyHandlers.ofString())
        } catch (e: IOException) {
            throw StreamQueueException("Failed to call ${request.uri()}", cause = e)
        }

        val status = response.statusCode()
        if (status !in 200..299) throw StreamQueueException("${request.uri()} returned $status: ${response.body()}", status)
        return response.body()
    }
}
