package io.daylightnebula.streamqueue.client

import com.sun.net.httpserver.HttpServer
import io.daylightnebula.streamqueue.core.EventMessage
import io.daylightnebula.streamqueue.core.HealthResponse
import io.daylightnebula.streamqueue.core.StreamEventResponse
import io.daylightnebula.streamqueue.core.Subscription
import io.daylightnebula.streamqueue.core.streamQueueJson
import org.junit.jupiter.api.Timeout
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.uuid.Uuid

/** Uses a fake server that records the calls made to it. */
@Timeout(30)
class StreamQueueConsumerTests {

    private val id = Uuid.random()
    private val calls = ConcurrentLinkedQueue<Pair<String, String>>()
    private val http = HttpClient.newHttpClient()
    private lateinit var server: HttpServer
    private lateinit var client: StreamQueueClient

    @Volatile
    private var status: (path: String) -> Int = { 200 }

    @BeforeTest
    fun open() {
        server = HttpServer.create(InetSocketAddress("localhost", 0), 0)
        server.executor = Executors.newVirtualThreadPerTaskExecutor()
        server.createContext("/api/") { exchange ->
            val path = exchange.requestURI.toString().removePrefix("/api")
            calls += path to exchange.requestBody.readBytes().decodeToString()
            val body = id.toString().encodeToByteArray()
            exchange.sendResponseHeaders(status(path), body.size.toLong())
            exchange.responseBody.write(body)
            exchange.close()
        }
        server.start()
        client = StreamQueueClient("http://localhost:${server.address.port}/")
    }

    @AfterTest
    fun close() = server.stop(0)

    private fun consumer(
        maxRunning: Int = 1,
        streamResponse: ((EventMessage) -> StreamEventResponse)? = null,
        handler: (EventMessage) -> Unit = {}
    ) = StreamQueueConsumer(
        client,
        "test",
        "test-event",
        maxRunning = maxRunning,
        streamResponse = streamResponse,
        handler = handler
    )

    private fun subscription(): Subscription =
        streamQueueJson.decodeFromString(calls.last { it.first == "/subscribe/test-event" }.second)

    private fun health(): HealthResponse {
        val request = HttpRequest.newBuilder(URI.create(subscription().healthEndpoint)).build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofString())
        assertEquals(200, response.statusCode())
        return streamQueueJson.decodeFromString(response.body())
    }

    private val eventBody = streamQueueJson.encodeToString(EventMessage(id, "test-event", "hello"))

    private fun sendEvent(body: String = eventBody): Int = sendEventForResponse(body).statusCode()

    private fun sendEventForResponse(body: String = eventBody): HttpResponse<String> {
        val request = HttpRequest
            .newBuilder(URI.create(subscription().eventEndpoint))
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
        return http.send(request, HttpResponse.BodyHandlers.ofString())
    }

    private fun waitFor(condition: () -> Boolean) {
        while (!condition()) Thread.sleep(5)
    }

    private fun called(path: String) = calls.any { it.first == path }

    @Test
    fun `client submits events`() {
        assertEquals(id, client.submitEvent("test-event", "héllo"))
        assertEquals(listOf("/submit-event/test-event" to "héllo"), calls.toList())
    }

    @Test
    fun `client submits events with an encoded contact address`() {
        assertEquals(id, client.submitEvent("test-event", "hello", "http://producer:3245/contact?a=b&c=é"))

        val query = "contact=http%3A%2F%2Fproducer%3A3245%2Fcontact%3Fa%3Db%26c%3D%C3%A9"
        assertEquals(listOf("/submit-event/test-event?$query" to "hello"), calls.toList())
    }

    @Test
    fun `client returns if an event is still held`() {
        assertTrue(client.completeEvent(id))

        status = { 404 }
        assertFalse(client.completeEvent(id))
    }

    @Test
    fun `client throws when a call fails`() {
        status = { 500 }
        assertEquals(500, assertFailsWith<StreamQueueException> { client.submitEvent("test-event", "") }.status)
        assertEquals(500, assertFailsWith<StreamQueueException> { client.completeEvent(id) }.status)

        server.stop(0)
        assertNull(assertFailsWith<StreamQueueException> { client.submitEvent("test-event", "") }.status)
    }

    @Test
    fun `consumer is active before it subscribes`() {
        var healthWhenSubscribing: HealthResponse? = null
        status = {
            healthWhenSubscribing = health()
            200
        }

        consumer().use { assertEquals(id, it.start()) }

        assertEquals(HealthResponse(isActive = true, acceptingEvents = true), healthWhenSubscribing)
    }

    @Test
    fun `consumer handles and completes an event`() {
        val handled = ConcurrentLinkedQueue<EventMessage>()
        consumer { handled += it }.use {
            it.start()
            assertEquals(200, sendEvent())
            waitFor { called("/complete-event/$id") }
        }

        assertEquals(listOf(EventMessage(id, "test-event", "hello")), handled.toList())
        assertEquals(listOf("/complete-event/$id"), calls.drop(1).map { it.first })
    }

    @Test
    fun `consumer returns no body without a stream response`() {
        consumer().use {
            it.start()
            assertEquals("", sendEventForResponse().body())
        }
    }

    @Test
    fun `consumer returns its stream response before the event is handled`() {
        val finish = CountDownLatch(1)
        val asked = ConcurrentLinkedQueue<EventMessage>()
        val stream = { event: EventMessage ->
            asked += event
            StreamEventResponse("http://consumer:3244/${event.id}", "info")
        }
        consumer(streamResponse = stream, handler = { finish.await() }).use {
            it.start()
            val response = sendEventForResponse()
            assertFalse(called("/complete-event/$id"))
            finish.countDown()

            assertEquals(200, response.statusCode())
            assertEquals("""{"contactAddress":"http://consumer:3244/$id","extra-info":"info"}""", response.body())
            waitFor { called("/complete-event/$id") }
        }

        assertEquals(listOf(EventMessage(id, "test-event", "hello")), asked.toList())
    }

    @Test
    fun `consumer rejects an event when its stream response throws and stays available`() {
        var handled = false
        consumer(streamResponse = { error("expected by the test") }, handler = { handled = true }).use {
            it.start()
            assertEquals(500, sendEvent())
            assertTrue(health().acceptingEvents)
        }

        assertFalse(handled)
        assertFalse(called("/complete-event/$id"))
    }

    @Test
    fun `consumer is busy until its event is handled and free before it is completed`() {
        val finish = CountDownLatch(1)
        var healthWhenCompleting: HealthResponse? = null
        status = {
            if (it.startsWith("/complete-event")) healthWhenCompleting = health()
            200
        }
        consumer { finish.await() }.use {
            it.start()
            sendEvent()
            assertEquals(HealthResponse(isActive = true, acceptingEvents = false), health())

            finish.countDown()
            waitFor { healthWhenCompleting != null }
        }

        assertEquals(HealthResponse(isActive = true, acceptingEvents = true), healthWhenCompleting)
    }

    @Test
    fun `consumer can run many events at once`() {
        val finish = CountDownLatch(1)
        consumer(maxRunning = 2) { finish.await() }.use {
            it.start()
            sendEvent()
            assertTrue(health().acceptingEvents)
            sendEvent()
            assertFalse(health().acceptingEvents)
            finish.countDown()
        }
    }

    @Test
    fun `consumer stays available when its event is no longer held`() {
        status = { if (it.startsWith("/complete-event")) 404 else 200 }
        consumer().use {
            it.start()
            sendEvent()
            waitFor { called("/complete-event/$id") && health().acceptingEvents }
        }
    }

    @Test
    fun `consumer does not complete an event when the handler throws and stays available`() {
        val started = CountDownLatch(1)
        consumer {
            started.countDown()
            error("expected by the test")
        }.use {
            it.start()
            sendEvent()
            started.await()
            waitFor { health().acceptingEvents }
        }

        assertFalse(called("/complete-event/$id"))
    }

    @Test
    fun `a paused consumer is inactive until it resumes`() {
        consumer().use {
            it.start()
            it.pause()
            assertEquals(HealthResponse(isActive = false, acceptingEvents = false), health())
            assertEquals(503, sendEvent())

            it.resume()
            assertEquals(HealthResponse(isActive = true, acceptingEvents = true), health())
            assertEquals(2, calls.count { (path) -> path == "/subscribe/test-event" })
        }
    }

    @Test
    fun `invalid events are rejected`() {
        consumer().use {
            it.start()
            assertEquals(400, sendEvent("not json"))
            assertTrue(health().acceptingEvents)
        }
    }

    @Test
    fun `a rejected subscription can be tried again`() {
        status = { 422 }
        consumer().use {
            assertEquals(422, assertFailsWith<StreamQueueException> { it.start() }.status)
            status = { 200 }
            assertEquals(id, it.resume())
        }
    }
}
