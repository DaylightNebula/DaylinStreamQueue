package io.daylightnebula.streamqueue.client

import com.sun.net.httpserver.HttpServer
import io.daylightnebula.streamqueue.core.EventMessage
import io.daylightnebula.streamqueue.core.HealthResponse
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
            val path = exchange.requestURI.path.removePrefix("/api")
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

    private fun consumer(maxRunning: Int = 1, handler: (EventMessage) -> Unit = {}) =
        StreamQueueConsumer(client, "test", "test-event", maxRunning = maxRunning, handler = handler)

    private fun subscription(): Subscription =
        streamQueueJson.decodeFromString(calls.last { it.first == "/subscribe/test-event" }.second)

    private fun health(): HealthResponse {
        val request = HttpRequest.newBuilder(URI.create(subscription().healthEndpoint)).build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofString())
        assertEquals(200, response.statusCode())
        return streamQueueJson.decodeFromString(response.body())
    }

    private fun sendEvent(body: String = streamQueueJson.encodeToString(EventMessage(id, "test-event", "hello"))): Int {
        val request = HttpRequest
            .newBuilder(URI.create(subscription().eventEndpoint))
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
        return http.send(request, HttpResponse.BodyHandlers.ofString()).statusCode()
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
    fun `client returns if an event is still held`() {
        assertTrue(client.receiveEvent(id))
        assertTrue(client.completeEvent(id))

        status = { 404 }
        assertFalse(client.receiveEvent(id))
        assertFalse(client.completeEvent(id))
    }

    @Test
    fun `client throws when a call fails`() {
        status = { 500 }
        assertEquals(500, assertFailsWith<StreamQueueException> { client.submitEvent("test-event", "") }.status)
        assertEquals(500, assertFailsWith<StreamQueueException> { client.receiveEvent(id) }.status)

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
    fun `consumer receives, handles and completes an event`() {
        val handled = ConcurrentLinkedQueue<EventMessage>()
        consumer { handled += it }.use {
            it.start()
            assertEquals(200, sendEvent())
            waitFor { called("/complete-event/$id") }
        }

        assertEquals(listOf(EventMessage(id, "test-event", "hello")), handled.toList())
        assertEquals(listOf("/recv-event/$id", "/complete-event/$id"), calls.drop(1).map { it.first })
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
    fun `consumer does not handle an event that is no longer held`() {
        status = { if (it.startsWith("/recv-event")) 404 else 200 }
        var handled = false
        consumer { handled = true }.use {
            it.start()
            sendEvent()
            waitFor { called("/recv-event/$id") && health().acceptingEvents }
        }

        assertFalse(handled)
        assertFalse(called("/complete-event/$id"))
    }

    @Test
    fun `consumer does not complete an event when the handler throws and stays available`() {
        consumer { error("expected by the test") }.use {
            it.start()
            sendEvent()
            waitFor { called("/recv-event/$id") && health().acceptingEvents }
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
