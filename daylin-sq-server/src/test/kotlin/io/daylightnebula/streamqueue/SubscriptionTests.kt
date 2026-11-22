package io.daylightnebula.streamqueue

import com.sun.net.httpserver.HttpServer
import io.daylightnebula.streamqueue.core.EventMessage
import io.daylightnebula.streamqueue.core.HealthResponse
import io.daylightnebula.streamqueue.core.StreamContact
import io.daylightnebula.streamqueue.core.StreamEventResponse
import io.daylightnebula.streamqueue.core.Subscription
import io.daylightnebula.streamqueue.core.streamQueueJson
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.io.TempDir
import org.rocksdb.ColumnFamilyDescriptor
import org.rocksdb.ColumnFamilyHandle
import org.rocksdb.DBOptions
import org.rocksdb.RocksDB
import org.rocksdb.WriteOptions
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.ResultActionsDsl
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import java.net.InetSocketAddress
import java.nio.file.Path
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeSource
import kotlin.uuid.Uuid

/** Uses a temporary folder and fake subscribers for every test. */
@Timeout(60)
class SubscriptionTests {

    @TempDir
    lateinit var folder: Path

    private lateinit var db: RocksDB
    private lateinit var subscriptionStore: SubscriptionStore
    private lateinit var first: Database.EventStore
    private val subscribers = mutableListOf<Subscriber>()
    private val producers = mutableListOf<Producer>()

    private val mockMvc = MockMvcBuilders.standaloneSetup(Service()).build()
    private val durations = Config.Durations(20.minutes, 20.minutes, 24.hours, subscriberTimeout = 500.milliseconds)

    /** Every check is always due with this. */
    private val alwaysDue = durations.copy(healthCheckInterval = 1.seconds)

    @BeforeTest
    fun open() {
        RocksDB.loadLibrary()
        Database.writeOptions = WriteOptions()
        val handles = mutableListOf<ColumnFamilyHandle>()
        val families = listOf("default", "subscriptions", "first", "second")
            .map { ColumnFamilyDescriptor(it.encodeToByteArray()) }
        val options = DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
        db = RocksDB.open(options, folder.toString(), families, handles)
        subscriptionStore = SubscriptionStore(db, handles[1])
        first = Database.EventStore(db, handles[2])
        Database.stores = mapOf("first" to first, "second" to Database.EventStore(db, handles[3]))
        Config.config = Config(events = emptyMap(), durations = durations)
        Subscriptions.init(subscriptionStore)
    }

    @AfterTest
    fun close() {
        Dispatcher.start(emptySet())
        subscribers.forEach { it.server.stop(0) }
        producers.forEach { it.server.stop(0) }
        db.close()
        Database.stores = emptyMap()
        Config.config = null
    }

    /** A subscriber with endpoints that can be told to fail. */
    private inner class Subscriber {
        @Volatile var health = HealthResponse(isActive = true, acceptingEvents = true)
        @Volatile var healthStatus = 200
        @Volatile var healthBody: String? = null
        @Volatile var healthDelay = 0L
        @Volatile var eventStatus = 200
        @Volatile var eventBody = ""
        @Volatile var eventDelay = 0L
        val healthChecks = AtomicInteger()
        val events = ConcurrentLinkedQueue<EventMessage>()

        val server: HttpServer = HttpServer.create(InetSocketAddress("localhost", 0), 0).also {
            it.executor = Executors.newVirtualThreadPerTaskExecutor()
            it.createContext("/health") { exchange ->
                healthChecks.incrementAndGet()
                Thread.sleep(healthDelay)
                val body = (healthBody ?: streamQueueJson.encodeToString(health)).encodeToByteArray()
                exchange.sendResponseHeaders(healthStatus, body.size.toLong())
                exchange.responseBody.write(body)
                exchange.close()
            }
            it.createContext("/event") { exchange ->
                assertEquals("POST", exchange.requestMethod)
                events += streamQueueJson.decodeFromString<EventMessage>(exchange.requestBody.readBytes().decodeToString())
                Thread.sleep(eventDelay)
                val body = eventBody.encodeToByteArray()
                exchange.sendResponseHeaders(eventStatus, if (body.isEmpty()) -1 else body.size.toLong())
                exchange.responseBody.write(body)
                exchange.close()
            }
            it.start()
        }

        val address = "http://localhost:${server.address.port}"

        fun subscription(type: String = "first", name: String = "test") =
            Subscription(name, type, "$address/health", "$address/event")

        fun subscribe(type: String = "first", name: String = "test"): Uuid =
            assertNotNull(Subscriptions.add(subscription(type, name)))

        fun messages() = events.map { it.message }
    }

    /** A producer that records the bodies posted to its contact address. */
    private inner class Producer {
        @Volatile var status = 200
        @Volatile var delay = 0L
        val contacts = ConcurrentLinkedQueue<String>()

        val server: HttpServer = HttpServer.create(InetSocketAddress("localhost", 0), 0).also {
            it.executor = Executors.newVirtualThreadPerTaskExecutor()
            it.createContext("/contact") { exchange ->
                assertEquals("POST", exchange.requestMethod)
                contacts += exchange.requestBody.readBytes().decodeToString()
                Thread.sleep(delay)
                exchange.sendResponseHeaders(status, -1)
                exchange.close()
            }
            it.start()
        }

        val address = "http://localhost:${server.address.port}/contact"
    }

    private fun subscriber() = Subscriber().also { subscribers += it }

    private fun producer() = Producer().also { producers += it }

    /** A subscriber to the first event type, which becomes a stream, that responds with the given body. */
    private fun streamSubscriber(body: String): Subscriber {
        val events = mapOf("first" to Config.EventDescription(Config.Mode.Stream, emptyList()))
        Config.config = Config(events = events, durations = durations)
        return subscriber().also {
            it.eventBody = body
            it.subscribe()
        }
    }

    private fun streamSubscriber(response: StreamEventResponse) = streamSubscriber(streamQueueJson.encodeToString(response))

    private fun post(subscription: String, type: String = "first"): ResultActionsDsl =
        mockMvc.post("/api/subscribe/$type") {
            contentType = MediaType.APPLICATION_JSON
            content = subscription
        }

    private fun post(subscription: Subscription, type: String = subscription.eventType) =
        post(streamQueueJson.encodeToString(subscription), type)

    private fun ids(type: String = "first") = Subscriptions.of(type).map { it.id }

    private fun push(vararg messages: String, type: String = "first") =
        messages.forEach { Database.push(type, Uuid.random(), it) }

    private fun waitFor(message: String, condition: () -> Boolean) {
        val start = TimeSource.Monotonic.markNow()
        while (!condition()) {
            assertTrue(start.elapsedNow() < 10.seconds, message)
            Thread.sleep(5)
        }
    }

    @Test
    fun `subscribe returns the id and saves the subscription`() {
        val subscription = subscriber().subscription()

        val id = post(subscription).andExpect { status { isOk() } }.andReturn().response.contentAsString

        assertEquals(listOf(Uuid.parse(id)), ids())
        assertEquals(mapOf(Uuid.parse(id) to subscription), subscriptionStore.load())
    }

    @Test
    fun `a busy subscriber can subscribe`() {
        val subscriber = subscriber()
        subscriber.health = HealthResponse(isActive = true, acceptingEvents = false)

        post(subscriber.subscription()).andExpect { status { isOk() } }
        assertEquals(1, ids().size)
    }

    @Test
    fun `subscribers that are not active are rejected`() {
        val changes = listOf<Subscriber.() -> Unit>(
            { health = HealthResponse(isActive = false, acceptingEvents = true) },
            { healthStatus = 500 },
            { healthStatus = 201 },
            { healthBody = "not json" },
            { healthBody = "{}" },
            { healthDelay = 2_000 },
            { server.stop(0) }
        )
        changes.forEach { change ->
            val subscriber = subscriber().apply(change)
            post(subscriber.subscription()).andExpect { status { isUnprocessableContent() } }
        }

        assertEquals(emptyList(), ids())
        assertTrue(subscriptionStore.load().isEmpty())
    }

    @Test
    fun `invalid subscriptions are rejected`() {
        val valid = subscriber().subscription()

        post(valid, "second").andExpect { status { isBadRequest() } }
        post(valid.copy(eventType = "unknown")).andExpect { status { isNotFound() } }
        listOf("", "not a url", "localhost", "ftp://localhost/health", "http://").forEach {
            post(valid.copy(healthEndpoint = it)).andExpect { status { isBadRequest() } }
            post(valid.copy(eventEndpoint = it)).andExpect { status { isBadRequest() } }
        }
        post("{}").andExpect { status { isBadRequest() } }
        post("not json").andExpect { status { isBadRequest() } }
        assertEquals(emptyList(), ids())
    }

    @Test
    fun `the oldest subscription with the same health endpoint is ejected`() {
        val subscriber = subscriber()
        val other = subscriber().subscribe()
        subscriber.subscribe(name = "old")
        val new = subscriber.subscribe(name = "new")

        assertEquals(setOf(other, new), ids().toSet())
        assertEquals(setOf(other, new), subscriptionStore.load().keys)
    }

    @Test
    fun `a health endpoint can be shared between event types`() {
        val subscriber = subscriber()
        val ids = listOf(subscriber.subscribe("first"), subscriber.subscribe("second"))

        assertEquals(ids, ids("first") + ids("second"))
    }

    @Test
    fun `subscriptions are loaded after a restart and checked first`() {
        val subscriber = subscriber()
        val id = subscriber.subscribe()
        Subscriptions.init(subscriptionStore)
        assertEquals(listOf(id), ids())

        val before = subscriber.healthChecks.get()
        Subscriptions.checkDue()
        waitFor("should be checked") { subscriber.healthChecks.get() == before + 1 }
    }

    @Test
    fun `subscriptions that fail the timer check are removed`() {
        Config.config = Config(events = emptyMap(), durations = alwaysDue)
        val healthy = subscriber().subscribe()
        val inactive = subscriber().also { it.subscribe() }
        val failing = subscriber().also { it.subscribe() }
        val stopped = subscriber().also { it.subscribe() }
        inactive.health = HealthResponse(isActive = false, acceptingEvents = true)
        failing.healthStatus = 503
        stopped.server.stop(0)

        Subscriptions.checkDue()

        waitFor("should be removed") { ids() == listOf(healthy) }
        assertEquals(setOf(healthy), subscriptionStore.load().keys)
    }

    @Test
    fun `the timer makes one check for a shared health endpoint`() {
        Config.config = Config(events = emptyMap(), durations = alwaysDue)
        val subscriber = subscriber()
        subscriber.subscribe("first")
        subscriber.subscribe("second")
        val before = subscriber.healthChecks.get()

        Subscriptions.checkDue()
        waitFor("should be checked") { subscriber.healthChecks.get() > before }
        Thread.sleep(100)

        assertEquals(before + 1, subscriber.healthChecks.get())
        subscriber.healthStatus = 500
        Subscriptions.checkDue()
        waitFor("should be removed from both") { ids("first").isEmpty() && ids("second").isEmpty() }
    }

    @Test
    fun `a subscription is only checked by the timer when it is due`() {
        val subscriber = subscriber()
        subscriber.subscribe()
        Subscriptions.checkDue()
        Thread.sleep(100)
        assertEquals(1, subscriber.healthChecks.get())

        // a shorter interval makes the subscribe check old enough, then its own check resets the timer
        Config.config = Config(events = emptyMap(), durations = durations.copy(healthCheckInterval = 2_050.milliseconds))
        Subscriptions.checkDue()
        waitFor("should be checked") { subscriber.healthChecks.get() == 2 }
        Config.config = Config(events = emptyMap(), durations = durations)
        Subscriptions.checkDue()
        Thread.sleep(100)
        assertEquals(2, subscriber.healthChecks.get())
    }

    @Test
    fun `a dispatch check resets the timer`() {
        val subscriber = subscriber()
        subscriber.subscribe()
        val before = assertNotNull(Subscriptions.of("first").single().lastHealthCheck)
        Thread.sleep(20)
        push("hello")
        Dispatcher.dispatch("first")

        assertTrue(assertNotNull(Subscriptions.of("first").single().lastHealthCheck) > before)
    }

    @Test
    fun `an event is sent with its id, type and message`() {
        val subscriber = subscriber()
        subscriber.subscribe()
        val id = Uuid.random()
        Database.push("first", id, "héllo \"日本\"")

        Dispatcher.dispatch("first")

        assertEquals(listOf(EventMessage(id, "first", "héllo \"日本\"")), subscriber.events.toList())
        assertTrue(Database.pop(id))
    }

    @Test
    fun `events are sent in order and round robin`() {
        val subscribers = List(3) { subscriber().also { it.subscribe() } }
        push("a", "b", "c", "d", "e", "f")

        Dispatcher.dispatch("first")

        assertEquals(listOf(listOf("a", "d"), listOf("b", "e"), listOf("c", "f")), subscribers.map { it.messages() })
    }

    @Test
    fun `busy subscribers are skipped and stay subscribed`() {
        val busy = subscriber().also { it.subscribe() }
        val free = subscriber().also { it.subscribe() }
        busy.health = HealthResponse(isActive = true, acceptingEvents = false)
        push("a", "b")

        Dispatcher.dispatch("first")

        assertEquals(emptyList(), busy.messages())
        assertEquals(listOf("a", "b"), free.messages())
        assertEquals(2, ids().size)
    }

    @Test
    fun `events stay waiting when every subscriber is busy`() {
        val subscriber = subscriber().also { it.subscribe() }
        subscriber.health = HealthResponse(isActive = true, acceptingEvents = false)
        push("a")

        Dispatcher.dispatch("first")

        assertEquals("a", first.borrow(durations)?.message)
    }

    @Test
    fun `subscribers that fail the check before an event are removed without being sent it`() {
        val failing = subscriber().also { it.subscribe() }
        val inactive = subscriber().also { it.subscribe() }
        val working = subscriber()
        val id = working.subscribe()
        failing.healthStatus = 500
        inactive.health = HealthResponse(isActive = false, acceptingEvents = true)
        push("a")

        Dispatcher.dispatch("first")

        assertEquals(emptyList(), failing.messages() + inactive.messages())
        assertEquals(listOf("a"), working.messages())
        assertEquals(listOf(id), ids())
    }

    @Test
    fun `a failed event goes to the next subscriber in order and its subscription is removed`() {
        val failing = subscriber().also { it.subscribe() }
        val working = subscriber()
        val id = working.subscribe()
        failing.eventStatus = 500
        push("a", "b")

        Dispatcher.dispatch("first")

        assertEquals(listOf("a"), failing.messages())
        assertEquals(listOf("a", "b"), working.messages())
        assertEquals(listOf(id), ids())
        assertEquals(setOf(id), subscriptionStore.load().keys)
    }

    @Test
    fun `an event that times out goes back to the front of the queue`() {
        val slow = subscriber().also { it.subscribe() }
        slow.eventDelay = 2_000
        push("a", "b")

        Dispatcher.dispatch("first")

        assertEquals(emptyList(), ids())
        assertEquals(listOf("a", "b"), List(2) { first.borrow(durations)?.message })
        assertNull(first.borrow(durations))
    }

    @Test
    fun `an event is received when the event endpoint returns 200`() {
        val subscriber = subscriber().also { it.subscribe() }
        push("a")

        Dispatcher.dispatch("first")
        Thread.sleep(10)

        assertNull(first.borrow(durations.copy(maxNoReceiveDuration = 1.milliseconds)))
        assertEquals("a", first.borrow(durations.copy(maxProcessingDuration = 1.milliseconds))?.message)
        assertEquals(listOf("a"), subscriber.messages())
    }

    @Test
    fun `an event is not received when the event endpoint returns another success status`() {
        listOf(201, 202, 204).forEach { status ->
            subscriber().also { it.subscribe() }.eventStatus = status
            push("$status")

            Dispatcher.dispatch("first")

            assertEquals(emptyList(), ids())
            assertEquals("$status", first.borrow(durations)?.message)
        }
    }

    @Test
    fun `an event completed before its endpoint responds is not sent again`() {
        val subscriber = subscriber().also { it.subscribe() }
        subscriber.eventDelay = 200
        push("a")
        val completing = Thread.startVirtualThread {
            waitFor("should be sent") { subscriber.events.isNotEmpty() }
            assertTrue(Database.pop(subscriber.events.single().id))
        }

        Dispatcher.dispatch("first")
        completing.join()

        assertEquals(listOf("a"), subscriber.messages())
        assertEquals(1, ids().size)
        assertNull(first.borrow(durations.copy(maxNoReceiveDuration = 0.milliseconds, maxProcessingDuration = 0.milliseconds)))
    }

    @Test
    fun `the producer of a stream event is told how to contact its subscriber`() {
        val producer = producer()
        val subscriber = streamSubscriber(StreamEventResponse("tcp://subscriber:1234", "héllo"))
        Database.push("first", Uuid.random(), "a \"日本\"", producer.address)

        Dispatcher.dispatch("first")

        waitFor("should be contacted") { producer.contacts.isNotEmpty() }
        val expected = StreamContact("a \"日本\"", "tcp://subscriber:1234", "héllo")
        assertEquals(expected, streamQueueJson.decodeFromString(producer.contacts.single()))
        assertEquals(1, subscriber.events.size)
    }

    @Test
    fun `stream bodies use the documented field names`() {
        val producer = producer()
        streamSubscriber("""{"contactAddress":"address","extra-info":"info"}""")
        Database.push("first", Uuid.random(), "a", producer.address)

        Dispatcher.dispatch("first")

        waitFor("should be contacted") { producer.contacts.isNotEmpty() }
        assertEquals("""{"message":"a","contact":"address","extra-info":"info"}""", producer.contacts.single())
    }

    @Test
    fun `extra info is optional`() {
        val producer = producer()
        streamSubscriber("""{"contactAddress":"address"}""")
        Database.push("first", Uuid.random(), "a", producer.address)

        Dispatcher.dispatch("first")

        waitFor("should be contacted") { producer.contacts.isNotEmpty() }
        assertEquals("""{"message":"a","contact":"address"}""", producer.contacts.single())
    }

    @Test
    fun `a submitted contact address is called`() {
        val producer = producer()
        streamSubscriber(StreamEventResponse("address"))
        Dispatcher.start(Database.stores.keys)

        mockMvc.post("/api/submit-event/first") {
            content = "hello"
            queryParam("contact", producer.address)
        }.andExpect { status { isOk() } }

        waitFor("should be contacted") { producer.contacts.isNotEmpty() }
        assertEquals(StreamContact("hello", "address"), streamQueueJson.decodeFromString(producer.contacts.single()))
    }

    @Test
    fun `a stream event is received without a contact address`() {
        val subscriber = streamSubscriber(StreamEventResponse("address"))
        push("a")

        Dispatcher.dispatch("first")
        Thread.sleep(10)

        assertEquals(listOf("a"), subscriber.messages())
        assertNull(first.borrow(durations.copy(maxNoReceiveDuration = 1.milliseconds)))
        assertEquals(1, ids().size)
    }

    @Test
    fun `a stream event without a valid response is a failed event`() {
        val producer = producer()
        val bodies = listOf("", "not json", "{}", "[]", """{"extra-info":"info"}""", """{"contactAddress":null}""")
        Database.push("first", Uuid.random(), "a", producer.address)
        bodies.forEach { body ->
            val subscriber = streamSubscriber(body)

            Dispatcher.dispatch("first")

            assertEquals(listOf("a"), subscriber.messages(), body)
            assertEquals(emptyList(), ids(), body)
        }
        assertEquals("a", first.borrow(durations)?.message)
        Thread.sleep(100)
        assertTrue(producer.contacts.isEmpty())
    }

    @Test
    fun `a failed stream event contacts its producer once it is received by another subscriber`() {
        val producer = producer()
        val failing = streamSubscriber(StreamEventResponse("failing")).also { it.eventStatus = 500 }
        streamSubscriber(StreamEventResponse("working"))
        Database.push("first", Uuid.random(), "a", producer.address)

        Dispatcher.dispatch("first")

        waitFor("should be contacted") { producer.contacts.isNotEmpty() }
        Thread.sleep(100)
        assertEquals(StreamContact("a", "working"), streamQueueJson.decodeFromString(producer.contacts.single()))
        assertEquals(listOf("a"), failing.messages())
    }

    @Test
    fun `nothing happens when a producer does not accept its contact`() {
        val changes = listOf<Producer.() -> Unit>({ status = 500 }, { delay = 2_000 }, { server.stop(0) })
        val subscriber = streamSubscriber(StreamEventResponse("address"))
        changes.forEach { change ->
            val producer = producer().apply(change)
            Database.push("first", Uuid.random(), "a", producer.address)
        }

        val start = TimeSource.Monotonic.markNow()
        Dispatcher.dispatch("first")
        assertTrue(start.elapsedNow() < 2.seconds, "should not wait for producers")
        Thread.sleep(1_000)

        assertEquals(listOf("a", "a", "a"), subscriber.messages())
        assertEquals(1, ids().size)
        assertNull(first.borrow(durations.copy(maxNoReceiveDuration = 1.milliseconds)))
        subscriber.events.forEach { assertTrue(Database.pop(it.id)) }
    }

    @Test
    fun `events are only sent to subscribers of their type`() {
        val subscriber = subscriber().also { it.subscribe("second") }
        push("a")

        Dispatcher.dispatch("first")
        Dispatcher.dispatch("second")

        assertEquals(emptyList(), subscriber.messages())
        assertEquals("a", first.borrow(durations)?.message)
    }

    @Test
    fun `no health check is made when there are no events`() {
        val subscriber = subscriber().also { it.subscribe() }

        Dispatcher.dispatch("first")

        assertEquals(1, subscriber.healthChecks.get())
    }

    @Test
    fun `submitting an event sends it`() {
        Dispatcher.start(Database.stores.keys)
        val subscriber = subscriber().also { it.subscribe() }

        mockMvc.post("/api/submit-event/first") { content = "hello" }.andExpect { status { isOk() } }

        waitFor("should be sent") { subscriber.messages() == listOf("hello") }
    }

    @Test
    fun `a new subscriber is sent waiting events`() {
        Dispatcher.start(Database.stores.keys)
        push("a", "b")
        val subscriber = subscriber()

        post(subscriber.subscription()).andExpect { status { isOk() } }

        waitFor("should be sent") { subscriber.messages() == listOf("a", "b") }
    }

    @Test
    fun `completing an event sends the next one`() {
        Dispatcher.start(Database.stores.keys)
        val subscriber = subscriber().also { it.subscribe() }
        push("a")
        Dispatcher.dispatch("first")
        push("b")

        mockMvc.post("/api/complete-event/${subscriber.events.single().id}").andExpect { status { isOk() } }

        waitFor("should be sent") { subscriber.messages() == listOf("a", "b") }
    }

    @Test
    fun `a timer check of a free subscriber sends waiting events of every type`() {
        Config.config = Config(events = emptyMap(), durations = alwaysDue)
        Dispatcher.start(Database.stores.keys)
        val subscriber = subscriber()
        subscriber.subscribe("first")
        subscriber.subscribe("second")
        push("a")
        push("b", type = "second")

        Subscriptions.checkDue()

        waitFor("should be sent") { subscriber.messages().toSet() == setOf("a", "b") }
    }
}
